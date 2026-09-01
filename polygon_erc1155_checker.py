#!/usr/bin/env python3
"""
Reliable asynchronous ERC-1155 activity scanner for Etherscan API V2 (v5).

Scans ERC-1155 TransferSingle and TransferBatch logs and stores unique
contract addresses. Defaults to Polygon (chain id 137), but --chain-id can
be changed for any chain supported by Etherscan API V2.

Main improvements over v4:
- shutdown-safe worker pool: a failed/cancelled worker cannot strand queue
  items and make queue.join() hang forever;
- clean distinction between user shutdown, fatal errors and normal completion;
- range-too-large responses are retried at minimum chunk size instead of
  immediately turning a transient API timeout into a fatal error;
- monotonic contiguous checkpointing with throttled state writes and a forced
  final checkpoint (re-scanning after a crash is possible; skipping blocks is not);
- stricter progress bookkeeping and split-range validation;
- Retry-After supports both seconds and HTTP-date form;
- unique atomic temp files + fsync for more durable state/output replacement;
- safer environment/CLI parsing and broader configuration controls;
- time-based progress reporting with blocks/request throughput;
- final completion is determined by contiguous scanned coverage, not merely by
  whether a shutdown signal happened near the end.

Requirements:
    pip install aiohttp

Examples:
    set ETHERSCAN_API_KEY=YOUR_KEY
    python erc1155_polygon_scanner_v5.py --blocks 5000
    python erc1155_polygon_scanner_v5.py --from-block 65000000 --to-block latest
    python erc1155_polygon_scanner_v5.py --resume
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import random
import re
import signal
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector


API_V2_URL = "https://api.etherscan.io/v2/api"
POLYGON_CHAIN_ID = 137

TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TRANSFER_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
ERC1155_TOPICS = (TRANSFER_SINGLE, TRANSFER_BATCH)

ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

TRANSIENT_MARKERS = (
    "rate limit",
    "max rate limit reached",
    "too many requests",
    "query timeout",
    "timeout occurred",
    "temporarily unavailable",
    "server too busy",
    "please try again",
    "gateway timeout",
    "service unavailable",
)

NO_RECORD_MARKERS = (
    "no records found",
    "no record found",
)

SPLIT_MARKERS = (
    "query timeout",
    "timeout occurred",
    "result window",
    "too many records",
    "response size exceeded",
    "please select a smaller result dataset",
)

logger = logging.getLogger("erc1155_scanner")


class ScannerError(RuntimeError):
    """Base scanner error."""


class ApiError(ScannerError):
    """Permanent or retry-exhausted API error."""


class SplitRange(ScannerError):
    """The current range is too large and should be split."""


class ShutdownRequested(ScannerError):
    """Cooperative shutdown requested by the user or another worker."""


@dataclass(frozen=True, slots=True)
class Config:
    api_key: str
    api_url: str
    chain_id: int

    output_file: Path
    state_file: Path

    blocks: int
    from_block: int | None
    to_block: str

    chunk_size: int
    min_chunk_size: int

    page_size: int
    split_after_pages: int
    max_pages_per_range: int

    concurrency: int
    rate_limit: float
    burst_size: int

    timeout_total: float
    timeout_connect: float

    retries: int
    backoff_base: float
    max_backoff: float

    dns_ttl: int
    confirmations: int

    resume: bool
    save_state: bool
    state_save_interval: float
    rewrite_output: bool
    fsync_output: bool

    progress_interval: float
    log_level: str


@dataclass(slots=True)
class Stats:
    started_at: float

    requests: int = 0
    retries: int = 0
    rate_limits: int = 0
    api_errors: int = 0

    ranges_completed: int = 0
    ranges_split: int = 0
    blocks_completed: int = 0

    pages: int = 0
    logs_seen: int = 0

    contracts_seen: int = 0
    new_contracts_written: int = 0

    @property
    def elapsed(self) -> float:
        return max(0.001, time.perf_counter() - self.started_at)

    @property
    def request_rate(self) -> float:
        return self.requests / self.elapsed

    @property
    def block_rate(self) -> float:
        return self.blocks_completed / self.elapsed


@dataclass(frozen=True, slots=True, order=True)
class WorkItem:
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("range start cannot be negative")
        if self.end < self.start:
            raise ValueError(f"invalid range {self.start}-{self.end}")

    @property
    def size(self) -> int:
        return self.end - self.start + 1


class ShutdownController:
    def __init__(self) -> None:
        self.event = asyncio.Event()
        self.reason: str | None = None
        self._announced = False

    def request(self, reason: str = "shutdown requested") -> None:
        if self.reason is None:
            self.reason = reason
        if not self._announced:
            logger.warning("%s; stopping cleanly after active operations...", reason)
            self._announced = True
        self.event.set()

    def raise_if_requested(self) -> None:
        if self.event.is_set():
            raise ShutdownRequested(self.reason or "shutdown requested")

    async def wait_or_timeout(self, delay: float) -> None:
        """Sleep interruptibly; raise ShutdownRequested if shutdown wins."""
        if delay <= 0:
            self.raise_if_requested()
            return
        try:
            await asyncio.wait_for(self.event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return
        raise ShutdownRequested(self.reason or "shutdown requested")


class TokenBucket:
    """Simple cooperative token bucket rate limiter."""

    def __init__(self, rate: float, burst: int, shutdown: ShutdownController) -> None:
        self.rate = rate
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()
        self.shutdown = shutdown

    async def acquire(self) -> None:
        while True:
            self.shutdown.raise_if_requested()

            async with self.lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.updated_at)
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.updated_at = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                wait = (1.0 - self.tokens) / self.rate

            await self.shutdown.wait_or_timeout(max(0.001, wait))


class ProgressTracker:
    """
    Tracks completed non-overlapping ranges and advances only through a
    contiguous prefix beginning at scan_start.

    A range completed ahead of a gap is retained in memory but is not written
    as last_scanned_block until every block before it is also complete.
    """

    def __init__(self, scan_start: int) -> None:
        if scan_start < 0:
            raise ValueError("scan_start cannot be negative")
        self.scan_start = scan_start
        self.next_block = scan_start
        self.completed: dict[int, int] = {}
        self.lock = asyncio.Lock()

    async def mark_done(self, item: WorkItem) -> int:
        async with self.lock:
            if item.end < self.next_block:
                # Duplicate completion should never happen, but treating it as
                # idempotent is safer than moving progress backwards.
                return self.next_block - 1

            previous = self.completed.get(item.start)
            if previous is not None and previous != item.end:
                raise ScannerError(
                    f"conflicting completed ranges at {item.start}: "
                    f"{previous} vs {item.end}"
                )

            self.completed[item.start] = item.end

            while True:
                end = self.completed.pop(self.next_block, None)
                if end is None:
                    break
                self.next_block = end + 1

            return self.next_block - 1

    @property
    def last_contiguous_block(self) -> int:
        return self.next_block - 1


class CheckpointWriter:
    """Throttle checkpoint writes while keeping monotonic state."""

    def __init__(
        self,
        cfg: Config,
        tracker: ProgressTracker,
        *,
        latest_at_start: int,
        scan_start: int,
        scan_target: int,
    ) -> None:
        self.cfg = cfg
        self.tracker = tracker
        self.latest_at_start = latest_at_start
        self.scan_start = scan_start
        self.scan_target = scan_target

        self.lock = asyncio.Lock()
        self.last_saved_block = tracker.last_contiguous_block
        self.last_saved_at = 0.0

    def _payload(self, last_block: int) -> dict[str, Any]:
        return {
            "version": 5,
            "chain_id": self.cfg.chain_id,
            "scan_start_block": self.scan_start,
            "scan_target_block": self.scan_target,
            "last_scanned_block": last_block,
            "latest_block_at_run": self.latest_at_start,
            "confirmations": self.cfg.confirmations,
            "output_file": str(self.cfg.output_file),
            "updated_at": int(time.time()),
        }

    async def save(self, last_block: int, *, force: bool = False) -> None:
        if not self.cfg.save_state:
            return
        if last_block <= self.last_saved_block:
            return

        now = time.monotonic()
        if not force and now - self.last_saved_at < self.cfg.state_save_interval:
            return

        async with self.lock:
            # Re-check after waiting for the lock.
            if last_block <= self.last_saved_block:
                return

            now = time.monotonic()
            if not force and now - self.last_saved_at < self.cfg.state_save_interval:
                return

            save_state_atomic(self.cfg.state_file, self._payload(last_block))
            self.last_saved_block = last_block
            self.last_saved_at = now

    async def force_current(self) -> None:
        await self.save(self.tracker.last_contiguous_block, force=True)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )


def install_signal_handlers(shutdown: ShutdownController) -> None:
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                shutdown.request,
                f"signal {getattr(sig, 'name', sig)} received",
            )
        except (NotImplementedError, RuntimeError):
            # Windows fallback.
            try:
                signal.signal(
                    sig,
                    lambda received, _frame: loop.call_soon_threadsafe(
                        shutdown.request,
                        f"signal {getattr(received, 'name', received)} received",
                    ),
                )
            except (ValueError, OSError):
                pass


def is_address(value: str) -> bool:
    return bool(ADDRESS_RE.fullmatch(value))


def normalize_address(value: Any) -> str | None:
    text = str(value).strip().lower()
    return text if is_address(text) else None


def load_addresses(path: Path) -> set[str]:
    if not path.exists():
        return set()

    addresses: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                address = normalize_address(raw)
                if address is not None:
                    addresses.add(address)
    except OSError as exc:
        raise ScannerError(f"cannot read output file {path}: {exc}") from exc

    return addresses


def _fsync_parent_directory(path: Path) -> None:
    """
    Best-effort directory fsync on POSIX so os.replace() is more crash durable.
    Windows does not expose the same portable directory-fsync semantics.
    """
    if os.name == "nt":
        return

    try:
        fd = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return

    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    tmp_path = Path(tmp_name)

    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, path)
        _fsync_parent_directory(path)
    except Exception:
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise


def write_addresses_atomic(path: Path, addresses: Iterable[str]) -> int:
    normalized = sorted(
        {
            address
            for value in addresses
            if (address := normalize_address(value)) is not None
        }
    )
    _atomic_write_text(path, "".join(f"{value}\n" for value in normalized))
    return len(normalized)


def append_addresses(path: Path, addresses: Iterable[str], *, fsync: bool) -> int:
    normalized = sorted(
        {
            address
            for value in addresses
            if (address := normalize_address(value)) is not None
        }
    )
    if not normalized:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.writelines(f"{value}\n" for value in normalized)
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())
    except OSError as exc:
        raise ScannerError(f"cannot append to output file {path}: {exc}") from exc

    return len(normalized)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Ignoring unreadable state file %s: %s", path, exc)
        return {}

    if not isinstance(value, dict):
        logger.warning("Ignoring state file %s: top-level JSON is not an object", path)
        return {}

    return value


def save_state_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    _atomic_write_text(path, text)


def retry_after_seconds(value: str | None, max_backoff: float) -> float | None:
    if not value:
        return None

    value = value.strip()

    try:
        seconds = float(value)
    except ValueError:
        seconds = -1.0

    if seconds >= 0:
        return min(max_backoff, max(0.05, seconds))

    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    seconds = (dt - datetime.now(timezone.utc)).total_seconds()
    return min(max_backoff, max(0.05, seconds))


def retry_delay(
    attempt: int,
    cfg: Config,
    retry_after: str | None = None,
) -> float:
    explicit = retry_after_seconds(retry_after, cfg.max_backoff)
    if explicit is not None:
        return explicit

    ceiling = min(
        cfg.max_backoff,
        cfg.backoff_base * (2 ** max(0, attempt - 1)),
    )
    # Full jitter helps concurrent workers stop retrying in lockstep.
    return random.uniform(0.05, max(0.05, ceiling))


def stringify_api_message(data: Mapping[str, Any]) -> str:
    parts = []
    for key in ("status", "message", "result"):
        value = data.get(key, "")
        if isinstance(value, (dict, list)):
            try:
                value = json.dumps(value, ensure_ascii=False)
            except (TypeError, ValueError):
                value = repr(value)
        parts.append(str(value))
    return " | ".join(parts).lower()


def contains_marker(text: str, markers: Sequence[str]) -> bool:
    return any(marker in text for marker in markers)


async def fetch_json(
    session: ClientSession,
    limiter: TokenBucket,
    cfg: Config,
    stats: Stats,
    shutdown: ShutdownController,
    params: Mapping[str, Any],
    *,
    allow_split: bool = False,
) -> dict[str, Any]:
    """
    Fetch JSON with application-level retries.

    allow_split=True means API responses that explicitly ask for a smaller
    dataset are surfaced immediately as SplitRange. At minimum chunk size the
    caller sets allow_split=False, so the same response is retried first and
    only becomes fatal after retries are exhausted.
    """
    query = {
        "chainid": str(cfg.chain_id),
        "apikey": cfg.api_key,
        **params,
    }

    last_error = "unknown error"

    for attempt in range(1, cfg.retries + 1):
        shutdown.raise_if_requested()
        await limiter.acquire()
        stats.requests += 1

        retry_after: str | None = None

        try:
            async with session.get(cfg.api_url, params=query) as response:
                retry_after = response.headers.get("Retry-After")
                text = await response.text(errors="replace")

                if response.status == 429:
                    stats.rate_limits += 1
                    last_error = f"HTTP 429: {text[:300]}"

                elif response.status in (408, 425) or response.status >= 500:
                    last_error = f"HTTP {response.status}: {text[:300]}"

                elif response.status >= 400:
                    stats.api_errors += 1
                    raise ApiError(f"HTTP {response.status}: {text[:800]}")

                else:
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError as exc:
                        last_error = f"invalid JSON: {exc}: {text[:300]}"
                    else:
                        if not isinstance(data, dict):
                            last_error = (
                                f"unexpected JSON type: {type(data).__name__}"
                            )
                        else:
                            combined = stringify_api_message(data)

                            if contains_marker(combined, NO_RECORD_MARKERS):
                                return data

                            if allow_split and contains_marker(combined, SPLIT_MARKERS):
                                raise SplitRange(combined[:800])

                            # Etherscan-style application error.
                            status = str(data.get("status", ""))
                            message = str(data.get("message", ""))

                            if status == "0" and message.upper() == "NOTOK":
                                if contains_marker(combined, TRANSIENT_MARKERS):
                                    if "rate limit" in combined:
                                        stats.rate_limits += 1
                                    last_error = combined[:800]
                                else:
                                    stats.api_errors += 1
                                    raise ApiError(combined[:800])
                            else:
                                return data

        except SplitRange:
            raise

        except ApiError:
            raise

        except ShutdownRequested:
            raise

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            ConnectionResetError,
            TimeoutError,
        ) as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        if attempt < cfg.retries:
            stats.retries += 1
            delay = retry_delay(attempt, cfg, retry_after)
            logger.warning(
                "Request failed (%s); retry %d/%d in %.2fs",
                last_error,
                attempt,
                cfg.retries,
                delay,
            )
            await shutdown.wait_or_timeout(delay)

    stats.api_errors += 1
    raise ApiError(
        f"request failed after {cfg.retries} attempts: {last_error}"
    )


async def latest_block(
    session: ClientSession,
    limiter: TokenBucket,
    cfg: Config,
    stats: Stats,
    shutdown: ShutdownController,
) -> int:
    data = await fetch_json(
        session,
        limiter,
        cfg,
        stats,
        shutdown,
        {
            "module": "proxy",
            "action": "eth_blockNumber",
        },
    )

    result = data.get("result")
    if not isinstance(result, str):
        raise ApiError(f"invalid eth_blockNumber response: {data}")

    try:
        value = int(result, 16)
    except ValueError as exc:
        raise ApiError(f"invalid latest block value: {result!r}") from exc

    if value < 0:
        raise ApiError(f"negative latest block value: {value}")

    return value


async def scan_topic(
    session: ClientSession,
    limiter: TokenBucket,
    cfg: Config,
    stats: Stats,
    shutdown: ShutdownController,
    item: WorkItem,
    topic0: str,
) -> set[str]:
    contracts: set[str] = set()
    page = 1

    while True:
        shutdown.raise_if_requested()

        data = await fetch_json(
            session,
            limiter,
            cfg,
            stats,
            shutdown,
            {
                "module": "logs",
                "action": "getLogs",
                "fromBlock": item.start,
                "toBlock": item.end,
                "topic0": topic0,
                "page": page,
                "offset": cfg.page_size,
            },
            allow_split=item.size > cfg.min_chunk_size,
        )

        result = data.get("result")

        if isinstance(result, str):
            lowered = result.lower()
            if contains_marker(lowered, NO_RECORD_MARKERS):
                break
            raise ApiError(
                f"invalid string getLogs result for "
                f"{item.start}-{item.end}, page {page}: {result[:800]}"
            )

        if not isinstance(result, list):
            raise ApiError(
                f"invalid getLogs response for "
                f"{item.start}-{item.end}, page {page}: {data}"
            )

        stats.pages += 1
        stats.logs_seen += len(result)

        for entry in result:
            if not isinstance(entry, dict):
                continue
            address = normalize_address(entry.get("address", ""))
            if address is not None:
                contracts.add(address)

        if len(result) < cfg.page_size:
            break

        if page >= cfg.max_pages_per_range:
            if item.size > cfg.min_chunk_size:
                raise SplitRange(
                    f"pagination ceiling reached for "
                    f"{item.start}-{item.end} "
                    f"({cfg.max_pages_per_range} full pages)"
                )
            raise ApiError(
                f"pagination safety ceiling reached for minimum-size range "
                f"{item.start}-{item.end} "
                f"({cfg.max_pages_per_range} full pages)"
            )

        page += 1

        if (
            page > cfg.split_after_pages
            and item.size > cfg.min_chunk_size
        ):
            raise SplitRange(
                f"more than {cfg.split_after_pages} full pages for "
                f"{item.start}-{item.end}"
            )

    return contracts


async def scan_item(
    session: ClientSession,
    limiter: TokenBucket,
    cfg: Config,
    stats: Stats,
    shutdown: ShutdownController,
    item: WorkItem,
) -> set[str]:
    found: set[str] = set()

    for topic in ERC1155_TOPICS:
        found.update(
            await scan_topic(
                session,
                limiter,
                cfg,
                stats,
                shutdown,
                item,
                topic,
            )
        )

    return found


def split_item(item: WorkItem) -> tuple[WorkItem, WorkItem]:
    if item.size <= 1:
        raise ValueError(f"cannot split single-block range {item}")

    middle = (item.start + item.end) // 2
    left = WorkItem(item.start, middle)
    right = WorkItem(middle + 1, item.end)

    if left.size <= 0 or right.size <= 0:
        raise ScannerError(f"invalid split result for {item}")

    if left.start != item.start or right.end != item.end:
        raise ScannerError(f"split does not cover original range {item}")

    if left.end + 1 != right.start:
        raise ScannerError(f"split created a gap/overlap for {item}")

    return left, right


def initial_ranges(start: int, end: int, size: int) -> list[WorkItem]:
    if start < 0 or end < start or size <= 0:
        raise ValueError(
            f"invalid initial range arguments: start={start}, end={end}, size={size}"
        )

    return [
        WorkItem(pos, min(end, pos + size - 1))
        for pos in range(start, end + 1, size)
    ]


async def run_workers(
    session: ClientSession,
    limiter: TokenBucket,
    cfg: Config,
    stats: Stats,
    shutdown: ShutdownController,
    ranges: Sequence[WorkItem],
    existing: set[str],
    tracker: ProgressTracker,
    checkpoint: CheckpointWriter,
) -> tuple[set[str], list[str]]:
    """
    Run a fixed-size worker pool.

    Important invariant:
    workers never terminate early merely because cooperative shutdown was
    requested. They keep consuming queued items and call task_done(), but skip
    unstarted work. Therefore queue.join() cannot be stranded by shutdown.
    """
    queue: asyncio.Queue[WorkItem] = asyncio.Queue()

    for item in ranges:
        queue.put_nowait(item)

    discovered: set[str] = set()
    discovery_lock = asyncio.Lock()

    fatal_errors: list[str] = []
    first_fatal = asyncio.Event()

    last_progress_log = time.monotonic()
    progress_lock = asyncio.Lock()

    async def maybe_log_progress() -> None:
        nonlocal last_progress_log

        now = time.monotonic()
        if now - last_progress_log < cfg.progress_interval:
            return

        async with progress_lock:
            now = time.monotonic()
            if now - last_progress_log < cfg.progress_interval:
                return

            last_progress_log = now
            logger.info(
                "Progress | ranges=%d | blocks=%d | contiguous=%d | "
                "queue=%d | logs=%d | contracts=%d | req=%.2f/s | blocks=%.1f/s",
                stats.ranges_completed,
                stats.blocks_completed,
                tracker.last_contiguous_block,
                queue.qsize(),
                stats.logs_seen,
                len(discovered),
                stats.request_rate,
                stats.block_rate,
            )

    async def worker(worker_id: int) -> None:
        while True:
            item = await queue.get()

            try:
                # After shutdown/fatal error, drain queued work without marking
                # it complete. This is what makes queue.join() deterministic.
                if shutdown.event.is_set():
                    continue

                try:
                    found = await scan_item(
                        session,
                        limiter,
                        cfg,
                        stats,
                        shutdown,
                        item,
                    )

                except SplitRange as exc:
                    if item.size <= cfg.min_chunk_size:
                        # This branch should normally be unreachable because
                        # scan_topic disables immediate split responses at the
                        # minimum size and retries them. Keep it defensive.
                        raise ApiError(
                            f"minimum-size range {item.start}-{item.end} "
                            f"still requested a split: {exc}"
                        ) from exc

                    left, right = split_item(item)

                    if left.size < cfg.min_chunk_size:
                        # For non-power-of-two sizes a naive split could create
                        # a child below the configured minimum. Shift boundary.
                        left = WorkItem(
                            item.start,
                            item.start + cfg.min_chunk_size - 1,
                        )
                        right = WorkItem(left.end + 1, item.end)

                    if right.size < cfg.min_chunk_size:
                        right = WorkItem(
                            item.end - cfg.min_chunk_size + 1,
                            item.end,
                        )
                        left = WorkItem(item.start, right.start - 1)

                    if left.size <= 0 or right.size <= 0:
                        raise ApiError(
                            f"cannot split {item.start}-{item.end} while "
                            f"respecting min chunk {cfg.min_chunk_size}"
                        )

                    stats.ranges_split += 1
                    logger.info(
                        "Splitting dense range %d-%d -> %d-%d + %d-%d",
                        item.start,
                        item.end,
                        left.start,
                        left.end,
                        right.start,
                        right.end,
                    )

                    queue.put_nowait(left)
                    queue.put_nowait(right)
                    continue

                # Serialize output discovery/update so no two workers can append
                # the same newly discovered address.
                async with discovery_lock:
                    new_now = found - existing - discovered
                    discovered.update(found)
                    stats.contracts_seen = len(discovered)

                    if new_now:
                        written = append_addresses(
                            cfg.output_file,
                            new_now,
                            fsync=cfg.fsync_output,
                        )
                        stats.new_contracts_written += written

                last_block = await tracker.mark_done(item)
                stats.ranges_completed += 1
                stats.blocks_completed += item.size

                await checkpoint.save(last_block)
                await maybe_log_progress()

            except ShutdownRequested:
                # Cooperative shutdown is not a fatal worker failure.
                # Do not re-raise; the loop must continue draining the queue.
                continue

            except asyncio.CancelledError:
                # asyncio task cancellation is reserved for supervisor teardown.
                raise

            except Exception as exc:
                message = (
                    f"worker {worker_id}, range {item.start}-{item.end}: "
                    f"{type(exc).__name__}: {exc}"
                )
                logger.error("%s", message)
                fatal_errors.append(message)

                if not first_fatal.is_set():
                    first_fatal.set()
                    shutdown.request("fatal worker error")

                # Continue looping to drain work items deterministically.

            finally:
                queue.task_done()

    workers = [
        asyncio.create_task(
            worker(index + 1),
            name=f"scanner-worker-{index + 1}",
        )
        for index in range(cfg.concurrency)
    ]

    try:
        await queue.join()
    except asyncio.CancelledError:
        shutdown.request("supervisor cancelled")
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise

    # The queue is empty and every queued item has task_done() accounted for.
    # Workers are currently waiting on queue.get(), so cancellation is the
    # cleanest deterministic teardown.
    for task in workers:
        task.cancel()

    await asyncio.gather(*workers, return_exceptions=True)

    # Force persistence of any contiguous progress that was intentionally
    # throttled during the run.
    await checkpoint.force_current()

    return discovered, fatal_errors


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")

    return parsed


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc

    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")

    return parsed


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")

    return parsed


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Reliable ERC-1155 log scanner using Etherscan API V2"
    )

    parser.add_argument(
        "--api-key",
        default=os.getenv("ETHERSCAN_API_KEY")
        or os.getenv("POLYGONSCAN_API_KEY", ""),
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("ETHERSCAN_API_URL", API_V2_URL),
    )
    parser.add_argument(
        "--chain-id",
        type=_positive_int,
        default=os.getenv("CHAIN_ID", str(POLYGON_CHAIN_ID)),
    )

    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_FILE", "erc1155_addresses.txt"),
    )
    parser.add_argument(
        "--state",
        default=os.getenv("STATE_FILE", "erc1155_scanner_state.json"),
    )

    parser.add_argument(
        "--blocks",
        type=_positive_int,
        default=os.getenv("BLOCKS_TO_SCAN", "500"),
        help="number of trailing blocks when --from-block is omitted",
    )
    parser.add_argument(
        "--from-block",
        type=_nonnegative_int,
    )
    parser.add_argument(
        "--to-block",
        default=os.getenv("TO_BLOCK", "latest"),
    )

    parser.add_argument(
        "--chunk-size",
        type=_positive_int,
        default=os.getenv("CHUNK_SIZE", "100"),
    )
    parser.add_argument(
        "--min-chunk-size",
        type=_positive_int,
        default=os.getenv("MIN_CHUNK_SIZE", "1"),
    )

    parser.add_argument(
        "--page-size",
        type=_positive_int,
        default=os.getenv("PAGE_SIZE", "1000"),
    )
    parser.add_argument(
        "--split-after-pages",
        type=_positive_int,
        default=os.getenv("SPLIT_AFTER_PAGES", "10"),
    )
    parser.add_argument(
        "--max-pages-per-range",
        type=_positive_int,
        default=os.getenv("MAX_PAGES_PER_RANGE", "1000"),
    )

    parser.add_argument(
        "--concurrency",
        type=_positive_int,
        default=os.getenv("CONCURRENCY", "4"),
    )
    parser.add_argument(
        "--rate",
        type=_positive_float,
        default=os.getenv("RATE_LIMIT_PER_SEC", "4.5"),
    )
    parser.add_argument(
        "--burst",
        type=_positive_int,
        default=os.getenv("BURST_SIZE", "4"),
    )

    parser.add_argument(
        "--timeout",
        dest="timeout_total",
        type=_positive_float,
        default=os.getenv("TIMEOUT", "30"),
        help="total timeout per HTTP request",
    )
    parser.add_argument(
        "--connect-timeout",
        type=_positive_float,
        default=os.getenv("CONNECT_TIMEOUT", "10"),
    )

    parser.add_argument(
        "--retries",
        type=_positive_int,
        default=os.getenv("RETRY_LIMIT", "7"),
    )
    parser.add_argument(
        "--backoff-base",
        type=_positive_float,
        default=os.getenv("BACKOFF_BASE", "1"),
    )
    parser.add_argument(
        "--max-backoff",
        type=_positive_float,
        default=os.getenv("MAX_BACKOFF", "60"),
    )

    parser.add_argument(
        "--dns-ttl",
        type=_positive_int,
        default=os.getenv("DNS_TTL", "300"),
    )
    parser.add_argument(
        "--confirmations",
        type=_nonnegative_int,
        default=os.getenv("CONFIRMATIONS", "0"),
        help="when --to-block=latest, stop this many blocks behind chain head",
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="continue from last_scanned_block + 1 in the state file",
    )
    parser.add_argument(
        "--no-state",
        action="store_true",
        help="disable checkpoint state writes",
    )
    parser.add_argument(
        "--state-save-interval",
        type=_positive_float,
        default=os.getenv("STATE_SAVE_INTERVAL", "2"),
        help="minimum seconds between non-final checkpoint writes",
    )

    parser.add_argument(
        "--rewrite-output",
        action="store_true",
        help="sort and deduplicate output before and after scanning",
    )
    parser.add_argument(
        "--no-fsync-output",
        action="store_true",
        help="skip fsync after incremental output appends (faster, less durable)",
    )

    parser.add_argument(
        "--progress-interval",
        type=_positive_float,
        default=os.getenv("PROGRESS_INTERVAL", "10"),
        help="seconds between progress log lines",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default=os.getenv("LOG_LEVEL", "INFO").upper(),
    )

    args = parser.parse_args()

    api_key = str(args.api_key).strip()
    api_url = str(args.api_url).strip()

    if not api_key:
        parser.error(
            "set ETHERSCAN_API_KEY (preferred), POLYGONSCAN_API_KEY, "
            "or pass --api-key"
        )

    if not api_url.startswith(("https://", "http://")):
        parser.error("--api-url must start with http:// or https://")

    if args.min_chunk_size > args.chunk_size:
        parser.error("--min-chunk-size cannot exceed --chunk-size")

    if not 1 <= args.page_size <= 1000:
        parser.error("--page-size must be between 1 and 1000")

    if args.split_after_pages >= args.max_pages_per_range:
        parser.error(
            "--split-after-pages must be smaller than --max-pages-per-range"
        )

    if args.max_backoff < args.backoff_base:
        parser.error("--max-backoff cannot be smaller than --backoff-base")

    to_block = str(args.to_block).strip().lower()
    if to_block != "latest":
        try:
            parsed_to = int(to_block)
        except ValueError:
            parser.error("--to-block must be a non-negative integer or 'latest'")
        if parsed_to < 0:
            parser.error("--to-block cannot be negative")
        to_block = str(parsed_to)

    return Config(
        api_key=api_key,
        api_url=api_url,
        chain_id=args.chain_id,

        output_file=Path(args.output),
        state_file=Path(args.state),

        blocks=args.blocks,
        from_block=args.from_block,
        to_block=to_block,

        chunk_size=args.chunk_size,
        min_chunk_size=args.min_chunk_size,

        page_size=args.page_size,
        split_after_pages=args.split_after_pages,
        max_pages_per_range=args.max_pages_per_range,

        concurrency=args.concurrency,
        rate_limit=args.rate,
        burst_size=args.burst,

        timeout_total=args.timeout_total,
        timeout_connect=min(args.connect_timeout, args.timeout_total),

        retries=args.retries,
        backoff_base=args.backoff_base,
        max_backoff=args.max_backoff,

        dns_ttl=args.dns_ttl,
        confirmations=args.confirmations,

        resume=args.resume,
        save_state=not args.no_state,
        state_save_interval=args.state_save_interval,
        rewrite_output=args.rewrite_output,
        fsync_output=not args.no_fsync_output,

        progress_interval=args.progress_interval,
        log_level=args.log_level,
    )


def resolve_resume_start(
    cfg: Config,
    state: Mapping[str, Any],
    to_block: int,
) -> int:
    if cfg.from_block is not None:
        return cfg.from_block

    if cfg.resume:
        state_chain = state.get("chain_id")
        if state_chain is not None and state_chain != cfg.chain_id:
            raise ScannerError(
                f"state file belongs to chain_id={state_chain}, "
                f"current chain_id={cfg.chain_id}"
            )

        last_scanned = state.get("last_scanned_block")
        if isinstance(last_scanned, int) and last_scanned >= -1:
            state_output = state.get("output_file")
            if state_output:
                try:
                    same_output = (
                        Path(str(state_output)).resolve()
                        == cfg.output_file.resolve()
                    )
                except OSError:
                    same_output = Path(str(state_output)) == cfg.output_file

                if not same_output:
                    logger.warning(
                        "State was created with output %s, current output is %s; "
                        "resume block is valid, but old addresses may live in "
                        "the previous output file",
                        state_output,
                        cfg.output_file,
                    )

            return last_scanned + 1

        logger.warning(
            "--resume requested but state contains no valid "
            "last_scanned_block; falling back to --blocks window"
        )

    return max(0, to_block - cfg.blocks + 1)


async def async_main(cfg: Config) -> int:
    shutdown = ShutdownController()
    install_signal_handlers(shutdown)

    stats = Stats(started_at=time.perf_counter())

    existing = load_addresses(cfg.output_file)

    if cfg.rewrite_output and cfg.output_file.exists():
        write_addresses_atomic(cfg.output_file, existing)

    timeout = ClientTimeout(
        total=cfg.timeout_total,
        connect=cfg.timeout_connect,
    )

    connector = TCPConnector(
        limit=max(10, cfg.concurrency * 3),
        limit_per_host=max(5, cfg.concurrency * 2),
        ttl_dns_cache=cfg.dns_ttl,
        enable_cleanup_closed=True,
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "erc1155-etherscan-v2-scanner/5.0",
    }

    tracker: ProgressTracker | None = None
    to_block: int | None = None
    discovered: set[str] = set()
    errors: list[str] = []

    async with ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    ) as session:
        limiter = TokenBucket(
            cfg.rate_limit,
            cfg.burst_size,
            shutdown,
        )

        chain_head = await latest_block(
            session,
            limiter,
            cfg,
            stats,
            shutdown,
        )

        if cfg.to_block == "latest":
            to_block = max(0, chain_head - cfg.confirmations)
        else:
            to_block = int(cfg.to_block)

        if to_block > chain_head:
            logger.warning(
                "Requested to-block %d is above current head %d; clamping",
                to_block,
                chain_head,
            )
            to_block = chain_head

        state = load_state(cfg.state_file) if cfg.resume else {}
        from_block = resolve_resume_start(cfg, state, to_block)

        if from_block > to_block:
            logger.info(
                "Nothing to scan: start=%d is above target=%d",
                from_block,
                to_block,
            )
            return 0

        ranges = initial_ranges(
            from_block,
            to_block,
            cfg.chunk_size,
        )

        tracker = ProgressTracker(from_block)
        checkpoint = CheckpointWriter(
            cfg,
            tracker,
            latest_at_start=chain_head,
            scan_start=from_block,
            scan_target=to_block,
        )

        logger.info(
            "Scanning chain=%d blocks=%d-%d (%d blocks) | "
            "ranges=%d chunk=%d min_chunk=%d | concurrency=%d | "
            "rate=%.2f/s burst=%d | existing=%d | confirmations=%d",
            cfg.chain_id,
            from_block,
            to_block,
            to_block - from_block + 1,
            len(ranges),
            cfg.chunk_size,
            cfg.min_chunk_size,
            cfg.concurrency,
            cfg.rate_limit,
            cfg.burst_size,
            len(existing),
            cfg.confirmations,
        )

        discovered, errors = await run_workers(
            session,
            limiter,
            cfg,
            stats,
            shutdown,
            ranges,
            existing,
            tracker,
            checkpoint,
        )

    assert tracker is not None
    assert to_block is not None

    all_addresses = existing | discovered

    if cfg.rewrite_output:
        write_addresses_atomic(
            cfg.output_file,
            all_addresses,
        )

    complete = tracker.last_contiguous_block >= to_block

    if errors:
        status = "STOPPED WITH ERRORS"
    elif complete:
        status = "DONE"
    elif shutdown.event.is_set():
        status = "STOPPED"
    else:
        status = "INCOMPLETE"

    logger.info("=" * 78)
    logger.info("%s", status)
    logger.info("Target block:           %d", to_block)
    logger.info("Last contiguous block:  %d", tracker.last_contiguous_block)
    logger.info("Ranges completed:       %d", stats.ranges_completed)
    logger.info("Ranges split:           %d", stats.ranges_split)
    logger.info("Blocks completed:       %d", stats.blocks_completed)
    logger.info("Requests / retries:     %d / %d", stats.requests, stats.retries)
    logger.info("Pages / logs:           %d / %d", stats.pages, stats.logs_seen)
    logger.info("Contracts in range:     %d", len(discovered))
    logger.info("New contracts:          %d", len(discovered - existing))
    logger.info("Incremental writes:     %d", stats.new_contracts_written)
    logger.info("Total output:           %d", len(all_addresses))
    logger.info("Rate limits:            %d", stats.rate_limits)
    logger.info("API errors:             %d", stats.api_errors)
    logger.info("Elapsed:                %.2fs", stats.elapsed)
    logger.info("Average request rate:   %.2f req/s", stats.request_rate)
    logger.info("Average scan rate:      %.2f blocks/s", stats.block_rate)
    logger.info("Output:                 %s", cfg.output_file)
    if cfg.save_state:
        logger.info("State:                  %s", cfg.state_file)
    if shutdown.reason:
        logger.info("Stop reason:            %s", shutdown.reason)
    logger.info("=" * 78)

    if errors:
        for error in errors[:10]:
            logger.error("%s", error)
        if len(errors) > 10:
            logger.error("... and %d more worker errors", len(errors) - 10)
        return 2

    if complete:
        return 0

    if shutdown.event.is_set():
        return 130

    return 2


def main() -> int:
    try:
        cfg = parse_args()
        configure_logging(cfg.log_level)
        return asyncio.run(async_main(cfg))

    except KeyboardInterrupt:
        return 130

    except (ScannerError, OSError, ValueError) as exc:
        # configure_logging may not have run if CLI/env parsing failed.
        if not logger.handlers and not logging.getLogger().handlers:
            configure_logging("INFO")
        logger.error("Fatal error: %s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

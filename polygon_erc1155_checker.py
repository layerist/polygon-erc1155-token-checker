#!/usr/bin/env python3
"""Reliable asynchronous Polygon ERC-1155 activity scanner.

Scans ERC-1155 TransferSingle and TransferBatch logs and stores unique
contract addresses. Uses Etherscan API V2 with Polygon chain id 137.

Highlights:
- Etherscan API V2 support (chainid=137)
- bounded worker queue instead of creating one task per range
- adaptive block-range splitting on query timeout / result-window saturation
- retries for HTTP, API-level, malformed JSON and transient errors
- progress checkpoint stores only contiguous completed ranges
- graceful shutdown without falsely marking unfinished blocks as scanned
- atomic state writes and optional output rewrite/deduplication
- strict address/block/config validation

Requirements:
    pip install aiohttp

Examples:
    set ETHERSCAN_API_KEY=YOUR_KEY
    python erc1155_polygon_scanner_v3.py --blocks 5000
    python erc1155_polygon_scanner_v3.py --from-block 65000000 --to-block latest
    python erc1155_polygon_scanner_v3.py --resume
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import re
import signal
import sys
import time
from dataclasses import asdict, dataclass
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
)
NO_RECORD_MARKERS = ("no records found", "no record found")
SPLIT_MARKERS = ("query timeout", "timeout occurred", "result window", "too many records")

logger = logging.getLogger("erc1155_scanner")


class ScannerError(RuntimeError):
    """Base scanner error."""


class ApiError(ScannerError):
    """Permanent API error."""


class SplitRange(ScannerError):
    """The current range is too large and must be split."""


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
    concurrency: int
    rate_limit: float
    burst_size: int
    timeout: float
    retries: int
    backoff_base: float
    max_backoff: float
    dns_ttl: int
    resume: bool
    save_state: bool
    rewrite_output: bool
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
    pages: int = 0
    logs_seen: int = 0
    contracts_seen: int = 0

    @property
    def elapsed(self) -> float:
        return max(0.001, time.perf_counter() - self.started_at)


@dataclass(frozen=True, slots=True)
class WorkItem:
    start: int
    end: int

    @property
    def size(self) -> int:
        return self.end - self.start + 1


class ShutdownController:
    def __init__(self) -> None:
        self.event = asyncio.Event()
        self._announced = False

    def request(self) -> None:
        if not self._announced:
            logger.warning("Shutdown requested; stopping after active requests...")
            self._announced = True
        self.event.set()


class TokenBucket:
    def __init__(self, rate: float, burst: int, shutdown: ShutdownController) -> None:
        self.rate = rate
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()
        self.shutdown = shutdown

    async def acquire(self) -> None:
        while True:
            if self.shutdown.event.is_set():
                raise asyncio.CancelledError
            async with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated_at) * self.rate)
                self.updated_at = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rate
            try:
                await asyncio.wait_for(self.shutdown.event.wait(), timeout=max(0.01, wait))
            except asyncio.TimeoutError:
                pass


class ProgressTracker:
    """Tracks completed ranges and advances only through a contiguous prefix."""

    def __init__(self, scan_start: int) -> None:
        self.next_block = scan_start
        self.completed: dict[int, int] = {}
        self.lock = asyncio.Lock()

    async def mark_done(self, item: WorkItem) -> int:
        async with self.lock:
            self.completed[item.start] = item.end
            while self.next_block in self.completed:
                self.next_block = self.completed.pop(self.next_block) + 1
            return self.next_block - 1

    @property
    def last_contiguous_block(self) -> int:
        return self.next_block - 1


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )


def install_signal_handlers(shutdown: ShutdownController) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.request)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(shutdown.request))
            except (ValueError, OSError):
                pass


def is_address(value: str) -> bool:
    return bool(ADDRESS_RE.fullmatch(value))


def load_addresses(path: Path) -> set[str]:
    if not path.exists():
        return set()
    addresses: set[str] = set()
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        value = raw.strip().lower()
        if is_address(value):
            addresses.add(value)
    return addresses


def write_addresses_atomic(path: Path, addresses: Iterable[str]) -> int:
    normalized = sorted({value.lower() for value in addresses if is_address(value)})
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("".join(f"{value}\n" for value in normalized), encoding="utf-8", newline="\n")
    tmp.replace(path)
    return len(normalized)


def append_addresses(path: Path, addresses: Iterable[str]) -> int:
    normalized = sorted({value.lower() for value in addresses if is_address(value)})
    if not normalized:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.writelines(f"{value}\n" for value in normalized)
        handle.flush()
        os.fsync(handle.fileno())
    return len(normalized)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Ignoring unreadable state file %s: %s", path, exc)
        return {}


def save_state_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def retry_delay(attempt: int, cfg: Config, retry_after: str | None = None) -> float:
    if retry_after:
        try:
            return min(cfg.max_backoff, max(0.1, float(retry_after)))
        except ValueError:
            pass
    ceiling = min(cfg.max_backoff, cfg.backoff_base * (2 ** (attempt - 1)))
    return random.uniform(0.25, max(0.25, ceiling))


def stringify_api_message(data: Mapping[str, Any]) -> str:
    return " | ".join(str(data.get(key, "")) for key in ("status", "message", "result")).lower()


async def fetch_json(
    session: ClientSession,
    limiter: TokenBucket,
    cfg: Config,
    stats: Stats,
    shutdown: ShutdownController,
    params: Mapping[str, Any],
) -> dict[str, Any]:
    query = {"chainid": str(cfg.chain_id), "apikey": cfg.api_key, **params}
    last_error = "unknown error"

    for attempt in range(1, cfg.retries + 1):
        if shutdown.event.is_set():
            raise asyncio.CancelledError
        await limiter.acquire()
        stats.requests += 1
        try:
            async with session.get(cfg.api_url, params=query) as response:
                text = await response.text(errors="replace")
                retry_after = response.headers.get("Retry-After")

                if response.status == 429:
                    stats.rate_limits += 1
                    last_error = f"HTTP 429: {text[:200]}"
                elif response.status >= 500:
                    last_error = f"HTTP {response.status}: {text[:200]}"
                elif response.status >= 400:
                    raise ApiError(f"HTTP {response.status}: {text[:500]}")
                else:
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError as exc:
                        last_error = f"invalid JSON: {exc}: {text[:200]}"
                    else:
                        if not isinstance(data, dict):
                            last_error = f"unexpected JSON type: {type(data).__name__}"
                        else:
                            combined = stringify_api_message(data)
                            if any(marker in combined for marker in NO_RECORD_MARKERS):
                                return data
                            if any(marker in combined for marker in SPLIT_MARKERS):
                                raise SplitRange(combined[:500])
                            if data.get("status") == "0" and data.get("message") == "NOTOK":
                                if any(marker in combined for marker in TRANSIENT_MARKERS):
                                    if "rate limit" in combined:
                                        stats.rate_limits += 1
                                    last_error = combined[:500]
                                else:
                                    stats.api_errors += 1
                                    raise ApiError(combined[:500])
                            else:
                                return data

        except SplitRange:
            raise
        except ApiError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        if attempt < cfg.retries:
            stats.retries += 1
            delay = retry_delay(attempt, cfg, retry_after if 'retry_after' in locals() else None)
            logger.warning("Request failed (%s); retry %d/%d in %.2fs", last_error, attempt, cfg.retries, delay)
            try:
                await asyncio.wait_for(shutdown.event.wait(), timeout=delay)
                raise asyncio.CancelledError
            except asyncio.TimeoutError:
                pass

    raise ApiError(f"request failed after {cfg.retries} attempts: {last_error}")


async def latest_block(
    session: ClientSession, limiter: TokenBucket, cfg: Config, stats: Stats, shutdown: ShutdownController
) -> int:
    data = await fetch_json(
        session, limiter, cfg, stats, shutdown, {"module": "proxy", "action": "eth_blockNumber"}
    )
    result = data.get("result")
    if not isinstance(result, str):
        raise ApiError(f"invalid eth_blockNumber response: {data}")
    try:
        return int(result, 16)
    except ValueError as exc:
        raise ApiError(f"invalid latest block value: {result!r}") from exc


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

    while not shutdown.event.is_set():
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
        )
        result = data.get("result")
        if isinstance(result, str) and any(marker in result.lower() for marker in NO_RECORD_MARKERS):
            break
        if not isinstance(result, list):
            raise ApiError(f"invalid getLogs response for {item.start}-{item.end}: {data}")

        stats.pages += 1
        stats.logs_seen += len(result)
        for entry in result:
            if isinstance(entry, dict):
                address = str(entry.get("address", "")).lower()
                if is_address(address):
                    contracts.add(address)

        if len(result) < cfg.page_size:
            break

        # A full page can mean there are more pages. At a practical API page
        # ceiling, split the range instead of risking a silently truncated scan.
        page += 1
        if page > 10:
            raise SplitRange(f"more than 10 full pages for {item.start}-{item.end}")

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
        found.update(await scan_topic(session, limiter, cfg, stats, shutdown, item, topic))
    return found


def split_item(item: WorkItem) -> tuple[WorkItem, WorkItem]:
    middle = (item.start + item.end) // 2
    return WorkItem(item.start, middle), WorkItem(middle + 1, item.end)


def initial_ranges(start: int, end: int, size: int) -> list[WorkItem]:
    return [WorkItem(pos, min(end, pos + size - 1)) for pos in range(start, end + 1, size)]


async def run_workers(
    session: ClientSession,
    limiter: TokenBucket,
    cfg: Config,
    stats: Stats,
    shutdown: ShutdownController,
    ranges: Sequence[WorkItem],
    existing: set[str],
    tracker: ProgressTracker,
    latest_at_start: int,
) -> tuple[set[str], list[str]]:
    queue: asyncio.Queue[WorkItem | None] = asyncio.Queue()
    for item in ranges:
        queue.put_nowait(item)

    discovered: set[str] = set()
    discovery_lock = asyncio.Lock()
    fatal_errors: list[str] = []

    async def checkpoint(last_block: int) -> None:
        if not cfg.save_state:
            return
        save_state_atomic(
            cfg.state_file,
            {
                "version": 3,
                "chain_id": cfg.chain_id,
                "last_scanned_block": last_block,
                "latest_block_at_run": latest_at_start,
                "output_file": str(cfg.output_file),
                "updated_at": int(time.time()),
            },
        )

    async def worker(worker_id: int) -> None:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                return
            if shutdown.event.is_set():
                queue.task_done()
                continue
            try:
                try:
                    found = await scan_item(session, limiter, cfg, stats, shutdown, item)
                except SplitRange as exc:
                    if item.size <= cfg.min_chunk_size:
                        raise ApiError(
                            f"range {item.start}-{item.end} still too dense at minimum chunk size: {exc}"
                        ) from exc
                    left, right = split_item(item)
                    stats.ranges_split += 1
                    logger.info("Splitting dense range %d-%d -> %d-%d + %d-%d", item.start, item.end, left.start, left.end, right.start, right.end)
                    queue.put_nowait(left)
                    queue.put_nowait(right)
                    continue

                async with discovery_lock:
                    new_now = found - existing - discovered
                    discovered.update(found)
                    stats.contracts_seen = len(discovered)
                    if new_now:
                        append_addresses(cfg.output_file, new_now)

                last_block = await tracker.mark_done(item)
                await checkpoint(last_block)
                stats.ranges_completed += 1
                if stats.ranges_completed % 10 == 0:
                    logger.info(
                        "Ranges=%d | Last contiguous=%d | Logs=%d | Contracts=%d | %.1f req/s",
                        stats.ranges_completed,
                        tracker.last_contiguous_block,
                        stats.logs_seen,
                        len(discovered),
                        stats.requests / stats.elapsed,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                message = f"worker {worker_id}, range {item.start}-{item.end}: {type(exc).__name__}: {exc}"
                logger.error("%s", message)
                fatal_errors.append(message)
                shutdown.request()
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker(i + 1), name=f"scanner-worker-{i + 1}") for i in range(cfg.concurrency)]
    await queue.join()
    for _ in workers:
        queue.put_nowait(None)
    await asyncio.gather(*workers, return_exceptions=True)
    return discovered, fatal_errors


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Reliable Polygon ERC-1155 log scanner")
    parser.add_argument("--api-key", default=os.getenv("ETHERSCAN_API_KEY") or os.getenv("POLYGONSCAN_API_KEY", ""))
    parser.add_argument("--api-url", default=os.getenv("ETHERSCAN_API_URL", API_V2_URL))
    parser.add_argument("--chain-id", type=int, default=int(os.getenv("CHAIN_ID", str(POLYGON_CHAIN_ID))))
    parser.add_argument("--output", default=os.getenv("OUTPUT_FILE", "erc1155_addresses.txt"))
    parser.add_argument("--state", default=os.getenv("STATE_FILE", "erc1155_scanner_state.json"))
    parser.add_argument("--blocks", type=int, default=int(os.getenv("BLOCKS_TO_SCAN", "500")))
    parser.add_argument("--from-block", type=int)
    parser.add_argument("--to-block", default=os.getenv("TO_BLOCK", "latest"))
    parser.add_argument("--chunk-size", type=int, default=int(os.getenv("CHUNK_SIZE", "100")))
    parser.add_argument("--min-chunk-size", type=int, default=int(os.getenv("MIN_CHUNK_SIZE", "1")))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("PAGE_SIZE", "1000")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "4")))
    parser.add_argument("--rate", type=float, default=float(os.getenv("RATE_LIMIT_PER_SEC", "4.5")))
    parser.add_argument("--burst", type=int, default=int(os.getenv("BURST_SIZE", "4")))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("TIMEOUT", "30")))
    parser.add_argument("--retries", type=int, default=int(os.getenv("RETRY_LIMIT", "7")))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-state", action="store_true")
    parser.add_argument("--rewrite-output", action="store_true", help="Sort and deduplicate the output before scanning")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default=os.getenv("LOG_LEVEL", "INFO").upper())
    args = parser.parse_args()

    if not args.api_key:
        parser.error("set ETHERSCAN_API_KEY (preferred), POLYGONSCAN_API_KEY, or pass --api-key")
    if args.chain_id <= 0:
        parser.error("--chain-id must be positive")
    if args.blocks <= 0 or args.chunk_size <= 0 or args.min_chunk_size <= 0:
        parser.error("--blocks, --chunk-size and --min-chunk-size must be positive")
    if args.min_chunk_size > args.chunk_size:
        parser.error("--min-chunk-size cannot exceed --chunk-size")
    if not 1 <= args.page_size <= 1000:
        parser.error("--page-size must be between 1 and 1000")
    if args.concurrency <= 0 or args.rate <= 0 or args.burst <= 0:
        parser.error("--concurrency, --rate and --burst must be positive")
    if args.timeout <= 0 or args.retries <= 0:
        parser.error("--timeout and --retries must be positive")
    if args.from_block is not None and args.from_block < 0:
        parser.error("--from-block cannot be negative")
    if str(args.to_block).lower() != "latest":
        try:
            parsed_to = int(args.to_block)
        except ValueError:
            parser.error("--to-block must be a non-negative integer or 'latest'")
        if parsed_to < 0:
            parser.error("--to-block cannot be negative")

    return Config(
        api_key=args.api_key,
        api_url=args.api_url,
        chain_id=args.chain_id,
        output_file=Path(args.output),
        state_file=Path(args.state),
        blocks=args.blocks,
        from_block=args.from_block,
        to_block=str(args.to_block),
        chunk_size=args.chunk_size,
        min_chunk_size=args.min_chunk_size,
        page_size=args.page_size,
        concurrency=args.concurrency,
        rate_limit=args.rate,
        burst_size=args.burst,
        timeout=args.timeout,
        retries=args.retries,
        backoff_base=1.0,
        max_backoff=60.0,
        dns_ttl=300,
        resume=args.resume,
        save_state=not args.no_state,
        rewrite_output=args.rewrite_output,
        log_level=args.log_level,
    )


async def async_main(cfg: Config) -> int:
    shutdown = ShutdownController()
    install_signal_handlers(shutdown)
    stats = Stats(started_at=time.perf_counter())

    existing = load_addresses(cfg.output_file)
    if cfg.rewrite_output and cfg.output_file.exists():
        write_addresses_atomic(cfg.output_file, existing)

    timeout = ClientTimeout(total=cfg.timeout, connect=min(10.0, cfg.timeout))
    connector = TCPConnector(
        limit=max(10, cfg.concurrency * 3),
        limit_per_host=max(5, cfg.concurrency * 2),
        ttl_dns_cache=cfg.dns_ttl,
        enable_cleanup_closed=True,
    )
    headers = {"Accept": "application/json", "User-Agent": "erc1155-polygon-scanner/3.0"}

    async with ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        limiter = TokenBucket(cfg.rate_limit, cfg.burst_size, shutdown)
        chain_head = await latest_block(session, limiter, cfg, stats, shutdown)
        to_block = chain_head if cfg.to_block.lower() == "latest" else int(cfg.to_block)
        if to_block > chain_head:
            logger.warning("Requested to-block %d is above current head %d; clamping", to_block, chain_head)
            to_block = chain_head

        state = load_state(cfg.state_file) if cfg.resume else {}
        if cfg.from_block is not None:
            from_block = cfg.from_block
        elif cfg.resume and isinstance(state.get("last_scanned_block"), int):
            if state.get("chain_id") not in (None, cfg.chain_id):
                raise ScannerError("state file belongs to a different chain_id")
            from_block = int(state["last_scanned_block"]) + 1
        else:
            from_block = max(0, to_block - cfg.blocks + 1)

        if from_block > to_block:
            logger.info("Nothing to scan: %d > %d", from_block, to_block)
            return 0

        ranges = initial_ranges(from_block, to_block, cfg.chunk_size)
        tracker = ProgressTracker(from_block)
        logger.info(
            "Scanning chain=%d blocks=%d-%d ranges=%d chunk=%d concurrency=%d existing=%d",
            cfg.chain_id, from_block, to_block, len(ranges), cfg.chunk_size, cfg.concurrency, len(existing),
        )

        discovered, errors = await run_workers(
            session, limiter, cfg, stats, shutdown, ranges, existing, tracker, chain_head
        )

    all_addresses = existing | discovered
    if cfg.rewrite_output:
        write_addresses_atomic(cfg.output_file, all_addresses)

    logger.info("=" * 72)
    logger.info("%s", "STOPPED WITH ERRORS" if errors else ("STOPPED" if shutdown.event.is_set() else "DONE"))
    logger.info("Last contiguous block: %d", tracker.last_contiguous_block)
    logger.info("Ranges completed:      %d", stats.ranges_completed)
    logger.info("Ranges split:          %d", stats.ranges_split)
    logger.info("Requests / retries:    %d / %d", stats.requests, stats.retries)
    logger.info("Pages / logs:          %d / %d", stats.pages, stats.logs_seen)
    logger.info("Contracts in range:    %d", len(discovered))
    logger.info("New contracts:         %d", len(discovered - existing))
    logger.info("Total output:          %d", len(all_addresses))
    logger.info("Rate limits:           %d", stats.rate_limits)
    logger.info("Elapsed:               %.2fs", stats.elapsed)
    logger.info("Output:                %s", cfg.output_file)
    logger.info("=" * 72)

    if errors:
        for error in errors[:10]:
            logger.error("%s", error)
        return 2
    return 130 if shutdown.event.is_set() else 0


def main() -> int:
    cfg = parse_args()
    configure_logging(cfg.log_level)
    try:
        return asyncio.run(async_main(cfg))
    except KeyboardInterrupt:
        return 130
    except (ScannerError, OSError, ValueError) as exc:
        logger.error("Fatal error: %s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

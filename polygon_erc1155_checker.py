#!/usr/bin/env python3
"""
Fast Polygon ERC1155 activity scanner.

Core idea:
- Do NOT scan block transactions and then check every tx.from / tx.to address.
- Scan logs directly by ERC1155 TransferSingle / TransferBatch topic0.
- The ERC1155 contract address is log["address"].

Requirements:
    pip install aiohttp

Examples:
    export POLYGONSCAN_API_KEY=YOUR_KEY
    python erc1155_polygon_scanner_improved.py --blocks 500
    python erc1155_polygon_scanner_improved.py --from-block 65000000 --to-block latest
    python erc1155_polygon_scanner_improved.py --blocks 2000 --chunk-size 100 --page-size 1000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import aiohttp
from aiohttp import ClientTimeout, TCPConnector


# ==========================================================
# CONSTANTS
# ==========================================================

POLYGONSCAN_URL = "https://api.polygonscan.com/api"

# keccak256("TransferSingle(address,address,address,uint256,uint256)")
ERC1155_TRANSFER_SINGLE = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)

# keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
ERC1155_TRANSFER_BATCH = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)

ERC1155_TOPICS = (
    ERC1155_TRANSFER_SINGLE,
    ERC1155_TRANSFER_BATCH,
)

NO_RECORDS_MARKERS = (
    "no records found",
    "no record found",
)

RATE_LIMIT_MARKERS = (
    "rate limit",
    "max rate limit reached",
    "too many requests",
)


# ==========================================================
# CONFIG / STATS
# ==========================================================

@dataclass(frozen=True)
class Config:
    api_key: str
    base_url: str
    output_file: Path
    state_file: Path
    blocks: int
    from_block: Optional[int]
    to_block: str
    chunk_size: int
    page_size: int
    rate_limit_per_sec: float
    burst_size: int
    concurrency: int
    timeout: float
    retry_limit: int
    backoff_base: float
    max_backoff: float
    dns_ttl: int
    resume: bool
    save_state: bool


@dataclass
class Stats:
    chunks_done: int = 0
    log_pages: int = 0
    logs_seen: int = 0
    unique_contracts: int = 0
    new_contracts: int = 0
    retries: int = 0
    rate_limits: int = 0
    started_at: float = 0.0

    def elapsed(self) -> float:
        return max(0.001, time.perf_counter() - self.started_at)


# ==========================================================
# LOGGING / SHUTDOWN
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("erc1155_scanner")

shutdown_event = asyncio.Event()


def request_shutdown() -> None:
    logger.warning("Shutdown requested; finishing current requests...")
    shutdown_event.set()


def install_signal_handlers() -> None:
    try:
        signal.signal(signal.SIGINT, lambda *_: request_shutdown())
        signal.signal(signal.SIGTERM, lambda *_: request_shutdown())
    except Exception:
        # Some environments do not allow signal registration.
        pass


# ==========================================================
# RATE LIMITER
# ==========================================================

class AsyncTokenBucket:
    def __init__(self, rate: float, burst: int) -> None:
        self.rate = max(0.1, float(rate))
        self.capacity = max(1, int(burst))
        self.tokens = float(self.capacity)
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while not shutdown_event.is_set():
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                missing = 1.0 - self.tokens
                sleep_for = min(1.0, missing / self.rate)

            await asyncio.sleep(max(0.01, sleep_for))


# ==========================================================
# FILE / STATE
# ==========================================================

def load_existing_addresses(path: Path) -> Set[str]:
    if not path.exists():
        return set()

    out: Set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().lower()
            if line.startswith("0x") and len(line) == 42:
                out.add(line)
    return out


def append_addresses(path: Path, addresses: Iterable[str]) -> int:
    unique_sorted = sorted(set(a.lower() for a in addresses))
    if not unique_sorted:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        for address in unique_sorted:
            f.write(address + "\n")
    return len(unique_sorted)


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("State file is corrupted or unreadable: %s", path)
        return {}


def save_state(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


# ==========================================================
# HTTP
# ==========================================================

def backoff_delay(attempt: int, base: float, max_backoff: float) -> float:
    return min(max_backoff, base ** attempt) + random.uniform(0.15, 0.85)


async def fetch_json(
    session: aiohttp.ClientSession,
    limiter: AsyncTokenBucket,
    cfg: Config,
    stats: Stats,
    params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    request_params = dict(params)
    request_params["apikey"] = cfg.api_key

    for attempt in range(1, cfg.retry_limit + 1):
        if shutdown_event.is_set():
            return None

        await limiter.acquire()

        try:
            async with session.get(cfg.base_url, params=request_params) as response:
                text = await response.text()
                lowered = text.lower()

                if response.status == 429 or any(m in lowered for m in RATE_LIMIT_MARKERS):
                    stats.rate_limits += 1
                    delay = backoff_delay(attempt, cfg.backoff_base, cfg.max_backoff)
                    logger.warning("Rate limited; retry in %.2fs", delay)
                    await asyncio.sleep(delay)
                    continue

                if response.status >= 500:
                    stats.retries += 1
                    await asyncio.sleep(backoff_delay(attempt, cfg.backoff_base, cfg.max_backoff))
                    continue

                if response.status >= 400:
                    logger.error("HTTP %s: %s", response.status, text[:300])
                    return None

                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    logger.error("Invalid JSON: %s", text[:300])
                    return None

        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as exc:
            stats.retries += 1
            delay = backoff_delay(attempt, cfg.backoff_base, cfg.max_backoff)
            logger.warning("Network error: %s; retry in %.2fs", type(exc).__name__, delay)
            await asyncio.sleep(delay)

    return None


async def get_latest_block(
    session: aiohttp.ClientSession,
    limiter: AsyncTokenBucket,
    cfg: Config,
    stats: Stats,
) -> Optional[int]:
    data = await fetch_json(
        session,
        limiter,
        cfg,
        stats,
        {"module": "proxy", "action": "eth_blockNumber"},
    )
    try:
        return int(str(data["result"]), 16)
    except Exception:
        logger.error("Could not parse latest block response: %s", data)
        return None


# ==========================================================
# SCANNING
# ==========================================================

def split_ranges(from_block: int, to_block: int, chunk_size: int) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    cur = from_block
    while cur <= to_block:
        end = min(to_block, cur + chunk_size - 1)
        ranges.append((cur, end))
        cur = end + 1
    return ranges


def is_no_records_response(data: Dict[str, Any]) -> bool:
    message = str(data.get("message", "")).lower()
    result = data.get("result")
    if isinstance(result, str):
        return any(marker in result.lower() for marker in NO_RECORDS_MARKERS)
    return any(marker in message for marker in NO_RECORDS_MARKERS)


async def get_logs_page(
    session: aiohttp.ClientSession,
    limiter: AsyncTokenBucket,
    cfg: Config,
    stats: Stats,
    from_block: int,
    to_block: int,
    topic0: str,
    page: int,
) -> List[Dict[str, Any]]:
    data = await fetch_json(
        session,
        limiter,
        cfg,
        stats,
        {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": from_block,
            "toBlock": to_block,
            "topic0": topic0,
            "page": page,
            "offset": cfg.page_size,
            "sort": "asc",
        },
    )

    if not isinstance(data, dict):
        return []

    if is_no_records_response(data):
        return []

    result = data.get("result")
    if isinstance(result, list):
        stats.log_pages += 1
        stats.logs_seen += len(result)
        return result

    # PolygonScan sometimes returns status/message/result strings for errors.
    logger.warning(
        "Unexpected getLogs response for %s-%s page=%s: %s",
        from_block,
        to_block,
        page,
        str(data)[:300],
    )
    return []


async def scan_topic_range(
    session: aiohttp.ClientSession,
    limiter: AsyncTokenBucket,
    cfg: Config,
    stats: Stats,
    from_block: int,
    to_block: int,
    topic0: str,
) -> Set[str]:
    contracts: Set[str] = set()
    page = 1

    while not shutdown_event.is_set():
        logs = await get_logs_page(
            session=session,
            limiter=limiter,
            cfg=cfg,
            stats=stats,
            from_block=from_block,
            to_block=to_block,
            topic0=topic0,
            page=page,
        )

        if not logs:
            break

        for log in logs:
            address = str(log.get("address", "")).lower()
            if address.startswith("0x") and len(address) == 42:
                contracts.add(address)

        if len(logs) < cfg.page_size:
            break

        page += 1

    return contracts


async def scan_chunk(
    session: aiohttp.ClientSession,
    limiter: AsyncTokenBucket,
    cfg: Config,
    stats: Stats,
    semaphore: asyncio.Semaphore,
    from_block: int,
    to_block: int,
) -> Set[str]:
    async with semaphore:
        found: Set[str] = set()
        for topic0 in ERC1155_TOPICS:
            if shutdown_event.is_set():
                break
            found.update(
                await scan_topic_range(
                    session=session,
                    limiter=limiter,
                    cfg=cfg,
                    stats=stats,
                    from_block=from_block,
                    to_block=to_block,
                    topic0=topic0,
                )
            )

        stats.chunks_done += 1
        if stats.chunks_done % 10 == 0:
            logger.info(
                "Chunks=%d | Logs=%d | Unique=%d | Speed=%.2f logs/sec",
                stats.chunks_done,
                stats.logs_seen,
                stats.unique_contracts,
                stats.logs_seen / stats.elapsed(),
            )
        return found


async def scan_ranges(
    session: aiohttp.ClientSession,
    cfg: Config,
    stats: Stats,
    ranges: List[Tuple[int, int]],
) -> Set[str]:
    limiter = AsyncTokenBucket(cfg.rate_limit_per_sec, cfg.burst_size)
    semaphore = asyncio.Semaphore(max(1, cfg.concurrency))

    all_contracts: Set[str] = set()

    tasks = [
        asyncio.create_task(scan_chunk(session, limiter, cfg, stats, semaphore, a, b))
        for a, b in ranges
    ]

    for task in asyncio.as_completed(tasks):
        if shutdown_event.is_set():
            for t in tasks:
                t.cancel()
            break

        try:
            all_contracts.update(await task)
            stats.unique_contracts = len(all_contracts)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Chunk failed")

    await asyncio.gather(*tasks, return_exceptions=True)
    return all_contracts


# ==========================================================
# CLI / MAIN
# ==========================================================

def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Fast Polygon ERC1155 activity scanner")
    parser.add_argument("--api-key", default=os.getenv("POLYGONSCAN_API_KEY", ""))
    parser.add_argument("--base-url", default=os.getenv("POLYGONSCAN_BASE_URL", POLYGONSCAN_URL))
    parser.add_argument("--output", default=os.getenv("OUTPUT_FILE", "erc1155_addresses.txt"))
    parser.add_argument("--state", default=os.getenv("STATE_FILE", "erc1155_scanner_state.json"))
    parser.add_argument("--blocks", type=int, default=int(os.getenv("BLOCKS_TO_SCAN", "500")))
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", default=os.getenv("TO_BLOCK", "latest"))
    parser.add_argument("--chunk-size", type=int, default=int(os.getenv("CHUNK_SIZE", "100")))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("PAGE_SIZE", "1000")))
    parser.add_argument("--rate", type=float, default=float(os.getenv("RATE_LIMIT_PER_SEC", "4.8")))
    parser.add_argument("--burst", type=int, default=int(os.getenv("BURST_SIZE", "5")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "4")))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("TIMEOUT", "20")))
    parser.add_argument("--retries", type=int, default=int(os.getenv("RETRY_LIMIT", "6")))
    parser.add_argument("--resume", action="store_true", help="Resume from last saved scanned block")
    parser.add_argument("--no-state", action="store_true", help="Do not save scanner state")

    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit("Missing API key. Set POLYGONSCAN_API_KEY or pass --api-key.")

    if args.blocks <= 0:
        raise SystemExit("--blocks must be positive")
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be positive")
    if args.page_size <= 0 or args.page_size > 1000:
        raise SystemExit("--page-size should be between 1 and 1000")

    return Config(
        api_key=args.api_key,
        base_url=args.base_url,
        output_file=Path(args.output),
        state_file=Path(args.state),
        blocks=args.blocks,
        from_block=args.from_block,
        to_block=str(args.to_block),
        chunk_size=args.chunk_size,
        page_size=args.page_size,
        rate_limit_per_sec=args.rate,
        burst_size=args.burst,
        concurrency=args.concurrency,
        timeout=args.timeout,
        retry_limit=args.retries,
        backoff_base=1.8,
        max_backoff=45.0,
        dns_ttl=300,
        resume=args.resume,
        save_state=not args.no_state,
    )


async def main() -> None:
    cfg = parse_args()
    install_signal_handlers()

    stats = Stats(started_at=time.perf_counter())
    existing = load_existing_addresses(cfg.output_file)

    timeout = ClientTimeout(total=cfg.timeout)
    connector = TCPConnector(
        limit=max(10, cfg.concurrency * 4),
        ttl_dns_cache=cfg.dns_ttl,
        ssl=False,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        bootstrap_limiter = AsyncTokenBucket(cfg.rate_limit_per_sec, cfg.burst_size)
        latest_block = await get_latest_block(session, bootstrap_limiter, cfg, stats)

        if latest_block is None:
            raise SystemExit("Failed to get latest block")

        if cfg.to_block.lower() == "latest":
            to_block = latest_block
        else:
            to_block = int(cfg.to_block)

        from_block: int
        state = load_state(cfg.state_file) if cfg.resume else {}

        if cfg.from_block is not None:
            from_block = cfg.from_block
        elif cfg.resume and isinstance(state.get("last_scanned_block"), int):
            from_block = int(state["last_scanned_block"]) + 1
        else:
            from_block = max(0, to_block - cfg.blocks + 1)

        if from_block > to_block:
            logger.info("Nothing to scan: from_block=%d > to_block=%d", from_block, to_block)
            return

        ranges = split_ranges(from_block, to_block, cfg.chunk_size)

        logger.info(
            "Scanning Polygon ERC1155 logs | blocks=%d-%d | chunks=%d | existing=%d",
            from_block,
            to_block,
            len(ranges),
            len(existing),
        )

        contracts = await scan_ranges(session, cfg, stats, ranges)
        new_contracts = contracts - existing
        stats.new_contracts = len(new_contracts)

        written = append_addresses(cfg.output_file, new_contracts)

        if cfg.save_state and not shutdown_event.is_set():
            save_state(
                cfg.state_file,
                {
                    "last_scanned_block": to_block,
                    "latest_block_at_run": latest_block,
                    "output_file": str(cfg.output_file),
                    "updated_at": int(time.time()),
                },
            )

        logger.info("=" * 72)
        logger.info("DONE")
        logger.info("Block range:       %d-%d", from_block, to_block)
        logger.info("Logs seen:         %d", stats.logs_seen)
        logger.info("Log pages:         %d", stats.log_pages)
        logger.info("Unique contracts:  %d", len(contracts))
        logger.info("Already existing:  %d", len(contracts & existing))
        logger.info("New written:       %d", written)
        logger.info("Rate limits:       %d", stats.rate_limits)
        logger.info("Retries:           %d", stats.retries)
        logger.info("Elapsed:           %.2fs", stats.elapsed())
        logger.info("Log speed:         %.2f logs/sec", stats.logs_seen / stats.elapsed())
        logger.info("Output:            %s", cfg.output_file)
        logger.info("=" * 72)


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        request_shutdown()

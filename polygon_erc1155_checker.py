#!/usr/bin/env python3

import asyncio
import aiohttp
import aiofiles
import os
import logging
import random
import time
import re
import signal
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass, field
from aiohttp import ClientTimeout, TCPConnector
from collections import OrderedDict

# ==========================================================
# CONFIG
# ==========================================================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("POLYGONSCAN_API_KEY", "")
    base_url: str = "https://api.polygonscan.com/api"
    output_file: str = "erc1155_addresses.txt"

    max_concurrency: int = 25
    retry_limit: int = 5
    timeout: float = 12.0

    rate_limit_per_sec: float = 4.5
    burst_size: int = 3

    dns_ttl: int = 300
    backoff_base: float = 2.0
    max_backoff: float = 60.0

    min_request_interval: float = 0.12

    # 🔥 NEW
    blocks_to_scan: int = 5         # scan last N blocks instead of 1
    tx_chunk_size: int = 200        # process addresses in chunks
    address_cache_size: int = 50000 # LRU cache size


CFG = Config()

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)

logger = logging.getLogger("erc1155_scanner")

# ==========================================================
# SHUTDOWN
# ==========================================================

shutdown_event = asyncio.Event()

def _handle_shutdown():
    logger.warning("Shutdown signal received...")
    shutdown_event.set()

signal.signal(signal.SIGINT, lambda *_: _handle_shutdown())
signal.signal(signal.SIGTERM, lambda *_: _handle_shutdown())

# ==========================================================
# RATE LIMITER
# ==========================================================

class RateLimiter:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            while self.tokens < 1:
                now = time.monotonic()
                elapsed = now - self.updated
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.updated = now
                await asyncio.sleep(0.02)
            self.tokens -= 1

rate_limiter = RateLimiter(CFG.rate_limit_per_sec, CFG.burst_size)

# ==========================================================
# SIMPLE LRU CACHE
# ==========================================================

class LRUCache:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def add(self, key: str):
        self.cache[key] = True
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def __contains__(self, key: str):
        return key in self.cache

address_cache = LRUCache(CFG.address_cache_size)

# ==========================================================
# NETWORK
# ==========================================================

def compute_backoff(attempt: int) -> float:
    base = min(CFG.backoff_base ** attempt, CFG.max_backoff)
    return base + random.uniform(0.3, 0.8)

async def fetch_json(session, params) -> Optional[Dict[str, Any]]:
    request_params = {**params, "apikey": CFG.api_key}

    for attempt in range(1, CFG.retry_limit + 1):

        if shutdown_event.is_set():
            return None

        await rate_limiter.wait()
        await asyncio.sleep(CFG.min_request_interval)

        try:
            async with session.get(CFG.base_url, params=request_params) as resp:

                text = await resp.text()

                if resp.status == 429 or "rate limit" in text.lower():
                    await asyncio.sleep(compute_backoff(attempt))
                    continue

                if resp.status >= 500:
                    await asyncio.sleep(compute_backoff(attempt))
                    continue

                if resp.status >= 400:
                    return None

                return await resp.json(content_type=None)

        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(compute_backoff(attempt))

    return None

# ==========================================================
# HELPERS
# ==========================================================

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

def normalize_address(addr: Optional[str]) -> Optional[str]:
    if not addr:
        return None
    addr = addr.lower()
    return addr if ADDRESS_RE.match(addr) else None

# ==========================================================
# BLOCKCHAIN
# ==========================================================

async def get_latest_block(session):
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_blockNumber",
    })
    try:
        return int(data["result"], 16)
    except:
        return None

async def get_block_transactions(session, block_number):
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
    })
    if not isinstance(data, dict):
        return []
    return data.get("result", {}).get("transactions", [])

# ==========================================================
# 🔥 FAST ERC1155 DETECTION (LOG-BASED)
# ==========================================================

ERC1155_TOPIC = "0xd9b67a26"  # ERC1155 TransferSingle/Batch signature prefix

async def has_erc1155_activity(session, address: str) -> bool:
    if address in address_cache:
        return False

    data = await fetch_json(session, {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": "latest",
        "toBlock": "latest",
        "address": address,
        "page": 1,
        "offset": 1
    })

    if not isinstance(data, dict):
        return False

    logs = data.get("result")
    if not isinstance(logs, list):
        return False

    for log in logs:
        topics = log.get("topics", [])
        if any(ERC1155_TOPIC in t.lower() for t in topics):
            address_cache.add(address)
            return True

    address_cache.add(address)
    return False

# ==========================================================
# PROCESSING
# ==========================================================

async def process_address(session, semaphore, address):
    if shutdown_event.is_set():
        return None

    async with semaphore:
        if await has_erc1155_activity(session, address):
            logger.info("ERC1155: %s", address)
            return address
    return None

# ==========================================================
# FILE
# ==========================================================

async def load_existing(path):
    if not os.path.exists(path):
        return set()

    async with aiofiles.open(path, "r") as f:
        return {line.strip() async for line in f}

async def append_results(path, addresses):
    if not addresses:
        return
    async with aiofiles.open(path, "a") as f:
        await f.write("\n".join(addresses) + "\n")

# ==========================================================
# MAIN
# ==========================================================

async def main():

    if not CFG.api_key:
        raise RuntimeError("Missing API key")

    start = time.perf_counter()

    existing = await load_existing(CFG.output_file)

    timeout = ClientTimeout(total=CFG.timeout)

    connector = TCPConnector(
        limit=CFG.max_concurrency * 2,
        ttl_dns_cache=CFG.dns_ttl,
        ssl=False,
    )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

        latest = await get_latest_block(session)
        if not latest:
            logger.error("Failed to fetch block")
            return

        logger.info("Scanning last %d blocks...", CFG.blocks_to_scan)

        all_addresses = set()

        for i in range(CFG.blocks_to_scan):
            block = latest - i
            txs = await get_block_transactions(session, block)

            for tx in txs:
                for field in ("from", "to"):
                    addr = normalize_address(tx.get(field))
                    if addr:
                        all_addresses.add(addr)

        addresses = all_addresses - existing

        if not addresses:
            logger.info("No new addresses")
            return

        logger.info("Total addresses: %d", len(addresses))

        semaphore = asyncio.Semaphore(CFG.max_concurrency)

        found = set()
        tasks = []

        addr_list = list(addresses)

        for i in range(0, len(addr_list), CFG.tx_chunk_size):

            chunk = addr_list[i:i + CFG.tx_chunk_size]

            tasks = [
                asyncio.create_task(process_address(session, semaphore, addr))
                for addr in chunk
            ]

            for coro in asyncio.as_completed(tasks):
                if shutdown_event.is_set():
                    break
                res = await coro
                if res:
                    found.add(res)

            logger.info("Progress: %d/%d", i + len(chunk), len(addr_list))

        if found:
            await append_results(CFG.output_file, sorted(found))
            logger.info("Saved %d addresses", len(found))
        else:
            logger.info("No ERC1155 found")

    logger.info("Done in %.2fs", time.perf_counter() - start)


if __name__ == "__main__":
    asyncio.run(main())

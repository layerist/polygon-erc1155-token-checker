#!/usr/bin/env python3

import asyncio
import aiohttp
import aiofiles
import os
import logging
import random
import time
import re
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass
from aiohttp import ClientTimeout, TCPConnector


# ==========================================================
# CONFIG
# ==========================================================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("POLYGONSCAN_API_KEY", "")
    base_url: str = "https://api.polygonscan.com/api"
    output_file: str = "erc1155_addresses.txt"

    max_concurrency: int = 20
    retry_limit: int = 4
    timeout: float = 12.0

    rate_limit_per_sec: float = 4.5   # polygonscan safe rate

    dns_ttl: int = 300
    backoff_base: float = 2.0
    max_backoff: float = 60.0


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
# GLOBAL RATE LIMITER
# ==========================================================

class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / rate_per_sec
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            delta = now - self._last

            if delta < self.interval:
                await asyncio.sleep(self.interval - delta)

            self._last = time.monotonic()


rate_limiter = RateLimiter(CFG.rate_limit_per_sec)


# ==========================================================
# NETWORK UTILITIES
# ==========================================================

async def exponential_backoff(attempt: int) -> None:
    delay = min(CFG.backoff_base ** attempt, CFG.max_backoff)
    jitter = random.uniform(0.2, 0.6)
    await asyncio.sleep(delay + jitter)


async def fetch_json(
    session: aiohttp.ClientSession,
    params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    request_params = {**params, "apikey": CFG.api_key}

    for attempt in range(1, CFG.retry_limit + 1):

        await rate_limiter.wait()

        try:
            async with session.get(CFG.base_url, params=request_params) as resp:

                text = await resp.text()

                if resp.status == 429 or "rate limit" in text.lower():
                    logger.warning("Rate limit hit (attempt %s)", attempt)
                    await exponential_backoff(attempt)
                    continue

                if resp.status >= 500:
                    logger.warning("Server error %s", resp.status)
                    await exponential_backoff(attempt)
                    continue

                resp.raise_for_status()

                try:
                    return await resp.json(content_type=None)
                except Exception:
                    logger.warning("Invalid JSON response")
                    return None

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Network error attempt %s: %s", attempt, e)
            await exponential_backoff(attempt)

    logger.error("Request failed: %s", params)
    return None


# ==========================================================
# BLOCKCHAIN HELPERS
# ==========================================================

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def normalize_address(addr: Optional[str]) -> Optional[str]:
    if not addr:
        return None

    addr = addr.lower()
    return addr if ADDRESS_RE.match(addr) else None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:

    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_blockNumber",
    })

    try:
        return int(data["result"], 16)
    except Exception:
        return None


async def get_block_transactions(
    session: aiohttp.ClientSession,
    block_number: int,
) -> List[Dict[str, Any]]:

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
# ERC1155 DETECTION
# ==========================================================

async def has_erc1155_activity(
    session: aiohttp.ClientSession,
    address: str,
) -> bool:

    data = await fetch_json(session, {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
    })

    if not isinstance(data, dict):
        return False

    txs = data.get("result")

    if not isinstance(txs, list):
        return False

    for tx in txs:
        if tx.get("tokenType") == "ERC-1155":
            return True

    return False


async def process_address(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    address: str,
) -> Optional[str]:

    async with semaphore:

        try:
            if await has_erc1155_activity(session, address):
                logger.info("ERC1155 detected: %s", address)
                return address

        except Exception:
            logger.exception("Failed address: %s", address)

    return None


# ==========================================================
# FILE HELPERS
# ==========================================================

async def load_existing(path: str) -> Set[str]:

    if not os.path.exists(path):
        return set()

    async with aiofiles.open(path, "r") as f:
        return {line.strip().lower() async for line in f if line.strip()}


async def append_results(path: str, addresses: List[str]) -> None:

    if not addresses:
        return

    async with aiofiles.open(path, "a") as f:
        await f.write("\n".join(addresses) + "\n")


# ==========================================================
# MAIN
# ==========================================================

async def main():

    if not CFG.api_key:
        raise RuntimeError("POLYGONSCAN_API_KEY missing")

    start = time.perf_counter()

    logger.info("Starting ERC1155 scanner")

    existing = await load_existing(CFG.output_file)
    logger.info("Loaded %s existing addresses", len(existing))

    timeout = ClientTimeout(total=CFG.timeout)

    connector = TCPConnector(
        limit=CFG.max_concurrency * 2,
        limit_per_host=CFG.max_concurrency,
        ttl_dns_cache=CFG.dns_ttl,
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        latest_block = await get_latest_block(session)

        if latest_block is None:
            logger.error("Cannot fetch latest block")
            return

        logger.info("Latest block: %s", latest_block)

        txs = await get_block_transactions(session, latest_block)

        raw_addresses = set()

        for tx in txs:
            for field in ("from", "to"):
                addr = normalize_address(tx.get(field))
                if addr:
                    raw_addresses.add(addr)

        addresses = raw_addresses - existing

        if not addresses:
            logger.info("No new addresses")
            return

        logger.info("Scanning %s addresses", len(addresses))

        semaphore = asyncio.Semaphore(CFG.max_concurrency)

        tasks = [
            asyncio.create_task(process_address(session, semaphore, addr))
            for addr in addresses
        ]

        found: Set[str] = set()

        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                found.add(result)

        if found:
            await append_results(CFG.output_file, sorted(found))
            logger.info("Saved %s ERC1155 addresses", len(found))
        else:
            logger.info("No ERC1155 activity detected")

    logger.info("Finished in %.2f seconds", time.perf_counter() - start)


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")

import asyncio
import aiohttp
import aiofiles
import os
import logging
import random
import time
from typing import Optional, Dict, Any, List, Set, Iterable
from dataclasses import dataclass
from aiohttp import ClientTimeout, TCPConnector


# ===========================
# CONFIG
# ===========================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("POLYGONSCAN_API_KEY", "")
    base_url: str = "https://api.polygonscan.com/api"
    output_file: str = "erc1155_addresses.txt"

    max_concurrency: int = 20
    retry_limit: int = 4
    timeout: float = 12.0
    dns_ttl: int = 300

    backoff_base: float = 2.0
    min_request_delay: float = 0.25
    max_backoff: float = 60.0


CFG = Config()


# ===========================
# LOGGING
# ===========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("erc1155_scanner")


# ===========================
# NETWORK HELPERS
# ===========================

async def exponential_backoff(attempt: int) -> None:
    delay = min(CFG.backoff_base ** attempt, CFG.max_backoff)
    delay += random.uniform(0.2, 0.6)
    await asyncio.sleep(delay)


async def fetch_json(
    session: aiohttp.ClientSession,
    params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    params["apikey"] = CFG.api_key

    for attempt in range(1, CFG.retry_limit + 1):
        try:
            async with session.get(CFG.base_url, params=params) as resp:
                text = await resp.text()

                if resp.status == 429 or "rate limit" in text.lower():
                    logger.warning("Rate limit hit, backing off")
                    await exponential_backoff(attempt)
                    continue

                if resp.status >= 500:
                    logger.warning("Server error %s, retrying", resp.status)
                    await exponential_backoff(attempt)
                    continue

                resp.raise_for_status()
                return await resp.json(content_type=None)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("HTTP error (attempt %s): %s", attempt, e)
            await exponential_backoff(attempt)

        except Exception as e:
            logger.error("Unexpected fetch error: %s", e)
            return None

    logger.error("Request failed after %s attempts: %s", CFG.retry_limit, params)
    return None


# ===========================
# BLOCKCHAIN HELPERS
# ===========================

def normalize_addr(addr: Optional[str]) -> Optional[str]:
    if not addr:
        return None
    addr = addr.lower()
    return addr if addr.startswith("0x") and len(addr) == 42 else None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_blockNumber",
    })
    try:
        return int(data["result"], 16) if data else None
    except (KeyError, ValueError, TypeError):
        return None


async def get_block_transactions(
    session: aiohttp.ClientSession,
    block: int,
) -> List[Dict[str, Any]]:
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block),
        "boolean": "true",
    })
    return data.get("result", {}).get("transactions", []) if isinstance(data, dict) else []


# ===========================
# ERC-1155 DETECTION
# ===========================

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

    await asyncio.sleep(CFG.min_request_delay * random.uniform(0.9, 1.1))

    txs = data.get("result") if isinstance(data, dict) else None
    if not isinstance(txs, list):
        return False

    return any(tx.get("tokenType") == "ERC-1155" for tx in txs)


async def process_address(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    address: str,
) -> Optional[str]:
    async with sem:
        try:
            if await has_erc1155_activity(session, address):
                return address
        except Exception as e:
            logger.error("Address %s failed: %s", address, e)
    return None


async def bounded_gather(
    coros: Iterable[asyncio.Future],
) -> List[Any]:
    results: List[Any] = []
    for coro in asyncio.as_completed(coros):
        results.append(await coro)
    return results


# ===========================
# FILE HELPERS
# ===========================

async def load_existing(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    async with aiofiles.open(path, "r") as f:
        return {
            line.strip().lower()
            for line in await f.readlines()
            if line.strip()
        }


async def append_results(path: str, addresses: List[str]) -> None:
    if not addresses:
        return
    async with aiofiles.open(path, "a") as f:
        await f.write("\n".join(addresses) + "\n")


# ===========================
# MAIN
# ===========================

async def main() -> None:
    if not CFG.api_key:
        raise RuntimeError("POLYGONSCAN_API_KEY is not set")

    logger.info("Starting ERC-1155 scanner")
    start = time.time()

    existing = await load_existing(CFG.output_file)
    logger.info("Loaded %s existing addresses", len(existing))

    timeout = ClientTimeout(total=CFG.timeout)
    connector = TCPConnector(
        limit_per_host=CFG.max_concurrency,
        ttl_dns_cache=CFG.dns_ttl,
    )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        block = await get_latest_block(session)
        if block is None:
            logger.error("Failed to fetch latest block")
            return

        logger.info("Latest block: %s", block)
        txs = await get_block_transactions(session, block)

        raw_addresses = {
            normalize_addr(tx.get(field))
            for tx in txs
            for field in ("from", "to")
        }

        addresses = {a for a in raw_addresses if a} - existing
        if not addresses:
            logger.info("No new addresses to scan")
            return

        logger.info("Scanning %s addresses", len(addresses))

        sem = asyncio.Semaphore(CFG.max_concurrency)
        tasks = [
            process_address(session, sem, addr)
            for addr in addresses
        ]

        results = await bounded_gather(tasks)
        found = sorted({r for r in results if r})

        if found:
            await append_results(CFG.output_file, found)
            logger.info("Saved %s ERC-1155 addresses", len(found))
        else:
            logger.info("No ERC-1155 activity detected")

    logger.info("Completed in %.2fs", time.time() - start)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")

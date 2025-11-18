import asyncio
import aiohttp
import aiofiles
import os
import logging
import random
import time
from typing import Optional, Dict, Any, List, Set
from aiohttp import ClientTimeout, TCPConnector
from dataclasses import dataclass


# ===========================
# CONFIG
# ===========================

@dataclass
class Config:
    api_key: str = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
    base_url: str = "https://api.polygonscan.com/api"
    output_file: str = "erc1155_addresses.txt"
    max_concurrency: int = 20
    retry_limit: int = 4
    timeout: int = 12
    backoff_base: float = 2.0
    rate_limit_delay: float = 0.3  # adaptive, lowered
    dns_ttl: int = 300


CFG = Config()


# ===========================
# LOGGING
# ===========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("erc1155_scanner")


# ===========================
# HELPERS
# ===========================

async def exponential_backoff(attempt: int) -> None:
    wait = min(CFG.backoff_base ** attempt, 60) + random.uniform(0.15, 0.9)
    logger.debug(f"Backoff {wait:.2f}s")
    await asyncio.sleep(wait)


async def fetch_json(session: aiohttp.ClientSession, params: Dict[str, Any]) -> Optional[Dict]:
    """
    Universal GET with retries, 429-detection and robust error handling.
    """
    for attempt in range(1, CFG.retry_limit + 1):
        try:
            async with session.get(CFG.base_url, params=params) as resp:
                text = await resp.text()
                if resp.status == 429 or "Max rate limit reached" in text:
                    logger.warning(f"[429 Rate limit] attempt {attempt}")
                    await exponential_backoff(attempt)
                    continue

                if resp.status >= 500:
                    logger.warning(f"[{resp.status}] Server error")
                    await exponential_backoff(attempt)
                    continue

                resp.raise_for_status()
                return await resp.json(content_type=None)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"HTTP Fail attempt {attempt}: {e}")
            await exponential_backoff(attempt)

        except Exception as e:
            logger.error(f"Unhandled fetch error: {e}")
            return None

    logger.error(f"Failed after {CFG.retry_limit} retries. Params={params}")
    return None


async def load_existing_results(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()

    async with aiofiles.open(path, "r") as f:
        return {line.strip().lower() for line in await f.readlines() if line.strip()}


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    params = {"module": "proxy", "action": "eth_blockNumber", "apikey": CFG.api_key}
    data = await fetch_json(session, params)
    if not data or "result" not in data:
        return None

    try:
        return int(data["result"], 16)
    except:
        return None


async def get_block_transactions(session: aiohttp.ClientSession, block_num: int) -> List[Dict]:
    params = {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_num),
        "boolean": "true",
        "apikey": CFG.api_key
    }
    data = await fetch_json(session, params)
    if not data or not data.get("result"):
        return []
    return data["result"].get("transactions", [])


def normalize_addr(addr: Optional[str]) -> Optional[str]:
    if not addr:
        return None
    addr = addr.lower()
    return addr if len(addr) == 42 else None


async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    params = {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": CFG.api_key
    }
    data = await fetch_json(session, params)
    await asyncio.sleep(CFG.rate_limit_delay * random.uniform(0.7, 1.4))

    if not data or not isinstance(data.get("result"), list):
        return False

    for tx in data["result"]:
        if tx.get("tokenType") == "ERC-1155":
            return True
        if tx.get("tokenID"):  # ERC1155 always has IDs
            return True
        if not tx.get("tokenSymbol"):  # ERC1155 often no symbol
            return True

    return False


async def process_address(session: aiohttp.ClientSession, addr: str, sem: asyncio.Semaphore) -> Optional[str]:
    async with sem:
        try:
            if await has_erc1155_tokens(session, addr):
                return addr
        except Exception as e:
            logger.error(f"Process error {addr}: {e}")
    return None


async def append_results(path: str, addresses: List[str]):
    if not addresses:
        return
    async with aiofiles.open(path, "a") as f:
        for a in addresses:
            await f.write(a + "\n")


# ===========================
# MAIN
# ===========================

async def main():
    logger.info("🚀 Starting improved ERC-1155 scanner...")
    t0 = time.time()

    previous_results = await load_existing_results(CFG.output_file)
    logger.info(f"Loaded {len(previous_results)} previously saved addresses.")

    timeout = ClientTimeout(total=CFG.timeout)
    connector = TCPConnector(limit_per_host=CFG.max_concurrency, ttl_dns_cache=CFG.dns_ttl)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

        block = await get_latest_block(session)
        if block is None:
            logger.error("Failed to get latest block")
            return

        logger.info(f"Latest block: {block}")
        txs = await get_block_transactions(session, block)

        raw_addrs = [normalize_addr(tx.get("from")) for tx in txs] + \
                    [normalize_addr(tx.get("to")) for tx in txs]

        addrs = {a for a in raw_addrs if a}
        addrs = addrs - previous_results  # skip already found

        if not addrs:
            logger.info("No new addresses to process.")
            return

        logger.info(f"Processing {len(addrs)} addresses...")

        sem = asyncio.Semaphore(CFG.max_concurrency)
        found: List[str] = []

        async with asyncio.TaskGroup() as tg:
            tasks = {tg.create_task(process_address(session, a, sem)): a for a in addrs}
            for task in tasks:
                res = await task
                if res:
                    found.append(res)

        found = sorted(set(found))
        if found:
            await append_results(CFG.output_file, found)
            logger.info(f"Saved {len(found)} new ERC-1155 addresses.")
        else:
            logger.info("No ERC-1155 addresses found.")

    logger.info(f"Finished in {time.time() - t0:.2f}s.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")

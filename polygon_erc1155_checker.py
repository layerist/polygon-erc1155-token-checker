import asyncio
import aiohttp
import aiofiles
import os
import logging
import random
import time
from typing import Optional, Dict, Any, List, Set
from tqdm.asyncio import tqdm
from aiohttp import ClientTimeout, TCPConnector

# === Configuration ===
API_KEY = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL = "https://api.polygonscan.com/api"
SAVE_FILE = "erc1155_addresses.txt"
MAX_CONCURRENT_TASKS = 10
RETRY_LIMIT = 3
BACKOFF_DELAY = 2
TIMEOUT = 10

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


async def fetch_json(session: aiohttp.ClientSession, params: Dict[str, Any], retries: int = RETRY_LIMIT) -> Optional[Dict[str, Any]]:
    """GET request with retries, backoff, and jitter."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(BASE_URL, params=params) as resp:
                if resp.status == 429:
                    wait = BACKOFF_DELAY * attempt + random.uniform(0, 1)
                    logger.warning(f"Rate limit hit. Sleeping {wait:.2f}s...")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = await resp.json()
                if data.get("status") == "0":
                    logger.warning(f"API error: {data.get('message')} | {params}")
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = BACKOFF_DELAY * attempt + random.uniform(0.5, 1.5)
            logger.warning(f"Attempt {attempt}/{retries} failed: {e} | Retrying in {wait:.2f}s...")
            await asyncio.sleep(wait)
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            break
    return None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Return the latest block number."""
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": API_KEY
    })
    if data and "result" in data:
        try:
            return int(data["result"], 16)
        except Exception as e:
            logger.error(f"Block number parsing failed: {e}")
    return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Return all transactions in a block."""
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY
    })
    return data.get("result", {}).get("transactions", []) if data else []


async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    """Check if an address interacted with ERC-1155 contracts."""
    data = await fetch_json(session, {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY
    })
    if data and isinstance(data.get("result"), list):
        for tx in data["result"]:
            if "ERC1155" in tx.get("tokenName", "") or tx.get("tokenID"):
                return True
    return False


async def process_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Optional[str]:
    """Check a single address (semaphore controlled)."""
    async with semaphore:
        if await has_erc1155_tokens(session, address):
            logger.debug(f"ERC-1155 found: {address}")
            return address
    return None


async def save_addresses_atomic(addresses: List[str], filename: str):
    """Safely write addresses to a file."""
    temp_file = filename + ".tmp"
    async with aiofiles.open(temp_file, "w") as f:
        await f.writelines(f"{addr}\n" for addr in addresses)
    os.replace(temp_file, filename)


async def main():
    """Main function."""
    start = time.time()
    timeout = ClientTimeout(total=TIMEOUT)

    connector = TCPConnector(limit=MAX_CONCURRENT_TASKS)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Could not retrieve latest block.")
            return

        logger.info(f"Scanning block #{latest_block} for addresses...")

        transactions = await get_block_transactions(session, latest_block)
        raw_addresses = {tx.get("from") for tx in transactions}.union({tx.get("to") for tx in transactions})
        addresses = {addr.lower() for addr in raw_addresses if addr}

        if not addresses:
            logger.warning(f"No addresses in block {latest_block}.")
            return

        logger.info(f"Found {len(addresses)} addresses. Checking for ERC-1155 activity...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        tasks = [process_address(session, addr, semaphore) for addr in tqdm(addresses, desc="Checking")]
        results = await asyncio.gather(*tasks)

        valid = sorted({addr for addr in results if addr})
        if valid:
            await save_addresses_atomic(valid, SAVE_FILE)
            logger.info(f"Saved {len(valid)} ERC-1155 addresses to {SAVE_FILE}.")
        else:
            logger.info("No ERC-1155 addresses found.")

    logger.info(f"Done in {time.time() - start:.2f}s.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")

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
API_KEY: str = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL: str = "https://api.polygonscan.com/api"
SAVE_FILE: str = "erc1155_addresses.txt"
MAX_CONCURRENT_TASKS: int = 10
RETRY_LIMIT: int = 3
BACKOFF_BASE: float = 2.0
TIMEOUT: float = 10.0

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


async def fetch_json(
    session: aiohttp.ClientSession,
    params: Dict[str, Any],
    retries: int = RETRY_LIMIT
) -> Optional[Dict[str, Any]]:
    """Perform GET request with retries, exponential backoff, and jitter."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(BASE_URL, params=params) as resp:
                if resp.status == 429:  # Rate limit
                    wait = min(BACKOFF_BASE * attempt + random.uniform(0.5, 1.5), 30)
                    logger.warning(f"[429] Rate limited. Sleeping {wait:.1f}s...")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                data = await resp.json()

                if data.get("status") == "0":
                    logger.debug(f"API returned error: {data.get('message')} | {params}")
                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = min(BACKOFF_BASE * attempt + random.uniform(0.5, 1.5), 30)
            logger.warning(f"Attempt {attempt}/{retries} failed: {e}. Retrying in {wait:.1f}s...")
            await asyncio.sleep(wait)

        except Exception as e:
            logger.exception(f"Unexpected error during fetch: {e}")
            break

    return None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Fetch the latest block number."""
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": API_KEY
    })
    if not data or "result" not in data:
        return None

    try:
        return int(data["result"], 16)
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to parse block number: {e}")
        return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Fetch all transactions from a given block."""
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY
    })
    return data.get("result", {}).get("transactions", []) if data else []


async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    """Check whether the given address has interacted with ERC-1155 contracts."""
    data = await fetch_json(session, {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY
    })
    if not data or not isinstance(data.get("result"), list):
        return False

    for tx in data["result"]:
        token_name = (tx.get("tokenName") or "").lower()
        if tx.get("tokenID") or "erc1155" in token_name:
            return True
    return False


async def process_address(
    session: aiohttp.ClientSession,
    address: str,
    semaphore: asyncio.Semaphore
) -> Optional[str]:
    """Check a single address with concurrency limits."""
    async with semaphore:
        if await has_erc1155_tokens(session, address):
            logger.debug(f"ERC-1155 activity found: {address}")
            return address
    return None


async def save_addresses(addresses: List[str], filename: str):
    """Append addresses to file safely."""
    if not addresses:
        return

    async with aiofiles.open(filename, "a") as f:
        await f.writelines(f"{addr}\n" for addr in addresses)


async def main():
    logger.info("Starting ERC-1155 address scanner...")
    start_time = time.time()

    timeout = ClientTimeout(total=TIMEOUT)
    connector = TCPConnector(limit=None)  # Don't limit connector, rely on semaphore

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Could not retrieve the latest block. Exiting.")
            return

        logger.info(f"Fetching transactions from block #{latest_block}...")
        transactions = await get_block_transactions(session, latest_block)

        addresses: Set[str] = {
            addr.lower()
            for tx in transactions
            for addr in (tx.get("from"), tx.get("to"))
            if addr
        }

        if not addresses:
            logger.warning(f"No addresses found in block {latest_block}.")
            return

        logger.info(f"{len(addresses)} addresses extracted. Checking ERC-1155 activity...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        tasks = [process_address(session, addr, semaphore) for addr in tqdm(addresses, desc="Checking")]

        try:
            results = await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logger.exception(f"Task execution failed: {e}")
            return

        valid_addresses = sorted(filter(None, results))
        if valid_addresses:
            await save_addresses(valid_addresses, SAVE_FILE)
            logger.info(f"{len(valid_addresses)} ERC-1155 addresses appended to {SAVE_FILE}.")
        else:
            logger.info("No ERC-1155 activity detected in this block.")

    elapsed = time.time() - start_time
    logger.info(f"Scan completed in {elapsed:.2f}s.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")

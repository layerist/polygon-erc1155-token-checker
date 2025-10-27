import asyncio
import aiohttp
import aiofiles
import os
import logging
import random
import time
from typing import Optional, Dict, Any, List, Set
from aiohttp import ClientTimeout, TCPConnector
from tqdm.asyncio import tqdm

# === Configuration ===
API_KEY: str = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL: str = "https://api.polygonscan.com/api"
SAVE_FILE: str = "erc1155_addresses.txt"
MAX_CONCURRENT_TASKS: int = 20
RETRY_LIMIT: int = 4
BACKOFF_BASE: float = 2.0
TIMEOUT: float = 12.0
RATE_LIMIT_DELAY: float = 0.8  # average delay between requests

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# === Utility functions ===
async def exponential_backoff(attempt: int, base: float = BACKOFF_BASE, jitter: bool = True) -> None:
    """Wait for exponential backoff time with optional jitter."""
    wait_time = min(base ** attempt, 60)
    if jitter:
        wait_time += random.uniform(0.2, 1.5)
    await asyncio.sleep(wait_time)


async def fetch_json(
    session: aiohttp.ClientSession,
    params: Dict[str, Any],
    retries: int = RETRY_LIMIT
) -> Optional[Dict[str, Any]]:
    """Perform GET request with retries, exponential backoff, and basic rate-limiting."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(BASE_URL, params=params) as resp:
                if resp.status == 429:
                    logger.warning(f"[429 Too Many Requests] Retrying (attempt {attempt})...")
                    await exponential_backoff(attempt)
                    continue

                if resp.status >= 500:
                    logger.warning(f"Server error {resp.status}, retrying...")
                    await exponential_backoff(attempt)
                    continue

                resp.raise_for_status()
                data = await resp.json(content_type=None)
                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Attempt {attempt}/{retries} failed: {e}")
            await exponential_backoff(attempt)
        except Exception as e:
            logger.exception(f"Unexpected error during fetch: {e}")
            break

    logger.error(f"All {retries} attempts failed for params: {params}")
    return None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Get the latest block number from PolygonScan."""
    params = {"module": "proxy", "action": "eth_blockNumber", "apikey": API_KEY}
    data = await fetch_json(session, params)
    if not data or "result" not in data:
        logger.error("Failed to retrieve latest block.")
        return None

    try:
        return int(data["result"], 16)
    except (ValueError, TypeError):
        logger.error(f"Invalid block number format: {data.get('result')}")
        return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Fetch all transactions from a specific block."""
    params = {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY
    }
    data = await fetch_json(session, params)
    if not data or "result" not in data:
        return []
    return data["result"].get("transactions", [])


async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    """Check if a given address has ERC-1155 token interactions."""
    params = {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY
    }
    data = await fetch_json(session, params)
    await asyncio.sleep(RATE_LIMIT_DELAY * random.uniform(0.5, 1.5))  # rate-limiting

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
    """Check one address for ERC-1155 activity under concurrency control."""
    async with semaphore:
        try:
            if await has_erc1155_tokens(session, address):
                logger.debug(f"ERC-1155 activity found for {address}")
                return address
        except Exception as e:
            logger.error(f"Error processing {address}: {e}")
    return None


async def save_addresses(addresses: List[str], filename: str) -> None:
    """Append found ERC-1155 addresses to file safely."""
    if not addresses:
        return
    async with aiofiles.open(filename, "a") as f:
        for addr in addresses:
            await f.write(f"{addr}\n")


async def main() -> None:
    logger.info("🚀 Starting ERC-1155 scanner...")
    start_time = time.time()

    timeout = ClientTimeout(total=TIMEOUT)
    connector = TCPConnector(limit_per_host=MAX_CONCURRENT_TASKS, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Could not retrieve latest block. Exiting.")
            return

        logger.info(f"📦 Fetching transactions from block #{latest_block}...")
        transactions = await get_block_transactions(session, latest_block)

        addresses: Set[str] = {
            addr.lower()
            for tx in transactions
            for addr in (tx.get("from"), tx.get("to"))
            if addr
        }

        if not addresses:
            logger.warning(f"No addresses found in block #{latest_block}.")
            return

        logger.info(f"🔍 {len(addresses)} unique addresses found. Checking for ERC-1155 activity...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        results: List[str] = []

        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(process_address(session, addr, semaphore)) for addr in addresses]

            for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking"):
                address = await task
                if address:
                    results.append(address)

        unique_addresses = sorted(set(results))
        if unique_addresses:
            await save_addresses(unique_addresses, SAVE_FILE)
            logger.info(f"✅ {len(unique_addresses)} ERC-1155 addresses saved to {SAVE_FILE}")
        else:
            logger.info("No ERC-1155 activity detected.")

    elapsed = time.time() - start_time
    logger.info(f"⏱️ Scan completed in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")

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
MAX_CONCURRENT_TASKS: int = 15
RETRY_LIMIT: int = 4
BACKOFF_BASE: float = 2.0
TIMEOUT: float = 12.0
RATE_LIMIT_SLEEP: float = 1.0

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
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
                if resp.status == 429:
                    wait = min(BACKOFF_BASE ** attempt + random.uniform(0.5, 2.0), 60)
                    logger.warning(f"[429] Rate limited. Retrying in {wait:.1f}s...")
                    await asyncio.sleep(wait)
                    continue

                if resp.status >= 500:
                    wait = min(BACKOFF_BASE ** attempt, 30)
                    logger.warning(f"Server error {resp.status}. Retrying in {wait:.1f}s...")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                data = await resp.json()

                if data.get("status") == "0" and data.get("message") != "No transactions found":
                    logger.debug(f"API returned status 0: {data.get('message')} | {params}")
                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = min(BACKOFF_BASE ** attempt + random.uniform(0.2, 1.0), 30)
            logger.warning(f"Attempt {attempt}/{retries} failed: {e}. Retrying in {wait:.1f}s...")
            await asyncio.sleep(wait)

        except Exception as e:
            logger.exception(f"Unexpected error during fetch: {e}")
            break

    logger.error(f"All {retries} retries failed for params: {params}")
    return None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Retrieve the latest block number."""
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": API_KEY
    })
    if not data or "result" not in data:
        logger.error("Failed to get latest block number.")
        return None

    try:
        return int(data["result"], 16)
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid block number format: {e}")
        return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Fetch all transactions from the specified block."""
    data = await fetch_json(session, {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY
    })
    if not data or "result" not in data:
        return []
    return data["result"].get("transactions", [])


async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    """Check if the given address has interacted with ERC-1155 contracts."""
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
    """Process one address under concurrency limit and detect ERC-1155 activity."""
    async with semaphore:
        try:
            result = await has_erc1155_tokens(session, address)
            if result:
                logger.debug(f"ERC-1155 activity detected for {address}")
                return address
        except Exception as e:
            logger.error(f"Error processing address {address}: {e}")
        finally:
            await asyncio.sleep(RATE_LIMIT_SLEEP * random.uniform(0.5, 1.5))  # Smooth request pacing
    return None


async def save_addresses(addresses: List[str], filename: str):
    """Safely append valid addresses to a file."""
    if not addresses:
        return
    async with aiofiles.open(filename, "a") as f:
        await f.writelines(f"{addr}\n" for addr in addresses)


async def main():
    logger.info("🚀 Starting ERC-1155 address scanner...")
    start_time = time.time()

    timeout = ClientTimeout(total=TIMEOUT)
    connector = TCPConnector(limit=None, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Failed to retrieve the latest block. Exiting.")
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
            logger.warning(f"No addresses found in block {latest_block}.")
            return

        logger.info(f"🔍 {len(addresses)} unique addresses found. Checking ERC-1155 interactions...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        tasks = [process_address(session, addr, semaphore) for addr in addresses]

        results = []
        for result in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking"):
            try:
                address = await result
                if address:
                    results.append(address)
            except Exception as e:
                logger.error(f"Task failed: {e}")

        valid_addresses = sorted(set(results))
        if valid_addresses:
            await save_addresses(valid_addresses, SAVE_FILE)
            logger.info(f"✅ {len(valid_addresses)} ERC-1155 addresses saved to {SAVE_FILE}.")
        else:
            logger.info("No ERC-1155 activity found in this block.")

    elapsed = time.time() - start_time
    logger.info(f"⏱️ Scan completed in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")

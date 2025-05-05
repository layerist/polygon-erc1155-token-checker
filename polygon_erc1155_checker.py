import asyncio
import aiohttp
import aiofiles
import os
import logging
from typing import Optional, Dict, Any, List, Set
from tqdm.asyncio import tqdm

# Configuration
API_KEY = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL = "https://api.polygonscan.com/api"
SAVE_FILE = "erc1155_addresses.txt"
MAX_CONCURRENT_TASKS = 10
RETRY_LIMIT = 3
BACKOFF_DELAY = 2
TIMEOUT = 10

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def fetch_json(session: aiohttp.ClientSession, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Perform an HTTP GET request with retries and exponential backoff."""
    url = BASE_URL
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            async with session.get(url, params=params, timeout=TIMEOUT) as response:
                if response.status == 429:
                    logger.warning("Rate limit hit. Retrying after delay.")
                    await asyncio.sleep(BACKOFF_DELAY * attempt)
                    continue

                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request error: {e} (Attempt {attempt}/{RETRY_LIMIT})")
            await asyncio.sleep(BACKOFF_DELAY * attempt)

    logger.error(f"Request failed after {RETRY_LIMIT} retries. Params: {params}")
    return None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Fetch the latest block number from Polygonscan."""
    data = await fetch_json(session, {
        "module": "proxy", "action": "eth_blockNumber", "apikey": API_KEY
    })
    if data and "result" in data:
        try:
            return int(data["result"], 16)
        except ValueError:
            logger.error("Failed to parse block number.")
    return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Fetch all transactions for a specific block."""
    data = await fetch_json(session, {
        "module": "proxy", "action": "eth_getBlockByNumber",
        "tag": hex(block_number), "boolean": "true", "apikey": API_KEY
    })
    return data.get("result", {}).get("transactions", []) if data else []


async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    """Check whether an address has ERC-1155 token transactions."""
    data = await fetch_json(session, {
        "module": "account", "action": "tokennfttx", "address": address,
        "startblock": 0, "endblock": 99999999, "sort": "asc", "apikey": API_KEY
    })

    return bool(data and isinstance(data.get("result"), list) and any(
        tx.get("tokenID") and tx.get("tokenName") for tx in data["result"]
    ))


async def process_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Optional[str]:
    """Evaluate an address for ERC-1155 token activity within a semaphore limit."""
    async with semaphore:
        if await has_erc1155_tokens(session, address):
            logger.debug(f"ERC-1155 activity found: {address}")
            return address
        return None


async def main():
    """Main coroutine to orchestrate blockchain scanning."""
    async with aiohttp.ClientSession() as session:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Could not fetch the latest block number. Exiting.")
            return

        transactions = await get_block_transactions(session, latest_block)
        addresses = {tx.get("from") for tx in transactions}.union(
                    {tx.get("to") for tx in transactions}) - {None}

        logger.info(f"Scanning {len(addresses)} addresses from block {latest_block}.")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        tasks = [process_address(session, addr, semaphore) for addr in tqdm(addresses, desc="Checking ERC1155")]

        results = await asyncio.gather(*tasks)
        valid_addresses = [addr for addr in results if addr]

        async with aiofiles.open(SAVE_FILE, "w") as f:
            await f.writelines(f"{addr}\n" for addr in valid_addresses)

        logger.info(f"{len(valid_addresses)} addresses saved with ERC-1155 token activity.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting.")

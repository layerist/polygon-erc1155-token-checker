import asyncio
import aiohttp
import aiofiles
import os
import logging
from typing import Optional, Dict, Any, List, Set
from tqdm.asyncio import tqdm

# Configuration constants
API_KEY = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL = "https://api.polygonscan.com/api"
SAVE_FILE = "erc1155_addresses.txt"
MAX_CONCURRENT_TASKS = 10
RETRY_LIMIT = 3
BACKOFF_DELAY = 2
TIMEOUT = 10

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def fetch_json(session: aiohttp.ClientSession, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Perform an HTTP GET request with retries and exponential backoff."""
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            async with session.get(url, params=params, timeout=TIMEOUT) as response:
                if response.status == 429:
                    logger.warning("Rate limit hit. Backing off.")
                    await asyncio.sleep(BACKOFF_DELAY * attempt)
                    continue

                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request failed: {e} (Attempt {attempt}/{RETRY_LIMIT})")
            await asyncio.sleep(BACKOFF_DELAY * attempt)
    logger.error(f"Failed to fetch data after {RETRY_LIMIT} attempts: {url}")
    return None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Retrieve the latest block number from Polygonscan."""
    params = {"module": "proxy", "action": "eth_blockNumber", "apikey": API_KEY}
    data = await fetch_json(session, BASE_URL, params)
    if data and "result" in data:
        return int(data["result"], 16)
    logger.error("Unable to retrieve latest block number.")
    return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Retrieve transactions for a specific block."""
    params = {
        "module": "proxy", "action": "eth_getBlockByNumber",
        "tag": hex(block_number), "boolean": "true", "apikey": API_KEY
    }
    data = await fetch_json(session, BASE_URL, params)
    return data.get("result", {}).get("transactions", []) if data else []


async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    """Check if an address has ERC1155 token transactions."""
    params = {
        "module": "account", "action": "tokennfttx", "address": address,
        "startblock": 0, "endblock": 99999999, "sort": "asc", "apikey": API_KEY
    }
    data = await fetch_json(session, BASE_URL, params)
    if data and isinstance(data.get("result"), list):
        return any(tx.get("tokenID") and tx.get("tokenName") for tx in data["result"])
    return False


async def process_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Optional[str]:
    """Process an address to check for ERC1155 transactions."""
    async with semaphore:
        if await has_erc1155_tokens(session, address):
            logger.debug(f"Address has ERC1155: {address}")
            return address
        return None


async def main():
    """Main function to coordinate the tasks."""
    async with aiohttp.ClientSession() as session:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Failed to retrieve latest block. Exiting.")
            return

        transactions = await get_block_transactions(session, latest_block)
        addresses: Set[str] = {tx.get("from") for tx in transactions} | {tx.get("to") for tx in transactions}
        addresses.discard(None)
        logger.info(f"Found {len(addresses)} unique addresses in block {latest_block}.")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        tasks = [asyncio.create_task(process_address(session, addr, semaphore)) for addr in tqdm(addresses, desc="Checking ERC1155")]

        results = await asyncio.gather(*tasks)
        valid_addresses = [addr for addr in results if addr]

        async with aiofiles.open(SAVE_FILE, "w") as file:
            await file.writelines(f"{addr}\n" for addr in valid_addresses)

        logger.info(f"Saved {len(valid_addresses)} addresses holding ERC1155 tokens.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Exiting...")

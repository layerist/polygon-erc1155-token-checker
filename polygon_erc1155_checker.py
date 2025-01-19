import asyncio
import aiohttp
import aiofiles
import time
import os
import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List, Set
from tqdm.asyncio import tqdm

# Configuration constants
API_KEY = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL = "https://api.polygonscan.com/api"
SAVE_FILE = "erc1155_addresses.txt"
MAX_CONCURRENT_TASKS = 10
RETRY_LIMIT = 3  # Retry limit for failed requests
BACKOFF_DELAY = 2  # Initial delay for exponential backoff

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@asynccontextmanager
async def retry_request(
    session: aiohttp.ClientSession, url: str, params: Dict[str, Any], retries: int = RETRY_LIMIT
) -> Optional[Dict[str, Any]]:
    """Retry failed HTTP requests with exponential backoff."""
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=10) as response:
                response.raise_for_status()
                yield await response.json()
                return
        except (aiohttp.ClientResponseError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                f"Error during request: {e} - URL: {url} (Attempt {attempt + 1}/{retries})"
            )
        await asyncio.sleep(BACKOFF_DELAY * (2 ** attempt))  # Exponential backoff
    logger.error(f"Failed to fetch data after {retries} attempts - URL: {url}")
    yield None

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Retrieve the latest block number."""
    params = {"module": "proxy", "action": "eth_blockNumber", "apikey": API_KEY}
    async with retry_request(session, BASE_URL, params) as data:
        if data and "result" in data:
            return int(data["result"], 16)
    return None

async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Retrieve transactions for a specific block."""
    params = {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY,
    }
    async with retry_request(session, BASE_URL, params) as data:
        if data and "result" in data:
            return data["result"].get("transactions", [])
    return []

async def get_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> List[Dict[str, Any]]:
    """Retrieve ERC1155 tokens for an address."""
    params = {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY,
    }
    async with retry_request(session, BASE_URL, params) as data:
        return data.get("result", []) if data else []

async def process_address(
    session: aiohttp.ClientSession,
    address: str,
    file_lock: asyncio.Lock,
    file_path: str,
    semaphore: asyncio.Semaphore,
) -> None:
    """Retrieve ERC1155 tokens for an address and save it if applicable."""
    async with semaphore:
        tokens = await get_erc1155_tokens(session, address)
        if tokens:
            async with file_lock:
                async with aiofiles.open(file_path, "a") as file:
                    await file.write(f"{address}\n")

async def main():
    """Main function to coordinate the tasks."""
    async with aiohttp.ClientSession() as session:
        # Fetch the latest block number
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Unable to fetch the latest block. Exiting.")
            return

        # Retrieve transactions and extract unique addresses
        transactions = await get_block_transactions(session, latest_block)
        addresses: Set[str] = {tx["from"] for tx in transactions if "from" in tx} | {
            tx["to"] for tx in transactions if "to" in tx
        }
        total_addresses = len(addresses)
        logger.info(f"Found {total_addresses} unique addresses in block {latest_block}")

        # Prepare for concurrent processing
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        file_lock = asyncio.Lock()

        # Initialize the output file
        async with aiofiles.open(SAVE_FILE, "w") as _:
            pass

        # Process addresses with progress tracking
        tasks = [
            process_address(session, address, file_lock, SAVE_FILE, semaphore)
            for address in tqdm(addresses, desc="Processing addresses")
        ]
        await asyncio.gather(*tasks)

        logger.info("All addresses processed successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Exiting...")

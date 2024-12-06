import asyncio
import aiohttp
import time
import os
import sys
import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List

# Configuration constants
API_KEY = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL = "https://api.polygonscan.com/api"
SAVE_FILE = "erc1155_addresses.txt"
MAX_CONCURRENT_TASKS = 10
RETRY_LIMIT = 3  # Retry limit for failed requests

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
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                yield await response.json()
                return
        except aiohttp.ClientResponseError as e:
            logger.warning(f"HTTP {e.status} error: {e.message} - URL: {url} (Attempt {attempt + 1}/{retries})")
        except aiohttp.ClientError as e:
            logger.warning(f"Connection error: {e} - URL: {url} (Attempt {attempt + 1}/{retries})")
        await asyncio.sleep(2 ** attempt)  # Exponential backoff
    logger.error(f"Failed to fetch data after {retries} attempts - URL: {url}")
    yield None

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Retrieve the latest block number."""
    params = {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": API_KEY
    }
    async with retry_request(session, BASE_URL, params) as data:
        if data and "result" in data:
            return int(data['result'], 16)
    return None

async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    """Retrieve transactions for a specific block."""
    params = {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY
    }
    async with retry_request(session, BASE_URL, params) as data:
        if data and "result" in data:
            return data['result'].get('transactions', [])
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
        "apikey": API_KEY
    }
    async with retry_request(session, BASE_URL, params) as data:
        return data.get('result', []) if data else []

async def process_address(
    session: aiohttp.ClientSession,
    address: str,
    file_lock: asyncio.Lock,
    file_path: str,
    semaphore: asyncio.Semaphore,
    i: int,
    total_addresses: int,
    start_time: float
) -> None:
    """Retrieve ERC1155 tokens for an address and track progress."""
    async with semaphore:
        tokens = await get_erc1155_tokens(session, address)
        if tokens:
            async with file_lock:
                async with aiofiles.open(file_path, "a") as file:
                    await file.write(f"{address}\n")

        # Update progress
        elapsed_time = time.time() - start_time
        avg_time_per_address = elapsed_time / i if i > 0 else 0
        remaining_time = avg_time_per_address * (total_addresses - i)
        sys.stdout.write(
            f"\rProgress: {i}/{total_addresses} addresses processed "
            f"({(i / total_addresses) * 100:.2f}%), "
            f"ETA: {remaining_time:.2f}s"
        )
        sys.stdout.flush()

async def main():
    """Main function to coordinate the tasks."""
    async with aiohttp.ClientSession() as session:
        # Fetch the latest block number
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Unable to fetch the latest block. Exiting.")
            return

        # Retrieve transactions and addresses
        transactions = await get_block_transactions(session, latest_block)
        addresses = {tx['from'] for tx in transactions} | {tx['to'] for tx in transactions}
        total_addresses = len(addresses)
        logger.info(f"Found {total_addresses} unique addresses in block {latest_block}")

        start_time = time.time()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        file_lock = asyncio.Lock()

        # Process addresses concurrently
        async with aiofiles.open(SAVE_FILE, "w") as _:  # Initialize the file
            tasks = [
                process_address(session, address, file_lock, SAVE_FILE, semaphore, i, total_addresses, start_time)
                for i, address in enumerate(addresses, 1)
            ]
            await asyncio.gather(*tasks)

        logger.info("All addresses processed successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Exiting...")

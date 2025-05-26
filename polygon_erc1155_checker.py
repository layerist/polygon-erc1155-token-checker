import asyncio
import aiohttp
import aiofiles
import os
import logging
from typing import Optional, Dict, Any, List, Set
from tqdm.asyncio import tqdm
from aiohttp import ClientTimeout

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

async def fetch_json(session: aiohttp.ClientSession, params: Dict[str, Any], retries: int = RETRY_LIMIT) -> Optional[Dict[str, Any]]:
    """Perform an HTTP GET request with retries and exponential backoff."""
    url = BASE_URL
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    logger.warning(f"Rate limit hit. Retrying after {BACKOFF_DELAY * attempt}s...")
                    await asyncio.sleep(BACKOFF_DELAY * attempt)
                    continue

                response.raise_for_status()
                data = await response.json()
                if data.get("status") == "0":
                    logger.warning(f"API returned error: {data.get('message')}. Params: {params}")
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request error: {e} (Attempt {attempt}/{retries})")
            await asyncio.sleep(BACKOFF_DELAY * attempt)
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            break
    logger.error(f"Request failed after {retries} retries. Params: {params}")
    return None

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    data = await fetch_json(session, {
        "module": "proxy", "action": "eth_blockNumber", "apikey": API_KEY
    })
    if data and "result" in data:
        try:
            return int(data["result"], 16)
        except (ValueError, TypeError):
            logger.error(f"Failed to parse block number: {data.get('result')}")
    return None

async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> List[Dict[str, Any]]:
    data = await fetch_json(session, {
        "module": "proxy", "action": "eth_getBlockByNumber",
        "tag": hex(block_number), "boolean": "true", "apikey": API_KEY
    })
    return data.get("result", {}).get("transactions", []) if data else []

async def has_erc1155_tokens(session: aiohttp.ClientSession, address: str) -> bool:
    data = await fetch_json(session, {
        "module": "account", "action": "tokennfttx",
        "address": address, "startblock": 0, "endblock": 99999999, "sort": "asc", "apikey": API_KEY
    })
    return bool(data and isinstance(data.get("result"), list) and any(
        tx.get("tokenID") and tx.get("tokenName") for tx in data["result"]
    ))

async def process_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Optional[str]:
    async with semaphore:
        if await has_erc1155_tokens(session, address):
            logger.debug(f"ERC-1155 activity found: {address}")
            return address
    return None

async def save_addresses(addresses: List[str], filename: str):
    async with aiofiles.open(filename, "w") as f:
        await f.writelines(f"{addr}\n" for addr in addresses)

async def main():
    timeout = ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            logger.error("Could not fetch the latest block number. Exiting.")
            return

        transactions = await get_block_transactions(session, latest_block)
        addresses: Set[str] = {tx.get("from") for tx in transactions}.union(
                               {tx.get("to") for tx in transactions}) - {None}

        if not addresses:
            logger.warning(f"No addresses found in block {latest_block}. Exiting.")
            return

        logger.info(f"Scanning {len(addresses)} addresses from block {latest_block}...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        tasks = [process_address(session, addr, semaphore) for addr in tqdm(addresses, desc="Checking ERC-1155")]
        results = await asyncio.gather(*tasks)

        valid_addresses = [addr for addr in results if addr]
        if valid_addresses:
            await save_addresses(valid_addresses, SAVE_FILE)
            logger.info(f"Saved {len(valid_addresses)} addresses with ERC-1155 token activity to {SAVE_FILE}.")
        else:
            logger.info("No addresses with ERC-1155 token activity found.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting.")

import asyncio
import aiohttp
import time
import os
import sys
import logging

# Задаем параметры через переменные окружения
API_KEY = os.getenv("POLYGONSCAN_API_KEY", "YOUR_API_KEY")
BASE_URL = "https://api.polygonscan.com/api"
SAVE_FILE = "erc1155_addresses.txt"

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def fetch_json(session, url, params):
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        logger.error(f"Request failed: {e}")
        return None

async def get_latest_block(session):
    params = {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": API_KEY
    }
    data = await fetch_json(session, BASE_URL, params)
    if data and "result" in data:
        latest_block = int(data['result'], 16)
        logger.info(f"Latest block number: {latest_block}")
        return latest_block
    else:
        logger.error("Failed to get the latest block")
        return None

async def get_block_transactions(session, block_number):
    params = {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY
    }
    data = await fetch_json(session, BASE_URL, params)
    if data and "result" in data:
        transactions = data['result'].get('transactions', [])
        logger.info(f"Retrieved {len(transactions)} transactions from block {block_number}")
        return transactions
    else:
        logger.error(f"Failed to get transactions for block {block_number}")
        return []

async def get_erc1155_tokens(session, address):
    params = {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY
    }
    data = await fetch_json(session, BASE_URL, params)
    if data and "result" in data:
        return data['result']
    else:
        logger.error(f"Failed to get ERC1155 tokens for address {address}")
        return []

async def process_address(session, address, file, i, total_addresses, start_time):
    tokens = await get_erc1155_tokens(session, address)
    if tokens:
        async with asyncio.Lock():
            file.write(f"{address}\n")

    # Обновление прогресса и оценка времени выполнения
    elapsed_time = time.time() - start_time
    remaining_addresses = total_addresses - i
    avg_time_per_address = elapsed_time / i if i > 0 else 0
    remaining_time = avg_time_per_address * remaining_addresses
    sys.stdout.write(
        f"\rProgress: {i}/{total_addresses} addresses checked, "
        f"Estimated time remaining: {remaining_time:.2f} seconds"
    )
    sys.stdout.flush()

async def main():
    async with aiohttp.ClientSession() as session, open(SAVE_FILE, "w") as file:
        latest_block = await get_latest_block(session)
        if latest_block is None:
            return

        transactions = await get_block_transactions(session, latest_block)
        addresses = {tx['from'] for tx in transactions} | {tx['to'] for tx in transactions}
        total_addresses = len(addresses)
        logger.info(f"Found {total_addresses} unique addresses in the latest block")

        start_time = time.time()

        tasks = [
            process_address(session, address, file, i, total_addresses, start_time)
            for i, address in enumerate(addresses, 1)
        ]
        await asyncio.gather(*tasks)

        logger.info("\nProcess completed successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user. Exiting...")

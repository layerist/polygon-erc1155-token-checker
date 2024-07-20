import requests
import time
import sys

# Replace with your actual Polygonscan API key
API_KEY = "YOUR_API_KEY"
BASE_URL = "https://api.polygonscan.com/api"

def get_latest_block():
    params = {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        latest_block = int(response.json().get('result', '0x0'), 16)
        print(f"Latest block number: {latest_block}")
        return latest_block
    except requests.RequestException as e:
        print(f"Failed to get the latest block: {e}")
        return None

def get_block_transactions(block_number):
    params = {
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        transactions = response.json().get('result', {}).get('transactions', [])
        print(f"Retrieved {len(transactions)} transactions from block {block_number}")
        return transactions
    except requests.RequestException as e:
        print(f"Failed to get transactions for block {block_number}: {e}")
        return []

def get_erc1155_tokens(address):
    params = {
        "module": "account",
        "action": "tokennfttx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        tokens = response.json().get('result', [])
        return tokens
    except requests.RequestException as e:
        print(f"Failed to get ERC1155 tokens for address {address}: {e}")
        return []

def save_data(data, filename="erc1155_addresses.txt"):
    try:
        with open(filename, "w") as file:
            for address in data:
                file.write(f"{address}\n")
        print(f"Data saved to {filename}")
    except IOError as e:
        print(f"Failed to save data to {filename}: {e}")

def main():
    latest_block = get_latest_block()
    if latest_block is None:
        return

    transactions = get_block_transactions(latest_block)
    addresses = {tx['from'] for tx in transactions} | {tx['to'] for tx in transactions}
    total_addresses = len(addresses)
    print(f"Found {total_addresses} unique addresses in the latest block")

    erc1155_addresses = []
    start_time = time.time()

    for i, address in enumerate(addresses, 1):
        print(f"Checking address {address} for ERC1155 tokens... ({i}/{total_addresses})")
        tokens = get_erc1155_tokens(address)
        if tokens:
            erc1155_addresses.append(address)

        # Progress and time estimation
        elapsed_time = time.time() - start_time
        remaining_addresses = total_addresses - i
        if i > 0:
            avg_time_per_address = elapsed_time / i
            remaining_time = avg_time_per_address * remaining_addresses
            sys.stdout.write(
                f"\rProgress: {i}/{total_addresses} addresses checked, "
                f"Estimated time remaining: {remaining_time:.2f} seconds"
            )
            sys.stdout.flush()

    save_data(erc1155_addresses)
    print("\nProcess completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting...")

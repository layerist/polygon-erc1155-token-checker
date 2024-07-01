# Polygon ERC1155 Token Checker

This Python script retrieves the latest block from the Polygon blockchain, identifies all wallet addresses involved in transactions within that block, checks each address for the presence of ERC1155 NFT tokens, and saves the addresses holding ERC1155 tokens to a file. The script provides real-time progress updates, including the number of addresses checked, the remaining number of addresses, and an estimated remaining time.

## Features

- Fetches the latest block number from the Polygon blockchain.
- Retrieves all transactions within the latest block.
- Identifies all unique wallet addresses involved in these transactions.
- Checks each address for ERC1155 NFT tokens.
- Saves addresses holding ERC1155 tokens to a file.
- Provides real-time progress updates and estimated remaining time.
- Handles keyboard interruptions gracefully.

## Prerequisites

- Python 3.x
- Requests library: Install using `pip install requests`
- A Polygonscan API key: Obtain one from [Polygonscan](https://polygonscan.com/)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/layerist/polygon-erc1155-checker.git
cd polygon-erc1155-checker
```

2. Install the required packages:

```bash
pip install requests
```

3. Replace `"YOUR_API_KEY"` with your actual Polygonscan API key in the script.

## Usage

Run the script using Python:

```bash
python polygon_erc1155_checker.py
```

The script will:
1. Fetch the latest block number.
2. Retrieve transactions in that block.
3. Identify all unique wallet addresses involved in the transactions.
4. Check each address for ERC1155 NFT tokens.
5. Save the addresses holding ERC1155 tokens to `erc1155_addresses.txt`.

### Example Output

The script provides real-time feedback in the console:

```
Latest block number: 12345678
Retrieved 100 transactions from block 12345678
Found 150 unique addresses in the latest block
Checking address 0xabc... for ERC1155 tokens... (1/150)
Checking address 0xdef... for ERC1155 tokens... (2/150)
...
Progress: 75/150 addresses checked, Estimated time remaining: 60.00 seconds
...
Data saved to erc1155_addresses.txt
Process completed successfully.
```

### Handling Interruptions

You can stop the script at any time using `Ctrl+C`. The script will handle the interruption gracefully and exit:

```
Script interrupted by user. Exiting...
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository.
2. Create your feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m 'Add some feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a pull request.

## Acknowledgments

- [Polygonscan](https://polygonscan.com/) for providing the API to interact with the Polygon blockchain.

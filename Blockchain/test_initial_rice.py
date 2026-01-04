#!/usr/bin/env python3
"""Test script for initial rice record functions."""

from web3 import Web3
import json

# --- Configuration ---
OPERATIONS_RPC_URL = "http://127.0.0.1:8546"
PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
WALLET_ADDRESS = Web3.to_checksum_address("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266")

# --- Connect to Operations Blockchain ---
web3_operations = Web3(Web3.HTTPProvider(OPERATIONS_RPC_URL))
print("Operations Blockchain Connected:", web3_operations.is_connected())

# Get chain ID
chain_id = web3_operations.eth.chain_id
print(f"Chain ID: {chain_id}")

# --- Load Operations ABI ---
try:
    with open("operations-abi.json") as f:
        operations_abi = json.load(f)
    print("✓ Loaded operations-abi.json")
except FileNotFoundError:
    print("✗ operations-abi.json not found")
    exit(1)

# Load Operations contract address
try:
    with open("operations-abi-address.json") as f:
        address_data = json.load(f)
        operations_address = address_data["address"]
except FileNotFoundError:
    print("✗ operations-abi-address.json not found")
    exit(1)

print(f"Operations Contract: {operations_address}")

# Initialize contract
operations_contract = web3_operations.eth.contract(
    address=Web3.to_checksum_address(operations_address),
    abi=operations_abi
)

# Check if saveInitialRiceRecord function exists
print("\n--- Checking contract functions ---")
try:
    # List all functions in the contract
    functions = [item['name'] for item in operations_abi if item.get('type') == 'function']
    if 'saveInitialRiceRecord' in functions:
        print("✓ saveInitialRiceRecord function found in ABI")
    else:
        print("✗ saveInitialRiceRecord function NOT found in ABI")
        print(f"Available functions: {functions}")
except Exception as e:
    print(f"Error checking functions: {e}")

# Test saving initial rice record
print("\n--- Testing Initial Rice Record ---")
try:
    print("Simulating saveInitialRiceRecord call...")
    result = operations_contract.functions.saveInitialRiceRecord(
        "farmer001",
        "Basmati",
        1000,
        1704326400,  # Unix timestamp
        True
    ).call({
        'from': WALLET_ADDRESS
    })
    print(f"✓ Call simulation succeeded, returned record ID: {result}")
    
    # Now actually send the transaction
    print("\nSending saveInitialRiceRecord transaction...")
    tx = operations_contract.functions.saveInitialRiceRecord(
        "farmer001",
        "Basmati",
        1000,
        1704326400,  # Unix timestamp
        True
    ).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
    })
    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print(f"Transaction sent: {tx_hash.hex()}")
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print(f"✓ Transaction mined! Block number: {receipt.blockNumber}")
except Exception as e:
    print(f"✗ Transaction failed: {e}")
    import traceback
    traceback.print_exc()

# Test getting initial rice record
print("\n--- Testing Get Initial Rice Record ---")
try:
    print("Retrieving initial rice record (ID: 1)...")
    result = operations_contract.functions.getInitialRiceRecord(1).call()
    print(f"✓ Retrieved record:")
    print(f"  User ID: {result[0]}")
    print(f"  Rice Type: {result[1]}")
    print(f"  Quantity: {result[2]}")
    print(f"  Date: {result[3]}")
    print(f"  Status: {result[4]}")
except Exception as e:
    print(f"✗ Failed to get record: {e}")

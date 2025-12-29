"""Test Sepolia network connection"""
from web3 import Web3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Sepolia RPC URL
SEPOLIA_RPC_URL = os.getenv('SEPOLIA_RPC_URL')
PRIVATE_KEY = os.getenv('PRIVATE_KEY')
CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')

print(f"Sepolia RPC URL: {SEPOLIA_RPC_URL}")
print(f"Contract Address: {CONTRACT_ADDRESS}")

# Connect to Sepolia
web3 = Web3(Web3.HTTPProvider(SEPOLIA_RPC_URL))

# Test connection
print(f"\nConnection Status: {web3.is_connected()}")

if web3.is_connected():
    print(f"Chain ID: {web3.eth.chain_id}")
    print(f"Latest Block: {web3.eth.block_number}")
    
    # Get wallet address from private key
    if PRIVATE_KEY:
        from eth_account import Account
        if not PRIVATE_KEY.startswith('0x'):
            PRIVATE_KEY = '0x' + PRIVATE_KEY
        account = Account.from_key(PRIVATE_KEY)
        wallet_address = account.address
        print(f"\nWallet Address: {wallet_address}")
        
        # Get balance
        balance_wei = web3.eth.get_balance(wallet_address)
        balance_eth = web3.from_wei(balance_wei, 'ether')
        print(f"Balance: {balance_eth} ETH")
        
        # Get current gas price
        gas_price = web3.eth.gas_price
        gas_price_gwei = web3.from_wei(gas_price, 'gwei')
        print(f"Current Gas Price: {gas_price_gwei} Gwei")
    
    # Check if contract exists
    if CONTRACT_ADDRESS:
        contract_code = web3.eth.get_code(Web3.to_checksum_address(CONTRACT_ADDRESS))
        if contract_code and contract_code != b'\x00':
            print(f"\n✓ Contract found at {CONTRACT_ADDRESS}")
            print(f"  Contract code size: {len(contract_code)} bytes")
        else:
            print(f"\n✗ No contract found at {CONTRACT_ADDRESS}")
else:
    print("✗ Failed to connect to Sepolia network")

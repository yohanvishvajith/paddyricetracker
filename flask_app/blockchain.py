from web3 import Web3
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
# --- Configuration ---
# Use Sepolia network for UserAccounts blockchain
ACCOUNTS_RPC_URL = os.getenv('SEPOLIA_RPC_URL')
OPERATIONS_RPC_URL = os.getenv('OPERATIONS_RPC_URL')

# Get private key from environment (remove '0x' prefix if present)
PRIVATE_KEY = os.getenv('PRIVATE_KEY')
if PRIVATE_KEY and not PRIVATE_KEY.startswith('0x'):
    PRIVATE_KEY = '0x' + PRIVATE_KEY

# Get wallet address from environment
WALLET_ADDRESS_ENV = os.getenv('WALLET_ADDRESS')
if WALLET_ADDRESS_ENV:
    WALLET_ADDRESS = Web3.to_checksum_address(WALLET_ADDRESS_ENV)
else:
    # Fallback: derive from private key
    from eth_account import Account
    account = Account.from_key(PRIVATE_KEY)
    WALLET_ADDRESS = Web3.to_checksum_address(account.address)

print(f"Using wallet address: {WALLET_ADDRESS}")

# --- Connect to UserAccounts Blockchain (Sepolia) ---
web3_accounts = Web3(Web3.HTTPProvider(ACCOUNTS_RPC_URL))
print(f"UserAccounts Blockchain Connected to {ACCOUNTS_RPC_URL}: {web3_accounts.is_connected()}")

# --- Connect to Operations Blockchain ---
web3_operations = Web3(Web3.HTTPProvider(OPERATIONS_RPC_URL))
print("Operations Blockchain Connected:", web3_operations.is_connected())

# --- Load UserAccounts ABI ---
with open("user-accounts-abi.json") as f:
    user_accounts_abi = json.load(f)

# Load UserAccounts contract address from environment or file
user_accounts_address = os.getenv('CONTRACT_ADDRESS')
if not user_accounts_address:
    try:
        with open("user-accounts-abi-address.json") as f:
            user_accounts_address = json.load(f)["address"]
    except FileNotFoundError:
        print("Warning: CONTRACT_ADDRESS not set in .env and user-accounts-abi-address.json not found")
        user_accounts_address = None

user_accounts_contract = web3_accounts.eth.contract(
    address=Web3.to_checksum_address(user_accounts_address),
    abi=user_accounts_abi
)

# --- Load Operations ABI ---
with open("operations-abi.json") as f:
    operations_abi = json.load(f)

# Load Operations contract address from environment or file
operations_address = os.getenv('OPERATIONS_ADDRESS')
if not operations_address:
    try:
        with open("operations-abi-address.json") as f:
            operations_address = json.load(f)["address"]
    except FileNotFoundError:
        print("Warning: OPERATIONS_ADDRESS not set in .env and operations-abi-address.json not found")
        operations_address = None

operations_contract = web3_operations.eth.contract(
    address=Web3.to_checksum_address(operations_address),
    abi=operations_abi
)

print(f"UserAccounts Contract: {user_accounts_address}")
print(f"Operations Contract: {operations_address}")

# Legacy compatibility - Keep web3 and contract references for backward compatibility
web3 = web3_accounts  # Default to accounts blockchain
contract = user_accounts_contract  # Default to user accounts contract


# ========================================
# HELPER FUNCTIONS
# ========================================

def get_gas_price(web3_instance):
    """Get current gas price from the network, with fallback."""
    try:
        gas_price = web3_instance.eth.gas_price
        # Add 10% buffer for faster confirmation
        return int(gas_price * 1.1)
    except Exception as e:
        print(f"Failed to fetch gas price: {e}, using default")
        return web3_instance.to_wei('20', 'gwei')


def build_and_send_transaction(web3_instance, contract_function, from_address, value=0):
    """Helper function to build, sign, and send a transaction."""
    try:
        # Build transaction
        tx = contract_function.build_transaction({
            'from': from_address,
            'nonce': web3_instance.eth.get_transaction_count(from_address),
            'gas': 2000000,
            'gasPrice': get_gas_price(web3_instance),
            'value': value
        })
        
        # Sign transaction
        signed_tx = web3_instance.eth.account.sign_transaction(tx, PRIVATE_KEY)
        
        # Send transaction
        tx_hash = web3_instance.eth.send_raw_transaction(signed_tx.raw_transaction)
        print("Transaction sent:", tx_hash.hex())
        
        # Wait for receipt
        receipt = web3_instance.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
        print("Transaction mined! Block number:", receipt.blockNumber)
        print("Transaction hash:", tx_hash.hex())
        
        return {
            'block_hash': receipt.blockHash.hex(),
            'block_number': receipt.blockNumber,
            'transaction_hash': tx_hash.hex()
        }
    except Exception as e:
        print(f"Transaction failed: {e}")
        return None


# ========================================
# FARMER FUNCTIONS
# ========================================

def add_farmer(
    farmer_id: str,
    nic: str,
    full_name: str,
   
    district: str,
   
    total_paddy_area: int,
    value_eth: float = 0.0,
):
    """Register a farmer on the blockchain."""
    farmer_input = (
        farmer_id,
        nic,
        full_name,
       
        district,
   
        total_paddy_area,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerFarmer(farmer_input).call({
            'from': WALLET_ADDRESS,
            'value': value
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerFarmer(farmer_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())

    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_farmer(
    farmer_id: str,
    nic: str,
    full_name: str,
    district: str,
    total_paddy_area: int,
    value_eth: float = 0.0,
):
    """Update a farmer on the blockchain."""
    farmer_input = (
        farmer_id,
        nic,
        full_name,
        district,
        total_paddy_area,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateFarmer(farmer_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateFarmer(farmer_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def view_farmer(farmer_id: str):
    """View a farmer by ID."""
    try:
        farmer = user_accounts_contract.functions.getFarmer(farmer_id).call()
        print("\n--- Farmer Data ---")
        print("ID:", farmer[0])
        print("NIC:", farmer[1])
        print("Full Name:", farmer[2])
        print("Address:", farmer[3])
        print("District:", farmer[4])
        print("Contact:", farmer[5])
        print("Total Paddy Field Area:", farmer[6])
        return farmer
    except Exception as e:
        print("Error fetching farmer:", e)
        return None



# ========================================
# COLLECTOR FUNCTIONS
# ========================================

def add_collector(
    collector_id: str,
    nic: str,
    full_name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register a collector on the blockchain."""
    collector_input = (
        collector_id,
        nic,
        full_name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerCollector(collector_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerCollector(collector_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_collector(
    collector_id: str,
    nic: str,
    full_name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Update a collector on the blockchain."""
    collector_input = (
        collector_id,
        nic,
        full_name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateCollector(collector_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateCollector(collector_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def view_collector(collector_id: str):
    """View a collector by ID."""
    try:
        collector = user_accounts_contract.functions.getCollector(collector_id).call()
        print("\n--- Collector Data ---")
        print("ID:", collector[0])
        print("NIC:", collector[1])
        print("Full Name:", collector[2])
        print("Address:", collector[3])
        print("District:", collector[4])
        print("Contact:", collector[5])
        return collector
    except Exception as e:
        print("Error fetching collector:", e)
        return None



# ========================================
# MILLER FUNCTIONS
# ========================================

def add_miller(
    miller_id: str,
    company_register_number: str,
    company_name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register a miller on the blockchain."""
    miller_input = (
        miller_id,
        company_register_number,
        company_name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerMiller(miller_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerMiller(miller_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_miller(
    miller_id: str,
    company_register_number: str,
    company_name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Update a miller on the blockchain."""
    miller_input = (
        miller_id,
        company_register_number,
        company_name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateMiller(miller_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateMiller(miller_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def view_miller(miller_id: str):
    """View a miller by ID."""
    try:
        miller = user_accounts_contract.functions.getMiller(miller_id).call()
        print("\n--- Miller Data ---")
        print("ID:", miller[0])
        print("Company Register Number:", miller[1])
        print("Company Name:", miller[2])
        print("Address:", miller[3])
        print("District:", miller[4])
        print("Contact:", miller[5])
        return miller
    except Exception as e:
        print("Error fetching miller:", e)
        return None



# ========================================
# WHOLESALER FUNCTIONS
# ========================================

def add_wholesaler(
    wholesaler_id: str,
    company_register_number: str,
    company_name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register a wholesaler on the blockchain."""
    wholesaler_input = (
        wholesaler_id,
        company_register_number,
        company_name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerWholesaler(wholesaler_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerWholesaler(wholesaler_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_wholesaler(
    wholesaler_id: str,
    company_register_number: str,
    company_name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Update a wholesaler on the blockchain."""
    wholesaler_input = (
        wholesaler_id,
        company_register_number,
        company_name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateWholesaler(wholesaler_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateWholesaler(wholesaler_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }



# ========================================
# RETAILER FUNCTIONS
# ========================================

def add_retailer(
    retailer_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register a retailer on the blockchain."""
    retailer_input = (
        retailer_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerRetailer(retailer_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerRetailer(retailer_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_retailer(
    retailer_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Update a retailer on the blockchain."""
    retailer_input = (
        retailer_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateRetailer(retailer_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateRetailer(retailer_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }



# ========================================
# BREWER (BEER) FUNCTIONS
# ========================================

def add_brewer(
    brewer_id: str,
    company_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register a brewer on the blockchain."""
    brewer_input = (
        brewer_id,
        company_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerBrewer(brewer_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerBrewer(brewer_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_brewer(
    brewer_id: str,
    company_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Update a brewer on the blockchain."""
    brewer_input = (
        brewer_id,
        company_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateBrewer(brewer_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateBrewer(brewer_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }



# ========================================
# ANIMAL FOOD FUNCTIONS
# ========================================

def add_animal_food(
    animal_food_id: str,
    company_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register an animal food company on the blockchain."""
    animal_food_input = (
        animal_food_id,
        company_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerAnimalFood(animal_food_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerAnimalFood(animal_food_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_animal_food(
    animal_food_id: str,
    company_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Update an animal food company on the blockchain."""
    animal_food_input = (
        animal_food_id,
        company_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateAnimalFood(animal_food_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateAnimalFood(animal_food_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


# ========================================
# EXPORTER FUNCTIONS
# ========================================

def add_exporter(
    exporter_id: str,
    company_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register an exporter on the blockchain."""
    exporter_input = (
        exporter_id,
        company_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.registerExporter(exporter_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.registerExporter(exporter_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }


def update_exporter(
    exporter_id: str,
    company_id: str,
    name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Update an exporter on the blockchain."""
    exporter_input = (
        exporter_id,
        company_id,
        name,
        home_address,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        user_accounts_contract.functions.updateExporter(exporter_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = user_accounts_contract.functions.updateExporter(exporter_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex()
    }



# ========================================
# TRANSACTION FUNCTIONS
# ========================================

def record_transaction(
    from_party: str,
    to_party: str,
    product_type: str,
    quantity: int,
    price: float = 0.0,
    value_eth: float = 0.0,
    status: bool = True,  # True for normal (1), False for revert (0)
):
    """Record a transaction on the blockchain."""
    value = web3_operations.to_wei(value_eth, 'ether')
    # Convert price to 2 decimal places and multiply by 100 to preserve precision in blockchain
    price_int = int(round(float(price) * 100, 0)) if price else 0

    try:
        operations_contract.functions.recordTransaction(
            from_party,
            to_party,
            product_type,
            quantity,
            price_int,
            status
        ).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = operations_contract.functions.recordTransaction(
        from_party,
        to_party,
        product_type,
        quantity,
        price_int,
        status
    ).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    
    # Extract transaction ID from TransactionRecorded event
    transaction_id = None
    try:
        print(f"DEBUG: Number of logs in receipt: {len(receipt.get('logs', []))}")
        for i, log in enumerate(receipt.get('logs', [])):
            print(f"DEBUG: Processing log {i}, topics: {log.get('topics', [])}")
            try:
                # Try TransactionRecorded event
                decoded = operations_contract.events.TransactionRecorded().process_log(log)
                tx_id = decoded['args']['txId']
                transaction_id = tx_id
                print(f"✓ Saved transaction id: {tx_id}")
                break
            except Exception as e:
                print(f"DEBUG: Log {i} decode failed: {str(e)[:100]}")
    except Exception as e:
        print(f"Could not extract transaction ID from event: {e}")
    
    if transaction_id is None:
        print("WARNING: No transaction ID extracted from event logs")
    
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex(),
        'transaction_id': transaction_id
    }


def view_all_transactions():
    """View all recorded transactions."""
    try:
        transactions = operations_contract.functions.getAllTransactions().call()
        print("\n--- All Recorded Transactions ---")
        for tx in transactions:
            print("\nFrom:", tx[0])
            print("To:", tx[1])
            print("Product Type:", tx[2])
            print("Quantity:", tx[3])
            print("Timestamp:", tx[4])
        return transactions
    except Exception as e:
        print("Error fetching transactions:", e)
        return []


# ========================================
# DAMAGE RECORD FUNCTIONS
# ========================================

def record_damage(
    user_id: str,
    paddy_type: str,
    quantity: int,
    damage_date: int,
    reason: str,
    value_eth: float = 0.0,
):
    """Record damage on the blockchain. Includes a `reason` field added in the updated contract."""
    # Pass arguments separately to match ABI: recordDamage(string,string,uint256,uint256,string)
    value = web3_operations.to_wei(value_eth, 'ether')

    try:
        operations_contract.functions.recordDamage(
            user_id,
            paddy_type,
            int(quantity),
            int(damage_date),
            reason,
        ).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = operations_contract.functions.recordDamage(
        user_id,
        paddy_type,
        int(quantity),
        int(damage_date),
        reason,
    ).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    # Try to decode DamageRecorded event to get damage id
    dmg_id = None
    try:
        for log in receipt.logs:
            try:
                decoded = operations_contract.events.DamageRecorded().process_log(log)
                dmg_id = decoded['args'].get('damageId')
                if dmg_id is None:
                    dmg_id = decoded['args'].get('damage_id') or decoded['args'].get('id')
                if dmg_id is not None:
                    print(f"✓ Saved damage id: {dmg_id}")
                    break
            except Exception:
                continue
    except Exception as e:
        print("Failed to decode DamageRecorded event:", e)
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex(),
        'damage_id': int(dmg_id) if dmg_id is not None else None
    }


def view_all_damage_records():
    """View all damage records."""
    try:
        damage_records = operations_contract.functions.getAllDamageRecords().call()
        print("\n--- All Damage Records ---")
        for dr in damage_records:
            print("\nUser ID:", dr[0])
            print("Paddy Type:", dr[1])
            print("Quantity:", dr[2])
            print("Damage Date:", dr[3])
        return damage_records
    except Exception as e:
        print("Error fetching damage records:", e)
        return []


def record_milling(miller_id, paddy_type, input_qty, output_qty, date, drying_duration=0, status_flag=True):
    """Record milling operation on the blockchain and return the block hash and milling ID.
    
    Args:
        miller_id: Miller ID
        paddy_type: Type of paddy
        input_qty: Input quantity
        output_qty: Output quantity
        date: Date timestamp
        drying_duration: Drying duration (default 0)
        status_flag: Status flag - True for completed (1), False for reverted (0) (default True)
    """
    print("\n--- Recording Milling on Blockchain ---")
    print(f"Miller ID: {miller_id}")
    print(f"Paddy Type: {paddy_type}")
    print(f"Input Qty: {input_qty}")
    print(f"Output Qty: {output_qty}")
    print(f"Date: {date}")
    print(f"Drying Duration: {drying_duration}")
    print(f"Status: {status_flag}")

    # Call recordMilling with individual arguments: inputPaddy, outputRice, dateTime, paddyType, dryingDuration, status
    value = 0  # No ETH value sent

    # Test call
    try:
        operations_contract.functions.recordMilling(
            input_qty,
            output_qty,
            date,
            paddy_type,
            drying_duration or 0,
            status_flag  # status flag (True = 1 for completed, False = 0 for reverted)
        ).call({'from': WALLET_ADDRESS, 'value': value})
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = operations_contract.functions.recordMilling(
        input_qty,
        output_qty,
        date,
        paddy_type,
        drying_duration or 0,
        status_flag  # status flag (True = 1 for completed, False = 0 for reverted)
    ).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    # Try to decode MillingRecorded event to get milling id
    milling_id = None
    try:
        for log in receipt.logs:
            try:
                decoded = operations_contract.events.MillingRecorded().process_log(log)
                milling_id = decoded['args'].get('millingId')
                if milling_id is None:
                    milling_id = decoded['args'].get('milling_id') or decoded['args'].get('id')
                if milling_id is not None:
                    print(f"✓ Saved milling id: {milling_id}")
                    break
            except Exception:
                continue
    except Exception as e:
        print("Failed to decode MillingRecorded event:", e)
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex(),
        'milling_id': int(milling_id) if milling_id is not None else None
    }


def view_all_milling_records():
    """View all milling records."""
    try:
        milling_records = operations_contract.functions.getAllMillingRecords().call()
        print("\n--- All Milling Records ---")
        for mr in milling_records:
            print("\nMiller ID:", mr[0])
            print("Paddy Type:", mr[1])
            print("Input Qty:", mr[2])
            print("Output Qty:", mr[3])
            print("Date:", mr[4])
        return milling_records
    except Exception as e:
        print("Error fetching milling records:", e)
        return []


def record_rice_transaction(from_party, to_party, rice_type, quantity, price=0.0, status=True):
    """Record a rice transaction on the blockchain using recordRiceTransaction and return the block hash."""
    print("\n--- Recording Rice Transaction on Blockchain ---")
    print(f"From: {from_party}")
    print(f"To: {to_party}")
    print(f"Rice Type: {rice_type}")
    print(f"Quantity: {quantity}")
    print(f"Price: {price:.2f}")
    print(f"Status: {status}")

    qty = int(quantity)
    # Convert price to 2 decimal places and multiply by 100 to preserve precision in blockchain
    price_int = int(round(float(price) * 100, 0)) if price else 0
    value = 0  # No ETH value sent

    # Test call
    try:
        operations_contract.functions.recordRiceTransaction(from_party, to_party, rice_type, qty, price_int, status).call({'from': WALLET_ADDRESS, 'value': value})
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = operations_contract.functions.recordRiceTransaction(from_party, to_party, rice_type, qty, price_int, status).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    
    # Extract transaction ID from RiceTransactionRecorded event
    rice_transaction_id = None
    try:
        print(f"DEBUG: Number of logs in receipt: {len(receipt.get('logs', []))}")
        for i, log in enumerate(receipt.get('logs', [])):
            print(f"DEBUG: Processing log {i}, topics: {log.get('topics', [])}")
            try:
                # Try RiceTransactionRecorded event
                decoded = operations_contract.events.RiceTransactionRecorded().process_log(log)
                rice_tx_id = decoded['args']['riceTxId']
                rice_transaction_id = rice_tx_id
                print(f"✓ Saved rice transaction id: {rice_tx_id}")
                break
            except Exception as e:
                print(f"DEBUG: Log {i} decode failed: {str(e)[:100]}")
    except Exception as e:
        print(f"Could not extract rice transaction ID from event: {e}")
    
    if rice_transaction_id is None:
        print("WARNING: No rice transaction ID extracted from event logs")
    
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex(),
        'transaction_id': rice_transaction_id
    }


def get_rice_transaction(rice_tx_id):
    """Retrieve a rice transaction from the blockchain by ID.
    
    Returns a dictionary with:
    - from_party: Source party identifier
    - to_party: Destination party identifier
    - rice_type: Type of rice
    - quantity: Quantity of rice
    - timestamp: Transaction timestamp
    - status: Transaction status (True/False)
    """
    print(f"\n--- Retrieving Rice Transaction ID: {rice_tx_id} ---")
    
    try:
        result = operations_contract.functions.getRiceTransaction(int(rice_tx_id)).call()
        
        rice_tx_data = {
            'from_party': result[0],
            'to_party': result[1],
            'rice_type': result[2],
            'quantity': result[3],
            'timestamp': result[4],
            'status': result[5]
        }
        
        print(f"Rice Transaction Retrieved:")
        print(f"  From: {rice_tx_data['from_party']}")
        print(f"  To: {rice_tx_data['to_party']}")
        print(f"  Rice Type: {rice_tx_data['rice_type']}")
        print(f"  Quantity: {rice_tx_data['quantity']}")
        print(f"  Timestamp: {rice_tx_data['timestamp']}")
        print(f"  Status: {rice_tx_data['status']}")
        
        return rice_tx_data
    
    except Exception as e:
        print(f"Error retrieving rice transaction: {e}")
        return None


def revert_rice_transaction(from_party, to_party, rice_type, quantity, price=0.0):
    """Revert a rice transaction on the blockchain (record with status=False).
    This is equivalent to recording a transaction with status=False to mark it as reversed.
    
    Args:
        price: Price per unit (will be converted to integer with 2 decimal precision)
    
    Returns a dictionary with:
    - block_hash: Hash of the block containing the revert
    - block_number: Block number
    - transaction_hash: Transaction hash
    - transaction_id: Revert transaction ID on blockchain
    """
    print("\n--- Reverting Rice Transaction on Blockchain ---")
    print(f"From: {from_party}")
    print(f"To: {to_party}")
    print(f"Rice Type: {rice_type}")
    print(f"Quantity: {quantity}")
    print(f"Price: {price:.2f}")
    print("Status: False (Revert)")

    qty = int(quantity)
    # Convert price to 2 decimal places and multiply by 100 to preserve precision in blockchain
    price_int = int(round(float(price) * 100, 0)) if price else 0
    value = 0  # No ETH value sent

    # Test call
    try:
        operations_contract.functions.recordRiceTransaction(from_party, to_party, rice_type, qty, price_int, False).call({'from': WALLET_ADDRESS, 'value': value})
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = operations_contract.functions.recordRiceTransaction(from_party, to_party, rice_type, qty, price_int, False).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Revert transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Revert transaction mined! Block number:", receipt.blockNumber)
    print("Revert transaction mined! Block hash:", receipt.blockHash.hex())
    
    # Extract revert transaction ID from RiceTransactionRecorded event
    revert_rice_transaction_id = None
    try:
        print(f"DEBUG: Number of logs in receipt: {len(receipt.get('logs', []))}")
        for i, log in enumerate(receipt.get('logs', [])):
            print(f"DEBUG: Processing log {i}, topics: {log.get('topics', [])}")
            try:
                # Try RiceTransactionRecorded event
                decoded = operations_contract.events.RiceTransactionRecorded().process_log(log)
                revert_tx_id = decoded['args']['riceTxId']
                revert_rice_transaction_id = revert_tx_id
                print(f"✓ Saved revert transaction id: {revert_tx_id}")
                break
            except Exception as e:
                print(f"DEBUG: Log {i} decode failed: {str(e)[:100]}")
    except Exception as e:
        print(f"Could not extract revert transaction ID from event: {e}")
    
    if revert_rice_transaction_id is None:
        print("WARNING: No revert transaction ID extracted from event logs")
    
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex(),
        'transaction_id': revert_rice_transaction_id
    }


def record_rice_damage(user_id, rice_type, quantity, damage_date, reason, value_eth: float = 0.0):
    """Record rice damage on the blockchain and return the block hash. Includes `reason`."""
    print("\n--- Recording Rice Damage on Blockchain ---")
    print(f"User ID: {user_id}")
    print(f"Rice Type: {rice_type}")
    print(f"Quantity: {quantity}")
    print(f"Damage Date: {damage_date}")
    print(f"Reason: {reason}")

    # Pass arguments separately to match ABI: recordRiceDamage(string,string,uint256,uint256,string)
    value = web3_operations.to_wei(value_eth, 'ether')

    try:
        operations_contract.functions.recordRiceDamage(
            user_id,
            rice_type,
            int(quantity),
            int(damage_date),
            reason,
        ).call({'from': WALLET_ADDRESS, 'value': value})
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    tx = operations_contract.functions.recordRiceDamage(
        user_id,
        rice_type,
        int(quantity),
        int(damage_date),
        reason,
    ).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
        'value': value,
    })

    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash.hex())
    
    # Extract damage ID from RiceDamageRecorded event
    rice_damage_id = None
    try:
        print(f"DEBUG: Number of logs in receipt: {len(receipt.get('logs', []))}")
        for i, log in enumerate(receipt.get('logs', [])):
            print(f"DEBUG: Processing log {i}, topics: {log.get('topics', [])}")
            try:
                # Try RiceDamageRecorded event
                decoded = operations_contract.events.RiceDamageRecorded().process_log(log)
                damage_id = decoded['args']['riceDamageId']
                rice_damage_id = damage_id
                print(f"✓ Saved rice damage id: {damage_id}")
                break
            except Exception as e:
                print(f"DEBUG: Log {i} decode failed: {str(e)[:100]}")
    except Exception as e:
        print(f"Could not extract rice damage ID from event: {e}")
    
    if rice_damage_id is None:
        print("WARNING: No rice damage ID extracted from event logs")
    
    return {
        'block_hash': receipt.blockHash.hex(),
        'block_number': receipt.blockNumber,
        'transaction_hash': tx_hash.hex(),
        'transaction_id': rice_damage_id
    }


def save_initial_paddy_record(user_id, paddy_type, quantity):
    """Record initial paddy to blockchain and return block info and recordId.
    Returns None if blockchain is unavailable (will still save to database).
    """
    import time
    
    print("\n--- Recording Initial Paddy on Blockchain ---")
    print(f"User ID: {user_id}")
    print(f"Paddy Type: {paddy_type}")
    print(f"Quantity: {quantity}")

    # Check if blockchain is connected
    if not web3_operations.is_connected():
        print("⚠️  Operations blockchain is not connected on port 8546")
        print("   Skipping blockchain recording. Record will be saved to database without blockchain data.")
        print("   To enable blockchain: Start Hardhat node with: npx hardhat node --port 8546")
        return None

    # Check if contract is deployed
    try:
        contract_code = web3_operations.eth.get_code(operations_contract.address)
        if len(contract_code) == 0:
            print("⚠️  Operations contract not deployed - skipping blockchain recording")
            print(f"   Contract address: {operations_contract.address}")
            print(f"   Please deploy the contract first using deployment scripts")
            return None
    except Exception as e:
        print(f"⚠️  Cannot check contract deployment: {e}")
        return None

    value = 0  # No ETH value sent
    current_timestamp = int(time.time())

    try:
        # Test call to get recordId
        record_id = operations_contract.functions.saveInitialPaddyRecord(
            user_id, 
            paddy_type, 
            int(quantity), 
            current_timestamp,
            True
        ).call({'from': WALLET_ADDRESS, 'value': value})
        print(f"Call simulation succeeded. Record ID from contract: {record_id}")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return None

    try:
        tx = operations_contract.functions.saveInitialPaddyRecord(
            user_id, 
            paddy_type, 
            int(quantity), 
            current_timestamp,
            True
        ).build_transaction({
            'from': WALLET_ADDRESS,
            'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
            'gas': 2000000,
            'gasPrice': web3_operations.to_wei('20', 'gwei'),
            'value': value,
        })

        signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
        print("Transaction sent:", tx_hash.hex())
        receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
        print("Transaction mined! Block number:", receipt.blockNumber)
        print("Transaction mined! Block hash:", receipt.blockHash.hex())
        
        return {
            'block_hash': receipt.blockHash.hex(),
            'block_number': receipt.blockNumber,
            'transaction_hash': tx_hash.hex(),
            'record_id': record_id
        }
    except Exception as e:
        print(f"Failed to record initial paddy on blockchain: {e}")
        return None


def save_initial_rice_record(user_id, rice_type, quantity):
    """Record initial rice to blockchain and return block info.
    Returns None if blockchain is unavailable (will still save to database).
    """
    import time
    
    print("\n--- Recording Initial Rice on Blockchain ---")
    print(f"User ID: {user_id}")
    print(f"Rice Type: {rice_type}")
    print(f"Quantity: {quantity}")

    # Check if blockchain is connected
    if not web3_operations.is_connected():
        print("⚠️  Operations blockchain is not connected on port 8546")
        print("   Skipping blockchain recording. Record will be saved to database without blockchain data.")
        return None

    # Check if contract is deployed
    try:
        contract_code = web3_operations.eth.get_code(operations_contract.address)
        if len(contract_code) == 0:
            print("⚠️  Operations contract not deployed - skipping blockchain recording")
            print(f"   Contract address: {operations_contract.address}")
            print(f"   Please deploy the contract first using deployment scripts")
            return None
    except Exception as e:
        print(f"⚠️  Cannot check contract deployment: {e}")
        return None

    value = 0  # No ETH value sent
    current_timestamp = int(time.time())

    try:
        # Test call to get recordId
        record_id = operations_contract.functions.saveInitialRiceRecord(
            user_id, 
            rice_type, 
            int(quantity), 
            current_timestamp,
            True
        ).call({'from': WALLET_ADDRESS, 'value': value})
        print(f"Call simulation succeeded. Record ID from contract: {record_id}")
    except Exception as e:
        print(f"⚠️  Call simulation reverted or failed: {e}")
        print("   This usually means the contract hasn't been redeployed with the saveInitialRiceRecord function.")
        print("   Please redeploy the contract on the blockchain.")
        return None

    try:
        tx = operations_contract.functions.saveInitialRiceRecord(
            user_id, 
            rice_type, 
            int(quantity), 
            current_timestamp,
            True
        ).build_transaction({
            'from': WALLET_ADDRESS,
            'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
            'gas': 2000000,
            'gasPrice': web3_operations.to_wei('20', 'gwei'),
            'value': value,
        })

        signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
        print("Transaction sent:", tx_hash.hex())
        receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
        print("Transaction mined! Block number:", receipt.blockNumber)
        print("Transaction mined! Block hash:", receipt.blockHash.hex())
        
        return {
            'block_id': receipt.blockNumber,
            'block_hash': receipt.blockHash.hex(),
            'block_number': receipt.blockNumber,
            'transaction_hash': tx_hash.hex(),
            'record_id': record_id
        }
    except Exception as e:
        print(f"⚠️  Failed to record initial rice on blockchain: {e}")
        print("   The record will still be saved to the database without blockchain fields.")
        return None



# ========================================
# UTILITY FUNCTIONS
# ========================================

def check_connection():
    """Check if web3 is connected to the blockchain."""
    if web3.is_connected():
        print("✓ Connected to blockchain")
        print("Network ID:", web3_operations.eth.chain_id)
        print("Latest block:", web3_operations.eth.block_number)
        return True
    else:
        print("✗ Not connected to blockchain")
        return False


if __name__ == "__main__":
    print("=" * 50)
    print("Rice Supply Chain Blockchain Interface")
    print("=" * 50)
    check_connection()
    
    # Test saveInitialRiceRecord function
    print("\n" + "=" * 50)
    print("Testing saveInitialRiceRecord function...")
    print("=" * 50)
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
    except Exception as e:
        print(f"✗ Call simulation failed: {e}")
        print("   The contract may not have been redeployed with the saveInitialRiceRecord function.")
        print("   Please redeploy the smart contract and update operations-abi-address.json")

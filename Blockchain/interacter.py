from web3 import Web3
import json
import os

# --- Configuration ---
# Two separate RPC URLs for two independent blockchains
ACCOUNTS_RPC_URL = "http://127.0.0.1:8545"  # UserAccounts blockchain
OPERATIONS_RPC_URL = "http://127.0.0.1:8546"  # Operations blockchain

PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
WALLET_ADDRESS = Web3.to_checksum_address("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266")

# --- Connect to UserAccounts Blockchain ---
web3_accounts = Web3(Web3.HTTPProvider(ACCOUNTS_RPC_URL))
print("UserAccounts Blockchain Connected:", web3_accounts.is_connected())

# --- Connect to Operations Blockchain ---
web3_operations = Web3(Web3.HTTPProvider(OPERATIONS_RPC_URL))
print("Operations Blockchain Connected:", web3_operations.is_connected())

# --- Load UserAccounts ABI ---
with open("user-accounts-abi.json") as f:
    user_accounts_abi = json.load(f)

# Load UserAccounts contract address
try:
    with open("user-accounts-abi-address.json") as f:
        user_accounts_address = json.load(f)["address"]
except FileNotFoundError:
    user_accounts_address = "0x5FbDB2315678afecb367f032d93F642f64180aa3"  # default
    print("Warning: user-accounts-abi-address.json not found, using default address")

user_accounts_contract = web3_accounts.eth.contract(
    address=Web3.to_checksum_address(user_accounts_address),
    abi=user_accounts_abi
)

# --- Load Operations ABI ---
with open("operations-abi.json") as f:
    operations_abi = json.load(f)

# Load Operations contract address
try:
    with open("operations-abi-address.json") as f:
        operations_address = json.load(f)["address"]
except FileNotFoundError:
    operations_address = "0x5FbDB2315678afecb367f032d93F642f64180aa3"  # default
    print("Warning: operations-abi-address.json not found, using default address")

operations_contract = web3_operations.eth.contract(
    address=Web3.to_checksum_address(operations_address),
    abi=operations_abi
)

print(f"UserAccounts Contract: {user_accounts_address}")
print(f"Operations Contract: {operations_address}")


# --- Function to Add Farmer ---
def add_farmer(
    farmer_id: str,
    nic: str,
    full_name: str,
    district: str,
    contact_number: str,
    total_paddy_area: int,
    value_eth: float = 0.0,
):
    # Build the struct tuple in the same order as FarmerInput in the Solidity contract
    farmer_input = (
        farmer_id,
        nic,
        full_name,
        district,
        contact_number,
        total_paddy_area,
    )

    # Convert provided ether value to wei
    value = web3_accounts.to_wei(value_eth, 'ether')

    # Optional: simulate the call locally to detect reverts before sending a transaction
    try:
        # Simulate the call locally to detect reverts before sending a transaction
        sim = user_accounts_contract.functions.registerFarmer(farmer_input).call({
            'from': WALLET_ADDRESS,
            'value': value
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

    # Create transaction (include value if you want to send ETH with the call)
    tx = user_accounts_contract.functions.registerFarmer(farmer_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_accounts.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_accounts.to_wei('20', 'gwei'),
        'value': value
    })

    # Sign transaction
    signed_tx = web3_accounts.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_accounts.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())

    # Wait for confirmation
    receipt = web3_accounts.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)
    print("Transaction mined! Block hash:", receipt.blockHash)
    
# --- Function to View Farmer ---
def view_farmer(farmer_id):
    farmer = user_accounts_contract.functions.getFarmer(farmer_id).call()
    print("\n--- Farmer Data ---")
    print("ID:", farmer[0])
    print("NIC:", farmer[1])
    print("Full Name:", farmer[2])
    print("District:", farmer[3])
    print("Contact:", farmer[4])
    print("Total Paddy Field Area:", farmer[5])


def view_all_farmers(from_block: int = 0):
    """Read past FarmerRegistered events starting at `from_block` and print each farmer.

    This uses the FarmerRegistered event emitted by the contract when a farmer is registered.
    Because the contract stores farmers in a mapping and doesn't provide an index/list function,
    we must enumerate registered farmer IDs from events.
    """
    farmers_data = []
    # First, try the on-chain helper that returns all farmers (added in Storage.sol)
    try:
        farmers = user_accounts_contract.functions.getAllFarmers().call()
        if farmers and len(farmers) > 0:
            print("\n--- All Registered Farmers (on-chain) ---")
            for f in farmers:
                # f is a tuple matching the Farmer struct
                farmer_dict = {
                    "id": f[0],
                    "nic": f[1],
                    "full_name": f[2],
                    "district": f[3],
                    "contact": f[4],
                    "total_paddy_field_area": f[5]
                }
                farmers_data.append(farmer_dict)
                print("\nID:", f[0])
                print("NIC:", f[1])
                print("Full Name:", f[2])
                print("District:", f[3])
                print("Contact:", f[4])
                print("Total Paddy Field Area:", f[5])
            # Save to JSON file
            with open("farmers.json", "w") as json_file:
                json.dump(farmers_data, json_file, indent=2)
            print("\n✓ Saved to farmers.json")
            return
        else:
            print("getAllFarmers returned no entries; falling back to event scan.")
    except Exception as e:
        # If the function doesn't exist or call fails, fallback to scanning events
        print("getAllFarmers on-chain call failed or not available, falling back to events:", e)

    # Fallback: enumerate FarmerRegistered events (off-chain)
    try:
        # use the web3.py parameter name 'from_block'
        event_filter = user_accounts_contract.events.FarmerRegistered.create_filter(from_block=from_block)
        entries = event_filter.get_all_entries()
    except Exception:
        print("Falling back to eth.get_logs for FarmerRegistered events")
        event_abi = next((a for a in user_accounts_abi if a.get('name') == 'FarmerRegistered' and a.get('type') == 'event'), None)
        if event_abi is None:
            print("FarmerRegistered event ABI not found")
            return
        # topic for the event signature
        topic = web3_accounts.keccak(text=f"{event_abi['name']}({','.join(i['type'] for i in event_abi['inputs'])})").hex()
        logs = web3_accounts.eth.get_logs({
            'fromBlock': from_block,
            'toBlock': 'latest',
            'address': user_accounts_address,
            'topics': [topic]
        })
        # use process_log (web3.py naming) to decode each log
        entries = [user_accounts_contract.events.FarmerRegistered().process_log(log) for log in logs]

    if not entries:
        print("No FarmerRegistered events found (no farmers registered yet).")
        return

    seen = set()
    print("\n--- All Registered Farmers (events) ---")
    for ev in entries:
        farmer_id = ev['args']['id']
        if farmer_id in seen:
            continue
        seen.add(farmer_id)
        try:
            farmer = user_accounts_contract.functions.getFarmer(farmer_id).call()

            farmer_dict = {
                "id": farmer[0],
                "nic": farmer[1],
                "full_name": farmer[2],
                "district": farmer[3],
                "contact": farmer[4],
                "total_paddy_field_area": farmer[5]
            }
            farmers_data.append(farmer_dict)
            print("\nID:", farmer[0])
            print("NIC:", farmer[1])
            print("Full Name:", farmer[2])
            print("District:", farmer[3])
            print("Contact:", farmer[4])
            print("Total Paddy Field Area:", farmer[5])
        except Exception as e:
            print(f"Failed to read farmer {farmer_id}:", e)
    
    # Save to JSON file
    if farmers_data:
        with open("farmers.json", "w") as json_file:
            json.dump(farmers_data, json_file, indent=2)
        print("\n✓ Saved to farmers.json")


def view_miller(miller_id: str):
    """Call getMiller and print a single miller."""
    miller = user_accounts_contract.functions.getMiller(miller_id).call()
    print("\n--- Miller Data ---")
    print("ID:", miller[0])
    print("Company Register Number:", miller[1])
    print("Company Name:", miller[2])
    print("Address:", miller[3])
    print("District:", miller[4])
    print("Contact:", miller[5])


def view_all_millers(from_block: int = 0):
    """List all millers using on-chain helper or by scanning MillerRegistered events."""
    millers_data = []
    # Try on-chain helper first
    try:
        millers = user_accounts_contract.functions.getAllMillers().call()
        if millers and len(millers) > 0:
            print("\n--- All Registered Millers (on-chain) ---")
            for m in millers:
                miller_dict = {
                    "id": m[0],
                    "company_register_number": m[1],
                    "company_name": m[2],
                    "address": m[3],
                    "district": m[4],
                    "contact": m[5]
                }
                millers_data.append(miller_dict)
                print("\nID:", m[0])
                print("Company Register Number:", m[1])
                print("Company Name:", m[2])
                print("Address:", m[3])
                print("District:", m[4])
                print("Contact:", m[5])
            # Save to JSON file
            with open("millers.json", "w") as json_file:
                json.dump(millers_data, json_file, indent=2)
            print("\n✓ Saved to millers.json")
            return
        else:
            print("getAllMillers returned no entries; falling back to event scan.")
    except Exception as e:
        print("getAllMillers on-chain call failed or not available, falling back to events:", e)

    # Fallback: enumerate MillerRegistered events
    try:
        event_filter = user_accounts_contract.events.MillerRegistered.create_filter(from_block=from_block)
        entries = event_filter.get_all_entries()
    except Exception:
        print("Falling back to eth.get_logs for MillerRegistered events")
        event_abi = next((a for a in user_accounts_abi if a.get('name') == 'MillerRegistered' and a.get('type') == 'event'), None)
        if event_abi is None:
            print("MillerRegistered event ABI not found")
            return
        topic = web3_accounts.keccak(text=f"{event_abi['name']}({','.join(i['type'] for i in event_abi['inputs'])})").hex()
        logs = web3_accounts.eth.get_logs({
            'fromBlock': from_block,
            'toBlock': 'latest',
            'address': user_accounts_address,
            'topics': [topic]
        })
        entries = [user_accounts_contract.events.MillerRegistered().process_log(log) for log in logs]

    if not entries:
        print("No MillerRegistered events found (no millers registered yet).")
        return

    seen = set()
    print("\n--- All Registered Millers (events) ---")
    for ev in entries:
        miller_id = ev['args']['id']
        if miller_id in seen:
            continue
        seen.add(miller_id)
        try:
            m = user_accounts_contract.functions.getMiller(miller_id).call()
            miller_dict = {
                "id": m[0],
                "company_register_number": m[1],
                "company_name": m[2],
                "address": m[3],
                "district": m[4],
                "contact": m[5]
            }
            millers_data.append(miller_dict)
            print("\nID:", m[0])
            print("Company Register Number:", m[1])
            print("Company Name:", m[2])
            print("Address:", m[3])
            print("District:", m[4])
            print("Contact:", m[5])
        except Exception as e:
            print(f"Failed to read miller {miller_id}:", e)
    
    # Save to JSON file
    if millers_data:
        with open("millers.json", "w") as json_file:
            json.dump(millers_data, json_file, indent=2)
        print("\n✓ Saved to millers.json")


def view_collector(collector_id: str):
    """Call getCollector and print a single collector."""
    collector = user_accounts_contract.functions.getCollector(collector_id).call()
    print("\n--- Collector Data ---")
    print("ID:", collector[0])
    print("NIC:", collector[1])
    print("Full Name:", collector[2])
    print("Address:", collector[3])
    print("District:", collector[4])
    print("Contact:", collector[5])


def view_all_collectors(from_block: int = 0):
    """List all collectors using on-chain helper or by scanning CollectorRegistered events."""
    collectors_data = []
    # Try on-chain helper first
    try:
        collectors_list = user_accounts_contract.functions.getAllCollectors().call()
        if collectors_list and len(collectors_list) > 0:
            print("\n--- All Registered Collectors (on-chain) ---")
            for c in collectors_list:
                collector_dict = {
                    "id": c[0],
                    "nic": c[1],
                    "full_name": c[2],
                    "address": c[3],
                    "district": c[4],
                    "contact": c[5]
                }
                collectors_data.append(collector_dict)
                print("\nID:", c[0])
                print("NIC:", c[1])
                print("Full Name:", c[2])
                print("Address:", c[3])
                print("District:", c[4])
                print("Contact:", c[5])
            # Save to JSON file
            with open("collectors.json", "w") as json_file:
                json.dump(collectors_data, json_file, indent=2)
            print("\n✓ Saved to collectors.json")
            return
        else:
            print("getAllCollectors returned no entries; falling back to event scan.")
    except Exception as e:
        print("getAllCollectors on-chain call failed or not available, falling back to events:", e)

    # Fallback: enumerate CollectorRegistered events
    try:
        event_filter = user_accounts_contract.events.CollectorRegistered.create_filter(from_block=from_block)
        entries = event_filter.get_all_entries()
    except Exception:
        print("Falling back to eth.get_logs for CollectorRegistered events")
        event_abi = next((a for a in user_accounts_abi if a.get('name') == 'CollectorRegistered' and a.get('type') == 'event'), None)
        if event_abi is None:
            print("CollectorRegistered event ABI not found")
            return
        topic = web3_accounts.keccak(text=f"{event_abi['name']}({','.join(i['type'] for i in event_abi['inputs'])})").hex()
        logs = web3_accounts.eth.get_logs({
            'fromBlock': from_block,
            'toBlock': 'latest',
            'address': user_accounts_address,
            'topics': [topic]
        })
        entries = [user_accounts_contract.events.CollectorRegistered().process_log(log) for log in logs]

    if not entries:
        print("No CollectorRegistered events found (no collectors registered yet).")
        return

    seen = set()
    print("\n--- All Registered Collectors (events) ---")
    for ev in entries:
        collector_id = ev['args']['id']
        if collector_id in seen:
            continue
        seen.add(collector_id)
        try:
            c = user_accounts_contract.functions.getCollector(collector_id).call()
            collector_dict = {
                "id": c[0],
                "nic": c[1],
                "full_name": c[2],
                "address": c[3],
                "district": c[4],
                "contact": c[5]
            }
            collectors_data.append(collector_dict)
            print("\nID:", c[0])
            print("NIC:", c[1])
            print("Full Name:", c[2])
            print("Address:", c[3])
            print("District:", c[4])
            print("Contact:", c[5])
        except Exception as e:
            print(f"Failed to read collector {collector_id}:", e)
    
    # Save to JSON file
    if collectors_data:
        with open("collectors.json", "w") as json_file:
            json.dump(collectors_data, json_file, indent=2)
        print("\n✓ Saved to collectors.json")


def add_collector(
    collector_id: str,
    nic: str,
    full_name: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register a Collector using the contract's registerCollector function."""
    collector_input = (
        collector_id,
        nic,
        full_name,
        district,
        contact_number,
    )

    value = web3_accounts.to_wei(value_eth, 'ether')

    try:
        # simulate call
        user_accounts_contract.functions.registerCollector(collector_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

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


def prompt_add_collector_interactive():
    print("\nEnter new collector details:")
    collector_id = input("ID: ").strip()
    nic = input("NIC: ").strip()
    full_name = input("Full name: ").strip()
    district = input("District: ").strip()
    contact_number = input("Contact number: ").strip()
    while True:
        value_eth_s = input("Value to send with tx in ETH (default 0): ").strip()
        if value_eth_s == "":
            value_eth = 0.0
            break
        try:
            value_eth = float(value_eth_s)
            break
        except ValueError:
            print("Please enter a numeric value for ETH (e.g. 0 or 0.01).")

    confirm = input(f"Register collector {collector_id} (y/N)? ").strip().lower()
    if confirm != "y":
        print("Cancelled")
        return

    add_collector(
        collector_id,
        nic,
        full_name,
        district,
        contact_number,
        value_eth,
    )

# --- Interactive menu ---
def prompt_add_farmer_interactive():
    print("\nEnter new farmer details:")
    farmer_id = input("ID: ").strip()
    nic = input("NIC: ").strip()
    full_name = input("Full name: ").strip()
    district = input("District: ").strip()
    contact_number = input("Contact number: ").strip()
    while True:
        total_paddy_area_s = input("Total paddy field area (integer): ").strip()
        try:
            total_paddy_area = int(total_paddy_area_s)
            break
        except ValueError:
            print("Please enter a valid integer for total paddy field area.")
    while True:
        value_eth_s = input("Value to send with tx in ETH (default 0): ").strip()
        if value_eth_s == "":
            value_eth = 0.0
            break
        try:
            value_eth = float(value_eth_s)
            break
        except ValueError:
            print("Please enter a numeric value for ETH (e.g. 0 or 0.01).")

    confirm = input(f"Register farmer {farmer_id} (y/N)? ").strip().lower()
    if confirm != "y":
        print("Cancelled")
        return

    add_farmer(
        farmer_id,
        nic,
        full_name,
        district,
        contact_number,
        total_paddy_area,
        value_eth,
    )


def add_miller(
    miller_id: str,
    company_register_number: str,
    company_name: str,
    home_address: str,
    district: str,
    contact_number: str,
    value_eth: float = 0.0,
):
    """Register a Miller using the contract's registerMiller function."""
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
        # simulate call
        user_accounts_contract.functions.registerMiller(miller_input).call({
            'from': WALLET_ADDRESS,
            'value': value,
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

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


def prompt_add_miller_interactive():
    print("\nEnter new miller details:")
    miller_id = input("ID: ").strip()
    company_register_number = input("Company register number: ").strip()
    company_name = input("Company name: ").strip()
    home_address = input("Home address: ").strip()
    district = input("District: ").strip()
    contact_number = input("Contact number: ").strip()
    while True:
        value_eth_s = input("Value to send with tx in ETH (default 0): ").strip()
        if value_eth_s == "":
            value_eth = 0.0
            break
        try:
            value_eth = float(value_eth_s)
            break
        except ValueError:
            print("Please enter a numeric value for ETH (e.g. 0 or 0.01).")

    confirm = input(f"Register miller {miller_id} (y/N)? ").strip().lower()
    if confirm != "y":
        print("Cancelled")
        return

    add_miller(
        miller_id,
        company_register_number,
        company_name,
        home_address,
        district,
        contact_number,
        value_eth,
    )


def record_transaction(from_party: str, to_party: str, product_type: str, quantity: int):
    """Record a transaction on-chain via recordTransaction."""
    try:
        # simulate
        operations_contract.functions.recordTransaction(from_party, to_party, product_type, quantity).call({
            'from': WALLET_ADDRESS
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

    tx = operations_contract.functions.recordTransaction(from_party, to_party, product_type, quantity).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
    })
    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)


def view_transaction(tx_id: int):
    """View a single transaction by ID."""
    t = operations_contract.functions.getTransaction(tx_id).call()
    print("\n--- Transaction ---")
    print("Tx ID:", tx_id)
    print("From:", t[0])
    print("To:", t[1])
    print("Product:", t[2])
    print("Quantity:", t[3])
    print("Timestamp:", t[4])


def view_all_transactions(from_block: int = 0):
    """List all transactions using on-chain helper or scanning TransactionRecorded events."""
    transactions_data = []
    try:
        txs = operations_contract.functions.getAllTransactions().call()
        if txs and len(txs) > 0:
            print("\n--- All Transactions (on-chain) ---")
            for idx, tx in enumerate(txs, start=1):
                tx_dict = {
                    "tx_id": idx,
                    "from_party": tx[0],
                    "to_party": tx[1],
                    "product_type": tx[2],
                    "quantity": tx[3],
                    "timestamp": tx[4]
                }
                transactions_data.append(tx_dict)
                print("\nTx ID:", idx)
                print("From:", tx[0])
                print("To:", tx[1])
                print("Product:", tx[2])
                print("Quantity:", tx[3])
                print("Timestamp:", tx[4])
            # Save to JSON file
            with open("transactions.json", "w") as json_file:
                json.dump(transactions_data, json_file, indent=2)
            print("\n✓ Saved to transactions.json")
            return
        else:
            print("getAllTransactions returned no entries; falling back to events.")
    except Exception as e:
        print("getAllTransactions on-chain call failed or not available, falling back to events:", e)

    # Fallback: scan TransactionRecorded events
    try:
        event_filter = operations_contract.events.TransactionRecorded.create_filter(from_block=from_block)
        entries = event_filter.get_all_entries()
    except Exception:
        print("Falling back to eth.get_logs for TransactionRecorded events")
        event_abi = next((a for a in operations_abi if a.get('name') == 'TransactionRecorded' and a.get('type') == 'event'), None)
        if event_abi is None:
            print("TransactionRecorded event ABI not found")
            return
        topic = web3_operations.keccak(text=f"{event_abi['name']}({','.join(i['type'] for i in event_abi['inputs'])})").hex()
        logs = web3_operations.eth.get_logs({
            'fromBlock': from_block,
            'toBlock': 'latest',
            'address': operations_address,
            'topics': [topic]
        })
        entries = [operations_contract.events.TransactionRecorded().process_log(log) for log in logs]

    if not entries:
        print("No TransactionRecorded events found.")
        return

    print("\n--- All Transactions (events) ---")
    for ev in entries:
        args = ev['args']
        tx_dict = {
            "tx_id": args['txId'],
            "from_party": args['fromParty'],
            "to_party": args['toParty'],
            "product_type": args['productType'],
            "quantity": args['quantity'],
            "timestamp": args['timestamp']
        }
        transactions_data.append(tx_dict)
        print("\nTx ID:", args['txId'])
        print("From:", args['fromParty'])
        print("To:", args['toParty'])
        print("Product:", args['productType'])
        print("Quantity:", args['quantity'])
        print("Timestamp:", args['timestamp'])
    
    # Save to JSON file
    if transactions_data:
        with open("transactions.json", "w") as json_file:
            json.dump(transactions_data, json_file, indent=2)
        print("\n✓ Saved to transactions.json")


# === Rice Transaction Functions ===
def record_rice_transaction(from_party: str, to_party: str, rice_type: str, quantity: int):
    """Record a rice transaction on-chain via recordRiceTransaction."""
    try:
        operations_contract.functions.recordRiceTransaction(from_party, to_party, rice_type, quantity).call({
            'from': WALLET_ADDRESS
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

    tx = operations_contract.functions.recordRiceTransaction(from_party, to_party, rice_type, quantity).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
    })
    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)


def view_all_rice_transactions():
    """List all rice transactions."""
    try:
        txs = operations_contract.functions.getAllRiceTransactions().call()
        if txs and len(txs) > 0:
            print("\n--- All Rice Transactions ---")
            for idx, tx in enumerate(txs, start=1):
                print(f"\nRice Tx ID: {idx}")
                print("From:", tx[0])
                print("To:", tx[1])
                print("Rice Type:", tx[2])
                print("Quantity:", tx[3])
                print("Timestamp:", tx[4])
            return
    except Exception as e:
        print("Failed to get rice transactions:", e)


# === Milling Functions ===
def record_milling(miller_id: str, paddy_type: str, input_qty: int, output_qty: int, date: int):
    """Record a milling operation on-chain."""
    milling_input = (miller_id, paddy_type, input_qty, output_qty, date)
    
    try:
        operations_contract.functions.recordMilling(milling_input).call({
            'from': WALLET_ADDRESS
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

    tx = operations_contract.functions.recordMilling(milling_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
    })
    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)


def view_all_milling_records():
    """List all milling records."""
    try:
        records = operations_contract.functions.getAllMillingRecords().call()
        if records and len(records) > 0:
            print("\n--- All Milling Records ---")
            for idx, rec in enumerate(records, start=1):
                print(f"\nMilling ID: {idx}")
                print("Miller ID:", rec[0])
                print("Paddy Type:", rec[1])
                print("Input Qty:", rec[2])
                print("Output Qty:", rec[3])
                print("Date:", rec[4])
            return
    except Exception as e:
        print("Failed to get milling records:", e)


# === Damage Recording Functions ===
def record_paddy_damage(user_id: str, paddy_type: str, quantity: int, damage_date: int):
    """Record paddy damage on-chain."""
    damage_input = (user_id, paddy_type, quantity, damage_date)
    
    try:
        operations_contract.functions.recordDamage(damage_input).call({
            'from': WALLET_ADDRESS
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

    tx = operations_contract.functions.recordDamage(damage_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
    })
    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)


def record_rice_damage(user_id: str, rice_type: str, quantity: int, damage_date: int):
    """Record rice damage on-chain."""
    damage_input = (user_id, rice_type, quantity, damage_date)
    
    try:
        operations_contract.functions.recordRiceDamage(damage_input).call({
            'from': WALLET_ADDRESS
        })
        print("Call simulation succeeded (no revert).")
    except Exception as e:
        print("Call simulation reverted or failed:", e)
        return

    tx = operations_contract.functions.recordRiceDamage(damage_input).build_transaction({
        'from': WALLET_ADDRESS,
        'nonce': web3_operations.eth.get_transaction_count(WALLET_ADDRESS),
        'gas': 2000000,
        'gasPrice': web3_operations.to_wei('20', 'gwei'),
    })
    signed_tx = web3_operations.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = web3_operations.eth.send_raw_transaction(signed_tx.raw_transaction)
    print("Transaction sent:", tx_hash.hex())
    receipt = web3_operations.eth.wait_for_transaction_receipt(tx_hash)
    print("Transaction mined! Block number:", receipt.blockNumber)


def menu_loop():
    try:
        while True:
            print("\n=== Storage Interactor ===")
            print("--- User Accounts (Blockchain 1 - Port 8545) ---")
            print("1) Add farmer")
            print("2) View all farmers")
            print("3) View farmer by ID")
            print("4) Add miller")
            print("5) View all millers")
            print("6) View miller by ID")
            print("7) View all collectors")
            print("8) View collector by ID")
            print("9) Add collector")
            print("\n--- Operations (Blockchain 2 - Port 8546) ---")
            print("10) Record paddy transaction")
            print("11) View all paddy transactions")
            print("12) View paddy transaction by ID")
            print("13) Record rice transaction")
            print("14) View all rice transactions")
            print("15) Record milling")
            print("16) View all milling records")
            print("17) Record paddy damage")
            print("18) Record rice damage")
            print("\n0) Exit")
            choice = input("Choose an option: ").strip()
            if choice == "1":
                prompt_add_farmer_interactive()
            elif choice == "2":
                from_block_s = input("From block (enter for 0): ").strip()
                try:
                    from_block = int(from_block_s) if from_block_s != "" else 0
                except ValueError:
                    print("Invalid block number, using 0")
                    from_block = 0
                view_all_farmers(from_block)
            elif choice == "3":
                fid = input("Farmer ID: ").strip()
                if fid:
                    try:
                        view_farmer(fid)
                    except Exception as e:
                        print("Failed to view farmer:", e)
                else:
                    print("No ID entered")
            elif choice == "4":
                prompt_add_miller_interactive()
            elif choice == "5":
                from_block_s = input("From block (enter for 0): ").strip()
                try:
                    from_block = int(from_block_s) if from_block_s != "" else 0
                except ValueError:
                    print("Invalid block number, using 0")
                    from_block = 0
                view_all_millers(from_block)
            elif choice == "6":
                mid = input("Miller ID: ").strip()
                if mid:
                    try:
                        view_miller(mid)
                    except Exception as e:
                        print("Failed to view miller:", e)
                else:
                    print("No ID entered")
            elif choice == "7":
                from_block_s = input("From block (enter for 0): ").strip()
                try:
                    from_block = int(from_block_s) if from_block_s != "" else 0
                except ValueError:
                    print("Invalid block number, using 0")
                    from_block = 0
                view_all_collectors(from_block)
            elif choice == "8":
                cid = input("Collector ID: ").strip()
                if cid:
                    try:
                        view_collector(cid)
                    except Exception as e:
                        print("Failed to view collector:", e)
                else:
                    print("No ID entered")
            elif choice == "9":
                prompt_add_collector_interactive()
            elif choice == "10":
                print("\nEnter transaction details:")
                from_party = input("From ID: ").strip()
                to_party = input("To ID: ").strip()
                product_type = input("Product type: ").strip()
                qty_s = input("Quantity (integer): ").strip()
                try:
                    quantity = int(qty_s)
                except ValueError:
                    print("Invalid quantity")
                    continue
                confirm = input(f"Record transaction from {from_party} to {to_party} (y/N)? ").strip().lower()
                if confirm != "y":
                    print("Cancelled")
                else:
                    try:
                        record_transaction(from_party, to_party, product_type, quantity)
                    except Exception as e:
                        print("Failed to record transaction:", e)
            elif choice == "11":
                from_block_s = input("From block (enter for 0): ").strip()
                try:
                    from_block = int(from_block_s) if from_block_s != "" else 0
                except ValueError:
                    print("Invalid block number, using 0")
                    from_block = 0
                view_all_transactions(from_block)
            elif choice == "12":
                txid_s = input("Transaction ID (integer): ").strip()
                try:
                    txid = int(txid_s)
                except ValueError:
                    print("Invalid transaction ID")
                    continue
                try:
                    view_transaction(txid)
                except Exception as e:
                    print("Failed to view transaction:", e)
            elif choice == "13":
                print("\nEnter rice transaction details:")
                from_party = input("From ID: ").strip()
                to_party = input("To ID: ").strip()
                rice_type = input("Rice type: ").strip()
                qty_s = input("Quantity (integer): ").strip()
                try:
                    quantity = int(qty_s)
                except ValueError:
                    print("Invalid quantity")
                    continue
                confirm = input(f"Record rice transaction from {from_party} to {to_party} (y/N)? ").strip().lower()
                if confirm == "y":
                    try:
                        record_rice_transaction(from_party, to_party, rice_type, quantity)
                    except Exception as e:
                        print("Failed to record rice transaction:", e)
            elif choice == "14":
                view_all_rice_transactions()
            elif choice == "15":
                print("\nEnter milling details:")
                miller_id = input("Miller ID: ").strip()
                paddy_type = input("Paddy type: ").strip()
                input_qty_s = input("Input quantity: ").strip()
                output_qty_s = input("Output quantity: ").strip()
                date_s = input("Date (unix timestamp): ").strip()
                try:
                    input_qty = int(input_qty_s)
                    output_qty = int(output_qty_s)
                    date = int(date_s)
                except ValueError:
                    print("Invalid input")
                    continue
                confirm = input(f"Record milling for miller {miller_id} (y/N)? ").strip().lower()
                if confirm == "y":
                    try:
                        record_milling(miller_id, paddy_type, input_qty, output_qty, date)
                    except Exception as e:
                        print("Failed to record milling:", e)
            elif choice == "16":
                view_all_milling_records()
            elif choice == "17":
                print("\nEnter paddy damage details:")
                user_id = input("User ID: ").strip()
                paddy_type = input("Paddy type: ").strip()
                qty_s = input("Quantity: ").strip()
                date_s = input("Damage date (unix timestamp): ").strip()
                try:
                    quantity = int(qty_s)
                    damage_date = int(date_s)
                except ValueError:
                    print("Invalid input")
                    continue
                confirm = input(f"Record paddy damage for user {user_id} (y/N)? ").strip().lower()
                if confirm == "y":
                    try:
                        record_paddy_damage(user_id, paddy_type, quantity, damage_date)
                    except Exception as e:
                        print("Failed to record damage:", e)
            elif choice == "18":
                print("\nEnter rice damage details:")
                user_id = input("User ID: ").strip()
                rice_type = input("Rice type: ").strip()
                qty_s = input("Quantity: ").strip()
                date_s = input("Damage date (unix timestamp): ").strip()
                try:
                    quantity = int(qty_s)
                    damage_date = int(date_s)
                except ValueError:
                    print("Invalid input")
                    continue
                confirm = input(f"Record rice damage for user {user_id} (y/N)? ").strip().lower()
                if confirm == "y":
                    try:
                        record_rice_damage(user_id, rice_type, quantity, damage_date)
                    except Exception as e:
                        print("Failed to record damage:", e)
            elif choice == "0" or choice.lower() in ("q", "exit"):
                print("Bye")
                break
            else:
                print("Invalid choice")
    except KeyboardInterrupt:
        print("\nInterrupted — exiting.")

if __name__ == "__main__":
    menu_loop()

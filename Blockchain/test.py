from web3 import Web3
from web3.exceptions import ContractLogicError
import json, os, time, sys
from dotenv import load_dotenv

# Fix Unicode encoding for Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# ------------------ SETUP ------------------
load_dotenv()

w3 = Web3(Web3.HTTPProvider(os.getenv("RPC_URL")))
assert w3.is_connected(), "❌ Blockchain not connected"

account = w3.eth.account.from_key(os.getenv("PRIVATE_KEY"))
w3.eth.default_account = account.address

with open("operations-abi.json") as f:
    abi = json.load(f)

contract = w3.eth.contract(
    address=Web3.to_checksum_address(os.getenv("CONTRACT_ADDRESS")),
    abi=abi
)

# ------------------ TX HELPER ------------------
def send_tx(tx):
    signed = w3.eth.account.sign_transaction(tx, account.key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    print(f"✅ TX Hash: {tx_hash.hex()} | Block: {receipt.blockNumber}")

    # Extract event logs from receipt - parse manually
    try:
        for log in receipt['logs']:
            try:
                # Try InitialPaddyRecorded
                decoded = contract.events.InitialPaddyRecorded().process_log(log)
                rec_id = decoded['args']['recordId']
                print(f"✓ Saved initial paddy id: {rec_id}")
            except:
                pass
            
            try:
                # Try TransactionRecorded
                decoded = contract.events.TransactionRecorded().process_log(log)
                tx_id = decoded['args']['txId']
                print(f"✓ Saved transaction id: {tx_id}")
            except:
                pass
            
            try:
                # Try DamageRecorded
                decoded = contract.events.DamageRecorded().process_log(log)
                dmg_id = decoded['args']['damageId']
                print(f"✓ Saved damage id: {dmg_id}")
            except:
                pass
    except Exception as e:
        pass

# ------------------ FUNCTIONS ------------------
def save_initial_paddy():
    user = input("User ID: ")
    paddy = input("Paddy Type: ")
    qty = int(input("Quantity: "))

    # Save date as yymmddhhmm integer
    date_yymmddhhmm = int(time.strftime("%y%m%d%H%M", time.localtime()))

    status = input("Status (1=active, 0=inactive): ") == "1"

    tx = contract.functions.saveInitialPaddyRecord(
        user, paddy, qty, date_yymmddhhmm, status
    ).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": 300000,
        "gasPrice": w3.eth.gas_price
    })
    send_tx(tx)

# bulk save removed

def record_transaction():
    from_party = input("From Party: ")
    to_party = input("To Party: ")
    product = input("Product Type: ")
    qty = int(input("Quantity: "))
    price = int(input("Price (per unit): "))
    status = input("Status (1=success, 0=fail): ") == "1"

    tx = contract.functions.recordTransaction(
        from_party, to_party, product, qty, price, status
    ).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": 300000,
        "gasPrice": w3.eth.gas_price
    })
    send_tx(tx)

def record_damage():
    user = input("User ID: ")
    paddy = input("Paddy Type: ")
    qty = int(input("Damaged Quantity: "))
    reason = input("Reason: ")

    # Save damage date as yymmddhhmm integer
    damage_date_yymmddhhmm = int(time.strftime("%y%m%d%H%M", time.localtime()))

    tx = contract.functions.recordDamage(
        user, paddy, qty, damage_date_yymmddhhmm, reason
    ).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": 300000,
        "gasPrice": w3.eth.gas_price
    })
    send_tx(tx)

def get_initial_paddy():
    try:
        rid = int(input("Record ID: "))
    except ValueError:
        print("❌ Invalid input — enter a numeric ID")
        return

    try:
        data = contract.functions.getInitialPaddyRecord(rid).call()
    except ContractLogicError:
        print("❌ Record not found or invalid ID")
        return
    except Exception as e:
        print("❌ Error calling contract:", e)
        return

    print("\nUser:", data[0])
    print("Paddy:", data[1])
    print("Quantity:", data[2])
    print("Date:", data[3])
    print("Status:", data[4])

def get_transaction():
    try:
        txid = int(input("Transaction ID: "))
    except ValueError:
        print("❌ Invalid input — enter a numeric ID")
        return

    try:
        data = contract.functions.getTransaction(txid).call()
    except ContractLogicError:
        print("❌ Transaction not found or invalid ID")
        return
    except Exception as e:
        print("❌ Error calling contract:", e)
        return

    print("\nFrom:", data[0])
    print("To:", data[1])
    print("Product:", data[2])
    print("Qty:", data[3])
    print("Price:", data[4])
    print("Time:", data[5])
    print("Status:", data[6])

def get_damage():
    try:
        did = int(input("Damage ID: "))
    except ValueError:
        print("❌ Invalid input — enter a numeric ID")
        return

    try:
        data = contract.functions.getDamage(did).call()
    except ContractLogicError:
        print("❌ Damage record not found or invalid ID")
        return
    except Exception as e:
        print("❌ Error calling contract:", e)
        return

    print("\nUser:", data[0])
    print("Paddy:", data[1])
    print("Qty:", data[2])
    print("Date:", data[3])
    print("Reason:", data[4])

# ------------------ MENU ------------------
def menu():
    print("""
========= OPERATIONS CONTRACT =========
1. Save Initial Paddy
2. Record Transaction
3. Record Damage
4. Get Initial Paddy
5. Get Transaction
6. Get Damage
0. Exit
=======================================
""")

# ------------------ MAIN LOOP ------------------
while True:
    menu()
    choice = input("Select option: ")

    match choice:
        case "1": save_initial_paddy()
        case "2": record_transaction()
        case "3": record_damage()
        case "4": get_initial_paddy()
        case "5": get_transaction()
        case "6": get_damage()
        case "0":
            print("👋 Exiting")
            break
        case _:
            print("❌ Invalid option")

from app import get_connection
import sys

conn = get_connection('rice_supply')
cursor = conn.cursor()

print("\n" + "="*80)
print("FIXING REVERTED RICE TRANSACTIONS WITH MISSING PRICES")
print("="*80)

# Find all reverted rice transactions with price = 0
cursor.execute('''
    SELECT id, `from`, `to`, rice_type, quantity, datetime 
    FROM rice_transaction 
    WHERE reverted = 1 AND (price = 0 OR price IS NULL)
    ORDER BY id
''')
reverted_with_no_price = cursor.fetchall()

if not reverted_with_no_price:
    print("\n✓ No reverted transactions with missing prices found!")
    conn.close()
    sys.exit(0)

print(f"\n Found {len(reverted_with_no_price)} reverted transactions with missing prices:")
print("-"*80)
for r in reverted_with_no_price:
    print(f"ID: {r[0]}, From: {r[1]}, To: {r[2]}, Rice: {r[3]}, Qty: {r[4]}, Date: {r[5]}")
print("-"*80)

# For each reverted transaction, find the most recent non-reverted transaction
# with the same from, to, and rice_type to copy the price
fixes_applied = 0
for r in reverted_with_no_price:
    reverted_id = r[0]
    from_user = r[1]
    to_user = r[2]
    rice_type = r[3]
    reverted_datetime = r[5]
    
    # Find a non-reverted transaction with same from/to/rice_type that happened before this reversal
    cursor.execute('''
        SELECT id, price 
        FROM rice_transaction 
        WHERE `from` = %s AND `to` = %s AND rice_type = %s 
        AND reverted = 0 AND price > 0
        AND datetime < %s
        ORDER BY datetime DESC
        LIMIT 1
    ''', (from_user, to_user, rice_type, reverted_datetime))
    
    original = cursor.fetchone()
    
    if original:
        original_id = original[0]
        original_price = original[1]
        
        # Update the reverted transaction with the original price
        cursor.execute('''
            UPDATE rice_transaction 
            SET price = %s 
            WHERE id = %s
        ''', (original_price, reverted_id))
        
        conn.commit()
        fixes_applied += 1
        print(f"✓ Fixed ID {reverted_id}: Copied price {original_price} from transaction ID {original_id}")
    else:
        print(f"✗ Could not find original transaction for ID {reverted_id}")

print("-"*80)
print(f"\n✓ Applied {fixes_applied} fixes!")
print("="*80 + "\n")

conn.close()

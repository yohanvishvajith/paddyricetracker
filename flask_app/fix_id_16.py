from app import get_connection

conn = get_connection('rice_supply')
cursor = conn.cursor()

print("\nManually fixing reverted transaction ID 16...")

# Get the price from the previous transaction (ID 15)
cursor.execute('SELECT price FROM rice_transaction WHERE id = 15')
row = cursor.fetchone()
if row:
    price = row[0]
    print(f"Found price {price} from transaction ID 15")
    
    # Update ID 16 with this price
    cursor.execute('UPDATE rice_transaction SET price = %s WHERE id = 16', (price,))
    conn.commit()
    print(f"✓ Updated transaction ID 16 with price {price}")
    
    # Verify
    cursor.execute('SELECT id, price, reverted FROM rice_transaction WHERE id IN (15, 16)')
    rows = cursor.fetchall()
    print("\nVerification:")
    for r in rows:
        print(f"  ID {r[0]}: price={r[1]}, reverted={r[2]}")
else:
    print("✗ Could not find transaction ID 15")

conn.close()

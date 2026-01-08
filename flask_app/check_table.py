from app import get_connection

conn = get_connection('rice_supply')
cursor = conn.cursor()
cursor.execute('DESCRIBE rice_transaction')
cols = cursor.fetchall()
print("\nCurrent rice_transaction table structure:")
print("-" * 50)
for c in cols:
    print(f"{c[0]}: {c[1].decode() if isinstance(c[1], bytes) else c[1]}")
print("-" * 50)

# Check some recent data
cursor.execute('SELECT id, quantity, price, reverted FROM rice_transaction ORDER BY id DESC LIMIT 6')
rows = cursor.fetchall()
print("\nRecent 6 rice transactions (id, quantity, price, reverted):")
print("-" * 50)
for r in rows:
    print(f"ID: {r[0]}, Qty: {r[1]}, Price: {r[2]}, Reverted: {r[3]}")
print("-" * 50)

conn.close()

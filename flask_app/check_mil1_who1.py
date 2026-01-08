from app import get_connection

conn = get_connection('rice_supply')
cursor = conn.cursor()

# Get all transactions for MIL1 -> WHO1
cursor.execute('''
    SELECT id, `from`, `to`, rice_type, quantity, price, reverted, datetime 
    FROM rice_transaction 
    WHERE `from` = 'MIL1' AND `to` = 'WHO1'
    ORDER BY datetime DESC
''')
rows = cursor.fetchall()

print("\nAll rice transactions from MIL1 to WHO1:")
print("-"*100)
print(f"{'ID':<5} {'From':<10} {'To':<10} {'Rice Type':<15} {'Qty':<10} {'Price':<10} {'Rev':<5} {'DateTime':<20}")
print("-"*100)
for r in rows:
    print(f"{r[0]:<5} {r[1]:<10} {r[2]:<10} {r[3]:<15} {r[4]:<10} {r[5] if r[5] else '-':<10} {r[6]:<5} {str(r[7]):<20}")
print("-"*100)

conn.close()

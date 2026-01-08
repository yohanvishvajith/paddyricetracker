from app import get_connection

conn = get_connection('rice_supply')
cursor = conn.cursor()

# Get transactions from today ordered by datetime descending
cursor.execute('''
    SELECT id, `from`, `to`, rice_type, quantity, price, reverted, datetime, block_hash 
    FROM rice_transaction 
    WHERE DATE(datetime) = CURDATE()
    ORDER BY datetime DESC
    LIMIT 10
''')
rows = cursor.fetchall()
print("\nAll rice transactions from today:")
print("-" * 100)
print(f"{'ID':<5} {'From':<10} {'To':<10} {'Rice Type':<15} {'Qty':<10} {'Price':<10} {'Rev':<5} {'DateTime':<20} {'Block':<12}")
print("-" * 100)
for r in rows:
    block = r[8][:10] if r[8] else 'None'
    print(f"{r[0]:<5} {r[1]:<10} {r[2]:<10} {r[3]:<15} {r[4]:<10} {r[5] or '-':<10} {r[6]:<5} {str(r[7]):<20} {block}...")
print("-" * 100)

conn.close()

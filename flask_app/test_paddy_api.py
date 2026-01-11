import mysql.connector
import os

# MySQL configuration - same as app.py
MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '12345678')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'rice_supply')

print(f"Connecting to MySQL with:")
print(f"  Host: {MYSQL_HOST}")
print(f"  Port: {MYSQL_PORT}")
print(f"  User: {MYSQL_USER}")
print(f"  Database: {MYSQL_DATABASE}")
print(f"  Password: {'(empty)' if not MYSQL_PASSWORD else '(set)'}")

try:
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT
    )
    print("\n✓ Connection successful!")
    
    cur = conn.cursor()
    
    # Check if paddy_type table exists
    cur.execute("SHOW TABLES LIKE 'paddy_type'")
    result = cur.fetchone()
    
    if result:
        print("\n✓ Table 'paddy_type' exists")
        
        # Check table structure
        cur.execute("DESCRIBE paddy_type")
        columns = cur.fetchall()
        print("\nTable structure:")
        for col in columns:
            print(f"  {col[0]} - {col[1]}")
        
        # Check data
        cur.execute("SELECT * FROM paddy_type")
        rows = cur.fetchall()
        print(f"\n✓ Found {len(rows)} paddy types:")
        for row in rows:
            print(f"  {row}")
    else:
        print("\n✗ Table 'paddy_type' does NOT exist")
        print("\nCreating table...")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS `paddy_type` (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        ''')
        print("✓ Table created successfully!")
    
    cur.close()
    conn.close()
    
except mysql.connector.Error as err:
    print(f"\n✗ Error: {err}")

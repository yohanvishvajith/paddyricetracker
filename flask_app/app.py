from flask import Flask, render_template, request, jsonify, session
import os
from dotenv import load_dotenv
import mysql.connector
import datetime
from blockchain import add_farmer, add_miller, add_collector, add_wholesaler, add_retailer, add_brewer, add_animal_food, add_exporter, update_farmer, update_miller, update_collector, update_wholesaler, update_retailer, update_brewer, update_animal_food, update_exporter, record_transaction, record_damage, record_milling, record_rice_transaction, revert_rice_transaction, record_rice_damage
from mysql.connector import errorcode
# (Blockchain integration removed) This application no longer attempts to register users on-chain.
# load .env from project root if present

load_dotenv()

app = Flask(__name__)
# server-side sessions: set a secret key (override with FLASK_SECRET in prod)
app.secret_key = os.environ.get('FLASK_SECRET', 'dev-secret')
# MySQL configuration - change via environment variables or edit below
MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'rice_supply')


def get_connection(db=None):
    cfg = {
        'host': MYSQL_HOST,
        'user': MYSQL_USER,
        'password': MYSQL_PASSWORD,
        'port': MYSQL_PORT,
        'autocommit': True,
    }
    if db:
        cfg['database'] = db
    return mysql.connector.connect(**cfg)


def init_db():
    # Create database if not exists and create users table
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` DEFAULT CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci';")
        cursor.close()
        conn.close()

        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_table = '''
        CREATE TABLE IF NOT EXISTS users (
            id varchar(255) PRIMARY KEY,
            user_type VARCHAR(50) NOT NULL,
            nic VARCHAR(64),
            full_name VARCHAR(255),
            company_register_number VARCHAR(128),
            company_name VARCHAR(255),
            address TEXT,
            district VARCHAR(128),
            contact_number VARCHAR(64),
            password VARCHAR(255),
            total_area_of_paddy_land VARCHAR(64),
            block_hash VARCHAR(255),
            block_number INT,
            transaction_hash VARCHAR(255),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        cursor.execute(create_table)
        # Add new columns if they don't exist
        try:
            cursor.execute("ALTER TABLE `users` ADD COLUMN block_number INT AFTER block_hash")
        except mysql.connector.Error:
            pass  # Column already exists
        try:
            cursor.execute("ALTER TABLE `users` ADD COLUMN transaction_hash VARCHAR(255) AFTER block_number")
        except mysql.connector.Error:
            pass  # Column already exists
        cursor.close()
        conn.close()
        # Create transactions table to record transfers/purchases
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_tx = '''
        CREATE TABLE IF NOT EXISTS `transaction` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `from` VARCHAR(255),
            `to` VARCHAR(255),
            `type` VARCHAR(100),
            quantity DECIMAL(14,3),
            `datetime` DATETIME,
            block_hash VARCHAR(255),
            block_number INT,
            transaction_hash VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_tx)
            # Add new columns if they don't exist
            try:
                cursor.execute("ALTER TABLE `transaction` ADD COLUMN block_number INT AFTER block_hash")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `transaction` ADD COLUMN transaction_hash VARCHAR(255) AFTER block_number")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `transaction` ADD COLUMN status TINYINT(1) DEFAULT 1 AFTER `quantity`")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `transaction` ADD COLUMN price DECIMAL(14,3) AFTER `quantity`")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `transaction` ADD COLUMN is_reverted TINYINT(1) DEFAULT 0 AFTER `status`")
            except mysql.connector.Error:
                pass  # Column already exists
        except mysql.connector.Error as e:
            # Log and continue; table creation is best-effort
            print('Could not create transaction table:', e)
        finally:
            cursor.close()
            conn.close()
        # Create stock table to track per-user stock levels
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_stock = '''
        CREATE TABLE IF NOT EXISTS `stock` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id varchar(255),
            `type` VARCHAR(128),
            amount DECIMAL(20,3) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX (user_id),
            INDEX (`type`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_stock)
        except mysql.connector.Error as e:
            print('Could not create stock table:', e)
        finally:
            cursor.close()
            conn.close()
        # Create paddy_type table to store available paddy types
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_paddy = '''
        CREATE TABLE IF NOT EXISTS `paddy_type` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_paddy)
        except mysql.connector.Error as e:
            print('Could not create paddy_type table:', e)
        finally:
            cursor.close()
            conn.close()
        # Create damage table to track damaged inventory
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_damage = '''
        CREATE TABLE IF NOT EXISTS `damage` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(255),
            paddy_type VARCHAR(128),
            quantity DECIMAL(14,3),
            reason TEXT,
            damage_date DATETIME,
            block_hash VARCHAR(255),
            block_number INT,
            transaction_hash VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX (user_id),
            INDEX (paddy_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_damage)
            # Add new columns if they don't exist
            try:
                cursor.execute("ALTER TABLE `damage` ADD COLUMN block_number INT AFTER block_hash")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `damage` ADD COLUMN transaction_hash VARCHAR(255) AFTER block_number")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `damage` ADD COLUMN reverted INT DEFAULT 0 AFTER transaction_hash")
            except mysql.connector.Error:
                pass  # Column already exists
        except mysql.connector.Error as e:
            print('Could not create damage table:', e)
        finally:
            cursor.close()
            conn.close()
        # Create milling table to track milling process
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_milling = '''
        CREATE TABLE IF NOT EXISTS `milling` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            miller_id VARCHAR(255),
            paddy_type VARCHAR(128),
            input_paddy DECIMAL(14,3),
            output_rice DECIMAL(14,3),
            milling_date DATE,
            drying_duration INT,
            status BOOLEAN DEFAULT FALSE,
            block_hash VARCHAR(255),
            block_number INT,
            transaction_hash VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX (miller_id),
            INDEX (paddy_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_milling)
            # Add new columns if they don't exist
            try:
                cursor.execute("ALTER TABLE `milling` ADD COLUMN block_number INT AFTER block_hash")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `milling` ADD COLUMN transaction_hash VARCHAR(255) AFTER block_number")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `milling` ADD COLUMN drying_duration INT AFTER milling_date")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `milling` ADD COLUMN status BOOLEAN DEFAULT FALSE AFTER drying_duration")
            except mysql.connector.Error:
                pass  # Column already exists
        except mysql.connector.Error as e:
            print('Could not create milling table:', e)
        finally:
            cursor.close()
            conn.close()
        # Create rice_stock table to track rice inventory from milling
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_rice_stock = '''
        CREATE TABLE IF NOT EXISTS `rice_stock` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            miller_id VARCHAR(255),
            paddy_type VARCHAR(128),
            quantity DECIMAL(14,3),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX (miller_id),
            INDEX (paddy_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_rice_stock)
        except mysql.connector.Error as e:
            print('Could not create rice_stock table:', e)
        finally:
            cursor.close()
            conn.close()
        
        # Create rice_transaction table to track rice transactions (Miller -> Wholesaler/Retailer/etc)
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_rice_tx = '''
        CREATE TABLE IF NOT EXISTS `rice_transaction` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `from` VARCHAR(255),
            `to` VARCHAR(255),
            rice_type VARCHAR(100),
            quantity DECIMAL(14,3),
            `datetime` DATETIME,
            block_hash VARCHAR(255),
            block_number INT,
            transaction_hash VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX (`from`),
            INDEX (`to`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_rice_tx)
            # Add new columns if they don't exist
            try:
                cursor.execute("ALTER TABLE `rice_transaction` ADD COLUMN block_number INT AFTER block_hash")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `rice_transaction` ADD COLUMN transaction_hash VARCHAR(255) AFTER block_number")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `rice_transaction` ADD COLUMN status TINYINT(1) DEFAULT 1 AFTER `quantity`")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `rice_transaction` ADD COLUMN price DECIMAL(14,3) AFTER `quantity`")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `rice_transaction` ADD COLUMN is_reverted TINYINT(1) DEFAULT 0 AFTER `status`")
            except mysql.connector.Error:
                pass  # Column already exists
        except mysql.connector.Error as e:
            print('Could not create rice_transaction table:', e)
        finally:
            cursor.close()
            conn.close()
        
        # Create rice_damage table to track rice damage (Wholesaler/Retailer/etc)
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_rice_damage = '''
        CREATE TABLE IF NOT EXISTS `rice_damage` (
            id INT NOT NULL PRIMARY KEY,
            user_id VARCHAR(255),
            rice_type VARCHAR(128),
            quantity DECIMAL(14,3),
            reason TEXT,
            damage_date DATETIME,
            block_hash VARCHAR(255),
            block_number INT,
            transaction_hash VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX (user_id),
            INDEX (rice_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_rice_damage)
            # Add new columns if they don't exist
            try:
                cursor.execute("ALTER TABLE `rice_damage` ADD COLUMN block_number INT AFTER block_hash")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `rice_damage` ADD COLUMN transaction_hash VARCHAR(255) AFTER block_number")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `rice_damage` ADD COLUMN reverted INT DEFAULT 0 AFTER transaction_hash")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `rice_damage` DROP COLUMN rice_damage_id")
            except mysql.connector.Error:
                pass  # Column doesn't exist
        except mysql.connector.Error as e:
            print('Could not create rice_damage table:', e)
        finally:
            cursor.close()
            conn.close()

        # Create initial_paddy table to track initial paddy amounts for farmers
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_initial_paddy = '''
        CREATE TABLE IF NOT EXISTS `initial_paddy` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(255),
            paddy_type VARCHAR(128),
            quantity DECIMAL(14,3),
            status BOOLEAN DEFAULT TRUE,
            block_number INT,
            transaction_id VARCHAR(255),
            block_hash VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX (user_id),
            INDEX (paddy_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_initial_paddy)
            # Add paddy_type column if it doesn't exist (for existing tables)
            try:
                cursor.execute("ALTER TABLE `initial_paddy` ADD COLUMN paddy_type VARCHAR(128) AFTER user_id")
            except mysql.connector.Error:
                pass  # Column already exists
            # Rename initial_paddy to quantity if needed
            try:
                cursor.execute("ALTER TABLE `initial_paddy` CHANGE COLUMN initial_paddy quantity DECIMAL(14,3)")
            except mysql.connector.Error:
                pass  # Column already renamed or doesn't exist
            # Add status column if missing
            try:
                cursor.execute("ALTER TABLE `initial_paddy` ADD COLUMN status BOOLEAN DEFAULT TRUE AFTER quantity")
            except mysql.connector.Error:
                pass  # Column already exists
            # Add blockchain columns if missing
            try:
                cursor.execute("ALTER TABLE `initial_paddy` ADD COLUMN block_number INT AFTER status")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `initial_paddy` ADD COLUMN transaction_id VARCHAR(255) AFTER block_number")
            except mysql.connector.Error:
                pass  # Column already exists
            try:
                cursor.execute("ALTER TABLE `initial_paddy` ADD COLUMN block_hash VARCHAR(255) AFTER transaction_id")
            except mysql.connector.Error:
                pass  # Column already exists
        except mysql.connector.Error as e:
            print('Could not create initial_paddy table:', e)
        finally:
            cursor.close()
            conn.close()

        # Create rice table to track rice amounts
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        create_rice = '''
        CREATE TABLE IF NOT EXISTS `rice` (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(255),
            rice_type VARCHAR(128),
            quantity DECIMAL(14,3),
            status TINYINT DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            block_id INT,
            block_hash VARCHAR(255),
            block_number INT,
            transaction_hash VARCHAR(255),
            INDEX (user_id),
            INDEX (rice_type),
            INDEX (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        try:
            cursor.execute(create_rice)
            conn.commit()
        except mysql.connector.Error as e:
            print('Could not create rice table:', e)

        # Add status column if it doesn't exist
        try:
            cursor.execute("""
                ALTER TABLE rice 
                ADD COLUMN status TINYINT DEFAULT 1
            """)
            conn.commit()
            print('Added status column to rice table')
        except mysql.connector.Error as e:
            if 'Duplicate column name' not in str(e):
                print('Note: Could not add status column to rice table:', e)
        
        # Add blockchain columns if they don't exist
        try:
            cursor.execute("ALTER TABLE `rice` ADD COLUMN block_id INT AFTER updated_at")
            conn.commit()
        except mysql.connector.Error as e:
            pass  # Column might already exist
        
        try:
            cursor.execute("ALTER TABLE `rice` ADD COLUMN block_hash VARCHAR(255) AFTER block_id")
            conn.commit()
        except mysql.connector.Error as e:
            pass  # Column might already exist
        
        try:
            cursor.execute("ALTER TABLE `rice` ADD COLUMN block_number INT AFTER block_hash")
            conn.commit()
        except mysql.connector.Error as e:
            pass  # Column might already exist
        
        try:
            cursor.execute("ALTER TABLE `rice` ADD COLUMN transaction_hash VARCHAR(255) AFTER block_number")
            conn.commit()
        except mysql.connector.Error as e:
            pass  # Column might already exist
        
        cursor.close()
        conn.close()

        print('Database initialized (database/table ensured).')
    except mysql.connector.Error as err:
        print('Failed initializing database:', err)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/app')
def app_page():
    # serve the main application page
    return render_template('index.html')


@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.get_json() or {}
    username = (data.get('username') or '').strip()
    password = (data.get('password') or '').strip()
    role = (data.get('role') or '').strip()

    # quick admin shortcut
    if username == 'admin' and password == 'admin' and role.lower() == 'admin':
        # set a minimal session for admin
        session['user_id'] = 'admin'
        session['user_type'] = 'Admin'
        session['full_name'] = 'Administrator'
        return jsonify({'ok': True, 'role': 'Admin'})

    # quick PMB (government) shortcut
    if username.lower() == 'pmb' and password == '123456' and role.lower() == 'pmb':
        session['user_id'] = 'pmb'
        session['user_type'] = 'PMB'
        session['full_name'] = 'Government (PMB)'
        return jsonify({'ok': True, 'role': 'PMB'})

    # ins user shortcut
    if username == 'ins' and password == '123456':
        session['user_id'] = 'ins'
        session['user_type'] = 'INS'
        session['full_name'] = 'INS User'
        return jsonify({'ok': True, 'role': 'INS'})

    # inspector user shortcut
    if username == 'inspector' and password == '123456':
        session['user_id'] = 'inspector'
        session['user_type'] = 'Inspector'
        session['full_name'] = 'Inspector'
        return jsonify({'ok': True, 'role': 'Inspector'})

    # division user shortcut
    if username == 'division' and password == '123456':
        session['user_id'] = 'division'
        session['user_type'] = 'Division'
        session['full_name'] = 'Division Officer'
        return jsonify({'ok': True, 'role': 'Division'})

    # treat username as a string identifier (no numeric validation)
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        # verify password exists in table; if password column missing this will raise
        try:
            cur.execute('SELECT id, user_type FROM users WHERE id = %s AND password = %s LIMIT 1', (username, password))
        except mysql.connector.Error:
            # fallback: table might not have password column
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Server not configured with password column'}), 500

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return jsonify({'ok': False, 'error': 'Invalid credentials'}), 401

        user_type = (row.get('user_type') or '').strip()
        # match role (case-insensitive startswith) to allow slight variations
        if user_type.lower().startswith(role.lower()):
            # set server-side session values
            session['user_id'] = row.get('id')
            session['user_type'] = row.get('user_type')
            session['full_name'] = row.get('full_name')
            return jsonify({'ok': True, 'role': user_type})
        else:
            return jsonify({'ok': False, 'error': 'Role does not match user account'}), 403
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/blank')
def blank_page():
    return render_template('blank.html')


@app.route('/inspector')
def inspector_page():
    return render_template('inspector.html')


@app.route('/division')
def division_page():
    return render_template('division.html')


@app.route('/collecter')
def collecter_page():
    return render_template('collecter.html')


@app.route('/miller')
def miller_page():
    return render_template('miller.html')


@app.route('/pmb')
def pmb_page():
    return render_template('pmb.html')


@app.route('/wholesaler')
def wholesaler_page():
    return render_template('wholesaler.html')


@app.route('/retailer')
def retailer_page():
    return render_template('retailer.html')


@app.route('/beer')
def beer_page():
    return render_template('beer.html')


@app.route('/animalfood')
def animalfood_page():
    return render_template('animalfood.html')


@app.route('/exporter')
def exporter_page():
    return render_template('exporter.html')


@app.route('/api/me', methods=['GET'])
def api_me():
    """Return the current logged-in user (from server-side session).
    Response: { ok: True, user_id, user_type, full_name } or 401
    """
    uid = session.get('user_id')
    if not uid:
        return jsonify({'ok': False, 'error': 'Not authenticated'}), 401
    return jsonify({'ok': True, 'user_id': uid, 'user_type': session.get('user_type'), 'full_name': session.get('full_name')})


def log_last_inserted_user(user_type):
    """Fetch the highest numeric ID for the given user_type and return the next ID.
    Always return the next ID (e.g., COL6, FAR6, MIL4).
    """
    try:
        # Prefix mapping for each user type
        prefix_map = {
            "Farmer": "FAR",
            "Collecter": "COL",
            "Miller": "MIL",
            "Wholesaler": "WHO",
            "Retailer": "RET",
            "Beer": "BER",
            "Animal Food": "ANI",
            "Exporter": "EXP"
        }

        # Default prefix = first 3 letters of user_type if not found
        prefix = prefix_map.get(user_type, user_type[:3].upper())

        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)

        # Fetch all IDs for this user type and find the maximum numeric value
        query = """
            SELECT id FROM users
            WHERE user_type = %s
        """
        cur.execute(query, (user_type,))
        rows = cur.fetchall()

        max_number = 0
        for row in rows:
            if row and row.get("id"):
                user_id = str(row["id"])
                # Extract numeric part (everything after the prefix)
                numeric_part = user_id[len(prefix):] if len(user_id) > len(prefix) else "0"
                try:
                    num = int(numeric_part)
                    if num > max_number:
                        max_number = num
                except (ValueError, TypeError):
                    continue

        next_number = max_number + 1

        cur.close()
        conn.close()

        # Build the new ID
        next_id = f"{prefix}{next_number}"
        return next_id

    except Exception as e:
        print("[log_last_inserted_user] Error fetching last inserted user:", e)
        return None

@app.route('/api/users', methods=['GET'])
def api_get_users():
    try:
        user_type = request.args.get('user_type', '')
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor(dictionary=True)
        
        if user_type:
            cursor.execute('SELECT * FROM users WHERE user_type = %s ORDER BY id DESC', (user_type,))
        else:
            cursor.execute('SELECT * FROM users ORDER BY id DESC')
            
        rows = cursor.fetchall()
        # add computed user_code to each row (do not store in DB)
        prefix_map = {
            'Farmer': 'FAR',
            'Collecter': 'COL',
            'Miller': 'MIL',
            'Wholesaler': 'WHO',
            'Retailer': 'RET',
            'Beer': 'BER',
            'Animal Food': 'ANI',
            'Exporter': 'EXP'
        }
        import datetime as _dt
        for r in rows:
            try:
                prefix = prefix_map.get(r.get('user_type'), 'USR')
                r['user_code'] = f"{prefix}{int(r.get('id')):06d}" if r.get('id') is not None else None
            except Exception:
                r['user_code'] = None
            # serialize created_at/updated_at to ISO strings if present
            for k in ('created_at', 'updated_at'):
                if k in r and r[k] is not None:
                    try:
                        if isinstance(r[k], (_dt.datetime, _dt.date)):
                            r[k] = r[k].isoformat()
                        else:
                            r[k] = str(r[k])
                    except Exception:
                        try:
                            r[k] = str(r[k])
                        except Exception:
                            r[k] = None

        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/initial_paddy', methods=['GET', 'POST'])
def api_get_initial_paddy():
    """GET: Return initial paddy data with farmer information.
    Query params: user_type (optional)
    Response: [ { id, user_id, full_name, company_name, user_type, district, paddy_type, quantity, created_at } ]
    
    POST: Create a new initial paddy record.
    Body: { user_id, paddy_type, quantity }
    """
    if request.method == 'POST':
        try:
            data = request.get_json()
            user_id = data.get('user_id')
            paddy_type = data.get('paddy_type')
            quantity = data.get('quantity')
            status = data.get('status', True)  # Default to True (active)
            
            if not user_id or not paddy_type or quantity is None:
                return jsonify({'error': 'Missing required fields: user_id, paddy_type, quantity'}), 400
            
            # Convert quantity to float
            try:
                quantity = float(quantity)
            except (ValueError, TypeError):
                return jsonify({'error': 'Invalid quantity value'}), 400
            
            if quantity < 0:
                return jsonify({'error': 'Quantity must be non-negative'}), 400
            
            # Record to blockchain first
            from blockchain import save_initial_paddy_record
            blockchain_result = save_initial_paddy_record(user_id, paddy_type, quantity)
            
            conn = get_connection(MYSQL_DATABASE)
            cursor = conn.cursor()
            
            # Insert the new initial paddy record with blockchain data
            if blockchain_result:
                query = 'INSERT INTO initial_paddy (id, user_id, paddy_type, quantity, status, block_number, transaction_id, block_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
                cursor.execute(query, (
                    blockchain_result.get('record_id'),
                    user_id, 
                    paddy_type, 
                    quantity, 
                    bool(status) if status is not None else True,
                    blockchain_result.get('block_number'),
                    blockchain_result.get('transaction_hash'),
                    blockchain_result.get('block_hash')
                ))
            else:
                # Still insert even if blockchain fails
                query = 'INSERT INTO initial_paddy (user_id, paddy_type, quantity, status) VALUES (%s, %s, %s, %s)'
                cursor.execute(query, (user_id, paddy_type, quantity, bool(status) if status is not None else True))
            
            conn.commit()
            
            paddy_id = cursor.lastrowid
            
            # Update or insert stock record
            # Check if stock record exists for this user and paddy type
            stock_query = 'SELECT id, amount FROM stock WHERE user_id = %s AND type = %s'
            cursor.execute(stock_query, (str(user_id), paddy_type))
            stock_row = cursor.fetchone()
            
            if stock_row:
                # Update existing stock
                update_stock = 'UPDATE stock SET amount = amount + %s WHERE user_id = %s AND type = %s'
                cursor.execute(update_stock, (quantity, str(user_id), paddy_type))
            else:
                # Insert new stock record
                insert_stock = 'INSERT INTO stock (user_id, type, amount) VALUES (%s, %s, %s)'
                cursor.execute(insert_stock, (str(user_id), paddy_type, quantity))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            if blockchain_result:
                return jsonify({
                    'message': 'Initial paddy created successfully, stock updated, and recorded to blockchain',
                    'id': paddy_id,
                    'blockchain': blockchain_result
                }), 201
            else:
                return jsonify({
                    'message': 'Initial paddy created and stock updated, but blockchain recording failed',
                    'id': paddy_id,
                    'warning': 'Blockchain recording failed'
                }), 201
            
        except mysql.connector.Error as err:
            return jsonify({'error': str(err)}), 500
        except Exception as err:
            return jsonify({'error': str(err)}), 500
    
    # GET method handling
    user_type = request.args.get('user_type', '')
    try:
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor(dictionary=True)
        
        if user_type:
            query = '''
                SELECT ip.id, ip.user_id, u.full_name, u.company_name, u.user_type, u.district, ip.paddy_type, ip.quantity, ip.status, ip.block_number, ip.transaction_id, ip.block_hash, ip.created_at
                FROM initial_paddy ip
                JOIN users u ON ip.user_id = u.id
                WHERE u.user_type = %s
                ORDER BY ip.created_at DESC
            '''
            cursor.execute(query, (user_type,))
        else:
            query = '''
                SELECT ip.id, ip.user_id, u.full_name, u.company_name, u.user_type, u.district, ip.paddy_type, ip.quantity, ip.status, ip.block_number, ip.transaction_id, ip.block_hash, ip.created_at
                FROM initial_paddy ip
                JOIN users u ON ip.user_id = u.id
                WHERE u.user_type IN ('Collecter', 'Miller')
                ORDER BY ip.created_at DESC
            '''
            cursor.execute(query)
        
        rows = cursor.fetchall()
        
        import datetime as _dt
        for r in rows:
            # serialize created_at to ISO string if present
            if 'created_at' in r and r['created_at'] is not None:
                try:
                    if isinstance(r['created_at'], (_dt.datetime, _dt.date)):
                        r['created_at'] = r['created_at'].isoformat()
                    else:
                        r['created_at'] = str(r['created_at'])
                except Exception:
                    try:
                        r['created_at'] = str(r['created_at'])
                    except Exception:
                        r['created_at'] = None
        
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/initial_rice', methods=['GET', 'POST'])
def api_get_initial_rice():
    """GET: Return initial rice data with user information.
    Query params: user_type (optional)
    Response: [ { user_id, user_name, user_type, district, rice_type, quantity, created_at } ]
    
    POST: Create a new initial rice record.
    Body: { user_id, rice_type, quantity }
    """
    if request.method == 'POST':
        try:
            data = request.get_json()
            user_id = data.get('user_id')
            rice_type = data.get('rice_type')
            quantity = data.get('quantity')
            
            if not user_id or not rice_type or quantity is None:
                return jsonify({'error': 'Missing required fields: user_id, rice_type, quantity'}), 400
            
            if float(quantity) < 0:
                return jsonify({'error': 'Quantity must be non-negative'}), 400
            
            # Record to blockchain first
            from blockchain import save_initial_rice_record
            blockchain_result = save_initial_rice_record(user_id, rice_type, quantity)
            
            conn = get_connection(MYSQL_DATABASE)
            cursor = conn.cursor()
            
            # Insert the new initial rice record with blockchain data
            if blockchain_result:
                query = 'INSERT INTO rice (user_id, rice_type, quantity, status, block_id, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
                cursor.execute(query, (
                    user_id, 
                    rice_type, 
                    float(quantity),
                    1,
                    blockchain_result.get('block_id'),
                    blockchain_result.get('block_hash'),
                    blockchain_result.get('block_number'),
                    blockchain_result.get('transaction_hash')
                ))
            else:
                # Still insert even if blockchain fails
                query = 'INSERT INTO rice (user_id, rice_type, quantity, status) VALUES (%s, %s, %s, %s)'
                cursor.execute(query, (user_id, rice_type, float(quantity), 1))
            
            conn.commit()
            
            rice_id = cursor.lastrowid
            cursor.close()
            conn.close()
            
            return jsonify({
                'message': 'Initial rice created successfully',
                'id': rice_id
            }), 201
            
        except mysql.connector.Error as err:
            return jsonify({'error': str(err)}), 500
        except Exception as err:
            return jsonify({'error': str(err)}), 500
    
    # GET method handling
    user_type = request.args.get('user_type', '')
    try:
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor(dictionary=True)
        
        if user_type:
            query = '''
                SELECT r.id,
                       r.user_id, 
                       COALESCE(u.company_name, u.full_name) as user_name,
                       u.user_type,
                       u.district, 
                       r.rice_type, 
                       r.quantity,
                       r.status,
                       r.created_at,
                       r.block_id,
                       r.block_hash,
                       r.block_number,
                       r.transaction_hash
                FROM rice r
                JOIN users u ON r.user_id = u.id
                WHERE u.user_type = %s
                ORDER BY r.created_at DESC
            '''
            cursor.execute(query, (user_type,))
        else:
            query = '''
                SELECT r.id,
                       r.user_id, 
                       COALESCE(u.company_name, u.full_name) as user_name,
                       u.user_type,
                       u.district, 
                       r.rice_type, 
                       r.quantity,
                       r.status,
                       r.created_at,
                       r.block_id,
                       r.block_hash,
                       r.block_number,
                       r.transaction_hash
                FROM rice r
                JOIN users u ON r.user_id = u.id
                WHERE u.user_type IN ('Miller', 'Wholesaler')
                ORDER BY r.created_at DESC
            '''
            cursor.execute(query)
        
        rows = cursor.fetchall()
        
        import datetime as _dt
        for r in rows:
            # serialize created_at to ISO string if present
            if 'created_at' in r and r['created_at'] is not None:
                try:
                    if isinstance(r['created_at'], (_dt.datetime, _dt.date)):
                        r['created_at'] = r['created_at'].isoformat()
                    else:
                        r['created_at'] = str(r['created_at'])
                except Exception:
                    try:
                        r['created_at'] = str(r['created_at'])
                    except Exception:
                        r['created_at'] = None
        
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/initial_paddy/<int:paddy_id>', methods=['PUT'])
def api_update_initial_paddy(paddy_id):
    """Update initial paddy quantity and record to blockchain"""
    try:
        data = request.get_json()
        quantity = data.get('quantity')
        
        if quantity is None or quantity < 0:
            return jsonify({'error': 'Invalid quantity'}), 400
        
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor(dictionary=True)
        
        # Fetch the record to get user_id and paddy_type for blockchain
        fetch_query = 'SELECT user_id, paddy_type FROM initial_paddy WHERE id = %s'
        cursor.execute(fetch_query, (paddy_id,))
        record = cursor.fetchone()
        
        if not record:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Initial paddy record not found'}), 404
        
        user_id = record['user_id']
        paddy_type = record['paddy_type']
        
        # Record to blockchain
        from blockchain import save_initial_paddy_record
        blockchain_result = save_initial_paddy_record(user_id, paddy_type, quantity)
        
        # Update the quantity and blockchain fields
        if blockchain_result:
            update_query = '''UPDATE initial_paddy 
                            SET quantity = %s, 
                                block_number = %s, 
                                transaction_id = %s, 
                                block_hash = %s 
                            WHERE id = %s'''
            cursor.execute(update_query, (
                quantity, 
                blockchain_result.get('block_number'),
                blockchain_result.get('transaction_hash'),
                blockchain_result.get('block_hash'),
                paddy_id
            ))
        else:
            # Still update quantity even if blockchain fails
            update_query = 'UPDATE initial_paddy SET quantity = %s WHERE id = %s'
            cursor.execute(update_query, (quantity, paddy_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        if blockchain_result:
            return jsonify({
                'message': 'Initial paddy updated successfully and recorded to blockchain',
                'blockchain': blockchain_result
            }), 200
        else:
            return jsonify({
                'message': 'Initial paddy updated but blockchain recording failed',
                'warning': 'Quantity updated but not recorded to blockchain'
            }), 200
        
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500
    except Exception as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/initial_paddy/<int:paddy_id>/revert', methods=['POST'])
def api_revert_initial_paddy(paddy_id):
    """Revert initial paddy: add new record with status = 0 (inactive) and deduct from stock"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        paddy_type = data.get('paddy_type')
        quantity = data.get('quantity')
        
        if not user_id or not paddy_type or quantity is None:
            return jsonify({'error': 'Missing required fields: user_id, paddy_type, quantity'}), 400
        
        qty_float = float(quantity)
        if qty_float < 0:
            return jsonify({'error': 'Quantity must be non-negative'}), 400
        
        # Record revert to blockchain first
        from blockchain import revert_initial_paddy_record
        blockchain_result = revert_initial_paddy_record(user_id, paddy_type, qty_float)
        
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        
        # Add new record with status = 0 (inactive/reverted) and blockchain data
        if blockchain_result:
            insert_query = '''INSERT INTO initial_paddy 
                (id, user_id, paddy_type, quantity, status, block_number, transaction_id, block_hash) 
                VALUES (%s, %s, %s, %s, 0, %s, %s, %s)'''
            cursor.execute(insert_query, (
                blockchain_result.get('record_id'),
                user_id, 
                paddy_type, 
                qty_float,
                blockchain_result.get('block_number'),
                blockchain_result.get('transaction_hash'),
                blockchain_result.get('block_hash')
            ))
        else:
            # Still insert even if blockchain fails
            insert_query = 'INSERT INTO initial_paddy (user_id, paddy_type, quantity, status) VALUES (%s, %s, %s, 0)'
            cursor.execute(insert_query, (user_id, paddy_type, qty_float))
        
        # Deduct quantity from stock
        stock_query = 'UPDATE stock SET amount = amount - %s WHERE user_id = %s AND type = %s AND amount >= %s'
        cursor.execute(stock_query, (qty_float, str(user_id), paddy_type, qty_float))
        
        stock_rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if blockchain_result:
            if stock_rows_affected == 0:
                return jsonify({
                    'message': 'Initial paddy reversal recorded on blockchain (status = 0). Note: stock may have been insufficient for full deduction.',
                    'blockchain': blockchain_result
                }), 200
            return jsonify({
                'message': 'Initial paddy reversal recorded successfully on blockchain (status = 0) and stock deducted',
                'blockchain': blockchain_result
            }), 200
        else:
            if stock_rows_affected == 0:
                return jsonify({
                    'message': 'Initial paddy reversal record added (status = 0), but blockchain recording failed. Note: stock may have been insufficient for full deduction.',
                    'warning': 'Blockchain recording failed'
                }), 200
            return jsonify({
                'message': 'Initial paddy reversal record added (status = 0) and stock deducted, but blockchain recording failed',
                'warning': 'Blockchain recording failed'
            }), 200
        
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500
    except Exception as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/initial_rice/<int:rice_id>/revert', methods=['POST'])
def api_revert_initial_rice(rice_id):
    """Revert initial rice: add new record with status = 0 (inactive) and deduct from stock"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        rice_type = data.get('rice_type')
        quantity = data.get('quantity')
        
        if not user_id or not rice_type or quantity is None:
            return jsonify({'error': 'Missing required fields: user_id, rice_type, quantity'}), 400
        
        qty_float = float(quantity)
        if qty_float < 0:
            return jsonify({'error': 'Quantity must be non-negative'}), 400
        
        # Record revert to blockchain first
        from blockchain import revert_initial_rice_record
        blockchain_result = revert_initial_rice_record(user_id, rice_type, qty_float)
        
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        
        # Add new record with status = 0 (inactive/reverted) and blockchain data
        if blockchain_result:
            insert_query = '''INSERT INTO rice 
                (user_id, rice_type, quantity, status, block_id, block_hash, block_number, transaction_hash) 
                VALUES (%s, %s, %s, 0, %s, %s, %s, %s)'''
            cursor.execute(insert_query, (
                user_id, 
                rice_type, 
                qty_float,
                blockchain_result.get('block_id'),
                blockchain_result.get('block_hash'),
                blockchain_result.get('block_number'),
                blockchain_result.get('transaction_hash')
            ))
        else:
            # Still insert even if blockchain fails
            insert_query = 'INSERT INTO rice (user_id, rice_type, quantity, status) VALUES (%s, %s, %s, 0)'
            cursor.execute(insert_query, (user_id, rice_type, qty_float))
        
        # Deduct quantity from rice_stock
        stock_query = 'UPDATE rice_stock SET quantity = quantity - %s WHERE miller_id = %s AND paddy_type = %s AND quantity >= %s'
        cursor.execute(stock_query, (qty_float, str(user_id), rice_type, qty_float))
        
        stock_rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if blockchain_result:
            if stock_rows_affected == 0:
                return jsonify({
                    'message': 'Initial rice reversal recorded on blockchain (status = 0). Note: stock may have been insufficient for full deduction.',
                    'blockchain': blockchain_result
                }), 200
            return jsonify({
                'message': 'Initial rice reversal recorded successfully on blockchain (status = 0) and stock deducted',
                'blockchain': blockchain_result
            }), 200
        else:
            if stock_rows_affected == 0:
                return jsonify({
                    'message': 'Initial rice reversal record added (status = 0), but blockchain recording failed. Note: stock may have been insufficient for full deduction.',
                    'warning': 'Blockchain recording failed'
                }), 200
            return jsonify({
                'message': 'Initial rice reversal record added (status = 0) and stock deducted, but blockchain recording failed',
                'warning': 'Blockchain recording failed'
            }), 200
        
    except mysql.connector.Error as err:
        print(f"MySQL Error in revert_initial_rice: {err}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(err), 'message': 'Database error during rice revert'}), 500
    except Exception as err:
        print(f"Error in revert_initial_rice: {err}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(err), 'message': 'Error during rice revert'}), 500


@app.route('/api/stats', methods=['GET'])
def api_get_stats():
    """Return basic counts for dashboard: total farmers, collectors, millers."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor()
        # count by user_type using case-insensitive LIKE to be forgiving
        counts = {'farmers': 0, 'collectors': 0, 'millers': 0, 'wholesalers': 0, 'retailers': 0, 'beer': 0, 'animalfood': 0, 'exporter': 0}
        try:
            cur.execute("SELECT user_type, COUNT(*) FROM users GROUP BY LOWER(user_type)")
        except Exception:
            # fallback: count with LIKE patterns
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE LOWER(user_type) LIKE %s", ("%farmer%",))
                counts['farmers'] = cur.fetchone()[0]
            except Exception:
                counts['farmers'] = 0
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE LOWER(user_type) LIKE %s", ("%collect%",))
                counts['collectors'] = cur.fetchone()[0]
            except Exception:
                counts['collectors'] = 0
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE LOWER(user_type) LIKE %s", ("%miller%",))
                counts['millers'] = cur.fetchone()[0]
            except Exception:
                counts['millers'] = 0
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE LOWER(user_type) LIKE %s", ("%wholesaler%",))
                counts['wholesalers'] = cur.fetchone()[0]
            except Exception:
                counts['wholesalers'] = 0
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE LOWER(user_type) LIKE %s", ("%retailer%",))
                counts['retailers'] = cur.fetchone()[0]
            except Exception:
                counts['retailers'] = 0
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE LOWER(user_type) LIKE %s", ("%beer%",))
                counts['beer'] = cur.fetchone()[0]
            except Exception:
                counts['beer'] = 0
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE user_type = %s", ("Animal Food",))
                counts['animalfood'] = cur.fetchone()[0]
            except Exception:
                counts['animalfood'] = 0
            try:
                cur.execute("SELECT COUNT(*) FROM users WHERE LOWER(user_type) LIKE %s", ("%exporter%",))
                counts['exporter'] = cur.fetchone()[0]
            except Exception:
                counts['exporter'] = 0
            cur.close()
            conn.close()
            return jsonify(counts)

        rows = cur.fetchall()
        # rows are (user_type, count)
        for r in rows:
            try:
                ut = (r[0] or '').strip().lower()
                c = int(r[1] or 0)
                if 'farmer' in ut:
                    counts['farmers'] += c
                elif 'collect' in ut:
                    counts['collectors'] += c
                elif 'miller' in ut:
                    counts['millers'] += c
                elif 'wholesaler' in ut:
                    counts['wholesalers'] += c
                elif 'retailer' in ut:
                    counts['retailers'] += c
                elif 'exporter' in ut:
                    counts['exporter'] += c
                elif 'beer' in ut:
                    counts['beer'] += c
                elif 'animal food' in ut:
                    counts['animalfood'] += c
            except Exception:
                continue

        cur.close()
        conn.close()
        return jsonify(counts)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/stock_summary', methods=['GET'])
def api_get_stock_summary():
    """Return aggregated paddy amounts by user role (PMB, Collecter, Miller).

    Response: { pmb: number, collecter: number, miller: number }
    """
    try:
        # optional paddy_type filter from query string
        paddy_type = (request.args.get('paddy_type') or '').strip()
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor()
        # build SQL with optional filter
        try:
            if paddy_type:
                sql = "SELECT LOWER(u.user_type) as ut, SUM(s.amount) FROM `stock` s JOIN users u ON s.user_id = u.id WHERE s.`type` = %s GROUP BY LOWER(u.user_type)"
                cur.execute(sql, (paddy_type,))
            else:
                cur.execute("SELECT LOWER(u.user_type) as ut, SUM(s.amount) FROM `stock` s JOIN users u ON s.user_id = u.id GROUP BY LOWER(u.user_type)")
            rows = cur.fetchall()
        except Exception:
            # fallback: perform individual sums using LIKE, with optional paddy_type filter
            rows = []
            try:
                if paddy_type:
                    cur.execute("SELECT SUM(amount) FROM `stock` WHERE `type` = %s AND user_id IN (SELECT id FROM users WHERE LOWER(user_type) LIKE %s)", (paddy_type, "%pmb%"))
                else:
                    cur.execute("SELECT SUM(amount) FROM `stock` WHERE user_id IN (SELECT id FROM users WHERE LOWER(user_type) LIKE %s)", ("%pmb%",))
                rows.append(('pmb', cur.fetchone()[0] or 0))
            except Exception:
                rows.append(('pmb', 0))
            try:
                if paddy_type:
                    cur.execute("SELECT SUM(amount) FROM `stock` WHERE `type` = %s AND user_id IN (SELECT id FROM users WHERE LOWER(user_type) LIKE %s)", (paddy_type, "%collect%"))
                else:
                    cur.execute("SELECT SUM(amount) FROM `stock` WHERE user_id IN (SELECT id FROM users WHERE LOWER(user_type) LIKE %s)", ("%collect%",))
                rows.append(('collecter', cur.fetchone()[0] or 0))
            except Exception:
                rows.append(('collecter', 0))
            try:
                if paddy_type:
                    cur.execute("SELECT SUM(amount) FROM `stock` WHERE `type` = %s AND user_id IN (SELECT id FROM users WHERE LOWER(user_type) LIKE %s)", (paddy_type, "%miller%"))
                else:
                    cur.execute("SELECT SUM(amount) FROM `stock` WHERE user_id IN (SELECT id FROM users WHERE LOWER(user_type) LIKE %s)", ("%miller%",))
                rows.append(('miller', cur.fetchone()[0] or 0))
            except Exception:
                rows.append(('miller', 0))

        totals = {'pmb': 0.0, 'collecter': 0.0, 'miller': 0.0}
        for r in rows:
            try:
                ut = (r[0] or '').lower()
                val = float(r[1] or 0)
                if 'pmb' in ut:
                    totals['pmb'] += val
                elif 'collect' in ut:
                    totals['collecter'] += val
                elif 'miller' in ut:
                    totals['miller'] += val
            except Exception:
                continue

        cur.close()
        conn.close()
        return jsonify(totals)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/stock_by_type', methods=['GET'])
def api_get_stock_by_type():
    """Return aggregated stock amounts by paddy/rice type for a specific user.
    
    Query params:
    - kind: 'paddy' (from stock table) or 'rice' (from rice_stock table)
    - user_id: (optional) filter by user_id/miller_id. If not provided, returns all stock
    
    Response: [ { type/paddy_type: str, quantity: float }, ... ]
    """
    try:
        kind = (request.args.get('kind') or 'paddy').strip().lower()
        user_id = (request.args.get('user_id') or '').strip()
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor()
        
        result = []
        
        if kind == 'rice':
            # Get rice stock aggregated by paddy_type from rice_stock table
            try:
                if user_id:
                    cur.execute("SELECT paddy_type, SUM(quantity) as total FROM `rice_stock` WHERE miller_id = %s GROUP BY paddy_type ORDER BY paddy_type", (user_id,))
                else:
                    cur.execute("SELECT paddy_type, SUM(quantity) as total FROM `rice_stock` GROUP BY paddy_type ORDER BY paddy_type")
                rows = cur.fetchall()
                for row in rows:
                    result.append({
                        'paddy_type': row[0],
                        'quantity': float(row[1] or 0)
                    })
            except Exception as e:
                print(f"Error fetching rice stock: {e}")
                return jsonify([])
        else:
            # Get paddy stock aggregated by type from stock table
            try:
                if user_id:
                    cur.execute("SELECT `type`, SUM(amount) as total FROM `stock` WHERE user_id = %s GROUP BY `type` ORDER BY `type`", (user_id,))
                else:
                    cur.execute("SELECT `type`, SUM(amount) as total FROM `stock` GROUP BY `type` ORDER BY `type`")
                rows = cur.fetchall()
                for row in rows:
                    result.append({
                        'type': row[0],
                        'quantity': float(row[1] or 0)
                    })
            except Exception as e:
                print(f"Error fetching paddy stock: {e}")
                return jsonify([])
        
        cur.close()
        conn.close()
        return jsonify(result)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/stock_history', methods=['GET'])
def api_get_stock_history():
    """Return daily stock amounts by user role for time-series chart.
    
    Response: { dates: [...], pmb: [...], collecter: [...], miller: [...] }
    """
    try:
        paddy_type = (request.args.get('paddy_type') or '').strip()
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor()
        try:
            if paddy_type:
                sql = "SELECT LOWER(u.user_type) as ut, SUM(s.amount) FROM `stock` s JOIN users u ON s.user_id = u.id WHERE s.`type` = %s GROUP BY LOWER(u.user_type)"
                cur.execute(sql, (paddy_type,))
            else:
                cur.execute("SELECT LOWER(u.user_type) as ut, SUM(s.amount) FROM `stock` s JOIN users u ON s.user_id = u.id GROUP BY LOWER(u.user_type)")
            rows = cur.fetchall()
        except Exception:
            rows = []

        totals = {'pmb': 0.0, 'collecter': 0.0, 'miller': 0.0}
        for r in rows:
            try:
                ut = (r[0] or '').lower()
                val = float(r[1] or 0)
                if 'pmb' in ut:
                    totals['pmb'] += val
                elif 'collect' in ut:
                    totals['collecter'] += val
                elif 'miller' in ut:
                    totals['miller'] += val
            except Exception:
                continue

        cur.close()
        conn.close()
        
        # Generate 7 days of data (simulation: variations around current totals)
        import datetime
        import random
        dates = []
        pmb_series = []
        col_series = []
        mil_series = []
        today = datetime.date.today()
        for i in range(6, -1, -1):
            d = today - datetime.timedelta(days=i)
            dates.append(d.strftime('%Y-%m-%d'))
            factor = 0.8 + (0.4 * (6-i)/6)
            pmb_series.append(round(totals['pmb'] * factor + random.uniform(-5, 5), 2))
            col_series.append(round(totals['collecter'] * factor + random.uniform(-10, 10), 2))
            mil_series.append(round(totals['miller'] * factor + random.uniform(-10, 10), 2))
        
        return jsonify({
            'dates': dates,
            'pmb': pmb_series,
            'collecter': col_series,
            'miller': mil_series
        })
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/users/by_type', methods=['GET'])
def api_get_users_by_type():
    """Return list of users for a given user type.
    Query params:
      - type (string): user type to filter by (case-insensitive, substring match)
    Response: JSON array of {id, full_name, user_code}
    """
    typ = (request.args.get('type') or request.args.get('user_type') or '').strip()
    if not typ:
        return jsonify({'error': 'query parameter "type" is required'}), 400

    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        # Case-insensitive substring match to be forgiving with stored values
        sql = 'SELECT id, full_name, company_name, user_type FROM users WHERE LOWER(user_type) LIKE %s ORDER BY id'
        cur.execute(sql, (f"%{typ.lower()}%",))
        rows = cur.fetchall()

        prefix_map = {
            'Farmer': 'FAR',
            'Collecter': 'COL',
            'Miller': 'MIL',
            'Wholesaler': 'WHO',
            'Retailer': 'RET',
            'Beer': 'BER',
            'Animal Food': 'ANI',
            'Exporter': 'EXP'
        }
        out = []
        for r in rows:
            try:
                prefix = prefix_map.get(r.get('user_type'), 'USR')
                user_code = f"{prefix}{int(r.get('id')):06d}" if r.get('id') is not None else None
            except Exception:
                user_code = None
            out.append({'id': r.get('id'), 'full_name': r.get('full_name'), 'company_name': r.get('company_name'), 'user_code': user_code})

        cur.close()
        conn.close()
        return jsonify(out)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/users/<user_id>', methods=['GET'])
def api_get_user(user_id):
    """Return user details including contact information.
    Response: JSON object with {id, full_name, user_type, contact_number, address, district, ...}
    """
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        sql = 'SELECT id, full_name, user_type, contact_number, address, district FROM users WHERE id = %s LIMIT 1'
        cur.execute(sql, (user_id,))
        user = cur.fetchone()
        cur.close()
        conn.close()
        
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        return jsonify(user)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/transactions', methods=['POST'])
def api_add_transaction():
    """Insert a transaction record into the transaction table.
    Expects JSON body: { from, to, type, quantity, datetime, price, status }
    status: 1 = normal transaction, 0 = revert transaction (restores stock)
    price: optional price per unit
    """
    payload = request.get_json() or {}
    from_val = payload.get('from')
    to_val = payload.get('to')
    ttype = payload.get('type')
    quantity = payload.get('quantity')
    dt = payload.get('datetime')
    # Convert price to float with 2 decimal places
    price_raw = payload.get('price')
    price = float(price_raw) if price_raw else 0.0
    price = round(price, 2)  # Ensure exactly 2 decimal places
    status = payload.get('status', 1)  # Default to 1 (normal) if not specified

    # basic validation
    if from_val is None or to_val is None or not ttype or quantity is None:
        return jsonify({'ok': False, 'error': 'Missing required fields (from,to,type,quantity)'}), 400

    try:
        # convert quantity to Decimal-like value (float is acceptable here)
        qty = float(quantity)
    except Exception:
        return jsonify({'ok': False, 'error': 'Invalid quantity'}), 400

    try:
        conn = get_connection(MYSQL_DATABASE)
        # perform insert + stock update atomically
        try:
            conn.start_transaction()
        except Exception:
            # some connectors use begin; ignore if not available
            pass
        cur = conn.cursor(buffered=True)
        # Determine sender type
        sender_type = None
        try:
            cur.execute('SELECT user_type FROM users WHERE id = %s LIMIT 1', (str(from_val),))
            urow = cur.fetchone()
            sender_type = urow[0] if urow else None
        except Exception:
            sender_type = None

        # Check if sender is a farmer (farmers don't have stock tracking)
        is_sender_farmer = isinstance(sender_type, str) and sender_type.strip().lower().startswith('farmer')

        # Determine if this is a rice transaction (sender is Miller, PMB, Wholesaler, or Retailer selling rice)
        is_rice_transaction = isinstance(sender_type, str) and (
            'miller' in sender_type.lower() or 
            'pmb' in sender_type.lower() or 
            'wholesaler' in sender_type.lower() or 
            'retailer' in sender_type.lower()
        )

        # Check if this is a revert transaction (status == 0)
        is_revert = status == 0

        # If sender is not a Farmer, ensure they have sufficient stock before proceeding (unless reverting)
        try:
            if not is_sender_farmer:
                if is_rice_transaction:
                    # For rice transactions, check rice_stock table
                    sel_s_sql = 'SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s FOR UPDATE'
                    cur.execute(sel_s_sql, (str(from_val), ttype))
                    srow = cur.fetchone()
                    if not srow:
                        # no stock row -> insufficient (unless reverting)
                        if not is_revert:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            cur.close()
                            conn.close()
                            return jsonify({'ok': False, 'error': 'Insufficient rice stock: sender has no rice stock for this type'}), 400
                        else:
                            # For revert with no stock row, create one to hold the restored amount
                            ins_stock_sql = 'INSERT INTO `rice_stock` (miller_id, paddy_type, quantity) VALUES (%s, %s, %s)'
                            cur.execute(ins_stock_sql, (str(from_val), ttype, qty))
                            srow = (cur.lastrowid, qty)
                    
                    s_stock_id, s_current = srow[0], srow[1] if srow[1] is not None else 0
                    
                    if not is_revert:
                        # Normal transaction: deduct from sender
                        if float(s_current) < float(qty):
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            cur.close()
                            conn.close()
                            return jsonify({'ok': False, 'error': 'Insufficient rice stock: sender balance is lower than requested quantity'}), 400
                        s_new = float(s_current) - float(qty)
                    else:
                        # Revert transaction: add back to sender
                        s_new = float(s_current) + float(qty)
                    
                    upd_s_sql = 'UPDATE `rice_stock` SET quantity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
                    cur.execute(upd_s_sql, (s_new, s_stock_id))
                else:
                    # For paddy transactions, check stock table
                    sel_s_sql = 'SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s FOR UPDATE'
                    cur.execute(sel_s_sql, (str(from_val), ttype))
                    srow = cur.fetchone()
                    if not srow:
                        # no stock row -> insufficient (unless reverting)
                        if not is_revert:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            cur.close()
                            conn.close()
                            return jsonify({'ok': False, 'error': 'Insufficient stock: sender has no stock for this paddy type'}), 400
                        else:
                            # For revert with no stock row, create one to hold the restored amount
                            ins_stock_sql = 'INSERT INTO `stock` (user_id, `type`, amount) VALUES (%s, %s, %s)'
                            cur.execute(ins_stock_sql, (str(from_val), ttype, qty))
                            srow = (cur.lastrowid, qty)
                    
                    s_stock_id, s_current = srow[0], srow[1] if srow[1] is not None else 0
                    
                    if not is_revert:
                        # Normal transaction: deduct from sender
                        if float(s_current) < float(qty):
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            cur.close()
                            conn.close()
                            return jsonify({'ok': False, 'error': 'Insufficient stock: sender balance is lower than requested quantity'}), 400
                        s_new = float(s_current) - float(qty)
                    else:
                        # Revert transaction: add back to sender
                        s_new = float(s_current) + float(qty)
                    
                    upd_s_sql = 'UPDATE `stock` SET amount = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
                    cur.execute(upd_s_sql, (s_new, s_stock_id))

        except mysql.connector.Error as e:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Failed checking/deducting sender stock: ' + str(e)}), 500

        # Update recipient stock: add for normal transactions, deduct for reversals (only if sender has stock tracking)
        # For revert: if sender is Farmer, only deduct from recipient (no add to farmer)
        #            if sender is Collector, deduct from recipient AND add to sender
        try:
            if is_rice_transaction:
                # For rice transactions, update recipient rice_stock table
                sel_sql = 'SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s FOR UPDATE'
                cur.execute(sel_sql, (str(to_val), ttype))
                row = cur.fetchone()
                if row:
                    stock_id, current_amount = row[0], row[1] if row[1] is not None else 0
                    if is_revert:
                        # Revert: deduct from recipient
                        new_amount = float(current_amount) - float(qty)
                        if new_amount < 0:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            cur.close()
                            conn.close()
                            return jsonify({'ok': False, 'error': 'Cannot revert: recipient has insufficient rice stock to deduct from'}), 400
                    else:
                        # Normal: add to recipient
                        new_amount = float(current_amount) + float(qty)
                    upd_sql = 'UPDATE `rice_stock` SET quantity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
                    cur.execute(upd_sql, (new_amount, stock_id))
                else:
                    # No recipient stock row
                    if is_revert:
                        # Cannot revert if recipient has no stock to deduct from
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        cur.close()
                        conn.close()
                        return jsonify({'ok': False, 'error': 'Cannot revert: recipient has no rice stock for this type to deduct from'}), 400
                    else:
                        # Normal: create new stock row for recipient
                        ins_sql = 'INSERT INTO `rice_stock` (miller_id, paddy_type, quantity) VALUES (%s, %s, %s)'
                        cur.execute(ins_sql, (str(to_val), ttype, qty))
            else:
                # For paddy transactions, update recipient stock table
                sel_sql = 'SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s FOR UPDATE'
                cur.execute(sel_sql, (str(to_val), ttype))
                row = cur.fetchone()
                if row:
                    stock_id, current_amount = row[0], row[1] if row[1] is not None else 0
                    if is_revert:
                        # Revert: deduct from recipient
                        new_amount = float(current_amount) - float(qty)
                        if new_amount < 0:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            cur.close()
                            conn.close()
                            return jsonify({'ok': False, 'error': 'Cannot revert: recipient has insufficient stock to deduct from'}), 400
                    else:
                        # Normal: add to recipient
                        new_amount = float(current_amount) + float(qty)
                    upd_sql = 'UPDATE `stock` SET amount = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s'
                    cur.execute(upd_sql, (new_amount, stock_id))
                else:
                    # No recipient stock row
                    if is_revert:
                        # Cannot revert if recipient has no stock to deduct from
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        cur.close()
                        conn.close()
                        return jsonify({'ok': False, 'error': 'Cannot revert: recipient has no stock for this paddy type to deduct from'}), 400
                    else:
                        # Normal: create new stock row for recipient
                        ins_sql = 'INSERT INTO `stock` (user_id, `type`, amount) VALUES (%s, %s, %s)'
                        cur.execute(ins_sql, (str(to_val), ttype, qty))

        except mysql.connector.Error as e:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Failed updating recipient stock: ' + str(e)}), 500

        # Now insert the transaction record (after stock updates)
        block_hash = None
        block_number = None
        transaction_hash = None
        blockchain_transaction_id = None
        block_number = None
        transaction_hash = None
        try:
            # Record transaction on blockchain
            try:
                # Convert quantity to int for blockchain (assuming kg)
                qty_int = int(float(qty))
                
                if is_rice_transaction:
                    # Use rice transaction blockchain function
                    # status=True for normal (1), False for revert (0)
                    result = record_rice_transaction(
                        str(from_val),
                        str(to_val),
                        ttype,
                        qty_int,
                        price,
                        status == 1  # Convert status 1/0 to True/False
                    )
                    if result and isinstance(result, dict):
                        block_hash = result.get('block_hash')
                        block_number = result.get('block_number')
                        transaction_hash = result.get('transaction_hash')
                        blockchain_transaction_id = result.get('transaction_id')
                    print(f"Blockchain RICE transaction recorded. Block hash: {block_hash}")
                else:
                    # Use regular paddy transaction blockchain function
                    # status=True for normal (1), False for revert (0)
                    result = record_transaction(
                        str(from_val),
                        str(to_val),
                        ttype,
                        qty_int,
                        price,
                        0.0,
                        status == 1  # Convert status 1/0 to True/False
                    )
                    if result and isinstance(result, dict):
                        block_hash = result.get('block_hash')
                        block_number = result.get('block_number')
                        transaction_hash = result.get('transaction_hash')
                        blockchain_transaction_id = result.get('transaction_id')
                        print(f"DEBUG app.py: blockchain_transaction_id = {blockchain_transaction_id}")
                    print(f"Blockchain PADDY transaction recorded. Block hash: {block_hash}")
            except Exception as e:
                print(f"Failed to record transaction on blockchain: {e}")
                # Continue with database insert even if blockchain fails
                block_hash = None
                block_number = None
                transaction_hash = None
                blockchain_transaction_id = None
            
            # Insert into appropriate table based on transaction type
            # Set is_reverted to 1 if this is a revert transaction (status == 0)
            is_reverted_val = 1 if is_revert else 0
            
            if is_rice_transaction:
                # Insert into rice_transaction table with status and blockchain transaction_id
                if blockchain_transaction_id:
                    print(f"DEBUG: Inserting rice transaction with blockchain_transaction_id = {blockchain_transaction_id}")
                    insert_sql = 'INSERT INTO `rice_transaction` (id, `from`, `to`, rice_type, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    cur.execute(insert_sql, (blockchain_transaction_id, str(from_val), str(to_val), ttype, qty, price, int(status), is_reverted_val, dt, block_hash, block_number, transaction_hash))
                else:
                    print(f"DEBUG: Inserting rice transaction WITHOUT blockchain_transaction_id")
                    insert_sql = 'INSERT INTO `rice_transaction` (`from`, `to`, rice_type, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    cur.execute(insert_sql, (str(from_val), str(to_val), ttype, qty, price, int(status), is_reverted_val, dt, block_hash, block_number, transaction_hash))
            else:
                # Insert into regular transaction table (paddy) with status and blockchain transaction_id
                if blockchain_transaction_id:
                    print(f"DEBUG: Inserting paddy transaction with blockchain_transaction_id = {blockchain_transaction_id}")
                    insert_sql = 'INSERT INTO `transaction` (id, `from`, `to`, `type`, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    cur.execute(insert_sql, (blockchain_transaction_id, str(from_val), str(to_val), ttype, qty, price, int(status), is_reverted_val, dt, block_hash, block_number, transaction_hash))
                else:
                    print(f"DEBUG: Inserting paddy transaction WITHOUT blockchain_transaction_id")
                    insert_sql = 'INSERT INTO `transaction` (`from`, `to`, `type`, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    cur.execute(insert_sql, (str(from_val), str(to_val), ttype, qty, price, int(status), is_reverted_val, dt, block_hash, block_number, transaction_hash))
            last_id = cur.lastrowid
        except mysql.connector.Error as e:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Failed inserting transaction: ' + str(e)}), 500

        # If this is a revert transaction, mark the original transaction as reverted
        original_transaction_id = payload.get('original_transaction_id')
        if is_revert and original_transaction_id:
            try:
                # Update the original transaction's is_reverted field to 1
                if is_rice_transaction:
                    update_sql = 'UPDATE `rice_transaction` SET is_reverted = 1 WHERE id = %s'
                else:
                    update_sql = 'UPDATE `transaction` SET is_reverted = 1 WHERE id = %s'
                cur.execute(update_sql, (int(original_transaction_id),))
            except Exception as e:
                print(f"Failed to mark original transaction as reverted: {e}")
                # Continue anyway - revert transaction was created

        # commit both transaction insert and stock update
        try:
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Failed to commit transaction'}), 500

        cur.close()
        conn.close()
        return jsonify({'ok': True, 'id': last_id, 'block_hash': block_hash}), 201
    except mysql.connector.Error as err:
        return jsonify({'ok': False, 'error': str(err)}), 500


@app.route('/api/transactions/<int:transaction_id>', methods=['PUT'])
def api_update_transaction(transaction_id):
    """Update a transaction quantity and adjust stock accordingly.
    Expects JSON body: { quantity }
    """
    try:
        data = request.get_json()
        new_quantity = data.get('quantity')
        
        if new_quantity is None or float(new_quantity) < 0:
            return jsonify({'ok': False, 'error': 'Invalid quantity'}), 400
        
        new_quantity = float(new_quantity)
        
        conn = get_connection(MYSQL_DATABASE)
        conn.start_transaction()
        cur = conn.cursor(buffered=True, dictionary=True)
        
        # Get the current transaction details
        cur.execute('SELECT `from`, `to`, `type`, quantity FROM `transaction` WHERE id = %s FOR UPDATE', (transaction_id,))
        
        if not tx_row:
            return jsonify({'ok': False, 'error': 'Transaction not found'}), 404
        
        old_quantity = tx_row['quantity']
        
        # Adjust stock based on the new quantity
        if old_quantity != new_quantity:
            # Determine sender type
            sender_type = None
            try:
                cur.execute('SELECT user_type FROM users WHERE id = %s LIMIT 1', (tx_row['from'],))
                urow = cur.fetchone()
                sender_type = urow[0] if urow else None
            except Exception:
                sender_type = None
            
            # Update stock for sender
            if isinstance(sender_type, str) and not sender_type.strip().lower().startswith('farmer'):
                # For rice transactions, check rice_stock table
                if 'miller' in sender_type.lower() or 'pmb' in sender_type.lower() or 'wholesaler' in sender_type.lower() or 'retailer' in sender_type.lower():
                    sel_s_sql = 'SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s FOR UPDATE'
                    cur.execute(sel_s_sql, (tx_row['from'], tx_row['type']))
                    srow = cur.fetchone()
                    if srow:
                        s_stock_id, s_current = srow[0], srow[1] if srow[1] is not None else 0
                        # Adjust stock
                        new_stock_quantity = s_current + (new_quantity - old_quantity)
                        upd_s_sql = 'UPDATE `rice_stock` SET quantity = %s WHERE id = %s'
                        cur.execute(upd_s_sql, (new_stock_quantity, s_stock_id))
                else:
                    # For paddy transactions, check stock table
                    sel_s_sql = 'SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s FOR UPDATE'
                    cur.execute(sel_s_sql, (tx_row['from'], tx_row['type']))
                    srow = cur.fetchone()
                    if srow:
                        s_stock_id, s_current = srow[0], srow[1] if srow[1] is not None else 0
                        # Adjust stock
                        new_stock_quantity = s_current + (new_quantity - old_quantity)
                        upd_s_sql = 'UPDATE `stock` SET amount = %s WHERE id = %s'
                        cur.execute(upd_s_sql, (new_stock_quantity, s_stock_id))
        
        # Update the transaction with the new quantity
        cur.execute('UPDATE `transaction` SET quantity = %s WHERE id = %s', (new_quantity, transaction_id))
        
        conn.commit()
        return jsonify({'ok': True, 'message': 'Transaction updated successfully'}), 200
    
    except mysql.connector.Error as err:
        return jsonify({'ok': False, 'error': str(err)}), 500
        tx_row = cur.fetchone()
        
        if not tx_row:
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Transaction not found'}), 404
        
        old_quantity = float(tx_row['quantity'] or 0)
        quantity_diff = new_quantity - old_quantity
        
        if quantity_diff == 0:
            cur.close()
            conn.close()
            return jsonify({'ok': True, 'message': 'No changes needed'}), 200
        
        from_user = tx_row['from']
        to_user = tx_row['to']
        paddy_type = tx_row['type']
        
        # Adjust recipient (to) stock: add the difference
        cur.execute('SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s FOR UPDATE', 
                    (str(to_user), paddy_type))
        to_stock = cur.fetchone()
        
        if to_stock:
            new_to_amount = float(to_stock['amount'] or 0) + quantity_diff
            if new_to_amount < 0:
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'ok': False, 'error': 'Insufficient stock for recipient after adjustment'}), 400
            cur.execute('UPDATE `stock` SET amount = %s WHERE id = %s', (new_to_amount, to_stock['id']))
        else:
            # If no stock exists for recipient, only allow increases
            if quantity_diff < 0:
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'ok': False, 'error': 'No stock record found for recipient'}), 400
            cur.execute('INSERT INTO `stock` (user_id, `type`, amount) VALUES (%s, %s, %s)', 
                        (str(to_user), paddy_type, quantity_diff))
        
        # Adjust sender (from) stock if not a farmer: subtract the difference
        # (Farmers don't track stock going out)
        cur.execute('SELECT user_type FROM users WHERE id = %s', (str(from_user),))
        from_user_type = cur.fetchone()
        is_farmer = from_user_type and 'farmer' in (from_user_type.get('user_type') or '').lower()
        
        if not is_farmer:
            cur.execute('SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s FOR UPDATE', 
                        (str(from_user), paddy_type))
            from_stock = cur.fetchone()
            
            if from_stock:
                new_from_amount = float(from_stock['amount'] or 0) - quantity_diff
                if new_from_amount < 0:
                    conn.rollback()
                    cur.close()
                    conn.close()
                    return jsonify({'ok': False, 'error': 'Insufficient stock for sender after adjustment'}), 400
                cur.execute('UPDATE `stock` SET amount = %s WHERE id = %s', (new_from_amount, from_stock['id']))
        
        # Update the transaction record
        cur.execute('UPDATE `transaction` SET quantity = %s WHERE id = %s', (new_quantity, transaction_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'ok': True, 'message': 'Transaction updated successfully'}), 200
        
    except mysql.connector.Error as err:
        try:
            conn.rollback()
        except:
            pass
        return jsonify({'ok': False, 'error': str(err)}), 500


@app.route('/api/transactions', methods=['GET'])
def api_get_transactions():
    """Return transactions from both transaction and rice_transaction tables.
    Optional query param `to` to filter by recipient, `from` to filter by sender, `user` for either.
    """
    to_param = request.args.get('to')
    from_param = request.args.get('from')
    user_param = request.args.get('user')
    
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Query paddy transactions with user details
        paddy_transactions = []
        if to_param:
            sql = '''SELECT t.id, t.`from`, t.`to`, t.`type`, t.quantity, t.price, t.status, t.is_reverted, 
                     t.`datetime`, t.block_hash, t.block_number, t.transaction_hash, t.created_at,
                     u_from.full_name as from_name, u_from.address as from_address, u_from.contact_number as from_contact,
                     u_to.full_name as to_name, u_to.address as to_address, u_to.contact_number as to_contact
                     FROM `transaction` t
                     LEFT JOIN users u_from ON t.`from` = u_from.id
                     LEFT JOIN users u_to ON t.`to` = u_to.id
                     WHERE t.`to` = %s ORDER BY t.id DESC'''
            cur.execute(sql, (str(to_param),))
        elif from_param:
            sql = '''SELECT t.id, t.`from`, t.`to`, t.`type`, t.quantity, t.price, t.status, t.is_reverted, 
                     t.`datetime`, t.block_hash, t.block_number, t.transaction_hash, t.created_at,
                     u_from.full_name as from_name, u_from.address as from_address, u_from.contact_number as from_contact,
                     u_to.full_name as to_name, u_to.address as to_address, u_to.contact_number as to_contact
                     FROM `transaction` t
                     LEFT JOIN users u_from ON t.`from` = u_from.id
                     LEFT JOIN users u_to ON t.`to` = u_to.id
                     WHERE t.`from` = %s ORDER BY t.id DESC'''
            cur.execute(sql, (str(from_param),))
        elif user_param:
            sql = '''SELECT t.id, t.`from`, t.`to`, t.`type`, t.quantity, t.price, t.status, t.is_reverted, 
                     t.`datetime`, t.block_hash, t.block_number, t.transaction_hash, t.created_at,
                     u_from.full_name as from_name, u_from.address as from_address, u_from.contact_number as from_contact,
                     u_to.full_name as to_name, u_to.address as to_address, u_to.contact_number as to_contact
                     FROM `transaction` t
                     LEFT JOIN users u_from ON t.`from` = u_from.id
                     LEFT JOIN users u_to ON t.`to` = u_to.id
                     WHERE t.`from` = %s OR t.`to` = %s ORDER BY t.id DESC'''
            cur.execute(sql, (str(user_param), str(user_param)))
        else:
            sql = '''SELECT t.id, t.`from`, t.`to`, t.`type`, t.quantity, t.price, t.status, t.is_reverted, 
                     t.`datetime`, t.block_hash, t.block_number, t.transaction_hash, t.created_at,
                     u_from.full_name as from_name, u_from.address as from_address, u_from.contact_number as from_contact,
                     u_to.full_name as to_name, u_to.address as to_address, u_to.contact_number as to_contact
                     FROM `transaction` t
                     LEFT JOIN users u_from ON t.`from` = u_from.id
                     LEFT JOIN users u_to ON t.`to` = u_to.id
                     ORDER BY t.id DESC LIMIT 200'''
            cur.execute(sql)
        paddy_transactions = cur.fetchall()
        
        # Query rice transactions
        rice_transactions = []
        if to_param:
            sql = 'SELECT id, `from`, `to`, rice_type as `type`, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash, created_at FROM `rice_transaction` WHERE `to` = %s ORDER BY id DESC'
            cur.execute(sql, (str(to_param),))
        elif from_param:
            sql = 'SELECT id, `from`, `to`, rice_type as `type`, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash, created_at FROM `rice_transaction` WHERE `from` = %s ORDER BY id DESC'
            cur.execute(sql, (str(from_param),))
        elif user_param:
            sql = 'SELECT id, `from`, `to`, rice_type as `type`, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash, created_at FROM `rice_transaction` WHERE `from` = %s OR `to` = %s ORDER BY id DESC'
            cur.execute(sql, (str(user_param), str(user_param)))
        else:
            sql = 'SELECT id, `from`, `to`, rice_type as `type`, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash, created_at FROM `rice_transaction` ORDER BY id DESC LIMIT 200'
            cur.execute(sql)
        rice_transactions = cur.fetchall()
        
        # Merge and sort by datetime
        all_transactions = paddy_transactions + rice_transactions
        all_transactions.sort(key=lambda x: x.get('datetime') or x.get('created_at') or '', reverse=True)
        
        cur.close()
        conn.close()
        return jsonify(all_transactions)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/paddy_types', methods=['GET'])
def api_get_paddy_types():
    """Return list of paddy types that have actual data in rice_stock."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        # Only return paddy types that exist in rice_stock table
        cur.execute('''
            SELECT DISTINCT rs.paddy_type as name
            FROM rice_stock rs
            WHERE rs.paddy_type IS NOT NULL AND rs.paddy_type != ''
            ORDER BY rs.paddy_type
        ''')
        rows = cur.fetchall()
        
        # If no data in rice_stock, get from paddy_type table
        if not rows:
            cur.execute('SELECT id, name FROM paddy_type ORDER BY id')
            rows = cur.fetchall()
        
        cur.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/paddy_type_list', methods=['GET'])
def api_get_paddy_type_list():
    """Return list of all paddy types from paddy_type table."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        cur.execute('SELECT id, name FROM paddy_type ORDER BY id')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/rice_types', methods=['GET'])
def api_get_rice_types():
    """Return list of rice types from paddy_type table."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        cur.execute('SELECT id, name FROM paddy_type ORDER BY id')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/rice_transactions', methods=['GET'])
def api_get_rice_transactions():
    """Return list of rice transactions with optional filters."""
    transaction_type = request.args.get('transaction_type')
    rice_type = request.args.get('rice_type')
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    search = request.args.get('search')
    transaction_id = request.args.get('transaction_id')
    
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        sql = '''
            SELECT 
                rt.id,
                rt.`from`,
                rt.`to`,
                rt.rice_type,
                rt.quantity,
                rt.price,
                rt.status,
                rt.is_reverted,
                rt.datetime,
                rt.block_hash,
                rt.block_number,
                rt.transaction_hash,
                u_from.user_type as from_user_type,
                u_from.full_name as from_full_name,
                u_to.user_type as to_user_type,
                u_to.full_name as to_full_name
            FROM rice_transaction rt
            LEFT JOIN users u_from ON rt.`from` = u_from.id
            LEFT JOIN users u_to ON rt.`to` = u_to.id
            WHERE 1=1
        '''
        params = []
        
        if transaction_type:
            # Transaction type format: "from_type-to_type" e.g., "miller-wholesaler"
            types = transaction_type.split('-')
            if len(types) == 2:
                from_type, to_type = types
                sql += ' AND LOWER(u_from.user_type) LIKE %s AND LOWER(u_to.user_type) LIKE %s'
                params.append('%' + from_type.lower() + '%')
                params.append('%' + to_type.lower() + '%')
        
        if rice_type:
            sql += ' AND rt.rice_type = %s'
            params.append(str(rice_type))
        
        if from_date:
            sql += ' AND DATE(rt.datetime) >= %s'
            params.append(str(from_date))
        
        if to_date:
            sql += ' AND DATE(rt.datetime) <= %s'
            params.append(str(to_date))
        
        if search:
            sql += ' AND (rt.`from` LIKE %s OR rt.`to` LIKE %s OR u_from.full_name LIKE %s OR u_to.full_name LIKE %s)'
            search_pattern = '%' + str(search) + '%'
            params.extend([search_pattern, search_pattern, search_pattern, search_pattern])
        
        if transaction_id:
            sql += ' AND rt.id LIKE %s'
            params.append('%' + str(transaction_id) + '%')
        
        sql += ' ORDER BY rt.datetime DESC, rt.id DESC'
        
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify(rows if rows else [])
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/rice_transactions/<int:transaction_id>/revert', methods=['POST'])
def api_revert_rice_transaction(transaction_id):
    """Revert a rice transaction by recording a reverse transaction on blockchain and updating stock."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Get the original rice transaction
        cur.execute('SELECT id, `from`, `to`, rice_type, quantity, price FROM `rice_transaction` WHERE id = %s LIMIT 1', (transaction_id,))
        tx = cur.fetchone()
        
        if not tx:
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Rice transaction not found'}), 404
        
        from_party = tx['from']
        to_party = tx['to']
        rice_type = tx['rice_type']
        quantity = float(tx['quantity'])
        price = float(tx['price']) if tx['price'] else 0.0
        
        print(f"Reverting rice transaction: {transaction_id}")
        print(f"  From: {from_party}, To: {to_party}, Type: {rice_type}, Qty: {quantity}, Price: {price}")
        print(f"DEBUG: Retrieved price from DB: {tx['price']}, converted to: {price}")
        
        # Start database transaction
        try:
            conn.start_transaction()
        except Exception:
            pass
        
        # Revert stock: return to sender, deduct from receiver
        try:
            # Add back to sender's rice_stock
            cur.execute('SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s FOR UPDATE', 
                        (str(from_party), rice_type))
            sender_stock = cur.fetchone()
            if sender_stock:
                new_qty = float(sender_stock['quantity']) + quantity
                cur.execute('UPDATE `rice_stock` SET quantity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s', 
                            (new_qty, sender_stock['id']))
            else:
                cur.execute('INSERT INTO `rice_stock` (miller_id, paddy_type, quantity) VALUES (%s, %s, %s)', 
                            (str(from_party), rice_type, quantity))
            
            # Deduct from receiver's rice_stock
            cur.execute('SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s FOR UPDATE', 
                        (str(to_party), rice_type))
            receiver_stock = cur.fetchone()
            if receiver_stock:
                new_qty = float(receiver_stock['quantity']) - quantity
                if new_qty < 0:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    cur.close()
                    conn.close()
                    return jsonify({'ok': False, 'error': 'Insufficient rice stock in receiver to revert'}), 400
                cur.execute('UPDATE `rice_stock` SET quantity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s', 
                            (new_qty, receiver_stock['id']))
            else:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.close()
                conn.close()
                return jsonify({'ok': False, 'error': 'Receiver has no rice stock to revert from'}), 400
        
        except mysql.connector.Error as e:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': f'Failed updating stock: {str(e)}'}), 500
        
        # Record revert transaction on blockchain
        block_hash = None
        block_number = None
        transaction_hash = None
        revert_transaction_id = None
        
        try:
            qty_int = int(quantity)
            result = revert_rice_transaction(str(from_party), str(to_party), rice_type, qty_int, price)
            if result and isinstance(result, dict):
                block_hash = result.get('block_hash')
                block_number = result.get('block_number')
                transaction_hash = result.get('transaction_hash')
                revert_transaction_id = result.get('transaction_id')
            print(f"Blockchain RICE revert recorded. Block hash: {block_hash}")
        except Exception as e:
            print(f"Failed to record revert on blockchain: {e}")
            # Continue with database update even if blockchain fails
        
        # Insert revert transaction record
        try:
            print(f"DEBUG: Inserting revert with price={price}")
            if revert_transaction_id:
                insert_sql = 'INSERT INTO `rice_transaction` (id, `from`, `to`, rice_type, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cur.execute(insert_sql, (revert_transaction_id, str(to_party), str(from_party), rice_type, quantity, price, 0, 0, datetime.datetime.now().isoformat(), block_hash, block_number, transaction_hash))
            else:
                insert_sql = 'INSERT INTO `rice_transaction` (`from`, `to`, rice_type, quantity, price, status, is_reverted, `datetime`, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cur.execute(insert_sql, (str(to_party), str(from_party), rice_type, quantity, price, 0, 0, datetime.datetime.now().isoformat(), block_hash, block_number, transaction_hash))
            
            conn.commit()
        except mysql.connector.Error as e:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': f'Failed inserting revert record: {str(e)}'}), 500
        
        cur.close()
        conn.close()
        
        return jsonify({'ok': True, 'message': 'Rice transaction reverted successfully', 'block_hash': block_hash}), 200
    
    except Exception as e:
        print(f"Error reverting rice transaction: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/damages', methods=['POST'])
def api_add_damage():
    """Insert a damage record into the damage table and deduct from stock.
    Expects JSON body: { user_id, paddy_type, quantity, reason, damage_date, reverted }
    Validates that sufficient stock exists before recording damage.
    """
    payload = request.get_json() or {}
    user_id = payload.get('user_id')
    paddy_type = payload.get('paddy_type')
    quantity = payload.get('quantity')
    reason = payload.get('reason')
    damage_date = payload.get('damage_date')
    reverted = payload.get('reverted', 0)  # Default to 0 (not reverted)

    # basic validation
    if not user_id or not paddy_type or quantity is None or not reason:
        return jsonify({'ok': False, 'error': 'Missing required fields (user_id, paddy_type, quantity, reason)'}), 400

    try:
        qty = float(quantity)
        if qty <= 0:
            return jsonify({'ok': False, 'error': 'Quantity must be greater than 0'}), 400
    except Exception:
        return jsonify({'ok': False, 'error': 'Invalid quantity'}), 400

    try:
        conn = get_connection(MYSQL_DATABASE)
        # Start transaction
        try:
            conn.start_transaction()
        except Exception:
            pass
        
        cur = conn.cursor(buffered=True)
        
        # Determine user type to decide which blockchain function to use
        user_type = None
        try:
            cur.execute('SELECT user_type FROM users WHERE id = %s LIMIT 1', (str(user_id),))
            urow = cur.fetchone()
            user_type = urow[0] if urow else None
        except Exception:
            user_type = None
        
        # Determine if this is rice damage based on kind override or user type
        kind_override = (payload.get('kind') or '').strip().lower()
        if kind_override == 'rice':
            is_rice_damage = True
        elif kind_override == 'paddy':
            is_rice_damage = False
        else:
            # Determine if this is rice damage (user is Wholesaler, Retailer, Brewer, Animal Food, Exporter, PMB)
            is_rice_damage = isinstance(user_type, str) and any(
                role in user_type.lower()
                for role in ['wholesaler', 'retailer', 'brewer', 'animal', 'exporter', 'pmb']
            )
        
        # Check stock from appropriate table based on damage type
        if is_rice_damage:
            # For rice damage, check rice_stock table
            cur.execute('SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s FOR UPDATE', 
                        (str(user_id), paddy_type))
            stock_row = cur.fetchone()
            
            if not stock_row:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.close()
                conn.close()
                return jsonify({'ok': False, 'error': f'No rice stock found for type "{paddy_type}". Cannot record damage.'}), 400
            
            stock_id, current_amount = stock_row[0], stock_row[1] if stock_row[1] is not None else 0
            
            # Check if this is a revert operation
            is_revert = reason and reason.lower() == 'revert'
            
            if not is_revert and float(current_amount) < qty:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.close()
                conn.close()
                return jsonify({'ok': False, 'error': f'Insufficient rice stock. Available: {current_amount} kg, Requested: {qty} kg'}), 400
            
            # Revert adds to stock, normal damage deducts from stock
            if is_revert:
                new_amount = float(current_amount) + qty
            else:
                new_amount = float(current_amount) - qty
            
            cur.execute('UPDATE `rice_stock` SET quantity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s', 
                        (new_amount, stock_id))
        else:
            # For paddy damage, check stock table
            cur.execute('SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s FOR UPDATE', 
                        (str(user_id), paddy_type))
            stock_row = cur.fetchone()
            
            if not stock_row:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.close()
                conn.close()
                return jsonify({'ok': False, 'error': f'No stock found for paddy type "{paddy_type}". Cannot record damage.'}), 400
            
            stock_id, current_amount = stock_row[0], stock_row[1] if stock_row[1] is not None else 0
            
            # Check if this is a revert operation
            is_revert = reason and reason.lower() == 'revert'
            
            if not is_revert and float(current_amount) < qty:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.close()
                conn.close()
                return jsonify({'ok': False, 'error': f'Insufficient stock. Available: {current_amount} kg, Requested: {qty} kg'}), 400
            
            # Revert adds to stock, normal damage deducts from stock
            if is_revert:
                new_amount = float(current_amount) + qty
            else:
                new_amount = float(current_amount) - qty
            
            cur.execute('UPDATE `stock` SET amount = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s', 
                        (new_amount, stock_id))
        
        # Record damage on blockchain
        block_hash = None
        block_number = None
        transaction_hash = None
        result = None
        try:
            # Convert damage_date to timestamp for blockchain
            import datetime
            if damage_date:
                try:
                    dt_obj = datetime.datetime.fromisoformat(damage_date.replace('Z', '+00:00'))
                    timestamp = int(dt_obj.timestamp())
                except:
                    timestamp = int(datetime.datetime.now().timestamp())
            else:
                timestamp = int(datetime.datetime.now().timestamp())
            
            # Convert quantity to int for blockchain (assuming kg)
            qty_int = int(float(qty))
            
            if is_rice_damage:
                # Use rice damage blockchain function
                result = record_rice_damage(
                    str(user_id),
                    paddy_type,  # This is actually rice type in this context
                    qty_int,
                    timestamp,
                    reason
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print(f"Blockchain RICE damage recorded. Block hash: {block_hash}")
            else:
                # Use regular paddy damage blockchain function
                result = record_damage(
                    str(user_id),
                    paddy_type,
                    qty_int,
                    timestamp,
                    reason
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print(f"Blockchain PADDY damage recorded. Block hash: {block_hash}")
        except Exception as e:
            print(f"Failed to record damage on blockchain: {e}")
            # Continue with database insert even if blockchain fails
            block_hash = None
            block_number = None
            transaction_hash = None
        
        # Insert damage record into appropriate table
        damage_block_id = None
        try:
            if result and isinstance(result, dict):
                # For rice damage, blockchain returns transaction_id; for paddy damage, it returns damage_id
                if is_rice_damage:
                    damage_block_id = result.get('transaction_id')
                else:
                    damage_block_id = result.get('damage_id')
        except Exception:
            damage_block_id = None

        if is_rice_damage:
            # Insert into rice_damage table; include blockchain damage id if provided
            if damage_block_id is not None:
                insert_sql = 'INSERT INTO `rice_damage` (id, user_id, rice_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, reverted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cur.execute(insert_sql, (int(damage_block_id), str(user_id), paddy_type, qty, reason, damage_date, block_hash, block_number, transaction_hash, reverted))
                last_id = int(damage_block_id)
            else:
                insert_sql = 'INSERT INTO `rice_damage` (user_id, rice_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, reverted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cur.execute(insert_sql, (str(user_id), paddy_type, qty, reason, damage_date, block_hash, block_number, transaction_hash, reverted))
                last_id = cur.lastrowid
        else:
            # Insert into regular damage table (paddy); include blockchain damage id if provided
            if damage_block_id is not None:
                insert_sql = 'INSERT INTO `damage` (id, user_id, paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, reverted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cur.execute(insert_sql, (int(damage_block_id), str(user_id), paddy_type, qty, reason, damage_date, block_hash, block_number, transaction_hash, reverted))
                last_id = int(damage_block_id)
            else:
                insert_sql = 'INSERT INTO `damage` (user_id, paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, reverted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                cur.execute(insert_sql, (str(user_id), paddy_type, qty, reason, damage_date, block_hash, block_number, transaction_hash, reverted))
                last_id = cur.lastrowid
        
        # Commit transaction
        try:
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Failed to commit damage record'}), 500
        
        cur.close()
        conn.close()
        return jsonify({'ok': True, 'id': last_id, 'remaining_stock': new_amount, 'block_hash': block_hash}), 201
    except mysql.connector.Error as err:
        return jsonify({'ok': False, 'error': str(err)}), 500


@app.route('/api/damages', methods=['GET'])
def api_get_damages():
    """Return damage records from both damage and rice_damage tables.
    Optional query param `user_id` to filter by user.
    """
    user_id_param = request.args.get('user_id')
    kind = request.args.get('kind')  # 'rice' or 'paddy' to filter by type
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Query paddy damages
        paddy_damages = []
        if user_id_param:
            sql = 'SELECT id, user_id, paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `damage` WHERE user_id = %s ORDER BY id DESC'
            cur.execute(sql, (str(user_id_param),))
        else:
            sql = 'SELECT id, user_id, paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `damage` ORDER BY id DESC LIMIT 200'
            cur.execute(sql)
        paddy_damages = cur.fetchall()
        for r in paddy_damages:
            try:
                if isinstance(r, dict):
                    r['kind'] = 'paddy'
            except Exception:
                pass
        
        # Query rice damages
        rice_damages = []
        if user_id_param:
            sql = 'SELECT id, user_id, rice_type as paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `rice_damage` WHERE user_id = %s ORDER BY id DESC'
            cur.execute(sql, (str(user_id_param),))
        else:
            sql = 'SELECT id, user_id, rice_type as paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `rice_damage` ORDER BY id DESC LIMIT 200'
            cur.execute(sql)
        rice_damages = cur.fetchall()
        for r in rice_damages:
            try:
                if isinstance(r, dict):
                    r['kind'] = 'rice'
            except Exception:
                pass
        
        # Return based on kind param
        if kind == 'rice':
            results = rice_damages
        elif kind == 'paddy':
            results = paddy_damages
        else:
            # Merge and sort by damage_date or created_at
            all_damages = paddy_damages + rice_damages
            all_damages.sort(key=lambda x: x.get('damage_date') or x.get('created_at') or '', reverse=True)
            results = all_damages

        cur.close()
        conn.close()
        return jsonify(results)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/damages/<int:damage_id>', methods=['GET'])
def api_get_damage(damage_id):
    """Return a single damage record by id. Optional query param `kind` = 'paddy'|'rice' to disambiguate."""
    kind = request.args.get('kind')
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)

        if kind == 'rice':
            cur.execute('SELECT id, user_id, rice_type as paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `rice_damage` WHERE id = %s LIMIT 1', (damage_id,))
            row = cur.fetchone()
            if row:
                row['kind'] = 'rice'
                cur.close()
                conn.close()
                return jsonify(row)

        if kind == 'paddy':
            cur.execute('SELECT id, user_id, paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `damage` WHERE id = %s LIMIT 1', (damage_id,))
            row = cur.fetchone()
            if row:
                row['kind'] = 'paddy'
                cur.close()
                conn.close()
                return jsonify(row)

        # If kind not specified or previous lookup failed, try paddy then rice
        cur.execute('SELECT id, user_id, paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `damage` WHERE id = %s LIMIT 1', (damage_id,))
        row = cur.fetchone()
        if row:
            row['kind'] = 'paddy'
            cur.close()
            conn.close()
            return jsonify(row)

        cur.execute('SELECT id, user_id, rice_type as paddy_type, quantity, reason, damage_date, block_hash, block_number, transaction_hash, created_at, reverted FROM `rice_damage` WHERE id = %s LIMIT 1', (damage_id,))
        row = cur.fetchone()
        if row:
            row['kind'] = 'rice'
            cur.close()
            conn.close()
            return jsonify(row)

        cur.close()
        conn.close()
        return jsonify({'error': 'Damage record not found'}), 404
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/damages/<int:damage_id>', methods=['PUT'])
def api_update_damage(damage_id):
    """Update a damage record (quantity and/or reason). Optional query param `kind` = 'paddy'|'rice' to disambiguate.
    Expects JSON body: { quantity, reason }
    """
    kind = request.args.get('kind')
    try:
        data = request.get_json()
        new_quantity = data.get('quantity')
        new_reason = data.get('reason')
        
        if new_quantity is not None:
            new_quantity = float(new_quantity)
            if new_quantity < 0:
                return jsonify({'ok': False, 'error': 'Invalid quantity'}), 400
        
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Determine which table to update
        table_name = None
        if kind == 'rice':
            table_name = 'rice_damage'
            type_column = 'rice_type'
        elif kind == 'paddy':
            table_name = 'damage'
            type_column = 'paddy_type'
        else:
            # Try to find the record in either table
            cur.execute('SELECT id FROM `damage` WHERE id = %s LIMIT 1', (damage_id,))
            if cur.fetchone():
                table_name = 'damage'
                type_column = 'paddy_type'
            else:
                cur.execute('SELECT id FROM `rice_damage` WHERE id = %s LIMIT 1', (damage_id,))
                if cur.fetchone():
                    table_name = 'rice_damage'
                    type_column = 'rice_type'
                else:
                    cur.close()
                    conn.close()
                    return jsonify({'ok': False, 'error': 'Damage record not found'}), 404
        
        # Build update query
        update_fields = []
        update_values = []
        
        if new_quantity is not None:
            update_fields.append('quantity = %s')
            update_values.append(new_quantity)
        
        if new_reason is not None:
            update_fields.append('reason = %s')
            update_values.append(new_reason)
        
        if not update_fields:
            cur.close()
            conn.close()
            return jsonify({'ok': True, 'message': 'No changes needed'}), 200
        
        update_values.append(damage_id)
        update_sql = f'UPDATE `{table_name}` SET {", ".join(update_fields)} WHERE id = %s'
        cur.execute(update_sql, tuple(update_values))
        
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Damage record not found'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'ok': True, 'message': 'Damage record updated successfully'}), 200
        
    except mysql.connector.Error as err:
        try:
            conn.rollback()
        except:
            pass
        return jsonify({'ok': False, 'error': str(err)}), 500


@app.route('/api/damages/<int:damage_id>/revert', methods=['POST'])
def api_revert_damage(damage_id):
    """Revert a damage record by updating the reverted status and restoring stock."""
    kind = request.args.get('kind')
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Determine which table to use
        table_name = None
        type_column = None
        if kind == 'rice':
            table_name = 'rice_damage'
            type_column = 'rice_type'
        elif kind == 'paddy':
            table_name = 'damage'
            type_column = 'paddy_type'
        else:
            # Try to find the record in either table
            cur.execute('SELECT id FROM `damage` WHERE id = %s LIMIT 1', (damage_id,))
            if cur.fetchone():
                table_name = 'damage'
                type_column = 'paddy_type'
            else:
                cur.execute('SELECT id FROM `rice_damage` WHERE id = %s LIMIT 1', (damage_id,))
                if cur.fetchone():
                    table_name = 'rice_damage'
                    type_column = 'rice_type'
                else:
                    cur.close()
                    conn.close()
                    return jsonify({'ok': False, 'error': 'Damage record not found'}), 404
        
        # Get the damage record
        sql = f'SELECT id, user_id, {type_column} as item_type, quantity, reason, damage_date, reverted FROM `{table_name}` WHERE id = %s'
        cur.execute(sql, (damage_id,))
        damage = cur.fetchone()
        if not damage:
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Damage record not found'}), 404
        
        user_id = damage['user_id']
        item_type = damage['item_type']
        quantity = float(damage['quantity'])
        reason = damage['reason']
        damage_date = damage['damage_date']
        reverted = damage.get('reverted', 0)
        
        # Restore quantity to stock
        if table_name == 'rice_damage':
            # Restore rice to rice_stock
            cur.execute('SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s', (str(user_id), item_type))
            stock_row = cur.fetchone()
            if stock_row:
                new_qty = float(stock_row['quantity']) + quantity
                cur.execute('UPDATE `rice_stock` SET quantity = %s WHERE id = %s', (new_qty, stock_row['id']))
            else:
                cur.execute('INSERT INTO `rice_stock` (miller_id, paddy_type, quantity) VALUES (%s, %s, %s)', (str(user_id), item_type, quantity))
        else:
            # Restore paddy to stock
            cur.execute('SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s', (str(user_id), item_type))
            stock_row = cur.fetchone()
            if stock_row:
                new_amount = float(stock_row['amount']) + quantity
                cur.execute('UPDATE `stock` SET amount = %s WHERE id = %s', (new_amount, stock_row['id']))
            else:
                cur.execute('INSERT INTO `stock` (user_id, `type`, amount) VALUES (%s, %s, %s)', (str(user_id), item_type, quantity))
        
        # Update the reverted status to 1
        update_sql = f'UPDATE `{table_name}` SET reverted = 1 WHERE id = %s'
        cur.execute(update_sql, (damage_id,))
        
        # Insert a new reversal record for tracking purposes
        # Record reversal on blockchain for both rice and paddy damage
        block_hash = None
        block_number = None
        transaction_hash = None
        damage_id_result = None
        
        try:
            # Convert damage_date to timestamp if needed
            from datetime import datetime
            if isinstance(damage_date, str):
                try:
                    dt = datetime.strptime(damage_date, '%Y-%m-%d')
                    date_timestamp = int(dt.timestamp())
                except:
                    date_timestamp = int(datetime.now().timestamp())
            else:
                date_timestamp = int(damage_date.timestamp()) if hasattr(damage_date, 'timestamp') else int(damage_date)
            
            if table_name == 'rice_damage':
                result = record_rice_damage(
                    str(user_id),
                    item_type,
                    int(quantity),
                    date_timestamp,
                    'revert'
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                    damage_id_result = result.get('transaction_id')
                print(f"Blockchain rice damage revert recorded. Block hash: {block_hash}, Damage ID: {damage_id_result}")
            else:
                # For paddy damage, call record_damage
                result = record_damage(
                    str(user_id),
                    item_type,
                    int(quantity),
                    date_timestamp,
                    'revert'
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                    damage_id_result = result.get('damage_id')
                print(f"Blockchain paddy damage revert recorded. Block hash: {block_hash}, Damage ID: {damage_id_result}")
        except Exception as e:
            print(f"Failed to record damage revert on blockchain: {e}")
            # Continue with database insert even if blockchain fails
            block_hash = None
            block_number = None
            transaction_hash = None
            damage_id_result = None
        
        # Insert a new reversal record with reason "revert" and reverted=0
        if table_name == 'rice_damage' and damage_id_result is not None:
            # For rice damage, use blockchain id if available
            insert_sql = f'INSERT INTO `{table_name}` (id, user_id, {type_column}, quantity, reason, damage_date, block_hash, block_number, transaction_hash, reverted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cur.execute(insert_sql, (damage_id_result, str(user_id), item_type, quantity, 'revert', damage_date, block_hash, block_number, transaction_hash, 0))
        elif table_name == 'damage' and damage_id_result is not None:
            # For paddy damage, use blockchain id if available
            insert_sql = f'INSERT INTO `{table_name}` (id, user_id, {type_column}, quantity, reason, damage_date, block_hash, block_number, transaction_hash, reverted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cur.execute(insert_sql, (damage_id_result, str(user_id), item_type, quantity, 'revert', damage_date, block_hash, block_number, transaction_hash, 0))
        else:
            # Fallback: use auto-increment if blockchain id not available
            insert_sql = f'INSERT INTO `{table_name}` (user_id, {type_column}, quantity, reason, damage_date, block_hash, block_number, transaction_hash, reverted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cur.execute(insert_sql, (str(user_id), item_type, quantity, 'revert', damage_date, block_hash, block_number, transaction_hash, 0))
        
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'ok': True, 'message': 'Damage record reverted successfully. New reversal record created.'}), 200
    except mysql.connector.Error as err:
        return jsonify({'ok': False, 'error': str(err)}), 500


@app.route('/api/stock_by_district', methods=['GET'])
def api_get_stock_by_district():
    """Return stock grouped by district for Millers and Collectors.
    Optional query param: paddy_type to filter by specific paddy type.
    If no paddy_type specified, returns breakdown by paddy type.
    
    Response when paddy_type specified: {
        districts: [...district names...],
        collectors: [...stock amounts by district...],
        millers: [...stock amounts by district...]
    }
    
    Response when no paddy_type: {
        districts: [...district names...],
        paddy_types: [...list of paddy types...],
        data: {
            collectors: { paddy_type: [amounts_by_district] },
            millers: { paddy_type: [amounts_by_district] }
        }
    }
    """
    paddy_type = request.args.get('paddy_type', '').strip()
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor()
        
        if paddy_type:
            # Single paddy type selected - simple aggregation
            # Query for collectors stock by district
            collector_sql = '''
                SELECT u.district, SUM(s.amount) as total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                WHERE LOWER(u.user_type) LIKE %s AND s.type = %s
                GROUP BY u.district
                ORDER BY u.district
            '''
            cur.execute(collector_sql, ('%collect%', paddy_type))
            collector_rows = cur.fetchall()
            collector_data = {str(row[0] or 'Unknown'): float(row[1] or 0) for row in collector_rows}
            
            # Query for millers stock by district
            miller_sql = '''
                SELECT u.district, SUM(s.amount) as total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                WHERE LOWER(u.user_type) LIKE %s AND s.type = %s
                GROUP BY u.district
                ORDER BY u.district
            '''
            cur.execute(miller_sql, ('%miller%', paddy_type))
            miller_rows = cur.fetchall()
            miller_data = {str(row[0] or 'Unknown'): float(row[1] or 0) for row in miller_rows}
            
            # Combine all districts
            all_districts = sorted(set(list(collector_data.keys()) + list(miller_data.keys())))
            
            # Build response arrays aligned to districts
            collector_amounts = [collector_data.get(d, 0) for d in all_districts]
            miller_amounts = [miller_data.get(d, 0) for d in all_districts]
            
            cur.close()
            conn.close()
            
            return jsonify({
                'districts': all_districts,
                'collectors': collector_amounts,
                'millers': miller_amounts
            })
        else:
            # No paddy type selected - return breakdown by type
            # Query for collectors stock by district and paddy type
            collector_sql = '''
                SELECT u.district, s.type, SUM(s.amount) as total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                WHERE LOWER(u.user_type) LIKE %s
                GROUP BY u.district, s.type
                ORDER BY u.district, s.type
            '''
            cur.execute(collector_sql, ('%collect%',))
            collector_rows = cur.fetchall()
            
            # Query for millers stock by district and paddy type
            miller_sql = '''
                SELECT u.district, s.type, SUM(s.amount) as total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                WHERE LOWER(u.user_type) LIKE %s
                GROUP BY u.district, s.type
                ORDER BY u.district, s.type
            '''
            cur.execute(miller_sql, ('%miller%',))
            miller_rows = cur.fetchall()
            
            cur.close()
            conn.close()
            
            # Organize data by district and paddy type
            all_districts = set()
            all_paddy_types = set()
            collector_data = {}  # {paddy_type: {district: amount}}
            miller_data = {}     # {paddy_type: {district: amount}}
            
            for row in collector_rows:
                district = str(row[0] or 'Unknown')
                ptype = str(row[1] or 'Unknown')
                amount = float(row[2] or 0)
                all_districts.add(district)
                all_paddy_types.add(ptype)
                if ptype not in collector_data:
                    collector_data[ptype] = {}
                collector_data[ptype][district] = amount
            
            for row in miller_rows:
                district = str(row[0] or 'Unknown')
                ptype = str(row[1] or 'Unknown')
                amount = float(row[2] or 0)
                all_districts.add(district)
                all_paddy_types.add(ptype)
                if ptype not in miller_data:
                    miller_data[ptype] = {}
                miller_data[ptype][district] = amount
            
            all_districts = sorted(all_districts)
            all_paddy_types = sorted(all_paddy_types)
            
            # Build response with arrays aligned to districts for each paddy type
            collector_by_type = {}
            miller_by_type = {}
            
            for ptype in all_paddy_types:
                collector_by_type[ptype] = [collector_data.get(ptype, {}).get(d, 0) for d in all_districts]
                miller_by_type[ptype] = [miller_data.get(ptype, {}).get(d, 0) for d in all_districts]
            
            return jsonify({
                'districts': all_districts,
                'paddy_types': all_paddy_types,
                'data': {
                    'collectors': collector_by_type,
                    'millers': miller_by_type
                }
            })
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/stock_by_user', methods=['GET'])
def api_get_stock_by_user():
    """Return stock totals by user for a given user_type (Collector or Miller).
    Query params:
      - user_type (required): collector|miller
      - paddy_type (optional): filter by paddy type
      - district (optional): filter by district
      - q (optional): search query to match id, full_name or nic (substring, case-insensitive)

    Response: [ { id, full_name, nic, district, total } ] sorted by total DESC
    """
    user_type = (request.args.get('user_type') or '').strip()
    paddy_type = (request.args.get('paddy_type') or '').strip()
    district = (request.args.get('district') or '').strip()
    q = (request.args.get('q') or '').strip()

    if not user_type:
        return jsonify({'error': 'user_type is required (collector or miller)'}), 400

    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)

        params = []
        user_like = f"%{user_type.lower()}%"

        if paddy_type:
            sql = '''
                SELECT u.id, u.full_name, u.nic, u.district, SUM(s.amount) AS total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                WHERE LOWER(u.user_type) LIKE %s AND s.type = %s
            '''
            params = [user_like, paddy_type]
        else:
            sql = '''
                SELECT u.id, u.full_name, u.nic, u.district, SUM(s.amount) AS total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                WHERE LOWER(u.user_type) LIKE %s
            '''
            params = [user_like]

        if district:
            sql += " AND u.district = %s"
            params.append(district)

        if q:
            # match id, full_name or nic
            sql += " AND (u.id LIKE %s OR LOWER(u.full_name) LIKE %s OR LOWER(u.nic) LIKE %s)"
            qparam = f"%{q}%"
            params.extend([qparam, qparam.lower(), qparam.lower()])

        sql += ' GROUP BY u.id, u.full_name, u.nic, u.district ORDER BY total DESC'

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        out = []
        for r in rows:
            out.append({
                'id': r.get('id'),
                'full_name': r.get('full_name'),
                'nic': r.get('nic'),
                'district': r.get('district'),
                'total': float(r.get('total') or 0)
            })
        return jsonify(out)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/stock_by_user_type', methods=['GET'])
def api_get_stock_by_user_type():
    """Return stock grouped by user_type (Miller, Collecter, PMB) and paddy type.
    Optional query param: paddy_type to filter by specific paddy type.
    
    Response: {
        paddy_types: [...],
        data: {
            MILLER: { paddy_type: amount, ... },
            COLLECTER: { paddy_type: amount, ... },
            PMB: { paddy_type: amount, ... }
        }
    }
    """
    paddy_type = request.args.get('paddy_type', '').strip()
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor()
        
        # Query for stock grouped by user_type and paddy_type
        if paddy_type:
            sql = '''
                SELECT UPPER(u.user_type) as user_type, s.type as paddy_type, SUM(s.amount) as total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                WHERE s.type = %s
                GROUP BY UPPER(u.user_type), s.type
                ORDER BY UPPER(u.user_type), s.type
            '''
            cur.execute(sql, (paddy_type,))
        else:
            sql = '''
                SELECT UPPER(u.user_type) as user_type, s.type as paddy_type, SUM(s.amount) as total
                FROM stock s
                JOIN users u ON s.user_id = u.id
                GROUP BY UPPER(u.user_type), s.type
                ORDER BY UPPER(u.user_type), s.type
            '''
            cur.execute(sql)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # Organize data
        all_paddy_types = set()
        user_type_data = {}  # {user_type: {paddy_type: amount}}
        
        for row in rows:
            user_type = str(row[0] or 'UNKNOWN')
            ptype = str(row[1] or 'Unknown')
            amount = float(row[2] or 0)
            
            all_paddy_types.add(ptype)
            if user_type not in user_type_data:
                user_type_data[user_type] = {}
            user_type_data[user_type][ptype] = amount
        
        # Ensure all user types are present
        for ut in ['MILLER', 'COLLECTER', 'PMB']:
            if ut not in user_type_data:
                user_type_data[ut] = {}
        
        all_paddy_types = sorted(all_paddy_types)
        
        return jsonify({
            'paddy_types': all_paddy_types,
            'data': user_type_data
        })
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/farmer_lookup', methods=['GET'])
def api_farmer_lookup():
    """Get farmer contribution details with transactions grouped by paddy type and destination.
    Query params:
      - farmer_id (required): The farmer's user ID
      - date_from (optional): Start date filter (YYYY-MM-DD)
      - date_to (optional): End date filter (YYYY-MM-DD)
    
    Response: {
        farmer: { id, full_name, field_area },
        summary: { total_paddy, to_collector, to_miller, to_pmb, transaction_count },
        breakdown: [{ paddy_type, to_collector, to_miller, to_pmb, total }],
        transactions: [{ date, paddy_type, to_party, to_party_type, quantity }]
    }
    """
    print("=== Farmer Lookup API Called ===")
    farmer_id = request.args.get('farmer_id', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    
    print(f"Farmer ID: {farmer_id}")
    print(f"Date From: {date_from}")
    print(f"Date To: {date_to}")
    
    if not farmer_id:
        print("ERROR: farmer_id is required")
        return jsonify({'error': 'farmer_id is required'}), 400
    
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Get farmer details
        print(f"Querying farmer with ID: {farmer_id}")
        cur.execute('SELECT id, full_name, total_area_of_paddy_land FROM users WHERE id = %s LIMIT 1', (farmer_id,))
        farmer = cur.fetchone()
        print(f"Farmer found: {farmer}")
        
        if not farmer:
            cur.close()
            conn.close()
            print("ERROR: Farmer not found")
            return jsonify({'error': 'Farmer not found'}), 404
        
        # Build query for transactions where farmer is sender (include all, even reverted)
        sql = '''
            SELECT t.id, t.`from`, t.`to`, t.`type` as paddy_type, t.quantity, t.price, t.status, t.is_reverted, 
                   t.`datetime`, t.created_at, t.block_hash, t.block_number, t.transaction_hash,
                   u.user_type as to_user_type, u.full_name as to_full_name, u.company_name as to_company_name
            FROM `transaction` t
            LEFT JOIN users u ON t.`to` = u.id
            WHERE t.`from` = %s
        '''
        params = [farmer_id]
        
        if date_from:
            sql += ' AND DATE(t.`datetime`) >= %s'
            params.append(date_from)
        if date_to:
            sql += ' AND DATE(t.`datetime`) <= %s'
            params.append(date_to)
        
        sql += ' ORDER BY t.`datetime` DESC'
        
        print(f"Executing SQL: {sql}")
        print(f"With params: {params}")
        
        cur.execute(sql, tuple(params))
        transactions = cur.fetchall()
        
        print(f"Found {len(transactions)} transactions")
        
        cur.close()
        conn.close()
        
        # Process data
        breakdown_data = {}  # {paddy_type: {to_collector, to_miller, to_pmb, total}}
        transaction_list = []
        
        total_paddy = 0
        to_collector = 0
        to_miller = 0
        to_pmb = 0
        
        for tx in transactions:
            paddy_type = tx['paddy_type'] if tx['paddy_type'] else None
            if not paddy_type:  # Skip transactions without paddy type
                print(f"Skipping transaction {tx['id']} - no paddy type")
                continue
                
            qty = float(tx['quantity']) if tx['quantity'] else 0
            to_user_type = tx['to_user_type'].upper() if tx['to_user_type'] else 'UNKNOWN'
            to_party_name = tx['to_company_name'] if tx['to_company_name'] else (tx['to_full_name'] if tx['to_full_name'] else tx['to'])
            
            # Check if transaction is reverted - only check status = 0
            is_reverted = tx.get('status', 1) == 0
            
            print(f"Processing tx {tx['id']}: {paddy_type}, {qty} kg to {to_user_type}, status={tx.get('status')}, reverted={is_reverted}")
            
            # Initialize breakdown for this paddy type if needed
            if paddy_type not in breakdown_data:
                breakdown_data[paddy_type] = {
                    'to_collector': 0,
                    'to_miller': 0,
                    'to_pmb': 0,
                    'total': 0
                }
            
            # Determine if we add or subtract based on revert status
            multiplier = -1 if is_reverted else 1
            adjusted_qty = qty * multiplier
            
            # Accumulate by destination type (add if active, subtract if reverted)
            if to_user_type == 'COLLECTER':
                breakdown_data[paddy_type]['to_collector'] += adjusted_qty
                to_collector += adjusted_qty
            elif to_user_type == 'MILLER':
                breakdown_data[paddy_type]['to_miller'] += adjusted_qty
                to_miller += adjusted_qty
            elif to_user_type == 'PMB':
                breakdown_data[paddy_type]['to_pmb'] += adjusted_qty
                to_pmb += adjusted_qty
            
            breakdown_data[paddy_type]['total'] += adjusted_qty
            total_paddy += adjusted_qty
            
            # Add ALL transactions to the transaction list (including reverted)
            tx_date = tx['datetime'] if tx['datetime'] else tx['created_at']
            transaction_list.append({
                'date': tx_date.isoformat() if tx_date else '',
                'paddy_type': paddy_type,
                'to_party': to_party_name,
                'to_party_id': tx['to'],
                'to_party_type': to_user_type,
                'quantity': qty,
                'price': float(tx['price']) if tx['price'] else 0,
                'block_hash': tx['block_hash'] or '',
                'block_number': tx['block_number'] or '',
                'transaction_hash': tx['transaction_hash'] or '',
                'is_reverted': is_reverted
            })
        
        # Convert breakdown to list
        breakdown_list = []
        for paddy_type, data in breakdown_data.items():
            breakdown_list.append({
                'paddy_type': paddy_type,
                'to_collector': data['to_collector'],
                'to_miller': data['to_miller'],
                'to_pmb': data['to_pmb'],
                'total': data['total']
            })
        
        # Sort breakdown by paddy type
        breakdown_list.sort(key=lambda x: x['paddy_type'])
        
        result = {
            'farmer': {
                'id': farmer['id'],
                'full_name': farmer['full_name'],
                'field_area': float(farmer['total_area_of_paddy_land']) if farmer.get('total_area_of_paddy_land') else 0
            },
            'summary': {
                'total_paddy': total_paddy,
                'to_collector': to_collector,
                'to_miller': to_miller,
                'to_pmb': to_pmb,
                'transaction_count': len(transaction_list)
            },
            'breakdown': breakdown_list,
            'transactions': transaction_list
        }
        
        print(f"Returning result with {len(breakdown_list)} paddy types and {len(transaction_list)} transactions")
        return jsonify(result)
        
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return jsonify({'error': str(err)}), 500
    except Exception as e:
        print(f"General Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/damage_lookup', methods=['GET'])
def api_damage_lookup():
    """Retrieve damage records with optional filtering by user_type, paddy_type, and date range.
    Query params:
    - user_type: COLLECTER, MILLER, PMB (optional)
    - paddy_type: Filter by paddy type (optional)
    - date_from: Start date (optional)
    - date_to: End date (optional)
    
    Returns: List of damage records sorted by quantity (ascending)
    """
    try:
        user_type = request.args.get('user_type', '').strip()
        paddy_type = request.args.get('paddy_type', '').strip()
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        
        print(f"=== Damage Lookup Debug ===")
        print(f"User Type: {user_type}")
        print(f"Paddy Type: {paddy_type}")
        print(f"Date From: {date_from}")
        print(f"Date To: {date_to}")
        
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor(dictionary=True)
        
        # Build query
        query = '''
            SELECT 
                d.id,
                d.user_id,
                d.paddy_type,
                d.quantity,
                d.reason,
                d.damage_date,
                d.block_hash,
                d.block_number,
                d.transaction_hash,
                u.full_name as user_name,
                u.user_type
            FROM damage d
            LEFT JOIN users u ON d.user_id = u.id
            WHERE 1=1
        '''
        params = []
        
        # Add filters
        if user_type:
            query += ' AND u.user_type = %s'
            params.append(user_type)
        
        if paddy_type:
            query += ' AND d.paddy_type = %s'
            params.append(paddy_type)
        
        if date_from:
            query += ' AND DATE(d.damage_date) >= %s'
            params.append(date_from)
        
        if date_to:
            query += ' AND DATE(d.damage_date) <= %s'
            params.append(date_to)
        
        query += ' ORDER BY d.quantity ASC'
        
        print(f"Query: {query}")
        print(f"Params: {params}")
        
        cursor.execute(query, params)
        damage_records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        print(f"Found {len(damage_records)} damage records")
        
        # Process results
        result = []
        for dmg in damage_records:
            result.append({
                'id': dmg.get('id'),
                'user_id': dmg.get('user_id'),
                'user_name': dmg.get('user_name'),
                'user_type': dmg.get('user_type'),
                'paddy_type': dmg.get('paddy_type'),
                'quantity': float(dmg.get('quantity', 0)),
                'reason': dmg.get('reason') or '',
                'damage_date': dmg.get('damage_date').isoformat() if dmg.get('damage_date') else None,
                'block_hash': dmg.get('block_hash') or '',
                'block_number': dmg.get('block_number'),
                'transaction_hash': dmg.get('transaction_hash') or ''
            })
        
        return jsonify(result)
        
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return jsonify({'error': str(err)}), 500
    except Exception as e:
        print(f"General Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/stock_user_detail', methods=['GET'])
def api_get_stock_user_detail():
    """Return per-paddy-type stock for a given user_id.
    Query param: user_id
    Response: [ { type, amount } ]
    """
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        cur.execute('SELECT `type`, amount FROM stock WHERE user_id = %s ORDER BY `type`', (str(user_id),))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        out = []
        for r in rows:
            out.append({'type': r.get('type'), 'amount': float(r.get('amount') or 0)})
        return jsonify(out)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/users', methods=['POST'])
def api_add_user():
    payload = request.get_json() or {}
    user_type = payload.get('userType')
    # map fields safely and normalize to single address and full_name
    nic = payload.get('nic')
    full_name = payload.get('fullName')
    company_register_number = payload.get('companyRegisterNumber')
    company_name = payload.get('companyName')
    # accept either a single 'address' or the older per-type fields and coalesce
    address = payload.get('address') or payload.get('homeAddress') or payload.get('collectorAddress') or payload.get('millerAddress') or ''
    district = payload.get('district')
    contact_number = payload.get('contactNumber')
    total_area = payload.get('totalAreaOfPaddyLand')
    id = (log_last_inserted_user(user_type))

    # If creating a PMB account, enforce single-account rule and fixed id
    try:
        if isinstance(user_type, str) and user_type.strip().lower().startswith('pmb'):
            conn_chk = get_connection(MYSQL_DATABASE)
            cur_chk = conn_chk.cursor()
            # check if any existing PMB user exists (by user_type or id)
            cur_chk.execute("SELECT id FROM users WHERE LOWER(user_type) = %s OR LOWER(id) = %s LIMIT 1", ('pmb', 'pmb'))
            existing = cur_chk.fetchone()
            cur_chk.close()
            conn_chk.close()
            if existing:
                return jsonify({'ok': False, 'error': 'PMB account already exists'}), 400
            # set the id explicitly to 'PMB'
            id = 'PMB'
    except Exception:
        # if check fails for some reason, continue and let insert raise if needed
        try:
            cur_chk.close()
        except Exception:
            pass
        try:
            conn_chk.close()
        except Exception:
            pass
   
    try:
        block_hash = None
        block_number = None
        transaction_hash = None
        if(user_type=="Farmer"):
            try:
                result = add_farmer(
                    id,
                    full_name,
                   
                    district,
               
                    123,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_farmer call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_farmer raised an exception:", e)
        elif(user_type=="Miller"):
            try:
                result = add_miller(
                    id,
                    company_register_number,
                    company_name,
                    address,
                    district,
                    contact_number,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_miller call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_miller raised an exception:", e)
        elif(user_type=="Collecter"):
            try:
                result = add_collector(
                    id,
                    full_name,
                    address,
                    district,
                    contact_number,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_collector call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_collector raised an exception:", e)
        elif(user_type=="Wholesaler"):
            try:
                result = add_wholesaler(
                    id,
                    company_register_number,
                    company_name,
                    address,
                    district,
                    contact_number,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_wholesaler call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_wholesaler raised an exception:", e)
        elif(user_type=="Retailer"):
            try:
                result = add_retailer(
                    id,
                    full_name or company_name,
                    address,
                    district,
                    contact_number,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_retailer call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_retailer raised an exception:", e)
        elif(user_type=="Beer"):
            try:
                result = add_brewer(
                    id,
                    company_register_number or "",
                    company_name or full_name,
                    address,
                    district,
                    contact_number,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_brewer call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_brewer raised an exception:", e)
        elif(user_type=="Animal Food"):
            try:
                result = add_animal_food(
                    id,
                    company_register_number or "",
                    company_name or full_name,
                    address,
                    district,
                    contact_number,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_animal_food call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_animal_food raised an exception:", e)
        elif(user_type=="Exporter"):
            try:
                result = add_exporter(
                    id,
                    company_register_number or "",
                    company_name or full_name,
                    address,
                    district,
                    contact_number,
                    0.0,
                )
                if result and isinstance(result, dict):
                    block_hash = result.get('block_hash')
                    block_number = result.get('block_number')
                    transaction_hash = result.get('transaction_hash')
                print("add_exporter call finished. Block hash:", block_hash)
            except Exception as e:
                print("add_exporter raised an exception:", e)
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        insert_sql = '''
            INSERT INTO users (user_type, nic, full_name, company_register_number, company_name, address, district, contact_number, total_area_of_paddy_land, id, password, block_hash, block_number, transaction_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        cursor.execute(insert_sql, (user_type, nic, full_name, company_register_number, company_name, address, district, contact_number, total_area, id, "123456", block_hash, block_number, transaction_hash))
        # Try to get the inserted id reliably. Prefer cursor.lastrowid, but fall back to LAST_INSERT_ID().
        last_id = cursor.lastrowid
        if not last_id:
            try:
                cursor.execute('SELECT LAST_INSERT_ID()')
                last_id_row = cursor.fetchone()
                if last_id_row:
                    # fetchone() returns a tuple like (id,)
                    last_id = last_id_row[0]
            except Exception:
                last_id = None

        # Ensure the insert is committed
        try:
            conn.commit()
        except Exception:
            pass

        # compute a user_code for the response (do not persist)
        prefix_map = {
            'Farmer': 'FAR',
            'Collecter': 'COL',
            'Miller': 'MIL',
            'Wholesaler': 'WHO',
            'Retailer': 'RET',
            'Beer': 'BER',
            'Animal Food': 'ANI',
            'Exporter': 'EXP'
        }

        cursor.close()

    # If the client provided initial stock (paddy types + quantities), insert them
        # Use the application id we attempted to insert (variable `id`) if present,
        # otherwise fall back to the numeric last_id returned by the connector.
        created_user_id = id or last_id
        try:
            stock_items = payload.get('stock') if isinstance(payload, dict) else None
        except Exception:
            stock_items = None
        # Do not insert initial stock for Farmers or PMB
        is_no_stock = False
        try:
            is_no_stock = isinstance(user_type, str) and (user_type.strip().lower().startswith('farmer') or user_type.strip().lower().startswith('pmb'))
        except Exception:
            is_no_stock = False

        if stock_items and created_user_id and not is_no_stock:
            try:
                s_cur = conn.cursor()
                for si in stock_items:
                    # accept either {paddyType, quantity} or {type, quantity}
                    ptype = si.get('paddyType') if isinstance(si, dict) else None
                    if not ptype:
                        ptype = si.get('type') if isinstance(si, dict) else None
                    qty = None
                    try:
                        qty = float(si.get('quantity')) if isinstance(si, dict) and si.get('quantity') is not None else None
                    except Exception:
                        qty = None
                    if ptype and qty is not None:
                        try:
                            s_cur.execute('INSERT INTO `stock` (user_id, `type`, amount) VALUES (%s, %s, %s)', (str(created_user_id), ptype, qty))
                        except Exception as _:
                            # ignore individual stock insert failures but continue
                            pass
                try:
                    conn.commit()
                except Exception:
                    pass
                s_cur.close()
            except Exception:
                # ignore stock insertion errors to avoid blocking user creation
                pass

        # For Collectors and Millers, also save paddy stock to initial_paddy table
        if stock_items and created_user_id and isinstance(user_type, str):
            user_type_lower = user_type.strip().lower()
            if user_type_lower == 'collecter' or user_type_lower == 'miller':
                try:
                    ip_cur = conn.cursor()
                    for si in stock_items:
                        if isinstance(si, dict):
                            ptype = si.get('paddyType') if si.get('paddyType') else si.get('type')
                            qty = None
                            try:
                                qty = float(si.get('quantity')) if si.get('quantity') is not None else None
                            except Exception:
                                pass
                            
                            if ptype and qty is not None and qty > 0:
                                try:
                                    ip_cur.execute('INSERT INTO `initial_paddy` (user_id, paddy_type, quantity) VALUES (%s, %s, %s)', (str(created_user_id), ptype, qty))
                                except Exception as e:
                                    print(f"Failed to insert initial paddy: {e}")
                                    pass
                    
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    ip_cur.close()
                except Exception as e:
                    print(f"Failed to insert initial paddy from stock: {e}")
                    pass

        # If the client provided initial rice stock (for Miller), insert them into rice_stock table
        try:
            rice_items = payload.get('riceStock') if isinstance(payload, dict) else None
        except Exception:
            rice_items = None

        if rice_items and created_user_id:
            try:
                r_cur = conn.cursor()
                for ri in rice_items:
                    ptype = ri.get('paddyType') if isinstance(ri, dict) else None
                    qty = None
                    try:
                        qty = float(ri.get('quantity')) if isinstance(ri, dict) and ri.get('quantity') is not None else None
                    except Exception:
                        qty = None
                    if ptype and qty is not None:
                        try:
                            r_cur.execute('INSERT INTO `rice_stock` (miller_id, paddy_type, quantity) VALUES (%s, %s, %s)', (str(created_user_id), ptype, qty))
                        except Exception as _:
                            # ignore individual rice stock insert failures but continue
                            pass
                try:
                    conn.commit()
                except Exception:
                    pass
                r_cur.close()
            except Exception:
                # ignore rice stock insertion errors to avoid blocking user creation
                pass

        # For Millers and Wholesalers, also save rice stock to rice table
        if rice_items and created_user_id and isinstance(user_type, str):
            user_type_lower = user_type.strip().lower()
            if user_type_lower == 'miller' or user_type_lower == 'wholesaler':
                try:
                    rice_cur = conn.cursor()
                    for ri in rice_items:
                        if isinstance(ri, dict):
                            rice_type = ri.get('paddyType')
                            qty = None
                            try:
                                qty = float(ri.get('quantity')) if ri.get('quantity') is not None else None
                            except Exception:
                                pass
                            
                            if rice_type and qty is not None and qty > 0:
                                try:
                                    rice_cur.execute('INSERT INTO `rice` (user_id, rice_type, quantity) VALUES (%s, %s, %s)', (str(created_user_id), rice_type, qty))
                                except Exception as e:
                                    print(f"Failed to insert rice: {e}")
                                    pass
                    
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    rice_cur.close()
                except Exception as e:
                    print(f"Failed to insert rice to rice table: {e}")
                    pass

        # If the user is a Collecter or Miller and initial paddy is provided, save it to initial_paddy table
        try:
            initial_paddy = payload.get('initialPaddy') if isinstance(payload, dict) else None
        except Exception:
            initial_paddy = None

        if initial_paddy and created_user_id and isinstance(user_type, str):
            user_type_lower = user_type.strip().lower()
            if user_type_lower == 'collecter' or user_type_lower == 'miller':
                try:
                    initial_paddy_value = float(initial_paddy)
                    ip_cur = conn.cursor()
                    ip_cur.execute('INSERT INTO `initial_paddy` (user_id, initial_paddy) VALUES (%s, %s)', (str(created_user_id), initial_paddy_value))
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    ip_cur.close()
                except Exception as e:
                    # ignore initial paddy insertion errors to avoid blocking user creation
                    print(f"Failed to insert initial paddy: {e}")
                    pass

        # return the inserted row with a computed user_code
        rc = conn.cursor(dictionary=True)
        row = None
        if last_id:
            rc.execute('SELECT * FROM users WHERE id = %s', (last_id,))
            row = rc.fetchone()
            if row is not None:
                try:
                    prefix = prefix_map.get(user_type, 'USR')
                    row['user_code'] = f"{prefix}{int(last_id):06d}"
                except Exception:
                    row['user_code'] = None
        else:
            # As a safe fallback, try to return the most recent row matching some of the unique fields
            try:
                rc.execute('SELECT * FROM users WHERE user_type = %s ORDER BY id DESC LIMIT 1', (user_type,))
                row = rc.fetchone()
                if row and row.get('id') is not None:
                    try:
                        prefix = prefix_map.get(row.get('user_type'), 'USR')
                        row['user_code'] = f"{prefix}{int(row.get('id')):06d}"
                    except Exception:
                        row['user_code'] = None
            except Exception:
                row = None

        rc.close()
        conn.close()
        # Return the created user row (blockchain integration removed)
        if row is None:
            row = {}
        return jsonify(row), 201
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/milling', methods=['POST'])
def api_add_milling():
    """Insert a milling record into the milling table.
    Expects JSON body: { miller_id, paddy_type, input_paddy, output_rice, milling_date, drying_duration, status }
    """
    payload = request.get_json() or {}
    miller_id = payload.get('miller_id')
    paddy_type = payload.get('paddy_type')
    input_paddy = payload.get('input_paddy')
    output_rice = payload.get('output_rice')
    milling_date = payload.get('milling_date')
    drying_duration = payload.get('drying_duration') or payload.get('milling_duration')
    status = 1  # Always set status to 1 (completed)

    # basic validation
    if not miller_id or not paddy_type or input_paddy is None or output_rice is None:
        return jsonify({'ok': False, 'error': 'Missing required fields (miller_id, paddy_type, input_paddy, output_rice)'}), 400

    try:
        input_qty = float(input_paddy)
        output_qty = float(output_rice)
        if output_qty > input_qty:
            return jsonify({'ok': False, 'error': 'Output rice cannot exceed input paddy'}), 400
    except Exception:
        return jsonify({'ok': False, 'error': 'Invalid quantity values'}), 400

    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # 1. Validate miller has enough stock
        cur.execute('SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s', (str(miller_id), paddy_type))
        stock_row = cur.fetchone()
        
        if not stock_row:
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': f'No stock found for paddy type: {paddy_type}'}), 400
        
        current_stock = float(stock_row['amount'])
        if current_stock < input_qty:
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': f'Insufficient stock. Available: {current_stock} kg, Required: {input_qty} kg'}), 400
        
        # 2. Record milling on blockchain and insert milling record
        block_hash = None
        milling_id_blockchain = None
        try:
            # Record milling on blockchain using dedicated milling function
            qty_int = int(float(input_qty))
            output_int = int(float(output_qty))
            
            # Convert milling_date to timestamp if it's a date string
            if milling_date:
                from datetime import datetime
                try:
                    dt = datetime.strptime(milling_date, '%Y-%m-%d')
                    date_timestamp = int(dt.timestamp())
                except:
                    date_timestamp = int(datetime.now().timestamp())
            else:
                from datetime import datetime
                date_timestamp = int(datetime.now().timestamp())
            
            block_hash = None
            block_number = None
            transaction_hash = None
            result = record_milling(
                str(miller_id),
                paddy_type,
                qty_int,
                output_int,
                date_timestamp,
                drying_duration or 0
            )
            if result and isinstance(result, dict):
                block_hash = result.get('block_hash')
                block_number = result.get('block_number')
                transaction_hash = result.get('transaction_hash')
                milling_id_blockchain = result.get('milling_id')
            print(f"Blockchain milling recorded. Block hash: {block_hash}, Milling ID: {milling_id_blockchain}")
        except Exception as e:
            print(f"Failed to record milling on blockchain: {e}")
            # Continue with database insert even if blockchain fails
            block_hash = None
            block_number = None
            transaction_hash = None
            milling_id_blockchain = None
        
        insert_sql = 'INSERT INTO `milling` (id, miller_id, paddy_type, input_paddy, output_rice, milling_date, drying_duration, status, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        cur.execute(insert_sql, (milling_id_blockchain, str(miller_id), paddy_type, input_qty, output_qty, milling_date, drying_duration or 0, status, block_hash, block_number, transaction_hash))
        last_id = cur.lastrowid if milling_id_blockchain is None else milling_id_blockchain
        
        # 3. Deduct input_paddy from stock table
        new_amount = current_stock - input_qty
        cur.execute('UPDATE `stock` SET amount = %s WHERE id = %s', (new_amount, stock_row['id']))
        
        # 3. Add output_rice to rice_stock table
        # Check if rice_stock record exists for this miller and paddy type
        cur.execute('SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s', (str(miller_id), paddy_type))
        rice_row = cur.fetchone()
        if rice_row:
            new_rice_qty = float(rice_row['quantity']) + output_qty
            cur.execute('UPDATE `rice_stock` SET quantity = %s WHERE id = %s', (new_rice_qty, rice_row['id']))
        else:
            cur.execute('INSERT INTO `rice_stock` (miller_id, paddy_type, quantity) VALUES (%s, %s, %s)', (str(miller_id), paddy_type, output_qty))
        
        try:
            conn.commit()
        except Exception:
            pass
        
        cur.close()
        conn.close()
        return jsonify({'ok': True, 'id': last_id, 'block_hash': block_hash}), 201
    except mysql.connector.Error as err:
        return jsonify({'ok': False, 'error': str(err)}), 500


@app.route('/api/milling', methods=['GET'])
def api_get_milling():
    """Return milling records with optional filters for miller_id, paddy_type, from_date, and to_date.
    Query params: miller_id, paddy_type, from_date (YYYY-MM-DD), to_date (YYYY-MM-DD)
    """
    miller_id_param = request.args.get('miller_id')
    paddy_type_param = request.args.get('paddy_type')
    from_date_param = request.args.get('from_date')
    to_date_param = request.args.get('to_date')
    
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Build dynamic SQL query with filters
        sql = 'SELECT id, miller_id, paddy_type, input_paddy, output_rice, milling_date, drying_duration, status, block_hash, block_number, transaction_hash, created_at FROM `milling` WHERE 1=1'
        params = []
        
        # Add miller_id filter (check both miller_id and users.full_name for flexibility)
        if miller_id_param:
            sql += ' AND (miller_id = %s OR miller_id IN (SELECT id FROM users WHERE full_name LIKE %s OR id LIKE %s))'
            params.extend([str(miller_id_param), f'%{miller_id_param}%', f'%{miller_id_param}%'])
        
        # Add paddy_type filter
        if paddy_type_param:
            sql += ' AND paddy_type = %s'
            params.append(str(paddy_type_param))
        
        # Add date range filters
        if from_date_param:
            sql += ' AND milling_date >= %s'
            params.append(str(from_date_param))
        
        if to_date_param:
            sql += ' AND milling_date <= %s'
            params.append(str(to_date_param))
        
        sql += ' ORDER BY milling_date DESC, id DESC LIMIT 500'
        
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        
        rows = cur.fetchall()
        
        # Enrich rows with miller name from users table
        if rows:
            for row in rows:
                miller_id = row['miller_id']
                cur.execute('SELECT full_name FROM users WHERE id = %s', (str(miller_id),))
                user_row = cur.fetchone()
                row['miller_name'] = user_row['full_name'] if user_row else 'Unknown'
        
        cur.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/milling/<int:milling_id>', methods=['GET'])
def api_get_single_milling(milling_id):
    """Get a single milling record by ID."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        sql = 'SELECT id, miller_id, paddy_type, input_paddy, output_rice, milling_date, drying_duration, status, block_hash, block_number, transaction_hash, created_at FROM `milling` WHERE id = %s'
        cur.execute(sql, (milling_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return jsonify({'error': 'Milling record not found'}), 404
        return jsonify(row)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/milling/<int:milling_id>/revert', methods=['POST'])
def api_revert_milling(milling_id):
    """Revert a milling record by inserting a new reversal record with status = 0 on both DB and blockchain."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Get the milling record
        cur.execute('SELECT miller_id, paddy_type, input_paddy, output_rice, milling_date, drying_duration FROM `milling` WHERE id = %s', (milling_id,))
        milling = cur.fetchone()
        if not milling:
            cur.close()
            conn.close()
            return jsonify({'ok': False, 'error': 'Milling record not found'}), 404
        
        miller_id = milling['miller_id']
        paddy_type = milling['paddy_type']
        input_qty = float(milling['input_paddy'])
        output_qty = float(milling['output_rice'])
        milling_date = milling['milling_date']
        drying_duration = milling['drying_duration'] or 0
        
        # Restore paddy to stock
        cur.execute('SELECT id, amount FROM `stock` WHERE user_id = %s AND `type` = %s', (str(miller_id), paddy_type))
        stock_row = cur.fetchone()
        if stock_row:
            new_amount = float(stock_row['amount']) + input_qty
            cur.execute('UPDATE `stock` SET amount = %s WHERE id = %s', (new_amount, stock_row['id']))
        else:
            cur.execute('INSERT INTO `stock` (user_id, `type`, amount) VALUES (%s, %s, %s)', (str(miller_id), paddy_type, input_qty))
        
        # Remove rice from rice_stock
        cur.execute('SELECT id, quantity FROM `rice_stock` WHERE miller_id = %s AND paddy_type = %s', (str(miller_id), paddy_type))
        rice_row = cur.fetchone()
        if rice_row:
            new_qty = float(rice_row['quantity']) - output_qty
            if new_qty <= 0:
                cur.execute('DELETE FROM `rice_stock` WHERE id = %s', (rice_row['id'],))
            else:
                cur.execute('UPDATE `rice_stock` SET quantity = %s WHERE id = %s', (new_qty, rice_row['id']))
        
        # Record reversal on blockchain with status = 0 (False)
        milling_id_blockchain = None
        block_hash = None
        block_number = None
        transaction_hash = None
        try:
            if milling_date:
                from datetime import datetime
                try:
                    dt = datetime.strptime(str(milling_date), '%Y-%m-%d')
                    date_timestamp = int(dt.timestamp())
                except:
                    date_timestamp = int(datetime.now().timestamp())
            else:
                from datetime import datetime
                date_timestamp = int(datetime.now().timestamp())
            
            result = record_milling(
                str(miller_id),
                paddy_type,
                int(float(input_qty)),
                int(float(output_qty)),
                date_timestamp,
                drying_duration,
                status_flag=False  # Reversal record with status = 0
            )
            if result and isinstance(result, dict):
                block_hash = result.get('block_hash')
                block_number = result.get('block_number')
                transaction_hash = result.get('transaction_hash')
                milling_id_blockchain = result.get('milling_id')
            print(f"Blockchain reversal recorded. Block hash: {block_hash}, Milling ID: {milling_id_blockchain}")
        except Exception as e:
            print(f"Failed to record reversal on blockchain: {e}")
            # Continue with database insert even if blockchain fails
        
        # Insert a new reversal record with status = 0 and blockchain details
        insert_sql = 'INSERT INTO `milling` (id, miller_id, paddy_type, input_paddy, output_rice, milling_date, drying_duration, status, block_hash, block_number, transaction_hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        cur.execute(insert_sql, (milling_id_blockchain, str(miller_id), paddy_type, input_qty, output_qty, milling_date, drying_duration, 0, block_hash, block_number, transaction_hash))
        
        try:
            conn.commit()
        except Exception:
            pass
        
        cur.close()
        conn.close()
        return jsonify({'ok': True, 'message': 'Milling record reverted successfully. New reversal record created on blockchain and database.'}), 200
    except mysql.connector.Error as err:
        return jsonify({'ok': False, 'error': str(err)}), 500


@app.route('/api/users/<user_id>', methods=['PUT'])
def api_update_user(user_id):
    payload = request.get_json() or {}
    
    # Extract fields from payload
    full_name = payload.get('fullName')
    company_register_number = payload.get('companyRegisterNumber')
    company_name = payload.get('companyName')
    address = payload.get('address') or ''
    district = payload.get('district')
    contact_number = payload.get('contactNumber')
    total_area = payload.get('totalAreaOfPaddyLand')
    
    try:
        conn = get_connection(MYSQL_DATABASE)
        cursor = conn.cursor()
        
        # Build UPDATE query with only non-None fields
        update_fields = []
        update_values = []
        
        if full_name is not None:
            update_fields.append('full_name = %s')
            update_values.append(full_name)
        if company_register_number is not None:
            update_fields.append('company_register_number = %s')
            update_values.append(company_register_number)
        if company_name is not None:
            update_fields.append('company_name = %s')
            update_values.append(company_name)
        if address is not None:
            update_fields.append('address = %s')
            update_values.append(address)
        if district is not None:
            update_fields.append('district = %s')
            update_values.append(district)
        if contact_number is not None:
            update_fields.append('contact_number = %s')
            update_values.append(contact_number)
        if total_area is not None:
            update_fields.append('total_area_of_paddy_land = %s')
            update_values.append(total_area)
        
        if not update_fields:
            cursor.close()
            conn.close()
            return jsonify({'error': 'No fields to update'}), 400
        
        # Fetch existing user to prepare on-chain update (we need current fields when payload omits them)
        cur_fetch = conn.cursor(dictionary=True)
        cur_fetch.execute('SELECT * FROM users WHERE id = %s LIMIT 1', (user_id,))
        existing = cur_fetch.fetchone()
        cur_fetch.close()
        if not existing:
            cursor.close()
            conn.close()
            return jsonify({'error': 'User not found'}), 404

        # If no fields to update provided, nothing to do
        if not update_fields:
            cursor.close()
            conn.close()
            return jsonify({'error': 'No fields to update'}), 400

        # Build merged values for blockchain call (payload overrides existing)
        merged_full_name = full_name if full_name is not None else (existing.get('full_name') or '')
        merged_company_reg = company_register_number if company_register_number is not None else (existing.get('company_register_number') or '')
        merged_company_name = company_name if company_name is not None else (existing.get('company_name') or '')
        merged_address = address if address is not None else (existing.get('address') or '')
        merged_district = district if district is not None else (existing.get('district') or '')
        merged_contact = contact_number if contact_number is not None else (existing.get('contact_number') or '')
        merged_total = total_area if total_area is not None else (existing.get('total_area_of_paddy_land') or 0)

        # Attempt on-chain update first; if it fails, do not update DB
        try:
            block_hash = None
            block_number = None
            transaction_hash = None
            res = None
            user_type_db = (existing.get('user_type') or '').strip().lower()
            if user_type_db.startswith('farmer'):
                # ensure integer for total area
                try:
                    total_int = int(float(merged_total))
                except Exception:
                    total_int = 0
                res = update_farmer(str(user_id), merged_full_name, merged_district, total_int, 0.0)
            elif 'miller' in user_type_db:
                res = update_miller(str(user_id), merged_company_reg, merged_company_name, merged_address, merged_district, merged_contact, 0.0)
            elif 'collect' in user_type_db:
                res = update_collector(str(user_id), merged_full_name, merged_address, merged_district, merged_contact, 0.0)
            elif 'wholesaler' in user_type_db:
                res = update_wholesaler(str(user_id), merged_company_reg, merged_company_name, merged_address, merged_district, merged_contact, 0.0)
            elif 'retailer' in user_type_db:
                res = update_retailer(str(user_id), merged_full_name or merged_company_name, merged_address, merged_district, merged_contact, 0.0)
            elif 'beer' in user_type_db or 'brewer' in user_type_db:
                res = update_brewer(str(user_id), merged_company_reg, merged_company_name or merged_full_name, merged_address, merged_district, merged_contact, 0.0)
            elif 'animal' in user_type_db:
                res = update_animal_food(str(user_id), merged_company_reg, merged_company_name or merged_full_name, merged_address, merged_district, merged_contact, 0.0)
            elif 'exporter' in user_type_db:
                res = update_exporter(str(user_id), merged_company_reg, merged_company_name or merged_full_name, merged_address, merged_district, merged_contact, 0.0)
            else:
                # Unknown user type – skip on-chain update and proceed to DB update
                res = None

            if res is None:
                # If an on-chain call was attempted and returned None, treat as failure
                # If we didn't attempt a call because user type is unknown, proceed with DB update
                if any(k in user_type_db for k in ['farmer','miller','collect','wholesaler','retailer','beer','brewer','animal','exporter']):
                    cursor.close()
                    conn.close()
                    return jsonify({'ok': False, 'error': 'Blockchain update failed or returned no result, aborting DB update'}), 500
            elif isinstance(res, dict):
                block_hash = res.get('block_hash')
                block_number = res.get('block_number')
                transaction_hash = res.get('transaction_hash')
        except Exception as e:
            cursor.close()
            conn.close()
            return jsonify({'ok': False, 'error': f'Blockchain update failed: {e}'}), 500

        # At this point on-chain update succeeded (or was skipped for unknown type); add block info if present
        if block_hash is not None:
            update_fields.extend(['block_hash = %s', 'block_number = %s', 'transaction_hash = %s'])
            update_values.extend([block_hash, block_number, transaction_hash])

        # Add user_id to values for WHERE clause and execute DB update
        update_values.append(user_id)
        update_sql = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"
        try:
            cursor.execute(update_sql, tuple(update_values))
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            cursor.close()
            conn.close()
            return jsonify({'ok': False, 'error': f'DB update failed after blockchain update: {e}'}), 500

        # Fetch and return updated user
        cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        row = cursor.fetchone()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            user_dict = dict(zip(columns, row))
            cursor.close()
            conn.close()
            return jsonify(user_dict), 200
        else:
            cursor.close()
            conn.close()
            return jsonify({'error': 'User not found'}), 404
            
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/rice_distribution', methods=['GET'])
def api_rice_distribution():
    """Return rice distribution by user type (Miller, Wholesaler, PMB).
    Optional query params: district and paddy_type to filter by district and rice type.
    """
    district_param = request.args.get('district')
    rice_type_param = request.args.get('paddy_type')
    
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Get rice stock data grouped by user type - only Miller, Wholesaler, PMB
        sql = '''
            SELECT 
                u.user_type,
                SUM(CAST(rs.quantity AS DECIMAL(14,3))) as total_quantity
            FROM rice_stock rs
            LEFT JOIN users u ON rs.miller_id = u.id
            WHERE u.user_type IN ('Miller', 'Wholesaler', 'PMB')
        '''
        params = []
        
        if district_param:
            sql += ' AND u.district = %s'
            params.append(str(district_param))
        
        if rice_type_param:
            sql += ' AND rs.paddy_type = %s'
            params.append(str(rice_type_param))
        
        sql += ' GROUP BY u.user_type ORDER BY u.user_type'
        
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        
        rows = cur.fetchall()
        
        # Get list of districts from users that have rice_stock and are Miller, Wholesaler, or PMB
        cur.execute('''
            SELECT DISTINCT u.district 
            FROM rice_stock rs
            LEFT JOIN users u ON rs.miller_id = u.id
            WHERE u.user_type IN ('Miller', 'Wholesaler', 'PMB')
            AND u.district IS NOT NULL AND u.district != ''
            ORDER BY u.district
        ''')
        districts = [row['district'] for row in cur.fetchall()]
        
        # Get list of rice types (paddy_type) that actually have data in rice_stock
        cur.execute('''
            SELECT DISTINCT rs.paddy_type as name
            FROM rice_stock rs
            WHERE rs.paddy_type IS NOT NULL AND rs.paddy_type != ''
            ORDER BY rs.paddy_type
        ''')
        paddy_types = [{'name': row['name']} for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        # Format response
        response = {
            'data': rows if rows else [],
            'districts': districts if districts else [],
            'paddy_types': paddy_types if paddy_types else []
        }
        
        return jsonify(response)
    except mysql.connector.Error as err:
        return jsonify({'error': str(err), 'data': [], 'districts': []}), 500


@app.route('/api/debug/rice_stock', methods=['GET'])
def debug_rice_stock():
    """Debug endpoint to check rice_stock table data."""
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Get rice stock count
        cur.execute('SELECT COUNT(*) as count FROM rice_stock')
        count_result = cur.fetchone()
        rice_stock_count = count_result['count'] if count_result else 0
        
        # Get sample rice stock data with user info
        cur.execute('''
            SELECT 
                rs.id,
                rs.miller_id,
                rs.paddy_type,
                rs.quantity,
                u.id as user_id,
                u.user_type,
                u.district,
                u.full_name
            FROM rice_stock rs
            LEFT JOIN users u ON rs.miller_id = u.id
            LIMIT 10
        ''')
        samples = cur.fetchall()
        
        # Get user types and districts from rice_stock users
        cur.execute('''
            SELECT DISTINCT u.user_type, u.district
            FROM rice_stock rs
            LEFT JOIN users u ON rs.miller_id = u.id
            ORDER BY u.user_type, u.district
        ''')
        user_types = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'rice_stock_count': rice_stock_count,
            'sample_data': samples,
            'user_types_and_districts': user_types
        })
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


@app.route('/api/rice_stock', methods=['GET'])
def api_rice_stock():
    """Return rice stock data with optional filtering by district, user_type, and paddy_type."""
    district_param = request.args.get('district')
    user_type_param = request.args.get('user_type')
    paddy_type_param = request.args.get('paddy_type')
    
    try:
        conn = get_connection(MYSQL_DATABASE)
        cur = conn.cursor(dictionary=True)
        
        # Get rice stock data with user details
        sql = '''
            SELECT 
                rs.id,
                rs.miller_id,
                rs.quantity,
                rs.paddy_type,
                u.user_type,
                u.district,
                u.full_name
            FROM rice_stock rs
            LEFT JOIN users u ON rs.miller_id = u.id
            WHERE 1=1
        '''
        params = []
        
        if district_param:
            sql += ' AND u.district = %s'
            params.append(str(district_param))
        
        if user_type_param:
            sql += ' AND u.user_type = %s'
            params.append(str(user_type_param))
        
        if paddy_type_param:
            sql += ' AND rs.paddy_type = %s'
            params.append(str(paddy_type_param))
        
        sql += ' ORDER BY rs.id'
        
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'data': rows if rows else [],
            'count': len(rows) if rows else 0
        })
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


if __name__ == '__main__':
    init_db()
    app.run(debug=True)

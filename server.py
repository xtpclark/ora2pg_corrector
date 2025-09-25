from flask import Flask, jsonify, request, g, send_from_directory
from ora2pg_ai_corrector import Ora2PgAICorrector
import os
import subprocess
import tempfile
import sqlite3
import psycopg2
import psycopg2.extras # Using extras for dict cursors
import jwt
from datetime import datetime, timedelta, timezone
from functools import wraps
from cryptography.fernet import Fernet
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')

# --- Security and Configuration ---
# IMPORTANT: Load secrets from environment variables. Do not hardcode them.
app.config['SECRET_KEY'] = os.environ.get('APP_SECRET_KEY')
if not app.config['SECRET_KEY']:
    raise ValueError("APP_SECRET_KEY environment variable not set. Please set a strong secret key.")

ENCRYPTION_KEY_STR = os.environ.get('APP_ENCRYPTION_KEY')
if not ENCRYPTION_KEY_STR:
    raise ValueError("APP_ENCRYPTION_KEY environment variable not set. Please generate and set a key.")
ENCRYPTION_KEY = ENCRYPTION_KEY_STR.encode()

STAGING_PG_DSN = os.environ.get('STAGING_PG_DSN')

# --- Database configuration ---
DB_BACKEND = os.environ.get('DB_BACKEND', 'sqlite')
# FIX: Use an absolute path for the SQLite database file inside the container.
# This makes the path unambiguous and corresponds to the volume mount.
SQLITE_DB = '/app/settings.db'
PG_DSN_CONFIG = os.environ.get('PG_DSN_CONFIG')

def get_db_connection():
    """Establishes a connection to the configured database."""
    try:
        if DB_BACKEND == 'sqlite':
            conn = sqlite3.connect(SQLITE_DB)
            # Use sqlite3.Row to access columns by name
            conn.row_factory = sqlite3.Row
            return conn
        elif DB_BACKEND == 'postgresql':
            if not PG_DSN_CONFIG:
                raise ValueError("DB_BACKEND is 'postgresql' but PG_DSN_CONFIG is not set.")
            # Use RealDictCursor to access columns by name
            return psycopg2.connect(PG_DSN_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            raise ValueError(f"Unsupported DB_BACKEND: {DB_BACKEND}")
    except Exception as e:
        logger.error(f"Error connecting to the {DB_BACKEND} database: {e}")
        return None

def execute_query(conn, query, params=None):
    """Executes a query with the correct parameter style for the DB backend."""
    cursor = conn.cursor()
    # Use '%s' for PostgreSQL and '?' for SQLite
    if DB_BACKEND == 'postgresql':
        query = query.replace('?', '%s')
    cursor.execute(query, params or ())
    return cursor

def init_db():
    """Initializes the database schema if tables don't exist."""
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection could not be established. Aborting DB initialization.")
        return
    
    try:
        is_sqlite = DB_BACKEND == 'sqlite'
        # Use appropriate syntax for auto-incrementing primary keys
        primary_key_type = 'INTEGER PRIMARY KEY AUTOINCREMENT' if is_sqlite else 'SERIAL PRIMARY KEY'
        timestamp_type = 'TIMESTAMP' if is_sqlite else 'TIMESTAMP WITH TIME ZONE'

        with conn:
            conn.execute(f'''
                CREATE TABLE IF NOT EXISTS clients (
                    client_id {primary_key_type},
                    client_name TEXT NOT NULL UNIQUE,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute(f'''
                CREATE TABLE IF NOT EXISTS configs (
                    config_id {primary_key_type},
                    client_id INTEGER NOT NULL,
                    config_type TEXT NOT NULL,
                    config_key TEXT NOT NULL,
                    config_value TEXT,
                    last_modified {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
                )
            ''')
            conn.execute(f'''
                CREATE TABLE IF NOT EXISTS audit_logs (
                    log_id {primary_key_type},
                    client_id INTEGER,
                    user_id TEXT,
                    action TEXT NOT NULL,
                    details TEXT,
                    timestamp {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
                )
            ''')
        logger.info("Database initialized successfully.")
    finally:
        if conn:
            conn.close()


with app.app_context():
    init_db()

# --- Authentication ---
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization header is missing or invalid'}), 401
        
        token = auth_header.split(' ')[1]
        try:
            payload = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            g.user_id = payload['sub']
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Token is invalid'}), 401
        
        return f(*args, **kwargs)
    return decorated

def log_audit(client_id, action, details):
    """Helper function to log audit events."""
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to log audit event due to DB connection failure.")
        return
    try:
        with conn:
            execute_query(
                conn,
                'INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                (client_id, g.get('user_id'), action, details)
            )
    finally:
        if conn:
            conn.close()

# --- Application Logic ---
def get_corrector_from_config(config_data):
    """Creates a corrector instance from a given configuration dictionary."""
    fernet = Fernet(ENCRYPTION_KEY)
    
    # Decrypt sensitive keys if they exist
    api_key = config_data.get('ai_api_key', '')
    if api_key:
        try:
            api_key = fernet.decrypt(api_key.encode()).decode()
        except Exception:
            # Fallback for unencrypted keys for backward compatibility
            logger.warning("Could not decrypt ai_api_key. Treating as plaintext.")

    return Ora2PgAICorrector(
        ora2pg_path='ora2pg',
        output_dir=config_data.get('output_dir', 'output'),
        ai_settings={
            'ai_provider': config_data.get('ai_provider'),
            'ai_endpoint': config_data.get('ai_endpoint'),
            'ai_model': config_data.get('ai_model'),
            'ai_api_key': api_key,
            'ai_temperature': float(config_data.get('ai_temperature', 0.7)),
            'ai_max_output_tokens': int(config_data.get('ai_max_output_tokens', 2048)),
        },
        encryption_key=ENCRYPTION_KEY
    )

# --- Routes ---
@app.route('/')
def serve_gui():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    # FIX: Removed hardcoded credentials.
    # Replace this with a proper user authentication system (e.g., Flask-Login with a user database).
    # This is a placeholder for demonstration.
    DUMMY_USER = os.environ.get("DUMMY_USER", "admin")
    DUMMY_PASSWORD = os.environ.get("DUMMY_PASSWORD", "password")

    if username == DUMMY_USER and password == DUMMY_PASSWORD:
        token = jwt.encode({
            'sub': username,
            'iat': datetime.now(timezone.utc),
            'exp': datetime.now(timezone.utc) + timedelta(hours=24)
        }, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'token': token})
    
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/clients', methods=['GET', 'POST'])
@token_required
def manage_clients():
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        if request.method == 'GET':
            cursor = execute_query(conn, '''
                SELECT c.client_id, c.client_name, MAX(cf.last_modified) as last_modified
                FROM clients c
                LEFT JOIN configs cf ON c.client_id = cf.client_id
                GROUP BY c.client_id, c.client_name
                ORDER BY c.client_name
            ''')
            clients = [dict(row) for row in cursor.fetchall()]
            log_audit(None, 'list_clients', 'Listed all clients')
            return jsonify(clients)

        elif request.method == 'POST':
            data = request.get_json()
            client_name = data.get('client_name')
            if not client_name:
                return jsonify({'error': 'Client name is required'}), 400

            try:
                with conn:
                    cursor = execute_query(conn, 'INSERT INTO clients (client_name) VALUES (?) RETURNING client_id, client_name, created_at as last_modified', (client_name,))
                    new_client = dict(cursor.fetchone())
                    log_audit(new_client['client_id'], 'create_client', f'Created client: {client_name}')
                    return jsonify(new_client), 201
            except (sqlite3.IntegrityError, psycopg2.IntegrityError):
                return jsonify({'error': 'Client name already exists'}), 409
    finally:
        if conn:
            conn.close()

@app.route('/api/client/<int:client_id>/config', methods=['GET', 'POST'])
@token_required
def manage_config(client_id):
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        if request.method == 'GET':
            cursor = execute_query(conn, 'SELECT config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
            rows = cursor.fetchall()
            
            # Start with default values
            config = {
                'oracle_dsn': '', 'oracle_user': '', 'oracle_password': '', 'output_dir': 'output',
                'type': 'SHOW_REPORT', 'dump_as_html': True, 'ai_provider': 'openai',
                'ai_endpoint': 'https://api.openai.com/v1/', 'ai_model': 'gpt-4', 'ai_api_key': '',
                'ai_temperature': 0.7, 'ai_max_output_tokens': 4096, 'ai_run_integrated': True
            }
            fernet = Fernet(ENCRYPTION_KEY)
            
            # Update with saved values
            for row in rows:
                key = row['config_key']
                value = row['config_value']
                
                if key in ['oracle_password', 'ai_api_key']:
                    # These values are stored encrypted
                    try:
                        config[key] = fernet.decrypt(value.encode()).decode()
                    except Exception:
                        config[key] = "" # Return empty if decryption fails
                elif key in ['dump_as_html', 'ai_run_integrated']:
                    config[key] = value.lower() in ('true', '1')
                elif key in ['ai_temperature', 'ai_max_output_tokens']:
                    # Safely convert to float/int
                    try:
                        config[key] = float(value) if '.' in value else int(value)
                    except (ValueError, TypeError):
                        pass # Keep default if value is invalid
                else:
                    config[key] = value
            
            log_audit(client_id, 'get_config', 'Loaded configuration')
            return jsonify(config)

        elif request.method == 'POST':
            config_data = request.get_json()
            fernet = Fernet(ENCRYPTION_KEY)
            
            with conn:
                # Clear existing config for simplicity
                execute_query(conn, 'DELETE FROM configs WHERE client_id = ?', (client_id,))
                
                for key, value in config_data.items():
                    # Encrypt sensitive fields before saving
                    if key in ['oracle_password', 'ai_api_key'] and value:
                        value = fernet.encrypt(str(value).encode()).decode()
                    
                    execute_query(
                        conn,
                        'INSERT INTO configs (client_id, config_type, config_key, config_value, last_modified) VALUES (?, ?, ?, ?, ?)',
                        (client_id, 'ora2pg', key, str(value), datetime.now(timezone.utc))
                    )
            log_audit(client_id, 'save_config', 'Saved configuration')
            return jsonify({'message': 'Configuration saved successfully'})
            
    finally:
        if conn:
            conn.close()

@app.route('/api/client/<int:client_id>', methods=['DELETE'])
@token_required
def delete_client(client_id):
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        with conn:
            # ON DELETE CASCADE will handle configs and audit logs
            cursor = execute_query(conn, 'DELETE FROM clients WHERE client_id = ?', (client_id,))
            if cursor.rowcount == 0:
                return jsonify({'error': 'Client not found'}), 404
        
        log_audit(client_id, 'delete_client', f'Deleted client with ID {client_id}')
        return jsonify({'message': 'Client and associated data deleted'})
    finally:
        if conn:
            conn.close()

@app.route('/api/run', methods=['POST'])
@token_required
def run_ora2pg():
    config = request.json
    client_id = config.get('client_id')
    output_dir = config.get('output_dir', 'output')
    os.makedirs(output_dir, exist_ok=True)

    fernet = Fernet(ENCRYPTION_KEY)
    
    # Decrypt password for the config file
    password = config.get('oracle_password', '')
    if password:
        try:
            password = fernet.decrypt(password.encode()).decode()
        except Exception:
            logger.warning("Could not decrypt Oracle password. Using raw value.")

    # Create a temporary config file for ora2pg
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as temp_conf:
        config_content = f"""
ORACLE_DSN={config.get('oracle_dsn', '')}
ORACLE_USER={config.get('oracle_user', '')}
ORACLE_PWD={password}
OUTPUT_DIR={output_dir}
TYPE={config.get('type', 'SHOW_REPORT')}
DUMP_AS_HTML={'1' if config.get('dump_as_html') else '0'}
"""
        temp_conf.write(config_content)
        temp_conf_path = temp_conf.name

    try:
        corrector = get_corrector_from_config(config)
        logs = corrector.run_ora2pg(temp_conf_path)
        
        # Store the corrector instance in the request context `g`
        g.corrector = corrector

        log_audit(client_id, 'run_ora2pg', f'Ran Ora2PG with type {config.get("type")}')
        return jsonify({'logs': logs, 'output_sql': corrector.sql_content})
    except Exception as e:
        logger.error(f"Error during Ora2PG run: {e}")
        return jsonify({'logs': f'Error: {str(e)}'}), 500
    finally:
        os.unlink(temp_conf_path)

@app.route('/api/objects', methods=['POST'])
@token_required
def get_objects():
    data = request.json
    sql_content = data.get('sql_content')
    config = data.get('config')
    
    if not sql_content or not config:
        return jsonify({'error': 'Missing SQL content or configuration'}), 400

    corrector = get_corrector_from_config(config)
    corrector.sql_content = sql_content
    objects = corrector.parse_sql_objects()
    return jsonify(objects)


@app.route('/api/correct', methods=['POST'])
@token_required
def correct_object():
    data = request.json
    sql_object = data.get('sql_object')
    issues = data.get('issues')
    config = data.get('config')
    client_id = config.get('client_id')

    if not all([sql_object, issues, config]):
        return jsonify({'error': 'Missing required data for correction'}), 400

    corrector = get_corrector_from_config(config)
    corrected_sql, metrics = corrector.ai_correct_sql(sql_object, issues)
    
    log_audit(client_id, 'correct_object', f'Ran AI correction. Metrics: {metrics}')
    return jsonify({'corrected_sql': corrected_sql, 'metrics': metrics})


@app.route('/api/validate', methods=['POST'])
@token_required
def validate_sql():
    if not STAGING_PG_DSN:
        return jsonify({'message': 'Validation skipped: STAGING_PG_DSN is not configured.'}), 200

    data = request.json
    sql = data.get('sql')
    client_id = data.get('client_id')

    if not sql:
        return jsonify({'error': 'No SQL content provided for validation'}), 400
        
    # Validation can be slow, so we don't instantiate a full corrector
    is_valid, message = Ora2PgAICorrector.validate_sql(sql, STAGING_PG_DSN)
    
    log_audit(client_id, 'validate_sql', f'Validation result: {message}')
    return jsonify({'message': message, 'is_valid': is_valid})


if __name__ == '__main__':
    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)
    # Use Gunicorn in production instead of app.run()
    app.run(debug=True, host='0.0.0.0', port=5000)

@app.route('/api/load_file', methods=['POST'])
@token_required
def load_sql_file():
    """Loads SQL content from a file in the output directory instead of running ora2pg."""
    data = request.json
    filename = data.get('filename')
    client_id = data.get('client_id')

    if not filename:
        return jsonify({'error': 'Filename is required'}), 400

    # Security: Prevent directory traversal attacks.
    # Ensure the file is read only from the 'output' directory.
    output_dir = 'output'
    # os.path.basename() strips any directory info from the filename
    safe_filename = os.path.basename(filename)
    file_path = os.path.join(output_dir, safe_filename)

    if not os.path.exists(file_path):
        return jsonify({'error': f'File not found in output directory: {safe_filename}'}), 404

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        log_audit(client_id, 'load_sql_file', f'Loaded SQL from file: {safe_filename}')
        # Mimic the response structure of the /api/run endpoint for frontend compatibility
        return jsonify({'logs': f'Successfully loaded content from {safe_filename}.', 'output_sql': sql_content})
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return jsonify({'error': 'Failed to read file'}), 500


@app.route('/api/objects', methods=['POST'])
@token_required
def get_objects():


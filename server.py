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
SQLITE_DB = '/app/settings.db'
PG_DSN_CONFIG = os.environ.get('PG_DSN_CONFIG')

# --- Database Connection Management ---
def get_db():
    if 'db' not in g:
        try:
            if DB_BACKEND == 'sqlite':
                g.db = sqlite3.connect(SQLITE_DB, timeout=10)
                g.db.row_factory = sqlite3.Row
            else:
                if not PG_DSN_CONFIG: raise ValueError("PG_DSN_CONFIG not set for PostgreSQL backend.")
                g.db = psycopg2.connect(PG_DSN_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            g.db = None
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def execute_query(conn, query, params=None):
    cursor = conn.cursor()
    if DB_BACKEND == 'postgresql':
        query = query.replace('?', '%s')
    cursor.execute(query, params or ())
    return cursor

def seed_ai_providers(conn):
    """Seeds the database with common AI provider configurations."""
    providers = [
        ('Google Gemini', 'https://generativelanguage.googleapis.com/v1beta/models/', 'gemini-1.5-flash-latest', 'https://aistudio.google.com/app/apikey', 'For Gemini models. The model name is appended to the endpoint URL in the API call.'),
        ('OpenAI (ChatGPT)', 'https://api.openai.com/v1/', 'gpt-4o', 'https://platform.openai.com/api-keys', 'For GPT-4, GPT-3.5, etc. Standard API endpoint.'),
        ('Groq', 'https://api.groq.com/openai/v1/', 'llama3-70b-8192', 'https://console.groq.com/keys', 'High-speed inference using an OpenAI-compatible API structure.'),
        ('Anthropic (Claude)', 'https://api.anthropic.com/v1/', 'claude-3-opus-20240229', 'https://console.anthropic.com/settings/keys', 'Requires a custom API connector due to different payload structure (not implemented by default).')
    ]
    
    insert_sql = 'INSERT INTO ai_providers (name, api_endpoint, default_model, key_url, notes) VALUES (?, ?, ?, ?, ?)'
    if DB_BACKEND == 'postgresql':
        # For Postgres, use ON CONFLICT to avoid errors on re-runs
        insert_sql = insert_sql.replace('INSERT INTO', 'INSERT INTO ai_providers (name, api_endpoint, default_model, key_url, notes) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (name) DO NOTHING')
    else:
        # For SQLite, use INSERT OR IGNORE
        insert_sql = insert_sql.replace('INSERT INTO', 'INSERT OR IGNORE INTO')

    with conn:
        cursor = conn.cursor()
        for provider in providers:
            cursor.execute(insert_sql, provider)
    logger.info(f"Seeded {len(providers)} AI providers.")

def init_db():
    with app.app_context():
        conn = get_db()
        if not conn:
            logger.error("DB connection failed, aborting initialization.")
            return
        
        try:
            is_sqlite = DB_BACKEND == 'sqlite'
            pk_type = 'INTEGER PRIMARY KEY AUTOINCREMENT' if is_sqlite else 'SERIAL PRIMARY KEY'
            ts_type = 'TIMESTAMP' if is_sqlite else 'TIMESTAMP WITH TIME ZONE'

            with conn:
                # Other tables (clients, configs, audit_logs) remain the same...
                execute_query(conn, f'''CREATE TABLE IF NOT EXISTS clients (client_id {pk_type}, client_name TEXT NOT NULL UNIQUE, created_at {ts_type} DEFAULT CURRENT_TIMESTAMP)''')
                execute_query(conn, f'''CREATE TABLE IF NOT EXISTS configs (config_id {pk_type}, client_id INTEGER NOT NULL, config_type TEXT NOT NULL, config_key TEXT NOT NULL, config_value TEXT, last_modified {ts_type} DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE)''')
                execute_query(conn, f'''CREATE TABLE IF NOT EXISTS audit_logs (log_id {pk_type}, client_id INTEGER, user_id TEXT, action TEXT NOT NULL, details TEXT, timestamp {ts_type} DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE)''')
                
                # NEW: Create and seed the ai_providers table
                execute_query(conn, f'''
                    CREATE TABLE IF NOT EXISTS ai_providers (
                        provider_id {pk_type},
                        name TEXT NOT NULL UNIQUE,
                        api_endpoint TEXT NOT NULL,
                        default_model TEXT,
                        key_url TEXT,
                        notes TEXT
                    )
                ''')
            # Seed the data after table creation
            seed_ai_providers(conn)
            logger.info("Database initialized successfully.")
        except Exception as e:
            logger.error(f"Error during DB initialization: {e}")

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
    conn = get_db()
    if not conn: return
    with conn:
        execute_query(conn, 'INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)', (client_id, g.get('user_id'), action, details))

def get_corrector_from_config(config_data):
    fernet = Fernet(ENCRYPTION_KEY)
    api_key = config_data.get('ai_api_key', '')
    if api_key:
        try: api_key = fernet.decrypt(api_key.encode()).decode()
        except Exception: pass
    return Ora2PgAICorrector(ora2pg_path='ora2pg', output_dir='/app/output', ai_settings={'ai_provider': config_data.get('ai_provider'), 'ai_endpoint': config_data.get('ai_endpoint'), 'ai_model': config_data.get('ai_model'), 'ai_api_key': api_key, 'ai_temperature': float(config_data.get('ai_temperature', 0.7)), 'ai_max_output_tokens': int(config_data.get('ai_max_output_tokens', 2048)),}, encryption_key=ENCRYPTION_KEY)

# --- Routes ---
@app.route('/')
def serve_gui():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username, password = data.get('username'), data.get('password')
    DUMMY_USER = os.environ.get("DUMMY_USER", "admin")
    DUMMY_PASSWORD = os.environ.get("DUMMY_PASSWORD", "password")
    if username == DUMMY_USER and password == DUMMY_PASSWORD:
        token = jwt.encode({'sub': username, 'iat': datetime.now(timezone.utc), 'exp': datetime.now(timezone.utc) + timedelta(hours=24)}, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'token': token})
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/ai_providers', methods=['GET'])
@token_required
def get_ai_providers():
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    cursor = execute_query(conn, 'SELECT * FROM ai_providers ORDER BY name')
    providers = [dict(row) for row in cursor.fetchall()]
    return jsonify(providers)

@app.route('/api/clients', methods=['GET', 'POST'])
@token_required
def manage_clients():
    conn = get_db()
    if not conn: return jsonify({'error': 'Database connection failed'}), 500
    if request.method == 'GET':
        cursor = execute_query(conn, 'SELECT c.client_id, c.client_name, MAX(cf.last_modified) as last_modified FROM clients c LEFT JOIN configs cf ON c.client_id = cf.client_id GROUP BY c.client_id, c.client_name ORDER BY c.client_name')
        return jsonify([dict(row) for row in cursor.fetchall()])
    elif request.method == 'POST':
        data = request.get_json()
        client_name = data.get('client_name')
        if not client_name: return jsonify({'error': 'Client name is required'}), 400
        try:
            with conn:
                cursor = execute_query(conn, 'INSERT INTO clients (client_name) VALUES (?) RETURNING client_id', (client_name,))
                client_id = cursor.lastrowid if DB_BACKEND == 'sqlite' else cursor.fetchone()['client_id']
                cursor = execute_query(conn, 'SELECT client_id, client_name, created_at as last_modified FROM clients WHERE client_id = ?', (client_id,))
                new_client = dict(cursor.fetchone())
                log_audit(new_client['client_id'], 'create_client', f'Created client: {client_name}')
                return jsonify(new_client), 201
        except (sqlite3.IntegrityError, psycopg2.IntegrityError):
            return jsonify({'error': 'Client name already exists'}), 409
        except Exception as e:
            logger.error(f"Error creating client: {e}")
            return jsonify({'error': 'An internal error occurred'}), 500

@app.route('/api/client/<int:client_id>/config', methods=['GET', 'POST'])
@token_required
def manage_config(client_id):
    conn = get_db()
    if not conn: return jsonify({'error': 'Database connection failed'}), 500
    if request.method == 'GET':
        cursor = execute_query(conn, 'SELECT config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
        rows = cursor.fetchall()
        config = {'oracle_dsn': '', 'oracle_user': '', 'oracle_password': '', 'type': 'SHOW_REPORT', 'dump_as_html': True, 'ai_provider': 'OpenAI (ChatGPT)', 'ai_endpoint': 'https://api.openai.com/v1/', 'ai_model': 'gpt-4o', 'ai_api_key': '', 'ai_temperature': 0.7, 'ai_max_output_tokens': 4096, 'ai_run_integrated': True}
        for row in rows:
            key, value = row['config_key'], row['config_value']
            if key in ['oracle_password', 'ai_api_key']: config[key] = value
            elif key in ['dump_as_html', 'ai_run_integrated']: config[key] = value.lower() in ('true', '1')
            elif key in ['ai_temperature', 'ai_max_output_tokens']:
                try: config[key] = float(value) if '.' in value else int(value)
                except (ValueError, TypeError): pass
            else: config[key] = value
        log_audit(client_id, 'get_config', 'Loaded configuration')
        return jsonify(config)
    elif request.method == 'POST':
        config_data = request.get_json()
        fernet = Fernet(ENCRYPTION_KEY)
        with conn:
            for key, value in config_data.items():
                if key in ['oracle_password', 'ai_api_key'] and value:
                    try: fernet.decrypt(value.encode())
                    except Exception: value = fernet.encrypt(str(value).encode()).decode()
                
                cursor = conn.cursor()
                if DB_BACKEND == 'postgresql':
                    cursor.execute('SELECT config_id FROM configs WHERE client_id = %s AND config_key = %s', (client_id, key))
                else:
                    cursor.execute('SELECT config_id FROM configs WHERE client_id = ? AND config_key = ?', (client_id, key))
                
                exists = cursor.fetchone()
                
                if exists:
                    execute_query(conn, 'UPDATE configs SET config_value = ?, last_modified = ? WHERE client_id = ? AND config_key = ?', (str(value), datetime.now(timezone.utc), client_id, key))
                else:
                    execute_query(conn, 'INSERT INTO configs (client_id, config_type, config_key, config_value, last_modified) VALUES (?, ?, ?, ?, ?)', (client_id, 'ora2pg', key, str(value), datetime.now(timezone.utc)))
        log_audit(client_id, 'save_config', 'Saved configuration')
        return jsonify({'message': 'Configuration saved successfully'})

@app.route('/api/load_file', methods=['POST'])
@token_required
def load_sql_file():
    data = request.json
    filename = data.get('filename')
    client_id = data.get('client_id')
    if not filename:
        return jsonify({'error': 'Filename is required'}), 400

    output_dir = '/app/output'
    # Security: Prevent directory traversal attacks
    safe_filename = os.path.basename(filename)
    file_path = os.path.join(output_dir, safe_filename)
    
    # Security: Ensure the final path is still within the intended directory
    if not os.path.abspath(file_path).startswith(os.path.abspath(output_dir)):
        return jsonify({'error': 'Invalid filename'}), 400

    if not os.path.exists(file_path):
        logger.error(f"File not found at path: {file_path}")
        return jsonify({'error': f'File not found in output directory: {safe_filename}'}), 404

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        log_audit(client_id, 'load_sql_file', f'Loaded SQL from file: {safe_filename}')
        return jsonify({'logs': f'Successfully loaded content from {safe_filename}.', 'output_sql': sql_content})
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return jsonify({'error': 'Failed to read file'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)

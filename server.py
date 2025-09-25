from flask import Flask, jsonify, request, g, send_from_directory, Response
from ora2pg_ai_corrector import Ora2PgAICorrector
import os
import subprocess
import tempfile
import sqlite3
import psycopg2
import jwt
from datetime import datetime, timedelta
from functools import wraps
from cryptography.fernet import Fernet
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')

# --- Security and Configuration ---
app.config['SECRET_KEY'] = os.environ.get('APP_SECRET_KEY', 'default-super-secret-key-for-dev')
ENCRYPTION_KEY = os.environ.get('APP_ENCRYPTION_KEY', 'R-gK8Pz0V_imDSG3P4w2tboyw0np2_p_soj63R2_B2E=').encode()
STAGING_PG_DSN = os.environ.get('STAGING_PG_DSN', 'dbname=staging user=postgres password=postgres host=localhost port=5432')

# --- Database configuration ---
DB_BACKEND = os.environ.get('DB_BACKEND', 'sqlite')
SQLITE_DB = 'settings.db'
PG_DSN = os.environ.get('PG_DSN_CONFIG', 'dbname=ora2pg_settings user=postgres password=postgres host=postgres port=5432')

def get_db_connection():
    try:
        if DB_BACKEND == 'sqlite':
            conn = sqlite3.connect(SQLITE_DB)
            conn.row_factory = sqlite3.Row
            return conn
        return psycopg2.connect(PG_DSN)
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if not conn:
        return
    is_sqlite = DB_BACKEND == 'sqlite'
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            client_id INTEGER PRIMARY KEY {} AUTOINCREMENT,
            client_name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    '''.format('' if is_sqlite else 'SERIAL'))
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS configs (
            config_id INTEGER PRIMARY KEY {} AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            config_type TEXT NOT NULL,
            config_key TEXT NOT NULL,
            config_value TEXT,
            last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
        )
    '''.format('' if is_sqlite else 'SERIAL'))
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_logs (
            log_id INTEGER PRIMARY KEY {} AUTOINCREMENT,
            client_id INTEGER,
            user_id TEXT,
            action TEXT NOT NULL,
            details TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
        )
    '''.format('' if is_sqlite else 'SERIAL'))
    conn.commit()
    conn.close()

with app.app_context():
    init_db()

# --- Authentication ---
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        try:
            payload = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            g.user_id = payload['sub']
        except:
            return jsonify({'error': 'Invalid or missing token'}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/')
def serve_gui():
    return send_from_directory(app.static_folder, 'index.html', mimetype='text/html')

# --- Request-based State Management ---
def get_corrector(settings):
    """Creates or retrieves a corrector instance for the current request context."""
    if 'corrector' not in g:
        g.corrector = Ora2PgAICorrector(
            ora2pg_path='ora2pg',
            output_dir=settings.get('output_dir', 'output'),
            ai_settings=settings,
            encryption_key=ENCRYPTION_KEY
        )
    return g.corrector

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    # Placeholder: Replace with proper user management
    if username == 'admin' and password == 'admin_password':
        token = jwt.encode({
            'sub': username,
            'exp': datetime.utcnow() + timedelta(hours=24)
        }, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'token': token})
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/configs', methods=['GET', 'POST'])
@token_required
def manage_configs():
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    cursor = conn.cursor()

    if request.method == 'GET':
        try:
            cursor.execute('''
                SELECT c.client_id, c.client_name, MAX(cf.last_modified) as last_modified,
                       (SELECT config_value FROM configs WHERE client_id = c.client_id AND config_key = 'output_dir' AND config_type = 'ora2pg') as output_dir
                FROM clients c
                LEFT JOIN configs cf ON c.client_id = cf.client_id
                GROUP BY c.client_id, c.client_name
            ''')
            configs = [
                {'client_id': row['client_id'], 'client_name': row['client_name'], 'output_dir': row['output_dir'], 'last_modified': row['last_modified']}
                for row in cursor.fetchall()
            ]
            cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                           (None, g.user_id, 'list_configs', 'Listed all configurations'))
            conn.commit()
            return jsonify(configs)
        finally:
            conn.close()

    elif request.method == 'POST':
        data = request.get_json()
        client_name = data.get('client_name')
        if not client_name:
            return jsonify({'error': 'Client name is required'}), 400
        try:
            cursor.execute('INSERT INTO clients (client_name) VALUES (?)', (client_name,))
            client_id = cursor.lastrowid
            cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                           (client_id, g.user_id, 'create_config', f'Created config for client {client_name}'))
            conn.commit()
            return jsonify({'client_id': client_id, 'client_name': client_name, 'last_modified': datetime.now().isoformat()}), 201
        except sqlite3.IntegrityError:
            return jsonify({'error': 'Client name already exists'}), 409
        finally:
            conn.close()

@app.route('/api/config/<int:client_id>', methods=['GET', 'DELETE'])
@token_required
def manage_config(client_id):
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    cursor = conn.cursor()

    if request.method == 'GET':
        try:
            cursor.execute('SELECT config_type, config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
            rows = cursor.fetchall()
            if not rows:
                return jsonify({'error': 'Config not found'}), 404
            config = {
                'oracle_dsn': '',
                'oracle_user': '',
                'oracle_password': '',
                'output_dir': 'output',
                'type': 'SHOW_REPORT',
                'dump_as_html': True,
                'ai_provider': 'openai',
                'ai_endpoint': 'https://api.openai.com/',
                'ai_model': 'gpt-4',
                'ai_api_key': '',
                'ai_temperature': 0.7,
                'ai_max_output_tokens': 2048,
                'ai_run_integrated': True,
                'report_filename': 'migration_report.html',
                'validation_timeout': '5s'
            }
            fernet = Fernet(ENCRYPTION_KEY)
            for row in rows:
                if row['config_key'] in config:
                    value = fernet.decrypt(row['config_value'].encode()).decode() if row['config_key'] in ['oracle_password', 'ai_api_key'] else row['config_value']
                    config[row['config_key']] = value if row['config_key'] not in ['dump_as_html', 'ai_run_integrated', 'ai_temperature', 'ai_max_output_tokens'] else (value == 'True' or float(value) if row['config_key'] in ['ai_temperature', 'ai_max_output_tokens'] else value)
            
            g.corrector = Ora2PgAICorrector(
                ora2pg_path='ora2pg',
                output_dir=config['output_dir'],
                ai_settings={
                    'ai_provider': config['ai_provider'],
                    'ai_endpoint': config['ai_endpoint'],
                    'ai_model': config['ai_model'],
                    'ai_api_key': fernet.decrypt(config['ai_api_key'].encode()).decode() if config['ai_api_key'] else '',
                    'ai_temperature': float(config['ai_temperature']),
                    'ai_max_output_tokens': int(config['ai_max_output_tokens']),
                    'ai_run_integrated': config['ai_run_integrated'],
                    'report_filename': config['report_filename'],
                    'validation_timeout': config['validation_timeout']
                },
                encryption_key=ENCRYPTION_KEY
            )
            output_sql = os.path.join(config['output_dir'], 'output.sql')
            if os.path.exists(output_sql):
                with open(output_sql, 'r') as f:
                    g.corrector.sql_content = f.read()
            cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                           (client_id, g.user_id, 'load_config', f'Loaded config for client_id {client_id}'))
            conn.commit()
            return jsonify(config)
        finally:
            conn.close()

    elif request.method == 'DELETE':
        try:
            cursor.execute('DELETE FROM configs WHERE client_id = ?', (client_id,))
            cursor.execute('DELETE FROM clients WHERE client_id = ?', (client_id,))
            cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                           (client_id, g.user_id, 'delete_config', f'Deleted config for client_id {client_id}'))
            conn.commit()
            return jsonify({'message': 'Config deleted'})
        finally:
            conn.close()

@app.route('/api/run', methods=['POST'])
@token_required
def run_ora2pg():
    config = request.json
    output_dir = config.get('output_dir', 'output')
    os.makedirs(output_dir, exist_ok=True)

    fernet = Fernet(ENCRYPTION_KEY)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as temp_conf:
        config_content = f"""
[oracle]
ORACLE_DSN={config.get('oracle_dsn', '')}
ORACLE_USER={config.get('oracle_user', '')}
ORACLE_PWD={fernet.decrypt(config.get('oracle_password', '').encode()).decode() if config.get('oracle_password') else ''}

[pg]
OUTPUT_DIR={output_dir}
TYPE={config.get('type', 'SHOW_REPORT')}
DUMP_AS_HTML={1 if config.get('dump_as_html', False) else 0}
ESTIMATE_COST=1
"""
        temp_conf.write(config_content)
        temp_conf_path = temp_conf.name

    try:
        corrector = get_corrector(config)
        logs = corrector.run_ora2pg(temp_conf_path)  # Sync call
        if config.get('type') == 'SHOW_REPORT':
            report_path = os.path.join(output_dir, config.get('report_filename', 'migration_report.html'))
            if os.path.exists(report_path):
                corrector.parse_migration_report(report_path)
        else:
            output_sql = os.path.join(output_dir, 'output.sql')
            if os.path.exists(output_sql):
                with open(output_sql, 'r') as f:
                    corrector.sql_content = f.read()
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                       (config.get('client_id'), g.user_id, 'run_ora2pg', f'Ran Ora2PG with type {config.get("type")}'))
        conn.commit()
        conn.close()
        return jsonify({'logs': logs})
    except Exception as e:
        return jsonify({'logs': f'Error: {str(e)}'})
    finally:
        os.unlink(temp_conf_path)

@app.route('/api/objects', methods=['GET'])
@token_required
def get_objects():
    corrector = g.get('corrector')
    if not corrector:
        return jsonify({'error': 'No active configuration loaded'}), 400
    objects = corrector.parse_sql_objects()
    for obj in objects:
        if obj['issues']:
            corrected_sql, metrics = corrector.ai_correct_sql(obj['sql'], obj['issues'])
            obj['corrected_sql'] = corrected_sql
            obj['metrics'] = metrics
        else:
            obj['corrected_sql'] = obj['sql']
            obj['metrics'] = {}
    return jsonify(objects)

@app.route('/api/correct', methods=['POST'])
@token_required
def correct_object():
    data = request.json
    name = data.get('name')
    client_id = data.get('client_id')
    corrector = g.get('corrector')
    if not corrector:
        return jsonify({'error': 'No active configuration loaded'}), 400
    for obj in corrector.parse_sql_objects():
        if obj['name'] == name:
            corrected_sql, metrics = corrector.ai_correct_sql(obj['sql'], obj['issues'])
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                           (client_id, g.user_id, 'correct_object', f'Re-ran AI correction for object {name}'))
            conn.commit()
            conn.close()
            return jsonify({'corrected_sql': corrected_sql, 'metrics': metrics})
    return jsonify({'error': 'Object not found'}), 404

@app.route('/api/validate', methods=['POST'])
@token_required
def validate_sql():
    data = request.json
    sql = data.get('sql')
    client_id = data.get('client_id')
    corrector = g.get('corrector')
    if not corrector:
        return jsonify({'error': 'No active configuration loaded'}), 400
    is_valid, message = corrector.validate_sql(sql, STAGING_PG_DSN)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                   (client_id, g.user_id, 'validate_sql', f'Validated SQL: {message}'))
    conn.commit()
    conn.close()
    return jsonify({'message': message})

@app.route('/api/save', methods=['POST'])
@token_required
def save_sql():
    data = request.json
    name = data.get('name')
    sql = data.get('sql')
    client_id = data.get('client_id')
    corrector = g.get('corrector')
    if not corrector:
        return jsonify({'error': 'No active configuration loaded'}), 400
    objects = corrector.parse_sql_objects()
    for obj in objects:
        if obj['name'] == name:
            obj['corrected_sql'] = sql
    output_path = corrector.save_corrected_file(corrector.sql_content, objects)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)',
                   (client_id, g.user_id, 'save_sql', f'Saved corrected SQL for object {name} to {output_path}'))
    conn.commit()
    conn.close()
    return jsonify({'message': 'Saved successfully'})

if __name__ == '__main__':
    os.makedirs('output', exist_ok=True)
    app.run(debug=True, port=5000)

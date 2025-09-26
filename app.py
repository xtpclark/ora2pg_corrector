from flask import Flask, jsonify, request, g, render_template
from modules.config import load_ora2pg_config, load_ai_providers
from modules.db import get_db, close_db, execute_query
from modules.auth import token_required, login
from modules.sql_processing import Ora2PgAICorrector
from modules.audit import log_audit
import os
import tempfile
import logging
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import sqlite3
import psycopg2

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates', static_folder='static')
app.config['SECRET_KEY'] = os.environ.get('APP_SECRET_KEY')
if not app.config['SECRET_KEY']:
    raise ValueError("APP_SECRET_KEY environment variable not set.")

ENCRYPTION_KEY_STR = os.environ.get('APP_ENCRYPTION_KEY')
if not ENCRYPTION_KEY_STR:
    logger.warning("APP_ENCRYPTION_KEY not set, generating default key for testing.")
    ENCRYPTION_KEY = Fernet.generate_key()
else:
    ENCRYPTION_KEY = ENCRYPTION_KEY_STR.encode()

STAGING_PG_DSN = os.environ.get('STAGING_PG_DSN')

# Database initialization
def init_db():
    conn = get_db()
    if not conn:
        logger.error("DB connection failed, aborting initialization.")
        return
    try:
        is_sqlite = os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite'
        pk_type = 'INTEGER PRIMARY KEY AUTOINCREMENT' if is_sqlite else 'SERIAL PRIMARY KEY'
        ts_type = 'TIMESTAMP' if is_sqlite else 'TIMESTAMP WITH TIME ZONE'
        with conn:
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS clients (
                client_id {pk_type},
                client_name TEXT NOT NULL UNIQUE,
                created_at {ts_type} DEFAULT CURRENT_TIMESTAMP
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS configs (
                config_id {pk_type},
                client_id INTEGER NOT NULL,
                config_type TEXT NOT NULL,
                config_key TEXT NOT NULL,
                config_value TEXT,
                last_modified {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS audit_logs (
                log_id {pk_type},
                client_id INTEGER,
                user_id TEXT,
                action TEXT NOT NULL,
                details TEXT,
                timestamp {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS ai_providers (
                provider_id {pk_type},
                name TEXT NOT NULL UNIQUE,
                api_endpoint TEXT NOT NULL,
                default_model TEXT,
                key_url TEXT,
                notes TEXT
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS ora2pg_config_options (
                option_id {pk_type},
                option_name TEXT NOT NULL UNIQUE,
                option_type TEXT NOT NULL,
                default_value TEXT,
                description TEXT,
                allowed_values TEXT
            )''')
        load_ai_providers(conn)
        load_ora2pg_config(conn)
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error during DB initialization: {e}")

with app.app_context():
    init_db()

# Error handlers
@app.errorhandler(405)
def method_not_allowed(e):
    logger.error(f"405 Method Not Allowed: {request.method} {request.url}")
    return jsonify({'error': 'Method not allowed'}), 405

@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Unhandled error: {str(e)}")
    return jsonify({'error': 'An unexpected error occurred'}), 500

# Routes
@app.route('/', methods=['GET'])
def index():
    return render_template('login.html')

@app.route('/configurator', methods=['GET'])
@token_required
def configurator():
    return render_template('configurator.html')

@app.route('/comparison', methods=['GET'])
@token_required
def comparison():
    return render_template('comparison.html')

@app.route('/api/login', methods=['POST'])
def login_route():
    return login()

@app.route('/api/ai_providers', methods=['GET'])
@token_required
def get_ai_providers():
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = execute_query(conn, 'SELECT * FROM ai_providers')
        providers = [dict(row) for row in cursor.fetchall()]
        return jsonify(providers)
    except Exception as e:
        logger.error(f"Error fetching AI providers: {e}")
        return jsonify({'error': 'Failed to fetch AI providers'}), 500

@app.route('/api/ora2pg_config_options', methods=['GET'])
@token_required
def get_ora2pg_config_options():
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = execute_query(conn, 'SELECT * FROM ora2pg_config_options')
        options = [dict(row) for row in cursor.fetchall()]
        return jsonify(options)
    except Exception as e:
        logger.error(f"Error fetching Ora2Pg config options: {e}")
        return jsonify({'error': 'Failed to fetch Ora2Pg config options'}), 500

@app.route('/api/clients', methods=['GET', 'POST'])
@token_required
def manage_clients():
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    if request.method == 'GET':
        try:
            cursor = execute_query(conn, 'SELECT client_id, client_name, created_at as last_modified FROM clients')
            clients = [dict(row) for row in cursor.fetchall()]
            return jsonify(clients)
        except Exception as e:
            logger.error(f"Error fetching clients: {e}")
            return jsonify({'error': 'Failed to fetch clients'}), 500
    elif request.method == 'POST':
        client_name = request.json.get('client_name')
        if not client_name:
            return jsonify({'error': 'Client name is required'}), 400
        try:
            with conn:
                if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                    cursor = execute_query(conn, 'INSERT INTO clients (client_name) VALUES (?)', (client_name,))
                    client_id = cursor.lastrowid
                else:
                    cursor = execute_query(conn, 'INSERT INTO clients (client_name) VALUES (%s) RETURNING client_id', (client_name,))
                    client_id = cursor.fetchone()['client_id']
                cursor = execute_query(conn, 'SELECT client_id, client_name, created_at as last_modified FROM clients WHERE client_id = ?', (client_id,))
                new_client = dict(cursor.fetchone())
                conn.commit()
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
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    if request.method == 'GET':
        try:
            cursor = execute_query(conn, 'SELECT config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
            rows = cursor.fetchall()
            config = {
                'oracle_dsn': '', 'oracle_user': '', 'oracle_pwd': '', 'type': 'TABLE', 'dump_as_html': True,
                'export_schema': False, 'create_schema': True, 'compile_schema': True, 'debug': False,
                'ai_provider': 'OpenAI (ChatGPT)', 'ai_endpoint': 'https://api.openai.com/v1/', 'ai_model': 'gpt-4o',
                'ai_api_key': '', 'ai_temperature': 0.7, 'ai_max_output_tokens': 4096, 'ai_run_integrated': True
            }
            for row in rows:
                key, value = row['config_key'], row['config_value']
                if key in ['dump_as_html', 'export_schema', 'create_schema', 'compile_schema', 'debug', 'ai_run_integrated']:
                    config[key] = value.lower() in ('true', '1')
                elif key in ['ai_temperature', 'ai_max_output_tokens']:
                    try:
                        config[key] = float(value) if '.' in value else int(value)
                    except (ValueError, TypeError):
                        pass
                else:
                    config[key] = value
            log_audit(client_id, 'get_config', 'Loaded configuration')
            return jsonify(config)
        except Exception as e:
            logger.error(f"Error fetching config: {e}")
            return jsonify({'error': 'Failed to fetch config'}), 500
    elif request.method == 'POST':
        config_data = request.get_json()
        if not config_data:
            return jsonify({'error': 'No configuration data provided'}), 400
        fernet = Fernet(ENCRYPTION_KEY)
        try:
            with conn:
                for key, value in config_data.items():
                    if key in ['oracle_pwd', 'ai_api_key'] and value:
                        try:
                            fernet.decrypt(value.encode())
                        except Exception:
                            value = fernet.encrypt(str(value).encode()).decode()
                    cursor = conn.cursor()
                    param_style_query = 'SELECT config_id FROM configs WHERE client_id = ? AND config_key = ?'
                    params = (client_id, key)
                    if os.environ.get('DB_BACKEND', 'sqlite') == 'postgresql':
                        param_style_query = param_style_query.replace('?', '%s')
                    cursor.execute(param_style_query, params)
                    exists = cursor.fetchone()
                    if exists:
                        execute_query(conn, 'UPDATE configs SET config_value = ?, last_modified = CURRENT_TIMESTAMP WHERE client_id = ? AND config_key = ?', (str(value), client_id, key))
                    else:
                        execute_query(conn, 'INSERT INTO configs (client_id, config_type, config_key, config_value, last_modified) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)', (client_id, 'ora2pg', key, str(value)))
                conn.commit()
            log_audit(client_id, 'save_config', 'Saved configuration')
            return jsonify({'message': 'Configuration saved successfully'})
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            return jsonify({'error': f'Failed to save config: {str(e)}'}), 500

@app.route('/api/client/<int:client_id>/export_config', methods=['GET'])
@token_required
def export_config(client_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = execute_query(conn, 'SELECT config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
        config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
        fernet = Fernet(ENCRYPTION_KEY)
        for key in ['oracle_pwd', 'ai_api_key']:
            if key in config and config[key]:
                try:
                    config[key] = fernet.decrypt(config[key].encode()).decode()
                except Exception:
                    logger.warning(f"Failed to decrypt {key}, treating as plain text.")
        config_content = "# Ora2Pg Configuration File\n"
        for key in ['oracle_dsn', 'oracle_user', 'oracle_pwd', 'type', 'dump_as_html', 'export_schema', 'create_schema', 'compile_schema', 'debug']:
            if key in config:
                config_key = key.upper()
                value = config[key]
                if isinstance(value, bool):
                    value = '1' if value else '0'
                config_content += f"{config_key}\t{value}\n"
        log_audit(client_id, 'export_config', 'Generated ora2pg.conf')
        return jsonify({'config_content': config_content})
    except Exception as e:
        logger.error(f"Error generating ora2pg.conf: {e}")
        return jsonify({'error': f'Failed to generate config: {str(e)}'}), 500

@app.route('/api/load_file', methods=['POST'])
@token_required
def load_sql_file():
    data = request.json
    filename, client_id = data.get('filename'), data.get('client_id')
    if not filename or not client_id:
        return jsonify({'error': 'Filename and client ID are required'}), 400
    output_dir = '/app/output'
    safe_filename = os.path.basename(filename)
    file_path = os.path.join(output_dir, safe_filename)
    if not os.path.abspath(file_path).startswith(os.path.abspath(output_dir)):
        return jsonify({'error': 'Invalid filename'}), 400
    if not os.path.exists(file_path):
        logger.error(f"File not found at path: {file_path}")
        return jsonify({'error': f'File not found: {safe_filename}'}), 404
    try:
        conn = get_db()
        cursor = execute_query(conn, 'SELECT config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
        config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
        fernet = Fernet(ENCRYPTION_KEY)
        for key in ['oracle_pwd', 'ai_api_key']:
            if key in config and config[key]:
                try:
                    config[key] = fernet.decrypt(config[key].encode()).decode()
                except Exception:
                    logger.warning(f"Failed to decrypt {key}, treating as plain text.")
        corrector = Ora2PgAICorrector(
            ora2pg_path='ora2pg',
            output_dir=output_dir,
            ai_settings={
                'ai_provider': config.get('ai_provider', 'OpenAI (ChatGPT)'),
                'ai_endpoint': config.get('ai_endpoint', 'https://api.openai.com/v1/'),
                'ai_model': config.get('ai_model', 'gpt-4o'),
                'ai_api_key': config.get('ai_api_key', ''),
                'ai_temperature': float(config.get('ai_temperature', 0.7)),
                'ai_max_output_tokens': int(config.get('ai_max_output_tokens', 4096))
            },
            encryption_key=ENCRYPTION_KEY
        )
        result = corrector.load_sql_file(safe_filename)
        if not result['sql_content']:
            return jsonify({'error': result['logs']}), 500
        corrected_sql, metrics = corrector.ai_correct_sql(result['sql_content'])
        log_audit(client_id, 'load_sql_file', f'Loaded and corrected SQL from file: {safe_filename}')
        return jsonify({
            'logs': result['logs'],
            'original_sql': result['sql_content'],
            'corrected_sql': corrected_sql,
            'metrics': metrics
        })
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return jsonify({'error': f'Failed to process file: {str(e)}'}), 500

@app.route('/api/validate', methods=['POST'])
@token_required
def validate_sql():
    data = request.json
    sql_to_validate = data.get('sql')
    client_id = data.get('client_id')
    if not sql_to_validate or not client_id:
        return jsonify({'error': 'SQL and client ID are required'}), 400
    if not STAGING_PG_DSN:
        return jsonify({'message': 'Staging database not configured. Skipping validation.', 'status': 'skipped'})
    conn = get_db()
    cursor = execute_query(conn, 'SELECT config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
    config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
    fernet = Fernet(ENCRYPTION_KEY)
    for key in ['oracle_pwd', 'ai_api_key']:
        if key in config and config[key]:
            try:
                config[key] = fernet.decrypt(config[key].encode()).decode()
            except Exception:
                logger.warning(f"Failed to decrypt {key}, treating as plain text.")
    corrector = Ora2PgAICorrector(
        ora2pg_path='ora2pg',
        output_dir='/app/output',
        ai_settings={
            'ai_provider': config.get('ai_provider', 'OpenAI (ChatGPT)'),
            'ai_endpoint': config.get('ai_endpoint', 'https://api.openai.com/v1/'),
            'ai_model': config.get('ai_model', 'gpt-4o'),
            'ai_api_key': config.get('ai_api_key', ''),
            'ai_temperature': float(config.get('ai_temperature', 0.7)),
            'ai_max_output_tokens': int(config.get('ai_max_output_tokens', 4096))
        },
        encryption_key=ENCRYPTION_KEY
    )
    try:
        is_valid, message = corrector.validate_sql(sql_to_validate, STAGING_PG_DSN)
        log_audit(client_id, 'validate_sql', f'Validation result: {is_valid}')
        return jsonify({'message': message, 'status': 'success' if is_valid else 'error'})
    except Exception as e:
        logger.error(f"Error validating SQL: {e}")
        return jsonify({'error': f'Failed to validate SQL: {str(e)}'}), 500

@app.route('/api/save', methods=['POST'])
@token_required
def save_sql():
    data = request.json
    original_sql = data.get('original_sql')
    corrected_sql = data.get('corrected_sql')
    client_id = data.get('client_id')
    if not corrected_sql or not client_id:
        return jsonify({'error': 'Corrected SQL and client ID are required'}), 400
    conn = get_db()
    cursor = execute_query(conn, 'SELECT config_key, config_value FROM configs WHERE client_id = ?', (client_id,))
    config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
    fernet = Fernet(ENCRYPTION_KEY)
    for key in ['oracle_pwd', 'ai_api_key']:
        if key in config and config[key]:
            try:
                config[key] = fernet.decrypt(config[key].encode()).decode()
            except Exception:
                logger.warning(f"Failed to decrypt {key}, treating as plain text.")
    corrector = Ora2PgAICorrector(
        ora2pg_path='ora2pg',
        output_dir='/app/output',
        ai_settings={
            'ai_provider': config.get('ai_provider', 'OpenAI (ChatGPT)'),
            'ai_endpoint': config.get('ai_endpoint', 'https://api.openai.com/v1/'),
            'ai_model': config.get('ai_model', 'gpt-4o'),
            'ai_api_key': config.get('ai_api_key', ''),
            'ai_temperature': float(config.get('ai_temperature', 0.7)),
            'ai_max_output_tokens': int(config.get('ai_max_output_tokens', 4096))
        },
        encryption_key=ENCRYPTION_KEY
    )
    try:
        output_path = corrector.save_corrected_file(original_sql, corrected_sql)
        log_audit(client_id, 'save_file', f'Saved corrected SQL to {output_path}')
        return jsonify({'message': f'Successfully saved corrected file to {output_path}'})
    except Exception as e:
        logger.error(f"Error saving SQL: {e}")
        return jsonify({'error': f'Failed to save SQL: {str(e)}'}), 500

@app.route('/logout', methods=['GET'])
@token_required
def logout():
    localStorage.removeItem('token');
    return redirect('/')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)

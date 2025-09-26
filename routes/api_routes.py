from flask import Blueprint, request, jsonify
from modules.db import get_db
from modules.auth import token_required
from modules.audit import log_audit
from modules.sql_processing import Ora2PgAICorrector
from cryptography.fernet import Fernet
import os
import sqlite3
import psycopg2

api_bp = Blueprint('api_bp', __name__)

ENCRYPTION_KEY_STR = os.environ.get('APP_ENCRYPTION_KEY')
if not ENCRYPTION_KEY_STR:
    ENCRYPTION_KEY = Fernet.generate_key()
else:
    ENCRYPTION_KEY = ENCRYPTION_KEY_STR.encode()

STAGING_PG_DSN = os.environ.get('STAGING_PG_DSN')

@api_bp.route('/api/ai_providers', methods=['GET'])
@token_required
def get_ai_providers():
    from modules.db import execute_query
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = execute_query(conn, 'SELECT * FROM ai_providers')
        providers = [dict(row) for row in cursor.fetchall()]
        return jsonify(providers)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch AI providers: {e}'}), 500

@api_bp.route('/api/ora2pg_config_options', methods=['GET'])
@token_required
def get_ora2pg_config_options():
    from modules.db import execute_query
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = execute_query(conn, 'SELECT * FROM ora2pg_config_options')
        options = [dict(row) for row in cursor.fetchall()]
        return jsonify(options)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch Ora2Pg config options: {e}'}), 500

@api_bp.route('/api/clients', methods=['GET', 'POST'])
@token_required
def manage_clients():
    from modules.db import execute_query
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    if request.method == 'GET':
        try:
            cursor = execute_query(conn, 'SELECT client_id, client_name, created_at as last_modified FROM clients')
            clients = [dict(row) for row in cursor.fetchall()]
            return jsonify(clients)
        except Exception as e:
            return jsonify({'error': f'Failed to fetch clients: {e}'}), 500
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
                
                # Fetch the newly created client to return it
                select_query = 'SELECT client_id, client_name, created_at as last_modified FROM clients WHERE client_id = ?'
                params = (client_id,)
                if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                    select_query = select_query.replace('?', '%s')

                cursor = execute_query(conn, select_query, params)
                new_client = dict(cursor.fetchone())
                conn.commit()
                log_audit(new_client['client_id'], 'create_client', f'Created client: {client_name}')
                return jsonify(new_client), 201
        except (sqlite3.IntegrityError, psycopg2.IntegrityError):
            return jsonify({'error': 'Client name already exists'}), 409
        except Exception as e:
            return jsonify({'error': f'An internal error occurred: {e}'}), 500

@api_bp.route('/api/client/<int:client_id>/config', methods=['GET', 'POST'])
@token_required
def manage_config(client_id):
    from modules.db import execute_query
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    if request.method == 'GET':
        try:
            query = 'SELECT config_key, config_value FROM configs WHERE client_id = ?'
            params = (client_id,)
            if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                query = query.replace('?', '%s')
            cursor = execute_query(conn, query, params)
            rows = cursor.fetchall()
            config = {
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
            return jsonify({'error': f'Failed to fetch config: {e}'}), 500
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
                    
                    param_style_query = 'SELECT config_id FROM configs WHERE client_id = ? AND config_key = ?'
                    params = (client_id, key)
                    if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                        param_style_query = param_style_query.replace('?', '%s')
                    
                    cursor = conn.cursor()
                    cursor.execute(param_style_query, params)
                    exists = cursor.fetchone()

                    if exists:
                        update_query = 'UPDATE configs SET config_value = ?, last_modified = CURRENT_TIMESTAMP WHERE client_id = ? AND config_key = ?'
                        update_params = (str(value), client_id, key)
                        if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                            update_query = update_query.replace('?', '%s')
                        execute_query(conn, update_query, update_params)
                    else:
                        insert_query = 'INSERT INTO configs (client_id, config_type, config_key, config_value, last_modified) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)'
                        insert_params = (client_id, 'ora2pg', key, str(value))
                        if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                           insert_query = insert_query.replace('?', '%s')
                        execute_query(conn, insert_query, insert_params)
                conn.commit()
            log_audit(client_id, 'save_config', 'Saved configuration')
            return jsonify({'message': 'Configuration saved successfully'})
        except Exception as e:
            return jsonify({'error': f'Failed to save config: {str(e)}'}), 500

@api_bp.route('/api/load_file', methods=['POST'])
@token_required
def load_sql_file():
    from modules.db import execute_query
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
        return jsonify({'error': f'File not found: {safe_filename}'}), 404
    
    try:
        conn = get_db()
        query = 'SELECT config_key, config_value FROM configs WHERE client_id = ?'
        params = (client_id,)
        if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
            query = query.replace('?', '%s')
        
        cursor = execute_query(conn, query, params)
        config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
        
        fernet = Fernet(ENCRYPTION_KEY)
        for key in ['oracle_pwd', 'ai_api_key']:
            if key in config and config[key]:
                try:
                    config[key] = fernet.decrypt(config[key].encode()).decode()
                except Exception:
                    pass
        
        corrector = Ora2PgAICorrector(
            output_dir=output_dir,
            ai_settings={
                'ai_provider': config.get('ai_provider'),
                'ai_endpoint': config.get('ai_endpoint'),
                'ai_model': config.get('ai_model'),
                'ai_api_key': config.get('ai_api_key'),
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
        return jsonify({'error': f'Failed to process file: {str(e)}'}), 500

@api_bp.route('/api/validate', methods=['POST'])
@token_required
def validate_sql():
    from modules.db import execute_query
    data = request.json
    sql_to_validate, client_id = data.get('sql'), data.get('client_id')
    if not sql_to_validate or not client_id:
        return jsonify({'error': 'SQL and client ID are required'}), 400
    if not STAGING_PG_DSN:
        return jsonify({'message': 'Staging database not configured. Skipping validation.', 'status': 'skipped'})
    
    conn = get_db()
    query = 'SELECT config_key, config_value FROM configs WHERE client_id = ?'
    params = (client_id,)
    if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
        query = query.replace('?', '%s')
    cursor = execute_query(conn, query, params)
    config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
    
    corrector = Ora2PgAICorrector(output_dir='/app/output', ai_settings={}, encryption_key=ENCRYPTION_KEY)
    
    try:
        is_valid, message = corrector.validate_sql(sql_to_validate, STAGING_PG_DSN)
        log_audit(client_id, 'validate_sql', f'Validation result: {is_valid}')
        return jsonify({'message': message, 'status': 'success' if is_valid else 'error'})
    except Exception as e:
        return jsonify({'error': f'Failed to validate SQL: {str(e)}'}), 500

@api_bp.route('/api/save', methods=['POST'])
@token_required
def save_sql():
    data = request.json
    original_sql, corrected_sql, client_id = data.get('original_sql'), data.get('corrected_sql'), data.get('client_id')
    if not corrected_sql or not client_id:
        return jsonify({'error': 'Corrected SQL and client ID are required'}), 400
    
    corrector = Ora2PgAICorrector(output_dir='/app/output', ai_settings={}, encryption_key=ENCRYPTION_KEY)
    
    try:
        output_path = corrector.save_corrected_file(original_sql, corrected_sql)
        log_audit(client_id, 'save_file', f'Saved corrected SQL to {output_path}')
        return jsonify({'message': f'Successfully saved corrected file to {output_path}'})
    except Exception as e:
        return jsonify({'error': f'Failed to save SQL: {str(e)}'}), 500

from flask import Blueprint, request, jsonify
from modules.db import get_db, execute_query
from modules.audit import log_audit
from modules.sql_processing import Ora2PgAICorrector
from cryptography.fernet import Fernet
import os
import sqlite3
import psycopg2

api_bp = Blueprint('api_bp', __name__, url_prefix='/api')

ENCRYPTION_KEY_STR = os.environ.get('APP_ENCRYPTION_KEY')
if not ENCRYPTION_KEY_STR:
    ENCRYPTION_KEY = Fernet.generate_key()
else:
    ENCRYPTION_KEY = ENCRYPTION_KEY_STR.encode()

@api_bp.route('/app_settings', methods=['GET'])
def get_app_settings():
    """Returns application-level settings to the frontend."""
    settings = {
        'staging_pg_dsn': os.environ.get('STAGING_PG_DSN', '')
    }
    return jsonify(settings)

@api_bp.route('/ai_providers', methods=['GET'])
def get_ai_providers():
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = execute_query(conn, 'SELECT * FROM ai_providers')
        providers = [dict(row) for row in cursor.fetchall()]
        return jsonify(providers)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch AI providers: {e}'}), 500

@api_bp.route('/ora2pg_config_options', methods=['GET'])
def get_ora2pg_config_options():
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = execute_query(conn, 'SELECT * FROM ora2pg_config_options')
        options = [dict(row) for row in cursor.fetchall()]
        return jsonify(options)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch Ora2Pg config options: {e}'}), 500

@api_bp.route('/clients', methods=['GET', 'POST'])
def manage_clients():
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    if request.method == 'GET':
        try:
            cursor = execute_query(conn, 'SELECT client_id, client_name, created_at as last_modified FROM clients ORDER BY client_name')
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

@api_bp.route('/client/<int:client_id>/config', methods=['GET', 'POST'])
def manage_config(client_id):
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
            config = {}
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

                    update_query = 'UPDATE configs SET config_value = ? WHERE client_id = ? AND config_key = ?'
                    insert_query = 'INSERT INTO configs (client_id, config_type, config_key, config_value) VALUES (?, ?, ?, ?)'
                    
                    if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                        update_query = update_query.replace('?', '%s')
                        insert_query = insert_query.replace('?', '%s')

                    if exists:
                        execute_query(conn, update_query, (str(value), client_id, key))
                    else:
                        execute_query(conn, insert_query, (client_id, 'ora2pg', key, str(value)))
                conn.commit()
            log_audit(client_id, 'save_config', 'Saved configuration')
            return jsonify({'message': 'Configuration saved successfully'})
        except Exception as e:
            return jsonify({'error': f'Failed to save config: {str(e)}'}), 500

@api_bp.route('/correct_sql', methods=['POST'])
def correct_sql_with_ai():
    data = request.json
    sql, client_id = data.get('sql'), data.get('client_id')
    if not sql or not client_id:
        return jsonify({'error': 'SQL content and client ID are required'}), 400

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
            output_dir='/app/output',
            ai_settings={
                'ai_provider': config.get('ai_provider'),
                'ai_endpoint': config.get('ai_endpoint'),
                'ai_model': config.get('ai_model'),
                'ai_api_key': config.get('ai_api_key'),
                'ai_temperature': float(config.get('ai_temperature', 0.2)),
                'ai_max_output_tokens': int(config.get('ai_max_output_tokens', 8192))
            },
            encryption_key=ENCRYPTION_KEY
        )
        
        corrected_sql, metrics = corrector.ai_correct_sql(sql)
        log_audit(client_id, 'correct_sql_with_ai', f'AI correction performed.')
        return jsonify({
            'corrected_sql': corrected_sql,
            'metrics': metrics
        })
    except Exception as e:
        return jsonify({'error': f'Failed to correct SQL with AI: {str(e)}'}), 500

@api_bp.route('/validate', methods=['POST'])
def validate_sql():
    data = request.json
    sql_to_validate, client_id = data.get('sql'), data.get('client_id')
    if not sql_to_validate or not client_id:
        return jsonify({'error': 'SQL and client ID are required'}), 400
    
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

        validation_dsn = config.get('validation_pg_dsn')
        if not validation_dsn:
            return jsonify({'message': 'Validation database not configured in client settings.', 'status': 'skipped'})

        corrector = Ora2PgAICorrector(
            output_dir='/app/output',
            ai_settings={
                'ai_provider': config.get('ai_provider'),
                'ai_endpoint': config.get('ai_endpoint'),
                'ai_model': config.get('ai_model'),
                'ai_api_key': config.get('ai_api_key'),
                'ai_temperature': float(config.get('ai_temperature', 0.2)),
                'ai_max_output_tokens': int(config.get('ai_max_output_tokens', 8192))
            },
            encryption_key=ENCRYPTION_KEY
        )
        
        is_valid, message, new_sql = corrector.validate_sql(sql_to_validate, validation_dsn)
        log_audit(client_id, 'validate_sql', f'Validation result: {is_valid} - {message}')
        return jsonify({'message': message, 'status': 'success' if is_valid else 'error', 'corrected_sql': new_sql})
    except Exception as e:
        return jsonify({'error': f'Failed to validate SQL: {str(e)}'}), 500

@api_bp.route('/save', methods=['POST'])
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

@api_bp.route('/client/<int:client_id>/audit_logs', methods=['GET'])
def get_audit_logs(client_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        query = 'SELECT timestamp, action, details FROM audit_logs WHERE client_id = ? ORDER BY timestamp DESC'
        params = (client_id,)
        if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
            query = query.replace('?', '%s')
        
        cursor = execute_query(conn, query, params)
        logs = [dict(row) for row in cursor.fetchall()]
        return jsonify(logs)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch audit logs: {e}'}), 500

@api_bp.route('/client/<int:client_id>/log_audit', methods=['POST'])
def log_audit_event(client_id):
    data = request.json
    action, details = data.get('action'), data.get('details')
    if not action:
        return jsonify({'error': 'Action is required for audit log'}), 400
    try:
        log_audit(client_id, action, details)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': f'Failed to log audit event: {e}'}), 500


from flask import Blueprint, request, jsonify, session
from modules.db import get_db, execute_query, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from modules.audit import log_audit
from modules.sql_processing import Ora2PgAICorrector
from modules.orchestrator import MigrationOrchestrator
from cryptography.fernet import Fernet
import os
import sqlite3
import psycopg2
import logging
import json
import threading

logger = logging.getLogger(__name__)

api_bp = Blueprint('api_bp', __name__, url_prefix='/api')

# Track running migrations for status polling
_running_migrations = {}

@api_bp.route('/app_settings', methods=['GET'])
def get_app_settings():
    """Returns application-level settings to the frontend."""
    settings = {
        'validation_pg_dsn': os.environ.get('VALIDATION_PG_DSN', '')
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
            config = {}
            for row in cursor.fetchall():
                key, value = row['config_key'], row['config_value']
                if key in ['dump_as_html', 'export_schema', 'create_schema', 'compile_schema', 'debug', 'file_per_table']:
                    config[key] = str(value) in ('1', 'true', 'True')
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
                        if key.startswith('ai_'):
                            config_type = 'ai'
                        elif key == 'validation_pg_dsn':
                            config_type = 'validation'
                        else:
                            config_type = 'ora2pg'
                        
                        execute_query(conn, insert_query, (client_id, config_type, key, str(value)))
                conn.commit()
            log_audit(client_id, 'save_config', 'Saved configuration')
            return jsonify({'message': 'Configuration saved successfully'})
        except Exception as e:
            return jsonify({'error': f'Failed to save config: {str(e)}'}), 500

@api_bp.route('/file/<int:file_id>/status', methods=['POST'])
def update_file_status(file_id):
    data = request.get_json()
    new_status = data.get('status')

    if not new_status:
        return jsonify({'error': 'New status is required.'}), 400

    allowed_statuses = ['generated', 'corrected', 'validated', 'failed']
    if new_status not in allowed_statuses:
        return jsonify({'error': f'Invalid status. Must be one of: {", ".join(allowed_statuses)}'}), 400

    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        with conn:
            query = 'UPDATE migration_files SET status = ? WHERE file_id = ?'
            params = (new_status, file_id)
            if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                query = query.replace('?', '%s')
            
            cursor = execute_query(conn, query, params)
            
            if cursor.rowcount == 0:
                return jsonify({'error': 'File not found.'}), 404
            
            conn.commit()
            return jsonify({'message': f'Status for file {file_id} updated to {new_status}.'})
    except Exception as e:
        logger.error(f"Failed to update status for file {file_id}: {e}", exc_info=True)
        return jsonify({'error': 'Failed to update file status.'}), 500

@api_bp.route('/client/<int:client_id>/sessions', methods=['GET'])
def get_sessions(client_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        query = 'SELECT session_id, session_name, created_at, export_type FROM migration_sessions WHERE client_id = ? ORDER BY created_at DESC'
        params = (client_id,)
        if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
            query = query.replace('?', '%s')
        
        cursor = execute_query(conn, query, params)
        sessions = [dict(row) for row in cursor.fetchall()]
        return jsonify(sessions)
    except Exception as e:
        logger.error(f"Failed to fetch sessions for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch sessions.'}), 500

@api_bp.route('/session/<int:session_id>/files', methods=['GET'])
def get_session_files(session_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        query = 'SELECT file_id, filename, status, last_modified FROM migration_files WHERE session_id = ? ORDER BY filename'
        params = (session_id,)
        if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
            query = query.replace('?', '%s')

        cursor = execute_query(conn, query, params)
        files = [dict(row) for row in cursor.fetchall()]
        return jsonify(files)
    except Exception as e:
        logger.error(f"Failed to fetch files for session {session_id}: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch session files.'}), 500

@api_bp.route('/client/<int:client_id>/test_ora2pg_connection', methods=['POST'])
def test_ora2pg_connection(client_id):
    config = request.get_json(force=True)
    if not config:
        return jsonify({'status': 'error', 'message': 'No configuration data provided for test.'}), 400
    try:
        fernet = Fernet(ENCRYPTION_KEY)
        for key in ['oracle_pwd']:
            if key in config and config[key]:
                try:
                    config[key] = fernet.decrypt(config[key].encode()).decode()
                except Exception:
                    pass
        
        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        report_args = ['-t', 'SHOW_VERSION']
        version_output, error_output = corrector.run_ora2pg_export(client_id, get_db(), config, extra_args=report_args)

        if error_output:
            log_audit(client_id, 'test_oracle_connection', f'Failed: {error_output}')
            return jsonify({'status': 'error', 'message': f'Connection failed: {error_output}'}), 400
        
        version_string = version_output.get('sql_output', '').strip()
        log_audit(client_id, 'test_oracle_connection', f'Success: {version_string}')
        return jsonify({'status': 'success', 'message': f'Connection successful! {version_string}'})

    except Exception as e:
        logger.error(f"Failed to test Oracle connection for client {client_id}: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'An unexpected error occurred: {str(e)}'}), 500

@api_bp.route('/client/<int:client_id>/get_object_list', methods=['GET'])
def get_object_list(client_id):
    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        object_list, error = corrector._get_object_list(conn, config)

        if error:
            log_audit(client_id, 'get_object_list', f'Failed: {error}')
            return jsonify({'error': error}), 500

        log_audit(client_id, 'get_object_list', f'Successfully fetched {len(object_list)} objects.')
        return jsonify(object_list)

    except Exception as e:
        logger.error(f"Failed to get object list for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500

@api_bp.route('/client/<int:client_id>/get_oracle_ddl', methods=['POST'])
def get_oracle_ddl(client_id):
    data = request.get_json()
    object_name = data.get('object_name')
    object_type = data.get('object_type', 'TABLE')
    pretty = data.get('pretty', False)

    if not object_name:
        return jsonify({'error': 'Object name is required.'}), 400

    try:
        config = get_client_config(client_id, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        ddl, error = corrector.get_oracle_ddl(config, object_type, object_name, pretty=pretty)

        if error:
            return jsonify({'error': error}), 500

        return jsonify({'ddl': ddl})

    except Exception as e:
        logger.error(f"Failed to fetch DDL for {object_name}: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500

@api_bp.route('/client/<int:client_id>/get_bulk_oracle_ddl', methods=['POST'])
def get_bulk_oracle_ddl(client_id):
    data = request.get_json()
    objects = data.get('objects')
    pretty = data.get('pretty', False)

    if not objects:
        return jsonify({'error': 'A list of objects is required.'}), 400

    try:
        config = get_client_config(client_id, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)

        all_ddls = []
        for obj in objects:
            ddl, error = corrector.get_oracle_ddl(config, obj['type'], obj['name'], pretty=pretty)
            if error:
                logger.warning(f"Could not fetch DDL for {obj['name']}: {error}")
                all_ddls.append(f"-- FAILED to retrieve DDL for {obj['type']} {obj['name']}: {error}\n\n")
            else:
                all_ddls.append(ddl + "\n\n")

        concatenated_ddl = "".join(all_ddls)
        return jsonify({'ddl': concatenated_ddl})

    except Exception as e:
        logger.error(f"Failed to fetch bulk DDL: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500

@api_bp.route('/client/<int:client_id>/generate_report', methods=['POST'])
def generate_report(client_id):
    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        report_args = ['-t', 'SHOW_REPORT', '--estimate_cost', '--dump_as_json']
        report_output, error_output = corrector.run_ora2pg_export(client_id, conn, config, extra_args=report_args)

        if error_output:
            log_audit(client_id, 'generate_report', f'Failed: {error_output}')
            return jsonify({'error': error_output}), 500

        json_string = report_output.get('sql_output', '{}')
        try:
            report_data = json.loads(json_string)
        except json.JSONDecodeError:
            log_audit(client_id, 'generate_report', 'Failed: Invalid JSON from ora2pg report.')
            return jsonify({'error': 'Failed to parse JSON report from Ora2Pg.'}), 500

        log_audit(client_id, 'generate_report', 'Successfully generated assessment report.')
        return jsonify(report_data)

    except Exception as e:
        logger.error(f"Failed to generate report for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500

@api_bp.route('/client/<int:client_id>/run_ora2pg', methods=['POST'])
def run_ora2pg(client_id):
    try:
        conn = get_db()
        config = get_client_config(client_id, conn)

        # Apply request overrides
        request_data = request.get_json(silent=True) or {}
        if 'type' in request_data:
            config['type'] = request_data['type']
        if 'selected_objects' in request_data and request_data['selected_objects']:
            config['ALLOW'] = ','.join(request_data['selected_objects'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        result_data, error_output = corrector.run_ora2pg_export(client_id, conn, config)

        if error_output:
            log_audit(client_id, 'run_ora2pg', f'Failed: {error_output}')
            return jsonify({'error': error_output}), 500

        log_audit(client_id, 'run_ora2pg', 'Successfully executed Ora2Pg export.')
        return jsonify(result_data)

    except Exception as e:
        logger.error(f"Failed to run Ora2Pg for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500
        
@api_bp.route('/get_exported_file', methods=['POST'])
def get_exported_file():
    file_path = None
    try:
        data = request.json
        file_id = data.get('file_id')
        if not file_id:
            return jsonify({'error': 'File ID is required.'}), 400

        conn = get_db()
        file_query = """
            SELECT mf.filename, ms.export_directory 
            FROM migration_files mf
            JOIN migration_sessions ms ON mf.session_id = ms.session_id
            WHERE mf.file_id = ?
        """
        params = (file_id,)
        if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
            file_query = file_query.replace('?', '%s')

        cursor = execute_query(conn, file_query, params)
        file_info = cursor.fetchone()

        if not file_info:
            return jsonify({'error': 'File record not found in database.'}), 404

        filename = file_info['filename']
        export_dir = file_info['export_directory']
        file_path = os.path.join(export_dir, filename)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return jsonify({'content': content, 'filename': filename})

    except FileNotFoundError:
        logger.error(f"File not found at persistent path: {file_path}")
        return jsonify({'error': f'File not found on the server filesystem.'}), 404
    except Exception as e:
        logger.error(f"Error reading exported file {file_path}: {e}", exc_info=True)
        return jsonify({'error': 'An unexpected error occurred while reading the file content.'}), 500

@api_bp.route('/correct_sql', methods=['POST'])
def correct_sql_with_ai():
    data = request.json
    sql = data.get('sql')
    client_id = data.get('client_id')
    source_dialect = data.get('source_dialect', 'oracle')

    if not sql or not client_id:
        return jsonify({'error': 'SQL content and client ID are required'}), 400

    try:
        config = get_client_config(client_id)

        corrector = Ora2PgAICorrector(
            output_dir='/app/output',
            ai_settings=extract_ai_settings(config),
            encryption_key=ENCRYPTION_KEY
        )

        corrected_sql, metrics = corrector.ai_correct_sql(sql, source_dialect=source_dialect)

        log_audit(client_id, 'correct_sql_with_ai', f'AI conversion from {source_dialect} to PostgreSQL performed.')
        return jsonify({
            'corrected_sql': corrected_sql,
            'metrics': metrics
        })
    except Exception as e:
        logger.error(f"Failed to correct SQL with AI: {e}", exc_info=True)
        return jsonify({'error': f'Failed to correct SQL with AI: {str(e)}'}), 500

@api_bp.route('/validate', methods=['POST'])
def validate_sql():
    data = request.json
    sql_to_validate, client_id = data.get('sql'), data.get('client_id')
    clean_slate = data.get('clean_slate', False)
    auto_create_ddl = data.get('auto_create_ddl', True)

    if not sql_to_validate or not client_id:
        return jsonify({'error': 'SQL and client ID are required'}), 400

    try:
        config = get_client_config(client_id)

        validation_dsn = config.get('validation_pg_dsn')
        if not validation_dsn:
            return jsonify({'message': 'Validation database not configured in client settings.', 'status': 'skipped'})

        corrector = Ora2PgAICorrector(
            output_dir='/app/output',
            ai_settings=extract_ai_settings(config),
            encryption_key=ENCRYPTION_KEY
        )

        is_valid, message, new_sql = corrector.validate_sql(
            sql_to_validate,
            validation_dsn,
            clean_slate=clean_slate,
            auto_create_ddl=auto_create_ddl
        )

        audit_details = f'Validation result: {is_valid} - {message}'
        options = []
        if clean_slate: options.append('Clean Slate')
        if auto_create_ddl: options.append('Auto-create DDL')
        if options: audit_details += f" (Options: {', '.join(options)})"
        log_audit(client_id, 'validate_sql', audit_details)
        
        return jsonify({'message': message, 'status': 'success' if is_valid else 'error', 'corrected_sql': new_sql})
    except Exception as e:
        logger.error(f"Failed to validate SQL: {e}", exc_info=True)
        return jsonify({'error': f'Failed to validate SQL: {str(e)}'}), 500

@api_bp.route('/save', methods=['POST'])
def save_sql():
    data = request.json
    original_sql = data.get('original_sql')
    corrected_sql = data.get('corrected_sql')
    client_id = data.get('client_id')
    filename = data.get('filename', 'corrected_output.sql')

    if not corrected_sql or not client_id:
        return jsonify({'error': 'Corrected SQL and client ID are required'}), 400
    
    output_dir = os.environ.get('OUTPUT_DIR', '/app/output')

    corrector = Ora2PgAICorrector(output_dir=output_dir, ai_settings={}, encryption_key=ENCRYPTION_KEY)
    
    try:
        output_path = corrector.save_corrected_file(original_sql, corrected_sql, filename)
        log_audit(client_id, 'save_file', f'Saved corrected SQL to {output_path}')
        return jsonify({'message': f'Successfully saved file to {output_path}'})
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

@api_bp.route('/test_pg_connection', methods=['POST'])
def test_pg_connection():
    data = request.json
    pg_dsn = data.get('pg_dsn')
    if not pg_dsn:
        return jsonify({'error': 'PostgreSQL DSN is required'}), 400
    try:
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                pg_version = cursor.fetchone()[0]
        return jsonify({'status': 'success', 'message': f'Connection successful! PostgreSQL version: {pg_version}'})
    except psycopg2.OperationalError as e:
        logger.error(f"PostgreSQL connection test failed for DSN {pg_dsn}: {e}")
        return jsonify({'status': 'error', 'message': f'Connection failed: {e}'}), 400
    except Exception as e:
        logger.error(f"An unexpected error occurred during PostgreSQL connection test: {e}")
        return jsonify({'status': 'error', 'message': f'An unexpected error occurred: {e}'}), 500


@api_bp.route('/client/<int:client_id>', methods=['PUT', 'DELETE'])
def manage_single_client(client_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    if request.method == 'PUT':
        # Rename client
        data = request.get_json()
        new_name = data.get('client_name')
        if not new_name:
            return jsonify({'error': 'Client name is required'}), 400
        
        try:
            query = 'UPDATE clients SET client_name = ? WHERE client_id = ?'
            params = (new_name, client_id)
            if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                query = query.replace('?', '%s')
            
            with conn:
                execute_query(conn, query, params)
                conn.commit()
            
            log_audit(client_id, 'rename_client', f'Renamed to: {new_name}')
            return jsonify({'message': 'Client renamed successfully'})
        except Exception as e:
            return jsonify({'error': f'Failed to rename client: {str(e)}'}), 500
    
    elif request.method == 'DELETE':
        # Delete client and all associated data
        try:
            with conn:
                # Delete in order: audit_logs, migration_files, migration_sessions, configs, client
                queries = [
                    'DELETE FROM audit_logs WHERE client_id = ?',
                    'DELETE FROM migration_files WHERE session_id IN (SELECT session_id FROM migration_sessions WHERE client_id = ?)',
                    'DELETE FROM migration_sessions WHERE client_id = ?',
                    'DELETE FROM configs WHERE client_id = ?',
                    'DELETE FROM clients WHERE client_id = ?'
                ]
                
                if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
                    queries = [q.replace('?', '%s') for q in queries]
                
                for query in queries:
                    execute_query(conn, query, (client_id,))
                
                conn.commit()
            
            # Delete physical files if they exist
            import shutil
            client_dir = os.path.join('/app/project_data', str(client_id))
            if os.path.exists(client_dir):
                shutil.rmtree(client_dir)
            
            return jsonify({'message': 'Client deleted successfully'})
        except Exception as e:
            logger.error(f"Failed to delete client {client_id}: {e}", exc_info=True)
            return jsonify({'error': f'Failed to delete client: {str(e)}'}), 500


# =============================================================================
# One-Click Migration Orchestration Endpoints
# =============================================================================

def _run_migration_thread(client_id, options, app):
    """Background thread function to run migration with Flask app context."""
    with app.app_context():
        try:
            orchestrator = MigrationOrchestrator(client_id)
            _running_migrations[client_id] = orchestrator
            result = orchestrator.run_full_migration(options)
            # Keep orchestrator in dict after completion so status remains accessible
            # The orchestrator.results will contain the final state
            log_audit(client_id, 'one_click_migration',
                     f"Completed: {result['successful']} successful, {result['failed']} failed")
        except Exception as e:
            logger.error(f"Migration thread failed for client {client_id}: {e}", exc_info=True)
            if client_id in _running_migrations:
                _running_migrations[client_id].results['status'] = 'failed'
                _running_migrations[client_id].results['errors'].append(str(e))


@api_bp.route('/client/<int:client_id>/start_migration', methods=['POST'])
def start_migration(client_id):
    """
    Start a one-click DDL migration for a client.

    This runs the migration in a background thread and returns immediately.
    Use the /migration_status endpoint to poll for progress.

    Request body (optional):
    {
        "clean_slate": false,       // Drop existing tables before validation
        "auto_create_ddl": true,    // Auto-create missing tables during validation
        "object_types": ["TABLE", "VIEW", ...]  // Limit to specific types (optional)
    }
    """
    # Check if migration is already running
    if client_id in _running_migrations:
        existing = _running_migrations[client_id]
        if existing.results.get('status') == 'running':
            return jsonify({
                'error': 'A migration is already running for this client',
                'status': existing.get_status()
            }), 409
        # Clear previous completed migration to start fresh
        del _running_migrations[client_id]

    data = request.get_json(silent=True) or {}
    options = {
        'clean_slate': data.get('clean_slate', False),
        'auto_create_ddl': data.get('auto_create_ddl', True),
    }
    if 'object_types' in data:
        options['object_types'] = data['object_types']

    # Get the Flask app for the thread context
    from flask import current_app
    app = current_app._get_current_object()

    # Start migration in background thread
    thread = threading.Thread(
        target=_run_migration_thread,
        args=(client_id, options, app),
        daemon=True
    )
    thread.start()

    log_audit(client_id, 'one_click_migration', 'Migration started')

    return jsonify({
        'message': 'Migration started',
        'status': 'running',
        'poll_url': f'/api/client/{client_id}/migration_status'
    })


@api_bp.route('/client/<int:client_id>/migration_status', methods=['GET'])
def get_migration_status(client_id):
    """
    Get the current status of a running or completed migration.

    Returns:
    {
        "status": "running|completed|partial|failed|pending",
        "phase": "discovery|export|converting|validating|null",
        "total_objects": 10,
        "processed_objects": 5,
        "successful": 4,
        "failed": 1,
        "errors": ["error message 1", ...],
        "files": [{"file_id": 1, "filename": "...", "status": "..."}, ...]
    }
    """
    # First check in-memory for running migration
    if client_id in _running_migrations:
        orchestrator = _running_migrations[client_id]
        return jsonify(orchestrator.get_status())

    # Fall back to database for completed migrations
    # This handles multi-worker environments where the migration ran in another worker
    conn = get_db()

    # Get the most recent TABLE session (the main export) and its related sessions
    # Sessions from the same migration run are within 5 minutes of each other
    cursor = execute_query(conn, '''
        SELECT session_id, workflow_status, created_at
        FROM migration_sessions
        WHERE client_id = ? AND export_type = 'TABLE'
        ORDER BY session_id DESC
        LIMIT 1
    ''', (client_id,))
    main_session = cursor.fetchone()

    if not main_session:
        return jsonify({
            'status': 'no_migration',
            'message': 'No migration found for this client'
        })

    main_session_id = main_session['session_id']
    workflow_status = main_session['workflow_status'] or 'unknown'

    # Get all sessions from this migration run (within a short time window)
    cursor = execute_query(conn, '''
        SELECT session_id FROM migration_sessions
        WHERE client_id = ? AND session_id >= ?
        ORDER BY session_id
    ''', (client_id, main_session_id))
    session_ids = [row['session_id'] for row in cursor.fetchall()]

    # Get file statuses for all related sessions
    placeholders = ','.join('?' * len(session_ids))
    cursor = execute_query(conn, f'''
        SELECT file_id, filename, status, error_message
        FROM migration_files
        WHERE session_id IN ({placeholders})
    ''', session_ids)
    files = cursor.fetchall()

    successful = sum(1 for f in files if f['status'] in ('validated', 'converted'))
    failed = sum(1 for f in files if f['status'] == 'failed')
    errors = [f"{f['filename']}: {f['error_message']}" for f in files if f['error_message']]

    return jsonify({
        'status': workflow_status,
        'phase': None,
        'total_objects': len(files),
        'processed_objects': len(files),
        'successful': successful,
        'failed': failed,
        'errors': errors,
        'files': [{'file_id': f['file_id'], 'filename': f['filename'], 'status': f['status']} for f in files],
        'started_at': main_session['created_at'],
        'completed_at': None
    })


@api_bp.route('/client/<int:client_id>/run_migration_sync', methods=['POST'])
def run_migration_sync(client_id):
    """
    Run a one-click DDL migration synchronously (blocking).

    Use this for smaller migrations or when you want to wait for completion.
    For larger migrations, use /start_migration with status polling.

    Request body (optional):
    {
        "clean_slate": false,
        "auto_create_ddl": true,
        "object_types": ["TABLE", "VIEW", ...]
    }
    """
    data = request.get_json(silent=True) or {}
    options = {
        'clean_slate': data.get('clean_slate', False),
        'auto_create_ddl': data.get('auto_create_ddl', True),
    }
    if 'object_types' in data:
        options['object_types'] = data['object_types']

    try:
        orchestrator = MigrationOrchestrator(client_id)
        result = orchestrator.run_full_migration(options)

        log_audit(client_id, 'one_click_migration_sync',
                 f"Completed: {result['successful']} successful, {result['failed']} failed")

        return jsonify(result)
    except Exception as e:
        logger.error(f"Sync migration failed for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': str(e), 'status': 'failed'}), 500

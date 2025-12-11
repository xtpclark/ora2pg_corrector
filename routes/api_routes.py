from flask import Blueprint, request, jsonify, session
from modules.db import get_db, execute_query, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from modules.audit import log_audit
from modules.sql_processing import Ora2PgAICorrector
from modules.orchestrator import MigrationOrchestrator
from modules.reports import MigrationReportGenerator
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


# =============================================================================
# DDL Cache Endpoints
# =============================================================================

@api_bp.route('/client/<int:client_id>/ddl_cache/stats', methods=['GET'])
def get_ddl_cache_stats(client_id):
    """
    Get DDL cache statistics for a client.

    Returns:
        - total_entries: Number of cached DDL entries
        - total_hits: Total cache hits
        - entries: List of cached objects with hit counts
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = '''SELECT object_name, object_type, hit_count, created_at, last_used,
                          ai_provider, ai_model
                   FROM ddl_cache WHERE client_id = ?
                   ORDER BY hit_count DESC'''
        cursor = execute_query(conn, query, (client_id,))
        entries = [dict(row) for row in cursor.fetchall()]

        total_hits = sum(e['hit_count'] for e in entries)

        return jsonify({
            'client_id': client_id,
            'total_entries': len(entries),
            'total_hits': total_hits,
            'entries': entries
        })
    except Exception as e:
        logger.error(f"Failed to get DDL cache stats: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/client/<int:client_id>/ddl_cache', methods=['DELETE'])
def clear_ddl_cache(client_id):
    """
    Clear all cached DDL entries for a client.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = 'DELETE FROM ddl_cache WHERE client_id = ?'
        execute_query(conn, query, (client_id,))
        conn.commit()

        log_audit(client_id, 'ddl_cache_cleared', 'All DDL cache entries cleared')

        return jsonify({'message': 'DDL cache cleared successfully'})
    except Exception as e:
        logger.error(f"Failed to clear DDL cache: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/session/<int:session_id>/generated_ddl', methods=['GET'])
def get_generated_ddl_list(session_id):
    """
    List all AI-generated DDL files for a session.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Get export directory for the session
        query = 'SELECT export_directory FROM migration_sessions WHERE session_id = ?'
        cursor = execute_query(conn, query, (session_id,))
        session = cursor.fetchone()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        export_dir = session['export_directory']
        ddl_dir = os.path.join(export_dir, 'ai_generated_ddl')

        # Check if manifest exists
        manifest_path = os.path.join(ddl_dir, '_manifest.json')
        if os.path.exists(manifest_path):
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            return jsonify({
                'session_id': session_id,
                'export_directory': ddl_dir,
                **manifest
            })
        else:
            return jsonify({
                'session_id': session_id,
                'export_directory': ddl_dir,
                'objects': [],
                'message': 'No AI-generated DDL files found'
            })

    except Exception as e:
        logger.error(f"Failed to get generated DDL list: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/session/<int:session_id>/generated_ddl/<object_name>', methods=['GET'])
def get_generated_ddl_content(session_id, object_name):
    """
    Get the content of a specific AI-generated DDL file.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Get export directory for the session
        query = 'SELECT export_directory FROM migration_sessions WHERE session_id = ?'
        cursor = execute_query(conn, query, (session_id,))
        session = cursor.fetchone()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        export_dir = session['export_directory']

        # Sanitize object name to match saved filename
        import re
        safe_name = re.sub(r'[^\w\-.]', '_', object_name.lower())
        ddl_file = os.path.join(export_dir, 'ai_generated_ddl', f"{safe_name}.sql")

        if not os.path.exists(ddl_file):
            return jsonify({'error': f"DDL file not found for '{object_name}'"}), 404

        with open(ddl_file, 'r', encoding='utf-8') as f:
            content = f.read()

        return jsonify({
            'session_id': session_id,
            'object_name': object_name,
            'filename': f"{safe_name}.sql",
            'content': content
        })

    except Exception as e:
        logger.error(f"Failed to get DDL content: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# Rollback Script Endpoints
# =============================================================================

@api_bp.route('/session/<int:session_id>/rollback', methods=['GET'])
def get_rollback_script(session_id):
    """
    Get the rollback script for a session.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = '''SELECT rollback_script, rollback_generated_at, export_directory
                   FROM migration_sessions WHERE session_id = ?'''
        cursor = execute_query(conn, query, (session_id,))
        session = cursor.fetchone()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        rollback_script = session['rollback_script']

        # If not in database, try to read from file
        if not rollback_script:
            export_dir = session['export_directory']
            rollback_file = os.path.join(export_dir, 'rollback.sql')
            if os.path.exists(rollback_file):
                with open(rollback_file, 'r', encoding='utf-8') as f:
                    rollback_script = f.read()

        if not rollback_script:
            return jsonify({
                'session_id': session_id,
                'message': 'No rollback script available for this session'
            }), 404

        return jsonify({
            'session_id': session_id,
            'generated_at': session['rollback_generated_at'],
            'content': rollback_script
        })

    except Exception as e:
        logger.error(f"Failed to get rollback script: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/session/<int:session_id>/rollback/preview', methods=['GET'])
def preview_rollback(session_id):
    """
    Preview what objects would be dropped by the rollback script (dry-run).
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = '''SELECT rollback_script, rollback_generated_at, export_directory
                   FROM migration_sessions WHERE session_id = ?'''
        cursor = execute_query(conn, query, (session_id,))
        session = cursor.fetchone()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        rollback_script = session['rollback_script']

        # If not in database, try to read from file
        if not rollback_script:
            export_dir = session['export_directory']
            rollback_file = os.path.join(export_dir, 'rollback.sql')
            if os.path.exists(rollback_file):
                with open(rollback_file, 'r', encoding='utf-8') as f:
                    rollback_script = f.read()

        if not rollback_script:
            return jsonify({
                'session_id': session_id,
                'message': 'No rollback script available for this session'
            }), 404

        # Parse DROP statements from the script
        import re
        drop_pattern = r'DROP\s+(TABLE|VIEW|MATERIALIZED\s+VIEW|INDEX|FUNCTION|PROCEDURE|SEQUENCE|TYPE|TRIGGER)\s+IF\s+EXISTS\s+"?([^"\s;]+)"?\s*(?:ON\s+"?([^"\s;]+)"?)?\s*CASCADE'
        matches = re.finditer(drop_pattern, rollback_script, re.IGNORECASE)

        objects_to_drop = []
        for match in matches:
            obj_type = match.group(1).upper()
            obj_name = match.group(2)
            table_name = match.group(3)  # For triggers

            drop_stmt = f'DROP {obj_type} IF EXISTS "{obj_name}"'
            if table_name:
                drop_stmt += f' ON "{table_name}"'
            drop_stmt += ' CASCADE;'

            objects_to_drop.append({
                'type': obj_type,
                'name': obj_name,
                'table': table_name,
                'drop_statement': drop_stmt
            })

        return jsonify({
            'session_id': session_id,
            'generated_at': session['rollback_generated_at'],
            'objects_to_drop': objects_to_drop,
            'total_objects': len(objects_to_drop),
            'warning': f'This script will DROP {len(objects_to_drop)} objects. Review carefully before executing.'
        })

    except Exception as e:
        logger.error(f"Failed to preview rollback: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/session/<int:session_id>/rollback/download', methods=['GET'])
def download_rollback_script(session_id):
    """
    Download the rollback script as a .sql file.
    """
    from flask import Response

    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = '''SELECT rollback_script, rollback_generated_at, export_directory
                   FROM migration_sessions WHERE session_id = ?'''
        cursor = execute_query(conn, query, (session_id,))
        session = cursor.fetchone()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        rollback_script = session['rollback_script']

        # If not in database, try to read from file
        if not rollback_script:
            export_dir = session['export_directory']
            rollback_file = os.path.join(export_dir, 'rollback.sql')
            if os.path.exists(rollback_file):
                with open(rollback_file, 'r', encoding='utf-8') as f:
                    rollback_script = f.read()

        if not rollback_script:
            return jsonify({'error': 'No rollback script available'}), 404

        return Response(
            rollback_script,
            mimetype='application/sql',
            headers={
                'Content-Disposition': f'attachment; filename=rollback_session_{session_id}.sql'
            }
        )

    except Exception as e:
        logger.error(f"Failed to download rollback script: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/session/<int:session_id>/rollback/execute', methods=['POST'])
def execute_rollback(session_id):
    """
    Execute the rollback script against the PostgreSQL validation database.
    Requires confirmation parameter to prevent accidental execution.
    """
    import psycopg2

    data = request.get_json() or {}
    if not data.get('confirm'):
        return jsonify({
            'error': 'Confirmation required',
            'message': 'Set confirm: true to execute rollback'
        }), 400

    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Get session and rollback script
        query = '''SELECT ms.rollback_script, ms.export_directory, c.client_id
                   FROM migration_sessions ms
                   JOIN clients c ON ms.client_id = c.client_id
                   WHERE ms.session_id = ?'''
        cursor = execute_query(conn, query, (session_id,))
        session = cursor.fetchone()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        rollback_script = session['rollback_script']

        # Fall back to file if not in database
        if not rollback_script:
            export_dir = session['export_directory']
            rollback_file = os.path.join(export_dir, 'rollback.sql')
            if os.path.exists(rollback_file):
                with open(rollback_file, 'r', encoding='utf-8') as f:
                    rollback_script = f.read()

        if not rollback_script:
            return jsonify({'error': 'No rollback script available for this session'}), 404

        # Get PostgreSQL connection string from client config (key-value table)
        config_query = '''SELECT config_value FROM configs
                          WHERE client_id = ? AND config_key = 'validation_pg_dsn' '''
        cursor = execute_query(conn, config_query, (session['client_id'],))
        config_row = cursor.fetchone()

        if not config_row or not config_row['config_value']:
            return jsonify({'error': 'PostgreSQL validation DSN not configured'}), 400

        pg_dsn = config_row['config_value']

        # Execute rollback on PostgreSQL
        dropped_objects = []
        errors = []

        try:
            pg_conn = psycopg2.connect(pg_dsn)
            pg_conn.autocommit = False
            pg_cursor = pg_conn.cursor()

            # Execute the full script (it has BEGIN/COMMIT)
            pg_cursor.execute(rollback_script)
            pg_conn.commit()

            # Parse what was dropped for the response
            import re
            drop_pattern = r'DROP\s+(\w+)\s+IF\s+EXISTS\s+"?([^"\s;]+)"?'
            for match in re.finditer(drop_pattern, rollback_script, re.IGNORECASE):
                dropped_objects.append({
                    'type': match.group(1).upper(),
                    'name': match.group(2)
                })

            pg_cursor.close()
            pg_conn.close()

            logger.info(f"Rollback executed for session {session_id}: {len(dropped_objects)} objects dropped")

            return jsonify({
                'success': True,
                'session_id': session_id,
                'message': f'Rollback completed successfully. {len(dropped_objects)} objects dropped.',
                'dropped_objects': dropped_objects
            })

        except psycopg2.Error as pg_error:
            logger.error(f"PostgreSQL error during rollback: {pg_error}")
            if 'pg_conn' in locals():
                pg_conn.rollback()
                pg_conn.close()
            return jsonify({
                'success': False,
                'error': f'PostgreSQL error: {str(pg_error)}',
                'hint': 'Some objects may not exist or have dependencies'
            }), 500

    except Exception as e:
        logger.error(f"Failed to execute rollback: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# Migration Report Endpoints
# =============================================================================

@api_bp.route('/session/<int:session_id>/report', methods=['GET'])
def get_migration_report(session_id):
    """
    Generate and return an AsciiDoc migration report for a session.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Get client_id from session
        query = 'SELECT client_id FROM migration_sessions WHERE session_id = ?'
        cursor = execute_query(conn, query, (session_id,))
        session_row = cursor.fetchone()

        if not session_row:
            return jsonify({'error': 'Session not found'}), 404

        client_id = session_row['client_id']

        # Generate report
        generator = MigrationReportGenerator(conn, client_id, session_id)
        generator.gather_data()
        content = generator.generate_asciidoc()

        return jsonify({
            'session_id': session_id,
            'format': 'asciidoc',
            'content': content
        })

    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/session/<int:session_id>/report/download', methods=['GET'])
def download_migration_report(session_id):
    """
    Download the migration report as a .adoc file.
    """
    from flask import Response

    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Get client_id from session
        query = 'SELECT client_id FROM migration_sessions WHERE session_id = ?'
        cursor = execute_query(conn, query, (session_id,))
        session_row = cursor.fetchone()

        if not session_row:
            return jsonify({'error': 'Session not found'}), 404

        client_id = session_row['client_id']

        # Generate report
        generator = MigrationReportGenerator(conn, client_id, session_id)
        generator.gather_data()
        content = generator.generate_asciidoc()

        return Response(
            content,
            mimetype='text/plain',
            headers={
                'Content-Disposition': f'attachment; filename=migration_report_session_{session_id}.adoc'
            }
        )

    except Exception as e:
        logger.error(f"Failed to download report: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/client/<int:client_id>/migration_report', methods=['GET'])
def get_client_migration_report(client_id):
    """
    Get migration report for the latest migration of a client.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Generate report for latest migration
        generator = MigrationReportGenerator(conn, client_id)
        generator.gather_data()

        if not generator.data.get('sessions'):
            return jsonify({
                'client_id': client_id,
                'message': 'No migration sessions found for this client'
            }), 404

        content = generator.generate_asciidoc()

        # Optionally save to file
        save_to_file = request.args.get('save', 'false').lower() == 'true'
        file_path = None
        if save_to_file and generator.data.get('export_directory'):
            try:
                file_path = generator.save_report()
            except Exception as e:
                logger.warning(f"Failed to save report to file: {e}")

        return jsonify({
            'client_id': client_id,
            'session_id': generator.session_id,
            'format': 'asciidoc',
            'content': content,
            'saved_to': file_path
        })

    except Exception as e:
        logger.error(f"Failed to generate client report: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# Migration Objects Endpoints (Per-Object Tracking)
# =============================================================================

@api_bp.route('/session/<int:session_id>/objects', methods=['GET'])
def get_session_objects(session_id):
    """
    Get all objects for a migration session with their status.
    Optional filters: ?type=TABLE&status=validated
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Build query with optional filters
        filters = ['session_id = ?']
        params = [session_id]

        obj_type = request.args.get('type')
        if obj_type:
            filters.append('object_type = ?')
            params.append(obj_type.upper())

        status = request.args.get('status')
        if status:
            filters.append('status = ?')
            params.append(status)

        where_clause = ' AND '.join(filters)
        query = f'''SELECT object_id, object_name, object_type, status,
                           error_message, ai_corrected, line_start, line_end,
                           created_at, validated_at
                    FROM migration_objects
                    WHERE {where_clause}
                    ORDER BY object_type, object_name'''

        cursor = execute_query(conn, query, tuple(params))
        objects = [dict(row) for row in cursor.fetchall()]

        # Get summary counts
        summary_query = '''SELECT object_type, status, COUNT(*) as count
                          FROM migration_objects
                          WHERE session_id = ?
                          GROUP BY object_type, status'''
        cursor = execute_query(conn, summary_query, (session_id,))
        summary_rows = cursor.fetchall()

        # Build summary structure
        summary = {}
        for row in summary_rows:
            obj_type = row['object_type']
            if obj_type not in summary:
                summary[obj_type] = {'total': 0, 'validated': 0, 'failed': 0, 'pending': 0}
            summary[obj_type][row['status']] = row['count']
            summary[obj_type]['total'] += row['count']

        return jsonify({
            'session_id': session_id,
            'total_objects': len(objects),
            'summary': summary,
            'objects': objects
        })

    except Exception as e:
        logger.error(f"Failed to get session objects: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/session/<int:session_id>/objects/summary', methods=['GET'])
def get_session_objects_summary(session_id):
    """
    Get summary counts of objects by type and status.
    Lightweight endpoint for progress display.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = '''SELECT object_type, status, COUNT(*) as count
                   FROM migration_objects
                   WHERE session_id = ?
                   GROUP BY object_type, status
                   ORDER BY object_type'''

        cursor = execute_query(conn, query, (session_id,))
        rows = cursor.fetchall()

        # Build structured summary
        by_type = {}
        totals = {'total': 0, 'validated': 0, 'failed': 0, 'pending': 0}

        for row in rows:
            obj_type = row['object_type']
            status = row['status']
            count = row['count']

            if obj_type not in by_type:
                by_type[obj_type] = {'total': 0, 'validated': 0, 'failed': 0, 'pending': 0}

            by_type[obj_type][status] = count
            by_type[obj_type]['total'] += count

            totals[status] = totals.get(status, 0) + count
            totals['total'] += count

        return jsonify({
            'session_id': session_id,
            'totals': totals,
            'by_type': by_type
        })

    except Exception as e:
        logger.error(f"Failed to get objects summary: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/object/<int:object_id>', methods=['GET'])
def get_object_detail(object_id):
    """
    Get detailed information about a specific object, including DDL.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = '''SELECT mo.*, mf.filename, ms.export_directory
                   FROM migration_objects mo
                   LEFT JOIN migration_files mf ON mo.file_id = mf.file_id
                   LEFT JOIN migration_sessions ms ON mo.session_id = ms.session_id
                   WHERE mo.object_id = ?'''

        cursor = execute_query(conn, query, (object_id,))
        obj = cursor.fetchone()

        if not obj:
            return jsonify({'error': 'Object not found'}), 404

        return jsonify(dict(obj))

    except Exception as e:
        logger.error(f"Failed to get object detail: {e}")
        return jsonify({'error': str(e)}), 500


@api_bp.route('/client/<int:client_id>/objects/summary', methods=['GET'])
def get_client_objects_summary(client_id):
    """
    Get object summary across all sessions for a client.
    Shows aggregate view of migration progress.
    """
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        query = '''SELECT mo.object_type, mo.status, COUNT(*) as count
                   FROM migration_objects mo
                   JOIN migration_sessions ms ON mo.session_id = ms.session_id
                   WHERE ms.client_id = ?
                   GROUP BY mo.object_type, mo.status
                   ORDER BY mo.object_type'''

        cursor = execute_query(conn, query, (client_id,))
        rows = cursor.fetchall()

        by_type = {}
        totals = {'total': 0, 'validated': 0, 'failed': 0, 'pending': 0}

        for row in rows:
            obj_type = row['object_type']
            status = row['status']
            count = row['count']

            if obj_type not in by_type:
                by_type[obj_type] = {'total': 0, 'validated': 0, 'failed': 0, 'pending': 0}

            by_type[obj_type][status] = count
            by_type[obj_type]['total'] += count

            totals[status] = totals.get(status, 0) + count
            totals['total'] += count

        return jsonify({
            'client_id': client_id,
            'totals': totals,
            'by_type': by_type
        })

    except Exception as e:
        logger.error(f"Failed to get client objects summary: {e}")
        return jsonify({'error': str(e)}), 500

"""Migration orchestration API endpoints."""

from flask import Blueprint, request, jsonify, current_app
from modules.db import get_db, execute_query, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from modules.audit import log_audit
from modules.sql_processing import Ora2PgAICorrector
from modules.orchestrator import MigrationOrchestrator
from modules.constants import OUTPUT_DIR
from cryptography.fernet import Fernet
import threading
import logging

logger = logging.getLogger(__name__)

migration_bp = Blueprint('migration', __name__)

# Track running migrations for status polling
_running_migrations = {}


def _run_migration_thread(client_id, options, app):
    """Background thread function to run migration with Flask app context."""
    with app.app_context():
        try:
            orchestrator = MigrationOrchestrator(client_id)
            _running_migrations[client_id] = orchestrator
            result = orchestrator.run_full_migration(options)
            log_audit(client_id, 'one_click_migration',
                     f"Completed: {result['successful']} successful, {result['failed']} failed")
        except Exception as e:
            logger.error(f"Migration thread failed for client {client_id}: {e}", exc_info=True)
            if client_id in _running_migrations:
                _running_migrations[client_id].results['status'] = 'failed'
                _running_migrations[client_id].results['errors'].append(str(e))


@migration_bp.route('/client/<int:client_id>/start_migration', methods=['POST'])
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


@migration_bp.route('/client/<int:client_id>/migration_status', methods=['GET'])
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
    conn = get_db()

    # Get the most recent TABLE session (the main export) and its related sessions
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


@migration_bp.route('/client/<int:client_id>/run_migration_sync', methods=['POST'])
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


@migration_bp.route('/client/<int:client_id>/test_ora2pg_connection', methods=['POST'])
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


@migration_bp.route('/client/<int:client_id>/get_object_list', methods=['GET'])
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


@migration_bp.route('/client/<int:client_id>/get_oracle_ddl', methods=['POST'])
def get_oracle_ddl(client_id):
    data = request.get_json()
    object_name = data.get('object_name')
    object_type = data.get('object_type', 'TABLE')

    if not object_name:
        return jsonify({'error': 'Object name is required'}), 400

    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        ddl, error = corrector._get_single_object_ddl(conn, config, object_name, object_type)

        if error:
            return jsonify({'error': error}), 500

        log_audit(client_id, 'get_oracle_ddl', f'Fetched DDL for {object_type} {object_name}.')
        return jsonify({'ddl': ddl})

    except Exception as e:
        logger.error(f"Failed to get Oracle DDL for {object_name}: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500


@migration_bp.route('/client/<int:client_id>/get_bulk_oracle_ddl', methods=['POST'])
def get_bulk_oracle_ddl(client_id):
    data = request.get_json()
    objects = data.get('objects', [])

    if not objects:
        return jsonify({'error': 'Object list is required'}), 400

    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        results = []

        for obj in objects:
            object_name = obj.get('name')
            object_type = obj.get('type', 'TABLE')
            ddl, error = corrector._get_single_object_ddl(conn, config, object_name, object_type)
            results.append({
                'name': object_name,
                'type': object_type,
                'ddl': ddl,
                'error': error
            })

        successful = sum(1 for r in results if r['ddl'] and not r['error'])
        log_audit(client_id, 'get_bulk_oracle_ddl', f'Fetched DDL for {successful}/{len(results)} objects.')
        return jsonify({'results': results})

    except Exception as e:
        logger.error(f"Failed to get bulk Oracle DDL: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500


@migration_bp.route('/client/<int:client_id>/generate_report', methods=['POST'])
def generate_ora2pg_report(client_id):
    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir=OUTPUT_DIR, ai_settings={}, encryption_key=ENCRYPTION_KEY)
        report_args = ['-t', 'SHOW_REPORT', '--dump_as_html']
        result, error = corrector.run_ora2pg_export(client_id, conn, config, extra_args=report_args)

        if error:
            log_audit(client_id, 'generate_report', f'Failed: {error}')
            return jsonify({'error': error}), 500

        log_audit(client_id, 'generate_report', 'Migration report generated successfully.')
        return jsonify(result)

    except Exception as e:
        logger.error(f"Failed to generate Ora2Pg report for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500


@migration_bp.route('/client/<int:client_id>/run_ora2pg', methods=['POST'])
def run_ora2pg(client_id):
    """Run ora2pg export using stored client configuration."""
    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        ai_settings = extract_ai_settings(config)
        corrector = Ora2PgAICorrector(
            output_dir=OUTPUT_DIR,
            ai_settings=ai_settings,
            encryption_key=ENCRYPTION_KEY
        )

        result, error = corrector.run_ora2pg_export(client_id, conn, config)

        if error:
            log_audit(client_id, 'run_ora2pg', f'Export failed: {error}')
            return jsonify({'error': error}), 500

        session_id = result.get('session_id')
        files = result.get('files', [])
        log_audit(client_id, 'run_ora2pg',
                 f'Export successful. Session: {session_id}, Files: {len(files)}')

        result_data = {
            'message': 'Export completed successfully',
            'session_id': session_id,
            'files': files
        }
        if 'sql_output' in result:
            result_data['sql_output'] = result['sql_output']

        return jsonify(result_data)

    except Exception as e:
        logger.error(f"Failed to run Ora2Pg for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500

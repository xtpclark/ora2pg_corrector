"""Migration orchestration API endpoints."""

from flask import Blueprint, request, jsonify, current_app
from modules.db import get_db, execute_query, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from modules.audit import log_audit
from modules.sql_processing import Ora2PgAICorrector
from modules.orchestrator import MigrationOrchestrator
from modules.constants import OUTPUT_DIR
from modules.responses import (
    success_response, error_response, validation_error_response,
    server_error_response, db_error_response
)
from cryptography.fernet import Fernet
from bs4 import BeautifulSoup
import threading
import logging
import re

logger = logging.getLogger(__name__)


def parse_ora2pg_html_report(html_content):
    """
    Parse ora2pg HTML report and extract structured data.

    Returns dict with:
        - Schema, Version, Size (from header)
        - objects: list of {object, number, invalid, comment, details}
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    result = {
        'Schema': 'N/A',
        'Version': 'N/A',
        'Size': 'N/A',
        'total cost': '0',
        'human days cost': 'N/A',
        'migration level': 'N/A',
        'objects': []
    }

    # Parse header table (Version, Schema, Size)
    header_div = soup.find('div', id='header')
    if header_div:
        header_table = header_div.find('table')
        if header_table:
            for row in header_table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.get_text(strip=True)
                    value = td.get_text(strip=True)
                    if key == 'Version':
                        result['Version'] = value
                    elif key == 'Schema':
                        result['Schema'] = value
                    elif key == 'Size':
                        result['Size'] = value

    # Parse content table (objects)
    content_div = soup.find('div', id='content')
    if content_div:
        content_table = content_div.find('table')
        if content_table:
            rows = content_table.find_all('tr')
            for row in rows[1:]:  # Skip header row
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 4:
                    obj_name = cells[0].get_text(strip=True)
                    obj_number = cells[1].get_text(strip=True)
                    obj_invalid = cells[2].get_text(strip=True)
                    obj_comment = cells[3].get_text(strip=True) if len(cells) > 3 else ''

                    result['objects'].append({
                        'object': obj_name,
                        'number': obj_number,
                        'invalid': obj_invalid,
                        'cost value': '0.00',  # SHOW_REPORT doesn't include cost
                        'comment': obj_comment
                    })

    return result

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
        "session_name": "My Migration",  // Friendly name for the session
        "clean_slate": false,       // Drop existing tables before validation
        "auto_create_ddl": true,    // Auto-create missing tables during validation
        "object_types": ["TABLE", "VIEW", ...]  // Limit to specific types (optional)
    }
    """
    # Check database for already running migration (cross-worker consistent)
    conn = get_db()
    cursor = execute_query(conn, '''
        SELECT session_id, workflow_status FROM migration_sessions
        WHERE client_id = ? AND workflow_status IN ('discovering', 'exporting', 'validating')
        ORDER BY session_id DESC
        LIMIT 1
    ''', (client_id,))
    running_session = cursor.fetchone()

    if running_session:
        return error_response(
            f"A migration is already running for this client (session {running_session['session_id']})",
            status_code=409
        )

    # Also check in-memory for this worker (extra safeguard)
    if client_id in _running_migrations:
        existing = _running_migrations[client_id]
        if existing.results.get('status') == 'running':
            return error_response(
                'A migration is already running for this client',
                status_code=409
            )
        # Clear previous completed migration to start fresh
        del _running_migrations[client_id]

    data = request.get_json(silent=True) or {}
    options = {
        'clean_slate': data.get('clean_slate', False),
        'auto_create_ddl': data.get('auto_create_ddl', True),
    }
    if 'session_name' in data:
        options['session_name'] = data['session_name']
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

    return success_response({
        'message': 'Migration started',
        'status': 'running',
        'poll_url': f'/api/client/{client_id}/migration_status'
    })


@migration_bp.route('/client/<int:client_id>/migration_status', methods=['GET'])
def get_migration_status(client_id):
    """
    Get the current status of a running or completed migration.

    Progress is read from the database to ensure cross-worker consistency
    (fixes the race condition where gunicorn workers don't share in-memory state).

    Returns:
    {
        "status": "running|completed|partial|failed|pending",
        "phase": "discovery|export|converting|validating|fk_constraints|completed|null",
        "total_objects": 10,
        "processed_objects": 5,
        "successful": 4,
        "failed": 1,
        "current_file": "EMPLOYEES.sql",
        "errors": ["error message 1", ...],
        "files": [{"file_id": 1, "filename": "...", "status": "..."}, ...]
    }
    """
    conn = get_db()

    # Get the most recent session for this client (regardless of export_type)
    # This handles both single-type exports and multi-type DDL exports
    cursor = execute_query(conn, '''
        SELECT session_id, workflow_status, current_phase, processed_count, total_count,
               current_file, created_at, export_type
        FROM migration_sessions
        WHERE client_id = ?
        ORDER BY session_id DESC
        LIMIT 1
    ''', (client_id,))
    main_session = cursor.fetchone()

    if not main_session:
        return success_response({
            'status': 'no_migration',
            'message': 'No migration found for this client'
        })

    main_session_id = main_session['session_id']
    workflow_status = main_session['workflow_status'] or 'unknown'

    # Read progress directly from database (cross-worker consistent)
    current_phase = main_session['current_phase']
    processed_count = main_session['processed_count'] or 0
    total_count = main_session['total_count'] or 0
    current_file = main_session['current_file']

    # Get all sessions from this migration run (for multi-type exports)
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

    # For completed migrations, use file counts as the final numbers
    if workflow_status in ('completed', 'partial', 'failed'):
        processed_count = len(files)
        total_count = len(files)

    return success_response({
        'status': workflow_status,
        'phase': current_phase,
        'session_id': main_session_id,
        'total_objects': total_count,
        'processed_objects': processed_count,
        'successful': successful,
        'failed': failed,
        'current_file': current_file,
        'errors': errors,
        'files': [{'file_id': f['file_id'], 'filename': f['filename'], 'status': f['status']} for f in files],
        'started_at': main_session['created_at'],
        'completed_at': None
    })


@migration_bp.route('/running_migrations', methods=['GET'])
def get_running_migrations():
    """
    Get all running migrations across all clients.

    Useful for dashboard views to monitor all active migrations,
    regardless of how they were started (UI, CLI, or curl).

    Returns:
    {
        "running_count": 2,
        "migrations": [
            {
                "client_id": 1,
                "client_name": "HR_Migration",
                "session_id": 5,
                "workflow_status": "exporting",
                "current_phase": "export",
                "processed_count": 45,
                "total_count": 100,
                "current_file": "Exporting EMPLOYEES...",
                "started_at": "2025-12-12 10:30:00"
            },
            ...
        ]
    }
    """
    conn = get_db()
    if not conn:
        return db_error_response()

    try:
        cursor = execute_query(conn, '''
            SELECT ms.session_id, ms.client_id, c.client_name,
                   ms.workflow_status, ms.current_phase,
                   ms.processed_count, ms.total_count, ms.current_file,
                   ms.export_type, ms.created_at
            FROM migration_sessions ms
            JOIN clients c ON ms.client_id = c.client_id
            WHERE ms.workflow_status IN ('discovering', 'exporting', 'validating', 'converting')
            ORDER BY ms.created_at DESC
        ''')
        running = cursor.fetchall()

        migrations = []
        for row in running:
            migrations.append({
                'client_id': row['client_id'],
                'client_name': row['client_name'],
                'session_id': row['session_id'],
                'workflow_status': row['workflow_status'],
                'current_phase': row['current_phase'],
                'processed_count': row['processed_count'] or 0,
                'total_count': row['total_count'] or 0,
                'current_file': row['current_file'],
                'export_type': row['export_type'],
                'started_at': row['created_at']
            })

        return success_response({
            'running_count': len(migrations),
            'migrations': migrations
        })
    except Exception as e:
        logger.error(f"Error fetching running migrations: {e}", exc_info=True)
        return server_error_response('Failed to fetch running migrations', str(e))


@migration_bp.route('/client/<int:client_id>/run_migration_sync', methods=['POST'])
def run_migration_sync(client_id):
    """
    Run a one-click DDL migration synchronously (blocking).

    Use this for smaller migrations or when you want to wait for completion.
    For larger migrations, use /start_migration with status polling.

    Request body (optional):
    {
        "session_name": "My Migration",
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
    if 'session_name' in data:
        options['session_name'] = data['session_name']
    if 'object_types' in data:
        options['object_types'] = data['object_types']

    try:
        orchestrator = MigrationOrchestrator(client_id)
        result = orchestrator.run_full_migration(options)

        log_audit(client_id, 'one_click_migration_sync',
                 f"Completed: {result['successful']} successful, {result['failed']} failed")

        return success_response(result)
    except Exception as e:
        logger.error(f"Sync migration failed for client {client_id}: {e}", exc_info=True)
        return server_error_response('Migration failed', str(e))


@migration_bp.route('/client/<int:client_id>/test_ora2pg_connection', methods=['POST'])
def test_ora2pg_connection(client_id):
    try:
        # Get config from database with proper decryption (not from request)
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        if not config.get('oracle_dsn') or not config.get('oracle_user'):
            return validation_error_response('Oracle connection settings not configured')

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        report_args = ['-t', 'SHOW_VERSION']
        version_output, error_output = corrector.run_ora2pg_export(client_id, conn, config, extra_args=report_args)

        if error_output:
            log_audit(client_id, 'test_oracle_connection', f'Failed: {error_output}')
            return error_response(f'Connection failed: {error_output}')

        version_string = version_output.get('sql_output', '').strip()
        log_audit(client_id, 'test_oracle_connection', f'Success: {version_string}')
        return success_response({'status': 'success', 'message': f'Connection successful! {version_string}'})

    except Exception as e:
        logger.error(f"Failed to test Oracle connection for client {client_id}: {e}", exc_info=True)
        return server_error_response('Failed to test Oracle connection', str(e))


@migration_bp.route('/client/<int:client_id>/get_object_list', methods=['GET'])
def get_object_list(client_id):
    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        object_list, error = corrector._get_object_list(conn, config)

        if error:
            log_audit(client_id, 'get_object_list', f'Failed: {error}')
            return server_error_response('Failed to get object list', error)

        log_audit(client_id, 'get_object_list', f'Successfully fetched {len(object_list)} objects.')
        return success_response(object_list)

    except Exception as e:
        logger.error(f"Failed to get object list for client {client_id}: {e}", exc_info=True)
        return server_error_response('Failed to get object list', str(e))


@migration_bp.route('/client/<int:client_id>/get_oracle_ddl', methods=['POST'])
def get_oracle_ddl_endpoint(client_id):
    """Fetch Oracle DDL for a single object."""
    data = request.get_json()
    object_name = data.get('object_name')
    object_type = data.get('object_type', 'TABLE')
    pretty = data.get('pretty', False)

    if not object_name:
        return validation_error_response('Object name is required')

    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        ddl, error = corrector.get_oracle_ddl(config, object_type, object_name, pretty=pretty)

        if error:
            return server_error_response('Failed to get Oracle DDL', error)

        log_audit(client_id, 'get_oracle_ddl', f'Fetched DDL for {object_type} {object_name}.')
        return success_response({'ddl': ddl})

    except Exception as e:
        logger.error(f"Failed to get Oracle DDL for {object_name}: {e}", exc_info=True)
        return server_error_response('Failed to get Oracle DDL', str(e))


@migration_bp.route('/client/<int:client_id>/get_bulk_oracle_ddl', methods=['POST'])
def get_bulk_oracle_ddl(client_id):
    """Fetch Oracle DDL for multiple objects and combine into single file."""
    data = request.get_json()
    objects = data.get('objects', [])
    pretty = data.get('pretty', False)

    if not objects:
        return validation_error_response('Object list is required')

    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        corrector = Ora2PgAICorrector(output_dir='', ai_settings={}, encryption_key=ENCRYPTION_KEY)
        ddl_parts = []
        errors = []

        for obj in objects:
            object_name = obj.get('name')
            object_type = obj.get('type', 'TABLE')
            ddl, error = corrector.get_oracle_ddl(config, object_type, object_name, pretty=pretty)

            if error:
                errors.append(f"-- ERROR fetching {object_type} {object_name}: {error}")
            elif ddl:
                ddl_parts.append(f"-- {object_type}: {object_name}")
                ddl_parts.append(ddl.strip())
                ddl_parts.append("")  # Empty line between objects

        # Combine all DDL into single string
        combined_ddl = "\n".join(ddl_parts)
        if errors:
            combined_ddl = "\n".join(errors) + "\n\n" + combined_ddl

        successful = len(ddl_parts) // 3  # Each object adds 3 entries (comment, ddl, blank)
        log_audit(client_id, 'get_bulk_oracle_ddl', f'Fetched DDL for {successful}/{len(objects)} objects.')
        return success_response({'ddl': combined_ddl})

    except Exception as e:
        logger.error(f"Failed to get bulk Oracle DDL: {e}", exc_info=True)
        return server_error_response('Failed to get bulk Oracle DDL', str(e))


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
            return server_error_response('Failed to generate report', error)

        # Parse HTML report into structured JSON for frontend
        html_content = result.get('sql_output', '')
        parsed_report = parse_ora2pg_html_report(html_content)

        log_audit(client_id, 'generate_report', 'Migration report generated successfully.')
        return success_response(parsed_report)

    except Exception as e:
        logger.error(f"Failed to generate Ora2Pg report for client {client_id}: {e}", exc_info=True)
        return server_error_response('Failed to generate Ora2Pg report', str(e))


@migration_bp.route('/client/<int:client_id>/run_ora2pg', methods=['POST'])
def run_ora2pg(client_id):
    """
    Run ora2pg export using stored client configuration.

    Request body (optional):
    {
        "type": "COPY",  // Override export type (TABLE, COPY, INSERT, etc.)
        "tables": ["TABLE1", "TABLE2"],  // Limit export to specific tables
        "where_clause": "created_date > '2024-01-01'",  // Filter data (for COPY/INSERT)
        "session_name": "My Export"  // Custom session name
    }
    """
    try:
        conn = get_db()
        config = get_client_config(client_id, conn, decrypt_keys=['oracle_pwd'])

        # Get optional parameters from request
        data = request.get_json(silent=True) or {}
        export_type = data.get('type')
        tables = data.get('tables')
        where_clause = data.get('where_clause')
        session_name = data.get('session_name')

        # Override export type if specified
        if export_type:
            config['type'] = export_type.upper()

        # Build extra args for ora2pg command
        extra_args = []
        if tables:
            config['ALLOW'] = ','.join(tables)
        if where_clause:
            extra_args.extend(['-W', where_clause])

        ai_settings = extract_ai_settings(config)
        corrector = Ora2PgAICorrector(
            output_dir=OUTPUT_DIR,
            ai_settings=ai_settings,
            encryption_key=ENCRYPTION_KEY
        )

        result, error = corrector.run_ora2pg_export(
            client_id, conn, config,
            extra_args=extra_args if extra_args else None,
            session_name=session_name
        )

        if error:
            log_audit(client_id, 'run_ora2pg', f'Export failed: {error}')
            return server_error_response('Export failed', error)

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

        return success_response(result_data)

    except Exception as e:
        logger.error(f"Failed to run Ora2Pg for client {client_id}: {e}", exc_info=True)
        return server_error_response('Failed to run Ora2Pg export', str(e))


@migration_bp.route('/client/<int:client_id>/table_counts', methods=['POST'])
def get_table_counts(client_id):
    """
    Get row counts for tables in the PostgreSQL validation database.

    Uses pg_class.reltuples for fast approximate counts by default.
    Set exact=true for precise counts (slower on large tables).

    Request body:
    {
        "tables": ["TABLE1", "TABLE2"],  // Tables to count (required)
        "exact": false  // Use exact COUNT(*) instead of estimates (default: false)
    }

    Returns:
    {
        "counts": {
            "table1": {"count": 1000, "exact": false},
            "table2": {"count": 500, "exact": false}
        },
        "total": 1500
    }
    """
    import psycopg2

    try:
        conn = get_db()
        config = get_client_config(client_id, conn)

        pg_dsn = config.get('validation_pg_dsn')
        if not pg_dsn:
            return validation_error_response('PostgreSQL validation DSN not configured')

        data = request.get_json(silent=True) or {}
        tables = data.get('tables', [])
        use_exact = data.get('exact', False)

        if not tables:
            return validation_error_response('No tables specified')

        counts = {}
        total = 0

        with psycopg2.connect(pg_dsn) as pg_conn:
            with pg_conn.cursor() as cursor:
                for table in tables:
                    table_lower = table.lower()
                    try:
                        if use_exact:
                            # Exact count - slower but accurate
                            cursor.execute(f'SELECT COUNT(*) FROM "{table_lower}"')
                            row_count = cursor.fetchone()[0]
                        else:
                            # Fast approximate count using pg_class
                            cursor.execute('''
                                SELECT COALESCE(reltuples::bigint, 0)
                                FROM pg_class
                                WHERE relname = %s AND relkind = 'r'
                            ''', (table_lower,))
                            result = cursor.fetchone()
                            row_count = result[0] if result else 0

                            # If reltuples is 0 or negative (never analyzed), try exact count
                            if row_count <= 0:
                                cursor.execute(f'SELECT COUNT(*) FROM "{table_lower}"')
                                row_count = cursor.fetchone()[0]

                        counts[table] = {'count': row_count, 'exact': use_exact or row_count == 0}
                        total += row_count

                    except psycopg2.Error as e:
                        # Table might not exist yet
                        counts[table] = {'count': 0, 'exact': True, 'error': str(e)}

        return success_response({
            'counts': counts,
            'total': total
        })

    except Exception as e:
        logger.error(f"Failed to get table counts for client {client_id}: {e}", exc_info=True)
        return server_error_response('Failed to get table counts', str(e))


@migration_bp.route('/session/<int:session_id>/load_data', methods=['POST'])
def load_session_data(session_id):
    """
    Load exported COPY/INSERT data from a session into PostgreSQL.

    Executes the SQL files from the session against the validation database.

    Returns:
    {
        "loaded_files": 3,
        "total_rows": 1500,
        "tables": {
            "employees": {"rows": 100, "status": "success"},
            "departments": {"rows": 50, "status": "success"}
        }
    }
    """
    import psycopg2
    import os
    import re

    try:
        conn = get_db()

        # Get session info
        cursor = execute_query(conn, '''
            SELECT client_id, export_directory, export_type
            FROM migration_sessions
            WHERE session_id = ?
        ''', (session_id,))
        row = cursor.fetchone()

        if not row:
            return error_response(f'Session {session_id} not found', status_code=404)

        client_id = row['client_id']
        export_dir = row['export_directory']
        export_type = row['export_type']

        # Get PG DSN from client config
        config = get_client_config(client_id, conn)
        pg_dsn = config.get('validation_pg_dsn')

        if not pg_dsn:
            return validation_error_response('PostgreSQL validation DSN not configured')

        if export_type not in ['COPY', 'INSERT']:
            return validation_error_response(f'Session type {export_type} is not a data export')

        if not export_dir or not os.path.exists(export_dir):
            return error_response(f'Export directory not found: {export_dir}', status_code=404)

        # Get SQL files from disk (data exports store content on disk, not DB)
        sql_files = [f for f in os.listdir(export_dir) if f.endswith('.sql') and 'output_' in f]

        if not sql_files:
            return error_response('No data files found in session', status_code=404)

        results = {
            'loaded_files': 0,
            'total_rows': 0,
            'tables': {},
            'errors': []
        }

        with psycopg2.connect(pg_dsn) as pg_conn:
            with pg_conn.cursor() as pg_cursor:
                for filename in sql_files:
                    # Skip aggregate files that use \i commands
                    if filename.startswith('output_'):
                        continue

                    # Read SQL content from disk
                    file_path = os.path.join(export_dir, filename)
                    try:
                        with open(file_path, 'r') as f:
                            sql_content = f.read()
                    except Exception as e:
                        results['errors'].append(f'{filename}: Could not read file: {e}')
                        continue

                    if not sql_content or not sql_content.strip():
                        results['errors'].append(f'{filename}: Empty file')
                        continue

                    # Extract table name from filename (e.g., EMPLOYEES_output_copy.sql)
                    table_match = re.match(r'([A-Za-z_][A-Za-z0-9_]*)_output_', filename)
                    table_name = table_match.group(1).lower() if table_match else 'unknown'

                    try:
                        # Handle COPY FROM STDIN format using copy_expert
                        if 'COPY' in sql_content and 'FROM STDIN' in sql_content:
                            from io import StringIO

                            # Execute SET commands first
                            for line in sql_content.split('\n'):
                                stripped = line.strip().upper()
                                if stripped.startswith('SET '):
                                    pg_cursor.execute(line)

                            # Find the COPY statement and extract data section
                            copy_match = re.search(
                                r'(COPY\s+\S+\s*\([^)]+\)\s*FROM\s+STDIN[^;]*;?)\s*\n(.*)',
                                sql_content,
                                re.IGNORECASE | re.DOTALL
                            )

                            if copy_match:
                                copy_cmd = copy_match.group(1).rstrip(';')
                                data_section = copy_match.group(2)
                                # Remove trailing \. marker if present
                                data_section = re.sub(r'\n\\.\s*$', '', data_section)

                                # Use copy_expert with the COPY command and data
                                pg_cursor.copy_expert(
                                    f"{copy_cmd} ",
                                    StringIO(data_section)
                                )

                            rows_affected = pg_cursor.rowcount if pg_cursor.rowcount > 0 else 0
                        else:
                            # Regular SQL (INSERT statements)
                            pg_cursor.execute(sql_content)
                            rows_affected = pg_cursor.rowcount if pg_cursor.rowcount > 0 else 0

                        # Get actual count from table
                        if table_name != 'unknown':
                            try:
                                pg_cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                                rows_affected = pg_cursor.fetchone()[0]
                            except:
                                pass

                        results['tables'][table_name] = {
                            'rows': rows_affected,
                            'status': 'success'
                        }
                        results['loaded_files'] += 1
                        results['total_rows'] += rows_affected

                    except psycopg2.Error as e:
                        error_msg = str(e).split('\n')[0]
                        results['tables'][table_name] = {
                            'rows': 0,
                            'status': 'failed',
                            'error': error_msg
                        }
                        results['errors'].append(f'{table_name}: {error_msg}')
                        # Continue with other files
                        pg_conn.rollback()

                pg_conn.commit()

        log_audit(client_id, 'load_data',
                 f'Session {session_id}: Loaded {results["loaded_files"]} files, {results["total_rows"]} rows')

        return success_response(results)

    except Exception as e:
        logger.error(f"Failed to load data for session {session_id}: {e}", exc_info=True)
        return server_error_response('Failed to load data', str(e))


# =============================================================================
# Migration History Endpoints
# =============================================================================

@migration_bp.route('/client/<int:client_id>/migration_history', methods=['GET'])
def get_migration_history(client_id):
    """
    Get recent completed migrations for a client.

    Query params:
        limit: Number of sessions to return (default 5, max 20)

    Returns:
    {
        "migrations": [
            {
                "session_id": 42,
                "session_name": "Export - 2025-12-12 10:30:00",
                "export_type": "TABLE",
                "workflow_status": "completed",
                "created_at": "2025-12-12T10:30:00",
                "completed_at": "2025-12-12T10:35:22",
                "total_files": 15,
                "successful_files": 14,
                "failed_files": 1,
                "total_input_tokens": 12500,
                "total_output_tokens": 8300,
                "estimated_cost_usd": 0.0234,
                "ai_model": "claude-3-5-sonnet-20241022"
            },
            ...
        ]
    }
    """
    conn = get_db()
    if not conn:
        return db_error_response()

    try:
        limit = min(int(request.args.get('limit', 5)), 20)

        cursor = execute_query(conn, '''
            SELECT ms.session_id, ms.session_name, ms.export_type, ms.workflow_status,
                   ms.created_at, ms.completed_at, ms.ai_model,
                   ms.total_input_tokens, ms.total_output_tokens, ms.estimated_cost_usd,
                   (SELECT COUNT(*) FROM migration_files WHERE session_id = ms.session_id) as total_files,
                   (SELECT COUNT(*) FROM migration_files WHERE session_id = ms.session_id AND status = 'validated') as successful_files,
                   (SELECT COUNT(*) FROM migration_files WHERE session_id = ms.session_id AND status = 'failed') as failed_files
            FROM migration_sessions ms
            WHERE ms.client_id = ?
              AND ms.workflow_status IN ('completed', 'partial', 'failed')
            ORDER BY ms.created_at DESC
            LIMIT ?
        ''', (client_id, limit))

        migrations = []
        for row in cursor.fetchall():
            migrations.append({
                'session_id': row['session_id'],
                'session_name': row['session_name'],
                'export_type': row['export_type'],
                'workflow_status': row['workflow_status'],
                'created_at': row['created_at'],
                'completed_at': row['completed_at'],
                'ai_model': row['ai_model'],
                'total_files': row['total_files'] or 0,
                'successful_files': row['successful_files'] or 0,
                'failed_files': row['failed_files'] or 0,
                'total_input_tokens': row['total_input_tokens'] or 0,
                'total_output_tokens': row['total_output_tokens'] or 0,
                'estimated_cost_usd': round(row['estimated_cost_usd'] or 0, 6)
            })

        return success_response({'migrations': migrations})

    except Exception as e:
        logger.error(f"Error fetching migration history for client {client_id}: {e}", exc_info=True)
        return server_error_response('Failed to fetch migration history', str(e))


@migration_bp.route('/session/<int:session_id>/details', methods=['GET'])
def get_session_details(session_id):
    """
    Get detailed information about a specific migration session.

    Includes the config snapshot (with sensitive values masked) and file-level metrics.

    Returns:
    {
        "session": {
            "session_id": 42,
            "session_name": "Export - 2025-12-12 10:30:00",
            "export_type": "TABLE",
            "workflow_status": "completed",
            "config_snapshot": { ... masked config ... },
            "ai_model": "claude-3-5-sonnet-20241022",
            "total_input_tokens": 12500,
            "total_output_tokens": 8300,
            "estimated_cost_usd": 0.0234,
            "created_at": "2025-12-12T10:30:00",
            "completed_at": "2025-12-12T10:35:22"
        },
        "files": [
            {
                "file_id": 1,
                "filename": "EMPLOYEES.sql",
                "status": "validated",
                "input_tokens": 500,
                "output_tokens": 300,
                "ai_attempts": 2,
                "error_message": null
            },
            ...
        ]
    }
    """
    import json as json_module
    conn = get_db()
    if not conn:
        return db_error_response()

    try:
        # Get session details
        cursor = execute_query(conn, '''
            SELECT ms.session_id, ms.client_id, ms.session_name, ms.export_type,
                   ms.workflow_status, ms.config_snapshot, ms.ai_model,
                   ms.total_input_tokens, ms.total_output_tokens, ms.estimated_cost_usd,
                   ms.created_at, ms.completed_at
            FROM migration_sessions ms
            WHERE ms.session_id = ?
        ''', (session_id,))
        session_row = cursor.fetchone()

        if not session_row:
            return error_response('Session not found', status_code=404)

        # Parse config snapshot if it exists
        config_snapshot = None
        if session_row['config_snapshot']:
            try:
                config_snapshot = json_module.loads(session_row['config_snapshot'])
            except json_module.JSONDecodeError:
                config_snapshot = {'_error': 'Failed to parse config snapshot'}

        session_data = {
            'session_id': session_row['session_id'],
            'client_id': session_row['client_id'],
            'session_name': session_row['session_name'],
            'export_type': session_row['export_type'],
            'workflow_status': session_row['workflow_status'],
            'config_snapshot': config_snapshot,
            'ai_model': session_row['ai_model'],
            'total_input_tokens': session_row['total_input_tokens'] or 0,
            'total_output_tokens': session_row['total_output_tokens'] or 0,
            'estimated_cost_usd': round(session_row['estimated_cost_usd'] or 0, 6),
            'created_at': session_row['created_at'],
            'completed_at': session_row['completed_at']
        }

        # Get file details
        cursor = execute_query(conn, '''
            SELECT file_id, filename, status, input_tokens, output_tokens,
                   ai_attempts, error_message
            FROM migration_files
            WHERE session_id = ?
            ORDER BY filename
        ''', (session_id,))

        files = []
        for row in cursor.fetchall():
            files.append({
                'file_id': row['file_id'],
                'filename': row['filename'],
                'status': row['status'],
                'input_tokens': row['input_tokens'] or 0,
                'output_tokens': row['output_tokens'] or 0,
                'ai_attempts': row['ai_attempts'] or 0,
                'error_message': row['error_message']
            })

        return success_response({
            'session': session_data,
            'files': files
        })

    except Exception as e:
        logger.error(f"Error fetching session details for session {session_id}: {e}", exc_info=True)
        return server_error_response('Failed to fetch session details', str(e))


@migration_bp.route('/migrations/history', methods=['GET'])
def get_all_migration_history():
    """
    Get recent completed migrations across all clients.

    Query params:
        limit: Number of sessions to return (default 20, max 100)

    Returns:
    {
        "migrations": [
            {
                "session_id": 42,
                "client_id": 1,
                "client_name": "My Oracle DB",
                "session_name": "Export - 2025-12-12 10:30:00",
                "export_type": "TABLE",
                "workflow_status": "completed",
                "created_at": "2025-12-12T10:30:00",
                "completed_at": "2025-12-12T10:35:22",
                "total_files": 15,
                "successful_files": 14,
                "failed_files": 1,
                "total_input_tokens": 12500,
                "total_output_tokens": 8300,
                "estimated_cost_usd": 0.0234,
                "ai_model": "claude-3-5-sonnet-20241022"
            },
            ...
        ]
    }
    """
    conn = get_db()
    if not conn:
        return db_error_response()

    try:
        limit = min(int(request.args.get('limit', 20)), 100)

        cursor = execute_query(conn, '''
            SELECT ms.session_id, ms.client_id, c.client_name,
                   ms.session_name, ms.export_type, ms.workflow_status,
                   ms.created_at, ms.completed_at, ms.ai_model,
                   ms.total_input_tokens, ms.total_output_tokens, ms.estimated_cost_usd,
                   (SELECT COUNT(*) FROM migration_files WHERE session_id = ms.session_id) as total_files,
                   (SELECT COUNT(*) FROM migration_files WHERE session_id = ms.session_id AND status = 'validated') as successful_files,
                   (SELECT COUNT(*) FROM migration_files WHERE session_id = ms.session_id AND status = 'failed') as failed_files
            FROM migration_sessions ms
            JOIN clients c ON ms.client_id = c.client_id
            WHERE ms.workflow_status IN ('completed', 'partial', 'failed')
            ORDER BY ms.created_at DESC
            LIMIT ?
        ''', (limit,))

        migrations = []
        for row in cursor.fetchall():
            migrations.append({
                'session_id': row['session_id'],
                'client_id': row['client_id'],
                'client_name': row['client_name'],
                'session_name': row['session_name'],
                'export_type': row['export_type'],
                'workflow_status': row['workflow_status'],
                'created_at': row['created_at'],
                'completed_at': row['completed_at'],
                'ai_model': row['ai_model'],
                'total_files': row['total_files'] or 0,
                'successful_files': row['successful_files'] or 0,
                'failed_files': row['failed_files'] or 0,
                'total_input_tokens': row['total_input_tokens'] or 0,
                'total_output_tokens': row['total_output_tokens'] or 0,
                'estimated_cost_usd': round(row['estimated_cost_usd'] or 0, 6)
            })

        return success_response({'migrations': migrations})

    except Exception as e:
        logger.error(f"Error fetching global migration history: {e}", exc_info=True)
        return server_error_response('Failed to fetch migration history', str(e))

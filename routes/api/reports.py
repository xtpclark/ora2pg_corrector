"""Report generation and rollback API endpoints."""

from flask import Blueprint, request, jsonify, Response
from modules.db import get_db, execute_query
from modules.reports import MigrationReportGenerator
import psycopg2
import os
import re
import logging

logger = logging.getLogger(__name__)

reports_bp = Blueprint('reports', __name__)


# =============================================================================
# Rollback Script Endpoints
# =============================================================================

@reports_bp.route('/session/<int:session_id>/rollback', methods=['GET'])
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


@reports_bp.route('/session/<int:session_id>/rollback/preview', methods=['GET'])
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


@reports_bp.route('/session/<int:session_id>/rollback/download', methods=['GET'])
def download_rollback_script(session_id):
    """
    Download the rollback script as a .sql file.
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


@reports_bp.route('/session/<int:session_id>/rollback/execute', methods=['POST'])
def execute_rollback(session_id):
    """
    Execute the rollback script against the PostgreSQL validation database.
    Requires confirmation parameter to prevent accidental execution.
    """
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

        try:
            pg_conn = psycopg2.connect(pg_dsn)
            pg_conn.autocommit = False
            pg_cursor = pg_conn.cursor()

            # Execute the full script (it has BEGIN/COMMIT)
            pg_cursor.execute(rollback_script)
            pg_conn.commit()

            # Parse what was dropped for the response
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

@reports_bp.route('/session/<int:session_id>/report', methods=['GET'])
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


@reports_bp.route('/session/<int:session_id>/report/download', methods=['GET'])
def download_migration_report(session_id):
    """
    Download the migration report as a .adoc file.
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


@reports_bp.route('/client/<int:client_id>/migration_report', methods=['GET'])
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

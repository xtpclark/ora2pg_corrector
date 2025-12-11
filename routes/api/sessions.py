"""Session and file management API endpoints."""

from flask import Blueprint, request, jsonify
from modules.db import get_db, execute_query
import os
import logging

logger = logging.getLogger(__name__)

sessions_bp = Blueprint('sessions', __name__)


@sessions_bp.route('/client/<int:client_id>/sessions', methods=['GET'])
def get_sessions(client_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        query = 'SELECT session_id, session_name, created_at, export_type FROM migration_sessions WHERE client_id = ? ORDER BY created_at DESC'
        params = (client_id,)
        cursor = execute_query(conn, query, params)
        sessions = [dict(row) for row in cursor.fetchall()]
        return jsonify(sessions)
    except Exception as e:
        logger.error(f"Failed to fetch sessions for client {client_id}: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch sessions.'}), 500


@sessions_bp.route('/session/<int:session_id>/files', methods=['GET'])
def get_session_files(session_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        query = 'SELECT file_id, filename, status, last_modified FROM migration_files WHERE session_id = ? ORDER BY filename'
        params = (session_id,)
        cursor = execute_query(conn, query, params)
        files = [dict(row) for row in cursor.fetchall()]
        return jsonify(files)
    except Exception as e:
        logger.error(f"Failed to fetch files for session {session_id}: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch session files.'}), 500


@sessions_bp.route('/file/<int:file_id>/status', methods=['POST'])
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
            cursor = execute_query(conn, query, params)

            if cursor.rowcount == 0:
                return jsonify({'error': 'File not found.'}), 404

            conn.commit()
            return jsonify({'message': f'Status for file {file_id} updated to {new_status}.'})
    except Exception as e:
        logger.error(f"Failed to update status for file {file_id}: {e}", exc_info=True)
        return jsonify({'error': 'Failed to update file status.'}), 500


@sessions_bp.route('/get_exported_file', methods=['POST'])
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
        return jsonify({'error': 'File not found on the server filesystem.'}), 404
    except Exception as e:
        logger.error(f"Error reading exported file {file_path}: {e}", exc_info=True)
        return jsonify({'error': 'An unexpected error occurred while reading the file content.'}), 500

"""DDL cache management API endpoints."""

from flask import Blueprint, request, jsonify
from modules.db import get_db, execute_query
from modules.audit import log_audit
import os
import json
import re
import logging

logger = logging.getLogger(__name__)

ddl_cache_bp = Blueprint('ddl_cache', __name__)


@ddl_cache_bp.route('/client/<int:client_id>/ddl_cache/stats', methods=['GET'])
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


@ddl_cache_bp.route('/client/<int:client_id>/ddl_cache', methods=['DELETE'])
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


@ddl_cache_bp.route('/session/<int:session_id>/generated_ddl', methods=['GET'])
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


@ddl_cache_bp.route('/session/<int:session_id>/generated_ddl/<object_name>', methods=['GET'])
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

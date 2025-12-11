"""Migration objects tracking API endpoints."""

from flask import Blueprint, request, jsonify
from modules.db import get_db, execute_query
from modules.responses import (
    success_response, error_response, not_found_response,
    server_error_response, db_error_response
)
import logging

logger = logging.getLogger(__name__)

objects_bp = Blueprint('objects', __name__)


@objects_bp.route('/session/<int:session_id>/objects', methods=['GET'])
def get_session_objects(session_id):
    """
    Get all objects for a migration session with their status.
    Optional filters: ?type=TABLE&status=validated
    """
    conn = get_db()
    if not conn:
        return db_error_response()

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

        return success_response({
            'session_id': session_id,
            'total_objects': len(objects),
            'summary': summary,
            'objects': objects
        })

    except Exception as e:
        logger.error(f"Failed to get session objects: {e}")
        return server_error_response('Failed to get session objects', str(e))


@objects_bp.route('/session/<int:session_id>/objects/summary', methods=['GET'])
def get_session_objects_summary(session_id):
    """
    Get summary counts of objects by type and status.
    Lightweight endpoint for progress display.
    """
    conn = get_db()
    if not conn:
        return db_error_response()

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

        return success_response({
            'session_id': session_id,
            'totals': totals,
            'by_type': by_type
        })

    except Exception as e:
        logger.error(f"Failed to get objects summary: {e}")
        return server_error_response('Failed to get objects summary', str(e))


@objects_bp.route('/object/<int:object_id>', methods=['GET'])
def get_object_detail(object_id):
    """
    Get detailed information about a specific object, including DDL.
    """
    conn = get_db()
    if not conn:
        return db_error_response()

    try:
        query = '''SELECT mo.*, mf.filename, ms.export_directory
                   FROM migration_objects mo
                   LEFT JOIN migration_files mf ON mo.file_id = mf.file_id
                   LEFT JOIN migration_sessions ms ON mo.session_id = ms.session_id
                   WHERE mo.object_id = ?'''

        cursor = execute_query(conn, query, (object_id,))
        obj = cursor.fetchone()

        if not obj:
            return not_found_response('Object')

        return success_response(dict(obj))

    except Exception as e:
        logger.error(f"Failed to get object detail: {e}")
        return server_error_response('Failed to get object detail', str(e))


@objects_bp.route('/client/<int:client_id>/objects/summary', methods=['GET'])
def get_client_objects_summary(client_id):
    """
    Get object summary across all sessions for a client.
    Shows aggregate view of migration progress.
    """
    conn = get_db()
    if not conn:
        return db_error_response()

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

        return success_response({
            'client_id': client_id,
            'totals': totals,
            'by_type': by_type
        })

    except Exception as e:
        logger.error(f"Failed to get client objects summary: {e}")
        return server_error_response('Failed to get client objects summary', str(e))

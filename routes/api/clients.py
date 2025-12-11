"""Client management API endpoints."""

from flask import Blueprint, request, jsonify
from modules.db import get_db, execute_query, insert_returning_id
from modules.audit import log_audit
from modules.constants import get_client_project_dir
from modules.responses import (
    success_response, error_response, created_response,
    not_found_response, validation_error_response, server_error_response, db_error_response
)
import sqlite3
import psycopg2
import shutil
import os
import logging

logger = logging.getLogger(__name__)

clients_bp = Blueprint('clients', __name__)


@clients_bp.route('/clients', methods=['GET', 'POST'])
def manage_clients():
    conn = get_db()
    if not conn:
        return db_error_response()

    if request.method == 'GET':
        try:
            cursor = execute_query(
                conn,
                'SELECT client_id, client_name, created_at as last_modified FROM clients ORDER BY client_name'
            )
            clients = [dict(row) for row in cursor.fetchall()]
            return success_response(clients)
        except Exception as e:
            return server_error_response("Failed to fetch clients", str(e))

    elif request.method == 'POST':
        client_name = request.json.get('client_name')
        if not client_name:
            return validation_error_response('Client name is required')
        try:
            with conn:
                client_id = insert_returning_id(
                    conn, 'clients', ('client_name',), (client_name,), 'client_id'
                )
                cursor = execute_query(
                    conn,
                    'SELECT client_id, client_name, created_at as last_modified FROM clients WHERE client_id = ?',
                    (client_id,)
                )
                new_client = dict(cursor.fetchone())
                conn.commit()
                log_audit(new_client['client_id'], 'create_client', f'Created client: {client_name}')
                return created_response(new_client, 'Client created successfully')
        except (sqlite3.IntegrityError, psycopg2.IntegrityError):
            return error_response('Client name already exists', status_code=409)
        except Exception as e:
            return server_error_response('An internal error occurred', str(e))


@clients_bp.route('/client/<int:client_id>', methods=['PUT', 'DELETE'])
def manage_single_client(client_id):
    conn = get_db()
    if not conn:
        return db_error_response()

    if request.method == 'PUT':
        # Rename client
        data = request.get_json()
        new_name = data.get('client_name')
        if not new_name:
            return validation_error_response('Client name is required')

        try:
            query = 'UPDATE clients SET client_name = ? WHERE client_id = ?'
            params = (new_name, client_id)
            with conn:
                execute_query(conn, query, params)
                conn.commit()

            log_audit(client_id, 'rename_client', f'Renamed to: {new_name}')
            return success_response(message='Client renamed successfully')
        except Exception as e:
            return server_error_response('Failed to rename client', str(e))

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

                for query in queries:
                    execute_query(conn, query, (client_id,))

                conn.commit()

            # Delete physical files if they exist
            client_dir = get_client_project_dir(client_id)
            if os.path.exists(client_dir):
                shutil.rmtree(client_dir)

            return success_response(message='Client deleted successfully')
        except Exception as e:
            logger.error(f"Failed to delete client {client_id}: {e}", exc_info=True)
            return server_error_response('Failed to delete client', str(e))


@clients_bp.route('/client/<int:client_id>/audit_logs', methods=['GET'])
def get_audit_logs(client_id):
    conn = get_db()
    if not conn:
        return db_error_response()
    try:
        query = 'SELECT timestamp, action, details FROM audit_logs WHERE client_id = ? ORDER BY timestamp DESC'
        params = (client_id,)
        cursor = execute_query(conn, query, params)
        logs = [dict(row) for row in cursor.fetchall()]
        return success_response(logs)
    except Exception as e:
        return server_error_response('Failed to fetch audit logs', str(e))


@clients_bp.route('/client/<int:client_id>/log_audit', methods=['POST'])
def log_audit_event(client_id):
    data = request.json
    action, details = data.get('action'), data.get('details')
    if not action:
        return validation_error_response('Action is required for audit log')
    try:
        log_audit(client_id, action, details)
        return success_response(message='Audit event logged successfully')
    except Exception as e:
        return server_error_response('Failed to log audit event', str(e))

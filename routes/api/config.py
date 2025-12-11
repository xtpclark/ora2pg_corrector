"""Configuration management API endpoints."""

from flask import Blueprint, request, jsonify
from modules.db import get_db, execute_query, get_client_config, ENCRYPTION_KEY
from modules.audit import log_audit
from cryptography.fernet import Fernet
import os
import logging

logger = logging.getLogger(__name__)

config_bp = Blueprint('config', __name__)


@config_bp.route('/app_settings', methods=['GET'])
def get_app_settings():
    """Returns application-level settings to the frontend."""
    settings = {
        'validation_pg_dsn': os.environ.get('VALIDATION_PG_DSN', '')
    }
    return jsonify(settings)


@config_bp.route('/ai_providers', methods=['GET'])
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


@config_bp.route('/ora2pg_config_options', methods=['GET'])
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


@config_bp.route('/client/<int:client_id>/config', methods=['GET', 'POST'])
def manage_config(client_id):
    conn = get_db()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    if request.method == 'GET':
        try:
            cursor = execute_query(
                conn,
                'SELECT config_key, config_value FROM configs WHERE client_id = ?',
                (client_id,)
            )
            config_items = {row['config_key']: row['config_value'] for row in cursor.fetchall()}
            return jsonify(config_items)
        except Exception as e:
            logger.error(f"Error fetching config for client {client_id}: {e}")
            return jsonify({'error': str(e)}), 500

    elif request.method == 'POST':
        new_config = request.json
        if not new_config:
            return jsonify({'error': 'No configuration data provided'}), 400

        fernet = Fernet(ENCRYPTION_KEY)
        sensitive_keys = ['oracle_pwd', 'ai_api_key']

        try:
            with conn:
                for key, value in new_config.items():
                    if value is None:
                        continue
                    if key in sensitive_keys and value:
                        value = fernet.encrypt(value.encode()).decode()

                    execute_query(
                        conn,
                        'DELETE FROM configs WHERE client_id = ? AND config_key = ?',
                        (client_id, key)
                    )
                    execute_query(
                        conn,
                        'INSERT INTO configs (client_id, config_type, config_key, config_value) VALUES (?, ?, ?, ?)',
                        (client_id, 'ora2pg', key, value)
                    )
                conn.commit()

            log_audit(client_id, 'save_config', f'Saved {len(new_config)} config items')
            return jsonify({'message': 'Configuration saved successfully'}), 200
        except Exception as e:
            logger.error(f"Error saving config for client {client_id}: {e}")
            return jsonify({'error': str(e)}), 500

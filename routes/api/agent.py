"""
API endpoints for the autonomous migration agent.

POST /api/agent/migrate     - Start a migration
GET  /api/agent/status/<id> - Check migration status
GET  /api/agent/migrations  - List all migrations
"""

import json
import logging
import os
import threading
from pathlib import Path
from typing import Optional

from flask import Blueprint, request, jsonify

from modules.db import get_db, get_db_standalone, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from modules.migration_agent import MigrationAgent
from modules.responses import success_response, error_response
from modules.constants import DATA_DIR

logger = logging.getLogger(__name__)

agent_bp = Blueprint('agent', __name__, url_prefix='/agent')

# File-backed migration store (works across Gunicorn workers)
MIGRATIONS_DIR = os.path.join(DATA_DIR, 'agent_migrations')
os.makedirs(MIGRATIONS_DIR, exist_ok=True)


def _migration_file(migration_id: int) -> str:
    return os.path.join(MIGRATIONS_DIR, f"{migration_id}.json")


def _save_migration(migration_id: int, data: dict):
    """Persist migration state to file."""
    with open(_migration_file(migration_id), 'w') as f:
        json.dump(data, f)


def _load_migration(migration_id: int) -> Optional[dict]:
    """Load migration state from file."""
    path = _migration_file(migration_id)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def _next_migration_id() -> int:
    """Get next migration ID based on existing files."""
    existing = [
        int(f.stem) for f in Path(MIGRATIONS_DIR).glob('*.json')
        if f.stem.isdigit()
    ]
    return max(existing, default=0) + 1


def _store_progress(migration_id: int):
    """Returns a callback that updates migration progress on disk."""
    def callback(phase: str, message: str, pct: int):
        data = _load_migration(migration_id)
        if data:
            data['phase'] = phase
            data['message'] = message
            data['progress'] = pct
            _save_migration(migration_id, data)
    return callback


def _run_migration(migration_id: int, client_id: int, config: dict, ai_settings: dict):
    """Run migration in background thread with its own DB connection."""
    db_conn = get_db_standalone()
    try:
        agent = MigrationAgent(
            client_id=client_id,
            client_config=config,
            app_db_conn=db_conn,
            encryption_key=ENCRYPTION_KEY,
            ai_settings=ai_settings,
            callback=_store_progress(migration_id),
        )
        result = agent.run()

        data = _load_migration(migration_id)
        if data:
            data['status'] = 'complete' if result.success else 'failed'
            data['result'] = {
                'success': result.success,
                'phase': result.phase.value,
                'total_objects': result.total_objects,
                'migrated': result.migrated,
                'failed': result.failed,
                'skipped': result.skipped,
                'data_tables_migrated': result.data_tables_migrated,
                'data_rows_migrated': result.data_rows_migrated,
                'ai_cost_usd': result.ai_cost_usd,
                'duration_seconds': result.duration_seconds,
                'errors': result.errors,
                'edge_cases': result.edge_cases,
                'objects': result.objects,
            }
            _save_migration(migration_id, data)

    except Exception as e:
        logger.exception(f"Migration {migration_id} failed unexpectedly")
        data = _load_migration(migration_id)
        if data:
            data['status'] = 'failed'
            data['result'] = {'error': str(e)}
            _save_migration(migration_id, data)
    finally:
        if db_conn:
            db_conn.close()


@agent_bp.route('/migrate', methods=['POST'])
def start_migration():
    """
    Start an autonomous migration for a client.

    JSON body:
        client_id: int (required) - Client with Oracle + PG config already set up
    """
    data = request.get_json()
    if not data or 'client_id' not in data:
        return error_response("client_id is required", 400)

    client_id = data['client_id']

    # Load client config
    try:
        config = get_client_config(client_id)
        if not config:
            return error_response(f"Client {client_id} not found", 404)
    except Exception as e:
        return error_response(f"Failed to load client config: {e}", 500)

    # Validate required settings
    if not config.get('oracle_schema'):
        config['oracle_schema'] = config.get('schema', '')

    required = ['oracle_dsn', 'oracle_user', 'oracle_pwd', 'oracle_schema', 'validation_pg_dsn']
    missing = [k for k in required if not config.get(k)]
    if missing:
        return error_response(f"Client config incomplete. Missing: {', '.join(missing)}", 400)

    ai_settings = extract_ai_settings(config)
    migration_id = _next_migration_id()

    # Create initial state file
    migration_data = {
        'id': migration_id,
        'client_id': client_id,
        'client_name': config.get('client_name', f'Client {client_id}'),
        'status': 'running',
        'phase': 'connect',
        'message': 'Starting migration...',
        'progress': 0,
        'result': None,
    }
    _save_migration(migration_id, migration_data)

    # Launch in background thread
    thread = threading.Thread(
        target=_run_migration,
        args=(migration_id, client_id, config, ai_settings),
        daemon=True,
    )
    thread.start()

    return success_response({
        'migration_id': migration_id,
        'status': 'running',
        'message': f'Autonomous migration started for client {client_id}',
    })


@agent_bp.route('/status/<int:migration_id>', methods=['GET'])
def migration_status(migration_id: int):
    """Get the current status of a running or completed migration."""
    migration = _load_migration(migration_id)
    if not migration:
        return error_response(f"Migration {migration_id} not found", 404)
    return success_response(migration)


@agent_bp.route('/migrations', methods=['GET'])
def list_migrations():
    """List all migrations (running and completed)."""
    migrations = []
    for f in sorted(Path(MIGRATIONS_DIR).glob('*.json')):
        try:
            with open(f) as fh:
                migrations.append(json.load(fh))
        except Exception:
            pass
    return success_response({'migrations': migrations})

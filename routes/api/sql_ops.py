"""SQL operations API endpoints (correct, validate, save, connection tests)."""

from flask import Blueprint, request, jsonify
from modules.db import get_db, get_client_config, extract_ai_settings, ENCRYPTION_KEY
from modules.audit import log_audit
from modules.sql_processing import Ora2PgAICorrector
from modules.constants import OUTPUT_DIR
import psycopg2
import logging

logger = logging.getLogger(__name__)

sql_ops_bp = Blueprint('sql_ops', __name__)


@sql_ops_bp.route('/correct_sql', methods=['POST'])
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
            output_dir=OUTPUT_DIR,
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


@sql_ops_bp.route('/validate', methods=['POST'])
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
            output_dir=OUTPUT_DIR,
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
        if clean_slate:
            options.append('Clean Slate')
        if auto_create_ddl:
            options.append('Auto-create DDL')
        if options:
            audit_details += f" (Options: {', '.join(options)})"
        log_audit(client_id, 'validate_sql', audit_details)

        return jsonify({'message': message, 'status': 'success' if is_valid else 'error', 'corrected_sql': new_sql})
    except Exception as e:
        logger.error(f"Failed to validate SQL: {e}", exc_info=True)
        return jsonify({'error': f'Failed to validate SQL: {str(e)}'}), 500


@sql_ops_bp.route('/save', methods=['POST'])
def save_sql():
    data = request.json
    original_sql = data.get('original_sql')
    corrected_sql = data.get('corrected_sql')
    client_id = data.get('client_id')
    filename = data.get('filename', 'corrected_output.sql')

    if not corrected_sql or not client_id:
        return jsonify({'error': 'Corrected SQL and client ID are required'}), 400

    corrector = Ora2PgAICorrector(output_dir=OUTPUT_DIR, ai_settings={}, encryption_key=ENCRYPTION_KEY)

    try:
        output_path = corrector.save_corrected_file(original_sql, corrected_sql, filename)
        log_audit(client_id, 'save_file', f'Saved corrected SQL to {output_path}')
        return jsonify({'message': f'Successfully saved file to {output_path}'})
    except Exception as e:
        return jsonify({'error': f'Failed to save SQL: {str(e)}'}), 500


@sql_ops_bp.route('/test_pg_connection', methods=['POST'])
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

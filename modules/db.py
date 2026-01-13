import sqlite3
import psycopg2
import psycopg2.extras
import os
import logging
from flask import g
from cryptography.fernet import Fernet

from .constants import (
    DATA_DIR, SQLITE_DB_PATH, ENCRYPTION_KEY_FILE,
    SENSITIVE_CONFIG_KEYS, BOOLEAN_CONFIG_KEYS,
    DEFAULT_AI_TEMPERATURE, DEFAULT_AI_MAX_OUTPUT_TOKENS
)

logger = logging.getLogger(__name__)

# Encryption key for sensitive config values
# Priority: 1) Environment variable, 2) Persisted key file, 3) Generate new (and persist)
_ENCRYPTION_KEY_STR = os.environ.get('APP_ENCRYPTION_KEY')

if _ENCRYPTION_KEY_STR:
    ENCRYPTION_KEY = _ENCRYPTION_KEY_STR.encode()
elif os.path.exists(ENCRYPTION_KEY_FILE):
    with open(ENCRYPTION_KEY_FILE, 'rb') as f:
        ENCRYPTION_KEY = f.read()
    logger.info("Loaded encryption key from persistent storage")
else:
    ENCRYPTION_KEY = Fernet.generate_key()
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(ENCRYPTION_KEY_FILE, 'wb') as f:
            f.write(ENCRYPTION_KEY)
        os.chmod(ENCRYPTION_KEY_FILE, 0o600)
        logger.info("Generated and persisted new encryption key")
    except Exception as e:
        logger.warning(f"Could not persist encryption key: {e}. Key will be lost on restart.")

def get_db():
    """Get the database connection from the Flask global context."""
    if 'db' not in g:
        try:
            if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                g.db = sqlite3.connect(SQLITE_DB_PATH, timeout=10)

                g.db.row_factory = sqlite3.Row
            else:
                if not os.environ.get('PG_DSN_CONFIG'):
                    raise ValueError("PG_DSN_CONFIG not set for PostgreSQL backend.")
                g.db = psycopg2.connect(os.environ.get('PG_DSN_CONFIG'), cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            g.db = None
    return g.db

def close_db(e=None):
    """Close the database connection."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def is_postgres():
    """Check if using PostgreSQL backend."""
    return os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite'


def normalize_query(query):
    """Convert query placeholders for the configured database backend.

    SQLite uses ? placeholders, PostgreSQL uses %s.
    This function converts ? to %s when using PostgreSQL.
    """
    if is_postgres():
        return query.replace('?', '%s')
    return query


def execute_query(conn, query, params=None):
    """Execute a SQL query with parameter substitution for different backends."""
    cursor = conn.cursor()
    query = normalize_query(query)
    try:
        cursor.execute(query, params or ())
        return cursor
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


def insert_returning_id(conn, table, columns, values, id_column='id'):
    """Insert a row and return the generated ID.

    Handles the difference between SQLite (lastrowid) and PostgreSQL (RETURNING).

    Args:
        conn: Database connection
        table: Table name
        columns: Tuple of column names
        values: Tuple of values to insert
        id_column: Name of the auto-increment column (for RETURNING clause)

    Returns:
        The generated ID
    """
    placeholders = ', '.join(['?' for _ in values])
    columns_str = ', '.join(columns)

    if is_postgres():
        query = f'INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) RETURNING {id_column}'
        query = normalize_query(query)
        cursor = conn.cursor()
        cursor.execute(query, values)
        result = cursor.fetchone()
        return result[id_column] if hasattr(result, '__getitem__') else result[0]
    else:
        query = f'INSERT INTO {table} ({columns_str}) VALUES ({placeholders})'
        cursor = conn.cursor()
        cursor.execute(query, values)
        return cursor.lastrowid

def init_db():
    """Initialize the database schema."""
    conn = get_db()
    if not conn:
        logger.error("DB connection failed, aborting initialization.")
        return
    try:
        is_sqlite = os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite'
        pk_type = 'INTEGER PRIMARY KEY AUTOINCREMENT' if is_sqlite else 'SERIAL PRIMARY KEY'
        ts_type = 'TIMESTAMP' if is_sqlite else 'TIMESTAMP WITH TIME ZONE'
        with conn:
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS clients (
                client_id {pk_type},
                client_name TEXT NOT NULL UNIQUE,
                created_at {ts_type} DEFAULT CURRENT_TIMESTAMP
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS configs (
                config_id {pk_type},
                client_id INTEGER NOT NULL,
                config_type TEXT NOT NULL,
                config_key TEXT NOT NULL,
                config_value TEXT,
                last_modified {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS audit_logs (
                log_id {pk_type},
                client_id INTEGER,
                action TEXT NOT NULL,
                details TEXT,
                timestamp {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS ai_providers (
                provider_id {pk_type},
                name TEXT NOT NULL UNIQUE,
                api_endpoint TEXT NOT NULL,
                default_model TEXT,
                key_url TEXT,
                notes TEXT
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS ora2pg_config_options (
                option_id {pk_type},
                option_name TEXT NOT NULL UNIQUE,
                option_type TEXT NOT NULL,
                default_value TEXT,
                description TEXT,
                allowed_values TEXT
            )''')
            # --- UPDATED: Added migration session and file tracking tables ---
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS migration_sessions (
                session_id {pk_type},
                client_id INTEGER NOT NULL,
                session_name TEXT NOT NULL,
                export_directory TEXT NOT NULL,
                export_type TEXT,
                workflow_status TEXT DEFAULT 'pending',
                current_phase TEXT,
                processed_count INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0,
                current_file TEXT,
                rollback_script TEXT,
                rollback_generated_at {ts_type},
                created_at {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS migration_files (
                file_id {pk_type},
                session_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'generated',
                corrected_content TEXT,
                error_message TEXT,
                last_modified {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES migration_sessions(session_id) ON DELETE CASCADE
            )''')
            # --- DDL Cache table for AI-generated DDL reuse ---
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS ddl_cache (
                cache_id {pk_type},
                client_id INTEGER NOT NULL,
                session_id INTEGER,
                object_name TEXT NOT NULL,
                object_type TEXT DEFAULT 'TABLE',
                generated_ddl TEXT NOT NULL,
                ai_provider TEXT,
                ai_model TEXT,
                hit_count INTEGER DEFAULT 0,
                created_at {ts_type} DEFAULT CURRENT_TIMESTAMP,
                last_used {ts_type},
                FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
            )''')
            # Create unique index for cache lookups
            if is_sqlite:
                execute_query(conn, '''CREATE UNIQUE INDEX IF NOT EXISTS idx_ddl_cache_lookup
                    ON ddl_cache(client_id, object_name)''')
            else:
                execute_query(conn, '''CREATE UNIQUE INDEX IF NOT EXISTS idx_ddl_cache_lookup
                    ON ddl_cache(client_id, object_name)''')

            # --- Migration Objects table for per-object tracking ---
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS migration_objects (
                object_id {pk_type},
                session_id INTEGER NOT NULL,
                file_id INTEGER,
                object_name TEXT NOT NULL,
                object_type TEXT NOT NULL,
                schema_name TEXT,
                status TEXT DEFAULT 'pending',
                original_ddl TEXT,
                corrected_ddl TEXT,
                error_message TEXT,
                line_start INTEGER,
                line_end INTEGER,
                ai_corrected INTEGER DEFAULT 0,
                created_at {ts_type} DEFAULT CURRENT_TIMESTAMP,
                validated_at {ts_type},
                FOREIGN KEY (session_id) REFERENCES migration_sessions(session_id) ON DELETE CASCADE,
                FOREIGN KEY (file_id) REFERENCES migration_files(file_id) ON DELETE SET NULL
            )''')
            # Index for efficient lookups by session
            execute_query(conn, '''CREATE INDEX IF NOT EXISTS idx_migration_objects_session
                ON migration_objects(session_id, object_type)''')

        # Run schema migrations for existing tables
        _run_schema_migrations(conn)

        logger.info("Database schema initialized successfully.")
    except Exception as e:
        logger.error(f"Error during DB schema initialization: {e}")


def _run_schema_migrations(conn):
    """Apply schema migrations to add missing columns to existing tables."""
    is_sqlite = os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite'

    # Define migrations: (table_name, column_name, column_definition)
    migrations = [
        ('migration_sessions', 'workflow_status', "TEXT DEFAULT 'pending'"),
        ('migration_files', 'corrected_content', 'TEXT'),
        ('migration_files', 'error_message', 'TEXT'),
        # Rollback script support
        ('migration_sessions', 'rollback_script', 'TEXT'),
        ('migration_sessions', 'rollback_generated_at', 'TIMESTAMP'),
        # Progress tracking columns (for multi-worker race condition fix)
        ('migration_sessions', 'current_phase', 'TEXT'),
        ('migration_sessions', 'processed_count', 'INTEGER DEFAULT 0'),
        ('migration_sessions', 'total_count', 'INTEGER DEFAULT 0'),
        ('migration_sessions', 'current_file', 'TEXT'),
        # Migration history feature - config snapshot and token tracking
        ('migration_sessions', 'config_snapshot', 'TEXT'),
        ('migration_sessions', 'total_input_tokens', 'INTEGER DEFAULT 0'),
        ('migration_sessions', 'total_output_tokens', 'INTEGER DEFAULT 0'),
        ('migration_sessions', 'estimated_cost_usd', 'REAL DEFAULT 0'),
        ('migration_sessions', 'ai_model', 'TEXT'),
        ('migration_sessions', 'completed_at', 'TIMESTAMP'),
        # Per-file token tracking
        ('migration_files', 'input_tokens', 'INTEGER DEFAULT 0'),
        ('migration_files', 'output_tokens', 'INTEGER DEFAULT 0'),
        ('migration_files', 'ai_attempts', 'INTEGER DEFAULT 0'),
    ]

    for table_name, column_name, column_def in migrations:
        try:
            if is_sqlite:
                # Check if column exists using pragma
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                if column_name not in columns:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
                    logger.info(f"Added column {column_name} to {table_name}")
            else:
                # PostgreSQL: use information_schema
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s
                """, (table_name, column_name))
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
                    conn.commit()
                    logger.info(f"Added column {column_name} to {table_name}")
        except Exception as e:
            logger.warning(f"Migration skipped for {table_name}.{column_name}: {e}")

def init_db_command():
    """Flask command to initialize the database."""
    init_db()
    print("Initialized the database.")


def get_client_config(client_id, conn=None, decrypt_keys=None):
    """
    Load client configuration from the database with optional decryption.

    :param int client_id: The client ID to load config for
    :param conn: Optional database connection (uses get_db() if not provided)
    :param list decrypt_keys: List of keys to decrypt (e.g., ['oracle_pwd', 'ai_api_key'])
                             Defaults to ['oracle_pwd', 'ai_api_key'] if None
    :return: Dictionary of config key-value pairs with decrypted values
    :rtype: dict
    """
    if conn is None:
        conn = get_db()

    if decrypt_keys is None:
        decrypt_keys = SENSITIVE_CONFIG_KEYS

    query = 'SELECT config_key, config_value FROM configs WHERE client_id = ?'
    cursor = execute_query(conn, query, (client_id,))
    config = {row['config_key']: row['config_value'] for row in cursor.fetchall()}

    # Decrypt sensitive values
    fernet = Fernet(ENCRYPTION_KEY)
    for key in decrypt_keys:
        if key in config and config[key]:
            try:
                config[key] = fernet.decrypt(config[key].encode()).decode()
            except Exception:
                # Value may not be encrypted (e.g., during testing)
                pass

    # Convert boolean string values
    for key in BOOLEAN_CONFIG_KEYS:
        if key in config:
            config[key] = str(config[key]) in ('1', 'true', 'True')

    return config


def extract_ai_settings(config):
    """
    Extract AI settings from a config dictionary into the format expected by Ora2PgAICorrector.

    :param dict config: Client configuration dictionary
    :return: Dictionary of AI settings
    :rtype: dict
    """
    return {
        'ai_provider': config.get('ai_provider'),
        'ai_endpoint': config.get('ai_endpoint'),
        'ai_model': config.get('ai_model'),
        'ai_api_key': config.get('ai_api_key'),
        'ai_temperature': float(config.get('ai_temperature', DEFAULT_AI_TEMPERATURE)),
        'ai_max_output_tokens': int(config.get('ai_max_output_tokens', DEFAULT_AI_MAX_OUTPUT_TOKENS)),
        # Corporate proxy settings
        'ai_user': config.get('ai_user', 'anonymous'),
        'ai_user_header': config.get('ai_user_header', ''),
        'ssl_cert_path': config.get('ssl_cert_path', ''),
        'ai_ssl_verify': config.get('ai_ssl_verify', True)
    }


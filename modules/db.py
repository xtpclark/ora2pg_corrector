import sqlite3
import psycopg2
import psycopg2.extras
import os
import logging
from flask import g

logger = logging.getLogger(__name__)

def get_db():
    """Get the database connection from the Flask global context."""
    if 'db' not in g:
        try:
            if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                g.db = sqlite3.connect('/app/data/settings.db', timeout=10)

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

def execute_query(conn, query, params=None):
    """Execute a SQL query with parameter substitution for different backends."""
    cursor = conn.cursor()
    if os.environ.get('DB_BACKEND', 'sqlite') != 'sqlite':
        query = query.replace('?', '%s')
    try:
        cursor.execute(query, params or ())
        return cursor
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise

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
                created_at {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
            )''')
            execute_query(conn, f'''CREATE TABLE IF NOT EXISTS migration_files (
                file_id {pk_type},
                session_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'generated',
                last_modified {ts_type} DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES migration_sessions(session_id) ON DELETE CASCADE
            )''')

        logger.info("Database schema initialized successfully.")
    except Exception as e:
        logger.error(f"Error during DB schema initialization: {e}")

def init_db_command():
    """Flask command to initialize the database."""
    init_db()
    print("Initialized the database.")


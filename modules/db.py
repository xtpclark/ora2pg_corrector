import sqlite3
import psycopg2
import psycopg2.extras
import os
import logging

logger = logging.getLogger(__name__)

def get_db():
    from flask import g
    if 'db' not in g:
        try:
            if os.environ.get('DB_BACKEND', 'sqlite') == 'sqlite':
                g.db = sqlite3.connect('/app/settings.db', timeout=10)
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
    from flask import g
    db = g.pop('db', None)
    if db is not None:
        db.close()

def execute_query(conn, query, params=None):
    cursor = conn.cursor()
    if os.environ.get('DB_BACKEND', 'sqlite') == 'postgresql':
        query = query.replace('?', '%s')
    try:
        cursor.execute(query, params or ())
        return cursor
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise

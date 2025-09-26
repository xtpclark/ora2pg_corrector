from .db import get_db, execute_query
from flask import g

def log_audit(client_id, action, details):
    conn = get_db()
    if not conn:
        return
    with conn:
        execute_query(conn, 'INSERT INTO audit_logs (client_id, user_id, action, details) VALUES (?, ?, ?, ?)', (client_id, g.get('user_id'), action, details))
        conn.commit()

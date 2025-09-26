def log_audit(client_id, action, details):
    """Logs an audit event to the database."""
    from .db import get_db, execute_query
    conn = get_db()
    if not conn:
        return
    with conn:
        execute_query(conn, 
                      'INSERT INTO audit_logs (client_id, action, details) VALUES (?, ?, ?)', 
                      (client_id, action, details))
        conn.commit()

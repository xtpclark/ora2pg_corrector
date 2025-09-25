CREATE TABLE IF NOT EXISTS clients (
    client_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS configs (
    config_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    config_type TEXT NOT NULL,
    config_key TEXT NOT NULL,
    config_value TEXT,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE,
    UNIQUE (client_id, config_type, config_key)
);

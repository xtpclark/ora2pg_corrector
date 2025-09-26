import configparser
import json
import os
import logging

logger = logging.getLogger(__name__)

def load_ora2pg_config(conn):
    config_path = '/app/ora2pg_config/default.cfg'
    if not os.path.exists(config_path):
        logger.error(f"Ora2Pg config file not found at {config_path}")
        return
    config = configparser.ConfigParser()
    try:
        config.read(config_path)
    except configparser.Error as e:
        logger.error(f"Error parsing {config_path}: {e}")
        return
    options = []
    for section in config.sections():
        option = (
            section,
            config.get(section, 'option_type', fallback='text'),
            config.get(section, 'default_value', fallback=''),
            config.get(section, 'description', fallback=''),
            config.get(section, 'allowed_values', fallback=None)
        )
        options.append(option)
    insert_sql = 'INSERT INTO ora2pg_config_options (option_name, option_type, default_value, description, allowed_values) VALUES (?, ?, ?, ?, ?)'
    if os.environ.get('DB_BACKEND', 'sqlite') == 'postgresql':
        insert_sql = 'INSERT INTO ora2pg_config_options (option_name, option_type, default_value, description, allowed_values) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (option_name) DO NOTHING'
    else:
        insert_sql = 'INSERT OR IGNORE INTO ora2pg_config_options (option_name, option_type, default_value, description, allowed_values) VALUES (?, ?, ?, ?, ?)'
    with conn:
        cursor = conn.cursor()
        for option in options:
            cursor.execute(insert_sql, option)
        conn.commit()
    logger.info(f"Seeded {len(options)} Ora2Pg config options from {config_path}.")

def load_ai_providers(conn):
    config_path = '/app/ai_config/ai_providers.json'
    if not os.path.exists(config_path):
        logger.error(f"AI providers config file not found at {config_path}")
        return
    try:
        with open(config_path, 'r') as f:
            data = json.load(f)
        providers = data.get('providers', [])
        insert_sql = 'INSERT INTO ai_providers (name, api_endpoint, default_model, key_url, notes) VALUES (?, ?, ?, ?, ?)'
        if os.environ.get('DB_BACKEND', 'sqlite') == 'postgresql':
            insert_sql = 'INSERT INTO ai_providers (name, api_endpoint, default_model, key_url, notes) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (name) DO NOTHING'
        else:
            insert_sql = 'INSERT OR IGNORE INTO ai_providers (name, api_endpoint, default_model, key_url, notes) VALUES (?, ?, ?, ?, ?)'
        with conn:
            cursor = conn.cursor()
            for provider in providers:
                cursor.execute(insert_sql, (
                    provider['name'],
                    provider['api_endpoint'],
                    provider['default_model'],
                    provider['key_url'],
                    provider['notes']
                ))
            conn.commit()
        logger.info(f"Seeded {len(providers)} AI providers from {config_path}.")
    except Exception as e:
        logger.error(f"Error loading AI providers from {config_path}: {e}")

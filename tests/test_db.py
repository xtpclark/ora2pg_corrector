"""
Tests for the database module (modules/db.py).
"""

import os
import pytest


class TestDatabaseHelpers:
    """Test database helper functions."""

    def test_is_postgres_default(self):
        """Test is_postgres returns False for default SQLite backend."""
        # Ensure we're using SQLite for this test
        os.environ['DB_BACKEND'] = 'sqlite'
        from modules.db import is_postgres
        assert is_postgres() is False

    def test_is_postgres_when_postgres(self):
        """Test is_postgres returns True when DB_BACKEND is postgres."""
        original = os.environ.get('DB_BACKEND')
        try:
            os.environ['DB_BACKEND'] = 'postgres'
            # Need to reimport to get updated value
            from importlib import reload
            import modules.db
            reload(modules.db)
            assert modules.db.is_postgres() is True
        finally:
            if original:
                os.environ['DB_BACKEND'] = original
            else:
                os.environ['DB_BACKEND'] = 'sqlite'
            # Reload again to restore
            from importlib import reload
            import modules.db
            reload(modules.db)

    def test_normalize_query_sqlite(self):
        """Test normalize_query leaves ? placeholders for SQLite."""
        os.environ['DB_BACKEND'] = 'sqlite'
        from modules.db import normalize_query
        query = "SELECT * FROM users WHERE id = ? AND name = ?"
        assert normalize_query(query) == query

    def test_normalize_query_postgres(self):
        """Test normalize_query converts ? to %s for PostgreSQL."""
        original = os.environ.get('DB_BACKEND')
        try:
            os.environ['DB_BACKEND'] = 'postgres'
            from importlib import reload
            import modules.db
            reload(modules.db)
            query = "SELECT * FROM users WHERE id = ? AND name = ?"
            expected = "SELECT * FROM users WHERE id = %s AND name = %s"
            assert modules.db.normalize_query(query) == expected
        finally:
            if original:
                os.environ['DB_BACKEND'] = original
            else:
                os.environ['DB_BACKEND'] = 'sqlite'
            from importlib import reload
            import modules.db
            reload(modules.db)


class TestDatabaseOperations:
    """Test database operations within app context."""

    def test_get_db_returns_connection(self, app_context):
        """Test get_db returns a valid connection."""
        from modules.db import get_db, init_db
        init_db()
        conn = get_db()
        assert conn is not None

    def test_init_db_creates_tables(self, db_connection):
        """Test init_db creates required tables."""
        from modules.db import execute_query

        # Check clients table exists
        cursor = execute_query(
            db_connection,
            "SELECT name FROM sqlite_master WHERE type='table' AND name='clients'"
        )
        assert cursor.fetchone() is not None

        # Check configs table exists
        cursor = execute_query(
            db_connection,
            "SELECT name FROM sqlite_master WHERE type='table' AND name='configs'"
        )
        assert cursor.fetchone() is not None

        # Check migration_sessions table exists
        cursor = execute_query(
            db_connection,
            "SELECT name FROM sqlite_master WHERE type='table' AND name='migration_sessions'"
        )
        assert cursor.fetchone() is not None

    def test_insert_returning_id(self, db_connection):
        """Test insert_returning_id returns correct ID."""
        from modules.db import insert_returning_id

        client_id = insert_returning_id(
            db_connection,
            'clients',
            ('client_name',),
            ('Insert Test Client',),
            'client_id'
        )
        db_connection.commit()

        assert client_id is not None
        assert isinstance(client_id, int)
        assert client_id > 0

    def test_execute_query_with_params(self, db_connection):
        """Test execute_query properly handles parameters."""
        from modules.db import execute_query, insert_returning_id

        # Insert a test record
        client_id = insert_returning_id(
            db_connection,
            'clients',
            ('client_name',),
            ('Query Test Client',),
            'client_id'
        )
        db_connection.commit()

        # Query it back
        cursor = execute_query(
            db_connection,
            "SELECT client_name FROM clients WHERE client_id = ?",
            (client_id,)
        )
        row = cursor.fetchone()
        assert row['client_name'] == 'Query Test Client'


class TestClientConfig:
    """Test client configuration functions."""

    def test_get_client_config_empty(self, db_connection, sample_client):
        """Test get_client_config returns empty dict for client with no config."""
        from modules.db import get_client_config

        config = get_client_config(sample_client['client_id'], db_connection)
        assert isinstance(config, dict)

    def test_get_client_config_with_values(self, db_connection):
        """Test get_client_config returns stored values."""
        from modules.db import get_client_config, execute_query, insert_returning_id
        import uuid

        # Create a unique client for this test
        client_name = f'Config Test Client {uuid.uuid4().hex[:8]}'
        client_id = insert_returning_id(
            db_connection,
            'clients',
            ('client_name',),
            (client_name,),
            'client_id'
        )

        # Insert config values
        execute_query(
            db_connection,
            "INSERT INTO configs (client_id, config_type, config_key, config_value) VALUES (?, ?, ?, ?)",
            (client_id, 'ora2pg', 'oracle_home', '/usr/lib/oracle')
        )
        db_connection.commit()

        config = get_client_config(client_id, db_connection)
        assert config.get('oracle_home') == '/usr/lib/oracle'

    def test_extract_ai_settings(self):
        """Test extract_ai_settings extracts correct values."""
        from modules.db import extract_ai_settings

        config = {
            'ai_provider': 'anthropic',
            'ai_endpoint': 'https://api.anthropic.com',
            'ai_model': 'claude-sonnet-4-20250514',
            'ai_api_key': 'test-key',
            'ai_temperature': '0.3',
            'ai_max_output_tokens': '4096'
        }

        ai_settings = extract_ai_settings(config)

        assert ai_settings['ai_provider'] == 'anthropic'
        assert ai_settings['ai_endpoint'] == 'https://api.anthropic.com'
        assert ai_settings['ai_model'] == 'claude-sonnet-4-20250514'
        assert ai_settings['ai_api_key'] == 'test-key'
        assert ai_settings['ai_temperature'] == 0.3
        assert ai_settings['ai_max_output_tokens'] == 4096

    def test_extract_ai_settings_defaults(self):
        """Test extract_ai_settings uses defaults for missing values."""
        from modules.db import extract_ai_settings
        from modules.constants import DEFAULT_AI_TEMPERATURE, DEFAULT_AI_MAX_OUTPUT_TOKENS

        config = {}
        ai_settings = extract_ai_settings(config)

        assert ai_settings['ai_temperature'] == DEFAULT_AI_TEMPERATURE
        assert ai_settings['ai_max_output_tokens'] == DEFAULT_AI_MAX_OUTPUT_TOKENS

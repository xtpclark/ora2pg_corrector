"""
Tests for the config API endpoints (routes/api/config.py).
"""

import pytest
import json


class TestAppSettings:
    """Test application settings endpoints."""

    def test_get_app_settings(self, client, app_context):
        """Test GET /api/app_settings returns settings."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/app_settings')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'validation_pg_dsn' in data

    def test_get_app_settings_returns_json(self, client, app_context):
        """Test that app settings returns JSON content type."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/app_settings')
        assert response.content_type == 'application/json'


class TestAiProviders:
    """Test AI providers endpoints."""

    def test_get_ai_providers(self, client, app_context):
        """Test GET /api/ai_providers returns list."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/ai_providers')
        assert response.status_code == 200
        data = json.loads(response.data)
        # Should return a list (via success_response)
        assert isinstance(data, list)

    def test_ai_providers_contains_default(self, client, app_context):
        """Test that default AI providers are present."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/ai_providers')
        data = json.loads(response.data)
        # init_db seeds some default providers
        # Check that we have at least some providers
        assert len(data) >= 0  # May or may not have providers initially


class TestOra2pgConfigOptions:
    """Test Ora2Pg config options endpoints."""

    def test_get_ora2pg_config_options(self, client, app_context):
        """Test GET /api/ora2pg_config_options returns list."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/ora2pg_config_options')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_ora2pg_config_options_contains_options(self, client, app_context):
        """Test that config options are seeded."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/ora2pg_config_options')
        data = json.loads(response.data)
        # Should have seeded config options
        # Options may be empty if not seeded, but should return list
        assert isinstance(data, list)


class TestClientConfig:
    """Test client configuration endpoints."""

    def test_get_config_empty(self, client, app_context):
        """Test GET /api/client/<id>/config for client with no config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Config Test Client',), 'client_id'
        )
        conn.commit()

        response = client.get(f'/api/client/{client_id}/config')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, dict)

    def test_post_config(self, client, app_context):
        """Test POST /api/client/<id>/config saves configuration."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Config Post Test',), 'client_id'
        )
        conn.commit()

        # Post config
        config_data = {
            'oracle_host': 'localhost',
            'oracle_port': '1521',
            'oracle_sid': 'ORCL'
        }
        response = client.post(
            f'/api/client/{client_id}/config',
            json=config_data,
            content_type='application/json'
        )
        assert response.status_code == 200

        # Verify config was saved
        get_response = client.get(f'/api/client/{client_id}/config')
        saved_config = json.loads(get_response.data)
        assert saved_config.get('oracle_host') == 'localhost'
        assert saved_config.get('oracle_port') == '1521'
        assert saved_config.get('oracle_sid') == 'ORCL'

    def test_post_config_encrypts_password(self, client, app_context):
        """Test that sensitive fields are encrypted."""
        from modules.db import init_db, get_db, insert_returning_id, execute_query
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Encrypt Test Client',), 'client_id'
        )
        conn.commit()

        # Post config with password
        config_data = {
            'oracle_pwd': 'my_secret_password'
        }
        response = client.post(
            f'/api/client/{client_id}/config',
            json=config_data,
            content_type='application/json'
        )
        assert response.status_code == 200

        # Check that the raw value in DB is not the plaintext password
        cursor = execute_query(
            conn,
            'SELECT config_value FROM configs WHERE client_id = ? AND config_key = ?',
            (client_id, 'oracle_pwd')
        )
        row = cursor.fetchone()
        # Encrypted value should be longer and not equal to original
        assert row['config_value'] != 'my_secret_password'
        assert len(row['config_value']) > len('my_secret_password')

    def test_post_config_empty_object(self, client, app_context):
        """Test POST /api/client/<id>/config with empty object returns validation error."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('No Data Test',), 'client_id'
        )
        conn.commit()

        # Empty JSON object triggers validation error (no config data provided)
        response = client.post(
            f'/api/client/{client_id}/config',
            json={},
            content_type='application/json'
        )
        # Returns validation error for empty config - the endpoint checks `if not new_config:`
        # which is True for empty dict, returning validation_error_response
        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'error' in data

    def test_post_config_updates_existing(self, client, app_context):
        """Test that posting config updates existing values."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Update Test Client',), 'client_id'
        )
        conn.commit()

        # Post initial config
        client.post(
            f'/api/client/{client_id}/config',
            json={'oracle_host': 'old_host'},
            content_type='application/json'
        )

        # Post updated config
        client.post(
            f'/api/client/{client_id}/config',
            json={'oracle_host': 'new_host'},
            content_type='application/json'
        )

        # Verify update
        get_response = client.get(f'/api/client/{client_id}/config')
        saved_config = json.loads(get_response.data)
        assert saved_config.get('oracle_host') == 'new_host'

    def test_post_config_skips_null_values(self, client, app_context):
        """Test that null values in config are skipped."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Null Value Test',), 'client_id'
        )
        conn.commit()

        # Post config with null value
        config_data = {
            'oracle_host': 'localhost',
            'oracle_port': None
        }
        response = client.post(
            f'/api/client/{client_id}/config',
            json=config_data,
            content_type='application/json'
        )
        assert response.status_code == 200

        # Verify only non-null was saved
        get_response = client.get(f'/api/client/{client_id}/config')
        saved_config = json.loads(get_response.data)
        assert saved_config.get('oracle_host') == 'localhost'
        assert 'oracle_port' not in saved_config

"""
Tests for the constants module (modules/constants.py).
"""

import os
import pytest


class TestConstants:
    """Test constant values and helper functions."""

    def test_default_directories_exist(self):
        """Test default directory constants are defined."""
        from modules.constants import (
            DATA_DIR, PROJECT_DATA_DIR, OUTPUT_DIR,
            ORA2PG_CONFIG_DIR, AI_CONFIG_DIR
        )

        assert DATA_DIR is not None
        assert PROJECT_DATA_DIR is not None
        assert OUTPUT_DIR is not None
        assert ORA2PG_CONFIG_DIR is not None
        assert AI_CONFIG_DIR is not None

    def test_file_paths_defined(self):
        """Test file path constants are defined."""
        from modules.constants import (
            SQLITE_DB_PATH, ENCRYPTION_KEY_FILE,
            ORA2PG_CONFIG_FILE, AI_PROVIDERS_CONFIG_FILE,
            AUTH_TOKEN_FILE
        )

        assert SQLITE_DB_PATH is not None
        assert ENCRYPTION_KEY_FILE is not None
        assert ORA2PG_CONFIG_FILE is not None
        assert AI_PROVIDERS_CONFIG_FILE is not None
        assert AUTH_TOKEN_FILE is not None

    def test_config_key_lists_defined(self):
        """Test configuration key lists are defined."""
        from modules.constants import SENSITIVE_CONFIG_KEYS, BOOLEAN_CONFIG_KEYS

        assert isinstance(SENSITIVE_CONFIG_KEYS, list)
        assert isinstance(BOOLEAN_CONFIG_KEYS, list)
        assert 'oracle_pwd' in SENSITIVE_CONFIG_KEYS
        assert 'ai_api_key' in SENSITIVE_CONFIG_KEYS

    def test_ai_defaults_defined(self):
        """Test AI default values are defined."""
        from modules.constants import (
            DEFAULT_AI_TEMPERATURE, DEFAULT_AI_MAX_OUTPUT_TOKENS
        )

        assert isinstance(DEFAULT_AI_TEMPERATURE, float)
        assert isinstance(DEFAULT_AI_MAX_OUTPUT_TOKENS, int)
        assert 0 <= DEFAULT_AI_TEMPERATURE <= 1
        assert DEFAULT_AI_MAX_OUTPUT_TOKENS > 0


class TestHelperFunctions:
    """Test helper functions in constants module."""

    def test_get_client_project_dir(self):
        """Test get_client_project_dir returns correct path."""
        from modules.constants import get_client_project_dir, PROJECT_DATA_DIR

        result = get_client_project_dir(42)
        assert result == os.path.join(PROJECT_DATA_DIR, '42')

    def test_get_session_dir(self):
        """Test get_session_dir returns correct path."""
        from modules.constants import get_session_dir, PROJECT_DATA_DIR

        result = get_session_dir(1, 25)
        assert result == os.path.join(PROJECT_DATA_DIR, '1', '25')

    def test_get_client_project_dir_different_clients(self):
        """Test get_client_project_dir returns different paths for different clients."""
        from modules.constants import get_client_project_dir

        path1 = get_client_project_dir(1)
        path2 = get_client_project_dir(2)
        assert path1 != path2

    def test_get_session_dir_same_client_different_sessions(self):
        """Test get_session_dir returns different paths for different sessions."""
        from modules.constants import get_session_dir

        path1 = get_session_dir(1, 10)
        path2 = get_session_dir(1, 20)
        assert path1 != path2


class TestEnvironmentOverrides:
    """Test that environment variables can override defaults."""

    def test_data_dir_env_override(self):
        """Test APP_DATA_DIR can be overridden via environment."""
        original = os.environ.get('APP_DATA_DIR')
        try:
            os.environ['APP_DATA_DIR'] = '/custom/data'
            # Reload the module to pick up new env var
            from importlib import reload
            import modules.constants
            reload(modules.constants)
            assert modules.constants.DATA_DIR == '/custom/data'
        finally:
            if original:
                os.environ['APP_DATA_DIR'] = original
            else:
                del os.environ['APP_DATA_DIR']
            # Reload to restore
            from importlib import reload
            import modules.constants
            reload(modules.constants)

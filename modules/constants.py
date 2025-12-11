"""
Centralized constants and paths for the ora2pg_corrector application.

All hardcoded paths and configuration values should be defined here.
"""

import os

# =============================================================================
# Base Directories
# =============================================================================

# Application data directory (persistent storage in container)
DATA_DIR = os.environ.get('APP_DATA_DIR', '/app/data')

# Project data directory (client migration data)
PROJECT_DATA_DIR = os.environ.get('PROJECT_DATA_DIR', '/app/project_data')

# Output directory for corrected SQL files
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '/app/output')

# Configuration directories
ORA2PG_CONFIG_DIR = os.environ.get('ORA2PG_CONFIG_DIR', '/app/ora2pg_config')
AI_CONFIG_DIR = os.environ.get('AI_CONFIG_DIR', '/app/ai_config')


# =============================================================================
# Database Configuration
# =============================================================================

# SQLite database path
SQLITE_DB_PATH = os.path.join(DATA_DIR, 'settings.db')

# Encryption key file path
ENCRYPTION_KEY_FILE = os.path.join(DATA_DIR, '.encryption_key')


# =============================================================================
# Configuration Files
# =============================================================================

# Ora2Pg default configuration file
ORA2PG_CONFIG_FILE = os.path.join(ORA2PG_CONFIG_DIR, 'default.cfg')

# AI providers configuration file
AI_PROVIDERS_CONFIG_FILE = os.path.join(AI_CONFIG_DIR, 'ai_providers.json')


# =============================================================================
# Authentication Paths
# =============================================================================

# Auth token file path
AUTH_TOKEN_FILE = os.path.join(DATA_DIR, '.auth_token')

# Database initialization lock file
DB_INIT_LOCK_FILE = os.path.join(DATA_DIR, '.db_init.lock')

# Database initialization marker file
DB_INIT_MARKER_FILE = os.path.join(DATA_DIR, '.db_initialized')


# =============================================================================
# Export Types
# =============================================================================

# Supported ora2pg export types
EXPORT_TYPES = [
    'TABLE',
    'VIEW',
    'MATERIALIZED_VIEW',
    'SEQUENCE',
    'FUNCTION',
    'PROCEDURE',
    'PACKAGE',
    'TRIGGER',
    'TYPE',
    'INDEX',
    'GRANT',
    'SYNONYM',
    'TABLESPACE',
    'PARTITION',
    'FDW',
    'MVIEW',
    'QUERY',
]

# Types that can be validated
VALIDATABLE_TYPES = ['TABLE', 'VIEW', 'SEQUENCE', 'FUNCTION', 'PROCEDURE', 'TYPE', 'INDEX']


# =============================================================================
# Rollback Type Order
# =============================================================================

# Order for dropping objects (reverse of creation order for safety)
ROLLBACK_TYPE_ORDER = [
    'TRIGGER',
    'PROCEDURE',
    'FUNCTION',
    'MATERIALIZED VIEW',
    'VIEW',
    'INDEX',
    'TABLE',
    'SEQUENCE',
    'TYPE',
]


# =============================================================================
# File Status Values
# =============================================================================

FILE_STATUS_GENERATED = 'generated'
FILE_STATUS_CORRECTED = 'corrected'
FILE_STATUS_VALIDATED = 'validated'
FILE_STATUS_FAILED = 'failed'

FILE_STATUSES = [
    FILE_STATUS_GENERATED,
    FILE_STATUS_CORRECTED,
    FILE_STATUS_VALIDATED,
    FILE_STATUS_FAILED,
]


# =============================================================================
# Workflow Status Values
# =============================================================================

WORKFLOW_STATUS_PENDING = 'pending'
WORKFLOW_STATUS_RUNNING = 'running'
WORKFLOW_STATUS_COMPLETED = 'completed'
WORKFLOW_STATUS_PARTIAL = 'partial'
WORKFLOW_STATUS_FAILED = 'failed'

WORKFLOW_STATUSES = [
    WORKFLOW_STATUS_PENDING,
    WORKFLOW_STATUS_RUNNING,
    WORKFLOW_STATUS_COMPLETED,
    WORKFLOW_STATUS_PARTIAL,
    WORKFLOW_STATUS_FAILED,
]


# =============================================================================
# AI Configuration Defaults
# =============================================================================

DEFAULT_AI_TEMPERATURE = 0.2
DEFAULT_AI_MAX_OUTPUT_TOKENS = 8192


# =============================================================================
# Sensitive Configuration Keys
# =============================================================================

# Keys that should be encrypted in the database
SENSITIVE_CONFIG_KEYS = ['oracle_pwd', 'ai_api_key']

# Keys that represent boolean values
BOOLEAN_CONFIG_KEYS = ['dump_as_html', 'export_schema', 'create_schema',
                       'compile_schema', 'debug', 'file_per_table']


# =============================================================================
# Helper Functions
# =============================================================================

def get_client_project_dir(client_id: int) -> str:
    """Get the project data directory for a specific client."""
    return os.path.join(PROJECT_DATA_DIR, str(client_id))


def get_session_dir(client_id: int, session_id: int) -> str:
    """Get the directory for a specific migration session."""
    return os.path.join(PROJECT_DATA_DIR, str(client_id), str(session_id))


def ensure_data_dir():
    """Ensure the data directory exists."""
    os.makedirs(DATA_DIR, exist_ok=True)


def ensure_project_dir(client_id: int):
    """Ensure the project directory for a client exists."""
    os.makedirs(get_client_project_dir(client_id), exist_ok=True)


def ensure_session_dir(client_id: int, session_id: int):
    """Ensure the session directory exists."""
    os.makedirs(get_session_dir(client_id, session_id), exist_ok=True)

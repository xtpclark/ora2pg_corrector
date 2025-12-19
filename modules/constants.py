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
# AI Pricing (per 1M tokens, in USD)
# =============================================================================

# Pricing as of December 2024 - prices in USD per 1M tokens
AI_MODEL_PRICING = {
    # Anthropic models
    'claude-3-5-sonnet-20241022': {'input': 3.00, 'output': 15.00},
    'claude-3-5-sonnet-latest': {'input': 3.00, 'output': 15.00},
    'claude-3-sonnet-20240229': {'input': 3.00, 'output': 15.00},
    'claude-3-opus-20240229': {'input': 15.00, 'output': 75.00},
    'claude-3-haiku-20240307': {'input': 0.25, 'output': 1.25},
    'claude-3-5-haiku-20241022': {'input': 1.00, 'output': 5.00},
    # OpenAI models
    'gpt-4o': {'input': 2.50, 'output': 10.00},
    'gpt-4o-2024-11-20': {'input': 2.50, 'output': 10.00},
    'gpt-4o-mini': {'input': 0.15, 'output': 0.60},
    'gpt-4-turbo': {'input': 10.00, 'output': 30.00},
    'gpt-4': {'input': 30.00, 'output': 60.00},
    'gpt-3.5-turbo': {'input': 0.50, 'output': 1.50},
    # Google models
    'gemini-1.5-pro': {'input': 1.25, 'output': 5.00},
    'gemini-1.5-flash': {'input': 0.075, 'output': 0.30},
    'gemini-2.0-flash-exp': {'input': 0.10, 'output': 0.40},
    # Default fallback for unknown models
    '_default': {'input': 5.00, 'output': 15.00},
}


def calculate_ai_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """
    Calculate the estimated cost for AI API usage.

    :param model: AI model name
    :param input_tokens: Number of input tokens
    :param output_tokens: Number of output tokens
    :return: Estimated cost in USD
    """
    pricing = AI_MODEL_PRICING.get(model, AI_MODEL_PRICING['_default'])
    input_cost = (input_tokens / 1_000_000) * pricing['input']
    output_cost = (output_tokens / 1_000_000) * pricing['output']
    return round(input_cost + output_cost, 6)


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


def mask_sensitive_config(config: dict) -> dict:
    """
    Create a copy of the config with sensitive values masked.

    :param config: Configuration dictionary
    :return: New dictionary with sensitive values masked
    """
    masked = config.copy()
    for key in SENSITIVE_CONFIG_KEYS:
        if key in masked and masked[key]:
            # Show first 4 and last 4 chars for identification, mask the rest
            value = str(masked[key])
            if len(value) > 12:
                masked[key] = f"{value[:4]}...{value[-4:]}"
            else:
                masked[key] = "****"
    return masked

"""
Pytest configuration and fixtures for Ora2Pg Corrector tests.
"""

import os
import sys
import tempfile
import shutil

# MUST set environment variables BEFORE any imports that load constants
# Create temp directories first
_test_data_dir = tempfile.mkdtemp(prefix='ora2pg_test_data_')
_test_project_dir = tempfile.mkdtemp(prefix='ora2pg_test_project_')
_test_output_dir = tempfile.mkdtemp(prefix='ora2pg_test_output_')

# Set environment before importing anything else
os.environ['APP_DATA_DIR'] = _test_data_dir
os.environ['PROJECT_DATA_DIR'] = _test_project_dir
os.environ['OUTPUT_DIR'] = _test_output_dir
os.environ['ORA2PG_CONFIG_DIR'] = _test_data_dir
os.environ['AI_CONFIG_DIR'] = _test_data_dir
os.environ['DB_BACKEND'] = 'sqlite'
os.environ['AUTH_MODE'] = 'none'
os.environ['FLASK_ENV'] = 'testing'
os.environ['APP_SECRET_KEY'] = 'test-secret-key-for-pytest'

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now we can import pytest and other modules
import pytest


def pytest_configure(config):
    """Called after command line options have been parsed."""
    pass


@pytest.fixture(scope='session')
def app():
    """Create and configure a test Flask application."""
    # Reimport constants to pick up our test directories
    from importlib import reload
    import modules.constants
    reload(modules.constants)

    from app import create_app

    app = create_app()
    app.config['TESTING'] = True

    yield app

    # Cleanup temp directories at end of session
    shutil.rmtree(_test_data_dir, ignore_errors=True)
    shutil.rmtree(_test_project_dir, ignore_errors=True)
    shutil.rmtree(_test_output_dir, ignore_errors=True)


@pytest.fixture
def client(app):
    """A test client for the app."""
    return app.test_client()


@pytest.fixture
def app_context(app):
    """An application context for tests that need it."""
    with app.app_context():
        yield


@pytest.fixture
def db_connection(app_context):
    """Get a database connection within app context."""
    from modules.db import get_db, init_db
    init_db()
    return get_db()


@pytest.fixture
def sample_client(db_connection, request):
    """Create a sample client for testing with unique name per test."""
    from modules.db import execute_query, insert_returning_id
    import uuid

    # Use test name + UUID for unique client name
    test_name = request.node.name if hasattr(request, 'node') else 'test'
    client_name = f'Test Client {test_name} {uuid.uuid4().hex[:8]}'

    client_id = insert_returning_id(
        db_connection,
        'clients',
        ('client_name',),
        (client_name,),
        'client_id'
    )
    db_connection.commit()

    return {'client_id': client_id, 'client_name': client_name}


@pytest.fixture
def sample_session(db_connection, sample_client):
    """Create a sample migration session for testing."""
    from modules.db import insert_returning_id

    session_id = insert_returning_id(
        db_connection,
        'migration_sessions',
        ('client_id', 'session_name', 'export_directory', 'export_type'),
        (sample_client['client_id'], 'Test Session', '/tmp/test', 'TABLE'),
        'session_id'
    )
    db_connection.commit()

    return {
        'session_id': session_id,
        'client_id': sample_client['client_id'],
        'session_name': 'Test Session'
    }

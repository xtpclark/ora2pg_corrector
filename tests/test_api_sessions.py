"""
Tests for the sessions API endpoints (routes/api/sessions.py).
"""

import pytest
import json


class TestSessionsAPI:
    """Test migration session API endpoints."""

    def test_get_sessions_empty(self, client, app_context):
        """Test GET /api/client/<id>/sessions returns empty list for new client."""
        from modules.db import init_db
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Sessions Test Client'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Get sessions
        response = client.get(f'/api/client/{client_id}/sessions')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_sessions_with_data(self, client, app_context):
        """Test GET /api/client/<id>/sessions returns sessions with files."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Sessions With Data Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Create a session directly in the database
        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Test Session', '/tmp/test', 'TABLE'),
            'session_id'
        )
        conn.commit()

        # Get sessions
        response = client.get(f'/api/client/{client_id}/sessions')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1


class TestFilesAPI:
    """Test migration file API endpoints."""

    def test_get_session_files_empty(self, client, app_context):
        """Test GET /api/session/<id>/files returns empty list for empty session."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Files Empty Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Create a session
        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Empty Session', '/tmp/test', 'TABLE'),
            'session_id'
        )
        conn.commit()

        # Get files
        response = client.get(f'/api/session/{session_id}/files')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_get_session_files_with_data(self, client, app_context):
        """Test GET /api/session/<id>/files returns files when they exist."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Files With Data Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Create a session
        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Files Session', '/tmp/test', 'TABLE'),
            'session_id'
        )

        # Create a file
        insert_returning_id(
            conn,
            'migration_files',
            ('session_id', 'filename', 'status'),
            (session_id, 'test_table.sql', 'generated'),
            'file_id'
        )
        conn.commit()

        # Get files
        response = client.get(f'/api/session/{session_id}/files')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1
        assert data[0]['filename'] == 'test_table.sql'


class TestFileStatusAPI:
    """Test file status API endpoints."""

    def test_update_file_status(self, client, app_context):
        """Test POST /api/file/<id>/status updates file status."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client and session
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'File Status Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Status Session', '/tmp/test', 'TABLE'),
            'session_id'
        )

        file_id = insert_returning_id(
            conn,
            'migration_files',
            ('session_id', 'filename', 'status'),
            (session_id, 'status_test.sql', 'generated'),
            'file_id'
        )
        conn.commit()

        # Update status - NOTE: endpoint uses POST not PATCH
        response = client.post(
            f'/api/file/{file_id}/status',
            json={'status': 'corrected'},
            content_type='application/json'
        )
        assert response.status_code == 200

    def test_update_file_status_invalid(self, client, app_context):
        """Test POST /api/file/<id>/status with invalid status returns 400."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client and session
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'File Invalid Status Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Invalid Status Session', '/tmp/test', 'TABLE'),
            'session_id'
        )

        file_id = insert_returning_id(
            conn,
            'migration_files',
            ('session_id', 'filename', 'status'),
            (session_id, 'invalid_status_test.sql', 'generated'),
            'file_id'
        )
        conn.commit()

        # Try to update with invalid status
        response = client.post(
            f'/api/file/{file_id}/status',
            json={'status': 'invalid_status'},
            content_type='application/json'
        )
        assert response.status_code == 400

    def test_update_file_status_not_found(self, client, app_context):
        """Test POST /api/file/<id>/status for non-existent file returns 404."""
        from modules.db import init_db
        init_db()

        response = client.post(
            '/api/file/99999/status',
            json={'status': 'corrected'},
            content_type='application/json'
        )
        assert response.status_code == 404


class TestGetExportedFileAPI:
    """Test exported file content API endpoints."""

    def test_get_exported_file_missing_id(self, client, app_context):
        """Test POST /api/get_exported_file without file_id returns 400."""
        from modules.db import init_db
        init_db()

        response = client.post(
            '/api/get_exported_file',
            json={},
            content_type='application/json'
        )
        assert response.status_code == 400

    def test_get_exported_file_not_found(self, client, app_context):
        """Test POST /api/get_exported_file for non-existent file returns 404."""
        from modules.db import init_db
        init_db()

        response = client.post(
            '/api/get_exported_file',
            json={'file_id': 99999},
            content_type='application/json'
        )
        assert response.status_code == 404

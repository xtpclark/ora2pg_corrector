"""
Tests for the clients API endpoints (routes/api/clients.py).
"""

import pytest
import json


class TestClientsAPI:
    """Test client management API endpoints."""

    def test_get_clients_empty(self, client, app_context):
        """Test GET /api/clients returns empty list when no clients exist."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/clients')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_create_client(self, client, app_context):
        """Test POST /api/clients creates a new client."""
        from modules.db import init_db
        init_db()

        response = client.post(
            '/api/clients',
            json={'client_name': 'API Test Client'},
            content_type='application/json'
        )
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data['client_name'] == 'API Test Client'
        assert 'client_id' in data

    def test_create_client_missing_name(self, client, app_context):
        """Test POST /api/clients without name returns 400."""
        from modules.db import init_db
        init_db()

        response = client.post(
            '/api/clients',
            json={},
            content_type='application/json'
        )
        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'error' in data

    def test_create_duplicate_client(self, client, app_context):
        """Test POST /api/clients with duplicate name returns 409."""
        from modules.db import init_db
        init_db()

        # Create first client
        client.post(
            '/api/clients',
            json={'client_name': 'Duplicate Test'},
            content_type='application/json'
        )

        # Try to create duplicate
        response = client.post(
            '/api/clients',
            json={'client_name': 'Duplicate Test'},
            content_type='application/json'
        )
        assert response.status_code == 409

    def test_get_clients_after_create(self, client, app_context):
        """Test GET /api/clients returns created clients."""
        from modules.db import init_db
        init_db()

        # Create a client
        client.post(
            '/api/clients',
            json={'client_name': 'List Test Client'},
            content_type='application/json'
        )

        # Get clients
        response = client.get('/api/clients')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1
        client_names = [c['client_name'] for c in data]
        assert 'List Test Client' in client_names

    def test_rename_client(self, client, app_context):
        """Test PUT /api/client/<id> renames client."""
        from modules.db import init_db
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Rename Test Original'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Rename it
        response = client.put(
            f'/api/client/{client_id}',
            json={'client_name': 'Rename Test Updated'},
            content_type='application/json'
        )
        assert response.status_code == 200

    def test_delete_client(self, client, app_context):
        """Test DELETE /api/client/<id> deletes client."""
        from modules.db import init_db
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Delete Test Client'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Delete it
        response = client.delete(f'/api/client/{client_id}')
        assert response.status_code == 200


class TestAuditLogsAPI:
    """Test audit log API endpoints."""

    def test_get_audit_logs_empty(self, client, app_context):
        """Test GET /api/client/<id>/audit_logs returns empty list for new client."""
        from modules.db import init_db
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Audit Log Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Get audit logs (there should be at least the create_client log)
        response = client.get(f'/api/client/{client_id}/audit_logs')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_log_audit_event(self, client, app_context):
        """Test POST /api/client/<id>/log_audit creates audit entry."""
        from modules.db import init_db
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Audit Event Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Log an audit event
        response = client.post(
            f'/api/client/{client_id}/log_audit',
            json={'action': 'test_action', 'details': 'Test audit event'},
            content_type='application/json'
        )
        assert response.status_code == 200

    def test_log_audit_missing_action(self, client, app_context):
        """Test POST /api/client/<id>/log_audit without action returns 400."""
        from modules.db import init_db
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Audit Missing Action Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Try to log without action
        response = client.post(
            f'/api/client/{client_id}/log_audit',
            json={'details': 'No action provided'},
            content_type='application/json'
        )
        assert response.status_code == 400

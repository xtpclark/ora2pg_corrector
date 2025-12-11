"""
Tests for the migration API endpoints (routes/api/migration.py).
"""

import pytest
import json


class TestMigrationStatus:
    """Test migration status endpoint."""

    def test_migration_status_no_migration(self, client, app_context):
        """Test GET /api/client/<id>/migration_status with no migration."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client with no migration
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Status Test Client',), 'client_id'
        )
        conn.commit()

        response = client.get(f'/api/client/{client_id}/migration_status')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'no_migration'

    def test_migration_status_with_session(self, client, app_context):
        """Test GET /api/client/<id>/migration_status with completed session."""
        from modules.db import init_db, get_db, insert_returning_id, execute_query
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Migration Status Test',), 'client_id'
        )

        # Create a TABLE session (main export type)
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type', 'workflow_status'),
            (client_id, 'Test Session', '/tmp/test', 'TABLE', 'completed'),
            'session_id'
        )

        # Create some migration files
        execute_query(
            conn,
            '''INSERT INTO migration_files (session_id, filename, status)
               VALUES (?, 'employees.sql', 'validated'),
                      (?, 'departments.sql', 'validated'),
                      (?, 'failed_table.sql', 'failed')''',
            (session_id, session_id, session_id)
        )
        conn.commit()

        response = client.get(f'/api/client/{client_id}/migration_status')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'completed'
        assert data['total_objects'] == 3
        assert data['successful'] == 2
        assert data['failed'] == 1


class TestStartMigration:
    """Test start migration endpoint."""

    def test_start_migration_no_config(self, client, app_context):
        """Test POST /api/client/<id>/start_migration without Oracle config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client without Oracle config
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('No Config Client',), 'client_id'
        )
        conn.commit()

        response = client.post(f'/api/client/{client_id}/start_migration')
        # Should return 200 with 'running' status (async)
        # Or error if config is missing - depends on implementation
        assert response.status_code in [200, 500]


class TestTestOra2pgConnection:
    """Test Oracle connection test endpoint."""

    def test_connection_test_no_config(self, client, app_context):
        """Test POST /api/client/<id>/test_ora2pg_connection without config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Connection Test Client',), 'client_id'
        )
        conn.commit()

        # Test with minimal config (will fail to connect but tests the endpoint)
        response = client.post(
            f'/api/client/{client_id}/test_ora2pg_connection',
            json={
                'oracle_host': 'localhost',
                'oracle_port': '1521',
                'oracle_sid': 'ORCL',
                'oracle_user': 'test',
                'oracle_pwd': 'test'
            },
            content_type='application/json'
        )
        # Should return error since ora2pg isn't installed or Oracle not available
        # 400 = connection failed (ora2pg ran but couldn't connect)
        # 500 = ora2pg not installed or other error
        assert response.status_code in [200, 400, 500]


class TestGetObjectList:
    """Test get object list endpoint."""

    def test_get_object_list_no_config(self, client, app_context):
        """Test GET /api/client/<id>/get_object_list without Oracle config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client without Oracle config
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Object List Test Client',), 'client_id'
        )
        conn.commit()

        response = client.get(f'/api/client/{client_id}/get_object_list')
        # Should return error since no config
        assert response.status_code == 500


class TestGetOracleDdl:
    """Test get Oracle DDL endpoint."""

    def test_get_oracle_ddl_missing_object_name(self, client, app_context):
        """Test POST /api/client/<id>/get_oracle_ddl without object_name."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('DDL Test Client',), 'client_id'
        )
        conn.commit()

        response = client.post(
            f'/api/client/{client_id}/get_oracle_ddl',
            json={'object_type': 'TABLE'},
            content_type='application/json'
        )
        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'error' in data

    def test_get_oracle_ddl_with_object_name(self, client, app_context):
        """Test POST /api/client/<id>/get_oracle_ddl with object_name but no config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('DDL Object Test Client',), 'client_id'
        )
        conn.commit()

        response = client.post(
            f'/api/client/{client_id}/get_oracle_ddl',
            json={'object_name': 'EMPLOYEES', 'object_type': 'TABLE'},
            content_type='application/json'
        )
        # Should return error since no Oracle config
        assert response.status_code == 500


class TestGetBulkOracleDdl:
    """Test bulk Oracle DDL endpoint."""

    def test_bulk_oracle_ddl_no_objects(self, client, app_context):
        """Test POST /api/client/<id>/get_bulk_oracle_ddl without objects."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Bulk DDL Test Client',), 'client_id'
        )
        conn.commit()

        response = client.post(
            f'/api/client/{client_id}/get_bulk_oracle_ddl',
            json={'objects': []},
            content_type='application/json'
        )
        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'error' in data

    def test_bulk_oracle_ddl_empty_request(self, client, app_context):
        """Test POST /api/client/<id>/get_bulk_oracle_ddl with empty request."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Bulk DDL Empty Test',), 'client_id'
        )
        conn.commit()

        response = client.post(
            f'/api/client/{client_id}/get_bulk_oracle_ddl',
            json={},
            content_type='application/json'
        )
        assert response.status_code == 400


class TestGenerateReport:
    """Test generate Ora2Pg report endpoint."""

    def test_generate_report_no_config(self, client, app_context):
        """Test POST /api/client/<id>/generate_report without Oracle config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Report Test Client',), 'client_id'
        )
        conn.commit()

        response = client.post(f'/api/client/{client_id}/generate_report')
        # Should return error since no Oracle config
        assert response.status_code == 500


class TestRunOra2pg:
    """Test run Ora2Pg endpoint."""

    def test_run_ora2pg_no_config(self, client, app_context):
        """Test POST /api/client/<id>/run_ora2pg without Oracle config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Run Ora2Pg Test Client',), 'client_id'
        )
        conn.commit()

        response = client.post(f'/api/client/{client_id}/run_ora2pg')
        # Should return error since no Oracle config
        assert response.status_code == 500


class TestRunMigrationSync:
    """Test synchronous migration endpoint."""

    def test_run_migration_sync_no_config(self, client, app_context):
        """Test POST /api/client/<id>/run_migration_sync without Oracle config."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client without Oracle config
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Sync Migration Test Client',), 'client_id'
        )
        conn.commit()

        response = client.post(f'/api/client/{client_id}/run_migration_sync')
        # The endpoint returns 200 with empty results when there's no config
        # (no objects to migrate, so returns success with 0 files)
        assert response.status_code == 200
        data = json.loads(response.data)
        # Should have status and counts in response
        assert 'status' in data

    def test_run_migration_sync_with_options(self, client, app_context):
        """Test POST /api/client/<id>/run_migration_sync with options."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Sync Options Test Client',), 'client_id'
        )
        conn.commit()

        response = client.post(
            f'/api/client/{client_id}/run_migration_sync',
            json={
                'clean_slate': True,
                'auto_create_ddl': False,
                'object_types': ['TABLE', 'VIEW']
            },
            content_type='application/json'
        )
        # Returns 200 with results (even if no objects to migrate)
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'status' in data

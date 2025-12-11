"""
Tests for the DDL cache API endpoints (routes/api/ddl_cache.py).
"""

import pytest
import json
import os
import tempfile


class TestDdlCacheStats:
    """Test DDL cache stats endpoint."""

    def test_get_cache_stats_empty(self, client, app_context):
        """Test GET /api/client/<id>/ddl_cache/stats with no cache entries."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Cache Stats Test Client',), 'client_id'
        )
        conn.commit()

        response = client.get(f'/api/client/{client_id}/ddl_cache/stats')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['total_entries'] == 0
        assert data['total_hits'] == 0
        assert data['entries'] == []

    def test_get_cache_stats_with_entries(self, client, app_context):
        """Test GET /api/client/<id>/ddl_cache/stats with cache entries."""
        from modules.db import init_db, get_db, insert_returning_id, execute_query
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Cache Entries Test',), 'client_id'
        )

        # Add cache entries
        execute_query(
            conn,
            '''INSERT INTO ddl_cache (client_id, object_name, object_type, generated_ddl, hit_count)
               VALUES (?, 'employees', 'TABLE', 'CREATE TABLE employees...', 5),
                      (?, 'departments', 'TABLE', 'CREATE TABLE departments...', 3)''',
            (client_id, client_id)
        )
        conn.commit()

        response = client.get(f'/api/client/{client_id}/ddl_cache/stats')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['total_entries'] == 2
        assert data['total_hits'] == 8  # 5 + 3
        assert len(data['entries']) == 2

        # Should be sorted by hit_count DESC
        assert data['entries'][0]['object_name'] == 'employees'
        assert data['entries'][0]['hit_count'] == 5


class TestClearDdlCache:
    """Test clear DDL cache endpoint."""

    def test_clear_cache_empty(self, client, app_context):
        """Test DELETE /api/client/<id>/ddl_cache with no entries."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Clear Empty Cache Test',), 'client_id'
        )
        conn.commit()

        response = client.delete(f'/api/client/{client_id}/ddl_cache')
        assert response.status_code == 200

    def test_clear_cache_with_entries(self, client, app_context):
        """Test DELETE /api/client/<id>/ddl_cache removes all entries."""
        from modules.db import init_db, get_db, insert_returning_id, execute_query
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('Clear Cache Test',), 'client_id'
        )

        # Add cache entries
        execute_query(
            conn,
            '''INSERT INTO ddl_cache (client_id, object_name, object_type, generated_ddl)
               VALUES (?, 'table1', 'TABLE', 'CREATE TABLE...'),
                      (?, 'table2', 'TABLE', 'CREATE TABLE...')''',
            (client_id, client_id)
        )
        conn.commit()

        # Verify entries exist
        cursor = execute_query(conn, 'SELECT COUNT(*) as cnt FROM ddl_cache WHERE client_id = ?', (client_id,))
        assert cursor.fetchone()['cnt'] == 2

        # Clear cache
        response = client.delete(f'/api/client/{client_id}/ddl_cache')
        assert response.status_code == 200

        # Verify entries are gone
        cursor = execute_query(conn, 'SELECT COUNT(*) as cnt FROM ddl_cache WHERE client_id = ?', (client_id,))
        assert cursor.fetchone()['cnt'] == 0

    def test_clear_cache_only_affects_target_client(self, client, app_context):
        """Test that clearing cache only affects the specified client."""
        from modules.db import init_db, get_db, insert_returning_id, execute_query
        init_db()

        # Create two clients
        conn = get_db()
        client_id1 = insert_returning_id(
            conn, 'clients', ('client_name',), ('Cache Client 1',), 'client_id'
        )
        client_id2 = insert_returning_id(
            conn, 'clients', ('client_name',), ('Cache Client 2',), 'client_id'
        )

        # Add cache entries for both
        execute_query(
            conn,
            '''INSERT INTO ddl_cache (client_id, object_name, object_type, generated_ddl)
               VALUES (?, 'table_a', 'TABLE', 'CREATE TABLE...'),
                      (?, 'table_b', 'TABLE', 'CREATE TABLE...')''',
            (client_id1, client_id2)
        )
        conn.commit()

        # Clear only client 1's cache
        response = client.delete(f'/api/client/{client_id1}/ddl_cache')
        assert response.status_code == 200

        # Verify client 1's cache is empty
        cursor = execute_query(conn, 'SELECT COUNT(*) as cnt FROM ddl_cache WHERE client_id = ?', (client_id1,))
        assert cursor.fetchone()['cnt'] == 0

        # Verify client 2's cache is intact
        cursor = execute_query(conn, 'SELECT COUNT(*) as cnt FROM ddl_cache WHERE client_id = ?', (client_id2,))
        assert cursor.fetchone()['cnt'] == 1


class TestGeneratedDdlList:
    """Test generated DDL list endpoint."""

    def test_get_generated_ddl_list_no_session(self, client, app_context):
        """Test GET /api/session/<id>/generated_ddl with non-existent session."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/session/99999/generated_ddl')
        assert response.status_code == 404

    def test_get_generated_ddl_list_no_ddl_folder(self, client, app_context):
        """Test GET /api/session/<id>/generated_ddl when no DDL folder exists."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        conn = get_db()
        client_id = insert_returning_id(
            conn, 'clients', ('client_name',), ('DDL List Test Client',), 'client_id'
        )

        # Create session with non-existent directory
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Test Session', '/tmp/nonexistent', 'TABLE'),
            'session_id'
        )
        conn.commit()

        response = client.get(f'/api/session/{session_id}/generated_ddl')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['objects'] == []
        assert 'No AI-generated DDL files found' in data.get('message', '')

    def test_get_generated_ddl_list_with_manifest(self, client, app_context):
        """Test GET /api/session/<id>/generated_ddl when manifest exists."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create temp directory with manifest
        with tempfile.TemporaryDirectory() as tmpdir:
            ddl_dir = os.path.join(tmpdir, 'ai_generated_ddl')
            os.makedirs(ddl_dir)

            # Create manifest
            manifest = {
                'generated_at': '2025-12-10T12:00:00',
                'objects': [
                    {'name': 'employees', 'type': 'TABLE', 'file': 'employees.sql'}
                ]
            }
            with open(os.path.join(ddl_dir, '_manifest.json'), 'w') as f:
                json.dump(manifest, f)

            # Create a client and session
            conn = get_db()
            client_id = insert_returning_id(
                conn, 'clients', ('client_name',), ('Manifest Test Client',), 'client_id'
            )
            session_id = insert_returning_id(
                conn,
                'migration_sessions',
                ('client_id', 'session_name', 'export_directory', 'export_type'),
                (client_id, 'Test Session', tmpdir, 'TABLE'),
                'session_id'
            )
            conn.commit()

            response = client.get(f'/api/session/{session_id}/generated_ddl')
            assert response.status_code == 200
            data = json.loads(response.data)
            assert 'objects' in data
            assert len(data['objects']) == 1
            assert data['objects'][0]['name'] == 'employees'


class TestGeneratedDdlContent:
    """Test generated DDL content endpoint."""

    def test_get_ddl_content_no_session(self, client, app_context):
        """Test GET /api/session/<id>/generated_ddl/<name> with non-existent session."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/session/99999/generated_ddl/employees')
        assert response.status_code == 404

    def test_get_ddl_content_file_not_found(self, client, app_context):
        """Test GET /api/session/<id>/generated_ddl/<name> when file doesn't exist."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
            ddl_dir = os.path.join(tmpdir, 'ai_generated_ddl')
            os.makedirs(ddl_dir)

            # Create a client and session
            conn = get_db()
            client_id = insert_returning_id(
                conn, 'clients', ('client_name',), ('Content Test Client',), 'client_id'
            )
            session_id = insert_returning_id(
                conn,
                'migration_sessions',
                ('client_id', 'session_name', 'export_directory', 'export_type'),
                (client_id, 'Test Session', tmpdir, 'TABLE'),
                'session_id'
            )
            conn.commit()

            response = client.get(f'/api/session/{session_id}/generated_ddl/nonexistent')
            assert response.status_code == 404

    def test_get_ddl_content_success(self, client, app_context):
        """Test GET /api/session/<id>/generated_ddl/<name> returns DDL content."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create temp directory with DDL file
        with tempfile.TemporaryDirectory() as tmpdir:
            ddl_dir = os.path.join(tmpdir, 'ai_generated_ddl')
            os.makedirs(ddl_dir)

            ddl_content = 'CREATE TABLE employees (id INTEGER PRIMARY KEY);'
            with open(os.path.join(ddl_dir, 'employees.sql'), 'w') as f:
                f.write(ddl_content)

            # Create a client and session
            conn = get_db()
            client_id = insert_returning_id(
                conn, 'clients', ('client_name',), ('DDL Content Test Client',), 'client_id'
            )
            session_id = insert_returning_id(
                conn,
                'migration_sessions',
                ('client_id', 'session_name', 'export_directory', 'export_type'),
                (client_id, 'Test Session', tmpdir, 'TABLE'),
                'session_id'
            )
            conn.commit()

            response = client.get(f'/api/session/{session_id}/generated_ddl/employees')
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['content'] == ddl_content
            assert data['object_name'] == 'employees'
            assert data['filename'] == 'employees.sql'

    def test_get_ddl_content_sanitizes_name(self, client, app_context):
        """Test that object names are sanitized in file lookup."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create temp directory with DDL file
        with tempfile.TemporaryDirectory() as tmpdir:
            ddl_dir = os.path.join(tmpdir, 'ai_generated_ddl')
            os.makedirs(ddl_dir)

            # File is stored with lowercase and sanitized name
            ddl_content = 'CREATE TABLE test_table (id INTEGER);'
            with open(os.path.join(ddl_dir, 'test_table.sql'), 'w') as f:
                f.write(ddl_content)

            # Create a client and session
            conn = get_db()
            client_id = insert_returning_id(
                conn, 'clients', ('client_name',), ('Sanitize Test Client',), 'client_id'
            )
            session_id = insert_returning_id(
                conn,
                'migration_sessions',
                ('client_id', 'session_name', 'export_directory', 'export_type'),
                (client_id, 'Test Session', tmpdir, 'TABLE'),
                'session_id'
            )
            conn.commit()

            # Request with uppercase name should still find the file
            response = client.get(f'/api/session/{session_id}/generated_ddl/TEST_TABLE')
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['content'] == ddl_content

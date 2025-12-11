"""
Tests for the reports module (modules/reports.py).
"""

import pytest
import json


class TestMigrationReportGenerator:
    """Test the MigrationReportGenerator class."""

    def test_report_generator_init(self, app_context, sample_client):
        """Test MigrationReportGenerator initializes correctly."""
        from modules.reports import MigrationReportGenerator
        from modules.db import get_db

        conn = get_db()
        generator = MigrationReportGenerator(conn, sample_client['client_id'])
        assert generator.client_id == sample_client['client_id']
        assert generator.conn == conn

    def test_gather_data_no_session(self, app_context, sample_client):
        """Test gather_data handles client with no sessions."""
        from modules.reports import MigrationReportGenerator
        from modules.db import get_db

        conn = get_db()
        generator = MigrationReportGenerator(conn, sample_client['client_id'])
        data = generator.gather_data()

        assert 'client_name' in data
        # No sessions exist, so sessions list should be empty
        assert data.get('sessions', []) == []

    def test_gather_data_with_session(self, app_context, sample_session):
        """Test gather_data retrieves session data."""
        from modules.reports import MigrationReportGenerator
        from modules.db import get_db

        conn = get_db()
        generator = MigrationReportGenerator(
            conn,
            sample_session['client_id'],
            sample_session['session_id']
        )
        data = generator.gather_data()

        # Session ID should be set on the generator and sessions list populated
        assert generator.session_id == sample_session['session_id']
        assert 'sessions' in data

    def test_generate_asciidoc_structure(self, app_context, sample_client):
        """Test generate_asciidoc produces valid AsciiDoc structure."""
        from modules.reports import MigrationReportGenerator
        from modules.db import get_db

        conn = get_db()
        generator = MigrationReportGenerator(conn, sample_client['client_id'])
        generator.gather_data()
        report = generator.generate_asciidoc()

        # Check AsciiDoc structure elements
        assert '= Migration Report:' in report
        assert ':toc:' in report
        assert '== Executive Summary' in report

    def test_generate_asciidoc_contains_client_name(self, app_context, sample_client):
        """Test report contains the client name."""
        from modules.reports import MigrationReportGenerator
        from modules.db import get_db

        conn = get_db()
        generator = MigrationReportGenerator(conn, sample_client['client_id'])
        generator.gather_data()
        report = generator.generate_asciidoc()

        assert sample_client['client_name'] in report


class TestReportsAPI:
    """Test reports API endpoints."""

    def test_get_migration_report_no_session(self, client, app_context):
        """Test GET /api/client/<id>/migration_report with no sessions returns 404."""
        from modules.db import init_db
        init_db()

        # Create a client with no sessions
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Report No Session Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Get report - should return 404 since no sessions exist
        response = client.get(f'/api/client/{client_id}/migration_report')
        assert response.status_code == 404

    def test_get_session_report(self, client, app_context):
        """Test GET /api/session/<id>/report generates report."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Session Report Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Create a session
        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Report Session', '/tmp/test', 'TABLE'),
            'session_id'
        )
        conn.commit()

        # Get session report - response has 'content' not 'report'
        response = client.get(f'/api/session/{session_id}/report')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert 'content' in data
        assert 'format' in data
        assert data['format'] == 'asciidoc'

    def test_session_report_not_found(self, client, app_context):
        """Test GET /api/session/<id>/report returns 404 for non-existent session."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/session/99999/report')
        assert response.status_code == 404


class TestRollbackAPI:
    """Test rollback script API endpoints."""

    def test_get_rollback_no_script(self, client, app_context):
        """Test GET /api/session/<id>/rollback with no rollback script returns 404."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Rollback No Script Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Create a session without rollback script
        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'No Rollback Session', '/tmp/test', 'TABLE'),
            'session_id'
        )
        conn.commit()

        # Get rollback - should return 404 since no rollback script
        response = client.get(f'/api/session/{session_id}/rollback')
        assert response.status_code == 404

    def test_rollback_preview_no_script(self, client, app_context):
        """Test GET /api/session/<id>/rollback/preview with no script returns 404."""
        from modules.db import init_db, get_db, insert_returning_id
        init_db()

        # Create a client
        create_response = client.post(
            '/api/clients',
            json={'client_name': 'Rollback Preview Test'},
            content_type='application/json'
        )
        client_id = json.loads(create_response.data)['client_id']

        # Create a session without rollback script
        conn = get_db()
        session_id = insert_returning_id(
            conn,
            'migration_sessions',
            ('client_id', 'session_name', 'export_directory', 'export_type'),
            (client_id, 'Preview Session', '/tmp/test', 'TABLE'),
            'session_id'
        )
        conn.commit()

        # Get rollback preview - should return 404 since no rollback script
        response = client.get(f'/api/session/{session_id}/rollback/preview')
        assert response.status_code == 404

    def test_rollback_not_found(self, client, app_context):
        """Test GET /api/session/<id>/rollback for non-existent session returns 404."""
        from modules.db import init_db
        init_db()

        response = client.get('/api/session/99999/rollback')
        assert response.status_code == 404

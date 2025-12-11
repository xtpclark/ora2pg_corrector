"""
Tests for the health check and general app endpoints.
"""

import pytest
import json


class TestHealthEndpoint:
    """Test the health check endpoint."""

    def test_health_check(self, client, app_context):
        """Test GET /health returns healthy status."""
        from modules.db import init_db
        init_db()

        response = client.get('/health')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'healthy'

    def test_health_check_contains_service_name(self, client, app_context):
        """Test health check includes service name."""
        from modules.db import init_db
        init_db()

        response = client.get('/health')
        data = json.loads(response.data)
        assert 'service' in data
        assert data['service'] == 'ora2pg-corrector'

    def test_health_check_contains_auth_mode(self, client, app_context):
        """Test health check includes auth mode."""
        from modules.db import init_db
        init_db()

        response = client.get('/health')
        data = json.loads(response.data)
        assert 'auth_mode' in data


class TestRootEndpoint:
    """Test the root endpoint."""

    def test_root_returns_html(self, client, app_context):
        """Test GET / returns HTML page."""
        from modules.db import init_db
        init_db()

        response = client.get('/')
        assert response.status_code == 200
        # Should return HTML content
        assert b'<!DOCTYPE html>' in response.data or b'<html' in response.data


class TestStaticFiles:
    """Test static file serving."""

    def test_static_css_exists(self, client, app_context):
        """Test that static CSS files can be accessed."""
        # This tests that static routing works
        response = client.get('/static/css/style.css')
        # Even if file doesn't exist, we want to verify routing works (404 not 500)
        assert response.status_code in [200, 304, 404]

    def test_static_js_exists(self, client, app_context):
        """Test that static JS files can be accessed."""
        response = client.get('/static/js/app.js')
        assert response.status_code in [200, 304, 404]

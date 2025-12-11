"""
Tests for the standardized API response helpers (modules/responses.py).
"""

import pytest
import json


class TestSuccessResponse:
    """Test success_response helper."""

    def test_success_response_with_dict(self, app_context):
        """Test success response with dictionary data."""
        from modules.responses import success_response

        response, status = success_response({'client_id': 1, 'name': 'Test'})
        data = json.loads(response.data)

        assert status == 200
        assert data['client_id'] == 1
        assert data['name'] == 'Test'

    def test_success_response_with_list(self, app_context):
        """Test success response with list data returns list directly for backward compat."""
        from modules.responses import success_response

        response, status = success_response([1, 2, 3])
        data = json.loads(response.data)

        assert status == 200
        # Lists are returned directly for backward compatibility
        assert data == [1, 2, 3]

    def test_success_response_with_message(self, app_context):
        """Test success response with message."""
        from modules.responses import success_response

        response, status = success_response(message="Operation successful")
        data = json.loads(response.data)

        assert status == 200
        assert data['message'] == "Operation successful"

    def test_success_response_custom_status(self, app_context):
        """Test success response with custom status code."""
        from modules.responses import success_response

        response, status = success_response({'id': 1}, status_code=201)

        assert status == 201


class TestErrorResponse:
    """Test error_response helper."""

    def test_error_response_basic(self, app_context):
        """Test basic error response."""
        from modules.responses import error_response

        response, status = error_response("Something went wrong")
        data = json.loads(response.data)

        assert status == 400
        assert data['error'] == "Something went wrong"

    def test_error_response_with_details(self, app_context):
        """Test error response with details."""
        from modules.responses import error_response

        response, status = error_response("Database error", "Connection refused")
        data = json.loads(response.data)

        assert status == 400
        assert data['error'] == "Database error"
        assert data['details'] == "Connection refused"

    def test_error_response_custom_status(self, app_context):
        """Test error response with custom status code."""
        from modules.responses import error_response

        response, status = error_response("Not found", status_code=404)

        assert status == 404


class TestConvenienceResponses:
    """Test convenience response helpers."""

    def test_not_found_response(self, app_context):
        """Test not_found_response helper."""
        from modules.responses import not_found_response

        response, status = not_found_response("Client")
        data = json.loads(response.data)

        assert status == 404
        assert "Client not found" in data['error']

    def test_validation_error_response(self, app_context):
        """Test validation_error_response helper."""
        from modules.responses import validation_error_response

        response, status = validation_error_response("Name is required")
        data = json.loads(response.data)

        assert status == 400
        assert data['error'] == "Name is required"

    def test_server_error_response(self, app_context):
        """Test server_error_response helper."""
        from modules.responses import server_error_response

        response, status = server_error_response("Failed to process", "Stack trace here")
        data = json.loads(response.data)

        assert status == 500
        assert "Failed to process" in data['error']
        assert data['details'] == "Stack trace here"

    def test_db_error_response(self, app_context):
        """Test db_error_response helper."""
        from modules.responses import db_error_response

        response, status = db_error_response()
        data = json.loads(response.data)

        assert status == 500
        assert "Database" in data['error']

    def test_created_response(self, app_context):
        """Test created_response helper."""
        from modules.responses import created_response

        response, status = created_response({'id': 42})
        data = json.loads(response.data)

        assert status == 201
        assert data['id'] == 42

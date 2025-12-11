"""
Standardized API response helpers for consistent response format.

Provides utility functions to create consistent JSON responses across all API endpoints.

Standard response format:
- Success: {"data": ..., "message": "..."} with 2xx status
- Error: {"error": "...", "details": "..."} with 4xx/5xx status
"""

from flask import jsonify
from typing import Any, Optional, Tuple


def success_response(
    data: Any = None,
    message: Optional[str] = None,
    status_code: int = 200
) -> Tuple[Any, int]:
    """
    Create a standardized success response.

    Args:
        data: The response data (can be dict, list, or any JSON-serializable value)
        message: Optional success message
        status_code: HTTP status code (default 200)

    Returns:
        Tuple of (Flask response, status code)

    Examples:
        return success_response({"client_id": 1})
        return success_response(clients_list)
        return success_response(message="Client created", status_code=201)
    """
    # If data is a list, return it directly for backward compatibility
    if isinstance(data, list):
        return jsonify(data), status_code

    response = {}

    if data is not None:
        if isinstance(data, dict):
            response = data
        else:
            response['data'] = data

    if message:
        response['message'] = message

    return jsonify(response), status_code


def error_response(
    error: str,
    details: Optional[str] = None,
    status_code: int = 400
) -> Tuple[Any, int]:
    """
    Create a standardized error response.

    Args:
        error: Main error message
        details: Optional detailed error information
        status_code: HTTP status code (default 400)

    Returns:
        Tuple of (Flask response, status code)

    Examples:
        return error_response("Client name is required")
        return error_response("Database error", str(e), 500)
    """
    response = {'error': error}

    if details:
        response['details'] = details

    return jsonify(response), status_code


def not_found_response(resource: str = "Resource") -> Tuple[Any, int]:
    """
    Create a standardized 404 not found response.

    Args:
        resource: Name of the resource that was not found

    Returns:
        Tuple of (Flask response, 404)
    """
    return error_response(f"{resource} not found", status_code=404)


def validation_error_response(message: str) -> Tuple[Any, int]:
    """
    Create a standardized 400 validation error response.

    Args:
        message: Validation error message

    Returns:
        Tuple of (Flask response, 400)
    """
    return error_response(message, status_code=400)


def server_error_response(
    message: str = "An unexpected error occurred",
    details: Optional[str] = None
) -> Tuple[Any, int]:
    """
    Create a standardized 500 server error response.

    Args:
        message: Error message
        details: Optional error details (e.g., exception message)

    Returns:
        Tuple of (Flask response, 500)
    """
    return error_response(message, details, status_code=500)


def db_error_response() -> Tuple[Any, int]:
    """
    Create a standardized database connection error response.

    Returns:
        Tuple of (Flask response, 500)
    """
    return error_response("Database connection failed", status_code=500)


def created_response(data: Any = None, message: str = "Created successfully") -> Tuple[Any, int]:
    """
    Create a standardized 201 created response.

    Args:
        data: The created resource data
        message: Success message

    Returns:
        Tuple of (Flask response, 201)
    """
    return success_response(data, message, status_code=201)

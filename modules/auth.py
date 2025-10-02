"""
Token-based authentication middleware for Ora2Pg Corrector
Supports both local development and nginx-proxied deployment
"""

import os
import secrets
import json
from pathlib import Path
from functools import wraps
from flask import request, jsonify, current_app
import logging

logger = logging.getLogger(__name__)

class TokenAuth:
    """
    Simple token authentication that works with:
    - Local development
    - Docker/Podman containers
    - nginx reverse proxy
    """
    
    def __init__(self, app=None):
        self.token = None
        self.auth_mode = None
        self.token_file_path = None
        
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize the auth system with Flask app"""
        self.auth_mode = os.environ.get('AUTH_MODE', 'token').lower()
        
        if self.auth_mode == 'none':
            logger.warning("AUTH_MODE=none - Authentication disabled!")
            self.token = None
            return
        
        # Force Docker path when running in container
        # Check multiple indicators for containerized environment
        is_container = any([
            os.path.exists('/.dockerenv'),
            os.environ.get('CONTAINER_ENV'),
            os.path.exists('/app/data'),  # This directory exists in our container
            os.environ.get('HOST_UID')     # Set in docker-compose
        ])
        
        if is_container:
            self.token_file_path = Path('/app/data/.auth_token')
            logger.info("Running in container, using /app/data/.auth_token")
        else:
            # Local development
            self.token_file_path = Path.home() / '.ora2pg_corrector' / 'auth_token'
            logger.info(f"Running locally, using {self.token_file_path}")
        
        # Load or generate token
        self.token = self._load_or_generate_token()
        
        # Register before_request handler
        if self.auth_mode != 'none':
            app.before_request(self._check_auth)
        
        # Add auth info endpoint
        @app.route('/api/auth/info', methods=['GET'])
        def auth_info():
            """Endpoint to check auth status - useful for nginx health checks"""
            if self._is_request_authenticated(request):
                return jsonify({
                    'authenticated': True,
                    'mode': self.auth_mode
                })
            return jsonify({
                'authenticated': False,
                'mode': self.auth_mode,
                'hint': 'Add X-Auth-Token header or ?token= parameter'
            }), 401
    
    def _load_or_generate_token(self):
        """Load existing token or generate new one"""
        # Check environment variable first
        env_token = os.environ.get('ACCESS_TOKEN')
        if env_token:
            logger.info("Using ACCESS_TOKEN from environment")
            return env_token
        
        # Try to load from file
        if self.token_file_path and self.token_file_path.exists():
            try:
                token = self.token_file_path.read_text().strip()
                logger.info(f"Loaded existing token from {self.token_file_path}")
                return token
            except Exception as e:
                logger.error(f"Failed to read token file: {e}")
        
        # Generate new token
        token = secrets.token_urlsafe(32)
        
        # Save to file if possible
        if self.token_file_path:
            try:
                self.token_file_path.parent.mkdir(parents=True, exist_ok=True)
                self.token_file_path.write_text(token)
                # Set restrictive permissions (owner read/write only)
                if os.name != 'nt':  # Unix-like systems
                    os.chmod(self.token_file_path, 0o600)
                logger.info(f"Generated new token and saved to {self.token_file_path}")
            except Exception as e:
                logger.error(f"Failed to save token file: {e}")
        
        # Log token for initial setup (only in development)
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('DEBUG'):
            logger.info(f"Access token: {token}")
            print(f"\n{'='*60}")
            print(f"ACCESS TOKEN: {token}")
            print(f"{'='*60}\n")
            print("Use this token in one of these ways:")
            print(f"1. Header: X-Auth-Token: {token}")
            print(f"2. Query: ?token={token}")
            print(f"3. nginx: proxy_set_header X-Auth-Token {token};")
            print(f"{'='*60}\n")
        else:
            logger.info("Token generated. Check token file or container logs for value.")
        
        return token
    
    def _is_request_authenticated(self, request):
        """Check if the request has valid authentication"""
        if self.auth_mode == 'none':
            return True
        
        # Check for token in multiple places
        provided_token = (
            # Standard header
            request.headers.get('X-Auth-Token') or
            # Authorization Bearer token
            self._extract_bearer_token(request) or
            # Query parameter (useful for downloads)
            request.args.get('token') or
            # Form data (for POST requests)
            request.form.get('token') if request.form else None
        )
        
        # For JSON requests, check body too
        if not provided_token and request.is_json:
            try:
                provided_token = request.get_json().get('token')
            except:
                pass
        
        return provided_token == self.token if self.token else False
    
    def _extract_bearer_token(self, request):
        """Extract token from Authorization: Bearer header"""
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            return auth_header[7:]
        return None
    
    def _check_auth(self):
        """Flask before_request handler to check authentication"""
        # Skip auth for static files and public endpoints
        if request.path.startswith('/static/') or request.path == '/favicon.ico':
            return None
        
        # Allow health check endpoint without auth
        if request.path == '/health':
            return None
        
        # Allow OPTIONS requests (for CORS)
        if request.method == 'OPTIONS':
            return None
        
        if not self._is_request_authenticated(request):
            # Check if request is from localhost (bypass for local development)
            if os.environ.get('ALLOW_LOCALHOST_BYPASS', 'false').lower() == 'true':
                remote_addr = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
                if remote_addr in ['127.0.0.1', '::1', 'localhost']:
                    logger.debug("Allowing localhost bypass")
                    return None
            
            # Return 401 with helpful message
            return jsonify({
                'error': 'Authentication required',
                'message': 'Please provide a valid access token',
                'hint': 'Add X-Auth-Token header or ?token= parameter'
            }), 401
    
    def require_auth(self, f):
        """Decorator for routes that require authentication"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not self._is_request_authenticated(request):
                return jsonify({
                    'error': 'Authentication required'
                }), 401
            return f(*args, **kwargs)
        return decorated_function

# Global auth instance
auth = TokenAuth()

def init_auth(app):
    """Initialize authentication for the Flask app"""
    auth.init_app(app)
    return auth

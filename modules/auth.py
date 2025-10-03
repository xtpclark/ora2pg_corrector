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
        
        # Debug endpoint to check token status
        @app.route('/api/auth/debug', methods=['GET'])
        def auth_debug():
            """Debug endpoint to troubleshoot auth issues"""

            provided_token = (
                request.headers.get('X-Auth-Token') or
                self._extract_bearer_token(request) or
                request.args.get('token') or
                (request.form.get('token') if request.form else None)
            )
            
            # Additional debug info
            query_token = request.args.get('token')
            header_token = request.headers.get('X-Auth-Token')
            
            # Only show partial tokens for security
            def mask_token(t):
                if t and len(t) > 8:
                    return f"{t[:4]}...{t[-4:]}"
                return t
            
            return jsonify({
                'auth_mode': self.auth_mode,
                'token_file_path': str(self.token_file_path) if self.token_file_path else None,
                'token_loaded': mask_token(self.token) if self.token else 'None',
                'provided_token': mask_token(provided_token) if provided_token else 'None',
                'query_token': mask_token(query_token) if query_token else 'None',
                'header_token': mask_token(header_token) if header_token else 'None',
                'tokens_match': provided_token == self.token if self.token else False,
                'request_url': request.url,
                'request_args': dict(request.args),
                'request_headers': list(request.headers.keys())
            })
    
    def _load_or_generate_token(self):
        """Load existing token or generate new one (thread-safe for multiple workers)"""
        # Check environment variable first and prioritize it
        env_token = os.environ.get('ACCESS_TOKEN')
        if env_token:
            logger.info("Using ACCESS_TOKEN from environment")
            
            # Update the token file to match the environment variable
            # This ensures all workers use the same token
            if self.token_file_path:
                try:
                    self.token_file_path.parent.mkdir(parents=True, exist_ok=True)
                    self.token_file_path.write_text(env_token)
                    # Set restrictive permissions (owner read/write only)
                    if os.name != 'nt':  # Unix-like systems
                        os.chmod(self.token_file_path, 0o600)
                    logger.info(f"Updated token file to match ACCESS_TOKEN environment variable")
                except Exception as e:
                    logger.error(f"Failed to update token file with env token: {e}")
            
            return env_token
        
        # Try to load from file if no env token
        if self.token_file_path and self.token_file_path.exists():
            try:
                token = self.token_file_path.read_text().strip()
                if token:  # Make sure it's not empty
                    logger.info(f"Loaded existing token from {self.token_file_path}")
                    return token
            except Exception as e:
                logger.error(f"Failed to read token file: {e}")
        
        # Generate new token with file locking to prevent race conditions
        import fcntl
        token = None
        
        if self.token_file_path:
            try:
                self.token_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Use a lock file to ensure only one worker generates the token
                lock_file = self.token_file_path.with_suffix('.lock')
                
                with open(lock_file, 'a+') as lock:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                    
                    # Check again if token was created by another worker
                    if self.token_file_path.exists():
                        try:
                            token = self.token_file_path.read_text().strip()
                            if token:
                                logger.info(f"Token was created by another worker, loaded from {self.token_file_path}")
                                return token
                        except:
                            pass
                    
                    # Generate new token since no valid token exists
                    token = secrets.token_urlsafe(32)
                    self.token_file_path.write_text(token)
                    
                    # Set restrictive permissions (owner read/write only)
                    if os.name != 'nt':  # Unix-like systems
                        os.chmod(self.token_file_path, 0o600)
                    
                    logger.info(f"Generated new token and saved to {self.token_file_path}")
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                    
            except Exception as e:
                logger.error(f"Failed to save token file: {e}")
                # If file operations fail, generate a token anyway
                if not token:
                    token = secrets.token_urlsafe(32)
        else:
            # No file path configured, just generate a token
            token = secrets.token_urlsafe(32)
            logger.warning("No token file path configured, using in-memory token only")
        
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
        
        # *** ADD THESE DEBUG LINES ***
        logger.info(f"DEBUG: request.path = {request.path}")
        logger.info(f"DEBUG: request.args = {dict(request.args)}")
        logger.info(f"DEBUG: request.query_string = {request.query_string}")
        logger.info(f"DEBUG: self.token = {self.token}")
        # *** END DEBUG ***
        
        # Check for token in multiple places
        provided_token = (
            # Standard header
            request.headers.get('X-Auth-Token') or
            # Authorization Bearer token
            self._extract_bearer_token(request) or
            # Query parameter (useful for downloads)
            request.args.get('token') or
            # Form data (for POST requests)
            (request.form.get('token') if request.form else None)
        )
        

        # *** ADD THIS DEBUG LINE ***
        logger.info(f"DEBUG: provided_token = {provided_token}")
        # *** END DEBUG ***

        # For JSON requests, check body too
        if not provided_token and request.is_json:
            try:
                provided_token = request.get_json().get('token')
            except:
                pass
        
        # Debug logging
        logger.debug(f"Auth check - Path: {request.path}, Method: {request.method}")
        logger.debug(f"Auth check - Token loaded: {self.token[:8] + '...' if self.token else 'None'}")
        logger.debug(f"Auth check - Token provided: {provided_token[:8] + '...' if provided_token else 'None'}")
        logger.debug(f"Auth check - Headers: {dict(request.headers)}")
        
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
        
            
        # *** ADD THIS LINE ***
        if request.path == '/':
            return None

        # Allow health check endpoint without auth
        if request.path == '/health':
            return None
        
        # Allow debug endpoint without auth for troubleshooting
        if request.path == '/api/auth/debug':
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

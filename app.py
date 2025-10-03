from flask import Flask, jsonify, request
from modules.db import close_db, init_db
from modules.config import load_ai_providers, load_ora2pg_config
from modules.auth import init_auth
from routes.main_routes import main_bp
from routes.api_routes import api_bp
import os
from dotenv import load_dotenv
import logging
import fcntl
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_database_once():
    """
    Initialize database only once, even with multiple workers.
    Uses file locking to ensure only one worker does the initialization.
    """
    lock_file = Path('/app/data/.db_init.lock')
    marker_file = Path('/app/data/.db_initialized')
    
    # Create directories if they don't exist
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(lock_file, 'a+') as lock:
            # Acquire exclusive lock
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            
            # Check if already initialized
            if marker_file.exists():
                logger.info("Database already initialized by another worker")
                return
            
            logger.info("This worker is initializing the database...")
            
            # Perform initialization
            from modules.db import get_db
            
            # Step 1: Create the database schema
            init_db()
            
            # Step 2: Get a connection and seed the data
            conn = get_db()
            if conn:
                load_ai_providers(conn)
                load_ora2pg_config(conn)
                conn.close()
            
            # Mark as initialized
            marker_file.touch()
            logger.info("Database initialization complete")
            
            # Release lock
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def create_app():
    """Create and configure an instance of the Flask application."""
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    app = Flask(__name__,
                static_folder=os.path.join(basedir, 'static'),
                template_folder=os.path.join(basedir, 'templates'))

    app.config['SECRET_KEY'] = os.environ.get('APP_SECRET_KEY')
    if not app.config['SECRET_KEY']:
        raise ValueError("APP_SECRET_KEY environment variable not set.")

    # Initialize authentication (this is safe to do per-worker)
    init_auth(app)
    logger.info(f"Authentication mode: {os.environ.get('AUTH_MODE', 'token')}")
    
    # Health check endpoint (no auth required)
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint for Docker/Kubernetes/nginx"""
        return jsonify({
            'status': 'healthy',
            'service': 'ora2pg-corrector',
            'auth_mode': os.environ.get('AUTH_MODE', 'token'),
            'worker_pid': os.getpid()
        })

    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp)

    # Teardown app context to close DB connection
    @app.teardown_appcontext
    def teardown_db(exception):
        close_db()

    # Initialize database ONCE across all workers
    with app.app_context():
        initialize_database_once()

    # Error handlers
    @app.errorhandler(405)
    def method_not_allowed(e):
        logger.error(f"405 Method Not Allowed: {request.method} {request.url}")
        return jsonify({'error': 'Method not allowed'}), 405

    @app.errorhandler(Exception)
    def handle_error(e):
        logger.error(f"Unhandled error: {str(e)}", exc_info=True)
        return jsonify({'error': 'An unexpected error occurred'}), 500

    return app

if __name__ == '__main__':
    app = create_app()
    
    # Determine bind address based on AUTH_MODE
    auth_mode = os.environ.get('AUTH_MODE', 'token').lower()
    
    # In Docker, always bind to 0.0.0.0 for container networking
    if os.path.exists('/.dockerenv') or os.environ.get('CONTAINER_ENV'):
        bind_host = '0.0.0.0'
        logger.info("Running in container, binding to 0.0.0.0")
    else:
        # For local development
        if auth_mode == 'none':
            bind_host = '127.0.0.1'
            logger.warning("AUTH_MODE=none, binding to localhost only")
        else:
            bind_host = os.environ.get('BIND_ADDRESS', '127.0.0.1')
    
    port = int(os.environ.get('PORT', 8000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    
    logger.info(f"Starting server on {bind_host}:{port} (auth_mode: {auth_mode})")
    app.run(debug=debug, host=bind_host, port=port)

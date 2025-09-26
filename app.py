from flask import Flask, g
from modules.db import close_db
from routes.main_routes import main_bp
from routes.auth_routes import auth_bp
from routes.api_routes import api_bp
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_app():
    """Create and configure an instance of the Flask application."""
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    app = Flask(__name__,
                static_folder=os.path.join(basedir, 'static'),
                template_folder=os.path.join(basedir, 'templates'))

    app.config['SECRET_KEY'] = os.environ.get('APP_SECRET_KEY')
    if not app.config['SECRET_KEY']:
        raise ValueError("APP_SECRET_KEY environment variable not set.")

    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(api_bp)

    # Teardown app context
    @app.teardown_appcontext
    def teardown_db(exception):
        close_db()

    with app.app_context():
        from modules.db import init_db
        init_db()

    # Error handlers
    @app.errorhandler(405)
    def method_not_allowed(e):
        logger.error(f"405 Method Not Allowed: {request.method} {request.url}")
        return jsonify({'error': 'Method not allowed'}), 405

    @app.errorhandler(Exception)
    def handle_error(e):
        logger.error(f"Unhandled error: {str(e)}")
        return jsonify({'error': 'An unexpected error occurred'}), 500

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=8000)

"""
API Blueprints Package

This package contains modular API blueprints organized by functionality:
- clients: Client CRUD operations
- config: Configuration management
- sessions: Session and file management
- migration: Migration orchestration
- objects: Migration object tracking
- ddl_cache: DDL caching endpoints
- reports: Report and rollback generation
- sql_ops: SQL operations (correct, validate, save)
"""

from flask import Blueprint

# Create main API blueprint
api_bp = Blueprint('api_bp', __name__, url_prefix='/api')

# Import and register sub-blueprints
from .clients import clients_bp
from .config import config_bp
from .sessions import sessions_bp
from .migration import migration_bp
from .objects import objects_bp
from .ddl_cache import ddl_cache_bp
from .reports import reports_bp
from .sql_ops import sql_ops_bp

# Register all sub-blueprints with the main api_bp
api_bp.register_blueprint(clients_bp)
api_bp.register_blueprint(config_bp)
api_bp.register_blueprint(sessions_bp)
api_bp.register_blueprint(migration_bp)
api_bp.register_blueprint(objects_bp)
api_bp.register_blueprint(ddl_cache_bp)
api_bp.register_blueprint(reports_bp)
api_bp.register_blueprint(sql_ops_bp)

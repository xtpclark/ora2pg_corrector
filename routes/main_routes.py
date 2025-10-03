from flask import Blueprint, render_template

main_bp = Blueprint('main_bp', __name__)

@main_bp.route('/', methods=['GET'])
def index():
    """Renders the main single-page application."""
    return render_template('index.html')

@main_bp.route('/favicon.ico')
def favicon():
    """Provides a route for the browser's favicon request to prevent 404 errors."""
    return '', 204

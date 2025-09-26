from flask import Blueprint, render_template

main_bp = Blueprint('main_bp', __name__)

@main_bp.route('/', methods=['GET'])
def index():
    """Renders the main single-page application."""
    return render_template('index.html')

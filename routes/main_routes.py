from flask import Blueprint, render_template

main_bp = Blueprint('main_bp', __name__)

@main_bp.route('/', methods=['GET'])
def index():
    """Renders the login page."""
    return render_template('login.html')

@main_bp.route('/configurator', methods=['GET'])
def configurator():
    """Renders the main configurator page."""
    return render_template('configurator.html')

@main_bp.route('/comparison', methods=['GET'])
def comparison():
    """Renders the SQL comparison page."""
    return render_template('comparison.html')

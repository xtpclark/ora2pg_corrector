from flask import Blueprint, redirect
from modules.auth import login

auth_bp = Blueprint('auth_bp', __name__)

@auth_bp.route('/api/login', methods=['POST'])
def login_route():
    """Handles the user login API call."""
    return login()

@auth_bp.route('/logout')
def logout():
    """Handles user logout and redirects to the login page."""
    # The token is removed from localStorage by the browser's JavaScript.
    # This server-side route simply needs to handle the redirect.
    return redirect('/')

from flask import jsonify, request, g
from functools import wraps
import jwt
from datetime import datetime, timedelta, timezone
import os

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization header is missing or invalid'}), 401
        token = auth_header.split(' ')[1]
        try:
            payload = jwt.decode(token, os.environ.get('APP_SECRET_KEY'), algorithms=['HS256'])
            g.user_id = payload['sub']
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Token is invalid'}), 401
        return f(*args, **kwargs)
    return decorated

def login():
    data = request.json
    username, password = data.get('username'), data.get('password')
    DUMMY_USER = os.environ.get("DUMMY_USER", "admin")
    DUMMY_PASSWORD = os.environ.get("DUMMY_PASSWORD", "password")
    if username == DUMMY_USER and password == DUMMY_PASSWORD:
        token = jwt.encode({'sub': username, 'iat': datetime.now(timezone.utc), 'exp': datetime.now(timezone.utc) + timedelta(hours=24)}, os.environ.get('APP_SECRET_KEY'), algorithm='HS256')
        return jsonify({'token': token})
    return jsonify({'error': 'Invalid credentials'}), 401

# routes/routes_auth.py
from flask import Blueprint, request, redirect, jsonify, make_response
import os, json, time, requests
import logging

auth_bp = Blueprint("auth", __name__)
logger = logging.getLogger("scanner_web.routes_auth")

# ---- Configure via environment (works out of the box with sensible defaults) ----
LOGIN_VALIDATE_URL = os.environ.get("LOGIN_VALIDATE_URL", "http://127.0.0.1:8010/api/me")
COOKIE_NAME       = os.environ.get("AUTH_COOKIE_NAME", "scanner_session")
COOKIE_DOMAIN     = os.environ.get("AUTH_COOKIE_DOMAIN", ".iamcalledned.ai")  # include leading dot
COOKIE_SECURE     = os.environ.get("AUTH_COOKIE_SECURE", "true").lower() == "true"
COOKIE_SAMESITE   = os.environ.get("AUTH_COOKIE_SAMESITE", "None")  # None|Lax|Strict

LOGIN_PROCESS_URL = os.environ.get('LOGIN_PROCESS_URL', 'http://127.0.0.1:8010/api/login')
LOGIN_API_URL = os.environ.get('LOGIN_API_URL', 'http://127.0.0.1:8010')



@auth_bp.route('/scanner/me')
def scanner_me():
    """
    Validate the 'scanner_session' cookie and refresh its TTL if valid.
    """
    # Read the session ID from the HttpOnly cookie sent by the browser.
    session_id = request.cookies.get('scanner_session')
    
    if not session_id:
        return jsonify({"authenticated": False, "error": "Missing session cookie"}), 401

    try:
        response = requests.get(f"{LOGIN_API_URL}/get_session_data",
                                params={"session_id": session_id}, timeout=3)
        response.raise_for_status()
        data = response.json()

        session = data.get("session")
        if not session:
            return jsonify({"authenticated": False, "error": "Invalid session"}), 401

        # The user is authenticated. Return their info.
        return jsonify({"authenticated": True, "userInfo": session}), 200

    except requests.exceptions.RequestException as e:
        logger.error("/scanner/me could not reach login service: %s", e)
        return jsonify({"authenticated": False, "error": "Auth service unavailable"}), 502



@auth_bp.route('/scanner/logout', methods=['POST'])
def scanner_logout():
    """Logout by getting session_id from JSON body."""
    # 1. Get the session_id from the cookie.
    session_id = request.cookies.get('scanner_session')

    if not session_id:
        # If no cookie, they are already logged out. Just confirm.
        return jsonify({"message": "No active session"}), 200

    try:
        # 2. Tell the login service to delete this session from Redis.
        response = requests.post(f"{LOGIN_API_URL}/api/logout", json={"session_id": session_id})
        response.raise_for_status()
        
        # 3. Create a response to clear the cookie in the user's browser.
        resp = jsonify({"message": "Logout successful"})
        resp.set_cookie('scanner_session', '', expires=0, domain=".iamcalledned.ai", path="/")
        return resp
        
    except requests.exceptions.RequestException as e:
        logger.error("Could not contact login service for logout: %s", e)
        return jsonify({"error": "Logout failed"}), 502

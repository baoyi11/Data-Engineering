from functools import wraps
from flask import request, jsonify, g
import secrets

# API Key store mapping keys to client identities and roles
API_KEYS = {
    "dev-key-sensor-read-only": {"client": "dashboard", "role": "reader"},
    "dev-key-sensor-write": {"client": "pipeline", "role": "writer"},
    "dev-key-admin": {"client": "admin", "role": "admin"},
}

def generate_api_key() -> str:
    """Generate a cryptographically secure 32-byte API key."""
    return secrets.token_urlsafe(32)

def require_api_key(f):
    """Decorator: validates X-API-Key header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key")
        if not key:
            return jsonify({"status": "error", "code": 401, "message": "X-API-Key header is required."}), 401
        client = API_KEYS.get(key)
        if not client:
            return jsonify({"status": "error", "code": 401, "message": "Invalid API key."}), 401

        g.api_client = client # Store client info in Flask g context
        return f(*args, **kwargs)
    return decorated

def require_role(role: str):
    """Decorator factory: checks that g.api_client has the given role."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            client = g.get("api_client", {})
            allowed = {
                "reader": ["reader", "writer", "admin"],
                "writer": ["writer", "admin"],
                "admin": ["admin"],
            }
            if client.get("role") not in allowed.get(role, []):
                return jsonify({"status": "error", "code": 403, "message": f"Role {role} or higher required."}), 403
            return f(*args, **kwargs)
        return decorated
    return decorator
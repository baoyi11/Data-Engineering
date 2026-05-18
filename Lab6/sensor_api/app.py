"""
Lab 5 & 6 -- Sensor Data REST API
==============================
app.py - Flask application entry point (Refactored for Swagger UI)
"""
from flask import Flask, jsonify, request, abort
from datetime import datetime, timezone
import json
import hmac


from kafka_utils import get_latest_readings, publish_reading
from lake_utils import get_statistics, get_sensor_types
from auth import require_api_key, require_role
from weather_utils import get_weather, CITIES


from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity, get_jwt
from datetime import timedelta
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_smorest import Api, Blueprint

app = Flask(__name__)

# 1. API Metadata, JWT, OpenAPI
API_VERSION = "1.0"
API_PREFIX = "/api/v1"
VALID_SENSORS = {"temperature", "humidity", "pressure"}
REQUIRED_FIELDS = {"sensor", "value"}

# JWT 
app.config["JWT_SECRET_KEY"] = "CHANGE-ME-IN-PRODUCTION" 
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1) 
jwt = JWTManager(app) 

# OpenAPI / Swagger UI
app.config.update({
    "API_TITLE": "Sensor Data API",
    "API_VERSION": "v1",
    "OPENAPI_VERSION": "3.0.3",
    "OPENAPI_URL_PREFIX": "/",
    "OPENAPI_SWAGGER_UI_PATH": "/swagger-ui",
    "OPENAPI_SWAGGER_UI_URL": "https://cdn.jsdelivr.net/npm/swagger-ui-dist/",
    "OPENAPI_REDOC_PATH": "/redoc",
})
api = Api(app)

# 2.Rate Limiting

limiter = Limiter(
    app=app,
    key_func=get_remote_address, 
    default_limits=["500 per day", "100 per hour"],
    storage_uri="memory://",
    headers_enabled=True, 
)

blp = Blueprint("sensors", __name__, url_prefix=API_PREFIX, description="Sensor Data API Operations")

# JWT
USERS = {
    "alice": {"password": "secret123", "role": "admin"},
    "bob": {"password": "pass456", "role": "reader"},
}


# 3. Endpoints

# 1b Health Check Endpoint
@blp.route("/health")
@limiter.exempt
def health():
    return jsonify({
        "status": "ok",
        "version": API_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "sensor-data-api",
    }), 200

# JWT Login
@blp.route("/auth/login", methods=["POST"])
@limiter.limit("10 per minute; 3 per second") 
def login():
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"status": "error", "message": "JSON body required."}), 400

    username = body.get("username", "")
    password = body.get("password", "")
    user = USERS.get(username)

    if not user or not hmac.compare_digest(user["password"], password):
        return jsonify({"status": "error", "message": "Invalid credentials."}), 401

    token = create_access_token(identity=username, additional_claims={"role": user["role"]})
    return jsonify({
        "status": "success",
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": 3600
    }), 200

# JWT Protected Endpoint
@blp.route("/me")
@jwt_required() 
def me():
    identity = get_jwt_identity() 
    claims = get_jwt() 
    return jsonify({
        "status": "success",
        "data": {
            "username": identity,
            "role": claims.get("role"),
        },
    }), 200

# 2a List All Sensor Types
@blp.route("/sensors")
@require_api_key
@limiter.limit("300 per minute")
def list_sensors():
    try:
        sensor_types = get_sensor_types()
        return jsonify({
            "status": "success",
            "count": len(sensor_types),
            "data": sensor_types,
        }), 200
    except Exception as exc:
        app.logger.error("list_sensors error: %s", exc)
        return jsonify({"status": "error", "message": "Failed to retrieve sensor list."}), 500
    
# 2b Get Latest Reading for a Sensor Type
@blp.route("/sensors/<sensor_type>/latest")
@require_api_key
def latest_reading(sensor_type: str):
    if sensor_type not in VALID_SENSORS:
        return jsonify({
            "status": "error",
            "message": f"Unknown sensor type '{sensor_type}'. Valid types: {sorted(VALID_SENSORS)}"
        }), 404
    try:
        reading = get_latest_readings(sensor_type, n=1)
        if not reading:
            return jsonify({"status": "error", "message": f"No readings available for sensor '{sensor_type}'."}), 404
        return jsonify({"status": "success", "data": reading[0]}), 200
    except Exception as exc:
        app.logger.error("latest_reading error: %s", exc)
        return jsonify({"status": "error", "message": "Failed to retrieve latest reading."}), 500

# 3 Statistics from Parquet
@blp.route("/sensors/<sensor_type>/stats")
@require_api_key
def sensor_stats(sensor_type: str):
    if sensor_type not in VALID_SENSORS:
        return jsonify({"status": "error", "message": f"Unknown sensor type '{sensor_type}'."}), 404
    try:
        days = int(request.args.get("days", 7))
        if days < 1 or days > 90:
            raise ValueError("days must be between 1 and 90")
    except (ValueError, TypeError) as exc:
        return jsonify({"status": "error", "message": f"Invalid 'days' parameter: {exc}"}), 400

    try:
        stats = get_statistics(sensor_type, days=days)
        return jsonify({
            "status": "success", "sensor_type": sensor_type, "days": days,
            "count": len(stats), "data": stats,
        }), 200
    except Exception as exc:
        app.logger.error("sensor_stats error: %s", exc)
        return jsonify({"status": "error", "message": "Failed to retrieve statistics."}), 500

# 4 POST: Write a Reading to Kafka
@blp.route("/readings", methods=["POST"])
@require_api_key
@require_role("writer")
def create_reading():
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({"status": "error", "message": "Request body must be valid JSON."}), 400

    missing = REQUIRED_FIELDS - set(body.keys())
    if missing:
        return jsonify({"status": "error", "message": f"Missing required fields: {missing}"}), 400

    sensor = body["sensor"]
    if sensor not in VALID_SENSORS:
        return jsonify({"status": "error", "message": f"Invalid sensor type '{sensor}'."}), 422

    try:
        value = float(body["value"])
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "'value' must be a number."}), 422

    reading = {
        "sensor": sensor, "value": value, "unit": body.get("unit", ""),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "source": "api-v1", "anomaly": False,
    }

    try:
        metadata = publish_reading(reading)
        return jsonify({
            "status": "success", "message": "Reading published to Kafka.",
            "data": {"reading": reading, "partition": metadata["partition"], "offset": metadata["offset"]}
        }), 201
    except Exception as exc:
        app.logger.error("create_reading error: %s", exc)
        return jsonify({"status": "error", "message": "Failed to publish reading."}), 500

# 5 Enriched Data (Kafka + External Weather API)
@blp.route("/enriched/<sensor_type>")
@require_api_key
def enriched_reading(sensor_type: str):
    city = request.args.get("city", "paris").lower()
    if city not in CITIES:
        return jsonify({"status": "error", "message": f"Unknown city. Valid: {list(CITIES.keys())}"}), 400

    try:
        readings = get_latest_readings(sensor_type, n=1)
        sensor_data = readings[0] if readings else None
    except Exception as exc:
        app.logger.error("Kafka error: %s", exc)
        return jsonify({"status": "error", "message": "Failed to fetch sensor reading."}), 503

    try:
        weather = get_weather(city)
    except Exception as exc:
        app.logger.error("Weather API error: %s", exc)
        return jsonify({"status": "success", "sensor": sensor_data, "weather": None, "warning": "Weather service unavailable."}), 200

    diff = None
    if sensor_type == "temperature" and sensor_data and weather:
        diff = round((sensor_data.get("value", 0) - weather.get("temperature_c", 0)), 2)

    return jsonify({
        "status": "success",
        "sensor": sensor_data,
        "weather": weather,
        "context": {"outdoor_vs_sensor_temp": diff}
    }), 200


# 4. Error Handlers

@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "code": 404, "message": "The requested resource was not found."}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({"status": "error", "code": 405, "message": "HTTP method not allowed for this endpoint."}), 405

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({"status": "error", "code": 429, "message": f"Rate limit exceeded: {e.description}"}), 429

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"status": "error", "code": 500, "message": "An internal server error occurred."}), 500


api.register_blueprint(blp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
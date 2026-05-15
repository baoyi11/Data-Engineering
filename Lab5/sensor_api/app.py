"""
Lab 5 -- Sensor Data REST API
==============================
app.py - Flask application entry point
"""
# 1a Create app.py 
from flask import Flask, jsonify, request, abort
from datetime import datetime, timezone
from kafka_utils import get_latest_readings, publish_reading
from lake_utils import get_statistics, get_sensor_types

app = Flask(__name__)

# API Metadata
API_VERSION = "1.0"
API_PREFIX = "/api/v1"

# 1b Health Check Endpoint
@app.route(f"{API_PREFIX}/health")
def health():
    return jsonify({
        "status": "ok",
        "version": API_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "sensor-data-api",
    }), 200

# 2a List All Sensor Types
@app.route(f"{API_PREFIX}/sensors")
def list_sensors():
    """
    GET /api/v1/ sensors
    Returns the list of all known sensor types .
    Data is read from the Parquet curated zone .
    """
    try:
        sensor_types = get_sensor_types()
        return jsonify({
            "status": "success",
            "count": len(sensor_types),
            "data": sensor_types,
        }), 200
    except Exception as exc:
        app.logger.error("list_sensors error: %s", exc)
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve sensor list.",
        }), 500
    
# 2b Get Latest Reading for a Sensor Type
VALID_SENSORS = {"temperature", "humidity", "pressure"}

@app.route(f"{API_PREFIX}/sensors/<sensor_type>/latest")
def latest_reading(sensor_type: str):
    """
    GET /api/v1/ sensors /{ sensor_type }/ latest
    Returns the most recent reading from Kafka for the given sensor type .
    Path parameters :
    sensor_type ( str): one of temperature , humidity , pressure
    Returns :
    200 + reading if found
    404 if sensor_type is unknown
    """
    if sensor_type not in VALID_SENSORS:
        return jsonify({
            "status": "error",
            "message": (f"Unknown sensor type '{sensor_type}'. "
                        f"Valid types: {sorted(VALID_SENSORS)}")
        }), 404
    try:
        reading = get_latest_readings(sensor_type, n=1)
        if not reading:
            return jsonify({
                "status": "error",
                "message": f"No readings available for sensor '{sensor_type}'."
            }), 404

        return jsonify({
            "status": "success",
            "data": reading[0],
        }), 200
    except Exception as exc:
        app.logger.error("latest_reading error: %s", exc)
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve latest reading.",
        }), 500

# 3 Statistics from the Parquet Data Lake 
@app.route(f"{API_PREFIX}/sensors/<sensor_type>/stats")
def sensor_stats(sensor_type: str):
    """
    GET /api/v1/ sensors /{ sensor_type }/ stats
    Returns daily statistics for a sensor type .
    Query parameters ( optional ):
    days (int ): number of recent days ( default : 7)
    Example :
    GET /api/v1/ sensors / temperature / stats ? days =3
    """
    if sensor_type not in VALID_SENSORS:
        return jsonify({
            "status": "error",
            "message": f"Unknown sensor type '{sensor_type}'.",
        }), 404
    try:
        days = int(request.args.get("days", 7))
        if days < 1 or days > 90:
            raise ValueError("days must be between 1 and 90")
    except (ValueError, TypeError) as exc:
        return jsonify({
            "status": "error",
            "message": f"Invalid 'days' parameter: {exc}",
        }), 400

    try:
        stats = get_statistics(sensor_type, days=days)
        return jsonify({
            "status": "success",
            "sensor_type": sensor_type,
            "days": days,
            "count": len(stats),
            "data": stats,
        }), 200
    except Exception as exc:
        app.logger.error("sensor_stats error: %s", exc)
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve statistics.",
        }), 500

# 4 POST: Write a Reading to Kafka
import json
REQUIRED_FIELDS = {"sensor", "value"}

@app.route(f"{API_PREFIX}/readings", methods=["POST"])
def create_reading():
    """
    POST /api/v1/readings
    Publish a new sensor reading to the Kafka topic.
    Request body ( JSON ):
    {
        "sensor": "temperature",
        "value": 28.5,
        "unit": "C" ( optional )
    }
    Returns :
    201 + published message metadata on success
    400 if body is malformed or missing required fields
    """
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({
            "status": "error",
            "message": "Request body must be valid JSON. Set Content-Type: application/json."
        }), 400

    missing = REQUIRED_FIELDS - set(body.keys())
    if missing:
        return jsonify({
            "status": "error",
            "message": f"Missing required fields: {missing}"
        }), 400

    sensor = body["sensor"]
    if sensor not in VALID_SENSORS:
        return jsonify({
            "status": "error",
            "message": f"Invalid sensor type '{sensor}'."
        }), 422

    try:
        value = float(body["value"])
    except (ValueError, TypeError):
        return jsonify({
            "status": "error",
            "message": "'value' must be a number."
        }), 422

    reading = {
        "sensor": sensor,
        "value": value,
        "unit": body.get("unit", ""),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "source": "api-v1",
        "anomaly": False,
    }

    try:
        metadata = publish_reading(reading)
        return jsonify({
            "status": "success",
            "message": "Reading published to Kafka.",
            "data": {
                "reading": reading,
                "partition": metadata["partition"],
                "offset": metadata["offset"],
            }
        }), 201
    except Exception as exc:
        app.logger.error("create_reading error: %s", exc)
        return jsonify({
            "status": "error",
            "message": "Failed to publish reading."
        }), 500

# 5 Error Handlers 
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "code": 404,
        "message": "The requested resource was not found.",
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "status": "error",
        "code": 405,
        "message": "HTTP method not allowed for this endpoint.",
    }), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "code": 500,
        "message": "An internal server error occurred.",
    }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
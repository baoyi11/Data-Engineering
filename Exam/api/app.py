from flask import Flask, jsonify, request
import json
import time
import os
import pandas as pd
from kafka import KafkaProducer

app = Flask(__name__)

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )
except Exception as e:
    app.logger.error(f"Failed to connect to Kafka: {e}")
    producer = None

SENSORS = ["temperature", "humidity", "pressure"]
LAKE_PATH = "./outputs/datalake"

@app.errorhandler(400)
def bad_request(e): return jsonify(error=str(e)), 400
@app.errorhandler(404)
def not_found(e): return jsonify(error="Resource not found"), 404
@app.errorhandler(405)
def method_not_allowed(e): return jsonify(error="Method not allowed"), 405
@app.errorhandler(422)
def unprocessable(e): return jsonify(error=str(e)), 422
@app.errorhandler(500)
def server_error(e): return jsonify(error="Internal server error"), 500

@app.route('/api/v1/health', methods=['GET'])
def health(): return jsonify(status="UP"), 200

@app.route('/api/v1/sensors', methods=['GET'])
def list_sensors(): return jsonify(sensors=SENSORS), 200

@app.route('/api/v1/sensors/<sensor_type>/latest', methods=['GET'])
def get_latest(sensor_type):
    if sensor_type not in SENSORS: return jsonify(error="Invalid sensor type"), 404
    try:
        df = pd.read_parquet(f"{LAKE_PATH}/curated/domain=iot/sensor={sensor_type}/")
        latest = df.sort_values(by="event_time", ascending=False).iloc[0].to_dict()
        latest['event_time'] = str(latest['event_time'])
        return jsonify(latest), 200
    except Exception:
        return jsonify(error="No data found for this sensor"), 404

@app.route('/api/v1/sensors/<sensor_type>/stats', methods=['GET'])
def get_stats(sensor_type):
    if sensor_type not in SENSORS: return jsonify(error="Invalid sensor type"), 404
    days = request.args.get('days', default=1, type=int)
    if days < 1 or days > 90: return jsonify(error="days must be between 1 and 90"), 400
    try:
        df = pd.read_parquet(f"{LAKE_PATH}/consumption/use_case=sensor_averages/sensor={sensor_type}/")
        return jsonify(df.head(days).to_dict(orient="records")), 200
    except Exception:
        return jsonify(error="Stats not available yet"), 404

@app.route('/api/v1/anomalies', methods=['GET'])
def get_anomalies():
    sensor_type = request.args.get('sensor')
    limit = request.args.get('limit', default=10, type=int)
    if sensor_type and sensor_type not in SENSORS: return jsonify(error="Invalid sensor type"), 400
    try:
        path = f"{LAKE_PATH}/curated/domain=iot/"
        if sensor_type: path += f"sensor={sensor_type}/"
        df = pd.read_parquet(path)
        anomalies = df[df['is_anomaly'] == True].head(limit)
        anomalies['event_time'] = anomalies['event_time'].astype(str)
        return jsonify(anomalies.to_dict(orient="records")), 200
    except Exception:
        return jsonify(error="Data not available"), 404

@app.route('/api/v1/readings', methods=['POST'])
def post_reading():
    data = request.json
    if not data or 'sensor' not in data or 'value' not in data:
        return jsonify(error="Malformed JSON. 'sensor' and 'value' required"), 400
    sensor = data['sensor']
    if sensor not in SENSORS:
        return jsonify(error=f"Invalid sensor. Must be one of {SENSORS}"), 422
    try:
        val = float(data['value'])
    except ValueError:
        return jsonify(error="Value must be numeric"), 422

    payload = {"sensor": sensor, "value": val, "unit": data.get("unit", ""), "timestamp": int(time.time() * 1000), "source": "api", "anomaly": False}
    if producer:
        producer.send('sensor-events', key=sensor, value=payload)
        producer.flush()
        return jsonify(message="Published successfully", payload=payload), 201
    return jsonify(error="Kafka unavailable"), 500

if __name__ == '__main__':
    app.run(port=5000)
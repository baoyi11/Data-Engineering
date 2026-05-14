# 1a Imports and Configuration
from kafka import KafkaProducer
import json
import time
import random

# 1b Instantiate the Producer with Reliability Settings
# Configure the Kafka producer with appropriate settings for reliability and performance
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9094', 'localhost:9096'],
    acks='all',
    retries=5,
    max_in_flight_requests_per_connection=1,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    linger_ms=10,
    batch_size=32768,
)

# 1c The Sending Loop
# Simulate sensor readings for temperature, humidity, and pressure
SENSOR_TYPES = ['temperature', 'humidity', 'pressure']
TOPIC = 'sensor-events'

def send_reading(sensor_type, value):
    record = {
        'sensor': sensor_type,
        'value': value,
        'unit': {'temperature': 'C', 'humidity': '%', 'pressure': 'hPa'}[sensor_type],
        'timestamp': int(time.time() * 1000),
    }
    future = producer.send(
        topic=TOPIC,
        key=sensor_type,
        value=record,
    )
    return future

try:
    for i in range(50):
        sensor = random.choice(SENSOR_TYPES)
        value = round(random.uniform(10, 40), 2)
        future = send_reading(sensor, value)

        # Block until broker confirms receipt
        metadata = future.get(timeout=10)
        print(f"Sent to partition {metadata.partition}, offset {metadata.offset}")
        time.sleep(0.2)
finally:
    producer.flush() # send any remaining buffered records
    producer.close()
    print("Producer closed.")
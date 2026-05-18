# sensor_api/kafka_utils.py
from kafka import KafkaProducer, KafkaConsumer
import json

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sensor-events' 

def publish_reading(reading):
    """Publish a new reading to Kafka and return metadata."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    future = producer.send(TOPIC_NAME, reading)
    result = future.get(timeout=10)
    return {"partition": result.partition, "offset": result.offset}

def get_latest_readings(sensor_type, n=1):
    """Consume recent messages and return the latest 'n' readings for a sensor type."""
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8', errors='ignore')),
        consumer_timeout_ms=5000  
    )
    
    records = []
    for message in consumer:
        val = message.value
        if isinstance(val, dict) and val.get("sensor") == sensor_type:
            records.append(val)
            
    records.sort(key=lambda r: r.get("timestamp") or 0, reverse=True)
    
    return records[:n]

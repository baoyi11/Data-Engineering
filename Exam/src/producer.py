import json
import time
import random
import argparse
from kafka import KafkaProducer

def get_sensor_data(sensor_type, is_anomaly):
    ranges = {
        "temperature": (15, 45, "C"),
        "humidity": (30, 95, "%"),
        "pressure": (980, 1040, "hPa")
    }
    min_val, max_val, unit = ranges[sensor_type]
    
    if is_anomaly:
        value = random.uniform(max_val + 1, max_val + 20) if random.choice([True, False]) else random.uniform(min_val - 20, min_val - 1)
    else:
        value = random.uniform(min_val, max_val)
        
    return round(value, 2), unit

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--source", type=str, default="site-A-rack-12")
    args = parser.parse_args()
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        acks='all',
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
        batch_size=16384,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    sensors = ["temperature", "humidity", "pressure"]

    for i in range(args.count):
        sensor = random.choice(sensors)
        is_anomaly = random.random() < 0.10 
        value, unit = get_sensor_data(sensor, is_anomaly)
        payload = {
            "sensor": sensor,
            "value": value,
            "unit": unit,
            "timestamp": int(time.time() * 1000),
            "source": args.source,
            "anomaly": is_anomaly
        }
        producer.send('sensor-events', key=sensor, value=payload)
        print(f"Sent: {payload}")
        time.sleep(1.0 / args.rate)

    producer.flush()

if __name__ == "__main__":
    main()
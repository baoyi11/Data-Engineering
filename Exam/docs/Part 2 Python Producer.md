## Part 2: Python Producer

Implement a real Kafka producer in Python to simulate temperature, humidity, and pressure data sent by high-frequency IoT sensors. The producer ensures ordered delivery by using the sensor type as the partition key. It must intentionally inject anomalous data in 10% of cases.

**`src/producer.py`**:

```python
import json
import time
import random
import argparse
from kafka import KafkaProducer

def get_sensor_data(sensor_type, is_anomaly):
    ranges = {
        "temperature": (15, 45, "C"),  # [cite: 211]
        "humidity": (30, 95, "%"),     # [cite: 216]
        "pressure": (980, 1040, "hPa") # [cite: 217]
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
```

![image-20260519083520761](D:\EFREI\Data_Engineering\LAB\Exam\Exam.assets\image-20260519083520761.png)

### Execution Result

```powershell
python src\producer.py --count 10 --rate 5
```

![image-20260519083749352](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519083749352.png)

Data format is correct: Each message strictly adheres to the agreed-upon schema and includes the fields sensor, value, unit, timestamp, source, and anomaly.

Comprehensive sensor types: The output successfully generated data for three types of sensors: humidity, temperature, and pressure.

Anomaly logic is functioning: Among these 10 data points, two (humidity at 105.78% and pressure at 1057.37 hPa, which clearly exceed normal physical ranges) had their `anomaly` field correctly marked as `True`. The anomaly rate reached 20%, meeting the experimental requirement that “at least 10% of messages must be outliers.”

```
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic sensor-events --property print.key=true --property print.partition=true --from-beginning --max-messages 15
```

![image-20260519084402273](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519084402273.png)

Key-based partitioning demonstrated, humidity is in partition 0, while temperature and pressure are in partition 2.
# 2a: Imports and Configuration
from kafka import KafkaConsumer, TopicPartition
import json

# 2b: Instantiate the Consumer
# Configure the Kafka consumer with appropriate settings for reliability and performance
consumer = KafkaConsumer(
    'sensor-events',
    bootstrap_servers=['localhost:9092', 'localhost:9094', 'localhost:9096'],
    group_id='sensor-analytics',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode('utf-8')),
    key_deserializer=lambda b: b.decode('utf-8') if b else None,
    max_poll_records=10,
    session_timeout_ms=45000,
    heartbeat_interval_ms=15000,
)

# check Partition Assignment
# Force a poll to trigger partition assignment
consumer.poll(timeout_ms=1000)
assigned = consumer.assignment()
print(f"Assigned partitions: {[f'P{p.partition}' for p in assigned]}")

# 2c: The Processing Loop with Manual Commit
# Start consuming messages from the 'sensor-events' topic and process them according to the business logic, then commit offsets manually after processing each batch of messages
print("Consumer started. Waiting for messages...")
try:
    for message in consumer:
        record = message.value
        key = message.key
        part = message.partition
        offset = message.offset

        # Business logic
        print(f"[P{part}|O{offset}] sensor={key} value={record['value']}{record['unit']}")

        if record['sensor'] == 'temperature' and record['value'] > 35:
            print(f"  >>> ALERT: high temperature {record['value']} C!")
        

        # Commit AFTER processing
        consumer.commit()
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
    print("Consumer closed.")


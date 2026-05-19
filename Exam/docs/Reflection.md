# Reflection Questions

### 1. Your pipeline crashes during processing, after writing to the raw zone but before writing to the curated zone. What is the impact on the data? Which checkpoint strategy prevents this issue?

**Impact:** If multiple sinks share the same checkpoint directory, a crash in the middle of a micro-batch could lead to an inconsistent state upon restart. Depending on when the offset was committed, you might either duplicate data in the raw zone (if the offset wasn't committed) or permanently lose data in the curated zone (if the offset was committed before the curated write finished).

**Strategy to prevent this:** The solution is to use **independent checkpoint directories** for each streaming sink (e.g., `/checkpoints/raw` and `/checkpoints/curated`). This allows Spark Structured Streaming's Write-Ahead Logs (WAL) to track the exact Kafka offsets processed for each specific sink independently. Upon restart, the pipeline will resume the curated sink exactly where it left off, ensuring at-least-once (or exactly-once) delivery without duplicating data in the raw zone.

### 2. You scale the producer up to 50,000 messages per second. In your opinion, what would be the first bottlenecks in your current architecture, and how would you fix them?

**Bottleneck 1: Kafka Partitions & Spark Parallelism.** A topic with only 3 partitions restricts Spark to a maximum of 3 concurrent consumer tasks, which cannot handle 50k msgs/sec.

Increase the number of Kafka partitions (e.g., to 50 or 100) and deploy a multi-node Spark cluster to allow massive parallel reading.

**Bottleneck 2: Python Producer Throughput.** The configuration `max_in_flight_requests_per_connection=1` heavily throttles the producer to guarantee strict ordering.

Increase this value (e.g., to 5) and enable producer idempotence (`enable.idempotence=true`), which maintains ordering and prevents duplicates while allowing much higher throughput.



### 3. Compare the advantages and drawbacks of using Kafka as the source of truth for historical data, versus a Parquet data lake. In which scenarios should each be preferred?

**Kafka:**

*Advantages:* Extremely low latency, highly decoupled, allows immediate replayability of events for real-time consumers.

*Drawbacks:* Storage is expensive. Sequential scanning makes historical analytical queries incredibly slow and inefficient, as there is no columnar indexing.

*Preferred Scenario:* Real-time operational alerting, event sourcing, and driving live microservices.

**Parquet Data Lake:**

*Advantages:* Cheap, highly scalable storage. Columnar formatting, Snappy compression, and partition pruning make it exceptionally fast for querying massive historical datasets.

*Drawbacks:* Higher latency (batch/micro-batch writes); not designed for real-time pub/sub or single-record lookups.

*Preferred Scenario:* Business intelligence (BI) reporting, machine learning model training, and historical trend analysis spanning months or years.

### 4. A sensor breaks and emits aberrant values for 2 hours. How does your architecture detect this case? How would you isolate these data points without deleting them?

The Spark pipeline evaluates every incoming event against predefined physical thresholds using SQL expressions (e.g., `temperature > 35`). It independently flags aberrant readings by setting the `is_anomaly` boolean column to `true`, ignoring the producer's self-reported anomaly flag to ensure central authority.

The aberrant data points are preserved and written to the Curated and Consumption zones with the `is_anomaly` flag attached. To isolate them, downstream analytical queries and REST API endpoints simply apply a filter (`WHERE is_anomaly = false`) to serve clean data to business users. Meanwhile, predictive maintenance teams can specifically query `WHERE is_anomaly = true` to investigate the broken sensor, ensuring no historical raw data is ever deleted.

### 5. You must add a new sensor type `co2`. Which parts of your pipeline must be modified? Give a precise list of files and changes.

1. **`src/producer.py`**:

   Add `"co2"` to the `SENSORS` list.

   Update the `get_sensor_data()` function to include plausible physical generation ranges and units for CO2 (e.g., `(400, 2000, "ppm")`).

2. **`src/spark_pipeline.py`**:

   Update the anomaly detection `expr()` logic to include threshold rules for the new sensor (e.g., `OR (sensor = 'co2' AND value > 1000)`). *(Note: The dynamic `partitionBy("sensor")` configuration will automatically handle creating the new folders in the data lake, requiring no changes to the write paths).*

3. **`api/app.py`**:

   Add `"co2"` to the `SENSORS` global list. This ensures incoming `POST` requests for CO2 pass the strict `422 Unprocessable Entity` input validation.
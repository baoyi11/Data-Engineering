# Lab 5

**Open the LAB5 folder, which contains Lab5_report.pdf,  Comprehensive Synthesis Exercises.pdf, app.py, kafka_utils.py, lake_utils.py，README.md and docker-compose.yml. It includes all the step-by-step instructions, screenshots of the results, and reflections.**



### 1. RESTful API Backend Development

Using Python and the Flask framework, we built a sensor data REST API that connects the backend data lake to the message broker.

**Core API Development**:

`GET /api/v1/health`: Provides a standard entry point for service health checks.

`GET /api/v1/sensors`: Reads and returns all known sensor types from the Curated layer of the data lake.

`GET /api/v1/sensors/<sensor_type>/latest`: Retrieves the latest data for a specified sensor in real-time from Kafka.

`GET /api/v1/sensors/<sensor_type>/stats`: Filters by day using query parameters and returns aggregated statistics from the data lake.

`POST /api/v1/readings`: Receives a JSON payload, performs field validation, and publishes new sensor readings to a Kafka topic.

A global HTTP error handler has been registered to ensure that 404 (Resource Not Found), 405 (Method Not Allowed), and 500 (Internal Server Error) all return JSON responses in a consistent format.

### 2. Comprehensive Synthesis Exercises
**Message Broker (Kafka)**: Calculated the expected order volume and variance under load balancing conditions, and analyzed data skew and hot partition issues caused by a single large corporate client. Additionally, derived system reliability and data loss probability under different replication mechanisms.

**Distributed Databases**: Based on strong consistency requirements, the required quorum for read and write operations was calculated, and the trade-offs in availability under network partitioning were analyzed in conjunction with the CAP theorem.

**Stream Processing (Spark Streaming)**: Amdahl’s Law was applied to analyze the actual benefits of increasing parallelism, pointing out that blindly adding hardware resources may not be cost-effective. Additionally, the memory footprint of sliding windows was analyzed, along with the cost thresholds between stream processing and batch processing.

**Data Lake Storage (S3 & Parquet)**: The column pruning and compression efficiencies of CSV and Parquet formats were compared. Calculations showed that introducing high-cardinality partition keys leads to the “small-file problem,” causing the system to spend far more time on metadata overhead than on actual data transfer.

**End-to-End System Evaluation**: By integrating Kafka, Spark, and S3, we calculated the system’s overall maximum throughput bottleneck, end-to-end latency distribution, and overall data loss rate.





## Key Takeaways

In the Lab 5 experiment, I realized that standard HTTP status codes form the foundation for the coordinated operation of the entire network infrastructure (such as caching mechanisms, load balancers, and client tools). Forcing all responses to return a 200 status code while placing actual errors in the response body disrupts the routing and caching logic of the standard HTTP protocol. Additionally, a deep understanding of the idempotency of operations is of great practical significance for building fault-tolerant systems.

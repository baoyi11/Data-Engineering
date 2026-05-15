# Kafka & Spark Data Engineering Labs Overview

This repository contains a comprehensive series of data engineering labs and theoretical exercises focusing on Apache Kafka, Apache Spark Structured Streaming, Data Lake design, and RESTful API development. The modules progress from basic cluster provisioning and fault tolerance to robust Python client development, real-time ETL pipelines, data lake architecture, and finally, backend API integration.

---

## 📂 Lab 1: Kafka Fundamentals, Cluster Setup & Fault Tolerance

This module focuses on the foundational setup of a Kafka cluster, command-line operations, and underlying fault-tolerance mechanisms.

**Included Files:**

* **`BaoyiZhou_Lab1Report.pdf` (Lab Report):** Documents the process of provisioning a Kafka cluster using Docker Compose, featuring 3 KRaft-mode Brokers and a Kafka UI. It demonstrates using CLI tools to create topics with replication, produce/consume messages, and simulate Broker failures to verify automatic Leader re-election and data synchronization.
* **`BaoyiZhou_Exercises.pdf` (Theoretical Exercises):** Contains mathematical probability and system design problems covering Replication Quorums, Hash Partitioning load balancing, and throughput calculations using Little's Law.
* **`docker-compose.yml`:** The Docker configuration file required to spin up the Lab 1 environment.

---

## 📂 Lab 2: Python Clients, Reliability & Consumer Group Scaling

This module advances to application-level development, using Python (`kafka-python-ng`) to build producers and consumers while testing delivery semantics and dynamic scaling.

**Included Files:**

* **`BaoyiZhou_20252194_LAB2-Report.pdf` & `README.md`:** * Details the creation of a robust producer configured for high durability (`acks='all'`, `retries=5`).
* Explains a consumer script configured for manual offset management (`enable_auto_commit=False`), guaranteeing "at-least-once" delivery.
* Demonstrates consumer group scaling (observing Kafka's Rebalance mechanism distributing partitions) and fault injection by crashing a consumer mid-processing to trigger uncommitted message replays.


* **`BaoyiZhou_20252194_Exercises2.pdf` (Theoretical Exercises):** Covers calculations for residual loss probabilities under different acknowledgment settings, latency impacts of network retries, and consumer lag accumulated during traffic spikes.
* **`producer.py` & `consumer.py`:** Python source code simulating the generation and consumption of IoT sensor data (temperature, humidity, pressure).
* **`docker-compose.yml`:** The Docker configuration for the Lab 2 environment.

---

## 📂 Lab 3: Spark Structured Streaming & ETL Pipeline

This module integrates Spark with Kafka to build an end-to-end, real-time streaming ETL pipeline capable of handling data transformations, windowing, and persistent storage.

**Included Files:**

* **`BaoyiZhou_20252194_Lab3-report.pdf` & `README.md`:**
* **Ingestion & Parsing:** Connects PySpark to the Kafka cluster to read raw binary JSON payloads and parse them into structured formats using explicit schemas.
* **Business Logic:** Filters out null records and flags sensor anomalies.
* **Windowed Aggregation:** Groups the data stream into 5-minute time windows based on event time, applying a 2-minute watermark to safely drop late-arriving data and clear state memory.
* **Storage:** Outputs the aggregated results continuously into Parquet files while managing state recovery via checkpoint directories.


* **`Baoyi_Zhou_20252194_Exercises3.pdf` (Theoretical Exercises):** * Evaluates Spark Parallelism and Amdahl's Law, highlighting the Out-of-Memory (OOM) risks of improperly coalescing partitions.
* Contrasts the memory footprint of sliding versus tumbling windows and calculates event drop probabilities using exponential distributions for event lateness.
* Compares the cost models of batch versus streaming architectures to find the most cost-effective solution for specific latency requirements.


* **`etl_pipeline.py` & `producer.py`:** The PySpark ETL script and the Python producer script used to feed the pipeline.
* **`docker-compose.yml`:** The Docker configuration for the Lab 3 environment.

---

## 📂 Lab 4: Designing and Managing Data Lakes

This module introduces the Medallion Architecture (Bronze, Silver, Gold zones) to build and optimize a local data lake using PySpark and Kafka.

**Included Files:**

* **`BaoyiZhou_20252194_Lab4-report.pdf` & `README.md`:**
* **Raw Zone (Bronze):** Ingests raw streaming events from Kafka and saves them immutably as JSON files, partitioned by ingestion time.
* **Curated Zone (Silver):** Parses the JSON data, applies data quality filters, and writes the output as Snappy-compressed Parquet files, partitioned by event time.
* **Consumption Zone (Gold):** Uses a batch Spark job to read the curated data and aggregate it into daily sensor metrics.
* **Querying & Partition Pruning:** Registers the curated data as a Spark SQL temporary view and provides a benchmark proving a 4.3x speedup when utilizing partition pruning.


* **`BaoyiZhou_20252194_Exercises4.pdf` (Theoretical Exercises):**
* Calculates the performance speed-up of switching from row-oriented CSV to columnar Parquet formats, and evaluates the benefits of column pruning and predicate pushdown.
* Explores partition design and calculates the optimal average file size for S3 to mitigate the "small-file problem".
* Explains the "high-cardinality trap" and justifies why adding highly unique columns to a partition scheme causes performance collapse due to metadata overhead.


* **`datalake_pipeline.py` & `producer.py`:** Python scripts for generating IoT sensor data and running the multi-zone PySpark data lake ETL pipeline.
* **`docker-compose.yml`:** Docker configuration for provisioning the Kafka cluster.

---

## 📂 Lab 5: APIs & Web Services and Comprehensive System Evaluation

This final module focuses on building a RESTful API backend to connect the data lake and message broker, alongside a comprehensive end-to-end evaluation of the entire data engineering system.

**Included Files:**

* **`BaoyiZhou_20252194_Lab5-report.pdf` & `README.md`:**
* **RESTful API Backend Development:** Built a sensor data REST API using Python and the Flask framework. It features standard endpoints for service health checks (`GET /api/v1/health`), retrieving all known sensor types from the Curated layer of the data lake (`GET /api/v1/sensors`), fetching real-time data for a specified sensor from Kafka (`GET /api/v1/sensors/<sensor_type>/latest`), and aggregating daily statistics from the data lake (`GET /api/v1/sensors/<sensor_type>/stats`).
* **Data Ingestion Endpoint:** Includes a `POST /api/v1/readings` endpoint to receive JSON payloads, perform field validation, and publish new sensor readings to a Kafka topic.
* **Error Handling:** Implements a global HTTP error handler to ensure that 404, 405, and 500 errors all return JSON responses in a consistent format.
* **Key Takeaways:** Highlights that standard HTTP status codes form the foundation for coordinated operation of network infrastructure, and demonstrates that a deep understanding of idempotency is crucial for building fault-tolerant systems.


* **`BaoyiZhou_20252194_ComprehensiveSynthesisExercises.pdf` (Theoretical Exercises):**
* **Message Broker (Kafka):** Calculated expected order volume and variance under load balancing conditions, and analyzed data skew, hot partitioning, and the CAP theorem.
* **Stream Processing (Spark Streaming):** Applied Amdahl’s Law to analyze the actual benefits of increasing parallelism, analyzed the memory footprint of sliding windows, and established cost thresholds between stream and batch processing.
* **Data Lake Storage (S3 & Parquet):** Compared column pruning and compression efficiencies of CSV and Parquet formats, showing that high-cardinality partition keys lead to the "small-file problem".
* **End-to-End System Evaluation:** Calculated the overall maximum throughput bottleneck, end-to-end latency distribution, and overall data loss rate by integrating Kafka, Spark, and S3.


* **`app.py`, `kafka_utils.py`, `lake_utils.py`:** Python source code handling the Flask application logic and backend connections.
* **`docker-compose.yml`:** Docker configuration for provisioning the Kafka cluster.

---

### 🛠️ Prerequisites & Setup

To successfully run the code and clusters across these labs, your local environment requires:

* **Docker & Docker Compose:** To provision the Kafka Brokers and Kafka UI.
* **Python 3.x:** To execute the producer, consumer, PySpark pipelines, and Flask API scripts.
* **Python Dependencies:** Install the required libraries via pip:
```bash
pip install kafka-python-ng pyspark==3.5.3 flask

```
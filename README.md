# Data Engineering Labs Overview

## 📂 Lab 1: Kafka Fundamentals, Cluster Setup & Fault Tolerance

This module focuses on the foundational setup of a Kafka cluster, command-line operations, and underlying fault-tolerance mechanisms.

**Included Files:**

* 
**`BaoyiZhou_Lab1Report.pdf` (Lab Report):** Documents the process of provisioning a Kafka cluster using Docker Compose, featuring 3 KRaft-mode Brokers and a Kafka UI. It demonstrates using CLI tools to create topics with replication, produce/consume messages, and simulate Broker failures to verify automatic Leader re-election and data synchronization.


* 
**`BaoyiZhou_Exercises.pdf` (Theoretical Exercises):** Contains mathematical probability and system design problems covering Replication Quorums, Hash Partitioning load balancing, and throughput calculations using Little's Law.


* 
**`docker-compose.yml`:** The Docker configuration file required to spin up the Lab 1 environment.



---

## 📂 Lab 2: Python Clients, Reliability & Consumer Group Scaling

This module advances to application-level development, using Python (`kafka-python-ng`) to build producers and consumers while testing delivery semantics and dynamic scaling.

**Included Files:**

* 
**`BaoyiZhou_20252194_LAB2-Report.pdf` & `README.md`:** * Details the creation of a robust producer configured for high durability (`acks='all'`, `retries=5`).


* Explains a consumer script configured for manual offset management (`enable_auto_commit=False`), guaranteeing "at-least-once" delivery.


* Demonstrates consumer group scaling (observing Kafka's Rebalance mechanism distributing partitions) and fault injection by crashing a consumer mid-processing to trigger uncommitted message replays.




* 
**`BaoyiZhou_20252194_Exercises2.pdf` (Theoretical Exercises):** Covers calculations for residual loss probabilities under different acknowledgment settings, latency impacts of network retries, and consumer lag accumulated during traffic spikes.


* 
**`producer.py` & `consumer.py`:** Python source code simulating the generation and consumption of IoT sensor data (temperature, humidity, pressure).


* **`docker-compose.yml`:** The Docker configuration for the Lab 2 environment.

---

## 📂 Lab 3: Spark Structured Streaming & ETL Pipeline

This final module integrates Spark with Kafka to build an end-to-end, real-time streaming ETL pipeline capable of handling data transformations, windowing, and persistent storage.

**Included Files:**

* **`BaoyiZhou_20252194_Lab3-report.pdf` & `README.md`:**
* 
**Ingestion & Parsing:** Connects PySpark to the Kafka cluster to read raw binary JSON payloads and parse them into structured formats using explicit schemas.


* 
**Business Logic:** Filters out null records and flags sensor anomalies (e.g., temperatures $> 35.0^\circ\text{C}$ or humidity $> 90.0\%$).


* 
**Windowed Aggregation:** Groups the data stream into 5-minute time windows based on event time, applying a 2-minute watermark to safely drop late-arriving data and clear state memory.


* 
**Storage:** Outputs the aggregated results continuously into Parquet files while managing state recovery via checkpoint directories.


* 
**`Baoyi_Zhou_20252194_Exercises3.pdf` (Theoretical Exercises):** * Evaluates Spark Parallelism and Amdahl's Law, highlighting the Out-of-Memory (OOM) risks of improperly coalescing partitions.


* Contrasts the memory footprint of sliding versus tumbling windows and calculates event drop probabilities using exponential distributions for event lateness.


* Compares the cost models of batch versus streaming architectures to find the most cost-effective solution for specific latency requirements.


* 
**`etl_pipeline.py` & `producer.py`:** The PySpark ETL script and the Python producer script used to feed the pipeline.


* **`docker-compose.yml`:** The Docker configuration for the Lab 3 environment.

---

### 🛠️ Prerequisites & Setup

To successfully run the code and clusters across these labs, your local environment requires:

* 
**Docker & Docker Compose:** To provision the Kafka Brokers and Kafka UI.


* 
**Python 3.x:** To execute the producer, consumer, and Spark pipeline scripts.


* **Python Dependencies:** Install the required libraries via pip:
```
pip install kafka-python-ng pyspark==3.5.3

```

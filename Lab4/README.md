# Lab 4: Designing and Managing Data Lakes

**Open the LAB4 folder, which contains Lab4_report.pdf, Exercises4.pdf, datalake_pipeline.py, producer.py, datalake，README.md and docker-compose.yml. It includes all the step-by-step instructions, screenshots of the results, and reflections.**

### 1. Theoretical Exercises (`BaoyiZhou_20252194_Exercises4.pdf`)

This document contains mathematical calculations and theoretical justifications regarding data lake optimization techniques.  

- **Exercise 1: Column Pruning & Predicate Pushdown**   
  - Calculates the performance speed-up of switching from row-oriented CSV to columnar Parquet formats.  
  - Demonstrates the byte-read reduction achieved through column pruning and the impact of data sorting on predicate pushdown efficiency.  
- **Exercise 2: Partition Design & The Small-File Problem**   
  - Explores the mathematical impact of directory structures and partition pruning on query times.  
  - Calculates the optimal average file size for S3 to mitigate the "small-file problem".  
  - Explains the "high-cardinality trap" and justifies why adding highly unique columns (like `user_id`) to a partition scheme causes performance collapse due to metadata overhead.  

### 2. Practical Lab Report (`BaoyiZhou_20252194_Lab4-report.pdf`)

This document outlines the step-by-step execution of building a local data lake pipeline using PySpark and Kafka.  

- **Step 0: Setup:** Initializes the physical directory hierarchy (`raw`, `curated`, `consumption`) and verifies the Kafka cluster and `sensor-events` topic.  
- **Step 1: Raw Zone (Bronze):** Ingests raw streaming events from Kafka and saves them immutably as JSON files, partitioned by ingestion time.  
- **Step 2: Curated Zone (Silver):** Parses the JSON data, applies data quality filters, and writes the output as Snappy-compressed Parquet files, partitioned by event time (`sensor_type`, `year`, `month`, `day`).  
- **Step 3: Consumption Zone (Gold):** Uses a batch Spark job to read the curated data and aggregate it into daily sensor metrics (averages, min, max, anomaly counts).  
- **Step 4: Querying & Partition Pruning:** Registers the curated data as a Spark SQL temporary view to perform anomaly analysis. Includes a benchmark proving a **4.3x speedup** when utilizing partition pruning compared to a full table scan.  
- **Reflection Questions:** Provides detailed answers to core data engineering concepts, including partitioning choices, the small file problem, schema evolution in Parquet, and checkpoint management.  

## Technologies & Tools Used

- **Apache Spark (PySpark):** Core processing engine for both streaming and batch data pipelines.
- **Apache Kafka:** Distributed event streaming platform used as the data source.
- **Docker:** Containerization for the Kafka cluster.
- **Storage Formats:** JSON (Raw Zone) and Apache Parquet with Snappy compression (Curated/Consumption Zones).
- **Environment:** PowerShell, Python.
# README

### **Open the LAB3 folder, which contains Lab3_report.pdf, Exercises.pdf, etl-pipeline, producer.py, spark-etl and docker-compose.yml. It includes all the step-by-step instructions, screenshots of the results, and reflections.**



## Lab3-report

**Environment Setup:** The process begins by starting a local Kafka cluster via Docker, configuring the PySpark Python environment, and creating local directories to store the final Parquet output and the streaming checkpoint state.  

**Data Ingestion & Parsing:** Spark is connected to the Kafka cluster to continuously read raw binary data from the `sensor-events` topic. This binary payload is then cast to strings and parsed into a structured JSON format using an explicitly defined schema.  

**Data Cleaning & Business Logic:** The stream is filtered to remove invalid records containing null values. It also applies business rules to flag anomalies, specifically when the temperature exceeds 35.0 or the humidity exceeds 90.0.  

**Windowed Aggregation:** The pipeline groups incoming data into 5-minute time windows based on the event time to calculate the average sensor values. A 2-minute watermark is applied to safely handle late-arriving data and allow Spark to clear old state memory.  

**Persistent Storage:** The aggregated data is continuously written to a Parquet output directory, triggered every 10 seconds. A checkpoint directory is utilized to track offsets and state, ensuring the pipeline can resume properly without reprocessing data if the job crashes.  



## Exercises

**Exercise 1 (Spark Parallelism & Amdahl's Law):** 

It highlights the dangers of improperly coalescing partitions. Reducing partitions from 4,000 to 80 increases the data per task to 6.25 GB, which exceeds the average 4 GB memory budget per core and risks Out-of-Memory (OOM) errors.  

Using Amdahl's Law, it calculates theoretical speed-ups, showing that a job with 90% parallelizable code is bottlenecked by a maximum theoretical speed-up of 10x, regardless of how many executors are added.  

**Exercise 2 (Windowing, Watermarks & Cost Models):**

**Window Arithmetic:** It contrasts the state memory required for overlapping sliding windows (8.4 MB) versus tumbling windows (2.4 MB), showing that tumbling windows are significantly more memory-efficient.  

**Event Lateness:** Assuming event lateness follows an exponential distribution, it calculates a 0.48% probability of dropping events with a 4-minute watermark. It then derives the optimal watermark threshold needed to achieve a stricter target drop rate ($10^{-4}$).  

**Cost Trade-offs:** By comparing the pricing models of running batch jobs per run versus continuous streaming per minute, it identifies the cost crossover point. It proves that for a strict latency requirement of 5 minutes, streaming is the more cost-effective option, saving $84 per hour compared to frequent batch processing.  
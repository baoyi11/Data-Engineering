## Part 4: Analytical Queries with Spark SQL

**`src/analytics.py`**:

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
os.environ['HADOOP_HOME'] = r'D:\EFREI\Data_Engineering\LAB\Lab3\hadoop'

def main():
    spark = SparkSession.builder.appName("DataLake_Analytics").getOrCreate()
    
    df = spark.read.parquet("./outputs/datalake/curated/domain=iot/")
    df.createOrReplaceTempView("curated_data")

    print("--- 1. Top 5 hours with highest anomalies ---")
    q1 = spark.sql("""
        SELECT hour(event_time) as h, count(*) as anomalies
        FROM curated_data WHERE is_anomaly = true
        GROUP BY h ORDER BY anomalies DESC LIMIT 5
    """)
    q1.show()
    q1.toPandas().to_csv('outputs/analytics/top_hours.csv', index=False)

    print("--- 2. Global stats per sensor ---")
    q2 = spark.sql("""
        SELECT sensor, avg(value) as mean_val, min(value) as min_val, max(value) as max_val, 
               stddev(value) as std_val, (sum(cast(is_anomaly as int))/count(*))*100 as anomaly_rate
        FROM curated_data GROUP BY sensor
    """)
    q2.show()
    q2.toPandas().to_csv('outputs/analytics/global_stats.csv', index=False)

    print("--- 3. Daily evolution for temperature ---")
    q3 = spark.sql("""
        SELECT date(event_time) as dt, avg(value) as daily_mean, sum(cast(is_anomaly as int)) as anomalies
        FROM curated_data WHERE sensor = 'temperature' GROUP BY dt ORDER BY dt
    """)
    q3.show()
    q3.toPandas().to_csv('outputs/analytics/daily_temp.csv', index=False)


    print("--- 4. Partition Pruning Demo ---")
    start_no_prune = time.time()
    spark.sql("SELECT count(*) FROM curated_data WHERE event_time >= '2026-05-19'").collect()
    time_no_prune = time.time() - start_no_prune
    
    start_prune = time.time()
    spark.sql("SELECT count(*) FROM curated_data WHERE year=2026 AND month=5 AND day=19 AND event_time >= '2026-05-19'").collect()
    time_prune = time.time() - start_prune
    
    speedup = time_no_prune / time_prune if time_prune > 0 else float('inf')
    print(f"Time without prune: {time_no_prune:.4f}s")
    print(f"Time with prune: {time_prune:.4f}s")
    print(f"Speedup Factor: {speedup:.2f}x")

if __name__ == "__main__":
    main()
```

### Execution Result

```
python src\analytics.py
```

![image-20260519093811093](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519093811093.png)

**Interpretation of Analytical Results**

Top 5 hours with highest anomalies

This table correctly identifies four outliers at the 8th hour (8:00 AM), confirming that Spark successfully read the `event_time` field from the Parquet file, extracted the hourly time dimension using the `hour()` function, and completed the global aggregation and descending sort.

Global stats per sensor

The mean, minimum, maximum, standard deviation, and anomaly rate were successfully calculated for the temperature, pressure, and humidity sensors. The application of complex aggregation functions was demonstrated.

Daily evolution for temperatur

The system correctly identified the daily average temperature (27.4125°C) and the total number of anomalies (0) for the date 2026-05-19, demonstrating its ability to perform time-series analysis on a specific partition (WHERE sensor = ‘temperature’).

Partition Pruning Demo

The query without filtering took 1.1847 seconds, while the query with partition-based filtering took 0.6406 seconds, resulting in a performance improvement of 1.85x. Although the current test dataset is very small (with a speedup ratio of 1.85x, primarily saving I/O overhead from directory scans), in a real production environment with terabytes of data and tens of thousands of partitions, this partition pruning mechanism can skip reading irrelevant data, boosting query speeds by hundreds or even thousands of times.
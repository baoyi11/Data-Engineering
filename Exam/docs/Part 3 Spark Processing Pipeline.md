## Part 3: Spark Processing Pipeline

Deploy a Spark Structured Streaming application to process events, filter out outliers, flag anomalies, and calculate 5-minute rolling window statistics. The data is stored in a three-tier data lake architecture: raw data (in raw JSON format), processed data (in Parquet format), and consumption data (aggregated metrics).

**`src/spark_pipeline.py`**:

```python
import os
os.environ['HADOOP_HOME'] = r'D:\EFREI\Data_Engineering\LAB\Lab3\hadoop'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, expr, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

def main():
    spark = SparkSession.builder \
        .appName("IoT_Sensor_Pipeline") \
        .config("spark.sql.streaming.checkpointLocation", "./outputs/checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

    schema = StructType([
        StructField("sensor", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("source", StringType(), True),
        StructField("anomaly", BooleanType(), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-events") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) \
        .withColumn("year", year("event_time")) \
        .withColumn("month", month("event_time")) \
        .withColumn("day", dayofmonth("event_time")) \
        .withColumn("hour", hour("event_time"))

    raw_query = parsed_df.writeStream \
        .format("json") \
        .partitionBy("year", "month", "day", "hour") \
        .option("path", "./outputs/datalake/raw/source=kafka/topic=sensor-events/") \
        .option("checkpointLocation", "./outputs/checkpoints/raw") \
        .start()

    validated_df = parsed_df.filter(col("value").isNotNull()) \
        .withColumn("is_anomaly", expr("""
            (sensor = 'temperature' AND value > 35) OR 
            (sensor = 'humidity' AND value > 90) OR 
            (sensor = 'pressure' AND (value < 990 OR value > 1030))
        """))

    curated_query = validated_df.writeStream \
        .format("parquet") \
        .partitionBy("sensor", "year", "month", "day") \
        .option("path", "./outputs/datalake/curated/domain=iot/") \
        .option("checkpointLocation", "./outputs/checkpoints/curated") \
        .start()

    agg_df = validated_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window(col("event_time"), "5 minutes"), col("sensor")) \
        .agg(
            expr("mean(value)").alias("avg_value"),
            expr("min(value)").alias("min_value"),
            expr("max(value)").alias("max_value"),
            expr("count(value)").alias("obs_count"),
            expr("sum(cast(is_anomaly as int))").alias("anomaly_count")
        )

    consumption_query = agg_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .partitionBy("sensor") \
        .option("path", "./outputs/datalake/consumption/use_case=sensor_averages/") \
        .option("checkpointLocation", "./outputs/checkpoints/consumption") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
```

![image-20260519091043286](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519091043286.png)

### Execution Result

```powershell
python src\spark_pipeline.py
```

![image-20260519091222917](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519091222917.png)

```
tree .\outputs\datalake /F
```

![image-20260519091422947](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519091422947.png)

```
tree .\outputs\checkpoints /F
```

![image-20260519091517707](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519091517707.png)

![image-20260519091615502](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519091615502.png)

Data Lake Verification: You should see three folders: `raw`, `curated`, and `consumption`. The `raw` folder contains JSON files partitioned by year/month/day/hour; the `curated` folder contains `snappy.parquet` files partitioned by sensor.

Checkpoint Verification: You can see folders such as /tmp/checkpoints/raw and /tmp/checkpoints/curated, which contain subfolders like commits, offsets, and sources.

Create a new file named `test_verify.py` to verify whether “parsing, anomaly detection, and window aggregation are working.”

```python
import os
os.environ['HADOOP_HOME'] = r'D:\EFREI\Data_Engineering\LAB\Lab3\hadoop'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Verify").getOrCreate()

curated_df = spark.read.parquet("./outputs/datalake/curated/domain=iot/")
curated_df.printSchema()
curated_df.show(5, truncate=False)

consumption_df = spark.read.parquet("./outputs/datalake/consumption/use_case=sensor_averages/")
consumption_df.printSchema()
consumption_df.show(5, truncate=False)
```

![image-20260519092209608](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519092209608.png)

Schema parsing was completely successful: The original JSON string was successfully parsed into structured fields.

Timestamp conversion was accurate: The original millisecond-level timestamp was successfully converted to Spark’s `event_time` (timestamp type), and the year, month, day, and hour were correctly extracted from it to serve as the basis for subsequent partitioning.

Business logic (anomaly detection) is active: The code successfully generated the `is_anomaly` field. Observing the data rows below, the temperature in the first row is 32.97°C, which does not exceed the 35°C threshold; the air pressure in the fifth row is 1025.45 hPa, which falls within the range of 990–1030. Therefore, their `is_anomaly` values are correctly calculated as `false`.

I chose to use separate checkpoint directories for each of the three sinks (raw, curated, consumption). This ensures strict fault isolation. If the pipeline crashes while writing to the curated zone, the curated stream will resume from its specific Kafka offset upon restart. Because the raw zone has its own independent offset tracker, it will not duplicate previously written raw data. This approach guarantees robust end-to-end at-least-once (or exactly-once with idempotent sinks) semantics without inter-sink interference.

For the windowed aggregation (Consumption zone), I used `OutputMode("append")` in combination with a 2-minute Watermark. This is the most suitable choice because Parquet files in a Data Lake are immutable. Using `Update` or `Complete` mode would require overwriting or modifying existing files, which standard Parquet sinks do not support efficiently in pure streaming. `Append` mode instructs Spark to hold the aggregated results in memory until the watermark passes (window closes + 2 minutes), and only then write the finalized, immutable result to the Parquet sink safely.
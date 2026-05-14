# 1a: mports and SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr, year, month, dayofmonth, hour, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType
)
import os

# Set up local paths relative to the current working directory
LAKE_ROOT = "./datalake"
RAW_PATH = f"{LAKE_ROOT}/raw/source=kafka/topic=sensor-events"
CURATED_PATH = f"{LAKE_ROOT}/curated/domain=iot"
CONSUME_PATH = f"{LAKE_ROOT}/consumption/use_case=sensor_averages"
CKPT_RAW = "./datalake-ckpt/raw"
CKPT_CUR = "./datalake-ckpt/curated"

KAFKA_BROKERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"

spark = SparkSession.builder \
    .appName("Data Lake Pipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1b: Define Schema and Read Raw Kafka Stream
SENSOR_SCHEMA = StructType([
    StructField("sensor", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), True),
    StructField("timestamp", LongType(), False),
    StructField("source", StringType(), True),
    StructField("anomaly", BooleanType(), True),
])

raw_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 1c: Write Raw Zone (JSON format, partitioned by date)
raw_for_lake = raw_kafka.select(
    col("value").cast("string").alias("raw_json"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("ingestion_ts"),
    year(col("timestamp")).alias("year"),
    month(col("timestamp")).alias("month"),
    dayofmonth(col("timestamp")).alias("day"),
    hour(col("timestamp")).alias("hour")
)

query_raw = raw_for_lake.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", RAW_PATH) \
    .option("checkpointLocation", CKPT_RAW) \
    .partitionBy("year", "month", "day", "hour") \
    .trigger(processingTime="30 seconds") \
    .start()

# 2a: Parse and Validate
parsed = raw_kafka.select(
    from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("d"),
    col("partition"),
    col("offset")
).select(
    col("d.sensor").alias("sensor_type"),
    col("d.value"),
    col("d.unit"),
    col("d.anomaly").alias("is_anomaly"),
    to_timestamp(expr("d.timestamp / 1000")).alias("event_time"),
    col("partition"),
    col("offset")
).filter(col("sensor_type").isNotNull()) \
 .filter(col("value").isNotNull()) \
 .filter(col("value").between(-100, 2000))

# 2b: Add Partitioning Columns from Event Time
curated = parsed \
    .withColumn("year", year(col("event_time"))) \
    .withColumn("month", month(col("event_time"))) \
    .withColumn("day", dayofmonth(col("event_time")))

# 2c & 2d: Write Curated Zone (Parquet) and Start
query_curated = curated.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", CURATED_PATH) \
    .option("checkpointLocation", CKPT_CUR) \
    .option("compression", "snappy") \
    .partitionBy("sensor_type", "year", "month", "day") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Data lake pipeline running. Ctrl+C to stop.")
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping all queries...")
    for q in spark.streams.active:
        q.stop()


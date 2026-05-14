# 1a. Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window, to_timestamp, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

import os
os.environ['HADOOP_HOME'] = r'D:\EFREI\Data_Engineering\LAB3\hadoop'

# 1b. Create a Spark session
spark = SparkSession.builder \
    .appName("SessionETLPipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Read the Raw Kafka Stream
KAFKA_BROKERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

raw_stream.printSchema()

# 3a. Define the Expected Schema
sensor_schema = StructType([
    StructField("sensor", StringType(), nullable=False),
    StructField("value", DoubleType(), nullable=False),
    StructField("unit", StringType(), nullable=True),
    StructField("timestamp", LongType(), nullable=False),
    StructField("source", StringType(), nullable=True),
])

# 3b. Cast and Parse
parsed_stream = raw_stream \
    .select(
        col("key").cast("string").alias("message_key"),
        from_json(col("value").cast("string"), sensor_schema).alias("data"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    ) \
    .select(
        col("message_key"),
        col("data.sensor"),
        col("data.value"),
        col("data.unit"),
        to_timestamp(expr("data.timestamp / 1000")).alias("event_time"),
        col("partition"),
        col("offset")
    )

# 4a. Filter out null records
clean_stream = parsed_stream \
    .filter(col("sensor").isNotNull()) \
    .filter(col("value").isNotNull())

# 4b. Flag anomalies
TEMP_THRESHOLD = 35.0
HUM_THRESHOLD = 90.0

flagged_stream = clean_stream.withColumn(
    "is_anomaly",
    (
        ((col("sensor") == "temperature") & (col("value") > TEMP_THRESHOLD)) |
        ((col("sensor") == "humidity") & (col("value") > HUM_THRESHOLD))
    )
)

# 5a. Add watermark
watermarked = flagged_stream \
    .withWatermark("event_time", "2 minutes")

# 5b. Group by 5-minute window
windowed_avg = watermarked \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor")
    ) \
    .agg(
        avg(col("value")).alias("avg_value")
    )

# 6a. Write the Aggregated Stream
OUTPUT_PATH = "./spark-etl/output"
CHECKPOINT_PATH = "./spark-etl/checkpoint"

query = windowed_avg \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()


# 6b. Write the Raw Flagged Stream
RAW_OUTPUT = "./spark-etl/raw"
RAW_CHECKPOINT = "./spark-etl/checkpoint-raw"

query_raw = flagged_stream \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", RAW_OUTPUT) \
    .option("checkpointLocation", RAW_CHECKPOINT) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming query started. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()

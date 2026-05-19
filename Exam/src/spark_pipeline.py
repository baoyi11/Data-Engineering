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
# sensor_api/lake_utils.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
os.environ['HADOOP_HOME'] = r"D:\EFREI\Data_Engineering\LAB\Lab3\hadoop"
DATALAKE_PATH = r"D:\EFREI\Data_Engineering\LAB\Lab4\datalake\curated\domain=iot"
def _get_spark():
    """Initialize SparkSession."""
    try:
        spark = SparkSession.builder \
            .appName("SensorAPI") \
            .master("local[*]") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"Failed to start Spark: {e}")
        return None
def get_sensor_types():
    """Read Parquet files to get all distinct sensor types."""
    try:
        spark = _get_spark()
        if not spark:
            return []
        df = spark.read.parquet(DATALAKE_PATH)
        types = [row[0] for row in df.select("sensor_type").distinct().collect()]
        return types
    except Exception as e:
        print(f"Error reading datalake for sensor types: {e}")
        return []

def get_statistics(sensor_type, days=7):
    """Mock statistics retrieval from Parquet."""
    try:
        spark = _get_spark()
        if not spark:
            return []
        
        df = spark.read.parquet(DATALAKE_PATH)
        stats_df = df.filter(col("sensor_type") == sensor_type).limit(days)
        
        return [row.asDict() for row in stats_df.collect()]
    except Exception as e:
        print(f"Error reading stats: {e}")
        return []
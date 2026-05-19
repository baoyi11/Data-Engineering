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
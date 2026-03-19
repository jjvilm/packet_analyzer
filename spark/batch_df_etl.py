import os
import shutil

from pyspark.sql import SparkSession


RAW_PATH = "/app/data/raw/"
OUTPUT_PATH = "/app/data/transformed/df_packets_by_protocol"


spark = SparkSession.builder \
    .appName("PacketBatchDF") \
    .getOrCreate()

# Read raw stream output
df = spark.read.parquet(RAW_PATH)

df.show(20, truncate=False)
df.write.mode("overwrite").csv(OUTPUT_PATH)

spark.stop()
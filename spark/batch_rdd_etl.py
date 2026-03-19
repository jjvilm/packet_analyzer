import os
import shutil
import json

from pyspark.sql import SparkSession
from utils.utils import ones_complement_checksum


RAW_PATH = "/app/data/raw/"
OUTPUT_PATH = "/app/data/transformed/rdd_packets_per_dst_ip"


def expected_checksum(packet_row):
    header_fields = {
        "packet_id": str(packet_row.packet_id),
        "version": packet_row.version,
        "data_length": packet_row.data_length,
        "protocol": packet_row.protocol,
        "src_ip": packet_row.src_ip,
        "dst_ip": packet_row.dst_ip,
    }
    header_str = json.dumps(header_fields, sort_keys=True)
    return ones_complement_checksum(header_str)

spark = SparkSession.builder \
    .appName("PacketBatchRDD") \
    .getOrCreate()

sc = spark.sparkContext

# Read raw parquet
df = spark.read.parquet(RAW_PATH)
rdd = df.rdd

#Filter out LoopBack address
no_loopback_rdd = rdd.filter(lambda x: x.src_ip != "127.0.0.1" and x.dst_ip != "127.0.0.1")

#Filter out incorrect checksums

verified_checksum_rdd = no_loopback_rdd.filter(
    lambda x: expected_checksum(x) == x.checksum
)

#Filter out packets that are on  the blacklist
try:
    with open("/app/data/blacklist.txt", 'r', encoding="utf-8") as file:
        blacklist_broadcast = sc.broadcast([line for line in file.read().splitlines() if line])
except FileNotFoundError:
    print("Error: The file blacklist.txt was not found.")
    blacklist_broadcast = sc.broadcast([])

safe_packets_rdd = verified_checksum_rdd.filter(
    lambda x: x.src_ip not in blacklist_broadcast.value and x.dst_ip not in blacklist_broadcast.value
)

packets_per_dst_ip = safe_packets_rdd \
    .map(lambda x: (x.dst_ip, 1)) \
    .reduceByKey(lambda left, right: left + right)


df = packets_per_dst_ip.toDF(["dst_ip", "total"])
df.show(5)
df.write.mode("overwrite").csv(OUTPUT_PATH)

spark.stop()
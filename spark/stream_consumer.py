import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_date
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


PACKET_SCHEMA = StructType([
    StructField("packet_id", StringType(), False),
    StructField("version", IntegerType(), False),
    StructField("ip_header_length", IntegerType(), False),
    StructField("data_length", IntegerType(), False),
    StructField("protocol", StringType(), False),
    StructField("checksum", IntegerType(), False),
    StructField("src_ip", StringType(), False),
    StructField("dst_ip", StringType(), False),
    StructField("src_port", IntegerType(), False),
    StructField("dest_port", IntegerType(), False),
    StructField("control_flags", StringType(), False),
    StructField("window_size", IntegerType(), False),
    StructField("data", StringType(), False),
    StructField("timestamp", LongType(), False),
])


def build_parser():
    parser = argparse.ArgumentParser(
        description="Consume all currently available Kafka packet events and persist them to Parquet."
    )
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="packets")
    parser.add_argument("--output-path", default="/app/data/raw")
    parser.add_argument("--checkpoint-path", default=None)
    parser.add_argument("--starting-offsets", default="earliest")
    return parser


def consume_available_packets(
    bootstrap_servers="kafka:9092",
    topic="packets",
    output_path="/app/data/raw",
    checkpoint_path=None,
    starting_offsets="earliest",
):
    checkpoint_location = checkpoint_path or f"{output_path.rstrip('/')}/_checkpoints"

    spark = SparkSession.builder \
        .appName("KafkaPacketIngestion") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_packets = kafka_df.select(
        from_json(col("value").cast("string"), PACKET_SCHEMA).alias("packet")
    ).select("packet.*")

    packets_with_date = parsed_packets.withColumn(
        "event_date",
        to_date(from_unixtime(col("timestamp")))
    )

    query = packets_with_date.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location) \
        .partitionBy("event_date") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    args = build_parser().parse_args()
    print("Starting bounded Kafka packet ingestion job...")
    consume_available_packets(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        output_path=args.output_path,
        checkpoint_path=args.checkpoint_path,
        starting_offsets=args.starting_offsets,
    )
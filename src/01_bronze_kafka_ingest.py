import argparse
from pathlib import Path
from pyspark.sql import SparkSession

def get_spark_session(format_type):
    if format_type == "cloudFiles":
        return SparkSession.builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip
        
        builder = (
            SparkSession.builder.appName("Clickstream_Kafka_Bronze_Ingestion")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        
        extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]
        return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

# Renamed parameter to spark_session to break the variable shadowing namespace collision
def run_kafka_bronze(spark_session, bootstrap_servers, topic, checkpoint_path, target_table, target_path=None):
    checkpoint_path = str(Path(checkpoint_path).resolve())
    if target_path:
        target_path = str(Path(target_path).resolve())

    print(f"Reading active stream from Kafka broker: {bootstrap_servers} on topic: {topic}")

    # Read the streaming binary payload stream
    kafka_stream = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Convert binary payload rows to string types for the raw Bronze Delta structure
    bronze_df = kafka_stream.selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "CAST(value AS STRING) as value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    )

    # Initialize standard append-only writer
    writer = (
        bronze_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        # Group incoming data and only write to disk once every 5 seconds
        .trigger(processingTime="5 seconds")
        .outputMode("append")
    )

    if target_path:
        writer = writer.option("path", target_path)

    # Fire the query activity execution block
    query = writer.toTable(target_table)
    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka/Redpanda Bronze Layer Ingestion")
    parser.add_argument("--bootstrap_servers", required=True)
    parser.add_argument("--topic", required=False, default="network-telemetry")
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--target_path", required=False, default=None)
    parser.add_argument("--format_type", required=False, default="json")

    args = parser.parse_args()

    # Instantiate our Spark environment securely
    spark = get_spark_session(args.format_type)

    run_kafka_bronze(
        spark_session=spark, # Pass the real session here
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        checkpoint_path=args.checkpoint_path,
        target_table=args.target_table,
        target_path=args.target_path
    )

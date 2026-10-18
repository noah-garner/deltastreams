import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

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

def run_kafka_bronze(spark_session, bootstrap_servers, topic, checkpoint_path, target_table, target_path=None, format_type="json", sasl_user=None, sasl_password=None):
    checkpoint_path = str(Path(checkpoint_path).resolve())
    if target_path and "redpanda.com" not in bootstrap_servers:
        target_path = str(Path(target_path).resolve())

    print(f"Reading active stream from Kafka broker: {bootstrap_servers} on topic: {topic}")

    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false"
    }

    if "redpanda.com" in bootstrap_servers:
        final_user = sasl_user or os.getenv("REDPANDA_SASL_USER")
        final_password = sasl_password or os.getenv("REDPANDA_SASL_PASSWORD")

        if not final_user or not final_password:
            raise ValueError("CRITICAL: Redpanda Cloud SASL credentials could not be resolved from parameters or environment!")

        kafka_options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-256",
            "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{final_user}" password="{final_password}";'
        })


    kafka_stream = (
        spark_session.readStream.format("kafka")
        .options(**kafka_options)
        .load()
    )

    bronze_df = kafka_stream.selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "CAST(value AS STRING) as value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    ).withColumn("_ingested_at", current_timestamp())

    writer = (
        bronze_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
    )

    if format_type == "cloudFiles":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")

    if target_path:
        writer = writer.option("path", target_path)

    query = writer.toTable(target_table)
    if format_type != "cloudFiles":
        print("Infinite stream active. Awaiting new data...")
        query.awaitTermination()
    else:
        query.processAllAvailable()
        print("Serverless micro-batch completed successfully. Exiting container context.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka/Redpanda Bronze Layer Ingestion")
    parser.add_argument("--bootstrap_servers", required=True)
    parser.add_argument("--topic", required=False, default="network-telemetry")
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--target_path", required=False, default=None)
    parser.add_argument("--format_type", required=False, default="json")
    parser.add_argument("--sasl_user", required=False, default=None)
    parser.add_argument("--sasl_password", required=False, default=None)

    args = parser.parse_args()

    spark = get_spark_session(args.format_type)

    run_kafka_bronze(
        spark_session=spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        checkpoint_path=args.checkpoint_path,
        target_table=args.target_table,
        target_path=args.target_path,
        format_type=args.format_type,
        sasl_user=args.sasl_user,
        sasl_password=args.sasl_password
    )

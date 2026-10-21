import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def get_spark_session(format_type):
    if format_type == "cloudFiles":
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.appName("DeltaStreams_Bronze_Ingestion")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]).getOrCreate()

def run_bronze(spark_session, bootstrap_servers, topic, checkpoint_path, target_table, target_path=None, format_type="json", sasl_user=None, sasl_password=None, stream_mode=False):
    cp_path = str(Path(checkpoint_path).resolve())
    is_cloud = (format_type == "cloudFiles" or "DATABRICKS_RUNTIME_VERSION" in os.environ)
    print(f"Reading active stream from Kafka broker: {bootstrap_servers} on topic: {topic}")

    # 1. Dynamic Environment Strategy Matrix
    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": "earliest" if (is_cloud or not stream_mode) else "latest",
        "failOnDataLoss": "false"
    }

    # Apply performance throttle limit strictly when processing single micro-batch scratch runs locally
    if not is_cloud and not stream_mode:
        kafka_options["maxOffsetsPerTrigger"] = 1

    # 2. Secure Redpanda Cloud SASL Configuration
    if "redpanda.com" in bootstrap_servers:
        user = sasl_user or os.getenv("REDPANDA_SASL_USER")
        pwd = sasl_password or os.getenv("REDPANDA_SASL_PASSWORD")
        if not user or not pwd:
            raise ValueError("CRITICAL: Redpanda Cloud SASL credentials missing!")
        
        login_mod = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule" if is_cloud else "org.apache.kafka.common.security.scram.ScramLoginModule"
        kafka_options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-256",
            "kafka.sasl.jaas.config": f'{login_mod} required username="{user}" password="{pwd}";'
        })

    # 3. Structural Data Frame Declarations
    bronze_df = (spark_session.readStream.format("kafka").options(**kafka_options).load()
        .selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as value",
            "topic", "partition", "offset",
            "timestamp as kafka_timestamp")
        .withColumn("_ingested_at", current_timestamp()))

    # 4. Storage Engine Core Lifecycles
    writer = bronze_df.writeStream.format("delta").option("checkpointLocation", cp_path).option("mergeSchema", "true").outputMode("append")
    
    # Check execution boundaries to configure engine triggers
    # If stream_mode is turned on, force use_batch_trigger to False everywhere
    use_batch_trigger = (is_cloud or format_type == "json") if not stream_mode else False
    writer = writer.trigger(availableNow=True)
    
    if target_path and not is_cloud:
        writer = writer.option("path", str(Path(target_path).resolve()))

    query = writer.start() if (not is_cloud and target_path and not target_table.startswith("main.")) else writer.toTable(target_table)

    if use_batch_trigger:
        query.processAllAvailable()
        print("Bronze micro-batch ingestion complete. State data flushed successfully.")
    else:
        print("Infinite live streaming engine active. Awaiting edge telemetry traffic...")
        query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DeltaStreams Core Bronze Engine")
    parser.add_argument("--bootstrap_servers", required=True)
    parser.add_argument("--topic", default="network-telemetry")
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--target_path", default=None)
    parser.add_argument("--format_type", default="json")
    parser.add_argument("--sasl_user", default=None)
    parser.add_argument("--sasl_password", default=None)
    # The Master Switch Flag: Defaults to single-batch mode unless explicitly called
    parser.add_argument("--stream", action="store_true", help="Activate infinite live streaming listener mode")

    args = parser.parse_args()
    run_bronze(
        get_spark_session(args.format_type), 
        args.bootstrap_servers, args.topic, args.checkpoint_path, args.target_table, args.target_path, 
        args.format_type, args.sasl_user, args.sasl_password, stream_mode=args.stream
    )
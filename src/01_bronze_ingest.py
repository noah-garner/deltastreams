import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def get_spark_session(format_type):
    if format_type == "cloudFiles":
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.appName("DeltaStreams_Bronze_Ingestion_Engine")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]).getOrCreate()

def run_bronze(spark_session, bootstrap_servers, topic, checkpoint_path, target_table, target_path=None, format_type="json", sasl_user=None, sasl_password=None, stream_mode=False):
    cp_path = str(Path(checkpoint_path).resolve())
    is_cloud = (format_type == "cloudFiles" or "DATABRICKS_RUNTIME_VERSION" in os.environ)

    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": "earliest" if (is_cloud or not stream_mode) else "latest",
        "failOnDataLoss": "false"
    }

    if not is_cloud and not stream_mode:
        kafka_options["maxOffsetsPerTrigger"] = 1

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

    bronze_df = (spark_session.readStream.format("kafka").options(**kafka_options).load()
        .selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as value",
            "topic", "partition", "offset",
            "timestamp as kafka_timestamp")
        .withColumn("_ingested_at", current_timestamp()))

    writer = bronze_df.writeStream.format("delta").option("checkpointLocation", cp_path).option("mergeSchema", "true").outputMode("append")

    use_batch_trigger = (is_cloud or format_type == "json") if not stream_mode else False
    if use_batch_trigger:
        writer = writer.trigger(availableNow=True)
    elif stream_mode:
        if is_cloud:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime="2 seconds")

    if target_path and not is_cloud:
        writer = writer.option("path", str(Path(target_path).resolve()))

    query = writer.start() if (not is_cloud and target_path and not target_table.startswith("main.")) else writer.toTable(target_table)

    if use_batch_trigger:
        query.processAllAvailable()
    else:
        query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DeltaStreams Bronze Ingestion Engine")
    parser.add_argument("--bootstrap_servers", required=True)
    parser.add_argument("--topic", default="network-telemetry")
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--target_path", default=None)
    parser.add_argument("--format_type", default="json")
    parser.add_argument("--sasl_user", default=None)
    parser.add_argument("--sasl_password", default=None)
    parser.add_argument("--stream", action="store_true", help="Activate infinite live streaming listener mode")

    args = parser.parse_args()

    final_user = args.sasl_user
    final_password = args.sasl_password

    dbutils_runtime = globals().get("dbutils")
    if dbutils_runtime and not final_user and not final_password:
        final_user = dbutils_runtime.secrets.get(scope="deltastreams_scope", key="redpanda_user")
        final_password = dbutils_runtime.secrets.get(scope="deltastreams_scope", key="redpanda_password")

    run_bronze(
        spark_session=get_spark_session(args.format_type), 
        bootstrap_servers=args.bootstrap_servers, 
        topic=args.topic, 
        checkpoint_path=args.checkpoint_path, 
        target_table=args.target_table, 
        target_path=args.target_path, 
        format_type=args.format_type, 
        sasl_user=final_user, 
        sasl_password=final_password, 
        stream_mode=args.stream
    )
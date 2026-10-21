import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, current_timestamp

def get_spark_session(format_type):
    if format_type == "cloudFiles":
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.appName("DeltaStreams_Gold_Security_Alerts")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_gold_security_analysis(spark_session, source_silver_table, target_gold_table, checkpoint_path, format_type, is_local=False, stream_mode=False):
    cp = str(Path(checkpoint_path).resolve())
    is_cloud = (format_type == "cloudFiles" or "DATABRICKS_RUNTIME_VERSION" in os.environ)
    print(f"Analyzing Silver telemetry logs from: {source_silver_table}")

    # 1. Standardize Inbound Input Streams
    src = str(Path(source_silver_table).resolve()) if (is_local and not source_silver_table.startswith("main.")) else source_silver_table
    silver_stream = spark_session.readStream.format("delta").load(src) if (is_local and not source_silver_table.startswith("main.")) else spark_session.readStream.table(source_silver_table)

    # 2. Watermark Management & Data Cleanliness Check
    watermarked_stream = silver_stream.withWatermark("timestamp", "10 minutes")
    clean_ip_stream = watermarked_stream.filter(col("attacker_ip").isNotNull() & (col("attacker_ip") != ""))

    # 3. Security Threat Stateful Window Aggregation (Per Attacker IP)
    security_alerts_df = (clean_ip_stream
        .groupBy(
            window(col("timestamp"), "1 minute", "30 seconds"),
            col("attacker_ip"),
            col("honeypot_region"),
            col("attacker_country"),
            col("attacker_country_code")
        )
        .agg(count("*").alias("request_count"))
        .filter(col("request_count") >= 3)
        .select(
            col("window.start").alias("alert_window_start"),
            col("window.end").alias("alert_window_end"),
            col("attacker_ip"),
            col("honeypot_region"),
            col("attacker_country"),
            col("attacker_country_code"),
            col("request_count"),
            current_timestamp().alias("_alert_triggered_at")
        ))

    # 4. Storage Engine Core Execution Architecture
    writer = security_alerts_df.writeStream.format("delta").option("checkpointLocation", cp).outputMode("complete")
    
    use_batch_trigger = (is_cloud or format_type == "delta") if not stream_mode else False
    writer = writer.trigger(availableNow=True)

    if not is_cloud and target_gold_table:
        writer = writer.option("path", str(Path(target_gold_table).resolve()))

    query = writer.start() if (not is_cloud and target_gold_table and not target_gold_table.startswith("main.")) else writer.toTable(target_gold_table)

    if use_batch_trigger:
        query.processAllAvailable()
        print("Serverless gold metric rollup aggregation complete. States written to disk.")
    else:
        print("Infinite gold analytics stream active. Awaiting telemetry windows...")
        query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DeltaStreams Gold Security Alerts Engine")
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--is_local", action="store_true")
    parser.add_argument("--stream", action="store_true")

    args = parser.parse_args()
    fmt = "delta" if args.is_local else "cloudFiles"

    run_gold_security_analysis(
        get_spark_session(fmt), args.source_table, args.gold_table, 
        args.checkpoint_path, fmt, is_local=args.is_local, stream_mode=args.stream
    )

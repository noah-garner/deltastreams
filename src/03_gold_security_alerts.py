import argparse
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

def run_gold_security_analysis(spark_session, source_silver_table, target_gold_table, checkpoint_path, format_type, is_local=False):
    cp = str(Path(checkpoint_path).resolve())
    print(f"Analyzing Silver telemetry logs from: {source_silver_table}")

    # Standardize data source loading strings across local and cloud environments
    src = str(Path(source_silver_table).resolve()) if (is_local and not source_silver_table.startswith("main.")) else source_silver_table
    silver_stream = spark_session.readStream.format("delta").load(src) if (is_local and not source_silver_table.startswith("main.")) else spark_session.readStream.table(source_silver_table)

    # 10-minute watermark using Silver processing clock field
    watermarked_stream = silver_stream.withWatermark("_processed_at", "10 minutes")
    clean_ip_stream = watermarked_stream.filter(col("attacker_ip").isNotNull() & (col("attacker_ip") != ""))

    # Brute-force detection window grouped by attacker and targeted region
    security_alerts_df = (clean_ip_stream
        .groupBy(
            window(col("_processed_at"), "1 minute", "30 seconds"),
            col("attacker_ip"),
            col("honeypot_region")
        )
        .agg(count("*").alias("request_count"))
        .filter(col("request_count") >= 5)
        .select(
            col("window.start").alias("alert_window_start"),
            col("window.end").alias("alert_window_end"),
            col("attacker_ip"),
            col("honeypot_region"),
            col("request_count"),
            current_timestamp().alias("_alert_triggered_at")
        ))

    # Configure streaming writer structure
    writer = security_alerts_df.writeStream.format("delta").option("checkpointLocation", cp).outputMode("complete")
    writer = writer.trigger(availableNow=True) if format_type == "cloudFiles" else writer.trigger(processingTime="5 seconds")

    query = writer.start(str(Path(target_gold_table).resolve())) if (is_local and not target_gold_table.startswith("main.")) else writer.toTable(target_gold_table)

    if format_type != "cloudFiles":
        print("Infinite gold aggregation stream active. Awaiting new alerts...")
        query.awaitTermination()
    else:
        query.processAllAvailable()
        print("Serverless gold micro-batch completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DeltaStreams Gold Security Alerts Engine")
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--is_local", action="store_true")

    args = parser.parse_args()
    fmt = "delta" if args.is_local else "cloudFiles"

    run_gold_security_analysis(get_spark_session(fmt), args.source_table, args.gold_table, args.checkpoint_path, fmt, args.is_local)

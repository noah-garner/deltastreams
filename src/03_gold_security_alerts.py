import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, current_timestamp

def get_spark_session(format_type):
    if format_type == "cloudFiles":
        return SparkSession.builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip
        builder = (
            SparkSession.builder.appName("DeltaStreams_Gold_Security_Alerts")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

def run_gold_security_analysis(spark_session, source_silver_table, target_gold_table, checkpoint_path, format_type, is_local=False):
    checkpoint_path = str(Path(checkpoint_path).resolve())
    
    print(f"Analyzing Silver telemetry logs from: {source_silver_table}")

    if is_local and not source_silver_table.startswith("main."):
        source_path = str(Path(source_silver_table).resolve())
        silver_stream = spark_session.readStream.format("delta").load(source_path)
    else:
        silver_stream = spark_session.readStream.table(source_silver_table)

    watermarked_stream = silver_stream.withWatermark("_processed_at", "10 minutes")

    clean_ip_stream = watermarked_stream.filter(col("src_ip").isNotNull() & (col("src_ip") != ""))

    security_alerts_df = (
        clean_ip_stream
        .groupBy(
            window(col("_processed_at"), "1 minute", "30 seconds"),
            col("src_ip"),
            col("protocol")
        )
        .agg(count("*").alias("request_count"))
        .filter(col("request_count") >= 5)
        .select(
            col("window.start").alias("alert_window_start"),
            col("window.end").alias("alert_window_end"),
            col("src_ip").alias("attacker_ip"),
            col("protocol"),
            col("request_count"),
            current_timestamp().alias("_alert_triggered_at")
        )
    )

    writer = (
        security_alerts_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("complete")
    )

    if format_type == "cloudFiles":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")

    if is_local and not target_gold_table.startswith("main."):
        query = writer.start(str(Path(target_gold_table).resolve()))
    else:
        query = writer.toTable(target_gold_table)

    if format_type != "cloudFiles":
        print("Infinite gold aggregation stream active. Awaiting new data...")
        query.awaitTermination()
    else:
        query.processAllAvailable()
        print("Serverless gold micro-batch completed successfully. Exiting container context.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DeltaStreams Gold Security Alerts Engine")
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--gold_table", required=True)
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--is_local", action="store_true")

    args = parser.parse_args()
    format_type = "delta" if args.is_local else "cloudFiles"
    spark = get_spark_session(format_type)

    run_gold_security_analysis(
        spark_session=spark,
        source_silver_table=args.source_table,
        target_gold_table=args.gold_table,
        checkpoint_path=args.checkpoint_path,
        format_type=format_type,
        is_local=args.is_local
    )

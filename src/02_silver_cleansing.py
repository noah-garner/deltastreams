import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    lower,
    to_date,
    to_timestamp,
    trim,
    when,
)


def get_spark_session(format_type):
    if format_type == "cloudFiles":
        return SparkSession.builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip
        builder = (
            SparkSession.builder.appName("Clickstream_Bronze_Ingestion")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()


def run_silver_cleansing(
    spark, source_table, silver_table, dlq_table, checkpoint_path, is_local=False
):
    print(f"Reading Silver source from: {source_table}")

    # Resolve local checkpoint paths cleanly if executing on host OS
    if is_local:
        checkpoint_path = str(Path(checkpoint_path).resolve())

    # 1. Read Stream from Source Table (or Delta Path)
    if is_local and not source_table.startswith("main."):
        source_path = str(Path(source_table).resolve())
        bronze_stream = spark.readStream.format("delta").load(source_path)
    else:
        bronze_stream = spark.readStream.table(source_table)

    # 2. Transformations & Type Conversions
    transformed_df = (
        bronze_stream.withColumn("event_timestamp", to_timestamp("event_time"))
        .withColumn("event_date", to_date("event_timestamp"))
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("price_numeric", expr("try_cast(price as decimal(10,2))"))
        .withColumn("_processed_at", current_timestamp())
    )

    # 3. Stream 1: Valid Clean Data
    valid_df = (
        transformed_df.filter(col("price_numeric").isNotNull())
        .withColumnRenamed("price_numeric", "price_amount")
        .drop("price")
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["user_id", "product_id", "event_time", "event_type"])
    )

    # 4. Stream 2: DLQ Data
    dlq_df = transformed_df.filter(
        col("price_numeric").isNull() & col("price").isNotNull()
    ).withColumn(
        "_dlq_reason",
        when(
            col("price") == "CORRUPT_PRICE_PAYLOAD", "INVALID_PRICE_PAYLOAD"
        ).otherwise("UNPARSEABLE_TYPE_CAST_FAILURE"),
    )

    # 5. Create Target Silver Table with Liquid Clustering (Unity Catalog only)
    if not is_local and silver_table.startswith("main."):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {silver_table} (
                user_id BIGINT,
                device_type STRING,
                product_id BIGINT,
                event_type STRING,
                price_amount DECIMAL(10,2),
                event_time STRING,
                event_timestamp TIMESTAMP,
                event_date DATE,
                _ingested_at TIMESTAMP,
                _processed_at TIMESTAMP,
                _source_file STRING
            )
            CLUSTER BY (event_date, event_type);
        """)

    # 6. Execute Streaming Writes
    clean_checkpoint = f"{checkpoint_path}/silver_clean"
    dlq_checkpoint = f"{checkpoint_path}/silver_dlq"

    valid_writer = (
        valid_df.writeStream.format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("mergeSchema", "true")
        .option("checkpointLocation", clean_checkpoint)
    )

    dlq_writer = (
        dlq_df.writeStream.format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("mergeSchema", "true")
        .option("checkpointLocation", dlq_checkpoint)
    )

    if is_local and not silver_table.startswith("main."):
        valid_query = valid_writer.start(str(Path(silver_table).resolve()))
        dlq_query = dlq_writer.start(str(Path(dlq_table).resolve()))
    else:
        valid_query = valid_writer.toTable(silver_table)
        dlq_query = dlq_writer.toTable(dlq_table)

    valid_query.awaitTermination()
    dlq_query.awaitTermination()
    print("Silver Cleansing & DLQ Routing Complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Layer Cleansing & DLQ Pipeline")
    parser.add_argument("--source_table", required=True, help="Bronze source table or path")
    parser.add_argument("--silver_table", required=True, help="Clean Silver target table or path")
    parser.add_argument("--dlq_table", required=True, help="DLQ target table or path")
    parser.add_argument("--checkpoint_path", required=True, help="Checkpoint volume directory")
    parser.add_argument("--is_local", action="store_true", help="Flag for local PySpark execution")

    args = parser.parse_args()
    format_type = "delta" if args.is_local else "cloudFiles"
    spark = get_spark_session(format_type=format_type)
    run_silver_cleansing(
        spark=spark,
        source_table=args.source_table,
        silver_table=args.silver_table,
        dlq_table=args.dlq_table,
        checkpoint_path=args.checkpoint_path,
        is_local=args.is_local,
    )
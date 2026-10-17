import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
from pyspark.sql.types import LongType, StringType, StructType

KAGGLE_SCHEMA = (
    StructType()
    .add("event_time", StringType(), True)
    .add("event_type", StringType(), True)
    .add("product_id", LongType(), True)
    .add("category_id", LongType(), True)
    .add("category_code", StringType(), True)
    .add("brand", StringType(), True)
    .add("price", StringType(), True)
    .add("user_id", LongType(), True)
    .add("user_session", StringType(), True)
    .add("device_type", StringType(), True)
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


def run_bronze_ingestion(spark, source_path, checkpoint_path, target_table, target_path=None, format_type="json"):
    # Always resolve absolute paths to eliminate working-directory mismatches
    source_path = str(Path(source_path).resolve())
    checkpoint_path = str(Path(checkpoint_path).resolve())
    if target_path:
        target_path = str(Path(target_path).resolve())

    print(f"Reading from absolute source path: {source_path}")

    if format_type == "cloudFiles":
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(KAGGLE_SCHEMA)
        )
    else:
        reader = (
            spark.readStream.format("json")
            .schema(KAGGLE_SCHEMA)
        )

    df_transformed = (
        reader.load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'")
        )
    )

    writer = (
        df_transformed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
    )

    if target_path:
        writer = writer.option("path", target_path)

    query = writer.toTable(target_table)
    query.awaitTermination()
    print("Bronze Ingestion Complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--source_path", required=True)
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--target_path", required=False, default=None)
    parser.add_argument("--format_type", required=False, default="json")

    args = parser.parse_args()

    spark = get_spark_session(args.format_type)
    run_bronze_ingestion(
        spark=spark,
        source_path=args.source_path,
        checkpoint_path=args.checkpoint_path,
        target_table=args.target_table,
        target_path=args.target_path,
        format_type=args.format_type
    )
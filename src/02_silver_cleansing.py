import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, regexp_extract, current_timestamp, when, broadcast, trim, split
from pyspark.sql.types import StructType, StructField, StringType

def get_spark_session(format_type):
    if format_type == "cloudFiles":
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.appName("DeltaStreams_Silver_Cleansing_Engine")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_silver_cleansing(spark_session, source_table, geoip_table, silver_table, dlq_table, checkpoint_path, format_type, is_local=False, stream_mode=False):
    base_cp = str(Path(checkpoint_path).resolve())
    is_cloud = (format_type == "cloudFiles" or "DATABRICKS_RUNTIME_VERSION" in os.environ)
    
    src = str(Path(source_table).resolve()) if (is_local and not source_table.startswith("main.")) else source_table
    stream = spark_session.readStream.format("delta").load(src) if (is_local and not source_table.startswith("main.")) else spark_session.readStream.table(source_table)

    vector_schema = StructType([
        StructField("host", StringType(), True), StructField("file", StringType(), True),
        StructField("message", StringType(), True), StructField("timestamp", StringType(), True)
    ])

    unpacked_df = stream.withColumn("payload", from_json(col("value"), vector_schema)).select("payload.*")

    base_enriched_df = unpacked_df.withColumn("event_timestamp", split(col("message"), " ").getItem(0).cast("timestamp"))

    is_wrapper_corrupt = col("message").isNull()
    is_network_noise = col("message").contains("Received disconnect") | col("message").contains("Bye Bye")
    is_initial_dlq = is_wrapper_corrupt | is_network_noise

    initial_dlq_df = (base_enriched_df
        .filter(is_initial_dlq)
        .withColumn("_dlq_reason",
            when(col("file").isNull(), "MALFORMED_JSON_METADATA_WRAPPER")
            .otherwise("NETWORK_NOISE_DISCONNECT_WITHOUT_AUTH")
        )
    )

    valid_raw_df = base_enriched_df.filter(~is_initial_dlq)

    regex_pattern = r"(?:Disconnected from (?:invalid user |authenticating user )?|Invalid user )([\w\-]+) from ([\d\.]+)"

    parsed_df = (valid_raw_df
        .withColumn("honeypot_host", col("host"))
        .withColumn("honeypot_region",
            when(col("host") == "honeypot-edge-01", "US-WEST (SFO)")
            .when(col("host") == "honeypot-edge-02", "US-EAST (NYC)")
            .when(col("host") == "honeypot-edge-03", "EU-WEST (AMS)")
            .otherwise("UNKNOWN_EDGE_REGION"))
        .withColumn("raw_user", regexp_extract(col("message"), regex_pattern, 1))
        .withColumn("targeted_username", trim(col("raw_user")))
        .withColumn("raw_ip", regexp_extract(col("message"), regex_pattern, 2))
        .withColumn("attacker_ip", trim(col("raw_ip")))
        .withColumn("processed_at", current_timestamp())
    )

    is_regex_failed = (col("attacker_ip") == "") | col("attacker_ip").isNull() | col("event_timestamp").isNull()
        
    clean_valid_df = parsed_df.filter(~is_regex_failed)
    
    regex_failed_dlq_df = parsed_df.filter(is_regex_failed).withColumn(
        "_dlq_reason", F.lit("UNMATCHED_AUTH_EVENT_PATTERN")
    ).select(col("host"), col("file"), col("message"), col("event_timestamp"), col("_dlq_reason"))

    final_dlq_df = initial_dlq_df.select("host", "file", "message", "event_timestamp", "_dlq_reason") \
        .union(regex_failed_dlq_df) \
        .withColumn("_processed_at", current_timestamp())

    geo_src = str(Path(geoip_table).resolve()) if (is_local and not geoip_table.startswith("main.")) else geoip_table
    dim_geoip_df = spark_session.read.format("delta").load(geo_src) if (is_local and not geoip_table.startswith("main.")) else spark_session.read.table(geoip_table)

    attack_octets = split(clean_valid_df.attacker_ip, r"\.")
    attacker_ip_num = (attack_octets.getItem(0).cast("long") * 16777216 +
                       attack_octets.getItem(1).cast("long") * 65536 +
                       attack_octets.getItem(2).cast("long") * 256 +
                       attack_octets.getItem(3).cast("long"))

    base_numeric_df = clean_valid_df.withColumn("attacker_ip_num", attacker_ip_num)

    enriched_valid_df = base_numeric_df.join(
        broadcast(dim_geoip_df),
        (base_numeric_df.attacker_ip_num >= dim_geoip_df.start_ip_num) & 
        (base_numeric_df.attacker_ip_num <= dim_geoip_df.end_ip_num),
        "left"
    ).select(
        base_numeric_df["*"], 
        col("country_name").alias("attacker_country"), 
        col("country_code").alias("attacker_country_code")
    ).drop("message", "raw_ip", "raw_user", "attacker_ip_num")

    writers = { enriched_valid_df: (silver_table, f"{base_cp}/clean"), final_dlq_df: (dlq_table, f"{base_cp}/dlq") }
    use_batch_trigger = (is_cloud or format_type in ["json", "delta"]) if not stream_mode else False
    queries = []

    for df, (target, cp) in writers.items():
        w = df.writeStream.format("delta").option("mergeSchema", "true").option("checkpointLocation", cp).outputMode("append")
        if use_batch_trigger:
            w = w.trigger(availableNow=True)
        elif stream_mode:
            if is_cloud:
                w = w.trigger(availableNow=True)
            else:
                w = w.trigger(processingTime="2 seconds")
                
        if not is_cloud and target:
            w = w.option("path", str(Path(target).resolve()))
        q = w.start() if (not is_cloud and target and not target.startswith("main.")) else w.toTable(target)
        queries.append(q)

    if use_batch_trigger:
        for q in queries: q.processAllAvailable()
    else:
        for q in queries: q.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DeltaStreams Silver Cleansing Engine")
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--geoip_table", required=True)
    parser.add_argument("--silver_table", required=True)
    parser.add_argument("--dlq_table", required=True)
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--is_local", action="store_true")
    parser.add_argument("--stream", action="store_true")

    args = parser.parse_args()
    fmt = "delta" if args.is_local else "cloudFiles"
    run_silver_cleansing(get_spark_session(fmt), args.source_table, args.geoip_table, args.silver_table, args.dlq_table, args.checkpoint_path, fmt, is_local=args.is_local, stream_mode=args.stream)

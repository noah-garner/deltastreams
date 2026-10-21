import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session(is_cloud: bool):
    """Initializes a local Delta session or hooks into standard Databricks context."""
    if is_cloud:
        return SparkSession.builder.getOrCreate()
        
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.appName("GeoIP_Data_Curation")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()

def merge_geoip_sources(spark, source_dir, target_table_or_path, is_cloud):
    print(f"Initializing compilation. Ingesting from: {source_dir}")
    
    # 1. Adapt ingestion paths cleanly for local system or cloud Volumes
    blocks_path = f"{source_dir}/GeoLite2-Country-Blocks-IPv4.csv"
    names_path = f"{source_dir}/GeoLite2-Country-Locations-en.csv"
    
    blocks_df = spark.read.option("header", "true").csv(blocks_path)
    names_df = spark.read.option("header", "true").csv(names_path)

    # 2. Parse the network column into the base IP and the routing mask length
    split_net = F.split(blocks_df.network, "/")
    base_ip = split_net.getItem(0)
    mask = split_net.getItem(1).cast("int")

    # 3. Rebuild the base IP address components into a single math integer value scalar
    octets = F.split(base_ip, r"\.")
    ip_num = (octets.getItem(0).cast("long") * 16777216 +
              octets.getItem(1).cast("long") * 65536 +
              octets.getItem(2).cast("long") * 256 +
              octets.getItem(3).cast("long"))

    # 4. Calculate exact numeric boundaries based on standard masking bit shifts
    num_ips = F.pow(2, 32 - mask).cast("long")
    range_df = (blocks_df.withColumn("start_ip_num", ip_num)
                         .withColumn("end_ip_num", ip_num + num_ips - 1))

    # 5. Combine dimensions and write to your final uniform Delta reference folder
    final_dim = range_df.join(names_df, on="geoname_id", how="inner").select(
        F.col("network").alias("cidr_block"),
        F.col("start_ip_num"),
        F.col("end_ip_num"),
        F.col("country_iso_code").alias("country_code"),
        F.col("country_name")
    )

    # 6. Save natively based on runtime context
    if is_cloud:
        print(f"Writing fully managed Delta table to Unity Catalog: {target_table_or_path}")
        final_dim.write.format("delta").mode("overwrite").saveAsTable(target_table_or_path)
    else:
        # Local relative path fallback mapping setup
        local_output = str(Path(target_table_or_path).resolve())
        print(f"Writing local Delta folder down to disk path: {local_output}")
        final_dim.write.format("delta").mode("overwrite").save(local_output)
        
    print("Success! Numeric range table compiled.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal GeoIP Hash Dimension Builder")
    parser.add_argument("--source_dir", type=str, default="data", help="Directory containing source CSVs")
    parser.add_argument("--target", type=str, default="data/tables/dim_geoip_lookup", help="Output table name or folder path")
    args = parser.parse_args()

    # Automatically identify if running on Databricks serverless runtime
    is_cloud_env = "DATABRICKS_RUNTIME_VERSION" in os.environ or args.target.startswith("main.")

    spark_session = get_spark_session(is_cloud_env)
    merge_geoip_sources(spark_session, args.source_dir, args.target, is_cloud_env)

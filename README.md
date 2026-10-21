# DeltaStreams

> **A unified, production-grade edge-to-lakehouse streaming reference architecture built with Databricks Asset Bundles (DABs), Auto Loader, Vector, Redpanda, and Delta Lake.**

---

## Architecture Overview

DeltaStreams demonstrates a fault-tolerant, low-latency **Medallion Architecture** that processes high-velocity network telemetry streams and enforces structural data quality gates without disrupting downstream analytics.

```text

    ┌─────────────────────────────────────────────────────────┐
    │                Vector Edge Listener                     │
    └────────────────────────────┬────────────────────────────┘
                                 │ (Zero-JVM Protocol Translation)
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │               Redpanda Broker (Kafka API)               │
    └────────────────────────────┬────────────────────────────┘
                                 │ (Incremental Spark Stream Scan)
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │               Bronze Layer: Kafka Ingest                │
    └────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │                    Silver Cleansing                     │
    │     (Schema Enforcement, Regex Casting & Validation)    │
    └───────────────────┬─────────────────┬───────────────────┘
                        │                 │
            Valid Data  │                 │ Corrupt / Failed
                        ▼                 ▼
    ┌─────────────────────────┐     ┌─────────────────────────┐
    │      Silver Clean       │     │    Silver DLQ (Table)   │
    │   (Kafka Silver Clean)  │     │   (Kafka Silver DLQ)    │
    └───────────────┬─────────┘     └─────────────────────────┘
                    │
                    ▼
    ┌─────────────────────────────────────────────────────────┐
    │                Gold Layer: Threat Alerts                │
    │        (Time-Series Windowed Threat Aggregations)       │
    └─────────────────────────────────────────────────────────┘
```

## Key Features

* **Vector Ingestion Plane:** Uses an ultra-lightweight Rust daemon (**Vector**) to process high-throughput UDP syslog packages at the network edge, keeping client memory footprints near zero.
* **Asynchronous Event Buffering:** Utilizes a distributed **Redpanda Cloud** cluster to provide structural backpressure control, ensuring zero loss of stream data even if downstream analytics compute nodes are offline.
* **Defensive Pipeline Protections (DLQ Routing):** Employs explicit structural checks to route malformed or broken payload strings containing `CORRUPT_BYTE_STREAM` to an isolated Dead Letter Queue while valid packets flow smoothly.
* **Stateful Streaming Aggregations:** Computes real-time rolling metrics in the Gold layer to flag distributed network brute-force security threats crossing defined volume thresholds.

## Quickstart

1. Requirements

    * Databricks CLI installed & configured

    * Python 3.10+ & uv package manager

    * Redpanda Account & existing `network-telemetry` topic

2. Run Locally

    ```bash

        # source .env so bronze ingest can reach redpanda and so vector.yml can produce to redpanda
        source .env

        # Deploy vector to digital ocean droplets (make sure to update .ssh/config with each droplet ip)
        # Local test to pull real internet data straight from the cloud queue to test processing layers
        ./scripts/deploy_grid.sh

        # buld geoip hash lookup table locally
        uv run src/00_build_geoip.py

        # Run Bronze Ingest (optional: --stream)
        uv run src/01_bronze_ingest.py \
        --bootstrap_servers $BUNDLE_VAR_rp_bootstrap_servers \
        --topic "network-telemetry" \
        --checkpoint_path "data/checkpoints/bronze" \
        --target_table "bronze_stream" \
        --target_path "data/tables/bronze_stream" \
        --sasl_user $BUNDLE_VAR_rp_user \
        --sasl_password $BUNDLE_VAR_rp_password \
  

        # Run Silver (optional: --stream)
        uv run src/02_silver_cleansing.py \
        --source_table "data/tables/bronze_stream" \
        --geoip_table "data/tables/dim_geoip_lookup" \
        --silver_table "data/tables/silver_clean" \
        --dlq_table "data/tables/silver_dlq" \
        --checkpoint_path "data/checkpoints/silver" \
        --is_local

        # Run Gold (optional: --stream)
        uv run src/03_gold_security_alerts.py \
        --source_table "data/tables/silver_clean" \
        --gold_table "data/tables/gold_alerts" \
        --checkpoint_path "data/checkpoints/gold" \
        --is_local
    ```

3. Inspect Data Locally

    Open the provided Jupyter notebooks under `/notebooks` to query and validate the results across each layer.

4. Deploy to Databricks

    ```bash

        # source .env so db.yml sees your redpanda user,pass,bootstrap
        source .env

        # Copy geoip files to cloud volume (clear other dirs from data first)
        databricks fs cp -r ./data/ dbfs:/Volumes/main/dev/udp_stream_volume/raw_sources/

        # Deploy bundle and run build_geoip_dimension (Do this whenever you update your geoip files)
        databricks bundle deploy && databricks bundle run build_geoip_dimension

        # Deploy bundle and run udp_stream_pipeline
        databricks bundle deploy && databricks bundle run udp_stream_pipeline
    ```

NOTE:

The stream flag --stream:

Local Machine --stream unset reads earliest offset, processing 1 at a time. Set reads latest waiting for new data

Databricks Cloud --stream unset processes all accumlated messasges. Set streams continuously from earliest.

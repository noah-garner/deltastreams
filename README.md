# DeltaStreams

> **A unified, production-grade edge-to-lakehouse streaming reference architecture built with Databricks Automation Bundles (DABs), Vector Edge Listeners, Redpanda Brokers, and Delta Lake.**

---

## Architecture Overview

DeltaStreams demonstrates a fault-tolerant, low-latency **Medallion Architecture** that processes high-velocity network telemetry streams and enforces structural data quality gates without disrupting downstream analytics.

```text

    ┌─────────────────────────────────────────────────────────┐
    │                Vector Edge Listener (DI Droplets)       │
    └────────────────────────────┬────────────────────────────┘
                                 │ 
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │               Redpanda Broker (Kafka API)               │
    └────────────────────────────┬────────────────────────────┘
                                 │ 
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │               Bronze Ingest (Databricks)                │
    └────────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │                    Silver Cleansing                     │
    │     (Schema Enforcement, DLQ, GEOIP Mapping)            │
    └───────────────────┬─────────────────┬───────────────────┘
                        │                 │
            Valid Data  │                 │ Corrupt / Failed / Garbage
                        ▼                 ▼
    ┌─────────────────────────┐     ┌─────────────────────────┐
    │      Silver Clean       │     │    Silver DLQ (Table)   │
    │                         │     │                         │
    └───────────────┬─────────┘     └─────────────────────────┘
                    │
                    ▼
    ┌─────────────────────────────────────────────────────────┐
    │                Gold Layer: Threat Alerts                │
    │        (Time-Series Windowed Threat Aggregations)       │
    └─────────────────────────────────────────────────────────┘
```

## Key Features

* **Vector Ingestion Plane:** Uses an ultra-lightweight Rust daemon (Vector) to securely tail native Linux host authentication logs (/var/log/auth.log) directly at the machine layer to process high-throughput UDP syslog packages at the network edge, keeping client memory footprints near zero.
* **Asynchronous Event Buffering:** Utilizes a distributed **Redpanda Cloud** cluster to provide structural backpressure control, ensuring zero loss of stream data even if downstream analytics compute nodes are offline.
* **Defensive Pipeline Protections (DLQ Routing):** Employs explicit structural checks to route malformed or broken payload strings to an isolated Dead Letter Queue while valid packets flow smoothly.
* **Stateful Streaming Aggregations:** Computes real-time rolling metrics in the Gold layer to flag distributed network brute-force security threats crossing defined volume thresholds.

## Quickstart

1. Requirements

    * Databricks CLI installed & configured

    * Python 3.10+ & uv package manager

    * Redpanda Account & existing `network-telemetry` topic with open read write permissions on current user.

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

        # Deploy and execute a sequential batch run
        databricks bundle deploy && databricks bundle run udp_batch_pipeline

        # Deploy and activate the infinite live streaming pipeline (executes 1 task at each layer and pauses)
        databricks bundle deploy && databricks bundle run udp_stream_pipeline

        # Control Stream behavior for udp stream pipeline. Note this will immediately stream each task!
        databricks bundle deploy --var="stream_status=UNPAUSED"

        # Batch: Run only gold task
        databricks bundle run udp_stream_pipeline --only run_gold_analysis

        # Batch: Run everything after a specific task (notice the "+") - runs silver -> gold
        databricks bundle run udp_stream_pipeline --only run_silver_cleansing+

    ```

5. Purge checkpoints and tables

    Open a notebook, select the serverless compute (not sql warehouse), and run these commands in separate cells:

    ```python
        dbutils.fs.rm("dbfs:/Volumes/main/dev/udp_stream_volume/checkpoints/bronze", True)
        dbutils.fs.rm("dbfs:/Volumes/main/dev/udp_stream_volume/checkpoints/silver", True)
        dbutils.fs.rm("dbfs:/Volumes/main/dev/udp_stream_volume/checkpoints/gold", True)
    ```

    ```sql
        %sql
        TRUNCATE TABLE main.dev.bronze;
        TRUNCATE TABLE main.dev.silver_clean;
        TRUNCATE TABLE main.dev.silver_dlq;
        TRUNCATE TABLE main.dev.gold_security_alerts;
    ```

## Extra

Databricks Cloud: Shifting streaming workloads to Serverless Compute utilizes the optimized `availableNow=True` micro-flush trigger coupled with a `continuous: {}` infrastructure job task framework to achieve infinite streaming latency without thread locking.

The stream flag --stream:

Local Machine --stream unset reads earliest offset, processing 1 at a time. Set reads latest waiting for new data

Databricks Cloud --stream unset processes all accumlated messasges. Set streams continuously from latest.

Another note: Running `databricks bundle run udp_stream_pipeline` will only run each task once all in parallel, so
it is likely gold and silver won't pickup data. This method may be not useful but it exists for now.
So for now to trigger one load for each layer just use `databricks bundle deploy && databricks bundle run udp_batch_pipeline`.

## Issues and Requests

Feel free to clone the repo and try it yourself. If you run into any bugs or have feature requests, do not hesitate to open a PR. Happy streaming!

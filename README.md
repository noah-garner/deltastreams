# DeltaStreams

> **A unified, production-grade edge-to-lakehouse streaming reference architecture built with Databricks Automation Bundles (DABs), Vector Edge Listeners, Redpanda Brokers, and Delta Lake.**

---

## Architecture Overview

DeltaStreams demonstrates a fault-tolerant, low-latency **Medallion Architecture** that processes high-velocity network telemetry streams and enforces structural data quality gates without disrupting downstream analytics.

```text

    ┌─────────────────────────────────────────────────────────┐
    │                Vector Edge Listener (DO Droplets)       │
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
    │          (Schema Enforcement, DLQ, GEOIP Mapping)       │
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

To optimize development velocity and eliminate cloud compute waste, DeltaStreams implements a decoupled, environment-fluid trigger engine. The exact same business logic transitions seamlessly between local development and cloud production runtimes without code changes.

```text

          ┌──────────────────────────────────────────┐
          │      STABLE STREAMING CORE PIPELINE      │
          │   (readStream & writeStream DAG Logic)   │
          └────────────────────┬─────────────────────┘
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
  [ LOCAL DEVELOPMENT ENGINE ]          [ CLOUD PRODUCTION RUNTIME ]
  • Mode: Infinite Live Stream          • Mode: Serverless Macro-Loop
  • Trigger: processingTime='2s'        • Trigger: availableNow=True
  • Compute Cost: $0.00 (Native .venv)  • Compute Cost: Scale-to-Zero Managed
  • Context: Rapid hot-reload testing   • Context: Production Databricks Jobs

```

## Key Features

* **Vector Ingestion Plane:** Uses an ultra-lightweight Rust daemon (Vector) to securely tail native Linux host authentication logs (/var/log/auth.log) directly at the machine layer to process high-throughput UDP syslog packages at the network edge, keeping client memory footprints near zero. Wipes out the typical 15-40% resource tax that heavy legacy logging agents place on edge compute nodes.
* **Asynchronous Event Buffering:** Utilizes a distributed **Redpanda Cloud** cluster to provide structural backpressure control, ensuring zero loss of stream data even if downstream analytics compute nodes are offline. Guarantees complete data resilience during cloud worker failovers or downstream maintenance windows without losing telemetry logs.
* **Defensive Pipeline Protections (DLQ Routing):** Employs explicit structural checks to route malformed or broken payload strings to an isolated Dead Letter Queue while valid packets flow smoothly. Stops data pollution at the gateway layer, cutting unnecessary cloud processing compute bills caused by parsing unindexed malformed junk.
* **Stateful Streaming Aggregations:** Computes real-time rolling metrics in the Gold layer to flag distributed network brute-force security threats crossing defined volume thresholds. Shifts heavy state analytics computation out of expensive downstream BI warehouses into optimized stream processing memory layers.

## Quickstart

1. Requirements

    * Databricks CLI installed & configured

    * Python 3.10+ & uv package manager

    * Redpanda Account & existing `network-telemetry` topic with open read write permissions on current user.

    * Atleast 1 Digital Ocean Droplet

2. Run Locally

    ```bash

        # Deploy vector to digital ocean droplets (make sure to update the script's IP addresses)
        set -a && source .env && set +a && ./scripts/deploy_grid.sh

        # Bulid geoip hash lookup table locally
        uv run src/00_build_geoip.py

        # Run Bronze Ingest (optional: --stream)
        set -a && source .env && set +a && uv run src/01_bronze_ingest.py \
        --bootstrap_servers "dba14vv73u5jpihrnbug.any.us-west-2.mpx.prd.cloud.redpanda.com:9092" \
        --topic "network-telemetry" \
        --checkpoint_path "data/checkpoints/bronze" \
        --target_table "bronze_raw" \
        --target_path "data/tables/bronze_raw" \
        --sasl_user $RP_USER \
        --sasl_password $RP_PASSWORD
  
        # Run Silver (optional: --stream)
        uv run src/02_silver_cleansing.py \
        --source_table "data/tables/bronze_raw" \
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

    Prerequisites: Secrets Configuration

    Before deploying the pipeline to Databricks production targets, you must register your Redpanda Cloud credentials inside your workspace's native encrypted Secret Manager using the Databricks CLI.

    ```bash
        # 1. Create the secure isolated credential scope
        databricks secrets create-scope deltastreams_scope

        # 2. Put your username and password keys into the scope
        databricks secrets put-secret deltastreams_scope redpanda_user --string-value "your-username"
        databricks secrets put-secret deltastreams_scope redpanda_password --string-value "your-password"

        # 3. (Optional) Verify your keys are registered successfully
        databricks secrets list-secrets deltastreams_scope
    ```

    Detabricks Deploy CLI Commands:

    ```bash

        # Copy geoip files to cloud volume (clear other dirs from data first)
        databricks fs cp -r ./data/ dbfs:/Volumes/main/dev/udp_stream_volume/raw_sources/

        # Deploy bundle and run build_geoip_dimension (Do this whenever you update your geoip files)
        databricks bundle deploy -t prod && databricks bundle run build_geoip_dimension

        # Deploy and execute a sequential batch run
        databricks bundle deploy -t prod && databricks bundle run udp_batch_pipeline

        # Deploy and activate the infinite live streaming pipeline (executes 1 task at each layer and pause)
        databricks bundle deploy -t prod && databricks bundle run udp_stream_pipeline

        # Control Stream behavior for udp stream pipeline. Note this will immediately stream each task!
        databricks bundle deploy -t prod --var="stream_status=UNPAUSED"

        # Batch: Run only gold task
        databricks bundle run udp_stream_pipeline --only run_gold_analysis

        # Batch: Run everything after a specific task (notice the "+") - runs silver -> gold
        databricks bundle run udp_stream_pipeline --only run_silver_cleansing+

    ```

5. Infrastructure State Resets & Schema Evolution Testing

    Our architecture methodology includes non-destructive isolation pipelines. To simulate fresh environment provisioning or run validation suites against updated schema evolution rules, the following routine resets cluster state indicators cleanly. Open a notebook, select the serverless compute (not sql warehouse), and run these commands in separate cells:

    ```python
        dbutils.fs.rm("dbfs:/Volumes/main/dev/udp_stream_volume/checkpoints/bronze", True)
        dbutils.fs.rm("dbfs:/Volumes/main/dev/udp_stream_volume/checkpoints/silver", True)
        dbutils.fs.rm("dbfs:/Volumes/main/dev/udp_stream_volume/checkpoints/gold", True)
    ```

    ```sql
        %sql
        TRUNCATE TABLE main.dev.bronze_raw;
        TRUNCATE TABLE main.dev.silver_clean;
        TRUNCATE TABLE main.dev.silver_dlq;
        TRUNCATE TABLE main.dev.gold_alerts;
    ```

## Extra

The stream flag --stream:

Local Machine --stream unset reads earliest offset, processing 1 at a time. Setting it reads latest waiting for new data

Databricks Cloud --stream unset processes all accumlated messasges. Seting it streams continuously from latest.

Another note: Running `databricks bundle run udp_stream_pipeline` will only run each task once all in parallel, so
it is likely gold and silver won't pickup data. This method may be not useful but it exists for now.
So for now to trigger one load for each layer just use `databricks bundle deploy && databricks bundle run udp_batch_pipeline`.

---

## Enterprise Architecture & Consulting

This repository is a specialized reference architecture for teams looking to orchestrate production-grade streaming pipelines using **Databricks Asset Bundles (DABs), PySpark Structured Streaming, and Redpanda**.

If you are trying to deploy a similar edge-to-lakehouse stack but are running into configuration issues, serverless task loop constraints, or pipeline errors, you can book a brief technical session with us to look over your setup.

* **Custom Deployments:** Need to adapt this exact DAB blueprint, Vector edge config, or Redpanda stream pipeline to your own cloud infrastructure?
* **Get in Touch:** Connect with our principal architect on [LinkedIn](https://www.linkedin.com/in/noah-douglas-garner/) or schedule a brief [Technical Exploration Call](https://calendly.com/noahdouglasgarner/initial-sync) to walk through your architecture.

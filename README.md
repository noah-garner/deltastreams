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

* **Zero JVM Ingestion Plane:** Uses an ultra-lightweight Rust daemon (**Vector**) to process high-throughput UDP syslog packages at the network edge, keeping client memory footprints near zero.
* **Asynchronous Event Buffering:** Utilizes a distributed **Redpanda Cloud** cluster to provide structural backpressure control, ensuring zero loss of stream data even if downstream analytics compute nodes are offline.
* **Defensive Pipeline Protections (DLQ Routing):** Employs explicit structural checks to route malformed or broken payload strings containing `CORRUPT_BYTE_STREAM` to an isolated Dead Letter Queue while valid packets flow smoothly.
* **Stateful Streaming Aggregations:** Computes real-time rolling metrics in the Gold layer to flag distributed network brute-force security threats crossing defined volume thresholds.

## Quickstart

1. Requirements

    * Databricks CLI installed & configured

    * Python 3.10+ & uv package manager

2. Run Locally

    ```bash

        # We have real live honeypots feeding redpanda, no use for docker anymore
        # Local test to pull real internet data straight from the cloud queue to test processing layers
        ./scripts/deploy_grid.sh

        # Run Bronze Kafka Ingest
        uv run src/01_bronze_kafka_ingest.py \
        --bootstrap_servers "localhost:9092" \
        --topic "network-telemetry" \
        --checkpoint_path "data/checkpoints/kafka_bronze" \
        --target_table "bronze_kafka_stream" \
        --target_path "data/tables/bronze_kafka_stream" \
        --format_type "json"

        # Run Silver
        uv run src/02_silver_kafka_cleansing.py \
        --source_table "data/tables/kafka_bronze_clickstream" \
        --silver_table "data/tables/silver_kafka_stream" \
        --dlq_table "data/tables/silver_kafka_stream_dlq" \
        --checkpoint_path "data/checkpoints/silver_kafka" \
        --is_local

        # Run Gold
        uv run src/03_gold_security_alerts.py \    
        --source_table "data/tables/silver_kafka_stream" \
        --gold_table "data/tables/gold_security_alerts" \
        --checkpoint_path "data/checkpoints/gold_security" \
        --is_local
    ```

3. Inspect Data Locally

    Open the provided Jupyter notebooks under `/notebooks` to query and validate the results across each layer.

4. Deploy to Databricks

    ```bash
        # Deploy bundle and run job
        databricks bundle deploy && databricks bundle run udp_stream_pipeline
    ```

5. Query Delta Tables

    ```sql
        -- Describe the structural layout of your active bronze stream table
        DESCRIBE EXTENDED main.bronze.kafka_bronze_clickstream;

        -- Query the contents of your bronze data layer
        SELECT * FROM main.bronze.kafka_bronze_clickstream LIMIT 10;

        -- Inspect your Clean Silver Table to confirm regex parsing and try_cast performance
        SELECT host, source_type, src_ip, dst_ip, protocol, src_port, dst_port, _processed_at 
        FROM main.bronze.kafka_silver_clean 
        LIMIT 20;

        -- Inspect your Dead Letter Queue to verify the corrupt packet isolation
        SELECT host, port, message, _dlq_reason, _processed_at 
        FROM main.bronze.kafka_silver_dlq 
        LIMIT 20;

        -- Check your active security alerts ordered by the highest violation volume
        SELECT alert_window_start, alert_window_end, attacker_ip, protocol, request_count, _alert_triggered_at
        FROM main.bronze.kafka_gold_security_alerts
        ORDER BY request_count DESC;

        -- Run a verification query to see which IP addresses are generating the most overall traffic
        SELECT src_ip, protocol, count(*) as total_packets
        FROM main.bronze.kafka_silver_clean
        WHERE src_ip IS NOT NULL 
        AND src_ip != ''   -- This explicitly filters out the empty regex matches
        GROUP BY src_ip, protocol
        ORDER BY total_packets DESC;

        -- Identifies malicious IP addresses generating more than 20 failed login attempts over a sliding time window.
        SELECT 
            window.start AS alert_window,
            src_ip AS attacker_ip,
            COUNT(*) AS total_brute_force_attempts,
            COLLECT_SET(dpt) AS targeted_ports,
            'CRITICAL BRUTE FORCE DETECTED' AS incident_status
        FROM main.dev.gold_security_alerts
        GROUP BY src_ip, window
        HAVING total_brute_force_attempts > 20
        ORDER BY total_brute_force_attempts DESC;

        -- Displays the unparseable, malformed data records that were safely isolated from the main analytics path.
        SELECT 
            timestamp,
            raw_payload,
            error_message,
            'PIPELINE PROTECTED' AS isolation_status
        FROM main.dev.silver_kafka_stream_dlq
        LIMIT 5;
    ```

# DeltaStreams 🚀

> **A unified, production-grade edge-to-lakehouse streaming reference architecture built with Databricks Asset Bundles (DABs), Auto Loader, Vector, Redpanda, and Delta Lake.**

---

## Architecture Overview

DeltaStreams demonstrates a fault-tolerant, multi-track **Medallion Architecture** handling both application file ingestion and volatile, high-velocity network packet streams without breaking downstream production tables.

```text
  TRACK 1: FILE INGESTION                      TRACK 2: NETWORK TELEMETRY
┌───────────────────────────┐                 ┌───────────────────────────┐
│Stream Replayer (Kaggle)   │                 │ Python UDP Packet Blaster │
└─────────────┬─────────────┘                 └─────────────┬─────────────┘
              │                                             │ (Raw Syslog UDP Strings)
              ▼                                             ▼
┌───────────────────────────┐                 ┌───────────────────────────┐
│   Landing Zone (Volume)   │                 │   Vector Edge Listener    │
└─────────────┬─────────────┘                 └─────────────┬─────────────┘
              │ (Auto Loader)                               │ (JSON Format Translation)
              ▼                                             ▼
┌───────────────────────────┐                 ┌───────────────────────────┐
│ Bronze Layer: Clickstream │                 │ Redpanda Broker (Kafka)   │
└─────────────┬─────────────┘                 └─────────────┬─────────────┘
              │                                             │ (Streaming Spark Query)
              │                                             ▼
              │                               ┌───────────────────────────┐
              │                               │   Bronze Layer: Kafka     │
              │                               └─────────────┬─────────────┘
              │                                             │
              └──────────────────────┬──────────────────────┘
                                     │
                                     ▼
            ┌──────────────────────────────────────────────────────────┐
            │                    Silver Cleansing                      │
            │     (Try Cast, Watermarking, Deduplication & Schema)     │
            └───────────────────┬─────────────────┬────────────────────┘
                                │                 │
                    Valid Data  │                 │ Corrupt / Failed
                                ▼                 ▼
                ┌─────────────────────────┐   ┌─────────────────────────┐
                │      Silver Valid       │   │       Silver DLQ        │
                │   (Liquid Clustering)   │   │     (Error Capture)     │
                └─────────────────────────┘   └─────────────────────────┘
```

## Key Features

* **Dual-Track Streaming Engine:** Accommodates high-volume file ingestion via Databricks Auto Loader alongside continuous live message streams via native Kafka/Redpanda integrations.
* **Zero JVM Ingestion Plane:** Uses an ultra-lightweight Rust daemon (**Vector**) to process UDP logs at the edge and pipes them into a high-throughput C++ broker (**Redpanda**), keeping edge memory overhead near zero.
* **Zero Pipeline Crashes (DLQ Routing):** Uses `try_cast` and explicit error checks to route unparseable or corrupted stream payloads (like intentional `CORRUPT_BYTE_STREAM` lines) to a dedicated Dead Letter Queue while clean data flows continuously.
* **Late-Arriving Data Management:** Configured with 10-minute watermarking and event-time deduplication to handle out-of-order logs across web and infrastructure components.
* **High-Performance Storage:** Leverages Liquid Clustering (`CLUSTER BY (event_date, event_type)`) in Unity Catalog for optimal query performance.
* **Databricks Asset Bundles:** Entire workflow is declarative, versioned, and deployable across cloud environments via CLI bundle commands.

## Repository Structure

```text
.
├── notebooks/
│   ├── 01_query_bronze.ipynb       # Inspect file & kafka bronze layer row counts
│   └── 02_query_silver.ipynb       # Inspect cleansed valid data and DLQ records
├── src/
│   ├── 00_network_udp_blaster.py   # Raw network infrastructure UDP simulator
│   ├── 00_stream_replayer.py       # Micro-batch stream generator (Kaggle dataset)
│   ├── 01_bronze_autoloader.py     # Auto Loader file ingestion into Delta Lake
│   ├── 01_bronze_kafka_ingest.py   # Continuous Spark Kafka stream ingestion
│   └── 02_silver_cleansing.py      # Transformations, Watermarking, & DLQ split
├── databricks.yml                  # Databricks Asset Bundle deployment config
├── docker-compose.yml              # Single-core developer Redpanda & Vector stack
├── pyproject.toml                  # Project dependencies managed via uv
├── vector.yml                      # Edge pipeline routing and socket configuration
└── README.md
```

## Quickstart

1. Requirements

    * Databricks CLI installed & configured

    * Python 3.10+ & uv package manager

    * Docker and Docker Compose (for local stream testing)

2. Local Setup & Simulation

    ```bash
        # Step 1: Install dependencies
        uv sync

        # Step 2: Run the stream replayer to generate local landing zone batches
        uv run src/00_stream_replayer.py

        # Step 3: Run Bronze Auto Loader ingestion
        uv run src/01_bronze_autoloader.py \
            --source_path "data/landing_zone" \
            --checkpoint_path "data/checkpoints/bronze_ingest" \
            --target_table "bronze_clickstream" \
            --target_path "data/tables/bronze_clickstream" \
            --format_type "json"

        # Step 4: Run Silver Cleansing, Deduplication, and DLQ routing
        uv run src/02_silver_cleansing.py \
            --source_table "data/tables/bronze_clickstream" \
            --silver_table "data/tables/silver_clickstream" \
            --dlq_table "data/tables/silver_clickstream_dlq" \
            --checkpoint_path "data/checkpoints" \
            --is_local
    ```

3. Inspect Data Locally

    Open the provided Jupyter notebooks to query and validate the results across layers:

    * notebooks/01_query_bronze.ipynb

    * notebooks/02_query_silver.ipynb

4. Deploy to Databricks

    ```bash

        # Step 1. Generate local micro-batches
        uv run src/00_stream_replayer.py

        # Step 2. Validate bundle configuration
        databricks bundle validate

        # Step 3. Deploy bundle to development target
        databricks bundle deploy -t dev

        # Step 4. Upload local batches to UC volume 
        # (dev target uses schema: dev_<username>_bronze)
        databricks fs cp -r data/landing_zone \
        dbfs:/Volumes/main/<your_dev_schema>/clickstream_volume/landing_zone

        # Step 5. Run the deployed pipeline
        databricks bundle run -t dev clickstream_medallion_pipeline
    ```

## Quickstart - UDP Stream

Commands:

```bash
# Restart containers
docker compose down && docker compose up -d

# Inspect Vector
docker exec -it docker-vector-1 vector tap

# Run udp_blaster
uv run src/00_network_udp_blaster.py

# Run Bronze Kafka Ingest
uv run src/01_bronze_kafka_ingest.py \
  --bootstrap_servers "localhost:9092" \
  --topic "network-telemetry" \
  --checkpoint_path "data/checkpoints/kafka_bronze" \
  --target_table "bronze_kafka_stream" \
  --target_path "data/tables/bronze_kafka_stream" \
  --format_type "json"

```

## Feedback & Customer Discovery

This project is an open-source reference architecture built to explore streaming data quality patterns on Databricks.

Have feedback or handle clickstream data differently? **Feel free to open an Issue or PR.**

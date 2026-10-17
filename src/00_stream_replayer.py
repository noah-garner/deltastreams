import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd

# Workspace directory setup
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing_zone"
KAGGLE_CSV_PATH = DATA_DIR / "2019-Oct.csv"

# Ensure landing zone directory exists
LANDING_DIR.mkdir(parents=True, exist_ok=True)


def format_timestamp(dt_obj):
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S UTC")


def generate_kaggle_fallback_chunk(num_rows=1000):
    """Generates Kaggle-structured e-commerce events if raw 2019-Oct.csv is missing."""
    event_types = ["view", "cart", "remove_from_cart", "purchase"]
    categories = [
        "electronics.smartphone",
        "apparel.shoes",
        "computers.notebook",
        "housewares.kitchen",
    ]
    brands = ["samsung", "apple", "xiaomi", "huawei", "lenovo"]

    events = []
    now = datetime.now(timezone.utc)

    for _ in range(num_rows):
        # 10% chance of late-arriving mobile logs (5 to 15 minutes late)
        is_late = random.random() < 0.10
        event_time = (
            now - timedelta(minutes=random.randint(5, 15)) if is_late else now
        )

        event = {
            "event_time": format_timestamp(event_time),
            "event_type": random.choices(
                event_types, weights=[0.70, 0.15, 0.10, 0.05]
            )[0],
            "product_id": random.randint(1000000, 9999999),
            "category_id": random.randint(
                2053013550000000000, 2053013560000000000
            ),
            "category_code": random.choice(categories),
            "brand": random.choice(brands),
            "price": round(random.uniform(10.0, 1200.0), 2),
            "user_id": random.randint(500000000, 599999999),
            "user_session": str(uuid.uuid4()),
            "device_type": random.choice(["ios", "android", "web"]),
        }

        # Inject 2% malformed string prices to test Dead Letter Queue / _rescued_data
        if random.random() < 0.02:
            event["price"] = "CORRUPT_PRICE_PAYLOAD"

        events.append(event)
    return pd.DataFrame(events)


def stream_kaggle_dataset(chunk_size=1000, delay_seconds=2, total_batches=5):
    """Reads Kaggle 2019-Oct.csv in chunks and emits micro-batch JSON files into landing_zone."""
    print(
        f"Starting Stream Replayer -> Output: {LANDING_DIR.resolve()}"
    )

    file_exists = KAGGLE_CSV_PATH.exists()

    if file_exists:
        print(f"Found Kaggle CSV at {KAGGLE_CSV_PATH.resolve()}")
        print(
            f"Streaming {total_batches} micro-batches ({chunk_size} events/batch)..."
        )
        csv_reader = pd.read_csv(KAGGLE_CSV_PATH, chunksize=chunk_size)
    else:
        print(
            f"2019-Oct.csv not found at {KAGGLE_CSV_PATH.resolve()}. Falling back to synthetic stream..."
        )

    for batch_idx in range(1, total_batches + 1):
        if file_exists:
            try:
                chunk = next(csv_reader)
            except StopIteration:
                print("🏁 Reached end of Kaggle CSV dataset.")
                break

            # Standardize missing/NaN values for JSON serialization
            chunk = chunk.where(pd.notnull(chunk), None)

            # Ensure device_type column exists
            if "device_type" not in chunk.columns:
                chunk["device_type"] = [
                    random.choice(["ios", "android", "web"]) for _ in range(len(chunk))
                ]

            # Inject 2% corrupt price strings into real CSV data for DLQ testing
            corrupt_mask = [random.random() < 0.02 for _ in range(len(chunk))]
            chunk["price"] = chunk["price"].astype("object")
            chunk.loc[corrupt_mask, "price"] = "CORRUPT_PRICE_PAYLOAD"

            records = chunk.to_dict(orient="records")
        else:
            chunk = generate_kaggle_fallback_chunk(num_rows=chunk_size)
            records = chunk.to_dict(orient="records")

        # Write micro-batch as newline-delimited JSON (NDJSON)
        batch_filename = (
            LANDING_DIR / f"clickstream_batch_{int(time.time())}_{batch_idx}.json"
        )

        with open(batch_filename, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

        print(
            f"Batch {batch_idx}/{total_batches} emitted -> {batch_filename.name} ({len(records)} events)"
        )
        time.sleep(delay_seconds)

    print("\nStream Replayer complete! Check your landing_zone directory.")


if __name__ == "__main__":
    # Generates 5 micro-batches of 1,000 events each
    stream_kaggle_dataset(chunk_size=1000, delay_seconds=1, total_batches=5)
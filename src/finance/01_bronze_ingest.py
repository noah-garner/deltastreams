import os
import time
from datetime import datetime, UTC
from kafka import KafkaConsumer
import polars as pl

def main():
    bootstrap_servers = "localhost:19092"
    topic = "liquidity_transactions"
    target_path = "./data/tables/bronze_liquidity"
    
    os.makedirs(target_path, exist_ok=True)
    
    # Initialize the real-time continuous consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id="bronze_ingest_group"
    )
    
    schema = {
        "kafka_key": pl.String,
        "value": pl.String,
        "partition": pl.Int32,
        "offset": pl.Int64,
        "_ingested_at": pl.String
    }
    
    print("Continuous Bronze Delta ingestion active. Listening for events...")
    
    while True:
        try:
            msg_pack = consumer.poll(timeout_ms=1000)
            records = []
            
            for tp, messages in msg_pack.items():
                for msg in messages:
                    records.append({
                        "kafka_key": msg.key.decode('utf-8') if msg.key else None,
                        "value": msg.value.decode('utf-8') if msg.value else None,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "_ingested_at": datetime.now(UTC).isoformat()
                    })
            
            if records:
                # Build micro-batch dataframe
                df = pl.DataFrame(records, schema=schema)
                
                # Append micro-batch safely straight into local Delta Lake Parquet structures
                df.write_delta(target_path, mode="append")
                
                # Commit offset coordinates to broker only after successful disk persistence
                consumer.commit()
                print(f"Successfully appended micro-batch of {len(records)} raw events to Bronze Delta layer.")
                
        except KeyboardInterrupt:
            print("\nShutting down Bronze continuous ingestion cleanly.")
            break
        except Exception as e:
            print(f"Error encountered during stream ingest cycle: {e}")
            
        # Standard microsecond throttle to prevent CPU burn when topic has 0 velocity
        time.sleep(0.1)

if __name__ == "__main__":
    main()

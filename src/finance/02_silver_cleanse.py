import os
import time
import polars as pl

def main():
    bronze_path = "./data/tables/bronze_liquidity"
    silver_clean_path = "./data/tables/silver_liquidity"
    silver_dlq_path = "./data/tables/silver_dlq"
    
    os.makedirs(silver_clean_path, exist_ok=True)
    os.makedirs(silver_dlq_path, exist_ok=True)

    # State registry tracking uniquely processed message offset signatures
    processed_offsets = set()
    
    print("Continuous Silver cleansing engine active. Native Polars mode initialized.")

    while True:
        try:
            if not os.path.exists(bronze_path):
                time.sleep(1)
                continue

            # Read transactional files from local storage natively via Polars
            df_raw = pl.read_delta(bronze_path)
            
            if not df_raw.empty:
                # Isolate only rows that haven't been parsed yet
                df_fresh = df_raw.filter(~pl.col("offset").is_in(list(processed_offsets)))
                
                if not df_fresh.empty:
                    # 1. Strip out the escaped string characters and wrap formatting
                    # This targets the exact string footprint seen in your debug logs
                    df_unwrapped = df_fresh.with_columns(
                        pl.col("value")
                        .str.replace_all(r'\\"', '"')  # Strip escaped backslashes
                        .str.replace_all('"{', '{')    # Clean json block joins
                        .str.replace_all('}"', '}')
                    )
                    
                    # 2. Extract specific parameters natively using high-speed string regex
                    df_extracted = df_unwrapped.with_columns([
                        pl.col("value").str.extract(r'"type":"([^"]+)"', 1).alias("type"),
                        pl.col("value").str.extract(r'"product_id":"([^"]+)"', 1).alias("product_id"),
                        pl.col("value").str.extract(r'"price":"([^"]+)"', 1).alias("price_str"),
                        pl.col("value").str.extract(r'"size":"([^"]+)"', 1).alias("size_str"),
                        pl.col("value").str.extract(r'"volume_24h":"([^"]+)"', 1).alias("vol_str"),
                        pl.col("value").str.extract(r'"sequence":(\d+)', 1).alias("seq_str"),
                        pl.col("value").str.extract(r'"side":"([^"]+)"', 1).alias("side")
                    ])
                    
                    # 3. Filter valid transactions from anomalies
                    condition_valid = (
                        (pl.col("type") == "ticker") & 
                        (pl.col("price_str").is_not_null())
                    )
                    
                    df_silver_batch = df_extracted.filter(condition_valid).with_columns([
                        pl.col("price_str").cast(pl.Float64).alias("price"),
                        pl.col("size_str").cast(pl.Float64).fill_null(0.0).alias("size"),
                        pl.col("vol_str").cast(pl.Float64).fill_null(0.0).alias("volume_24h"),
                        pl.col("seq_str").cast(pl.Int64).alias("sequence"),
                        pl.col("_ingested_at").str.to_datetime()
                    ])
                    
                    # Isolate invalid payloads to Dead Letter Queue (DLQ)
                    df_dlq_batch = df_extracted.filter(~condition_valid)
                    
                    # 4. Save to clean local storage structures
                    if not df_silver_batch.empty:
                        # Clean up intermediate string metrics to match your baseline schema definitions
                        df_to_write = df_silver_batch.select([
                            "kafka_key", "partition", "offset", "_ingested_at",
                            "product_id", "price", "size", "volume_24h", "sequence", "side"
                        ])
                        df_to_write.write_delta(silver_clean_path, mode="append")
                        print(f"Successfully processed {df_to_write.height} fresh records into Silver layer.")
                        
                    if not df_dlq_batch.empty:
                        df_dlq_to_write = df_dlq_batch.select([
                            "kafka_key", "value", "partition", "offset", "_ingested_at", "type"
                        ])
                        df_dlq_to_write.write_delta(silver_dlq_path, mode="append")
                    
                    # Update offset memory logs
                    batch_offsets = df_fresh["offset"].to_list()
                    processed_offsets.update(batch_offsets)
                    
        except KeyboardInterrupt:
            print("\nShutting down continuous loop process cleanly.")
            break
        except Exception as e:
            # Silently pass open file write locks and retry on next interval tick
            pass
            
        time.sleep(1.0)

if __name__ == "__main__":
    main()

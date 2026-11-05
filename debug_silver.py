import duckdb
import os

def main():
    bronze_path = "./data/tables/bronze_liquidity/*.parquet"
    if not os.path.exists("./data/tables/bronze_liquidity"):
        print("Bronze directory not found.")
        return

    con = duckdb.connect()
    
    print("--- 1. ACTIVE RAW SCHEMA INSIDE YOUR PARQUET FILES ---")
    try:
        # Fetching column definitions as a dataframe to bypass layout display truncation
        schema_df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{bronze_path}');").df()
        print(schema_df)
    except Exception as e:
        print(f"Failed to read schema: {e}")
    
    print("\n--- 2. RAW RECORD VALUE SAMPLE ---")
    try:
        values_df = con.execute(f"SELECT value FROM read_parquet('{bronze_path}') LIMIT 3;").df()
        for idx, row in values_df.iterrows():
            print(f"Row {idx+1}: {row['value']}\n")
    except Exception as e:
        print(f"Failed to read raw value text sample: {e}")

if __name__ == "__main__":
    main()

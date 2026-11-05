import os
import time
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

GOLD_BASE_DIR = "./data/tables/gold_alerts"
GOLD_PATH = "./data/tables/gold_alerts/*/*.parquet"

@st.cache_resource
def get_db():
    return duckdb.connect(database=":memory:")

def main():
    st.title("Market Regime Matrix")
    st.subheader("Real-Time Manipulation Detection")
    st.markdown("---")
    
    if not os.path.exists(GOLD_BASE_DIR):
        st.info(f"Waiting for Gold layer directory to initialize...")
        time.sleep(2)
        return

    con = get_db()
    
    try:
        unique_assets_df = con.execute(f"""
            SELECT DISTINCT product_id 
            FROM read_parquet('{GOLD_PATH}')
            ORDER BY product_id ASC;
        """).df()
        asset_list = unique_assets_df["product_id"].tolist()
    except Exception:
        asset_list = ["BTC-USD", "ETH-USD", "SOL-USD"]

    selected_asset = st.sidebar.selectbox("Target Asset Corridor", asset_list)
    
    placeholder = st.empty()

    while True:
        try:
            query = f"""
                SELECT 
                    _ingested_at as timestamp,
                    product_id,
                    price,
                    size,
                    price_velocity,
                    size_velocity_ratio,
                    market_regime,
                    anomaly_score
                FROM read_parquet('{GOLD_PATH}')
                WHERE product_id = '{selected_asset}'
                ORDER BY anomaly_score ASC, _ingested_at DESC;
            """
            
            df = con.execute(query).df()

            if not df.empty:
                latest = df.iloc[0]
                
                with placeholder.container():
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Asset", str(latest["product_id"]))
                    col2.metric("Price", f"${float(latest['price']):,.2f}")
                    col3.metric("Status", str(latest["market_regime"]))
                    
                    st.markdown("### High-Frequency Tick Stream")
                    
                    chart_df = df.copy()
                    chart_df = chart_df.sort_values("timestamp").reset_index(drop=True)
                    
                    st.line_chart(
                        chart_df["price"],
                        use_container_width=True
                    )
                    
                    st.markdown("### Live Threat Ledger")
                    st.dataframe(
                        df[["timestamp", "product_id", "price", "size", "price_velocity", "size_velocity_ratio", "market_regime"]], 
                        use_container_width=True, 
                        hide_index=True
                    )
                    
        except Exception as e:
            st.error(f"Engine mapping sync issue: {e}")
            
        time.sleep(1)

if __name__ == "__main__":
    main()

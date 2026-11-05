import os
import time
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

GOLD_PATH = "./data/tables/gold_alerts/*/*.parquet"

@st.cache_resource
def get_db():
    return duckdb.connect(database=":memory:")

def main():
    st.title("Financial Market Matrix")
    st.subheader("Real-Time Regime and Structural Anomaly Detection Engine")
    st.markdown("---")
    
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

    # FIX: Added unique static key argument to eliminate the duplicate selectbox error
    selected_asset = st.sidebar.selectbox("Target Asset Corridor", asset_list, key="asset_selector_widget")

    placeholder = st.empty()

    while True:
        try:
            # FIX: Replaced threat_label with market_regime to match your actual file schema columns
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
                
                total_scanned = len(df)
                anomalies = df[df["anomaly_score"] == -1]
                total_anomalies = len(anomalies)
                contamination_rate = (total_anomalies / total_scanned) * 100 if total_scanned > 0 else 0
                
                with placeholder.container():
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Active Data Feed Pipeline", str(latest["product_id"]))
                    col2.metric("Raw Packets Ingested", f"{total_scanned:,} ticks")
                    col3.metric("ML Flagged Anomalies", f"{total_anomalies} vectors")
                    col4.metric("Engine Contamination Density", f"{contamination_rate:.1f}%")
                    
                    st.markdown("---")
                    
                    st.markdown("### Real-Time Pipeline Micro-Feature Engineering")
                    
                    feat_col1, feat_col2, feat_col3 = st.columns(3)
                    with feat_col1:
                        st.metric("Price Momentum Delta", f"{float(latest['price_velocity']):+,.2f} pts")
                    with feat_col2:
                        st.metric("Order Book Volatility Baselines", f"{float(latest['size_velocity_ratio']):,.2f}x")
                    with feat_col3:
                        if int(latest["anomaly_score"]) == -1:
                            st.error(f"SYSTEM REGIME: {str(latest['market_regime'])}")
                        else:
                            st.success("SYSTEM REGIME: NORMAL MARKET EXECUTION")
                    
                    st.markdown("---")
                    
                    st.markdown("### Sequential Vector Space Analytics")
                    chart_df = df.copy()
                    chart_df = chart_df.sort_values("timestamp").reset_index(drop=True)
                    st.line_chart(chart_df["price"], use_container_width=True)
                    
                    st.markdown("### Live Market Ledger (Direct ML Feature Engine Feed)")
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

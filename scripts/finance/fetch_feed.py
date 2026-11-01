import json
import websocket

def run_websocket_stream(log_path):
    url = "wss://ws-feed.exchange.coinbase.com"
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD"],
        "channels": ["ticker"]
    }

    print(f"Establishing Live Enterprise WebSocket Feed -> {log_path}")

    try:
        # 1. Establish connection (handles RFC-6455 masking & handshakes automatically)
        ws = websocket.create_connection(url, timeout=10)
        print("Handshake successful. Connected to Coinbase WebSocket.")

        # 2. Transmit the payload
        ws.send(json.dumps(subscribe_msg))

        # 3. Stream data natively to the tracking file
        with open(log_path, "a") as f:
            while True:
                data = ws.recv()
                if not data:
                    print("Connection closed by remote host.")
                    break
                
                # Write and instantly flush the clean message line for Vector
                f.write(data + "\n")
                f.flush()

    except Exception as e:
        print(f"Streaming error occurred: {e}")

if __name__ == "__main__":
    run_websocket_stream("./data/market.log")
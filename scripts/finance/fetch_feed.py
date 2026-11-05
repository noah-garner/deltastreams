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
        ws = websocket.create_connection(url, timeout=10)
        print("Handshake successful. Connected to Coinbase WebSocket.")

        ws.send(json.dumps(subscribe_msg))

        with open(log_path, "a") as f:
            while True:
                data = ws.recv()
                if not data:
                    print("Connection closed by remote host.")
                    break
                
                f.write(data + "\n")
                f.flush()

    except Exception as e:
        print(f"Streaming error occurred: {e}")

if __name__ == "__main__":
    run_websocket_stream("./data/market.log")
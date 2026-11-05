import json
import time
import random
from datetime import datetime, UTC
from kafka import KafkaProducer

def create_base_event(product_id, event_type, price, size):
    """Generates standard schema matching your exchange feed."""
    return {
        "type": event_type,
        "product_id": product_id,
        "price": str(round(price, 2)),
        "size": str(round(size, 4)),
        "volume_24h": "15420.45",
        "sequence": random.randint(1000000, 9999999),
        "_ingested_at": datetime.now(UTC).isoformat()
    }

def simulate_order_book_spoofing(producer, topic, product_id, base_price):
    """
    Attacks via Spoofing:
    Floods the broker with massive fake buy orders, instantly cancelled.
    """
    print(f"[!] ATTACK: Launching Order Book Spoofing on {product_id}...")
    for _ in range(50):
        # Fake massive buy wall just below the current price
        fake_bid = base_price * 0.998
        fake_size = random.uniform(50.0, 120.0) # Massive artificial size
        
        event = create_base_event(product_id, "ticker", fake_bid, fake_size)
        producer.send(topic, value=json.dumps(event).encode('utf-8'))
        
        # Microsecond scale pauses
        time.sleep(0.005)
    
    producer.flush()
    print("[+] Spoofing wave sent. (Simulated instant cancellation via micro-burst).")

def simulate_flash_crash(producer, topic, product_id, start_price):
    """
    Attacks via Flash Crash:
    Drops price violently by 15% across a precise 2-second window.
    """
    print(f"[!] ATTACK: Executing 15% Flash Crash on {product_id} across 2 seconds...")
    current_price = start_price
    steps = 100
    sleep_duration = 2.0 / steps
    price_drop_per_step = (start_price * 0.15) / steps

    for i in range(steps):
        current_price -= price_drop_per_step * random.uniform(0.8, 1.2)
        crash_size = random.uniform(5.0, 30.0) # High panic volume
        
        event = create_base_event(product_id, "ticker", current_price, crash_size)
        producer.send(topic, value=json.dumps(event).encode('utf-8'))
        time.sleep(sleep_duration)
        
    producer.flush()
    print(f"[+] Flash crash complete. Bottomed out at: ${current_price:.2f}")

def simulate_wash_trading(producer, topic, product_id, base_price):
    """
    Attacks via Wash Trading:
    High-frequency circular trading back and forth to pump artificial volume.
    """
    print(f"[!] ATTACK: Pumping artificial volume via Wash Trading on {product_id}...")
    for _ in range(100):
        # Microscopic price deltas, but massive artificial volume execution
        wash_price = base_price + random.uniform(-0.02, 0.02)
        wash_size = random.uniform(10.0, 45.0)
        
        event = create_base_event(product_id, "ticker", wash_price, wash_size)
        producer.send(topic, value=json.dumps(event).encode('utf-8'))
        time.sleep(0.002)
        
    producer.flush()
    print("[+] Wash trading volume cycle injected.")

def main():
    topic = "liquidity_transactions"
    broker = "localhost:19092"
    product_id = "BTC-USD"
    reference_price = 95000.0
    
    print(f"Connecting Attack Rig to Redpanda at {broker}...")
    try:
        producer = KafkaProducer(bootstrap_servers=[broker])
    except Exception as e:
        print(f"[-] Connection failed. Is Redpanda running? Error: {e}")
        return

    print("\n--- ATTACK COCKPIT ---")
    print("1: Order Book Spoofing (Massive fake orders)")
    print("2: Flash Crash (15% drop over 2s)")
    print("3: Wash Trading (Circular volume pumping)")
    print("4: Run full multi-stage vector sequence")
    
    choice = input("Select cyber-finance matrix to deploy: ")
    
    if choice == "1":
        simulate_order_book_spoofing(producer, topic, product_id, reference_price)
    elif choice == "2":
        simulate_flash_crash(producer, topic, product_id, reference_price)
    elif choice == "3":
        simulate_wash_trading(producer, topic, product_id, reference_price)
    elif choice == "4":
        simulate_order_book_spoofing(producer, topic, product_id, reference_price)
        time.sleep(1)
        simulate_wash_trading(producer, topic, product_id, reference_price)
        time.sleep(1)
        simulate_flash_crash(producer, topic, product_id, reference_price)
    else:
        print("Invalid strategy selected.")

if __name__ == "__main__":
    main()

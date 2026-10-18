import socket
import time
import random

# Network Target Configuration
UDP_IP = "127.0.0.1"
UDP_PORT = 5140

# Client Socket Initialization
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Data Profiles for Simulation
malicious_ips = ["185.220.101.5", "45.143.203.12"]
clean_ips = ["192.168.1.50", "192.168.1.100", "10.0.0.15"]

print("Blasting network telemetry stream... Press Ctrl+C to stop.")

while True:
    # -----------------------------------------------------------------
    # SCENARIO A: 5% Chance - Malformed Payload (Routes directly to DLQ)
    # -----------------------------------------------------------------
    if random.random() < 0.05:
        log = "!!! SYSTEM ERROR !!! CORRUPT_BYTE_STREAM TRUNCATED"
        print("Blasting broken payload to populate Dead Letter Queue (DLQ)...")
    
    # -----------------------------------------------------------------
    # SCENARIO B: 95% Chance - Valid Struct Data (Routes to Medallion)
    # -----------------------------------------------------------------
    else:
        # Sub-Scenario 1: 20% Chance - Aggressive Distributed Brute-Force
        if random.random() < 0.20:
            src_ip = random.choice(malicious_ips)
            # Targeting critical server access ports
            dpt = random.choice([22, 23, 3389, 445, 80])
            status = "Failed"
            msg = "Invalid credentials / Port Scan"
        
        # Sub-Scenario 2: 80% Chance - Normal Enterprise Traffic
        else:
            src_ip = random.choice(clean_ips)
            # Default web/secure communication ports
            dpt = random.choice([80, 443])
            status = "Accept"
            msg = "Established outbound connection"

        # Construct the valid syslog-style network payload string
        log = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} perimeter-fw01 SRC={src_ip} DST=10.0.0.5 PROTO=TCP DPT={dpt} STATUS={status} MSG='{msg}'"

    # Stream out the message payload over the active UDP channel
    sock.sendto(bytes(log, "utf-8"), (UDP_IP, UDP_PORT))
    
    # Randomize interval pacing to simulate authentic network packet jitter
    time.sleep(random.choice([0.05, 0.1, 0.2]))

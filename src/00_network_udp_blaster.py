import socket
import time
import random

UDP_IP = "127.0.0.1"
UDP_PORT = 5140

# Clean array of unparsed raw log entries to test your edge node
payloads = [
    "2026-10-16T15:45:00Z firewall-core-01 SRC=192.168.1.100 DST=10.0.0.5 PROTO=TCP SPT=443 DPT=5140",
    "2026-10-16T15:45:02Z perimeter-fw02 sshd: Failed password for root from 203.0.113.88 port 49152",
    "!!! CORRUPT_BYTE_STREAM_INVALID_UTF8_7761527361_NULL_EXCEPTION !!!"
]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print(f"Blasting network logs over UDP to Vector on port {UDP_PORT}... Press Ctrl+C to halt.")

try:
    while True:
        msg = random.choice(payloads)
        sock.sendto(bytes(msg, "utf-8"), (UDP_IP, UDP_PORT))
        time.sleep(0.4)  # Fires a network log package roughly every 400ms
except KeyboardInterrupt:
    print("\nUDP traffic simulation stopped cleanly.")

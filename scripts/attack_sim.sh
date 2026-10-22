echo "Launching distributed brute-force simulation..."
for host in 167.71.156.175 157.245.209.237 134.122.61.153; do
    echo "Spamming 50 parallel connection threads to $host..."
    for i in {1..50}; do
        # The '&' at the end makes it concurrent and lightning fast
        ssh -o ConnectTimeout=2 -o ConnectionAttempts=1 -o StrictHostKeyChecking=no invalid_delta_demo@$host 2>/dev/null &
    done
done
wait
echo "All concurrent attack vectors delivered!"
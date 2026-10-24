#!/bin/bash

TARGET_IP=$1
if [ -z "$TARGET_IP" ]; then
    echo "Error: Missing target IP address to purge."
    echo "Usage: ./clear_droplet.sh <public_ip>"
    exit 1
fi

echo "Wiping remote environment for root@${TARGET_IP}..."

ssh -i ~/.ssh/id_digitalocean root@${TARGET_IP} << 'REMOTE_EXEC'
    # Force kill and disable the systemd background daemon loops
    systemctl stop vector 2>/dev/null
    systemctl disable vector 2>/dev/null

    # Forcefully terminate any lingering or hanging binary execution processes
    killall -9 vector 2>/dev/null
    pkill -9 -f vector 2>/dev/null

    # Scrub out all configuration blueprints, environment tokens, and tracking directories
    rm -f /lib/systemd/system/vector.service
    rm -rf /etc/systemd/system/vector.service.d
    rm -rf /etc/vector
    rm -rf /var/lib/vector
    rm -f /usr/local/bin/vector
    rm -f /usr/bin/vector

    # Force systemd to purge its internal process runtime layout cache
    systemctl daemon-reload
    systemctl reset-failed

    echo "Remote server successfully purged back to stock state!"
REMOTE_EXEC

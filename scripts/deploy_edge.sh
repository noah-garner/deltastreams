#!/bin/bash
# ==============================================================================
# DELTASTREAMS: Edge Honeypot Provisioning & GitOps Deployment Automation Script
# Usage: ./deploy_edge.sh <target_droplet_public_ip>
# ==============================================================================

TARGET_IP=$1
if [ -z "$TARGET_IP" ]; then
    echo "Error: Missing target deployment IP address."
    echo "Usage: ./deploy_edge.sh <public_ip>"
    exit 1
fi

echo "Initiating Edge Deployment Sequence for root@${TARGET_IP}..."

# 1. Provision target directories natively on the remote host
echo "Syncing remote directory layouts..."
ssh -i ~/.ssh/id_digitalocean -o StrictHostKeyChecking=no root@${TARGET_IP} "mkdir -p /etc/vector /var/lib/vector"

# 2. Push your localized repository configurations up to the target machine via SCP
echo "Uploading core configurations and environment tokens..."
scp -i ~/.ssh/id_digitalocean ./vector.yml root@${TARGET_IP}:/etc/vector/vector.yaml
scp -i ~/.ssh/id_digitalocean ./.env root@${TARGET_IP}:/etc/vector/vector.env

# 3. Execute remote shell orchestration block to download and configure the engine
echo "Orchestrating remote installation and process structures..."
ssh -i ~/.ssh/id_digitalocean root@${TARGET_IP} << 'REMOTE_EXEC'
    # Download and install Vector binary natively using Bash
    if ! command -v vector &> /dev/null; then
        echo "Downloading Vector engine binary..."
        curl --proto '=https' --tlsv1.2 -sSfL https://sh.vector.dev > install_vector.sh
        bash install_vector.sh -y --prefix /usr/local
        rm install_vector.sh
    fi

    # Build the native systemd Linux background service architecture configuration
    echo "Registering systemd process definitions..."
    cat << 'EOF' > /lib/systemd/system/vector.service
[Unit]
Description=Vector Observability Agent
Documentation=https://vector.dev
After=network-online.target
Requires=network-online.target

[Service]
User=root
Group=root
ExecStart=/usr/local/bin/vector --config /etc/vector/vector.yaml --dangerously-allow-env-var-interpolation
ExecReload=/usr/local/bin/vector validate --config /etc/vector/vector.yaml
Restart=always

[Install]
WantedBy=multi-user.target
EOF

    # Configure the systemd environment payload injection overrides
    mkdir -p /etc/systemd/system/vector.service.d
    cat << 'EOF' > /etc/systemd/system/vector.service.d/override.conf
[Service]
EnvironmentFile=/etc/vector/vector.env
EOF

    # Trigger process manager reloads and ignite the background engine
    echo "Activating background edge pipeline..."
    systemctl daemon-reload
    systemctl enable vector
    systemctl restart vector

    echo "Remote Edge Initialization Complete!"
REMOTE_EXEC

echo "Deployment successfully executed! Vector is now running on root@${TARGET_IP}"

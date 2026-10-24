#!/bin/bash

TARGET_IP=$1
RP_USER=$2
RP_PASSWORD=$3

if [ -z "$TARGET_IP" ] || [ -z "$RP_USER" ] || [ -z "$RP_PASSWORD" ]; then
    echo "Error: Missing target deployment arguments."
    echo "Usage: ./deploy_edge.sh <public_ip> <rp_user> <rp_password>"
    exit 1
fi

echo "Initiating Edge Deployment Sequence for root@${TARGET_IP}..."

# Provision target directories natively on the remote host
echo "Syncing remote directory layouts..."
ssh -i ~/.ssh/id_digitalocean -o StrictHostKeyChecking=no root@${TARGET_IP} "mkdir -p /etc/vector /var/lib/vector"

# Push localized repository configurations up to the target machine via SCP
echo "Uploading core configurations..."
scp -i ~/.ssh/id_digitalocean ./vector.yml root@${TARGET_IP}:/etc/vector/vector.yaml

# Securely generate the target environment payload file directly on the host droplet
echo "Generating secure isolated environment file on host..."
ssh -i ~/.ssh/id_digitalocean root@${TARGET_IP} << EOF
    cat << 'ENV_EOF' > /etc/vector/vector.env
RP_USER=${RP_USER}
RP_PASSWORD=${RP_PASSWORD}
ENV_EOF
    chmod 600 /etc/vector/vector.env
EOF

# Execute remote shell orchestration block to download and configure the engine
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

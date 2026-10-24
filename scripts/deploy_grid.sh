#!/bin/bash
# ==============================================================================
# DELTASTREAMS: Fleet Edge Core Deployment Orchestration Loop
# ==============================================================================

# Clear out any legacy host keys for these specific endpoints to bypass warnings
for host in 167.71.156.175 157.245.209.237 134.122.61.153; do
    ssh-keygen -R $host
done

# Check that production variables are loaded in the shell environment before deploying
if [ -z "$rp_user" ] || [ -z "$rp_password" ]; then
    echo "Error: Environment variables 'rp_user' and 'rp_password' are required."
    echo "Please source your .env file or export them before running."
    exit 1
fi

# Sequential deployment orchestration loop via your GitOps script
for host in 167.71.156.175 157.245.209.237 134.122.61.153; do
    echo "========================================================================="
    echo "Deploying edge telemetry configurations to target IP: $host"
    echo "========================================================================="
    
    # Securely pass host, username, and password down into the provisioning layer
    GIT_SSH_COMMAND="ssh -o IdentitiesOnly=yes -o UserKnownHostsFile=/dev/null" \
    ./scripts/deploy_edge.sh "$host" "$RP_USER" "$RP_PASSWORD"
done
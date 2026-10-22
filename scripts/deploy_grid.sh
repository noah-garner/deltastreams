# 1. Clear out any legacy host keys for these specific endpoints to bypass warnings
for host in 167.71.156.175 157.245.209.237 134.122.61.153; do
    ssh-keygen -R $host
done

# 2. Sequential deployment orchestration loop via your GitOps script
for host in 167.71.156.175 157.245.209.237 134.122.61.153; do
    echo "===================================================================="
    echo "Deploying edge telemetry configurations to target IP: $host"
    echo "===================================================================="
    GIT_SSH_COMMAND="ssh -o IdentitiesOnly=yes -o UserKnownHostsFile=/dev/null" ./scripts/deploy_edge.sh $host
done
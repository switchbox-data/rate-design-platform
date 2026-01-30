#!/usr/bin/env bash
set -euo pipefail

# Set up EC2 instance (run once by admin). Idempotent: safe to run multiple times.
# Run from repo root: infra/dev-setup.sh (or from infra: ./dev-setup.sh)

echo "🚀 Setting up EC2 instance..."
echo

# Change to infra directory (script may be run from repo root or infra)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Initialize Terraform if needed
if [ ! -d ".terraform" ]; then
  echo "📦 Initializing Terraform..."
  terraform init
  echo
fi

# Apply Terraform configuration
echo "🏗️  Applying Terraform configuration..."
terraform apply -auto-approve
echo

# Get instance information
INSTANCE_ID=$(terraform output -raw instance_id)
AVAILABILITY_ZONE=$(terraform output -raw availability_zone)
PUBLIC_IP=$(terraform output -raw instance_public_ip 2>/dev/null || echo "")

echo "⏳ Waiting for instance to be ready..."
aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"
echo "✅ Instance is ready"
echo

# Wait for SSM agent to be ready (no SSH keys needed - uses AWS SSO!)
echo "⏳ Waiting for SSM agent to be ready..."
for i in {1..30}; do
  if aws ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
    --query 'InstanceInformationList[0].PingStatus' \
    --output text 2>/dev/null | grep -q "Online"; then
    echo "✅ SSM agent is ready"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "⚠️  SSM agent not ready yet, but continuing..."
  fi
  sleep 2
done
echo

# Connect via SSM and set up tools (no SSH keys needed!)
echo "🔧 Installing just, uv, gh, AWS CLI, and setting up shared directories..."
SETUP_CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=[
        "bash -c \"set -eu; apt-get update; if [ ! -x /usr/local/bin/just ]; then curl --proto =https --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin; fi; if [ ! -x /usr/local/bin/uv ]; then curl -LsSf https://astral.sh/uv/install.sh | sh && cp /root/.cargo/bin/uv /usr/local/bin/uv && chmod +x /usr/local/bin/uv; fi; if ! command -v gh >/dev/null 2>&1; then curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg && echo \\\"deb [arch=amd64 signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main\\\" > /etc/apt/sources.list.d/github-cli.list && apt-get update && apt-get install -y gh; fi; if ! command -v aws >/dev/null 2>&1; then apt-get install -y awscli; fi; mkdir -p /data/home /data/shared; chmod 755 /data/home; chmod 777 /data/shared\""
    ]' \
  --query 'Command.CommandId' \
  --output text 2>/dev/null)

# Wait for setup command to complete
echo "   Waiting for tools installation to complete..."
for i in {1..60}; do
  STATUS=$(aws ssm get-command-invocation \
    --command-id "$SETUP_CMD_ID" \
    --instance-id "$INSTANCE_ID" \
    --query 'Status' \
    --output text 2>/dev/null || echo "InProgress")

  if [ "$STATUS" = "Success" ]; then
    echo "   ✅ Tools installed successfully"
    break
  elif [ "$STATUS" = "Failed" ]; then
    echo "   ⚠️  Tool installation had issues, but continuing..."
    break
  fi
  sleep 2
done

echo "✅ Setup complete!"
echo
echo "Instance ID: $INSTANCE_ID"
echo "Public IP: $PUBLIC_IP"
echo "Availability Zone: $AVAILABILITY_ZONE"
echo
echo "Users can now connect with: just dev-login"

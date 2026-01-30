#!/usr/bin/env bash
set -euo pipefail

# Stop EC2 instance (preserves instance and data, can be restarted with dev-login)
# Run from repo root: infra/dev-stop.sh (or from infra: ./dev-stop.sh)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CONFIG_FILE="$REPO_ROOT/.secrets/aws-sso-config.sh"
if [ -f "$CONFIG_FILE" ]; then
  . "$CONFIG_FILE"
fi

# When run via `just dev-stop`, `aws` already ran (Justfile dependency).

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# Disable AWS CLI pager so output prints directly to screen (no less/more)
export AWS_PAGER=""

export_aws_creds() {
  eval "$(aws configure export-credentials --format env 2>/dev/null)"
}
if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  echo "⚠️  Credentials not exported (SSO may be expired). Running 'aws sso login'..."
  aws sso login || true
  if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "❌ Could not export AWS credentials. Run 'just aws' to log in, then run this script again." >&2
    exit 1
  fi
fi

PROJECT_NAME="${PROJECT_NAME:-rate-design-platform}"

echo "⏸️  Stopping EC2 instance..."
echo

# Find instance (running or stopped)
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=$PROJECT_NAME" "Name=instance-state-name,Values=running,stopping,stopped" \
  --query 'Reservations[0].Instances[0].InstanceId' \
  --output text 2>/dev/null || echo "None")

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "None" ]; then
  echo "❌ ERROR: Instance not found." >&2
  exit 1
fi

# Check current state
CURRENT_STATE=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].State.Name' \
  --output text 2>/dev/null || echo "unknown")

if [ "$CURRENT_STATE" = "stopped" ]; then
  echo "✅ Instance is already stopped"
  exit 0
elif [ "$CURRENT_STATE" = "stopping" ]; then
  echo "⏳ Instance is already stopping, waiting for it to complete..."
  aws ec2 wait instance-stopped --instance-ids "$INSTANCE_ID"
  echo "✅ Instance stopped"
  exit 0
elif [ "$CURRENT_STATE" != "running" ]; then
  echo "⚠️  Instance is in state: $CURRENT_STATE (cannot stop)"
  exit 1
fi

# Stop the instance
echo "   Stopping instance: $INSTANCE_ID"
aws ec2 stop-instances --instance-ids "$INSTANCE_ID" >/dev/null 2>&1

echo "   Waiting for instance to stop..."
aws ec2 wait instance-stopped --instance-ids "$INSTANCE_ID"

echo
echo "✅ Instance stopped successfully"
echo "   💡 You only pay for EBS storage while stopped (~$0.10/GB/month)"
echo "   💡 Run 'just dev-login' to automatically start and connect"

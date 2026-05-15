#!/usr/bin/env bash
set -euo pipefail

# Start a stopped EC2 instance (e.g. after auto-idle stop).
# Run from repo root: just dev-start

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

PROJECT_NAME="${PROJECT_NAME:-rate-design-platform}"

echo "🔍 Looking for EC2 instance..."
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=$PROJECT_NAME" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].InstanceId' \
  --output text 2>/dev/null || echo "")

if [ -n "$INSTANCE_ID" ] && [ "$INSTANCE_ID" != "None" ]; then
  echo "✅ Instance is already running ($INSTANCE_ID)"
  exit 0
fi

INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=$PROJECT_NAME" "Name=instance-state-name,Values=stopped" \
  --query 'Reservations[0].Instances[0].InstanceId' \
  --output text 2>/dev/null || echo "")

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "None" ]; then
  echo "❌ ERROR: No instance found (running or stopped). Run 'just dev-setup' first." >&2
  exit 1
fi

echo "💤 Instance is stopped ($INSTANCE_ID). Starting..."
aws ec2 start-instances --instance-ids "$INSTANCE_ID" >/dev/null
echo "⏳ Waiting for instance to start..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
echo "✅ Instance is running"
echo
echo "Connect with: just dev-login"

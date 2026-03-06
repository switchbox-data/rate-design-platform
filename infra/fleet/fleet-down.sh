#!/usr/bin/env bash
set -euo pipefail

# Tear down fleet workers and optionally delete the EBS snapshot.
#
# Usage: fleet-down.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FLEET_DIR="$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# AWS credentials
# ---------------------------------------------------------------------------
CONFIG_FILE="$REPO_ROOT/.secrets/aws-sso-config.sh"
if [ -f "$CONFIG_FILE" ]; then
  # shellcheck source=/dev/null
  . "$CONFIG_FILE"
fi

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

export_aws_creds() {
  eval "$(aws configure export-credentials --format env 2>/dev/null)"
}
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "Could not export AWS credentials. Run 'just aws' first." >&2
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Terraform destroy
# ---------------------------------------------------------------------------
cd "$FLEET_DIR"

if [ ! -d ".terraform" ]; then
  echo "Fleet Terraform not initialized — nothing to destroy."
  exit 0
fi

WORKER_COUNT=$(terraform output -json worker_instances 2>/dev/null |
  python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")

if [ "$WORKER_COUNT" = "0" ]; then
  echo "No fleet workers found in Terraform state."
else
  echo "Destroying $WORKER_COUNT fleet workers..."
  terraform destroy -auto-approve
  echo "Fleet workers destroyed."
fi

# ---------------------------------------------------------------------------
# Delete fleet snapshot(s)
# ---------------------------------------------------------------------------
echo ""
echo "Looking for fleet snapshots to clean up..."
SNAP_IDS=$(aws ec2 describe-snapshots \
  --filters "Name=tag:Project,Values=rdp-fleet" \
  --query 'Snapshots[*].SnapshotId' --output text 2>/dev/null || echo "")

if [ -z "$SNAP_IDS" ]; then
  echo "No fleet snapshots found."
else
  for snap in $SNAP_IDS; do
    echo "Deleting snapshot: $snap"
    aws ec2 delete-snapshot --snapshot-id "$snap" || true
  done
  echo "Snapshots deleted."
fi

echo ""
echo "Fleet teardown complete."

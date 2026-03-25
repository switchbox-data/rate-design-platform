#!/usr/bin/env bash
set -euo pipefail

# Create the persistent EBS data volume (run once, ever).
# Safe to run again — exits immediately if a volume tagged
# rate-design-platform-data already exists in the target AZ.
#
# The volume intentionally lives outside Terraform's normal setup/teardown
# lifecycle so that losing the local terraform.tfstate never triggers
# accidental recreation. dev-setup uses a data source to find this volume
# by tag; dev-teardown never touches it.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CONFIG_FILE="$REPO_ROOT/.secrets/aws-sso-config.sh"
if [ -f "$CONFIG_FILE" ]; then
  . "$CONFIG_FILE"
fi

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

PROJECT_NAME="${PROJECT_NAME:-rate-design-platform}"
VOLUME_SIZE="${VOLUME_SIZE:-500}"
AVAILABILITY_ZONE="${AVAILABILITY_ZONE:-us-west-2d}"

echo "🔍 Checking for existing data volume..."
EXISTING=$(aws ec2 describe-volumes \
  --filters \
  "Name=tag:Name,Values=${PROJECT_NAME}-data" \
  "Name=tag:Persistent,Values=true" \
  "Name=status,Values=available,in-use" \
  --query 'Volumes[*].VolumeId' \
  --output text 2>/dev/null || echo "")

if [ -n "$EXISTING" ]; then
  echo "✅ Data volume already exists — nothing to do."
  for vol_id in $EXISTING; do
    STATE=$(aws ec2 describe-volumes --volume-ids "$vol_id" --query 'Volumes[0].State' --output text)
    echo "   $vol_id  ($STATE)"
  done
  echo
  echo "Run 'just dev-setup' to create an instance and attach this volume."
  exit 0
fi

echo "📦 Creating persistent EBS data volume..."
echo "   Size: ${VOLUME_SIZE} GiB  AZ: ${AVAILABILITY_ZONE}  Type: gp3  Encrypted: yes"
echo

VOLUME_ID=$(aws ec2 create-volume \
  --availability-zone "$AVAILABILITY_ZONE" \
  --size "$VOLUME_SIZE" \
  --volume-type gp3 \
  --encrypted \
  --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=${PROJECT_NAME}-data},{Key=Project,Value=${PROJECT_NAME}},{Key=Persistent,Value=true}]" \
  --query 'VolumeId' \
  --output text)

echo "✅ Volume created: $VOLUME_ID"
echo
echo "Run 'just dev-setup' to create an instance and attach this volume."

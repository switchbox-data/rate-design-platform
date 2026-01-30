#!/usr/bin/env bash
set -euo pipefail

# Destroy everything including data volume (WARNING: destroys all data!)
# Run from repo root: infra/dev-teardown-all.sh (or from infra: ./dev-teardown-all.sh)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CONFIG_FILE="$REPO_ROOT/.secrets/aws-sso-config.sh"
if [ -f "$CONFIG_FILE" ]; then
  . "$CONFIG_FILE"
fi

# When run via `just dev-teardown-all`, `aws` already ran (Justfile dependency).

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

export_aws_creds() {
  eval "$(aws configure export-credentials --format env 2>/dev/null)"
}
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "⚠️  Credentials not exported (SSO may be expired). Running 'aws sso login'..."
    aws sso login || true
    if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
      echo "❌ Could not export AWS credentials for Terraform. Run 'just aws' to log in, then run this script again." >&2
      exit 1
    fi
  fi
fi

PROJECT_NAME="${PROJECT_NAME:-rate-design-platform}"

echo "⚠️  WARNING: This will destroy EVERYTHING including the data volume!"
echo "   All data on the EBS volume will be permanently deleted."
echo
read -p "Are you sure? Type 'yes' to confirm: " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
  echo "Aborted."
  exit 1
fi
echo

echo "🗑️  Destroying all resources..."
echo

cd "$SCRIPT_DIR"

if [ ! -d ".terraform" ]; then
  echo "📦 Initializing Terraform..."
  terraform init
  echo
fi

terraform destroy -auto-approve || true
echo

echo "🧹 Cleaning up any orphaned AWS resources..."
echo

INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=$PROJECT_NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[0].Instances[0].InstanceId' \
  --output text 2>/dev/null || echo "None")

if [ -n "$INSTANCE_ID" ] && [ "$INSTANCE_ID" != "None" ]; then
  echo "   Terminating EC2 instance: $INSTANCE_ID"
  aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" >/dev/null 2>&1 || true
  echo "   Waiting for instance to terminate..."
  aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" 2>/dev/null || true
fi

VOLUME_ID=$(aws ec2 describe-volumes \
  --filters "Name=tag:Name,Values=${PROJECT_NAME}-data" \
  --query 'Volumes[0].VolumeId' \
  --output text 2>/dev/null || echo "None")

if [ -n "$VOLUME_ID" ] && [ "$VOLUME_ID" != "None" ]; then
  echo "   Deleting EBS volume: $VOLUME_ID"
  for i in {1..30}; do
    STATE=$(aws ec2 describe-volumes --volume-ids "$VOLUME_ID" --query 'Volumes[0].State' --output text 2>/dev/null || echo "deleted")
    if [ "$STATE" = "available" ] || [ "$STATE" = "deleted" ]; then
      break
    fi
    sleep 2
  done
  aws ec2 delete-volume --volume-id "$VOLUME_ID" 2>/dev/null || true
fi

SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=${PROJECT_NAME}-sg" \
  --query 'SecurityGroups[0].GroupId' \
  --output text 2>/dev/null || echo "None")

if [ -n "$SG_ID" ] && [ "$SG_ID" != "None" ]; then
  echo "   Deleting security group: $SG_ID"
  for i in {1..10}; do
    if aws ec2 delete-security-group --group-id "$SG_ID" 2>/dev/null; then
      break
    fi
    sleep 3
  done
fi

ROLE_NAME="${PROJECT_NAME}-ec2-role"
PROFILE_NAME="${PROJECT_NAME}-ec2-profile"

if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "   Cleaning up IAM role: $ROLE_NAME"

  aws iam remove-role-from-instance-profile \
    --instance-profile-name "$PROFILE_NAME" \
    --role-name "$ROLE_NAME" 2>/dev/null || true

  aws iam delete-instance-profile --instance-profile-name "$PROFILE_NAME" 2>/dev/null || true

  POLICIES=$(aws iam list-role-policies --role-name "$ROLE_NAME" --query 'PolicyNames[]' --output text 2>/dev/null || echo "")
  for policy in $POLICIES; do
    aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$policy" 2>/dev/null || true
  done

  ATTACHED=$(aws iam list-attached-role-policies --role-name "$ROLE_NAME" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null || echo "")
  for policy_arn in $ATTACHED; do
    aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "$policy_arn" 2>/dev/null || true
  done

  aws iam delete-role --role-name "$ROLE_NAME" 2>/dev/null || true
fi

aws iam delete-instance-profile --instance-profile-name "$PROFILE_NAME" 2>/dev/null || true

echo
echo "✅ Complete teardown finished (all resources destroyed)"
echo
echo "To recreate everything from scratch, run: just dev-setup"

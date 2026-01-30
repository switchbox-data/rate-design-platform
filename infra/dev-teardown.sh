#!/usr/bin/env bash
set -euo pipefail

# Destroy EC2 instance but preserve data volume (to recreate, run dev-setup again)
# Run from repo root: infra/dev-teardown.sh (or from infra: ./dev-teardown.sh)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CONFIG_FILE="$REPO_ROOT/.secrets/aws-sso-config.sh"
if [ -f "$CONFIG_FILE" ]; then
  . "$CONFIG_FILE"
fi

# When run via `just dev-teardown`, `aws` already ran (Justfile dependency).

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

export_aws_creds() {
  eval "$(aws configure export-credentials --format env 2>/dev/null)"
}
if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  echo "⚠️  Credentials not exported (SSO may be expired). Running 'aws sso login'..."
  aws sso login || true
  if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "❌ Could not export AWS credentials for Terraform. Run 'just aws' to log in, then run this script again." >&2
    exit 1
  fi
fi

PROJECT_NAME="${PROJECT_NAME:-rate-design-platform}"

echo "🗑️  Destroying EC2 instance (preserving data volume)..."
echo

cd "$SCRIPT_DIR"

# Check if Terraform is initialized
if [ ! -d ".terraform" ]; then
  echo "📦 Initializing Terraform..."
  terraform init
  echo
fi

# Destroy only instance-related resources, keeping the EBS volume
echo "🏗️  Destroying instance resources (keeping data volume)..."
terraform destroy -auto-approve \
  -target=aws_volume_attachment.data \
  -target=aws_instance.main \
  -target=aws_security_group.ec2_sg \
  -target=aws_iam_instance_profile.ec2_profile \
  -target=aws_iam_role_policy.s3_access \
  -target=aws_iam_role_policy.ssm_managed_instance \
  -target=aws_iam_role.ec2_role ||
  true
echo

# Clean up any orphaned AWS resources that might exist outside Terraform state
echo "🧹 Cleaning up any orphaned AWS resources..."
echo

# 1. Terminate EC2 instance by tag (if exists)
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

# 2. Delete security group (if exists) - NOT the EBS volume!
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

# 3. Clean up IAM resources
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

VOLUME_ID=$(aws ec2 describe-volumes \
  --filters "Name=tag:Name,Values=${PROJECT_NAME}-data" \
  --query 'Volumes[0].VolumeId' \
  --output text 2>/dev/null || echo "None")

echo
echo "✅ Teardown complete"
if [ -n "$VOLUME_ID" ] && [ "$VOLUME_ID" != "None" ]; then
  echo "   📦 Data volume preserved: $VOLUME_ID"
fi
echo
echo "To recreate the instance, run: just dev-setup"

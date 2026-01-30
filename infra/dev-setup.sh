#!/usr/bin/env bash
set -euo pipefail

# Set up EC2 instance (run once by admin). Idempotent: safe to run multiple times.
# Run from repo root: infra/dev-setup.sh (or from infra: ./dev-setup.sh)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Use same profile/config as `just aws` (script runs in a new process, so we must load it here)
CONFIG_FILE="$REPO_ROOT/.secrets/aws-sso-config.sh"
if [ -f "$CONFIG_FILE" ]; then
  # shellcheck source=.secrets/aws-sso-config.sh
  . "$CONFIG_FILE"
fi

# When run via `just dev-setup`, `aws` already ran (Justfile dependency). When run
# directly, export_aws_creds below will prompt for SSO login if needed.

# Use same region as Terraform (infra/variables.tf default)
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# Export credentials so Terraform (and child processes) see them; Terraform's
# provider doesn't use the same SSO cache as the CLI without this.
export_aws_creds() {
  eval "$(aws configure export-credentials --format env 2>/dev/null)"
}
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "⚠️  Credentials not exported (SSO may be expired). Running 'aws sso login'..."
    aws sso login || true
    if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
      echo "❌ Could not export AWS credentials for Terraform. Run 'just aws' to log in, then run 'just dev-setup' again." >&2
      exit 1
    fi
  fi
fi

echo "🚀 Setting up EC2 instance..."
echo

cd "$SCRIPT_DIR"

# Initialize Terraform if needed
if [ ! -d ".terraform" ]; then
  echo "📦 Initializing Terraform..."
  terraform init
  echo
fi

# Clean up any orphaned AWS resources that might exist from previous runs
# Only removes resources that exist in AWS but NOT in Terraform state (truly orphaned)
# This ensures idempotency - resources managed by Terraform are never touched
PROJECT_NAME="${PROJECT_NAME:-rate-design-platform}"

# Disable AWS CLI pager so output prints directly to screen (no less/more)
export AWS_PAGER=""

echo "🧹 Checking for orphaned AWS resources (exist in AWS but not in Terraform state)..."
echo

# Helper function to check if a resource exists in Terraform state
# Returns 0 (true) if resource is in state, 1 (false) if not
resource_in_state() {
  # terraform state list works regardless of backend (local, S3, etc.)
  # If state doesn't exist or command fails, assume resource is not in state
  terraform state list 2>/dev/null | grep -q "^$1$" || return 1
}

# 1. Check for orphaned EC2 instance
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=$PROJECT_NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[0].Instances[0].InstanceId' \
  --output text 2>/dev/null || echo "None")

if [ -n "$INSTANCE_ID" ] && [ "$INSTANCE_ID" != "None" ]; then
  if ! resource_in_state "aws_instance.main"; then
    echo "   Found orphaned EC2 instance: $INSTANCE_ID (not in Terraform state)"
    echo "   Terminating orphaned instance..."
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" || true
    echo "   Waiting for instance to terminate..."
    aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" || true
  else
    echo "   EC2 instance exists and is managed by Terraform (skipping)"
  fi
fi

# 2. Check for orphaned security group
SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=${PROJECT_NAME}-sg" \
  --query 'SecurityGroups[0].GroupId' \
  --output text 2>/dev/null || echo "None")

if [ -n "$SG_ID" ] && [ "$SG_ID" != "None" ]; then
  if ! resource_in_state "aws_security_group.ec2_sg"; then
    echo "   Found orphaned security group: $SG_ID (not in Terraform state)"
    echo "   Deleting orphaned security group..."
    for i in {1..10}; do
      if aws ec2 delete-security-group --group-id "$SG_ID"; then
        break
      fi
      sleep 3
    done
  else
    echo "   Security group exists and is managed by Terraform (skipping)"
  fi
fi

# 3. Check for orphaned IAM resources
ROLE_NAME="${PROJECT_NAME}-ec2-role"
PROFILE_NAME="${PROJECT_NAME}-ec2-profile"

if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  if ! resource_in_state "aws_iam_role.ec2_role"; then
    echo "   Found orphaned IAM role: $ROLE_NAME (not in Terraform state)"
    echo "   Cleaning up orphaned IAM resources..."

    # Remove role from instance profile
    aws iam remove-role-from-instance-profile \
      --instance-profile-name "$PROFILE_NAME" \
      --role-name "$ROLE_NAME" || true

    # Delete instance profile
    aws iam delete-instance-profile --instance-profile-name "$PROFILE_NAME" || true

    # Delete inline policies
    POLICIES=$(aws iam list-role-policies --role-name "$ROLE_NAME" --query 'PolicyNames[]' --output text 2>/dev/null || echo "")
    for policy in $POLICIES; do
      aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$policy" || true
    done

    # Detach managed policies
    ATTACHED=$(aws iam list-attached-role-policies --role-name "$ROLE_NAME" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null || echo "")
    for policy_arn in $ATTACHED; do
      aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "$policy_arn" || true
    done

    # Delete role
    aws iam delete-role --role-name "$ROLE_NAME" || true
  else
    echo "   IAM role exists and is managed by Terraform (skipping)"
  fi
fi

# Check for orphaned instance profile separately
if aws iam get-instance-profile --instance-profile-name "$PROFILE_NAME" >/dev/null 2>&1; then
  if ! resource_in_state "aws_iam_instance_profile.ec2_profile"; then
    echo "   Found orphaned IAM instance profile: $PROFILE_NAME (not in Terraform state)"
    echo "   Deleting orphaned instance profile..."
    aws iam delete-instance-profile --instance-profile-name "$PROFILE_NAME" || true
  fi
fi

echo "✅ Cleanup check complete"
echo

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

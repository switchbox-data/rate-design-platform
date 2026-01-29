default: help

help:
    @echo "Available recipes:"
    @just -l

install:
    pip install --upgrade pip
    pip install -e .[dev]

test:
    pytest -q

# =============================================================================
# 🔍 AWS
# =============================================================================

# Authenticate with AWS via SSO (for manual AWS CLI usage like S3 access)
# Automatically configures SSO if not already configured
aws:
    #!/usr/bin/env bash
    set -euo pipefail

    # Check for AWS CLI (silent if installed, error if not)
    if ! command -v aws >/dev/null 2>&1; then
        echo "❌ ERROR: AWS CLI is not installed" >&2
        echo "" >&2
        echo "Install AWS CLI first:" >&2
        echo "  https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" >&2
        echo "" >&2
        exit 1
    fi

    # Check if credentials are already valid (early exit if so)
    # Test with an actual EC2 API call since DevPod uses EC2
    if aws sts get-caller-identity &>/dev/null && \
       aws ec2 describe-instances --max-results 5 &>/dev/null; then
        echo "✅ AWS credentials are already valid"
        echo
        exit 0
    fi

    # Credentials are not valid, so we need to configure and/or login
    # Load AWS SSO configuration from shell script (needed for session name)
    CONFIG_FILE=".secrets/aws-sso-config.sh"
    if [ -f "$CONFIG_FILE" ]; then
        # shellcheck source=.secrets/aws-sso-config.sh
        . "$CONFIG_FILE"
    fi
    # Check if SSO is already configured for the default profile
    # (default profile is used when --profile is not specified)
    NEEDS_CONFIG=false
    if ! aws configure get sso_start_url &>/dev/null || \
       ! aws configure get sso_region &>/dev/null; then
        NEEDS_CONFIG=true
    fi

    if [ "$NEEDS_CONFIG" = true ]; then
        echo "🔧 AWS SSO not configured. Setting up SSO configuration..."
        echo

        # Load AWS SSO configuration from shell script
        CONFIG_FILE=".secrets/aws-sso-config.sh"
        if [ ! -f "$CONFIG_FILE" ]; then
            echo "❌ ERROR: Missing AWS SSO configuration file" >&2
            echo "" >&2
            echo "   The file '$CONFIG_FILE' is required but not found." >&2
            echo "   Please ask a team member for this file and place it in the .secrets/ directory." >&2
            echo "" >&2
            exit 1
        fi

        # Source the configuration file
        # shellcheck source=.secrets/aws-sso-config.sh
        . "$CONFIG_FILE"


        # Configure default profile with SSO settings
        aws configure set sso_start_url "$SSO_START_URL"
        aws configure set sso_region "$SSO_REGION"
        aws configure set sso_account_id "$SSO_ACCOUNT_ID"
        aws configure set sso_role_name "$SSO_ROLE_NAME"
        aws configure set region "$SSO_REGION"
        aws configure set output "json"

        echo "✅ AWS SSO configuration complete"
        echo
    fi

    # Run SSO login (handles browser authentication)
    # Use profile-based login since we configure SSO settings directly on the profile
    echo "🔓 Starting AWS SSO login..."
    echo
    aws sso login
    echo

# =============================================================================
# 🚀 DEVELOPMENT ENVIRONMENT
# =============================================================================

# Ensure Terraform is installed (internal dependency)
_terraform:
    #!/usr/bin/env bash
    set -euo pipefail

    if command -v terraform >/dev/null 2>&1; then
        exit 0
    fi

    echo "📦 Terraform not found. Installing..."
    echo ""

    # Detect OS
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    if [ "$OS" = "darwin" ]; then
        # macOS - prefer Homebrew if available
        if command -v brew >/dev/null 2>&1; then
            echo "   Installing via Homebrew..."
            brew tap hashicorp/tap
            brew install hashicorp/tap/terraform
        else
            # Manual install
            if [ "$ARCH" = "arm64" ]; then
                TF_ARCH="arm64"
            else
                TF_ARCH="amd64"
            fi
            echo "   Downloading Terraform for macOS ($TF_ARCH)..."
            TF_VERSION="1.7.5"
            TEMP_DIR=$(mktemp -d)
            curl -sSL "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_darwin_${TF_ARCH}.zip" -o "$TEMP_DIR/terraform.zip"
            unzip -q "$TEMP_DIR/terraform.zip" -d "$TEMP_DIR"
            sudo mv "$TEMP_DIR/terraform" /usr/local/bin/terraform
            rm -rf "$TEMP_DIR"
        fi
    elif [ "$OS" = "linux" ]; then
        # Linux - download binary
        if [ "$ARCH" = "x86_64" ]; then
            TF_ARCH="amd64"
        elif [ "$ARCH" = "aarch64" ]; then
            TF_ARCH="arm64"
        else
            TF_ARCH="amd64"
        fi
        echo "   Downloading Terraform for Linux ($TF_ARCH)..."
        TF_VERSION="1.7.5"
        TEMP_DIR=$(mktemp -d)
        curl -sSL "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_${TF_ARCH}.zip" -o "$TEMP_DIR/terraform.zip"
        unzip -q "$TEMP_DIR/terraform.zip" -d "$TEMP_DIR"
        sudo mv "$TEMP_DIR/terraform" /usr/local/bin/terraform
        rm -rf "$TEMP_DIR"
    else
        echo "❌ ERROR: Unsupported OS: $OS" >&2
        echo "   Please install Terraform manually:" >&2
        echo "   https://developer.hashicorp.com/terraform/downloads" >&2
        exit 1
    fi

    # Verify installation
    if command -v terraform >/dev/null 2>&1; then
        echo "✅ Terraform installed: $(terraform version -json | head -1)"
    else
        echo "❌ ERROR: Terraform installation failed" >&2
        exit 1
    fi

# Set up EC2 instance (run once by admin)
# Idempotent: safe to run multiple times
dev-setup: aws _terraform
    #!/usr/bin/env bash
    set -euo pipefail

    # No SSH required - we use AWS Systems Manager Session Manager!

    echo "🚀 Setting up EC2 instance..."
    echo

    # Change to infra directory
    cd infra

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
    echo "🔧 Installing just, uv, AWS CLI, and setting up shared directories..."
    SETUP_CMD_ID=$(aws ssm send-command \
        --instance-ids "$INSTANCE_ID" \
        --document-name "AWS-RunShellScript" \
        --parameters 'commands=[
            "bash -c \"set -eu; apt-get update; if [ ! -x /usr/local/bin/just ]; then curl --proto =https --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin; fi; if [ ! -x /usr/local/bin/uv ]; then curl -LsSf https://astral.sh/uv/install.sh | sh && cp /root/.cargo/bin/uv /usr/local/bin/uv && chmod +x /usr/local/bin/uv; fi; if ! command -v aws >/dev/null 2>&1; then apt-get install -y awscli; fi; mkdir -p /data/home /data/shared; chmod 755 /data/home; chmod 777 /data/shared\""
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

# Destroy EC2 instance but preserve data volume (to recreate, run dev-setup again)
dev-teardown: aws _terraform
    #!/usr/bin/env bash
    set -euo pipefail

    PROJECT_NAME="rate-design-platform"

    echo "🗑️  Destroying EC2 instance (preserving data volume)..."
    echo

    # Change to infra directory
    cd infra

    # Check if Terraform is initialized
    if [ ! -d ".terraform" ]; then
        echo "📦 Initializing Terraform..."
        terraform init
        echo
    fi

    # Destroy only instance-related resources, keeping the EBS volume
    # Use targeted destroy to preserve the data volume
    echo "🏗️  Destroying instance resources (keeping data volume)..."
    terraform destroy -auto-approve \
        -target=aws_volume_attachment.data \
        -target=aws_instance.main \
        -target=aws_security_group.ec2_sg \
        -target=aws_iam_instance_profile.ec2_profile \
        -target=aws_iam_role_policy.s3_access \
        -target=aws_iam_role_policy.ssm_managed_instance \
        -target=aws_iam_role.ec2_role \
        || true
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
        # Retry a few times (might need to wait for instance to fully terminate)
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

    # Check if role exists
    if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
        echo "   Cleaning up IAM role: $ROLE_NAME"
        
        # Remove role from instance profile
        aws iam remove-role-from-instance-profile \
            --instance-profile-name "$PROFILE_NAME" \
            --role-name "$ROLE_NAME" 2>/dev/null || true
        
        # Delete instance profile
        aws iam delete-instance-profile --instance-profile-name "$PROFILE_NAME" 2>/dev/null || true
        
        # Delete inline policies from role
        POLICIES=$(aws iam list-role-policies --role-name "$ROLE_NAME" --query 'PolicyNames[]' --output text 2>/dev/null || echo "")
        for policy in $POLICIES; do
            aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$policy" 2>/dev/null || true
        done
        
        # Detach managed policies from role
        ATTACHED=$(aws iam list-attached-role-policies --role-name "$ROLE_NAME" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null || echo "")
        for policy_arn in $ATTACHED; do
            aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "$policy_arn" 2>/dev/null || true
        done
        
        # Delete the role
        aws iam delete-role --role-name "$ROLE_NAME" 2>/dev/null || true
    fi

    # Also try to delete instance profile if it exists but role doesn't
    aws iam delete-instance-profile --instance-profile-name "$PROFILE_NAME" 2>/dev/null || true

    # Check if data volume was preserved
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

# Destroy everything including data volume (WARNING: destroys all data!)
dev-teardown-all: aws _terraform
    #!/usr/bin/env bash
    set -euo pipefail

    PROJECT_NAME="rate-design-platform"

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

    # Change to infra directory
    cd infra

    # Check if Terraform is initialized
    if [ ! -d ".terraform" ]; then
        echo "📦 Initializing Terraform..."
        terraform init
        echo
    fi

    # Destroy ALL Terraform-managed resources
    terraform destroy -auto-approve || true
    echo

    # Clean up any orphaned AWS resources
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

    # 2. Delete EBS volume by tag (if exists)
    VOLUME_ID=$(aws ec2 describe-volumes \
        --filters "Name=tag:Name,Values=${PROJECT_NAME}-data" \
        --query 'Volumes[0].VolumeId' \
        --output text 2>/dev/null || echo "None")
    
    if [ -n "$VOLUME_ID" ] && [ "$VOLUME_ID" != "None" ]; then
        echo "   Deleting EBS volume: $VOLUME_ID"
        # Wait for volume to become available (detached)
        for i in {1..30}; do
            STATE=$(aws ec2 describe-volumes --volume-ids "$VOLUME_ID" --query 'Volumes[0].State' --output text 2>/dev/null || echo "deleted")
            if [ "$STATE" = "available" ] || [ "$STATE" = "deleted" ]; then
                break
            fi
            sleep 2
        done
        aws ec2 delete-volume --volume-id "$VOLUME_ID" 2>/dev/null || true
    fi

    # 3. Delete security group (if exists)
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

    # 4. Clean up IAM resources
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

# User login (run by any authorized user)
dev-login: aws
    #!/usr/bin/env bash
    set -euo pipefail

    # Check for Session Manager plugin (required for SSM sessions)
    if ! command -v session-manager-plugin >/dev/null 2>&1; then
        echo "📦 Session Manager plugin not found. Installing from AWS..."
        echo ""
        
        # Detect architecture (Intel vs Apple Silicon)
        ARCH=$(uname -m)
        if [ "$ARCH" = "arm64" ]; then
            DOWNLOAD_URL="https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac_arm64/session-manager-plugin.pkg"
            echo "   Detected Apple Silicon (arm64)"
        else
            DOWNLOAD_URL="https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac/session-manager-plugin.pkg"
            echo "   Detected Intel (x86_64)"
        fi
        
        # Create temp directory
        TEMP_DIR=$(mktemp -d)
        PKG_FILE="$TEMP_DIR/session-manager-plugin.pkg"
        
        echo "   Downloading from AWS..."
        curl -sSL "$DOWNLOAD_URL" -o "$PKG_FILE"
        
        if [ ! -f "$PKG_FILE" ] || [ ! -s "$PKG_FILE" ]; then
            echo "❌ ERROR: Failed to download Session Manager plugin" >&2
            rm -rf "$TEMP_DIR"
            exit 1
        fi
        
        echo "   Installing (requires sudo)..."
        sudo installer -pkg "$PKG_FILE" -target / >/dev/null 2>&1
        
        # Create symlink if it doesn't exist
        if [ ! -f /usr/local/bin/session-manager-plugin ]; then
            sudo mkdir -p /usr/local/bin
            sudo ln -sf /usr/local/sessionmanagerplugin/bin/session-manager-plugin /usr/local/bin/session-manager-plugin
        fi
        
        # Clean up
        rm -rf "$TEMP_DIR"
        
        # Verify installation
        if ! command -v session-manager-plugin >/dev/null 2>&1; then
            echo "❌ ERROR: Failed to install Session Manager plugin" >&2
            echo "" >&2
            echo "   Please install manually:" >&2
            echo "     https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html" >&2
            echo "" >&2
            exit 1
        fi
        
        echo "✅ Session Manager plugin installed successfully"
        echo ""
    fi

    # No SSH required - we use AWS Systems Manager Session Manager!

    # Get AWS IAM username (handle both IAM users and SSO)
    IAM_USERNAME=$(aws sts get-caller-identity --query User.UserName --output text 2>/dev/null || echo "")
    # For SSO users, try Identity.UserName or extract from ARN
    if [ -z "$IAM_USERNAME" ] || [ "$IAM_USERNAME" = "None" ]; then
        IAM_USERNAME=$(aws sts get-caller-identity --query Identity.UserName --output text 2>/dev/null || echo "")
    fi
    # If still empty, try extracting from ARN (format: arn:aws:sts::ACCOUNT:assumed-role/ROLE/USERNAME)
    if [ -z "$IAM_USERNAME" ] || [ "$IAM_USERNAME" = "None" ]; then
        ARN=$(aws sts get-caller-identity --query Arn --output text 2>/dev/null || echo "")
        if [ -n "$ARN" ]; then
            # Extract username from ARN (last part after the last /)
            IAM_USERNAME=$(echo "$ARN" | awk -F'/' '{print $NF}')
        fi
    fi
    if [ -z "$IAM_USERNAME" ] || [ "$IAM_USERNAME" = "None" ]; then
        echo "❌ ERROR: Could not get AWS IAM username" >&2
        echo "   Got: $(aws sts get-caller-identity --output json 2>/dev/null || echo 'unknown')" >&2
        exit 1
    fi

    # Sanitize username for Linux (lowercase, replace invalid chars with underscores)
    LINUX_USERNAME=$(echo "$IAM_USERNAME" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')

    echo "🔐 Logging in as: $LINUX_USERNAME"
    echo

    # Get instance information using AWS CLI (no Terraform required)
    # Find instance by project tag (default: rate-design-platform)
    PROJECT_NAME="${PROJECT_NAME:-rate-design-platform}"
    
    echo "🔍 Looking for EC2 instance..."
    INSTANCE_ID=$(aws ec2 describe-instances \
        --filters "Name=tag:Project,Values=$PROJECT_NAME" "Name=instance-state-name,Values=running" \
        --query 'Reservations[0].Instances[0].InstanceId' \
        --output text 2>/dev/null || echo "")
    
    if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "None" ]; then
        echo "❌ ERROR: Instance not found. Run 'just dev-setup' first." >&2
        exit 1
    fi
    
    AVAILABILITY_ZONE=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' \
        --output text 2>/dev/null || echo "")
    
    PUBLIC_IP=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text 2>/dev/null || echo "")
    
    PRIVATE_IP=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' \
        --output text 2>/dev/null || echo "")

    # Use private IP if no public IP (instance in private subnet)
    if [ -z "$PUBLIC_IP" ]; then
        CONNECT_IP="$PRIVATE_IP"
        echo "⚠️  Instance has no public IP, using private IP (ensure you're connected via VPN/bastion)"
    else
        CONNECT_IP="$PUBLIC_IP"
    fi

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

    # Create/setup user account via SSM (no SSH keys needed!)
    echo "👤 Setting up user account..."
    USER_HOME="/data/home/$LINUX_USERNAME"
    COMMAND_ID=$(aws ssm send-command \
        --instance-ids "$INSTANCE_ID" \
        --document-name "AWS-RunShellScript" \
        --parameters "commands=[
            'bash -c \"set -eu; USER_HOME=\\\"$USER_HOME\\\"; LINUX_USERNAME=\\\"$LINUX_USERNAME\\\"; if ! id \\\"\\\$LINUX_USERNAME\\\" &>/dev/null; then echo \\\"Creating user account: \\\$LINUX_USERNAME\\\"; mkdir -p \\\"\\\$USER_HOME\\\"; useradd -d \\\"\\\$USER_HOME\\\" -s /bin/bash \\\"\\\$LINUX_USERNAME\\\"; usermod -aG sudo \\\"\\\$LINUX_USERNAME\\\"; chown -R \\\"\\\$LINUX_USERNAME:\\\$LINUX_USERNAME\\\" \\\"\\\$USER_HOME\\\"; chmod 755 \\\"\\\$USER_HOME\\\"; echo \\\"User created and added to sudo group\\\"; else echo \\\"User account already exists: \\\$LINUX_USERNAME\\\"; usermod -aG sudo \\\"\\\$LINUX_USERNAME\\\" 2>/dev/null || true; echo \\\"Ensured user is in sudo group\\\"; fi\"'
        ]" \
        --query 'Command.CommandId' \
        --output text 2>/dev/null)
    
    # Wait for command to complete and check ACTUAL output/errors
    if [ -n "$COMMAND_ID" ]; then
        echo "   Waiting for user creation to complete..."
        for i in {1..30}; do
            STATUS=$(aws ssm get-command-invocation \
                --command-id "$COMMAND_ID" \
                --instance-id "$INSTANCE_ID" \
                --query 'Status' \
                --output text 2>/dev/null || echo "InProgress")
            
            if [ "$STATUS" = "Success" ] || [ "$STATUS" = "Failed" ] || [ "$STATUS" = "Cancelled" ]; then
                # Get ACTUAL output and error output
                OUTPUT=$(aws ssm get-command-invocation \
                    --command-id "$COMMAND_ID" \
                    --instance-id "$INSTANCE_ID" \
                    --query 'StandardOutputContent' \
                    --output text 2>/dev/null || echo "")
                ERROR_OUTPUT=$(aws ssm get-command-invocation \
                    --command-id "$COMMAND_ID" \
                    --instance-id "$INSTANCE_ID" \
                    --query 'StandardErrorContent' \
                    --output text 2>/dev/null || echo "")
                
                echo "   Command status: $STATUS"
                if [ -n "$OUTPUT" ]; then
                    echo "   Output: $OUTPUT"
                fi
                if [ -n "$ERROR_OUTPUT" ]; then
                    echo "   Errors: $ERROR_OUTPUT"
                fi
                
                if [ "$STATUS" = "Failed" ]; then
                    echo "   ERROR: User creation command failed!" >&2
                    exit 1
                fi
                
                # Now verify user actually exists by checking /etc/passwd
                VERIFY_CMD_ID=$(aws ssm send-command \
                    --instance-ids "$INSTANCE_ID" \
                    --document-name "AWS-RunShellScript" \
                    --parameters "commands=['bash -c \"if getent passwd \\\"$LINUX_USERNAME\\\" >/dev/null 2>&1; then echo EXISTS; else echo NOT_EXISTS; fi\"']" \
                    --query 'Command.CommandId' \
                    --output text 2>/dev/null)
                
                if [ -n "$VERIFY_CMD_ID" ]; then
                    sleep 3
                    VERIFY_OUTPUT=$(aws ssm get-command-invocation \
                        --command-id "$VERIFY_CMD_ID" \
                        --instance-id "$INSTANCE_ID" \
                        --query 'StandardOutputContent' \
                        --output text 2>/dev/null || echo "")
                    
                    if echo "$VERIFY_OUTPUT" | grep -q "EXISTS"; then
                        echo "   User account verified in /etc/passwd"
                        break
                    else
                        echo "   WARNING: User not found in /etc/passwd, output: $VERIFY_OUTPUT"
                        if [ $i -lt 30 ]; then
                            echo "   Retrying verification... ($i/30)"
                            sleep 2
                            continue
                        fi
                    fi
                fi
                break
            fi
            sleep 1
        done
    else
        echo "   ERROR: Could not get command ID" >&2
        exit 1
    fi

    # Ensure user is in sudo group, configure passwordless sudo, and ensure S3 mount
    echo "🔧 Configuring user access..."
    aws ssm send-command \
        --instance-ids "$INSTANCE_ID" \
        --document-name "AWS-RunShellScript" \
        --parameters "commands=[
            'bash -c \"set -eu; usermod -aG sudo \\\"$LINUX_USERNAME\\\" 2>/dev/null || true; echo \\\"$LINUX_USERNAME ALL=(ALL) NOPASSWD:ALL\\\" > /etc/sudoers.d/$LINUX_USERNAME; chmod 440 /etc/sudoers.d/$LINUX_USERNAME; mountpoint -q /s3 || mount /s3 2>/dev/null || true\"'
        ]" \
        --output text >/dev/null
    sleep 2

    # Forward local git config to remote instance
    LOCAL_GIT_EMAIL=$(git config --global user.email 2>/dev/null || echo "")
    LOCAL_GIT_NAME=$(git config --global user.name 2>/dev/null || echo "")
    if [ -n "$LOCAL_GIT_EMAIL" ] || [ -n "$LOCAL_GIT_NAME" ]; then
        echo "🔧 Forwarding local git config..."
        aws ssm send-command \
            --instance-ids "$INSTANCE_ID" \
            --document-name "AWS-RunShellScript" \
            --parameters "commands=[
                'bash -c \"set -eu; LINUX_USERNAME=\\\"$LINUX_USERNAME\\\"; USER_HOME=\\\"$USER_HOME\\\"; GIT_EMAIL=\\\"$LOCAL_GIT_EMAIL\\\"; GIT_NAME=\\\"$LOCAL_GIT_NAME\\\"; if [ -n \\\"\\\$GIT_EMAIL\\\" ]; then runuser -u \\\"\\\$LINUX_USERNAME\\\" -- git config --global user.email \\\"\\\$GIT_EMAIL\\\"; echo \\\"   git user.email: \\\$GIT_EMAIL\\\"; fi; if [ -n \\\"\\\$GIT_NAME\\\" ]; then runuser -u \\\"\\\$LINUX_USERNAME\\\" -- git config --global user.name \\\"\\\$GIT_NAME\\\"; echo \\\"   git user.name: \\\$GIT_NAME\\\"; fi\"'
            ]" \
            --output text >/dev/null
        echo "   ✅ Git config synced from local machine"
    fi

    # Set up repo on first login via SSM (only clone + uv sync on first time)
    echo "📦 Setting up development environment..."
    REPO_DIR="$USER_HOME/rate-design-platform"
    REPO_URL="https://github.com/switchbox-data/rate-design-platform.git"
    REPO_COMMAND_ID=$(aws ssm send-command \
        --instance-ids "$INSTANCE_ID" \
        --document-name "AWS-RunShellScript" \
        --parameters "commands=[
            'bash -c \"set -eu; REPO_DIR=\\\"$REPO_DIR\\\"; REPO_URL=\\\"$REPO_URL\\\"; LINUX_USERNAME=\\\"$LINUX_USERNAME\\\"; if [ -d \\\"\\\$REPO_DIR/.git\\\" ]; then echo \\\"Repository already exists, skipping clone\\\"; elif [ -d \\\"\\\$REPO_DIR\\\" ]; then echo \\\"Directory exists but is not a git repo, removing and cloning fresh...\\\"; rm -rf \\\"\\\$REPO_DIR\\\"; runuser -u \\\"\\\$LINUX_USERNAME\\\" -- git clone \\\"\\\$REPO_URL\\\" \\\"\\\$REPO_DIR\\\"; echo \\\"Running uv sync...\\\"; cd \\\"\\\$REPO_DIR\\\" && runuser -u \\\"\\\$LINUX_USERNAME\\\" -- /usr/local/bin/uv sync; echo \\\"Repository cloned and dependencies installed\\\"; else echo \\\"Cloning repository...\\\"; runuser -u \\\"\\\$LINUX_USERNAME\\\" -- git clone \\\"\\\$REPO_URL\\\" \\\"\\\$REPO_DIR\\\"; echo \\\"Running uv sync...\\\"; cd \\\"\\\$REPO_DIR\\\" && runuser -u \\\"\\\$LINUX_USERNAME\\\" -- /usr/local/bin/uv sync; echo \\\"Repository cloned and dependencies installed\\\"; fi\"'
        ]" \
        --query 'Command.CommandId' \
        --output text 2>/dev/null)
    
    # Wait for repo setup to complete (only if first-time clone)
    if [ -n "$REPO_COMMAND_ID" ]; then
        echo "   Waiting for repository setup..."
        for i in {1..60}; do
            STATUS=$(aws ssm get-command-invocation \
                --command-id "$REPO_COMMAND_ID" \
                --instance-id "$INSTANCE_ID" \
                --query 'Status' \
                --output text 2>/dev/null || echo "Pending")
            
            if [ "$STATUS" = "Success" ]; then
                OUTPUT=$(aws ssm get-command-invocation \
                    --command-id "$REPO_COMMAND_ID" \
                    --instance-id "$INSTANCE_ID" \
                    --query 'StandardOutputContent' \
                    --output text 2>/dev/null || echo "")
                if echo "$OUTPUT" | grep -q "already exists"; then
                    echo "   ✅ Repository ready"
                else
                    echo "   ✅ Repository cloned and dependencies installed"
                fi
                break
            elif [ "$STATUS" = "Failed" ] || [ "$STATUS" = "Cancelled" ]; then
                echo "   ❌ Repository setup failed!"
                AWS_PAGER="" aws ssm get-command-invocation \
                    --command-id "$REPO_COMMAND_ID" \
                    --instance-id "$INSTANCE_ID" \
                    --query 'StandardErrorContent' \
                    --output text 2>/dev/null || true
                break
            fi
            sleep 2
        done
    fi

    echo ""

    # Verify user actually exists before starting session (critical check)
    echo "   Verifying user account exists..."
    USER_EXISTS=false
    MAX_RETRIES=20
    
    for i in $(seq 1 $MAX_RETRIES); do
        VERIFY_COMMAND_ID=$(aws ssm send-command \
            --instance-ids "$INSTANCE_ID" \
            --document-name "AWS-RunShellScript" \
            --parameters "commands=['bash -c \"if id \\\"$LINUX_USERNAME\\\" >/dev/null 2>&1; then echo EXISTS; else echo NOT_EXISTS; fi\"']" \
            --query 'Command.CommandId' \
            --output text 2>/dev/null)
        
        if [ -n "$VERIFY_COMMAND_ID" ]; then
            # Wait for command to complete
            sleep 3
            OUTPUT=$(aws ssm get-command-invocation \
                --command-id "$VERIFY_COMMAND_ID" \
                --instance-id "$INSTANCE_ID" \
                --query 'StandardOutputContent' \
                --output text 2>/dev/null || echo "")
            
            if echo "$OUTPUT" | grep -q "EXISTS"; then
                USER_EXISTS=true
                echo "   User account verified"
                break
            else
                if [ $i -lt $MAX_RETRIES ]; then
                    echo "   User not found yet, retrying... ($i/$MAX_RETRIES)"
                fi
            fi
        fi
        
        sleep 2
    done
    
    if [ "$USER_EXISTS" = false ]; then
        echo "   ERROR: User account '$LINUX_USERNAME' does not exist after $MAX_RETRIES attempts" >&2
        echo "   The user creation may have failed. Check the SSM command output." >&2
        exit 1
    fi
    
    # Set up SSH access for Cursor (use dedicated keypair for this project)
    echo "Setting up SSH access for Cursor..."
    SSH_KEY_NAME="rate_design_platform_ec2"
    SSH_KEY=~/.ssh/${SSH_KEY_NAME}.pub
    SSH_KEY_PRIVATE=~/.ssh/${SSH_KEY_NAME}
    
    # Check for dedicated keypair, generate if missing
    if [ ! -f "$SSH_KEY" ] || [ ! -f "$SSH_KEY_PRIVATE" ]; then
        echo "   Generating dedicated SSH keypair for EC2 access..."
        mkdir -p ~/.ssh
        chmod 700 ~/.ssh
        ssh-keygen -t ed25519 -f "$SSH_KEY_PRIVATE" -N "" -C "rate-design-platform-ec2-$(date +%Y%m%d)" >/dev/null 2>&1
        echo "   SSH keypair generated: $SSH_KEY_PRIVATE"
    else
        echo "   Using existing SSH keypair: $SSH_KEY_PRIVATE"
    fi
    
    if [ -f "$SSH_KEY" ]; then
        SSH_KEY_CONTENT=$(cat "$SSH_KEY" | sed 's/"/\\"/g')
        aws ssm send-command \
            --instance-ids "$INSTANCE_ID" \
            --document-name "AWS-RunShellScript" \
            --parameters "commands=[
                'bash -c \"set -eu; USER_HOME=\\\"$USER_HOME\\\"; LINUX_USERNAME=\\\"$LINUX_USERNAME\\\"; SSH_KEY=\\\"$SSH_KEY_CONTENT\\\"; mkdir -p \\\"\\\$USER_HOME/.ssh\\\"; chmod 700 \\\"\\\$USER_HOME/.ssh\\\"; chown \\\"\\\$LINUX_USERNAME:\\\$LINUX_USERNAME\\\" \\\"\\\$USER_HOME/.ssh\\\"; if ! grep -qF \\\"\\\$SSH_KEY\\\" \\\"\\\$USER_HOME/.ssh/authorized_keys\\\" 2>/dev/null; then echo \\\"\\\$SSH_KEY\\\" >> \\\"\\\$USER_HOME/.ssh/authorized_keys\\\"; chmod 600 \\\"\\\$USER_HOME/.ssh/authorized_keys\\\"; chown \\\"\\\$LINUX_USERNAME:\\\$LINUX_USERNAME\\\" \\\"\\\$USER_HOME/.ssh/authorized_keys\\\"; fi\"'
            ]" \
            --output text >/dev/null
        sleep 2
        echo "   SSH key configured on instance"
    fi
    
    # Configure SSH config for SSM port forwarding
    # Use SSM port forwarding for SSH (more secure, works regardless of security group rules)
    LOCAL_SSH_PORT=2222
    if [ -f "$SSH_KEY" ]; then
        mkdir -p ~/.ssh
        chmod 700 ~/.ssh
        
        # Update or add SSH config entry for SSM port forwarding
        if grep -q "Host rate-design-platform" ~/.ssh/config 2>/dev/null; then
            # Remove old entry and add new one (handle both Linux and macOS sed)
            if [[ "$OSTYPE" == "darwin"* ]]; then
                sed -i '' '/^Host rate-design-platform$/,/^$/d' ~/.ssh/config 2>/dev/null || true
            else
                sed -i.bak '/^Host rate-design-platform$/,/^$/d' ~/.ssh/config 2>/dev/null || true
            fi
        fi
        
        {
            echo "Host rate-design-platform"
            echo "    HostName localhost"
            echo "    Port $LOCAL_SSH_PORT"
            echo "    User $LINUX_USERNAME"
            echo "    IdentityFile $SSH_KEY_PRIVATE"
            echo "    StrictHostKeyChecking no"
            echo "    UserKnownHostsFile /dev/null"
            echo ""
        } >> ~/.ssh/config
        chmod 600 ~/.ssh/config
        echo "   SSH config updated for SSM port forwarding (localhost:$LOCAL_SSH_PORT)"
    fi
    
    # Try to open Cursor with remote workspace using SSM port forwarding
    REPO_DIR="$USER_HOME/rate-design-platform"
    
    # Function to start SSM port forwarding
    start_port_forwarding() {
        echo "📡 Starting SSM port forwarding in background..."
        echo "   Forwarding local port $LOCAL_SSH_PORT to SSH (port 22) on the instance"
        
        nohup aws ssm start-session \
            --target "$INSTANCE_ID" \
            --document-name "AWS-StartPortForwardingSession" \
            --parameters "{\"portNumber\":[\"22\"],\"localPortNumber\":[\"$LOCAL_SSH_PORT\"]}" \
            > /tmp/ssm-port-forward-${INSTANCE_ID}.log 2>&1 &
        SSM_PID=$!
        
        echo "   Waiting for port forwarding to establish..."
        for i in {1..20}; do
            sleep 1
            if command -v lsof >/dev/null 2>&1; then
                if lsof -i ":$LOCAL_SSH_PORT" >/dev/null 2>&1; then
                    echo "   ✅ Port forwarding active (PID: $SSM_PID)"
                    echo "   Logs: /tmp/ssm-port-forward-${INSTANCE_ID}.log"
                    return 0
                fi
            elif command -v netstat >/dev/null 2>&1; then
                if netstat -an 2>/dev/null | grep -q ":$LOCAL_SSH_PORT.*LISTEN"; then
                    echo "   ✅ Port forwarding active (PID: $SSM_PID)"
                    return 0
                fi
            fi
        done
        
        echo "   ⚠️  Port forwarding may not be ready yet, but continuing..."
        return 1
    }
    
    # Function to kill any process using the port
    kill_port_process() {
        if command -v lsof >/dev/null 2>&1; then
            local PID=$(lsof -ti ":$LOCAL_SSH_PORT" 2>/dev/null || echo "")
            if [ -n "$PID" ]; then
                echo "   Killing stale process on port $LOCAL_SSH_PORT (PID: $PID)"
                kill $PID 2>/dev/null || true
                sleep 2
            fi
        fi
    }
    
    # Check if port is already in use
    PORT_IN_USE=false
    if command -v lsof >/dev/null 2>&1; then
        if lsof -i ":$LOCAL_SSH_PORT" >/dev/null 2>&1; then
            PORT_IN_USE=true
        fi
    elif command -v netstat >/dev/null 2>&1; then
        if netstat -an 2>/dev/null | grep -q ":$LOCAL_SSH_PORT.*LISTEN"; then
            PORT_IN_USE=true
        fi
    fi
    
    # If port is in use, test if SSH actually works through the tunnel
    if [ "$PORT_IN_USE" = true ]; then
        echo "🔍 Port $LOCAL_SSH_PORT is in use, testing existing tunnel..."
        if ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -i "$SSH_KEY_PRIVATE" -p $LOCAL_SSH_PORT "$LINUX_USERNAME@localhost" "echo test" >/dev/null 2>&1; then
            echo "   ✅ Existing tunnel is working"
        else
            echo "   ⚠️  Existing tunnel is stale, restarting..."
            kill_port_process
            start_port_forwarding
        fi
        echo ""
    else
        # No existing tunnel, start a new one
        start_port_forwarding
        echo ""
    fi
    
    # Final SSH verification before opening Cursor
    echo "🔍 Verifying SSH connection..."
    SSH_TEST_SUCCESS=false
    for i in {1..10}; do
        if ssh -o ConnectTimeout=3 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -i "$SSH_KEY_PRIVATE" -p $LOCAL_SSH_PORT "$LINUX_USERNAME@localhost" "echo test" >/dev/null 2>&1; then
            SSH_TEST_SUCCESS=true
            echo "   ✅ SSH connection verified"
            break
        else
            if [ $i -lt 10 ]; then
                echo "   Waiting for SSH to be ready... ($i/10)"
                sleep 2
            fi
        fi
    done
    
    if [ "$SSH_TEST_SUCCESS" = false ]; then
        echo "   ⚠️  SSH connection test failed, but continuing..."
        echo "   You may need to manually reconnect in Cursor"
    fi
    echo ""
    
    # Try to open Cursor
    if command -v cursor >/dev/null 2>&1 && [ -f "$SSH_KEY" ]; then
        echo "Opening Cursor with remote workspace..."
        if cursor --remote ssh-remote+rate-design-platform "$REPO_DIR" 2>/dev/null; then
            echo "   ✅ Cursor opened successfully"
            echo ""
            echo "   💡 Keep the SSM port forwarding session running while using Cursor"
            if [ -n "${SSM_PID:-}" ]; then
                echo "   To stop port forwarding: kill $SSM_PID"
            fi
        else
            echo "   ⚠️  Could not open Cursor remotely"
            echo ""
            echo "   You can manually connect in Cursor to: ssh-remote+rate-design-platform"
            if [ -n "${SSM_PID:-}" ]; then
                echo "   Port forwarding is running (PID: $SSM_PID)"
            fi
        fi
    else
        echo ""
        echo "📋 To connect Cursor manually:"
        echo ""
        echo "   In Cursor, connect to: ssh-remote+rate-design-platform"
        echo ""
        if ! command -v cursor >/dev/null 2>&1; then
            echo "💡 Tip: Install Cursor CLI for automatic connection:"
            echo "     curl https://cursor.com/install -fsS | bash"
            echo ""
        fi
        if [ -n "${SSM_PID:-}" ]; then
            echo "   Port forwarding is running in background (PID: $SSM_PID)"
        fi
    fi
    echo ""
    
    # Open interactive SSM session
    echo "Opening interactive session..."
    echo "   (Press Ctrl+D to exit)"
    echo ""
    aws ssm start-session \
        --target "$INSTANCE_ID" \
        --document-name "AWS-StartInteractiveCommand" \
        --parameters "command=sudo -u $LINUX_USERNAME bash -l"

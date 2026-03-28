#!/usr/bin/env bash
set -euo pipefail

# User login (run by any authorized user). Run from repo root: infra/dev-login.sh

# Resolve script dir (infra/) for first-login.sh path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# When run via `just dev-login`, `aws` already ran (Justfile dependency).

# Use same region as Terraform (infra/variables.tf default) so we find the instance
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# Check for Session Manager plugin (required for SSM sessions)
if ! command -v session-manager-plugin >/dev/null 2>&1; then
  echo "📦 Session Manager plugin not found. Installing from AWS..."
  echo ""

  ARCH=$(uname -m)
  if [ "$ARCH" = "arm64" ]; then
    DOWNLOAD_URL="https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac_arm64/session-manager-plugin.pkg"
    echo "   Detected Apple Silicon (arm64)"
  else
    DOWNLOAD_URL="https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac/session-manager-plugin.pkg"
    echo "   Detected Intel (x86_64)"
  fi

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

  if [ ! -f /usr/local/bin/session-manager-plugin ]; then
    sudo mkdir -p /usr/local/bin
    sudo ln -sf /usr/local/sessionmanagerplugin/bin/session-manager-plugin /usr/local/bin/session-manager-plugin
  fi

  rm -rf "$TEMP_DIR"

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

# Get AWS IAM username (handle both IAM users and SSO)
IAM_USERNAME=$(aws sts get-caller-identity --query User.UserName --output text 2>/dev/null || echo "")
if [ -z "$IAM_USERNAME" ] || [ "$IAM_USERNAME" = "None" ]; then
  IAM_USERNAME=$(aws sts get-caller-identity --query Identity.UserName --output text 2>/dev/null || echo "")
fi
if [ -z "$IAM_USERNAME" ] || [ "$IAM_USERNAME" = "None" ]; then
  ARN=$(aws sts get-caller-identity --query Arn --output text 2>/dev/null || echo "")
  if [ -n "$ARN" ]; then
    IAM_USERNAME=$(echo "$ARN" | awk -F'/' '{print $NF}')
  fi
fi
if [ -z "$IAM_USERNAME" ] || [ "$IAM_USERNAME" = "None" ]; then
  echo "❌ ERROR: Could not get AWS IAM username" >&2
  echo "   Got: $(aws sts get-caller-identity --output json 2>/dev/null || echo 'unknown')" >&2
  exit 1
fi

LINUX_USERNAME=$(echo "$IAM_USERNAME" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')

echo "🔐 Logging in as: $LINUX_USERNAME"
echo

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

if [ -z "$PUBLIC_IP" ]; then
  CONNECT_IP="$PRIVATE_IP"
  echo "⚠️  Instance has no public IP, using private IP (ensure you're connected via VPN/bastion)"
else
  CONNECT_IP="$PUBLIC_IP"
fi

echo "⏳ Waiting for SSM agent to be ready..."
SSM_READY=false
for i in {1..120}; do
  if aws ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
    --query 'InstanceInformationList[0].PingStatus' \
    --output text 2>/dev/null | grep -q "Online"; then
    echo "✅ SSM agent is ready"
    SSM_READY=true
    break
  fi
  if [ $((i % 10)) -eq 0 ]; then
    echo "   Still waiting... ($i/120)"
  fi
  sleep 2
done

if [ "$SSM_READY" = false ]; then
  echo "⚠️  SSM agent not ready after 4 minutes" >&2
  echo "   The instance may need more time to start the SSM agent." >&2
  echo "   You can check status with: aws ssm describe-instance-information --filters 'Key=InstanceIds,Values=$INSTANCE_ID'" >&2
  exit 1
fi
echo

# Create/setup user account via SSM
echo "👤 Setting up user account..."
USER_HOME="/ebs/home/$LINUX_USERNAME"
echo "   User: $LINUX_USERNAME"
echo "   Home: $USER_HOME"
TEMP_SCRIPT=$(mktemp)
printf '#!/bin/bash\nset -eu\nUSER_HOME="%s"\nLINUX_USERNAME="%s"\nif ! id "$LINUX_USERNAME" &>/dev/null; then\n  echo "Creating user account: $LINUX_USERNAME"\n  mkdir -p "$USER_HOME"\n  useradd -d "$USER_HOME" -s /bin/bash "$LINUX_USERNAME"\n  usermod -aG sudo "$LINUX_USERNAME"\n  chown -R "$LINUX_USERNAME:$LINUX_USERNAME" "$USER_HOME"\n  chmod 755 "$USER_HOME"\n  echo "User created and added to sudo group"\nelse\n  echo "User account already exists: $LINUX_USERNAME"\n  usermod -aG sudo "$LINUX_USERNAME" 2>/dev/null || true\n  echo "Ensured user is in sudo group"\nfi\n' "$USER_HOME" "$LINUX_USERNAME" >"$TEMP_SCRIPT"
SCRIPT_B64=$(base64 <"$TEMP_SCRIPT" | tr -d '\n')
rm -f "$TEMP_SCRIPT"
echo "   Script encoded (length: ${#SCRIPT_B64} chars)"
echo "   Sending SSM command..."
COMMAND_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[\"echo $SCRIPT_B64 | base64 -d | bash\"]" \
  --query 'Command.CommandId' \
  --output text 2>/dev/null)

if [ -z "$COMMAND_ID" ]; then
  echo "   ERROR: Failed to send SSM command" >&2
  exit 1
fi

echo "   Command ID: $COMMAND_ID"
echo "   Waiting for user creation to complete..."

for i in {1..30}; do
  STATUS=$(aws ssm get-command-invocation \
    --command-id "$COMMAND_ID" \
    --instance-id "$INSTANCE_ID" \
    --query 'Status' \
    --output text 2>/dev/null || echo "InProgress")

  if [ "$STATUS" = "Success" ]; then
    OUTPUT=$(aws ssm get-command-invocation \
      --command-id "$COMMAND_ID" \
      --instance-id "$INSTANCE_ID" \
      --query 'StandardOutputContent' \
      --output text 2>/dev/null || echo "")
    if [ -n "$OUTPUT" ]; then
      echo "   $OUTPUT"
    fi
    break
  elif [ "$STATUS" = "Failed" ] || [ "$STATUS" = "Cancelled" ]; then
    ERROR_OUTPUT=$(aws ssm get-command-invocation \
      --command-id "$COMMAND_ID" \
      --instance-id "$INSTANCE_ID" \
      --query 'StandardErrorContent' \
      --output text 2>/dev/null || echo "")
    echo "   ERROR: User creation command failed!" >&2
    if [ -n "$ERROR_OUTPUT" ]; then
      echo "   $ERROR_OUTPUT" >&2
    fi
    exit 1
  fi
  sleep 1
done

# Set up oh-my-zsh for this user
echo "🐚 Setting up oh-my-zsh for $LINUX_USERNAME..."
OMZ_TEMP=$(mktemp)
printf '#!/bin/bash\nset -eu\nfor i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24; do\n  [ -x /usr/local/bin/setup-ohmyzsh-for-user.sh ] && break\n  sleep 5\ndone\nrunuser -u %s -- /usr/local/bin/setup-ohmyzsh-for-user.sh %s\n' "$LINUX_USERNAME" "$USER_HOME" >"$OMZ_TEMP"
OMZ_B64=$(base64 <"$OMZ_TEMP" | tr -d '\n')
rm -f "$OMZ_TEMP"
OMZ_CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[\"echo $OMZ_B64 | base64 -d | bash\"]" \
  --query 'Command.CommandId' \
  --output text 2>/dev/null) || true
OMZ_OK=false
if [ -n "$OMZ_CMD_ID" ]; then
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12; do
    STATUS=$(aws ssm get-command-invocation --command-id "$OMZ_CMD_ID" --instance-id "$INSTANCE_ID" --query 'Status' --output text 2>/dev/null || echo "InProgress")
    [ "$STATUS" = "Success" ] && OMZ_OK=true && break
    [ "$STATUS" = "Failed" ] && break
    sleep 10
  done
fi
if [ "$OMZ_OK" = true ]; then
  aws ssm send-command \
    --instance-ids "$INSTANCE_ID" \
    --document-name "AWS-RunShellScript" \
    --parameters "commands=[\"chsh -s /usr/bin/zsh $LINUX_USERNAME 2>/dev/null || true\"]" \
    --output text >/dev/null
fi
sleep 2

# Ensure user is in sudo group, configure passwordless sudo, and ensure S3 mount
echo "🔧 Configuring user access..."
aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[
        'bash -c \"set -eu; usermod -aG sudo \\\"$LINUX_USERNAME\\\" 2>/dev/null || true; echo \\\"$LINUX_USERNAME ALL=(ALL) NOPASSWD:ALL\\\" > /etc/sudoers.d/$LINUX_USERNAME; chmod 440 /etc/sudoers.d/$LINUX_USERNAME; mountpoint -q /data.sb || mount /data.sb 2>/dev/null || true\"'
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
echo

# Clone repo on first login
echo "📦 Setting up development environment..."
REPO_DIR="$USER_HOME/rate-design-platform"
REPO_URL="https://github.com/switchbox-data/rate-design-platform.git"
REPO_COMMAND_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[
        'bash -c \"set -eu; REPO_DIR=\\\"$REPO_DIR\\\"; REPO_URL=\\\"$REPO_URL\\\"; LINUX_USERNAME=\\\"$LINUX_USERNAME\\\"; if [ -d \\\"\\\$REPO_DIR/.git\\\" ]; then echo \\\"Repository already exists\\\"; else echo \\\"Cloning repository...\\\"; runuser -u \\\"\\\$LINUX_USERNAME\\\" -- git clone \\\"\\\$REPO_URL\\\" \\\"\\\$REPO_DIR\\\"; echo \\\"Repository cloned. Run: gh auth login && uv sync --python 3.13\\\"; fi\"'
    ]" \
  --query 'Command.CommandId' \
  --output text 2>/dev/null)

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
        REPO_CLONED=false
      else
        echo "   ✅ Repository cloned"
        REPO_CLONED=true
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

# Verify user exists
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
echo

# Remediation: if user has no .zshrc, run oh-my-zsh setup now
REMEDIATE_CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[\"if [ ! -f $USER_HOME/.zshrc ] && [ -x /usr/local/bin/setup-ohmyzsh-for-user.sh ]; then runuser -u $LINUX_USERNAME -- /usr/local/bin/setup-ohmyzsh-for-user.sh $USER_HOME; chsh -s /usr/bin/zsh $LINUX_USERNAME 2>/dev/null || true; fi\"]" \
  --query 'Command.CommandId' \
  --output text 2>/dev/null) || true
if [ -n "$REMEDIATE_CMD_ID" ]; then
  for _ in 1 2 3 4 5; do
    RSTATUS=$(aws ssm get-command-invocation --command-id "$REMEDIATE_CMD_ID" --instance-id "$INSTANCE_ID" --query 'Status' --output text 2>/dev/null || echo "InProgress")
    [ "$RSTATUS" = "Success" ] || [ "$RSTATUS" = "Failed" ] && break
    sleep 5
  done
fi

# Set up SSH access for Cursor
echo "Setting up SSH access for Cursor..."
SSH_KEY_NAME="rate_design_platform_ec2"
SSH_KEY=~/.ssh/${SSH_KEY_NAME}.pub
SSH_KEY_PRIVATE=~/.ssh/${SSH_KEY_NAME}

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

# Configure SSH config with SSM ProxyCommand (replaces port-forwarding, which
# had a busy-polling loop that macOS killed under load via CPU wake limits)
REPO_DIR="$USER_HOME/rate-design-platform"

if [ -f "$SSH_KEY" ]; then
  mkdir -p ~/.ssh
  chmod 700 ~/.ssh

  if grep -q "Host rate-design-platform" ~/.ssh/config 2>/dev/null; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
      sed -i '' '/^Host rate-design-platform$/,/^$/d' ~/.ssh/config 2>/dev/null || true
    else
      sed -i.bak '/^Host rate-design-platform$/,/^$/d' ~/.ssh/config 2>/dev/null || true
    fi
  fi

  {
    echo "Host rate-design-platform"
    echo "    HostName $INSTANCE_ID"
    echo "    User $LINUX_USERNAME"
    echo "    IdentityFile $SSH_KEY_PRIVATE"
    echo "    StrictHostKeyChecking no"
    echo "    UserKnownHostsFile /dev/null"
    echo "    ProxyCommand aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters 'portNumber=%p'"
    echo ""
  } >>~/.ssh/config
  chmod 600 ~/.ssh/config
  echo "   SSH config updated (SSM ProxyCommand → $INSTANCE_ID)"
fi

echo "🔍 Verifying SSH connection..."
SSH_TEST_SUCCESS=false
for i in {1..10}; do
  if ssh -o ConnectTimeout=10 rate-design-platform "echo test" >/dev/null 2>&1; then
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

# Run first-login setup script via SSH only if repo was just cloned
if [ "$SSH_TEST_SUCCESS" = true ] && [ "${REPO_CLONED:-false}" = true ]; then
  FIRST_LOGIN_SCRIPT="$SCRIPT_DIR/first-login.sh"
  if [ -f "$FIRST_LOGIN_SCRIPT" ]; then
    echo "🚀 Running first-login setup (gh auth + uv sync)..."
    ssh -t rate-design-platform 'bash -s' <"$FIRST_LOGIN_SCRIPT"
    echo ""
  fi
fi

# Try to open Cursor
if command -v cursor >/dev/null 2>&1 && [ -f "$SSH_KEY" ]; then
  echo "Opening Cursor with remote workspace..."
  if cursor --remote ssh-remote+rate-design-platform "$REPO_DIR" 2>/dev/null; then
    echo "   ✅ Cursor opened successfully"
  else
    echo "   ⚠️  Could not open Cursor remotely"
    echo "   You can manually connect in Cursor to: ssh-remote+rate-design-platform"
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
fi
echo ""

# Open interactive SSM session
echo "Opening interactive session..."
echo "   (Press Ctrl+D to exit)"
echo ""
aws ssm start-session \
  --target "$INSTANCE_ID" \
  --document-name "AWS-StartInteractiveCommand" \
  --parameters "{\"command\":[\"sudo -i -u $LINUX_USERNAME\"]}"

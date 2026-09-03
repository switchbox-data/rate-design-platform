#!/usr/bin/env bash
set -euo pipefail

# aws.sh - Authenticate with AWS via SSO
#
# Usage:
#   .devcontainer/devpod/aws.sh
#
# This script authenticates with AWS via SSO and automatically configures
# SSO if not already configured. It checks for valid credentials first
# and exits early if they're already valid.
#
# It also upgrades pre-existing configs that predate the [sso-session] block.
# Those configs work but cannot refresh, so they force a browser login every
# few hours; upgrading is the only way an already-configured machine gets
# auto-renewing credentials.

# Check for AWS CLI (silent if installed, error if not)
if ! command -v aws >/dev/null 2>&1; then
  echo "❌ ERROR: AWS CLI is not installed" >&2
  echo "" >&2
  echo "Install AWS CLI first:" >&2
  echo "  https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" >&2
  echo "" >&2
  exit 1
fi

# Does the default profile point at an [sso-session] block? Only that shape gets
# OIDC refresh tokens. Older configs set sso_start_url directly in [default],
# which authenticates fine but cannot renew, so treat them as needing an upgrade.
HAS_SSO_SESSION=false
if [ -n "$(aws configure get sso_session 2>/dev/null || true)" ]; then
  HAS_SSO_SESSION=true
fi

# Fast path: config is already the refreshable shape and credentials still work.
# aws sts get-caller-identity exercises the full credential chain and fails
# if the SSO token is expired.
if [ "$HAS_SSO_SESSION" = true ] && aws sts get-caller-identity &>/dev/null; then
  echo "✅ AWS credentials are already valid"
  echo
  exit 0
fi

CONFIG_FILE=".secrets/aws-sso-config.sh"

if [ "$HAS_SSO_SESSION" = false ]; then
  if [ ! -f "$CONFIG_FILE" ]; then
    if [ -f ~/.aws/config ]; then
      # An existing config still works, so warn instead of failing.
      echo "⚠️  Cannot enable credential auto-renewal: '$CONFIG_FILE' not found."
      echo "   Continuing with your existing config, which means logins will keep"
      echo "   expiring after a few hours. Ask a team member for that file to fix it."
      echo
    else
      echo "❌ ERROR: Missing AWS SSO configuration file" >&2
      echo "" >&2
      echo "   The file '$CONFIG_FILE' is required but not found." >&2
      echo "   Please ask a team member for this file and place it in the .secrets/ directory." >&2
      echo "" >&2
      exit 1
    fi
  else
    # shellcheck source=.secrets/aws-sso-config.sh
    . "$CONFIG_FILE"
    SSO_SESSION_NAME="${SSO_SESSION_NAME:-switchbox}"

    if [ -f ~/.aws/config ]; then
      echo "🔧 Upgrading AWS SSO config to enable credential auto-renewal..."
      CONFIG_BACKUP=~/.aws/config.bak.$(date +%Y%m%d-%H%M%S)
      cp ~/.aws/config "$CONFIG_BACKUP"
      echo "   Backed up existing config to $CONFIG_BACKUP"
      # The config is rewritten wholesale, so flag anything that will be dropped.
      EXTRA_SECTIONS=$(grep -E '^\[' ~/.aws/config | grep -vE '^\[(default\]|sso-session )' || true)
      if [ -n "$EXTRA_SECTIONS" ]; then
        echo "   ⚠️  These sections are not carried over; re-add them from the backup:"
        echo "$EXTRA_SECTIONS" | sed 's/^/        /'
      fi
    else
      echo "🔧 AWS SSO not configured. Setting up SSO configuration..."
    fi
    echo

    # Write config with sso-session block (enables OIDC refresh tokens so
    # credentials auto-renew instead of expiring after a few hours)
    mkdir -p ~/.aws
    cat >~/.aws/config <<AWSCFG
[sso-session ${SSO_SESSION_NAME}]
sso_start_url = ${SSO_START_URL}
sso_region = ${SSO_REGION}
sso_registration_scopes = ${SSO_REGISTRATION_SCOPES:-sso:account:access}

[default]
sso_session = ${SSO_SESSION_NAME}
sso_account_id = ${SSO_ACCOUNT_ID}
sso_role_name = ${SSO_ROLE_NAME}
region = ${SSO_REGION}
output = json
AWSCFG
    chmod 600 ~/.aws/config

    echo "✅ AWS SSO configuration complete"
    echo "   One browser login is needed now. After that, credentials renew on"
    echo "   their own until the SSO session duration set in IAM Identity Center."
    echo
  fi
fi

# Run SSO login (handles browser authentication)
echo "🔓 Starting AWS SSO login..."
echo
aws sso login
echo

# A refresh token in the token cache is what lets credentials renew without a
# browser. Report it, since the failure mode is otherwise invisible until the
# next expiry (SSH to the dev box goes through AWS, so it dies with the token).
if grep -qs refreshToken ~/.aws/sso/cache/*.json; then
  echo "✅ Credentials will auto-renew without another browser login"
else
  echo "⚠️  No refresh token was issued, so logins will expire in a few hours"
fi
echo

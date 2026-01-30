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
if aws sts get-caller-identity &>/dev/null &&
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
if ! aws configure get sso_start_url &>/dev/null ||
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

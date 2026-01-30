#!/usr/bin/env bash
set -euo pipefail

# up-aws.sh - Launch devcontainer on AWS EC2
#
# Usage:
#   .devcontainer/devpod/up-aws.sh [MACHINE_TYPE] [rebuild]
#
# This script launches the devcontainer on an AWS EC2 instance using DevPod.
# MACHINE_TYPE defaults to t3.xlarge if not specified.
# If "rebuild" is passed as an argument, it will rebuild the devcontainer image.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source the aws.sh script to ensure SSO is configured and logged in
# This runs in the same shell session, so the SSO cache will be available to DevPod
# We need to source it in a subshell to avoid its early exit, but still get the SSO login effect
if ! (
  # shellcheck source=.devcontainer/devpod/aws.sh
  . "$SCRIPT_DIR/aws.sh"
); then
  # If aws.sh exited early (credentials already valid), that's fine
  # But we still want to ensure SSO session is fresh for DevPod
  # Force a refresh by running sso login (it will use cached browser session if valid)
  echo "🔄 Refreshing SSO session for DevPod..."
  aws sso login --no-browser 2>/dev/null || aws sso login || true
  echo
fi
# The SSO login from aws.sh will have refreshed the cache, which DevPod can now use

# Use same region as DevPod provider
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# Check for DevPod CLI (silent if installed, error if not)
if ! command -v devpod >/dev/null 2>&1; then
  echo "❌ ERROR: DevPod CLI is not installed" >&2
  echo "" >&2
  echo "Install skevetter fork of DevPod first:" >&2
  echo "  https://github.com/skevetter/devpod/releases" >&2
  echo "" >&2
  exit 1
fi

# Parse arguments
MACHINE_TYPE="${1:-t3.xlarge}"
REBUILD="${2:-}"

# Detect if "rebuild" was passed as first argument (common mistake: just up-aws rebuild)
# If so, treat it as the rebuild flag and use default machine type
if [ "${MACHINE_TYPE}" = "rebuild" ]; then
  MACHINE_TYPE="t3.xlarge"
  REBUILD="rebuild"
fi

# If rebuilding and using default machine type, check if a workspace exists with a different machine type
# This allows "just up-aws rebuild" to rebuild the existing workspace rather than defaulting to t3.xlarge
if [ -n "${REBUILD}" ] && [ "${MACHINE_TYPE}" = "t3.xlarge" ]; then
  # Look for any existing rate-design-platform-aws-* workspace
  EXISTING_WORKSPACE=$(devpod list --output json 2>/dev/null | jq -r '.[] | select(.id | startswith("rate-design-platform-aws-")) | .id' | head -1)
  if [ -n "${EXISTING_WORKSPACE}" ]; then
    # Extract machine type from workspace ID (e.g., rate-design-platform-aws-t3-xlarge -> t3.xlarge)
    DETECTED_TYPE=$(echo "${EXISTING_WORKSPACE}" | sed 's/^rate-design-platform-aws-//' | tr '-' '.')
    echo "🔍 Detected existing workspace with machine type: ${DETECTED_TYPE}"
    echo "   Using this machine type for rebuild instead of default."
    echo
    MACHINE_TYPE="${DETECTED_TYPE}"
  fi
fi

# Warn user and get confirmation if rebuilding
if [ -n "${REBUILD}" ]; then
  echo "==========================================================================="
  echo "⚠️  WARNING: You are about to rebuild the devcontainer image and relaunch the container."
  echo "   This action will wipe any uncommitted files on the server."
  echo "==========================================================================="
  echo
  read -p "Do you really want to proceed? (yes/no): " CONFIRM
  if [ "${CONFIRM}" != "yes" ]; then
    echo "Aborted."
    exit 0
  fi
  echo
fi

# Replace dots with dashes for provider name
PROVIDER_NAME="aws-${MACHINE_TYPE//./-}"

# Add DevPod AWS provider if needed (silent if already exists)
# Using skevetter's fork which has important fixes: https://github.com/skevetter/devpod-provider-aws
if devpod provider add github.com/skevetter/devpod-provider-aws \
  --name "${PROVIDER_NAME}" \
  --option AWS_AMI="ami-05134c8ef96964280" \
  --option AWS_DISK_SIZE="100" \
  --option AWS_INSTANCE_TYPE="${MACHINE_TYPE}" \
  --option AWS_REGION="us-west-2" \
  --option AWS_VPC_ID="vpc-0d19afce59d2395d9" >/dev/null 2>&1; then
  echo "➕ Added DevPod AWS provider '${PROVIDER_NAME}' with machine type ${MACHINE_TYPE}"
  echo
fi

# Create unique workspace ID using provider name
WORKSPACE_ID="rate-design-platform-${PROVIDER_NAME}"

# Check if workspace already exists
if devpod list --output json 2>/dev/null | jq -e --arg id "${WORKSPACE_ID}" '.[] | select(.id == $id)' >/dev/null 2>&1; then
  echo "============================================================================"
  echo "🔄 EC2 instance '${WORKSPACE_ID}' found, reconnecting to devcontainer"
  echo "============================================================================"
  echo
  echo "   This may take a moment..."
else
  echo "==========================================================================="
  echo "🚀 Launching new EC2 instance '${WORKSPACE_ID}' to run devcontainer"
  echo "==========================================================================="
  echo
  echo "   This may take a few minutes..."
fi
echo

# Use prebuilt images from GHCR (built by CI/CD) for fast startup
if [ -n "${REBUILD}" ]; then
  devpod up . \
    --id "${WORKSPACE_ID}" \
    --provider "${PROVIDER_NAME}" \
    --prebuild-repository ghcr.io/switchbox-data/rate-design-platform \
    --ide cursor \
    --recreate \
    --debug
else
  devpod up . \
    --id "${WORKSPACE_ID}" \
    --provider "${PROVIDER_NAME}" \
    --prebuild-repository ghcr.io/switchbox-data/rate-design-platform \
    --ide cursor \
    --debug
fi
echo

echo "========================================================================"
echo "✨ Successfully connected to devcontainer on '${WORKSPACE_ID}'"
echo "========================================================================"
echo

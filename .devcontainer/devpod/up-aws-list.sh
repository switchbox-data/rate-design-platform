#!/usr/bin/env bash
set -euo pipefail

# up-aws-list.sh - Show active EC2 instances running devcontainers
#
# Usage:
#   .devcontainer/devpod/up-aws-list.sh
#
# This script lists all active EC2 instances running devcontainers and provides
# commands to delete them.

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

instances=$(devpod list --output json 2>/dev/null | jq -r '.[] | select(.provider.name | startswith("aws-")) | "  \(.id)\n    Provider: \(.provider.name)\n    Status: \(.status // "unknown")\n    IDE: \(.ide.name // "none")\n    └─ To delete: devpod delete \(.id)"' || echo "")
if [ -z "$instances" ]; then
  echo "Active EC2 instances: None"
  echo ""
  echo "Use just up-aws to launch the rate-design-platform devcontainer on an EC2 instance."
else
  echo "Active EC2 instances:"
  echo ""
  echo "$instances"
  echo ""
  echo "Copy and run a delete command above to terminate an instance."
fi

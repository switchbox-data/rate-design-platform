#!/usr/bin/env bash
set -euo pipefail

# up-aws-list.sh - Show active EC2 instances running devcontainers
#
# Usage:
#   .devcontainer/devpod/up-aws-list.sh
#
# This script lists all active EC2 instances running devcontainers and provides
# commands to delete them.

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

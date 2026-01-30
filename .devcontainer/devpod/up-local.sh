#!/usr/bin/env bash
set -euo pipefail

# up-local.sh - Launch devcontainer locally with Docker
#
# Usage:
#   .devcontainer/devpod/up-local.sh [rebuild]
#
# This script launches the devcontainer locally using Docker and DevPod.
# If "rebuild" is passed as an argument, it will rebuild the devcontainer image.

# Check for DevPod CLI (silent if installed, error if not)
if ! command -v devpod >/dev/null 2>&1; then
  echo "❌ ERROR: DevPod CLI is not installed" >&2
  echo "" >&2
  echo "Install skevetter fork of DevPod first:" >&2
  echo "  https://github.com/skevetter/devpod/releases" >&2
  echo "" >&2
  exit 1
fi

REBUILD="${1:-}"

# Warn user if rebuilding
if [ -n "${REBUILD}" ]; then
  echo "⚠️  WARNING: You are about to rebuild the devcontainer image and relaunch the container."
  echo "   This will reset the container state (runtime package installs will be lost)."
  echo "   Workspace files will persist."
  echo
fi

if ! devpod provider list 2>/dev/null | grep -q "^docker\s"; then
  devpod provider add docker 2>/dev/null || true
fi

# Use prebuilt images from GHCR (built by CI/CD) for fast startup
if [ -n "${REBUILD}" ]; then
  devpod up . \
    --id "rate-design-platform-docker" \
    --provider docker \
    --prebuild-repository ghcr.io/switchbox-data/rate-design-platform \
    --ide cursor \
    --recreate \
    --debug
else
  devpod up . \
    --id "rate-design-platform-docker" \
    --provider docker \
    --prebuild-repository ghcr.io/switchbox-data/rate-design-platform \
    --ide cursor \
    --debug
fi

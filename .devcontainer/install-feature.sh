#!/usr/bin/env bash
set -euo pipefail

# install-feature.sh - Install devcontainer features from GitHub releases
#
# Usage: ./install-feature.sh FEATURE_ID [key=value ...]
# Example: ./install-feature.sh "ghcr.io/devcontainers/features/python:1" version=os-provided installTools=false
#
# This script downloads features from their GitHub repositories and runs their install scripts
# with options passed as environment variables.

if [ $# -lt 1 ]; then
  echo "Usage: $0 FEATURE_ID [key=value ...]" >&2
  exit 1
fi

FEATURE_ID="$1"
shift

echo "===================================================================="
echo "Installing feature: ${FEATURE_ID}"
echo "===================================================================="

# Parse feature ID to extract registry, owner, repo, feature name, and version
# Format examples:
#   ghcr.io/devcontainers/features/python:1
#   ghcr.io/rocker-org/devcontainer-features/r-rig:1
#   ghcr.io/guiyomh/features/just:0.1.0

if [[ ! "$FEATURE_ID" =~ ^([^/]+)/([^/]+)/([^/]+)/([^:]+):(.+)$ ]]; then
  echo "ERROR: Invalid feature ID format: ${FEATURE_ID}" >&2
  echo "Expected format: registry/owner/repo/feature:version" >&2
  exit 1
fi

REGISTRY="${BASH_REMATCH[1]}"
OWNER="${BASH_REMATCH[2]}"
REPO="${BASH_REMATCH[3]}"
FEATURE_NAME="${BASH_REMATCH[4]}"
VERSION="${BASH_REMATCH[5]}"

# Map GitHub Container Registry to GitHub repo
# ghcr.io/devcontainers/features -> github.com/devcontainers/features
GITHUB_REPO="https://github.com/${OWNER}/${REPO}"

echo "Repository: ${GITHUB_REPO}"
echo "Feature: ${FEATURE_NAME}"
echo "Version: ${VERSION}"

# Create temp directory for feature
FEATURE_DIR=$(mktemp -d)
trap "rm -rf ${FEATURE_DIR}" EXIT

# Download feature tarball from GitHub
# Features are stored in src/${FEATURE_NAME}/ directory in the repo
FEATURE_URL="${GITHUB_REPO}/archive/refs/heads/main.tar.gz"

echo "Downloading feature from ${FEATURE_URL}..."
curl -fsSL "${FEATURE_URL}" | tar -xz -C "${FEATURE_DIR}" --strip-components=1

# Check if feature directory exists
INSTALL_SCRIPT="${FEATURE_DIR}/src/${FEATURE_NAME}/install.sh"
if [ ! -f "${INSTALL_SCRIPT}" ]; then
  echo "ERROR: Feature install script not found at ${INSTALL_SCRIPT}" >&2
  echo "Available paths:" >&2
  find "${FEATURE_DIR}" -name "install.sh" || true
  exit 1
fi

# Export options as environment variables (uppercase, no prefix)
# Example: version=3.11 -> VERSION=3.11
# Example: installTools=false -> INSTALLTOOLS=false
for arg in "$@"; do
  if [[ "$arg" =~ ^([^=]+)=(.*)$ ]]; then
    key="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"

    # Convert to uppercase, remove hyphens
    env_var="$(echo "${key}" | tr '[:lower:]' '[:upper:]' | tr '-' '_')"

    export "${env_var}=${value}"
    echo "  ${env_var}=${value}"
  fi
done

# Set required environment variables that features expect
# We run the container as root, so set user defaults accordingly
export _REMOTE_USER="${_REMOTE_USER:-root}"
export _REMOTE_USER_HOME="${_REMOTE_USER_HOME:-/root}"
export _CONTAINER_USER="${_CONTAINER_USER:-root}"
export _CONTAINER_USER_HOME="${_CONTAINER_USER_HOME:-/root}"

# Run the install script
echo "Running install script..."
chmod +x "${INSTALL_SCRIPT}"
cd "${FEATURE_DIR}/src/${FEATURE_NAME}"
bash "${INSTALL_SCRIPT}"

echo "===================================================================="
echo "Feature installed successfully: ${FEATURE_NAME}"
echo "===================================================================="

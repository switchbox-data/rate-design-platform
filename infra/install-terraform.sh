#!/usr/bin/env bash
set -euo pipefail

# Ensure Terraform is installed. Run from repo root: infra/install-terraform.sh
# Idempotent: exits 0 if terraform already present.

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

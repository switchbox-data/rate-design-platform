#! /usr/bin/env bash

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Dependencies
uv sync

# Install pre-commit hooks
uv run pre-commit install --install-hooks

# Detect architecture and install appropriate Quarto version
ARCH=$(dpkg --print-architecture)
echo "Detected architecture: $ARCH"

if [ "$ARCH" = "amd64" ]; then
    QUARTO_URL="https://quarto.org/download/latest/quarto-linux-amd64.deb"
    QUARTO_FILE="quarto-linux-amd64.deb"
elif [ "$ARCH" = "arm64" ]; then
    QUARTO_URL="https://quarto.org/download/latest/quarto-linux-arm64.deb"
    QUARTO_FILE="quarto-linux-arm64.deb"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

echo "Installing Quarto CLI for $ARCH..."
curl -LO "$QUARTO_URL"
sudo dpkg -i "$QUARTO_FILE"
rm "$QUARTO_FILE"

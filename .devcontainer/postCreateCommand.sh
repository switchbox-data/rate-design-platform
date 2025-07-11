#! /usr/bin/env bash

# Install system dependencies for Python GUI support
echo "Installing python3-tk for tkinter support..."
apt-get update && apt-get install -y python3-tk

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Dependencies
uv sync --group=dev --group=docs

# Install pre-commit hooks
uv run pre-commit install --install-hooks

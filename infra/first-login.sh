#!/bin/bash
# First-login setup script - runs in interactive session

REPO_DIR="$HOME/rate-design-platform"

# Skip if already set up (venv exists)
if [ -d "$REPO_DIR/.venv" ]; then
    exit 0
fi

# Check if repo exists
if [ ! -d "$REPO_DIR" ]; then
    echo "Repository not found at $REPO_DIR"
    exit 0
fi

cd "$REPO_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  First-time setup required                                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Check if gh is authenticated
if ! gh auth status &>/dev/null; then
    echo "📦 GitHub authentication required for private dependencies..."
    echo ""
    gh auth login
    # Configure git to use gh as credential helper (so uv/git can fetch private repos)
    gh auth setup-git
    echo ""
else
    # Ensure git is configured to use gh even if already authenticated
    gh auth setup-git 2>/dev/null || true
fi

# Run uv sync
if [ ! -d ".venv" ]; then
    echo "📦 Installing dependencies..."
    uv sync --python 3.13
    echo ""
    echo "✅ Setup complete!"
fi

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
  # Ensure BROWSER is set so gh can attempt to open browser (or print URL)
  # In SSH/non-interactive sessions, browser won't open but URL will be printed
  export BROWSER="${BROWSER:-}"
  echo "🔐 Starting GitHub authentication..."
  echo "   (If browser doesn't open automatically, copy the URL below and open it manually)"
  echo ""
  # Use --web flag - it will print a URL if browser can't open
  gh auth login --web 2>&1 || {
    echo ""
    echo "⚠️  GitHub authentication failed or was cancelled."
    echo "   You can run 'gh auth login --web' manually later."
    echo ""
  }
  # Configure git to use gh as credential helper (so uv/git can fetch private repos)
  gh auth setup-git 2>/dev/null || true
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

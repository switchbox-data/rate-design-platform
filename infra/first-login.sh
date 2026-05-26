#!/bin/bash
# First-login setup script - runs in interactive session

REPO_DIR="$HOME/rate-design-platform"

# Check if repo exists
if [ ! -d "$REPO_DIR" ]; then
  echo "Repository not found at $REPO_DIR"
  exit 0
fi

cd "$REPO_DIR"

# gh auth + uv sync: only needed when the venv hasn't been created yet.
if [ ! -d ".venv" ]; then
  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║  First-time setup required                                   ║"
  echo "╚══════════════════════════════════════════════════════════════╝"
  echo ""

  # Check if gh is authenticated
  if ! gh auth status &>/dev/null; then
    echo "📦 GitHub authentication required for private dependencies..."
    echo ""
    # Always use --web flag to work reliably in both interactive and non-interactive environments
    # This will either open a browser or print a URL to visit manually
    echo "Opening browser for GitHub authentication..."
    echo "   (If browser doesn't open, visit the URL shown below)"
    echo ""
    gh auth login --web
    # Configure git to use gh as credential helper (so uv/git can fetch private repos)
    gh auth setup-git
    echo ""
  else
    # Ensure git is configured to use gh even if already authenticated
    gh auth setup-git 2>/dev/null || true
  fi

  echo "📦 Installing dependencies..."
  uv sync --python 3.13
  echo ""
fi

# Install git pre-commit hooks (always; idempotent).
# prek is a project dependency so `uv run prek` works after uv sync.
# Non-fatal: hook installation failure should not block environment setup.
echo "🪝 Installing pre-commit hooks..."
if uv run prek install --install-hooks; then
  echo "   ✅ Pre-commit hooks installed"
else
  echo "   ⚠️  Pre-commit hook installation failed (non-fatal)."
  echo "      Run manually later: cd $REPO_DIR && uv run prek install --install-hooks"
fi
echo ""
echo "✅ Setup complete!"

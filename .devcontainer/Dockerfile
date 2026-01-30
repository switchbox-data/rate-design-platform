# syntax=docker/dockerfile:1.4

FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

# ============================================================================
# All commands run as root. The devcontainer also runs as root (remoteUser).
# This eliminates permission issues between build-time and run-time users.
# ============================================================================

# Copy devcontainer-feature installer script
COPY .devcontainer/install-feature.sh /tmp/install-feature.sh
RUN chmod +x /tmp/install-feature.sh

# Install Python
# os-provided: workaround to avoid compiling Python from source
RUN /tmp/install-feature.sh "ghcr.io/devcontainers/features/python:1" \
    version=os-provided \
    installTools=false

# Install build dependencies needed for building Python packages from source (e.g., cairo from git)
# build-essential: gcc, g++, make, and other build tools
# python3-dev: Python development headers needed for building C extensions
# git: needed for cloning git-based dependencies (e.g., cairo)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        git \
    && rm -rf /var/lib/apt/lists/*

# Install just CLI
RUN /tmp/install-feature.sh "ghcr.io/guiyomh/features/just:0.1.0" \
    version=1.42.4

# Install Chrome/Chromium for testing
RUN /tmp/install-feature.sh "ghcr.io/kreemer/features/chrometesting:1"

# Install zsh with plugins and custom plugin repositories
# The zsh feature installs oh-my-zsh for root user and enables plugins in .zshrc.
# Built-in plugins (git, colored-man-pages, colorize, history) are included with oh-my-zsh.
# Custom plugins (zsh-autosuggestions, fast-syntax-highlighting, zsh-autocomplete) need to be cloned separately.
COPY .devcontainer/install-ohmyzsh-plugins.sh /tmp/install-ohmyzsh-plugins.sh
RUN chmod +x /tmp/install-ohmyzsh-plugins.sh && \
    /tmp/install-feature.sh "ghcr.io/nils-geistmann/devcontainers-features/zsh:0" \
        plugins="git colored-man-pages colorize history zsh-autosuggestions fast-syntax-highlighting zsh-autocomplete" \
        theme="bira" && \
    /tmp/install-ohmyzsh-plugins.sh

# Install prek CLI for pre-commit hooks
# Note: The actual hook installation (prek install --install-hooks) happens in postCreateCommand
# because it needs to write to .git/hooks/, which is mounted from the host after container starts
COPY .devcontainer/install-prek.sh /tmp/install-prek.sh
RUN chmod +x /tmp/install-prek.sh && \
    /tmp/install-prek.sh

# Install Python dependencies from pyproject.toml + uv.lock
# This layer rebuilds when either file changes
# Note: venv is installed to /opt/venv (not workspace) to avoid being shadowed by mounted code
COPY .devcontainer/install-python-deps.sh /tmp/install-python-deps.sh
COPY pyproject.toml uv.lock /tmp/
# Set UV_PROJECT_ENVIRONMENT to install venv to /opt/venv
# This ensures uv commands (uv add, uv run, etc.) always use this location
ENV UV_PROJECT_ENVIRONMENT="/opt/venv"
RUN mkdir -p /opt/venv && \
    chmod +x /tmp/install-python-deps.sh && \
    /tmp/install-python-deps.sh /tmp

# Add uv/uvx to PATH (installed to /root/.local/bin)
ENV PATH="/root/.local/bin:${PATH}"

# Set working directory for development
WORKDIR /workspaces/rate-design-platform

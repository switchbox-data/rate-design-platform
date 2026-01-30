# =============================================================================
# ⭐ DEFAULT
# =============================================================================
# If you run `just`, you see all available commands
default:
    @just --list


# =============================================================================
# 🔍 CODE QUALITY & TESTING
# =============================================================================
# These commands check your code quality and run tests

# Run code quality tools (same as CI)
check:
    echo "🚀 Checking lock file consistency with 'pyproject.toml'"
    uv lock --locked
    echo "🚀 Linting, formatting, and type checking code"
    prek run -a

# Check for obsolete dependencies
check-deps:
    echo "🚀 Checking for obsolete dependencies: Running deptry"
    uv run deptry .

# Run tests
test:
    echo "🚀 Testing code: Running pytest"
    uv run python -m pytest --doctest-modules tests/

# =============================================================================
# 🏗️  DEVELOPMENT ENVIRONMENT SETUP
# =============================================================================
# These commands help you set up your development environment

# Install uv, python packages, r packages, prek, and pre-commit hooks
install:
    @echo "🚀 Setting up development environment\n"
    @.devcontainer/install-python-deps.sh .
    @.devcontainer/install-prek.sh
    @.devcontainer/install-prek-deps.sh
    @echo "✨ Development environment ready!\n"

# Clean generated files and caches
clean:
    rm -rf .pytest_cache .ruff_cache tmp

# =============================================================================
# 🔍 AWS
# =============================================================================

# Authenticate with AWS via SSO (for manual AWS CLI usage like S3 access)
# Automatically configures SSO if not already configured
aws:
    .devcontainer/devpod/aws.sh

# Launch devcontainer locally with Docker
up-local rebuild="":
    .devcontainer/devpod/up-local.sh {{ rebuild }}

# Launch devcontainer on AWS EC2, using the specified machine type
up-aws MACHINE_TYPE="t3.xlarge" rebuild="": aws
    .devcontainer/devpod/up-aws.sh {{ MACHINE_TYPE }} {{ rebuild }}

# Show active EC2 instances running devcontainers, and commands to delete them
up-aws-list:
    .devcontainer/devpod/up-aws-list.sh

# =============================================================================
# 🚀 DEVELOPMENT ENVIRONMENT
# =============================================================================

# Ensure Terraform is installed (internal dependency). Depends on aws so credentials
# are valid before any Terraform or infra script runs.
_terraform: aws
    bash infra/install-terraform.sh

# Set up EC2 instance (run once by admin)
# Idempotent: safe to run multiple times
dev-setup: _terraform
    bash infra/dev-setup.sh

# Destroy EC2 instance but preserve data volume (to recreate, run dev-setup again)
dev-teardown: _terraform
    bash infra/dev-teardown.sh

# Destroy everything including data volume (WARNING: destroys all data!)
dev-teardown-all: _terraform
    bash infra/dev-teardown-all.sh

# User login (run by any authorized user)
dev-login: aws
    bash infra/dev-login.sh

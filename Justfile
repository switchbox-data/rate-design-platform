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
    #!/usr/bin/env bash
    set -euo pipefail

    # Check for AWS CLI (silent if installed, error if not)
    if ! command -v aws >/dev/null 2>&1; then
        echo "❌ ERROR: AWS CLI is not installed" >&2
        echo "" >&2
        echo "Install AWS CLI first:" >&2
        echo "  https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" >&2
        echo "" >&2
        exit 1
    fi

    # Check if credentials are already valid (early exit if so)
    # Test with an actual EC2 API call since DevPod uses EC2
    if aws sts get-caller-identity &>/dev/null && \
       aws ec2 describe-instances --max-results 5 &>/dev/null; then
        echo "✅ AWS credentials are already valid"
        echo
        exit 0
    fi

    # Credentials are not valid, so we need to configure and/or login
    # Load AWS SSO configuration from shell script (needed for session name)
    CONFIG_FILE=".secrets/aws-sso-config.sh"
    if [ -f "$CONFIG_FILE" ]; then
        # shellcheck source=.secrets/aws-sso-config.sh
        . "$CONFIG_FILE"
    fi
    # Check if SSO is already configured for the default profile
    # (default profile is used when --profile is not specified)
    NEEDS_CONFIG=false
    if ! aws configure get sso_start_url &>/dev/null || \
       ! aws configure get sso_region &>/dev/null; then
        NEEDS_CONFIG=true
    fi

    if [ "$NEEDS_CONFIG" = true ]; then
        echo "🔧 AWS SSO not configured. Setting up SSO configuration..."
        echo

        # Load AWS SSO configuration from shell script
        CONFIG_FILE=".secrets/aws-sso-config.sh"
        if [ ! -f "$CONFIG_FILE" ]; then
            echo "❌ ERROR: Missing AWS SSO configuration file" >&2
            echo "" >&2
            echo "   The file '$CONFIG_FILE' is required but not found." >&2
            echo "   Please ask a team member for this file and place it in the .secrets/ directory." >&2
            echo "" >&2
            exit 1
        fi

        # Source the configuration file
        # shellcheck source=.secrets/aws-sso-config.sh
        . "$CONFIG_FILE"


        # Configure default profile with SSO settings
        aws configure set sso_start_url "$SSO_START_URL"
        aws configure set sso_region "$SSO_REGION"
        aws configure set sso_account_id "$SSO_ACCOUNT_ID"
        aws configure set sso_role_name "$SSO_ROLE_NAME"
        aws configure set region "$SSO_REGION"
        aws configure set output "json"

        echo "✅ AWS SSO configuration complete"
        echo
    fi

    # Run SSO login (handles browser authentication)
    # Use profile-based login since we configure SSO settings directly on the profile
    echo "🔓 Starting AWS SSO login..."
    echo
    aws sso login
    echo

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

# Ensure Terraform is installed (internal dependency)
_terraform:
    bash infra/install-terraform.sh

# Set up EC2 instance (run once by admin)
# Idempotent: safe to run multiple times
dev-setup: aws _terraform
    bash infra/dev-setup.sh

# Destroy EC2 instance but preserve data volume (to recreate, run dev-setup again)
dev-teardown: aws _terraform
    bash infra/dev-teardown.sh

# Destroy everything including data volume (WARNING: destroys all data!)
dev-teardown-all: aws _terraform
    bash infra/dev-teardown-all.sh

# User login (run by any authorized user)
dev-login: aws
    bash infra/dev-login.sh

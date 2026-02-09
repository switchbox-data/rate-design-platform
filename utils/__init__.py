"""Utility helpers wrapping external packages (stub)."""

import os
import subprocess
from pathlib import Path


def get_aws_region(default: str = "us-west-2") -> str:
    """Return AWS region from env (AWS_REGION / AWS_DEFAULT_REGION) or from AWS config (~/.aws/config) via boto3."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        return region
    try:
        import boto3
        session = boto3.Session()
        if session.region_name:
            return session.region_name
    except ImportError:
        pass
    return default


def get_project_root() -> Path:
    """Return the repository root as an absolute path (via git rev-parse --show-toplevel)."""
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        cwd=Path.cwd(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Could not find project root (run from inside the git repository)"
        )
    return Path(result.stdout.strip()).resolve()

"""Utility helpers wrapping external packages (stub)."""

import subprocess
from pathlib import Path


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

"""Types shared by marginal-cost input loaders."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from utils import get_project_root


@dataclass
class InputContext:
    """One allocation run: which utility, which years, and how to reach S3."""

    utility: str
    year: int
    load_year: int
    storage_options: dict[str, str]


@dataclass
class Resolved:
    """A loader result: the value the method receives, plus provenance.

    ``inputs`` are ``input_file`` / ``input_partition`` records for the manifest.
    ``meta`` holds scalars the manifest key needs that are not the method argument
    itself (the filed ``$/kW-yr``, the CPI factor, the load-source label).
    """

    value: Any
    inputs: list[dict[str, Any]] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)


def resolve_path(path: str) -> str:
    """Return ``path`` unchanged when it is S3 or absolute; otherwise under the repo root."""
    if path.startswith("s3://") or Path(path).is_absolute():
        return path
    return str(get_project_root() / path)

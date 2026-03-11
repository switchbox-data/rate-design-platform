"""S3 batch discovery for CAIRO run outputs.

Locates the latest complete batch of runs under:
  s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{batch_name}/

Each batch_name directory contains one subdirectory per CAIRO run, named:
  {cairo_ts}_{run_name}/

Batch names follow the format: {state}_{YYYYMMDD}{letter}_r{run_range} (e.g., "ny_20260305a_r1-2").
"""

from __future__ import annotations

import re
from typing import Any

import boto3

# S3 bucket that holds all CAIRO outputs for this platform.
_CAIRO_OUTPUT_BUCKET = "data.sb"

# Batch name format: {state}_{YYYYMMDD}{letter}_r{run_range} (e.g., "ny_20260305a_r1-2")
_BATCH_NAME_RE = re.compile(r"^[a-z]{2}_\d{8}[a-z]_r\d+-\d+$")


def _cairo_output_prefix(state: str, utility: str) -> str:
    """Return the S3 key prefix (no leading slash, trailing slash included) for a utility.

    Example: ``"switchbox/cairo/outputs/hp_rates/ri/rie/"``
    """
    return f"switchbox/cairo/outputs/hp_rates/{state.lower()}/{utility.lower()}/"


def _list_batch_names(
    s3_client: Any, bucket: str, utility_prefix: str, state: str
) -> list[str]:
    """List CAIRO batch name directories under a utility prefix, sorted ascending.

    Filters to entries matching the {state}_{YYYYMMDD}{letter}_r{run_range} pattern so stray objects
    or unrelated prefixes are ignored. Only includes batches for the given state.

    Args:
        s3_client: Boto3 S3 client (``boto3.client("s3")``).
        bucket: S3 bucket name.
        utility_prefix: S3 key prefix ending with ``"/"`` for the utility directory.
        state: State abbreviation (e.g. ``"ny"``, ``"ri"``) to filter batch names.

    Returns:
        Sorted list of batch name strings (e.g. ``["ny_20260305a_r1-2", ...]``).
    """
    utility_prefix = utility_prefix.rstrip("/") + "/"
    batch_names: list[str] = []
    state_lower = state.lower()

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=utility_prefix, Delimiter="/"):
        for entry in page.get("CommonPrefixes", []):
            # Strip the parent prefix and trailing slash to isolate the dir name.
            dir_name = entry["Prefix"][len(utility_prefix) :].rstrip("/")
            # Match batch name format and ensure it starts with the correct state
            if _BATCH_NAME_RE.match(dir_name) and dir_name.startswith(
                f"{state_lower}_"
            ):
                batch_names.append(dir_name)

    return sorted(batch_names)


def _find_run_dir(
    s3_client: Any,
    bucket: str,
    batch_prefix: str,
    run_name: str,
) -> str | None:
    """Find the run directory for ``run_name`` within a single batch directory.

    Run directories are named ``{cairo_ts}_{run_name}``.  This scans all
    CommonPrefixes under ``batch_prefix`` and returns the first whose
    name ends with ``"_{run_name}"``.

    Args:
        s3_client: Boto3 S3 client.
        bucket: S3 bucket name.
        batch_prefix: Full S3 key prefix for the batch directory,
            ending with ``"/"``.
        run_name: The run_name string as defined in the scenario YAML
            (e.g. ``"ri_rie_run1_up00_precalc__flat"``).

    Returns:
        Full ``s3://`` URI to the run directory (no trailing slash), or ``None`` if
        no matching directory is found.
    """
    batch_prefix = batch_prefix.rstrip("/") + "/"
    run_name_suffix = f"_{run_name}"  # Run dirs are named {cairo_ts}_{run_name}

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=batch_prefix, Delimiter="/"):
        for entry in page.get("CommonPrefixes", []):
            entry_prefix = entry["Prefix"]
            dir_name = entry_prefix[len(batch_prefix) :].rstrip("/")
            if dir_name.endswith(run_name_suffix):
                return f"s3://{bucket}/{entry_prefix.rstrip('/')}"

    return None


def find_latest_complete_batch(
    state: str,
    utility: str,
    run_names: dict[int, str],
    expected_run_count: int | None = None,
) -> tuple[str, dict[int, str]]:
    """Find the most recent batch that contains all expected runs.

    Scans batch name directories under the utility prefix from newest to oldest
    and returns the first one that contains every run in ``run_names``.

    Args:
        state: State abbreviation (e.g. ``"ny"``, ``"ri"``; case-insensitive).
        utility: Utility identifier (e.g. ``"coned"``, ``"rie"``; case-insensitive).
        run_names: ``{run_num: run_name}`` — the run_name values from the scenario
            YAML (e.g. ``{1: "ri_rie_run1_up00_precalc__flat", ...}``).
        expected_run_count: Number of runs that must be present for a batch to be
            considered complete.  Defaults to ``len(run_names)``.

    Returns:
        ``(batch_name, {run_num: s3_dir})`` where:
        - ``batch_name`` is the batch name string (e.g. ``"ny_20260305a_r1-2"``).
        - ``s3_dir`` is the full ``s3://`` URI to each run directory (no trailing slash).

    Raises:
        FileNotFoundError: If no batch directories exist under the utility
            prefix, or if none of them contain all expected runs.
    """
    s3_client = boto3.client("s3")
    bucket = _CAIRO_OUTPUT_BUCKET
    utility_prefix = _cairo_output_prefix(state, utility)

    batch_names = _list_batch_names(s3_client, bucket, utility_prefix, state)
    if not batch_names:
        raise FileNotFoundError(
            f"No batch directories found under s3://{bucket}/{utility_prefix}"
        )

    required_count = (
        expected_run_count if expected_run_count is not None else len(run_names)
    )

    # Search newest-first so we return the most recent complete batch.
    for batch_name in reversed(batch_names):
        batch_prefix = f"{utility_prefix}{batch_name}/"
        run_dirs: dict[int, str] = {}

        for run_num, run_name in run_names.items():
            run_dir = _find_run_dir(s3_client, bucket, batch_prefix, run_name)
            if run_dir is not None:
                run_dirs[run_num] = run_dir

        if len(run_dirs) == required_count:
            return batch_name, run_dirs

    raise FileNotFoundError(
        f"No complete batch (all {required_count} runs) found in "
        f"{len(batch_names)} batch directories under "
        f"s3://{bucket}/{utility_prefix}"
    )


def resolve_batch(
    state: str,
    utility: str,
    batch_name: str,
    run_names: dict[int, str],
) -> dict[int, str]:
    """Resolve run directories for a specific, known batch.

    Unlike :func:`find_latest_complete_batch`, this does not search — it targets
    exactly the given batch.  Runs that are missing from S3 are omitted
    from the result (no error is raised for them).

    Args:
        state: State abbreviation (e.g. ``"ny"``, ``"ri"``; case-insensitive).
        utility: Utility identifier (e.g. ``"coned"``, ``"rie"``; case-insensitive).
        batch_name: Batch name string in {state}_{YYYYMMDD}{letter}_r{run_range} format
            (e.g. ``"ny_20260305a_r1-2"``).
        run_names: ``{run_num: run_name}`` mapping to resolve.

    Returns:
        ``{run_num: s3_dir}`` for each run found in S3.  Missing runs are omitted.

    Raises:
        ValueError: If ``batch_name`` does not match the batch name format or
            does not start with the given state.
    """
    state_lower = state.lower()
    if not _BATCH_NAME_RE.match(batch_name):
        raise ValueError(
            f"batch_name must be in {{state}}_{{YYYYMMDD}}{{letter}}_r{{run_range}} format "
            f"(e.g. 'ny_20260305a_r1-2'), got: {batch_name!r}"
        )
    if not batch_name.startswith(f"{state_lower}_"):
        raise ValueError(
            f"batch_name must start with state '{state_lower}_', got: {batch_name!r}"
        )

    s3_client = boto3.client("s3")
    bucket = _CAIRO_OUTPUT_BUCKET
    batch_prefix = f"{_cairo_output_prefix(state, utility)}{batch_name}/"

    return {
        run_num: run_dir
        for run_num, run_name in run_names.items()
        if (run_dir := _find_run_dir(s3_client, bucket, batch_prefix, run_name))
        is not None
    }

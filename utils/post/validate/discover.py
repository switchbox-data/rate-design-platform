"""S3 batch discovery for CAIRO run outputs.

Locates the latest complete batch of runs under:
  s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{execution_time}/

Each execution_time directory contains one subdirectory per CAIRO run, named:
  {cairo_ts}_{run_name}/

Batch names follow the format: {state}_{YYYYMMDD}_{letter} (e.g., "ny_20250115_a").
"""

from __future__ import annotations

import re
from typing import Any

import boto3

# S3 bucket that holds all CAIRO outputs for this platform.
_CAIRO_OUTPUT_BUCKET = "data.sb"

# Batch name format: {state}_{YYYYMMDD}_{letter} (e.g., "ny_20250115_a")
_BATCH_NAME_RE = re.compile(r"^[a-z]{2}_\d{8}_[a-z]$")


def _cairo_output_prefix(state: str, utility: str) -> str:
    """Return the S3 key prefix (no leading slash, trailing slash included) for a utility.

    Example: ``"switchbox/cairo/outputs/hp_rates/ri/rie/"``
    """
    return f"switchbox/cairo/outputs/hp_rates/{state.lower()}/{utility.lower()}/"


def _list_execution_times(
    s3_client: Any, bucket: str, utility_prefix: str, state: str
) -> list[str]:
    """List CAIRO execution_time directories under a utility prefix, sorted ascending.

    Filters to entries matching the {state}_{YYYYMMDD}_{letter} pattern so stray objects
    or unrelated prefixes are ignored. Only includes batches for the given state.

    Args:
        s3_client: Boto3 S3 client (``boto3.client("s3")``).
        bucket: S3 bucket name.
        utility_prefix: S3 key prefix ending with ``"/"`` for the utility directory.
        state: State abbreviation (e.g. ``"ny"``, ``"ri"``) to filter batch names.

    Returns:
        Sorted list of execution_time strings (e.g. ``["ny_20250115_a", ...]``).
    """
    utility_prefix = utility_prefix.rstrip("/") + "/"
    execution_times: list[str] = []
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
                execution_times.append(dir_name)

    return sorted(execution_times)


def _find_run_dir(
    s3_client: Any,
    bucket: str,
    execution_time_prefix: str,
    run_name: str,
) -> str | None:
    """Find the run directory for ``run_name`` within a single execution_time directory.

    Run directories are named ``{cairo_ts}_{run_name}``.  This scans all
    CommonPrefixes under ``execution_time_prefix`` and returns the first whose
    name ends with ``"_{run_name}"``.

    Args:
        s3_client: Boto3 S3 client.
        bucket: S3 bucket name.
        execution_time_prefix: Full S3 key prefix for the execution_time directory,
            ending with ``"/"``.
        run_name: The run_name string as defined in the scenario YAML
            (e.g. ``"ri_rie_run1_up00_precalc__flat"``).

    Returns:
        Full ``s3://`` URI to the run directory (no trailing slash), or ``None`` if
        no matching directory is found.
    """
    execution_time_prefix = execution_time_prefix.rstrip("/") + "/"
    run_name_suffix = f"_{run_name}"  # Run dirs are named {cairo_ts}_{run_name}

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket, Prefix=execution_time_prefix, Delimiter="/"
    ):
        for entry in page.get("CommonPrefixes", []):
            entry_prefix = entry["Prefix"]
            dir_name = entry_prefix[len(execution_time_prefix) :].rstrip("/")
            if dir_name.endswith(run_name_suffix):
                return f"s3://{bucket}/{entry_prefix.rstrip('/')}"

    return None


def find_latest_complete_batch(
    state: str,
    utility: str,
    run_names: dict[int, str],
    expected_run_count: int | None = None,
) -> tuple[str, dict[int, str]]:
    """Find the most recent execution_time batch that contains all expected runs.

    Scans execution_time directories under the utility prefix from newest to oldest
    and returns the first one that contains every run in ``run_names``.

    Args:
        state: State abbreviation (e.g. ``"ny"``, ``"ri"``; case-insensitive).
        utility: Utility identifier (e.g. ``"coned"``, ``"rie"``; case-insensitive).
        run_names: ``{run_num: run_name}`` — the run_name values from the scenario
            YAML (e.g. ``{1: "ri_rie_run1_up00_precalc__flat", ...}``).
        expected_run_count: Number of runs that must be present for a batch to be
            considered complete.  Defaults to ``len(run_names)``.

    Returns:
        ``(execution_time, {run_num: s3_dir})`` where:
        - ``execution_time`` is the batch name string (e.g. ``"ny_20250115_a"``).
        - ``s3_dir`` is the full ``s3://`` URI to each run directory (no trailing slash).

    Raises:
        FileNotFoundError: If no execution_time directories exist under the utility
            prefix, or if none of them contain all expected runs.
    """
    s3_client = boto3.client("s3")
    bucket = _CAIRO_OUTPUT_BUCKET
    utility_prefix = _cairo_output_prefix(state, utility)

    execution_times = _list_execution_times(s3_client, bucket, utility_prefix, state)
    if not execution_times:
        raise FileNotFoundError(
            f"No execution_time directories found under s3://{bucket}/{utility_prefix}"
        )

    required_count = (
        expected_run_count if expected_run_count is not None else len(run_names)
    )

    # Search newest-first so we return the most recent complete batch.
    for execution_time in reversed(execution_times):
        et_prefix = f"{utility_prefix}{execution_time}/"
        run_dirs: dict[int, str] = {}

        for run_num, run_name in run_names.items():
            run_dir = _find_run_dir(s3_client, bucket, et_prefix, run_name)
            if run_dir is not None:
                run_dirs[run_num] = run_dir

        if len(run_dirs) == required_count:
            return execution_time, run_dirs

    raise FileNotFoundError(
        f"No complete batch (all {required_count} runs) found in "
        f"{len(execution_times)} execution_time directories under "
        f"s3://{bucket}/{utility_prefix}"
    )


def resolve_batch(
    state: str,
    utility: str,
    execution_time: str,
    run_names: dict[int, str],
) -> dict[int, str]:
    """Resolve run directories for a specific, known execution_time.

    Unlike :func:`find_latest_complete_batch`, this does not search — it targets
    exactly the given execution_time.  Runs that are missing from S3 are omitted
    from the result (no error is raised for them).

    Args:
        state: State abbreviation (e.g. ``"ny"``, ``"ri"``; case-insensitive).
        utility: Utility identifier (e.g. ``"coned"``, ``"rie"``; case-insensitive).
        execution_time: Batch name string in {state}_{YYYYMMDD}_{letter} format
            (e.g. ``"ny_20250115_a"``).
        run_names: ``{run_num: run_name}`` mapping to resolve.

    Returns:
        ``{run_num: s3_dir}`` for each run found in S3.  Missing runs are omitted.

    Raises:
        ValueError: If ``execution_time`` does not match the batch name format or
            does not start with the given state.
    """
    state_lower = state.lower()
    if not _BATCH_NAME_RE.match(execution_time):
        raise ValueError(
            f"execution_time must be in {{state}}_{{YYYYMMDD}}_{{letter}} format "
            f"(e.g. 'ny_20250115_a'), got: {execution_time!r}"
        )
    if not execution_time.startswith(f"{state_lower}_"):
        raise ValueError(
            f"execution_time batch name must start with state '{state_lower}_', "
            f"got: {execution_time!r}"
        )

    s3_client = boto3.client("s3")
    bucket = _CAIRO_OUTPUT_BUCKET
    et_prefix = f"{_cairo_output_prefix(state, utility)}{execution_time}/"

    return {
        run_num: run_dir
        for run_num, run_name in run_names.items()
        if (run_dir := _find_run_dir(s3_client, bucket, et_prefix, run_name))
        is not None
    }

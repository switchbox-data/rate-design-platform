"""Validation helpers for the ResStock data pipeline.

Each function exits with a clear error message if its check fails, so the
pipeline halts immediately at the point of failure rather than propagating
bad state downstream.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import polars as pl


def validate_local_files(
    label: str,
    state: list[str],
    upgrade_ids: list[str],
    file_types: list[str],
    base_path: Path,
) -> None:
    """Exit with an error if any (file_type, state, upgrade_id) directory has no .parquet files."""
    errors: list[str] = []
    for ft in file_types:
        for s in state:
            for uid in upgrade_ids:
                upgrade_id_padded = uid.zfill(2)
                d = base_path / ft / f"state={s}" / f"upgrade={upgrade_id_padded}"
                files = list(d.glob("*.parquet")) if d.is_dir() else []
                if not files:
                    errors.append(
                        f"  {ft}/state={s}/upgrade={upgrade_id_padded}:"
                        f" no .parquet files found in {d}"
                    )
    if errors:
        print(
            f"ERROR: Validation failed after {label}.\n"
            + "\n".join(errors)
            + "\nExiting.",
            flush=True,
        )
        sys.exit(1)
    print(
        "  Validation passed: found .parquet files for all"
        " (file_type, state, upgrade) combinations.",
        flush=True,
    )


def validate_s3_objects(
    label: str,
    state: list[str],
    upgrade_ids: list[str],
    file_types: list[str],
    s3_base: str,
    local_base: Path,
    spot_check_max: int = 5,
) -> None:
    """Spot-check up to `spot_check_max` specific S3 objects per (file_type, state, upgrade).

    Rather than listing the entire S3 prefix (slow for large directories), picks up to
    `spot_check_max` real filenames from the local directory and checks each one exists on S3.
    """
    errors: list[str] = []
    for ft in file_types:
        for s in state:
            for uid in upgrade_ids:
                upgrade_id_padded = uid.zfill(2)
                local_dir = (
                    local_base / ft / f"state={s}" / f"upgrade={upgrade_id_padded}"
                )
                local_files = (
                    sorted(local_dir.glob("*.parquet")) if local_dir.is_dir() else []
                )
                files_to_check = local_files[:spot_check_max]
                if not files_to_check:
                    errors.append(
                        f"  {ft}/state={s}/upgrade={upgrade_id_padded}:"
                        f" no local .parquet files to spot-check under {local_dir}"
                    )
                    continue
                s3_prefix = (
                    f"{s3_base.rstrip('/')}/{ft}/state={s}/upgrade={upgrade_id_padded}"
                )
                for local_file in files_to_check:
                    s3_path = f"{s3_prefix}/{local_file.name}"
                    result = subprocess.run(
                        ["aws", "s3", "ls", s3_path],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    if result.returncode != 0 or not result.stdout.strip():
                        errors.append(
                            f"  {ft}/state={s}/upgrade={upgrade_id_padded}:"
                            f" object not found at {s3_path}"
                        )
    if errors:
        print(
            f"ERROR: Validation failed after {label}.\n"
            + "\n".join(errors)
            + "\nExiting.",
            flush=True,
        )
        sys.exit(1)
    print(
        f"  Validation passed: spot-checked up to {spot_check_max} S3 objects per"
        " (file_type, state, upgrade) combination.",
        flush=True,
    )


def validate_metadata_readable(
    input_metadata: pl.LazyFrame,
    input_path: Path,
    loc: str,
) -> None:
    """Exit if the metadata LazyFrame schema is empty (file unreadable or corrupt)."""
    if not input_metadata.collect_schema():
        print(
            f"ERROR: Validation failed reading metadata ({loc}).\n"
            f"  File appears empty or unreadable: {input_path}\n"
            f"Exiting.",
            flush=True,
        )
        sys.exit(1)


def validate_metadata_columns(
    output_metadata: pl.LazyFrame,
    expected_cols: frozenset[str],
    transform_name: str,
    loc: str,
) -> None:
    """Exit if expected columns are missing from the output metadata schema."""
    output_cols = set(output_metadata.collect_schema().names())
    missing = expected_cols - output_cols
    if missing:
        print(
            f"ERROR: Validation failed after {transform_name} ({loc}).\n"
            f"  Missing columns: {sorted(missing)}\n"
            f"  Available columns: {sorted(output_cols)}\n"
            f"Exiting.",
            flush=True,
        )
        sys.exit(1)


def validate_metadata_output(
    output_path: Path,
    loc: str,
) -> None:
    """Exit if the output metadata file is missing or empty after sink_parquet."""
    if not output_path.exists() or output_path.stat().st_size == 0:
        print(
            f"ERROR: Validation failed after metadata modification ({loc}).\n"
            f"  Output file missing or empty: {output_path}\n"
            f"Exiting.",
            flush=True,
        )
        sys.exit(1)

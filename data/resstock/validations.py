"""Validation helpers for the ResStock data pipeline.

Each function raises ``RuntimeError`` if its check fails, so the pipeline
halts immediately at the point of failure and the caller's ``except``
handler can record the failure in the manifest.
"""

from __future__ import annotations

import subprocess
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
        raise RuntimeError(f"Validation failed after {label}.\n" + "\n".join(errors))
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
        raise RuntimeError(f"Validation failed after {label}.\n" + "\n".join(errors))
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
    """Raise if the metadata LazyFrame schema is empty (file unreadable or corrupt)."""
    if not input_metadata.collect_schema():
        raise RuntimeError(
            f"Validation failed reading metadata ({loc}).\n"
            f"  File appears empty or unreadable: {input_path}"
        )


def validate_metadata_columns(
    output_metadata: pl.LazyFrame,
    expected_cols: frozenset[str],
    transform_name: str,
    loc: str,
) -> None:
    """Raise if expected columns are missing from the output metadata schema."""
    output_cols = set(output_metadata.collect_schema().names())
    missing = expected_cols - output_cols
    if missing:
        raise RuntimeError(
            f"Validation failed after {transform_name} ({loc}).\n"
            f"  Missing columns: {sorted(missing)}\n"
            f"  Available columns: {sorted(output_cols)}"
        )


def validate_metadata_output(
    output_path: Path,
    loc: str,
) -> None:
    """Raise if the output metadata file is missing or empty after sink_parquet."""
    if not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError(
            f"Validation failed after metadata modification ({loc}).\n"
            f"  Output file missing or empty: {output_path}"
        )


def validate_utility_assignment_args(
    states: list[str],
    upgrade_ids: list[str],
    assign_utility: bool,
    utility_assign_upgrade: str,
    supported_states: frozenset[str],
) -> None:
    """Raise RuntimeError if utility assignment args are inconsistent.

    Checks two things when ``assign_utility`` is enabled:

    1. The upgrade used for utility assignment (typically ``"00"``) must be in
       ``upgrade_ids``.
    2. Every requested state must have an assignment implementation.
    """
    if not assign_utility:
        return

    padded = [u.zfill(2) for u in upgrade_ids]
    if utility_assign_upgrade not in padded:
        raise RuntimeError(
            f"Utility assignment requires upgrade {utility_assign_upgrade} metadata, "
            f"but that upgrade is not in --upgrade-ids ({upgrade_ids}). "
            f"Either add {utility_assign_upgrade} to --upgrade-ids or disable "
            f"utility assignment with --assign-utility False."
        )

    unsupported = [s for s in states if s not in supported_states]
    if unsupported:
        raise RuntimeError(
            f"Utility assignment is not implemented for state(s): "
            f"{unsupported}. Supported states: {sorted(supported_states)}. "
            f"Remove the unsupported state(s) or disable utility assignment "
            f"with --assign-utility False."
        )


def validate_no_stale_aggregate_loads(
    state: list[str],
    upgrade_ids: list[str],
    file_types: list[str],
    add_monthly_loads: bool,
    add_annual_loads: bool,
    path_sb: Path,
) -> None:
    """Raise RuntimeError if fetching hourly would leave monthly/annual files stale.

    ``load_curve_monthly`` and ``_sb`` ``load_curve_annual`` are derived from
    ``_sb`` hourly by the aggregation step. If hourly is re-fetched (and then
    modified by non-HP approx / MF adj), any existing aggregate outputs that
    are NOT being regenerated would become inconsistent.

    Note: having ``load_curve_monthly`` in ``--file-types`` does NOT help —
    that clones raw NREL monthly into ``_sb``, which still disagrees with
    modified ``_sb`` hourly. The only fix is enabling the aggregation flag.

    Fires when:

    - ``load_curve_hourly`` is in ``file_types`` (hourly will be refreshed)
    - Existing monthly or annual parquets are found for a requested
      (state, upgrade) whose corresponding aggregation flag is NOT enabled
    """
    if "load_curve_hourly" not in file_types:
        return

    checks: list[str] = []
    if not add_monthly_loads:
        checks.append("load_curve_monthly")
    if not add_annual_loads:
        checks.append("load_curve_annual")

    if not checks:
        return

    stale: list[str] = []
    for s in state:
        for uid in upgrade_ids:
            upgrade_id_padded = uid.zfill(2)
            for file_type in checks:
                out_dir = (
                    path_sb / file_type / f"state={s}" / f"upgrade={upgrade_id_padded}"
                )
                if out_dir.is_dir():
                    existing = list(out_dir.glob("*.parquet"))
                    if existing:
                        stale.append(
                            f"  {file_type}/state={s}/upgrade={upgrade_id_padded}:"
                            f" {len(existing):,} file(s) at {out_dir}"
                        )

    if not stale:
        return

    stale_list = "\n".join(stale)
    raise RuntimeError(
        f"Stale aggregate load curve conflict detected.\n"
        f"\n"
        f"You requested load_curve_hourly without regenerating all existing "
        f"aggregate outputs. The following files in the _sb release would become "
        f"inconsistent with the new hourly files:\n"
        f"\n"
        f"{stale_list}\n"
        f"\n"
        f"Monthly and annual files in the _sb release are derived from modified "
        f"_sb hourly and must be kept in sync. Enable the corresponding "
        f"aggregation flags:\n"
        f"\n"
        f"    --add-monthly-loads True --add-annual-loads True\n"
    )

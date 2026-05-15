"""Orchestration entry point for the ResStock data pipeline.

Runs three phases for each requested state:
  1. Fetch  — download raw ResStock parquet files to the local EBS mount via bsf.
  2. Modify — apply Switchbox-specific metadata transformations (HP customers,
              heating type, natural gas connection, utility assignment, etc.).
              Steps will be added here incrementally.
  3. Upload — sync the modified local files to the corresponding S3 path.

Usage::

    uv run python -m data.resstock.main --state NY
    uv run python -m data.resstock.main --state NY RI
    uv run python -m data.resstock.main --state RI --path-output-dir /data.sb/nrel/resstock
    uv run python -m data.resstock.main --state NY --file-types metadata load_curve_hourly
    uv run python -m data.resstock.main --state NY --upgrade-ids 0 2
    uv run python -m data.resstock.main --state NY --identify-heating-type False
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import polars as pl
import yaml

from data.resstock import fetch_resstock_data
from data.resstock.copy_resstock_data import copy_dir
from data.resstock.identify_heating_type import identify_heating_type
from data.resstock.constants import HEATING_TYPE_COLS
from data.resstock.validations import (
    validate_local_files,
    validate_metadata_columns,
    validate_metadata_output,
    validate_metadata_readable,
    validate_s3_objects,
)


def _parse_bool(v: str) -> bool:
    if v.lower() in ("true", "1", "yes"):
        return True
    if v.lower() in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError(f"Expected a boolean (True/False), got '{v}'")


# ── Defaults from data/resstock/config.yaml ───────────────────────────────────

_CONFIG_PATH = Path(__file__).parent / "config.yaml"

with _CONFIG_PATH.open() as _f:
    _cfg = yaml.safe_load(_f)

_DEFAULT_OUTPUT_DIR: str = _cfg["paths"]["output_dir"]
_DEFAULT_S3_DIR: str = _cfg["paths"]["s3_dir"]
_DEFAULT_RELEASE_YEAR: int = _cfg["resstock"]["release_year"]
_DEFAULT_WEATHER_FILE: str = _cfg["resstock"]["weather_file"]
_DEFAULT_RELEASE_VERSION: int = _cfg["resstock"]["release_version"]
_DEFAULT_UPGRADE_IDS: list[str] = _cfg["resstock"]["upgrade_ids"]
_DEFAULT_FILE_TYPES: list[str] = _cfg["resstock"]["file_types"]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ResStock data pipeline: fetch, modify, and upload.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--state",
        nargs="+",
        required=True,
        metavar="STATE",
        help="One or more 2-letter state codes (e.g. NY RI).",
    )
    parser.add_argument(
        "--path-output-dir",
        default=_DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help="Local EBS directory where ResStock parquet files are written.",
    )
    parser.add_argument(
        "--path-s3-dir",
        default=_DEFAULT_S3_DIR,
        metavar="S3_URI",
        help="S3 base URI mirroring the local output directory.",
    )
    parser.add_argument(
        "--release-year",
        type=int,
        default=_DEFAULT_RELEASE_YEAR,
        help="ResStock release year.",
    )
    parser.add_argument(
        "--weather-file",
        default=_DEFAULT_WEATHER_FILE,
        help="AMY weather file identifier.",
    )
    parser.add_argument(
        "--release-version",
        type=int,
        default=_DEFAULT_RELEASE_VERSION,
        help="ResStock release version number.",
    )
    parser.add_argument(
        "--upgrade-ids",
        nargs="+",
        default=_DEFAULT_UPGRADE_IDS,
        metavar="ID",
        help="Upgrade IDs to download (space-separated integers).",
    )
    parser.add_argument(
        "--file-types",
        nargs="+",
        default=_DEFAULT_FILE_TYPES,
        metavar="TYPE",
        help="File types to download (e.g. metadata load_curve_hourly load_curve_annual).",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        help="Number of buildings to sample (0 = all).",
    )
    parser.add_argument(
        "--identify-heating-type",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add heating-type columns to metadata.parquet, writing metadata-sb.parquet "
            "(default: True). Pass --identify-heating-type False to skip."
        ),
    )
    return parser.parse_args(argv)


def _upload(
    state: list[str],
    file_types: list[str],
    release: str,
    path_output_dir: str | Path,
    path_s3_dir: str,
) -> None:
    """Sync fetched/modified local files to S3, scoped to fetched states and file types."""
    local_base = Path(path_output_dir) / release
    s3_base = f"{path_s3_dir.rstrip('/')}/{release}"

    for file_type in file_types:
        for s in state:
            local_path = local_base / file_type / f"state={s}"
            s3_path = f"{s3_base}/{file_type}/state={s}/"
            print(f"Uploading {local_path} → {s3_path}", flush=True)
            result = subprocess.run(
                ["aws", "s3", "sync", str(local_path), s3_path],
                check=False,
            )
            if result.returncode != 0:
                print(
                    f"  WARNING: aws s3 sync exited with code {result.returncode}",
                    flush=True,
                )


def _modify_metadata(
    metadata: pl.LazyFrame,
    upgrade_id: str,
    run_identify_heating_type: bool,
) -> pl.LazyFrame:
    """Apply all metadata transformations and return the modified LazyFrame.

    Each transformation receives and returns a LazyFrame; nothing is materialised here.
    I/O (scan_parquet / sink_parquet) and iteration over states/upgrades is handled by
    the caller.
    """
    if run_identify_heating_type:
        metadata = identify_heating_type(metadata=metadata, upgrade_id=upgrade_id)
    # TODO: add further metadata transformations here, e.g.:
    #   metadata = identify_hp_customers(metadata=metadata, upgrade_id=upgrade_id)
    #   metadata = add_vulnerability_columns(metadata=metadata, ...)
    return metadata


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    release = f"res_{args.release_year}_{args.weather_file}_{args.release_version}"
    release_sb = f"{release}_sb"
    path_raw = Path(args.path_output_dir) / release
    path_sb = Path(args.path_output_dir) / release_sb
    s3_base_raw = f"{args.path_s3_dir.rstrip('/')}/{release}"
    s3_base_sb = f"{args.path_s3_dir.rstrip('/')}/{release_sb}"

    # ── 1. Fetch ──────────────────────────────────────────────────────────────

    # ── 1a. Fetch raw ResStock data ────────────────────────────────────────────
    print("Fetching raw ResStock data...", flush=True)
    rc = fetch_resstock_data.run(
        state=args.state,
        path_output_dir=args.path_output_dir,
        release_year=args.release_year,
        weather_file=args.weather_file,
        release_version=args.release_version,
        upgrade_ids=args.upgrade_ids,
        file_types=args.file_types,
        sample=args.sample,
    )
    if rc != 0:
        print(f"ERROR: bsf exited with code {rc}. Exiting.", flush=True)
        sys.exit(rc)
    print("Validating fetch...", flush=True)
    validate_local_files(
        label="fetch (step 1a)",
        state=args.state,
        upgrade_ids=args.upgrade_ids,
        file_types=args.file_types,
        base_path=path_raw,
    )

    # ── 1b. Clone raw release to _sb ──────────────────────────────────────────
    # Clone the raw release into _sb so all modifications are applied in-place
    # within the _sb tree, leaving the original NREL files untouched.
    print(f"Cloning {path_raw} → {path_sb}...", flush=True)
    n_copied = copy_dir(path_raw, path_sb)
    print(f"  Cloned {n_copied} files.", flush=True)
    print("Validating clone...", flush=True)
    validate_local_files(
        label="clone (step 1b)",
        state=args.state,
        upgrade_ids=args.upgrade_ids,
        file_types=args.file_types,
        base_path=path_sb,
    )

    # ── 2. Modify ─────────────────────────────────────────────────────────────

    # ── 2a. Modify metadata ────────────────────────────────────────────────────
    print("Modifying metadata...", flush=True)
    for s in args.state:
        for uid in args.upgrade_ids:
            upgrade_id_padded = uid.zfill(2)
            loc = f"state={s} upgrade={upgrade_id_padded}"
            metadata_dir = (
                path_sb / "metadata" / f"state={s}" / f"upgrade={upgrade_id_padded}"
            )
            input_path = metadata_dir / "metadata.parquet"
            output_path = metadata_dir / "metadata-sb.parquet"

            if not input_path.exists():
                print(f"  WARNING: {input_path} not found, skipping.", flush=True)
                continue
            print(f"  {loc}", flush=True)

            # Read
            input_metadata = pl.scan_parquet(str(input_path))
            validate_metadata_readable(input_metadata, input_path, loc)

            # Transform
            output_metadata = _modify_metadata(
                metadata=input_metadata,
                upgrade_id=upgrade_id_padded,
                run_identify_heating_type=args.identify_heating_type,
            )

            # Validate schema after each transformation
            if args.identify_heating_type:
                validate_metadata_columns(
                    output_metadata, HEATING_TYPE_COLS, "identify_heating_type", loc
                )
            # TODO: add validate_metadata_columns calls for further transformations here.

            # Write
            output_metadata.sink_parquet(str(output_path))
            validate_metadata_output(output_path, loc)

    # ── 2b. Modify load curves ─────────────────────────────────────────────────
    # TODO: add load curve modification steps here.

    # ── 3. Upload ─────────────────────────────────────────────────────────────
    print("Uploading raw ResStock data to S3...", flush=True)
    _upload(
        state=args.state,
        file_types=args.file_types,
        release=release,
        path_output_dir=args.path_output_dir,
        path_s3_dir=args.path_s3_dir,
    )
    print("Uploading modified sb ResStock data to S3...", flush=True)
    _upload(
        state=args.state,
        file_types=args.file_types,
        release=release_sb,
        path_output_dir=args.path_output_dir,
        path_s3_dir=args.path_s3_dir,
    )
    print("Validating S3 uploads...", flush=True)
    validate_s3_objects(
        label="upload raw (step 3)",
        state=args.state,
        upgrade_ids=args.upgrade_ids,
        file_types=args.file_types,
        s3_base=s3_base_raw,
    )
    validate_s3_objects(
        label="upload sb (step 3)",
        state=args.state,
        upgrade_ids=args.upgrade_ids,
        file_types=args.file_types,
        s3_base=s3_base_sb,
    )


if __name__ == "__main__":
    main()

"""Orchestration entry point for the ResStock data pipeline.

Runs three phases for each requested state:
  1. Fetch  — download raw ResStock parquet files to the local EBS mount via bsf.
  2. Modify — apply Switchbox-specific metadata transformations (HP customers,
              heating type, natural gas connection, vulnerability columns).
              All four transforms are on by default; pass False to skip any.
  3. Upload — sync the modified local files to the corresponding S3 path.

Metadata transforms run in dependency order:
  identify_hp_customers → identify_heating_type → identify_natgas_connection
  → add_vulnerability_columns (NY only)

Usage::

    uv run python -m data.resstock.main --state NY
    uv run python -m data.resstock.main --state NY RI
    uv run python -m data.resstock.main --state RI --path-output-dir /data.sb/nrel/resstock
    uv run python -m data.resstock.main --state NY --file-types metadata load_curve_hourly load_curve_annual
    uv run python -m data.resstock.main --state NY --upgrade-ids 0 2
    uv run python -m data.resstock.main --state NY --identify-hp-customers False
    uv run python -m data.resstock.main --state NY --identify-natgas-connection False
    uv run python -m data.resstock.main --state RI --add-vulnerability-columns False
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import polars as pl
import yaml

from data.resstock import fetch_resstock_data
from data.resstock.add_vulnerability_columns import (
    add_vulnerability_columns,
    load_puma_conditional_probs,
)
from data.resstock.constants import (
    HEATING_TYPE_COLS,
    HP_CUSTOMERS_COLS,
    NATGAS_CONNECTION_COLS,
    VULNERABILITY_COLS,
)
from data.resstock.copy_resstock_data import copy_dir
from data.resstock.identify_heating_type import identify_heating_type
from data.resstock.identify_hp_customers import identify_hp_customers
from data.resstock.identify_natgas_connection import identify_natgas_connection
from data.resstock.manifest import (
    fail_run,
    finish_run,
    new_run_record,
    record_step,
    upsert_run,
    write_manifest,
)
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
_DEFAULT_S3_PUMS_DIR: str = _cfg["paths"]["s3_pums_dir"]
_DEFAULT_RELEASE_YEAR: int = _cfg["resstock"]["release_year"]
_DEFAULT_WEATHER_FILE: str = _cfg["resstock"]["weather_file"]
_DEFAULT_RELEASE_VERSION: int = _cfg["resstock"]["release_version"]
_DEFAULT_UPGRADE_IDS: list[str] = _cfg["resstock"]["upgrade_ids"]
_DEFAULT_FILE_TYPES: list[str] = _cfg["resstock"]["file_types"]
_DEFAULT_PUMS_SURVEY: str = _cfg["pums"]["survey"]
_DEFAULT_PUMS_YEAR: str = str(_cfg["pums"]["year"])


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
        "--identify-hp-customers",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add postprocess_group.has_hp to metadata (default: True). "
            "Must be True for identify-heating-type to work correctly."
        ),
    )
    parser.add_argument(
        "--identify-heating-type",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add heating-type columns to metadata (default: True). "
            "Requires identify-hp-customers."
        ),
    )
    parser.add_argument(
        "--identify-natgas-connection",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add has_natgas_connection to metadata from load_curve_annual (default: True). "
            "Requires identify-heating-type and 'load_curve_annual' in --file-types."
        ),
    )
    parser.add_argument(
        "--add-vulnerability-columns",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add LMI vulnerability columns from PUMS (default: True). "
            "NY only; pass False for RI."
        ),
    )
    parser.add_argument(
        "--path-s3-pums-dir",
        default=_DEFAULT_S3_PUMS_DIR,
        metavar="S3_URI",
        help="S3 base URI for Census PUMS person data (used by add-vulnerability-columns).",
    )
    parser.add_argument(
        "--pums-survey",
        default=_DEFAULT_PUMS_SURVEY,
        help="PUMS survey type (e.g. acs5) — selects PUMS vintage.",
    )
    parser.add_argument(
        "--pums-year",
        default=_DEFAULT_PUMS_YEAR,
        help="PUMS end year (e.g. 2021) — selects PUMS vintage.",
    )
    return parser.parse_args(argv)


def _upload(
    state: list[str],
    file_types: list[str],
    release: str,
    path_output_dir: str | Path,
    path_s3_dir: str,
) -> None:
    """Sync fetched/modified local files to S3 (data only, not the manifest)."""
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


def _upload_manifest(local_dir: Path, s3_base: str) -> None:
    """Push the manifest to S3 after the run record has been finalized on disk."""
    local_manifest = local_dir / "manifest.yaml"
    s3_manifest = f"{s3_base}/manifest.yaml"
    if not local_manifest.exists():
        return
    print(f"Uploading manifest {local_manifest} → {s3_manifest}", flush=True)
    result = subprocess.run(
        ["aws", "s3", "cp", str(local_manifest), s3_manifest],
        check=False,
    )
    if result.returncode != 0:
        print(
            f"  WARNING: manifest upload exited with code {result.returncode}",
            flush=True,
        )


def _modify_metadata(
    metadata: pl.LazyFrame,
    upgrade_id: str,
    *,
    run_identify_hp_customers: bool,
    run_identify_heating_type: bool,
    run_identify_natgas_connection: bool,
    path_sb: Path,
    state: str,
    run_add_vulnerability_columns: bool,
    pums_base_dir: str | None = None,
    pums_survey: str | None = None,
    pums_year: str | None = None,
) -> pl.LazyFrame:
    """Chain all metadata transformations in dependency order.

    Steps and their dependencies:
      1. identify_hp_customers       → adds postprocess_group.has_hp
      2. identify_heating_type       → adds heating_type columns (requires has_hp)
      3. identify_natgas_connection  → adds has_natgas_connection (requires heats_with_natgas;
                                       loads load_curve_annual from path_sb)
      4. add_vulnerability_columns   → adds LMI vulnerability columns (NY only;
                                       loads PUMS conditional probs from S3)

    Each step receives and returns a LazyFrame. Steps 3 and 4 have internal collects for
    validation; step 4 always materialises the full frame. I/O (scan_parquet / sink_parquet)
    and iteration over states/upgrades is handled by the caller.
    """
    if run_identify_hp_customers:
        metadata = identify_hp_customers(metadata=metadata, upgrade_id=upgrade_id)
    if run_identify_heating_type:
        metadata = identify_heating_type(metadata=metadata, upgrade_id=upgrade_id)
    if run_identify_natgas_connection:
        lca_dir = (
            path_sb / "load_curve_annual" / f"state={state}" / f"upgrade={upgrade_id}"
        )
        if not lca_dir.exists():
            raise RuntimeError(
                f"[state={state} upgrade={upgrade_id}] load_curve_annual not found "
                f"at {lca_dir}. Ensure 'load_curve_annual' is in --file-types."
            )
        load_curve_annual = pl.scan_parquet(str(lca_dir))
        metadata = identify_natgas_connection(
            metadata=metadata, load_curve_annual=load_curve_annual
        )
    if run_add_vulnerability_columns:
        if pums_base_dir is None or pums_survey is None or pums_year is None:
            raise ValueError(
                "pums_base_dir, pums_survey, and pums_year are required "
                "when --add-vulnerability-columns True."
            )
        puma_conditional_probs = load_puma_conditional_probs(
            pums_base_dir=pums_base_dir,
            survey=pums_survey,
            year=pums_year,
            state=state,
        )
        metadata = add_vulnerability_columns(
            metadata=metadata,
            puma_conditional_probs=puma_conditional_probs,
        )
    return metadata


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    release = f"res_{args.release_year}_{args.weather_file}_{args.release_version}"
    release_sb = f"{release}_sb"
    path_raw = Path(args.path_output_dir) / release
    path_sb = Path(args.path_output_dir) / release_sb
    s3_base_raw = f"{args.path_s3_dir.rstrip('/')}/{release}"
    s3_base_sb = f"{args.path_s3_dir.rstrip('/')}/{release_sb}"

    # ── Manifest: start a run record ──────────────────────────────────────────
    run = new_run_record(
        release=release,
        release_sb=release_sb,
        states=args.state,
        upgrade_ids=args.upgrade_ids,
        file_types=args.file_types,
        flags={
            "identify_hp_customers": args.identify_hp_customers,
            "identify_heating_type": args.identify_heating_type,
            "identify_natgas_connection": args.identify_natgas_connection,
            "add_vulnerability_columns": args.add_vulnerability_columns,
            "sample": args.sample,
        },
    )

    try:
        # ── 1. Fetch ──────────────────────────────────────────────────────────
        # ── 1a. Fetch raw ResStock data ────────────────────────────────────────
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
            raise RuntimeError(f"bsf exited with code {rc}")
        print("Validating fetch...", flush=True)
        validate_local_files(
            label="fetch (step 1a)",
            state=args.state,
            upgrade_ids=args.upgrade_ids,
            file_types=args.file_types,
            base_path=path_raw,
        )
        record_step(run, "fetch", path=str(path_raw))
        upsert_run(path_raw, run)

        # ── 1b. Clone raw release to _sb ──────────────────────────────────────
        print(f"Cloning {path_raw} → {path_sb}...", flush=True)
        n_copied = copy_dir(path_raw, path_sb)
        print(f"  Cloned {n_copied} files.", flush=True)
        # Reset the sb manifest: copy_dir copies everything including manifest.yaml
        # from raw. The sb manifest must start fresh and track only sb operations.
        write_manifest(path_sb, {"runs": []})
        print("Validating clone...", flush=True)
        validate_local_files(
            label="clone (step 1b)",
            state=args.state,
            upgrade_ids=args.upgrade_ids,
            file_types=args.file_types,
            base_path=path_sb,
        )
        record_step(run, "clone", files_copied=n_copied, path=str(path_sb))
        upsert_run(path_sb, run)

        # ── 2. Modify ─────────────────────────────────────────────────────────

        # ── 2a. Modify metadata ────────────────────────────────────────────────
        metadata_transforms_applied: list[str] = []
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

                # Read metadata
                input_metadata = pl.scan_parquet(str(input_path))
                validate_metadata_readable(input_metadata, input_path, loc)

                output_metadata = _modify_metadata(
                    metadata=input_metadata,
                    upgrade_id=upgrade_id_padded,
                    run_identify_hp_customers=args.identify_hp_customers,
                    run_identify_heating_type=args.identify_heating_type,
                    run_identify_natgas_connection=args.identify_natgas_connection,
                    path_sb=path_sb,
                    state=s,
                    run_add_vulnerability_columns=args.add_vulnerability_columns,
                    pums_base_dir=args.path_s3_pums_dir,
                    pums_survey=args.pums_survey,
                    pums_year=args.pums_year,
                )

                # Validate output schema for each active transformation.
                if args.identify_hp_customers:
                    validate_metadata_columns(
                        output_metadata, HP_CUSTOMERS_COLS, "identify_hp_customers", loc
                    )
                    if "identify_hp_customers" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("identify_hp_customers")
                if args.identify_heating_type:
                    validate_metadata_columns(
                        output_metadata, HEATING_TYPE_COLS, "identify_heating_type", loc
                    )
                    if "identify_heating_type" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("identify_heating_type")
                if args.identify_natgas_connection:
                    validate_metadata_columns(
                        output_metadata,
                        NATGAS_CONNECTION_COLS,
                        "identify_natgas_connection",
                        loc,
                    )
                    if "identify_natgas_connection" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("identify_natgas_connection")
                if args.add_vulnerability_columns:
                    validate_metadata_columns(
                        output_metadata,
                        VULNERABILITY_COLS,
                        "add_vulnerability_columns",
                        loc,
                    )
                    if "add_vulnerability_columns" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("add_vulnerability_columns")

                # Single sink at the end of the full transformation chain.
                output_metadata.sink_parquet(str(output_path))
                validate_metadata_output(output_path, loc)

        record_step(run, "modify_metadata", transforms=metadata_transforms_applied)
        upsert_run(path_sb, run)

        # ── 2b. Modify load curves ─────────────────────────────────────────────
        # TODO: add load curve modification steps here.

        # ── 3. Upload ─────────────────────────────────────────────────────────
        print("Uploading raw ResStock data to S3...", flush=True)
        _upload(
            state=args.state,
            file_types=args.file_types,
            release=release,
            path_output_dir=args.path_output_dir,
            path_s3_dir=args.path_s3_dir,
        )
        record_step(run, "upload_raw", s3_base=s3_base_raw)
        upsert_run(path_raw, run)
        _upload_manifest(path_raw, s3_base_raw)

        print("Uploading modified sb ResStock data to S3...", flush=True)
        _upload(
            state=args.state,
            file_types=args.file_types,
            release=release_sb,
            path_output_dir=args.path_output_dir,
            path_s3_dir=args.path_s3_dir,
        )
        record_step(run, "upload_sb", s3_base=s3_base_sb)
        upsert_run(path_sb, run)
        _upload_manifest(path_sb, s3_base_sb)

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

        # ── Done ──────────────────────────────────────────────────────────────
        finish_run(run)
        upsert_run(path_raw, run)
        upsert_run(path_sb, run)
        print("Pipeline complete.", flush=True)

    except Exception as e:
        error_msg = str(e)
        print(f"ERROR: {error_msg}", flush=True)
        fail_run(run, error_msg)
        # Best-effort: record the failure in whichever manifests exist on disk.
        for path in (path_raw, path_sb):
            if path.exists():
                upsert_run(path, run)
        sys.exit(1)


if __name__ == "__main__":
    main()

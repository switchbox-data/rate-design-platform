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
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

from data.resstock import fetch_resstock_data

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


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    release = f"res_{args.release_year}_{args.weather_file}_{args.release_version}"

    # ── 1. Fetch ──────────────────────────────────────────────────────────────
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
        sys.exit(rc)

    # ── 2. Modify ─────────────────────────────────────────────────────────────
    # TODO: add metadata transformation steps here (identify HP customers,
    # heating type, natural gas connection, utility assignment, etc.).

    # ── 3. Upload ─────────────────────────────────────────────────────────────
    _upload(
        state=args.state,
        file_types=args.file_types,
        release=release,
        path_output_dir=args.path_output_dir,
        path_s3_dir=args.path_s3_dir,
    )


if __name__ == "__main__":
    main()

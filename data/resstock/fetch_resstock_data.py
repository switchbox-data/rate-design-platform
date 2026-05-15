"""Fetch ResStock metadata and load curves for one or more states via buildstock-fetch (bsf).

Mirrors the ``fetch`` recipe in ``data/resstock/Justfile``, with a configurable
output directory defaulting to the local EBS data mount.

Usage::

    uv run python data/resstock/fetch_resstock_data.py --state NY
    uv run python data/resstock/fetch_resstock_data.py --state NY RI
    uv run python data/resstock/fetch_resstock_data.py --state RI --path-output-dir /data.sb/nrel/resstock
    uv run python data/resstock/fetch_resstock_data.py --state NY --file-types metadata load_curve_hourly
    uv run python data/resstock/fetch_resstock_data.py --state NY --upgrade-ids 0 2
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

# ── Defaults from data/resstock/config.yaml ───────────────────────────────────

_CONFIG_PATH = Path(__file__).parent / "config.yaml"

with _CONFIG_PATH.open() as _f:
    _cfg = yaml.safe_load(_f)

_DEFAULT_OUTPUT_DIR: str = _cfg["paths"]["output_dir"]
_DEFAULT_RELEASE_YEAR: int = _cfg["resstock"]["release_year"]
_DEFAULT_WEATHER_FILE: str = _cfg["resstock"]["weather_file"]
_DEFAULT_RELEASE_VERSION: int = _cfg["resstock"]["release_version"]
_DEFAULT_UPGRADE_IDS: list[str] = _cfg["resstock"]["upgrade_ids"]
_DEFAULT_FILE_TYPES: list[str] = _cfg["resstock"]["file_types"]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch ResStock metadata and load curves via buildstock-fetch (bsf).",
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
        help="Local directory where ResStock parquet files are written.",
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


def run(
    state: list[str],
    path_output_dir: str | Path = _DEFAULT_OUTPUT_DIR,
    release_year: int = _DEFAULT_RELEASE_YEAR,
    weather_file: str = _DEFAULT_WEATHER_FILE,
    release_version: int = _DEFAULT_RELEASE_VERSION,
    upgrade_ids: list[str] = _DEFAULT_UPGRADE_IDS,
    file_types: list[str] = _DEFAULT_FILE_TYPES,
    sample: int = 0,
) -> int:
    """Invoke bsf and return its exit code."""
    states = " ".join(state)
    upgrade_ids_str = " ".join(str(u) for u in upgrade_ids)
    file_types_str = " ".join(file_types)
    output_dir = Path(path_output_dir)

    cmd = [
        "bsf",
        "--product",
        "resstock",
        "--release_year",
        str(release_year),
        "--weather_file",
        weather_file,
        "--release_version",
        str(release_version),
        "--states",
        states,
        "--file_type",
        file_types_str,
        "--upgrade_id",
        upgrade_ids_str,
        "--output_directory",
        str(output_dir),
        "--sample",
        str(sample),
    ]

    print(f"Fetching ResStock data for state(s): {states}", flush=True)
    print(f"  Output directory : {output_dir}", flush=True)
    print(f"  File types       : {file_types_str}", flush=True)
    print(f"  Upgrade IDs      : {upgrade_ids_str}", flush=True)
    print(f"  Running: {' '.join(cmd)}", flush=True)

    return subprocess.run(cmd, check=False).returncode


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    sys.exit(run(**vars(args)))


if __name__ == "__main__":
    main()

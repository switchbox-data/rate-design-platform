"""Orchestration entry point for the ResStock data pipeline.

Currently runs the fetch step. Future steps (identify HP customers, heating
type, natural gas connection, etc.) will be added here.

Usage::

    uv run python -m data.resstock.main --state NY
    uv run python -m data.resstock.main --state NY RI
    uv run python -m data.resstock.main --state RI --path-output-dir /data.sb/nrel/resstock
    uv run python -m data.resstock.main --state NY --file-types metadata load_curve_hourly
    uv run python -m data.resstock.main --state NY --upgrade-ids 0 2
"""

from __future__ import annotations

import argparse

from data.resstock import fetch_resstock_data

# ── Defaults matching data/resstock/Justfile ──────────────────────────────────

_DEFAULT_OUTPUT_DIR = fetch_resstock_data._DEFAULT_OUTPUT_DIR
_DEFAULT_RELEASE_YEAR = fetch_resstock_data._DEFAULT_RELEASE_YEAR
_DEFAULT_WEATHER_FILE = fetch_resstock_data._DEFAULT_WEATHER_FILE
_DEFAULT_RELEASE_VERSION = fetch_resstock_data._DEFAULT_RELEASE_VERSION
_DEFAULT_UPGRADE_IDS = fetch_resstock_data._DEFAULT_UPGRADE_IDS
_DEFAULT_FILE_TYPES = fetch_resstock_data._DEFAULT_FILE_TYPES


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ResStock data pipeline: fetch metadata and load curves.",
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


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    fetch_resstock_data.run(**vars(args))


if __name__ == "__main__":
    main()

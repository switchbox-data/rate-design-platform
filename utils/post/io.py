"""Shared I/O helpers and constants for post-processing scripts."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl
from cloudpathlib import S3Path

ANNUAL_MONTH = "Annual"
BLDG_ID = "bldg_id"
BILL_LEVEL = "bill_level"

LoadCurveType = Literal["hourly", "monthly", "annual"]


def path_or_s3(path_str: str) -> S3Path | Path:
    if path_str.startswith("s3://"):
        return S3Path(path_str)
    return Path(path_str)


def scan(path: str, fmt: Literal["parquet", "csv"] = "csv") -> pl.LazyFrame:
    """Lazy-scan a parquet or CSV file (local or S3)."""
    return pl.scan_parquet(path) if fmt == "parquet" else pl.scan_csv(path)


def scan_load_curves_for_utility(
    path_resstock_release: str,
    state: str,
    upgrade: str,
    utility: str,
    load_curve_type: LoadCurveType = "hourly",
) -> pl.LazyFrame:
    """Scan only the load curve files belonging to a specific electric utility.

    Reads metadata_utility to get bldg_ids, then constructs per-building file
    paths and passes them directly to scan_parquet — no directory listing or
    footer probing of irrelevant files. See context/tools/parquet_reads_local_vs_s3.md.

    Parameters
    ----------
    path_resstock_release:
        Root of the ResStock release, e.g. "s3://data.sb/nrel/resstock/res_2024_amy2018_2"
        or a local path like "/data.sb/nrel/resstock/res_2024_amy2018_2".
    state:
        Two-letter state code (uppercase), e.g. "NY".
    upgrade:
        Zero-padded upgrade id, e.g. "00" or "02".
    utility:
        Electric utility short code as it appears in sb.electric_utility,
        e.g. "or", "coned", "nimo".
    load_curve_type:
        Which load curve dataset to scan: "hourly", "monthly", or "annual".
    """
    base = path_resstock_release.rstrip("/")
    meta_path = f"{base}/metadata_utility/state={state}/utility_assignment.parquet"

    bldg_ids: list[int] = (
        pl.scan_parquet(meta_path)
        .filter(pl.col("sb.electric_utility") == utility)
        .select(BLDG_ID)
        .collect()
        .to_series()
        .to_list()
    )

    if not bldg_ids:
        msg = (
            f"No buildings found for utility '{utility}' in "
            f"{meta_path}. Available utilities: check sb.electric_utility."
        )
        raise ValueError(msg)

    dir_name = f"load_curve_{load_curve_type}"
    load_dir = f"{base}/{dir_name}/state={state}/upgrade={upgrade}"
    upgrade_int = str(int(upgrade))
    paths = [f"{load_dir}/{bldg_id}-{upgrade_int}.parquet" for bldg_id in bldg_ids]

    return pl.scan_parquet(paths)

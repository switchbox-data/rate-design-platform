"""Shared I/O helpers and constants for post-processing scripts."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl
from cloudpathlib import S3Path

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

ANNUAL_MONTH = "Annual"
BLDG_ID = "bldg_id"
BILL_LEVEL = "bill_level"


def path_or_s3(path_str: str) -> S3Path | Path:
    if path_str.startswith("s3://"):
        return S3Path(path_str)
    return Path(path_str)


def _is_s3(path: S3Path | Path) -> bool:
    return isinstance(path, S3Path)


def _load_lazy(
    path: str,
    storage_options: dict[str, str] | None,
    fmt: Literal["parquet", "csv"],
) -> pl.LazyFrame:
    if storage_options is None:
        return pl.scan_parquet(path) if fmt == "parquet" else pl.scan_csv(path)
    if fmt == "parquet":
        return pl.scan_parquet(path, storage_options=storage_options)
    return pl.scan_csv(path, storage_options=storage_options)


def scan_load_curves_for_utility(
    path_resstock_release: str,
    state: str,
    upgrade: str,
    utility: str,
    load_curve_type: Literal[
        "load_curve_hourly", "load_curve_monthly"
    ] = "load_curve_hourly",
    storage_options: dict[str, str] | None = None,
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
        Which load curve dataset to scan.
    storage_options:
        Passed to scan_parquet; required for S3 paths.
    """
    base = path_resstock_release.rstrip("/")
    meta_path = f"{base}/metadata_utility/state={state}/utility_assignment.parquet"

    meta_kwargs: dict[str, object] = {}
    if storage_options is not None:
        meta_kwargs["storage_options"] = storage_options

    bldg_ids: list[int] = (
        pl.scan_parquet(meta_path, **meta_kwargs)
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

    load_dir = f"{base}/{load_curve_type}/state={state}/upgrade={upgrade}"
    upgrade_int = str(int(upgrade))
    paths = [f"{load_dir}/{bldg_id}-{upgrade_int}.parquet" for bldg_id in bldg_ids]

    scan_kwargs: dict[str, object] = {}
    if storage_options is not None:
        scan_kwargs["storage_options"] = storage_options

    return pl.scan_parquet(paths, **scan_kwargs)


class Loader:
    """Lazy-scan loader that auto-detects S3 vs local from a reference path.

    Usage::

        load = Loader(args.path_metadata)
        lf = load("some/file.csv")
        lf = load("some/file.parquet", "parquet")
        # Access raw storage options when needed by other APIs:
        prices = load_monthly_fuel_prices(..., load.storage)
    """

    storage: dict[str, str] | None

    def __init__(self, reference_path: str) -> None:
        self.storage = (
            get_aws_storage_options() if _is_s3(path_or_s3(reference_path)) else None
        )

    def __call__(
        self, path: str, fmt: Literal["csv", "parquet"] = "csv"
    ) -> pl.LazyFrame:
        return _load_lazy(str(path_or_s3(path)), self.storage, fmt)

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


def storage_opts() -> dict[str, str]:
    return get_aws_storage_options()


def path_or_s3(path_str: str) -> S3Path | Path:
    if path_str.startswith("s3://"):
        return S3Path(path_str)
    return Path(path_str)


def is_s3(path: S3Path | Path) -> bool:
    return isinstance(path, S3Path)


def load_lazy(
    path: str,
    storage_options: dict[str, str] | None,
    fmt: Literal["parquet", "csv"],
) -> pl.LazyFrame:
    if storage_options is None:
        return pl.scan_parquet(path) if fmt == "parquet" else pl.scan_csv(path)
    if fmt == "parquet":
        return pl.scan_parquet(path, storage_options=storage_options)
    return pl.scan_csv(path, storage_options=storage_options)

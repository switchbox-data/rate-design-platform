"""Shared data-parsing helpers used by multiple mid-run and pre-run scripts."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import polars as pl


def get_residential_customer_count_from_utility_stats(
    path: str | Path,
    utility: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> int:
    """Read EIA-861 utility stats parquet and return residential customer count for the utility.

    The parquet is state-partitioned (e.g. state=NY/data.parquet) with columns
    utility_code and residential_customers. Filters for utility_code == utility
    (the YAML utility field, e.g. 'coned', 'rie') and returns the single row's
    residential_customers value.

    Raises:
        ValueError: If path has no row for that utility, or more than one row.
    """
    path_str = str(path)
    opts = storage_options if path_str.startswith("s3://") else None
    lf = (
        pl.scan_parquet(path_str, storage_options=opts)
        .filter(pl.col("utility_code") == utility)
        .select("residential_customers")
    )
    df = cast(pl.DataFrame, lf.collect())
    if df.height == 0:
        raise ValueError(
            f"No row with utility_code={utility!r} in {path_str}. "
            "Check path_electric_utility_stats and utility in the scenario YAML."
        )
    if df.height > 1:
        raise ValueError(
            f"Expected one row for utility_code={utility!r} in {path_str}, got {df.height}"
        )
    value = df.item(0, 0)
    if value is None:
        raise ValueError(
            f"residential_customers is null for utility_code={utility!r} in {path_str}"
        )
    return int(value)

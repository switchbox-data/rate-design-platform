"""Reusable helpers for the EIA data pipeline.

Loading functions for PUDL tables and our own S3-hosted stats.  Imported by
``fetch_service_territory.py`` and available for ad-hoc analysis.
"""

from __future__ import annotations

from typing import cast

import polars as pl

from data.eia.constants import (
    PUDL_SERVICE_TERRITORY_URL,
    S3_UTILITY_STATS_BASE,
)
from utils import get_aws_region


def _storage_options() -> dict[str, str]:
    """Build Polars-compatible S3 storage options from the project-level AWS config."""
    region = get_aws_region()
    return {"region": region, "default_region": region}


def load_pudl_service_territory(state: str, year: int) -> pl.DataFrame:
    """Load PUDL county service territory for one state and year.

    Returns a DataFrame with columns ``county_id_fips``, ``county``,
    ``utility_id_eia``, and ``utility_name_eia``.
    """
    return cast(
        pl.DataFrame,
        pl.scan_parquet(PUDL_SERVICE_TERRITORY_URL)
        .filter(
            (pl.col("state") == state.upper())
            & (pl.col("report_date").dt.year() == year)
        )
        .select(["county_id_fips", "county", "utility_id_eia", "utility_name_eia"])
        .collect(),
    )


def load_utility_stats(state: str, year: int) -> pl.DataFrame:
    """Load our EIA-861 utility stats for one state/year from S3.

    Returns a DataFrame with columns ``utility_id_eia``, ``entity_type``,
    and ``residential_customers``.
    """
    opts = _storage_options()
    path = f"{S3_UTILITY_STATS_BASE}year={year}/state={state.upper()}/data.parquet"
    return cast(
        pl.DataFrame,
        pl.scan_parquet(path, storage_options=opts)
        .select(["utility_id_eia", "entity_type", "residential_customers"])
        .collect(),
    )

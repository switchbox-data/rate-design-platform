"""Reusable helpers for the EIA data pipeline.

Loading functions for PUDL tables and our own S3-hosted stats.  Imported by
``fetch_service_territory.py`` and available for ad-hoc analysis.
"""

from __future__ import annotations


import polars as pl

from data.eia.constants import (
    PUDL_EIA176_BY_CONSUMER_URL,
    PUDL_EIA176_DISPOSITION_URL,
    PUDL_SERVICE_TERRITORY_URL,
    S3_UTILITY_STATS_BASE,
)
from utils import get_aws_region
from utils.units import KWH_PER_MMBTU


def _storage_options() -> dict[str, str]:
    """Build Polars-compatible S3 storage options from the project-level AWS config."""
    region = get_aws_region()
    return {"region": region, "default_region": region}


def load_pudl_service_territory(state: str, year: int) -> pl.DataFrame:
    """Load PUDL county service territory for one state and year.

    Returns a DataFrame with columns ``county_id_fips``, ``county``,
    ``utility_id_eia``, and ``utility_name_eia``.
    """
    return (
        pl.scan_parquet(PUDL_SERVICE_TERRITORY_URL)
        .filter(
            (pl.col("state") == state.upper())
            & (pl.col("report_date").dt.year() == year)
        )
        .select(["county_id_fips", "county", "utility_id_eia", "utility_name_eia"])
        .collect()
    )


def load_utility_stats(state: str, year: int) -> pl.DataFrame:
    """Load our EIA-861 utility stats for one state/year from S3.

    Returns a DataFrame with columns ``utility_id_eia``, ``entity_type``,
    and ``residential_customers``.
    """
    opts = _storage_options()
    path = f"{S3_UTILITY_STATS_BASE}year={year}/state={state.upper()}/data.parquet"
    return (
        pl.scan_parquet(path, storage_options=opts)
        .select(["utility_id_eia", "entity_type", "residential_customers"])
        .collect()
    )


def load_eia176_residential_by_operator(
    state: str,
    year: int,
    path_by_consumer: str = PUDL_EIA176_BY_CONSUMER_URL,
    path_disposition: str = PUDL_EIA176_DISPOSITION_URL,
) -> pl.DataFrame:
    """EIA-176 residential sales + transportation per operator, in kWh and consumers.

    Transportation is gas the utility delivers to residential customers who buy
    the commodity from a marketer; ResStock models their use too. ``consumers``
    counts meters, not dwellings.

    Some operators file the disposition schedule but leave delivered-gas heat
    content blank (24 of 1,290 operators with residential deliveries in 2018).
    Heat content varies little across operators (2018 median 1.031 MMBtu/Mcf,
    middle half 1.021 to 1.052), so those operators get the state median rather
    than being dropped, which would remove their entire volume.
    ``heat_content_imputed`` flags them.

    Returns one row per operator with ``operator_id_eia``, ``volume_mcf``,
    ``heat_content_mmbtu_per_mcf``, ``heat_content_imputed``, ``eia_kwh``, and
    ``eia_customers``.
    """
    deliveries = (
        pl.scan_parquet(path_by_consumer)
        .filter(
            (pl.col("operating_state") == state)
            & (pl.col("report_year") == year)
            & (pl.col("customer_class") == "residential")
            & pl.col("revenue_class").cast(pl.String).is_in(["sales", "transport"])
        )
        .group_by("operator_id_eia")
        .agg(
            pl.col("volume_mcf").cast(pl.Float64).sum().alias("volume_mcf"),
            pl.col("consumers").cast(pl.Float64).sum().alias("eia_customers"),
        )
    )
    heat = (
        pl.scan_parquet(path_disposition)
        .filter((pl.col("operating_state") == state) & (pl.col("report_year") == year))
        .select(
            "operator_id_eia",
            pl.col("delivered_gas_heat_content_mmbtu_per_mcf")
            .cast(pl.Float64)
            .alias("heat_content_mmbtu_per_mcf"),
        )
    )
    joined = deliveries.join(heat, on="operator_id_eia", how="left").collect()
    if joined.height == 0:
        raise ValueError(f"EIA-176 has no residential deliveries for {state} {year}.")
    heat_col = pl.col("heat_content_mmbtu_per_mcf")
    if joined["heat_content_mmbtu_per_mcf"].drop_nulls().len() == 0:
        raise ValueError(
            f"EIA-176 has no delivered-gas heat content for {state} {year}."
        )
    return (
        joined.with_columns(
            heat_col.is_null().alias("heat_content_imputed"),
            heat_col.fill_null(heat_col.median()),
        )
        .with_columns(
            (pl.col("volume_mcf") * heat_col * KWH_PER_MMBTU).alias("eia_kwh")
        )
        .select(
            "operator_id_eia",
            "volume_mcf",
            "heat_content_mmbtu_per_mcf",
            "heat_content_imputed",
            "eia_kwh",
            "eia_customers",
        )
        .sort("operator_id_eia")
    )

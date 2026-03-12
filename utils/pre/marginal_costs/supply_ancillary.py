"""Ancillary service marginal cost computation for ISO-NE supply MCs."""

from __future__ import annotations

import polars as pl

from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ISONE_ANCILLARY_S3_BASE,
    prepare_component_output,
    strip_tz_if_needed,
)


def load_ancillary_for_year(
    year: int,
    storage_options: dict[str, str],
    ancillary_s3_base: str = DEFAULT_ISONE_ANCILLARY_S3_BASE,
) -> pl.DataFrame:
    """Load ISO-NE ancillary service prices for a given year.

    Reads from the Hive-partitioned ``s3://data.sb/isone/ancillary/`` tree,
    filters by *year*, sums ``reg_service_price_usd_per_mwh`` and
    ``reg_capacity_price_usd_per_mwh`` into a single ``ancillary_cost_enduse``
    column ($/MWh), and returns a DataFrame with columns ``timestamp`` and
    ``ancillary_cost_enduse``.
    """
    base = ancillary_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("year") == year)
        .select(
            "interval_start_et",
            "reg_service_price_usd_per_mwh",
            "reg_capacity_price_usd_per_mwh",
        )
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from ISO-NE ancillary collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ISO-NE ancillary data found for year={year} under {base}"
        )

    collected = strip_tz_if_needed(collected, "interval_start_et").rename(
        {"interval_start_et": "timestamp"}
    )

    # Report DST fallback duplicates explicitly. Stripping the tz from an
    # America/New_York timestamp makes the two 1:00 AM fall-back hours collide
    # on the same naive timestamp. We surface that here; prepare_component_output
    # will average the pair and align the series to exactly 8760 hours.
    n_rows = collected.height
    n_unique_ts = collected.select(pl.col("timestamp").n_unique()).item()
    if n_rows != n_unique_ts:
        n_dst_dup = n_rows - n_unique_ts
        print(
            f"  DST fallback: {n_dst_dup} duplicate naive timestamp(s) detected "
            f"({n_rows} rows, {n_unique_ts} unique). Both fallback-hour values will be "
            f"preserved and averaged during alignment to 8760."
        )

    result = collected.with_columns(
        (
            pl.col("reg_service_price_usd_per_mwh")
            + pl.col("reg_capacity_price_usd_per_mwh")
        ).alias("ancillary_cost_enduse")
    ).select("timestamp", "ancillary_cost_enduse")

    # Validate data completeness - check for missing months
    months_present = (
        result.with_columns(pl.col("timestamp").dt.month().alias("month"))
        .select("month")
        .unique()
        .sort("month")
        .to_series()
        .to_list()
    )
    expected_months = list(range(1, 13))
    missing_months = sorted(set(expected_months) - set(months_present))
    if missing_months:
        raise ValueError(
            f"ISO-NE ancillary data is incomplete for year {year}. "
            f"Missing months: {missing_months}. Present months: {months_present}. "
            f"Backfill missing months in source data (s3://data.sb/isone/ancillary/) "
            f"before generating marginal costs."
        )

    avg_ancillary = result["ancillary_cost_enduse"].mean()
    print(
        f"Loaded ISO-NE ancillary data: {len(result):,} hourly rows, year {year}, "
        f"avg ancillary cost = ${avg_ancillary:.2f}/MWh"
    )
    return result


def compute_supply_ancillary_mc(
    year: int,
    storage_options: dict[str, str],
    ancillary_s3_base: str = DEFAULT_ISONE_ANCILLARY_S3_BASE,
) -> pl.DataFrame:
    """Compute hourly supply ancillary MC from ISO-NE regulation clearing prices.

    Loads ISO-NE ancillary data, sums regulation service and capacity prices,
    and returns a Cairo-compatible 8760 hourly DataFrame with columns
    ``timestamp`` and ``ancillary_cost_enduse`` ($/MWh).

    Raises:
        ValueError: If source data is incomplete (missing months).
    """
    ancillary_df = load_ancillary_for_year(year, storage_options, ancillary_s3_base)
    output = prepare_component_output(
        df=ancillary_df,
        year=year,
        input_col="ancillary_cost_enduse",
        output_col="ancillary_cost_enduse",
        scale=1.0,
    )

    avg_cost = output["ancillary_cost_enduse"].mean()
    print(
        f"  Ancillary MC (ISO-NE): {output.height} hours, "
        f"avg cost = ${avg_cost:.2f}/MWh"
    )
    return output

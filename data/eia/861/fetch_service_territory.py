#!/usr/bin/env python3
"""Fetch EIA-861 electric utility service territory for a state and write to S3.

Uses PUDL's cleaned ``core_eia861__yearly_service_territory`` table, which
maps each distribution utility to the counties it serves, and joins with our
EIA-861 utility stats (already on S3) to compute a per-county utility weight.

The weight column distributes each county across the utilities that serve it,
proportional to their statewide residential customer counts.  For counties
served by a single utility the weight is 1.0.  These weights feed the
county-based PUMA overlap calculation in ``assign_utility_md.py``.

Output:
    s3://data.sb/eia/861/service_territory/state=<STATE>/data.parquet

Schema:
    county_id_fips  str    5-char FIPS (e.g. "24031")
    county          str    county name
    utility_id_eia  i32    EIA utility ID
    utility_name_eia str   EIA utility name
    residential_customers  f32  statewide residential customer count
    weight          f64    fraction of county attributed to this utility
    report_year     i32    EIA-861 report year used

Usage (from project root or data/eia/861/ Justfile):
    uv run python data/eia/861/fetch_service_territory.py MD [--year 2023]
"""

from __future__ import annotations

import argparse

import polars as pl

from data.eia.constants import (
    DISTRIBUTION_ENTITY_TYPES,
    VALID_STATE_CODES,
    service_territory_s3_path,
)
from data.eia.utils import load_pudl_service_territory, load_utility_stats
from utils import get_aws_region


def build_county_utility_weights(
    service_territory: pl.DataFrame,
    utility_stats: pl.DataFrame,
) -> pl.DataFrame:
    """Compute per-county utility weights.

    Joins the county-utility mapping with statewide residential customer
    counts, then normalises within each county so weights sum to 1.0.
    Utilities with zero residential customers (retail marketers, power
    marketers) are excluded.

    Args:
        service_territory: (county_id_fips, county, utility_id_eia,
            utility_name_eia) from PUDL.
        utility_stats: (utility_id_eia, entity_type, residential_customers)
            from our EIA-861 stats.

    Returns:
        DataFrame with columns county_id_fips, county, utility_id_eia,
        utility_name_eia, residential_customers, weight.
    """
    distrib = (
        utility_stats.filter(
            (pl.col("entity_type").is_in(list(DISTRIBUTION_ENTITY_TYPES)))
            | pl.col("entity_type").is_null()
        )
        .filter(pl.col("residential_customers") > 0)
        .select(["utility_id_eia", "residential_customers"])
    )

    merged = service_territory.join(distrib, on="utility_id_eia", how="inner")

    merged = merged.with_columns(
        (
            pl.col("residential_customers")
            / pl.col("residential_customers").sum().over("county_id_fips")
        ).alias("weight")
    )

    return merged.sort(["county_id_fips", "utility_id_eia"])


def fetch_and_upload(state: str, year: int) -> None:
    """Fetch service territory for ``state`` and upload to S3."""
    print(
        f"Fetching PUDL service territory for {state.upper()} year={year} ...",
        flush=True,
    )
    service_territory = load_pudl_service_territory(state, year)
    print(f"  {len(service_territory)} county-utility rows from PUDL")

    print(
        f"Loading EIA-861 utility stats for {state.upper()} year={year} ...",
        flush=True,
    )
    utility_stats = load_utility_stats(state, year)

    weights = build_county_utility_weights(service_territory, utility_stats)
    weights = weights.with_columns(pl.lit(year).cast(pl.Int32).alias("report_year"))
    print(
        f"  {len(weights)} county-utility rows after filtering "
        f"({weights['county_id_fips'].n_unique()} counties, "
        f"{weights['utility_id_eia'].n_unique()} utilities)"
    )

    s3_path = service_territory_s3_path(state)
    region = get_aws_region()
    opts = {"region": region, "default_region": region}
    print(f"Writing to {s3_path} ...", flush=True)
    weights.write_parquet(s3_path, storage_options=opts)
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch EIA-861 county service territory for a state and write to S3. "
            "Requires EIA-861 utility stats to already be on S3 (run "
            "'just -f data/eia/861/Justfile update' first)."
        )
    )
    parser.add_argument(
        "state",
        type=str,
        metavar="STATE",
        help="Two-letter state abbreviation (e.g. MD).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        metavar="YEAR",
        help="EIA-861 report year to use (default: 2023).",
    )
    args = parser.parse_args()

    state = args.state.strip().upper()
    if state.lower() not in VALID_STATE_CODES:
        parser.error(f"Invalid state '{args.state}'. Use a two-letter US state or DC.")

    fetch_and_upload(state=state, year=args.year)


if __name__ == "__main__":
    main()

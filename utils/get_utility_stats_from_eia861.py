#!/usr/bin/env python3
"""Print investor-owned utility stats for a state as CSV to stdout.

Uses EIA-861 yearly sales data (PUDL). Filtered to Investor Owned only.
Uses central utility crosswalk (utils.utility_codes) for utility_code column.

Customer classes are discovered at runtime from the parquet (commercial,
industrial, other, residential, transportation). Dataset schema and
customer_class values are validated in tests/test_get_utility_stats_from_eia861.py.

Freshness: Each row has report_date (EIA-861 reporting period). The script uses
the latest report_date per utility; the parquet has no "file built" date (source:
PUDL Catalyst Coop nightly, EIA-861 temporal coverage 2001-2024).

Columns: utility_id_eia, utility_code, utility_name, business_model, entity_type,
report_date, total_sales_mwh, total_sales_revenue, then for each customer class
({class}_sales_mwh, {class}_sales_revenue, {class}_customers).
Unmapped EIA IDs have null utility_code.
"""

from __future__ import annotations

import argparse
import sys

import polars as pl

from utils.utility_codes import get_eia_utility_id_to_std_name

# EIA-861 yearly sales (PUDL Catalyst Coop nightly)
CORE_EIA861_YEARLY_SALES_URL = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/core_eia861__yearly_sales.parquet"

VALID_STATE_CODES = frozenset(
    {
        "al",
        "ak",
        "az",
        "ar",
        "ca",
        "co",
        "ct",
        "de",
        "fl",
        "ga",
        "hi",
        "id",
        "il",
        "in",
        "ia",
        "ks",
        "ky",
        "la",
        "me",
        "md",
        "ma",
        "mi",
        "mn",
        "ms",
        "mo",
        "mt",
        "ne",
        "nv",
        "nh",
        "nj",
        "nm",
        "ny",
        "nc",
        "nd",
        "oh",
        "ok",
        "or",
        "pa",
        "ri",
        "sc",
        "sd",
        "tn",
        "tx",
        "ut",
        "vt",
        "va",
        "wa",
        "wv",
        "wi",
        "wy",
        "dc",
    }
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print investor-owned utility stats for a state as CSV to stdout."
    )
    parser.add_argument(
        "state",
        type=str,
        metavar="STATE",
        help="Two-letter state abbreviation (e.g. NY, CA).",
    )
    args = parser.parse_args()

    state_raw = args.state.strip().upper()
    if state_raw.lower() not in VALID_STATE_CODES:
        parser.error(f"Invalid state '{args.state}'. Use a two-letter US state or DC.")

    df = (
        pl.read_parquet(CORE_EIA861_YEARLY_SALES_URL)
        .filter(pl.col("state") == state_raw)
        .filter(pl.col("entity_type") == "Investor Owned")
        .filter(
            pl.col("report_date") == pl.col("report_date").max().over("utility_id_eia")
        )
    )

    customer_classes = (
        df.select("customer_class")
        .unique()
        .sort("customer_class")
        .to_series()
        .to_list()
    )

    agg_exprs = [
        pl.col("utility_name_eia").last().alias("utility_name"),
        pl.col("business_model").last().alias("business_model"),
        pl.col("entity_type").last().alias("entity_type"),
        pl.col("report_date").first().alias("report_date"),
        pl.col("sales_mwh").sum().alias("total_sales_mwh"),
        pl.col("sales_revenue").sum().alias("total_sales_revenue"),
    ]
    for c in customer_classes:
        agg_exprs.append(
            pl.col("sales_mwh")
            .filter(pl.col("customer_class") == c)
            .sum()
            .alias(f"{c}_sales_mwh")
        )
        agg_exprs.append(
            pl.col("sales_revenue")
            .filter(pl.col("customer_class") == c)
            .sum()
            .alias(f"{c}_sales_revenue")
        )
        agg_exprs.append(
            pl.col("customers")
            .filter(pl.col("customer_class") == c)
            .sum()
            .alias(f"{c}_customers")
        )

    eia_to_std = get_eia_utility_id_to_std_name(state_raw)
    map_df = pl.DataFrame(
        {
            "utility_id_eia": list(eia_to_std.keys()),
            "utility_code": list(eia_to_std.values()),
        }
    )
    result = (
        df.group_by("utility_id_eia")
        .agg(agg_exprs)
        .sort(["total_sales_mwh", "utility_name"], descending=[True, False])
        .with_columns(pl.col("utility_id_eia").cast(pl.Int64))
        .join(map_df, on="utility_id_eia", how="left")
    )

    # Column order: id, utility_code, name, business_model, entity_type, totals, then per-class triplets
    class_cols = []
    for c in customer_classes:
        class_cols.extend([f"{c}_sales_mwh", f"{c}_sales_revenue", f"{c}_customers"])
    result = result.select(
        [
            "utility_id_eia",
            "utility_code",
            "utility_name",
            "business_model",
            "entity_type",
            "report_date",
            "total_sales_mwh",
            "total_sales_revenue",
        ]
        + class_cols
    )

    result.write_csv(sys.stdout)


if __name__ == "__main__":
    main()

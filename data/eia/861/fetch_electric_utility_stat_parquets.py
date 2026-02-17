#!/usr/bin/env python3
"""Build investor-owned utility stats from EIA-861 yearly sales (PUDL).

Two modes:

1. Parquet output (all states): pass --output-dir. Reads EIA-861 once, aggregates
   IOUs per state, adds a state column, and writes state-partitioned parquet under
   output_dir/state=<state>/data.parquet. Use for building local/S3 utility stats.

2. CSV to stdout (single state): pass STATE as positional argument. Writes one state's
   stats as CSV for ad-hoc use (e.g. just fetch_electric_utility_stat_parquets.py NY).

Uses EIA-861 yearly sales data (PUDL). Filtered to Investor Owned only.
Uses central utility crosswalk (utils.utility_codes) for utility_code column.

Customer classes are discovered at runtime from the parquet (commercial,
industrial, other, residential, transportation). Dataset schema and
customer_class values are validated in tests/test_fetch_electric_utility_stat_parquets.py.

Freshness: Each row has report_date (EIA-861 reporting period). The script uses
the latest report_date per utility; the parquet has no "file built" date (source:
PUDL Catalyst Coop nightly, EIA-861 temporal coverage 2001-2024).

Columns: state (when writing parquet), utility_id_eia, utility_code, utility_name,
business_model, entity_type, report_date, total_sales_mwh, total_sales_revenue,
then for each customer class ({class}_sales_mwh, {class}_sales_revenue, {class}_customers).
Unmapped EIA IDs have null utility_code.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

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


def _aggregate_one_state(df: pl.DataFrame, state_raw: str) -> pl.DataFrame:
    """Aggregate IOU stats for one state. df must already be filtered to that state."""
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
    if eia_to_std:
        map_df = pl.DataFrame(
            {
                "utility_id_eia": list(eia_to_std.keys()),
                "utility_code": list(eia_to_std.values()),
            }
        )
    else:
        map_df = pl.DataFrame(
            schema={"utility_id_eia": pl.Int64, "utility_code": pl.Utf8}
        )
    result = (
        df.group_by("utility_id_eia")
        .agg(agg_exprs)
        .sort(["total_sales_mwh", "utility_name"], descending=[True, False])
        .with_columns(pl.col("utility_id_eia").cast(pl.Int64))
        .join(map_df, on="utility_id_eia", how="left")
    )

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
    return result


def _write_one_state_parquet(
    result: pl.DataFrame, state_raw: str, output_dir: Path
) -> None:
    """Add state column and write result to output_dir/state=<state>/data.parquet."""
    result = result.with_columns(pl.lit(state_raw).alias("state"))
    # Column order: state first, then rest
    result = result.select(["state", *[c for c in result.columns if c != "state"]])
    partition_dir = output_dir / f"state={state_raw}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    result.write_parquet(partition_dir / "data.parquet")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build investor-owned utility stats from EIA-861. Use --output-dir for parquet (all states), or pass STATE for CSV to stdout."
    )
    parser.add_argument(
        "state",
        type=str,
        nargs="?",
        default=None,
        metavar="STATE",
        help="Two-letter state abbreviation (e.g. NY). Required when not using --output-dir.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Write state-partitioned parquet here (state=<state>/data.parquet). Builds all states.",
    )
    args = parser.parse_args()

    if args.output_dir is not None:
        out_str = str(args.output_dir)
        if "{{" in out_str or "}}" in out_str:
            parser.error(
                f"--output-dir looks like an uninterpolated Just variable: {out_str!r}. "
                "Use an absolute path (e.g. project_root + '/data/eia/861/parquet/' in the Justfile)."
            )
        # Build all states to local parquet
        output_dir = args.output_dir.resolve()
        df_all = (
            pl.read_parquet(CORE_EIA861_YEARLY_SALES_URL)
            .filter(pl.col("entity_type") == "Investor Owned")
            .filter(
                pl.col("report_date")
                == pl.col("report_date").max().over(["utility_id_eia", "state"])
            )
        )
        for state_key in sorted(VALID_STATE_CODES):
            state_raw = state_key.upper()
            df_state = df_all.filter(pl.col("state") == state_raw)
            if df_state.is_empty():
                continue
            result = _aggregate_one_state(df_state, state_raw)
            _write_one_state_parquet(result, state_raw, output_dir)
        return

    # Single-state CSV to stdout (backward compat)
    if args.state is None:
        parser.error("STATE is required when not using --output-dir")
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
    result = _aggregate_one_state(df, state_raw)
    result.write_csv(sys.stdout)


if __name__ == "__main__":
    main()

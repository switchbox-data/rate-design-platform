#!/usr/bin/env python3
"""Build utility stats from EIA-861 yearly sales (PUDL).

Two modes:

1. Parquet output (all states, all years): pass --output-dir. Reads EIA-861 once,
   aggregates per (utility, state, report year), and writes parquet partitioned by
   year and state under output_dir/year=<year>/state=<state>/ using Polars partition_by.

2. CSV to stdout (single state): pass STATE as positional argument. Writes one state's
   stats (all years) as CSV for ad-hoc use.

All entity types are included (no filter). Uses central utility crosswalk
(utils.utility_codes) for utility_code column where available.

Customer classes are fixed (commercial, industrial, other, residential, transportation)
to allow a fully lazy pipeline; dataset is validated in tests to match.

Source: PUDL Catalyst Coop stable release (see PUDL_STABLE_VERSION; EIA-861 coverage 2001-2024).

Columns: year, state (when writing parquet), utility_id_eia, utility_code, utility_name,
business_model, entity_type, report_date, total_sales_mwh, total_sales_revenue,
then per customer class ({class}_sales_mwh, {class}_sales_revenue, {class}_customers).
Unmapped EIA IDs have null utility_code.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import cast

import polars as pl

from utils.utility_codes import get_eia_utility_id_to_std_name

# EIA-861 yearly sales (PUDL Catalyst Coop stable release; see https://github.com/catalyst-cooperative/pudl/releases)
PUDL_STABLE_VERSION = "v2026.2.0"
CORE_EIA861_YEARLY_SALES_URL = f"https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/{PUDL_STABLE_VERSION}/core_eia861__yearly_sales.parquet"

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

# Fixed order for lazy aggregation and column output; must match dataset (validated in tests).
CUSTOMER_CLASSES_ORDERED = (
    "commercial",
    "industrial",
    "other",
    "residential",
    "transportation",
)


def _utility_code_map_df() -> pl.DataFrame:
    """(utility_id_eia, state) -> utility_code for all states with EIA mappings."""
    rows: list[dict[str, int | str]] = []
    for state_key in sorted(VALID_STATE_CODES):
        state_raw = state_key.upper()
        eia_to_std = get_eia_utility_id_to_std_name(state_raw)
        for eia_id, std_name in eia_to_std.items():
            rows.append(
                {"utility_id_eia": eia_id, "state": state_raw, "utility_code": std_name}
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "utility_id_eia": pl.Int64,
                "state": pl.Utf8,
                "utility_code": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


def _aggregation_exprs() -> list[pl.Expr]:
    """Aggregation expressions for group_by(utility_id_eia, state)."""
    agg_exprs: list[pl.Expr] = [
        pl.col("utility_name_eia").last().alias("utility_name"),
        pl.col("business_model").last().alias("business_model"),
        pl.col("entity_type").last().alias("entity_type"),
        pl.col("report_date").first().alias("report_date"),
        pl.col("sales_mwh").sum().alias("total_sales_mwh"),
        pl.col("sales_revenue").sum().alias("total_sales_revenue"),
    ]
    for c in CUSTOMER_CLASSES_ORDERED:
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
    return agg_exprs


def _output_columns() -> list[str]:
    """Final column order: year, state (partition cols), then rest."""
    class_cols = []
    for c in CUSTOMER_CLASSES_ORDERED:
        class_cols.extend([f"{c}_sales_mwh", f"{c}_sales_revenue", f"{c}_customers"])
    return [
        "year",
        "state",
        "utility_id_eia",
        "utility_code",
        "utility_name",
        "business_model",
        "entity_type",
        "report_date",
        "total_sales_mwh",
        "total_sales_revenue",
    ] + class_cols


def _base_lazy() -> pl.LazyFrame:
    """Scan EIA-861 and add report year; no entity-type or latest-date filter."""
    return pl.scan_parquet(CORE_EIA861_YEARLY_SALES_URL).with_columns(
        pl.col("report_date").dt.year().alias("year")
    )


def _aggregated_lazy(lf: pl.LazyFrame, utility_code_map: pl.DataFrame) -> pl.LazyFrame:
    """Aggregate to one row per (utility_id_eia, state, year), join utility_code, select output columns."""
    map_lf = utility_code_map.lazy()
    return (
        lf.group_by("utility_id_eia", "state", "year")
        .agg(_aggregation_exprs())
        .sort(
            ["year", "total_sales_mwh", "utility_name"], descending=[False, True, False]
        )
        .with_columns(pl.col("utility_id_eia").cast(pl.Int64))
        .join(map_lf, on=["utility_id_eia", "state"], how="left")
        .select(_output_columns())
    )


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
        help="Write parquet here (year=<year>/state=<state>/). Builds all states and years.",
    )
    args = parser.parse_args()

    if args.output_dir is not None:
        out_str = str(args.output_dir)
        if "{{" in out_str or "}}" in out_str:
            parser.error(
                f"--output-dir looks like an uninterpolated Just variable: {out_str!r}. "
                "Use an absolute path (e.g. project_root + '/data/eia/861/parquet/' in the Justfile)."
            )
        output_dir = args.output_dir.resolve()
        utility_code_map = _utility_code_map_df()
        # Lazy pipeline; single collect then native partition write.
        # Polars writes 00000000.parquet per partition; rename to data.parquet for downstream.
        result = cast(
            pl.DataFrame,
            _aggregated_lazy(_base_lazy(), utility_code_map).collect(),
        )
        result.write_parquet(output_dir, partition_by=["year", "state"], mkdir=True)
        for year_dir in output_dir.iterdir():
            if year_dir.is_dir():
                for state_dir in year_dir.iterdir():
                    if state_dir.is_dir():
                        default_file = state_dir / "00000000.parquet"
                        if default_file.exists():
                            default_file.rename(state_dir / "data.parquet")
        return

    # Single-state CSV to stdout (backward compat)
    if args.state is None:
        parser.error("STATE is required when not using --output-dir")
    state_raw = args.state.strip().upper()
    if state_raw.lower() not in VALID_STATE_CODES:
        parser.error(f"Invalid state '{args.state}'. Use a two-letter US state or DC.")

    utility_code_map = _utility_code_map_df()
    result = cast(
        pl.DataFrame,
        _aggregated_lazy(
            _base_lazy().filter(pl.col("state") == state_raw),
            utility_code_map,
        ).collect(),
    )
    # CSV: drop state (partition col); keep year.
    result.select([c for c in result.columns if c != "state"]).write_csv(sys.stdout)


if __name__ == "__main__":
    main()

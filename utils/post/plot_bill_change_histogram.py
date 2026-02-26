"""Plot weighted histogram of annual bill change (before vs after HP) under default rates.

Reads comb_bills_year_target from run 2 (before) and run 4 (after), tops up with
delivered fuel (oil/propane) costs via add_delivered_fuel_bills, computes delta,
joins to metadata-sb (upgrade 00), applies a population filter, and plots the
weighted distribution. Produces figures 2, 4, 5, 6 depending on --filter.
"""

from __future__ import annotations

import argparse
from typing import Literal, cast

import polars as pl
from plotnine import (
    aes,
    annotate,
    geom_col,
    geom_vline,
    ggplot,
    labs,
    scale_fill_manual,
    theme_minimal,
)

from utils.post.delivered_fuel_bills import (
    add_delivered_fuel_bills,
    load_avg_annual_fuel_price,
)
from utils.post.io import (
    ANNUAL_MONTH,
    BLDG_ID,
    BILL_LEVEL,
    is_s3,
    load_lazy,
    path_or_s3,
    storage_opts,
)

BIN_WIDTH = 100
COMB_BILLS_CSV = "bills/comb_bills_year_target.csv"

FILTER_OPTIONS = ("non_hp", "natgas", "oil_propane", "electric_resistance")

QUADRANT_COLORS = {
    "savings > $1k": "#1b5e20",
    "savings $0-1k": "#81c784",
    "losses $0-1k": "#ef9a9a",
    "losses > $1k": "#b71c1c",
}
QUADRANT_ORDER = [
    "savings > $1k",
    "savings $0-1k",
    "losses $0-1k",
    "losses > $1k",
]


def _apply_population_filter(df: pl.DataFrame, filter_name: str) -> pl.DataFrame:
    if filter_name == "non_hp":
        return df.filter(pl.col("postprocess_group.has_hp") == False)  # noqa: E712
    if filter_name == "natgas":
        return df.filter(pl.col("heats_with_natgas") == True)  # noqa: E712
    if filter_name == "oil_propane":
        return df.filter(
            (pl.col("heats_with_oil") == True)  # noqa: E712
            | (pl.col("heats_with_propane") == True)  # noqa: E712
        )
    if filter_name == "electric_resistance":
        return df.filter(
            pl.col("postprocess_group.heating_type") == "electrical_resistance"
        )
    raise ValueError(f"Unknown filter: {filter_name}. Use one of {FILTER_OPTIONS}")


META_COLS = [
    BLDG_ID,
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
]


def _compute_bill_deltas(
    comb_before: pl.LazyFrame,
    comb_after: pl.LazyFrame,
    annual_before: pl.LazyFrame,
    annual_after: pl.LazyFrame,
    metadata: pl.DataFrame,
    oil_price: float,
    propane_price: float,
    filter_name: str,
) -> pl.DataFrame:
    """Collect bills, add delivered fuel, compute delta, join metadata, filter, return DataFrame."""
    before_topped = add_delivered_fuel_bills(
        comb_before, annual_before, oil_price, propane_price
    )
    after_topped = add_delivered_fuel_bills(
        comb_after, annual_after, oil_price, propane_price
    )

    before_annual = cast(
        pl.DataFrame,
        before_topped.filter(pl.col("month") == ANNUAL_MONTH)
        .select(
            pl.col(BLDG_ID), pl.col("weight"), pl.col(BILL_LEVEL).alias("bill_before")
        )
        .collect(),
    )
    after_annual = cast(
        pl.DataFrame,
        after_topped.filter(pl.col("month") == ANNUAL_MONTH)
        .select(pl.col(BLDG_ID), pl.col(BILL_LEVEL).alias("bill_after"))
        .collect(),
    )

    delta_df = before_annual.join(after_annual, on=BLDG_ID, how="inner").with_columns(
        (pl.col("bill_after") - pl.col("bill_before")).alias("delta")
    )

    meta_cols = metadata.select([pl.col(c) for c in META_COLS])
    with_meta = delta_df.join(meta_cols, on=BLDG_ID, how="inner")
    filtered = _apply_population_filter(with_meta, filter_name)

    df = filtered.select(pl.col("delta"), pl.col("weight"))
    assert not df.is_empty(), (
        f"No rows after applying filter '{filter_name}'. Check metadata and run outputs."
    )
    return df


def _plot_bill_change_histogram(df: pl.DataFrame) -> ggplot:
    """Build weighted histogram of annual bill change from a delta+weight DataFrame."""
    binned = (
        df.with_columns(
            ((pl.col("delta") / BIN_WIDTH).floor() * BIN_WIDTH + BIN_WIDTH / 2).alias(
                "bin_center"
            ),
        )
        .with_columns(
            pl.when(pl.col("bin_center") <= -1000)
            .then(pl.lit("savings > $1k"))
            .when((pl.col("bin_center") > -1000) & (pl.col("bin_center") < 0))
            .then(pl.lit("savings $0-1k"))
            .when((pl.col("bin_center") >= 0) & (pl.col("bin_center") < 1000))
            .then(pl.lit("losses $0-1k"))
            .otherwise(pl.lit("losses > $1k"))
            .alias("quadrant"),
        )
        .group_by("bin_center", "quadrant")
        .agg(pl.col("weight").sum().alias("weight_sum"))
    )

    total_weight = df["weight"].sum()
    pct_savings_gt1k = (
        df.filter(pl.col("delta") < -1000)["weight"].sum() / total_weight * 100
    )
    pct_savings_0_1k = (
        df.filter((pl.col("delta") >= -1000) & (pl.col("delta") < 0))["weight"].sum()
        / total_weight
        * 100
    )
    pct_losses_0_1k = (
        df.filter((pl.col("delta") >= 0) & (pl.col("delta") < 1000))["weight"].sum()
        / total_weight
        * 100
    )
    pct_losses_gt1k = (
        df.filter(pl.col("delta") >= 1000)["weight"].sum() / total_weight * 100
    )

    quadrant_dtype = pl.Enum(QUADRANT_ORDER)
    plot_df = binned.with_columns(pl.col("quadrant").cast(quadrant_dtype))
    max_weight: float = (
        cast(float, plot_df["weight_sum"].max()) if not plot_df.is_empty() else 0.0
    )
    y_annot = max_weight * 1.05

    return (
        ggplot(plot_df, aes(x="bin_center", y="weight_sum", fill="quadrant"))
        + geom_col(width=BIN_WIDTH * 0.9)
        + geom_vline(xintercept=[-1000, 0, 1000], linetype="dotted", color="gray")
        + scale_fill_manual(
            values=QUADRANT_COLORS,
            breaks=QUADRANT_ORDER,
        )
        + annotate(
            "text",
            x=-2500,
            y=y_annot,
            label=f"savings > $1k\n{pct_savings_gt1k:.1f}%",
            ha="center",
            size=9,
        )
        + annotate(
            "text",
            x=-500,
            y=y_annot,
            label=f"savings $0-1k\n{pct_savings_0_1k:.1f}%",
            ha="center",
            size=9,
        )
        + annotate(
            "text",
            x=500,
            y=y_annot,
            label=f"losses $0-1k\n{pct_losses_0_1k:.1f}%",
            ha="center",
            size=9,
        )
        + annotate(
            "text",
            x=2500,
            y=y_annot,
            label=f"losses > $1k\n{pct_losses_gt1k:.1f}%",
            ha="center",
            size=9,
        )
        + labs(
            x="Annual Bill Change ($)",
            y="# of Homes",
            title="Change in Total Energy Bills after Switching to Heat Pumps",
        )
        + theme_minimal()
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot weighted histogram of annual bill change (before vs after HP).",
    )
    parser.add_argument(
        "--run-dir-before",
        required=True,
        help="S3 or local path to run 2 output (up00, delivery+supply).",
    )
    parser.add_argument(
        "--run-dir-after",
        required=True,
        help="S3 or local path to run 4 output (up02, delivery+supply).",
    )
    parser.add_argument(
        "--path-metadata",
        required=True,
        help="S3 or local path to metadata-sb.parquet for upgrade 00.",
    )
    parser.add_argument(
        "--path-annual-results-before",
        required=True,
        help="S3 or local path to load_curve_annual upgrade 00 parquet.",
    )
    parser.add_argument(
        "--path-annual-results-after",
        required=True,
        help="S3 or local path to load_curve_annual upgrade 02 parquet.",
    )
    parser.add_argument(
        "--state",
        default="RI",
        help="State code for EIA heating fuel price lookup (default: RI).",
    )
    parser.add_argument(
        "--price-year",
        type=int,
        default=2024,
        help="Year for EIA heating fuel price lookup (default: 2024).",
    )
    parser.add_argument(
        "--filter",
        choices=FILTER_OPTIONS,
        required=True,
        help="Population filter: non_hp, natgas, oil_propane, electric_resistance.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to save chart PNG.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    storage = storage_opts() if is_s3(path_or_s3(args.run_dir_before)) else None

    def _load(path: str, fmt: Literal["csv", "parquet"] = "csv") -> pl.LazyFrame:
        return load_lazy(str(path_or_s3(path)), storage, fmt)

    oil_price = load_avg_annual_fuel_price(
        "heating_oil", args.state, args.price_year, "residential", storage
    )
    propane_price = load_avg_annual_fuel_price(
        "propane", args.state, args.price_year, "residential", storage
    )

    metadata = cast(
        pl.DataFrame,
        _load(args.path_metadata, "parquet").collect(),
    )

    df = _compute_bill_deltas(
        comb_before=_load(str(path_or_s3(args.run_dir_before) / COMB_BILLS_CSV)),
        comb_after=_load(str(path_or_s3(args.run_dir_after) / COMB_BILLS_CSV)),
        annual_before=_load(args.path_annual_results_before, "parquet"),
        annual_after=_load(args.path_annual_results_after, "parquet"),
        metadata=metadata,
        oil_price=oil_price,
        propane_price=propane_price,
        filter_name=args.filter,
    )

    p = _plot_bill_change_histogram(df)

    if args.output:
        p.save(args.output, dpi=150)
        print(f"Saved chart to {args.output}")
    else:
        p.draw()


if __name__ == "__main__":
    main()

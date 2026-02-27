"""Plot stacked bar chart of median electric bill components (supply / delivery volumetric / delivery fixed).

Reads CAIRO bill outputs from two run pairs (current HVAC + HP), joins to metadata-sb.parquet
to restrict to fossil-fuel-heated homes, computes bill decomposition from delivery-only vs
delivery+supply runs, finds the weighted-median customer per scenario, and renders a
two-bar stacked chart with plotnine.

Electric-only; delivered fuels (gas/propane) top-up is out of scope.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

import polars as pl
from plotnine import (
    aes,
    geom_col,
    geom_text,
    ggplot,
    labs,
    position_stack,
    scale_fill_manual,
    scale_x_discrete,
    theme_minimal,
)

from utils.post.io import (
    ANNUAL_MONTH,
    BLDG_ID,
    BILL_LEVEL,
    scan,
)

HEATING_TYPE_COL = "postprocess_group.heating_type"
FOSSIL_FUEL = "fossil_fuel"
BILLS_CSV = "/bills/elec_bills_year_run.csv"


def _assert_join_preserves_keys(
    left: pl.DataFrame,
    right: pl.DataFrame,
    joined: pl.DataFrame,
    key_cols: list[str],
) -> None:
    """Raise if left/right have different key counts or inner join dropped any rows."""
    n_left = left.select([pl.col(c) for c in key_cols]).unique().height
    n_right = right.select([pl.col(c) for c in key_cols]).unique().height
    n_joined = joined.height
    if n_left != n_right:
        raise ValueError(
            f"Key mismatch before join: left has {n_left} unique {key_cols} combos, "
            f"right has {n_right}. Both sides must have the same keys."
        )
    if n_joined != n_left:
        raise ValueError(
            f"Inner join dropped rows: left had {n_left} {key_cols} combos, "
            f"join result has {n_joined}. {n_left - n_joined} rows were lost."
        )


def _assert_weights_match(
    joined: pl.DataFrame,
    left_weight: str = "weight",
    right_weight: str = "weight_supply",
    tolerance: float = 1e-9,
) -> None:
    """Raise if the two weight columns in joined differ beyond tolerance."""
    diff = (joined[left_weight] - joined[right_weight]).abs()
    n_differ = (diff > tolerance).sum()
    if n_differ > 0:
        max_diff = diff.max()
        raise ValueError(
            f"Weight columns '{left_weight}' and '{right_weight}' differ: "
            f"{n_differ} rows have |diff| > {tolerance}, max |diff| = {max_diff}."
        )


def _read_fixed_charge_from_tariff(path_tariff_json: str) -> float:
    """Read fixedchargefirstmeter ($/month) from delivery-only tariff JSON."""
    path = Path(path_tariff_json)
    if not path.exists():
        raise FileNotFoundError(f"Tariff JSON not found: {path}")
    with path.open() as f:
        data = json.load(f)
    items = data.get("items") or []
    if not items:
        raise ValueError(f"Tariff JSON has no 'items': {path_tariff_json}")
    first = items[0]
    if "fixedchargefirstmeter" not in first:
        raise ValueError(
            f"Tariff JSON items[0] has no 'fixedchargefirstmeter': {path_tariff_json}"
        )
    return float(first["fixedchargefirstmeter"])


def _join_bills_and_compute_components(
    delivery_bills: pl.LazyFrame,
    supply_bills: pl.LazyFrame,
    annual_fixed_delivery: float,
) -> pl.DataFrame:
    """Collect delivery + supply bills, join eagerly, validate, and compute components.

    Single collect per input LazyFrame (2 total). All validation runs on materialized data.
    """
    delivery_df = cast(pl.DataFrame, delivery_bills.collect())
    supply_df = cast(
        pl.DataFrame,
        supply_bills.select(
            pl.col(BLDG_ID),
            pl.col("month"),
            pl.col(BILL_LEVEL).alias("bill_supply"),
            pl.col("weight").alias("weight_supply"),
        ).collect(),
    )
    join_keys = [BLDG_ID, "month"]
    joined = delivery_df.join(supply_df, on=join_keys, how="inner")

    _assert_join_preserves_keys(delivery_df, supply_df, joined, join_keys)
    _assert_weights_match(joined, left_weight="weight", right_weight="weight_supply")

    return joined.with_columns(
        (pl.col("bill_supply") - pl.col(BILL_LEVEL)).alias("supply"),
        pl.lit(annual_fixed_delivery).alias("delivery_fixed"),
        (pl.col(BILL_LEVEL) - annual_fixed_delivery).alias("delivery_volumetric"),
        pl.col("bill_supply").alias("total"),
    )


def _weighted_median_row(
    df: pl.DataFrame,
    sort_col: str = "total",
    weight_col: str = "weight",
) -> pl.DataFrame:
    """Return single row at weighted median by sort_col."""
    total_weight = df[weight_col].sum()
    sorted_df = df.sort(sort_col)
    cum = sorted_df[weight_col].cum_sum()
    median_idx = (cum >= 0.5 * total_weight).arg_max()
    assert median_idx is not None, (
        "No row reaches 50% of total weight — data is empty or weights are non-positive/NaN/null."
    )
    return sorted_df.slice(median_idx, 1)


def _median_customer_components(
    delivery_bills: pl.LazyFrame,
    supply_bills: pl.LazyFrame,
    metadata: pl.DataFrame,
    annual_fixed_delivery: float,
) -> pl.DataFrame:
    """Collect bills once, join to metadata, filter to fossil-fuel, return weighted-median row.

    Collects each bill LazyFrame once (inside _join_bills_and_compute_components).
    Metadata is already materialized; no additional reads.
    """
    components = _join_bills_and_compute_components(
        delivery_bills, supply_bills, annual_fixed_delivery
    )
    annual = components.filter(pl.col("month") == ANNUAL_MONTH)

    meta_cols = metadata.select(pl.col(BLDG_ID), pl.col(HEATING_TYPE_COL))
    with_meta = annual.join(meta_cols, on=BLDG_ID, how="inner")

    fossil = with_meta.filter(pl.col(HEATING_TYPE_COL) == FOSSIL_FUEL).select(
        pl.col(BLDG_ID),
        pl.col("weight"),
        pl.col("supply"),
        pl.col("delivery_volumetric"),
        pl.col("delivery_fixed"),
        pl.col("total"),
    )
    assert not fossil.is_empty(), (
        "No fossil-fuel-heated buildings in metadata after join."
    )
    return _weighted_median_row(fossil, sort_col="total", weight_col="weight")


COMPONENT_NAMES: dict[str, str] = {
    "supply": "Supply Charge",
    "delivery_fixed": "Delivery Charge (Fixed)",
    "delivery_volumetric": "Delivery Charge (Volumetric)",
}


def _unpivot_median_row(median_df: pl.DataFrame, scenario: str) -> pl.DataFrame:
    """Unpivot a single-row median DataFrame into long-form (scenario, component, value)."""
    cols = list(COMPONENT_NAMES.keys())
    return (
        median_df.select(cols)
        .unpivot(on=cols, variable_name="col", value_name="value")
        .with_columns(
            pl.col("col").replace_strict(COMPONENT_NAMES).alias("component"),
            pl.lit(scenario).alias("scenario"),
        )
        .drop("col")
    )


def _plot_bill_components_stacked(
    median_current: pl.DataFrame,
    median_hp: pl.DataFrame,
) -> ggplot:
    """Build stacked bar chart of median bill components (current HVAC vs HP)."""
    scenario_order = ["Current HVAC", "Air-source heat pump\n(mid-efficiency)"]
    component_order = [
        "Delivery Charge (Fixed)",
        "Delivery Charge (Volumetric)",
        "Supply Charge",
    ]

    long = pl.concat(
        [
            _unpivot_median_row(median_current, scenario_order[0]),
            _unpivot_median_row(median_hp, scenario_order[1]),
        ]
    )

    for scenario in scenario_order:
        n = long.filter(pl.col("scenario") == scenario).height
        assert n == len(COMPONENT_NAMES), (
            f"Expected {len(COMPONENT_NAMES)} component rows for scenario {scenario!r}; got {n}. "
            "Check for empty data or invalid weights (e.g. negative)."
        )

    scenario_dtype = pl.Enum(scenario_order)
    component_dtype = pl.Enum(component_order)

    plot_df = long.with_columns(
        pl.col("scenario").cast(scenario_dtype),
        pl.col("component").cast(component_dtype),
        pl.col("value")
        .map_elements(lambda v: f"${v:,.0f}", return_dtype=pl.String)
        .alias("label"),
    )

    totals = (
        plot_df.group_by("scenario")
        .agg(pl.col("value").sum())
        .with_columns(
            pl.col("value")
            .map_elements(lambda v: f"${v:,.0f}", return_dtype=pl.String)
            .alias("label"),
        )
    )

    return (
        ggplot(plot_df, aes(x="scenario", y="value", fill="component"))
        + geom_col(position=position_stack(reverse=True), width=0.6)
        + geom_text(
            aes(label="label"),
            position=position_stack(vjust=0.5, reverse=True),
            color="white",
            fontweight="bold",
            size=11,
        )
        + geom_text(
            aes(x="scenario", y="value", label="label"),
            data=totals,
            va="bottom",
            color="#333333",
            fontweight="bold",
            size=11,
            nudge_y=30,
            inherit_aes=False,
        )
        + scale_fill_manual(
            values={
                "Supply Charge": "#E69F00",
                "Delivery Charge (Fixed)": "#01796F",
                "Delivery Charge (Volumetric)": "#56B4E9",
            },
        )
        + scale_x_discrete(limits=scenario_order)
        + labs(
            x="",
            y="Annual electric bill ($)",
            title="Median electric bill components: current HVAC vs mid-efficiency heat pump",
        )
        + theme_minimal()
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot stacked bar chart of median electric bill components (current HVAC vs HP).",
    )
    parser.add_argument(
        "--run-dir-delivery",
        required=True,
        help="S3 or local path to run output (delivery-only, current HVAC e.g. run 1).",
    )
    parser.add_argument(
        "--run-dir-supply",
        required=True,
        help="S3 or local path to run output (delivery+supply, current HVAC e.g. run 2).",
    )
    parser.add_argument(
        "--run-dir-delivery-hp",
        required=True,
        help="S3 or local path to run output (delivery-only, HP e.g. run 3).",
    )
    parser.add_argument(
        "--run-dir-supply-hp",
        required=True,
        help="S3 or local path to run output (delivery+supply, HP e.g. run 4).",
    )
    parser.add_argument(
        "--path-tariff-json",
        required=True,
        help="Path to delivery-only tariff JSON (e.g. rie_flat.json); fixedchargefirstmeter read from it.",
    )
    parser.add_argument(
        "--path-metadata",
        required=True,
        help="S3 or local path to metadata-sb.parquet for upgrade 0 (used to filter fossil-fuel-heated homes).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to save chart PNG. If omitted, chart is shown interactively.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    annual_fixed = _read_fixed_charge_from_tariff(args.path_tariff_json) * 12

    metadata = cast(
        pl.DataFrame,
        scan(args.path_metadata, "parquet").collect(),
    )

    median_current = _median_customer_components(
        scan(args.run_dir_delivery + BILLS_CSV),
        scan(args.run_dir_supply + BILLS_CSV),
        metadata,
        annual_fixed,
    )
    median_hp = _median_customer_components(
        scan(args.run_dir_delivery_hp + BILLS_CSV),
        scan(args.run_dir_supply_hp + BILLS_CSV),
        metadata,
        annual_fixed,
    )

    p = _plot_bill_components_stacked(median_current, median_hp)

    if args.output:
        p.save(args.output, dpi=150)
        print(f"Saved chart to {args.output}")
    else:
        p.draw()


if __name__ == "__main__":
    main()

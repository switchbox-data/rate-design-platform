"""Allocate an annual $/kW-yr onto the top N hours by load share.

Weight each of the top ``n_hours`` by its load divided by the sum of load in
those hours. Every other hour is zero.

    weight_h = load_h / sum(load of top N)
    cost_h   = ($/kW-yr) * weight_h

This is the distribution and sub-transmission probability-of-peak path
(``n_hours`` 100) and the PJM bulk-transmission PCAF path (``n_hours`` 150).
The formula is the same. The caller chooses the load curve, the ``$/kW-yr``,
N, and the output column name.

The load is already the output clock: one row per hour, ``timestamp`` and
``load_mw``, timestamps already on the output year. This module does not know
the state, the utility, the ISO, where the file came from, or the manifest.
See context/methods/marginal_costs/top_n_load_share_allocation.md.

Usage:
    uv run python utils/data_prep/marginal_costs/methods/top_n_load_share.py \\
        --path-load load.parquet \\
        --annual-cost-kw-year 32.0 \\
        --n-hours 100 \\
        --path-output allocated.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

_LOAD_COLUMNS = ("timestamp", "load_mw")


def top_n_load_share(
    load: pl.DataFrame,
    annual_cost_kw_year: float,
    n_hours: int,
    *,
    output_column: str = "mc_total_per_kwh",
) -> pl.DataFrame:
    """Spread ``annual_cost_kw_year`` across the top ``n_hours`` by load share.

    Args:
        load: One row per hour. Required columns are ``timestamp`` and ``load_mw``.
            Extra columns are ignored. Production callers pass the CAIRO 8760.
        annual_cost_kw_year: The ``$/kW-yr`` the hourly costs must sum to.
        n_hours: How many top hours receive a nonzero cost.
        output_column: Name of the cost column. Distribution uses
            ``mc_total_per_kwh``. PJM bulk transmission uses ``bulk_tx_cost_enduse``.

    Returns:
        ``timestamp`` and ``output_column``, one row per input hour, sorted by
        timestamp. Hours outside the top ``n_hours`` are 0. The cost column
        sums to ``annual_cost_kw_year``.

    Raises:
        ValueError: ``n_hours`` is below 1, the load is shorter than ``n_hours``,
            a required column is missing, or the top hours' load sums to zero
            or less.
    """
    if n_hours < 1:
        raise ValueError(f"n_hours must be at least 1, got {n_hours}")
    if output_column in _LOAD_COLUMNS:
        raise ValueError(f"output_column {output_column!r} collides with a load column")
    missing = set(_LOAD_COLUMNS) - set(load.columns)
    if missing:
        raise ValueError(f"Load is missing columns: {sorted(missing)}")
    if load.height < n_hours:
        raise ValueError(f"Load has {load.height} hours; top-N load-share needs at least {n_hours}.")

    ranked = (
        load.select("timestamp", "load_mw").with_row_index("_row").sort("load_mw", descending=True, nulls_last=True)
    )
    top = ranked.head(n_hours)
    total_peak_load = top["load_mw"].sum()
    if total_peak_load is None or float(total_peak_load) <= 0:
        raise ValueError(f"Total load across the top {n_hours} hours is zero or negative.")

    weights = top.select(
        "_row",
        (pl.col("load_mw") / float(total_peak_load) * annual_cost_kw_year).alias(output_column),
    )
    return (
        load.select("timestamp")
        .with_row_index("_row")
        .join(weights, on="_row", how="left")
        .with_columns(pl.col(output_column).fill_null(0.0).cast(pl.Float64))
        .sort("timestamp")
        .select("timestamp", output_column)
    )


def main(argv: list[str] | None = None) -> None:
    """Read one load parquet, allocate, and write the hourly cost parquet."""
    parser = argparse.ArgumentParser(
        description=(
            "Allocate an annual $/kW-yr across the top N hours of a load curve, "
            "weighted by each hour's share of load in those hours."
        )
    )
    parser.add_argument(
        "--path-load",
        type=Path,
        required=True,
        help="Parquet with timestamp and load_mw. One row per hour of the output clock.",
    )
    parser.add_argument(
        "--annual-cost-kw-year",
        type=float,
        required=True,
        help="Annual cost to recover, in $/kW-yr. The output cost column sums to this.",
    )
    parser.add_argument(
        "--n-hours",
        type=int,
        required=True,
        help="How many top hours receive a nonzero cost. Distribution uses 100; PJM bulk tx uses 150.",
    )
    parser.add_argument(
        "--output-column",
        type=str,
        default="mc_total_per_kwh",
        help="Cost column name (default: mc_total_per_kwh). PJM bulk tx uses bulk_tx_cost_enduse.",
    )
    parser.add_argument(
        "--path-output",
        type=Path,
        required=True,
        help="Parquet to write. Columns: timestamp and the cost column.",
    )
    args = parser.parse_args(argv)

    allocated = top_n_load_share(
        pl.read_parquet(args.path_load),
        args.annual_cost_kw_year,
        args.n_hours,
        output_column=args.output_column,
    )
    args.path_output.parent.mkdir(parents=True, exist_ok=True)
    allocated.write_parquet(args.path_output)
    total = float(allocated[args.output_column].sum())
    nonzero = allocated.filter(pl.col(args.output_column) > 0).height
    print(f"Wrote {allocated.height} rows to {args.path_output}")
    print(f"Non-zero hours: {nonzero}")
    print(f"Sum of {args.output_column}: {total:.6f}")


if __name__ == "__main__":
    main()

"""Allocate an annual $/kW-yr onto the top N hours by exceedance.

Take the top ``n_hours``. The threshold is the highest load strictly below the
Nth-highest hour. Weight each of those hours by how far it sits above the
threshold. Every other hour is zero.

    exceedance_h = load_h - threshold
    weight_h     = exceedance_h / sum(exceedance)
    cost_h       = ($/kW-yr) * weight_h

A tie at the cutoff keeps ``n_hours`` eligible rows. An eligible hour whose
load equals the threshold gets weight zero, so fewer than ``n_hours`` hours
can carry a positive cost.

RI and CT bulk transmission pass ``n_hours`` 100 and write
``bulk_tx_cost_enduse``. ISO-NE supply capacity (FCA) passes the same N and
writes ``capacity_cost_per_kw``. The formula is the same. The caller chooses
the load curve, the ``$/kW-yr``, N, and the output column name.

The load is already the output clock: one row per hour, ``timestamp`` and
``load_mw``, timestamps already on the output year. This module does not know
the state, the utility, the ISO, where the file came from, or the manifest.
See context/methods/marginal_costs/top_n_exceedance_allocation.md.

Usage:
    uv run python utils/data_prep/marginal_costs/methods/top_n_exceedance.py \\
        --path-load load.parquet \\
        --annual-cost-kw-year 69.0 \\
        --n-hours 100 \\
        --path-output allocated.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from utils.numeric import as_float

_LOAD_COLUMNS = ("timestamp", "load_mw")


def top_n_exceedance(
    load: pl.DataFrame,
    annual_cost_kw_year: float,
    n_hours: int,
    *,
    output_column: str = "mc_total_per_kwh",
) -> pl.DataFrame:
    """Spread ``annual_cost_kw_year`` across the top ``n_hours`` by exceedance.

    Args:
        load: One row per hour. Required columns are ``timestamp`` and ``load_mw``.
            Extra columns are ignored. Production callers pass the CAIRO 8760.
        annual_cost_kw_year: The ``$/kW-yr`` the hourly costs must sum to.
        n_hours: How many top hours are eligible. A tie at the cutoff can leave
            fewer hours with a positive cost.
        output_column: Name of the cost column. Bulk transmission uses
            ``bulk_tx_cost_enduse``. ISO-NE supply capacity uses
            ``capacity_cost_per_kw``.

    Returns:
        ``timestamp`` and ``output_column``, one row per input hour, sorted by
        timestamp. Hours outside the eligible set are 0. The cost column sums
        to ``annual_cost_kw_year``.

    Raises:
        ValueError: ``n_hours`` is below 1, the load is shorter than ``n_hours``,
            a required column is missing, the Nth-highest load is null, or the
            eligible hours' exceedance sums to zero or less.
    """
    if n_hours < 1:
        raise ValueError(f"n_hours must be at least 1, got {n_hours}")
    if output_column in _LOAD_COLUMNS:
        raise ValueError(f"output_column {output_column!r} collides with a load column")
    missing = set(_LOAD_COLUMNS) - set(load.columns)
    if missing:
        raise ValueError(f"Load is missing columns: {sorted(missing)}")
    if load.height < n_hours:
        raise ValueError(f"Load has {load.height} hours; top-N exceedance needs at least {n_hours}.")

    ranked = (
        load.select("timestamp", "load_mw").with_row_index("_row").sort("load_mw", descending=True, nulls_last=True)
    )
    top = ranked.head(n_hours)
    raw_nth = top["load_mw"][-1]
    if raw_nth is None:
        raise ValueError(f"The {n_hours}th-highest load is null.")
    load_nth = as_float(raw_nth)

    # Threshold = highest load strictly below the Nth-highest (tie-safe).
    # When nothing is strictly below, the threshold is 0, and eligible hours
    # sitting on 0 get weight zero.
    below = load.filter(pl.col("load_mw") < load_nth)["load_mw"]
    threshold = as_float(below.max()) if below.len() > 0 else 0.0

    exceedance = pl.col("load_mw") - threshold
    total_exceedance = as_float(top.select(exceedance.sum()).item())
    if total_exceedance <= 0:
        peak = ranked["load_mw"][0]
        peak_mw = as_float(peak) if peak is not None else float("nan")
        raise ValueError(
            f"Total exceedance is zero or negative. Threshold={threshold:.2f} MW, max load={peak_mw:.2f} MW"
        )

    weights = top.select(
        "_row",
        (exceedance / total_exceedance * annual_cost_kw_year).alias(output_column),
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
            "weighted by each hour's load above the threshold."
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
        help="How many top hours are eligible for a nonzero cost. RI and CT bulk tx use 100.",
    )
    parser.add_argument(
        "--output-column",
        type=str,
        default="mc_total_per_kwh",
        help="Cost column name (default: mc_total_per_kwh). Bulk tx uses bulk_tx_cost_enduse.",
    )
    parser.add_argument(
        "--path-output",
        type=Path,
        required=True,
        help="Parquet to write. Columns: timestamp and the cost column.",
    )
    args = parser.parse_args(argv)

    allocated = top_n_exceedance(
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

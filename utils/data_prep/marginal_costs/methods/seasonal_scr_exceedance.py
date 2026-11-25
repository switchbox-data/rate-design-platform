"""Allocate an annual $/kW-yr onto seasonal SCR hours by exceedance.

Split the year into summer and winter. Mark the top ``n_hours_per_season``
hours of each season. Give each season a share of the annual cost from how
far that season's peak sits above the lower of the two seasonal thresholds.
Inside a season, spread that season's share by exceedance above the season's
own threshold (the (N+1)th-highest hour in the season).

    phi_season = (peak_season - min(threshold_summer, threshold_winter))
                 / sum of those two surpluses
    exceedance_h = max(load_h - threshold_season, 0)   on SCR hours, else 0
    weight_h = (exceedance_h / sum of exceedance in the season) * phi_season
    cost_h   = ($/kW-yr) * weight_h

Winter is the months the caller passes. Summer is every other month. NY bulk
transmission passes 40 hours per season and winter months October–March
(``[10, 11, 12, 1, 2, 3]``). The formula is the same for any other caller.
The caller chooses the load curve, the ``$/kW-yr``, N, the winter months,
and the output column name.

A tie at a season's cutoff keeps ``n_hours_per_season`` eligible rows in that
season. An eligible hour whose load equals the threshold gets weight zero, so
a season can carry a positive cost on fewer than N hours.

The load is already the output clock: one row per hour, ``timestamp`` and
``load_mw``, timestamps already on the output year. This module does not know
the state, the utility, the ISO, where the file came from, or the manifest.
See context/methods/marginal_costs/seasonal_scr_exceedance_allocation.md.

Usage:
    uv run python utils/data_prep/marginal_costs/methods/seasonal_scr_exceedance.py \\
        --path-load load.parquet \\
        --annual-cost-kw-year 50.0 \\
        --n-hours-per-season 40 \\
        --winter-months 10,11,12,1,2,3 \\
        --path-output allocated.parquet
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

import polars as pl

from utils.numeric import as_float

_LOAD_COLUMNS = ("timestamp", "load_mw")
_ALL_MONTHS = frozenset(range(1, 13))


def _winter_month_set(winter_months: Sequence[int]) -> frozenset[int]:
    """Validate winter months and return them as a set.

    Summer is the complement. The set is what the season label uses, so
    duplicate months in the input do not change the split.
    """
    months = {int(month) for month in winter_months}
    if not months:
        raise ValueError("winter_months must not be empty")
    if any(month < 1 or month > 12 for month in months):
        raise ValueError(f"winter_months must contain only integers in 1..12, got {sorted(months)}")
    if months == _ALL_MONTHS:
        raise ValueError("winter_months must not include all 12 months")
    return frozenset(months)


def _season_window(
    labeled: pl.DataFrame,
    season: str,
    n_hours: int,
) -> tuple[pl.DataFrame, float, float]:
    """Return one season's SCR rows, its peak, and its (N+1)th-highest load.

    The threshold is the minimum of the top ``n_hours + 1`` loads. When the
    season has exactly ``n_hours`` rows, that minimum is the lowest SCR hour.
    """
    ranked = labeled.filter(pl.col("season") == season).sort("load_mw", descending=True, nulls_last=True)
    if ranked.height < n_hours:
        raise ValueError(
            f"{season.capitalize()} has {ranked.height} hours; seasonal SCR exceedance needs at least {n_hours}."
        )
    scr = ranked.head(n_hours)
    if scr["load_mw"].null_count() > 0:
        raise ValueError(f"The {season} SCR hours include a null load.")
    raw_peak = scr["load_mw"][0]
    raw_threshold = ranked.head(n_hours + 1)["load_mw"].min()
    if raw_peak is None or raw_threshold is None:
        raise ValueError(f"The {season} peak or threshold load is null.")
    return scr, as_float(raw_peak), as_float(raw_threshold)


def _season_weights(
    scr: pl.DataFrame,
    threshold: float,
    phi: float,
    season: str,
) -> pl.DataFrame:
    """Weight one season's SCR rows by exceedance above ``threshold``."""
    exceeded = scr.with_columns((pl.col("load_mw") - threshold).clip(lower_bound=0.0).alias("exceedance"))
    total = as_float(exceeded.select(pl.col("exceedance").sum()).item())
    if total <= 0:
        raise ValueError(f"Zero {season} exceedance; cannot allocate SCR weights")
    return exceeded.select("_row", (pl.col("exceedance") / total * phi).alias("_weight"))


def seasonal_scr_exceedance(
    load: pl.DataFrame,
    annual_cost_kw_year: float,
    n_hours_per_season: int,
    winter_months: Sequence[int],
    *,
    output_column: str = "mc_total_per_kwh",
) -> pl.DataFrame:
    """Spread ``annual_cost_kw_year`` across summer and winter SCR hours.

    Args:
        load: One row per hour. Required columns are ``timestamp`` and ``load_mw``.
            Extra columns are ignored. Production callers pass the CAIRO 8760.
        annual_cost_kw_year: The ``$/kW-yr`` the hourly costs must sum to.
        n_hours_per_season: How many top hours are eligible in summer, and again
            in winter. NY bulk transmission uses 40.
        winter_months: Months in the winter season, numbered 1–12. Summer is
            every other month. NY bulk transmission uses October–March.
        output_column: Name of the cost column. NY bulk transmission uses
            ``bulk_tx_cost_enduse``.

    Returns:
        ``timestamp`` and ``output_column``, one row per input hour, sorted by
        timestamp. Hours outside the SCR set are 0. The cost column sums to
        ``annual_cost_kw_year``.

    Raises:
        ValueError: ``n_hours_per_season`` is below 1, a required column is
            missing, ``winter_months`` is empty, outside 1–12, or all 12
            months, a season has fewer than ``n_hours_per_season`` hours, a
            season's peak or threshold is null, the seasonal surplus is zero
            or negative, or a season's exceedance sums to zero or less.
    """
    if n_hours_per_season < 1:
        raise ValueError(f"n_hours_per_season must be at least 1, got {n_hours_per_season}")
    if output_column in _LOAD_COLUMNS:
        raise ValueError(f"output_column {output_column!r} collides with a load column")
    missing = set(_LOAD_COLUMNS) - set(load.columns)
    if missing:
        raise ValueError(f"Load is missing columns: {sorted(missing)}")

    winter = _winter_month_set(winter_months)
    labeled = (
        load.select("timestamp", "load_mw")
        .with_row_index("_row")
        .with_columns(
            pl.when(pl.col("timestamp").dt.month().is_in(sorted(winter)))
            .then(pl.lit("winter"))
            .otherwise(pl.lit("summer"))
            .alias("season")
        )
    )

    summer_scr, summer_peak, summer_threshold = _season_window(labeled, "summer", n_hours_per_season)
    winter_scr, winter_peak, winter_threshold = _season_window(labeled, "winter", n_hours_per_season)

    tau_min = min(summer_threshold, winter_threshold)
    summer_surplus = summer_peak - tau_min
    winter_surplus = winter_peak - tau_min
    total_surplus = summer_surplus + winter_surplus
    if total_surplus <= 0:
        raise ValueError("Invalid SCR surplus; cannot compute seasonal weights")
    phi_summer = summer_surplus / total_surplus
    phi_winter = 1.0 - phi_summer

    weights = pl.concat(
        [
            _season_weights(summer_scr, summer_threshold, phi_summer, "summer"),
            _season_weights(winter_scr, winter_threshold, phi_winter, "winter"),
        ]
    ).select("_row", (pl.col("_weight") * annual_cost_kw_year).alias(output_column))
    return (
        labeled.select("timestamp", "_row")
        .join(weights, on="_row", how="left")
        .with_columns(pl.col(output_column).fill_null(0.0).cast(pl.Float64))
        .sort("timestamp")
        .select("timestamp", output_column)
    )


def _parse_winter_months(text: str) -> list[int]:
    """Parse ``10,11,12,1,2,3`` into month numbers. Range checks happen in the formula."""
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        raise ValueError(f"winter months must name at least one month, got {text!r}")
    try:
        return [int(part) for part in parts]
    except ValueError as exc:
        raise ValueError(f"winter months must be comma-separated integers, got {text!r}") from exc


def main(argv: list[str] | None = None) -> None:
    """Read one load parquet, allocate, and write the hourly cost parquet."""
    parser = argparse.ArgumentParser(
        description=(
            "Allocate an annual $/kW-yr across the top N hours of summer and of winter, "
            "split by seasonal peak surplus and weighted by exceedance within each season."
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
        "--n-hours-per-season",
        type=int,
        required=True,
        help="How many top hours are eligible in summer and again in winter. NY bulk tx uses 40.",
    )
    parser.add_argument(
        "--winter-months",
        type=str,
        required=True,
        help=(
            "Comma-separated winter months, e.g. 10,11,12,1,2,3. "
            "Summer is every other month. NY bulk tx uses October-March."
        ),
    )
    parser.add_argument(
        "--output-column",
        type=str,
        default="mc_total_per_kwh",
        help="Cost column name (default: mc_total_per_kwh). NY bulk tx uses bulk_tx_cost_enduse.",
    )
    parser.add_argument(
        "--path-output",
        type=Path,
        required=True,
        help="Parquet to write. Columns: timestamp and the cost column.",
    )
    args = parser.parse_args(argv)

    allocated = seasonal_scr_exceedance(
        pl.read_parquet(args.path_load),
        args.annual_cost_kw_year,
        args.n_hours_per_season,
        _parse_winter_months(args.winter_months),
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

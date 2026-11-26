"""Allocate an annual $/kW-yr by a month x day-type x hour KDE probability of peak.

From one or more complete years of hourly load, estimate a probability of
peak for each month x day-type x hour cell, then place the annual cost on the
CAIRO 8760.

1. Label each hour by month, hour beginning, and day type. Weekend-and-holiday
   days are Saturday, Sunday and the eleven US federal holidays (5 U.S.C.
   § 6103), a Saturday holiday observed on the Friday before and a Sunday
   holiday on the Monday after. Juneteenth counts from 2021.
2. Divide each hour's load by the average hourly load of its calendar year.
   Everything after this is in those normalized units.
3. The *delimiter* is the mean over years of the highest month x day-type x
   hour mean of normalized load in that year. On Eversource's 2022-2025
   substation load it is 1.656 times the year's average load, about 0.834 of
   the annual peak.
4. In each month x day-type x hour cell, fit a Gaussian kernel density to the
   normalized loads (Scott bandwidth, the ``scipy.stats.gaussian_kde``
   default) with each kernel truncated at +/-3 bandwidths and renormalized.
5. The cell's exceedance share is the mass of that density above the
   delimiter, computed exactly as the mean over kernels of the truncated
   Gaussian tail (what a large number of simulated draws converges to). PoP is
   that share divided by its sum over all cells, so it sums to one. It is not
   multiplied by the number of days in the cell.

``$/kWh = ($/kW-yr) * PoP / num_days``, with ``num_days`` from the target
calendar, then expanded onto the 8760 with the weekday / weekend-and-holiday
lookup. A flat 1 kW load pays the annual cost exactly, so this path does not
apply an extra uniform scale.

This is the reading of Eversource's OCC-733 response (PURA Docket 26-05-10)
that matches the PoP table on OCC-863 Attachment 2 page 3. The caller chooses
the load history, the ``$/kW-yr``, the timestamp year, the calendar year, and
the output column name. This module does not know the state, the utility, the
ISO, where the file came from, or the manifest.
See context/methods/marginal_costs/probability_of_peak_kde.md.

Usage:
    uv run python utils/data_prep/marginal_costs/methods/pop_kde.py \\
        --path-load load.parquet \\
        --annual-cost-kw-year 69.0 \\
        --year 2025 \\
        --path-output allocated.parquet
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
from scipy.special import ndtr

from utils.data_prep.marginal_costs.methods.month_hour import (
    expand_month_hour_to_8760,
    us_federal_holidays,
)
from utils.data_prep.marginal_costs.supply_utils import build_cairo_8760_timestamps

WEEKDAY = "weekday"
WEEKEND_HOLIDAY = "weekend_holiday"
DAY_TYPES = (WEEKDAY, WEEKEND_HOLIDAY)

DEFAULT_TRUNCATION_SIGMAS = 3.0
# The fewest hourly rows a calendar year may have and still set its own
# average load. A day of DST or missing data is tolerated; a partial year is not.
MIN_HOURS_PER_YEAR = 8_736
_N_CELLS = 12 * 2 * 24


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def label_hour_types(frame: pl.DataFrame, *, timestamp_col: str = "timestamp") -> pl.DataFrame:
    """Add ``year``, ``month``, ``hour`` and ``daytype`` from a naive timestamp.

    ``daytype`` is ``"weekday"`` or ``"weekend_holiday"`` under the rule in
    :func:`us_federal_holidays`. Timestamps are hour beginning, local time.
    """
    ts = pl.col(timestamp_col)
    years = frame.select(ts.dt.year().unique())[timestamp_col].to_list()
    holidays = sorted(set().union(*(us_federal_holidays(int(y)) for y in years)))
    return frame.with_columns(
        ts.dt.year().alias("year"),
        ts.dt.month().alias("month"),
        ts.dt.hour().alias("hour"),
        # Polars weekday is ISO: Monday = 1, Sunday = 7.
        pl.when((ts.dt.weekday() >= 6) | ts.dt.date().is_in(holidays))
        .then(pl.lit(WEEKEND_HOLIDAY))
        .otherwise(pl.lit(WEEKDAY))
        .alias("daytype"),
    )


def count_day_types(calendar_year: int) -> pl.DataFrame:
    """Days of each month x day type on the CAIRO 8760 of ``calendar_year``.

    Counted on the 8,760-hour clock CAIRO uses, so in a leap year December 31
    is not counted (the 8760 drops it). Columns: ``month``, ``daytype``,
    ``num_days``.
    """
    clock = label_hour_types(build_cairo_8760_timestamps(calendar_year))
    return (
        clock.group_by("month", "daytype")
        .agg(pl.col("timestamp").dt.date().n_unique().alias("num_days"))
        .sort("month", "daytype")
    )


# ---------------------------------------------------------------------------
# Kernel density tail
# ---------------------------------------------------------------------------


def scott_bandwidth(x: np.ndarray) -> float:
    """Scott's rule for one dimension: sample standard deviation x n^(-1/5)."""
    if x.size < 2:
        return 0.0
    return float(x.std(ddof=1) * x.size ** (-1 / 5))


def truncated_kde_tail_share(
    x: np.ndarray,
    threshold: float,
    *,
    bandwidth: float | None = None,
    truncation_sigmas: float = DEFAULT_TRUNCATION_SIGMAS,
) -> float:
    """Mass above ``threshold`` of a truncated Gaussian KDE fit to ``x``.

    Each kernel is a Gaussian centred on one observation with the given
    bandwidth (Scott's rule by default), cut at ``+/- truncation_sigmas``
    bandwidths and renormalized. ``truncation_sigmas`` of ``inf`` is the
    untruncated KDE. The result is the exact expectation of the share of
    draws above the threshold, so it needs no random seed.

    With fewer than two observations, or a bandwidth of zero, the density is
    a point mass at the observations and the share is the fraction above the
    threshold.
    """
    x = np.asarray(x, dtype=float)
    if x.size == 0:
        raise ValueError("cannot fit a density to zero observations")
    if truncation_sigmas <= 0:
        raise ValueError("truncation_sigmas must be positive")
    bw = scott_bandwidth(x) if bandwidth is None else float(bandwidth)
    if bw <= 0.0:
        return float(np.mean(x > threshold))
    z = (threshold - x) / bw
    if np.isinf(truncation_sigmas):
        return float(np.mean(ndtr(-z)))
    k = float(truncation_sigmas)
    cdf_hi, cdf_lo = ndtr(k), ndtr(-k)
    above = cdf_hi - ndtr(np.clip(z, -k, k))
    return float(np.mean(above / (cdf_hi - cdf_lo)))


# ---------------------------------------------------------------------------
# Probability of peak
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ProbabilityOfPeak:
    """PoP by cell with the delimiter and per-year normalization behind it.

    ``cells`` has one row per month x daytype x hour (576 rows): ``month``,
    ``daytype``, ``hour``, ``n_obs`` (hours in the history), ``bandwidth``
    (normalized units), ``p_exceed`` (share of the cell's truncated KDE above
    the delimiter) and ``probability_of_peak`` (``p_exceed`` / its sum).

    ``years`` has one row per calendar year: ``year``, ``n_hours``,
    ``mean_load`` and ``peak_load`` in input units, ``max_cell_mean`` (the
    year's highest cell mean / its mean load, the quantity the delimiter
    averages) and ``delimiter_load`` (the delimiter in input units for that
    year).

    ``delimiter`` is in normalized units: multiples of a year's average load.
    """

    cells: pl.DataFrame
    delimiter: float
    years: pl.DataFrame

    @property
    def delimiter_share_of_peak(self) -> float:
        """Delimiter as a share of the mean over years of the normalized peak."""
        normalized_peak = self.years.select((pl.col("peak_load") / pl.col("mean_load")).mean()).item()
        return self.delimiter / float(normalized_peak)


def max_cell_mean_by_year(labelled: pl.DataFrame, *, load_col: str = "load_norm") -> pl.DataFrame:
    """Each year's highest month x daytype x hour mean of ``load_col``.

    ``labelled`` must carry ``year``, ``month``, ``daytype`` and ``hour``, with
    ``load_col`` already divided by its year's mean. The delimiter is the mean
    of ``max_cell_mean`` over years. Sorted by ``year``.
    """
    return (
        labelled.group_by("year", "month", "daytype", "hour")
        .agg(pl.col(load_col).mean().alias("cell_mean"))
        .group_by("year")
        .agg(pl.col("cell_mean").max().alias("max_cell_mean"))
        .sort("year")
    )


def probability_of_peak(
    load: pl.DataFrame,
    *,
    load_col: str = "load_mw",
    timestamp_col: str = "timestamp",
    truncation_sigmas: float = DEFAULT_TRUNCATION_SIGMAS,
    bandwidth_multiplier: float = 1.0,
) -> ProbabilityOfPeak:
    """Probability of peak by month x day type x hour from hourly history.

    ``load`` has one row per hour with a naive hour-beginning local
    ``timestamp_col`` and a non-null ``load_col``. Every calendar year it
    covers must be essentially complete (at least ``MIN_HOURS_PER_YEAR``
    rows), because each year is normalized by its own average load. Years
    are weighted equally through the number of hours they contribute to each
    cell.

    ``truncation_sigmas`` and ``bandwidth_multiplier`` exist for sensitivity
    runs; the method is the default (+/-3 bandwidths, Scott bandwidth).
    """
    if bandwidth_multiplier <= 0:
        raise ValueError("bandwidth_multiplier must be positive")
    missing = {load_col, timestamp_col} - set(load.columns)
    if missing:
        raise ValueError(f"load is missing columns {sorted(missing)}")
    if load[load_col].null_count() or load[timestamp_col].null_count():
        raise ValueError(f"{load_col!r} and {timestamp_col!r} must have no nulls")
    if not isinstance(load.schema[timestamp_col], pl.Datetime):
        raise TypeError(f"{timestamp_col!r} must be a Datetime column")

    labelled = label_hour_types(
        load.select(timestamp_col, pl.col(load_col).cast(pl.Float64)),
        timestamp_col=timestamp_col,
    )
    years = (
        labelled.group_by("year")
        .agg(
            pl.len().alias("n_hours"),
            pl.col(load_col).mean().alias("mean_load"),
            pl.col(load_col).max().alias("peak_load"),
        )
        .sort("year")
    )
    short = years.filter(pl.col("n_hours") < MIN_HOURS_PER_YEAR)
    if short.height:
        raise ValueError(
            f"every calendar year must be complete (at least {MIN_HOURS_PER_YEAR} hours); short years: {short.rows()}"
        )
    labelled = labelled.with_columns((pl.col(load_col) / pl.col(load_col).mean().over("year")).alias("load_norm"))
    max_cell_mean = max_cell_mean_by_year(labelled)
    delimiter = float(max_cell_mean.select(pl.col("max_cell_mean").mean()).item())
    years = years.join(max_cell_mean, on="year").with_columns(
        (pl.lit(delimiter) * pl.col("mean_load")).alias("delimiter_load")
    )

    groups = {
        (int(m), str(d), int(h)): g["load_norm"].to_numpy()
        for (m, d, h), g in labelled.group_by("month", "daytype", "hour")
    }
    records: list[dict[str, object]] = []
    for month in range(1, 13):
        for daytype in DAY_TYPES:
            for hour in range(24):
                x = groups.get((month, daytype, hour))
                if x is None or x.size < 2:
                    raise ValueError(
                        f"month {month} {daytype} hour {hour} has fewer than two "
                        "observations; the history must cover every cell"
                    )
                bw = scott_bandwidth(x) * bandwidth_multiplier
                records.append(
                    {
                        "month": month,
                        "daytype": daytype,
                        "hour": hour,
                        "n_obs": int(x.size),
                        "bandwidth": bw,
                        "p_exceed": truncated_kde_tail_share(
                            x,
                            delimiter,
                            bandwidth=bw,
                            truncation_sigmas=truncation_sigmas,
                        ),
                    }
                )
    cells = pl.DataFrame(records).cast(
        {
            "month": pl.Int64,
            "daytype": pl.String,
            "hour": pl.Int64,
            "n_obs": pl.Int64,
            "bandwidth": pl.Float64,
            "p_exceed": pl.Float64,
        }
    )
    total = float(cells["p_exceed"].sum())
    if total <= 0.0:
        raise ValueError("no cell has any density above the delimiter")
    cells = cells.with_columns((pl.col("p_exceed") / total).alias("probability_of_peak"))
    return ProbabilityOfPeak(cells=cells, delimiter=delimiter, years=years)


# ---------------------------------------------------------------------------
# Allocation of an annual $/kW-yr
# ---------------------------------------------------------------------------


def _check_pop_cells(cells: pl.DataFrame) -> pl.DataFrame:
    required = {"month", "daytype", "hour", "probability_of_peak"}
    missing = required - set(cells.columns)
    if missing:
        raise ValueError(f"PoP table is missing columns {sorted(missing)}")
    keyed = cells.select(
        pl.col("month").cast(pl.Int64),
        pl.col("daytype").cast(pl.String),
        pl.col("hour").cast(pl.Int64),
        pl.col("probability_of_peak").cast(pl.Float64),
    )
    if keyed.height != _N_CELLS or keyed.select("month", "daytype", "hour").n_unique() != _N_CELLS:
        raise ValueError(f"PoP table must have {_N_CELLS} unique month x daytype x hour rows")
    bad_daytype = set(keyed["daytype"].unique().to_list()) - set(DAY_TYPES)
    if bad_daytype:
        raise ValueError(f"unknown daytype values {sorted(bad_daytype)}")
    total = float(keyed["probability_of_peak"].sum())
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"probability_of_peak sums to {total}, expected 1")
    if keyed.select(pl.col("probability_of_peak").min()).item() < 0.0:
        raise ValueError("probability_of_peak has negative values")
    return keyed


def month_hour_costs_from_pop(
    cells: pl.DataFrame,
    annual_cost_kw_year: float,
    calendar_year: int,
    *,
    value_col: str = "mc_total_per_kwh",
) -> pl.DataFrame:
    """Spread one $/kW-yr over month x daytype x hour cells by PoP.

    Each cell gets ``annual_cost_kw_year x probability_of_peak / num_days``,
    with ``num_days`` the number of days of that month and day type on
    ``calendar_year``'s CAIRO 8760 (:func:`count_day_types`). Returns the
    cells with ``num_days`` and ``value_col`` added, sorted by month, daytype
    and hour.
    """
    keyed = _check_pop_cells(cells)
    days = count_day_types(calendar_year)
    out = keyed.join(days, on=["month", "daytype"], how="left")
    if out["num_days"].null_count():
        raise ValueError(f"no days for some month x daytype in {calendar_year}")
    return out.with_columns(
        (pl.lit(float(annual_cost_kw_year)) * pl.col("probability_of_peak") / pl.col("num_days")).alias(value_col)
    ).sort("month", "daytype", "hour")


def allocate_annual_cost_by_pop(
    cells: pl.DataFrame,
    annual_cost_kw_year: float,
    year: int,
    *,
    calendar_year: int | None = None,
    value_col: str = "mc_total_per_kwh",
) -> pl.DataFrame:
    """Hourly 8760 of ``value_col`` ($/kWh) that recovers ``annual_cost_kw_year``.

    The per-cell $/kWh from :func:`month_hour_costs_from_pop` is placed on
    the CAIRO 8760 by :func:`expand_month_hour_to_8760`: each hour takes
    its month-hour value from the weekday or the weekend-and-holiday table
    according to ``calendar_year`` (default ``year``), and ``year`` is stamped
    on the timestamps. The uniform scale month-hour applies is skipped: the
    day counts and the calendar match, so a flat 1 kW load pays the annual
    cost exactly. That is checked before returning.

    Returns ``timestamp`` (naive, hour beginning, 8,760 rows) and ``value_col``.
    """
    calendar_year = year if calendar_year is None else calendar_year
    costs = month_hour_costs_from_pop(cells, annual_cost_kw_year, calendar_year, value_col="mc_total_per_kwh")
    tables = {
        daytype: costs.filter(pl.col("daytype") == daytype).select("month", "hour", "mc_total_per_kwh")
        for daytype in DAY_TYPES
    }
    expanded = expand_month_hour_to_8760(
        tables[WEEKDAY],
        tables[WEEKEND_HOLIDAY],
        year,
        include_holidays=True,
        calendar_year=calendar_year,
        output_column=value_col,
    )
    total = float(expanded[value_col].sum())
    if abs(total - annual_cost_kw_year) > 1e-6 * max(1.0, abs(annual_cost_kw_year)):
        raise ValueError(
            f"8760 sums to {total}, expected {annual_cost_kw_year}: the day counts and the calendar expansion disagree"
        )
    return expanded


def pop_kde(
    load: pl.DataFrame,
    annual_cost_kw_year: float,
    year: int,
    calendar_year: int | None = None,
    *,
    output_column: str = "mc_total_per_kwh",
    fitted: ProbabilityOfPeak | None = None,
) -> pl.DataFrame:
    """Fit a KDE probability of peak and spread ``annual_cost_kw_year`` on the 8760.

    Args:
        load: One or more complete calendar years, stacked. Required columns
            are ``timestamp`` and ``load_mw``. Extra columns are ignored. Each
            year needs at least ``MIN_HOURS_PER_YEAR`` rows, because each year
            is divided by its own average load. Ignored when ``fitted`` is passed.
        annual_cost_kw_year: The ``$/kW-yr`` the hourly costs must sum to,
            already in the dollar year of the run.
        year: Year stamped on the output timestamps.
        calendar_year: Calendar for day types and for ``num_days``. Defaults
            to ``year``. Must match ``year`` on leap versus non-leap.
        output_column: Name of the cost column. Bulk transmission uses
            ``bulk_tx_cost_enduse``.
        fitted: A :class:`ProbabilityOfPeak` already fit on ``load``. When
            passed, the load is not fit again. Callers that only want the
            8760 leave this unset.

    Returns:
        ``timestamp`` and ``output_column``, 8,760 rows, sorted by timestamp.
        The cost column sums to ``annual_cost_kw_year``.

    Raises:
        ValueError: A required column is missing, a year is short, a cell has
            fewer than two observations, no cell has density above the
            delimiter, or the 8760 does not sum to ``annual_cost_kw_year``.
        TypeError: ``timestamp`` is not a datetime.
    """
    if output_column == "timestamp":
        raise ValueError("output_column 'timestamp' collides with the timestamp column")
    cells = probability_of_peak(load).cells if fitted is None else fitted.cells
    return allocate_annual_cost_by_pop(
        cells,
        annual_cost_kw_year,
        year,
        calendar_year=calendar_year,
        value_col=output_column,
    )


def print_pop_summary(pop: ProbabilityOfPeak) -> None:
    """Print the delimiter, the per-year normalization, and where the probability lands."""
    print("\n── Probability of Peak (KDE) ──")
    print(f"  Delimiter: {pop.delimiter:.4f} x year mean load")
    print(f"  Delimiter / normalized peak: {pop.delimiter_share_of_peak:.4f}")
    print(pop.years)
    by_month = (
        pop.cells.group_by("month")
        .agg(pl.col("probability_of_peak").sum().alias("pop"))
        .sort("month")
        .filter(pl.col("pop") > 0)
    )
    weekend = pop.cells.filter(pl.col("daytype") == WEEKEND_HOLIDAY)["probability_of_peak"].sum()
    nonzero = pop.cells.filter(pl.col("probability_of_peak") > 0).height
    print(f"  Weekend-and-holiday share: {float(weekend):.4f}")
    print(f"  Nonzero cells: {nonzero} of {pop.cells.height}")
    print("  PoP by month (nonzero):")
    print(by_month)
    print("  Top 5 cells:")
    print(
        pop.cells.sort("probability_of_peak", descending=True)
        .head(5)
        .select("month", "daytype", "hour", "n_obs", "probability_of_peak")
    )


def main(argv: list[str] | None = None) -> None:
    """Read one load parquet, allocate, and write the hourly cost parquet."""
    parser = argparse.ArgumentParser(
        description=(
            "Allocate an annual $/kW-yr by a month x day-type x hour probability of peak, "
            "fit with a truncated Gaussian KDE on one or more years of hourly load."
        )
    )
    parser.add_argument(
        "--path-load",
        type=Path,
        required=True,
        help=(
            "Parquet with timestamp and load_mw. One or more complete calendar years, "
            "stacked. The years present are the history."
        ),
    )
    parser.add_argument(
        "--annual-cost-kw-year",
        type=float,
        required=True,
        help="Annual cost to recover, in $/kW-yr, already in the output dollar year.",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year stamped on the output timestamps.",
    )
    parser.add_argument(
        "--calendar-year",
        type=int,
        default=None,
        help="Calendar for day types and day counts. Defaults to --year. Must match --year on leap years.",
    )
    parser.add_argument(
        "--output-column",
        type=str,
        default="mc_total_per_kwh",
        help="Cost column name (default: mc_total_per_kwh). CT bulk tx uses bulk_tx_cost_enduse.",
    )
    parser.add_argument(
        "--path-output",
        type=Path,
        required=True,
        help="Parquet to write. Columns: timestamp and the cost column. 8,760 rows.",
    )
    args = parser.parse_args(argv)

    allocated = pop_kde(
        pl.read_parquet(args.path_load),
        args.annual_cost_kw_year,
        args.year,
        args.calendar_year,
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

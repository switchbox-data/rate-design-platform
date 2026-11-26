"""Look up a month-hour table onto one calendar year and scale it to a $/kW-yr.

Two table shapes feed the same weekday / weekend / holiday lookup. ``table_kind``
selects the shape. Both paths then multiply every hour by one factor so the
8,760 sums to ``annual_cost_kw_year``.

- ``rates``: the tables are already ``$/kWh`` (``month``, ``hour``,
  ``mc_total_per_kwh``). The annual cost is only the scale target.
- ``probability``: the tables are a probability of peak (``month``, ``hour``,
  ``num_days``, ``probability_of_peak``). Each cell is first
  ``annual_cost_kw_year * probability_of_peak / num_days``, then scaled.
  ``num_days`` is the column on the table, not the target calendar's day count.
  The scale absorbs that mismatch.

Saturday and Sunday take the weekend table. With ``include_holidays`` (the
default), US federal holidays do too: a fixed-date holiday on Saturday is
observed the Friday before, and one on Sunday the Monday after. Juneteenth
counts from 2021. With ``include_holidays`` false, a holiday on Monday through
Friday stays on the weekday table.

``calendar_year`` classifies the days. ``year`` is stamped on the timestamps
by position. The two years must both be leap years or both not. The 8,760
drops December 31 in a leap year, matching ``build_cairo_8760_timestamps``.

``pop_kde`` calls :func:`expand_month_hour_to_8760` with cells it just computed
and skips the scale: its day counts already match the calendar, so the sum is
the annual cost. The holiday calendar both scripts use is :func:`us_federal_holidays`.

This module does not know the state, the utility, the ISO, where the files
came from, or the manifest.
See context/methods/marginal_costs/month_hour_allocation.md.

Usage:
    uv run python utils/data_prep/marginal_costs/methods/month_hour.py \\
        --table-kind rates \\
        --path-weekday weekday.parquet \\
        --path-weekend weekend.parquet \\
        --annual-cost-kw-year 21.22 \\
        --year 2025 \\
        --path-output allocated.parquet
"""

from __future__ import annotations

import argparse
import calendar
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from utils.data_prep.marginal_costs.supply_utils import build_cairo_8760_timestamps

TABLE_KINDS = ("rates", "probability")
_RATE_COLUMNS = ("month", "hour", "mc_total_per_kwh")
_PROBABILITY_COLUMNS = ("month", "hour", "num_days", "probability_of_peak")
_N_MONTH_HOURS = 12 * 24
_RESERVED_COLUMNS = frozenset({"timestamp", "month", "hour", "is_weekend", "is_holiday", "_weekday_mc", "_weekend_mc"})


def us_federal_holidays(year: int) -> set[date]:
    """US federal holidays observed in ``year`` (5 U.S.C. § 6103).

    A fixed-date holiday on Saturday is observed the Friday before and one on
    Sunday the Monday after, so December 31 is a holiday when the next
    January 1 is a Saturday. Juneteenth is included from 2021, the year it
    became a federal holiday.
    """
    fixed = [
        date(year, 1, 1),
        date(year, 7, 4),
        date(year, 11, 11),
        date(year, 12, 25),
        date(year + 1, 1, 1),
    ]
    if year >= 2021:
        fixed.append(date(year, 6, 19))
    floating = [
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
        _nth_weekday(year, 5, 0, -1),  # Memorial Day
        _nth_weekday(year, 9, 0, 1),  # Labor Day
        _nth_weekday(year, 10, 0, 2),  # Columbus Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving
    ]
    observed = {_observe_holiday(day) for day in fixed + floating}
    return {day for day in observed if day.year == year}


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The nth ``weekday`` of ``month`` (Monday = 0). ``n`` of -1 is the last."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    last = date(year, 12, 31) if month == 12 else date(year, month + 1, 1) - timedelta(1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observe_holiday(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def month_hour_costs_from_probability(
    probability: pl.DataFrame,
    annual_cost_kw_year: float,
    *,
    label: str,
) -> pl.DataFrame:
    """Month-hour ``$/kWh`` from an annual ``$/kW-yr`` and a probability of peak.

    Each cell is ``annual_cost_kw_year * probability_of_peak / num_days``.
    ``num_days`` is the column on ``probability``, not a count taken from the
    target calendar. The result has ``month``, ``hour``, and
    ``mc_total_per_kwh``, ready for :func:`expand_month_hour_to_8760`.
    """
    required = set(_PROBABILITY_COLUMNS)
    missing = required - set(probability.columns)
    if missing:
        raise ValueError(f"{label} probability table is missing {sorted(missing)}")
    if annual_cost_kw_year <= 0:
        raise ValueError(f"{label}: annual $/kW-yr must be positive, got {annual_cost_kw_year}")
    rates = probability.select(sorted(required))
    n_unique = rates.select("month", "hour").n_unique()
    if rates.height != _N_MONTH_HOURS or n_unique != rates.height:
        raise ValueError(f"{label} probability table must have 288 unique month-hour rows")
    if rates.filter(pl.col("num_days") <= 0).height:
        raise ValueError(f"{label} probability table has a non-positive day count")
    out_of_range = rates.filter((pl.col("probability_of_peak") < 0) | (pl.col("probability_of_peak") > 1))
    if out_of_range.height:
        raise ValueError(f"{label} probability_of_peak is outside 0–1: {out_of_range.select('month', 'hour').rows()}")
    return rates.select(
        "month",
        "hour",
        (annual_cost_kw_year * pl.col("probability_of_peak") / pl.col("num_days")).alias("mc_total_per_kwh"),
    )


def scale_8760_to_annual(
    df: pl.DataFrame,
    annual_cost_kw_year: float,
    *,
    output_column: str = "mc_total_per_kwh",
) -> tuple[pl.DataFrame, float]:
    """Multiply every hour by one factor so ``output_column`` sums to the annual cost.

    Zero hours stay zero, and the ratio between any two hours is unchanged.
    Returns the scaled frame and the factor (1.0 when the column already sums
    to the target, including a column of zeros scaled to zero).
    """
    total = float(df[output_column].sum())
    if total == 0.0:
        if annual_cost_kw_year != 0.0:
            raise ValueError(f"cannot scale {output_column} to ${annual_cost_kw_year}/kW-yr: the 8760 sums to 0")
        return df, 1.0
    factor = annual_cost_kw_year / total
    return df.with_columns((pl.col(output_column) * factor).alias(output_column)), factor


def expand_month_hour_to_8760(
    weekday: pl.DataFrame,
    weekend: pl.DataFrame,
    year: int,
    *,
    include_holidays: bool = True,
    calendar_year: int | None = None,
    output_column: str = "mc_total_per_kwh",
) -> pl.DataFrame:
    """Expand month-hour ``$/kWh`` onto one calendar year's CAIRO 8760.

    ``weekday`` and ``weekend`` each have ``month`` (1–12), ``hour`` (0–23),
    and ``mc_total_per_kwh``. Saturday and Sunday take the weekend value. When
    ``include_holidays`` is true, federal holidays do too
    (:func:`us_federal_holidays`). When it is false, a holiday on Monday
    through Friday takes the weekday value.

    ``calendar_year`` (default ``year``) classifies each day. ``year`` is
    stamped on ``timestamp``. When they differ, row *i* of ``calendar_year``'s
    8760 is restamped as row *i* of ``year``'s. The two years must both be
    leap years or both not, so each row keeps its month.

    Timestamps match ``build_cairo_8760_timestamps``: naive hour-beginning
    local time, 8,760 rows, with December 31 dropped in a leap year. Does not
    scale. Returns ``timestamp`` and ``output_column``.
    """
    calendar_year = year if calendar_year is None else calendar_year
    if calendar.isleap(calendar_year) != calendar.isleap(year):
        raise ValueError(
            f"calendar_year {calendar_year} and year {year} must both be leap "
            "years or both not; otherwise rows after February shift by a day"
        )
    if output_column in _RESERVED_COLUMNS:
        raise ValueError(f"output_column {output_column!r} collides with a column the lookup uses")
    clock = build_cairo_8760_timestamps(calendar_year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    holidays = us_federal_holidays(calendar_year) if include_holidays else set()
    clock = clock.with_columns(
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.hour().alias("hour"),
        # Polars weekday is ISO: Monday = 1, Sunday = 7.
        (pl.col("timestamp").dt.weekday() >= 6).alias("is_weekend"),
        pl.col("timestamp").dt.date().is_in(sorted(holidays)).alias("is_holiday"),
    )
    weekday_rates = _month_hour_rates(weekday, "weekday").rename({"mc_total_per_kwh": "_weekday_mc"})
    weekend_rates = _month_hour_rates(weekend, "weekend").rename({"mc_total_per_kwh": "_weekend_mc"})
    joined = clock.join(weekday_rates, on=["month", "hour"], how="left").join(
        weekend_rates, on=["month", "hour"], how="left"
    )
    expanded = joined.with_columns(
        pl.when(pl.col("is_weekend") | pl.col("is_holiday"))
        .then(pl.col("_weekend_mc"))
        .otherwise(pl.col("_weekday_mc"))
        .alias(output_column)
    )
    if expanded[output_column].null_count() != 0:
        raise ValueError(f"month-hour lookup missed hours in {calendar_year}")
    expanded = expanded.sort("timestamp").with_columns(
        build_cairo_8760_timestamps(year)["timestamp"].cast(pl.Datetime("us")).alias("timestamp")
    )
    return expanded.select("timestamp", output_column)


def month_hour(
    weekday: pl.DataFrame,
    weekend: pl.DataFrame,
    annual_cost_kw_year: float,
    year: int,
    calendar_year: int | None = None,
    *,
    table_kind: str = "rates",
    include_holidays: bool = True,
    output_column: str = "mc_total_per_kwh",
) -> pl.DataFrame:
    """Expand a month-hour table onto ``year`` and scale it to ``annual_cost_kw_year``.

    Args:
        weekday: 288 rows, ``month`` 1–12 and ``hour`` 0–23. Columns depend on
            ``table_kind``.
        weekend: Same shape, for Saturday, Sunday, and (by default) holidays.
        annual_cost_kw_year: The ``$/kW-yr`` the hourly costs must sum to.
            For ``rates`` this is only the scale target. For ``probability``
            each cell is first this value times ``probability_of_peak`` divided
            by ``num_days``, then scaled.
        year: Year stamped on the output timestamps.
        calendar_year: Calendar that classifies weekdays, weekends, and
            holidays. Defaults to ``year``. Must match ``year`` on leap versus
            non-leap.
        table_kind: ``rates`` or ``probability``.
        include_holidays: When true, federal holidays take the weekend table.
        output_column: Name of the cost column. Distribution uses
            ``mc_total_per_kwh``. Bulk transmission uses ``bulk_tx_cost_enduse``.

    Returns:
        ``timestamp`` and ``output_column``, 8,760 rows, sorted by timestamp.
        The cost column sums to ``annual_cost_kw_year``.

    Raises:
        ValueError: ``table_kind`` is unknown, a table is missing columns or
            is not 288 unique month-hours, a probability is outside 0–1, the
            two years disagree on leap days, the lookup misses an hour, or the
            unscaled 8760 sums to zero.
    """
    if table_kind not in TABLE_KINDS:
        raise ValueError(f"table_kind must be one of {TABLE_KINDS}, got {table_kind!r}")
    if table_kind == "probability":
        weekday = month_hour_costs_from_probability(weekday, annual_cost_kw_year, label="weekday")
        weekend = month_hour_costs_from_probability(weekend, annual_cost_kw_year, label="weekend")
    expanded = expand_month_hour_to_8760(
        weekday,
        weekend,
        year,
        include_holidays=include_holidays,
        calendar_year=calendar_year,
        output_column=output_column,
    )
    scaled, _factor = scale_8760_to_annual(expanded, annual_cost_kw_year, output_column=output_column)
    return scaled


def _month_hour_rates(frame: pl.DataFrame, label: str) -> pl.DataFrame:
    required = set(_RATE_COLUMNS)
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{label} month-hour table is missing {sorted(missing)}")
    rates = frame.select("month", "hour", "mc_total_per_kwh")
    n_unique = rates.select("month", "hour").n_unique()
    if rates.height != _N_MONTH_HOURS or n_unique != rates.height:
        raise ValueError(f"{label} month-hour table must have 288 unique month-hour rows")
    return rates


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise argparse.ArgumentTypeError(f"expected true or false, got {value!r}")


def main(argv: list[str] | None = None) -> None:
    """Read two month-hour parquets, expand and scale, and write the 8760."""
    parser = argparse.ArgumentParser(
        description=(
            "Look up a 288-row month-hour table onto one calendar year and scale "
            "the 8760 so it sums to an annual $/kW-yr."
        )
    )
    parser.add_argument(
        "--table-kind",
        choices=TABLE_KINDS,
        default="rates",
        help=(
            "rates: tables are already $/kWh (month, hour, mc_total_per_kwh). "
            "probability: tables are month, hour, num_days, probability_of_peak, "
            "converted to $/kWh before the lookup. Default: rates."
        ),
    )
    parser.add_argument(
        "--path-weekday",
        type=Path,
        required=True,
        help="Weekday month-hour parquet. 288 rows, month 1-12 and hour 0-23.",
    )
    parser.add_argument(
        "--path-weekend",
        type=Path,
        required=True,
        help="Weekend-and-holiday month-hour parquet. Same shape as --path-weekday.",
    )
    parser.add_argument(
        "--annual-cost-kw-year",
        type=float,
        required=True,
        help="Annual cost to recover, in $/kW-yr. The output cost column sums to this.",
    )
    parser.add_argument("--year", type=int, required=True, help="Year stamped on the output timestamps.")
    parser.add_argument(
        "--calendar-year",
        type=int,
        default=None,
        help="Calendar that classifies weekdays, weekends, and holidays. Defaults to --year.",
    )
    parser.add_argument(
        "--include-holidays",
        type=_parse_bool,
        default=True,
        help="true: federal holidays take the weekend table. false: only Saturday and Sunday do. Default: true.",
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
        help="Parquet to write. Columns: timestamp and the cost column. 8,760 rows.",
    )
    args = parser.parse_args(argv)

    allocated = month_hour(
        pl.read_parquet(args.path_weekday),
        pl.read_parquet(args.path_weekend),
        args.annual_cost_kw_year,
        args.year,
        args.calendar_year,
        table_kind=args.table_kind,
        include_holidays=args.include_holidays,
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

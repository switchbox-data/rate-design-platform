"""OCC-863 Attachment 2 page 1 → month-hour marginal costs. No S3.

The real workbook is not in git. These tests build a small stand-in with the
three matrices page 1 has: all day types, weekdays, and weekends plus holidays.
"""

from __future__ import annotations

import polars as pl
import pytest

from utils.data_prep.marginal_costs.convert_ct_occ863_month_hour_mc import (
    Voltage,
    _MATRIX_TITLES,
    month_hour_marginal_costs,
)


def _matrix_block(
    title: str, day_counts: list[int], value: float
) -> list[tuple[object, ...]]:
    width = 16
    rows: list[tuple[object, ...]] = []
    title_row: list[object] = [None] * width
    title_row[2] = title
    rows.append(tuple(title_row))

    days_row: list[object] = [None] * width
    days_row[3] = "No Days"
    month_row: list[object] = [None] * width
    month_row[3] = "Month >"
    for month, days in enumerate(day_counts, start=1):
        days_row[3 + month] = days
        month_row[3 + month] = month
    rows.append(tuple(days_row))
    rows.append(tuple(month_row))

    header: list[object] = [None] * width
    header[2] = "Hour"
    rows.append(tuple(header))
    for hour in range(24):
        hour_row: list[object] = [None] * width
        hour_row[2] = hour
        for month in range(1, 13):
            hour_row[3 + month] = value if month == 8 and hour == 16 else 0
        rows.append(tuple(hour_row))
    average: list[object] = [None] * width
    average[3] = "Daily Average"
    rows.append(tuple(average))
    return rows


def _page(voltage: Voltage, *, weekday_days: list[int] | None = None) -> pl.DataFrame:
    default_weekday = [19, 19, 23, 22, 20, 21, 21, 22, 21, 20, 20, 22]
    weekend_days = [10, 9, 10, 8, 10, 10, 9, 10, 9, 9, 12, 9]
    if weekday_days is None:
        weekday_days = default_weekday
    all_days = [a + b for a, b in zip(default_weekday, weekend_days, strict=True)]
    all_days_title, weekday_title, weekend_title = _MATRIX_TITLES[voltage]
    rows = (
        [("title",)]
        + _matrix_block(all_days_title, all_days, 0.001)
        + _matrix_block(weekday_title, weekday_days, 0.02557)
        + _matrix_block(weekend_title, weekend_days, 0.00011)
    )
    width = max(len(row) for row in rows)
    padded = [row + (None,) * (width - len(row)) for row in rows]
    return pl.DataFrame(padded, orient="row")


def test_melts_weekdays_with_the_day_count_repeated_on_every_hour() -> None:
    all_days, weekday, weekend = month_hour_marginal_costs(
        _page("secondary"), "secondary"
    )
    assert weekday.columns == ["month", "hour", "num_days", "mc_total_per_kwh"]
    assert all_days.height == 288
    assert weekday.height == 288
    assert weekend.height == 288
    january = weekday.filter(pl.col("month") == 1)
    assert january["hour"].to_list() == list(range(24))
    assert january["num_days"].unique().to_list() == [19]
    assert january["mc_total_per_kwh"].sum() == 0
    august_hour_16 = weekday.filter((pl.col("month") == 8) & (pl.col("hour") == 16))
    assert august_hour_16["mc_total_per_kwh"].item() == pytest.approx(0.02557)
    assert august_hour_16["num_days"].item() == 22
    weekend_august = weekend.filter((pl.col("month") == 8) & (pl.col("hour") == 16))
    assert weekend_august["mc_total_per_kwh"].item() == pytest.approx(0.00011)
    assert weekend_august["num_days"].item() == 10
    all_days_august = all_days.filter((pl.col("month") == 8) & (pl.col("hour") == 16))
    assert all_days_august["mc_total_per_kwh"].item() == pytest.approx(0.001)
    assert all_days_august["num_days"].item() == 32


def test_reads_primary_titles() -> None:
    all_days, _, _ = month_hour_marginal_costs(_page("primary"), "primary")
    assert all_days.filter((pl.col("month") == 8) & (pl.col("hour") == 16))[
        "mc_total_per_kwh"
    ].item() == pytest.approx(0.001)


def test_rejects_day_counts_that_do_not_add_up() -> None:
    weekday_days = [18, 19, 23, 22, 20, 21, 21, 22, 21, 20, 20, 22]
    with pytest.raises(ValueError, match="day counts"):
        month_hour_marginal_costs(
            _page("secondary", weekday_days=weekday_days), "secondary"
        )

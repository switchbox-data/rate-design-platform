"""OCC-863 Attachment 2 page 3 → month-hour probability of peak. No S3.

The real workbook is not in git. These tests build a small stand-in for page 3
(two probability matrices beside a long table in columns A–G) and for the
weekday and weekend matrices page 1 derives from it.
"""

from __future__ import annotations

import polars as pl
import pytest

from utils.data_prep.marginal_costs.convert_ct_occ863_probability_of_peak import (
    _WEEKDAY_TITLE,
    _WEEKEND_TITLE,
    attach_day_counts,
    month_hour_probability_of_peak,
)

_WIDTH = 29
_WEEKDAY_DAYS = [19, 19, 23, 22, 20, 21, 21, 22, 21, 20, 20, 22]
_WEEKEND_DAYS = [10, 9, 10, 8, 10, 10, 9, 10, 9, 9, 12, 9]
_SCALAR = 21.2157


def _long_table_row(index: int) -> list[object]:
    row: list[object] = [None] * _WIDTH
    row[0:7] = [index, "1_2_0", 7, 2, 6, 0, 0]
    return row


def _matrix_block(
    title: str, cells: dict[tuple[int, int], float]
) -> list[list[object]]:
    rows: list[list[object]] = []
    title_row = _long_table_row(1)
    title_row[16] = title
    rows.append(title_row)
    month_row = _long_table_row(2)
    month_row[11] = "Month >"
    for month in range(1, 13):
        month_row[11 + month] = month
    month_row[27] = "Cumulative PoP"
    rows.append(month_row)
    header = _long_table_row(3)
    header[10] = "Hour"
    header[26] = "Hour"
    rows.append(header)
    for hour in range(24):
        hour_row = _long_table_row(4 + hour)
        hour_row[10] = hour
        for month in range(1, 13):
            hour_row[11 + month] = cells.get((month, hour), 0.0)
        rows.append(hour_row)
    rows.append(_long_table_row(99))
    return rows


def _page_3(
    weekday: dict[tuple[int, int], float], weekend: dict[tuple[int, int], float]
) -> pl.DataFrame:
    rows = (
        [_long_table_row(0)]
        + _matrix_block(_WEEKDAY_TITLE, weekday)
        + _matrix_block(_WEEKEND_TITLE, weekend)
    )
    return pl.DataFrame([tuple(row) for row in rows], orient="row")


def _cost_matrix(pop: pl.DataFrame, day_counts: list[int]) -> pl.DataFrame:
    days = pl.DataFrame({"month": list(range(1, 13)), "num_days": day_counts})
    return pop.join(days, on="month").with_columns(
        mc_total_per_kwh=_SCALAR * pl.col("probability_of_peak") / pl.col("num_days")
    )


_WEEKDAY_CELLS = {(7, 17): 0.5, (7, 18): 0.25}
_WEEKEND_CELLS = {(7, 18): 0.25}


def test_melts_both_matrices_as_fractions() -> None:
    weekday, weekend = month_hour_probability_of_peak(
        _page_3(_WEEKDAY_CELLS, _WEEKEND_CELLS)
    )
    assert weekday.columns == ["month", "hour", "probability_of_peak"]
    assert weekday.height == 288
    assert weekend.height == 288
    july_17 = weekday.filter((pl.col("month") == 7) & (pl.col("hour") == 17))
    assert july_17["probability_of_peak"].item() == pytest.approx(0.5)
    july_18 = weekend.filter((pl.col("month") == 7) & (pl.col("hour") == 18))
    assert july_18["probability_of_peak"].item() == pytest.approx(0.25)
    assert weekend["probability_of_peak"].sum() == pytest.approx(0.25)


def test_rejects_matrices_that_do_not_sum_to_one() -> None:
    with pytest.raises(ValueError, match="sum to"):
        month_hour_probability_of_peak(_page_3(_WEEKDAY_CELLS, {(7, 18): 0.2}))


def test_attaches_day_counts_from_the_cost_page() -> None:
    weekday, weekend = month_hour_probability_of_peak(
        _page_3(_WEEKDAY_CELLS, _WEEKEND_CELLS)
    )
    out = attach_day_counts(weekday, _cost_matrix(weekday, _WEEKDAY_DAYS), "weekday")
    assert out.columns == ["month", "hour", "num_days", "probability_of_peak"]
    assert out.filter(pl.col("month") == 7)["num_days"].unique().to_list() == [21]
    out_weekend = attach_day_counts(
        weekend, _cost_matrix(weekend, _WEEKEND_DAYS), "weekend"
    )
    assert out_weekend.filter(pl.col("month") == 11)["num_days"].unique().to_list() == [
        12
    ]


def test_rejects_day_counts_that_do_not_explain_the_cost() -> None:
    weekday, _ = month_hour_probability_of_peak(_page_3(_WEEKDAY_CELLS, _WEEKEND_CELLS))
    cost = _cost_matrix(weekday, _WEEKDAY_DAYS).with_columns(
        mc_total_per_kwh=pl.when((pl.col("month") == 7) & (pl.col("hour") == 18))
        .then(pl.col("mc_total_per_kwh") * 2)
        .otherwise(pl.col("mc_total_per_kwh"))
    )
    with pytest.raises(ValueError, match="not proportional"):
        attach_day_counts(weekday, cost, "weekday")

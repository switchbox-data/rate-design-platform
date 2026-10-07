"""OCC-863 Attachment 3 → hourly system load and monthly peaks. No S3.

The real workbook is not in git. These tests build a small stand-in with the
timestamp drift the workbook has, and a page-1 table with a title row and a
source note.
"""

from __future__ import annotations

import polars as pl
import pytest

from utils.data_prep.marginal_costs.convert_ct_occ863_system_load import (
    assert_monthly_peaks_match_hourly,
    hourly_system_load,
    monthly_peak_mw,
)


def _year_hours(year: int) -> pl.Series:
    n_hours = 8784 if year % 4 == 0 else 8760
    end = pl.select(
        pl.lit(f"{year}-01-01").str.to_datetime() + pl.duration(hours=n_hours - 1)
    ).item()
    return pl.datetime_range(
        pl.select(pl.lit(f"{year}-01-01").str.to_datetime()).item(),
        end,
        interval="1h",
        eager=True,
    )


def _page2(
    years: list[int], *, drift_first_of_second_year: bool = False
) -> pl.DataFrame:
    frames = [pl.DataFrame({"DateTime": _year_hours(year)}) for year in years]
    raw = pl.concat(frames).with_columns(
        (1000.0 + (pl.int_range(pl.len()) % 24)).alias("Actual Load"),
    )
    if drift_first_of_second_year:
        first_of_second = _year_hours(years[1])[0]
        raw = raw.with_columns(
            pl.when(pl.col("DateTime") == first_of_second)
            .then(pl.col("DateTime") - pl.duration(microseconds=2000))
            .otherwise(pl.col("DateTime"))
            .alias("DateTime")
        )
    return raw


def _page1_sheet() -> pl.DataFrame:
    """Title, year header, unit row, twelve months, then a source note."""
    rows: list[tuple[str | None, ...]] = [
        ("Monthly Max MW", None, None, None, None),
        ("Month", "2025", "2024", "2023", "2022"),
        (None, "Max MW", "Max MW", "Max MW", "Max MW"),
    ]
    for month in range(1, 13):
        rows.append(
            (
                str(month),
                str(3000 + month),
                str(2000 + month),
                str(1000 + month),
                str(500 + month),
            )
        )
    rows.append(("Source:", "from substations", None, None, None))
    return pl.DataFrame(
        rows,
        schema=["column_1", "column_2", "column_3", "column_4", "column_5"],
        orient="row",
    )


def test_rounds_a_timestamp_that_excel_stored_just_before_the_hour() -> None:
    load = hourly_system_load(
        _page2([2022, 2023, 2024, 2025], drift_first_of_second_year=True)
    )
    new_year = load.filter(
        pl.col("timestamp").dt.strftime("%Y-%m-%d %H") == "2023-01-01 00"
    )
    assert new_year.height == 1
    assert load["timestamp"].n_unique() == load.height


def test_keeps_the_leap_day_and_names_the_load_column_in_mw() -> None:
    load = hourly_system_load(_page2([2022, 2023, 2024, 2025]))
    assert load.columns == ["timestamp", "load_mw"]
    counts = dict(
        load.with_columns(pl.col("timestamp").dt.year().alias("year"))
        .group_by("year")
        .len()
        .iter_rows()
    )
    assert counts == {2022: 8760, 2023: 8760, 2024: 8784, 2025: 8760}
    leap = load.filter(
        pl.col("timestamp").dt.strftime("%Y-%m-%d %H") == "2024-02-29 00"
    )
    assert leap.height == 1


def test_rejects_a_non_positive_load() -> None:
    raw = _page2([2022, 2023, 2024, 2025]).with_columns(
        pl.when(pl.col("DateTime").dt.strftime("%Y-%m-%d %H") == "2023-06-01 12")
        .then(0.0)
        .otherwise(pl.col("Actual Load"))
        .alias("Actual Load")
    )
    with pytest.raises(ValueError, match="non-positive"):
        hourly_system_load(raw)


def test_monthly_peaks_skip_the_title_and_the_source_note() -> None:
    peaks = monthly_peak_mw(_page1_sheet())
    assert peaks.columns == ["year", "month", "peak_mw"]
    assert peaks.height == 48
    jan_2025 = peaks.filter((pl.col("year") == 2025) & (pl.col("month") == 1))
    assert jan_2025["peak_mw"][0] == 3001.0
    assert peaks["year"].unique().sort().to_list() == [2022, 2023, 2024, 2025]


def test_rejects_a_monthly_peak_that_is_not_the_hourly_maximum() -> None:
    load = hourly_system_load(_page2([2022, 2023, 2024, 2025]))
    peaks = (
        load.with_columns(
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
        )
        .group_by("year", "month")
        .agg(pl.col("load_mw").max().alias("peak_mw"))
        .sort("year", "month")
    )
    broken = peaks.with_columns(
        pl.when((pl.col("year") == 2022) & (pl.col("month") == 11))
        .then(pl.col("peak_mw") + 100)
        .otherwise(pl.col("peak_mw"))
        .alias("peak_mw")
    )
    with pytest.raises(ValueError, match="do not match"):
        assert_monthly_peaks_match_hourly(load, broken)

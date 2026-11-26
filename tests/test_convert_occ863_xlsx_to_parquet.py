"""OCC-863 workbooks -> parquet converters. No S3.

The real workbooks are not in git. These tests build small stand-ins for each
attachment page: the timestamp drift in Attachment 3, the Interval Ending EST
stamps in Attachment 4, the three month-hour matrices on Attachment 2 pages 1
and 2, and the probability matrices on page 3. One test writes stand-in
workbooks to disk and runs the ``all`` subcommand.
"""

from __future__ import annotations

import sys
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from data.eversource.occ863 import convert_occ863_xlsx_to_parquet as occ863
from data.eversource.occ863.convert_occ863_xlsx_to_parquet import (
    _MATRIX_TITLES,
    _WEEKDAY_TITLE,
    _WEEKEND_TITLE,
    Voltage,
    assert_monthly_peaks_match_hourly,
    attach_day_counts,
    hourly_system_load,
    month_hour_marginal_costs,
    month_hour_probability_of_peak,
    monthly_peak_mw,
    rate_class_loads,
    read_sheet,
)

# ---------------------------------------------------------------------------
# Attachment 3: system load and monthly peaks
# ---------------------------------------------------------------------------


def _year_hours(year: int) -> pl.Series:
    n_hours = 8784 if year % 4 == 0 else 8760
    end = pl.select(pl.lit(f"{year}-01-01").str.to_datetime() + pl.duration(hours=n_hours - 1)).item()
    return pl.datetime_range(
        pl.select(pl.lit(f"{year}-01-01").str.to_datetime()).item(),
        end,
        interval="1h",
        eager=True,
    )


def _page2(years: list[int], *, drift_first_of_second_year: bool = False) -> pl.DataFrame:
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
    load = hourly_system_load(_page2([2022, 2023, 2024, 2025], drift_first_of_second_year=True))
    new_year = load.filter(pl.col("timestamp").dt.strftime("%Y-%m-%d %H") == "2023-01-01 00")
    assert new_year.height == 1
    assert load["timestamp"].n_unique() == load.height


def test_keeps_the_leap_day_and_names_the_load_column_in_mw() -> None:
    load = hourly_system_load(_page2([2022, 2023, 2024, 2025]))
    assert load.columns == ["timestamp", "load_mw"]
    # Same unit as the rate-class files and the ISO-NE loads, so joins need no cast.
    assert load.schema["timestamp"] == pl.Datetime("us")
    counts = dict(load.with_columns(pl.col("timestamp").dt.year().alias("year")).group_by("year").len().iter_rows())
    assert counts == {2022: 8760, 2023: 8760, 2024: 8784, 2025: 8760}
    leap = load.filter(pl.col("timestamp").dt.strftime("%Y-%m-%d %H") == "2024-02-29 00")
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


# ---------------------------------------------------------------------------
# Attachment 4: rate-class load
# ---------------------------------------------------------------------------


def _interval_ending_hours() -> pl.Series:
    start = pl.select(pl.lit("2025-01-01 01:00:00").str.to_datetime()).item()
    end = pl.select(pl.lit("2026-01-01 00:00:00").str.to_datetime()).item()
    return pl.datetime_range(start, end, interval="1h", eager=True)


def _sheet() -> pl.DataFrame:
    stamps = _interval_ending_hours()
    n = stamps.len()
    rate1 = pl.Series("r1", [1000.0 + (i % 24) for i in range(n)])
    rate5 = pl.Series("r5", [500.0 + (i % 24) for i in range(n)])
    rate7 = pl.Series("r7", [10.0 + (i % 10) for i in range(n)])
    rows: list[tuple[str | None, str | None, str | None, str | None]] = [
        ("Connecticut Light and Power Company", None, None, None),
        ("Year 2025 Hourly Rate Class Loads", None, None, None),
        (None, None, None, None),
        ("Rate", "1", "5", "7"),
        ("Peak Demand", str(rate1.max()), str(rate5.max()), str(rate7.max())),
        ("Total Usage", str(rate1.sum()), str(rate5.sum()), str(rate7.sum())),
        (None, None, None, None),
        ("Interval Ending EST", "kW", "kW", "kW"),
    ]
    for stamp, a, b, c in zip(stamps.to_list(), rate1.to_list(), rate5.to_list(), rate7.to_list(), strict=True):
        rows.append((str(stamp), str(a), str(b), str(c)))
    rows.append((None, str(rate1.max()), str(rate5.max()), str(rate7.max())))
    rows.append((None, "true", "true", "true"))
    return pl.DataFrame(
        rows,
        schema=["column_1", "column_2", "column_3", "column_4"],
        orient="row",
    )


def test_shifts_interval_ending_back_one_hour_and_drops_the_summary() -> None:
    loads = rate_class_loads(_sheet())
    assert set(loads) == {1, 5, 7}
    rate1 = loads[1]
    assert rate1.columns == ["timestamp", "load_kw", "load_mw"]
    assert rate1.schema["timestamp"] == pl.Datetime("us")
    assert rate1.height == 8760
    assert rate1["timestamp"][0] == pl.select(pl.lit("2025-01-01 00:00:00").str.to_datetime()).item()
    assert rate1["timestamp"][-1] == pl.select(pl.lit("2025-12-31 23:00:00").str.to_datetime()).item()
    first = rate1.row(0, named=True)
    assert first["load_kw"] == 1000.0
    assert first["load_mw"] == pytest.approx(1.0)


def _kw_at(load: pl.DataFrame, stamp: str) -> float:
    row = load.filter(pl.col("timestamp") == pl.lit(stamp).str.to_datetime())
    assert row.height == 1
    return float(row["load_kw"][0])


def test_maps_standard_time_onto_local_clock() -> None:
    """Winter hours stay put. Summer hours move forward one hour.

    The synthetic load is 1000 plus the standard-time hour of day. Local
    16:00 in July therefore carries the 15:00 standard-time value.
    """
    rate1 = rate_class_loads(_sheet())[1]
    assert _kw_at(rate1, "2025-01-15 15:00:00") == 1015.0
    assert _kw_at(rate1, "2025-07-15 16:00:00") == 1015.0
    # Spring-forward 02:00 is the average of local 01:00 (EST 01:00) and
    # local 03:00 (EST 02:00): (1001 + 1002) / 2.
    assert _kw_at(rate1, "2025-03-09 02:00:00") == pytest.approx(1001.5)
    # Fall-back local 01:00 is the average of EST 00:00 and EST 01:00.
    assert _kw_at(rate1, "2025-11-02 01:00:00") == pytest.approx(1000.5)


def test_rejects_a_peak_that_does_not_match_the_hourly_column() -> None:
    raw = _sheet().with_columns(
        pl.when(pl.col("column_1") == "Peak Demand").then(pl.lit("1")).otherwise(pl.col("column_2")).alias("column_2")
    )
    with pytest.raises(ValueError, match="Peak Demand"):
        rate_class_loads(raw)


# ---------------------------------------------------------------------------
# Attachment 2 pages 1 and 2: month-hour marginal costs
# ---------------------------------------------------------------------------


def _mc_matrix_block(title: str, day_counts: list[int], value: float) -> list[tuple[object, ...]]:
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
    rows = [
        ("title",),
        *_mc_matrix_block(all_days_title, all_days, 0.001),
        *_mc_matrix_block(weekday_title, weekday_days, 0.02557),
        *_mc_matrix_block(weekend_title, weekend_days, 0.00011),
    ]
    width = max(len(row) for row in rows)
    padded = [row + (None,) * (width - len(row)) for row in rows]
    return pl.DataFrame(padded, orient="row")


def test_melts_weekdays_with_the_day_count_repeated_on_every_hour() -> None:
    all_days, weekday, weekend = month_hour_marginal_costs(_page("secondary"), "secondary")
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
    assert all_days.filter((pl.col("month") == 8) & (pl.col("hour") == 16))["mc_total_per_kwh"].item() == pytest.approx(
        0.001
    )


def test_read_sheet_keeps_full_precision_in_mixed_columns(tmp_path: Path) -> None:
    path = tmp_path / "mixed.xlsx"
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.title = "Page 1"
    sheet["A1"] = "a title"
    sheet["A2"] = 3.561253561253561e-05
    workbook.save(path)

    raw = read_sheet(path, "Page 1")
    assert raw.rows() == [("a title",), (3.561253561253561e-05,)]


def test_rejects_day_counts_that_do_not_add_up() -> None:
    weekday_days = [18, 19, 23, 22, 20, 21, 21, 22, 21, 20, 20, 22]
    with pytest.raises(ValueError, match="day counts"):
        month_hour_marginal_costs(_page("secondary", weekday_days=weekday_days), "secondary")


# ---------------------------------------------------------------------------
# Attachment 2 page 3: probability of peak
# ---------------------------------------------------------------------------


_WIDTH = 29
_WEEKDAY_DAYS = [19, 19, 23, 22, 20, 21, 21, 22, 21, 20, 20, 22]
_WEEKEND_DAYS = [10, 9, 10, 8, 10, 10, 9, 10, 9, 9, 12, 9]
_SCALAR = 21.2157


def _long_table_row(index: int) -> list[object]:
    row: list[object] = [None] * _WIDTH
    row[0:7] = [index, "1_2_0", 7, 2, 6, 0, 0]
    return row


def _pop_matrix_block(title: str, cells: dict[tuple[int, int], float]) -> list[list[object]]:
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


def _page_3(weekday: dict[tuple[int, int], float], weekend: dict[tuple[int, int], float]) -> pl.DataFrame:
    rows = [
        _long_table_row(0),
        *_pop_matrix_block(_WEEKDAY_TITLE, weekday),
        *_pop_matrix_block(_WEEKEND_TITLE, weekend),
    ]
    return pl.DataFrame([tuple(row) for row in rows], orient="row")


def _cost_matrix(pop: pl.DataFrame, day_counts: list[int]) -> pl.DataFrame:
    days = pl.DataFrame({"month": list(range(1, 13)), "num_days": day_counts})
    return pop.join(days, on="month").with_columns(
        mc_total_per_kwh=_SCALAR * pl.col("probability_of_peak") / pl.col("num_days")
    )


_WEEKDAY_CELLS = {(7, 17): 0.5, (7, 18): 0.25}
_WEEKEND_CELLS = {(7, 18): 0.25}


def test_melts_both_matrices_as_fractions() -> None:
    weekday, weekend = month_hour_probability_of_peak(_page_3(_WEEKDAY_CELLS, _WEEKEND_CELLS))
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
    weekday, weekend = month_hour_probability_of_peak(_page_3(_WEEKDAY_CELLS, _WEEKEND_CELLS))
    out = attach_day_counts(weekday, _cost_matrix(weekday, _WEEKDAY_DAYS), "weekday")
    assert out.columns == ["month", "hour", "num_days", "probability_of_peak"]
    assert out.filter(pl.col("month") == 7)["num_days"].unique().to_list() == [21]
    out_weekend = attach_day_counts(weekend, _cost_matrix(weekend, _WEEKEND_DAYS), "weekend")
    assert out_weekend.filter(pl.col("month") == 11)["num_days"].unique().to_list() == [12]


def test_rejects_day_counts_that_do_not_explain_the_cost() -> None:
    weekday, _ = month_hour_probability_of_peak(_page_3(_WEEKDAY_CELLS, _WEEKEND_CELLS))
    cost = _cost_matrix(weekday, _WEEKDAY_DAYS).with_columns(
        mc_total_per_kwh=pl.when((pl.col("month") == 7) & (pl.col("hour") == 18))
        .then(pl.col("mc_total_per_kwh") * 2)
        .otherwise(pl.col("mc_total_per_kwh"))
    )
    with pytest.raises(ValueError, match="not proportional"):
        attach_day_counts(weekday, cost, "weekday")


# ---------------------------------------------------------------------------
# The ``all`` subcommand
# ---------------------------------------------------------------------------

_EXPECTED_FILES = {
    "system_load.parquet",
    "system_monthly_peak_mw.parquet",
    "rate_1_load.parquet",
    "rate_5_load.parquet",
    "rate_7_load.parquet",
    "month_hour_marginal_costs_all_days_secondary.parquet",
    "month_hour_marginal_costs_weekday_secondary.parquet",
    "month_hour_marginal_costs_weekend_holidays_secondary.parquet",
    "month_hour_marginal_costs_all_days_primary.parquet",
    "month_hour_marginal_costs_weekday_primary.parquet",
    "month_hour_marginal_costs_weekend_holidays_primary.parquet",
    "month_hour_probability_of_peak_weekday.parquet",
    "month_hour_probability_of_peak_weekend_holidays.parquet",
}


def _write_workbook(path: Path, sheets: dict[str, list[tuple[object, ...]]]) -> None:
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for name, rows in sheets.items():
        sheet = workbook.create_sheet(name)
        for row in rows:
            sheet.append(list(row))
    workbook.save(path)


def _cost_page(voltage: Voltage) -> list[tuple[object, ...]]:
    """Page 1 or 2 whose weekday and weekend cells follow page 3's probabilities."""
    all_days_title, weekday_title, weekend_title = _MATRIX_TITLES[voltage]
    all_days = [a + b for a, b in zip(_WEEKDAY_DAYS, _WEEKEND_DAYS, strict=True)]

    def block(title: str, days: list[int], cells: dict[tuple[int, int], float]) -> list[tuple[object, ...]]:
        rows = [list(row) for row in _mc_matrix_block(title, days, 0.0)]
        for (month, hour), pop in cells.items():
            rows[4 + hour][3 + month] = _SCALAR * pop / days[month - 1]
        return [tuple(row) for row in rows]

    return [
        ("title",),
        *block(all_days_title, all_days, {(8, 16): 0.001}),
        *block(weekday_title, _WEEKDAY_DAYS, _WEEKDAY_CELLS),
        *block(weekend_title, _WEEKEND_DAYS, _WEEKEND_CELLS),
    ]


def test_all_writes_every_expected_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path_xlsx = tmp_path / "xlsx"
    path_parquet = tmp_path / "parquet"
    path_xlsx.mkdir()

    page2 = _page2([2022, 2023, 2024, 2025])
    # Every month holds at least 24 hours, so each month's maximum is 1023.0.
    peak_rows: list[tuple[object, ...]] = [("Month", "2025", "2024", "2023", "2022")]
    peak_rows += [(str(month), "1023.0", "1023.0", "1023.0", "1023.0") for month in range(1, 13)]
    _write_workbook(
        path_xlsx / "attachment-3.xlsx",
        {
            "OCC-863 Attachment 3 Page 1": peak_rows,
            "OCC-863 Attachment 3 Page 2": [tuple(page2.columns), *page2.rows()],
        },
    )
    _write_workbook(path_xlsx / "attachment-4.xlsx", {"OCC-863 Attachment 4": _sheet().rows()})
    _write_workbook(
        path_xlsx / "attachment-2.xlsx",
        {
            "OCC-863 Attachment 2 Page 1": _cost_page("secondary"),
            "OCC-863 Attachment 2 Page 2": _cost_page("primary"),
            "OCC-863 Attachment 2 Page 3 ": _page_3(_WEEKDAY_CELLS, _WEEKEND_CELLS).rows(),
        },
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "convert_occ863_xlsx_to_parquet.py",
            "all",
            "--path-local-xlsx",
            str(path_xlsx),
            "--path-local-parquet",
            str(path_parquet),
        ],
    )
    occ863.main()

    assert {path.name for path in path_parquet.glob("*.parquet")} == _EXPECTED_FILES
    weekday = pl.read_parquet(path_parquet / "month_hour_probability_of_peak_weekday.parquet")
    assert weekday.columns == ["month", "hour", "num_days", "probability_of_peak"]
    assert weekday.height == 288
    assert pl.read_parquet(path_parquet / "system_load.parquet").height == 8760 * 3 + 8784
    assert pl.read_parquet(path_parquet / "rate_1_load.parquet").height == 8760


def test_all_rejects_an_uninterpolated_just_variable(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="uninterpolated"):
        occ863.convert_all(tmp_path, Path("{{ path_local_parquet }}"))

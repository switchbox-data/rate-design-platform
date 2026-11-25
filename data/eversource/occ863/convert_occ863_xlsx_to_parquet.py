"""Convert Eversource CT OCC-863 workbooks (PURA Docket 26-05-10) to parquet.

OCC-863 is Eversource's answer to an Office of Consumer Counsel data request.
Three workbooks hold the tables the CT marginal-cost allocation reads:

- Attachment 2, subpart (b): month-hour marginal costs (pages 1 and 2) and
  month-hour probabilities of peak (page 3).
- Attachment 3, subpart (c): hourly distribution-substation load, 2022-2025
  (page 2), and its monthly maximum (page 1).
- Attachment 4, subpart (d): 2025 hourly load for residential Rates 1, 5, 7.

Subcommands:

- ``system-load``: Attachment 3 -> ``system_load.parquet`` and
  ``system_monthly_peak_mw.parquet``.
- ``rate-class-load``: Attachment 4 -> ``rate_{1,5,7}_load.parquet``.
- ``month-hour-mc --voltage {secondary,primary}``: Attachment 2 page 1 or 2 ->
  three ``month_hour_marginal_costs_*_<voltage>.parquet`` files.
- ``probability-of-peak``: Attachment 2 page 3 ->
  ``month_hour_probability_of_peak_{weekday,weekend_holidays}.parquet``.
- ``all``: every output above, both voltages, from a directory holding
  ``attachment-2.xlsx``, ``attachment-3.xlsx``, and ``attachment-4.xlsx``.

Every converter checks its outputs (row counts, page 1 peaks against page 2
hours, day counts that add up, probabilities that sum to 1, cost proportional
to probability over day count) and raises before any parquet is written.
Details are in ``context/code/data/ct_occ863_system_load.md``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal

import openpyxl
import polars as pl

Voltage = Literal["primary", "secondary"]

# ---------------------------------------------------------------------------
# Shared constants and cell helpers
# ---------------------------------------------------------------------------

_SYSTEM_HOURLY_SHEET = "OCC-863 Attachment 3 Page 2"
_SYSTEM_MONTHLY_SHEET = "OCC-863 Attachment 3 Page 1"
_SYSTEM_HOURLY_FILENAME = "system_load.parquet"
_SYSTEM_MONTHLY_FILENAME = "system_monthly_peak_mw.parquet"
_EXPECTED_HOURS = {2022: 8760, 2023: 8760, 2024: 8784, 2025: 8760}

_RATE_CLASS_SHEET = "OCC-863 Attachment 4"
_RATES = (1, 5, 7)

_MARGINAL_COST_SHEETS: dict[Voltage, str] = {
    "secondary": "OCC-863 Attachment 2 Page 1",
    "primary": "OCC-863 Attachment 2 Page 2",
}
_N_MONTHS = 12
_N_HOURS = 24
# Titles as stored on the sheet, with line breaks collapsed to single spaces.
# The weekend secondary title has no space before "("; the primary one does.
_MATRIX_TITLES: dict[Voltage, tuple[str, str, str]] = {
    "secondary": (
        "Average of Hourly Marginal Cost, across all day types, Secondary $/kWh",
        "Average Hourly Marginal Cost by Month, Weekdays Only. Secondary ($/kWh)",
        "Average Hourly Marginal Cost by Month, Weekend & Public Holiday, Secondary( $/kWh)",
    ),
    "primary": (
        "Average of Hourly Marginal Cost Across day types, Primary $/kWh",
        "Average Hourly Marginal Cost by Month, Weekdays Only. Primary ($/kWh)",
        "Average Hourly Marginal Cost by Month, Weekend & Public Holiday, Primary ($/kWh)",
    ),
}

# The sheet name on the workbook ends in a space.
_PROBABILITY_SHEET = "OCC-863 Attachment 2 Page 3 "
_WEEKDAY_TITLE = "Weekday Hourly Probability of Peak"
_WEEKEND_TITLE = "Weekend / Holiday Hourly Probability of Peak"
_SUM_TOLERANCE = 1e-9
_RATIO_TOLERANCE = 1e-6

_ATTACHMENT_2 = "attachment-2.xlsx"
_ATTACHMENT_3 = "attachment-3.xlsx"
_ATTACHMENT_4 = "attachment-4.xlsx"


def _reject_just_placeholders(*paths: Path) -> None:
    """Raise if a path still holds an uninterpolated Just variable."""
    for path in paths:
        text = str(path)
        if "{{" in text or "}}" in text:
            raise ValueError(f"path looks like an uninterpolated Just variable: {text}")


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _as_number(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _as_int(value: object) -> int | None:
    number = _as_number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def read_sheet(path_xlsx: Path, sheet_name: str) -> pl.DataFrame:
    """One sheet's cached cell values as untyped (Object) columns.

    ``pl.read_excel`` turns a column that mixes text and numbers into strings
    and drops digits (``3.561253561e-05`` becomes ``0.000035613``). openpyxl
    returns the stored floats.
    """
    workbook = openpyxl.load_workbook(path_xlsx, data_only=True, read_only=True)
    try:
        rows = list(workbook[sheet_name].iter_rows(values_only=True))
    finally:
        workbook.close()
    width = max((len(row) for row in rows), default=0)
    return pl.DataFrame(
        [
            pl.Series(
                f"column_{col + 1}",
                [row[col] if col < len(row) else None for row in rows],
                dtype=pl.Object,
            )
            for col in range(width)
        ]
    )


# ---------------------------------------------------------------------------
# Attachment 3: hourly system load and monthly peaks
#
# Page 2 is the hourly sum of Eversource CT distribution-substation loads for
# 2022-2025, in megawatts. Page 1 is the monthly maximum of that series.
#
# Page 2 is the source for ``system_load.parquet``, not Attachment 1. The two
# sheets agree on every hour except 1:00 a.m. on the three fall-back Sundays,
# where Attachment 1 stores twice the page 2 value. Page 1's November maximum
# matches page 2.
#
# Excel stores a handful of timestamps a fraction of a second before the hour.
# Those are rounded to the nearest hour. 2024 keeps its leap day (8784 hours).
# ---------------------------------------------------------------------------


def hourly_system_load(raw: pl.DataFrame) -> pl.DataFrame:
    """Return ``timestamp`` and ``load_mw`` from Attachment 3 page 2.

    ``raw`` must have ``DateTime`` and ``Actual Load``. Blank rows are dropped.
    Timestamps are rounded to the nearest hour and must form one contiguous
    hour-beginning series for each of 2022, 2023, 2024, and 2025.
    """
    required = {"DateTime", "Actual Load"}
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Attachment 3 page 2 is missing columns: {sorted(missing)}")

    load = (
        raw.select(
            pl.col("DateTime").alias("timestamp"),
            pl.col("Actual Load").cast(pl.Float64).alias("load_mw"),
        )
        .drop_nulls(subset=["timestamp", "load_mw"])
        .with_columns(pl.col("timestamp").dt.round("1h").cast(pl.Datetime("us")))
        .sort("timestamp")
    )
    _validate_system_load(load)
    return load


def monthly_peak_mw(raw: pl.DataFrame) -> pl.DataFrame:
    """Return ``year``, ``month``, and ``peak_mw`` from Attachment 3 page 1.

    ``raw`` is the sheet read with no header. The year labels sit on the row
    whose first cell is ``Month``; the twelve data rows follow, and a source
    note follows those. Years come out oldest-first.
    """
    label_col = raw.columns[0]
    labels = raw.get_column(label_col).to_list()
    try:
        header_idx = labels.index("Month")
    except ValueError as exc:
        raise ValueError("Attachment 3 page 1 has no Month header row") from exc

    year_cols: list[tuple[str, int]] = []
    for col in raw.columns[1:]:
        year_label = raw[col][header_idx]
        if year_label is not None and str(year_label).isdigit():
            year_cols.append((col, int(str(year_label))))
    if not year_cols:
        raise ValueError("Attachment 3 page 1 has no year columns")

    rows: list[dict[str, int | float]] = []
    for record in raw.slice(header_idx + 1).iter_rows(named=True):
        month_label = record[label_col]
        if month_label is None or not str(month_label).isdigit():
            continue
        month = int(str(month_label))
        for col, year in year_cols:
            value = record[col]
            if value is None:
                raise ValueError(f"missing peak for {year}-{month:02d}")
            rows.append({"year": year, "month": month, "peak_mw": float(value)})

    peaks = pl.DataFrame(rows).sort("year", "month")
    _validate_monthly_peaks(peaks)
    return peaks


def assert_monthly_peaks_match_hourly(load: pl.DataFrame, peaks: pl.DataFrame) -> None:
    """Raise if a page 1 peak is not the maximum of that month on page 2."""
    from_hourly = (
        load.with_columns(
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
        )
        .group_by("year", "month")
        .agg(pl.col("load_mw").max().alias("hourly_max"))
    )
    compared = peaks.join(from_hourly, on=["year", "month"], how="left")
    mismatched = compared.filter(
        pl.col("hourly_max").is_null() | ((pl.col("peak_mw") - pl.col("hourly_max")).abs() > 1e-6)
    )
    if mismatched.height:
        sample = mismatched.select("year", "month", "peak_mw", "hourly_max").head(4)
        raise ValueError(f"monthly peaks do not match the hourly series:\n{sample}")


def _validate_system_load(load: pl.DataFrame) -> None:
    if load["timestamp"].n_unique() != load.height:
        raise ValueError("rounded timestamps are not unique")
    if load.filter(pl.col("load_mw") <= 0).height:
        raise ValueError("system load has a non-positive MW value")

    years = load.with_columns(pl.col("timestamp").dt.year().alias("year"))
    counts = dict(years.group_by("year").len().iter_rows())
    if counts != _EXPECTED_HOURS:
        raise ValueError(f"expected hours {_EXPECTED_HOURS}, got {counts}")

    for year, hours in _EXPECTED_HOURS.items():
        year_ts = years.filter(pl.col("year") == year)["timestamp"]
        start = year_ts[0]
        if (start.year, start.month, start.day, start.hour) != (year, 1, 1, 0):
            raise ValueError(f"{year} does not start at Jan 1 hour 0: {start}")
        if year_ts.len() != hours:
            raise ValueError(f"{year} has {year_ts.len()} hours, expected {hours}")


def _validate_monthly_peaks(peaks: pl.DataFrame) -> None:
    expected = {(year, month) for year in _EXPECTED_HOURS for month in range(1, 13)}
    found = set(peaks.select("year", "month").iter_rows())
    if found != expected:
        raise ValueError(f"expected monthly peaks {sorted(expected)}, got {sorted(found)}")
    if peaks.filter(pl.col("peak_mw") <= 0).height:
        raise ValueError("monthly peak has a non-positive MW value")


def write_occ863_load_parquets(path_xlsx: Path, path_output_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read Attachment 3 and write both parquets under ``path_output_dir``."""
    _reject_just_placeholders(path_xlsx, path_output_dir)
    raw_hourly = pl.read_excel(
        path_xlsx,
        sheet_name=_SYSTEM_HOURLY_SHEET,
        columns=["DateTime", "Actual Load"],
    )
    raw_monthly = pl.read_excel(path_xlsx, sheet_name=_SYSTEM_MONTHLY_SHEET, has_header=False)
    load = hourly_system_load(raw_hourly)
    peaks = monthly_peak_mw(raw_monthly)
    assert_monthly_peaks_match_hourly(load, peaks)

    path_output_dir.mkdir(parents=True, exist_ok=True)
    load.write_parquet(path_output_dir / _SYSTEM_HOURLY_FILENAME)
    peaks.write_parquet(path_output_dir / _SYSTEM_MONTHLY_FILENAME)
    return load, peaks


# ---------------------------------------------------------------------------
# Attachment 4: 2025 hourly rate-class load
#
# The sheet labels each stamp "Interval Ending EST". The hour that stamp closes
# is the previous hour, so 2025-01-01 01:00:00 becomes a hour-beginning
# timestamp of 2025-01-01 00:00:00. The last stamp, 2026-01-01 00:00:00,
# becomes 2025-12-31 23:00:00.
#
# Those hour-beginning stamps are Eastern Standard Time all year. The other
# hourly files (``system_load.parquet``, the CAIRO 8760) are local wall-clock
# time with no timezone stored. This converter maps standard time onto that
# clock: winter hours stay put, summer hours move forward one hour, the
# fall-back hour is the average of the two standard-time hours that land on it,
# and the missing spring-forward hour is the average of the hours on either
# side.
#
# Title rows, the Peak Demand and Total Usage summaries, and the footer check
# are dropped. Those summaries are checked against the hourly column before
# they are dropped.
# ---------------------------------------------------------------------------


def rate_class_filename(rate: int) -> str:
    """Parquet name for one residential rate class."""
    return f"rate_{rate}_load.parquet"


def rate_class_loads(raw: pl.DataFrame) -> dict[int, pl.DataFrame]:
    """Return one ``timestamp`` / ``load_kw`` / ``load_mw`` frame per rate.

    ``raw`` is Attachment 4 read with no header and with empty rows kept, so
    the title block stays aligned with the sheet. Blank rows, the summary
    block, and the footer are not part of the returned frames.
    """
    label_col = raw.columns[0]
    labels = [None if value is None else str(value) for value in raw.get_column(label_col).to_list()]
    rate_idx = _label_index(labels, "Rate")
    peak_idx = _label_index(labels, "Peak Demand")
    usage_idx = _label_index(labels, "Total Usage")
    header_idx = _label_index(labels, "Interval Ending EST")

    frames: dict[int, pl.DataFrame] = {}
    for col in raw.columns[1:]:
        rate_label = raw[col][rate_idx]
        if rate_label is None or not str(rate_label).isdigit():
            continue
        rate = int(str(rate_label))
        if rate not in _RATES:
            raise ValueError(f"unexpected rate column {rate}")
        peak_kw = float(str(raw[col][peak_idx]))
        usage_kwh = float(str(raw[col][usage_idx]))
        frames[rate] = _one_rate(raw, label_col, col, header_idx, peak_kw, usage_kwh)

    missing = [rate for rate in _RATES if rate not in frames]
    if missing:
        raise ValueError(f"Attachment 4 is missing rates {missing}")
    return frames


def _one_rate(
    raw: pl.DataFrame,
    label_col: str,
    value_col: str,
    header_idx: int,
    peak_kw: float,
    usage_kwh: float,
) -> pl.DataFrame:
    load = (
        raw.slice(header_idx + 1)
        .select(
            pl.col(label_col).str.to_datetime(strict=False).alias("interval_ending"),
            pl.col(value_col).cast(pl.Float64, strict=False).alias("load_kw"),
        )
        .drop_nulls(subset=["interval_ending", "load_kw"])
        .with_columns(
            (pl.col("interval_ending") - pl.duration(hours=1)).alias("timestamp"),
            (pl.col("load_kw") / 1000.0).alias("load_mw"),
        )
        .select("timestamp", "load_kw", "load_mw")
        .sort("timestamp")
    )
    _validate_rate_load(load, peak_kw, usage_kwh)
    local = est_hour_beginning_to_local(load)
    _validate_local_clock(local)
    return local


def est_hour_beginning_to_local(load: pl.DataFrame) -> pl.DataFrame:
    """Map hour-beginning Eastern Standard Time onto local wall-clock time.

    ``load`` has ``timestamp``, ``load_kw``, and ``load_mw``. The timestamps
    are naive and mean Eastern Standard Time (UTC-5), which is how Attachment
    4 labels the year. The returned timestamps are naive ``America/New_York``
    wall time, matching ``system_load.parquet``.

    Two clock-change hours are not a one-hour shift. On the fall-back morning
    two standard-time hours share local 01:00; those loads are averaged. On
    the spring-forward morning local 02:00 does not exist in the standard-time
    series; it is filled with the average of local 01:00 and 03:00.
    """
    local = load.with_columns(
        pl.col("timestamp")
        .dt.replace_time_zone("Etc/GMT+5")
        .dt.convert_time_zone("America/New_York")
        .dt.replace_time_zone(None)
        .cast(pl.Datetime("us"))
        .alias("timestamp")
    )
    local = local.group_by("timestamp").agg(pl.col("load_kw").mean(), pl.col("load_mw").mean()).sort("timestamp")
    year = local["timestamp"][0].year
    expected_n = 8784 if year % 4 == 0 else 8760
    if local.height != expected_n - 1:
        raise ValueError(f"expected {expected_n - 1} local hours before the spring-forward fill, got {local.height}")

    start = pl.select(pl.lit(f"{year}-01-01 00:00:00").str.to_datetime()).item()
    end = pl.select(pl.lit(f"{year}-12-31 23:00:00").str.to_datetime()).item()
    expected = pl.DataFrame({"timestamp": pl.datetime_range(start, end, interval="1h", eager=True)}).with_columns(
        pl.col("timestamp").cast(pl.Datetime("us"))
    )
    joined = expected.join(local, on="timestamp", how="left").sort("timestamp")
    missing = joined.filter(pl.col("load_kw").is_null())
    if missing.height != 1:
        raise ValueError(f"expected one missing local hour (spring-forward 02:00), got {missing.height}")
    gap = missing["timestamp"][0]
    if (gap.month, gap.hour) != (3, 2):
        raise ValueError(f"unexpected gap in local rate-class load: {gap}")
    # One missing hour between its neighbors is their average.
    filled = joined.with_columns(
        pl.col("load_kw").interpolate(),
        pl.col("load_mw").interpolate(),
    )
    if filled.filter(pl.col("load_kw").is_null()).height:
        raise ValueError("spring-forward hour was not filled")
    return filled


def _validate_local_clock(load: pl.DataFrame) -> None:
    if load["timestamp"].n_unique() != load.height:
        raise ValueError("local rate-class timestamps are not unique")
    start = load["timestamp"][0]
    end = load["timestamp"][-1]
    year = start.year
    expected = 8784 if year % 4 == 0 else 8760
    if load.height != expected:
        raise ValueError(f"expected {expected} local hours in {year}, got {load.height}")
    if (start.month, start.day, start.hour) != (1, 1, 0):
        raise ValueError(f"local series does not start at Jan 1 hour 0: {start}")
    if (end.year, end.month, end.day, end.hour) != (year, 12, 31, 23):
        raise ValueError(f"local series does not end at Dec 31 hour 23: {end}")
    steps = load.select(pl.col("timestamp").diff().dt.total_hours().alias("dh")).drop_nulls()
    if steps.filter(pl.col("dh") != 1).height:
        raise ValueError("local rate-class timestamps are not one hour apart")


def _validate_rate_load(load: pl.DataFrame, peak_kw: float, usage_kwh: float) -> None:
    if load["timestamp"].n_unique() != load.height:
        raise ValueError("rate-class timestamps are not unique")
    if load.filter(pl.col("load_kw") <= 0).height:
        raise ValueError("rate-class load has a non-positive kW value")

    start = load["timestamp"][0]
    end = load["timestamp"][-1]
    year = start.year
    expected = 8784 if year % 4 == 0 else 8760
    if load.height != expected:
        raise ValueError(f"expected {expected} hours in {year}, got {load.height}")
    if (start.month, start.day, start.hour) != (1, 1, 0):
        raise ValueError(f"series does not start at Jan 1 hour 0: {start}")
    if (end.year, end.month, end.day, end.hour) != (year, 12, 31, 23):
        raise ValueError(f"series does not end at Dec 31 hour 23: {end}")
    steps = load.select(pl.col("timestamp").diff().dt.total_hours().alias("dh")).drop_nulls()
    if steps.filter(pl.col("dh") != 1).height:
        raise ValueError("rate-class timestamps are not one hour apart")

    hourly_peak = load.select(pl.col("load_kw").max()).item()
    hourly_sum = load.select(pl.col("load_kw").sum()).item()
    if not isinstance(hourly_peak, (int, float)) or not isinstance(hourly_sum, (int, float)):
        raise TypeError("rate-class load did not aggregate to a number")
    if abs(hourly_peak - peak_kw) > 0.05:
        raise ValueError(f"hourly peak {hourly_peak} does not match Peak Demand {peak_kw}")
    if abs(hourly_sum - usage_kwh) > 1.0:
        raise ValueError(f"hourly sum {hourly_sum} does not match Total Usage {usage_kwh}")


def _label_index(labels: list[str | None], label: str) -> int:
    try:
        return labels.index(label)
    except ValueError as exc:
        raise ValueError(f"Attachment 4 has no {label!r} row") from exc


def write_rate_class_loads(path_xlsx: Path, path_output_dir: Path) -> dict[int, pl.DataFrame]:
    """Read Attachment 4 and write one parquet per rate under ``path_output_dir``."""
    _reject_just_placeholders(path_xlsx, path_output_dir)
    raw = pl.read_excel(
        path_xlsx,
        sheet_name=_RATE_CLASS_SHEET,
        has_header=False,
        drop_empty_rows=False,
        drop_empty_cols=False,
    )
    if not isinstance(raw, pl.DataFrame):
        raise TypeError("expected one sheet from Attachment 4")
    loads = rate_class_loads(raw)
    path_output_dir.mkdir(parents=True, exist_ok=True)
    for rate, load in loads.items():
        load.write_parquet(path_output_dir / rate_class_filename(rate))
    return loads


# ---------------------------------------------------------------------------
# Attachment 2 pages 1 and 2: month-hour marginal costs
#
# Page 1 is secondary service and page 2 is primary. Each holds three
# month-by-hour matrices of average marginal cost in $/kWh: all day types,
# weekdays, and weekends plus public holidays. Each matrix has a "No Days" row
# giving the day count behind that month.
#
# For each voltage all three matrices are written. The all-day-types day count
# must equal the weekday count plus the weekend-and-holiday count. Page 3 is
# the probability-of-peak table those matrices are built from. A fourth sheet
# is a copy of page 3 with an extra ``dist_mc`` column; it is not page 2.
# ---------------------------------------------------------------------------


def month_hour_marginal_costs(raw: pl.DataFrame, voltage: Voltage) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return the all-days, weekday, and weekend-and-holiday matrices.

    ``voltage`` is ``primary`` or ``secondary``. It selects the three matrix
    titles on that sheet. The caller reads the matching sheet.

    Each frame has ``month`` (1-12), ``hour`` (0-23), ``num_days`` (the "No
    Days" count for that month, repeated on every hour), and ``mc_total_per_kwh``.
    """
    all_days_title, weekday_title, weekend_title = _MATRIX_TITLES[voltage]
    rows = raw.rows()
    all_days = _melt_matrix(rows, all_days_title)
    weekday = _melt_matrix(rows, weekday_title)
    weekend = _melt_matrix(rows, weekend_title)
    _check_day_counts_add(all_days, weekday, weekend)
    return all_days, weekday, weekend


def _melt_matrix(rows: list[tuple[object, ...]], title: str) -> pl.DataFrame:
    title_idx = _find_title(rows, title)
    days_idx = _find_label(rows, title_idx, "No Days")
    month_idx = _find_label(rows, days_idx, "Month")
    hour_header_idx, hour_col = _find_hour_header(rows, month_idx)
    month_cols = _month_columns(rows[month_idx])
    day_counts = _day_counts(rows[days_idx], month_cols)

    records: list[dict[str, int | float]] = []
    for row in rows[hour_header_idx + 1 :]:
        hour = _as_int(row[hour_col] if hour_col < len(row) else None)
        if hour is None:
            break
        if not 0 <= hour < _N_HOURS:
            raise ValueError(f"hour {hour} is outside 0-23 in {title!r}")
        for col, month in month_cols:
            mc = _as_number(row[col] if col < len(row) else None)
            if mc is None:
                raise ValueError(f"missing {title!r} value at month {month} hour {hour}")
            if mc < 0:
                raise ValueError(f"negative {title!r} value at month {month} hour {hour}")
            records.append(
                {
                    "month": month,
                    "hour": hour,
                    "num_days": day_counts[month],
                    "mc_total_per_kwh": mc,
                }
            )

    frame = pl.DataFrame(records).cast(
        {
            "month": pl.Int64,
            "hour": pl.Int64,
            "num_days": pl.Int64,
            "mc_total_per_kwh": pl.Float64,
        }
    )
    _validate_matrix(frame, title)
    return frame.sort("month", "hour")


def _find_title(rows: list[tuple[object, ...]], title: str) -> int:
    for idx, row in enumerate(rows):
        for cell in row:
            if _cell_text(cell) == title:
                return idx
    raise ValueError(f"Attachment 2 has no matrix titled {title!r}")


def _find_label(rows: list[tuple[object, ...]], start: int, label: str) -> int:
    for idx in range(start, len(rows)):
        if any(_cell_text(cell).startswith(label) for cell in rows[idx]):
            return idx
    raise ValueError(f"Attachment 2 has no {label!r} row after the matrix title")


def _find_hour_header(rows: list[tuple[object, ...]], start: int) -> tuple[int, int]:
    for idx in range(start, len(rows)):
        for col, cell in enumerate(rows[idx]):
            if _cell_text(cell) == "Hour":
                return idx, col
    raise ValueError("Attachment 2 has no Hour header")


def _month_columns(row: tuple[object, ...]) -> list[tuple[int, int]]:
    found: list[tuple[int, int]] = []
    for col, cell in enumerate(row):
        month = _as_int(cell)
        if month is not None and 1 <= month <= 12:
            found.append((col, month))
    months = [month for _, month in found]
    if months != list(range(1, 13)):
        raise ValueError(f"month header is {months}, expected 1-12 in order")
    return found


def _day_counts(row: tuple[object, ...], month_cols: list[tuple[int, int]]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for col, month in month_cols:
        days = _as_int(row[col] if col < len(row) else None)
        if days is None or days <= 0:
            raise ValueError(f"month {month} has no positive day count")
        counts[month] = days
    return counts


def _validate_matrix(frame: pl.DataFrame, title: str) -> None:
    if frame.height != _N_MONTHS * _N_HOURS:
        raise ValueError(f"{title!r} has {frame.height} rows, expected 288")
    if frame.select("month", "hour").n_unique() != frame.height:
        raise ValueError(f"{title!r} repeats a month-hour")
    hours = set(frame["hour"].unique().to_list())
    months = set(frame["month"].unique().to_list())
    if hours != set(range(_N_HOURS)) or months != set(range(1, 13)):
        raise ValueError(f"{title!r} does not cover months 1-12 and hours 0-23")
    varying = frame.group_by("month").agg(pl.col("num_days").n_unique()).filter(pl.col("num_days") != 1)
    if varying.height:
        raise ValueError(f"{title!r} day count changes within a month")


def _check_day_counts_add(all_days: pl.DataFrame, weekday: pl.DataFrame, weekend: pl.DataFrame) -> None:
    """Weekday days plus weekend-and-holiday days equal the all-days count."""
    combined = (
        weekday.select("month", pl.col("num_days").alias("weekday"))
        .unique()
        .join(
            weekend.select("month", pl.col("num_days").alias("weekend")).unique(),
            on="month",
        )
        .join(
            all_days.select("month", pl.col("num_days").alias("all_days")).unique(),
            on="month",
        )
        .filter(pl.col("weekday") + pl.col("weekend") != pl.col("all_days"))
    )
    if combined.height:
        raise ValueError(f"weekday plus weekend day counts do not match the all-days row: {combined.rows()}")


def _month_hour_mc_filename(kind: str, voltage: Voltage) -> str:
    return f"month_hour_marginal_costs_{kind}_{voltage}.parquet"


def write_month_hour_parquets(
    path_xlsx: Path, path_output_dir: Path, voltage: Voltage
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Read one Attachment 2 sheet and write its three parquets."""
    _reject_just_placeholders(path_xlsx, path_output_dir)
    raw = read_sheet(path_xlsx, _MARGINAL_COST_SHEETS[voltage])
    all_days, weekday, weekend = month_hour_marginal_costs(raw, voltage)
    path_output_dir.mkdir(parents=True, exist_ok=True)
    for kind, frame in (
        ("all_days", all_days),
        ("weekday", weekday),
        ("weekend_holidays", weekend),
    ):
        frame.write_parquet(path_output_dir / _month_hour_mc_filename(kind, voltage))
    return all_days, weekday, weekend


# ---------------------------------------------------------------------------
# Attachment 2 page 3: month-hour probabilities of peak
#
# Page 3 holds two month-by-hour matrices: "Weekday Hourly Probability of Peak"
# and "Weekend / Holiday Hourly Probability of Peak". Each cell is the share of
# the distribution peak (from 2022-2025 hourly substation loads) that falls in
# that month, hour, and day type. Both matrices together sum to 1.
#
# Pages 1 and 2 are built from these matrices. Each weekday cell there is the
# loss-adjusted $/kW-yr times the weekday probability, divided by that month's
# weekday "No Days" count; the weekend-and-holiday matrix works the same way.
# Page 3 has no day counts, so this converter takes them from page 1, checks
# that page 2 has the same counts, and checks that both pages are proportional
# to probability / num_days.
# ---------------------------------------------------------------------------


def month_hour_probability_of_peak(
    raw: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return the weekday and weekend-and-holiday matrices from page 3.

    Each frame has ``month`` (1-12), ``hour`` (0-23), and
    ``probability_of_peak`` (a fraction, not a percent).
    """
    rows = raw.rows()
    weekday = _melt_probability_matrix(rows, _WEEKDAY_TITLE)
    weekend = _melt_probability_matrix(rows, _WEEKEND_TITLE)
    total = float(weekday["probability_of_peak"].sum()) + float(weekend["probability_of_peak"].sum())
    if abs(total - 1.0) > _SUM_TOLERANCE:
        raise ValueError(f"probabilities of peak sum to {total}, expected 1")
    return weekday, weekend


def _melt_probability_matrix(rows: list[tuple[object, ...]], title: str) -> pl.DataFrame:
    title_idx = _find_cell(rows, 0, title)[0]
    month_idx, label_col = _find_cell(rows, title_idx, "Month >")
    hour_idx, hour_col = _find_cell(rows, month_idx, "Hour")
    month_cols = _month_columns_after(rows[month_idx], label_col)

    records: list[dict[str, int | float]] = []
    for row in rows[hour_idx + 1 :]:
        hour = _as_int(row[hour_col] if hour_col < len(row) else None)
        if hour is None:
            break
        if not 0 <= hour < _N_HOURS:
            raise ValueError(f"hour {hour} is outside 0-23 in {title!r}")
        for col, month in month_cols:
            pop = _as_number(row[col] if col < len(row) else None)
            if pop is None:
                raise ValueError(f"missing {title!r} value at month {month} hour {hour}")
            if not 0 <= pop <= 1:
                raise ValueError(f"{title!r} value {pop} at month {month} hour {hour} is outside 0-1")
            records.append({"month": month, "hour": hour, "probability_of_peak": pop})

    frame = pl.DataFrame(records).cast({"month": pl.Int64, "hour": pl.Int64, "probability_of_peak": pl.Float64})
    if frame.height != _N_MONTHS * _N_HOURS:
        raise ValueError(f"{title!r} has {frame.height} rows, expected 288")
    if frame.select("month", "hour").n_unique() != frame.height:
        raise ValueError(f"{title!r} repeats a month-hour")
    return frame.sort("month", "hour")


def _find_cell(rows: list[tuple[object, ...]], start: int, text: str) -> tuple[int, int]:
    for idx in range(start, len(rows)):
        for col, cell in enumerate(rows[idx]):
            if _cell_text(cell) == text:
                return idx, col
    raise ValueError(f"Attachment 2 page 3 has no {text!r} cell after row {start}")


def _month_columns_after(row: tuple[object, ...], label_col: int) -> list[tuple[int, int]]:
    """Month header cells to the right of "Month >".

    Columns A-G hold the long PoP table, whose numbers would otherwise be read
    as month headers.
    """
    found: list[tuple[int, int]] = []
    for col in range(label_col + 1, len(row)):
        month = _as_int(row[col])
        if month is None:
            continue
        found.append((col, month))
        if len(found) == _N_MONTHS:
            break
    months = [month for _, month in found]
    if months != list(range(1, 13)):
        raise ValueError(f"month header is {months}, expected 1-12 in order")
    return found


def attach_day_counts(probability: pl.DataFrame, marginal_cost: pl.DataFrame, label: str) -> pl.DataFrame:
    """Join ``num_days`` from a page 1 or 2 matrix onto a page 3 matrix.

    Also checks that ``mc_total_per_kwh * num_days / probability_of_peak`` is
    one constant across the month-hours with a nonzero probability, and that
    the cost is zero wherever the probability is zero.
    """
    joined = probability.join(
        marginal_cost.select("month", "hour", "num_days", "mc_total_per_kwh"),
        on=["month", "hour"],
        how="inner",
        validate="1:1",
    )
    if joined.height != probability.height:
        raise ValueError(f"{label}: page 3 and the cost page cover different month-hours")

    zero_pop_cost = joined.filter((pl.col("probability_of_peak") == 0) & (pl.col("mc_total_per_kwh") != 0))
    if zero_pop_cost.height:
        raise ValueError(
            f"{label}: nonzero cost where the probability of peak is zero: "
            f"{zero_pop_cost.select('month', 'hour').rows()}"
        )
    ratios = joined.filter(pl.col("probability_of_peak") > 0).select(
        pl.col("mc_total_per_kwh") * pl.col("num_days") / pl.col("probability_of_peak")
    )
    ratio_min = ratios.min().item()
    ratio_max = ratios.max().item()
    if ratio_max - ratio_min > _RATIO_TOLERANCE * abs(ratio_max):
        raise ValueError(
            f"{label}: cost is not proportional to probability_of_peak / num_days "
            f"(ratio ranges {ratio_min} to {ratio_max})"
        )
    return joined.select("month", "hour", "num_days", "probability_of_peak")


def _probability_filename(kind: str) -> str:
    return f"month_hour_probability_of_peak_{kind}.parquet"


def write_probability_of_peak_parquets(path_xlsx: Path, path_output_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read pages 1-3 and write the weekday and weekend-and-holiday parquets."""
    _reject_just_placeholders(path_xlsx, path_output_dir)
    pop_weekday, pop_weekend = month_hour_probability_of_peak(read_sheet(path_xlsx, _PROBABILITY_SHEET))

    outputs: dict[Voltage, tuple[pl.DataFrame, pl.DataFrame]] = {}
    for voltage, sheet in _MARGINAL_COST_SHEETS.items():
        _, mc_weekday, mc_weekend = month_hour_marginal_costs(read_sheet(path_xlsx, sheet), voltage)
        outputs[voltage] = (
            attach_day_counts(pop_weekday, mc_weekday, f"{voltage} weekday"),
            attach_day_counts(pop_weekend, mc_weekend, f"{voltage} weekend"),
        )

    weekday, weekend = outputs["secondary"]
    primary_weekday, primary_weekend = outputs["primary"]
    if not (weekday.equals(primary_weekday) and weekend.equals(primary_weekend)):
        raise ValueError("pages 1 and 2 give different day counts")

    path_output_dir.mkdir(parents=True, exist_ok=True)
    weekday.write_parquet(path_output_dir / _probability_filename("weekday"))
    weekend.write_parquet(path_output_dir / _probability_filename("weekend_holidays"))
    return weekday, weekend


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def _print_system_load(load: pl.DataFrame, peaks: pl.DataFrame, path_output_dir: Path) -> None:
    print(f"Wrote {load.height:,} hours to {path_output_dir / _SYSTEM_HOURLY_FILENAME}")
    print(f"  load_mw min={load['load_mw'].min():.1f} max={load['load_mw'].max():.1f}")
    print(f"Wrote {peaks.height} monthly peaks to {path_output_dir / _SYSTEM_MONTHLY_FILENAME}")


def _print_rate_class(loads: dict[int, pl.DataFrame], path_output_dir: Path) -> None:
    for rate, load in loads.items():
        path = path_output_dir / rate_class_filename(rate)
        print(
            f"Wrote {load.height:,} hours to {path} "
            f"(load_kw min={load['load_kw'].min():.1f} max={load['load_kw'].max():.1f})"
        )


def _print_month_hour(
    frames: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame], voltage: Voltage, path_output_dir: Path
) -> None:
    for kind, frame in zip(("all_days", "weekday", "weekend_holidays"), frames, strict=True):
        print(
            f"Wrote {frame.height} rows to {path_output_dir / _month_hour_mc_filename(kind, voltage)} "
            f"(mc_total_per_kwh max={frame['mc_total_per_kwh'].max():.6f})"
        )


def _print_probability(weekday: pl.DataFrame, weekend: pl.DataFrame, path_output_dir: Path) -> None:
    for kind, frame in (("weekday", weekday), ("weekend_holidays", weekend)):
        print(
            f"Wrote {frame.height} rows to {path_output_dir / _probability_filename(kind)} "
            f"(probability_of_peak sum={frame['probability_of_peak'].sum():.6f})"
        )


def convert_all(path_local_xlsx: Path, path_local_parquet: Path) -> None:
    """Write every OCC-863 parquet from the three attachment workbooks."""
    _reject_just_placeholders(path_local_xlsx, path_local_parquet)
    path_attachment_2 = path_local_xlsx / _ATTACHMENT_2
    path_attachment_3 = path_local_xlsx / _ATTACHMENT_3
    path_attachment_4 = path_local_xlsx / _ATTACHMENT_4
    missing = [str(path) for path in (path_attachment_2, path_attachment_3, path_attachment_4) if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing OCC-863 workbooks: {missing}")

    load, peaks = write_occ863_load_parquets(path_attachment_3, path_local_parquet)
    _print_system_load(load, peaks, path_local_parquet)
    _print_rate_class(write_rate_class_loads(path_attachment_4, path_local_parquet), path_local_parquet)
    voltages: tuple[Voltage, ...] = ("secondary", "primary")
    for voltage in voltages:
        frames = write_month_hour_parquets(path_attachment_2, path_local_parquet, voltage)
        _print_month_hour(frames, voltage, path_local_parquet)
    weekday, weekend = write_probability_of_peak_parquets(path_attachment_2, path_local_parquet)
    _print_probability(weekday, weekend, path_local_parquet)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_attachment_command(name: str, help_text: str, workbook_help: str) -> argparse.ArgumentParser:
        command = sub.add_parser(name, help=help_text)
        command.add_argument("--path-xlsx", type=Path, required=True, help=workbook_help)
        command.add_argument("--path-local-parquet", type=Path, required=True, help="Directory for output parquets")
        return command

    add_attachment_command(
        "system-load",
        "Attachment 3 -> system_load.parquet and system_monthly_peak_mw.parquet",
        "OCC-863 Attachment 3 workbook",
    )
    add_attachment_command(
        "rate-class-load",
        "Attachment 4 -> rate_{1,5,7}_load.parquet",
        "OCC-863 Attachment 4 workbook",
    )
    month_hour = add_attachment_command(
        "month-hour-mc",
        "Attachment 2 page 1 or 2 -> month_hour_marginal_costs_* parquets",
        "OCC-863 Attachment 2 workbook",
    )
    month_hour.add_argument(
        "--voltage",
        choices=tuple(_MARGINAL_COST_SHEETS),
        required=True,
        help="secondary is Attachment 2 page 1; primary is page 2",
    )
    add_attachment_command(
        "probability-of-peak",
        "Attachment 2 page 3 -> month_hour_probability_of_peak_* parquets",
        "OCC-863 Attachment 2 workbook",
    )
    everything = sub.add_parser("all", help="Every output, both voltages, from the three attachment workbooks")
    everything.add_argument(
        "--path-local-xlsx",
        type=Path,
        required=True,
        help=f"Directory holding {_ATTACHMENT_2}, {_ATTACHMENT_3}, and {_ATTACHMENT_4}",
    )
    everything.add_argument("--path-local-parquet", type=Path, required=True, help="Directory for output parquets")

    args = parser.parse_args()
    if args.command == "all":
        convert_all(args.path_local_xlsx, args.path_local_parquet)
    elif args.command == "system-load":
        load, peaks = write_occ863_load_parquets(args.path_xlsx, args.path_local_parquet)
        _print_system_load(load, peaks, args.path_local_parquet)
    elif args.command == "rate-class-load":
        _print_rate_class(write_rate_class_loads(args.path_xlsx, args.path_local_parquet), args.path_local_parquet)
    elif args.command == "month-hour-mc":
        voltage: Voltage = args.voltage
        frames = write_month_hour_parquets(args.path_xlsx, args.path_local_parquet, voltage)
        _print_month_hour(frames, voltage, args.path_local_parquet)
    else:
        weekday, weekend = write_probability_of_peak_parquets(args.path_xlsx, args.path_local_parquet)
        _print_probability(weekday, weekend, args.path_local_parquet)


if __name__ == "__main__":
    main()

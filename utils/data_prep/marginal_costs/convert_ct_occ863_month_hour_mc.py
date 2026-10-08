"""Convert OCC-863 Attachment 2 into month-hour marginal costs.

Attachment 2 answers OCC-863 subpart (b) for PURA Docket 26-05-10. Page 1 is
secondary service and page 2 is primary. Each holds three month-by-hour
matrices of average marginal cost in $/kWh: all day types, weekdays, and
weekends plus public holidays. Each matrix has a "No Days" row giving the day
count behind that month.

``--voltage`` selects the sheet. For each voltage this module writes all three
matrices. It checks that the all-day-types day count equals the weekday count
plus the weekend-and-holiday count. Page 3 is the probability-of-peak table
those matrices are built from. A fourth sheet is a copy of page 3 with an
extra ``dist_mc`` column; it is not page 2.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal

import polars as pl

Voltage = Literal["primary", "secondary"]

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


def month_hour_marginal_costs(
    raw: pl.DataFrame, voltage: Voltage
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return the all-days, weekday, and weekend-and-holiday matrices.

    ``voltage`` is ``primary`` or ``secondary``. It selects the three matrix
    titles on that sheet. The caller reads the matching sheet.

    Each frame has ``month`` (1–12), ``hour`` (0–23), ``num_days`` (the "No
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
            raise ValueError(f"hour {hour} is outside 0–23 in {title!r}")
        for col, month in month_cols:
            mc = _as_number(row[col] if col < len(row) else None)
            if mc is None:
                raise ValueError(
                    f"missing {title!r} value at month {month} hour {hour}"
                )
            if mc < 0:
                raise ValueError(
                    f"negative {title!r} value at month {month} hour {hour}"
                )
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
        raise ValueError(f"month header is {months}, expected 1–12 in order")
    return found


def _day_counts(
    row: tuple[object, ...], month_cols: list[tuple[int, int]]
) -> dict[int, int]:
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
        raise ValueError(f"{title!r} does not cover months 1–12 and hours 0–23")
    varying = (
        frame.group_by("month")
        .agg(pl.col("num_days").n_unique())
        .filter(pl.col("num_days") != 1)
    )
    if varying.height:
        raise ValueError(f"{title!r} day count changes within a month")


def _check_day_counts_add(
    all_days: pl.DataFrame, weekday: pl.DataFrame, weekend: pl.DataFrame
) -> None:
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
        raise ValueError(
            "weekday plus weekend day counts do not match the all-days row: "
            f"{combined.rows()}"
        )


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


def _filename(kind: str, voltage: Voltage) -> str:
    return f"month_hour_marginal_costs_{kind}_{voltage}.parquet"


def write_month_hour_parquets(
    path_xlsx: Path, path_output_dir: Path, voltage: Voltage
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Read one Attachment 2 sheet and write its three parquets."""
    if "{{" in str(path_xlsx) or "{{" in str(path_output_dir):
        raise ValueError(
            f"path looks like an uninterpolated Just variable: {path_xlsx} {path_output_dir}"
        )
    raw = pl.read_excel(
        path_xlsx, sheet_name=_MARGINAL_COST_SHEETS[voltage], has_header=False
    )
    if not isinstance(raw, pl.DataFrame):
        raise TypeError(f"expected one sheet from Attachment 2 {voltage}")
    all_days, weekday, weekend = month_hour_marginal_costs(raw, voltage)
    path_output_dir.mkdir(parents=True, exist_ok=True)
    for kind, frame in (
        ("all_days", all_days),
        ("weekday", weekday),
        ("weekend_holidays", weekend),
    ):
        frame.write_parquet(path_output_dir / _filename(kind, voltage))
    return all_days, weekday, weekend


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path-xlsx", type=Path, required=True, help="OCC-863 Attachment 2 workbook"
    )
    parser.add_argument(
        "--path-output-dir",
        type=Path,
        required=True,
        help="Directory for the three month_hour_marginal_costs_* parquets",
    )
    parser.add_argument(
        "--voltage",
        choices=tuple(_MARGINAL_COST_SHEETS),
        required=True,
        help="secondary is Attachment 2 page 1; primary is page 2",
    )
    args = parser.parse_args()
    voltage: Voltage = args.voltage
    all_days, weekday, weekend = write_month_hour_parquets(
        args.path_xlsx, args.path_output_dir, voltage
    )
    for kind, frame in (
        ("all_days", all_days),
        ("weekday", weekday),
        ("weekend_holidays", weekend),
    ):
        print(
            f"Wrote {frame.height} rows to {args.path_output_dir / _filename(kind, voltage)} "
            f"(mc_total_per_kwh max={frame['mc_total_per_kwh'].max():.6f})"
        )


if __name__ == "__main__":
    main()

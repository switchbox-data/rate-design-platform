"""Convert Eversource CT OCC-733 Attachment 1 (PURA Docket 26-05-10) to parquet.

OCC-733 is Eversource's description of the Python calculation behind the
probability-of-peak table on OCC-863 Attachment 2 page 3. Attachment 1 is that
calculation's input: the ``Code_Input`` sheet, one row per hour for 2022-2025,
with gross distribution-substation load, the behind-the-meter PV the Company
subtracted ("Lost load PV", zero or negative), and net load.

Writes ``hourly_load.parquet`` with columns ``timestamp`` (hour-beginning, no
timezone), ``year``, ``month``, ``day``, ``hour``, ``weekday`` (Excel
``WEEKDAY``: 1 = Sunday ... 7 = Saturday), ``total_mw``, ``lost_load_pv_mw``,
and ``net_load_mw``.

The converter checks the header, the hours in each year, that timestamps are
unique, that ``DateTime`` rounds to the Year/Month/Day/Hour columns, that
``weekday`` matches the date, and that net load is total plus lost PV. It
raises before writing if any check fails.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import openpyxl
import polars as pl

# The sheet name on the workbook ends in a space.
_SHEET = "Code_Input "
_HEADER = (
    "DateTime",
    "Year",
    "Month",
    "Day",
    "Hour",
    "Weekday",
    "Total",
    "Lost load PV",
    "Net Load",
)
_EXPECTED_HOURS = {2022: 8760, 2023: 8760, 2024: 8784, 2025: 8760}
_NET_TOLERANCE_MW = 1e-6
_OUTPUT_FILENAME = "hourly_load.parquet"


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


def _as_int(value: object, excel_row: int, column: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not float(value).is_integer():
        raise ValueError(f"Excel row {excel_row}: {column} {value!r} is not an integer")
    return int(value)


def _as_float(value: object, excel_row: int, column: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Excel row {excel_row}: {column} {value!r} is not a number")
    return float(value)


def _round_to_hour(value: datetime) -> datetime:
    return (value + timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0)


def _excel_weekday(value: datetime) -> int:
    """Excel ``WEEKDAY`` with its default return type: 1 = Sunday ... 7 = Saturday."""
    return value.isoweekday() % 7 + 1


def read_code_input(path_xlsx: Path) -> list[tuple[object, ...]]:
    """The data rows of ``Code_Input``, columns A through I, header checked."""
    workbook = openpyxl.load_workbook(path_xlsx, data_only=True, read_only=True)
    try:
        rows = [tuple(row[: len(_HEADER)]) for row in workbook[_SHEET].iter_rows(values_only=True)]
    finally:
        workbook.close()
    if not rows:
        raise ValueError(f"{path_xlsx}: sheet {_SHEET!r} is empty")
    header = tuple(_cell_text(value) for value in rows[0])
    if header != _HEADER:
        raise ValueError(f"{path_xlsx}: header {header} does not match {_HEADER}")

    data = rows[1:]
    n_data = next((i for i, row in enumerate(data) if row[0] is None), len(data))
    trailing = [i + 2 for i, row in enumerate(data[n_data:], n_data) if any(v is not None for v in row)]
    if trailing:
        raise ValueError(f"{path_xlsx}: values below the first blank DateTime, Excel rows {trailing[:5]}")
    return data[:n_data]


def hourly_load(path_xlsx: Path) -> pl.DataFrame:
    """Attachment 1 ``Code_Input`` as one row per hour, checked."""
    rows = read_code_input(path_xlsx)
    records = []
    for excel_row, row in enumerate(rows, start=2):
        stamp = row[0]
        if not isinstance(stamp, datetime):
            raise ValueError(f"Excel row {excel_row}: DateTime {stamp!r} is not a datetime")
        year, month, day, hour, weekday = (
            _as_int(value, excel_row, column) for value, column in zip(row[1:6], _HEADER[1:6], strict=True)
        )
        total, lost_pv, net = (
            _as_float(value, excel_row, column) for value, column in zip(row[6:9], _HEADER[6:9], strict=True)
        )
        timestamp = datetime(year, month, day, hour)
        if _round_to_hour(stamp) != timestamp:
            raise ValueError(f"Excel row {excel_row}: DateTime {stamp} does not round to {timestamp}")
        if weekday != _excel_weekday(timestamp):
            raise ValueError(f"Excel row {excel_row}: Weekday {weekday} does not match {timestamp:%A}")
        records.append(
            {
                "timestamp": timestamp,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "weekday": weekday,
                "total_mw": total,
                "lost_load_pv_mw": lost_pv,
                "net_load_mw": net,
            }
        )
    df = pl.DataFrame(
        records,
        schema={
            "timestamp": pl.Datetime("us"),
            "year": pl.Int32,
            "month": pl.Int8,
            "day": pl.Int8,
            "hour": pl.Int8,
            "weekday": pl.Int8,
            "total_mw": pl.Float64,
            "lost_load_pv_mw": pl.Float64,
            "net_load_mw": pl.Float64,
        },
    )

    if df["timestamp"].n_unique() != df.height:
        raise ValueError("Code_Input has duplicate hours")
    counts = dict(df.group_by("year").len().iter_rows())
    if counts != _EXPECTED_HOURS:
        raise ValueError(f"hours per year {counts} do not match {_EXPECTED_HOURS}")
    if (df["lost_load_pv_mw"] > 0).any():
        raise ValueError("Lost load PV is positive in some hour; expected zero or negative")
    net_error = df.select((pl.col("total_mw") + pl.col("lost_load_pv_mw") - pl.col("net_load_mw")).abs().max()).item()
    if net_error > _NET_TOLERANCE_MW:
        raise ValueError(f"Net Load differs from Total + Lost load PV by up to {net_error} MW")
    return df.sort("timestamp")


def write_hourly_load(path_xlsx: Path, path_local_parquet: Path) -> pl.DataFrame:
    _reject_just_placeholders(path_xlsx, path_local_parquet)
    df = hourly_load(path_xlsx)
    path_local_parquet.mkdir(parents=True, exist_ok=True)
    path_out = path_local_parquet / _OUTPUT_FILENAME
    df.write_parquet(path_out)
    print(
        f"Wrote {df.height:,} hours to {path_out} "
        f"(total_mw max={df['total_mw'].max():.1f}, lost_load_pv_mw min={df['lost_load_pv_mw'].min():.1f})"
    )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--path-xlsx", type=Path, required=True, help="OCC-733 Attachment 1 workbook")
    parser.add_argument("--path-local-parquet", type=Path, required=True, help="Directory to write hourly_load.parquet")
    args = parser.parse_args()
    write_hourly_load(args.path_xlsx, args.path_local_parquet)


if __name__ == "__main__":
    main()

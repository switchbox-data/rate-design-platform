"""Convert OCC-863 Attachment 3 to hourly system load and monthly peaks.

Attachment 3 answers OCC-863 subpart (c) for PURA Docket 26-05-10. Page 2 is the
hourly sum of Eversource CT distribution-substation loads for 2022–2025, in
megawatts. Page 1 is the monthly maximum of that series.

Page 2 is the source for ``system_load.parquet``, not Attachment 1. The two
sheets agree on every hour except 1:00 a.m. on the three fall-back Sundays,
where Attachment 1 stores twice the page 2 value. Page 1's November maximum
matches page 2.

Excel stores a handful of timestamps a fraction of a second before the hour.
Those are rounded to the nearest hour. 2024 keeps its leap day (8784 hours).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

_HOURLY_SHEET = "OCC-863 Attachment 3 Page 2"
_MONTHLY_SHEET = "OCC-863 Attachment 3 Page 1"
_HOURLY_FILENAME = "system_load.parquet"
_MONTHLY_FILENAME = "system_monthly_peak_mw.parquet"
_EXPECTED_HOURS = {2022: 8760, 2023: 8760, 2024: 8784, 2025: 8760}


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
        pl.col("hourly_max").is_null()
        | ((pl.col("peak_mw") - pl.col("hourly_max")).abs() > 1e-6)
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
        raise ValueError(
            f"expected monthly peaks {sorted(expected)}, got {sorted(found)}"
        )
    if peaks.filter(pl.col("peak_mw") <= 0).height:
        raise ValueError("monthly peak has a non-positive MW value")


def write_occ863_load_parquets(
    path_xlsx: Path, path_output_dir: Path
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read Attachment 3 and write both parquets under ``path_output_dir``."""
    if "{{" in str(path_xlsx) or "{{" in str(path_output_dir):
        raise ValueError(
            f"path looks like an uninterpolated Just variable: {path_xlsx} {path_output_dir}"
        )
    raw_hourly = pl.read_excel(
        path_xlsx,
        sheet_name=_HOURLY_SHEET,
        columns=["DateTime", "Actual Load"],
    )
    raw_monthly = pl.read_excel(path_xlsx, sheet_name=_MONTHLY_SHEET, has_header=False)
    load = hourly_system_load(raw_hourly)
    peaks = monthly_peak_mw(raw_monthly)
    assert_monthly_peaks_match_hourly(load, peaks)

    path_output_dir.mkdir(parents=True, exist_ok=True)
    load.write_parquet(path_output_dir / _HOURLY_FILENAME)
    peaks.write_parquet(path_output_dir / _MONTHLY_FILENAME)
    return load, peaks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path-xlsx", type=Path, required=True, help="OCC-863 Attachment 3 workbook"
    )
    parser.add_argument(
        "--path-output-dir",
        type=Path,
        required=True,
        help="Directory for system_load.parquet and system_monthly_peak_mw.parquet",
    )
    args = parser.parse_args()
    load, peaks = write_occ863_load_parquets(args.path_xlsx, args.path_output_dir)
    print(f"Wrote {load.height:,} hours to {args.path_output_dir / _HOURLY_FILENAME}")
    print(f"  load_mw min={load['load_mw'].min():.1f} max={load['load_mw'].max():.1f}")
    print(
        f"Wrote {peaks.height} monthly peaks to {args.path_output_dir / _MONTHLY_FILENAME}"
    )


if __name__ == "__main__":
    main()

"""Convert OCC-863 Attachment 4 to hourly rate-class load parquets.

Attachment 4 answers OCC-863 subpart (d) for PURA Docket 26-05-10: 2025 hourly
load for Eversource CT residential Rates 1, 5, and 7. The sheet labels each
stamp "Interval Ending EST". The hour that stamp closes is the previous hour,
so 2025-01-01 01:00:00 becomes a hour-beginning timestamp of 2025-01-01 00:00:00.
The last stamp, 2026-01-01 00:00:00, becomes 2025-12-31 23:00:00.

The series is already 8760 hours in standard time. Spring-forward and fall-back
days each have 24 stamps, so the conversion is a one-hour shift and not a
timezone conversion.

Title rows, the Peak Demand and Total Usage summaries, and the footer check
are dropped. Those summaries are checked against the hourly column before
they are dropped.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

_SHEET = "OCC-863 Attachment 4"
_RATES = (1, 5, 7)


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
    labels = [
        None if value is None else str(value)
        for value in raw.get_column(label_col).to_list()
    ]
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
    return load


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
    steps = load.select(
        pl.col("timestamp").diff().dt.total_hours().alias("dh")
    ).drop_nulls()
    if steps.filter(pl.col("dh") != 1).height:
        raise ValueError("rate-class timestamps are not one hour apart")

    hourly_peak = load.select(pl.col("load_kw").max()).item()
    hourly_sum = load.select(pl.col("load_kw").sum()).item()
    if not isinstance(hourly_peak, (int, float)) or not isinstance(
        hourly_sum, (int, float)
    ):
        raise TypeError("rate-class load did not aggregate to a number")
    if abs(hourly_peak - peak_kw) > 0.05:
        raise ValueError(
            f"hourly peak {hourly_peak} does not match Peak Demand {peak_kw}"
        )
    if abs(hourly_sum - usage_kwh) > 1.0:
        raise ValueError(
            f"hourly sum {hourly_sum} does not match Total Usage {usage_kwh}"
        )


def _label_index(labels: list[str | None], label: str) -> int:
    try:
        return labels.index(label)
    except ValueError as exc:
        raise ValueError(f"Attachment 4 has no {label!r} row") from exc


def write_rate_class_loads(
    path_xlsx: Path, path_output_dir: Path
) -> dict[int, pl.DataFrame]:
    """Read Attachment 4 and write one parquet per rate under ``path_output_dir``."""
    if "{{" in str(path_xlsx) or "{{" in str(path_output_dir):
        raise ValueError(
            f"path looks like an uninterpolated Just variable: {path_xlsx} {path_output_dir}"
        )
    raw = pl.read_excel(
        path_xlsx,
        sheet_name=_SHEET,
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path-xlsx", type=Path, required=True, help="OCC-863 Attachment 4 workbook"
    )
    parser.add_argument(
        "--path-output-dir",
        type=Path,
        required=True,
        help="Directory for rate_1_load.parquet, rate_5_load.parquet, and rate_7_load.parquet",
    )
    args = parser.parse_args()
    loads = write_rate_class_loads(args.path_xlsx, args.path_output_dir)
    for rate, load in loads.items():
        path = args.path_output_dir / rate_class_filename(rate)
        print(
            f"Wrote {load.height:,} hours to {path} "
            f"(load_kw min={load['load_kw'].min():.1f} max={load['load_kw'].max():.1f})"
        )


if __name__ == "__main__":
    main()

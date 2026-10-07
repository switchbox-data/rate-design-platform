"""OCC-863 Attachment 4 → hourly rate-class loads. No S3.

The real workbook is not in git. These tests build a one-year stand-in with the
title block, a footer, and interval-ending stamps.
"""

from __future__ import annotations

import polars as pl
import pytest

from utils.data_prep.marginal_costs.convert_ct_occ863_rate_class_load import (
    rate_class_loads,
)


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
    for stamp, a, b, c in zip(
        stamps.to_list(), rate1.to_list(), rate5.to_list(), rate7.to_list(), strict=True
    ):
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
    assert rate1.height == 8760
    assert (
        rate1["timestamp"][0]
        == pl.select(pl.lit("2025-01-01 00:00:00").str.to_datetime()).item()
    )
    assert (
        rate1["timestamp"][-1]
        == pl.select(pl.lit("2025-12-31 23:00:00").str.to_datetime()).item()
    )
    first = rate1.row(0, named=True)
    assert first["load_kw"] == 1000.0
    assert first["load_mw"] == pytest.approx(1.0)


def test_rejects_a_peak_that_does_not_match_the_hourly_column() -> None:
    raw = _sheet().with_columns(
        pl.when(pl.col("column_1") == "Peak Demand")
        .then(pl.lit("1"))
        .otherwise(pl.col("column_2"))
        .alias("column_2")
    )
    with pytest.raises(ValueError, match="Peak Demand"):
        rate_class_loads(raw)

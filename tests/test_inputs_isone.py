"""Tests for the ISO-NE zone-load reader."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.data_prep.marginal_costs.inputs import InputContext
from utils.data_prep.marginal_costs.inputs.isone import (
    _load_single_zone,
    isone_zone_load,
    parse_history_years,
)
from utils.data_prep.marginal_costs.supply_utils import build_cairo_8760_timestamps


def _write_zone(base: Path, zone: str, year: int, frame: pl.DataFrame) -> None:
    directory = base / f"zone={zone}" / f"year={year}"
    directory.mkdir(parents=True)
    frame.write_parquet(directory / "data.parquet")


def _zone_frame(year: int) -> pl.DataFrame:
    clock = build_cairo_8760_timestamps(year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    return clock.with_columns(pl.int_range(0, clock.height).cast(pl.Float64).alias("load_mw")).rename(
        {"timestamp": "interval_start_et"}
    )


def test_parse_history_years() -> None:
    assert parse_history_years("2022-2025") == (2022, 2023, 2024, 2025)
    assert parse_history_years("2024,2022") == (2022, 2024)
    with pytest.raises(ValueError, match="end before start"):
        parse_history_years("2025-2022")


def test_single_zone_averages_fallback_and_interpolates_spring(tmp_path: Path) -> None:
    year = 2025
    frame = _zone_frame(year)
    duplicated = frame.slice(100, 1).with_columns(pl.lit(0.0).alias("load_mw"))
    dropped_ts = frame["interval_start_et"][200]
    edited = pl.concat([frame, duplicated]).filter(pl.col("interval_start_et") != dropped_ts)
    _write_zone(tmp_path, "RI", year, edited)

    ref = build_cairo_8760_timestamps(year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    loaded = _load_single_zone(str(tmp_path) + "/", "RI", year, ref, {})

    fallback = loaded.filter(pl.col("timestamp") == frame["interval_start_et"][100])
    assert fallback["load_mw"].item() == pytest.approx(50.0)
    gap = loaded.filter(pl.col("timestamp") == dropped_ts)
    assert gap["load_mw"].item() == pytest.approx(200.0)
    assert loaded.height == 8760


def test_single_zone_rejects_a_missing_month(tmp_path: Path) -> None:
    year = 2025
    frame = _zone_frame(year).filter(pl.col("interval_start_et").dt.month() < 12)
    _write_zone(tmp_path, "RI", year, frame)
    ref = build_cairo_8760_timestamps(year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))

    with pytest.raises(ValueError, match="missing months"):
        _load_single_zone(str(tmp_path) + "/", "RI", year, ref, {})


def test_history_keeps_each_year(tmp_path: Path) -> None:
    for year in (2024, 2025):
        _write_zone(tmp_path, "RI", year, _zone_frame(year))
    ctx = InputContext(utility="ri_rie", year=2025, load_year=2025, storage_options={})

    resolved = isone_zone_load(
        {"path": str(tmp_path), "zones": ["RI"], "history_years": "2024-2025"},
        ctx,
        "load",
    )

    assert resolved.value.height == 8760 * 2
    assert resolved.value["timestamp"].dt.year().unique().sort().to_list() == [2024, 2025]
    assert resolved.meta["history_years"] == [2024, 2025]
    assert resolved.meta["allocation_load"] == "utility_zone"
    assert len(resolved.inputs) == 2

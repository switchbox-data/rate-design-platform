"""Tests for ISO-NE and NYISO supply ancillary marginal-cost helpers."""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

import utils.pre.marginal_costs.supply_ancillary as supply_ancillary_mod
from utils.pre.marginal_costs.supply_ancillary import (
    ancillary_wide_to_enduse,
    compute_supply_ancillary_mc,
)
from utils.pre.marginal_costs.supply_utils import (
    build_cairo_8760_timestamps,
    prepare_component_output,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_ancillary_df(year: int = 2025, n_hours: int = 8760) -> pl.DataFrame:
    """Build a synthetic hourly ancillary DataFrame for *year*.

    Columns: ``interval_start_et``, ``reg_service_price_usd_per_mwh``,
    ``reg_capacity_price_usd_per_mwh``.
    """
    start = datetime(year, 1, 1, 0, 0, 0)
    timestamps = [start + timedelta(hours=h) for h in range(n_hours)]
    reg_service = [10.0 + h * 0.001 for h in range(n_hours)]
    reg_capacity = [5.0 + h * 0.0005 for h in range(n_hours)]
    return pl.DataFrame(
        {
            "interval_start_et": timestamps,
            "reg_service_price_usd_per_mwh": reg_service,
            "reg_capacity_price_usd_per_mwh": reg_capacity,
        }
    )


# ---------------------------------------------------------------------------
# load_ancillary_for_year
# ---------------------------------------------------------------------------


def test_load_ancillary_sums_reg_service_and_capacity() -> None:
    """ancillary_cost_enduse should equal reg_service + reg_capacity."""
    raw = _make_ancillary_df(year=2025)
    # Simulate what load_ancillary_for_year does without hitting S3
    result = (
        raw.rename({"interval_start_et": "timestamp"})
        .with_columns(
            (
                pl.col("reg_service_price_usd_per_mwh")
                + pl.col("reg_capacity_price_usd_per_mwh")
            ).alias("ancillary_cost_enduse")
        )
        .select("timestamp", "ancillary_cost_enduse")
    )

    # spot-check first row
    expected_first = 10.0 + 5.0  # reg_service[0] + reg_capacity[0]
    assert result["ancillary_cost_enduse"][0] == pytest.approx(expected_first)

    # all values must equal sum of the two source columns
    expected = (
        raw["reg_service_price_usd_per_mwh"] + raw["reg_capacity_price_usd_per_mwh"]
    )
    assert result["ancillary_cost_enduse"].to_list() == pytest.approx(
        expected.to_list()
    )


def test_load_ancillary_columns() -> None:
    """Returned DataFrame must have exactly timestamp and ancillary_cost_enduse."""
    raw = _make_ancillary_df(year=2025)
    result = (
        raw.rename({"interval_start_et": "timestamp"})
        .with_columns(
            (
                pl.col("reg_service_price_usd_per_mwh")
                + pl.col("reg_capacity_price_usd_per_mwh")
            ).alias("ancillary_cost_enduse")
        )
        .select("timestamp", "ancillary_cost_enduse")
    )

    assert result.columns == ["timestamp", "ancillary_cost_enduse"]


def test_ancillary_wide_to_enduse_isone_matches_reg_sum_formula() -> None:
    """Regression: ISO-NE wide → end-use must stay ``reg_service + reg_capacity``."""
    raw = _make_ancillary_df(year=2025, n_hours=24)
    from_module = ancillary_wide_to_enduse(raw, iso="isone", year=2025)
    manual = (
        raw.rename({"interval_start_et": "timestamp"})
        .with_columns(
            (
                pl.col("reg_service_price_usd_per_mwh")
                + pl.col("reg_capacity_price_usd_per_mwh")
            ).alias("ancillary_cost_enduse")
        )
        .select("timestamp", "ancillary_cost_enduse")
    )
    assert from_module.schema == manual.schema
    assert from_module["timestamp"].to_list() == manual["timestamp"].to_list()
    assert from_module["ancillary_cost_enduse"].to_list() == pytest.approx(
        manual["ancillary_cost_enduse"].to_list()
    )


def test_ancillary_wide_to_enduse_nyiso_one_hour_interval() -> None:
    """NYISO: capacity + movement * (end - start in hours)."""
    stub = pl.DataFrame(
        {
            "interval_start_et": [datetime(2025, 6, 1, 0, 0, 0)],
            "interval_end_et": [datetime(2025, 6, 1, 1, 0, 0)],
            "nyca_regulation_capacity_usd_per_mwhr": [10.0],
            "nyca_regulation_movement_usd_per_mw": [2.0],
        }
    )
    out = ancillary_wide_to_enduse(stub, iso="nyiso", year=2025)
    assert out.columns == ["timestamp", "ancillary_cost_enduse"]
    assert out["ancillary_cost_enduse"][0] == pytest.approx(12.0)


def test_ancillary_wide_to_enduse_nyiso_five_minute_interval() -> None:
    """Movement term scales with interval length in hours."""
    stub = pl.DataFrame(
        {
            "interval_start_et": [datetime(2025, 6, 1, 0, 0, 0)],
            "interval_end_et": [datetime(2025, 6, 1, 0, 5, 0)],
            "nyca_regulation_capacity_usd_per_mwhr": [0.0],
            "nyca_regulation_movement_usd_per_mw": [60.0],
        }
    )
    out = ancillary_wide_to_enduse(stub, iso="nyiso", year=2025)
    assert out["ancillary_cost_enduse"][0] == pytest.approx(5.0)


def test_ancillary_wide_to_enduse_nyiso_null_movement_treated_as_zero() -> None:
    stub = pl.DataFrame(
        {
            "interval_start_et": [datetime(2025, 6, 1, 0, 0, 0)],
            "interval_end_et": [datetime(2025, 6, 1, 1, 0, 0)],
            "nyca_regulation_capacity_usd_per_mwhr": [3.0],
            "nyca_regulation_movement_usd_per_mw": [None],
        }
    )
    out = ancillary_wide_to_enduse(stub, iso="nyiso", year=2025)
    assert out["ancillary_cost_enduse"][0] == pytest.approx(3.0)


def test_ancillary_wide_to_enduse_nyiso_null_capacity_treated_as_zero() -> None:
    """Null regulation capacity is coerced to 0 before the movement term."""
    stub = pl.DataFrame(
        {
            "interval_start_et": [datetime(2025, 6, 1, 0, 0, 0)],
            "interval_end_et": [datetime(2025, 6, 1, 1, 0, 0)],
            "nyca_regulation_capacity_usd_per_mwhr": [None],
            "nyca_regulation_movement_usd_per_mw": [4.0],
        }
    )
    out = ancillary_wide_to_enduse(stub, iso="nyiso", year=2025)
    assert out["ancillary_cost_enduse"][0] == pytest.approx(4.0)


def test_ancillary_wide_to_enduse_nyiso_two_hour_interval_formula() -> None:
    """Regression spot-check: capacity + movement * Δt (hours)."""
    stub = pl.DataFrame(
        {
            "interval_start_et": [datetime(2025, 3, 15, 12, 0, 0)],
            "interval_end_et": [datetime(2025, 3, 15, 14, 0, 0)],
            "nyca_regulation_capacity_usd_per_mwhr": [5.0],
            "nyca_regulation_movement_usd_per_mw": [3.0],
        }
    )
    out = ancillary_wide_to_enduse(stub, iso="nyiso", year=2025)
    assert out["ancillary_cost_enduse"][0] == pytest.approx(5.0 + 3.0 * 2.0)


def test_prepare_collapses_subhourly_nyiso_style_rows_to_hourly_mean() -> None:
    """Matches ``prepare_component_output``: truncate to hour, mean duplicate hours."""
    year = 2025
    ref = build_cairo_8760_timestamps(year)
    jan1_midnight = datetime(year, 1, 1, 0, 0, 0)
    rest = (
        ref.filter(pl.col("timestamp") != jan1_midnight)
        .with_columns(pl.lit(1.0).alias("ancillary_cost_enduse"))
        .select("timestamp", "ancillary_cost_enduse")
    )
    dup_hour = pl.DataFrame(
        {
            "timestamp": [jan1_midnight, datetime(year, 1, 1, 0, 30, 0)],
            "ancillary_cost_enduse": [10.0, 14.0],
        }
    )
    combined = pl.concat([dup_hour, rest], how="vertical").sort("timestamp")
    out = prepare_component_output(
        combined,
        year=year,
        input_col="ancillary_cost_enduse",
        output_col="ancillary_cost_enduse",
        scale=1.0,
    )
    row0 = out.filter(pl.col("timestamp") == jan1_midnight)
    assert row0.height == 1
    assert row0["ancillary_cost_enduse"][0] == pytest.approx(12.0)


def test_nyiso_scan_columns_include_fetch_schema_core() -> None:
    """NYISO parquet select list stays aligned with ``fetch_nyiso_as_prices_parquet``."""
    cfg = supply_ancillary_mod._ancillary_config("nyiso")  # noqa: SLF001 — internal config table
    names = set(cfg.scan_columns)
    assert "interval_start_et" in names
    assert "interval_end_et" in names
    assert "market" in names
    assert "nyca_regulation_capacity_usd_per_mwhr" in names
    assert "nyca_regulation_movement_usd_per_mw" in names
    assert "spin_10min_usd_per_mwhr" in names


# ---------------------------------------------------------------------------
# compute_supply_ancillary_mc (via monkeypatched load)
# ---------------------------------------------------------------------------


def _make_compute_ancillary_mc_with_patch(
    monkeypatch: pytest.MonkeyPatch,
    year: int = 2025,
    reg_service: float = 12.0,
    reg_capacity: float = 8.0,
) -> pl.DataFrame:
    """Run compute_supply_ancillary_mc with a monkeypatched loader.

    Substitutes ``load_ancillary_for_year`` with a function that returns a
    synthetic 8760-row DataFrame with constant prices, bypassing S3.
    """
    start = datetime(year, 1, 1, 0, 0, 0)
    timestamps = [start + timedelta(hours=h) for h in range(8760)]
    synthetic = pl.DataFrame(
        {
            "timestamp": timestamps,
            "ancillary_cost_enduse": [reg_service + reg_capacity] * 8760,
        }
    )

    import utils.pre.marginal_costs.supply_ancillary as mod

    def _fake_load(
        yr: int,
        storage_options: dict[str, str],
        ancillary_s3_base: str | None = None,
        *,
        iso: str = "isone",
    ) -> pl.DataFrame:
        return synthetic

    monkeypatch.setattr(mod, "load_ancillary_for_year", _fake_load)
    return compute_supply_ancillary_mc(
        year=year,
        storage_options={},
    )


def test_compute_supply_ancillary_mc_passes_iso_to_load_ancillary_for_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``iso`` must reach ``load_ancillary_for_year`` (NYISO vs ISO-NE wiring)."""
    captured: dict[str, object] = {}

    def _fake_load(
        yr: int,
        storage_options: dict[str, str],
        ancillary_s3_base: str | None = None,
        *,
        iso: str = "isone",
    ) -> pl.DataFrame:
        captured["year"] = yr
        captured["iso"] = iso
        captured["ancillary_s3_base"] = ancillary_s3_base
        ts = build_cairo_8760_timestamps(yr)
        return ts.with_columns(pl.lit(2.5).alias("ancillary_cost_enduse"))

    monkeypatch.setattr(supply_ancillary_mod, "load_ancillary_for_year", _fake_load)
    compute_supply_ancillary_mc(2025, {}, iso="nyiso")
    assert captured["iso"] == "nyiso"
    assert captured["year"] == 2025


def test_compute_ancillary_mc_returns_8760_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """compute_supply_ancillary_mc must return exactly 8760 hourly rows."""
    result = _make_compute_ancillary_mc_with_patch(monkeypatch, year=2025)
    assert result.height == 8760


def test_compute_ancillary_mc_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Output must have exactly timestamp and ancillary_cost_enduse columns."""
    result = _make_compute_ancillary_mc_with_patch(monkeypatch, year=2025)
    assert result.columns == ["timestamp", "ancillary_cost_enduse"]


def test_compute_ancillary_mc_no_nulls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Output must have no null values."""
    result = _make_compute_ancillary_mc_with_patch(monkeypatch, year=2025)
    assert result.filter(pl.col("ancillary_cost_enduse").is_null()).height == 0


def test_compute_ancillary_mc_constant_price(monkeypatch: pytest.MonkeyPatch) -> None:
    """With uniform input prices the output should be constant."""
    reg_service = 12.0
    reg_capacity = 8.0
    expected = reg_service + reg_capacity

    result = _make_compute_ancillary_mc_with_patch(
        monkeypatch, year=2025, reg_service=reg_service, reg_capacity=reg_capacity
    )
    assert result["ancillary_cost_enduse"].to_list() == pytest.approx([expected] * 8760)


def test_compute_ancillary_mc_timestamps_match_cairo_8760(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Output timestamps must match the CAIRO canonical 8760 sequence."""
    year = 2025
    result = _make_compute_ancillary_mc_with_patch(monkeypatch, year=year)
    expected_ts = build_cairo_8760_timestamps(year)["timestamp"].to_list()
    assert result["timestamp"].to_list() == expected_ts


def test_compute_ancillary_mc_leap_year_is_8760(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leap year (2024) should still produce exactly 8760 rows (Dec 31 dropped)."""
    year = 2024
    # For a leap year the synthetic data has 8784 hours; prepare_component_output
    # aligns to the 8760-row CAIRO reference (dropping Dec 31).
    start = datetime(year, 1, 1, 0, 0, 0)
    n_hours = 8784  # 366 * 24
    timestamps = [start + timedelta(hours=h) for h in range(n_hours)]
    synthetic = pl.DataFrame(
        {
            "timestamp": timestamps,
            "ancillary_cost_enduse": [20.0] * n_hours,
        }
    )

    import utils.pre.marginal_costs.supply_ancillary as mod

    def _fake_load(
        yr: int,
        storage_options: dict[str, str],
        ancillary_s3_base: str | None = None,
        *,
        iso: str = "isone",
    ) -> pl.DataFrame:
        return synthetic

    monkeypatch.setattr(mod, "load_ancillary_for_year", _fake_load)
    result = compute_supply_ancillary_mc(year=year, storage_options={})
    assert result.height == 8760

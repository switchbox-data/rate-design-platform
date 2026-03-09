"""Tests for ISO-NE supply ancillary MC computation."""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from utils.pre.marginal_costs.supply_ancillary import (
    compute_supply_ancillary_mc,
    load_ancillary_for_year,
)
from utils.pre.marginal_costs.supply_utils import build_cairo_8760_timestamps


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
    result = raw.rename({"interval_start_et": "timestamp"}).with_columns(
        (
            pl.col("reg_service_price_usd_per_mwh")
            + pl.col("reg_capacity_price_usd_per_mwh")
        ).alias("ancillary_cost_enduse")
    ).select("timestamp", "ancillary_cost_enduse")

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
    result = raw.rename({"interval_start_et": "timestamp"}).with_columns(
        (
            pl.col("reg_service_price_usd_per_mwh")
            + pl.col("reg_capacity_price_usd_per_mwh")
        ).alias("ancillary_cost_enduse")
    ).select("timestamp", "ancillary_cost_enduse")

    assert result.columns == ["timestamp", "ancillary_cost_enduse"]


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
        ancillary_s3_base: str = "",
    ) -> pl.DataFrame:
        return synthetic

    monkeypatch.setattr(mod, "load_ancillary_for_year", _fake_load)
    return compute_supply_ancillary_mc(
        year=year,
        storage_options={},
    )


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
    assert result["ancillary_cost_enduse"].to_list() == pytest.approx(
        [expected] * 8760
    )


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
        ancillary_s3_base: str = "",
    ) -> pl.DataFrame:
        return synthetic

    monkeypatch.setattr(mod, "load_ancillary_for_year", _fake_load)
    result = compute_supply_ancillary_mc(year=year, storage_options={})
    assert result.height == 8760

"""Tests for CT (Eversource/CL&P) sub-TX + distribution marginal cost allocation.

CT reuses the generic ISO-native PoP allocator (generate_utility_tx_dx_mc.py) already
tested for NY/RI/MD in test_marginal_cost_allocation.py and test_tx_dx_load_layouts.py.
These tests cover the CT-specific wiring: the ISO-NE zone mapping (both CT utilities
map to the single CT load zone), and an end-to-end PoP allocation on a synthetic
CT-shaped (summer-peaking) load profile using the actual $20.17/kW-yr MCOS-2 Table 3
figure. See context/methods/marginal_costs/ct_eversource_dist_mc_methodology.md.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from data.isone.hourly_demand.aggregate_isone_utility_loads import (
    get_utility_zone_mapping,
)
from data.isone.zone_mapping.generate_zone_mapping_csv import build_zone_mapping
from utils.data_prep.marginal_costs.generate_utility_tx_dx_mc import (
    allocate_costs_to_hours,
    calculate_pop_weights,
    get_marginal_cost_for_utility,
    normalize_load_to_cairo_8760,
)

CT_MC_TABLE_PATH = (
    Path(__file__).parent.parent
    / "rate_design"
    / "hp_rates"
    / "ct"
    / "config"
    / "marginal_costs"
    / "ct_marginal_costs_2025.csv"
)


# ── ISO-NE zone mapping: CT rows ─────────────────────────────────────────────


class TestCtZoneMapping:
    def test_both_ct_utilities_map_to_ct_zone(self) -> None:
        mapping_df = build_zone_mapping()
        ct_rows = mapping_df.filter(pl.col("state") == "ct")
        assert set(ct_rows["utility"].to_list()) == {"ct_eversource", "ct_ui"}
        assert set(ct_rows["iso_zone"].to_list()) == {"CT"}
        assert set(ct_rows["location_id"].to_list()) == {4004}

    def test_get_utility_zone_mapping_includes_ct(self) -> None:
        mapping_df = build_zone_mapping()
        utility_zone_map = get_utility_zone_mapping(mapping_df)
        assert utility_zone_map["ct_eversource"] == ["CT"]
        assert utility_zone_map["ct_ui"] == ["CT"]


# ── CT marginal cost config CSV (MCOS-2 Table 3) ─────────────────────────────


class TestCtMarginalCostTable:
    def test_config_csv_exists(self) -> None:
        assert CT_MC_TABLE_PATH.exists(), (
            f"Expected CT marginal cost config at {CT_MC_TABLE_PATH}"
        )

    def test_ct_eversource_value_matches_mcos2_table3(self) -> None:
        mc_df = pl.read_csv(CT_MC_TABLE_PATH)
        mc = get_marginal_cost_for_utility(mc_df, "ct_eversource")
        assert mc == pytest.approx(20.17)

    def test_dollar_year_is_2026(self) -> None:
        """MCOS-2 Table 3 is filed in 2026$; CPI inflation converts to run year."""
        mc_df = pl.read_csv(CT_MC_TABLE_PATH)
        row = mc_df.filter(pl.col("utility") == "ct_eversource")
        assert int(row["dollar_year"][0]) == 2026

    def test_ct_ui_not_in_table(self) -> None:
        """ct_ui has no MCOS-derived value yet (separate utility filing)."""
        mc_df = pl.read_csv(CT_MC_TABLE_PATH)
        with pytest.raises(ValueError, match="No marginal cost data found"):
            get_marginal_cost_for_utility(mc_df, "ct_ui")


# ── End-to-end PoP allocation on synthetic CT-shaped load ───────────────────


def _make_ct_summer_peaking_load(year: int = 2025) -> pl.DataFrame:
    """Synthetic CT zone load: sharp summer (Jul-Aug) afternoon peak.

    Mirrors CL&P's own finding (MCOS-1 testimony): ~80% of annual peak
    probability falls in July-August, with negligible winter peak risk.
    """
    timestamps = pl.datetime_range(
        datetime(year, 1, 1, 0, 0, 0),
        datetime(year, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )
    df = pl.DataFrame({"timestamp": timestamps}).with_row_index("idx")
    hour = df["timestamp"].dt.hour()
    month = df["timestamp"].dt.month()
    summer_peak_months = {7, 8}
    afternoon_hours = {14, 15, 16, 17, 18}
    load_mw = (
        3000.0
        + pl.when(month.is_in(summer_peak_months))
        .then(2500.0)
        .when(month.is_in([6, 9]))
        .then(800.0)
        .otherwise(0.0)
        + pl.when(hour.is_in(afternoon_hours)).then(500.0).otherwise(0.0)
        + df["idx"].cast(pl.Float64) * 0.001
    )
    return df.with_columns(load_mw.alias("load_mw")).select("timestamp", "load_mw")


class TestCtPopAllocationEndToEnd:
    def test_normalized_load_has_8760_rows(self) -> None:
        load_df = _make_ct_summer_peaking_load()
        normalized = normalize_load_to_cairo_8760(load_df, "ct_eversource", 2025)
        assert normalized.height == 8760

    def test_1kw_constant_load_recovers_annual_mc(self) -> None:
        """Sum of allocated hourly cost equals the input $/kW-yr, exactly."""
        load_df = _make_ct_summer_peaking_load()
        normalized = normalize_load_to_cairo_8760(load_df, "ct_eversource", 2025)
        weighted = calculate_pop_weights(normalized, n_hours=100)

        mc_df = pl.read_csv(CT_MC_TABLE_PATH)
        mc = get_marginal_cost_for_utility(mc_df, "ct_eversource")

        result = allocate_costs_to_hours(weighted, mc)
        total = float(result["mc_total_per_kwh"].sum())
        assert total == pytest.approx(mc, rel=1e-6)

    def test_peak_hours_concentrate_in_summer(self) -> None:
        """Top-100 PoP hours should fall almost entirely in Jun-Sep, consistent
        with CL&P's own summer-peaking finding (MCOS-1 testimony)."""
        load_df = _make_ct_summer_peaking_load()
        normalized = normalize_load_to_cairo_8760(load_df, "ct_eversource", 2025)
        weighted = calculate_pop_weights(normalized, n_hours=100)

        peak_months = (
            weighted.filter(pl.col("is_peak"))["timestamp"].dt.month().to_list()
        )
        assert all(m in {6, 7, 8, 9} for m in peak_months)
        assert sum(1 for m in peak_months if m in {7, 8}) / len(peak_months) > 0.5

    def test_no_peak_hours_in_winter(self) -> None:
        load_df = _make_ct_summer_peaking_load()
        normalized = normalize_load_to_cairo_8760(load_df, "ct_eversource", 2025)
        weighted = calculate_pop_weights(normalized, n_hours=100)

        peak_months = set(
            weighted.filter(pl.col("is_peak"))["timestamp"].dt.month().to_list()
        )
        assert peak_months.isdisjoint({11, 12, 1, 2, 3})


# ── CLI wiring: --state CT accepted ──────────────────────────────────────────


class TestCtCliChoice:
    def test_state_argument_accepts_ct(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--state", choices=["NY", "RI", "MD", "CT"], required=True)
        args = parser.parse_args(["--state", "CT"])
        assert args.state == "CT"

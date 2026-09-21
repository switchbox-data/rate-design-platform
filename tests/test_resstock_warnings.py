"""Tests for data/resstock/warnings.py — skip-step mismatch messages."""

from __future__ import annotations

from data.resstock.warnings import collect_run_warnings


def test_approx_warns_when_none_of_configured_upgrades_requested() -> None:
    warnings = collect_run_warnings(
        file_types=["metadata", "load_curve_hourly"],
        upgrade_ids=["0", "3"],
        approximate_non_hp_load=True,
        approx_upgrades=["01", "02"],
        adjust_mf_electricity=False,
        mf_adj_upgrades=["00", "02"],
        assign_utility=False,
        add_monthly_loads=False,
        add_annual_loads=False,
    )
    assert any("approx_upgrade_ids" in w for w in warnings)


def test_approx_does_not_warn_when_one_configured_upgrade_is_requested() -> None:
    warnings = collect_run_warnings(
        file_types=["metadata", "load_curve_hourly"],
        upgrade_ids=["1"],
        approximate_non_hp_load=True,
        approx_upgrades=["01", "02"],
        adjust_mf_electricity=False,
        mf_adj_upgrades=["00", "02"],
        assign_utility=False,
        add_monthly_loads=False,
        add_annual_loads=False,
    )
    assert not any("approx_upgrade_ids" in w for w in warnings)

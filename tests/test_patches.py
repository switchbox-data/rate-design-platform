# tests/test_patches.py
"""Tests for rate_design/ri/hp_rates/patches.py — CAIRO performance monkey-patches."""
from __future__ import annotations

import pandas as pd
import pytest
from pathlib import Path

LOAD_DIR = Path("/data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=RI/upgrade=00/")
SAMPLE_IDS = [100147, 100151, 100312]  # first 3 buildings from the upgrade dir
SOLAR_IDS = [8584, 85645, 121546]      # solar PV buildings (from RI upgrade=00)
ALL_IDS = SAMPLE_IDS + SOLAR_IDS


@pytest.fixture
def sample_filepaths():
    """Return {bldg_id: path} for ALL_IDS. Skip if data not accessible."""
    from utils.cairo import build_bldg_id_to_load_filepath
    if not LOAD_DIR.exists():
        pytest.skip("ResStock load dir not accessible")
    fps = build_bldg_id_to_load_filepath(
        path_resstock_loads=LOAD_DIR,
        building_ids=ALL_IDS,
    )
    if len(fps) < len(ALL_IDS):
        pytest.skip("Not all sample building IDs found")
    return fps


def test_combined_reader_matches_separate_reads(sample_filepaths):
    """_return_loads_combined returns same data as two separate _return_load calls."""
    from cairo.rates_tool.loads import _return_load
    from rate_design.ri.hp_rates.patches import _return_loads_combined

    target_year = 2025
    bldg_ids = list(sample_filepaths.keys())

    # reference: two separate reads
    ref_elec = _return_load(
        load_type="electricity",
        target_year=target_year,
        building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )
    ref_gas = _return_load(
        load_type="gas",
        target_year=target_year,
        building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )

    # patched: one combined read
    new_elec, new_gas = _return_loads_combined(
        target_year=target_year,
        building_ids=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )

    pd.testing.assert_frame_equal(
        new_elec.sort_index(),
        ref_elec.sort_index(),
        check_exact=False,
        rtol=1e-4,
        check_names=True,
    )
    pd.testing.assert_frame_equal(
        new_gas.sort_index(),
        ref_gas.sort_index(),
        check_exact=False,
        rtol=1e-4,
        check_names=True,
    )


def test_vectorized_aggregation_matches_cairo(sample_filepaths):
    """_vectorized_process_building_demand_by_period returns same agg_load as CAIRO."""
    from pathlib import Path
    from cairo.rates_tool.loads import _return_load, process_building_demand_by_period
    from cairo.rates_tool.tariffs import get_default_tariff_structures
    from rate_design.ri.hp_rates.patches import _vectorized_process_building_demand_by_period

    # Load the flat tariff used in run 1 — convert from URDB JSON to PySAM format,
    # matching how _initialize_tariffs prepares tariffs before calling
    # process_building_demand_by_period in the real run.
    tariff_path = Path(__file__).resolve().parent.parent / "rate_design/ri/hp_rates/config/tariffs/electric/rie_flat.json"
    if not tariff_path.exists():
        pytest.skip("rie_flat.json not found")

    tariff_base = get_default_tariff_structures(["rie_flat"], {"rie_flat": tariff_path})
    bldg_ids = list(sample_filepaths.keys())
    tariff_map = pd.DataFrame({"bldg_id": bldg_ids, "tariff_key": "rie_flat"})

    # Load electricity
    raw_load = _return_load(
        load_type="electricity",
        target_year=2025,
        building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )

    # Reference: CAIRO's version
    ref_agg_load, ref_agg_solar = process_building_demand_by_period(
        target_year=2025,
        load_col_key="total_fuel_electricity",
        prototype_ids=bldg_ids,
        tariff_base=tariff_base,
        tariff_map=tariff_map,
        prepassed_load=raw_load,
        solar_pv_compensation=None,
    )

    # Patched: vectorized version
    new_agg_load, new_agg_solar = _vectorized_process_building_demand_by_period(
        target_year=2025,
        load_col_key="total_fuel_electricity",
        prototype_ids=bldg_ids,
        tariff_base=tariff_base,
        tariff_map=tariff_map,
        prepassed_load=raw_load,
        solar_pv_compensation=None,
    )

    # Sort both for comparison (row order may differ)
    sort_cols = [c for c in ["bldg_id", "month", "period", "tier", "charge_type"] if c in ref_agg_load.reset_index().columns]
    ref_sorted = ref_agg_load.reset_index().sort_values(sort_cols).reset_index(drop=True)
    new_sorted = new_agg_load.reset_index().sort_values(sort_cols).reset_index(drop=True)

    # Compare numeric columns present in both
    numeric_cols = [c for c in ref_sorted.select_dtypes("number").columns if c in new_sorted.columns]
    for col in numeric_cols:
        pd.testing.assert_series_equal(
            ref_sorted[col].reset_index(drop=True),
            new_sorted[col].reset_index(drop=True),
            check_exact=False,
            rtol=1e-4,
            check_names=False,
        )


def test_vectorized_billing_matches_cairo(sample_filepaths):
    """_vectorized_run_system_revenues returns same bills as CAIRO."""
    from cairo.rates_tool import loads as _cairo_loads_orig
    from cairo.rates_tool.tariffs import get_default_tariff_structures
    from cairo.rates_tool.system_revenues import run_system_revenues
    from rate_design.ri.hp_rates.patches import _vectorized_run_system_revenues

    tariff_path = Path(__file__).resolve().parent.parent / "rate_design/ri/hp_rates/config/tariffs/electric/rie_flat.json"
    if not tariff_path.exists():
        pytest.skip("rie_flat.json not found")

    # Load tariff in PySAM format (same as the real run uses via _initialize_tariffs)
    tariff_base = get_default_tariff_structures(["rie_flat"], {"rie_flat": tariff_path})
    bldg_ids = list(sample_filepaths.keys())
    tariff_map = pd.DataFrame({"bldg_id": bldg_ids, "tariff_key": "rie_flat"})

    raw = _cairo_loads_orig._return_load(
        load_type="electricity", target_year=2025, building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths, force_tz="EST",
    )
    # Use the original (pre-patch) process_building_demand_by_period for clean agg_load
    from rate_design.ri.hp_rates.patches import _orig_process_building_demand_by_period
    agg_load, agg_solar = _orig_process_building_demand_by_period(
        target_year=2025, load_col_key="total_fuel_electricity",
        prototype_ids=bldg_ids, tariff_base=tariff_base,
        tariff_map=tariff_map, prepassed_load=raw, solar_pv_compensation=None,
    )

    ref_bills = run_system_revenues(
        aggregated_load=agg_load, aggregated_solar=agg_solar,
        solar_compensation_df=None, prototype_ids=bldg_ids,
        tariff_config=tariff_base, tariff_strategy=tariff_map,
    )
    new_bills = _vectorized_run_system_revenues(
        aggregated_load=agg_load, aggregated_solar=agg_solar,
        solar_compensation_df=None, prototype_ids=bldg_ids,
        tariff_config=tariff_base, tariff_strategy=tariff_map,
    )

    pd.testing.assert_frame_equal(
        ref_bills.sort_index(),
        new_bills.sort_index(),
        check_exact=False,
        rtol=1e-4,
    )

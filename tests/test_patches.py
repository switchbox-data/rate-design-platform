# tests/test_patches.py
"""Tests for rate_design/ri/hp_rates/patches.py — CAIRO performance monkey-patches."""
from __future__ import annotations

import pandas as pd
import pytest
from pathlib import Path

LOAD_DIR = Path("/data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=RI/upgrade=00/")
SAMPLE_IDS = [100147, 100151, 100312]  # first 3 buildings from the upgrade dir


@pytest.fixture
def sample_filepaths():
    """Return {bldg_id: path} for SAMPLE_IDS. Skip if data not accessible."""
    from utils.cairo import build_bldg_id_to_load_filepath
    if not LOAD_DIR.exists():
        pytest.skip("ResStock load dir not accessible")
    fps = build_bldg_id_to_load_filepath(
        path_resstock_loads=LOAD_DIR,
        building_ids=SAMPLE_IDS,
    )
    if len(fps) < len(SAMPLE_IDS):
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

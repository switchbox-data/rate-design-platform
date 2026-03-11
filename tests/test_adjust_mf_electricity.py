"""Tests for adjust_mf_electricity: MF non-HVAC column-by-column adjustment."""

from pathlib import Path
from typing import cast
from unittest.mock import patch

import polars as pl
import pytest

from utils.pre.adjust_mf_electricity import (
    ANNUAL_ELECTRICITY_COL,
    BUILDING_TYPE_RECS_COL,
    FLOOR_AREA_COL,
    HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL,
    HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL,
    MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL,
    NON_HVAC_RELATED_ELECTRICITY_COLS,
    annual_to_hourly_cols,
    adjust_mf_electricity_parquet,
    _adjust_mf_electricity_hourly_one_bldg,
    _get_non_hvac_mf_to_sf_ratios,
    _parse_floor_area_sqft,
)

BLDG_ID_COL = "bldg_id"


# ---- annual_to_hourly_cols ----


def test_annual_to_hourly_cols_valid():
    col = "out.electricity.ceiling_fan.energy_consumption.kwh"
    got = annual_to_hourly_cols(col)
    assert got == [
        "out.electricity.ceiling_fan.energy_consumption",
        "out.electricity.ceiling_fan.energy_consumption_intensity",
    ]


def test_annual_to_hourly_cols_empty_for_non_kwh():
    assert annual_to_hourly_cols("out.electricity.ceiling_fan.energy_consumption") == []
    assert annual_to_hourly_cols("other.column.name") == []


def test_annual_to_hourly_cols_all_non_hvac_mapped():
    for col in NON_HVAC_RELATED_ELECTRICITY_COLS:
        got = annual_to_hourly_cols(col)
        assert len(got) == 2
        assert got[0].endswith(".energy_consumption") and not got[0].endswith(".kwh")
        assert got[1].endswith(".energy_consumption_intensity")


# ---- _parse_floor_area_sqft ----


def test_parse_floor_area_sqft_plus_suffix():
    assert _parse_floor_area_sqft("4000+") == 5000.0
    assert _parse_floor_area_sqft("1000+") == 5000.0


def test_parse_floor_area_sqft_range():
    assert _parse_floor_area_sqft("750-999") == 874.5
    assert _parse_floor_area_sqft("0-500") == 250.0


def test_parse_floor_area_sqft_single_value():
    assert _parse_floor_area_sqft("1200") == 1200.0


def test_parse_floor_area_sqft_empty_or_none():
    import math

    assert math.isnan(_parse_floor_area_sqft(None))
    assert math.isnan(_parse_floor_area_sqft(""))
    assert math.isnan(_parse_floor_area_sqft("   "))


def test_parse_floor_area_sqft_invalid_returns_nan():
    import math

    assert math.isnan(_parse_floor_area_sqft("not-a-number"))


# ---- _get_non_hvac_mf_to_sf_ratios ----


def test_get_non_hvac_mf_to_sf_ratios_empty_when_metadata_missing_columns():
    lf_annual = pl.DataFrame(
        {
            BLDG_ID_COL: [1],
            "out.electricity.ceiling_fan.energy_consumption.kwh": [100.0],
        }
    ).lazy()
    meta = pl.DataFrame({BLDG_ID_COL: [1]}).lazy()  # no building type or floor area
    got = _get_non_hvac_mf_to_sf_ratios(lf_annual, meta)
    assert got == {}


def test_get_non_hvac_mf_to_sf_ratios_empty_when_no_non_hvac_in_annual():
    meta = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2],
            BUILDING_TYPE_RECS_COL: ["Single-Family Detached", "Multi-Family"],
            FLOOR_AREA_COL: ["1000-1499", "750-999"],
        }
    ).lazy()
    lf_annual = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2],
            ANNUAL_ELECTRICITY_COL: [5000.0, 3000.0],
        }
    ).lazy()
    got = _get_non_hvac_mf_to_sf_ratios(lf_annual, meta)
    assert got == {}


def test_get_non_hvac_mf_to_sf_ratios_returns_ratio_for_present_column():
    # SF: 2 bldgs, MF: 2 bldgs; one non-HVAC column. SF mean kWh/sqft vs MF mean kWh/sqft -> ratio
    meta = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2, 3, 4],
            BUILDING_TYPE_RECS_COL: [
                "Single-Family Detached",
                "Single-Family Detached",
                "Multi-Family",
                "Multi-Family",
            ],
            FLOOR_AREA_COL: [
                "1000-1499",
                "1000-1499",
                "750-999",
                "750-999",
            ],  # 1249.5, 874.5
        }
    ).lazy()
    # kWh/sqft: bldg 1: 100/1249.5, bldg 2: 200/1249.5 -> SF mean; bldg 3: 80/874.5, bldg 4: 160/874.5 -> MF mean
    ceiling_fan_col = "out.electricity.ceiling_fan.energy_consumption.kwh"
    lf_annual = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2, 3, 4],
            ceiling_fan_col: [100.0, 200.0, 80.0, 160.0],
        }
    ).lazy()
    got = _get_non_hvac_mf_to_sf_ratios(lf_annual, meta)
    assert ceiling_fan_col in got
    assert isinstance(got[ceiling_fan_col], float)
    assert got[ceiling_fan_col] > 0
    # SF mean (100+200)/2 / 1249.5 = 150/1249.5; MF mean (80+160)/2 / 874.5 = 120/874.5
    # ratio = MF/SF = (120/874.5) / (150/1249.5) ≈ 1.14
    assert 0.5 < got[ceiling_fan_col] < 2.0


def test_get_non_hvac_mf_to_sf_ratios_defaults_to_one_when_insufficient_samples():
    meta = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2],
            BUILDING_TYPE_RECS_COL: ["Single-Family Detached", "Multi-Family"],
            FLOOR_AREA_COL: ["1000-1499", "750-999"],
        }
    ).lazy()
    ceiling_fan_col = "out.electricity.ceiling_fan.energy_consumption.kwh"
    lf_annual = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2],
            ceiling_fan_col: [100.0, 200.0],
        }
    ).lazy()
    got = _get_non_hvac_mf_to_sf_ratios(lf_annual, meta)
    # Only 1 SF and 1 MF with non-zero -> len < 2 for at least one -> ratio 1.0
    assert got[ceiling_fan_col] == 1.0


# ---- _adjust_mf_electricity_hourly_one_bldg ----


def _minimal_hourly_df_one_non_hvac(
    consumption_col: str,
    intensity_col: str,
    total_consumption: float = 18.0,
    total_intensity: float = 0.018,
    non_hvac_val: float = 1.0,
    non_hvac_intensity_val: float = 0.001,
    n_rows: int = 3,
) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL: [total_consumption] * n_rows,
            HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL: [total_intensity] * n_rows,
            consumption_col: [non_hvac_val] * n_rows,
            intensity_col: [non_hvac_intensity_val] * n_rows,
        }
    ).lazy()


def test_adjust_mf_electricity_hourly_one_bldg_raises_when_missing_total_consumption():
    lf = pl.DataFrame(
        {
            HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL: [0.1],
            "out.electricity.ceiling_fan.energy_consumption": [1.0],
            "out.electricity.ceiling_fan.energy_consumption_intensity": [0.01],
        }
    ).lazy()
    with pytest.raises(ValueError, match="missing required column"):
        _adjust_mf_electricity_hourly_one_bldg(lf, {})


def test_adjust_mf_electricity_hourly_one_bldg_raises_when_missing_total_intensity():
    lf = pl.DataFrame(
        {
            HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL: [10.0],
            "out.electricity.ceiling_fan.energy_consumption": [1.0],
            "out.electricity.ceiling_fan.energy_consumption_intensity": [0.01],
        }
    ).lazy()
    with pytest.raises(ValueError, match="missing required column"):
        _adjust_mf_electricity_hourly_one_bldg(lf, {})


@patch(
    "utils.pre.adjust_mf_electricity.NON_HVAC_RELATED_ELECTRICITY_COLS",
    ("out.electricity.ceiling_fan.energy_consumption.kwh",),
)
def test_adjust_mf_electricity_hourly_one_bldg_scales_and_recomputes_total():
    consumption_col = "out.electricity.ceiling_fan.energy_consumption"
    intensity_col = "out.electricity.ceiling_fan.energy_consumption_intensity"
    annual_col = "out.electricity.ceiling_fan.energy_consumption.kwh"
    lf = _minimal_hourly_df_one_non_hvac(
        consumption_col,
        intensity_col,
        total_consumption=18.0,
        total_intensity=0.018,
        non_hvac_val=1.0,
        non_hvac_intensity_val=0.001,
        n_rows=2,
    )
    ratios = {annual_col: 2.0}
    out = _adjust_mf_electricity_hourly_one_bldg(lf, ratios)
    df = cast(pl.DataFrame, out.collect())
    assert df.get_column(consumption_col).to_list() == [0.5, 0.5]
    assert df.get_column(intensity_col).to_list() == [0.0005, 0.0005]
    # total = 18 - 1*2 + 0.5*2 = 18 - 2 + 1 = 17 per row (one non-HVAC column, two rows: 1 each)
    # Actually per row: old_non_hvac=1, adjusted=0.5, so new_total = 18 - 1 + 0.5 = 17.5
    assert df.get_column(HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL).to_list() == [
        17.5,
        17.5,
    ]
    # total intensity: 0.018 - 0.001 + 0.0005 = 0.0175 (use approx for float)
    intensity_list = df.get_column(HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL).to_list()
    assert intensity_list[0] == pytest.approx(0.0175)
    assert intensity_list[1] == pytest.approx(0.0175)


@patch(
    "utils.pre.adjust_mf_electricity.NON_HVAC_RELATED_ELECTRICITY_COLS",
    ("out.electricity.ceiling_fan.energy_consumption.kwh",),
)
def test_adjust_mf_electricity_hourly_one_bldg_ratio_one_leaves_unchanged():
    consumption_col = "out.electricity.ceiling_fan.energy_consumption"
    intensity_col = "out.electricity.ceiling_fan.energy_consumption_intensity"
    annual_col = "out.electricity.ceiling_fan.energy_consumption.kwh"
    lf = _minimal_hourly_df_one_non_hvac(consumption_col, intensity_col, n_rows=2)
    ratios = {annual_col: 1.0}
    out = _adjust_mf_electricity_hourly_one_bldg(lf, ratios)
    df = cast(pl.DataFrame, out.collect())
    assert df.get_column(consumption_col).to_list() == [1.0, 1.0]
    assert df.get_column(HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL).to_list() == [
        18.0,
        18.0,
    ]


# ---- adjust_mf_electricity_parquet (integration) ----


@patch(
    "utils.pre.adjust_mf_electricity.NON_HVAC_RELATED_ELECTRICITY_COLS",
    ("out.electricity.ceiling_fan.energy_consumption.kwh",),
)
def test_adjust_mf_electricity_parquet_no_op_when_no_unadjusted_mf(tmp_path: Path):
    """When no unadjusted multifamily bldg_ids exist, metadata is not sunk and no hourly files written."""
    meta_path = tmp_path / "metadata.parquet"
    meta = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2],
            BUILDING_TYPE_RECS_COL: [
                "Single-Family Detached",
                "Single-Family Attached",
            ],
            FLOOR_AREA_COL: ["1000-1499", "750-999"],
            MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL: [False, False],
        }
    )
    meta.write_parquet(meta_path)
    metadata_lf = pl.scan_parquet(str(meta_path))

    annual_path = tmp_path / "annual.parquet"
    ceiling_fan_col = "out.electricity.ceiling_fan.energy_consumption.kwh"
    annual = pl.DataFrame(
        {
            BLDG_ID_COL: [1, 2],
            ANNUAL_ELECTRICITY_COL: [5000.0, 4000.0],
            ceiling_fan_col: [100.0, 80.0],
        }
    )
    annual.write_parquet(annual_path)
    annual_lf = pl.scan_parquet(str(annual_path))

    hourly_dir = tmp_path / "hourly"
    hourly_dir.mkdir()
    # No hourly files; unadjusted_multifamily_bldg_ids will be empty (no MF in meta)

    adjust_mf_electricity_parquet(
        metadata=metadata_lf,
        input_load_curve_annual=annual_lf,
        load_curve_hourly_dir=hourly_dir,
        path_metadata=meta_path,
        upgrade_id="00",
        storage_options={},
    )
    # Metadata should be unchanged (no MF to adjust)
    assert meta_path.exists()
    read_meta = pl.read_parquet(meta_path)
    assert read_meta[MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL].to_list() == [False, False]


@patch(
    "utils.pre.adjust_mf_electricity.NON_HVAC_RELATED_ELECTRICITY_COLS",
    ("out.electricity.ceiling_fan.energy_consumption.kwh",),
)
def test_adjust_mf_electricity_parquet_runs_when_metadata_lacks_mf_column(
    tmp_path: Path,
):
    """When metadata does not have mf_non_hvac_electricity_adjusted, function adds it in memory and runs without error."""
    meta_path = tmp_path / "metadata.parquet"
    meta = pl.DataFrame(
        {
            BLDG_ID_COL: [1],
            BUILDING_TYPE_RECS_COL: ["Single-Family Detached"],
            FLOOR_AREA_COL: ["1000-1499"],
        }
    )
    meta.write_parquet(meta_path)
    metadata_lf = pl.scan_parquet(str(meta_path))

    annual_path = tmp_path / "annual.parquet"
    annual = pl.DataFrame(
        {
            BLDG_ID_COL: [1],
            ANNUAL_ELECTRICITY_COL: [5000.0],
            "out.electricity.ceiling_fan.energy_consumption.kwh": [100.0],
        }
    )
    annual.write_parquet(annual_path)
    annual_lf = pl.scan_parquet(str(annual_path))

    hourly_dir = tmp_path / "hourly"
    hourly_dir.mkdir()

    adjust_mf_electricity_parquet(
        metadata=metadata_lf,
        input_load_curve_annual=annual_lf,
        load_curve_hourly_dir=hourly_dir,
        path_metadata=meta_path,
        upgrade_id="00",
        storage_options={},
    )
    # No MF bldgs so metadata is not re-sunk; run completes without error

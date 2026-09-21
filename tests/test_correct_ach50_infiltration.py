"""Tests for fitted-ratio ACH50 correction fractions."""

from __future__ import annotations

import math

import polars as pl
import pytest

from data.resstock.load_curve.correct_ach50_infiltration import (
    BuildingCorrection,
    ContextConfig,
    EndUseGroup,
    GroupDefinition,
    WLSResult,
    _apply_corrections_to_annual,
    _compute_correction_fractions,
    _production_ach_release,
    _report_yaml_name,
    run_correction,
)

HEATING_COL = "out.electricity.heating.energy_consumption.kwh"
COOLING_COL = "out.electricity.cooling.energy_consumption.kwh"
GAS_COL = "out.natural_gas.heating.energy_consumption.kwh"


def _ctx(enduses: list[EndUseGroup]) -> ContextConfig:
    return ContextConfig(
        climate_zone="A_4",
        vintage_mapping={},
        stories_to_height_m={},
        groups=[
            GroupDefinition(
                name="Electric Resistance",
                conditions=[{"heats_with_electricity": True}],
                enduse_groups=enduses,
            )
        ],
        ach50_column="in.infiltration",
        sqft_column="in.sqft",
        vintage_column="in.vintage",
        stories_column="in.geometry_stories",
        weight_column="weight",
        frac_clip_lower=-0.5,
        frac_clip_upper=1.0,
    )


def _building_row(
    *,
    bldg_id: int,
    ach50: float,
    chan_ach50: float,
    sqft: float,
    heating_kwh: float,
    cooling_kwh: float = 0.0,
    gas_kwh: float = 0.0,
) -> dict[str, object]:
    return {
        "bldg_id": bldg_id,
        "_group": "Electric Resistance",
        "sqft": sqft,
        "ach50": ach50,
        "chan_ach50": chan_ach50,
        "delta_ach50": ach50 - chan_ach50,
        HEATING_COL: heating_kwh,
        COOLING_COL: cooling_kwh,
        GAS_COL: gas_kwh,
    }


def _heating_eu() -> EndUseGroup:
    return EndUseGroup(
        name="elec_heating",
        hourly_consumption_cols=["out.electricity.heating.energy_consumption"],
        annual_kwh_cols=[HEATING_COL],
    )


def test_on_line_house_matches_voucher() -> None:
    """When actual intensity equals predicted intensity, fitted-ratio = voucher."""
    intercept, slope = 2.0, 0.1
    ach50_rs, ach50_chan = 20.0, 10.0
    sqft = 100.0
    pred_rs = intercept + slope * ach50_rs  # 4.0 kWh/sqft
    annual_kwh = pred_rs * sqft  # 400
    voucher = slope * sqft * (ach50_rs - ach50_chan) / annual_kwh  # 0.25

    ctx = _ctx([_heating_eu()])
    table = pl.DataFrame(
        [
            _building_row(
                bldg_id=1,
                ach50=ach50_rs,
                chan_ach50=ach50_chan,
                sqft=sqft,
                heating_kwh=annual_kwh,
            )
        ]
    )
    regs = {
        ("Electric Resistance", "elec_heating"): WLSResult(
            intercept=intercept, slope=slope, r_squared=1.0, n=10
        )
    }
    corr = _compute_correction_fractions(table, regs, ctx)[1][0]
    assert corr.frac == pytest.approx(voucher)
    assert corr.frac == pytest.approx(0.25)


def test_below_line_house_not_zeroed() -> None:
    """Voucher would clip to 1.0; fitted-ratio stays the group percent."""
    intercept, slope = 2.0, 0.1
    ach50_rs, ach50_chan = 20.0, 10.0
    sqft = 100.0
    annual_kwh = 50.0  # intensity 0.5, well below pred_rs = 4.0
    voucher_raw = slope * sqft * (ach50_rs - ach50_chan) / annual_kwh  # 2.0

    ctx = _ctx([_heating_eu()])
    table = pl.DataFrame(
        [
            _building_row(
                bldg_id=1,
                ach50=ach50_rs,
                chan_ach50=ach50_chan,
                sqft=sqft,
                heating_kwh=annual_kwh,
            )
        ]
    )
    regs = {
        ("Electric Resistance", "elec_heating"): WLSResult(
            intercept=intercept, slope=slope, r_squared=1.0, n=10
        )
    }
    corr = _compute_correction_fractions(table, regs, ctx)[1][0]
    assert voucher_raw > 1.0
    assert 0.0 < corr.frac < 1.0
    assert corr.frac == pytest.approx(0.25)


def test_negative_delta_increases_load_and_clips() -> None:
    intercept, slope = 2.0, 0.1
    ach50_rs, ach50_chan = 10.0, 20.0  # Chan leakier
    ctx = _ctx([_heating_eu()])
    table = pl.DataFrame(
        [
            _building_row(
                bldg_id=1,
                ach50=ach50_rs,
                chan_ach50=ach50_chan,
                sqft=100.0,
                heating_kwh=400.0,
            )
        ]
    )
    regs = {
        ("Electric Resistance", "elec_heating"): WLSResult(
            intercept=intercept, slope=slope, r_squared=1.0, n=10
        )
    }
    corr = _compute_correction_fractions(table, regs, ctx)[1][0]
    pred_rs = intercept + slope * ach50_rs  # 3.0
    pred_chan = intercept + slope * ach50_chan  # 4.0
    expected = 1.0 - pred_chan / pred_rs  # -1/3
    assert corr.frac == pytest.approx(expected)
    assert corr.frac < 0.0


def test_negative_delta_clips_at_lower_bound() -> None:
    """Large load-increase frac is clipped at frac_clip_lower."""
    intercept, slope = 1.0, 1.0
    ach50_rs, ach50_chan = 1.0, 10.0  # raw frac = 1 - 11/2 = -4.5
    ctx = _ctx([_heating_eu()])
    table = pl.DataFrame(
        [
            _building_row(
                bldg_id=1,
                ach50=ach50_rs,
                chan_ach50=ach50_chan,
                sqft=100.0,
                heating_kwh=400.0,
            )
        ]
    )
    regs = {
        ("Electric Resistance", "elec_heating"): WLSResult(
            intercept=intercept, slope=slope, r_squared=1.0, n=10
        )
    }
    corr = _compute_correction_fractions(table, regs, ctx)[1][0]
    assert corr.frac == pytest.approx(-0.5)


def test_pred_rs_nonpositive_leaves_uncorrected() -> None:
    intercept, slope = -5.0, 0.1
    ach50_rs = 20.0  # pred_rs = -3
    ctx = _ctx([_heating_eu()])
    table = pl.DataFrame(
        [
            _building_row(
                bldg_id=1,
                ach50=ach50_rs,
                chan_ach50=10.0,
                sqft=100.0,
                heating_kwh=400.0,
            )
        ]
    )
    regs = {
        ("Electric Resistance", "elec_heating"): WLSResult(
            intercept=intercept, slope=slope, r_squared=1.0, n=10
        )
    }
    corr = _compute_correction_fractions(table, regs, ctx)[1][0]
    assert corr.frac == 0.0


def test_cooling_and_gas_use_same_formula() -> None:
    heat_reg = WLSResult(intercept=2.0, slope=0.1, r_squared=1.0, n=10)
    cool_reg = WLSResult(intercept=0.5, slope=0.2, r_squared=1.0, n=10)
    gas_reg = WLSResult(intercept=3.0, slope=0.2, r_squared=1.0, n=10)
    ach50_rs, ach50_chan = 20.0, 10.0
    ctx = _ctx(
        [
            _heating_eu(),
            EndUseGroup(
                name="elec_cooling",
                hourly_consumption_cols=["out.electricity.cooling.energy_consumption"],
                annual_kwh_cols=[COOLING_COL],
            ),
            EndUseGroup(
                name="gas_heating",
                hourly_consumption_cols=["out.natural_gas.heating.energy_consumption"],
                annual_kwh_cols=[GAS_COL],
            ),
        ]
    )
    table = pl.DataFrame(
        [
            _building_row(
                bldg_id=1,
                ach50=ach50_rs,
                chan_ach50=ach50_chan,
                sqft=100.0,
                heating_kwh=400.0,
                cooling_kwh=200.0,
                gas_kwh=800.0,
            )
        ]
    )
    regs = {
        ("Electric Resistance", "elec_heating"): heat_reg,
        ("Electric Resistance", "elec_cooling"): cool_reg,
        ("Electric Resistance", "gas_heating"): gas_reg,
    }
    corrs = {
        c.enduse_name: c for c in _compute_correction_fractions(table, regs, ctx)[1]
    }

    def expected(reg: WLSResult) -> float:
        pred_rs = reg.intercept + reg.slope * ach50_rs
        pred_chan = reg.intercept + reg.slope * ach50_chan
        return 1.0 - pred_chan / pred_rs

    assert corrs["elec_heating"].frac == pytest.approx(expected(heat_reg))
    assert corrs["elec_cooling"].frac == pytest.approx(expected(cool_reg))
    assert corrs["gas_heating"].frac == pytest.approx(expected(gas_reg))
    assert not math.isclose(corrs["elec_heating"].frac, corrs["elec_cooling"].frac)
    assert not math.isclose(corrs["elec_heating"].frac, corrs["gas_heating"].frac)


def test_apply_corrections_to_annual_scales_and_recomputes_total() -> None:
    heating = "out.electricity.heating.energy_consumption.kwh"
    other = "out.electricity.lighting.energy_consumption.kwh"
    total = "out.electricity.total.energy_consumption.kwh"
    annual = pl.DataFrame(
        {
            "bldg_id": [1],
            heating: [400.0],
            other: [100.0],
            total: [500.0],
        }
    )
    corrections = {
        1: [
            BuildingCorrection(
                enduse_name="elec_heating",
                annual_kwh=400.0,
                frac=0.25,
                delta_ach50=10.0,
                slope=0.1,
                intercept=2.0,
            )
        ]
    }
    group_lookup = {
        "Electric Resistance": GroupDefinition(
            name="Electric Resistance",
            conditions=[],
            enduse_groups=[_heating_eu()],
        )
    }
    out = _apply_corrections_to_annual(
        annual, corrections, {1: "Electric Resistance"}, group_lookup
    )
    assert out[heating][0] == pytest.approx(300.0)
    assert out[other][0] == pytest.approx(100.0)
    assert out[total][0] == pytest.approx(400.0)


def test_production_ach_release_name() -> None:
    assert (
        _production_ach_release("res_2024_amy2018_2_sb") == "res_2024_amy2018_2_sb_ach"
    )


def test_report_yaml_name_uses_output_suffix() -> None:
    assert (
        _report_yaml_name("res_2024_amy2018_2_sb_ach_ratio", "01")
        == "ach50_correction_report_ach_ratio_u01.yaml"
    )


def test_run_correction_refuses_production_ach_release(tmp_path) -> None:
    with pytest.raises(SystemExit, match="Refusing to write"):
        run_correction(
            path_local=tmp_path,
            path_s3="s3://unused",
            input_release="res_2024_amy2018_2_sb",
            output_release="res_2024_amy2018_2_sb_ach",
            state="MD",
            upgrade_ids=["01"],
            path_chan_coefficients=tmp_path / "missing.yaml",
            path_context_config=tmp_path / "missing.yaml",
            skip_hourly=True,
        )

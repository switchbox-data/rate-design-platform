"""Tests for data.resstock.validations.real_world_checks."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from data.resstock.validations import real_world_checks as rw
from data.resstock.validations.real_world_checks import (
    ANNUAL_ELEC,
    ANNUAL_GAS,
    RealWorldCheckConfig,
    ReleaseInputs,
)

GAS = "out.natural_gas.{}.energy_consumption.kwh"


def _metadata(rows: list[dict]) -> pl.DataFrame:
    defaults = {
        "weight": 1.0,
        "in.vacancy_status": "Occupied",
        "in.heating_fuel": "Natural Gas",
        "in.geometry_building_type_recs": "Single-Family Detached",
        "in.geometry_building_type_height": "Single-Family Detached",
        "in.geometry_building_number_units_mf": None,
        "in.hvac_has_shared_system": "None",
        "in.water_heater_in_unit": "Yes",
        "in.water_heater_fuel": "Natural Gas",
        "in.vintage": "1970s",
    }
    return pl.DataFrame(
        [{**defaults, **r} for r in rows],
        schema_overrides={"in.geometry_building_number_units_mf": pl.String},
    )


def _annual(rows: list[dict]) -> pl.DataFrame:
    defaults = {
        ANNUAL_ELEC: 10_000.0,
        ANNUAL_GAS: 0.0,
        GAS.format("heating"): 0.0,
        GAS.format("heating_hp_bkup"): 0.0,
        GAS.format("hot_water"): 0.0,
        GAS.format("range_oven"): 0.0,
    }
    return pl.DataFrame([{**defaults, **r} for r in rows])


# ── Package layout ────────────────────────────────────────────────────────────


def test_preflight_functions_still_importable_from_package():
    from data.resstock.validations import validate_local_files
    from data.resstock.validations.preflight import (
        validate_local_files as preflight_fn,
    )

    assert validate_local_files is preflight_fn


# ── Gas meter estimate ────────────────────────────────────────────────────────


def _meter_fixture() -> tuple[pl.DataFrame, pl.DataFrame]:
    shared_mf = {
        "in.geometry_building_type_recs": "Multi-Family with 5+ Units",
        "in.geometry_building_number_units_mf": "10",
        "in.hvac_has_shared_system": "Heating Only",
    }
    metadata = _metadata(
        [
            {"bldg_id": 1},  # single-family: one meter
            {  # individually metered multifamily: one meter
                "bldg_id": 2,
                "in.geometry_building_type_recs": "Multi-Family with 5+ Units",
                "in.geometry_building_number_units_mf": "10",
            },
            {"bldg_id": 3, **shared_mf},  # shared boiler, no in-unit gas
            {"bldg_id": 4, **shared_mf},  # shared boiler plus gas range
            {"bldg_id": 5, "in.heating_fuel": "Electricity"},  # no gas: excluded
        ]
    )
    annual = _annual(
        [
            {"bldg_id": 1, ANNUAL_GAS: 100.0, GAS.format("heating"): 100.0},
            {"bldg_id": 2, ANNUAL_GAS: 100.0, GAS.format("heating"): 100.0},
            {"bldg_id": 3, ANNUAL_GAS: 100.0, GAS.format("heating"): 100.0},
            {
                "bldg_id": 4,
                ANNUAL_GAS: 120.0,
                GAS.format("heating"): 100.0,
                GAS.format("range_oven"): 20.0,
            },
            {"bldg_id": 5},
        ]
    )
    return metadata, annual


def test_estimate_gas_meters_bounds():
    metadata, annual = _meter_fixture()
    row = rw.estimate_gas_meters(metadata, annual).row(0, named=True)
    assert row["gas_dwellings"] == 4
    # Fewest meters: both shared-boiler units are 1/10 of one building meter.
    assert row["meters_min"] == pytest.approx(1 + 1 + 0.1 + 0.1)
    # Most meters: the unit with a gas range also has its own meter.
    assert row["meters_max"] == pytest.approx(1 + 1 + 0.1 + 1.1)
    assert row["master_share_min"] == pytest.approx(25.0)
    assert row["master_share_max"] == pytest.approx(50.0)


def test_estimate_gas_meters_by_utility():
    metadata, annual = _meter_fixture()
    assignment = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            "sb.gas_utility": ["a", "a", "b", "b", None],
        }
    )
    out = rw.estimate_gas_meters(metadata, annual, assignment)
    assert out["utility_code"].to_list() == ["statewide", "a", "b"]
    b = out.filter(pl.col("utility_code") == "b").row(0, named=True)
    assert b["meters_min"] == pytest.approx(0.2)
    assert b["meters_max"] == pytest.approx(1.2)


def test_meter_band_inside_and_outside():
    cfg = RealWorldCheckConfig(gas_meters_band_pct=10)
    row = {
        "meters_min": 100.0,
        "meters_max": 200.0,
        "master_share_min": 10.0,
        "master_share_max": 30.0,
        "gas_dwellings": 250.0,
        "n_buildings": 10_000,
    }
    inside = rw._meter_band_result(
        scope="statewide", release="r", row=row, eia_customers=150.0, cfg=cfg
    )
    assert inside.status == "ok"
    assert inside.diff == 0.0
    assert "about 20%" in (inside.note or "")
    within_slack = rw._meter_band_result(
        scope="statewide", release="r", row=row, eia_customers=95.0, cfg=cfg
    )
    assert within_slack.status == "ok"
    below = rw._meter_band_result(
        scope="statewide", release="r", row=row, eia_customers=80.0, cfg=cfg
    )
    assert below.status == "warn"
    assert below.diff == pytest.approx(-20.0)


# ── EIA-861 loading ───────────────────────────────────────────────────────────


def test_load_eia861_maps_ids_and_drops_unmatched(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(rw, "get_eia_utility_id_to_std_name", lambda state: {10: "big"})
    path = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {
            "utility_id_eia": [10, 11, 12],
            "utility_code": [None, "small", None],
            "residential_sales_mwh": [100.0, 10.0, 999.0],
            "residential_customers": [50.0, 5.0, 999.0],
        }
    ).write_parquet(path)
    out = rw.load_eia861_residential(str(path), "XX").sort("utility_code")
    assert out["utility_code"].to_list() == ["big", "small"]
    assert out["eia_kwh"].to_list() == [100_000.0, 10_000.0]
    assert out["eia_customers"].to_list() == [50.0, 5.0]


# ── Electricity checks ────────────────────────────────────────────────────────


def _elec_inputs() -> ReleaseInputs:
    ids = list(range(1, 7))
    metadata = _metadata([{"bldg_id": i, "weight": 1000.0} for i in ids])
    annual = _annual([{"bldg_id": i} for i in ids])
    assignment = pl.DataFrame(
        {
            "bldg_id": ids,
            "sb.electric_utility": ["big"] * 4 + ["muni", "industrial"],
            "sb.gas_utility": [None] * 6,
        }
    )
    return ReleaseInputs(
        state="XX",
        release_raw="raw",
        release_sb="raw_sb",
        metadata_raw=metadata,
        metadata_sb=metadata,
        annual_raw=annual,
        annual_sb=annual,
        utility_assignment=assignment,
    )


def test_check_electricity_statewide_and_utility_rules():
    inputs = _elec_inputs()
    eia861 = pl.DataFrame(
        {
            "utility_code": ["big", "muni", "industrial"],
            "eia_kwh": [40_000_000.0, 2_000_000.0, 0.0],
            "eia_customers": [5_000.0, 1_000.0, 0.0],
        }
    )
    # z = 0 turns off the sampling-noise widening so the base tolerances apply.
    cfg = RealWorldCheckConfig(sampling_noise_z=0)
    results = {
        (r.check, r.scope, r.release): r
        for r in rw.check_electricity(inputs, eia861, cfg)
    }

    statewide = results[("electric_customers_statewide", "statewide", "raw_sb")]
    assert statewide.benchmark_value == 6_000.0
    assert statewide.status == "ok"

    big = results[("electric_customers_utility", "utility=big", "raw_sb")]
    assert big.diff == pytest.approx(-20.0)
    assert big.status == "ok"
    # Customer-normalized: 4,000 dwellings x 10,000 kWh scaled to 5,000 customers.
    big_kwh = results[("electric_kwh_utility", "utility=big", "raw_sb")]
    assert big_kwh.resstock_value == pytest.approx(50_000_000.0)
    assert big_kwh.status == "warn"

    # Small utilities are compared, not skipped.
    muni = results[("electric_kwh_utility", "utility=muni", "raw_sb")]
    assert muni.status == "warn"
    industrial = results[("electric_customers_utility", "utility=industrial", "raw_sb")]
    assert industrial.status == "skipped"
    assert "zero residential customers" in (industrial.note or "")


def test_sampling_tolerance():
    # 2 / sqrt(4) = 100%; 2 / sqrt(400) = 10% is below the 20% floor.
    assert rw.sampling_tolerance(20.0, 4, 2.0) == pytest.approx(100.0)
    assert rw.sampling_tolerance(20.0, 400, 2.0) == pytest.approx(20.0)
    # Percentage points: relative noise times the benchmark share.
    assert rw.sampling_tolerance(5.0, 4, 2.0, scale=40.0) == pytest.approx(40.0)
    assert rw.sampling_tolerance(20.0, 0, 2.0) == 20.0


def test_per_utility_tolerance_widens_for_few_buildings():
    inputs = _elec_inputs()
    eia861 = pl.DataFrame(
        {
            "utility_code": ["big"],
            "eia_kwh": [40_000_000.0],
            "eia_customers": [5_000.0],
        }
    )
    results = rw.check_electricity(inputs, eia861, RealWorldCheckConfig())
    big_kwh = next(
        r
        for r in results
        if r.check == "electric_kwh_utility" and r.scope == "utility=big"
    )
    # 4 buildings: tolerance max(20%, 2 / sqrt(4)) = 100%, so +25% is noise.
    assert big_kwh.n_buildings == 4
    assert big_kwh.tolerance == pytest.approx(100.0)
    assert big_kwh.status == "ok"
    assert "4 ResStock buildings" in (big_kwh.note or "")


def test_raw_electricity_is_context_only():
    inputs = _elec_inputs()
    eia861 = pl.DataFrame(
        {"utility_code": ["big"], "eia_kwh": [1.0], "eia_customers": [6_000.0]}
    )
    results = rw.check_electricity(inputs, eia861, RealWorldCheckConfig())
    raw = next(
        r for r in results if r.check == "electric_kwh_statewide" and r.release == "raw"
    )
    assert raw.diff is not None and raw.diff > 100
    assert raw.status == "ok"


# ── Gas checks ────────────────────────────────────────────────────────────────


def _gas_inputs() -> ReleaseInputs:
    metadata = _metadata([{"bldg_id": i, "weight": 10_000.0} for i in (1, 2)])
    annual_raw = _annual(
        [
            {"bldg_id": i, ANNUAL_GAS: 1_000.0, GAS.format("heating"): 1_000.0}
            for i in (1, 2)
        ]
    )
    annual_sb = annual_raw.with_columns(pl.col(ANNUAL_GAS) * 2)
    assignment = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "sb.electric_utility": ["e", "e"],
            "sb.gas_utility": ["g1", "g2"],
        }
    )
    return ReleaseInputs(
        state="XX",
        release_raw="raw",
        release_sb="raw_sb",
        metadata_raw=metadata,
        metadata_sb=metadata,
        annual_raw=annual_raw,
        annual_sb=annual_sb,
        utility_assignment=assignment,
    )


def _eia176() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "operator_id_eia": ["op1", "op2"],
            "eia_kwh": [20_000_000.0, 20_000_000.0],
            "eia_customers": [10_000.0, 6_000.0],
            "heat_content_imputed": [False, False],
        }
    )


def test_check_gas_statewide_and_utility_shares():
    inputs = _gas_inputs()
    cfg = RealWorldCheckConfig(sampling_noise_z=0)
    results = rw.check_gas(inputs, _eia176(), {"op1": "g1", "op2": "g2"}, cfg)
    by_key = {(r.check, r.scope): r for r in results}

    statewide = by_key[("gas_kwh_statewide", "statewide")]
    assert statewide.resstock_value == pytest.approx(40_000_000.0)
    assert statewide.status == "ok"
    # _sb differs from raw, so the scaled (tight) tolerance applies.
    assert statewide.tolerance == cfg.gas_kwh_statewide_scaled_pct

    # Both utilities use half the gas on both sides. g2's share is not shrunk
    # by it having fewer EIA meters than dwellings.
    for check in ("gas_share_utility_pp", "gas_share_utility_rel"):
        g2 = by_key[(check, "utility=g2")]
        assert g2.resstock_value == pytest.approx(50.0)
        assert g2.benchmark_value == pytest.approx(50.0)
        assert g2.status == "ok"
    assert by_key[("gas_meters", "utility=g1")].status == "ok"
    assert by_key[("gas_meters", "utility=g2")].status == "warn"


def test_gas_shares_use_mapped_utilities_only():
    inputs = _gas_inputs()
    assert inputs.annual_sb is not None and inputs.utility_assignment is not None
    # An unmapped ResStock utility and an unmapped EIA operator, each large.
    metadata = pl.concat(
        [inputs.metadata_sb, _metadata([{"bldg_id": 3, "weight": 10_000.0}])]
    )
    annual_sb = pl.concat(
        [
            inputs.annual_sb,
            _annual([{"bldg_id": 3, ANNUAL_GAS: 50_000.0}]),
        ],
        how="diagonal",
    )
    assignment = pl.concat(
        [
            inputs.utility_assignment,
            pl.DataFrame(
                {"bldg_id": [3], "sb.electric_utility": ["e"], "sb.gas_utility": ["g3"]}
            ),
        ]
    )
    eia = pl.concat(
        [
            _eia176(),
            pl.DataFrame(
                {
                    "operator_id_eia": ["op3"],
                    "eia_kwh": [900_000_000.0],
                    "eia_customers": [50_000.0],
                    "heat_content_imputed": [False],
                }
            ),
        ]
    )
    inputs = replace_inputs(
        metadata_raw=metadata,
        metadata_sb=metadata,
        annual_raw=annual_sb,
        annual_sb=annual_sb,
        utility_assignment=assignment,
    )
    results = rw.check_gas(
        inputs, eia, {"op1": "g1", "op2": "g2"}, RealWorldCheckConfig()
    )
    g1 = next(
        r
        for r in results
        if r.check == "gas_share_utility_pp" and r.scope == "utility=g1"
    )
    assert g1.resstock_value == pytest.approx(50.0)
    assert g1.benchmark_value == pytest.approx(50.0)
    assert not any(r.scope == "utility=g3" for r in results)


def test_share_pair_relative_check_catches_small_categories():
    pp, rel = rw._share_pair(
        check="acs_heating_fuel",
        label="heating-fuel share",
        scope="statewide occupied, Propane",
        release="r",
        resstock_pct=8.0,
        benchmark_name="ACS",
        benchmark_pct=4.0,
        n_buildings=10_000,
        pp_tolerance=5.0,
        rel_tolerance=25.0,
        noise_z=2.0,
    )
    assert (pp.check, pp.status) == ("acs_heating_fuel_pp", "ok")
    assert (rel.check, rel.status) == ("acs_heating_fuel_rel", "warn")
    assert rel.diff == pytest.approx(100.0)


def test_share_pair_skips_relative_check_below_floor():
    _, rel = rw._share_pair(
        check="acs_building_type",
        label="building-type share",
        scope="statewide all units, Mobile home",
        release="r",
        resstock_pct=1.5,
        benchmark_name="ACS",
        benchmark_pct=0.8,
        n_buildings=10_000,
        pp_tolerance=5.0,
        rel_tolerance=25.0,
        noise_z=2.0,
        min_rel_share_pct=2.0,
    )
    assert rel.status == "skipped"
    assert "below" in (rel.note or "")


def test_check_gas_without_operator_map_skips_per_utility():
    results = rw.check_gas(_gas_inputs(), _eia176(), None, RealWorldCheckConfig())
    skipped = [r for r in results if r.scope == "by utility"]
    assert {r.check for r in skipped} == {
        "gas_meters",
        "gas_share_utility_pp",
        "gas_share_utility_rel",
    }
    assert all(r.status == "skipped" for r in skipped)
    assert any(r.check == "gas_meters" and r.scope == "statewide" for r in results)


def test_check_gas_notes_imputed_heat_content():
    eia = _eia176().with_columns(pl.Series("heat_content_imputed", [True, False]))
    results = rw.check_gas(_gas_inputs(), eia, None, RealWorldCheckConfig())
    statewide = next(r for r in results if r.check == "gas_kwh_statewide")
    assert "imputed" in (statewide.note or "")
    assert "op1" in (statewide.note or "")


def _sb_run(
    tmp_path: Path,
    run_id: str,
    *,
    states: list[str],
    file_types: list[str],
    steps: list[tuple[str, dict[str, Any]]],
) -> None:
    from data.resstock.manifest import new_run_record, record_step, upsert_run

    run = new_run_record(
        release="r",
        release_sb="r_sb",
        states=states,
        upgrade_ids=["0"],
        file_types=file_types,
        flags={},
    )
    run["run_id"] = run_id
    for name, details in steps:
        record_step(run, name, **details)
    upsert_run(tmp_path, run)


def test_gas_scaled_from_manifest_last_hourly_write_wins(tmp_path: Path):
    _sb_run(
        tmp_path,
        "scale",
        states=["CT"],
        file_types=["load_curve_hourly"],
        steps=[
            ("clone", {"files_copied": 10}),
            ("adjust_gas_usage", {"scale_factors": {"CT": 1.2}}),
        ],
    )
    assert rw.gas_scaled_from_manifest(tmp_path, "CT") is True

    # Metadata-only clone does not touch hourly, so the prior scale still holds.
    _sb_run(
        tmp_path,
        "metadata",
        states=["CT"],
        file_types=["metadata"],
        steps=[("clone", {"files_copied": 1})],
    )
    assert rw.gas_scaled_from_manifest(tmp_path, "CT") is True

    # Cloning hourly without scaling overwrites the scaled files.
    _sb_run(
        tmp_path,
        "reclone",
        states=["CT"],
        file_types=["load_curve_hourly", "metadata"],
        steps=[("clone", {"files_copied": 10})],
    )
    assert rw.gas_scaled_from_manifest(tmp_path, "CT") is False
    assert rw.gas_scaled_from_manifest(tmp_path / "missing", "CT") is None


def test_gas_scaled_from_manifest_ignores_other_states(tmp_path: Path):
    _sb_run(
        tmp_path,
        "ny",
        states=["NY"],
        file_types=["load_curve_hourly"],
        steps=[("adjust_gas_usage", {"scale_factors": {"NY": 1.1}})],
    )
    assert rw.gas_scaled_from_manifest(tmp_path, "CT") is None


def test_check_gas_uses_manifest_over_inference(tmp_path: Path):
    # Files look scaled (annual_sb = 2× raw) but the last hourly write was clone.
    _sb_run(
        tmp_path,
        "reclone",
        states=["XX"],
        file_types=["load_curve_hourly"],
        steps=[("clone", {"files_copied": 10})],
    )
    inputs = replace(_gas_inputs(), base_sb=str(tmp_path))
    cfg = RealWorldCheckConfig()
    statewide = next(
        r
        for r in rw.check_gas(inputs, _eia176(), None, cfg)
        if r.check == "gas_kwh_statewide"
    )
    assert statewide.tolerance == cfg.gas_kwh_statewide_unscaled_pct
    assert "clone" in (statewide.note or "")


# ── HP approximation ──────────────────────────────────────────────────────────


def test_hp_approximation_share_and_unclassified():
    sb = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "weight": [1.0, 1.0, 1.0, 1.0],
            "approximated_hp_load": [True, True, False, False],
        }
    )
    raw = _metadata(
        [
            {
                "bldg_id": 1,
                "in.geometry_building_type_height": "Multifamily with 8+ stories",
            },
            {"bldg_id": 2},  # single-family gas home: matches neither rule
            {"bldg_id": 3},
            {"bldg_id": 4},
        ]
    )
    inputs = replace_inputs(approx_metadata={"02": (sb, raw)})
    results = {
        r.check: r for r in rw.check_hp_approximation(inputs, RealWorldCheckConfig())
    }
    assert results["hp_approx_share"].resstock_value == pytest.approx(50.0)
    assert results["hp_approx_share"].status == "warn"
    assert results["hp_approx_unclassified"].resstock_value == 1.0
    assert results["hp_approx_unclassified"].status == "warn"


def replace_inputs(**changes) -> ReleaseInputs:
    return replace(_elec_inputs(), **changes)


# ── ACS ───────────────────────────────────────────────────────────────────────


def test_vintage_resstock_maps_known_and_drops_unknown():
    df = pl.DataFrame(
        {"v": ["<1940", "1940s", "1950s", "2010s", None, "", "unknown", "2000s"]}
    )
    out = df.select(rw.vintage_resstock(pl.col("v")).alias("b"))
    assert out["b"].to_list() == [
        "Before 1950",
        "Before 1950",
        "1950-59",
        "2010+",
        None,
        None,
        None,
        "2000-09",
    ]


def test_acs_category_mappings():
    df = pl.DataFrame(
        {
            "bld": ["01", "02", "03", "06", "10"],
            "yrblt": ["1939", "1955", "2015", "bad", "2005"],
        }
    )
    out = df.select(
        rw.building_class_acs(pl.col("bld")).alias("b"),
        rw.vintage_acs(pl.col("yrblt")).alias("v"),
    )
    assert out["b"].to_list() == [
        "Mobile home",
        "Single-family",
        "Single-family",
        "Multifamily",
        None,
    ]
    assert out["v"].to_list() == ["Before 1950", "1950-59", "2010+", None, "2000-09"]


def test_acs_master_metered_share():
    pums = pl.DataFrame(
        {"gasfp": ["1", "4", "4", "3", "2"], "wgtp": [10, 20, 10, 100, 0]}
    )
    assert rw.acs_master_metered_share(pums) == pytest.approx(25.0)


def test_load_acs_housing_flags_occupied_units(tmp_path: Path):
    path = tmp_path / "pums.parquet"
    pl.DataFrame(
        {
            "typehugq": ["1", "1", "1", "2"],
            "vacs": [None, "1", None, None],
            "np": [2, 0, 0, 1],
            "hfl": ["1", None, "3", None],
            "bld": ["02", "06", "02", None],
            "yrblt": ["1975", "1990", "2005", None],
            "gasfp": ["4", None, "3", None],
            "wgtp": [10, 20, 30, 0],
        }
    ).write_parquet(path)
    out = rw.load_acs_housing(str(path))
    # Group quarters are dropped; occupied needs blank VACS *and* NP > 0.
    assert out.height == 3
    assert out["occupied"].to_list() == [True, False, False]


def test_check_acs_occupancy_by_attribute():
    metadata = _metadata(
        [
            {"bldg_id": 1},
            {"bldg_id": 2, "in.heating_fuel": "Fuel Oil"},
            {
                "bldg_id": 3,
                "in.heating_fuel": "Fuel Oil",
                "in.vacancy_status": "Vacant",
                "in.geometry_building_type_recs": "Multi-Family with 5+ Units",
            },
        ]
    )
    inputs = replace_inputs(
        metadata_raw=metadata,
        annual_raw=_annual(
            [
                {"bldg_id": 1, ANNUAL_GAS: 1.0, GAS.format("heating"): 1.0},
                {"bldg_id": 2},
                {"bldg_id": 3},
            ]
        ),
    )
    pums = pl.DataFrame(
        {
            "hfl": ["1", "4", None],
            "bld": ["02", "02", "06"],
            "yrblt": ["1975", "1975", "1975"],
            "gasfp": ["4", "3", None],
            "wgtp": [50, 50, 50],
            "occupied": [True, True, False],
        }
    )
    results = rw.check_acs(inputs, pums, RealWorldCheckConfig())
    by_key = {(r.check, r.scope): r for r in results}

    # Heating fuel: occupied only, so the vacant oil home is left out.
    oil = by_key[("acs_heating_fuel_pp", "statewide occupied, Fuel oil")]
    assert oil.resstock_value == pytest.approx(50.0)
    assert oil.benchmark_value == pytest.approx(50.0)
    assert oil.status == "ok"
    # Building type: all units, so the vacant multifamily unit counts on both sides.
    mf = by_key[("acs_building_type_pp", "statewide all units, Multifamily")]
    assert mf.resstock_value == pytest.approx(100 / 3)
    assert mf.benchmark_value == pytest.approx(100 / 3)
    assert ("acs_building_type_rel", "statewide all units, Multifamily") in by_key

    master = next(r for r in results if r.check == "acs_master_metering")
    assert master.benchmark_value == 0.0
    assert master.status == "ok"


def test_check_acs_excludes_unmapped_vintages():
    metadata = _metadata(
        [
            {"bldg_id": 1, "in.vintage": "1970s"},
            {"bldg_id": 2, "in.vintage": "1970s"},
            {"bldg_id": 3, "in.vintage": None},
        ]
    )
    inputs = replace_inputs(
        metadata_raw=metadata,
        annual_raw=_annual(
            [
                {"bldg_id": 1, ANNUAL_GAS: 1.0, GAS.format("heating"): 1.0},
                {"bldg_id": 2},
                {"bldg_id": 3},
            ]
        ),
    )
    pums = pl.DataFrame(
        {
            "hfl": ["1", "4"],
            "bld": ["02", "02"],
            "yrblt": ["1975", "1975"],
            "gasfp": ["4", "3"],
            "wgtp": [50, 50],
            "occupied": [True, True],
        }
    )
    results = rw.check_acs(inputs, pums, RealWorldCheckConfig())
    year = next(
        r for r in results if r.check == "acs_year_built_pp" and "1960-79" in r.scope
    )
    # The unmapped building is out of the denominator, so 1970s is 100%.
    assert year.resstock_value == pytest.approx(100.0)
    assert year.n_buildings == 2
    assert "excluded" in (year.note or "")


# ── Messages, config, runner ──────────────────────────────────────────────────


def test_warning_points_to_notebook_section():
    result = rw._pct_result(
        check="electric_customers_utility",
        label="electric customers",
        scope="utility=big",
        release="raw_sb",
        unit="dwellings",
        resstock=150.0,
        benchmark_name="EIA-861",
        benchmark=100.0,
        tolerance=20.0,
    )
    msg = rw.format_warning("XX", result)
    assert result.status == "warn"
    assert "+50.0%" in msg
    assert "investigate_resstock.qmd" in msg
    assert "Customer counts by electric utility" in msg
    assert "#sec-electric-customers-by-utility" in msg


def test_every_check_has_a_notebook_section():
    for name in rw.NOTEBOOK_SECTIONS:
        assert rw.NOTEBOOK_SECTIONS[name].anchor.startswith("sec-")


def test_merge_config_rejects_unknown_keys_and_appends_ignore():
    base = RealWorldCheckConfig(ignore=("a",))
    merged = rw._merge_config(base, {"acs_share_pp": 7, "ignore": ["b"]})
    assert merged.acs_share_pp == 7
    assert merged.ignore == ("a", "b")
    with pytest.raises(ValueError, match="Unknown"):
        rw._merge_config(base, {"not_a_key": 1})


def test_config_yaml_tolerances_are_valid():
    cfg = rw.load_check_config("CT")
    assert cfg.gas_meters_band_pct > 0


def test_ignore_marks_warning_ignored():
    inputs = _elec_inputs()
    eia861 = pl.DataFrame(
        {"utility_code": ["big"], "eia_kwh": [1.0], "eia_customers": [100_000.0]}
    )
    cfg = RealWorldCheckConfig(ignore=("electric_customers_statewide",))
    results = rw.run_real_world_checks(
        inputs=inputs,
        eia_year=2018,
        path_eia861="unused",
        path_pums_housing="unused",
        cfg=cfg,
        load_eia861=lambda path, state: eia861,
        load_eia176=lambda state, year: _eia176(),
        load_acs=lambda path: pl.DataFrame(),
    )
    statewide = next(r for r in results if r.check == "electric_customers_statewide")
    assert statewide.status == "ignored"


def test_runner_turns_failures_into_skipped_results():
    def boom(*args):
        raise OSError("no network")

    results = rw.run_real_world_checks(
        inputs=_elec_inputs(),
        eia_year=2018,
        path_eia861="unused",
        path_pums_housing="unused",
        cfg=RealWorldCheckConfig(),
        load_eia861=boom,
        load_eia176=boom,
        load_acs=boom,
    )
    failed = [r for r in results if r.note and "no network" in r.note]
    assert len(failed) == 3
    assert all(r.status == "skipped" for r in failed)


def _post_pipeline(
    tmp_path: Path, upgrade_ids: list[str], sample: int
) -> rw.PostPipelineChecks:
    return rw.run_post_pipeline_checks(
        states=["XX"],
        upgrade_ids=upgrade_ids,
        sample=sample,
        release="res_2024_amy2018_2",
        path_raw=tmp_path,
        path_sb=tmp_path,
        eia_year=None,
        path_s3_eia861_dir="s3://x",
        path_s3_pums_dir="s3://x",
        pums_survey="acs5",
        pums_year="2021",
        approx_upgrades=["01", "02"],
    )


def test_post_pipeline_checks_skip_conditions(tmp_path: Path):
    sampled = _post_pipeline(tmp_path, upgrade_ids=["0"], sample=10)
    assert sampled.skipped_reason is not None and "--sample" in sampled.skipped_reason
    assert sampled.manifest_details() == {"skipped": sampled.skipped_reason}

    no_baseline = _post_pipeline(tmp_path, upgrade_ids=["2"], sample=0)
    assert no_baseline.skipped_reason == "upgrade 00 is not in --upgrade-ids"


def test_post_pipeline_checks_never_raise_on_missing_files(tmp_path: Path):
    out = rw.run_post_pipeline_checks(
        states=["XX"],
        upgrade_ids=["0"],
        sample=0,
        release="res_2024_amy2018_2",
        path_raw=tmp_path / "raw",
        path_sb=tmp_path / "sb",
        eia_year=2018,
        path_s3_eia861_dir="s3://x",
        path_s3_pums_dir="s3://x",
        pums_survey="acs5",
        pums_year="2021",
        approx_upgrades=[],
    )
    assert out.results == {}
    assert len(out.warnings) == 1
    assert "could not run" in out.warnings[0]

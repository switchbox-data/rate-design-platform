"""Tests for data.resstock.validations.real_world_checks."""

from __future__ import annotations

from pathlib import Path

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
    cfg = RealWorldCheckConfig(min_utility_customers=2_000)
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

    assert (
        results[("electric_customers_utility", "utility=muni", "raw_sb")].status
        == "skipped"
    )
    industrial = results[("electric_customers_utility", "utility=industrial", "raw_sb")]
    assert industrial.status == "skipped"
    assert "zero residential customers" in (industrial.note or "")


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


def test_check_gas_uses_statewide_kwh_scaling_not_customers():
    inputs = _gas_inputs()
    cfg = RealWorldCheckConfig(min_utility_customers=0)
    results = rw.check_gas(inputs, _eia176(), {"op1": "g1", "op2": "g2"}, cfg)
    by_key = {(r.check, r.scope): r for r in results}

    statewide = by_key[("gas_kwh_statewide", "statewide")]
    assert statewide.resstock_value == pytest.approx(40_000_000.0)
    assert statewide.status == "ok"
    # _sb differs from raw, so the scaled (tight) tolerance applies.
    assert statewide.tolerance == cfg.gas_kwh_statewide_scaled_pct

    # Statewide scale is 1, and g2 has fewer EIA meters than dwellings: its kWh
    # must not be shrunk by the meter count.
    g2 = by_key[("gas_kwh_utility", "utility=g2")]
    assert g2.resstock_value == pytest.approx(20_000_000.0)
    assert g2.status == "ok"
    assert by_key[("gas_meters", "utility=g1")].status == "ok"
    assert by_key[("gas_meters", "utility=g2")].status == "warn"


def test_check_gas_without_operator_map_skips_per_utility():
    results = rw.check_gas(_gas_inputs(), _eia176(), None, RealWorldCheckConfig())
    skipped = [r for r in results if r.scope == "by utility"]
    assert {r.check for r in skipped} == {"gas_kwh_utility", "gas_meters"}
    assert all(r.status == "skipped" for r in skipped)
    assert any(r.check == "gas_meters" and r.scope == "statewide" for r in results)


def test_check_gas_notes_imputed_heat_content():
    eia = _eia176().with_columns(pl.Series("heat_content_imputed", [True, False]))
    results = rw.check_gas(_gas_inputs(), eia, None, RealWorldCheckConfig())
    statewide = next(r for r in results if r.check == "gas_kwh_statewide")
    assert "imputed" in (statewide.note or "")
    assert "op1" in (statewide.note or "")


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
    from dataclasses import replace

    return replace(_elec_inputs(), **changes)


# ── ACS ───────────────────────────────────────────────────────────────────────


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


def test_check_acs_compares_occupied_stock_only():
    metadata = _metadata(
        [
            {"bldg_id": 1},
            {"bldg_id": 2, "in.heating_fuel": "Fuel Oil"},
            {
                "bldg_id": 3,
                "in.heating_fuel": "Fuel Oil",
                "in.vacancy_status": "Vacant",
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
            "hfl": ["1", "4"],
            "bld": ["02", "02"],
            "yrblt": ["1975", "1975"],
            "gasfp": ["4", "3"],
            "wgtp": [50, 50],
        }
    )
    results = rw.check_acs(inputs, pums, RealWorldCheckConfig())
    fuel = {r.scope: r for r in results if r.check == "acs_heating_fuel"}
    assert fuel["statewide, Fuel oil"].resstock_value == pytest.approx(50.0)
    assert fuel["statewide, Fuel oil"].status == "ok"
    master = next(r for r in results if r.check == "acs_master_metering")
    assert master.benchmark_value == 0.0
    assert master.status == "ok"


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

"""Tests for subclass revenue requirement postprocessing."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.mid.compute_subclass_rr import (
    DEFAULT_SEASONAL_OUTPUT_FILENAME,
    _load_run_fields,
    _write_revenue_requirement_yamls,
    _write_seasonal_inputs_csv,
    compute_hp_flat_discount_inputs,
    compute_hp_seasonal_discount_inputs,
    compute_subclass_rr,
    parse_group_value_to_subclass,
)

RUN2_SUPPLY_OFFSET = 50.0
_FIXED_CHARGE = 2.0


def _write_urdb_tariff(
    path: Path, *, fixed_charge: float = _FIXED_CHARGE, rate: float = 0.21
) -> Path:
    """Write a minimal URDB v7 tariff JSON and return its path."""
    tariff = {
        "items": [
            {
                "label": "test_tariff",
                "fixedchargefirstmeter": fixed_charge,
                "energyratestructure": [[{"rate": rate, "adj": 0.0, "unit": "kWh"}]],
                "energyweekdayschedule": [[0] * 24 for _ in range(12)],
                "energyweekendschedule": [[0] * 24 for _ in range(12)],
            }
        ]
    }
    path.write_text(json.dumps(tariff), encoding="utf-8")
    return path


def _write_sample_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    # Monthly bills (winter: Jan/Feb/Dec, summer: Jul) + Annual rows.
    # Annual values are used by compute_subclass_rr; monthly by seasonal discount.
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4],
            "month": [
                "Jan",
                "Jul",
                "Annual",
                "Jan",
                "Jul",
                "Annual",
                "Feb",
                "Dec",
                "Annual",
                "Jan",
                "Jul",
                "Annual",
            ],
            "bill_level": [
                5.0,
                8.0,
                100.0,
                10.0,
                12.0,
                200.0,
                6.0,
                7.0,
                300.0,
                20.0,
                22.0,
                400.0,
            ],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")

    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "BAT_percustomer": [3.0, 20.0, 4.0, 40.0],
            "BAT_vol": [1.0, 2.0, 3.0, 4.0],
        }
    ).write_csv(run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv")

    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "weight": [1.0, 1.0, 1.0, 1.0],
            "postprocess_group.has_hp": [True, False, True, False],
            "postprocess_group.heating_type": ["hp", "gas", "hp", "resistance"],
        }
    ).write_csv(run_dir / "customer_metadata.csv")

    return run_dir


_LOADS_STATE = "NY"
_LOADS_UPGRADE = "00"


def _write_sample_resstock_loads_dir(tmp_path: Path) -> Path:
    """Write sample ResStock loads in hive-partition layout and return the base path."""
    resstock_base = tmp_path / "resstock"
    partition_dir = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    partition_dir.mkdir(parents=True)
    # HP buildings (1, 3): winter kWh = 10 + 20 + 30 = 60
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 3, 3],
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
                "2025-02-01 00:00:00",
                "2025-12-01 00:00:00",
            ],
            "out.electricity.total.energy_consumption": [10.0, 999.0, 20.0, 30.0],
            "out.electricity.pv.energy_consumption": [0.0, 0.0, 0.0, 0.0],
        }
    ).write_parquet(partition_dir / "sample_hp_loads.parquet")
    pl.DataFrame(
        {
            "bldg_id": [2, 4],
            "timestamp": ["2025-01-01 00:00:00", "2025-01-01 00:00:00"],
            "out.electricity.total.energy_consumption": [500.0, 600.0],
            "out.electricity.pv.energy_consumption": [0.0, 0.0],
        }
    ).write_parquet(partition_dir / "sample_nonhp_loads.parquet")
    return resstock_base


@pytest.mark.parametrize(
    ("group_col", "cross_subsidy_col", "expected"),
    [
        (
            "has_hp",
            "BAT_percustomer",
            {
                "false": (600.0, 60.0, 540.0),
                "true": (400.0, 7.0, 393.0),
            },
        ),
        (
            "postprocess_group.heating_type",
            "BAT_vol",
            {
                "gas": (200.0, 2.0, 198.0),
                "hp": (400.0, 4.0, 396.0),
                "resistance": (400.0, 4.0, 396.0),
            },
        ),
    ],
)
def test_compute_subclass_rr_for_multiple_groupings(
    tmp_path: Path,
    group_col: str,
    cross_subsidy_col: str,
    expected: dict[str, tuple[float, float, float]],
) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    breakdown = compute_subclass_rr(
        run_dir, group_col=group_col, cross_subsidy_col=cross_subsidy_col
    )

    assert breakdown.height == len(expected)
    for subclass, (sum_bills, sum_cross_subsidy, rr) in expected.items():
        row = breakdown.filter(pl.col("subclass") == subclass)
        assert row["sum_bills"][0] == pytest.approx(sum_bills)
        assert row["sum_cross_subsidy"][0] == pytest.approx(sum_cross_subsidy)
        assert row["revenue_requirement"][0] == pytest.approx(rr)


def test_compute_subclass_rr_missing_annual_rows_raises(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    pl.DataFrame({"bldg_id": [1], "month": ["Jan"], "bill_level": [100.0]}).write_csv(
        run_dir / "bills" / "elec_bills_year_target.csv"
    )
    pl.DataFrame({"bldg_id": [1], "BAT_percustomer": [10.0]}).write_csv(
        run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv"
    )
    pl.DataFrame(
        {"bldg_id": [1], "weight": [1.0], "postprocess_group.has_hp": [True]}
    ).write_csv(run_dir / "customer_metadata.csv")

    with pytest.raises(ValueError, match="Missing annual target bills"):
        compute_subclass_rr(run_dir)


def test_load_run_fields_from_scenario_config(tmp_path: Path) -> None:
    scenario_config = tmp_path / "scenarios.yaml"
    scenario_config.write_text(
        yaml.safe_dump(
            {
                "runs": {
                    1: {
                        "state": "RI",
                        "utility": "rie",
                        "utility_revenue_requirement": 241869601,
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    state, utility, rr = _load_run_fields(scenario_config, run_num=1)
    assert state == "RI"
    assert utility == "rie"
    assert rr == pytest.approx(241869601.0)


def test_compute_subclass_rr_applies_weights(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 2],
            "month": ["Jan", "Annual", "Jan", "Annual"],
            "bill_level": [0.0, 100.0, 0.0, 100.0],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "BAT_percustomer": [10.0, 10.0],
        }
    ).write_csv(run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv")
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 9.0],
            "postprocess_group.has_hp": [True, False],
        }
    ).write_csv(run_dir / "customer_metadata.csv")

    breakdown = compute_subclass_rr(run_dir)
    hp = breakdown.filter(pl.col("subclass") == "true")
    nonhp = breakdown.filter(pl.col("subclass") == "false")
    assert hp["sum_bills"][0] == pytest.approx(100.0)
    assert hp["sum_cross_subsidy"][0] == pytest.approx(10.0)
    assert hp["revenue_requirement"][0] == pytest.approx(90.0)
    assert nonhp["sum_bills"][0] == pytest.approx(900.0)
    assert nonhp["sum_cross_subsidy"][0] == pytest.approx(90.0)
    assert nonhp["revenue_requirement"][0] == pytest.approx(810.0)


def _write_sample_run2_dir(tmp_path: Path) -> Path:
    """Write a run-2 (delivery+supply) output dir with higher bills than run 1."""
    run_dir = tmp_path / "run2"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    # Same structure as run 1 but annual bills are higher by RUN2_SUPPLY_OFFSET
    # per building (supply adds cost).
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 2, 3, 3, 4, 4],
            "month": ["Jan", "Annual"] * 4,
            "bill_level": [
                5.0,
                100.0 + RUN2_SUPPLY_OFFSET,
                10.0,
                200.0 + RUN2_SUPPLY_OFFSET,
                15.0,
                300.0 + RUN2_SUPPLY_OFFSET,
                20.0,
                400.0 + RUN2_SUPPLY_OFFSET,
            ],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")

    # Same BAT values — the cross-subsidy allocation is independent
    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "BAT_percustomer": [3.0, 20.0, 4.0, 40.0],
            "BAT_vol": [1.0, 2.0, 3.0, 4.0],
        }
    ).write_csv(run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv")

    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "weight": [1.0, 1.0, 1.0, 1.0],
            "postprocess_group.has_hp": [True, False, True, False],
            "postprocess_group.heating_type": ["hp", "gas", "hp", "resistance"],
        }
    ).write_csv(run_dir / "customer_metadata.csv")
    return run_dir


def test_write_revenue_requirement_yamls_two_runs(tmp_path: Path) -> None:
    """Verify the nested YAML: delivery from run 1, total from run 2, supply = diff."""
    run1_dir = _write_sample_run_dir(tmp_path)
    run2_dir = _write_sample_run2_dir(tmp_path)
    delivery_breakdown = compute_subclass_rr(run1_dir)
    total_breakdown = compute_subclass_rr(run2_dir)
    differentiated_yaml = tmp_path / "config/rev_requirement/rie_hp_vs_nonhp.yaml"
    default_yaml = tmp_path / "config/rev_requirement/rie.yaml"

    gv_map = {"true": "hp", "false": "non-hp"}

    out_diff, out_default = _write_revenue_requirement_yamls(
        delivery_breakdown,
        run_dir=run1_dir,
        group_col="has_hp",
        cross_subsidy_col="BAT_percustomer",
        utility="rie",
        default_revenue_requirement=241869601.0,
        differentiated_yaml_path=differentiated_yaml,
        default_yaml_path=default_yaml,
        group_value_to_subclass=gv_map,
        total_breakdown=total_breakdown,
        total_delivery_rr=933.0,
        total_delivery_and_supply_rr=1133.0,
    )

    assert out_diff == differentiated_yaml
    assert differentiated_yaml.exists()

    diff_data = yaml.safe_load(differentiated_yaml.read_text(encoding="utf-8"))
    assert diff_data["utility"] == "rie"
    assert diff_data["group_col"] == "has_hp"
    assert diff_data["source_run_dir"] == str(run1_dir)
    assert diff_data["total_delivery_revenue_requirement"] == pytest.approx(933.0)
    assert diff_data["total_delivery_and_supply_revenue_requirement"] == pytest.approx(
        1133.0
    )

    sr = diff_data["subclass_revenue_requirements"]
    assert "hp" in sr
    assert "non-hp" in sr

    # Run 1 delivery: hp=400-7=393, non-hp=600-60=540
    # Run 2 total: hp=500-7=493, non-hp=700-60=640 (each bldg +50, same BAT)
    # Supply = total - delivery
    assert sr["hp"]["delivery"] == pytest.approx(393.0)
    assert sr["hp"]["total"] == pytest.approx(493.0)
    assert sr["hp"]["supply"] == pytest.approx(100.0)
    assert sr["non-hp"]["delivery"] == pytest.approx(540.0)
    assert sr["non-hp"]["total"] == pytest.approx(640.0)
    assert sr["non-hp"]["supply"] == pytest.approx(100.0)


def test_write_revenue_requirement_yamls_round_trip(tmp_path: Path) -> None:
    """Delivery sums match total_delivery_rr, total == delivery + supply per subclass."""
    run1_dir = _write_sample_run_dir(tmp_path)
    run2_dir = _write_sample_run2_dir(tmp_path)
    delivery_breakdown = compute_subclass_rr(run1_dir)
    total_breakdown = compute_subclass_rr(run2_dir)
    differentiated_yaml = tmp_path / "config/rev_requirement/test_hp_vs_nonhp.yaml"
    default_yaml = tmp_path / "config/rev_requirement/test.yaml"

    gv_map = {"true": "hp", "false": "non-hp"}
    delivery_total = sum(
        float(row["revenue_requirement"]) for row in delivery_breakdown.to_dicts()
    )
    total_total = sum(
        float(row["revenue_requirement"]) for row in total_breakdown.to_dicts()
    )

    _write_revenue_requirement_yamls(
        delivery_breakdown,
        run_dir=run1_dir,
        group_col="has_hp",
        cross_subsidy_col="BAT_percustomer",
        utility="test",
        default_revenue_requirement=0.0,
        differentiated_yaml_path=differentiated_yaml,
        default_yaml_path=default_yaml,
        group_value_to_subclass=gv_map,
        total_breakdown=total_breakdown,
        total_delivery_rr=delivery_total,
        total_delivery_and_supply_rr=total_total,
    )

    data = yaml.safe_load(differentiated_yaml.read_text(encoding="utf-8"))
    sr = data["subclass_revenue_requirements"]

    sum_delivery = sum(v["delivery"] for v in sr.values())
    sum_total = sum(v["total"] for v in sr.values())

    assert sum_delivery == pytest.approx(data["total_delivery_revenue_requirement"])
    assert sum_total == pytest.approx(
        data["total_delivery_and_supply_revenue_requirement"]
    )

    for alias, vals in sr.items():
        assert vals["total"] == pytest.approx(vals["delivery"] + vals["supply"]), (
            f"Subclass {alias}: total != delivery + supply"
        )


def test_parse_group_value_to_subclass() -> None:
    result = parse_group_value_to_subclass("true=hp,false=non-hp")
    assert result == {"true": "hp", "false": "non-hp"}

    with pytest.raises(ValueError, match="Invalid"):
        parse_group_value_to_subclass("bad_format")

    with pytest.raises(ValueError, match="Duplicate"):
        parse_group_value_to_subclass("true=hp,true=non-hp")


def test_compute_hp_seasonal_discount_inputs(tmp_path: Path) -> None:
    # Use _write_sample_run_dir for bills/metadata (HP bldgs 1 and 3, FC=2):
    #   winter rev: (5-2) + (6-2) + (7-2) = 12.0 (Jan bldg1, Feb bldg3, Dec bldg3)
    #   summer rev: (8-2) = 6.0 (Jul bldg1)
    #   CS: BAT_percustomer bldg1=3, bldg3=4 → total 7.0
    # Use balanced loads (winter-heavy) so equivalent_flat_rate > winter_discount.
    run_dir = _write_sample_run_dir(tmp_path)
    tariff_path = _write_urdb_tariff(tmp_path / "base_tariff.json")

    # Custom loads: bldg 1 (Jan=200, Jul=100), bldg 3 (Feb=200, Dec=100)
    # winter_kwh = 200+200+100 = 500, summer_kwh = 100, total = 600
    resstock_base = tmp_path / "resstock"
    part = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 4, 3, 3],
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-02-01 00:00:00",
                "2025-12-01 00:00:00",
            ],
            "out.electricity.total.energy_consumption": [
                200.0,
                100.0,
                500.0,
                600.0,
                200.0,
                100.0,
            ],
            "out.electricity.pv.energy_consumption": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        }
    ).write_parquet(part / "loads.parquet")

    out = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        cross_subsidy_col="BAT_percustomer",
        base_tariff_json_path=tariff_path,
    )

    assert out.height == 1
    assert out["subclass"][0] == "true"
    # HP ids are 1 and 3 -> BAT_percustomer = 3 + 4 = 7 (weighted by 1 each)
    assert out["total_cross_subsidy_hp"][0] == pytest.approx(7.0)
    # Winter kWh (Oct-Mar): bldg 1 Jan=200, bldg 3 Feb=200 + Dec=100 → 500
    assert out["winter_kwh_hp"][0] == pytest.approx(500.0)
    # Summer kWh: bldg 1 Jul=100 → 100
    assert out["summer_kwh_hp"][0] == pytest.approx(100.0)
    # Annual energy revenue (fixed_charge=2.0, 12*FC=24):
    #   bldg 1: Annual=100, energy=100-24=76
    #   bldg 3: Annual=300, energy=300-24=276
    #   total = 352.0
    assert out["annual_energy_rev_hp"][0] == pytest.approx(352.0)
    # equivalent_flat_rate = 352.0 / 600.0; winter_discount = 7.0 / 500.0
    _flat = 352.0 / 600.0
    assert out["equivalent_flat_rate"][0] == pytest.approx(_flat)
    assert out["winter_discount"][0] == pytest.approx(7.0 / 500.0)
    assert out["summer_rate"][0] == pytest.approx(_flat)
    assert out["winter_rate_hp"][0] == pytest.approx(_flat - 7.0 / 500.0)
    assert out["annual_kwh_hp"][0] == pytest.approx(600.0)
    assert out["winter_months"][0] == "10,11,12,1,2,3"


def test_compute_hp_seasonal_discount_inputs_applies_weights(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "cross_subsidization").mkdir(parents=True)
    (run_dir / "bills").mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 9.0],
            "postprocess_group.has_hp": [True, True],
        }
    ).write_csv(run_dir / "customer_metadata.csv")
    pl.DataFrame({"bldg_id": [1, 2], "BAT_percustomer": [10.0, 10.0]}).write_csv(
        run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv"
    )
    # Bills: Jan (winter 100 kWh), Jul (summer 50 kWh), and Annual.
    # Tariff: FC=2, rate=0.21 → Jan bill=23, Jul bill=12.5.
    # Annual bill = 12*FC + rate*(winter_kwh + summer_kwh) = 24 + 0.21*150 = 55.5
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 1, 2, 2, 2],
            "month": ["Jan", "Jul", "Annual", "Jan", "Jul", "Annual"],
            "bill_level": [23.0, 12.5, 55.5, 23.0, 12.5, 55.5],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")
    tariff_path = _write_urdb_tariff(tmp_path / "base_tariff.json")

    resstock_base = tmp_path / "resstock"
    partition_dir = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    partition_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 2],
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
            ],
            "out.electricity.total.energy_consumption": [100.0, 50.0, 100.0, 50.0],
            "out.electricity.pv.energy_consumption": [0.0, 0.0, 0.0, 0.0],
        }
    ).write_parquet(partition_dir / "sample.parquet")

    out = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        cross_subsidy_col="BAT_percustomer",
        base_tariff_json_path=tariff_path,
    )
    # Weighted cross subsidy = 10*1 + 10*9 = 100
    assert out["total_cross_subsidy_hp"][0] == pytest.approx(100.0)
    # Weighted winter kWh = 100*1 + 100*9 = 1000
    assert out["winter_kwh_hp"][0] == pytest.approx(1000.0)
    # Weighted summer kWh = 50*1 + 50*9 = 500
    assert out["summer_kwh_hp"][0] == pytest.approx(500.0)
    # Annual energy revenue from Annual row (55.5 - 12*2 = 31.5 per bldg):
    #   weighted = 31.5*1 + 31.5*9 = 31.5 + 283.5 = 315.0
    assert out["annual_energy_rev_hp"][0] == pytest.approx(315.0)
    # equivalent_flat_rate = 315 / 1500 = 0.21 (flat tariff: same as single rate)
    assert out["equivalent_flat_rate"][0] == pytest.approx(0.21)
    # winter_discount = 100 / 1000 = 0.10
    assert out["winter_discount"][0] == pytest.approx(0.10)
    # winter_rate = 0.21 - 0.10 = 0.11
    assert out["winter_rate_hp"][0] == pytest.approx(0.11)
    # summer_rate = equivalent_flat_rate = 0.21
    assert out["summer_rate"][0] == pytest.approx(0.21)


def test_compute_hp_seasonal_discount_inputs_flat_equivalence(tmp_path: Path) -> None:
    """Revenue-based formula reproduces the old formula when the tariff is flat."""
    flat_rate = 0.21
    fixed_charge = 2.0
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 1.0],
            "postprocess_group.has_hp": [True, True],
        }
    ).write_csv(run_dir / "customer_metadata.csv")
    pl.DataFrame({"bldg_id": [1, 2], "BAT_percustomer": [5.0, 3.0]}).write_csv(
        run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv"
    )

    # Bills consistent with flat rate: bill = fixed_charge + rate * kwh.
    # Annual bill = 12*FC + rate*(winter_kwh + summer_kwh).
    winter_kwh_1, summer_kwh_1 = 200.0, 100.0
    winter_kwh_2, summer_kwh_2 = 300.0, 150.0
    annual_bill_1 = 12 * fixed_charge + flat_rate * (winter_kwh_1 + summer_kwh_1)
    annual_bill_2 = 12 * fixed_charge + flat_rate * (winter_kwh_2 + summer_kwh_2)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 1, 2, 2, 2],
            "month": ["Jan", "Jul", "Annual", "Jan", "Jul", "Annual"],
            "bill_level": [
                fixed_charge + flat_rate * winter_kwh_1,
                fixed_charge + flat_rate * summer_kwh_1,
                annual_bill_1,
                fixed_charge + flat_rate * winter_kwh_2,
                fixed_charge + flat_rate * summer_kwh_2,
                annual_bill_2,
            ],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")
    tariff_path = _write_urdb_tariff(
        tmp_path / "base_tariff.json", fixed_charge=fixed_charge, rate=flat_rate
    )

    resstock_base = tmp_path / "resstock"
    part = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 2],
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
            ],
            "out.electricity.total.energy_consumption": [
                winter_kwh_1,
                summer_kwh_1,
                winter_kwh_2,
                summer_kwh_2,
            ],
            "out.electricity.pv.energy_consumption": [0.0, 0.0, 0.0, 0.0],
        }
    ).write_parquet(part / "loads.parquet")

    out = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        cross_subsidy_col="BAT_percustomer",
        base_tariff_json_path=tariff_path,
    )

    total_cs = 5.0 + 3.0
    total_winter_kwh = winter_kwh_1 + winter_kwh_2
    total_summer_kwh = summer_kwh_1 + summer_kwh_2
    total_kwh = total_winter_kwh + total_summer_kwh
    # Annual energy revenue = (annual_bill - 12*FC) per building, summed.
    # annual_bill_i = 12*FC + flat_rate * (winter_kwh_i + summer_kwh_i)
    # → energy_i = flat_rate * (winter_kwh_i + summer_kwh_i)
    # → total = flat_rate * total_kwh
    expected_annual_energy_rev = flat_rate * total_kwh
    assert out["annual_energy_rev_hp"][0] == pytest.approx(expected_annual_energy_rev)
    # For flat tariffs: equivalent_flat_rate = flat_rate * total_kwh / total_kwh = flat_rate
    assert out["equivalent_flat_rate"][0] == pytest.approx(flat_rate)
    assert out["winter_discount"][0] == pytest.approx(total_cs / total_winter_kwh)
    assert out["summer_rate"][0] == pytest.approx(flat_rate)
    assert out["winter_rate_hp"][0] == pytest.approx(
        flat_rate - total_cs / total_winter_kwh
    )
    assert out["annual_kwh_hp"][0] == pytest.approx(total_kwh)


def test_compute_hp_seasonal_discount_inputs_structured_tariff(tmp_path: Path) -> None:
    """Blended flat rate is used even when bills reflect a structured tariff.

    Simulates a tariff where winter bills per kWh exceed summer bills per kWh
    (e.g. a seasonal tiered tariff). The new formula produces the blended rate
    for summer (not the season-specific summer rate), and the winter rate is the
    blended rate minus the cross-subsidy discount.
    """
    fixed_charge = 2.0
    # Two HP buildings with non-flat seasonal bills:
    # bldg 1: Jan (winter) 100 kWh billed at $0.30/kWh + FC, Jul (summer) 100 kWh at $0.15/kWh + FC
    # bldg 2: Jan (winter) 200 kWh at $0.30/kWh + FC, Jul (summer) 200 kWh at $0.15/kWh + FC
    winter_rate_filed = 0.30
    summer_rate_filed = 0.15
    winter_kwh_1, summer_kwh_1 = 100.0, 100.0
    winter_kwh_2, summer_kwh_2 = 200.0, 200.0
    cs_1, cs_2 = 4.0, 8.0

    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 1.0],
            "postprocess_group.has_hp": [True, True],
        }
    ).write_csv(run_dir / "customer_metadata.csv")
    pl.DataFrame({"bldg_id": [1, 2], "BAT_percustomer": [cs_1, cs_2]}).write_csv(
        run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv"
    )
    # Annual bill = 12*FC + winter_rate*winter_kwh + summer_rate*summer_kwh
    annual_bill_1 = (
        12 * fixed_charge
        + winter_rate_filed * winter_kwh_1
        + summer_rate_filed * summer_kwh_1
    )
    annual_bill_2 = (
        12 * fixed_charge
        + winter_rate_filed * winter_kwh_2
        + summer_rate_filed * summer_kwh_2
    )
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 1, 2, 2, 2],
            "month": ["Jan", "Jul", "Annual", "Jan", "Jul", "Annual"],
            "bill_level": [
                fixed_charge + winter_rate_filed * winter_kwh_1,
                fixed_charge + summer_rate_filed * summer_kwh_1,
                annual_bill_1,
                fixed_charge + winter_rate_filed * winter_kwh_2,
                fixed_charge + summer_rate_filed * summer_kwh_2,
                annual_bill_2,
            ],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")
    tariff_path = _write_urdb_tariff(
        tmp_path / "base_tariff.json", fixed_charge=fixed_charge, rate=summer_rate_filed
    )

    resstock_base = tmp_path / "resstock"
    part = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 2],
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
            ],
            "out.electricity.total.energy_consumption": [
                winter_kwh_1,
                summer_kwh_1,
                winter_kwh_2,
                summer_kwh_2,
            ],
            "out.electricity.pv.energy_consumption": [0.0, 0.0, 0.0, 0.0],
        }
    ).write_parquet(part / "loads.parquet")

    out = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        cross_subsidy_col="BAT_percustomer",
        base_tariff_json_path=tariff_path,
    )

    total_cs = cs_1 + cs_2
    total_winter_kwh = winter_kwh_1 + winter_kwh_2
    total_summer_kwh = summer_kwh_1 + summer_kwh_2
    total_kwh = total_winter_kwh + total_summer_kwh
    # Annual energy revenue = annual_bill - 12*FC per building
    annual_energy_rev = (annual_bill_1 - 12 * fixed_charge) + (
        annual_bill_2 - 12 * fixed_charge
    )
    expected_flat = annual_energy_rev / total_kwh

    # Blended flat rate is between the filed summer and winter rates, not equal to either.
    assert expected_flat != pytest.approx(summer_rate_filed)
    assert expected_flat != pytest.approx(winter_rate_filed)

    assert out["annual_energy_rev_hp"][0] == pytest.approx(annual_energy_rev)
    assert out["equivalent_flat_rate"][0] == pytest.approx(expected_flat)
    assert out["winter_discount"][0] == pytest.approx(total_cs / total_winter_kwh)
    # summer_rate is the blended rate, NOT the filed summer rate
    assert out["summer_rate"][0] == pytest.approx(expected_flat)
    assert out["winter_rate_hp"][0] == pytest.approx(
        expected_flat - total_cs / total_winter_kwh
    )
    # Revenue neutrality: CAIRO aggregate HP revenue at rate_unity=1 equals RR_HP
    total_annual_bills = annual_bill_1 + annual_bill_2
    rr_hp = total_annual_bills - total_cs
    rev_hp = (
        fixed_charge * 12 * 2  # 2 buildings, 12 months
        + out["summer_rate"][0] * total_summer_kwh
        + out["winter_rate_hp"][0] * total_winter_kwh
    )
    assert rev_hp == pytest.approx(rr_hp)


def test_compute_hp_seasonal_discount_inputs_raises_when_non_positive_rate(
    tmp_path: Path,
) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    resstock_base = _write_sample_resstock_loads_dir(tmp_path)
    # High fixed charge makes energy revenue negative → winter_rate < 0.
    tariff_path = _write_urdb_tariff(tmp_path / "base_tariff.json", fixed_charge=20.0)

    with pytest.raises(ValueError, match="Computed winter_rate_hp is negative"):
        compute_hp_seasonal_discount_inputs(
            run_dir=run_dir,
            resstock_base=str(resstock_base),
            state=_LOADS_STATE,
            upgrade=_LOADS_UPGRADE,
            cross_subsidy_col="BAT_percustomer",
            base_tariff_json_path=tariff_path,
        )


def test_compute_hp_seasonal_discount_inputs_raises_without_base_tariff(
    tmp_path: Path,
) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    resstock_base = _write_sample_resstock_loads_dir(tmp_path)

    with pytest.raises(ValueError, match="base_tariff_json_path is required"):
        compute_hp_seasonal_discount_inputs(
            run_dir=run_dir,
            resstock_base=str(resstock_base),
            state=_LOADS_STATE,
            upgrade=_LOADS_UPGRADE,
        )


def test_write_seasonal_inputs_csv_uses_output_dir_override(tmp_path: Path) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    tariff_path = _write_urdb_tariff(tmp_path / "base_tariff.json")
    # Balanced loads so equivalent_flat_rate > winter_discount (same layout as
    # test_compute_hp_seasonal_discount_inputs above).
    resstock_base = tmp_path / "resstock"
    part = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 4, 3, 3],
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-02-01 00:00:00",
                "2025-12-01 00:00:00",
            ],
            "out.electricity.total.energy_consumption": [
                200.0,
                100.0,
                500.0,
                600.0,
                200.0,
                100.0,
            ],
            "out.electricity.pv.energy_consumption": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        }
    ).write_parquet(part / "loads.parquet")
    seasonal_inputs = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        base_tariff_json_path=tariff_path,
    )
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    output_path = _write_seasonal_inputs_csv(
        seasonal_inputs=seasonal_inputs,
        run_dir=run_dir,
        output_dir=output_dir,
    )

    expected = output_dir / DEFAULT_SEASONAL_OUTPUT_FILENAME
    assert output_path == str(expected)
    assert expected.exists()


# =============================================================================
# Flat discount inputs tests
# =============================================================================


def test_compute_hp_flat_discount_inputs(tmp_path: Path) -> None:
    """Basic flat discount rate computation using the same fixtures as the seasonal test."""
    run_dir = _write_sample_run_dir(tmp_path)
    tariff_path = _write_urdb_tariff(tmp_path / "base_tariff.json")

    resstock_base = tmp_path / "resstock"
    part = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 4, 3, 3],
            "timestamp": [
                "2025-01-01 00:00:00",
                "2025-07-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-01-01 00:00:00",
                "2025-02-01 00:00:00",
                "2025-12-01 00:00:00",
            ],
            "out.electricity.total.energy_consumption": [
                200.0,
                100.0,
                500.0,
                600.0,
                200.0,
                100.0,
            ],
            "out.electricity.pv.energy_consumption": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        }
    ).write_parquet(part / "loads.parquet")

    out = compute_hp_flat_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        cross_subsidy_col="BAT_percustomer",
        base_tariff_json_path=tariff_path,
    )

    assert out.height == 1
    assert out["subclass"][0] == "true"
    # HP ids are 1 and 3 -> BAT_percustomer = 3 + 4 = 7 (weighted by 1 each)
    assert out["total_cross_subsidy_hp"][0] == pytest.approx(7.0)
    # Annual kWh HP: bldg 1 (200+100) + bldg 3 (200+100) = 600
    assert out["annual_kwh_hp"][0] == pytest.approx(600.0)
    # Annual energy revenue (fixed_charge=2.0, 12*FC=24):
    #   bldg 1: Annual=100, energy=100-24=76
    #   bldg 3: Annual=300, energy=300-24=276
    #   total = 352.0
    assert out["annual_energy_rev_hp"][0] == pytest.approx(352.0)
    assert out["fixed_charge"][0] == pytest.approx(2.0)
    # equivalent_flat_rate = 352 / 600, flat_discount = 7 / 600
    equiv = 352.0 / 600.0
    disc = 7.0 / 600.0
    assert out["equivalent_flat_rate"][0] == pytest.approx(equiv)
    assert out["flat_discount"][0] == pytest.approx(disc)
    assert out["flat_rate_hp"][0] == pytest.approx(equiv - disc)


def test_compute_hp_flat_discount_inputs_applies_weights(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "cross_subsidization").mkdir(parents=True)
    (run_dir / "bills").mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "month": ["Annual", "Annual"],
            "bill_level": [120.0, 200.0],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")
    pl.DataFrame({"bldg_id": [1, 2], "BAT_percustomer": [10.0, 20.0]}).write_csv(
        run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv"
    )
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [2.0, 3.0],
            "postprocess_group.has_hp": [True, True],
        }
    ).write_csv(run_dir / "customer_metadata.csv")

    tariff_path = _write_urdb_tariff(tmp_path / "tariff.json", fixed_charge=5.0)

    resstock_base = tmp_path / "resstock"
    part = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "timestamp": ["2025-06-01 00:00:00", "2025-06-01 00:00:00"],
            "out.electricity.total.energy_consumption": [100.0, 200.0],
            "out.electricity.pv.energy_consumption": [0.0, 0.0],
        }
    ).write_parquet(part / "loads.parquet")

    out = compute_hp_flat_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        base_tariff_json_path=tariff_path,
    )

    # weighted CS = 10*2 + 20*3 = 80
    assert out["total_cross_subsidy_hp"][0] == pytest.approx(80.0)
    # weighted kWh = 100*2 + 200*3 = 800
    assert out["annual_kwh_hp"][0] == pytest.approx(800.0)
    # energy rev: (120-60)*2 + (200-60)*3 = 120 + 420 = 540
    assert out["annual_energy_rev_hp"][0] == pytest.approx(540.0)
    # flat_rate_hp = (540 - 80) / 800 = 0.575
    assert out["flat_rate_hp"][0] == pytest.approx(0.575)


def test_compute_hp_flat_discount_inputs_raises_without_base_tariff(
    tmp_path: Path,
) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    resstock_base = _write_sample_resstock_loads_dir(tmp_path)
    with pytest.raises(ValueError, match="base_tariff_json_path is required"):
        compute_hp_flat_discount_inputs(
            run_dir=run_dir,
            resstock_base=str(resstock_base),
            state=_LOADS_STATE,
            upgrade=_LOADS_UPGRADE,
            base_tariff_json_path=None,
        )


def test_compute_hp_flat_discount_inputs_raises_when_negative_rate(
    tmp_path: Path,
) -> None:
    """Negative flat rate should raise ValueError."""
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)
    pl.DataFrame({"bldg_id": [1], "month": ["Annual"], "bill_level": [50.0]}).write_csv(
        run_dir / "bills" / "elec_bills_year_target.csv"
    )
    # Cross-subsidy larger than energy revenue forces negative rate
    pl.DataFrame({"bldg_id": [1], "BAT_percustomer": [1000.0]}).write_csv(
        run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv"
    )
    pl.DataFrame(
        {
            "bldg_id": [1],
            "weight": [1.0],
            "postprocess_group.has_hp": [True],
        }
    ).write_csv(run_dir / "customer_metadata.csv")
    tariff_path = _write_urdb_tariff(tmp_path / "tariff.json", fixed_charge=1.0)

    resstock_base = tmp_path / "resstock"
    part = (
        resstock_base
        / "load_curve_hourly"
        / f"state={_LOADS_STATE}"
        / f"upgrade={_LOADS_UPGRADE}"
    )
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1],
            "timestamp": ["2025-06-01 00:00:00"],
            "out.electricity.total.energy_consumption": [100.0],
            "out.electricity.pv.energy_consumption": [0.0],
        }
    ).write_parquet(part / "loads.parquet")

    with pytest.raises(ValueError, match="flat_rate_hp is negative"):
        compute_hp_flat_discount_inputs(
            run_dir=run_dir,
            resstock_base=str(resstock_base),
            state=_LOADS_STATE,
            upgrade=_LOADS_UPGRADE,
            base_tariff_json_path=tariff_path,
        )

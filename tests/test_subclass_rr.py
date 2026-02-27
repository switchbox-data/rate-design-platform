"""Tests for subclass revenue requirement postprocessing."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.mid.compute_subclass_rr import (
    DEFAULT_SEASONAL_OUTPUT_FILENAME,
    _load_run_fields,
    _write_revenue_requirement_yamls,
    _write_seasonal_inputs_csv,
    compute_hp_seasonal_discount_inputs,
    compute_subclass_rr,
    parse_group_value_to_subclass,
)


def _write_sample_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 2, 3, 3, 4, 4],
            "month": ["Jan", "Annual"] * 4,
            "bill_level": [5.0, 100.0, 10.0, 200.0, 15.0, 300.0, 20.0, 400.0],
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

    (run_dir / "tariff_final_config.json").write_text(
        '{"rie_a16":{"ur_ec_tou_mat":[[1,1,1e+38,0,0.21,0.0,0]]}}',
        encoding="utf-8",
    )
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
            "out.electricity.net.energy_consumption": [10.0, 999.0, 20.0, 30.0],
        }
    ).write_parquet(partition_dir / "sample_hp_loads.parquet")
    pl.DataFrame(
        {
            "bldg_id": [2, 4],
            "timestamp": ["2025-01-01 00:00:00", "2025-01-01 00:00:00"],
            "out.electricity.net.energy_consumption": [500.0, 600.0],
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
                        "utility_delivery_revenue_requirement": 241869601,
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


def test_write_revenue_requirement_yamls_new_format(tmp_path: Path) -> None:
    """Verify the new nested YAML format with delivery/supply/total per subclass."""
    run_dir = _write_sample_run_dir(tmp_path)
    breakdown = compute_subclass_rr(run_dir)
    differentiated_yaml = tmp_path / "config/rev_requirement/rie_hp_vs_nonhp.yaml"
    default_yaml = tmp_path / "config/rev_requirement/rie.yaml"

    gv_map = {"true": "hp", "false": "non-hp"}
    supply_mc = {"true": 50.0, "false": 100.0}

    out_diff, out_default = _write_revenue_requirement_yamls(
        breakdown,
        run_dir=run_dir,
        group_col="has_hp",
        cross_subsidy_col="BAT_percustomer",
        utility="rie",
        default_revenue_requirement=241869601.0,
        differentiated_yaml_path=differentiated_yaml,
        default_yaml_path=default_yaml,
        group_value_to_subclass=gv_map,
        supply_mc_by_subclass=supply_mc,
        total_delivery_rr=933.0,
        total_delivery_and_supply_rr=1083.0,
    )

    assert out_diff == differentiated_yaml
    assert differentiated_yaml.exists()

    diff_data = yaml.safe_load(differentiated_yaml.read_text(encoding="utf-8"))
    assert diff_data["utility"] == "rie"
    assert diff_data["group_col"] == "has_hp"
    assert diff_data["source_run_dir"] == str(run_dir)
    assert diff_data["total_delivery_revenue_requirement"] == pytest.approx(933.0)
    assert diff_data["total_delivery_and_supply_revenue_requirement"] == pytest.approx(
        1083.0
    )

    sr = diff_data["subclass_revenue_requirements"]
    assert "hp" in sr
    assert "non-hp" in sr
    assert sr["hp"]["delivery"] == pytest.approx(393.0)
    assert sr["hp"]["supply"] == pytest.approx(50.0)
    assert sr["hp"]["total"] == pytest.approx(443.0)
    assert sr["non-hp"]["delivery"] == pytest.approx(540.0)
    assert sr["non-hp"]["supply"] == pytest.approx(100.0)
    assert sr["non-hp"]["total"] == pytest.approx(640.0)


def test_write_revenue_requirement_yamls_round_trip(tmp_path: Path) -> None:
    """V2: delivery sums match total, and total == delivery + supply per subclass."""
    run_dir = _write_sample_run_dir(tmp_path)
    breakdown = compute_subclass_rr(run_dir)
    differentiated_yaml = tmp_path / "config/rev_requirement/test_hp_vs_nonhp.yaml"
    default_yaml = tmp_path / "config/rev_requirement/test.yaml"

    gv_map = {"true": "hp", "false": "non-hp"}
    supply_mc = {"true": 70.0, "false": 130.0}
    delivery_total = sum(
        float(row["revenue_requirement"]) for row in breakdown.to_dicts()
    )
    supply_total = sum(supply_mc.values())

    _write_revenue_requirement_yamls(
        breakdown,
        run_dir=run_dir,
        group_col="has_hp",
        cross_subsidy_col="BAT_percustomer",
        utility="test",
        default_revenue_requirement=0.0,
        differentiated_yaml_path=differentiated_yaml,
        default_yaml_path=default_yaml,
        group_value_to_subclass=gv_map,
        supply_mc_by_subclass=supply_mc,
        total_delivery_rr=delivery_total,
        total_delivery_and_supply_rr=delivery_total + supply_total,
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
    run_dir = _write_sample_run_dir(tmp_path)
    resstock_base = _write_sample_resstock_loads_dir(tmp_path)

    out = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        cross_subsidy_col="BAT_percustomer",
    )

    assert out.height == 1
    assert out["subclass"][0] == "true"
    assert out["default_rate"][0] == pytest.approx(0.21)
    # HP ids are 1 and 3 -> BAT_percustomer = 3 + 4 = 7 (weighted by 1 each)
    assert out["total_cross_subsidy_hp"][0] == pytest.approx(7.0)
    # Winter season (Oct-Mar) over this fixture still sums Jan + Feb + Dec = 60.
    assert out["winter_kwh_hp"][0] == pytest.approx(60.0)
    assert out["winter_rate_hp"][0] == pytest.approx(0.21 - (7.0 / 60.0))
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
    pl.DataFrame(
        {"bldg_id": [1, 2], "month": ["Annual", "Annual"], "bill_level": [0.0, 0.0]}
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")
    (run_dir / "tariff_final_config.json").write_text(
        '{"rie_a16":{"ur_ec_tou_mat":[[1,1,1e+38,0,0.2,0.0,0]]}}',
        encoding="utf-8",
    )

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
            "bldg_id": [1, 2],
            "timestamp": ["2025-01-01 00:00:00", "2025-01-01 00:00:00"],
            "out.electricity.net.energy_consumption": [100.0, 100.0],
        }
    ).write_parquet(partition_dir / "sample.parquet")

    out = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
        cross_subsidy_col="BAT_percustomer",
    )
    # Weighted cross subsidy = 10*1 + 10*9 = 100
    assert out["total_cross_subsidy_hp"][0] == pytest.approx(100.0)
    # Weighted winter kWh = 100*1 + 100*9 = 1000
    assert out["winter_kwh_hp"][0] == pytest.approx(1000.0)
    assert out["winter_rate_hp"][0] == pytest.approx(0.2 - (100.0 / 1000.0))


def test_compute_hp_seasonal_discount_inputs_raises_when_non_positive_rate(
    tmp_path: Path,
) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    resstock_base = _write_sample_resstock_loads_dir(tmp_path)
    # Force very low default rate so formula goes non-positive.
    (run_dir / "tariff_final_config.json").write_text(
        '{"rie_a16":{"ur_ec_tou_mat":[[1,1,1e+38,0,0.01,0.0,0]]}}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Computed winter_rate_hp is negative"):
        compute_hp_seasonal_discount_inputs(
            run_dir=run_dir,
            resstock_base=str(resstock_base),
            state=_LOADS_STATE,
            upgrade=_LOADS_UPGRADE,
            cross_subsidy_col="BAT_percustomer",
        )


def test_write_seasonal_inputs_csv_uses_output_dir_override(tmp_path: Path) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    resstock_base = _write_sample_resstock_loads_dir(tmp_path)
    seasonal_inputs = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_LOADS_STATE,
        upgrade=_LOADS_UPGRADE,
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

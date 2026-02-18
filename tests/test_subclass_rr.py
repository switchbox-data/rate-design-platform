"""Tests for subclass revenue requirement postprocessing."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.post.compute_subclass_rr import (
    _load_default_revenue_requirement,
    _write_revenue_requirement_yamls,
    compute_subclass_rr,
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
            "BAT_percustomer": [10.0, 20.0, 30.0, 40.0],
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


@pytest.mark.parametrize(
    ("group_col", "cross_subsidy_col", "expected"),
    [
        (
            "has_hp",
            "BAT_percustomer",
            {
                "false": (600.0, 60.0, 540.0),
                "true": (400.0, 40.0, 360.0),
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


def test_load_default_revenue_requirement_from_scenario_config(tmp_path: Path) -> None:
    scenario_config = tmp_path / "scenarios.yaml"
    scenario_config.write_text(
        yaml.safe_dump(
            {
                "runs": {
                    1: {
                        "utility": "rie",
                        "utility_delivery_revenue_requirement": 241869601,
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    utility, rr = _load_default_revenue_requirement(scenario_config, run_num=1)
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


def test_write_revenue_requirement_yamls(tmp_path: Path) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    breakdown = compute_subclass_rr(run_dir)
    differentiated_yaml = tmp_path / "config/rev_requirement/rie_hp_vs_nonhp.yaml"
    default_yaml = tmp_path / "config/rev_requirement/rie.yaml"

    out_diff, out_default = _write_revenue_requirement_yamls(
        breakdown,
        run_dir=run_dir,
        group_col="has_hp",
        cross_subsidy_col="BAT_percustomer",
        utility="rie",
        default_revenue_requirement=241869601.0,
        differentiated_yaml_path=differentiated_yaml,
        default_yaml_path=default_yaml,
    )

    assert out_diff == differentiated_yaml
    assert out_default == default_yaml
    assert differentiated_yaml.exists()
    assert default_yaml.exists()

    diff_data = yaml.safe_load(differentiated_yaml.read_text(encoding="utf-8"))
    assert diff_data["utility"] == "rie"
    assert diff_data["group_col"] == "has_hp"
    assert "false" in diff_data["subclass_revenue_requirements"]
    assert "true" in diff_data["subclass_revenue_requirements"]

    default_data = yaml.safe_load(default_yaml.read_text(encoding="utf-8"))
    assert default_data["utility"] == "rie"
    assert default_data["revenue_requirement"] == pytest.approx(241869601.0)

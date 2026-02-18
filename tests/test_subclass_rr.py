"""Tests for subclass revenue requirement postprocessing."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.post.compute_subclass_rr import (
    DEFAULT_OUTPUT_FILENAME,
    DEFAULT_SEASONAL_OUTPUT_FILENAME,
    _write_breakdown_csv,
    _write_seasonal_inputs_csv,
    compute_hp_seasonal_discount_inputs,
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
            "postprocess_group.has_hp": [True, False, True, False],
            "postprocess_group.heating_type": ["hp", "gas", "hp", "resistance"],
        }
    ).write_csv(run_dir / "customer_metadata.csv")

    (run_dir / "tariff_final_config.json").write_text(
        (
            '{"items":[{"energyratestructure":[[{"rate":0.2,"adj":0.01,"unit":"kWh"}]],'
            '"fixedchargefirstmeter":6.75}]}'
        ),
        encoding="utf-8",
    )
    return run_dir


def _write_sample_resstock_loads_dir(tmp_path: Path) -> Path:
    loads_dir = tmp_path / "loads"
    loads_dir.mkdir(parents=True)
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
            "total_fuel_electricity": [10.0, 999.0, 20.0, 30.0],
        }
    ).write_parquet(loads_dir / "sample_hp_loads.parquet")
    pl.DataFrame(
        {
            "bldg_id": [2, 4],
            "timestamp": ["2025-01-01 00:00:00", "2025-01-01 00:00:00"],
            "total_fuel_electricity": [500.0, 600.0],
        }
    ).write_parquet(loads_dir / "sample_nonhp_loads.parquet")
    return loads_dir


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
    pl.DataFrame({"bldg_id": [1], "postprocess_group.has_hp": [True]}).write_csv(
        run_dir / "customer_metadata.csv"
    )

    with pytest.raises(ValueError, match="Missing annual target bills"):
        compute_subclass_rr(run_dir)


def test_write_breakdown_csv_uses_output_dir_override(tmp_path: Path) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    breakdown = compute_subclass_rr(run_dir)
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    output_path = _write_breakdown_csv(
        breakdown,
        run_dir=run_dir,
        output_dir=output_dir,
    )

    expected = output_dir / DEFAULT_OUTPUT_FILENAME
    assert output_path == str(expected)
    assert expected.exists()


def test_compute_hp_seasonal_discount_inputs(tmp_path: Path) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    loads_dir = _write_sample_resstock_loads_dir(tmp_path)

    out = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_loads_path=loads_dir,
        cross_subsidy_col="BAT_percustomer",
    )

    assert out.height == 1
    assert out["subclass"][0] == "true"
    assert out["default_rate"][0] == pytest.approx(0.21)
    # HP ids are 1 and 3 -> BAT_percustomer = 10 + 30 = 40
    assert out["total_cross_subsidy_hp"][0] == pytest.approx(40.0)
    assert out["winter_kwh_hp"][0] == pytest.approx(60.0)
    assert out["winter_rate_hp"][0] == pytest.approx(0.21 - (40.0 / 60.0))
    assert out["winter_months"][0] == "12,1,2"


def test_write_seasonal_inputs_csv_uses_output_dir_override(tmp_path: Path) -> None:
    run_dir = _write_sample_run_dir(tmp_path)
    loads_dir = _write_sample_resstock_loads_dir(tmp_path)
    seasonal_inputs = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_loads_path=loads_dir,
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

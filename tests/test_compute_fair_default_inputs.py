"""Tests for fair default rate-design input computation."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from utils.mid.compute_fair_default_inputs import compute_fair_default_inputs

_STATE = "NY"
_UPGRADE = "00"
_FIXED_CHARGE = 2.0
_BASE_RATE = 0.5


def _write_urdb_tariff(path: Path) -> Path:
    tariff = {
        "items": [
            {
                "label": "test_tariff",
                "fixedchargefirstmeter": _FIXED_CHARGE,
                "energyratestructure": [[{"rate": _BASE_RATE, "unit": "kWh"}]],
                "energyweekdayschedule": [[0] * 24 for _ in range(12)],
                "energyweekendschedule": [[0] * 24 for _ in range(12)],
            }
        ]
    }
    path.write_text(json.dumps(tariff), encoding="utf-8")
    return path


def _write_fair_default_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    loads = {
        1: (100.0, 100.0, True),
        2: (200.0, 100.0, True),
        3: (100.0, 300.0, False),
        4: (100.0, 500.0, False),
    }
    pl.DataFrame(
        {
            "bldg_id": list(loads),
            "weight": [1.0, 1.0, 1.0, 1.0],
            "postprocess_group.has_hp": [values[2] for values in loads.values()],
        }
    ).write_csv(run_dir / "customer_metadata.csv")
    pl.DataFrame(
        {
            "bldg_id": list(loads),
            "BAT_percustomer": [20.0, 30.0, 0.0, 0.0],
        }
    ).write_csv(run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv")

    annual_bills = [
        12.0 * _FIXED_CHARGE + _BASE_RATE * (winter_kwh + summer_kwh)
        for winter_kwh, summer_kwh, _is_hp in loads.values()
    ]
    pl.DataFrame(
        {
            "bldg_id": list(loads),
            "month": ["Annual"] * len(loads),
            "bill_level": annual_bills,
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")

    resstock_base = tmp_path / "resstock"
    partition_dir = (
        resstock_base / "load_curve_hourly" / f"state={_STATE}" / f"upgrade={_UPGRADE}"
    )
    partition_dir.mkdir(parents=True)
    rows: list[dict[str, object]] = []
    for bldg_id, (winter_kwh, summer_kwh, _is_hp) in loads.items():
        rows.extend(
            [
                {
                    "bldg_id": bldg_id,
                    "timestamp": "2025-01-01 00:00:00",
                    "out.electricity.total.energy_consumption": winter_kwh,
                    "out.electricity.pv.energy_consumption": 0.0,
                },
                {
                    "bldg_id": bldg_id,
                    "timestamp": "2025-07-01 00:00:00",
                    "out.electricity.total.energy_consumption": summer_kwh,
                    "out.electricity.pv.energy_consumption": 0.0,
                },
            ]
        )
    pl.DataFrame(rows).write_parquet(partition_dir / "loads.parquet")

    tariff_path = _write_urdb_tariff(tmp_path / "base_tariff.json")
    return run_dir, resstock_base, tariff_path


def test_compute_fair_default_inputs_closed_forms(tmp_path: Path) -> None:
    run_dir, resstock_base, tariff_path = _write_fair_default_fixture(tmp_path)

    out = compute_fair_default_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        group_col="has_hp",
        subclass_value="true",
        cross_subsidy_col="BAT_percustomer",
        base_tariff_json_path=tariff_path,
        mc_seasonal_ratio=2.0,
    )

    assert out.height == 1
    rr = out["class_current_bill"][0]
    target = out["subclass_fair_bill"][0]
    assert rr == pytest.approx(846.0)
    assert out["subclass_current_bill"][0] == pytest.approx(298.0)
    assert out["subclass_cross_subsidy"][0] == pytest.approx(50.0)
    assert target == pytest.approx(248.0)
    assert out["class_customer_count"][0] == pytest.approx(4.0)
    assert out["subclass_customer_count"][0] == pytest.approx(2.0)
    assert out["class_winter_kwh"][0] == pytest.approx(500.0)
    assert out["class_summer_kwh"][0] == pytest.approx(1000.0)
    assert out["subclass_winter_kwh"][0] == pytest.approx(300.0)
    assert out["subclass_summer_kwh"][0] == pytest.approx(200.0)

    assert out["fixed_charge_only_fixed_charge"][0] == pytest.approx(-4.25)
    assert out["seasonal_rates_only_winter_rate"][0] == pytest.approx(0.25)
    assert out["seasonal_rates_only_summer_rate"][0] == pytest.approx(0.625)
    assert out["seasonal_rates_only_feasible"][0]
    assert out[
        "seasonal_rates_only_residual_cross_subsidy_after_clipping"
    ][0] == pytest.approx(
        0.0
    )
    assert out["fixed_plus_seasonal_mc_fixed_charge"][0] == pytest.approx(
        -18.833333333333332
    )
    assert out["fixed_plus_seasonal_mc_winter_rate"][0] == pytest.approx(1.75)
    assert out["fixed_plus_seasonal_mc_summer_rate"][0] == pytest.approx(0.875)
    assert not out["fixed_plus_seasonal_mc_feasible"][0]

    customer_count = out["class_customer_count"][0]
    subclass_customer_count = out["subclass_customer_count"][0]
    winter_kwh = out["class_winter_kwh"][0]
    summer_kwh = out["class_summer_kwh"][0]
    subclass_winter_kwh = out["subclass_winter_kwh"][0]
    subclass_summer_kwh = out["subclass_summer_kwh"][0]

    fixed_a = out["fixed_charge_only_fixed_charge"][0]
    class_energy_revenue = out["class_energy_revenue"][0]
    subclass_energy_revenue = out["subclass_energy_revenue"][0]
    lambda_a = (rr - 12.0 * fixed_a * customer_count) / class_energy_revenue
    assert (
        12.0 * fixed_a * customer_count + lambda_a * class_energy_revenue
    ) == pytest.approx(rr)
    assert (
        12.0 * fixed_a * subclass_customer_count + lambda_a * subclass_energy_revenue
    ) == pytest.approx(target)

    fixed_b = out["seasonal_rates_only_fixed_charge"][0]
    winter_b = out["seasonal_rates_only_winter_rate"][0]
    summer_b = out["seasonal_rates_only_summer_rate"][0]
    assert (
        12.0 * fixed_b * customer_count
        + winter_b * winter_kwh
        + summer_b * summer_kwh
    ) == pytest.approx(rr)
    assert (
        12.0 * fixed_b * subclass_customer_count
        + winter_b * subclass_winter_kwh
        + summer_b * subclass_summer_kwh
    ) == pytest.approx(target)

    fixed_c = out["fixed_plus_seasonal_mc_fixed_charge"][0]
    winter_c = out["fixed_plus_seasonal_mc_winter_rate"][0]
    summer_c = out["fixed_plus_seasonal_mc_summer_rate"][0]
    assert winter_c / summer_c == pytest.approx(2.0)
    assert (
        12.0 * fixed_c * customer_count
        + winter_c * winter_kwh
        + summer_c * summer_kwh
    ) == pytest.approx(rr)
    assert (
        12.0 * fixed_c * subclass_customer_count
        + winter_c * subclass_winter_kwh
        + summer_c * subclass_summer_kwh
    ) == pytest.approx(target)


def test_compute_fair_default_inputs_strategy_b_clipped_residual(
    tmp_path: Path,
) -> None:
    run_dir, resstock_base, tariff_path = _write_fair_default_fixture(tmp_path)
    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "BAT_percustomer": [-130.0, -130.0, 0.0, 0.0],
        }
    ).write_csv(run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv")

    out = compute_fair_default_inputs(
        run_dir=run_dir,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        group_col="has_hp",
        subclass_value="true",
        base_tariff_json_path=tariff_path,
    )

    assert not out["seasonal_rates_only_feasible"][0]
    assert out["seasonal_rates_only_summer_rate"][0] < 0.0
    assert out["seasonal_rates_only_clipped_summer_rate"][0] == pytest.approx(0.0)
    assert out["seasonal_rates_only_clipped_winter_rate"][0] == pytest.approx(1.5)
    assert out[
        "seasonal_rates_only_residual_cross_subsidy_after_clipping"
    ][0] == pytest.approx(-60.0)

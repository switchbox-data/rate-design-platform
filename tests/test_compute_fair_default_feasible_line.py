"""Tests for compute_feasible_line_from_runs (in compute_fair_default_inputs)."""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl
import pytest

from utils.mid.compute_fair_default_inputs import (
    FeasibleLineData,
    compute_feasible_line_from_runs,
    fixed_charge_feasibility,
)

_STATE = "NY"
_UPGRADE = "00"
_FIXED_CHARGE = 2.0
_BASE_RATE = 0.5

# Monthly-load test data: bldg_id → (winter_kwh, summer_kwh, is_hp)
# Winter months: Oct–Mar (10, 11, 12, 1, 2, 3) — 6 months
# Summer months: Apr–Sep (4, 5, 6, 7, 8, 9) — 6 months
_LOADS: dict[int, tuple[float, float, bool]] = {
    1: (100.0, 100.0, True),
    2: (200.0, 100.0, True),
    3: (100.0, 300.0, False),
    4: (100.0, 500.0, False),
}


def _write_urdb_tariff(path: Path, fixed_charge: float = _FIXED_CHARGE) -> Path:
    tariff = {
        "items": [
            {
                "label": "test_tariff",
                "fixedchargefirstmeter": fixed_charge,
                "energyratestructure": [[{"rate": _BASE_RATE, "unit": "kWh"}]],
                "energyweekdayschedule": [[0] * 24 for _ in range(12)],
                "energyweekendschedule": [[0] * 24 for _ in range(12)],
            }
        ]
    }
    path.write_text(json.dumps(tariff), encoding="utf-8")
    return path


def _write_run_dir(
    tmp_path: Path,
    name: str,
    cross_subsidies: list[float],
) -> Path:
    """Write a minimal CAIRO run dir with monthly loads fixture."""
    run_dir = tmp_path / name
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    bldg_ids = list(_LOADS)
    pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "weight": [1.0] * len(bldg_ids),
            "postprocess_group.has_hp": [v[2] for v in _LOADS.values()],
        }
    ).write_csv(run_dir / "customer_metadata.csv")

    pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "BAT_percustomer": cross_subsidies,
        }
    ).write_csv(
        run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv"
    )

    annual_bills = [
        12.0 * _FIXED_CHARGE + _BASE_RATE * (winter_kwh + summer_kwh)
        for winter_kwh, summer_kwh, _ in _LOADS.values()
    ]
    pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "month": ["Annual"] * len(bldg_ids),
            "bill_level": annual_bills,
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")

    return run_dir


def _write_monthly_loads(tmp_path: Path) -> Path:
    """Write load_curve_monthly parquets matching _LOADS."""
    resstock_base = tmp_path / "resstock"
    partition_dir = (
        resstock_base / "load_curve_monthly" / f"state={_STATE}" / f"upgrade={_UPGRADE}"
    )
    partition_dir.mkdir(parents=True)

    # 6 winter months (Jan=1, Feb=2, Mar=3, Oct=10, Nov=11, Dec=12)
    # 6 summer months (Apr=4, May=5, Jun=6, Jul=7, Aug=8, Sep=9)
    winter_months = (1, 2, 3, 10, 11, 12)
    summer_months = (4, 5, 6, 7, 8, 9)

    rows: list[dict[str, object]] = []
    for bldg_id, (winter_kwh, summer_kwh, _) in _LOADS.items():
        for m in winter_months:
            rows.append(
                {
                    "bldg_id": bldg_id,
                    "month": m,
                    "out.electricity.total.energy_consumption": winter_kwh / len(winter_months),
                    "out.electricity.pv.energy_consumption": 0.0,
                }
            )
        for m in summer_months:
            rows.append(
                {
                    "bldg_id": bldg_id,
                    "month": m,
                    "out.electricity.total.energy_consumption": summer_kwh / len(summer_months),
                    "out.electricity.pv.energy_consumption": 0.0,
                }
            )

    pl.DataFrame(rows).with_columns(pl.col("month").cast(pl.Int8)).write_parquet(
        partition_dir / "loads.parquet"
    )
    return resstock_base


def _build_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path]:
    """Return (run_dir_del, run_dir_sup, resstock_base, tariff_path)."""
    run_dir_del = _write_run_dir(tmp_path, "run1", [20.0, 30.0, 0.0, 0.0])
    run_dir_sup = _write_run_dir(tmp_path, "run2", [10.0, 15.0, 0.0, 0.0])
    resstock_base = _write_monthly_loads(tmp_path)
    tariff_del = _write_urdb_tariff(tmp_path / "tariff_del.json")
    tariff_sup = _write_urdb_tariff(tmp_path / "tariff_sup.json", fixed_charge=3.0)
    # Use same tariff for both variants in simpler delivery-only tests
    return run_dir_del, run_dir_sup, resstock_base, tariff_del


def test_compute_feasible_line_returns_dict_with_both_variants(tmp_path: Path) -> None:
    run_dir_del, run_dir_sup, resstock_base, tariff = _build_fixture(tmp_path)

    result = compute_feasible_line_from_runs(
        run_dir_delivery=run_dir_del,
        run_dir_supply=run_dir_sup,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        path_base_tariff_delivery=tariff,
        path_base_tariff_supply=tariff,
    )

    assert set(result) == {"delivery", "supply"}
    for variant, data in result.items():
        assert isinstance(data, FeasibleLineData), f"{variant} is not FeasibleLineData"


def test_compute_feasible_line_affine_evaluates_at_base_fixed_charge(tmp_path: Path) -> None:
    """r_win(base_F) and r_sum(base_F) match strategy B (seasonal_rates_only)."""
    run_dir_del, run_dir_sup, resstock_base, tariff = _build_fixture(tmp_path)

    result = compute_feasible_line_from_runs(
        run_dir_delivery=run_dir_del,
        run_dir_supply=run_dir_sup,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        path_base_tariff_delivery=tariff,
        path_base_tariff_supply=tariff,
    )

    data: FeasibleLineData = result["delivery"]

    # Strategy B is at base_fixed_charge; affine line evaluated there must equal B's rates.
    strategy_b = next(sp for sp in data.strategies if sp.label == "B")
    assert strategy_b.winter_rate is not None
    assert strategy_b.summer_rate is not None
    assert data.r_win.at(strategy_b.fixed_charge) == pytest.approx(
        strategy_b.winter_rate, rel=1e-6
    )
    assert data.r_sum.at(strategy_b.fixed_charge) == pytest.approx(
        strategy_b.summer_rate, rel=1e-6
    )


def test_compute_feasible_line_intercepts_match_feasibility(tmp_path: Path) -> None:
    """Intercepts derived via compute_feasible_line_from_runs match fixed_charge_feasibility."""
    from utils.mid.compute_fair_default_inputs import (
        CustomerGroupTotals,
        FairDefaultInputs,
        energy_revenue,
    )

    run_dir_del, run_dir_sup, resstock_base, tariff = _build_fixture(tmp_path)

    result = compute_feasible_line_from_runs(
        run_dir_delivery=run_dir_del,
        run_dir_supply=run_dir_sup,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        path_base_tariff_delivery=tariff,
        path_base_tariff_supply=tariff,
    )

    data = result["delivery"]

    # Reconstruct expected inputs from known test values.
    # class: 4 buildings, weight=1 each
    # annual_kwh: sum of all winter+summer: (100+100)+(200+100)+(100+300)+(100+500)=1500
    # winter_kwh: 100+200+100+100=500; summer_kwh=1000
    # subclass (has_hp=True): bldg 1+2
    # subclass winter: 100+200=300; summer: 100+100=200; annual: 500
    # cross_subsidy: 20+30=50; current_bill: delivery BAT values
    # current_bill = 12*2*(1+1+1+1) + 0.5*(100+100+200+100+100+300+100+500) = 96+846=... wait
    # bills are computed as 12*F + rate*(winter+summer) per building
    # bldg1: 12*2 + 0.5*200 = 24+100=124
    # bldg2: 12*2 + 0.5*300 = 24+150=174
    # bldg3: 12*2 + 0.5*400 = 24+200=224
    # bldg4: 12*2 + 0.5*600 = 24+300=324
    # class total: 124+174+224+324=846; subclass: 124+174=298

    class_bill = 846.0
    subclass_bill = 298.0
    cross_subsidy = 50.0
    subclass_fair_bill = subclass_bill - cross_subsidy  # 248

    class_totals = CustomerGroupTotals(
        customer_count=4.0,
        current_bill=class_bill,
        annual_kwh=1500.0,
        winter_kwh=500.0,
        summer_kwh=1000.0,
    )
    subclass_totals = CustomerGroupTotals(
        customer_count=2.0,
        current_bill=subclass_bill,
        annual_kwh=500.0,
        winter_kwh=300.0,
        summer_kwh=200.0,
    )
    inputs = FairDefaultInputs(
        class_totals=class_totals,
        subclass_totals=subclass_totals,
        subclass_cross_subsidy=cross_subsidy,
        base_fixed_charge=_FIXED_CHARGE,
        fixed_charge_floor=0.0,
    )
    feas = fixed_charge_feasibility(inputs)

    assert data.r_win.intercept == pytest.approx(feas.winter_rate_at_zero_fixed_charge, rel=1e-6)
    assert data.r_win.slope == pytest.approx(feas.winter_rate_per_fixed_charge_dollar, rel=1e-6)
    assert data.r_sum.intercept == pytest.approx(feas.summer_rate_at_zero_fixed_charge, rel=1e-6)
    assert data.r_sum.slope == pytest.approx(feas.summer_rate_per_fixed_charge_dollar, rel=1e-6)


def test_compute_feasible_line_strategy_c_skipped_without_mc_ratio(tmp_path: Path) -> None:
    """When mc_seasonal_ratio_* are None, no strategy C point is produced."""
    run_dir_del, run_dir_sup, resstock_base, tariff = _build_fixture(tmp_path)

    result = compute_feasible_line_from_runs(
        run_dir_delivery=run_dir_del,
        run_dir_supply=run_dir_sup,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        path_base_tariff_delivery=tariff,
        path_base_tariff_supply=tariff,
    )

    for variant, data in result.items():
        labels = {sp.label for sp in data.strategies}
        assert "C" not in labels, f"{variant} has unexpected strategy C"


def test_compute_feasible_line_strategy_c_present_with_mc_ratio(tmp_path: Path) -> None:
    """When mc_seasonal_ratio_* are supplied, strategy C is included."""
    run_dir_del, run_dir_sup, resstock_base, tariff = _build_fixture(tmp_path)

    result = compute_feasible_line_from_runs(
        run_dir_delivery=run_dir_del,
        run_dir_supply=run_dir_sup,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        path_base_tariff_delivery=tariff,
        path_base_tariff_supply=tariff,
        mc_seasonal_ratio_delivery=2.0,
        mc_seasonal_ratio_supply=1.8,
    )

    assert "C" in {sp.label for sp in result["delivery"].strategies}
    assert "C" in {sp.label for sp in result["supply"].strategies}


def test_compute_feasible_line_feasible_interval_finite(tmp_path: Path) -> None:
    """Feasible interval should be finite and well-ordered for typical inputs."""
    run_dir_del, run_dir_sup, resstock_base, tariff = _build_fixture(tmp_path)

    result = compute_feasible_line_from_runs(
        run_dir_delivery=run_dir_del,
        run_dir_supply=run_dir_sup,
        resstock_base=str(resstock_base),
        state=_STATE,
        upgrade=_UPGRADE,
        path_base_tariff_delivery=tariff,
        path_base_tariff_supply=tariff,
    )

    data = result["delivery"]
    if data.feasible_exists:
        assert math.isfinite(data.feasible_min) or math.isfinite(data.feasible_max)
        if math.isfinite(data.feasible_min) and math.isfinite(data.feasible_max):
            assert data.feasible_min <= data.feasible_max

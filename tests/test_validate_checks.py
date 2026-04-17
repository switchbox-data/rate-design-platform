from __future__ import annotations

import polars as pl

from utils.post.validate.checks import (
    bat_col_for_allocation,
    check_bat_near_zero,
    check_flex_subclass_revenue_expectations,
    check_hp_subclass_revenue_lower_with_flex,
    check_seasonal_winter_below_summer,
)


def test_bat_col_for_allocation_maps_methods() -> None:
    assert bat_col_for_allocation("percustomer") == "BAT_percustomer"
    assert bat_col_for_allocation("epmc") == "BAT_epmc"
    assert bat_col_for_allocation("volumetric") == "BAT_vol"
    assert bat_col_for_allocation(None) == "BAT_percustomer"


def test_bat_near_zero_uses_specified_metric() -> None:
    bat = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "BAT_percustomer": [500.0, -500.0],
            "BAT_epmc": [0.5, -0.5],
            "BAT_vol": [10.0, -10.0],
        }
    ).lazy()
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 1.0],
            "postprocess_group.has_hp": [True, False],
        }
    ).lazy()

    result_pc = check_bat_near_zero(bat, metadata, bat_metric="BAT_percustomer")
    assert result_pc.status == "FAIL"

    result_epmc = check_bat_near_zero(bat, metadata, bat_metric="BAT_epmc")
    assert result_epmc.status == "PASS"


def _seasonal_tou_schedule() -> list[list[int]]:
    schedule: list[list[int]] = []
    for month in range(12):
        if month in {9, 10, 11, 0, 1, 2}:  # Oct-Mar winter
            schedule.append([1] * 20 + [2] * 4)
        else:
            schedule.append([3] * 20 + [4] * 4)
    return schedule


def test_seasonal_tou_ordering_allows_summer_offpeak_below_winter_peak() -> None:
    tariff_config = {
        "utility_hp_seasonalTOU": {
            "ur_ec_sched_weekday": _seasonal_tou_schedule(),
            "ur_ec_tou_mat": [
                [1, 1, 1e38, 0, 0.048708, 0.0, 0],
                [2, 1, 1e38, 0, 0.062486, 0.0, 0],
                [3, 1, 1e38, 0, 0.047692, 0.0, 0],
                [4, 1, 1e38, 0, 0.090000, 0.0, 0],
            ],
        }
    }

    result = check_seasonal_winter_below_summer(tariff_config, run_num=9)

    assert result.status == "PASS"


def test_seasonal_tou_ordering_requires_summer_peak_above_winter_peak() -> None:
    tariff_config = {
        "utility_hp_seasonalTOU": {
            "ur_ec_sched_weekday": _seasonal_tou_schedule(),
            "ur_ec_tou_mat": [
                [1, 1, 1e38, 0, 0.040000, 0.0, 0],
                [2, 1, 1e38, 0, 0.080000, 0.0, 0],
                [3, 1, 1e38, 0, 0.050000, 0.0, 0],
                [4, 1, 1e38, 0, 0.070000, 0.0, 0],
            ],
        }
    }

    result = check_seasonal_winter_below_summer(tariff_config, run_num=9)

    assert result.status == "FAIL"
    assert "summer peak period 4" in result.message


def test_seasonal_check_skips_nonseasonal_companion_flat_tariff() -> None:
    tariff_config = {
        "utility_flat": {
            "ur_ec_sched_weekday": [[1] * 24 for _ in range(12)],
            "ur_ec_tou_mat": [[1, 1, 1e38, 0, 0.099080, 0.0, 0]],
        },
        "utility_hp_seasonalTOU": {
            "ur_ec_sched_weekday": _seasonal_tou_schedule(),
            "ur_ec_tou_mat": [
                [1, 1, 1e38, 0, 0.048708, 0.0, 0],
                [2, 1, 1e38, 0, 0.062486, 0.0, 0],
                [3, 1, 1e38, 0, 0.047692, 0.0, 0],
                [4, 1, 1e38, 0, 0.162280, 0.0, 0],
            ],
        },
    }

    result = check_seasonal_winter_below_summer(tariff_config, run_num=9)

    assert result.status == "PASS"


def _quarterly_seasonal_schedule() -> list[list[int]]:
    """RIE-style 3-period quarterly schedule: Jan-Mar / Apr-Jun / Jul-Dec."""
    schedule: list[list[int]] = []
    for month in range(12):
        if month in {0, 1, 2}:
            schedule.append([1] * 24)
        elif month in {3, 4, 5}:
            schedule.append([2] * 24)
        else:
            schedule.append([3] * 24)
    return schedule


def test_seasonal_check_skips_nonhp_default_structure_tariff() -> None:
    tariff_config = {
        "rie_nonhp_default": {
            "ur_ec_sched_weekday": _quarterly_seasonal_schedule(),
            "ur_ec_tou_mat": [
                [1, 1, 1e38, 0, 0.14191, 0.0, 0],
                [2, 1, 1e38, 0, 0.14078, 0.0, 0],
                [3, 1, 1e38, 0, 0.14752, 0.0, 0],
            ],
        },
        "rie_hp_seasonal": {
            "ur_ec_sched_weekday": _seasonal_tou_schedule(),
            "ur_ec_tou_mat": [
                [1, 1, 1e38, 0, 0.048708, 0.0, 0],
                [2, 1, 1e38, 0, 0.062486, 0.0, 0],
                [3, 1, 1e38, 0, 0.047692, 0.0, 0],
                [4, 1, 1e38, 0, 0.162280, 0.0, 0],
            ],
        },
    }

    result = check_seasonal_winter_below_summer(tariff_config, run_num=5)

    assert result.status == "PASS"


def _monthly_schedule() -> list[list[int]]:
    """Monthly tariff: each month has its own period (1-based, period = month + 1)."""
    return [[i + 1] * 24 for i in range(12)]


def test_seasonal_check_skips_monthly_tariff() -> None:
    """Monthly tariffs (12 distinct periods) should be skipped, not failed."""
    tariff_config = {
        "cenhud_non_electric_heating": {
            "ur_ec_sched_weekday": _monthly_schedule(),
            "ur_ec_tou_mat": [
                [i + 1, 1, 1e38, 0, 0.05 + i * 0.005, 0.0, 0] for i in range(12)
            ],
        }
    }

    result = check_seasonal_winter_below_summer(tariff_config, run_num=33)

    assert result.status == "PASS"


def test_flex_subclass_revenue_expectations_require_nonhp_neutral_and_hp_negative() -> (
    None
):
    bills = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "month": ["Annual", "Annual"],
            "bill_level": [99.7, 89.0],
        }
    ).lazy()
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 1.0],
            "postprocess_group.has_hp": [False, True],
        }
    ).lazy()
    subclass_rr = {
        "subclass_revenue_requirements": {
            "non-hp": {"delivery": 100.0, "total": 100.0},
            "hp": {"delivery": 100.0, "total": 100.0},
        }
    }

    result = check_flex_subclass_revenue_expectations(
        bills, metadata, subclass_rr, cost_scope="delivery"
    )

    assert result.status == "PASS"


def test_flex_subclass_revenue_expectations_fail_when_hp_exceeds_baseline() -> None:
    bills = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "month": ["Annual", "Annual"],
            "bill_level": [100.0, 101.0],
        }
    ).lazy()
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 1.0],
            "postprocess_group.has_hp": [False, True],
        }
    ).lazy()
    subclass_rr = {
        "subclass_revenue_requirements": {
            "non-hp": {"delivery": 100.0, "total": 100.0},
            "hp": {"delivery": 100.0, "total": 100.0},
        }
    }

    result = check_flex_subclass_revenue_expectations(
        bills, metadata, subclass_rr, cost_scope="delivery"
    )

    assert result.status == "FAIL"
    assert "hp should not exceed baseline RR" in result.message


def test_hp_subclass_revenue_lower_with_flex_passes_when_hp_revenue_drops() -> None:
    bills_noflex = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "month": ["Annual", "Annual"],
            "bill_level": [100.0, 120.0],
        }
    ).lazy()
    bills_flex = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "month": ["Annual", "Annual"],
            "bill_level": [100.0, 115.0],
        }
    ).lazy()
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 1.0],
            "postprocess_group.has_hp": [False, True],
        }
    ).lazy()

    result = check_hp_subclass_revenue_lower_with_flex(
        bills_noflex, bills_flex, metadata, metadata, 9, 13
    )

    assert result.status == "PASS"


def test_hp_subclass_revenue_lower_with_flex_fails_when_hp_revenue_rises() -> None:
    bills_noflex = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "month": ["Annual", "Annual"],
            "bill_level": [100.0, 120.0],
        }
    ).lazy()
    bills_flex = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "month": ["Annual", "Annual"],
            "bill_level": [100.0, 125.0],
        }
    ).lazy()
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 1.0],
            "postprocess_group.has_hp": [False, True],
        }
    ).lazy()

    result = check_hp_subclass_revenue_lower_with_flex(
        bills_noflex, bills_flex, metadata, metadata, 9, 13
    )

    assert result.status == "FAIL"


def test_flex_subclass_revenue_expectations_fail_when_hp_subclass_missing() -> None:
    bills = pl.DataFrame(
        {
            "bldg_id": [1],
            "month": ["Annual"],
            "bill_level": [99.7],
        }
    ).lazy()
    metadata = pl.DataFrame(
        {
            "bldg_id": [1],
            "weight": [1.0],
            "postprocess_group.has_hp": [False],
        }
    ).lazy()
    subclass_rr = {
        "subclass_revenue_requirements": {
            "non-hp": {"delivery": 100.0, "total": 100.0},
            "hp": {"delivery": 100.0, "total": 100.0},
        }
    }

    result = check_flex_subclass_revenue_expectations(
        bills, metadata, subclass_rr, cost_scope="delivery"
    )

    assert result.status == "FAIL"
    assert "missing subclasses hp" in result.message


# ---------------------------------------------------------------------------
# _maybe_downgrade_bat_near_zero
# ---------------------------------------------------------------------------


def test_bat_near_zero_downgraded_to_warn_for_mixed_alloc() -> None:
    from utils.post.validate.__main__ import _maybe_downgrade_bat_near_zero
    from utils.post.validate.checks import CheckResult

    failing = CheckResult(
        name="bat_near_zero", status="FAIL", message="EPMC BAT — hp: $+500.0"
    )

    result = _maybe_downgrade_bat_near_zero(
        failing,
        cost_scope="delivery+supply",
        residual_allocation_delivery="epmc",
        residual_allocation_supply="percustomer",
    )

    assert result.status == "WARN"
    assert "mixed delivery/supply allocation" in result.message
    assert "epmc vs percustomer" in result.message
    assert result.name == "bat_near_zero"


def test_bat_near_zero_not_downgraded_when_same_alloc() -> None:
    from utils.post.validate.__main__ import _maybe_downgrade_bat_near_zero
    from utils.post.validate.checks import CheckResult

    failing = CheckResult(
        name="bat_near_zero", status="FAIL", message="EPMC BAT — hp: $+500.0"
    )

    result = _maybe_downgrade_bat_near_zero(
        failing,
        cost_scope="delivery+supply",
        residual_allocation_delivery="epmc",
        residual_allocation_supply="epmc",
    )

    assert result.status == "FAIL"


def test_bat_near_zero_not_downgraded_for_delivery_only_run() -> None:
    from utils.post.validate.__main__ import _maybe_downgrade_bat_near_zero
    from utils.post.validate.checks import CheckResult

    failing = CheckResult(
        name="bat_near_zero", status="FAIL", message="EPMC BAT — hp: $+500.0"
    )

    result = _maybe_downgrade_bat_near_zero(
        failing,
        cost_scope="delivery",
        residual_allocation_delivery="epmc",
        residual_allocation_supply="percustomer",
    )

    assert result.status == "FAIL"


def test_bat_near_zero_pass_unchanged_for_mixed_alloc() -> None:
    from utils.post.validate.__main__ import _maybe_downgrade_bat_near_zero
    from utils.post.validate.checks import CheckResult

    passing = CheckResult(
        name="bat_near_zero", status="PASS", message="EPMC BAT — all near zero"
    )

    result = _maybe_downgrade_bat_near_zero(
        passing,
        cost_scope="delivery+supply",
        residual_allocation_delivery="epmc",
        residual_allocation_supply="percustomer",
    )

    assert result.status == "PASS"

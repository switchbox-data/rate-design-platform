from __future__ import annotations

import polars as pl

from utils.post.validate.checks import (
    check_flex_subclass_revenue_expectations,
    check_hp_subclass_revenue_lower_with_flex,
    check_seasonal_winter_below_summer,
)


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

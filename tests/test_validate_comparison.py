from __future__ import annotations

import polars as pl

from utils.post.validate.comparison import (
    _check_gap_directionality,
    _check_role_directionality,
    resolve_ny_hp_only_vs_electrified_pairs,
)
from utils.post.validate.config import RunConfig


def _config(run_num: int) -> RunConfig:
    return RunConfig(
        run_num=run_num,
        run_name=f"run_{run_num}",
        run_type="precalc" if run_num % 2 == 1 else "default",
        upgrade="0" if run_num in {5, 6, 29, 30, 33, 34} else "2",
        cost_scope="delivery" if run_num % 2 == 1 else "delivery+supply",
        has_subclasses=run_num in {5, 6, 29, 30, 33, 34},
        tariff_type="flat"
        if run_num in {29, 30, 31, 32}
        else "seasonal",
        elasticity=0.0,
        path_resstock_loads="",
        path_dist_and_sub_tx_mc="",
        path_bulk_tx_mc=None,
        path_supply_energy_mc="",
        path_supply_capacity_mc="",
    )


def test_resolve_ny_hp_only_vs_electrified_pairs_matches_flat_and_seasonal_runs() -> (
    None
):
    configs = {run_num: _config(run_num) for run_num in list(range(5, 9)) + list(range(29, 37))}

    families = resolve_ny_hp_only_vs_electrified_pairs(configs)

    assert [
        (family.name, family.hp_only_runs, family.electrified_runs)
        for family in families
    ] == [
        ("flat_percustomer", (5, 6, 7, 8), (29, 30, 31, 32)),
        ("seasonal_percustomer", (5, 6, 7, 8), (33, 34, 35, 36)),
    ]


def test_gap_directionality_passes_when_only_magnitude_changes() -> None:
    hp_only = {
        "subclass": ["hp", "non-hp"],
        "rr_value": [120.0, 100.0],
    }
    electrified = {
        "subclass": ["electric_heating", "non_electric_heating"],
        "rr_value": [130.0, 90.0],
    }

    result = _check_gap_directionality(
        subject="flat_delivery_revenue_requirement",
        metric_frame_hp=pl.DataFrame(hp_only),
        metric_frame_elec=pl.DataFrame(electrified),
        hp_focus="hp",
        hp_other="non-hp",
        elec_focus="electric_heating",
        elec_other="non_electric_heating",
        metric_cols=["rr_value"],
        run_nums=[5, 29],
    )

    assert result.status == "PASS"


def test_gap_directionality_fails_when_ordering_flips() -> None:
    hp_only = {
        "subclass": ["hp", "non-hp"],
        "rr_value": [80.0, 100.0],
    }
    electrified = {
        "subclass": ["electric_heating", "non_electric_heating"],
        "rr_value": [130.0, 90.0],
    }

    result = _check_gap_directionality(
        subject="flat_delivery_revenue_requirement",
        metric_frame_hp=pl.DataFrame(hp_only),
        metric_frame_elec=pl.DataFrame(electrified),
        hp_focus="hp",
        hp_other="non-hp",
        elec_focus="electric_heating",
        elec_other="non_electric_heating",
        metric_cols=["rr_value"],
        run_nums=[5, 29],
    )

    assert result.status == "FAIL"
    assert "flips sign" in result.message


def test_role_directionality_fails_when_subclass_sign_flips() -> None:
    hp_only = {
        "subclass": ["hp", "non-hp"],
        "bill_delta": [-25.0, 12.0],
    }
    electrified = {
        "subclass": ["electric_heating", "non_electric_heating"],
        "bill_delta": [10.0, 15.0],
    }

    result = _check_role_directionality(
        subject="flat_delivery_bill_delta",
        metric_frame_hp=pl.DataFrame(hp_only),
        metric_frame_elec=pl.DataFrame(electrified),
        hp_alias_map={"focus": "hp", "other": "non-hp"},
        elec_alias_map={
            "focus": "electric_heating",
            "other": "non_electric_heating",
        },
        metric_cols=["bill_delta"],
        run_nums=[7, 31],
    )

    assert result.status == "FAIL"
    assert "focus bill_delta flips sign" in result.message

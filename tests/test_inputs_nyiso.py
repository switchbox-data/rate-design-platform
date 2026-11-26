"""Tests for the NYISO bulk-transmission input loader."""

from __future__ import annotations

import polars as pl
import pytest

from utils.data_prep.marginal_costs.inputs.nyiso import (
    build_nested_locality_load_profiles,
    compute_paying_locality_costs,
    icap_components,
)


def test_paying_locality_cost_is_the_mean() -> None:
    table = pl.DataFrame(
        {
            "nested_localities_str": ["LI", "LI"],
            "constraint_group": ["a", "b"],
            "v_constraint_group_kw_yr": [10.0, 30.0],
            "tightest_nested_locality": ["LI", "LI"],
            "paying_localities": ["LI", "LI|NYC"],
        }
    )

    costs = compute_paying_locality_costs(table)

    assert costs["LI"] == pytest.approx(20.0)
    assert costs["NYC"] == pytest.approx(30.0)


def test_nested_locality_profile_sums_its_zones() -> None:
    loads = pl.DataFrame(
        {
            "timestamp": [1, 1, 2],
            "zone": ["N.Y.C.", "LONGIL", "N.Y.C."],
            "load_mw": [4.0, 6.0, 5.0],
        }
    )

    profiles = build_nested_locality_load_profiles(loads, ["NYC", "LI"])

    assert profiles["NYC"]["load_mw"].to_list() == [4.0, 5.0]
    assert profiles["LI"]["load_mw"].to_list() == [6.0]


def test_icap_components_weight_the_paying_locality() -> None:
    rows = pl.DataFrame(
        {
            "icap_locality": ["LI"],
            "gen_capacity_zone": ["LI"],
            "capacity_weight": [0.5],
        }
    )
    profile = pl.DataFrame({"timestamp": [1], "load_mw": [8.0]})

    components = icap_components(rows, {"LI": 40.0}, {"LI": profile})

    assert components[0]["annual_cost_kw_year"] == pytest.approx(20.0)
    assert components[0]["load"]["load_mw"].item() == 8.0

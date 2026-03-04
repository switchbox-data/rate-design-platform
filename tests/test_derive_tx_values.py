"""Tests for NY bulk-tx derivation naming and aggregation behavior."""

from __future__ import annotations

import polars as pl
import pytest

from data.nyiso.transmission.derive_tx_values import (
    annotate_constraint_group_paying_localities,
    assign_constraint_groups_to_paying_localities,
    collapse_scenario_variants,
    compute_constraint_group_secant_vavg,
    compute_paying_locality_vavg,
    nested_localities_to_str,
    parse_nested_localities,
    paying_localities_for_nested_localities_str,
    select_tightest_nested_locality,
)


def test_parse_nested_localities_and_canonical_string() -> None:
    parsed = parse_nested_localities("NYCA|LHV|NYC")
    assert parsed == frozenset({"NYCA", "LHV", "NYC"})
    assert nested_localities_to_str(parsed) == "LHV|NYC|NYCA"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("NYCA", ["ROS"]),
        ("LHV|NYCA", ["LHV"]),
        ("LHV|NYC|NYCA", ["NYC", "LHV"]),
        ("LI|NYCA", ["LI"]),
    ],
)
def test_paying_locality_mapping(raw: str, expected: list[str]) -> None:
    assert paying_localities_for_nested_localities_str(raw) == expected


@pytest.mark.parametrize(
    ("tokens", "expected"),
    [
        (frozenset({"NYCA"}), "NYCA"),
        (frozenset({"NYCA", "LI"}), "LI"),
        (frozenset({"NYCA", "LHV"}), "LHV"),
        (frozenset({"NYCA", "LHV", "NYC"}), "NYC"),
    ],
)
def test_tightest_nested_locality(tokens: frozenset[str], expected: str) -> None:
    assert select_tightest_nested_locality(tokens) == expected


def test_collapse_variants() -> None:
    df = pl.DataFrame(
        {
            "project": ["p1", "p1", "p2"],
            "delta_mw": [100.0, 100.0, 200.0],
            "nested_localities_str": ["NYCA", "NYCA", "NYCA"],
            "constraint_group": ["g1", "g1", "g1"],
            "annual_benefit_m_yr": [10.0, 14.0, 40.0],
        }
    )
    collapsed = collapse_scenario_variants(df)
    assert collapsed.height == 2
    p1 = collapsed.filter(pl.col("project") == "p1")["mean_benefit_m_yr"][0]
    assert float(p1) == 12.0


def test_compute_constraint_group_secant_vavg_and_annotation() -> None:
    collapsed = pl.DataFrame(
        {
            "project": ["p1", "p2"],
            "delta_mw": [100.0, 100.0],
            "nested_localities_str": ["LHV|NYC|NYCA", "LHV|NYC|NYCA"],
            "constraint_group": ["mmu", "mmu"],
            "mean_benefit_m_yr": [10.0, 30.0],
        }
    )
    group_df = compute_constraint_group_secant_vavg(collapsed)
    assert group_df.height == 1

    row = group_df.row(0, named=True)
    assert row["constraint_group"] == "mmu"
    assert row["nested_localities_str"] == "LHV|NYC|NYCA"
    assert row["tightest_nested_locality"] == "NYC"
    assert row["v_constraint_group_kw_yr"] == 150.0

    annotated = annotate_constraint_group_paying_localities(group_df)
    assert annotated["paying_localities"][0] == "NYC|LHV"


def test_assign_and_compute_paying_locality_vavg() -> None:
    # Covers all required paying localities ROS/LHV/NYC/LI
    group_df = pl.DataFrame(
        {
            "nested_localities_str": ["NYCA", "LHV|NYCA", "LHV|NYC|NYCA", "LI|NYCA"],
            "constraint_group": ["g_ros", "g_lhv", "g_nyc", "g_li"],
            "v_constraint_group_kw_yr": [10.0, 20.0, 30.0, 40.0],
            "n_points": [1, 1, 1, 1],
            "project_list": ["p1", "p2", "p3", "p4"],
            "tightest_nested_locality": ["NYCA", "LHV", "NYC", "LI"],
            "paying_localities": ["ROS", "LHV", "NYC|LHV", "LI"],
        }
    )

    contributions = assign_constraint_groups_to_paying_localities(group_df)
    assert contributions["ROS"] == [10.0]
    assert contributions["LI"] == [40.0]
    assert sorted(contributions["LHV"]) == [20.0, 30.0]
    assert contributions["NYC"] == [30.0]

    zones = compute_paying_locality_vavg(contributions).sort("gen_capacity_zone")
    as_map = {row["gen_capacity_zone"]: row["v_avg_kw_yr"] for row in zones.to_dicts()}
    assert as_map["ROS"] == 10.0
    assert as_map["LHV"] == 25.0
    assert as_map["NYC"] == 30.0
    assert as_map["LI"] == 40.0


def test_invalid_nested_locality_raises() -> None:
    with pytest.raises(ValueError, match="Invalid nested locality tokens"):
        parse_nested_localities("NYCA|BAD")

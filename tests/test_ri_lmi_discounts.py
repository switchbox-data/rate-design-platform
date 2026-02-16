"""Unit tests for RI LMI postprocessing (FPL, tier assignment, participation, discounts)."""

from __future__ import annotations

import polars as pl

from utils.post.lmi_common import (
    assign_ri_tier_expr,
    compute_fpl_threshold,
    discount_fractions_for_ri,
    fpl_pct_expr,
    fpl_threshold_expr,
    load_fpl_guidelines,
    load_ri_lidr_config,
    parse_occupants_expr,
    participation_uniform_expr,
    select_participants_weighted,
)


def test_compute_fpl_threshold() -> None:
    base, inc = 15060, 5380
    assert compute_fpl_threshold(1, base, inc) == 15060
    assert compute_fpl_threshold(2, base, inc) == 15060 + 5380
    assert compute_fpl_threshold(4, base, inc) == 15060 + 3 * 5380


def test_parse_occupants_expr() -> None:
    df = pl.DataFrame(
        {
            "in.occupants": ["1", "2", "10+", "5", "10"],
        }
    )
    out = df.with_columns(parse_occupants_expr("in.occupants").alias("n"))
    assert out["n"].to_list() == [1, 2, 10, 5, 10]


def test_fpl_pct_expr() -> None:
    df = pl.DataFrame(
        {
            "income": [15060.0, 30120.0, 0.0],
            "threshold": [15060.0, 15060.0, 15060.0],
        }
    )
    out = df.with_columns(fpl_pct_expr("income", pl.col("threshold")).alias("fpl_pct"))
    assert out["fpl_pct"].to_list() == [100.0, 200.0, 0.0]


def test_assign_ri_tier_expr() -> None:
    df = pl.DataFrame({"fpl_pct": [50.0, 75.0, 100.0, 150.0, 200.0, 250.0, 300.0]})
    out = df.with_columns(assign_ri_tier_expr("fpl_pct").alias("tier"))
    # Tier 3 <= 75, Tier 2 <= 150, Tier 1 <= 250, 0 > 250
    assert out["tier"].to_list() == [3, 3, 2, 2, 1, 1, 0]


def test_load_fpl_guidelines() -> None:
    fpl = load_fpl_guidelines(2024)
    assert fpl["base"] == 15060
    assert fpl["increment"] == 5380


def test_fpl_threshold_expr() -> None:
    df = pl.DataFrame({"occupants_num": [1, 2, 4]})
    base, inc = 15060, 5380
    out = df.with_columns(
        fpl_threshold_expr("occupants_num", base, inc).alias("thresh")
    )
    assert out["thresh"].to_list() == [15060, 15060 + 5380, 15060 + 3 * 5380]


def test_discount_fractions_for_ri() -> None:
    elec, gas = discount_fractions_for_ri()
    assert elec[3] == 0.60
    assert elec[2] == 0.30
    assert elec[1] == 0.10
    assert gas[3] == 0.60


def test_participation_uniform_expr() -> None:
    df = pl.DataFrame(
        {
            "bldg_id": list(range(100)),
            "eligible": [True] * 50 + [False] * 50,
        }
    )
    out = df.with_columns(
        participation_uniform_expr("bldg_id", 0.5, 42, pl.col("eligible")).alias(
            "participates"
        )
    )
    # Only eligible can participate; ~50% of eligible
    part = out.filter(pl.col("participates"))
    assert part.height <= 50
    assert part.filter(pl.col("eligible")).height == part.height


def test_select_participants_weighted() -> None:
    eligible_df = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            "fpl_pct": [30.0, 60.0, 90.0, 120.0, 150.0],
            "tier": [3, 3, 2, 2, 2],
            "weight": [1 / 30, 1 / 60, 1 / 90, 1 / 120, 1 / 150],
        }
    )
    out = select_participants_weighted(
        eligible_df, rate=0.4, seed=42, weight_col="weight"
    )
    assert "participates" in out.columns
    assert out["participates"].sum() >= 1
    assert out["participates"].sum() <= 3


def test_load_ri_lidr_config() -> None:
    config = load_ri_lidr_config()
    assert config["max_eligible_fpl"] == 250
    tiers = config["tiers"]
    assert len(tiers) == 3
    assert tiers[0]["fpl_upper_bound"] == 75
    assert tiers[0]["electric_discount_pct"] == 0.60


def test_occupants_cap() -> None:
    df = pl.DataFrame({"in.occupants": ["10+", "99"]})
    out = df.with_columns(parse_occupants_expr("in.occupants").alias("n"))
    # 10+ -> 10 (cap); 99 parses and is clipped to 10
    assert out["n"].to_list() == [10, 10]

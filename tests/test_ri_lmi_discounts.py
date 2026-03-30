"""Unit tests for RI LMI postprocessing (FPL, tier assignment, participation, discounts)."""

from __future__ import annotations

import polars as pl

from utils.post.lmi_common import (
    assign_ri_gas_tier_expr,
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
    assert gas[2] == 0.30
    assert gas[1] == 0.25
    assert 3 not in gas


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
    assert config["electric"]["max_eligible_fpl"] == 250
    elec_tiers = config["electric"]["tiers"]
    assert len(elec_tiers) == 3
    assert elec_tiers[0]["fpl_upper_bound"] == 75
    assert elec_tiers[0]["discount_pct"] == 0.60
    gas_tiers = config["gas"]["tiers"]
    assert len(gas_tiers) == 2
    assert gas_tiers[0]["fpl_upper_bound"] == 138
    assert gas_tiers[0]["discount_pct"] == 0.30


def test_occupants_cap() -> None:
    df = pl.DataFrame({"in.occupants": ["10+", "99"]})
    out = df.with_columns(parse_occupants_expr("in.occupants").alias("n"))
    # 10+ -> 10 (cap); 99 parses and is clipped to 10
    assert out["n"].to_list() == [10, 10]


def test_gas_tier_zeroed_without_gas_service() -> None:
    """Buildings without gas service (sb.gas_utility=None) must get gas_lmi_tier=0.

    This mirrors the masking logic in _build_ri_raw_tiers: after assigning gas
    tiers from FPL%, the tier is overridden to 0 for buildings where
    sb.gas_utility is null. The test covers all FPL ranges (below tier-2 cutoff,
    between tiers, and above the eligibility ceiling) to confirm the mask is
    applied regardless of income.
    """
    # Three buildings: two below the gas eligibility ceiling (FPL <= 185%),
    # one above. The first two would normally get gas tiers 2 and 1 respectively,
    # but none have gas service.
    df = pl.DataFrame(
        {
            "fpl_pct": [100.0, 160.0, 250.0],
            "sb.gas_utility": [None, None, None],
        }
    )
    out = df.with_columns(
        assign_ri_gas_tier_expr("fpl_pct").alias("gas_tier_raw"),
    ).with_columns(
        pl.when(pl.col("sb.gas_utility").is_not_null())
        .then(pl.col("gas_tier_raw"))
        .otherwise(pl.lit(0))
        .alias("gas_lmi_tier"),
        (
            pl.when(pl.col("sb.gas_utility").is_not_null())
            .then(pl.col("gas_tier_raw"))
            .otherwise(pl.lit(0))
            >= 1
        ).alias("is_lmi_gas"),
    )

    # Raw assignment would give tiers 2, 1, 0 — but masking must zero them all
    assert out["gas_tier_raw"].to_list() == [2, 1, 0]
    assert out["gas_lmi_tier"].to_list() == [0, 0, 0]
    assert out["is_lmi_gas"].to_list() == [False, False, False]


def test_gas_tier_preserved_with_gas_service() -> None:
    """Buildings WITH gas service keep their income-based gas tier assignment."""
    df = pl.DataFrame(
        {
            "fpl_pct": [100.0, 160.0, 250.0],
            "sb.gas_utility": ["rie", "rie", "rie"],
        }
    )
    out = df.with_columns(
        assign_ri_gas_tier_expr("fpl_pct").alias("gas_tier_raw"),
    ).with_columns(
        pl.when(pl.col("sb.gas_utility").is_not_null())
        .then(pl.col("gas_tier_raw"))
        .otherwise(pl.lit(0))
        .alias("gas_lmi_tier"),
        (
            pl.when(pl.col("sb.gas_utility").is_not_null())
            .then(pl.col("gas_tier_raw"))
            .otherwise(pl.lit(0))
            >= 1
        ).alias("is_lmi_gas"),
    )

    # <= 138% FPL -> tier 2 (30%), 138-185% FPL -> tier 1 (25%), > 185% -> 0
    assert out["gas_lmi_tier"].to_list() == [2, 1, 0]
    assert out["is_lmi_gas"].to_list() == [True, True, False]


def test_participation_preserves_gas_tier_zero_for_non_gas_buildings() -> None:
    """_sample_ri_participation must not promote non-gas buildings to gas participants.

    When raw_tiers already has gas_lmi_tier_raw=0 for a building (because it
    has no gas service), _sample_ri_participation must keep gas_lmi_tier=0 and
    is_lmi_gas=False even when the building participates (is elec-eligible).
    """
    from utils.post.apply_ri_lmi_discounts_to_bills import _sample_ri_participation

    raw_tiers = pl.DataFrame(
        {
            "bldg_id": [1001, 1002],
            # bldg 1001: has gas service -> gas_lmi_tier_raw=2
            # bldg 1002: no gas service -> gas_lmi_tier_raw=0 (fix applied upstream)
            "lmi_tier_raw": [2, 2],
            "gas_lmi_tier_raw": [2, 0],
            "is_lmi_elec": [True, True],
            "is_lmi_gas": [True, False],
            "fpl_pct": [100.0, 100.0],
        }
    )

    tier_info = _sample_ri_participation(raw_tiers, 1.0, "uniform", 42)

    # Both buildings participate (p100, both elec-eligible)
    assert tier_info.filter(pl.col("participates"))["bldg_id"].to_list() == [1001, 1002]

    tier_by_id = {row["bldg_id"]: row for row in tier_info.to_dicts()}

    # bldg 1001 (has gas): gas tier preserved, is_lmi_gas=True
    assert tier_by_id[1001]["gas_lmi_tier"] == 2
    assert tier_by_id[1001]["is_lmi_gas"] is True

    # bldg 1002 (no gas): gas tier stays 0, is_lmi_gas=False
    assert tier_by_id[1002]["gas_lmi_tier"] == 0
    assert tier_by_id[1002]["is_lmi_gas"] is False

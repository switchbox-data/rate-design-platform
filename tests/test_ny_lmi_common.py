"""Unit tests for NY EAP/EEAP helpers in lmi_common.py."""

from __future__ import annotations

import polars as pl

from utils.post.lmi_common import (
    assign_ny_tier_expr,
    get_ami_territories,
    get_ny_eap_credits_df,
    load_ny_eap_config,
    smi_pct_expr,
    smi_threshold_by_hh_size,
)


def test_load_ny_eap_config() -> None:
    config = load_ny_eap_config()
    assert "utilities" in config
    assert "ami_territories" in config
    assert "coned" in config["utilities"]
    assert "psegli" in config["utilities"]


def test_get_ami_territories() -> None:
    territories = get_ami_territories()
    assert "coned" in territories
    assert "kedny" in territories
    assert "kedli" in territories
    assert "nyseg" not in territories


def test_get_ny_eap_credits_df_shape() -> None:
    df = get_ny_eap_credits_df()
    assert "utility" in df.columns
    assert "tier" in df.columns
    assert "elec_heat" in df.columns
    assert "gas_nonheat" in df.columns
    # 10 utilities × 7 tiers = 70 rows
    assert df.height == 70


def test_get_ny_eap_credits_coned_tier3() -> None:
    df = get_ny_eap_credits_df()
    row = df.filter((pl.col("utility") == "coned") & (pl.col("tier") == 3))
    assert row.height == 1
    assert row["elec_heat"][0] == 126.21
    assert row["elec_nonheat"][0] == 73.47
    assert row["gas_heat"][0] == 189.83
    assert row["gas_nonheat"][0] == 3.00


def test_get_ny_eap_credits_psegli_flat() -> None:
    """PSEG LI has flat $45 for all electric tiers."""
    df = get_ny_eap_credits_df()
    psegli = df.filter(pl.col("utility") == "psegli")
    assert psegli.height == 7
    for row in psegli.iter_rows(named=True):
        assert row["elec_heat"] == 45.00
        assert row["elec_nonheat"] == 45.00
        assert row["gas_heat"] is None
        assert row["gas_nonheat"] is None


def test_get_ny_eap_credits_nfg_gas_only() -> None:
    """National Fuel Gas is gas-only."""
    df = get_ny_eap_credits_df()
    nfg = df.filter(pl.col("utility") == "nfg")
    for row in nfg.iter_rows(named=True):
        assert row["elec_heat"] is None
        assert row["elec_nonheat"] is None


def test_get_ny_eap_credits_null_for_unpublished() -> None:
    """Unpublished EEAP tiers should be null."""
    df = get_ny_eap_credits_df()
    # NiMo Tier 5 should be null (not published)
    nimo_t5 = df.filter((pl.col("utility") == "nimo") & (pl.col("tier") == 5))
    assert nimo_t5["elec_heat"][0] is None


def test_smi_threshold_by_hh_size() -> None:
    """Derive 60% SMI from l50 (50% of median) values."""
    smi_row = pl.DataFrame(
        {
            "l50_1": [30000.0],
            "l50_2": [34000.0],
            "l50_3": [38000.0],
            "l50_4": [42000.0],
            "l50_5": [46000.0],
            "l50_6": [50000.0],
            "l50_7": [54000.0],
            "l50_8": [58000.0],
        }
    )
    result = smi_threshold_by_hh_size(smi_row, pct=60.0)
    # 60% = l50 * 1.2
    assert result[1] == 30000.0 * 1.2
    assert result[4] == 42000.0 * 1.2
    assert len(result) == 8


def test_smi_threshold_100pct() -> None:
    smi_row = pl.DataFrame(
        {"l50_1": [25000.0], **{f"l50_{i}": [0.0] for i in range(2, 9)}}
    )
    result = smi_threshold_by_hh_size(smi_row, pct=100.0)
    assert result[1] == 25000.0 * 2.0


def test_smi_pct_expr() -> None:
    df = pl.DataFrame(
        {
            "income": [30000.0, 60000.0, 100000.0],
            "smi_thresh": [50000.0, 50000.0, 50000.0],
        }
    )
    out = df.with_columns(smi_pct_expr("income", "smi_thresh").alias("smi_pct"))
    assert out["smi_pct"].to_list() == [60.0, 120.0, 200.0]


def test_assign_ny_tier_basic() -> None:
    """Test core tier assignment logic with various income/vulnerability combos."""
    df = pl.DataFrame(
        {
            "fpl_pct": [
                50.0,  # ≤130% FPL, vulnerable → Tier 3
                50.0,  # ≤130% FPL, not vulnerable → Tier 2
                100.0,  # ≤130% FPL, not vulnerable → Tier 2
                140.0,  # >130% FPL, ≤60% SMI, vulnerable → Tier 2
                140.0,  # >130% FPL, ≤60% SMI, not vulnerable → Tier 1
                200.0,  # >60% SMI, ≤80% SMI → Tier 6
                250.0,  # >80% SMI, ≤100% SMI → Tier 7
                300.0,  # >100% SMI → ineligible (0)
            ],
            "smi_pct": [
                30.0,  # ≤60% SMI
                30.0,  # ≤60% SMI
                50.0,  # ≤60% SMI
                50.0,  # ≤60% SMI
                50.0,  # ≤60% SMI
                70.0,  # 60-80% SMI
                90.0,  # 80-100% SMI
                120.0,  # >100% SMI
            ],
            "is_vulnerable": [True, False, False, True, False, False, False, False],
            "heats_with_oil": [False] * 8,
            "heats_with_propane": [False] * 8,
        }
    )
    out = df.with_columns(
        assign_ny_tier_expr(
            "fpl_pct",
            "smi_pct",
            "is_vulnerable",
            "heats_with_oil",
            "heats_with_propane",
        ).alias("tier")
    )
    assert out["tier"].to_list() == [3, 2, 2, 2, 1, 6, 7, 0]


def test_assign_ny_tier_deliverable_fuel() -> None:
    """Deliverable fuel (oil/propane) + ≤60% SMI → Tier 1."""
    df = pl.DataFrame(
        {
            "fpl_pct": [200.0, 200.0],
            "smi_pct": [50.0, 50.0],
            "is_vulnerable": [False, False],
            "heats_with_oil": [True, False],
            "heats_with_propane": [False, True],
        }
    )
    out = df.with_columns(
        assign_ny_tier_expr(
            "fpl_pct",
            "smi_pct",
            "is_vulnerable",
            "heats_with_oil",
            "heats_with_propane",
        ).alias("tier")
    )
    # Both should be Tier 1: >130% FPL but ≤60% SMI and deliverable fuel
    # Actually, they're >130% FPL and ≤60% SMI without vulnerability → Tier 1 already.
    # The deliverable fuel rule is redundant here but still correct.
    assert out["tier"].to_list() == [1, 1]


def test_assign_ny_tier_deliverable_fuel_explicitly() -> None:
    """Test that deliverable fuel doesn't override higher-tier assignment."""
    df = pl.DataFrame(
        {
            "fpl_pct": [50.0],  # ≤130% FPL + vulnerable → Tier 3
            "smi_pct": [30.0],
            "is_vulnerable": [True],
            "heats_with_oil": [True],
            "heats_with_propane": [False],
        }
    )
    out = df.with_columns(
        assign_ny_tier_expr(
            "fpl_pct",
            "smi_pct",
            "is_vulnerable",
            "heats_with_oil",
            "heats_with_propane",
        ).alias("tier")
    )
    # Even with deliverable fuel, ≤130% FPL + vulnerable → Tier 3 (higher priority)
    assert out["tier"].to_list() == [3]

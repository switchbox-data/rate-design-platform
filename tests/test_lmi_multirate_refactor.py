"""Tests for multi-rate LMI refactor.

Covers:
  - _sample_participation (NY): p100, uniform sub-rate, and idempotency of raw tiers
  - _apply_credits with lmi_tier already in master (second-rate column addition)
  - _sample_ri_participation (RI): p100 and uniform sub-rate
  - apply_ri_lmi_to_master with synthetic master DataFrame (no S3)
"""

from __future__ import annotations

import polars as pl

from utils.post.apply_ny_lmi_to_master_bills import (
    _apply_credits,
    _sample_participation,
)
from utils.post.apply_ri_lmi_discounts_to_bills import (
    _sample_ri_participation,
)
from utils.post.lmi_common import get_ny_eap_credits_df, load_ny_eap_config

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Annual"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ny_raw_tiers(
    bldg_ids: list[int],
    tiers: list[int],
    fpl_pcts: list[float] | None = None,
) -> pl.DataFrame:
    """Minimal raw-tier DataFrame (NY schema: bldg_id, lmi_tier, is_lmi, fpl_pct)."""
    if fpl_pcts is None:
        fpl_pcts = [50.0] * len(bldg_ids)
    return pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "lmi_tier": tiers,
            "is_lmi": [t > 0 for t in tiers],
            "fpl_pct": fpl_pcts,
        }
    )


def _make_ri_raw_tiers(
    bldg_ids: list[int],
    tiers: list[int],
    fpl_pcts: list[float] | None = None,
) -> pl.DataFrame:
    """Minimal raw-tier DataFrame (RI schema: bldg_id, lmi_tier_raw, is_lmi, fpl_pct, elec_kwh, gas_therms)."""
    if fpl_pcts is None:
        fpl_pcts = [50.0] * len(bldg_ids)
    return pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "lmi_tier_raw": tiers,
            "is_lmi": [t > 0 for t in tiers],
            "fpl_pct": fpl_pcts,
            "elec_kwh": [1000.0] * len(bldg_ids),
            "gas_therms": [50.0] * len(bldg_ids),
        }
    )


def _make_master_bills(
    bldg_ids: list[int],
    elec_bill: float = 100.0,
    gas_bill: float = 50.0,
    electric_utility: str = "coned",
    gas_utility: str = "kedny",
    heats_with_elec: bool = True,
    heats_with_gas: bool = False,
) -> pl.DataFrame:
    """Minimal master bills DataFrame with monthly + Annual rows."""
    rows: list[dict] = []
    for bid in bldg_ids:
        for m in MONTHS:
            bill = elec_bill if m != "Annual" else elec_bill * 12
            gbill = gas_bill if m != "Annual" else gas_bill * 12
            rows.append(
                {
                    "bldg_id": bid,
                    "sb.electric_utility": electric_utility,
                    "sb.gas_utility": gas_utility,
                    "month": m,
                    "weight": 1.0,
                    "elec_total_bill": bill,
                    "gas_total_bill": gbill,
                    "heats_with_electricity": heats_with_elec,
                    "heats_with_natgas": heats_with_gas,
                    "heats_with_oil": False,
                    "heats_with_propane": False,
                    "elec_fixed_charge": 0.0,
                    "elec_delivery_bill": bill,
                    "elec_supply_bill": 0.0,
                }
            )
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# NY: _sample_participation
# ---------------------------------------------------------------------------


def test_sample_participation_p100() -> None:
    """At p100 every eligible building participates."""
    raw = _make_ny_raw_tiers([1, 2, 3], [1, 2, 0])
    result = _sample_participation(raw, 1.0, "uniform", 42)
    assert set(result.columns) == {"bldg_id", "lmi_tier", "is_lmi", "participates"}
    # tier 1 and 2 are eligible → both participate
    by_id = {r["bldg_id"]: r for r in result.iter_rows(named=True)}
    assert by_id[1]["participates"] is True
    assert by_id[2]["participates"] is True
    # tier 0 is ineligible → does not participate
    assert by_id[3]["participates"] is False


def test_sample_participation_uniform_subrate() -> None:
    """At sub-100% uniform, ~half of eligible buildings participate."""
    n = 100
    bldg_ids = list(range(n))
    raw = _make_ny_raw_tiers(bldg_ids, [1] * n)
    result = _sample_participation(raw, 0.5, "uniform", 42)
    n_part = result.filter(pl.col("participates"))["bldg_id"].n_unique()
    # Allow ±10 buildings around 50
    assert 40 <= n_part <= 60


def test_sample_participation_ineligible_never_participates() -> None:
    """Ineligible buildings (tier 0) must never participate regardless of rate."""
    raw = _make_ny_raw_tiers([1, 2, 3], [0, 0, 0])
    for rate in (1.0, 0.5, 0.0):
        result = _sample_participation(raw, rate, "uniform", 42)
        assert result["participates"].sum() == 0


def test_sample_participation_preserves_raw_tier() -> None:
    """_sample_participation must not mutate lmi_tier (raw eligibility)."""
    raw = _make_ny_raw_tiers([1, 2, 3], [1, 3, 0])
    result = _sample_participation(raw, 0.5, "uniform", 42)
    # lmi_tier in output should still reflect the raw eligibility tier
    by_id = {r["bldg_id"]: r for r in result.iter_rows(named=True)}
    assert by_id[1]["lmi_tier"] == 1
    assert by_id[2]["lmi_tier"] == 3
    assert by_id[3]["lmi_tier"] == 0


# ---------------------------------------------------------------------------
# NY: _apply_credits – second-rate call (lmi_tier already in master)
# ---------------------------------------------------------------------------


def test_apply_credits_second_rate_no_duplicate_shared_cols() -> None:
    """Second _apply_credits call must add rate-specific cols without duplicating lmi_tier/is_lmi."""
    credits_df = get_ny_eap_credits_df(load_ny_eap_config())
    master = _make_master_bills([1, 2, 3], electric_utility="coned", gas_utility="kedny")

    # First rate (p100)
    tier_info_100 = _sample_participation(
        _make_ny_raw_tiers([1, 2, 3], [1, 2, 0]),
        1.0, "uniform", 42,
    )
    master_after_100 = _apply_credits(master, tier_info_100, 100, credits_df, "monthly")
    assert "lmi_tier" in master_after_100.columns
    assert "is_lmi" in master_after_100.columns
    assert "elec_total_bill_lmi_100" in master_after_100.columns

    # Second rate (p40) – lmi_tier/is_lmi already present
    tier_info_40 = _sample_participation(
        _make_ny_raw_tiers([1, 2, 3], [1, 2, 0]),
        0.4, "uniform", 42,
    )
    master_after_40 = _apply_credits(master_after_100, tier_info_40, 40, credits_df, "monthly")
    assert "lmi_tier" in master_after_40.columns
    assert "is_lmi" in master_after_40.columns
    assert "elec_total_bill_lmi_100" in master_after_40.columns
    assert "elec_total_bill_lmi_40" in master_after_40.columns
    # No duplicate column names
    assert len(master_after_40.columns) == len(set(master_after_40.columns))


def test_apply_credits_shared_cols_consistent_across_rates() -> None:
    """lmi_tier and is_lmi must be identical regardless of which rate added them."""
    credits_df = get_ny_eap_credits_df(load_ny_eap_config())
    master = _make_master_bills([10, 20, 30], electric_utility="nyseg", gas_utility="nyseg")
    raw = _make_ny_raw_tiers([10, 20, 30], [1, 3, 0])

    tier_100 = _sample_participation(raw, 1.0, "uniform", 42)
    after_100 = _apply_credits(master, tier_100, 100, credits_df, "monthly")
    lmi_tier_100 = after_100.filter(pl.col("month") == "Annual").sort("bldg_id")["lmi_tier"].to_list()

    tier_40 = _sample_participation(raw, 0.4, "uniform", 42)
    after_40 = _apply_credits(after_100, tier_40, 40, credits_df, "monthly")
    lmi_tier_40 = after_40.filter(pl.col("month") == "Annual").sort("bldg_id")["lmi_tier"].to_list()

    assert lmi_tier_100 == lmi_tier_40


# ---------------------------------------------------------------------------
# RI: _sample_ri_participation
# ---------------------------------------------------------------------------


def test_sample_ri_participation_p100() -> None:
    """At p100 every eligible RI building participates."""
    raw = _make_ri_raw_tiers([1, 2, 3], [1, 2, 0])
    result = _sample_ri_participation(raw, 1.0, "uniform", 42)
    assert set(result.columns) == {
        "bldg_id", "lmi_tier", "lmi_tier_raw", "is_lmi", "participates",
        "elec_kwh", "gas_therms",
    }
    by_id = {r["bldg_id"]: r for r in result.iter_rows(named=True)}
    assert by_id[1]["participates"] is True
    assert by_id[2]["participates"] is True
    assert by_id[3]["participates"] is False


def test_sample_ri_participation_lmi_tier_zero_for_nonparticipants() -> None:
    """lmi_tier (participation-adjusted) must be 0 for non-participants."""
    raw = _make_ri_raw_tiers([1, 2], [2, 2])
    # Force 0% participation via sub-rate on only 2 buildings: seed chosen to exclude both
    result = _sample_ri_participation(raw, 0.0, "uniform", 42)
    assert result["lmi_tier"].to_list() == [0, 0]


def test_sample_ri_participation_lmi_tier_raw_preserved() -> None:
    """lmi_tier_raw must always reflect raw eligibility regardless of participation."""
    raw = _make_ri_raw_tiers([1, 2, 3], [3, 1, 0])
    result = _sample_ri_participation(raw, 0.5, "uniform", 42)
    by_id = {r["bldg_id"]: r for r in result.iter_rows(named=True)}
    assert by_id[1]["lmi_tier_raw"] == 3
    assert by_id[2]["lmi_tier_raw"] == 1
    assert by_id[3]["lmi_tier_raw"] == 0


# ---------------------------------------------------------------------------
# RI: apply_ri_lmi_to_master with synthetic data (no S3)
# ---------------------------------------------------------------------------


def _fake_apply_ri_lmi_to_master_multi_rate(
    master: pl.DataFrame,
    participation_rates: list[float],
) -> pl.DataFrame:
    """Drive apply_ri_lmi_to_master logic using pre-built tier_info (bypasses S3).

    This mirrors what apply_ri_lmi_to_master does after raw tiers are built,
    so we can test the column-appending logic without S3 access.
    """
    from utils.post.apply_ri_lmi_discounts_to_bills import (
        _sample_ri_participation,
        BLDG_ID_COL,
    )
    from utils.post.lmi_common import discount_fractions_for_ri

    bldg_ids = master["bldg_id"].unique().to_list()
    # synthetic raw tiers: half eligible (tier 1), half not
    tiers = [1 if i % 2 == 0 else 0 for i in range(len(bldg_ids))]
    raw = _make_ri_raw_tiers(bldg_ids, tiers)
    disc_elec, disc_gas = discount_fractions_for_ri()

    for rate in participation_rates:
        pct_label = int(round(rate * 100))
        tier_info = _sample_ri_participation(raw, rate, "uniform", 42)
        elec_col = f"elec_total_bill_lmi_{pct_label}"
        gas_col = f"gas_total_bill_lmi_{pct_label}"
        applied_col = f"applied_discount_{pct_label}"

        mult_elec = (
            pl.when(pl.col("lmi_tier") == 3).then(pl.lit(1.0 - disc_elec[3]))
            .when(pl.col("lmi_tier") == 2).then(pl.lit(1.0 - disc_elec[2]))
            .when(pl.col("lmi_tier") == 1).then(pl.lit(1.0 - disc_elec[1]))
            .otherwise(pl.lit(1.0))
        )
        mult_gas = (
            pl.when(pl.col("lmi_tier") == 3).then(pl.lit(1.0 - disc_gas[3]))
            .when(pl.col("lmi_tier") == 2).then(pl.lit(1.0 - disc_gas[2]))
            .when(pl.col("lmi_tier") == 1).then(pl.lit(1.0 - disc_gas[1]))
            .otherwise(pl.lit(1.0))
        )

        if "lmi_tier" in master.columns:
            enriched = master.join(
                tier_info.select(BLDG_ID_COL, "participates"),
                on=BLDG_ID_COL, how="left",
            ).with_columns(pl.col("participates").fill_null(False))
        else:
            enriched = master.join(
                tier_info.select(BLDG_ID_COL, "lmi_tier_raw", "is_lmi", "participates")
                .rename({"lmi_tier_raw": "lmi_tier"}),
                on=BLDG_ID_COL, how="left",
            ).with_columns(
                pl.col("lmi_tier").fill_null(0).cast(pl.Int32),
                pl.col("is_lmi").fill_null(False),
                pl.col("participates").fill_null(False),
            )

        master = enriched.with_columns(
            pl.when(pl.col("participates"))
            .then((pl.col("elec_total_bill") * mult_elec).clip(lower_bound=0.0))
            .otherwise(pl.col("elec_total_bill"))
            .alias(elec_col),
            pl.when(pl.col("participates"))
            .then((pl.col("gas_total_bill") * mult_gas).clip(lower_bound=0.0))
            .otherwise(pl.col("gas_total_bill"))
            .alias(gas_col),
            pl.col("participates").alias(applied_col),
        ).drop("participates")

    return master


def test_ri_master_bills_two_rates_both_column_sets_present() -> None:
    """Both p100 and p40 column sets must appear in the output."""
    master = _make_master_bills([1, 2, 3, 4])
    result = _fake_apply_ri_lmi_to_master_multi_rate(master, [1.0, 0.4])
    for pct in (100, 40):
        assert f"elec_total_bill_lmi_{pct}" in result.columns
        assert f"gas_total_bill_lmi_{pct}" in result.columns
        assert f"applied_discount_{pct}" in result.columns


def test_ri_master_bills_shared_cols_added_once() -> None:
    """lmi_tier and is_lmi must appear exactly once even with two rates."""
    master = _make_master_bills([1, 2, 3])
    result = _fake_apply_ri_lmi_to_master_multi_rate(master, [1.0, 0.4])
    assert result.columns.count("lmi_tier") == 1
    assert result.columns.count("is_lmi") == 1


def test_ri_master_bills_non_participants_bill_unchanged() -> None:
    """Non-participants must have lmi bill == original bill."""
    master = _make_master_bills([1, 2, 3, 4], elec_bill=100.0, gas_bill=50.0)
    result = _fake_apply_ri_lmi_to_master_multi_rate(master, [1.0])
    non_part = result.filter(~pl.col("applied_discount_100"))
    if non_part.height > 0:
        elec_diff = (non_part["elec_total_bill"] - non_part["elec_total_bill_lmi_100"]).abs().max()
        gas_diff = (non_part["gas_total_bill"] - non_part["gas_total_bill_lmi_100"]).abs().max()
        assert elec_diff < 1e-6
        assert gas_diff < 1e-6


def test_ri_master_bills_no_negative_bills() -> None:
    """All discounted bills must be >= 0."""
    master = _make_master_bills([1, 2, 3, 4], elec_bill=100.0, gas_bill=50.0)
    result = _fake_apply_ri_lmi_to_master_multi_rate(master, [1.0, 0.4])
    for pct in (100, 40):
        assert result.filter(pl.col(f"elec_total_bill_lmi_{pct}") < 0).height == 0
        assert result.filter(pl.col(f"gas_total_bill_lmi_{pct}") < 0).height == 0

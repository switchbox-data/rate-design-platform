"""Unit tests for CT LIDR postprocessing (tier assignment, cap math, participation)."""

from __future__ import annotations

import polars as pl
import pytest

from utils.post.apply_ct_lidr_to_master_bills import (
    DEFAULT_PARTICIPATION_RATES,
    _apply_lidr_discount,
    _sample_ct_participation,
)
from utils.post.lmi_common import (
    assign_ct_lidr_tier_expr,
    ct_lidr_smi_100_by_hh_size,
    ct_usage_cap_kwh_expr,
    discount_fractions_for_ct,
    load_ct_lidr_config,
)


def test_load_ct_lidr_config() -> None:
    config = load_ct_lidr_config()
    tiers = {t["tier"]: t for t in config["tiers"]}
    assert len(tiers) == 5
    assert tiers[5]["discount_pct"] == 0.50
    assert tiers[5]["fpl_upper_bound"] == 100
    assert tiers[1]["discount_pct"] == 0.05
    assert tiers[1]["smi_upper_bound"] == 60
    assert config["usage_cap_kwh"]["non_electric_heat"] == 800
    assert config["usage_cap_kwh"]["electric_heat"] == 1200
    smi_60 = {int(k): v for k, v in config["smi"]["by_household_size"].items()}
    assert smi_60[1] == 48714
    assert smi_60[4] == 93681
    assert smi_60[8] == 129279
    assert config["smi"]["dollar_year"] == 2026


def test_ct_lidr_smi_100_at_published_cap_is_sixty_percent() -> None:
    """Income exactly at DSS/UI 60% SMI must yield smi_pct == 60, not HUD's
    lower 4-person cap (~$74,760).
    """
    smi_100 = ct_lidr_smi_100_by_hh_size()
    assert 93681.0 / smi_100[4] * 100.0 == pytest.approx(60.0)
    assert 48714.0 / smi_100[1] * 100.0 == pytest.approx(60.0)
    # HUD FY2025 60% of 4-person MFI is $74,760 -- well below this table.
    assert smi_100[4] * 0.60 > 90000


def test_discount_fractions_for_ct() -> None:
    disc = discount_fractions_for_ct()
    assert disc == {5: 0.50, 4: 0.40, 3: 0.20, 2: 0.15, 1: 0.05}


def test_assign_ct_lidr_tier_expr_fpl_bands() -> None:
    """FPL% within each tier's band, holding SMI% comfortably ineligible."""
    df = pl.DataFrame(
        {
            "fpl_pct": [50.0, 100.0, 110.0, 125.0, 140.0, 160.0, 180.0, 211.0, 250.0],
            "smi_pct": [80.0] * 9,  # >60% SMI everywhere: no SMI-based fallback tier
        }
    )
    out = df.with_columns(assign_ct_lidr_tier_expr("fpl_pct", "smi_pct").alias("tier"))
    # <=100 -> 5, <=125 -> 4, <=160 -> 3, <=211 -> 2, >211 (and >60% SMI) -> 0
    assert out["tier"].to_list() == [5, 5, 4, 4, 3, 3, 2, 2, 0]


def test_assign_ct_lidr_tier_expr_smi_fallback() -> None:
    """Households above the FPL bands but within 60% SMI land in Tier 1.

    Tier 1's eligibility rule is SMI-only (PURA 2024 Annual Report, p. 80:
    "an overall eligibility cap at 60% State Median Income (i.e., Tier 1)").
    The "212-275% FPL" figure in Table 14 is descriptive of the individual
    qualifying benefit programs bundled into Tier 1, not an independent
    OR-condition -- so a household must pass the SMI test regardless of
    where its FPL% falls.
    """
    df = pl.DataFrame(
        {
            "fpl_pct": [300.0, 300.0],
            "smi_pct": [55.0, 65.0],
        }
    )
    out = df.with_columns(assign_ct_lidr_tier_expr("fpl_pct", "smi_pct").alias("tier"))
    assert out["tier"].to_list() == [1, 0]


def test_assign_ct_lidr_tier_expr_low_income_gets_smi_tier_too() -> None:
    """A very-low-income household (Tier 5 by FPL) also passes the Tier-1 SMI
    bound, but nesting must resolve to the more generous Tier 5, not Tier 1.
    """
    df = pl.DataFrame({"fpl_pct": [50.0], "smi_pct": [20.0]})
    out = df.with_columns(assign_ct_lidr_tier_expr("fpl_pct", "smi_pct").alias("tier"))
    assert out["tier"].to_list() == [5]


def test_ct_usage_cap_kwh_expr() -> None:
    df = pl.DataFrame({"heats_with_electricity": [True, False, None]})
    out = df.with_columns(ct_usage_cap_kwh_expr("heats_with_electricity").alias("cap"))
    assert out["cap"].to_list() == [1200.0, 800.0, 800.0]


MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
    "Annual",
]


def _bldg_rows(
    bldg_id: int,
    monthly_usage: float,
    heats_elec: bool,
    fixed: float = 20.0,
    rate: float = 0.25,
) -> list[dict[str, object]]:
    """Jan-Dec + Annual rows for one building at a constant monthly usage/rate.

    Annual usage/bill are the straight sum of the 12 identical monthly
    values, matching how master bills report the Annual row.
    """
    annual_usage = monthly_usage * 12
    rows: list[dict[str, object]] = []
    for month in MONTHS:
        u = annual_usage if month == "Annual" else monthly_usage
        f = fixed * 12 if month == "Annual" else fixed
        row: dict[str, object] = {
            "bldg_id": bldg_id,
            "month": month,
            "elec_total_bill": f + rate * u,
            "elec_fixed_charge": f,
            "elec_grid_kwh": u,
            "heats_with_electricity": heats_elec,
        }
        rows.append(row)
    return rows


def _synthetic_master(*specs: tuple[int, float, bool]) -> pl.DataFrame:
    """Build a master-bills DataFrame from (bldg_id, monthly_usage, heats_elec) specs."""
    rows = [row for spec in specs for row in _bldg_rows(*spec)]
    return pl.DataFrame(rows)


def _tier_info(
    bldg_ids: list[int],
    tiers: list[int],
    participates: list[bool] | None = None,
) -> pl.DataFrame:
    if participates is None:
        participates = [t >= 1 for t in tiers]
    return pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "elec_lmi_tier": tiers,
            "is_lmi_elec": [t >= 1 for t in tiers],
            "participates": participates,
        }
    )


def test_apply_lidr_discount_matches_interpretation_a() -> None:
    """Verify the capped discount formula against a hand-computed expectation.

    The discount applies only to the volumetric charge, up to the cap; the
    fixed charge is never discounted (per the utility FAQ: "a discount
    applied to the first N kWh of your monthly electric usage").

    bldg 1: 1200 kWh/month, 800 kWh cap (non-electric heat), Tier 5 (50%).
      discount = 0.50 * 0.25 * min(1200, 800) = 0.50*0.25*800 = 100
      bill after = (20 + 0.25*1200) - 100 = 320 - 100 = 220
    bldg 2: 400 kWh/month (under cap), Tier 5 (50%).
      discount = 0.50 * 0.25 * min(400, 800) = 0.50*0.25*400 = 50
      bill after = (20 + 0.25*400) - 50 = 120 - 50 = 70
    """
    master = _synthetic_master((1, 1200.0, False), (2, 400.0, False))
    tier_info = _tier_info([1, 2], [5, 5])
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)

    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )

    jan_bldg1 = result.filter((pl.col("bldg_id") == 1) & (pl.col("month") == "Jan"))
    jan_bldg2 = result.filter((pl.col("bldg_id") == 2) & (pl.col("month") == "Jan"))
    assert jan_bldg1["elec_total_bill_lmi_100"][0] == 220.0
    assert jan_bldg2["elec_total_bill_lmi_100"][0] == 70.0

    # Annual row must equal the sum of the 12 monthly discounted bills, not a
    # cap applied to the annual total usage (9,600 kWh/yr vs. the monthly cap).
    annual_bldg1 = result.filter(
        (pl.col("bldg_id") == 1) & (pl.col("month") == "Annual")
    )
    assert annual_bldg1["elec_total_bill_lmi_100"][0] == 220.0 * 12


@pytest.mark.parametrize(
    ("tier", "discount_pct", "expected_discount"),
    [
        (5, 0.50, 75.0),
        (4, 0.40, 60.0),
        (3, 0.20, 30.0),
        (2, 0.15, 22.5),
        (1, 0.05, 7.5),
        (0, 0.0, 0.0),
    ],
)
def test_apply_lidr_discount_by_income_tier_under_cap(
    tier: int, discount_pct: float, expected_discount: float
) -> None:
    """Bill before/after at every income tier, same usage (under the 800 kWh
    non-electric-heat cap), so only the discount percentage varies with
    income level. Lower income (higher tier number) -> bigger discount, and
    Tier 0 (ineligible / ineligible-but-passed-through) gets no discount at
    all -- the bill before and after must be identical.

    Shared inputs: fixed=$20, rate=$0.25/kWh, usage=600 kWh/month (< 800 cap).
    bill_before = 20 + 0.25*600 = 170.
    discount = discount_pct * 0.25 * 600 = discount_pct * 150.
    """
    master = _synthetic_master((1, 600.0, False))
    tier_info = _tier_info([1], [tier])
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)

    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )
    jan = result.filter(pl.col("month") == "Jan")

    bill_before = jan["elec_total_bill"][0]
    bill_after = jan["elec_total_bill_lmi_100"][0]
    assert bill_before == 170.0
    assert bill_before - bill_after == pytest.approx(expected_discount)
    assert bill_after == pytest.approx(170.0 - expected_discount)
    assert jan["applied_discount_elec_100"][0] == (tier >= 1)


@pytest.mark.parametrize(
    ("tier", "discount_pct", "expected_discount"),
    [
        (5, 0.50, 100.0),
        (3, 0.20, 40.0),
        (1, 0.05, 10.0),
    ],
)
def test_apply_lidr_discount_by_income_tier_over_cap_non_electric_heat(
    tier: int, discount_pct: float, expected_discount: float
) -> None:
    """Same income-tier sweep as the under-cap test, but usage (1000 kWh/mo)
    exceeds the 800 kWh non-electric-heat cap, so the discount is computed
    on the capped 800 kWh, not the full 1000 kWh billed.

    bill_before = 20 + 0.25*1000 = 270.
    discount = discount_pct * 0.25 * min(1000, 800) = discount_pct * 200.
    """
    master = _synthetic_master((1, 1000.0, False))
    tier_info = _tier_info([1], [tier])
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)

    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )
    jan = result.filter(pl.col("month") == "Jan")

    bill_before = jan["elec_total_bill"][0]
    bill_after = jan["elec_total_bill_lmi_100"][0]
    assert bill_before == 270.0
    assert bill_before - bill_after == pytest.approx(expected_discount)


def test_apply_lidr_discount_electric_heat_higher_cap() -> None:
    """At the same usage and tier, an electric-heat household is capped at
    1200 kWh instead of 800, so it keeps more of its usage discounted and
    ends up with a bigger dollar discount than a non-electric-heat
    household at identical usage/rate/tier.

    Both buildings: 1000 kWh/month, fixed=$20, rate=$0.25/kWh, Tier 5 (50%).
    bldg 1 (non-electric heat, 800 kWh cap):
      discount = 0.50 * 0.25 * min(1000, 800) = 100
    bldg 2 (electric heat, 1200 kWh cap):
      discount = 0.50 * 0.25 * min(1000, 1200) = 125 (uncapped: full usage)
    """
    master = _synthetic_master((1, 1000.0, False), (2, 1000.0, True))
    tier_info = _tier_info([1, 2], [5, 5])
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)

    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )
    jan_bldg1 = result.filter((pl.col("bldg_id") == 1) & (pl.col("month") == "Jan"))
    jan_bldg2 = result.filter((pl.col("bldg_id") == 2) & (pl.col("month") == "Jan"))

    bill_before = 270.0  # 20 + 0.25*1000, same for both
    assert bill_before - jan_bldg1["elec_total_bill_lmi_100"][0] == pytest.approx(100.0)
    assert bill_before - jan_bldg2["elec_total_bill_lmi_100"][0] == pytest.approx(125.0)


def test_apply_lidr_discount_savings_monotonic_in_income_tier() -> None:
    """Holding usage/rate/fixed-charge fixed, dollar savings should increase
    monotonically as the tier number increases (i.e. as income falls),
    since discount_pct increases from 5% at Tier 1 to 50% at Tier 5, and
    Tier 0 (ineligible) saves nothing at all.
    """
    tiers = [0, 1, 2, 3, 4, 5]
    master = _synthetic_master(*[(i, 900.0, False) for i in tiers])
    tier_info = _tier_info(list(tiers), tiers)
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)

    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )
    jan = result.filter(pl.col("month") == "Jan").sort("bldg_id")
    savings = (jan["elec_total_bill"] - jan["elec_total_bill_lmi_100"]).to_list()

    assert savings == sorted(savings)  # non-decreasing as tier (bldg_id) increases
    assert savings[0] == 0.0  # Tier 0: no discount at all
    assert savings[-1] > savings[1] > 0.0  # Tier 5 saves strictly more than Tier 1


def test_apply_lidr_discount_non_participant_unchanged() -> None:
    """An eligible household (Tier 5) that opts out (participates=False) sees
    no change at all -- distinct from an ineligible (Tier 0) household,
    which also sees no change but for a different reason.
    """
    master = _synthetic_master((1, 900.0, False), (2, 900.0, False))
    tier_info = _tier_info(
        [1, 2],
        [5, 0],
        participates=[False, False],  # bldg 1 opts out despite being eligible
    )
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)
    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )

    for bldg_id in (1, 2):
        jan = result.filter((pl.col("bldg_id") == bldg_id) & (pl.col("month") == "Jan"))
        assert jan["elec_total_bill_lmi_100"][0] == jan["elec_total_bill"][0]
        assert jan["applied_discount_elec_100"][0] is False


def test_multiple_participation_scenarios_use_independent_flags() -> None:
    """A later partial-participation scenario must not reuse p100 flags."""
    master = _synthetic_master((1, 900.0, False), (2, 900.0, False))
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)

    p100 = _apply_lidr_discount(
        master,
        _tier_info([1, 2], [5, 5]),
        100,
        disc_by_tier,
        config,
        n_expected_rows=master.height,
    )
    result = _apply_lidr_discount(
        p100,
        _tier_info([1, 2], [5, 5], participates=[True, False]),
        53,
        disc_by_tier,
        config,
        n_expected_rows=master.height,
    )

    jan = result.filter(pl.col("month") == "Jan").sort("bldg_id")
    assert jan["applied_discount_elec_100"].to_list() == [True, True]
    assert jan["applied_discount_elec_53"].to_list() == [True, False]
    assert jan["elec_total_bill_lmi_53"][0] < jan["elec_total_bill"][0]
    assert jan["elec_total_bill_lmi_53"][1] == jan["elec_total_bill"][1]
    assert "participates" not in result.columns


def test_apply_lidr_discount_zero_usage_no_divide_by_zero() -> None:
    """A building with zero electric usage in a month (e.g. vacant, or data
    gap) must not blow up the volumetric-rate division and should see no
    discount -- there's no volumetric charge to discount.
    """
    master = _synthetic_master((1, 0.0, False))
    tier_info = _tier_info([1], [5])
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)

    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )
    jan = result.filter(pl.col("month") == "Jan")
    assert jan["elec_total_bill"][0] == 20.0  # fixed charge only
    assert jan["elec_total_bill_lmi_100"][0] == 20.0  # unchanged: no volumetric charge


def test_bill_change_by_income_level_end_to_end() -> None:
    """Full pipeline: assign a LIDR tier from income (FPL%/SMI%), then apply
    the discount to a bill, and confirm the before/after bill matches
    expectations at each income level -- from comfortably above LIDR
    eligibility (no change) down to the lowest-income tier (biggest
    discount).

    Households, in decreasing income order:
      A: 300% FPL, 80% SMI  -> ineligible (Tier 0)   -> no discount
      B: 50%  SMI            -> Tier 1 (5%, SMI-only path)
      C: 180% FPL            -> Tier 2 (15%)
      D: 140% FPL            -> Tier 3 (20%)
      E: 110% FPL            -> Tier 4 (40%)
      F: 60%  FPL            -> Tier 5 (50%)
    All households: 1000 kWh/month, non-electric heat (800 kWh cap),
    fixed=$20, rate=$0.25/kWh -> bill_before = 270, capped_usage=800.
    """
    incomes = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5, 6],
            "fpl_pct": [300.0, 300.0, 180.0, 140.0, 110.0, 60.0],
            "smi_pct": [80.0, 50.0, 80.0, 80.0, 80.0, 80.0],
        }
    )
    tiers = incomes.with_columns(
        assign_ct_lidr_tier_expr("fpl_pct", "smi_pct").alias("elec_lmi_tier")
    )
    assert tiers.sort("bldg_id")["elec_lmi_tier"].to_list() == [0, 1, 2, 3, 4, 5]

    master = _synthetic_master(*[(i, 1000.0, False) for i in range(1, 7)])
    tier_info = tiers.with_columns(
        (pl.col("elec_lmi_tier") >= 1).alias("is_lmi_elec"),
        (pl.col("elec_lmi_tier") >= 1).alias("participates"),
    )
    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)
    result = _apply_lidr_discount(
        master, tier_info, 100, disc_by_tier, config, n_expected_rows=master.height
    )
    jan = result.filter(pl.col("month") == "Jan").sort("bldg_id")

    expected_bill_after = {
        1: 270.0,  # Tier 0: unchanged
        2: 260.0,  # Tier 1: 270 - 0.05*0.25*800 = 270 - 10
        3: 240.0,  # Tier 2: 270 - 0.15*0.25*800 = 270 - 30
        4: 230.0,  # Tier 3: 270 - 0.20*0.25*800 = 270 - 40
        5: 190.0,  # Tier 4: 270 - 0.40*0.25*800 = 270 - 80
        6: 170.0,  # Tier 5: 270 - 0.50*0.25*800 = 270 - 100
    }
    for bldg_id, expected in expected_bill_after.items():
        row = jan.filter(pl.col("bldg_id") == bldg_id)
        assert row["elec_total_bill"][0] == 270.0, f"bldg {bldg_id} bill_before"
        assert row["elec_total_bill_lmi_100"][0] == pytest.approx(expected), (
            f"bldg {bldg_id} bill_after"
        )


def test_default_participation_rates_are_p100_and_p53() -> None:
    """CT production defaults match RI/NY/MD's two-rate pattern: 100% then
    the observed-behavior rate. 53% is Eversource enrolled / estimated
    eligible (lmi_discounts_in_ct.md Section 4.2); 45-60% stays a later
    sensitivity range, not extra default columns.
    """
    assert DEFAULT_PARTICIPATION_RATES == [1.0, 0.53]


def test_sample_ct_participation_full_takeup() -> None:
    """At rate=1.0, every eligible building participates (the p100 scenario)."""
    raw_tiers = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            "lidr_tier_raw": [5, 0, 2],
            "is_lmi_elec": [True, False, True],
            "fpl_pct": [50.0, 300.0, 180.0],
        }
    )
    out = _sample_ct_participation(raw_tiers, 1.0, "uniform", 42)
    assert out.sort("bldg_id")["participates"].to_list() == [True, False, True]
    assert out.sort("bldg_id")["elec_lmi_tier"].to_list() == [5, 0, 2]


def test_sample_ct_participation_partial_uniform_takeup() -> None:
    """At rate<1.0, only ineligible (Tier 0) buildings are guaranteed
    non-participation; participation among eligible buildings is a subset,
    and the observed share should be in the right ballpark for a large pool.
    """
    n = 2000
    raw_tiers = pl.DataFrame(
        {
            "bldg_id": list(range(n)),
            "lidr_tier_raw": [3] * n,  # all eligible, Tier 3
            "is_lmi_elec": [True] * n,
            "fpl_pct": [140.0] * n,
        }
    )
    out = _sample_ct_participation(raw_tiers, 0.5, "uniform", 42)
    n_participants = out["participates"].sum()
    # Deterministic hash-based sampling: not exactly half, but well within a
    # generous tolerance band for n=2000.
    assert 0.4 * n < n_participants < 0.6 * n
    # Non-participants keep tier 0 (no discount), participants keep tier 3.
    assert set(
        out.filter(~pl.col("participates"))["elec_lmi_tier"].unique().to_list()
    ) <= {0}
    assert set(
        out.filter(pl.col("participates"))["elec_lmi_tier"].unique().to_list()
    ) <= {3}

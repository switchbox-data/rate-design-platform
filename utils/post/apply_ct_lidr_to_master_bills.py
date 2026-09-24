"""Apply CT LIDR (Low-Income Discount Rate) discounts to master bills.

Five-tier percentage discount (5/15/20/40/50%) on the **total** monthly
electric bill, per PURA's 2024 Annual Report, Table 14 (the regulatory
decision binding on both Eversource and UI -- see
context/domain/charges/lmi_discounts_in_ct.md Section 2.2 for full sourcing).
The discount is capped at the dollar amount a customer using exactly
800 kWh/month (non-electric-heat) or 1200 kWh/month (electric-heat)
would receive on their total bill (including the fixed/customer charge).

LIDR is electric-only: there is no CT gas LIDR, so gas bills are untouched.
An ``energy_total_bill_lmi_{pct}`` column is also emitted (discounted elec +
unchanged gas/oil/propane) so downstream reports can use the same
``energy_total_bill_lmi`` column pattern as MD.
CT's other affordability programs (CEAP, Operation Fuel, MPP) are out of
scope for this module -- see lmi_discounts_in_ct.md Section 1 for why they
are not folded into this discount.  CEAP and Operation Fuel could
theoretically be toggled on later as independent components, but are not
implemented.

Tier 1 eligibility uses the DSS/CEAP **60% HHS LIHEAP SMI** dollar table
encoded in ``ct_lidr.yaml`` (not HUD SMI). Tiers 2-5 use HHS FPL% for
``--fpl-year``. Pair ``--fpl-year 2026`` with the current YAML vintage.

Usage-cap proration mechanic (documented in lmi_discounts_in_ct.md Section
3.1):

    bill_at_cap = fixed_charge + avg_volumetric_rate * cap_kwh
    discount = discount_pct * min(total_bill, bill_at_cap)

The discount applies to the **whole bill** (fixed + volumetric).  For
households using more than the cap, the discount is capped at what a
cap-kWh customer's total bill (including fixed charge) would yield.  For
households at or below the cap, the full bill is discounted at the tier
percentage.  ``avg_volumetric_rate`` is derived per building-month as
``(elec_total_bill - elec_fixed_charge) / elec_grid_kwh``, a locally linear
approximation that is exact for flat volumetric rates and an approximation
for any tiered/seasonal block rate (CT's own Eversource Rate 6, for example,
has a 700 kWh winter block break).

Default participation scenarios match RI/NY/MD's two-rate pattern:
100% take-up (policy / full-eligibility case) and 53% take-up (observed
Eversource enrollment). The 53% is enrolled / estimated-eligible from
Eversource's CY2025 Order 10 filing vs ACS PUMS -- see
lmi_discounts_in_ct.md Section 4.2. Keep 45-60% as a future sensitivity
range; do not emit those as default column sets.

Uses polars lazy execution for metadata reads; collects once to build
eligibility tiers, matching the RI/NY/MD LMI modules' pattern.
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv

from utils.file_io import get_aws_storage_options
from utils.post.lmi_common import (
    assign_ct_lidr_tier_expr,
    ct_lidr_smi_100_by_hh_size,
    ct_usage_cap_kwh_expr,
    discount_fractions_for_ct,
    fpl_pct_expr,
    fpl_threshold_expr,
    inflate_income_expr,
    load_cpi_ratio,
    load_ct_lidr_config,
    load_fpl_guidelines,
    parse_occupants_expr,
    participation_uniform_expr,
    select_participants_weighted,
    smi_pct_expr,
)

ANNUAL_MONTH = "Annual"
BLDG_ID = "bldg_id"

# Production defaults: p100 (full take-up) then p53 (observed Eversource
# participation). Derivation and the 45-60% sensitivity range: see
# context/domain/charges/lmi_discounts_in_ct.md Section 4.2.
DEFAULT_PARTICIPATION_RATES: list[float] = [1.0, 0.53]

REQUIRED_MASTER_COLS = {
    "elec_total_bill",
    "elec_fixed_charge",
    "elec_grid_kwh",
    "heats_with_electricity",
    "month",
}


def _storage_opts() -> dict[str, str]:
    return get_aws_storage_options()


def _build_smi_threshold_column(
    occupants_col: str,
    smi_thresholds: dict[int, float],
) -> pl.Expr:
    """Map occupant count to annual SMI threshold. >8 uses 8-person threshold."""
    expr = pl.lit(smi_thresholds.get(8, 0.0))
    for hh_size in range(8, 0, -1):
        thresh = smi_thresholds.get(hh_size, 0.0)
        expr = (
            pl.when(pl.col(occupants_col) == hh_size)
            .then(pl.lit(thresh))
            .otherwise(expr)
        )
    return expr


def _build_ct_raw_tiers(
    meta_path: str,
    util_path: str,
    utility: str,
    inflation_year: int,
    cpi_ratio: float,
    smi_100: dict[int, float],
    config: dict[str, Any],
    opts: dict[str, str],
) -> pl.DataFrame:
    """Load metadata for utility; compute FPL%, SMI%, and eligibility tier.

    Returns a collected DataFrame with bldg_id, lidr_tier_raw (LIDR tier
    0-5), is_lmi_elec, fpl_pct. fpl_pct is retained for weighted
    participation sampling downstream.
    """
    occupants_num = "occupants_num"
    income_inflated = "income_inflated"
    fpl_threshold = "fpl_threshold"
    fpl_pct = "fpl_pct"
    smi_threshold = "smi_threshold"
    smi_pct = "smi_pct"
    tier_col = "lidr_tier_raw"

    fpl = load_fpl_guidelines(inflation_year)
    meta = pl.scan_parquet(meta_path, storage_options=opts)
    util = pl.scan_parquet(util_path, storage_options=opts)
    meta = meta.join(
        util.filter(pl.col("sb.electric_utility") == utility).select(BLDG_ID),
        on=BLDG_ID,
        how="inner",
    )

    meta = meta.with_columns(parse_occupants_expr("in.occupants").alias(occupants_num))
    meta = meta.with_columns(
        fpl_threshold_expr(occupants_num, fpl["base"], fpl["increment"]).alias(
            fpl_threshold
        )
    )
    meta = meta.with_columns(
        inflate_income_expr("in.representative_income", cpi_ratio).alias(
            income_inflated
        )
    )
    meta = meta.with_columns(
        fpl_pct_expr(income_inflated, pl.col(fpl_threshold)).alias(fpl_pct)
    )
    meta = meta.with_columns(
        _build_smi_threshold_column(occupants_num, smi_100).alias(smi_threshold)
    )
    meta = meta.with_columns(
        smi_pct_expr(income_inflated, smi_threshold).alias(smi_pct)
    )
    meta = meta.with_columns(
        assign_ct_lidr_tier_expr(fpl_pct, smi_pct, config).alias(tier_col)
    )
    meta = meta.filter(pl.col("in.vacancy_status") != "Vacant")
    meta = meta.with_columns((pl.col(tier_col) >= 1).alias("is_lmi_elec"))

    result = (
        meta.select(BLDG_ID, tier_col, "is_lmi_elec", fpl_pct).sort(BLDG_ID).collect()
    )
    assert isinstance(result, pl.DataFrame)
    return result


def _sample_ct_participation(
    raw_tiers: pl.DataFrame,
    participation_rate: float,
    participation_mode: str,
    seed: int,
) -> pl.DataFrame:
    """Add participation flags and a participation-adjusted tier column.

    Sampling uses LIDR eligibility (lidr_tier_raw >= 1) as the pool. At
    participation_rate=1.0 (the p100 policy scenario), every eligible
    household participates.

    Returns bldg_id, elec_lmi_tier (participation-adjusted), lidr_tier_raw,
    is_lmi_elec, participates.
    """
    tier_col = "lidr_tier_raw"
    fpl_pct = "fpl_pct"
    eligible = pl.col(tier_col) >= 1

    if participation_rate >= 1.0:
        result = raw_tiers.with_columns(eligible.alias("participates"))
    elif participation_mode == "uniform":
        result = raw_tiers.with_columns(
            participation_uniform_expr(
                BLDG_ID, participation_rate, seed, eligible
            ).alias("participates")
        )
    else:
        eligible_df = (
            raw_tiers.filter(eligible)
            .with_columns((1.0 / pl.col(fpl_pct).clip(lower_bound=1.0)).alias("weight"))
            .select(BLDG_ID, fpl_pct, tier_col, "weight")
        )
        part_df = select_participants_weighted(
            eligible_df, participation_rate, seed, "weight", BLDG_ID
        )
        result = raw_tiers.join(part_df, on=BLDG_ID, how="left")
        result = result.with_columns(
            pl.when(eligible)
            .then(pl.col("participates").fill_null(False))
            .otherwise(pl.lit(False))
            .alias("participates")
        )

    result = result.with_columns(
        pl.when(pl.col("participates"))
        .then(pl.col(tier_col))
        .otherwise(pl.lit(0))
        .alias("elec_lmi_tier")
    )
    return result.select(
        BLDG_ID, "elec_lmi_tier", tier_col, "is_lmi_elec", "participates"
    )


def _apply_lidr_discount(
    master: pl.DataFrame,
    tier_info: pl.DataFrame,
    pct_label: int,
    disc_by_tier: dict[int, float],
    config: dict[str, Any],
    n_expected_rows: int,
) -> pl.DataFrame:
    """Join tier info; apply the capped percentage discount per month; derive
    the Annual row as the sum of the 12 discounted monthly bills so
    Annual == sum(Jan..Dec), matching the NY 'monthly' calculation_type
    convention.
    """
    elec_col = f"elec_total_bill_lmi_{pct_label}"
    energy_col = f"energy_total_bill_lmi_{pct_label}"
    applied_col = f"applied_discount_elec_{pct_label}"

    if "elec_lmi_tier" in master.columns:
        # ``master`` may be the output of a prior participation scenario.
        # Never reuse its temporary ``participates`` flag: each rate must join
        # the independently sampled flag from its own ``tier_info``.
        joined = (
            master.drop("participates", strict=False)
            .join(tier_info.select(BLDG_ID, "participates"), on=BLDG_ID, how="left")
            .with_columns(pl.col("participates").fill_null(False))
        )
    else:
        joined = master.join(
            tier_info.select(BLDG_ID, "elec_lmi_tier", "is_lmi_elec", "participates"),
            on=BLDG_ID,
            how="left",
        ).with_columns(
            pl.col("elec_lmi_tier").fill_null(0).cast(pl.Int32),
            pl.col("is_lmi_elec").fill_null(False),
            pl.col("participates").fill_null(False),
        )
    if joined.height != n_expected_rows:
        raise AssertionError(
            f"CT LIDR join changed row count: {n_expected_rows} -> {joined.height}"
        )

    disc_pct = pl.lit(0.0)
    for tier, pct in disc_by_tier.items():
        disc_pct = (
            pl.when(pl.col("elec_lmi_tier") == tier)
            .then(pl.lit(pct))
            .otherwise(disc_pct)
        )

    cap_kwh = ct_usage_cap_kwh_expr("heats_with_electricity", config)
    # The discount applies to the whole bill (fixed + volumetric), but the
    # dollar discount is capped at what a cap-kWh customer would receive on
    # their total bill.  See module docstring and lmi_discounts_in_ct.md §3.1.
    # avg_rate is a locally linear $/kWh approximation (exact for flat rates;
    # approximation for tiered/seasonal block rates).
    avg_rate = (
        (pl.col("elec_total_bill") - pl.col("elec_fixed_charge"))
        / pl.when(pl.col("elec_grid_kwh") > 0)
        .then(pl.col("elec_grid_kwh"))
        .otherwise(None)
    ).fill_null(0.0)
    bill_at_cap = pl.col("elec_fixed_charge") + avg_rate * cap_kwh
    discountable_bill = pl.min_horizontal(pl.col("elec_total_bill"), bill_at_cap)
    discount_amount = disc_pct * discountable_bill

    joined = joined.with_columns(
        pl.when(pl.col("month") != ANNUAL_MONTH)
        .then(
            pl.when(pl.col("participates"))
            .then((pl.col("elec_total_bill") - discount_amount).clip(lower_bound=0.0))
            .otherwise(pl.col("elec_total_bill"))
        )
        .otherwise(pl.lit(None))
        .alias(elec_col),
        pl.col("participates").alias(applied_col),
    )
    # energy_total_bill_lmi = discounted elec + unchanged gas/oil/propane.
    # LIDR is electric-only so the other fuels pass through.
    joined = joined.with_columns(
        pl.when(pl.col("month") != ANNUAL_MONTH)
        .then(
            pl.col(elec_col)
            + pl.col("gas_total_bill")
            + pl.col("propane_total_bill")
            + pl.col("oil_total_bill")
        )
        .otherwise(pl.lit(None))
        .alias(energy_col),
    )

    monthly_sums = (
        joined.filter(pl.col("month") != ANNUAL_MONTH)
        .group_by(BLDG_ID)
        .agg(
            pl.col(elec_col).sum().alias("_annual_elec_lmi"),
            pl.col(energy_col).sum().alias("_annual_energy_lmi"),
        )
    )
    joined = joined.join(monthly_sums, on=BLDG_ID, how="left")
    if joined.height != n_expected_rows:
        raise AssertionError(
            f"CT LIDR annual-sum join changed row count: {n_expected_rows} -> {joined.height}"
        )

    return joined.with_columns(
        pl.when(pl.col("month") == ANNUAL_MONTH)
        .then(pl.col("_annual_elec_lmi"))
        .otherwise(pl.col(elec_col))
        .alias(elec_col),
        pl.when(pl.col("month") == ANNUAL_MONTH)
        .then(pl.col("_annual_energy_lmi"))
        .otherwise(pl.col(energy_col))
        .alias(energy_col),
    ).drop("_annual_elec_lmi", "_annual_energy_lmi", "participates")


def apply_ct_lidr_to_master(
    master: pl.DataFrame,
    *,
    utility: str,
    state_upper: str,
    upgrade: str,
    path_resstock_release: str,
    lmi_fpl_year: int,
    lmi_cpi_s3_path: str,
    participation_rates: list[float],
    participation_mode: str,
    seed: int,
    opts: dict[str, str],
) -> pl.DataFrame:
    """Append CT LIDR columns to a master bills DataFrame.

    Loads ResStock metadata and the LIDR tier config once, builds
    eligibility tiers once, then loops over each participation rate --
    adding a set of LMI columns per rate -- so multi-rate runs do not
    re-read S3 metadata (mirrors apply_ri_lmi_to_master).

    Output columns added per rate (pct = int(rate * 100)):
        elec_lmi_tier (Int32)               -- shared; added on first rate
        is_lmi_elec (Bool)                  -- shared; added on first rate
        applied_discount_elec_{pct} (Bool)
        elec_total_bill_lmi_{pct} (Float64)
        energy_total_bill_lmi_{pct} (Float64) -- discounted elec + unchanged
            gas/oil/propane, so downstream reports can use the same
            ``energy_total_bill_lmi_{pct}`` column pattern as MD.

    No gas columns are added: LIDR has no gas component.
    """
    missing_cols = REQUIRED_MASTER_COLS - set(master.columns)
    if missing_cols:
        raise ValueError(
            f"CT LIDR requires master bills columns {sorted(missing_cols)}; "
            "rebuild master bills with a current builder version."
        )

    s3_base = path_resstock_release.rstrip("/")
    meta_path = (
        f"{s3_base}/metadata/state={state_upper}/upgrade={upgrade}/metadata-sb.parquet"
    )
    util_path = (
        f"{s3_base}/metadata_utility/state={state_upper}/utility_assignment.parquet"
    )

    config = load_ct_lidr_config()
    disc_by_tier = discount_fractions_for_ct(config)
    smi_cfg = config["smi"]
    expected_year = int(smi_cfg["dollar_year"])
    if lmi_fpl_year != expected_year:
        print(
            f"[CT LIDR] Warning: --fpl-year={lmi_fpl_year} does not match "
            f"ct_lidr.yaml smi.dollar_year={expected_year} "
            f"(paired FPL guideline year {smi_cfg['fpl_guideline_year']}). "
            "Income is inflated to --fpl-year while Tier 1 uses the YAML "
            "60% SMI dollar table; a year mismatch will bias SMI eligibility."
        )

    print(f"[CT LIDR] Loading CPI ratio (fpl_year={lmi_fpl_year})...")
    cpi_ratio = load_cpi_ratio(lmi_cpi_s3_path, lmi_fpl_year, opts)
    smi_100 = ct_lidr_smi_100_by_hh_size(config)

    print(f"[CT LIDR] Building eligibility tiers for {utility}...")
    raw_tiers = _build_ct_raw_tiers(
        meta_path=meta_path,
        util_path=util_path,
        utility=utility,
        inflation_year=lmi_fpl_year,
        cpi_ratio=cpi_ratio,
        smi_100=smi_100,
        config=config,
        opts=opts,
    )
    print(
        f"[CT LIDR] {raw_tiers.height} buildings, "
        f"{raw_tiers['is_lmi_elec'].sum()} elec-eligible"
    )

    n_expected_rows = master.height
    for rate in participation_rates:
        pct_label = round(rate * 100)
        print(f"[CT LIDR] Applying discounts (p{pct_label})...")
        tier_info = _sample_ct_participation(raw_tiers, rate, participation_mode, seed)
        master = _apply_lidr_discount(
            master, tier_info, pct_label, disc_by_tier, config, n_expected_rows
        )

        n_part = (
            master.filter(pl.col("month") == ANNUAL_MONTH)
            .filter(pl.col(f"applied_discount_elec_{pct_label}"))[BLDG_ID]
            .n_unique()
        )
        print(f"[CT LIDR] p{pct_label}: {n_part} participating buildings")

    return master


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Apply CT LIDR discounts to a built master-bills parquet."
    )
    parser.add_argument(
        "--master-bills-path",
        required=True,
        help="Path (local or s3://) to master bills parquet",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Path (local or s3://) to write augmented master bills",
    )
    parser.add_argument(
        "--utility", required=True, help="Electric utility code (e.g. ct_eversource)"
    )
    parser.add_argument("--state", default="CT")
    parser.add_argument("--upgrade", default="00")
    parser.add_argument(
        "--path-resstock-release",
        default="s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
    )
    parser.add_argument("--fpl-year", type=int, required=True)
    parser.add_argument("--cpi-s3-path", required=True)
    parser.add_argument(
        "--participation-rates",
        type=float,
        nargs="+",
        default=DEFAULT_PARTICIPATION_RATES,
        help="One or more participation fractions (0-1). Default 1.0 0.53 "
        "matches RI/NY/MD's two-rate pattern: 100%% take-up (policy case) "
        "then 53%% (observed Eversource LIDR participation; see "
        "lmi_discounts_in_ct.md Section 4.2). Keep 45-60%% as a later "
        "sensitivity range, not as extra default columns.",
    )
    parser.add_argument(
        "--participation-mode", choices=["uniform", "weighted"], default="uniform"
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    opts = _storage_opts()
    storage = opts if args.master_bills_path.startswith("s3://") else None
    master = pl.read_parquet(args.master_bills_path, storage_options=storage)

    result = apply_ct_lidr_to_master(
        master,
        utility=args.utility,
        state_upper=args.state.upper(),
        upgrade=args.upgrade,
        path_resstock_release=args.path_resstock_release,
        lmi_fpl_year=args.fpl_year,
        lmi_cpi_s3_path=args.cpi_s3_path,
        participation_rates=args.participation_rates,
        participation_mode=args.participation_mode,
        seed=args.seed,
        opts=opts,
    )

    if args.output_path.startswith("s3://"):
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            result.write_parquet(tmp.name)
            subprocess.run(["aws", "s3", "cp", tmp.name, args.output_path], check=True)
    else:
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output_path)
    print(f"Wrote {args.output_path}")


if __name__ == "__main__":
    main()

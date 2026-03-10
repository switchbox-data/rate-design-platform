"""Apply NY EAP/EEAP LMI discounts to master bills tables.

Reads master bills parquet (Hive-partitioned by sb.electric_utility) and
ResStock metadata from S3, assigns EAP tiers (0-7) using FPL%, SMI%,
vulnerability, and heating fuel, then applies per-utility fixed monthly
credits from the NY EAP credit table.

Output adds columns to the master table:
  - lmi_tier (Int32): raw EAP tier (0 = ineligible, 1-7 = eligible regardless of participation)
  - is_lmi (Bool): True if building is EAP-eligible (tier > 0)
  - applied_discount_{pct} (Bool): True if discount was actually applied
  - elec_total_bill_lmi_{pct} (Float64): max(0, elec_total_bill - credit)
  - gas_total_bill_lmi_{pct} (Float64): max(0, gas_total_bill - credit)

By default writes in-place (back to --master-bills-path). Pass --output-path
to redirect. Handles idempotent re-runs: drops existing rate-specific columns
before recomputing, and verifies shared lmi_tier/is_lmi if already present.

See context/tools/lmi_master_bills_workflow.md for full documentation.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, cast

import polars as pl
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.post.lmi_common import (
    assign_ny_tier_expr,
    fpl_pct_expr,
    fpl_threshold_expr,
    get_ami_territories,
    get_ami_threshold_for_utility,
    get_ny_eap_credits_df,
    inflate_income_expr,
    load_cpi_ratio,
    load_fpl_guidelines,
    load_ny_eap_config,
    load_smi_for_state,
    parse_occupants_expr,
    participation_uniform_expr,
    select_participants_weighted,
    smi_pct_expr,
    smi_threshold_by_hh_size,
)

ANNUAL_MONTH = "Annual"
BLDG_ID = "bldg_id"

# ---------------------------------------------------------------------------
# Logging (same pattern as build_master_bills.py)
# ---------------------------------------------------------------------------

_t0 = 0.0


def _log(msg: str) -> float:
    elapsed = time.monotonic() - _t0
    mm, ss = divmod(int(elapsed), 60)
    print(f"[{mm:02d}:{ss:02d}] {msg}", file=sys.stderr, flush=True)
    return time.monotonic()


def _log_done(label: str, start: float, detail: str = "") -> None:
    dt = time.monotonic() - start
    suffix = f" ({detail})" if detail else ""
    _log(f"{label}... done ({dt:.1f}s{suffix})")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _infer_upgrade_from_run_pair(master_bills_path: str) -> str:
    """Extract run pair from path and infer upgrade (00 or 02)."""
    match = re.search(r"run_(\d+)\+(\d+)", master_bills_path)
    if not match:
        raise ValueError(
            f"Cannot parse run pair from path: {master_bills_path}. "
            "Expected pattern 'run_X+Y'."
        )
    run_delivery = int(match.group(1))
    if run_delivery in (1, 2, 5, 6):
        return "00"
    if run_delivery in (3, 4, 7, 8):
        return "02"
    raise ValueError(f"Cannot infer upgrade for run {run_delivery}; expected 1-8")


# ---------------------------------------------------------------------------
# Tier assignment
# ---------------------------------------------------------------------------


def _build_tier_for_utility(
    electric_utility: str,
    meta_path: str,
    util_assignment_path: str,
    inflation_year: int,
    cpi_ratio: float,
    fpl: dict[str, int],
    smi_100: dict[int, float],
    ami_100: dict[int, float] | None,
    participation_rate: float,
    participation_mode: str,
    seed: int,
    opts: dict[str, str],
) -> pl.DataFrame:
    """Compute tier assignment and participation for one electric utility.

    Args:
        ami_100: AMI thresholds at 100% by household size, or None for SMI
            territories.  When provided, an ami_pct column is computed and
            used for EEAP tier assignment (Tiers 5-7).

    Returns a collected DataFrame with bldg_id, lmi_tier, is_lmi, participates.
    """
    occupants_num = "occupants_num"
    income_inflated = "income_inflated"
    fpl_threshold = "fpl_threshold"
    fpl_pct = "fpl_pct"
    smi_threshold = "smi_threshold"
    smi_pct = "smi_pct"
    tier_col = "lmi_tier_raw"

    # Read utility assignment to get bldg_ids for this utility
    util_df = pl.scan_parquet(util_assignment_path, storage_options=opts)
    bldg_ids_for_util = util_df.filter(
        pl.col("sb.electric_utility") == electric_utility
    ).select(BLDG_ID)

    # Read metadata and filter to this utility's buildings
    meta = pl.scan_parquet(meta_path, storage_options=opts)
    meta = meta.join(bldg_ids_for_util, on=BLDG_ID, how="inner")

    # Parse occupants, inflate income, compute FPL% and SMI%
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

    # AMI threshold and percentage (AMI territories only)
    ami_pct_col_name: str | None = None
    if ami_100 is not None:
        ami_threshold = "ami_threshold"
        ami_pct_name = "ami_pct"
        ami_pct_col_name = ami_pct_name
        meta = meta.with_columns(
            _build_smi_threshold_column(occupants_num, ami_100).alias(ami_threshold)
        )
        meta = meta.with_columns(
            smi_pct_expr(income_inflated, ami_threshold).alias(ami_pct_name)
        )

    # Filter out vacant units
    meta = meta.filter(pl.col("in.vacancy_status") != "Vacant")

    # Assign tier
    meta = meta.with_columns(
        assign_ny_tier_expr(
            fpl_pct,
            smi_pct,
            "is_vulnerable",
            "heats_with_oil",
            "heats_with_propane",
            ami_pct_col=ami_pct_col_name,
        ).alias(tier_col)
    )

    eligible = pl.col(tier_col) >= 1

    # Participation sampling
    if participation_rate >= 1.0:
        meta = meta.with_columns(eligible.alias("participates"))
    elif participation_mode == "uniform":
        meta = meta.with_columns(
            participation_uniform_expr(
                BLDG_ID, participation_rate, seed, eligible
            ).alias("participates")
        )
    else:
        eligible_df = cast(
            pl.DataFrame,
            meta.filter(eligible)
            .select(BLDG_ID, fpl_pct, tier_col)
            .with_columns((1.0 / pl.col(fpl_pct).clip(lower_bound=1.0)).alias("weight"))
            .collect(),
        )
        part_df = select_participants_weighted(
            eligible_df, participation_rate, seed, "weight", BLDG_ID
        )
        meta = meta.join(part_df.lazy(), on=BLDG_ID, how="left")
        meta = meta.with_columns(
            pl.when(eligible)
            .then(pl.col("participates").fill_null(False))
            .otherwise(pl.lit(False))
            .alias("participates")
        )

    # lmi_tier: raw tier (0 = ineligible, 1-7 = eligible) regardless of participation.
    # Use applied_discount_{pct} to distinguish who actually received the discount.
    meta = meta.with_columns(pl.col(tier_col).alias("lmi_tier"))
    meta = meta.with_columns(eligible.alias("is_lmi"))

    return cast(
        pl.DataFrame,
        meta.select(BLDG_ID, "lmi_tier", "is_lmi", "participates").collect(),
    )


def _build_all_tiers(
    utilities: list[str],
    meta_path: str,
    util_assignment_path: str,
    inflation_year: int,
    cpi_ratio: float,
    fpl: dict[str, int],
    smi_100: dict[int, float],
    ny_eap_config: dict[str, Any],
    participation_rate: float,
    participation_mode: str,
    seed: int,
    opts: dict[str, str],
) -> pl.DataFrame:
    """Build tier assignments for all utilities.

    For AMI-territory utilities, loads area-level AMI thresholds so that
    EEAP tiers 5-7 use AMI instead of SMI.
    """
    ami_territory_set = set(get_ami_territories(ny_eap_config))
    ami_cache: dict[str, dict[int, float]] = {}

    all_tiers: list[pl.DataFrame] = []
    for i, utility in enumerate(utilities, 1):
        ami_100: dict[int, float] | None = None
        if utility in ami_territory_set:
            if utility not in ami_cache:
                _log(f"  Loading AMI thresholds for {utility}...")
                ami_cache[utility] = get_ami_threshold_for_utility(
                    utility, inflation_year, 100.0, opts, ny_eap_config
                )
            ami_100 = ami_cache[utility]

        t = _log(f"  Assigning tiers for {utility} ({i}/{len(utilities)})...")
        tier_df = _build_tier_for_utility(
            electric_utility=utility,
            meta_path=meta_path,
            util_assignment_path=util_assignment_path,
            inflation_year=inflation_year,
            cpi_ratio=cpi_ratio,
            fpl=fpl,
            smi_100=smi_100,
            ami_100=ami_100,
            participation_rate=participation_rate,
            participation_mode=participation_mode,
            seed=seed,
            opts=opts,
        )
        n_eligible = tier_df.filter(pl.col("is_lmi")).height
        n_part = tier_df.filter(pl.col("participates")).height
        _log_done(
            f"  Tiers for {utility}",
            t,
            f"{tier_df.height} buildings, {n_eligible} eligible, {n_part} participants",
        )
        all_tiers.append(tier_df)
    return pl.concat(all_tiers)


# ---------------------------------------------------------------------------
# Credit application
# ---------------------------------------------------------------------------


def _apply_credits(
    master: pl.DataFrame,
    tier_info: pl.DataFrame,
    pct_label: int,
    credits_df: pl.DataFrame,
) -> pl.DataFrame:
    """Join tier info to master bills, look up credits, compute discounted bills.

    Uses the master table's sb.electric_utility and sb.gas_utility for credit
    lookup (not the tier_info). Clamps discounted bills to >= 0.
    """
    joined = master.join(
        tier_info.select(BLDG_ID, "lmi_tier", "is_lmi", "participates"),
        on=BLDG_ID,
        how="left",
    )
    # Buildings not in tier_info (e.g. vacant) get tier 0, not eligible
    joined = joined.with_columns(
        pl.col("lmi_tier").fill_null(0),
        pl.col("is_lmi").fill_null(False),
        pl.col("participates").fill_null(False),
    )

    n_before_joins = joined.height

    # --- Electric credits ---
    # Nulls in the credit table (unpublished amounts) are preserved so that
    # unconfigured utilities propagate null to the discounted bill columns
    # instead of silently becoming $0.
    elec_credits = credits_df.select(
        pl.col("utility").alias("sb.electric_utility"),
        pl.col("tier").alias("lmi_tier"),
        pl.col("elec_heat").alias("_cr_elec_heat"),
        pl.col("elec_nonheat").alias("_cr_elec_nonheat"),
    )
    joined = joined.join(
        elec_credits, on=["sb.electric_utility", "lmi_tier"], how="left"
    )
    if joined.height != n_before_joins:
        raise AssertionError(
            f"Electric credit join changed row count: {n_before_joins} → "
            f"{joined.height} (duplicate (utility, tier) in credits_df?)"
        )

    elec_monthly = (
        pl.when(pl.col("heats_with_electricity").fill_null(False))
        .then(pl.col("_cr_elec_heat"))
        .otherwise(pl.col("_cr_elec_nonheat"))
    )
    # Unpublished credits (null in YAML) are treated as $0 for bill calculation —
    # we can't give a discount we don't know the amount of.
    elec_credit_monthly = (
        pl.when(pl.col("participates")).then(elec_monthly.fill_null(0.0)).otherwise(0.0)
    )

    # --- Gas credits ---
    gas_credits = credits_df.select(
        pl.col("utility").alias("sb.gas_utility"),
        pl.col("tier").alias("lmi_tier"),
        pl.col("gas_heat").alias("_cr_gas_heat"),
        pl.col("gas_nonheat").alias("_cr_gas_nonheat"),
    )
    joined = joined.join(gas_credits, on=["sb.gas_utility", "lmi_tier"], how="left")
    if joined.height != n_before_joins:
        raise AssertionError(
            f"Gas credit join changed row count: {n_before_joins} → "
            f"{joined.height} (duplicate (utility, tier) in credits_df?)"
        )

    gas_monthly = (
        pl.when(pl.col("heats_with_natgas").fill_null(False))
        .then(pl.col("_cr_gas_heat"))
        .otherwise(pl.col("_cr_gas_nonheat"))
    )
    gas_credit_monthly = (
        pl.when(pl.col("participates")).then(gas_monthly.fill_null(0.0)).otherwise(0.0)
    )

    # Warn about null credits only for the cases that indicate missing data:
    # utilities that ARE in the YAML but have null credits for specific tiers
    # (i.e., unpublished EEAP amounts). Expected nulls we suppress:
    #   - No utility assigned (sb.gas_utility is None): no gas service
    #   - Utility not in YAML at all (small municipals like corning, fillmore)
    #   - Tier 0 (ineligible): no match in credit table by design
    configured_elec_utils = set(
        credits_df.filter(
            pl.col("elec_heat").is_not_null() | pl.col("elec_nonheat").is_not_null()
        )["utility"]
        .unique()
        .to_list()
    )
    configured_gas_utils = set(
        credits_df.filter(
            pl.col("gas_heat").is_not_null() | pl.col("gas_nonheat").is_not_null()
        )["utility"]
        .unique()
        .to_list()
    )

    eligible_participants = joined.filter(
        pl.col("participates") & (pl.col("lmi_tier") > 0)
    )
    if eligible_participants.height > 0:
        # Electric: only warn for utilities that have SOME electric credits
        # configured (i.e., they're in the YAML with non-null electric values
        # for other tiers) but this specific tier is null.
        null_elec = eligible_participants.filter(
            pl.col("sb.electric_utility").is_in(list(configured_elec_utils))
            & pl.col("_cr_elec_heat").is_null()
            & pl.col("_cr_elec_nonheat").is_null()
        )
        # Gas: skip None utility (no gas service) and unconfigured utilities
        null_gas = eligible_participants.filter(
            pl.col("sb.gas_utility").is_not_null()
            & pl.col("sb.gas_utility").is_in(list(configured_gas_utils))
            & pl.col("_cr_gas_heat").is_null()
            & pl.col("_cr_gas_nonheat").is_null()
        )
        if null_elec.height > 0:
            combos = (
                null_elec.select("sb.electric_utility", "lmi_tier")
                .unique()
                .sort("sb.electric_utility", "lmi_tier")
            )
            _log(
                f"  WARNING: {null_elec.height} participant rows have unpublished "
                f"electric credits (treated as $0): {combos.to_dicts()}"
            )
        if null_gas.height > 0:
            combos = (
                null_gas.select("sb.gas_utility", "lmi_tier")
                .unique()
                .sort("sb.gas_utility", "lmi_tier")
            )
            _log(
                f"  WARNING: {null_gas.height} participant rows have unpublished "
                f"gas credits (treated as $0): {combos.to_dicts()}"
            )

    # Apply monthly credit per row, clamped >= 0.
    # For the Annual row, we do NOT multiply the monthly credit by 12 and clamp
    # once — instead, after computing all monthly rows, we derive the Annual row
    # as the sum of the 12 clamped monthly values (below).
    elec_col = f"elec_total_bill_lmi_{pct_label}"
    gas_col = f"gas_total_bill_lmi_{pct_label}"
    applied_col = f"applied_discount_{pct_label}"

    joined = joined.with_columns(
        pl.when(pl.col("month") != ANNUAL_MONTH)
        .then((pl.col("elec_total_bill") - elec_credit_monthly).clip(lower_bound=0.0))
        .otherwise(pl.lit(None))
        .alias(elec_col),
        pl.when(pl.col("month") != ANNUAL_MONTH)
        .then((pl.col("gas_total_bill") - gas_credit_monthly).clip(lower_bound=0.0))
        .otherwise(pl.lit(None))
        .alias(gas_col),
        pl.col("participates").alias(applied_col),
    )

    # Derive Annual rows as the sum of the 12 monthly discounted bills, so that
    # per-month clamping is respected and Annual == sum(Jan..Dec).
    monthly_sums = (
        joined.filter(pl.col("month") != ANNUAL_MONTH)
        .group_by(BLDG_ID)
        .agg(
            pl.col(elec_col).sum().alias("_annual_elec_lmi"),
            pl.col(gas_col).sum().alias("_annual_gas_lmi"),
        )
    )
    joined = joined.join(monthly_sums, on=BLDG_ID, how="left")
    if joined.height != n_before_joins:
        raise AssertionError(
            f"Annual sum join changed row count: {n_before_joins} → {joined.height}"
        )
    joined = joined.with_columns(
        pl.when(pl.col("month") == ANNUAL_MONTH)
        .then(pl.col("_annual_elec_lmi"))
        .otherwise(pl.col(elec_col))
        .alias(elec_col),
        pl.when(pl.col("month") == ANNUAL_MONTH)
        .then(pl.col("_annual_gas_lmi"))
        .otherwise(pl.col(gas_col))
        .alias(gas_col),
    )

    # Drop helper columns
    drop_cols = [
        "_cr_elec_heat",
        "_cr_elec_nonheat",
        "_cr_gas_heat",
        "_cr_gas_nonheat",
        "_annual_elec_lmi",
        "_annual_gas_lmi",
        "participates",
    ]
    joined = joined.drop([c for c in drop_cols if c in joined.columns])

    return joined


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate(df: pl.DataFrame, pct_label: int, participation_rate: float) -> None:
    """Run validation checks and print summary statistics."""
    elec_col = f"elec_total_bill_lmi_{pct_label}"
    gas_col = f"gas_total_bill_lmi_{pct_label}"
    applied_col = f"applied_discount_{pct_label}"

    # No nulls in new columns
    for c in ["lmi_tier", "is_lmi", applied_col, elec_col, gas_col]:
        n_null = df[c].null_count()
        if n_null > 0:
            raise AssertionError(f"Column '{c}' has {n_null} nulls")

    # Floor check: all discounted bills >= 0
    elec_neg = df.filter(pl.col(elec_col) < 0).height
    gas_neg = df.filter(pl.col(gas_col) < 0).height
    if elec_neg > 0 or gas_neg > 0:
        raise AssertionError(f"Negative bills: {elec_neg} electric, {gas_neg} gas")

    # Non-discounted identity: buildings without the discount applied should have
    # discounted == original (covers both ineligible tier-0 and eligible-but-excluded)
    no_discount = df.filter(~pl.col(applied_col))
    if no_discount.height > 0:
        nd_elec_diff = cast(
            float,
            (no_discount["elec_total_bill"] - no_discount[elec_col]).abs().max(),
        )
        nd_gas_diff = cast(
            float,
            (no_discount["gas_total_bill"] - no_discount[gas_col]).abs().max(),
        )
        if nd_elec_diff > 1e-6 or nd_gas_diff > 1e-6:
            raise AssertionError(
                f"Non-discounted bills differ: elec diff={nd_elec_diff}, "
                f"gas diff={nd_gas_diff}"
            )

    # Monotonicity: discounted <= original
    tol = 1e-6
    elec_over = df.filter(pl.col(elec_col) > pl.col("elec_total_bill") + tol).height
    gas_over = df.filter(pl.col(gas_col) > pl.col("gas_total_bill") + tol).height
    if elec_over > 0 or gas_over > 0:
        raise AssertionError(
            f"Discounted > original: {elec_over} elec rows, {gas_over} gas rows"
        )

    # is_lmi == (lmi_tier > 0) consistency
    is_lmi_mismatch = df.filter(pl.col("is_lmi") != (pl.col("lmi_tier") > 0)).height
    if is_lmi_mismatch > 0:
        raise AssertionError(f"is_lmi != (lmi_tier > 0) for {is_lmi_mismatch} rows")

    # p100: applied_discount should equal is_lmi (every eligible building participates)
    if participation_rate >= 1.0:
        applied_vs_lmi = df.filter(pl.col(applied_col) != pl.col("is_lmi")).height
        if applied_vs_lmi > 0:
            raise AssertionError(
                f"At 100% participation, {applied_col} != is_lmi "
                f"for {applied_vs_lmi} rows"
            )

    # Annual row == sum of 12 monthly rows for discounted bill columns
    monthly = df.filter(pl.col("month") != ANNUAL_MONTH)
    monthly_sums = monthly.group_by(BLDG_ID).agg(
        pl.col(elec_col).sum().alias("_check_elec_sum"),
        pl.col(gas_col).sum().alias("_check_gas_sum"),
    )
    annual_check = df.filter(pl.col("month") == ANNUAL_MONTH).join(
        monthly_sums, on=BLDG_ID, how="left"
    )
    elec_annual_diff = cast(
        float,
        (annual_check[elec_col] - annual_check["_check_elec_sum"]).abs().max(),
    )
    gas_annual_diff = cast(
        float,
        (annual_check[gas_col] - annual_check["_check_gas_sum"]).abs().max(),
    )
    if elec_annual_diff > 1e-6 or gas_annual_diff > 1e-6:
        raise AssertionError(
            f"Annual != sum(monthly) for discounted bills: "
            f"elec max diff={elec_annual_diff}, gas max diff={gas_annual_diff}"
        )

    # Participation rate achieved
    annual = df.filter(pl.col("month") == ANNUAL_MONTH)
    n_eligible = annual.filter(pl.col("is_lmi"))[BLDG_ID].n_unique()
    n_participants = annual.filter(pl.col(applied_col))[BLDG_ID].n_unique()
    if n_eligible > 0:
        actual_rate = n_participants / n_eligible
        # For 100%, exact match. For <100%, allow 2pp tolerance (weighted
        # sampling is discrete and rounding to nearest integer count).
        rate_tol = 0.0 if participation_rate >= 1.0 else 0.02
        if abs(actual_rate - participation_rate) > rate_tol:
            raise AssertionError(
                f"Participation rate mismatch: target={participation_rate:.4f}, "
                f"actual={actual_rate:.4f} ({n_participants}/{n_eligible}), "
                f"tolerance={rate_tol}"
            )

    _log("Validation passed")

    # --- Summary statistics ---
    n_bldgs = annual[BLDG_ID].n_unique()
    _log(
        f"Buildings: {n_bldgs}, eligible: {n_eligible} "
        f"({100 * n_eligible / max(n_bldgs, 1):.1f}%), "
        f"participants: {n_participants} "
        f"({100 * n_participants / max(n_eligible, 1):.1f}% of eligible)"
    )

    # Tier distribution by utility
    tier_dist = (
        annual.group_by("sb.electric_utility", "lmi_tier")
        .agg(pl.len().alias("n"))
        .sort("sb.electric_utility", "lmi_tier")
    )
    _log("Tier distribution (annual rows):")
    for row in tier_dist.iter_rows(named=True):
        _log(
            f"  {row['sb.electric_utility']:>8s}  "
            f"tier {row['lmi_tier']}: {row['n']:>6d}"
        )

    # Total annual discount
    participants = annual.filter(pl.col(applied_col))
    if participants.height > 0:
        totals = participants.select(
            (pl.col("elec_total_bill") - pl.col(elec_col)).sum().alias("total_elec"),
            (pl.col("gas_total_bill") - pl.col(gas_col)).sum().alias("total_gas"),
        )
        row = totals.row(0, named=True)
        _log(
            f"Total annual discount: electric=${row['total_elec']:,.0f}, "
            f"gas=${row['total_gas']:,.0f}"
        )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def _write_hive_partitioned(
    df: pl.DataFrame,
    output_path: str,
    partition_col: str = "sb.electric_utility",
) -> None:
    """Write Hive-partitioned parquet via temp dir + aws s3 sync."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="lmi_master_"))
    try:
        for util_name, util_df in df.group_by(partition_col):
            part_dir = tmp_dir / f"{partition_col}={util_name[0]}"
            part_dir.mkdir(parents=True, exist_ok=True)
            util_df.drop(partition_col).write_parquet(part_dir / "data.parquet")
        subprocess.run(
            ["aws", "s3", "sync", str(tmp_dir), output_path],
            check=True,
            capture_output=True,
        )
    finally:
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    global _t0
    _t0 = time.monotonic()
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Apply NY EAP/EEAP LMI discounts to master bills tables."
    )
    parser.add_argument(
        "--master-bills-path",
        required=True,
        help="S3 path to comb_bills_year_target/ (Hive-partitioned parquet)",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="S3 path for output (Hive-partitioned parquet). "
        "Defaults to --master-bills-path (in-place update).",
    )
    parser.add_argument("--state", required=True, help="State abbreviation (NY)")
    parser.add_argument(
        "--fpl-year",
        type=int,
        required=True,
        help="FPL/SMI guideline year; income is inflated from 2019 to this year",
    )
    parser.add_argument(
        "--cpi-s3-path",
        required=True,
        help="S3 path to CPI parquet (year, value)",
    )
    parser.add_argument(
        "--participation-rate",
        type=float,
        default=1.0,
        help="Fraction of eligible customers who participate (0-1)",
    )
    parser.add_argument(
        "--participation-mode",
        choices=["uniform", "weighted"],
        default="weighted",
        help="Participation sampling: uniform or weighted by need",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="RNG seed for participation"
    )
    parser.add_argument(
        "--resstock-release",
        default="s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
        help="S3 path to ResStock release (must have is_vulnerable in metadata)",
    )
    args = parser.parse_args()

    state = args.state.upper()
    output_path = args.output_path or args.master_bills_path
    upgrade = _infer_upgrade_from_run_pair(args.master_bills_path)
    pct_label = int(round(args.participation_rate * 100))
    opts = get_aws_storage_options()

    _log(
        f"Applying NY LMI discounts: state={state}, upgrade={upgrade}, "
        f"participation={args.participation_rate} (p{pct_label}), "
        f"mode={args.participation_mode}, seed={args.seed}"
    )
    _log(f"  Master bills: {args.master_bills_path}")
    _log(f"  Output:       {output_path}")
    if output_path == args.master_bills_path:
        _log("  (in-place update)")

    # 1. Load master bills
    t = _log("Loading master bills...")
    master = cast(
        pl.DataFrame,
        pl.scan_parquet(args.master_bills_path, hive_partitioning=True).collect(),
    )
    n_rows = master.height
    n_bldgs = master[BLDG_ID].n_unique()
    utilities = sorted(master["sb.electric_utility"].unique().to_list())
    _log_done(
        "Loading master bills",
        t,
        f"{n_rows} rows, {n_bldgs} buildings, utilities={utilities}",
    )

    # 2. Load shared config
    t = _log("Loading CPI, FPL, SMI config...")
    cpi_ratio = load_cpi_ratio(args.cpi_s3_path, args.fpl_year, opts)
    fpl = load_fpl_guidelines(args.fpl_year)
    ny_eap_config = load_ny_eap_config()
    smi_row = load_smi_for_state(state, args.fpl_year, opts)
    smi_100 = smi_threshold_by_hh_size(smi_row, pct=100.0)
    credits_df = get_ny_eap_credits_df(ny_eap_config)
    _log_done("Loading config", t, f"CPI ratio={cpi_ratio:.4f}")

    # 3. Build tier assignment for all utilities
    release = args.resstock_release.rstrip("/")
    meta_path = (
        f"{release}/metadata/state={state}/upgrade={upgrade}/metadata-sb.parquet"
    )
    util_assignment_path = (
        f"{release}/metadata_utility/state={state}/utility_assignment.parquet"
    )

    tier_info = _build_all_tiers(
        utilities=utilities,
        meta_path=meta_path,
        util_assignment_path=util_assignment_path,
        inflation_year=args.fpl_year,
        cpi_ratio=cpi_ratio,
        fpl=fpl,
        smi_100=smi_100,
        ny_eap_config=ny_eap_config,
        participation_rate=args.participation_rate,
        participation_mode=args.participation_mode,
        seed=args.seed,
        opts=opts,
    )
    _log(f"Total tier assignments: {tier_info.height} buildings")

    # 4. Idempotency: drop existing columns for this rate (safe re-run)
    rate_cols = [
        f"elec_total_bill_lmi_{pct_label}",
        f"gas_total_bill_lmi_{pct_label}",
        f"applied_discount_{pct_label}",
    ]
    existing_rate_cols = [c for c in rate_cols if c in master.columns]
    if existing_rate_cols:
        _log(f"  Dropping existing rate columns for re-run: {existing_rate_cols}")
        master = master.drop(existing_rate_cols)

    # If lmi_tier / is_lmi already present (from a prior rate run on the same
    # data), verify consistency with recomputed values then drop so they get
    # re-added below. Skip the check when writing to a different output path
    # (staging mode) — the source data may have stale values from a previous
    # code version, and we intentionally recompute from scratch.
    shared_cols = ["lmi_tier", "is_lmi"]
    has_existing_shared = all(c in master.columns for c in shared_cols)
    in_place = output_path == args.master_bills_path
    if has_existing_shared:
        if in_place:
            _log("  lmi_tier/is_lmi already present — verifying consistency...")
            check = (
                master.select(BLDG_ID, "lmi_tier", "is_lmi")
                .unique(subset=[BLDG_ID])
                .join(
                    tier_info.select(BLDG_ID, "lmi_tier", "is_lmi").rename(
                        {"lmi_tier": "_new_tier", "is_lmi": "_new_lmi"}
                    ),
                    on=BLDG_ID,
                    how="inner",
                )
            )
            tier_mismatch = check.filter(pl.col("lmi_tier") != pl.col("_new_tier")).height
            lmi_mismatch = check.filter(pl.col("is_lmi") != pl.col("_new_lmi")).height
            if tier_mismatch > 0 or lmi_mismatch > 0:
                raise AssertionError(
                    f"Existing lmi_tier/is_lmi mismatch with recomputed values: "
                    f"{tier_mismatch} tier mismatches, {lmi_mismatch} is_lmi mismatches"
                )
            _log("  Verified: existing lmi_tier/is_lmi match recomputed values")
        else:
            _log("  Staging mode: dropping existing lmi_tier/is_lmi to recompute from scratch")
        master = master.drop(shared_cols)

    # 5. Apply credits to master bills
    t = _log("Applying credits to master bills...")
    result = _apply_credits(master, tier_info, pct_label, credits_df)
    _log_done("Applying credits", t, f"{result.height} rows")

    # 6. Validate
    t = _log("Validating...")
    _validate(result, pct_label, args.participation_rate)
    _log_done("Validation", t)

    # 7. Write output
    t = _log(f"Writing to {output_path}...")
    _write_hive_partitioned(result, output_path)
    _log_done("Writing", t)

    total_elapsed = time.monotonic() - _t0
    mm, ss = divmod(int(total_elapsed), 60)
    _log(f"Done (total: {mm}m {ss}s)")


if __name__ == "__main__":
    main()

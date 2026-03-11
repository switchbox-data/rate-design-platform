"""Apply NY EAP/EEAP LMI discounts to CAIRO bill outputs (postprocessing).

Reads bills and ResStock metadata from S3, assigns EAP tiers (0-7) using
FPL%, SMI%, vulnerability, and heating fuel, then applies per-utility
fixed monthly credits from the NY EAP credit table.

Same four-step structure as the RI script:
  1. Load CPI and compute income-inflation ratio
  2. Build per-building tier and participation data
  3. Apply fixed monthly credits (subtract from each month's bill)
  4. Write or print outputs

Key differences from RI:
  - NY uses fixed $/month credits (not percentage discounts)
  - Credits vary by utility × tier × fuel × heating status
  - Tier assignment uses FPL + SMI/AMI + vulnerability + deliverable fuel
  - Multiple gas utilities can serve buildings in one electric utility territory

Uses polars lazy execution; minimal collects for rider totals.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast as typecast

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.post.lmi_common import (
    assign_ny_tier_expr,
    fpl_pct_expr,
    fpl_threshold_expr,
    get_ami_territories,
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

ANNUAL_MONTH_VALUE = "Annual"
BLDG_ID_COL = "bldg_id"
KWH_PER_THERM = 29.3


def _storage_opts() -> dict[str, str]:
    return get_aws_storage_options()


def _run_dir_bills(run_dir: S3Path | Path, name: str) -> str:
    return str(run_dir / "bills" / name)


def _build_smi_threshold_column(
    occupants_col: str,
    smi_thresholds: dict[int, float],
) -> pl.Expr:
    """Build a polars expression that maps occupant count to annual SMI threshold.

    For occupants > 8, uses the 8-person threshold (HUD max).
    """
    expr = pl.lit(smi_thresholds.get(8, 0.0))
    for hh_size in range(8, 0, -1):
        thresh = smi_thresholds.get(hh_size, 0.0)
        expr = (
            pl.when(pl.col(occupants_col) == hh_size)
            .then(pl.lit(thresh))
            .otherwise(expr)
        )
    return expr


def _build_tier_consumption(
    meta_path: str,
    util_path: str,
    electric_utility: str,
    inflation_year: int,
    cpi_ratio: float,
    participation_rate: float,
    participation_mode: str,
    seed: int,
    opts: dict[str, str],
) -> pl.LazyFrame:
    """Load metadata for utility, add FPL/SMI/tier/participation.

    Returns lazy frame with bldg_id, lmi_tier, participates,
    heats_with_electricity, heats_with_natgas, gas_utility, elec_kwh, gas_therms.
    """
    tier_col = "lmi_tier_raw"
    occupants_num = "occupants_num"
    income_inflated = "income_inflated"
    fpl_threshold = "fpl_threshold"
    fpl_pct = "fpl_pct"
    smi_threshold = "smi_threshold"
    smi_pct = "smi_pct"
    elec_kwh_col = "out.electricity.total.energy_consumption.kwh"
    gas_kwh_col = "out.natural_gas.total.energy_consumption.kwh"

    fpl = load_fpl_guidelines(inflation_year)

    # Load SMI thresholds for NY at 100% (we'll compute %-of-SMI in the expression)
    ny_eap_config = load_ny_eap_config()
    ami_territories = get_ami_territories(ny_eap_config)

    # For SMI/AMI threshold: use 100% SMI as the denominator so smi_pct = income/threshold*100
    # gives us the percentage of SMI directly
    smi_row = load_smi_for_state("NY", inflation_year, opts)
    smi_100 = smi_threshold_by_hh_size(smi_row, pct=100.0)

    # TODO: For AMI territories (coned, kedny, kedli), use area-level AMI
    # instead of state-level SMI. Currently falls back to SMI for all utilities.
    # This means EEAP thresholds will be conservative (lower) for AMI territories
    # where true AMI > SMI. See get_ami_threshold_for_utility() in lmi_common.py.
    if electric_utility in ami_territories:
        income_thresholds = smi_100  # TODO: replace with AMI thresholds
    else:
        income_thresholds = smi_100

    meta = pl.scan_parquet(meta_path, storage_options=opts)
    util = pl.scan_parquet(util_path, storage_options=opts, hive_partitioning=True)

    # Filter to buildings served by this electric utility
    meta = meta.join(
        util.filter(pl.col("electric_utility") == electric_utility).select(
            BLDG_ID_COL, "gas_utility"
        ),
        on=BLDG_ID_COL,
        how="inner",
    )

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

    # SMI threshold by household size
    meta = meta.with_columns(
        _build_smi_threshold_column(occupants_num, income_thresholds).alias(
            smi_threshold
        )
    )
    meta = meta.with_columns(
        smi_pct_expr(income_inflated, smi_threshold).alias(smi_pct)
    )

    # Filter out vacant units (income = 0, demographics unavailable)
    meta = meta.filter(pl.col("in.vacancy_status") != "Vacant")

    # Assign tier
    meta = meta.with_columns(
        assign_ny_tier_expr(
            fpl_pct, smi_pct, "is_vulnerable", "heats_with_oil", "heats_with_propane"
        ).alias(tier_col)
    )

    eligible = pl.col(tier_col) >= 1

    # Participation
    if participation_rate >= 1.0:
        meta = meta.with_columns(eligible.alias("participates"))
    elif participation_mode == "uniform":
        participates = participation_uniform_expr(
            BLDG_ID_COL, participation_rate, seed, eligible
        )
        meta = meta.with_columns(participates.alias("participates"))
    else:
        eligible_df = typecast(
            pl.DataFrame,
            meta.filter(eligible)
            .select(BLDG_ID_COL, fpl_pct, tier_col)
            .with_columns((1.0 / pl.col(fpl_pct).clip(1.0, None)).alias("weight"))
            .collect(),
        )
        part_df = select_participants_weighted(
            eligible_df, participation_rate, seed, "weight", BLDG_ID_COL
        )
        meta = meta.join(part_df.lazy(), on=BLDG_ID_COL, how="left")
        meta = meta.with_columns(
            pl.when(eligible)
            .then(pl.col("participates").fill_null(False))
            .otherwise(pl.lit(False))
            .alias("participates")
        )

    meta = meta.with_columns(
        pl.when(pl.col("participates"))
        .then(pl.col(tier_col))
        .otherwise(pl.lit(0))
        .alias("lmi_tier")
    )
    meta = meta.with_columns((pl.col(gas_kwh_col) / KWH_PER_THERM).alias("gas_therms"))

    return meta.select(
        BLDG_ID_COL,
        "lmi_tier",
        tier_col,
        "participates",
        "heats_with_electricity",
        "heats_with_natgas",
        "gas_utility",
        pl.col(elec_kwh_col).alias("elec_kwh"),
        "gas_therms",
    )


def _apply_discounts_to_bills(
    run_dir: S3Path | Path,
    tier_consumption: pl.LazyFrame,
    electric_utility: str,
    rider: bool,
    opts: dict[str, str],
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Apply NY EAP fixed monthly credits to electric and gas bills.

    For each customer, looks up the credit by:
      - electric utility (run-level)
      - gas utility (per-building, from metadata_utility)
      - tier (from tier assignment)
      - heating status (heats_with_electricity / heats_with_natgas)

    Subtracts the monthly credit from each month's bill (and Annual = 12× monthly).
    """
    tier_col = "lmi_tier_raw"
    storage = opts if isinstance(run_dir, S3Path) else None

    elec_bills = pl.scan_csv(
        _run_dir_bills(run_dir, "elec_bills_year_run.csv"), storage_options=storage
    )
    gas_bills = pl.scan_csv(
        _run_dir_bills(run_dir, "gas_bills_year_run.csv"), storage_options=storage
    )

    tc = tier_consumption

    # Join tier/heating info to bills
    elec_bills = elec_bills.join(
        tc.select(
            BLDG_ID_COL,
            "lmi_tier",
            tier_col,
            "participates",
            "heats_with_electricity",
            "elec_kwh",
        ),
        on=BLDG_ID_COL,
        how="left",
    )
    gas_bills = gas_bills.join(
        tc.select(
            BLDG_ID_COL,
            "lmi_tier",
            tier_col,
            "participates",
            "heats_with_natgas",
            "gas_utility",
            "gas_therms",
        ),
        on=BLDG_ID_COL,
        how="left",
    )
    elec_bills = elec_bills.with_columns(pl.col("lmi_tier").fill_null(0))
    gas_bills = gas_bills.with_columns(pl.col("lmi_tier").fill_null(0))

    # Load credit table
    credits_df = get_ny_eap_credits_df()

    # --- Electric credits ---
    # Look up credit for the electric utility by tier and heating status
    elec_credits = credits_df.filter(pl.col("utility") == electric_utility).select(
        pl.col("tier"),
        pl.col("elec_heat").fill_null(0.0).alias("credit_heat"),
        pl.col("elec_nonheat").fill_null(0.0).alias("credit_nonheat"),
    )
    elec_bills = elec_bills.join(
        elec_credits.lazy().rename({"tier": "lmi_tier"}),
        on="lmi_tier",
        how="left",
    )
    # Monthly credit: heat or nonheat based on heats_with_electricity
    elec_monthly_credit = (
        pl.when(pl.col("heats_with_electricity").fill_null(False))
        .then(pl.col("credit_heat").fill_null(0.0))
        .otherwise(pl.col("credit_nonheat").fill_null(0.0))
    )
    # Apply credit: subtract from bill. Annual = 12× monthly credit.
    elec_credit_amount = (
        pl.when(pl.col("month") == ANNUAL_MONTH_VALUE)
        .then(elec_monthly_credit * 12.0)
        .otherwise(elec_monthly_credit)
    )
    # Only apply to participants with tier > 0
    elec_credit_applied = (
        pl.when(pl.col("lmi_tier") > 0).then(elec_credit_amount).otherwise(0.0)
    )
    elec_bills = elec_bills.with_columns(elec_credit_applied.alias("discount_elec"))

    # --- Gas credits ---
    # Gas credits depend on the gas utility serving each building (can differ from electric)
    # Join credit table on gas_utility × tier
    gas_credit_lookup = credits_df.select(
        pl.col("utility").alias("gas_utility"),
        pl.col("tier").alias("lmi_tier"),
        pl.col("gas_heat").fill_null(0.0).alias("credit_gas_heat"),
        pl.col("gas_nonheat").fill_null(0.0).alias("credit_gas_nonheat"),
    )
    gas_bills = gas_bills.join(
        gas_credit_lookup.lazy(),
        on=["gas_utility", "lmi_tier"],
        how="left",
    )
    gas_monthly_credit = (
        pl.when(pl.col("heats_with_natgas").fill_null(False))
        .then(pl.col("credit_gas_heat").fill_null(0.0))
        .otherwise(pl.col("credit_gas_nonheat").fill_null(0.0))
    )
    gas_credit_amount = (
        pl.when(pl.col("month") == ANNUAL_MONTH_VALUE)
        .then(gas_monthly_credit * 12.0)
        .otherwise(gas_monthly_credit)
    )
    gas_credit_applied = (
        pl.when(pl.col("lmi_tier") > 0).then(gas_credit_amount).otherwise(0.0)
    )
    gas_bills = gas_bills.with_columns(gas_credit_applied.alias("discount_gas"))

    # --- Rider (optional cost recovery) ---
    if rider:
        elec_annual_rows = elec_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE)
        gas_annual_rows = gas_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE)
        elec_totals_df = typecast(
            pl.DataFrame,
            elec_annual_rows.select(
                pl.col("discount_elec").sum().alias("total_discount_elec"),
                pl.when(~pl.col("participates").fill_null(False))
                .then(pl.col("elec_kwh"))
                .otherwise(0)
                .sum()
                .alias("total_kwh_non"),
            ).collect(),
        )
        gas_totals_df = typecast(
            pl.DataFrame,
            gas_annual_rows.select(
                pl.col("discount_gas").sum().alias("total_discount_gas"),
                pl.when(~pl.col("participates").fill_null(False))
                .then(pl.col("gas_therms"))
                .otherwise(0)
                .sum()
                .alias("total_gas_non"),
            ).collect(),
        )
        td_elec = float(elec_totals_df["total_discount_elec"][0] or 0.0)
        tk_non = float(elec_totals_df["total_kwh_non"][0] or 0.0)
        td_gas = float(gas_totals_df["total_discount_gas"][0] or 0.0)
        tg_non = float(gas_totals_df["total_gas_non"][0] or 0.0)
        rider_per_kwh = (td_elec / tk_non) if tk_non > 0 else 0.0
        rider_per_therm = (td_gas / tg_non) if tg_non > 0 else 0.0
        rider_elec_annual = (
            pl.when(~pl.col("participates").fill_null(False))
            .then(pl.col("elec_kwh") * rider_per_kwh)
            .otherwise(0.0)
        )
        rider_gas_annual = (
            pl.when(~pl.col("participates").fill_null(False))
            .then(pl.col("gas_therms") * rider_per_therm)
            .otherwise(0.0)
        )
    else:
        rider_elec_annual = pl.lit(0.0)
        rider_gas_annual = pl.lit(0.0)

    # Apply: subtract credit, add rider
    rider_elec_row = (
        pl.when(pl.col("month") == ANNUAL_MONTH_VALUE)
        .then(rider_elec_annual)
        .otherwise(rider_elec_annual / 12)
    )
    rider_gas_row = (
        pl.when(pl.col("month") == ANNUAL_MONTH_VALUE)
        .then(rider_gas_annual)
        .otherwise(rider_gas_annual / 12)
    )
    elec_bills = elec_bills.with_columns(
        (pl.col("bill_level") - pl.col("discount_elec") + rider_elec_row).alias(
            "bill_level"
        )
    )
    gas_bills = gas_bills.with_columns(
        (pl.col("bill_level") - pl.col("discount_gas") + rider_gas_row).alias(
            "bill_level"
        )
    )

    # Drop helper columns
    drop_elec = [
        c
        for c in [
            "participates",
            "heats_with_electricity",
            "elec_kwh",
            "discount_elec",
            "credit_heat",
            "credit_nonheat",
            tier_col,
        ]
        if c in elec_bills.collect_schema().names()
    ]
    drop_gas = [
        c
        for c in [
            "participates",
            "heats_with_natgas",
            "gas_utility",
            "gas_therms",
            "discount_gas",
            "credit_gas_heat",
            "credit_gas_nonheat",
            tier_col,
        ]
        if c in gas_bills.collect_schema().names()
    ]
    if drop_elec:
        elec_bills = elec_bills.drop(drop_elec)
    if drop_gas:
        gas_bills = gas_bills.drop(drop_gas)
    return elec_bills, gas_bills


def _upload_discounted_bills(
    elec_bills: pl.LazyFrame,
    gas_bills: pl.LazyFrame,
    run_dir: S3Path | Path,
    suffix: str,
    opts: dict[str, str],
) -> None:
    """Write discounted elec/gas/comb CSVs to run_dir/bills (S3 or local)."""
    out_elec = _run_dir_bills(run_dir, f"elec_bills_year_run_with_lmi_{suffix}.csv")
    out_gas = _run_dir_bills(run_dir, f"gas_bills_year_run_with_lmi_{suffix}.csv")
    out_comb = _run_dir_bills(run_dir, f"comb_bills_year_run_with_lmi_{suffix}.csv")
    storage = opts if isinstance(run_dir, S3Path) else None
    elec_bills.sink_csv(out_elec, storage_options=storage)
    gas_bills.sink_csv(out_gas, storage_options=storage)
    elec_for_comb = elec_bills.select(
        BLDG_ID_COL,
        "weight",
        "month",
        "dollar_year",
        pl.col("bill_level").alias("bill_level_elec"),
        "lmi_tier",
    )
    gas_for_comb = gas_bills.select(
        BLDG_ID_COL,
        "weight",
        "month",
        "dollar_year",
        pl.col("bill_level").alias("bill_level_gas"),
    )
    comb = elec_for_comb.join(
        gas_for_comb,
        on=[BLDG_ID_COL, "weight", "month", "dollar_year"],
        how="left",
    ).with_columns(
        (pl.col("bill_level_elec") + pl.col("bill_level_gas").fill_null(0)).alias(
            "bill_level"
        )
    )
    comb = comb.select(
        BLDG_ID_COL, "weight", "month", "bill_level", "dollar_year", "lmi_tier"
    )
    comb.sink_csv(out_comb, storage_options=storage)
    print(f"Wrote {out_elec}, {out_gas}, {out_comb}")


def _write_or_print_outputs(
    elec_bills: pl.LazyFrame,
    gas_bills: pl.LazyFrame,
    run_dir: S3Path | Path,
    participation_rate: float,
    rider: bool,
    upload: bool,
    opts: dict[str, str],
) -> None:
    """Write discounted elec/gas/comb CSVs to run_dir/bills or print paths."""
    pct_label = int(round(participation_rate * 100))
    rider_label = "rider" if rider else "no_rider"
    suffix = f"p{pct_label}_{rider_label}"
    out_elec = _run_dir_bills(run_dir, f"elec_bills_year_run_with_lmi_{suffix}.csv")
    out_gas = _run_dir_bills(run_dir, f"gas_bills_year_run_with_lmi_{suffix}.csv")
    out_comb = _run_dir_bills(run_dir, f"comb_bills_year_run_with_lmi_{suffix}.csv")

    if not upload:
        print(f"Would write {out_elec}, {out_gas}, {out_comb} (run with --upload)")
        return
    _upload_discounted_bills(elec_bills, gas_bills, run_dir, suffix, opts)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Apply NY EAP/EEAP discounts to CAIRO electric and gas bills."
    )
    parser.add_argument(
        "--run-dir", required=True, help="S3 path to CAIRO run directory"
    )
    parser.add_argument("--state", required=True, help="State abbreviation (NY)")
    parser.add_argument(
        "--utility", required=True, help="Electric utility std_name (e.g. coned, nimo)"
    )
    parser.add_argument(
        "--resstock-release", default="res_2024_amy2018_2", help="ResStock release name"
    )
    parser.add_argument("--upgrade", default="00", help="ResStock upgrade ID")
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
        default="uniform",
        help="Participation sampling: uniform or weighted by need",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="RNG seed for participation"
    )
    parser.add_argument(
        "--rider",
        action="store_true",
        help="Apply cost-recovery rider to non-participants",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Write outputs to S3; otherwise print paths only",
    )
    args = parser.parse_args()

    run_dir = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    s3_base = f"s3://data.sb/nrel/resstock/{args.resstock_release}"
    meta_path = f"{s3_base}/metadata/state={args.state}/upgrade={args.upgrade}/metadata-sb.parquet"
    util_path = f"{s3_base}/metadata_utility/state={args.state}"
    opts = _storage_opts()

    # 1. Load CPI and compute income-inflation ratio
    cpi_ratio = load_cpi_ratio(args.cpi_s3_path, args.fpl_year, opts)

    # 2. Build per-bldg tier and consumption
    tier_consumption = _build_tier_consumption(
        meta_path,
        util_path,
        args.utility,
        args.fpl_year,
        cpi_ratio,
        args.participation_rate,
        args.participation_mode,
        args.seed,
        opts,
    )

    # 3. Load bills, join tier, apply fixed monthly credits and optional rider
    elec_bills, gas_bills = _apply_discounts_to_bills(
        run_dir, tier_consumption, args.utility, args.rider, opts
    )

    # 4. Write outputs (or print paths)
    _write_or_print_outputs(
        elec_bills,
        gas_bills,
        run_dir,
        args.participation_rate,
        args.rider,
        args.upload,
        opts,
    )


if __name__ == "__main__":
    main()

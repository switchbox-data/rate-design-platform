"""Apply RI LIDR+ LMI discounts to CAIRO bill outputs (postprocessing).

Reads bills and ResStock metadata from S3, assigns tiers by FPL%, applies
tiered percentage discounts and optional volumetric cost-recovery rider,
writes discounted bills with lmi_tier to the run directory.

Uses polars lazy execution; minimal collects for rider totals and weighted
participation.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from utils.eia_region_config import get_aws_storage_options
from utils.post.lmi_common import (
    RESSTOCK_INCOME_DOLLAR_YEAR,
    assign_ri_tier_expr,
    discount_fractions_for_ri,
    fpl_pct_expr,
    fpl_threshold_expr,
    inflate_income_expr,
    load_fpl_guidelines,
    parse_occupants_expr,
    participation_uniform_expr,
    select_participants_weighted,
)

# Bill CSV: CAIRO writes long format (bldg_id, weight, month, bill_level, dollar_year)
# month values: Jan, Feb, ..., Dec, Annual
ANNUAL_MONTH_VALUE = "Annual"
BLDG_ID_COL = "bldg_id"
# ResStock gas energy: column is in kWh; convert to therms for rider (1 therm ≈ 29.3 kWh)
KWH_PER_THERM = 29.3


def _storage_opts() -> dict[str, str]:
    return get_aws_storage_options()


def _run_dir_bills(run_dir: S3Path | Path, name: str) -> str:
    return str(run_dir / "bills" / name)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Apply RI LIDR+ discounts to CAIRO electric and gas bills."
    )
    parser.add_argument(
        "--run-dir", required=True, help="S3 path to CAIRO run directory"
    )
    parser.add_argument("--state", required=True, help="State abbreviation (e.g. RI)")
    parser.add_argument(
        "--utility", required=True, help="Electric utility code (e.g. rie)"
    )
    parser.add_argument(
        "--resstock-release",
        default="res_2024_amy2018_2",
        help="ResStock release name",
    )
    parser.add_argument("--upgrade", default="00", help="ResStock upgrade ID")
    parser.add_argument(
        "--inflation-year",
        type=int,
        required=True,
        help="Target dollar year for income (must match FPL guideline year)",
    )
    parser.add_argument(
        "--fpl-guideline-year",
        type=int,
        default=None,
        help="FPL guideline year (default: same as inflation-year)",
    )
    parser.add_argument(
        "--cpi-s3-path",
        required=True,
        help="S3 path to CPI parquet (year, cpi_value) from fetch_cpi_from_fred.py",
    )
    parser.add_argument(
        "--participation-rate",
        type=float,
        default=1.0,
        help="Fraction of eligible customers who participate (0–1)",
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

    fpl_year = (
        args.fpl_guideline_year
        if args.fpl_guideline_year is not None
        else args.inflation_year
    )
    if fpl_year != args.inflation_year:
        parser.error("FPL guideline year must match inflation-year (or omit)")

    run_dir = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    s3_base = f"s3://data.sb/nrel/resstock/{args.resstock_release}"
    meta_path = f"{s3_base}/metadata/state={args.state}/upgrade={args.upgrade}/metadata-sb.parquet"
    util_path = f"{s3_base}/metadata_utility/state={args.state}"
    opts = _storage_opts()

    # Load FPL and CPI (scan + filter to two years, then collect minimal rows)
    fpl = load_fpl_guidelines(args.inflation_year)
    cpi_df = (
        pl.scan_parquet(args.cpi_s3_path, storage_options=opts)
        .filter(
            pl.col("year").is_in([RESSTOCK_INCOME_DOLLAR_YEAR, args.inflation_year])
        )
        .collect()
    )
    assert isinstance(cpi_df, pl.DataFrame)
    cpi_2019 = cpi_df.filter(pl.col("year") == RESSTOCK_INCOME_DOLLAR_YEAR)
    cpi_target = cpi_df.filter(pl.col("year") == args.inflation_year)
    if cpi_2019.is_empty() or cpi_target.is_empty():
        raise ValueError(
            f"CPI data must contain {RESSTOCK_INCOME_DOLLAR_YEAR} and {args.inflation_year}"
        )
    cpi_ratio = float(cpi_target["cpi_value"][0]) / float(cpi_2019["cpi_value"][0])

    # Lazy: metadata + utility filter (no vacancy filter; if bldg has bills, apply discount)
    meta = pl.scan_parquet(meta_path, storage_options=opts)
    util = pl.scan_parquet(util_path, storage_options=opts, hive_partitioning=True)
    meta = meta.join(
        util.filter(pl.col("electric_utility") == args.utility).select(BLDG_ID_COL),
        on=BLDG_ID_COL,
        how="inner",
    )

    # Occupants (numeric), inflated income, FPL threshold, FPL%, tier
    occupants_num = "occupants_num"
    income_inflated = "income_inflated"
    fpl_threshold = "fpl_threshold"
    fpl_pct = "fpl_pct"
    tier_col = "lmi_tier_raw"
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
    meta = meta.with_columns(assign_ri_tier_expr(fpl_pct).alias(tier_col))
    eligible = pl.col(tier_col) >= 1

    # Participation
    if args.participation_rate >= 1.0:
        meta = meta.with_columns(eligible.alias("participates"))
    elif args.participation_mode == "uniform":
        participates = participation_uniform_expr(
            BLDG_ID_COL, args.participation_rate, args.seed, eligible
        )
        meta = meta.with_columns(participates.alias("participates"))
    else:
        eligible_df = (
            meta.filter(eligible)
            .select(BLDG_ID_COL, fpl_pct, tier_col)
            .with_columns((1.0 / pl.col(fpl_pct).clip(1.0, None)).alias("weight"))
            .collect()
        )
        assert isinstance(eligible_df, pl.DataFrame)
        part_df = select_participants_weighted(
            eligible_df, args.participation_rate, args.seed, "weight", BLDG_ID_COL
        )
        meta = meta.join(part_df.lazy(), on=BLDG_ID_COL, how="left")
        meta = meta.with_columns(
            pl.when(eligible)
            .then(pl.col("participates").fill_null(False))
            .otherwise(pl.lit(False))
            .alias("participates")
        )

    # lmi_tier: 0 if not participant else tier
    meta = meta.with_columns(
        pl.when(pl.col("participates"))
        .then(pl.col(tier_col))
        .otherwise(pl.lit(0))
        .alias("lmi_tier")
    )

    # Consumption for rider (from metadata); gas in therms
    elec_kwh_col = "out.electricity.total.energy_consumption.kwh"
    gas_kwh_col = "out.natural_gas.total.energy_consumption.kwh"
    meta = meta.with_columns((pl.col(gas_kwh_col) / KWH_PER_THERM).alias("gas_therms"))
    tier_consumption = meta.select(
        BLDG_ID_COL,
        "lmi_tier",
        tier_col,
        "participates",
        pl.col(elec_kwh_col).alias("elec_kwh"),
        pl.col("gas_therms"),
    )

    # Bills (lazy): CAIRO long format = bldg_id, weight, month, bill_level, dollar_year
    storage = opts if isinstance(run_dir, S3Path) else None
    elec_bills = pl.scan_csv(
        _run_dir_bills(run_dir, "elec_bills_year_run.csv"), storage_options=storage
    )
    gas_bills = pl.scan_csv(
        _run_dir_bills(run_dir, "gas_bills_year_run.csv"), storage_options=storage
    )

    # Original annual bill per bldg (from the "Annual" month row)
    elec_annual = elec_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE).select(
        BLDG_ID_COL, pl.col("bill_level").alias("original_annual_elec")
    )
    gas_annual = gas_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE).select(
        BLDG_ID_COL, pl.col("bill_level").alias("original_annual_gas")
    )

    # Join tier, consumption, and original annual to bills
    tc_lazy = tier_consumption
    elec_bills = elec_bills.join(
        tc_lazy.select(BLDG_ID_COL, "lmi_tier", tier_col, "participates", "elec_kwh"),
        on=BLDG_ID_COL,
        how="left",
    ).join(elec_annual, on=BLDG_ID_COL, how="left")
    gas_bills = gas_bills.join(
        tc_lazy.select(BLDG_ID_COL, "lmi_tier", tier_col, "participates", "gas_therms"),
        on=BLDG_ID_COL,
        how="left",
    ).join(gas_annual, on=BLDG_ID_COL, how="left")
    elec_bills = elec_bills.with_columns(pl.col("lmi_tier").fill_null(0))
    gas_bills = gas_bills.with_columns(pl.col("lmi_tier").fill_null(0))

    # Discount amount per customer (same for all 13 rows per bldg; used for rider)
    elec_disc, gas_disc = discount_fractions_for_ri()
    disc_elec_expr = pl.when(pl.col("lmi_tier") == 3).then(
        pl.col("original_annual_elec") * elec_disc[3]
    )
    disc_elec_expr = disc_elec_expr.when(pl.col("lmi_tier") == 2).then(
        pl.col("original_annual_elec") * elec_disc[2]
    )
    disc_elec_expr = disc_elec_expr.when(pl.col("lmi_tier") == 1).then(
        pl.col("original_annual_elec") * elec_disc[1]
    )
    disc_elec_expr = disc_elec_expr.otherwise(0.0)
    elec_bills = elec_bills.with_columns(disc_elec_expr.alias("discount_elec"))
    disc_gas_expr = pl.when(pl.col("lmi_tier") == 3).then(
        pl.col("original_annual_gas") * gas_disc[3]
    )
    disc_gas_expr = disc_gas_expr.when(pl.col("lmi_tier") == 2).then(
        pl.col("original_annual_gas") * gas_disc[2]
    )
    disc_gas_expr = disc_gas_expr.when(pl.col("lmi_tier") == 1).then(
        pl.col("original_annual_gas") * gas_disc[1]
    )
    disc_gas_expr = disc_gas_expr.otherwise(0.0)
    gas_bills = gas_bills.with_columns(disc_gas_expr.alias("discount_gas"))

    # Rider totals from Annual rows only (one row per bldg) to avoid 13x overcount
    if args.rider:
        elec_annual_rows = elec_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE)
        gas_annual_rows = gas_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE)
        elec_totals_df = elec_annual_rows.select(
            pl.col("discount_elec").sum().alias("total_discount_elec"),
            pl.when(~pl.col("participates").fill_null(False))
            .then(pl.col("elec_kwh"))
            .otherwise(0)
            .sum()
            .alias("total_kwh_non"),
        ).collect()
        gas_totals_df = gas_annual_rows.select(
            pl.col("discount_gas").sum().alias("total_discount_gas"),
            pl.when(~pl.col("participates").fill_null(False))
            .then(pl.col("gas_therms"))
            .otherwise(0)
            .sum()
            .alias("total_gas_non"),
        ).collect()
        assert isinstance(elec_totals_df, pl.DataFrame) and isinstance(
            gas_totals_df, pl.DataFrame
        )
        row_elec = elec_totals_df.row(0)
        row_gas = gas_totals_df.row(0)
        td_elec = float(row_elec[0] or 0.0)
        tk_non = float(row_elec[1] or 0.0)
        td_gas = float(row_gas[0] or 0.0)
        tg_non = float(row_gas[1] or 0.0)
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

    # Apply discount and rider in long format: one row per (bldg_id, month)
    mult_elec = pl.when(pl.col("lmi_tier") == 3).then(1.0 - elec_disc[3])
    mult_elec = mult_elec.when(pl.col("lmi_tier") == 2).then(1.0 - elec_disc[2])
    mult_elec = mult_elec.when(pl.col("lmi_tier") == 1).then(1.0 - elec_disc[1])
    mult_elec = mult_elec.otherwise(1.0)
    mult_gas = pl.when(pl.col("lmi_tier") == 3).then(1.0 - gas_disc[3])
    mult_gas = mult_gas.when(pl.col("lmi_tier") == 2).then(1.0 - gas_disc[2])
    mult_gas = mult_gas.when(pl.col("lmi_tier") == 1).then(1.0 - gas_disc[1])
    mult_gas = mult_gas.otherwise(1.0)
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
        (pl.col("bill_level") * mult_elec + rider_elec_row).alias("bill_level")
    )
    gas_bills = gas_bills.with_columns(
        (pl.col("bill_level") * mult_gas + rider_gas_row).alias("bill_level")
    )

    # Drop helper columns for output (keep bldg_id, weight, month, bill_level, dollar_year, lmi_tier)
    drop_elec = [
        c
        for c in [
            "participates",
            "elec_kwh",
            "original_annual_elec",
            "discount_elec",
            tier_col,
        ]
        if c in elec_bills.collect_schema().names()
    ]
    drop_gas = [
        c
        for c in [
            "participates",
            "gas_therms",
            "original_annual_gas",
            "discount_gas",
            tier_col,
        ]
        if c in gas_bills.collect_schema().names()
    ]
    if drop_elec:
        elec_bills = elec_bills.drop(drop_elec)
    if drop_gas:
        gas_bills = gas_bills.drop(drop_gas)

    # Output filename suffix
    pct_label = int(round(args.participation_rate * 100))
    rider_label = "rider" if args.rider else "no_rider"
    suffix = f"p{pct_label}_{rider_label}"
    out_elec = _run_dir_bills(run_dir, f"elec_bills_year_run_with_lmi_{suffix}.csv")
    out_gas = _run_dir_bills(run_dir, f"gas_bills_year_run_with_lmi_{suffix}.csv")
    out_comb = _run_dir_bills(run_dir, f"comb_bills_year_run_with_lmi_{suffix}.csv")

    if args.upload:
        elec_bills.sink_csv(
            out_elec, storage_options=opts if isinstance(run_dir, S3Path) else None
        )
        gas_bills.sink_csv(
            out_gas, storage_options=opts if isinstance(run_dir, S3Path) else None
        )
        # Combined: merge elec + gas on (bldg_id, weight, month, dollar_year), sum bill_level
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
        comb.sink_csv(
            out_comb, storage_options=opts if isinstance(run_dir, S3Path) else None
        )
        print(f"Wrote {out_elec}, {out_gas}, {out_comb}")
    else:
        print(f"Would write {out_elec}, {out_gas}, {out_comb} (run with --upload)")


if __name__ == "__main__":
    main()

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

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.post.lmi_common import (
    assign_ri_tier_expr,
    discount_fractions_for_ri,
    fpl_pct_expr,
    fpl_threshold_expr,
    inflate_income_expr,
    load_cpi_ratio,
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


def _build_ri_raw_tiers(
    meta_path: str,
    util_path: str,
    utility: str,
    inflation_year: int,
    cpi_ratio: float,
    opts: dict[str, str],
) -> pl.DataFrame:
    """Load metadata for utility and compute FPL% and eligibility tier (no participation).

    Returns a collected DataFrame with bldg_id, lmi_tier_raw (raw eligibility
    tier 0-3), is_lmi, fpl_pct, elec_kwh, gas_therms.  fpl_pct is retained so
    _sample_ri_participation can do weighted sampling without re-reading metadata.
    """
    tier_col = "lmi_tier_raw"
    occupants_num = "occupants_num"
    income_inflated = "income_inflated"
    fpl_threshold = "fpl_threshold"
    fpl_pct = "fpl_pct"
    elec_kwh_col = "out.electricity.total.energy_consumption.kwh"
    gas_kwh_col = "out.natural_gas.total.energy_consumption.kwh"

    fpl = load_fpl_guidelines(inflation_year)
    meta = pl.scan_parquet(meta_path, storage_options=opts)
    util = pl.scan_parquet(util_path, storage_options=opts, hive_partitioning=True)
    meta = meta.join(
        util.filter(pl.col("electric_utility") == utility).select(BLDG_ID_COL),
        on=BLDG_ID_COL,
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
    meta = meta.with_columns(assign_ri_tier_expr(fpl_pct).alias(tier_col))
    meta = meta.with_columns(
        (pl.col(gas_kwh_col) / KWH_PER_THERM).alias("gas_therms"),
        (pl.col(tier_col) >= 1).alias("is_lmi"),
    )
    result = meta.select(
        BLDG_ID_COL,
        tier_col,
        "is_lmi",
        fpl_pct,
        pl.col(elec_kwh_col).alias("elec_kwh"),
        "gas_therms",
    ).collect()
    assert isinstance(result, pl.DataFrame)
    return result


def _sample_ri_participation(
    raw_tiers: pl.DataFrame,
    participation_rate: float,
    participation_mode: str,
    seed: int,
) -> pl.DataFrame:
    """Add participation flags to a raw-tier DataFrame.

    Args:
        raw_tiers: DataFrame from _build_ri_raw_tiers with bldg_id, lmi_tier_raw,
            is_lmi, fpl_pct, elec_kwh, gas_therms.

    Returns a DataFrame with bldg_id, lmi_tier (participation-adjusted: 0 for
    non-participants), lmi_tier_raw, is_lmi, participates, elec_kwh, gas_therms.
    """
    tier_col = "lmi_tier_raw"
    fpl_pct = "fpl_pct"
    eligible = pl.col(tier_col) >= 1

    if participation_rate >= 1.0:
        result = raw_tiers.with_columns(eligible.alias("participates"))
    elif participation_mode == "uniform":
        result = raw_tiers.with_columns(
            participation_uniform_expr(
                BLDG_ID_COL, participation_rate, seed, eligible
            ).alias("participates")
        )
    else:
        eligible_df = (
            raw_tiers.filter(eligible)
            .with_columns(
                (1.0 / pl.col(fpl_pct).clip(lower_bound=1.0)).alias("weight")
            )
            .select(BLDG_ID_COL, fpl_pct, tier_col, "weight")
        )
        part_df = select_participants_weighted(
            eligible_df, participation_rate, seed, "weight", BLDG_ID_COL
        )
        result = raw_tiers.join(part_df, on=BLDG_ID_COL, how="left")
        result = result.with_columns(
            pl.when(eligible)
            .then(pl.col("participates").fill_null(False))
            .otherwise(pl.lit(False))
            .alias("participates")
        )

    # lmi_tier (participation-adjusted): 0 for non-participants (used by CSV workflow)
    result = result.with_columns(
        pl.when(pl.col("participates"))
        .then(pl.col(tier_col))
        .otherwise(pl.lit(0))
        .alias("lmi_tier")
    )
    return result.select(
        BLDG_ID_COL,
        "lmi_tier",
        tier_col,
        "is_lmi",
        "participates",
        "elec_kwh",
        "gas_therms",
    )


def _build_tier_consumption(
    meta_path: str,
    util_path: str,
    utility: str,
    inflation_year: int,
    cpi_ratio: float,
    participation_rate: float,
    participation_mode: str,
    seed: int,
    opts: dict[str, str],
) -> pl.LazyFrame:
    """Build per-building tier and consumption for the CSV discount workflow.

    Delegates to _build_ri_raw_tiers + _sample_ri_participation, then returns
    a LazyFrame for downstream lazy joins in _apply_discounts_to_bills.
    """
    raw = _build_ri_raw_tiers(
        meta_path=meta_path,
        util_path=util_path,
        utility=utility,
        inflation_year=inflation_year,
        cpi_ratio=cpi_ratio,
        opts=opts,
    )
    return _sample_ri_participation(
        raw, participation_rate, participation_mode, seed
    ).lazy()


def _apply_discounts_to_bills(
    run_dir: S3Path | Path,
    tier_consumption: pl.LazyFrame,
    rider: bool,
    opts: dict[str, str],
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Load elec/gas bill CSVs (long format), join tier/consumption, apply tiered
    discount and optional rider, drop helper columns. Returns (elec_bills, gas_bills).
    """
    tier_col = "lmi_tier_raw"
    storage = opts if isinstance(run_dir, S3Path) else None
    elec_bills = pl.scan_csv(
        _run_dir_bills(run_dir, "elec_bills_year_run.csv"), storage_options=storage
    )
    gas_bills = pl.scan_csv(
        _run_dir_bills(run_dir, "gas_bills_year_run.csv"), storage_options=storage
    )

    elec_annual = elec_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE).select(
        BLDG_ID_COL, pl.col("bill_level").alias("original_annual_elec")
    )
    gas_annual = gas_bills.filter(pl.col("month") == ANNUAL_MONTH_VALUE).select(
        BLDG_ID_COL, pl.col("bill_level").alias("original_annual_gas")
    )
    tc = tier_consumption
    elec_bills = elec_bills.join(
        tc.select(BLDG_ID_COL, "lmi_tier", tier_col, "participates", "elec_kwh"),
        on=BLDG_ID_COL,
        how="left",
    ).join(elec_annual, on=BLDG_ID_COL, how="left")
    gas_bills = gas_bills.join(
        tc.select(BLDG_ID_COL, "lmi_tier", tier_col, "participates", "gas_therms"),
        on=BLDG_ID_COL,
        how="left",
    ).join(gas_annual, on=BLDG_ID_COL, how="left")
    elec_bills = elec_bills.with_columns(pl.col("lmi_tier").fill_null(0))
    gas_bills = gas_bills.with_columns(pl.col("lmi_tier").fill_null(0))

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

    if rider:
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
    return elec_bills, gas_bills


def apply_ri_lmi_to_master(
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
    """Append RI LIDR+ LMI columns to a master bills DataFrame.

    Loads ResStock metadata and LIDR+ config once, builds eligibility tiers once,
    then loops over each participation rate — adding a set of LMI columns per
    rate — so multi-rate runs do not re-read S3 metadata.

    Output columns added per rate (pct = int(rate * 100)):
        lmi_tier (Int32)              — shared across all rates; added on first call
        is_lmi (Bool)                 — shared across all rates; added on first call
        applied_discount_{pct} (Bool)
        elec_total_bill_lmi_{pct} (Float64)
        gas_total_bill_lmi_{pct} (Float64)

    Discounts are tier-based percentage reductions: bill * (1 - disc_frac).
    The cost-recovery rider is not applied in the master-bills workflow.
    """
    s3_base = f"{path_resstock_release.rstrip('/')}"
    meta_path = (
        f"{s3_base}/metadata/state={state_upper}/upgrade={upgrade}/metadata-sb.parquet"
    )
    util_path = f"{s3_base}/metadata_utility/state={state_upper}"

    print(f"[RI LMI] Loading CPI ratio (fpl_year={lmi_fpl_year})...")
    cpi_ratio = load_cpi_ratio(lmi_cpi_s3_path, lmi_fpl_year, opts)

    print(f"[RI LMI] Building eligibility tiers for {utility}...")
    raw_tiers = _build_ri_raw_tiers(
        meta_path=meta_path,
        util_path=util_path,
        utility=utility,
        inflation_year=lmi_fpl_year,
        cpi_ratio=cpi_ratio,
        opts=opts,
    )
    print(f"[RI LMI] {raw_tiers.height} buildings, {raw_tiers['is_lmi'].sum()} eligible")

    disc_elec, disc_gas = discount_fractions_for_ri()

    for rate in participation_rates:
        pct_label = int(round(rate * 100))
        print(f"[RI LMI] Applying discounts (p{pct_label})...")
        tier_info = _sample_ri_participation(raw_tiers, rate, participation_mode, seed)

        elec_col = f"elec_total_bill_lmi_{pct_label}"
        gas_col = f"gas_total_bill_lmi_{pct_label}"
        applied_col = f"applied_discount_{pct_label}"

        # Multipliers use the master bills lmi_tier column, which stores the raw
        # eligibility tier (lmi_tier_raw was renamed to lmi_tier on the first join).
        mult_elec = (
            pl.when(pl.col("lmi_tier") == 3)
            .then(pl.lit(1.0 - disc_elec[3]))
            .when(pl.col("lmi_tier") == 2)
            .then(pl.lit(1.0 - disc_elec[2]))
            .when(pl.col("lmi_tier") == 1)
            .then(pl.lit(1.0 - disc_elec[1]))
            .otherwise(pl.lit(1.0))
        )
        mult_gas = (
            pl.when(pl.col("lmi_tier") == 3)
            .then(pl.lit(1.0 - disc_gas[3]))
            .when(pl.col("lmi_tier") == 2)
            .then(pl.lit(1.0 - disc_gas[2]))
            .when(pl.col("lmi_tier") == 1)
            .then(pl.lit(1.0 - disc_gas[1]))
            .otherwise(pl.lit(1.0))
        )

        # First rate: join lmi_tier_raw, is_lmi, and participates together.
        # Subsequent rates: lmi_tier/is_lmi already present; only join participates.
        if "lmi_tier" in master.columns:
            enriched = master.join(
                tier_info.select(BLDG_ID_COL, "participates"),
                on=BLDG_ID_COL,
                how="left",
            ).with_columns(pl.col("participates").fill_null(False))
        else:
            enriched = master.join(
                tier_info.select(
                    BLDG_ID_COL, "lmi_tier_raw", "is_lmi", "participates"
                ).rename({"lmi_tier_raw": "lmi_tier"}),
                on=BLDG_ID_COL,
                how="left",
            ).with_columns(
                pl.col("lmi_tier").fill_null(0).cast(pl.Int32),
                pl.col("is_lmi").fill_null(False),
                pl.col("participates").fill_null(False),
            )

        master = enriched.with_columns(
            pl.when(pl.col("participates"))
            .then(
                (pl.col("elec_total_bill") * mult_elec).clip(lower_bound=0.0)
            )
            .otherwise(pl.col("elec_total_bill"))
            .alias(elec_col),
            pl.when(pl.col("participates"))
            .then(
                (pl.col("gas_total_bill") * mult_gas).clip(lower_bound=0.0)
            )
            .otherwise(pl.col("gas_total_bill"))
            .alias(gas_col),
            pl.col("participates").alias(applied_col),
        ).drop("participates")

        n_part = master.filter(pl.col("month") == ANNUAL_MONTH_VALUE).filter(
            pl.col(applied_col)
        )[BLDG_ID_COL].n_unique()
        print(f"[RI LMI] p{pct_label}: {n_part} participating buildings")

    return master


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
        default="res_2024_amy2018_2_sb",
        help="ResStock release name",
    )
    parser.add_argument("--upgrade", default="00", help="ResStock upgrade ID")
    parser.add_argument(
        "--fpl-year",
        type=int,
        required=True,
        help="FPL guideline year; income is inflated from 2019 to this year before comparing",
    )
    parser.add_argument(
        "--cpi-s3-path",
        required=True,
        help="S3 path to CPI parquet (year, value) from data/fred/cpi/fetch_cpi_parquet.py (default series CPIAUCSL)",
    )
    parser.add_argument(
        "--participation-rates",
        type=float,
        nargs="+",
        default=[1.0],
        help="One or more participation fractions (0–1). Each rate produces a separate "
        "set of LMI output files. Example: --participation-rates 1.0 0.4",
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

    # 1. Load CPI and build eligibility tiers once (shared across all rates)
    cpi_ratio = load_cpi_ratio(args.cpi_s3_path, args.fpl_year, opts)
    raw_tiers = _build_ri_raw_tiers(
        meta_path,
        util_path,
        args.utility,
        args.fpl_year,
        cpi_ratio,
        opts,
    )

    # 2. For each participation rate: sample, apply discounts, write outputs
    for rate in args.participation_rates:
        tier_consumption = _sample_ri_participation(
            raw_tiers, rate, args.participation_mode, args.seed
        ).lazy()

        elec_bills, gas_bills = _apply_discounts_to_bills(
            run_dir, tier_consumption, args.rider, opts
        )

        _write_or_print_outputs(
            elec_bills,
            gas_bills,
            run_dir,
            rate,
            args.rider,
            args.upload,
            opts,
        )


if __name__ == "__main__":
    main()

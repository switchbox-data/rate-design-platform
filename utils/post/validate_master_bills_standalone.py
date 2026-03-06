"""Standalone validation of master bills table: source round-trips, sanity checks, spot-check report.

Does NOT depend on the old code path. Can run in parallel with validate_master_bills.py.

Checks:
  1. Source round-trips: each output column traces back exactly to its source file.
  2. Aggregate sanity: plausible ranges, zero/nonzero consistency, structural integrity.
  3. Spot-check report: full arithmetic trace for a few buildings (human review).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import cast

import polars as pl

from utils.post.build_master_bills import (
    _build_output_path_suffix,
    _find_run_dir,
    _parse_batch_overrides,
)
from utils.post.io import (
    ANNUAL_MONTH,
    BLDG_ID,
    BILL_LEVEL,
    scan,
)

ELEC_BILLS_CSV = "bills/elec_bills_year_target.csv"
GAS_BILLS_CSV = "bills/gas_bills_year_target.csv"

FLOAT_TOL = 1e-4


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _read_fixed_charge(state: str, utility: str) -> float:
    repo_root = Path(__file__).resolve().parents[2]
    tariff_path = (
        repo_root
        / "rate_design"
        / "hp_rates"
        / state
        / "config"
        / "tariffs"
        / "electric"
        / f"{utility}_flat.json"
    )
    with tariff_path.open() as f:
        data = json.load(f)
    return float(data["items"][0]["fixedchargefirstmeter"])


# ---------------------------------------------------------------------------
# 1. Source round-trip checks
# ---------------------------------------------------------------------------


def _check_source_round_trips(
    master: pl.DataFrame,
    utility: str,
    state: str,
    util_batch: str,
    run_delivery: int,
    run_supply: int,
) -> list[tuple[str, bool, str]]:
    """Verify each output column traces back to its source file."""
    results: list[tuple[str, bool, str]] = []

    s3_base = (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{util_batch}"
    )
    _log(f"  Using batch {util_batch} for {utility}")
    dir_delivery = _find_run_dir(s3_base, run_delivery)
    dir_supply = _find_run_dir(s3_base, run_supply)

    util_rows = master.filter(pl.col("sb.electric_utility") == utility)

    # elec_total_bill == bill_level from supply run elec_bills_year_target
    _log("  Round-trip: elec_total_bill vs supply run elec_bills_year_target...")
    elec_supply_src = cast(
        pl.DataFrame, scan(f"{dir_supply}/{ELEC_BILLS_CSV}").collect()
    )
    check = util_rows.select(BLDG_ID, "month", "elec_total_bill").join(
        elec_supply_src.select(BLDG_ID, "month", pl.col(BILL_LEVEL).alias("src")),
        on=[BLDG_ID, "month"],
        how="inner",
    )
    diff = (check["elec_total_bill"] - check["src"]).abs()
    max_diff = cast(float, diff.max()) if diff.len() > 0 else 0.0
    n_bad = (diff > FLOAT_TOL).sum()
    ok = n_bad == 0
    results.append(
        (
            "elec_total_bill == supply elec src",
            ok,
            f"max_diff={max_diff:.8f}, n_bad={n_bad}",
        )
    )

    # gas_total_bill == bill_level from supply run gas_bills_year_target
    _log("  Round-trip: gas_total_bill vs supply run gas_bills_year_target...")
    gas_src = cast(pl.DataFrame, scan(f"{dir_supply}/{GAS_BILLS_CSV}").collect())
    check = util_rows.select(BLDG_ID, "month", "gas_total_bill").join(
        gas_src.select(BLDG_ID, "month", pl.col(BILL_LEVEL).alias("src")),
        on=[BLDG_ID, "month"],
        how="inner",
    )
    diff = (check["gas_total_bill"] - check["src"]).abs()
    max_diff = cast(float, diff.max()) if diff.len() > 0 else 0.0
    n_bad = (diff > FLOAT_TOL).sum()
    ok = n_bad == 0
    results.append(
        (
            "gas_total_bill == supply gas src",
            ok,
            f"max_diff={max_diff:.8f}, n_bad={n_bad}",
        )
    )

    # elec_fixed_charge + elec_delivery_bill == bill_level from delivery run
    _log("  Round-trip: elec_fixed + elec_delivery vs delivery run elec src...")
    elec_delivery_src = cast(
        pl.DataFrame, scan(f"{dir_delivery}/{ELEC_BILLS_CSV}").collect()
    )
    check = (
        util_rows.select(BLDG_ID, "month", "elec_fixed_charge", "elec_delivery_bill")
        .join(
            elec_delivery_src.select(BLDG_ID, "month", pl.col(BILL_LEVEL).alias("src")),
            on=[BLDG_ID, "month"],
            how="inner",
        )
        .with_columns(
            (pl.col("elec_fixed_charge") + pl.col("elec_delivery_bill")).alias(
                "computed"
            )
        )
    )
    diff = (check["computed"] - check["src"]).abs()
    max_diff = cast(float, diff.max()) if diff.len() > 0 else 0.0
    n_bad = (diff > FLOAT_TOL).sum()
    ok = n_bad == 0
    results.append(
        (
            "elec_fixed + elec_delivery == delivery src",
            ok,
            f"max_diff={max_diff:.8f}, n_bad={n_bad}",
        )
    )

    # elec_fixed_charge == fixedchargefirstmeter (monthly) or *12 (Annual)
    _log("  Round-trip: elec_fixed_charge vs tariff JSON...")
    monthly_fc = _read_fixed_charge(state, utility)
    monthly_rows = util_rows.filter(pl.col("month") != ANNUAL_MONTH)
    annual_rows = util_rows.filter(pl.col("month") == ANNUAL_MONTH)
    diff_monthly = (monthly_rows["elec_fixed_charge"] - monthly_fc).abs()
    diff_annual = (annual_rows["elec_fixed_charge"] - monthly_fc * 12).abs()
    max_diff_m = cast(float, diff_monthly.max()) if diff_monthly.len() > 0 else 0.0
    max_diff_a = cast(float, diff_annual.max()) if diff_annual.len() > 0 else 0.0
    n_bad_m = (diff_monthly > FLOAT_TOL).sum()
    n_bad_a = (diff_annual > FLOAT_TOL).sum()
    ok = n_bad_m == 0 and n_bad_a == 0
    results.append(
        (
            "elec_fixed_charge == tariff JSON",
            ok,
            f"monthly: max_diff={max_diff_m:.8f}, n_bad={n_bad_m}; "
            f"annual: max_diff={max_diff_a:.8f}, n_bad={n_bad_a}",
        )
    )

    return results


# ---------------------------------------------------------------------------
# 2. Aggregate sanity checks
# ---------------------------------------------------------------------------


def _check_aggregate_sanity(
    master: pl.DataFrame, utility: str
) -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    util = master.filter(pl.col("sb.electric_utility") == utility)
    annual = util.filter(pl.col("month") == ANNUAL_MONTH)

    # No negative bills
    for col in [
        "elec_total_bill",
        "gas_total_bill",
        "oil_total_bill",
        "propane_total_bill",
    ]:
        n_neg = annual.filter(pl.col(col) < -FLOAT_TOL).height
        ok = n_neg == 0
        results.append((f"no negative {col}", ok, f"n_negative={n_neg}"))

    # Plausible elec range
    mean_elec = cast(
        float,
        annual.select(
            (pl.col("elec_total_bill") * pl.col("weight")).sum()
            / pl.col("weight").sum()
        ).item(),
    )
    ok = 500 <= mean_elec <= 5000
    level = "PASS" if ok else "WARN"
    results.append(
        (f"mean annual elec in $500-$5000 ({level})", ok, f"${mean_elec:,.0f}")
    )

    # Zero/nonzero consistency
    for fuel, bill_col in [
        ("heats_with_oil", "oil_total_bill"),
        ("heats_with_propane", "propane_total_bill"),
    ]:
        non_heater_with_bill = annual.filter(
            (pl.col(fuel) == False) & (pl.col(bill_col) > FLOAT_TOL)  # noqa: E712
        ).height
        results.append(
            (
                f"{bill_col} > 0 only when {fuel} (info-only)",
                True,
                f"secondary-use buildings={non_heater_with_bill}",
            )
        )

    # 13 rows per building
    counts = util.group_by(BLDG_ID).agg(pl.len().alias("n"))
    bad_counts = counts.filter(pl.col("n") != 13)
    ok = bad_counts.is_empty()
    results.append(("13 rows per building", ok, f"exceptions={bad_counts.height}"))

    # Annual == sum of monthly for bill components
    monthly_sums = (
        util.filter(pl.col("month") != ANNUAL_MONTH)
        .group_by(BLDG_ID)
        .agg(
            pl.col("elec_total_bill").sum().alias("elec_sum"),
            pl.col("gas_total_bill").sum().alias("gas_sum"),
            pl.col("oil_total_bill").sum().alias("oil_sum"),
            pl.col("propane_total_bill").sum().alias("propane_sum"),
        )
    )
    annual_vals = annual.select(
        BLDG_ID,
        pl.col("elec_total_bill").alias("elec_annual"),
        pl.col("gas_total_bill").alias("gas_annual"),
        pl.col("oil_total_bill").alias("oil_annual"),
        pl.col("propane_total_bill").alias("propane_annual"),
    )
    check = monthly_sums.join(annual_vals, on=BLDG_ID, how="inner")
    for prefix in ["elec", "gas", "oil", "propane"]:
        diff = (check[f"{prefix}_sum"] - check[f"{prefix}_annual"]).abs()
        max_diff = cast(float, diff.max()) if diff.len() > 0 else 0.0
        n_bad = (diff > FLOAT_TOL).sum()
        ok = n_bad == 0
        results.append(
            (
                f"annual {prefix} == sum(monthly)",
                ok,
                f"max_diff={max_diff:.6f}, n_bad={n_bad}",
            )
        )

    # Consistent weight per building
    weight_var = util.group_by(BLDG_ID).agg(
        pl.col("weight").n_unique().alias("n_weights")
    )
    multi_weight = weight_var.filter(pl.col("n_weights") > 1)
    ok = multi_weight.is_empty()
    results.append(
        ("consistent weight per building", ok, f"exceptions={multi_weight.height}")
    )

    return results


# ---------------------------------------------------------------------------
# 3. Spot-check report
# ---------------------------------------------------------------------------


def _spot_check_report(
    master: pl.DataFrame,
    utility: str,
    state: str,
    util_batch: str,
    run_delivery: int,
    run_supply: int,
    path_load_curves_local: str,
) -> None:
    """Print a trace for a few buildings for human review."""
    _log("\n=== Spot-check report ===")
    util = master.filter(pl.col("sb.electric_utility") == utility)
    annual = util.filter(pl.col("month") == ANNUAL_MONTH)

    candidates: list[tuple[str, pl.DataFrame]] = []

    gas_heated = annual.filter(pl.col("heats_with_natgas") == True)  # noqa: E712
    if not gas_heated.is_empty():
        candidates.append(("gas-heated", gas_heated.head(1)))

    oil_heated = annual.filter(pl.col("heats_with_oil") == True)  # noqa: E712
    if not oil_heated.is_empty():
        candidates.append(("oil-heated", oil_heated.head(1)))

    propane_heated = annual.filter(pl.col("heats_with_propane") == True)  # noqa: E712
    if not propane_heated.is_empty():
        candidates.append(("propane-heated", propane_heated.head(1)))

    if not candidates:
        candidates.append(("random", annual.head(1)))

    s3_base = (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{util_batch}"
    )
    dir_delivery = _find_run_dir(s3_base, run_delivery)
    dir_supply = _find_run_dir(s3_base, run_supply)
    monthly_fc = _read_fixed_charge(state, utility)

    for label, row_df in candidates:
        bldg = cast(int, row_df[BLDG_ID].item())
        _log(f"\n  --- Building {bldg} ({label}) ---")

        master_row = annual.filter(pl.col(BLDG_ID) == bldg).to_dicts()[0]

        # Source values
        elec_delivery_src = cast(
            pl.DataFrame,
            scan(f"{dir_delivery}/{ELEC_BILLS_CSV}")
            .filter((pl.col(BLDG_ID) == bldg) & (pl.col("month") == ANNUAL_MONTH))
            .collect(),
        )
        elec_supply_src = cast(
            pl.DataFrame,
            scan(f"{dir_supply}/{ELEC_BILLS_CSV}")
            .filter((pl.col(BLDG_ID) == bldg) & (pl.col("month") == ANNUAL_MONTH))
            .collect(),
        )
        gas_src = cast(
            pl.DataFrame,
            scan(f"{dir_supply}/{GAS_BILLS_CSV}")
            .filter((pl.col(BLDG_ID) == bldg) & (pl.col("month") == ANNUAL_MONTH))
            .collect(),
        )

        d_bill = cast(float, elec_delivery_src[BILL_LEVEL].item())
        s_bill = cast(float, elec_supply_src[BILL_LEVEL].item())
        g_bill = cast(float, gas_src[BILL_LEVEL].item())

        _log(f"  Source: elec_delivery bill_level = ${d_bill:,.2f}")
        _log(f"  Source: elec_supply bill_level   = ${s_bill:,.2f}")
        _log(f"  Source: gas bill_level            = ${g_bill:,.2f}")
        _log(
            f"  Tariff: fixedchargefirstmeter     = ${monthly_fc:,.2f}/mo = ${monthly_fc * 12:,.2f}/yr"
        )
        _log("  Decomposition:")
        _log(
            f"    elec_fixed_charge   = ${master_row['elec_fixed_charge']:,.2f} (expected ${monthly_fc * 12:,.2f})"
        )
        _log(
            f"    elec_delivery_bill  = ${master_row['elec_delivery_bill']:,.2f} (expected ${d_bill - monthly_fc * 12:,.2f} = delivery - fixed)"
        )
        _log(
            f"    elec_supply_bill    = ${master_row['elec_supply_bill']:,.2f} (expected ${s_bill - d_bill:,.2f} = supply - delivery)"
        )
        _log(
            f"    elec_total_bill     = ${master_row['elec_total_bill']:,.2f} (expected ${s_bill:,.2f})"
        )
        _log(
            f"    gas_total_bill      = ${master_row['gas_total_bill']:,.2f} (expected ${g_bill:,.2f})"
        )
        _log(f"    oil_total_bill      = ${master_row['oil_total_bill']:,.2f}")
        _log(f"    propane_total_bill  = ${master_row['propane_total_bill']:,.2f}")
        _log(f"    energy_total_bill   = ${master_row['energy_total_bill']:,.2f}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standalone validation of master bills: round-trips, sanity, spot-checks.",
    )
    parser.add_argument("--state", required=True)
    parser.add_argument("--utility", required=True)
    parser.add_argument(
        "--batch",
        required=True,
        help="Batch name (e.g. ny_20260305c_r1-8).",
    )
    parser.add_argument(
        "--batch-override",
        action="append",
        default=None,
        help="Per-utility batch override in UTILITY=BATCH format. Repeatable.",
    )
    parser.add_argument("--run-delivery", type=int, required=True)
    parser.add_argument("--run-supply", type=int, required=True)
    parser.add_argument(
        "--path-resstock-release",
        default="s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
    )
    parser.add_argument("--path-load-curves-local", required=True)
    parser.add_argument(
        "--path-heating-fuel-prices", default="s3://data.sb/eia/heating_fuel_prices/"
    )
    parser.add_argument("--price-year", type=int, default=2024)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    state = args.state.lower()
    batch_overrides = _parse_batch_overrides(args.batch_override)
    batch_suffix = _build_output_path_suffix(args.batch, batch_overrides)
    util_batch = batch_overrides.get(args.utility, args.batch)

    s3_all_utils = f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/all_utilities/{batch_suffix}"
    master_path = f"{s3_all_utils}/run_{args.run_delivery}+{args.run_supply}/comb_bills_year_target/"

    _log(
        f"Standalone validation for {args.utility} (state={state}, batch={util_batch})"
    )
    _log(f"  Master: {master_path}")

    _log("\nLoading master table...")
    master = cast(
        pl.DataFrame,
        pl.scan_parquet(master_path, hive_partitioning=True).collect(),
    )
    _log(f"  {master.height} rows, {master[BLDG_ID].n_unique()} buildings")

    all_results: list[tuple[str, bool, str]] = []

    # 1. Source round-trips
    _log("\n=== Source round-trip checks ===")
    rt_results = _check_source_round_trips(
        master,
        args.utility,
        state,
        util_batch,
        args.run_delivery,
        args.run_supply,
    )
    all_results.extend(rt_results)

    # 2. Aggregate sanity
    _log("\n=== Aggregate sanity checks ===")
    sanity_results = _check_aggregate_sanity(master, args.utility)
    all_results.extend(sanity_results)

    # 3. Spot-check report
    _spot_check_report(
        master,
        args.utility,
        state,
        util_batch,
        args.run_delivery,
        args.run_supply,
        args.path_load_curves_local,
    )

    # Summary
    _log("\n=== Summary ===")
    n_pass = sum(1 for _, ok, _ in all_results if ok)
    n_fail = sum(1 for _, ok, _ in all_results if not ok)
    for name, ok, detail in all_results:
        status = "PASS" if ok else "FAIL"
        _log(f"  {status}: {name} -- {detail}")

    _log(f"\n{n_pass} passed, {n_fail} failed")
    if n_fail > 0:
        sys.exit(1)
    else:
        _log(f"All checks passed for {args.utility}")


if __name__ == "__main__":
    main()

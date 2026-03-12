"""Validate master bills table by comparing against the trusted old code path.

For a single utility, runs both:
  1. The OLD per-utility code path (importing functions from the existing plotting scripts)
  2. The NEW master-table code path (reading the pre-built master CSV)

Then asserts the results match for:
  - Electric bill decomposition (stacked bar components)
  - Total energy bill delta (histogram before/after)
"""

from __future__ import annotations

import argparse
import sys
from typing import cast

import polars as pl

from utils.post.delivered_fuel_bills import (
    add_delivered_fuel_bills,
    load_monthly_fuel_prices,
)
from utils.post.io import (
    ANNUAL_MONTH,
    BLDG_ID,
    BILL_LEVEL,
    scan,
    scan_load_curves_for_utility,
)
from utils.post.build_master_bills import (
    VALID_RUN_PAIRS,
    _build_output_path_suffix,
    _find_run_dir,
    _parse_batch_overrides,
)
from utils.post.plot_bill_change_histogram import _apply_population_filter
from utils.post.plot_bill_components_stacked_bar import (
    _join_bills_and_compute_components,
    _read_fixed_charge_from_tariff,
)

FLOAT_TOL = 1e-4
ELEC_BILLS_CSV = "bills/elec_bills_year_target.csv"
COMB_BILLS_CSV = "bills/comb_bills_year_target.csv"

META_COLS = [
    BLDG_ID,
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
]


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _compare_dfs(
    old: pl.DataFrame,
    new: pl.DataFrame,
    compare_cols: list[str],
    label: str,
) -> bool:
    """Join old and new on bldg_id, compare columns, return True if all match."""
    joined = old.join(new, on=BLDG_ID, how="inner", suffix="_new")
    if joined.height != old.height:
        _log(
            f"  FAIL [{label}]: Row count mismatch after join. "
            f"old={old.height}, new={new.height}, joined={joined.height}"
        )
        return False

    all_pass = True
    for col in compare_cols:
        diff = (joined[col] - joined[f"{col}_new"]).abs()
        max_diff = cast(float, diff.max()) if diff.len() > 0 else 0.0
        n_bad = (diff > FLOAT_TOL).sum()
        if n_bad > 0:
            _log(
                f"  FAIL [{label}]: Column '{col}' has {n_bad} rows exceeding tolerance, max_diff={max_diff:.8f}"
            )
            all_pass = False
        else:
            _log(f"  PASS [{label}]: Column '{col}' matches (max_diff={max_diff:.8f})")
    return all_pass


# ---------------------------------------------------------------------------
# Stacked bar comparison (electric bill decomposition)
# ---------------------------------------------------------------------------


def _validate_stacked_bar(
    utility: str,
    state: str,
    util_batch: str,
    master_parquet_path: str,
    path_resstock_release: str,
    run_delivery: int,
    run_supply: int,
) -> bool:
    """Compare electric decomposition: old path vs master table."""
    _log("\n=== Stacked bar comparison (electric decomposition) ===")

    master_df = cast(
        pl.DataFrame,
        pl.scan_parquet(master_parquet_path, hive_partitioning=True).collect(),
    )
    s3_base = (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{util_batch}"
    )
    _log(f"  Using batch {util_batch} for {utility}")
    dir_delivery = _find_run_dir(s3_base, run_delivery)
    dir_supply = _find_run_dir(s3_base, run_supply)

    repo_root = __import__("pathlib").Path(__file__).resolve().parents[2]
    tariff_path = str(
        repo_root
        / "rate_design"
        / "hp_rates"
        / state
        / "config"
        / "tariffs"
        / "electric"
        / f"{utility}_flat.json"
    )
    annual_fixed = _read_fixed_charge_from_tariff(tariff_path) * 12

    # --- Old path ---
    _log("  Old path: _join_bills_and_compute_components on _target files...")
    old_components = _join_bills_and_compute_components(
        scan(f"{dir_delivery}/{ELEC_BILLS_CSV}"),
        scan(f"{dir_supply}/{ELEC_BILLS_CSV}"),
        annual_fixed,
    )
    old_annual = old_components.filter(pl.col("month") == ANNUAL_MONTH).select(
        BLDG_ID,
        pl.col("supply"),
        pl.col("delivery_volumetric"),
        pl.col("delivery_fixed"),
        pl.col("total"),
    )
    _log(f"  Old path: {old_annual.height} annual rows")

    # --- New path ---
    _log("  New path: reading master table, filtering to utility...")
    new_annual = master_df.filter(
        (pl.col("sb.electric_utility") == utility) & (pl.col("month") == ANNUAL_MONTH)
    ).select(
        BLDG_ID,
        pl.col("elec_supply_bill").alias("supply"),
        pl.col("elec_delivery_bill").alias("delivery_volumetric"),
        pl.col("elec_fixed_charge").alias("delivery_fixed"),
        pl.col("elec_total_bill").alias("total"),
    )
    _log(f"  New path: {new_annual.height} annual rows")

    return _compare_dfs(
        old_annual,
        new_annual,
        ["supply", "delivery_volumetric", "delivery_fixed", "total"],
        "stacked_bar",
    )


# ---------------------------------------------------------------------------
# Histogram comparison (total energy bill delta)
# ---------------------------------------------------------------------------


def _validate_histogram(
    utility: str,
    state: str,
    util_batch: str,
    master_before_path: str,
    master_after_path: str,
    run_supply_before: int,
    run_supply_after: int,
    path_resstock_release: str,
    path_load_curves_local: str,
    monthly_prices: pl.DataFrame,
    metadata: pl.DataFrame,
    filter_name: str,
) -> bool:
    """Compare energy bill delta: old path vs master tables."""
    _log(f"\n=== Histogram comparison (energy bill delta, filter={filter_name}) ===")

    s3_base = (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{util_batch}"
    )
    _log(f"  Using batch {util_batch} for {utility}")
    dir_supply_before = _find_run_dir(s3_base, run_supply_before)
    dir_supply_after = _find_run_dir(s3_base, run_supply_after)

    # --- Old path ---
    _log("  Old path: add_delivered_fuel_bills on comb_bills_year_target...")
    comb_before = scan(f"{dir_supply_before}/{COMB_BILLS_CSV}")
    comb_after = scan(f"{dir_supply_after}/{COMB_BILLS_CSV}")

    load_monthly_u00 = scan_load_curves_for_utility(
        path_load_curves_local, state.upper(), "00", utility, "monthly"
    )
    load_monthly_u02 = scan_load_curves_for_utility(
        path_load_curves_local, state.upper(), "02", utility, "monthly"
    )

    topped_before = add_delivered_fuel_bills(
        comb_before, load_monthly_u00, monthly_prices
    )
    topped_after = add_delivered_fuel_bills(
        comb_after, load_monthly_u02, monthly_prices
    )

    annual_before = cast(
        pl.DataFrame,
        topped_before.filter(pl.col("month") == ANNUAL_MONTH)
        .select(BLDG_ID, "weight", pl.col(BILL_LEVEL).alias("bill_before"))
        .collect(),
    )
    annual_after = cast(
        pl.DataFrame,
        topped_after.filter(pl.col("month") == ANNUAL_MONTH)
        .select(BLDG_ID, pl.col(BILL_LEVEL).alias("bill_after"))
        .collect(),
    )

    old_delta = annual_before.join(annual_after, on=BLDG_ID, how="inner").with_columns(
        (pl.col("bill_after") - pl.col("bill_before")).alias("delta")
    )

    meta_cols = metadata.select([pl.col(c) for c in META_COLS])
    old_with_meta = old_delta.join(meta_cols, on=BLDG_ID, how="inner")
    old_filtered = _apply_population_filter(old_with_meta, filter_name)
    old_result = old_filtered.select(BLDG_ID, "delta", "weight").sort(BLDG_ID)
    _log(f"  Old path: {old_result.height} buildings after filter '{filter_name}'")

    # --- New path ---
    _log("  New path: reading master tables, computing delta...")
    m12 = cast(
        pl.DataFrame,
        pl.scan_parquet(master_before_path, hive_partitioning=True).collect(),
    )
    m34 = cast(
        pl.DataFrame,
        pl.scan_parquet(master_after_path, hive_partitioning=True).collect(),
    )

    new_before = m12.filter(
        (pl.col("sb.electric_utility") == utility) & (pl.col("month") == ANNUAL_MONTH)
    ).select(BLDG_ID, "weight", pl.col("energy_total_bill").alias("bill_before"))
    new_after = m34.filter(
        (pl.col("sb.electric_utility") == utility) & (pl.col("month") == ANNUAL_MONTH)
    ).select(BLDG_ID, pl.col("energy_total_bill").alias("bill_after"))

    new_delta = new_before.join(new_after, on=BLDG_ID, how="inner").with_columns(
        (pl.col("bill_after") - pl.col("bill_before")).alias("delta")
    )
    new_with_meta = new_delta.join(meta_cols, on=BLDG_ID, how="inner")
    new_filtered = _apply_population_filter(new_with_meta, filter_name)
    new_result = new_filtered.select(BLDG_ID, "delta", "weight").sort(BLDG_ID)
    _log(f"  New path: {new_result.height} buildings after filter '{filter_name}'")

    return _compare_dfs(old_result, new_result, ["delta", "weight"], "histogram")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate master bills by comparing against old per-utility code path.",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. ny)")
    parser.add_argument(
        "--utility", required=True, help="Utility to validate (e.g. coned)"
    )
    parser.add_argument(
        "--batch",
        required=True,
        help="Batch name (e.g. ny_20260311b_r1-12).",
    )
    parser.add_argument(
        "--batch-override",
        action="append",
        default=None,
        help="Per-utility batch override in UTILITY=BATCH format. Repeatable.",
    )
    parser.add_argument(
        "--path-resstock-release",
        default="s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
    )
    parser.add_argument("--path-load-curves-local", required=True)
    parser.add_argument(
        "--path-heating-fuel-prices", default="s3://data.sb/eia/heating_fuel_prices/"
    )
    parser.add_argument("--price-year", type=int, default=2024)
    parser.add_argument(
        "--filter", default="non_hp", help="Population filter for histogram comparison."
    )
    parser.add_argument(
        "--run-delivery-before",
        type=int,
        default=1,
        help="Baseline delivery run for before-period master table (default: 1).",
    )
    parser.add_argument(
        "--run-supply-before",
        type=int,
        default=2,
        help="Baseline supply run for before-period master table (default: 2).",
    )
    parser.add_argument(
        "--run-delivery-after",
        type=int,
        default=3,
        help="After-period delivery run for after master table (default: 3).",
    )
    parser.add_argument(
        "--run-supply-after",
        type=int,
        default=4,
        help="After-period supply run for after master table (default: 4).",
    )
    return parser.parse_args()


def _validate_run_pair(run_delivery: int, run_supply: int, label: str) -> None:
    """Require a valid delivery+supply pair (1+2, 3+4, ..., 11+12)."""
    if (run_delivery, run_supply) not in VALID_RUN_PAIRS:
        expected = ", ".join(f"{d}+{s}" for d, s in sorted(VALID_RUN_PAIRS))
        raise ValueError(
            f"Invalid {label} run pair {run_delivery}+{run_supply}. Expected one of: {expected}"
        )


def main() -> None:
    args = _parse_args()
    state = args.state.lower()
    _validate_run_pair(
        args.run_delivery_before,
        args.run_supply_before,
        "before",
    )
    _validate_run_pair(
        args.run_delivery_after,
        args.run_supply_after,
        "after",
    )
    batch_overrides = _parse_batch_overrides(args.batch_override)
    batch_suffix = _build_output_path_suffix(args.batch, batch_overrides)
    util_batch = batch_overrides.get(args.utility, args.batch)

    s3_all_utils = f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/all_utilities/{batch_suffix}"
    before_pair = f"{args.run_delivery_before}+{args.run_supply_before}"
    after_pair = f"{args.run_delivery_after}+{args.run_supply_after}"
    master_before_path = f"{s3_all_utils}/run_{before_pair}/comb_bills_year_target/"
    master_after_path = f"{s3_all_utils}/run_{after_pair}/comb_bills_year_target/"

    _log(
        f"Validating master bills for {args.utility} (state={state}, batch={util_batch})"
    )
    _log(f"  Master before ({before_pair}): {master_before_path}")
    _log(f"  Master after ({after_pair}): {master_after_path}")

    monthly_prices = load_monthly_fuel_prices(
        args.path_heating_fuel_prices, state.upper(), args.price_year
    )
    meta_path = f"{args.path_resstock_release.rstrip('/')}/metadata_utility/state={state.upper()}/utility_assignment.parquet"
    metadata = cast(
        pl.DataFrame,
        pl.scan_parquet(meta_path).collect(),
    )

    results: list[tuple[str, bool]] = []

    # Stacked bar for before run pair
    ok = _validate_stacked_bar(
        args.utility,
        state,
        util_batch,
        master_before_path,
        args.path_resstock_release,
        args.run_delivery_before,
        args.run_supply_before,
    )
    results.append((f"stacked_bar_{before_pair}", ok))

    # Stacked bar for after run pair
    ok = _validate_stacked_bar(
        args.utility,
        state,
        util_batch,
        master_after_path,
        args.path_resstock_release,
        args.run_delivery_after,
        args.run_supply_after,
    )
    results.append((f"stacked_bar_{after_pair}", ok))

    # Histogram
    ok = _validate_histogram(
        args.utility,
        state,
        util_batch,
        master_before_path,
        master_after_path,
        args.run_supply_before,
        args.run_supply_after,
        args.path_resstock_release,
        args.path_load_curves_local,
        monthly_prices,
        metadata,
        args.filter,
    )
    results.append(("histogram", ok))

    _log("\n=== Summary ===")
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        _log(f"  {status}: {name}")
        if not passed:
            all_pass = False

    if all_pass:
        _log(f"\nAll checks passed for {args.utility}")
    else:
        _log(f"\nSome checks FAILED for {args.utility}")
        sys.exit(1)


if __name__ == "__main__":
    main()

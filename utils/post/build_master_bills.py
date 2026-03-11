"""Build a master bills table across all utilities for a state and run pair.

Reads CAIRO bill outputs, ResStock metadata, tariff fixed charges, and EIA fuel
prices to produce a Hive-partitioned parquet dataset (partitioned by
sb.electric_utility) with fully decomposed energy bills: electric (fixed +
volumetric delivery + supply), gas, oil, and propane.

Output schema (13 rows per building: Jan..Dec + Annual):
    bldg_id, sb.electric_utility, sb.gas_utility, upgrade, postprocess_group.has_hp,
    postprocess_group.heating_type, heats_with_electricity, heats_with_natgas,
    heats_with_oil, heats_with_propane, month, weight,
    elec_fixed_charge, elec_delivery_bill, elec_supply_bill, elec_total_bill,
    gas_fixed_charge, gas_volumetric_bill, gas_total_bill,
    propane_total_bill, oil_total_bill, energy_total_bill

Identities:
    elec_total_bill = elec_fixed_charge + elec_delivery_bill + elec_supply_bill
    gas_total_bill = gas_fixed_charge + gas_volumetric_bill
    energy_total_bill = elec_total_bill + gas_total_bill + propane_total_bill + oil_total_bill
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import cast

import polars as pl

from utils.post.delivered_fuel_bills import compute_fuel_bills, load_monthly_fuel_prices
from utils.post.gas_bills import (
    build_fixed_charge_table,
    build_rate_table,
    compute_gas_bills,
    load_gas_tariff_map,
    load_gas_tariffs,
)
from utils.post.io import (
    ANNUAL_MONTH,
    BLDG_ID,
    BILL_LEVEL,
    scan,
    scan_load_curves_for_utility,
)

ELEC_BILLS_CSV = "bills/elec_bills_year_target.csv"

META_COLS = [
    BLDG_ID,
    "sb.electric_utility",
    "sb.gas_utility",
    "upgrade",
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "heats_with_electricity",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
]

OUTPUT_COLS = [
    BLDG_ID,
    "sb.electric_utility",
    "sb.gas_utility",
    "upgrade",
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "heats_with_electricity",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
    "month",
    "weight",
    "elec_fixed_charge",
    "elec_delivery_bill",
    "elec_supply_bill",
    "elec_total_bill",
    "gas_fixed_charge",
    "gas_volumetric_bill",
    "gas_total_bill",
    "propane_total_bill",
    "oil_total_bill",
    "energy_total_bill",
]

FLOAT_TOL = 1e-4

# ---------------------------------------------------------------------------
# Logging
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


def _read_utilities(state: str) -> list[str]:
    """Read UTILITIES from state.env."""
    repo_root = Path(__file__).resolve().parents[2]
    env_file = repo_root / "rate_design" / "hp_rates" / state / "state.env"
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("UTILITIES="):
            return line.split("=", 1)[1].split(",")
    raise ValueError(f"UTILITIES not found in {env_file}")


def _read_fixed_charge(state: str, utility: str) -> float:
    """Read fixedchargefirstmeter ($/month) from the flat tariff JSON."""
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
    items = data.get("items") or []
    if not items:
        raise ValueError(f"No items in tariff JSON: {tariff_path}")
    fc = items[0].get("fixedchargefirstmeter")
    if fc is None:
        raise ValueError(f"No fixedchargefirstmeter in {tariff_path}")
    return float(fc)


def _s3_ls_prefixes(s3_path: str) -> list[str]:
    """List S3 PRE (directory) entries under a path."""
    result = subprocess.run(
        ["aws", "s3", "ls", s3_path.rstrip("/") + "/"],
        capture_output=True,
        text=True,
        check=True,
    )
    prefixes = []
    for line in result.stdout.strip().splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and parts[0] == "PRE":
            prefixes.append(parts[1].rstrip("/"))
    return prefixes


def _find_run_dir(s3_base: str, run_num: int) -> str:
    """Find the subdirectory matching a run number under an execution-time prefix."""
    prefixes = _s3_ls_prefixes(s3_base)
    marker = f"_run{run_num}_"
    for dirname in prefixes:
        if marker in dirname:
            return f"{s3_base.rstrip('/')}/{dirname}"
    raise FileNotFoundError(
        f"No run directory matching '{marker}' under {s3_base}. Available: {prefixes}"
    )


def _parse_batch_overrides(raw: list[str] | None) -> dict[str, str]:
    """Parse --batch-override arguments like 'cenhud=ny_20260306_r1-8' into a dict."""
    if not raw:
        return {}
    overrides: dict[str, str] = {}
    for item in raw:
        if "=" not in item:
            raise ValueError(
                f"Invalid --batch-override format '{item}'; expected UTILITY=BATCH"
            )
        utility, batch = item.split("=", 1)
        overrides[utility] = batch
    return overrides


def _build_output_path_suffix(batch: str, overrides: dict[str, str]) -> str:
    """Build the composite batch component of the output S3 path.

    No overrides: 'ny_20260305c_r1-8'
    With overrides: 'ny_20260305c_r1-8-cenhud=ny_20260306_r1-8'
    """
    if not overrides:
        return batch
    sorted_parts = sorted(f"{u}={b}" for u, b in overrides.items())
    return f"{batch}-{'-'.join(sorted_parts)}"


def _upgrade_for_run(run_delivery: int) -> str:
    """Infer the upgrade id from the delivery run number."""
    if run_delivery in (1, 2, 5, 6):
        return "00"
    if run_delivery in (3, 4, 7, 8):
        return "02"
    raise ValueError(f"Cannot infer upgrade for run {run_delivery}; expected 1-8")


def _assert_building_match(
    name_a: str,
    ids_a: set[int],
    name_b: str,
    ids_b: set[int],
    utility: str,
) -> None:
    if ids_a != ids_b:
        only_a = ids_a - ids_b
        only_b = ids_b - ids_a
        examples_a = sorted(only_a)[:10]
        examples_b = sorted(only_b)[:10]
        raise AssertionError(
            f"[{utility}] Building mismatch between {name_a} ({len(ids_a)}) and "
            f"{name_b} ({len(ids_b)}). "
            f"Only in {name_a}: {examples_a}{'...' if len(only_a) > 10 else ''}. "
            f"Only in {name_b}: {examples_b}{'...' if len(only_b) > 10 else ''}."
        )


def _assert_rows_per_building(
    df: pl.DataFrame, expected: int, label: str, utility: str
) -> None:
    counts = df.group_by(BLDG_ID).agg(pl.len().alias("n"))
    bad = counts.filter(pl.col("n") != expected)
    if not bad.is_empty():
        examples = bad.head(5).to_dicts()
        raise AssertionError(
            f"[{utility}] {label}: expected {expected} rows per building, "
            f"but found exceptions: {examples}"
        )


def _assert_identity(
    df: pl.DataFrame, lhs: str, rhs_cols: list[str], tol: float, utility: str
) -> None:
    rhs_sum = pl.sum_horizontal(*[pl.col(c) for c in rhs_cols])
    diff = (pl.col(lhs) - rhs_sum).abs()
    violations = df.filter(diff > tol)
    if not violations.is_empty():
        n = violations.height
        max_diff = cast(float, violations.select(diff.alias("d"))["d"].max())
        example = violations.head(3).select(BLDG_ID, "month", lhs, *rhs_cols).to_dicts()
        raise AssertionError(
            f"[{utility}] Identity violation: {lhs} != sum({rhs_cols}). "
            f"{n} rows exceed tolerance {tol}, max diff={max_diff:.6f}. "
            f"Examples: {example}"
        )


def _assert_no_nulls(df: pl.DataFrame, cols: list[str], utility: str) -> None:
    for c in cols:
        n_null = df[c].null_count()
        if n_null > 0:
            raise AssertionError(f"[{utility}] Column '{c}' has {n_null} null values.")


# ---------------------------------------------------------------------------
# Per-utility processing
# ---------------------------------------------------------------------------


def _process_utility(
    utility: str,
    state: str,
    s3_base: str,
    run_delivery: int,
    run_supply: int,
    metadata_for_utility: pl.DataFrame,
    monthly_prices: pl.DataFrame,
    path_load_curves_local: str,
    upgrade: str,
    gas_rate_table: pl.DataFrame,
    gas_fixed_charges: pl.DataFrame,
) -> pl.DataFrame:
    """Build the master table fragment for a single utility."""
    meta_bldg_ids = set(metadata_for_utility[BLDG_ID].to_list())
    n_bldgs = len(meta_bldg_ids)

    # --- Find run directories ---
    t = _log(f"  Finding run directories (runs {run_delivery}, {run_supply})...")
    dir_delivery = _find_run_dir(s3_base, run_delivery)
    dir_supply = _find_run_dir(s3_base, run_supply)
    _log_done(
        "  Finding run directories",
        t,
        f"delivery={dir_delivery.split('/')[-1]}, supply={dir_supply.split('/')[-1]}",
    )

    # --- Electric bills ---
    t = _log("  Reading elec_bills_year_target.csv (delivery)...")
    elec_delivery_df = cast(
        pl.DataFrame, scan(f"{dir_delivery}/{ELEC_BILLS_CSV}").collect()
    )
    _log_done("  Reading elec delivery", t, f"{elec_delivery_df.height} rows")

    t = _log("  Reading elec_bills_year_target.csv (supply)...")
    elec_supply_df = cast(
        pl.DataFrame, scan(f"{dir_supply}/{ELEC_BILLS_CSV}").collect()
    )
    _log_done("  Reading elec supply", t, f"{elec_supply_df.height} rows")

    elec_d_ids = set(elec_delivery_df[BLDG_ID].unique().to_list())
    elec_s_ids = set(elec_supply_df[BLDG_ID].unique().to_list())
    _assert_building_match(
        "elec_delivery", elec_d_ids, "elec_supply", elec_s_ids, utility
    )
    _assert_building_match("elec_bills", elec_d_ids, "metadata", meta_bldg_ids, utility)
    _assert_rows_per_building(elec_delivery_df, 13, "elec_delivery", utility)
    _assert_rows_per_building(elec_supply_df, 13, "elec_supply", utility)

    # Validate weights match
    weight_check = elec_delivery_df.select(
        BLDG_ID, "month", pl.col("weight").alias("w_d")
    ).join(
        elec_supply_df.select(BLDG_ID, "month", pl.col("weight").alias("w_s")),
        on=[BLDG_ID, "month"],
        how="inner",
    )
    weight_diff = (weight_check["w_d"] - weight_check["w_s"]).abs()
    n_weight_diff = (weight_diff > 1e-9).sum()
    if n_weight_diff > 0:
        raise AssertionError(
            f"[{utility}] Weights differ between delivery and supply elec bills: "
            f"{n_weight_diff} rows, max diff={weight_diff.max()}"
        )

    # --- Electric decomposition ---
    t = _log("  Computing electric bill decomposition...")
    monthly_fixed = _read_fixed_charge(state, utility)
    annual_fixed = monthly_fixed * 12

    elec = (
        elec_delivery_df.select(
            BLDG_ID,
            "month",
            "weight",
            pl.col(BILL_LEVEL).alias("bill_delivery"),
        )
        .join(
            elec_supply_df.select(
                BLDG_ID,
                "month",
                pl.col(BILL_LEVEL).alias("bill_supply"),
            ),
            on=[BLDG_ID, "month"],
            how="inner",
        )
        .with_columns(
            pl.when(pl.col("month") == ANNUAL_MONTH)
            .then(annual_fixed)
            .otherwise(monthly_fixed)
            .alias("elec_fixed_charge"),
        )
        .with_columns(
            (pl.col("bill_delivery") - pl.col("elec_fixed_charge")).alias(
                "elec_delivery_bill"
            ),
            (pl.col("bill_supply") - pl.col("bill_delivery")).alias("elec_supply_bill"),
            pl.col("bill_supply").alias("elec_total_bill"),
        )
        .select(
            BLDG_ID,
            "month",
            "weight",
            "elec_fixed_charge",
            "elec_delivery_bill",
            "elec_supply_bill",
            "elec_total_bill",
        )
    )
    _log_done("  Electric decomposition", t, f"{elec.height} rows")

    # --- Gas bills (computed post-hoc from tariff JSONs + ResStock consumption) ---
    t = _log("  Computing gas bills from tariff JSONs...")
    gas_tariff_map = load_gas_tariff_map(state, utility, upgrade)
    gas_map_ids = set(gas_tariff_map[BLDG_ID].to_list())
    _assert_building_match(
        "gas_tariff_map", gas_map_ids, "metadata", meta_bldg_ids, utility
    )

    # --- Load curves (shared by gas + oil/propane) ---
    t = _log(f"  Reading load_curve_monthly (local, {n_bldgs} buildings)...")
    load_curves = scan_load_curves_for_utility(
        path_load_curves_local, state.upper(), upgrade, utility, "monthly"
    )
    _log_done("  Reading load curves", t)

    gas = cast(
        pl.DataFrame,
        compute_gas_bills(
            load_curves, gas_tariff_map, gas_rate_table, gas_fixed_charges
        ).collect(),
    )
    _log_done("  Gas bills", t, f"{gas.height} rows")

    gas_ids = set(gas[BLDG_ID].unique().to_list())
    _assert_building_match("gas_bills", gas_ids, "elec_bills", elec_d_ids, utility)
    _assert_rows_per_building(gas, 13, "gas_bills", utility)

    n_gas = gas.filter(
        (pl.col("month") != ANNUAL_MONTH) & (pl.col("gas_total_bill") > 0)
    )[BLDG_ID].n_unique()
    _log(f"  Buildings with nonzero gas: {n_gas}")

    # --- Oil and propane bills ---
    t = _log("  Computing oil and propane bills...")
    fuel_bills = cast(
        pl.DataFrame, compute_fuel_bills(load_curves, monthly_prices).collect()
    )
    _log_done("  Oil/propane bills", t, f"{fuel_bills.height} rows")

    fuel_ids = set(fuel_bills[BLDG_ID].unique().to_list())
    _assert_building_match("load_curves", fuel_ids, "metadata", meta_bldg_ids, utility)

    n_oil = fuel_bills.filter(
        (pl.col("month") != ANNUAL_MONTH) & (pl.col("oil_total_bill") > 0)
    )[BLDG_ID].n_unique()
    n_propane = fuel_bills.filter(
        (pl.col("month") != ANNUAL_MONTH) & (pl.col("propane_total_bill") > 0)
    )[BLDG_ID].n_unique()
    _log(f"  Buildings with nonzero oil: {n_oil}, propane: {n_propane}")

    _assert_no_nulls(fuel_bills, ["oil_total_bill", "propane_total_bill"], utility)

    # --- Join all components ---
    t = _log("  Joining components...")
    joined = (
        elec.join(gas, on=[BLDG_ID, "month"], how="inner")
        .join(fuel_bills, on=[BLDG_ID, "month"], how="inner")
        .join(
            metadata_for_utility.select(META_COLS),
            on=BLDG_ID,
            how="inner",
        )
        .with_columns(
            (
                pl.col("elec_total_bill")
                + pl.col("gas_total_bill")
                + pl.col("propane_total_bill")
                + pl.col("oil_total_bill")
            ).alias("energy_total_bill"),
            pl.lit(int(upgrade)).alias("upgrade"),
        )
        .select(OUTPUT_COLS)
    )
    _log_done("  Joining components", t, f"{joined.height} rows")

    expected_rows = n_bldgs * 13
    if joined.height != expected_rows:
        raise AssertionError(
            f"[{utility}] Expected {expected_rows} rows ({n_bldgs} bldgs * 13), "
            f"got {joined.height}"
        )

    _assert_identity(
        joined,
        "elec_total_bill",
        ["elec_fixed_charge", "elec_delivery_bill", "elec_supply_bill"],
        FLOAT_TOL,
        utility,
    )
    _assert_identity(
        joined,
        "gas_total_bill",
        ["gas_fixed_charge", "gas_volumetric_bill"],
        FLOAT_TOL,
        utility,
    )
    bill_cols = [
        "weight",
        "elec_fixed_charge",
        "elec_delivery_bill",
        "elec_supply_bill",
        "elec_total_bill",
        "gas_fixed_charge",
        "gas_volumetric_bill",
        "gas_total_bill",
        "propane_total_bill",
        "oil_total_bill",
        "energy_total_bill",
    ]
    _assert_no_nulls(joined, bill_cols, utility)

    return joined


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build master bills table across all utilities for a state and run pair.",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. ny)")
    parser.add_argument(
        "--batch",
        required=True,
        help="Batch name (e.g. ny_20260305c_r1-8). Used as the default batch "
        "for all utilities and as the base of the output path.",
    )
    parser.add_argument(
        "--batch-override",
        action="append",
        default=None,
        help="Per-utility batch override in UTILITY=BATCH format. "
        "Repeatable (e.g. --batch-override cenhud=ny_20260306_r1-8).",
    )
    parser.add_argument(
        "--run-delivery",
        type=int,
        required=True,
        help="Delivery run number (e.g. 1 or 3)",
    )
    parser.add_argument(
        "--run-supply", type=int, required=True, help="Supply run number (e.g. 2 or 4)"
    )
    parser.add_argument(
        "--path-resstock-release",
        default="s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
        help="S3 path to ResStock release (for metadata).",
    )
    parser.add_argument(
        "--path-load-curves-local",
        required=True,
        help="Local path to ResStock release (for load_curve_monthly).",
    )
    parser.add_argument(
        "--path-heating-fuel-prices",
        default="s3://data.sb/eia/heating_fuel_prices/",
        help="S3 path to Hive-partitioned EIA heating fuel prices.",
    )
    parser.add_argument(
        "--price-year", type=int, default=2024, help="Year for EIA fuel prices."
    )
    parser.add_argument(
        "--utilities",
        default=None,
        help="Comma-separated list of utilities to process (default: all from state.env).",
    )
    return parser.parse_args()


def main() -> None:
    global _t0
    _t0 = time.monotonic()
    args = _parse_args()

    state = args.state.lower()
    state_upper = state.upper()
    upgrade = _upgrade_for_run(args.run_delivery)
    utilities = args.utilities.split(",") if args.utilities else _read_utilities(state)
    batch_overrides = _parse_batch_overrides(args.batch_override)
    s3_output_base = "s3://data.sb/switchbox/cairo/outputs/hp_rates"

    _log(
        f"Building master bills: state={state_upper}, runs {args.run_delivery}+{args.run_supply}, "
        f"upgrade={upgrade}, batch={args.batch}, utilities={utilities}"
    )
    if batch_overrides:
        _log(f"  Batch overrides: {batch_overrides}")

    # --- Load shared data ---
    t = _log(f"Loading gas tariffs for {state}...")
    gas_tariffs = load_gas_tariffs(state)
    gas_rate_table = build_rate_table(gas_tariffs)
    gas_fixed_charges = build_fixed_charge_table(gas_tariffs)
    _log_done(
        "Loading gas tariffs",
        t,
        f"{len(gas_tariffs)} tariffs, {gas_rate_table.height} rate rows",
    )

    t = _log(
        f"Loading EIA fuel prices (state={state_upper}, year={args.price_year})..."
    )
    monthly_prices = load_monthly_fuel_prices(
        args.path_heating_fuel_prices, state_upper, args.price_year
    )
    _log_done("Loading EIA fuel prices", t)

    t = _log("Loading metadata from utility_assignment.parquet...")
    meta_path = f"{args.path_resstock_release.rstrip('/')}/metadata_utility/state={state_upper}/utility_assignment.parquet"
    metadata = cast(
        pl.DataFrame,
        pl.scan_parquet(meta_path).collect(),
    )
    n_bldgs_total = metadata[BLDG_ID].n_unique()
    _log_done("Loading metadata", t, f"{n_bldgs_total} buildings")

    bldgs_per_utility: dict[str, int] = {}
    for u in utilities:
        n = metadata.filter(pl.col("sb.electric_utility") == u)[BLDG_ID].n_unique()
        bldgs_per_utility[u] = n
        _log(f"  {u}: {n} buildings")

    # --- Process each utility ---
    all_dfs: list[pl.DataFrame] = []
    for i, utility in enumerate(utilities, 1):
        util_batch = batch_overrides.get(utility, args.batch)
        _log(f"Processing utility {i}/{len(utilities)}: {utility} (batch={util_batch})")
        s3_base = f"{s3_output_base}/{state}/{utility}/{util_batch}"
        meta_for_util = metadata.filter(pl.col("sb.electric_utility") == utility)

        df = _process_utility(
            utility=utility,
            state=state,
            s3_base=s3_base,
            run_delivery=args.run_delivery,
            run_supply=args.run_supply,
            metadata_for_utility=meta_for_util,
            monthly_prices=monthly_prices,
            path_load_curves_local=args.path_load_curves_local,
            upgrade=upgrade,
            gas_rate_table=gas_rate_table,
            gas_fixed_charges=gas_fixed_charges,
        )
        all_dfs.append(df)

    # --- Concatenate ---
    t = _log("Concatenating all utilities...")
    master = pl.concat(all_dfs)
    _log_done(
        "Concatenating",
        t,
        f"{master.height} rows, {master[BLDG_ID].n_unique()} buildings",
    )

    # --- Final validation ---
    t = _log("Validating final table...")
    final_bldg_count = master[BLDG_ID].n_unique()
    expected_bldgs = sum(bldgs_per_utility[u] for u in utilities)
    if final_bldg_count != expected_bldgs:
        raise AssertionError(
            f"Final building count {final_bldg_count} != expected {expected_bldgs} "
            f"(across {len(utilities)} utilities)"
        )

    per_util_check = master.group_by("sb.electric_utility").agg(
        pl.col(BLDG_ID).n_unique().alias("n_bldgs")
    )
    for row in per_util_check.iter_rows(named=True):
        u = row["sb.electric_utility"]
        actual = row["n_bldgs"]
        expected = bldgs_per_utility.get(u, -1)
        if actual != expected:
            raise AssertionError(
                f"Utility {u}: expected {expected} buildings, got {actual}"
            )

    _assert_identity(
        master,
        "energy_total_bill",
        ["elec_total_bill", "gas_total_bill", "propane_total_bill", "oil_total_bill"],
        FLOAT_TOL,
        "ALL",
    )
    _log_done("Validation", t)

    # --- Write output (Hive-partitioned parquet) ---
    batch_suffix = _build_output_path_suffix(args.batch, batch_overrides)
    output_s3 = (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/all_utilities/"
        f"{batch_suffix}/run_{args.run_delivery}+{args.run_supply}/"
        f"comb_bills_year_target/"
    )
    t = _log(f"Writing to {output_s3}...")
    tmp_dir = Path(tempfile.mkdtemp(prefix="master_bills_"))
    try:
        partition_col = "sb.electric_utility"
        for util_name, util_df in master.group_by(partition_col):
            part_dir = tmp_dir / f"{partition_col}={util_name[0]}"
            part_dir.mkdir(parents=True, exist_ok=True)
            util_df.drop(partition_col).write_parquet(part_dir / "data.parquet")
        subprocess.run(
            ["aws", "s3", "sync", str(tmp_dir), output_s3],
            check=True,
            capture_output=True,
        )
    finally:
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)
    _log_done("Writing", t)

    total_elapsed = time.monotonic() - _t0
    mm, ss = divmod(int(total_elapsed), 60)
    _log(f"Done (total: {mm}m {ss}s)")


if __name__ == "__main__":
    main()

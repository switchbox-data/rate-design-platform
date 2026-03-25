"""Build a master BAT (Bill Alignment Test) table across all utilities for a state and run pair.

Reads CAIRO cross-subsidization outputs from paired delivery/supply runs and
ResStock metadata to produce a Hive-partitioned parquet dataset (partitioned by
sb.electric_utility) with BAT metrics decomposed into delivery, supply, and
total components.

For each building, a delivery run (e.g. run 1) produces BAT values reflecting
delivery-only bill alignment, while the paired supply run (e.g. run 2) produces
BAT values reflecting delivery+supply (total) bill alignment.  The supply-only
component is the column-wise delta: supply = total - delivery.

In addition to the BAT metrics, the table includes the three cost-allocation
components that CAIRO computes before deriving BAT:

- **annual_bill** — the customer's annual electric bill (the ``Annual`` column
  in CAIRO's output).
- **economic_burden** — the customer's marginal-cost allocation
  (``customer_level_economic_burden``): Σ(hourly_load × hourly_MC).
- **residual_share** — the customer's per-customer residual-cost allocation
  (``customer_level_residual_share_percustomer``).

All three are decomposed into delivery / supply / total using the same identity
as the BAT metrics.

Output schema (1 row per building):
    bldg_id, sb.electric_utility, sb.gas_utility, upgrade,
    postprocess_group.has_hp, postprocess_group.heating_type,
    heats_with_electricity, heats_with_natgas, heats_with_oil,
    heats_with_propane, weight,
    BAT_vol_delivery, BAT_vol_supply, BAT_vol_total,
    BAT_peak_delivery, BAT_peak_supply, BAT_peak_total,
    BAT_percustomer_delivery, BAT_percustomer_supply, BAT_percustomer_total,
    annual_bill_delivery, annual_bill_supply, annual_bill_total,
    economic_burden_delivery, economic_burden_supply, economic_burden_total,
    residual_share_delivery, residual_share_supply, residual_share_total

Identities (per metric m in {vol, peak, percustomer}):
    BAT_m_total = BAT_m_delivery + BAT_m_supply

Identities (per component c in {annual_bill, economic_burden, residual_share}):
    c_total = c_delivery + c_supply
    annual_bill_total ≈ economic_burden_total + residual_share_total + BAT_percustomer_total
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import cast

import polars as pl

from utils.post.io import BLDG_ID, scan

BAT_CSV = "cross_subsidization/cross_subsidization_BAT_values.csv"
UPGRADE_00_RUNS = {1, 2, 5, 6, 9, 10, 17, 18}
UPGRADE_02_RUNS = {3, 4, 7, 8, 11, 12, 19, 20}
VALID_RUN_PAIRS = {(r, r + 1) for r in (1, 3, 5, 7, 9, 11, 17, 19)}

BAT_METRICS = ["BAT_vol", "BAT_peak", "BAT_percustomer"]

# CAIRO source columns → short output names for the cost-allocation components.
# These follow the same delivery/supply/total decomposition as BAT_METRICS.
COST_COMPONENTS_SRC = {
    "Annual": "annual_bill",
    "customer_level_economic_burden": "economic_burden",
    "customer_level_residual_share_percustomer": "residual_share",
}
COST_COMPONENTS = list(COST_COMPONENTS_SRC.values())

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
    "weight",
    "BAT_vol_delivery",
    "BAT_vol_supply",
    "BAT_vol_total",
    "BAT_peak_delivery",
    "BAT_peak_supply",
    "BAT_peak_total",
    "BAT_percustomer_delivery",
    "BAT_percustomer_supply",
    "BAT_percustomer_total",
    "annual_bill_delivery",
    "annual_bill_supply",
    "annual_bill_total",
    "economic_burden_delivery",
    "economic_burden_supply",
    "economic_burden_total",
    "residual_share_delivery",
    "residual_share_supply",
    "residual_share_total",
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
    """Parse --batch-override arguments like 'cenhud=ny_20260312_r1-12' into a dict."""
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

    No overrides: 'ny_20260311b_r1-12'
    With overrides: 'ny_20260311b_r1-12-cenhud=ny_20260312_r1-12'
    """
    if not overrides:
        return batch
    sorted_parts = sorted(f"{u}={b}" for u, b in overrides.items())
    return f"{batch}-{'-'.join(sorted_parts)}"


def _upgrade_for_run(run_delivery: int) -> str:
    """Infer the upgrade id from the delivery run number."""
    if run_delivery in UPGRADE_00_RUNS:
        return "00"
    if run_delivery in UPGRADE_02_RUNS:
        return "02"
    raise ValueError(
        f"Cannot infer upgrade for run {run_delivery}; not in UPGRADE_00_RUNS or UPGRADE_02_RUNS"
    )


def _validate_run_pair(run_delivery: int, run_supply: int) -> None:
    """Require a valid delivery+supply pair (1+2, 3+4, ..., 11+12)."""
    if (run_delivery, run_supply) not in VALID_RUN_PAIRS:
        expected = ", ".join(f"{d}+{s}" for d, s in sorted(VALID_RUN_PAIRS))
        raise ValueError(
            f"Invalid run pair {run_delivery}+{run_supply}. Expected one of: {expected}"
        )


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


def _assert_bat_identity(
    df: pl.DataFrame, metric: str, tol: float, utility: str
) -> None:
    """Assert BAT_m_total == BAT_m_delivery + BAT_m_supply."""
    total_col = f"{metric}_total"
    delivery_col = f"{metric}_delivery"
    supply_col = f"{metric}_supply"
    diff = (pl.col(total_col) - pl.col(delivery_col) - pl.col(supply_col)).abs()
    violations = df.filter(diff > tol)
    if not violations.is_empty():
        n = violations.height
        max_diff = cast(float, violations.select(diff.alias("d"))["d"].max())
        example = (
            violations.head(3)
            .select(BLDG_ID, total_col, delivery_col, supply_col)
            .to_dicts()
        )
        raise AssertionError(
            f"[{utility}] Identity violation: {total_col} != "
            f"{delivery_col} + {supply_col}. "
            f"{n} rows exceed tolerance {tol}, max diff={max_diff:.6f}. "
            f"Examples: {example}"
        )


def _assert_bill_decomposition(df: pl.DataFrame, tol: float, utility: str) -> None:
    """Assert annual_bill_total ≈ economic_burden_total + residual_share_total + BAT_percustomer_total."""
    diff = (
        pl.col("annual_bill_total")
        - pl.col("economic_burden_total")
        - pl.col("residual_share_total")
        - pl.col("BAT_percustomer_total")
    ).abs()
    violations = df.filter(diff > tol)
    if not violations.is_empty():
        n = violations.height
        max_diff = cast(float, violations.select(diff.alias("d"))["d"].max())
        example = (
            violations.head(3)
            .select(
                BLDG_ID,
                "annual_bill_total",
                "economic_burden_total",
                "residual_share_total",
                "BAT_percustomer_total",
            )
            .to_dicts()
        )
        raise AssertionError(
            f"[{utility}] Bill decomposition violation: "
            f"annual_bill_total != economic_burden_total + residual_share_total "
            f"+ BAT_percustomer_total. "
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
    s3_base: str,
    run_delivery: int,
    run_supply: int,
    metadata_for_utility: pl.DataFrame,
    upgrade: str,
) -> pl.DataFrame:
    """Build the master BAT table fragment for a single utility."""
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

    # --- Read BAT CSVs ---
    t = _log("  Reading BAT values (delivery run)...")
    bat_delivery_df = cast(pl.DataFrame, scan(f"{dir_delivery}/{BAT_CSV}").collect())
    _log_done("  Reading BAT delivery", t, f"{bat_delivery_df.height} rows")

    t = _log("  Reading BAT values (supply run)...")
    bat_supply_df = cast(pl.DataFrame, scan(f"{dir_supply}/{BAT_CSV}").collect())
    _log_done("  Reading BAT supply", t, f"{bat_supply_df.height} rows")

    # --- Validate building IDs ---
    delivery_ids = set(bat_delivery_df[BLDG_ID].unique().to_list())
    supply_ids = set(bat_supply_df[BLDG_ID].unique().to_list())
    _assert_building_match(
        "bat_delivery", delivery_ids, "bat_supply", supply_ids, utility
    )
    _assert_building_match(
        "bat_delivery", delivery_ids, "metadata", meta_bldg_ids, utility
    )
    _assert_rows_per_building(bat_delivery_df, 1, "bat_delivery", utility)
    _assert_rows_per_building(bat_supply_df, 1, "bat_supply", utility)

    # --- Validate weights match between runs ---
    weight_check = bat_delivery_df.select(BLDG_ID, pl.col("weight").alias("w_d")).join(
        bat_supply_df.select(BLDG_ID, pl.col("weight").alias("w_s")),
        on=BLDG_ID,
        how="inner",
    )
    weight_diff = (weight_check["w_d"] - weight_check["w_s"]).abs()
    n_weight_diff = (weight_diff > 1e-9).sum()
    if n_weight_diff > 0:
        raise AssertionError(
            f"[{utility}] Weights differ between delivery and supply BAT CSVs: "
            f"{n_weight_diff} rows, max diff={weight_diff.max()}"
        )

    # --- Validate no nulls in source BAT columns ---
    _assert_no_nulls(bat_delivery_df, BAT_METRICS, utility)
    _assert_no_nulls(bat_supply_df, BAT_METRICS, utility)

    # --- Validate no nulls in cost-component source columns ---
    _assert_no_nulls(bat_delivery_df, list(COST_COMPONENTS_SRC.keys()), utility)
    _assert_no_nulls(bat_supply_df, list(COST_COMPONENTS_SRC.keys()), utility)

    # --- Join delivery and supply, compute decomposition ---
    t = _log("  Computing BAT decomposition (delivery / supply / total)...")
    delivery_select = (
        [BLDG_ID, "weight"]
        + [pl.col(m).alias(f"{m}_delivery") for m in BAT_METRICS]
        + [
            pl.col(src).alias(f"{short}_delivery")
            for src, short in COST_COMPONENTS_SRC.items()
        ]
    )
    supply_select = (
        [BLDG_ID]
        + [pl.col(m).alias(f"{m}_total") for m in BAT_METRICS]
        + [
            pl.col(src).alias(f"{short}_total")
            for src, short in COST_COMPONENTS_SRC.items()
        ]
    )
    bat = (
        bat_delivery_df.select(delivery_select)
        .join(
            bat_supply_df.select(supply_select),
            on=BLDG_ID,
            how="inner",
        )
        .with_columns(
            [
                (pl.col(f"{m}_total") - pl.col(f"{m}_delivery")).alias(f"{m}_supply")
                for m in BAT_METRICS
            ]
            + [
                (pl.col(f"{c}_total") - pl.col(f"{c}_delivery")).alias(f"{c}_supply")
                for c in COST_COMPONENTS
            ]
        )
    )
    _log_done("  BAT decomposition", t, f"{bat.height} rows")

    # --- Join with metadata ---
    t = _log("  Joining with metadata...")
    joined = (
        bat.join(
            metadata_for_utility.select(META_COLS),
            on=BLDG_ID,
            how="inner",
        )
        .with_columns(pl.lit(int(upgrade)).alias("upgrade"))
        .select(OUTPUT_COLS)
    )
    _log_done("  Joining with metadata", t, f"{joined.height} rows")

    if joined.height != n_bldgs:
        raise AssertionError(
            f"[{utility}] Expected {n_bldgs} rows (1 per building), got {joined.height}"
        )

    # --- Validate identities and nulls ---
    for m in BAT_METRICS:
        _assert_bat_identity(joined, m, FLOAT_TOL, utility)
    for c in COST_COMPONENTS:
        _assert_bat_identity(joined, c, FLOAT_TOL, utility)
    _assert_bill_decomposition(joined, FLOAT_TOL, utility)

    bat_output_cols = [
        f"{m}_{component}"
        for m in BAT_METRICS
        for component in ("delivery", "supply", "total")
    ]
    cost_output_cols = [
        f"{c}_{component}"
        for c in COST_COMPONENTS
        for component in ("delivery", "supply", "total")
    ]
    _assert_no_nulls(joined, ["weight"] + bat_output_cols + cost_output_cols, utility)

    return joined


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build master BAT table across all utilities for a state and run pair.",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. ny)")
    parser.add_argument(
        "--batch",
        required=True,
        help="Batch name (e.g. ny_20260311b_r1-12). Used as the default batch "
        "for all utilities and as the base of the output path.",
    )
    parser.add_argument(
        "--output-batch",
        default=None,
        help="Override the output batch name used in the output S3 path. "
        "If omitted, defaults to --batch (with any --batch-override suffixes).",
    )
    parser.add_argument(
        "--batch-override",
        action="append",
        default=None,
        help="Per-utility batch override in UTILITY=BATCH format. "
        "Repeatable (e.g. --batch-override cenhud=ny_20260312_r1-12).",
    )
    parser.add_argument(
        "--run-delivery",
        type=int,
        required=True,
        help="Delivery run number (valid pairs: 1+2, 3+4, ..., 11+12).",
    )
    parser.add_argument(
        "--run-supply",
        type=int,
        required=True,
        help="Supply run number paired to --run-delivery.",
    )
    parser.add_argument(
        "--path-resstock-release",
        default="s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
        help="S3 path to ResStock release (for metadata).",
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
    _validate_run_pair(args.run_delivery, args.run_supply)
    upgrade = _upgrade_for_run(args.run_delivery)
    utilities = args.utilities.split(",") if args.utilities else _read_utilities(state)
    batch_overrides = _parse_batch_overrides(args.batch_override)
    s3_output_base = "s3://data.sb/switchbox/cairo/outputs/hp_rates"

    _log(
        f"Building master BAT: state={state_upper}, runs {args.run_delivery}+{args.run_supply}, "
        f"upgrade={upgrade}, batch={args.batch}, utilities={utilities}"
    )
    if batch_overrides:
        _log(f"  Batch overrides: {batch_overrides}")

    # --- Load metadata ---
    t = _log("Loading metadata from utility_assignment.parquet...")
    meta_path = (
        f"{args.path_resstock_release.rstrip('/')}"
        f"/metadata_utility/state={state_upper}/utility_assignment.parquet"
    )
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
            s3_base=s3_base,
            run_delivery=args.run_delivery,
            run_supply=args.run_supply,
            metadata_for_utility=meta_for_util,
            upgrade=upgrade,
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

    for m in BAT_METRICS:
        _assert_bat_identity(master, m, FLOAT_TOL, "ALL")
    for c in COST_COMPONENTS:
        _assert_bat_identity(master, c, FLOAT_TOL, "ALL")
    _assert_bill_decomposition(master, FLOAT_TOL, "ALL")
    _log_done("Validation", t)

    # --- Write output (Hive-partitioned parquet) ---
    output_batch = args.output_batch or _build_output_path_suffix(
        args.batch, batch_overrides
    )
    output_s3 = (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state}/all_utilities/"
        f"{output_batch}/run_{args.run_delivery}+{args.run_supply}/"
        f"cross_subsidization_BAT_values/"
    )
    t = _log(f"Writing to {output_s3}...")
    tmp_dir = Path(tempfile.mkdtemp(prefix="master_bat_"))
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

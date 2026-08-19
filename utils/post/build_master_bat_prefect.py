"""Build master BAT (Bill Alignment Test) tables for a Prefect pipeline batch.

Prefect-native counterpart of ``build_master_bat.py``.  Runs are discovered from
each utility's pipeline YAML plus the batch's ``.runs/`` index instead of
numeric run numbers, and one invocation covers every ``(scenario, stage)`` in
the batch — each becoming its own master table.

Reads CAIRO cross-subsidization outputs from each segment's paired
delivery/supply runs and ResStock metadata to produce a Hive-partitioned parquet
dataset (partitioned by sb.electric_utility) with BAT metrics decomposed into
delivery, supply, and total components.

Within a segment, the delivery run produces BAT values reflecting delivery-only
bill alignment while the paired supply run produces delivery+supply (total) bill
alignment.  The supply-only component is the column-wise delta:
supply = total - delivery.

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

Outputs, one pair per ``{scenario}_{stage}`` segment:
    per-utility:   {output_base}/{state}/{utility}/{batch}/{segment}/cross_subsidization_BAT_values/
    all utilities: {output_base}/{state}/all_utilities/{batch}/{segment}/cross_subsidization_BAT_values/

Output schema (1 row per building):
    bldg_id, sb.electric_utility, sb.gas_utility, upgrade,
    postprocess_group.has_hp, postprocess_group.heating_type,
    postprocess_group.heating_type_v2,
    heats_with_electricity, heats_with_natgas, heats_with_oil,
    heats_with_propane, weight,
    BAT_{m}_{delivery,supply,total} for each BAT metric present in CAIRO output,
    {component}_{delivery,supply,total} for each cost component present,
    baseline_elec_fixed_charge, baseline_elec_delivery_bill, baseline_elec_supply_bill

Known BAT metrics: BAT_vol, BAT_peak, BAT_percustomer, BAT_epmc.
Known cost components: annual_bill, economic_burden, residual_share, residual_share_epmc.
Metrics not present in the CAIRO CSV are logged and omitted from the output.

The ``baseline_elec_*`` columns are the annual electric bill components of the
segment named by the pipeline YAML's ``bill_change_baseline``, read from the
master **bills** tables.  Build those first (``build_master_bills_prefect.py``).

Identities (per metric m in whichever metrics are present):
    BAT_m_total = BAT_m_delivery + BAT_m_supply

Identities (per component c):
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

from rate_design.hp_rates.pipeline_config import PipelineConfig, load_pipeline_config
from utils.file_io import get_aws_storage_options
from utils.post.baseline_bills import (
    BASELINE_COLS,
    baseline_ref_root,
    load_baseline_reference_annual,
)
from utils.post.io import BLDG_ID, scan
from utils.post.master_metadata import UTILITY_COLS, heating_type_v2, load_metadata
from utils.post.pipeline_runs import (
    RunPair,
    baseline_segment,
    batch_dir,
    build_order,
    expected_segments,
    find_run_pairs,
    pipeline_yaml_path,
    s3_uri,
    upgrade_for_stage,
)

BAT_CSV = "cross_subsidization/cross_subsidization_BAT_values.csv"

BAT_METRICS_KNOWN = ["BAT_vol", "BAT_peak", "BAT_percustomer", "BAT_epmc"]

# CAIRO source columns → short output names for the cost-allocation components.
# These follow the same delivery/supply/total decomposition as BAT metrics.
# At runtime, we filter to whichever columns are actually present in the CSV.
COST_COMPONENTS_SRC_KNOWN = {
    "Annual": "annual_bill",
    "customer_level_economic_burden": "economic_burden",
    "customer_level_residual_share_percustomer": "residual_share",
    "customer_level_residual_share_epmc": "residual_share_epmc",
}

META_COLS = [
    BLDG_ID,
    *UTILITY_COLS,
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "heats_with_electricity",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
]

META_OUTPUT_COLS = [
    BLDG_ID,
    *UTILITY_COLS,
    "upgrade",
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "postprocess_group.heating_type_v2",
    "heats_with_electricity",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
    "weight",
]

FLOAT_TOL = 1e-4


def _build_output_cols(bat_metrics: list[str], cost_components: list[str]) -> list[str]:
    """Build the output column list from detected metrics and cost components."""
    cols = list(META_OUTPUT_COLS)
    for m in bat_metrics:
        cols.extend(f"{m}_{c}" for c in ("delivery", "supply", "total"))
    for c in cost_components:
        cols.extend(f"{c}_{s}" for s in ("delivery", "supply", "total"))
    return cols


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
# Batch resolution
# ---------------------------------------------------------------------------


def _load_configs(state: str, utilities: list[str]) -> dict[str, PipelineConfig]:
    """Load one pipeline YAML per utility, resolved by naming convention."""
    configs: dict[str, PipelineConfig] = {}
    for utility in utilities:
        yaml_path = pipeline_yaml_path(state, utility)
        config = load_pipeline_config(yaml_path)
        if config.utility != utility or config.state != state:
            raise ValueError(
                f"{yaml_path} declares state={config.state}/utility={config.utility}, "
                f"expected {state}/{utility}"
            )
        configs[utility] = config
    return configs


def _resolve_baseline(
    state: str, configs: dict[str, PipelineConfig]
) -> tuple[str, str]:
    """The baseline segment and its upgrade, which every utility must agree on."""
    baselines: dict[str, tuple[str, str]] = {}
    for utility, config in configs.items():
        segment = baseline_segment(config, pipeline_yaml_path(state, utility))
        assert config.bill_change_baseline is not None  # baseline_segment checked it
        upgrade = upgrade_for_stage(config, config.bill_change_baseline.stage)
        baselines[utility] = (segment, upgrade)

    if len(set(baselines.values())) > 1:
        raise ValueError(
            f"Utilities in this batch declare different bill_change_baseline "
            f"segments or upgrades ({baselines}); one master table cannot mix "
            f"baselines."
        )
    return next(iter(baselines.values()))


def _resolve_run_pairs(
    configs: dict[str, PipelineConfig],
    batch: str,
    *,
    scenarios: list[str] | None,
) -> dict[str, dict[str, RunPair]]:
    """Per utility, the run pair for each segment that completed in the batch."""
    pairs: dict[str, dict[str, RunPair]] = {}
    for utility, config in configs.items():
        found = find_run_pairs(config, batch, scenarios=scenarios)
        skipped = [
            segment
            for segment in expected_segments(config, scenarios=scenarios)
            if segment not in found
        ]
        _log(f"  {utility}: {len(found)} segment(s) found: {list(found)}")
        if skipped:
            _log(f"  {utility}: skipping {len(skipped)} segment(s) not run: {skipped}")
        if not found:
            raise FileNotFoundError(
                f"No completed runs for {utility} in batch {batch}: no index "
                f"files under {batch_dir(config, batch)}/.runs/"
            )
        pairs[utility] = found
    return pairs


def _ordered_segments(pairs: dict[str, dict[str, RunPair]], baseline: str) -> list[str]:
    """Every segment present for at least one utility, baseline first."""
    ordered: list[str] = []
    for per_utility in pairs.values():
        for segment in per_utility:
            if segment not in ordered:
                ordered.append(segment)
    return build_order(ordered, baseline)


# ---------------------------------------------------------------------------
# Per-utility processing
# ---------------------------------------------------------------------------


def _process_utility(
    utility: str,
    run: RunPair,
    metadata_for_utility: pl.DataFrame,
    *,
    state_lower: str,
    batch: str,
    baseline: str,
    baseline_upgrade: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """Build the master BAT table fragment for a single utility and segment."""
    meta_bldg_ids = set(metadata_for_utility[BLDG_ID].to_list())
    n_bldgs = len(meta_bldg_ids)

    _log(f"  delivery={run.dir_delivery.rstrip('/').split('/')[-1]}")
    _log(f"  supply={run.dir_supply.rstrip('/').split('/')[-1]}")

    # --- Read BAT CSVs ---
    t = _log("  Reading BAT values (delivery run)...")
    bat_delivery_df = scan(f"{run.dir_delivery}/{BAT_CSV}").collect()
    _log_done("  Reading BAT delivery", t, f"{bat_delivery_df.height} rows")

    t = _log("  Reading BAT values (supply run)...")
    bat_supply_df = scan(f"{run.dir_supply}/{BAT_CSV}").collect()
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

    # --- Detect available BAT metrics and cost components ---
    delivery_cols = set(bat_delivery_df.columns)
    bat_metrics = [m for m in BAT_METRICS_KNOWN if m in delivery_cols]
    missing_bat = set(BAT_METRICS_KNOWN) - set(bat_metrics)
    if missing_bat:
        _log(
            f"  [{utility}] BAT metrics not in CAIRO output (skipping): "
            f"{sorted(missing_bat)}"
        )

    cost_components_src = {
        k: v for k, v in COST_COMPONENTS_SRC_KNOWN.items() if k in delivery_cols
    }
    missing_cost = set(COST_COMPONENTS_SRC_KNOWN) - set(cost_components_src)
    if missing_cost:
        _log(
            f"  [{utility}] Cost components not in CAIRO output (skipping): "
            f"{sorted(missing_cost)}"
        )
    cost_components = list(cost_components_src.values())

    # --- Validate no nulls in source columns ---
    _assert_no_nulls(bat_delivery_df, bat_metrics, utility)
    _assert_no_nulls(bat_supply_df, bat_metrics, utility)
    _assert_no_nulls(bat_delivery_df, list(cost_components_src.keys()), utility)
    _assert_no_nulls(bat_supply_df, list(cost_components_src.keys()), utility)

    # --- Join delivery and supply, compute decomposition ---
    t = _log("  Computing BAT decomposition (delivery / supply / total)...")
    delivery_select = (
        [BLDG_ID, "weight"]
        + [pl.col(m).alias(f"{m}_delivery") for m in bat_metrics]
        + [
            pl.col(src).alias(f"{short}_delivery")
            for src, short in cost_components_src.items()
        ]
    )
    supply_select = (
        [BLDG_ID]
        + [pl.col(m).alias(f"{m}_total") for m in bat_metrics]
        + [
            pl.col(src).alias(f"{short}_total")
            for src, short in cost_components_src.items()
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
                for m in bat_metrics
            ]
            + [
                (pl.col(f"{c}_total") - pl.col(f"{c}_delivery")).alias(f"{c}_supply")
                for c in cost_components
            ]
        )
    )
    _log_done("  BAT decomposition", t, f"{bat.height} rows")

    # --- Join with metadata ---
    t = _log("  Joining with metadata...")
    output_cols = _build_output_cols(bat_metrics, cost_components)
    joined_core = (
        bat.join(
            metadata_for_utility.select(META_COLS),
            on=BLDG_ID,
            how="inner",
        )
        .with_columns(
            pl.lit(int(run.upgrade)).alias("upgrade"),
            heating_type_v2(),
        )
        .select(output_cols)
    )
    ref = load_baseline_reference_annual(
        state_lower=state_lower,
        output_batch=batch,
        segment=baseline,
        utility=utility,
        expected_upgrade=baseline_upgrade,
        storage_options=storage_options,
    )
    joined = joined_core.join(ref, on=BLDG_ID, how="left")
    n_null = int(joined["baseline_elec_delivery_bill"].null_count())
    if n_null > 0:
        root = baseline_ref_root(
            state_lower=state_lower, output_batch=batch, segment=baseline
        )
        raise FileNotFoundError(
            f"[{utility}] Could not join baseline columns from {baseline!r} "
            f"({n_null} null rows). Build master comb bills for the baseline "
            f"segment first: {root}"
        )
    _log_done("  Joining with metadata", t, f"{joined.height} rows")

    if joined.height != n_bldgs:
        raise AssertionError(
            f"[{utility}] Expected {n_bldgs} rows (1 per building), got {joined.height}"
        )

    # --- Validate identities and nulls ---
    for m in bat_metrics:
        _assert_bat_identity(joined, m, FLOAT_TOL, utility)
    for c in cost_components:
        _assert_bat_identity(joined, c, FLOAT_TOL, utility)
    _assert_bill_decomposition(joined, FLOAT_TOL, utility)

    bat_output_cols = [
        f"{m}_{component}"
        for m in bat_metrics
        for component in ("delivery", "supply", "total")
    ]
    cost_output_cols = [
        f"{c}_{component}"
        for c in cost_components
        for component in ("delivery", "supply", "total")
    ]
    _assert_no_nulls(
        joined,
        ["weight"] + bat_output_cols + cost_output_cols + BASELINE_COLS,
        utility,
    )

    return joined


def _write_parquet_dir(df: pl.DataFrame, output_s3: str) -> None:
    """Write one parquet file to an S3 prefix."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="master_bat_"))
    try:
        df.write_parquet(tmp_dir / "data.parquet")
        subprocess.run(
            ["aws", "s3", "sync", str(tmp_dir), output_s3],
            check=True,
            capture_output=True,
        )
    finally:
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)


def _write_hive_partitioned(
    df: pl.DataFrame, output_s3: str, partition_col: str
) -> None:
    """Write a Hive-partitioned dataset (one parquet per partition value) to S3."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="master_bat_all_"))
    try:
        for value, part_df in df.group_by(partition_col):
            part_dir = tmp_dir / f"{partition_col}={value[0]}"
            part_dir.mkdir(parents=True, exist_ok=True)
            part_df.drop(partition_col).write_parquet(part_dir / "data.parquet")
        subprocess.run(
            ["aws", "s3", "sync", str(tmp_dir), output_s3],
            check=True,
            capture_output=True,
        )
    finally:
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build master BAT tables for every scenario/stage in a "
        "Prefect pipeline batch.",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. md)")
    parser.add_argument(
        "--batch",
        required=True,
        help="Batch name as passed to run_pipeline.py (e.g. md_20260803_a).",
    )
    parser.add_argument(
        "--utilities",
        default=None,
        help="Comma-separated utilities to process (default: all from state.env). "
        "Each needs a pipeline YAML at "
        "{state}/config/scenarios/pipeline_{utility}.yaml.",
    )
    parser.add_argument(
        "--scenarios",
        nargs="*",
        default=None,
        help="Optional scenario names to process (space-separated). Omit for all.",
    )
    return parser.parse_args()


def main() -> None:
    global _t0
    _t0 = time.monotonic()
    args = _parse_args()

    state = args.state.lower()
    state_upper = state.upper()
    utilities = args.utilities.split(",") if args.utilities else _read_utilities(state)
    storage_options = get_aws_storage_options()

    _log(
        f"Building master BAT: state={state_upper}, batch={args.batch}, "
        f"utilities={utilities}"
    )

    # --- Resolve the batch: configs, baseline, and completed run pairs ---
    configs = _load_configs(state, utilities)
    baseline, baseline_upgrade = _resolve_baseline(state, configs)
    run_pairs = _resolve_run_pairs(configs, args.batch, scenarios=args.scenarios)
    segments = _ordered_segments(run_pairs, baseline)
    _log(f"Baseline segment: {baseline} (upgrade {baseline_upgrade})")
    _log(f"Segments: {segments}")

    # --- Load metadata (one read per distinct ResStock release) ---
    metadata_by_base: dict[str, pl.DataFrame] = {}
    for utility, config in configs.items():
        base = config.run_defaults.resstock_base.rstrip("/")
        if base in metadata_by_base:
            continue
        t = _log(f"Loading metadata from {base}...")
        metadata_by_base[base] = load_metadata(base, state_upper)
        _log_done("Loading metadata", t, f"{metadata_by_base[base].height} rows")

    bldgs_per_utility: dict[str, int] = {}
    for utility, config in configs.items():
        metadata = metadata_by_base[config.run_defaults.resstock_base.rstrip("/")]
        n = metadata.filter(pl.col("sb.electric_utility") == utility)[
            BLDG_ID
        ].n_unique()
        bldgs_per_utility[utility] = n
        _log(f"  {utility}: {n} buildings")

    output_base_s3 = s3_uri(next(iter(configs.values())).output_base).rstrip("/")

    # --- One master table per segment ---
    for seg_i, segment in enumerate(segments, 1):
        seg_utilities = [u for u in utilities if segment in run_pairs.get(u, {})]
        _log(
            f"=== Segment {seg_i}/{len(segments)}: {segment} "
            f"({len(seg_utilities)} utility/ies: {seg_utilities}) ==="
        )

        all_dfs: list[pl.DataFrame] = []
        for i, utility in enumerate(seg_utilities, 1):
            config = configs[utility]
            base = config.run_defaults.resstock_base.rstrip("/")
            _log(f"Processing utility {i}/{len(seg_utilities)}: {utility}")

            df = _process_utility(
                utility=utility,
                run=run_pairs[utility][segment],
                metadata_for_utility=metadata_by_base[base].filter(
                    pl.col("sb.electric_utility") == utility
                ),
                state_lower=state,
                batch=args.batch,
                baseline=baseline,
                baseline_upgrade=baseline_upgrade,
                storage_options=storage_options,
            )

            per_util_output_s3 = (
                f"{output_base_s3}/{state}/{utility}/{args.batch}/{segment}/"
                f"cross_subsidization_BAT_values/"
            )
            t_util = _log(f"  Writing per-utility output to {per_util_output_s3}...")
            _write_parquet_dir(df, per_util_output_s3)
            _log_done(f"  Writing per-utility {utility}", t_util)

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
        expected_bldgs = sum(bldgs_per_utility[u] for u in seg_utilities)
        if final_bldg_count != expected_bldgs:
            raise AssertionError(
                f"Final building count {final_bldg_count} != expected "
                f"{expected_bldgs} (across {len(seg_utilities)} utilities)"
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

        master_cols = set(master.columns)
        final_bat_metrics = [
            m for m in BAT_METRICS_KNOWN if f"{m}_delivery" in master_cols
        ]
        final_cost_components = [
            v
            for v in COST_COMPONENTS_SRC_KNOWN.values()
            if f"{v}_delivery" in master_cols
        ]
        for m in final_bat_metrics:
            _assert_bat_identity(master, m, FLOAT_TOL, "ALL")
        for c in final_cost_components:
            _assert_bat_identity(master, c, FLOAT_TOL, "ALL")
        _assert_bill_decomposition(master, FLOAT_TOL, "ALL")
        _log_done("Validation", t)

        # --- Write output (Hive-partitioned parquet) ---
        output_s3 = (
            f"{output_base_s3}/{state}/all_utilities/{args.batch}/{segment}/"
            f"cross_subsidization_BAT_values/"
        )
        t = _log(f"Writing to {output_s3}...")
        _write_hive_partitioned(master, output_s3, "sb.electric_utility")
        _log_done("Writing", t)

    total_elapsed = time.monotonic() - _t0
    mm, ss = divmod(int(total_elapsed), 60)
    _log(f"Done: {len(segments)} segment(s) (total: {mm}m {ss}s)")


if __name__ == "__main__":
    main()

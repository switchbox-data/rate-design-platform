"""Build master bills tables for a Prefect pipeline batch.

Prefect-native counterpart of ``build_master_bills.py``.  Runs are discovered
from each utility's pipeline YAML plus the batch's ``.runs/`` index instead of
numeric run numbers, and one invocation covers every ``(scenario, stage)`` in
the batch — each becoming its own master table.

Reads CAIRO bill outputs, ResStock metadata, tariff fixed charges, and EIA fuel
prices to produce a Hive-partitioned parquet dataset (partitioned by
sb.electric_utility) with fully decomposed energy bills: electric (fixed +
volumetric delivery + supply), gas, oil, and propane.

Outputs, one pair per ``{scenario}_{stage}`` segment:
    per-utility:   {output_base}/{state}/{utility}/{batch}/{segment}/comb_bills_year_target/
    all utilities: {output_base}/{state}/all_utilities/{batch}/{segment}/comb_bills_year_target/

Output schema (13 rows per building: Jan..Dec + Annual):
    bldg_id, sb.electric_utility, sb.gas_utility, upgrade, postprocess_group.has_hp,
    postprocess_group.heating_type, postprocess_group.heating_type_v2,
    heats_with_electricity, heats_with_natgas,
    heats_with_oil, heats_with_propane, in.representative_income,
    in.hvac_cooling_partial_space_conditioning, month, weight,
    elec_fixed_charge, elec_delivery_bill, elec_supply_bill, elec_total_bill,
    baseline_elec_fixed_charge, baseline_elec_delivery_bill, baseline_elec_supply_bill,
    gas_fixed_charge, gas_volumetric_bill, gas_total_bill,
    propane_total_bill, oil_total_bill, energy_total_bill

The ``baseline_elec_*`` columns are the electric bill components of the segment
named by the pipeline YAML's ``bill_change_baseline``, repeated on every other
segment for the same ``(bldg_id, month)``.  On the baseline segment itself they
equal the row's own ``elec_*`` values.  The baseline segment is therefore built
first.

Identities:
    elec_total_bill = elec_fixed_charge + elec_delivery_bill + elec_supply_bill
    gas_total_bill = gas_fixed_charge + gas_volumetric_bill
    energy_total_bill = elec_total_bill + gas_total_bill + propane_total_bill + oil_total_bill

When ``--calculate-lmi`` is passed for MD, NY, or RI, the script also appends the LMI
discount columns before writing each final output.
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

from rate_design.hp_rates.pipeline_config import PipelineConfig, load_pipeline_config
from utils.file_io import get_aws_storage_options
from utils.post import apply_ny_lmi_to_master_bills as ny_lmi_master_bills
from utils.post.apply_md_ohep_to_master_bills import apply_md_ohep_to_master
from utils.post.apply_ny_lmi_to_master_bills import apply_ny_lmi_to_master
from utils.post.apply_ri_lmi_discounts_to_bills import apply_ri_lmi_to_master
from utils.post.baseline_bills import (
    BASELINE_COLS,
    baseline_columns_from_self,
    baseline_ref_root,
    load_baseline_reference_monthly,
)
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
    BILL_LEVEL,
    BLDG_ID,
    scan,
    scan_load_curves_for_utility,
)
from utils.post.master_metadata import (
    ATTR_COLS,
    METADATA_UPGRADE,
    UTILITY_COLS,
    heating_type_v2,
    load_metadata,
)
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

ELEC_BILLS_CSV = "bills/elec_bills_year_target.csv"

META_COLS = [BLDG_ID, *UTILITY_COLS, *ATTR_COLS]

OUTPUT_COLS = [
    BLDG_ID,
    "sb.electric_utility",
    "sb.gas_utility",
    "upgrade",
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "postprocess_group.heating_type_v2",
    "heats_with_electricity",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
    "in.representative_income",
    "in.hvac_cooling_partial_space_conditioning",
    "month",
    "weight",
    "elec_fixed_charge",
    "elec_delivery_bill",
    "elec_supply_bill",
    "elec_total_bill",
    *BASELINE_COLS,
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


def _s3_get_json(uri: str) -> dict:
    """Fetch and parse a JSON file from S3 via boto3."""
    import boto3

    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def _extract_fixed_charges_from_tariff_config(
    payload: dict,
) -> dict[str, float]:
    """Extract ``{tariff_key: monthly_fixed_charge}`` from ``tariff_final_config.json``.

    Handles both CAIRO format (top-level keys → dicts with
    ``ur_monthly_fixed_charge``) and URDB format (``items`` list with
    ``fixedchargefirstmeter``).
    """
    if "items" in payload:
        items = payload["items"]
        if items and isinstance(items[0], dict):
            label = items[0].get("label") or items[0].get("name") or "calibrated"
            fc = items[0].get("fixedchargefirstmeter")
            if fc is None:
                raise ValueError(
                    "URDB tariff_final_config has 'items' but no fixedchargefirstmeter"
                )
            return {str(label): float(fc)}
        raise ValueError("tariff_final_config has 'items' but it is empty")

    result: dict[str, float] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            fc = value.get("ur_monthly_fixed_charge")
            if fc is None:
                raise ValueError(
                    f"Tariff key '{key}' in tariff_final_config.json "
                    "has no ur_monthly_fixed_charge"
                )
            result[key] = float(fc)
    if not result:
        raise ValueError(
            "No tariff keys found in tariff_final_config.json "
            "(expected top-level keys with dict values or URDB 'items')"
        )
    return result


def _read_fixed_charges(dir_delivery: str, bldg_ids: set[int]) -> pl.DataFrame:
    """Read per-building monthly fixed charges from the delivery run directory.

    Returns a DataFrame with columns ``(bldg_id, elec_fixed_charge_monthly)``.

    1. Reads ``tariff_final_config.json`` from *dir_delivery*.
    2. If all tariff keys share the same fixed charge, broadcasts it.
    3. If they differ, reads the tariff map CSV named in the run's
       ``scenario_settings.json`` and joins to get per-building charges.
    """
    payload = _s3_get_json(f"{dir_delivery.rstrip('/')}/tariff_final_config.json")
    fc_by_key = _extract_fixed_charges_from_tariff_config(payload)

    unique_fcs = set(fc_by_key.values())
    if len(unique_fcs) == 1:
        fc_value = next(iter(unique_fcs))
        return pl.DataFrame(
            {BLDG_ID: sorted(bldg_ids), "elec_fixed_charge_monthly": fc_value}
        ).cast({BLDG_ID: pl.Int64, "elec_fixed_charge_monthly": pl.Float64})

    tariff_map_path = _resolve_tariff_map_path(dir_delivery)
    tariff_map = pl.read_csv(tariff_map_path, schema_overrides={BLDG_ID: pl.Int64})

    fc_lookup = pl.DataFrame(
        {
            "tariff_key": list(fc_by_key.keys()),
            "elec_fixed_charge_monthly": list(fc_by_key.values()),
        }
    )
    result = tariff_map.join(fc_lookup, on="tariff_key", how="inner").select(
        BLDG_ID, "elec_fixed_charge_monthly"
    )

    result_ids = set(result[BLDG_ID].to_list())
    missing = bldg_ids - result_ids
    if missing:
        examples = sorted(missing)[:10]
        raise ValueError(
            f"Tariff map does not cover {len(missing)} buildings. "
            f"Examples: {examples}. "
            f"Tariff keys in config: {sorted(fc_by_key.keys())}. "
            f"Tariff map: {tariff_map_path}"
        )
    return result


def _resolve_tariff_map_path(dir_delivery: str) -> str:
    """Read the electric tariff map CSV path from the run's ``scenario_settings.json``."""
    settings_uri = f"{dir_delivery.rstrip('/')}/scenario_settings.json"
    try:
        settings = _s3_get_json(settings_uri)
    except Exception as exc:
        raise FileNotFoundError(
            f"Multiple tariff keys with different fixed charges but no "
            f"scenario_settings.json at {settings_uri} to name the tariff map."
        ) from exc

    path = settings.get("path_tariff_maps_electric")
    if not path:
        raise ValueError(
            f"scenario_settings.json at {settings_uri} has no "
            "path_tariff_maps_electric field"
        )
    return str(path)


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


def _attach_baseline_columns(
    df: pl.DataFrame,
    *,
    state_lower: str,
    utility: str,
    batch: str,
    segment: str,
    baseline: str,
    baseline_upgrade: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """Attach the baseline segment's electric bill components to every row."""
    if segment == baseline:
        return df.with_columns(baseline_columns_from_self())

    ref = load_baseline_reference_monthly(
        state_lower=state_lower,
        output_batch=batch,
        segment=baseline,
        utility=utility,
        expected_upgrade=baseline_upgrade,
        storage_options=storage_options,
    )
    out = df.join(ref, on=[BLDG_ID, "month"], how="left")
    n_null = int(out["baseline_elec_delivery_bill"].null_count())
    if n_null > 0:
        root = baseline_ref_root(
            state_lower=state_lower, output_batch=batch, segment=baseline
        )
        raise FileNotFoundError(
            f"[{utility}] Could not join baseline columns from {baseline!r} "
            f"({n_null} null rows). Build master comb bills for the baseline "
            f"segment first: {root}"
        )
    return out


def _apply_lmi_discounts_to_master(
    master: pl.DataFrame,
    *,
    state_upper: str,
    utilities: list[str],
    upgrade: str,
    path_resstock_release: str,
    lmi_fpl_year: int,
    lmi_cpi_s3_path: str,
    lmi_participation_rates: list[float],
    lmi_participation_mode: str,
    lmi_seed: int,
    lmi_calculation_type: str,
) -> pl.DataFrame:
    """Dispatch LMI discount augmentation to the appropriate state module."""
    opts = get_aws_storage_options()

    if state_upper == "MD":
        if lmi_calculation_type != "monthly":
            _log(
                "  MD OHEP allocates annual grants proportionally across months; "
                f"ignoring LMI calculation type {lmi_calculation_type!r}"
            )
        return apply_md_ohep_to_master(
            master,
            state=state_upper,
            upgrade=upgrade,
            path_resstock_release=path_resstock_release,
            fpl_year=lmi_fpl_year,
            cpi_s3_path=lmi_cpi_s3_path,
            participation_rates=lmi_participation_rates,
            participation_mode=lmi_participation_mode,
            seed=lmi_seed,
            opts=opts,
        )

    if state_upper == "NY":
        # Sync elapsed-time logging so NY helper uses this script's start time.
        ny_lmi_master_bills._t0 = _t0
        return apply_ny_lmi_to_master(
            master,
            utilities=utilities,
            upgrade=upgrade,
            path_resstock_release=path_resstock_release,
            lmi_fpl_year=lmi_fpl_year,
            lmi_cpi_s3_path=lmi_cpi_s3_path,
            participation_rates=lmi_participation_rates,
            participation_mode=lmi_participation_mode,
            seed=lmi_seed,
            calculation_type=lmi_calculation_type,
            opts=opts,
        )

    if state_upper == "RI":
        if len(utilities) != 1:
            raise ValueError(
                f"RI LMI expects exactly one utility in the run; got {utilities}"
            )
        return apply_ri_lmi_to_master(
            master,
            utility=utilities[0],
            state_upper=state_upper,
            upgrade=upgrade,
            path_resstock_release=path_resstock_release,
            lmi_fpl_year=lmi_fpl_year,
            lmi_cpi_s3_path=lmi_cpi_s3_path,
            participation_rates=lmi_participation_rates,
            participation_mode=lmi_participation_mode,
            seed=lmi_seed,
            opts=opts,
        )

    raise ValueError(
        f"--calculate-lmi is not supported for state {state_upper!r}. "
        "Supported states: MD, NY, RI."
    )


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
    """The baseline segment and its upgrade, which every utility must agree on.

    The upgrade comes from the config rather than from the baseline run, so it
    is known even when the baseline segment is not part of this invocation.
    """
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
    state: str,
    run: RunPair,
    metadata_for_utility: pl.DataFrame,
    monthly_prices: pl.DataFrame,
    path_resstock_base: str,
    gas_rate_table: pl.DataFrame,
    gas_fixed_charges: pl.DataFrame,
    *,
    batch: str,
    baseline: str,
    baseline_upgrade: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """Build the master table fragment for a single utility and segment."""
    meta_bldg_ids = set(metadata_for_utility[BLDG_ID].to_list())
    n_bldgs = len(meta_bldg_ids)
    upgrade = run.upgrade

    _log(f"  delivery={run.dir_delivery.rstrip('/').split('/')[-1]}")
    _log(f"  supply={run.dir_supply.rstrip('/').split('/')[-1]}")

    # --- Electric bills ---
    t = _log("  Reading elec_bills_year_target.csv (delivery)...")
    elec_delivery_df = scan(f"{run.dir_delivery}/{ELEC_BILLS_CSV}").collect()
    _log_done("  Reading elec delivery", t, f"{elec_delivery_df.height} rows")

    t = _log("  Reading elec_bills_year_target.csv (supply)...")
    elec_supply_df = scan(f"{run.dir_supply}/{ELEC_BILLS_CSV}").collect()
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
    t = _log("  Reading fixed charges from tariff_final_config.json...")
    fixed_charges_df = _read_fixed_charges(run.dir_delivery, meta_bldg_ids)
    n_unique_fc = fixed_charges_df["elec_fixed_charge_monthly"].n_unique()
    _log_done("  Reading fixed charges", t, f"{n_unique_fc} distinct value(s)")

    t = _log("  Computing electric bill decomposition...")
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
        .join(fixed_charges_df, on=BLDG_ID, how="inner")
        .with_columns(
            pl.when(pl.col("month") == ANNUAL_MONTH)
            .then(pl.col("elec_fixed_charge_monthly") * 12)
            .otherwise(pl.col("elec_fixed_charge_monthly"))
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
        path_resstock_base, state.upper(), upgrade, utility, "monthly"
    )
    _log_done("  Reading load curves", t)

    gas = compute_gas_bills(
        load_curves, gas_tariff_map, gas_rate_table, gas_fixed_charges
    ).collect()
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
    fuel_bills = compute_fuel_bills(load_curves, monthly_prices).collect()
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
    joined_pre = (
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
        .with_columns(heating_type_v2())
    )
    joined = _attach_baseline_columns(
        joined_pre,
        state_lower=state,
        utility=utility,
        batch=batch,
        segment=run.segment,
        baseline=baseline,
        baseline_upgrade=baseline_upgrade,
        storage_options=storage_options,
    ).select(OUTPUT_COLS)
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
        *BASELINE_COLS,
        "gas_fixed_charge",
        "gas_volumetric_bill",
        "gas_total_bill",
        "propane_total_bill",
        "oil_total_bill",
        "energy_total_bill",
    ]
    _assert_no_nulls(joined, bill_cols, utility)

    return joined


def _write_parquet_dir(df: pl.DataFrame, output_s3: str) -> None:
    """Write one parquet file to an S3 prefix."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="master_bills_"))
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
    tmp_dir = Path(tempfile.mkdtemp(prefix="master_bills_all_"))
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
        description="Build master bills tables for every scenario/stage in a "
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
    parser.add_argument(
        "--path-heating-fuel-prices",
        default="s3://data.sb/eia/heating_fuel_prices/",
        help="S3 path to Hive-partitioned EIA heating fuel prices.",
    )
    parser.add_argument(
        "--price-year", type=int, default=2024, help="Year for EIA fuel prices."
    )
    parser.add_argument(
        "--calculate-lmi",
        action="store_true",
        help="If set, append LMI discount columns before writing master bills.",
    )
    parser.add_argument(
        "--lmi-fpl-year",
        type=int,
        default=2025,
        help="FPL/SMI guideline year for LMI discount calculation (used with --calculate-lmi).",
    )
    parser.add_argument(
        "--lmi-cpi-s3-path",
        default="s3://data.sb/fred/cpi/",
        help="S3 path to CPI parquet for LMI discount calculation (used with --calculate-lmi).",
    )
    parser.add_argument(
        "--lmi-participation-rates",
        type=float,
        nargs="+",
        default=[1.0],
        help="One or more participation fractions (0–1). Each rate produces a separate "
        "column set (e.g. --lmi-participation-rates 1.0 0.4 → columns for p100 and p40). "
        "Used with --calculate-lmi.",
    )
    parser.add_argument(
        "--lmi-participation-mode",
        choices=["uniform", "weighted"],
        default="weighted",
        help="Participation sampling mode for LMI discounts (used with --calculate-lmi).",
    )
    parser.add_argument(
        "--lmi-seed",
        type=int,
        default=42,
        help="RNG seed for LMI participation sampling (used with --calculate-lmi).",
    )
    parser.add_argument(
        "--lmi-calculation-type",
        choices=["monthly", "budget"],
        default="budget",
        help="LMI bill calculation method (used with --calculate-lmi).",
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
        f"Building master bills: state={state_upper}, batch={args.batch}, "
        f"utilities={utilities}"
    )
    if args.calculate_lmi:
        _log(
            "  LMI discounts enabled: "
            f"rates={args.lmi_participation_rates}, "
            f"mode={args.lmi_participation_mode}, "
            f"calculation={args.lmi_calculation_type}, "
            f"seed={args.lmi_seed}"
        )

    # --- Resolve the batch: configs, baseline, and completed run pairs ---
    configs = _load_configs(state, utilities)
    baseline, baseline_upgrade = _resolve_baseline(state, configs)
    run_pairs = _resolve_run_pairs(configs, args.batch, scenarios=args.scenarios)
    segments = _ordered_segments(run_pairs, baseline)
    _log(f"Baseline segment: {baseline} (upgrade {baseline_upgrade})")
    _log(f"Build order: {segments}")

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

    metadata_by_base: dict[str, pl.DataFrame] = {}
    for utility, config in configs.items():
        base = config.run_defaults.resstock_base.rstrip("/")
        if base in metadata_by_base:
            continue
        t = _log(f"Loading metadata from {base} (upgrade {METADATA_UPGRADE})...")
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

    # --- One master table per segment, baseline first ---
    for seg_i, segment in enumerate(segments, 1):
        seg_utilities = [u for u in utilities if segment in run_pairs.get(u, {})]
        _log(
            f"=== Segment {seg_i}/{len(segments)}: {segment} "
            f"({len(seg_utilities)} utility/ies: {seg_utilities}) ==="
        )

        upgrades = {run_pairs[u][segment].upgrade for u in seg_utilities}
        if len(upgrades) > 1:
            raise ValueError(
                f"Segment {segment} maps to multiple upgrades across utilities "
                f"({sorted(upgrades)}); pipeline YAMLs disagree on resstock upgrades."
            )
        upgrade = next(iter(upgrades))

        all_dfs: list[pl.DataFrame] = []
        for i, utility in enumerate(seg_utilities, 1):
            config = configs[utility]
            base = config.run_defaults.resstock_base.rstrip("/")
            _log(f"Processing utility {i}/{len(seg_utilities)}: {utility}")

            df = _process_utility(
                utility=utility,
                state=state,
                run=run_pairs[utility][segment],
                metadata_for_utility=metadata_by_base[base].filter(
                    pl.col("sb.electric_utility") == utility
                ),
                monthly_prices=monthly_prices,
                path_resstock_base=base,
                gas_rate_table=gas_rate_table,
                gas_fixed_charges=gas_fixed_charges,
                batch=args.batch,
                baseline=baseline,
                baseline_upgrade=baseline_upgrade,
                storage_options=storage_options,
            )

            per_util_output_s3 = (
                f"{output_base_s3}/{state}/{utility}/{args.batch}/{segment}/"
                f"comb_bills_year_target/"
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

        _assert_identity(
            master,
            "energy_total_bill",
            [
                "elec_total_bill",
                "gas_total_bill",
                "propane_total_bill",
                "oil_total_bill",
            ],
            FLOAT_TOL,
            "ALL",
        )
        _log_done("Validation", t)

        # --- Optional LMI discount augmentation ---
        if args.calculate_lmi:
            master = _apply_lmi_discounts_to_master(
                master,
                state_upper=state_upper,
                utilities=seg_utilities,
                upgrade=upgrade,
                path_resstock_release=configs[
                    seg_utilities[0]
                ].run_defaults.resstock_base,
                lmi_fpl_year=args.lmi_fpl_year,
                lmi_cpi_s3_path=args.lmi_cpi_s3_path,
                lmi_participation_rates=args.lmi_participation_rates,
                lmi_participation_mode=args.lmi_participation_mode,
                lmi_seed=args.lmi_seed,
                lmi_calculation_type=args.lmi_calculation_type,
            )

        # --- Write output (Hive-partitioned parquet) ---
        output_s3 = (
            f"{output_base_s3}/{state}/all_utilities/{args.batch}/{segment}/"
            f"comb_bills_year_target/"
        )
        t = _log(f"Writing to {output_s3}...")
        _write_hive_partitioned(master, output_s3, "sb.electric_utility")
        _log_done("Writing", t)

    total_elapsed = time.monotonic() - _t0
    mm, ss = divmod(int(total_elapsed), 60)
    _log(f"Done: {len(segments)} segment(s) (total: {mm}m {ss}s)")


if __name__ == "__main__":
    main()

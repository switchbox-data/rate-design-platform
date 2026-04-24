"""CLI entrypoint for CAIRO run validation.

Orchestrates validation blocks discovered from scenario runs (delivery vs
delivery+supply pairs), runs checks, builds summary tables, generates plots,
and saves outputs to a structured directory.

Usage::

    uv run python -m utils.post.validate \\
        --state ny --utility coned \\
        [--batch-name ny_20260305a_r1-2] \\
        [--runs 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16] \\
        [--output-dir validation_outputs/coned]
"""

from __future__ import annotations

import argparse
import traceback
from pathlib import Path
from typing import Any

import polars as pl

from utils.post.validate import (
    CheckResult,
    bat_col_for_allocation,
    check_bat_direction,
    check_bat_near_zero,
    check_bills_increase_with_supply,
    check_flex_subclass_revenue_expectations,
    check_hp_subclass_revenue_lower_with_flex,
    check_hp_bat_increases_with_supply,
    check_nonhp_calibrated_above_original,
    check_nonhp_customers_in_upgrade02,
    check_output_completeness,
    check_revenue_neutrality,
    check_seasonal_winter_below_summer,
    check_subclass_revenue_neutrality,
    check_subclass_rr_sums_to_total,
    check_supply_passthrough_revenue_requirement,
    check_tariff_unchanged,
    check_weights_sum_to_n_customers,
    compute_bill_deltas,
    compute_hourly_cost_of_service,
    compute_weighted_loads_by_subclass_from_collected,
    load_all_mc_components,
    load_bat,
    load_bills,
    load_metadata,
    load_revenue_requirement,
    load_tariff_config,
    plot_avg_bills_by_subclass,
    plot_bat_by_subclass,
    plot_bat_heatmap,
    plot_bill_deltas,
    plot_hourly_cost_of_service,
    plot_hourly_loads_by_subclass,
    plot_nonhp_composition,
    plot_revenue_vs_rr,
    plot_subclass_rr_stacked,
    plot_tariff_comparison,
    plot_tariff_stability,
    plot_weighted_customer_counts,
    scan_utility_loads,
    summarize_bat_by_subclass,
    summarize_bills_by_subclass,
    summarize_customer_counts,
    summarize_customer_weight_stats,
    summarize_nonhp_composition,
    summarize_revenue,
    summarize_tariff_rates,
)
from utils.post.validate.comparison import (
    NY_HP_ONLY_VS_ELECTRIFIED,
    run_ny_hp_only_vs_electrified_comparison,
)
from utils.post.validate.config import (
    RunBlock,
    RunConfig,
    define_run_blocks,
    load_run_configs_from_yaml,
)
from utils.post.validate.discover import find_latest_complete_batch, resolve_batch
from utils.post.validate.subclasses import SubclassSpec, display_subclass
from utils.scenario_config import resolve_subclass_rr_for_validation


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate CAIRO runs for a utility")
    p.add_argument(
        "--state", required=True, help="State abbreviation (e.g. 'ny', 'ri')"
    )
    p.add_argument(
        "--utility", required=True, help="Utility identifier (e.g. 'coned', 'rie')"
    )
    p.add_argument(
        "--batch-name",
        default=None,
        dest="batch_name",
        help="Batch name in {state}_{YYYYMMDD}{letter}_r{run_range} format (e.g. 'ny_20260305a_r1-2'); "
        "omit to use latest complete batch",
    )
    p.add_argument(
        "--runs",
        default=None,
        help="Comma-separated run numbers (default: all runs in the scenario YAML)",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: validation_outputs/{utility})",
    )
    p.add_argument(
        "--skip-loads",
        action="store_true",
        help="Skip hourly load plots and cost-of-service plots "
        "(auto-set for large utilities like coned)",
    )
    p.add_argument(
        "--comparison-profile",
        default=None,
        choices=[NY_HP_ONLY_VS_ELECTRIFIED],
        help="Optional comparison profile to run after standard validation.",
    )
    return p.parse_args()


def _save(plot: Any, path: Path) -> None:
    """Save a plotnine plot to PNG, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    plot.save(str(path), dpi=150, width=10, height=7, verbose=False)


def _emit(result: CheckResult) -> CheckResult:
    """Print a check result and return it for chaining."""
    print(f"    {result.name}: {result.status} — {result.message}")
    return result


def _annotate_result(
    result: CheckResult,
    *,
    block_name: str | None = None,
    run_num: int | None = None,
    run_nums: list[int] | tuple[int, ...] | None = None,
) -> CheckResult:
    """Return a copy of a result with block/run context in details."""
    details = dict(result.details)
    if block_name is not None:
        details["block_name"] = block_name
    if run_num is not None:
        details["run_num"] = run_num
    if run_nums is not None:
        details["run_nums"] = list(run_nums)
    return CheckResult(
        name=result.name,
        status=result.status,
        message=result.message,
        details=details,
    )


def _format_result_context(result: CheckResult) -> str:
    """Format run context for summary output."""
    run_num = result.details.get("run_num")
    run_nums = result.details.get("run_nums")
    if isinstance(run_num, int):
        return f"[run {run_num}] "
    if isinstance(run_nums, list) and run_nums:
        return f"[runs {', '.join(str(n) for n in run_nums)}] "
    return ""


def _maybe_downgrade_bat_near_zero(
    result: CheckResult,
    *,
    cost_scope: str,
    residual_allocation_delivery: str | None,
    residual_allocation_supply: str | None,
) -> CheckResult:
    """Downgrade a FAIL bat_near_zero to WARN for mixed-allocation delivery+supply runs.

    When a delivery+supply run uses different residual allocation methods for
    delivery and supply (e.g. EPMC for delivery, percustomer for supply), the
    delivery-only BAT metric will not be near zero on the combined bill.  A
    FAIL in this case is a false positive, so we downgrade it to WARN.
    """
    is_mixed_alloc = (
        cost_scope == "delivery+supply"
        and residual_allocation_delivery != residual_allocation_supply
    )
    if not (is_mixed_alloc and result.status == "FAIL"):
        return result
    return CheckResult(
        name=result.name,
        status="WARN",
        message=result.message + " [WARN: mixed delivery/supply allocation — "
        f"{residual_allocation_delivery} vs {residual_allocation_supply}]",
        details=result.details,
    )


def _find_matching_noflex_run(
    configs: dict[int, RunConfig],
    *,
    run_num: int,
    config: RunConfig,
) -> int | None:
    """Find the matching no-flex run for a flex run with the same mechanism."""
    if config.elasticity == 0.0 or not config.tariff_type.endswith("_flex"):
        return None

    base_tariff_type = config.tariff_type.removesuffix("_flex")
    matches = sorted(
        candidate_run_num
        for candidate_run_num, candidate in configs.items()
        if candidate_run_num != run_num
        and candidate.run_type == config.run_type
        and candidate.elasticity == 0.0
        and candidate.upgrade == config.upgrade
        and candidate.cost_scope == config.cost_scope
        and candidate.has_subclasses == config.has_subclasses
        and candidate.tariff_type == base_tariff_type
    )
    if len(matches) == 1:
        return matches[0]
    return None


def _find_matching_precalc_run(
    configs: dict[int, RunConfig],
    *,
    run_num: int,
    config: RunConfig,
) -> int | None:
    """Find the precalc run that a default run should inherit from."""
    if config.run_type != "default":
        return None

    # Explicit default->precalc pairing.  This intentionally allows pairs that
    # differ in has_subclasses/upgrade: default runs inherit calibrated tariffs
    # from their precalc counterpart and apply them on upgrade-02 without
    # subclasses.  Runs 1-16 are the canonical orchestration; runs 17-36 are
    # the NY EPMC and electrified-heating variants.
    explicit_pairs = {
        3: 1,
        4: 2,
        7: 5,
        8: 6,
        11: 9,
        12: 10,
        15: 13,
        16: 14,
        19: 17,
        20: 18,
        23: 21,
        24: 22,
        27: 25,
        28: 26,
        31: 29,
        32: 30,
        35: 33,
        36: 34,
    }
    if (paired_precalc := explicit_pairs.get(run_num)) is not None:
        candidate = configs.get(paired_precalc)
        if (
            candidate is not None
            and candidate.run_type == "precalc"
            and candidate.cost_scope == config.cost_scope
        ):
            return paired_precalc

    matches = sorted(
        candidate_run_num
        for candidate_run_num, candidate in configs.items()
        if candidate_run_num != run_num
        and candidate.run_type == "precalc"
        and candidate.cost_scope == config.cost_scope
        and candidate.has_subclasses == config.has_subclasses
        and candidate.tariff_type == config.tariff_type
        and candidate.elasticity == config.elasticity
    )
    if len(matches) == 1:
        return matches[0]
    return None


def _find_matching_original_flat_run(
    configs: dict[int, RunConfig],
    *,
    run_num: int,
    config: RunConfig,
) -> int | None:
    """Find the baseline flat precalc run used for non-HP tariff comparisons."""
    if config.run_type != "precalc" or not config.has_subclasses:
        return None

    matches = sorted(
        candidate_run_num
        for candidate_run_num, candidate in configs.items()
        if candidate_run_num != run_num
        and candidate.run_type == "precalc"
        and candidate.upgrade == "0"
        and candidate.cost_scope == config.cost_scope
        and not candidate.has_subclasses
        and candidate.tariff_type == "flat"
        and candidate.elasticity == 0.0
    )
    if len(matches) == 1:
        return matches[0]
    return None


def _safe_execute(
    operation_name: str, func: Any, *args: Any, **kwargs: Any
) -> tuple[Any, bool]:
    """Execute a function safely, logging errors but not raising exceptions.

    Returns:
        Tuple of (result, success) where result is the function return value
        (or None if it failed) and success is a boolean.
    """
    try:
        result = func(*args, **kwargs)
        return result, True
    except Exception as e:
        print(f"    ERROR in {operation_name}: {type(e).__name__}: {e}")
        print(f"    Traceback: {traceback.format_exc()}")
        return None, False


def _resolve_block_subclass_spec(
    block: RunBlock,
    configs: dict[int, RunConfig],
) -> SubclassSpec | None:
    for config in block.configs:
        if config.subclass_spec is not None:
            return config.subclass_spec

    for config in block.configs:
        matched = _find_matching_precalc_run(
            configs, run_num=config.run_num, config=config
        )
        if matched is None:
            continue
        matched_config = configs.get(matched)
        if matched_config is not None and matched_config.subclass_spec is not None:
            return matched_config.subclass_spec
    return None


def _validate_block(
    block: RunBlock,
    run_dirs: dict[int, str],
    state: str,
    utility: str,
    output_dir: Path,
    configs: dict[int, RunConfig],
    *,
    skip_loads: bool = False,
    mc_components: dict[str, pl.Series] | None = None,
) -> list[CheckResult]:
    nums = list(block.run_nums)
    dirs_opt = [run_dirs.get(n) for n in nums]

    if any(d is None for d in dirs_opt):
        missing = [n for n, d in zip(nums, dirs_opt) if d is None]
        print(f"  WARNING: Skipping {block.name} — missing run dirs for: {missing}")
        return []

    dirs: list[str] = [
        d for d in dirs_opt if d is not None
    ]  # all present after guard above

    run_nums_str = ", ".join(map(str, sorted(nums)))
    print(f"\n  {block.name} (runs {run_nums_str}): {block.description}")
    block_dir = output_dir / block.name
    block_dir.mkdir(parents=True, exist_ok=True)
    plots = block_dir / "plots"
    results: list[CheckResult] = []
    block_subclass_spec = _resolve_block_subclass_spec(block, configs)

    def _record(
        result: CheckResult,
        *,
        run_num: int | None = None,
        run_nums: list[int] | tuple[int, ...] | None = None,
    ) -> None:
        annotated = _annotate_result(
            result, block_name=block.name, run_num=run_num, run_nums=run_nums
        )
        results.append(_emit(annotated))

    # Load metadata and bills with error handling
    metas: list[pl.LazyFrame] = []
    all_bills: list[dict[str, pl.LazyFrame]] = []
    for d in dirs:
        meta, meta_ok = _safe_execute(f"load_metadata({d})", load_metadata, d)
        if meta_ok and meta is not None:
            metas.append(meta)
        else:
            metas.append(pl.LazyFrame())  # Empty frame as placeholder

        bills_dict: dict[str, pl.LazyFrame] = {}
        for bill_type in ["elec", "gas", "comb"]:
            bills, bills_ok = _safe_execute(
                f"load_bills({d}, {bill_type})", load_bills, d, bill_type
            )
            if bills_ok and bills is not None:
                bills_dict[bill_type] = bills
            else:
                bills_dict[bill_type] = pl.LazyFrame()  # Empty frame as placeholder
        all_bills.append(bills_dict)

    runs = list(zip(nums, dirs, block.configs, metas, all_bills))

    # --- Output completeness ---
    for run_num, s3_dir, *_ in runs:
        check_result, ok = _safe_execute(
            f"check_output_completeness(run {run_num})",
            check_output_completeness,
            s3_dir,
        )
        if ok and check_result is not None:
            _record(check_result, run_num=run_num)

    # --- Bills summary + plots ---
    for run_num, _, _, meta, bills in runs:
        run_dir = block_dir / f"run_{run_num}"
        run_dir.mkdir(parents=True, exist_ok=True)
        print(
            f"    Run {run_num}: Saving diagnostics to {run_dir.relative_to(output_dir)}/"
        )
        summary, ok = _safe_execute(
            f"summarize_bills_by_subclass(run {run_num})",
            summarize_bills_by_subclass,
            bills,
            meta,
            block_subclass_spec,
        )
        if ok and summary is not None:
            try:
                summary.write_csv(run_dir / "bills_summary.csv")
            except Exception as e:
                print(f"    ERROR writing bills_summary.csv for run {run_num}: {e}")
            try:
                plot = plot_avg_bills_by_subclass(
                    summary, f"Annual Bills by Subclass — Run {run_num}"
                )
                _save(plot, plots / "bills" / f"bills_by_subclass_run{run_num}.png")
            except Exception as e:
                print(f"    ERROR creating bills plot for run {run_num}: {e}")

    # --- Cross-run sanity checks (precalc blocks 1-2 and 5-6) ---
    if block.revenue_neutral and len(runs) == 2:
        (num_a, dir_a, _, meta_a, bills_a), (num_b, dir_b, _, _, bills_b) = (
            runs[0],
            runs[1],
        )
        print(f"\n    Cross-run checks: run {num_a} vs run {num_b}")

        # Bills must rise for both subclasses when supply is added
        check_result, ok = _safe_execute(
            f"check_bills_increase_with_supply(run {num_a}→{num_b})",
            check_bills_increase_with_supply,
            bills_a["comb"],
            bills_b["comb"],
            meta_a,
            num_a,
            num_b,
            block_subclass_spec,
        )
        if ok and check_result is not None:
            _record(check_result, run_nums=[num_a, num_b])

        # For no-flex blocks, adding supply should deepen HP cross-subsidy. For
        # demand-flex blocks, run A already uses supply-informed TOU ratios, so
        # this comparison is not a meaningful contract.
        if any(config.elasticity != 0.0 for config in block.configs):
            print("    Skipping hp_bat_increases_with_supply for demand-flex block")
        elif block_subclass_spec is None or "hp" not in block_subclass_spec.aliases:
            print("    Skipping hp_bat_increases_with_supply for non-HP subclass block")
        else:
            bat_a, bat_a_ok = _safe_execute(f"load_bat(run {num_a})", load_bat, dir_a)
            bat_b, bat_b_ok = _safe_execute(f"load_bat(run {num_b})", load_bat, dir_b)
            if bat_a_ok and bat_b_ok and bat_a is not None and bat_b is not None:
                check_result, ok = _safe_execute(
                    f"check_hp_bat_increases_with_supply(run {num_a}→{num_b})",
                    check_hp_bat_increases_with_supply,
                    bat_a,
                    bat_b,
                    meta_a,
                    num_a,
                    num_b,
                )
                if ok and check_result is not None:
                    _record(check_result, run_nums=[num_a, num_b])

    # --- Revenue neutrality (precalc runs 1-2, 5-6) ---
    if block.revenue_neutral:
        has_sub = block.configs[0].has_subclasses
        total_rr, rr_ok = _safe_execute(
            "load_revenue_requirement", load_revenue_requirement, state, utility
        )
        if not rr_ok or total_rr is None:
            print(
                "    ERROR: Failed to load revenue requirement, skipping revenue checks"
            )
            total_rr = None

        subclass_rr_raw = None
        if has_sub and total_rr is not None:
            rr_filename = block.configs[0].revenue_requirement_filename
            if rr_filename is None:
                print("    ERROR: Missing subclass revenue requirement filename")
            else:
                subclass_rr_raw, sub_rr_ok = _safe_execute(
                    "load_revenue_requirement (subclass)",
                    load_revenue_requirement,
                    state,
                    utility,
                    rr_filename,
                )
                if not sub_rr_ok:
                    subclass_rr_raw = None

        if has_sub and subclass_rr_raw is not None and len(runs) == 2:
            delivery_run = next(
                (run for run in runs if run[2].cost_scope == "delivery"),
                None,
            )
            total_run = next(
                (run for run in runs if run[2].cost_scope == "delivery+supply"),
                None,
            )
            if delivery_run is not None and total_run is not None:
                (
                    delivery_num,
                    _,
                    delivery_config,
                    delivery_meta,
                    delivery_bills,
                ) = delivery_run
                total_num, _, total_config, _, total_bills = total_run
                check_result, ok = _safe_execute(
                    "check_supply_passthrough_revenue_requirement"
                    f"(run {delivery_num}→{total_num})",
                    check_supply_passthrough_revenue_requirement,
                    delivery_bills["elec"],
                    total_bills["elec"],
                    delivery_meta,
                    subclass_rr_raw,
                    subclass_spec=total_config.subclass_spec
                    or delivery_config.subclass_spec
                    or block_subclass_spec,
                )
                if ok and check_result is not None:
                    _record(check_result, run_nums=[delivery_num, total_num])

        for run_num, _, config, meta, bills in runs:
            run_dir = block_dir / f"run_{run_num}"
            run_dir.mkdir(parents=True, exist_ok=True)
            print(
                f"    Run {run_num}: Saving revenue diagnostics to {run_dir.relative_to(output_dir)}/"
            )
            is_flex_run = config.elasticity != 0.0
            subclass_rr = None
            if has_sub and subclass_rr_raw is not None:
                try:
                    resolved = resolve_subclass_rr_for_validation(
                        subclass_rr_raw,
                        config.cost_scope,
                        residual_allocation_delivery=config.residual_allocation_delivery
                        or "percustomer",
                        residual_allocation_supply=config.residual_allocation_supply
                        or "passthrough",
                    )
                    subclass_rr = {
                        "subclass_revenue_requirements": resolved,
                    }
                    for total_key in (
                        "total_delivery_revenue_requirement",
                        "total_delivery_and_supply_revenue_requirement",
                    ):
                        if total_key in subclass_rr_raw:
                            subclass_rr[total_key] = subclass_rr_raw[total_key]
                except Exception:
                    subclass_rr = subclass_rr_raw
            # Build effective RR target: for subclassed runs, use the sum of
            # resolved subclass RRs (the actual calibration target) rather than
            # the utility-wide total which may cover a broader population.
            effective_rr: dict[str, Any] | None = None
            if total_rr is not None:
                if has_sub and subclass_rr is not None:
                    rr_key = (
                        "total"
                        if config.cost_scope == "delivery+supply"
                        else "delivery"
                    )
                    sub_sum = sum(
                        float(v[rr_key])
                        for v in subclass_rr["subclass_revenue_requirements"].values()
                    )
                    total_key = (
                        "total_delivery_and_supply_revenue_requirement"
                        if config.cost_scope == "delivery+supply"
                        else "total_delivery_revenue_requirement"
                    )
                    effective_rr = {total_key: sub_sum}
                else:
                    effective_rr = total_rr

            if effective_rr is not None and not is_flex_run:
                check_result, ok = _safe_execute(
                    f"check_revenue_neutrality(run {run_num})",
                    check_revenue_neutrality,
                    bills["elec"],
                    meta,
                    effective_rr,
                    config.cost_scope,
                )
                if ok and check_result is not None:
                    _record(check_result, run_num=run_num)

            if has_sub and subclass_rr is not None:
                if is_flex_run:
                    check_result, ok = _safe_execute(
                        f"check_flex_subclass_revenue_expectations(run {run_num})",
                        check_flex_subclass_revenue_expectations,
                        bills["elec"],
                        meta,
                        subclass_rr,
                        config.cost_scope,
                        config.subclass_spec or block_subclass_spec,
                    )
                else:
                    check_result, ok = _safe_execute(
                        f"check_subclass_revenue_neutrality(run {run_num})",
                        check_subclass_revenue_neutrality,
                        bills["elec"],
                        meta,
                        subclass_rr,
                        config.cost_scope,
                        subclass_spec=config.subclass_spec or block_subclass_spec,
                    )
                if ok and check_result is not None:
                    _record(check_result, run_num=run_num)

                if is_flex_run:
                    noflex_run_num = _find_matching_noflex_run(
                        configs, run_num=run_num, config=config
                    )
                    noflex_run_dir = (
                        run_dirs.get(noflex_run_num)
                        if noflex_run_num is not None
                        else None
                    )
                    if noflex_run_num is None or noflex_run_dir is None:
                        print(
                            f"    WARNING: No matching no-flex run found for flex run {run_num}"
                        )
                    else:
                        noflex_meta, noflex_meta_ok = _safe_execute(
                            f"load_metadata(run {noflex_run_num})",
                            load_metadata,
                            noflex_run_dir,
                        )
                        noflex_bills, noflex_bills_ok = _safe_execute(
                            f"load_bills(run {noflex_run_num}, elec)",
                            load_bills,
                            noflex_run_dir,
                            "elec",
                        )
                        if (
                            noflex_meta_ok
                            and noflex_bills_ok
                            and noflex_meta is not None
                            and noflex_bills is not None
                        ):
                            check_result, ok = _safe_execute(
                                "check_hp_subclass_revenue_lower_with_flex"
                                f"(run {noflex_run_num}→{run_num})",
                                check_hp_subclass_revenue_lower_with_flex,
                                noflex_bills,
                                bills["elec"],
                                noflex_meta,
                                meta,
                                noflex_run_num,
                                run_num,
                            )
                            if ok and check_result is not None:
                                _record(
                                    check_result, run_nums=[noflex_run_num, run_num]
                                )

                if not is_flex_run and effective_rr is not None:
                    check_result, ok = _safe_execute(
                        f"check_subclass_rr_sums_to_total(run {run_num})",
                        check_subclass_rr_sums_to_total,
                        subclass_rr,
                        effective_rr,
                        config.cost_scope,
                    )
                    if ok and check_result is not None:
                        _record(check_result, run_num=run_num)

                try:
                    if total_rr is None:
                        raise ValueError("total_rr is None")
                    sub_rrs = subclass_rr["subclass_revenue_requirements"]
                    rr_key = (
                        "total"
                        if config.cost_scope == "delivery+supply"
                        else "delivery"
                    )
                    rr_vals = {
                        display_subclass(alias): float(values[rr_key])
                        for alias, values in sub_rrs.items()
                    }
                    total_rr_val = float(
                        total_rr[
                            "total_delivery_and_supply_revenue_requirement"
                            if config.cost_scope == "delivery+supply"
                            else "total_delivery_revenue_requirement"
                        ]
                    )

                    rev, rev_ok = _safe_execute(
                        f"summarize_revenue(run {run_num})",
                        summarize_revenue,
                        bills["elec"],
                        meta,
                        config.subclass_spec or block_subclass_spec,
                    )
                    if rev_ok and rev is not None:
                        try:
                            rev.write_csv(run_dir / "revenue_summary.csv")
                        except Exception as e:
                            print(
                                f"    ERROR writing revenue_summary.csv for run {run_num}: {e}"
                            )
                        try:
                            _save(
                                plot_revenue_vs_rr(
                                    rev, rr_vals, f"Revenue vs RR — Run {run_num}"
                                ),
                                plots
                                / "revenue_neutrality"
                                / f"revenue_vs_rr_run{run_num}.png",
                            )
                            _save(
                                plot_subclass_rr_stacked(
                                    rr_vals,
                                    total_rr_val,
                                    f"Subclass RR vs Total — Run {run_num}",
                                ),
                                plots
                                / "revenue_neutrality"
                                / f"subclass_rr_stacked_run{run_num}.png",
                            )
                        except Exception as e:
                            print(
                                f"    ERROR creating revenue plots for run {run_num}: {e}"
                            )
                except Exception as e:
                    print(f"    ERROR processing revenue data for run {run_num}: {e}")

    # --- BAT direction and magnitude (precalc runs 1-2, 5-6) ---
    if block.bat_relevant:
        for run_num, s3_dir, config, meta, _ in runs:
            run_dir = block_dir / f"run_{run_num}"
            run_dir.mkdir(parents=True, exist_ok=True)
            print(
                f"    Run {run_num}: Saving BAT diagnostics to {run_dir.relative_to(output_dir)}/"
            )
            bat, bat_ok = _safe_execute(f"load_bat(run {run_num})", load_bat, s3_dir)
            if not bat_ok or bat is None:
                continue

            check_result, ok = _safe_execute(
                f"check_bat_direction(run {run_num})",
                check_bat_direction,
                bat,
                meta,
                config.subclass_spec or block_subclass_spec,
            )
            if ok and check_result is not None:
                _record(check_result, run_num=run_num)

            if config.has_subclasses or config.elasticity != 0.0:
                operative_bat = bat_col_for_allocation(
                    config.residual_allocation_delivery
                )
                check_result, ok = _safe_execute(
                    f"check_bat_near_zero(run {run_num})",
                    check_bat_near_zero,
                    bat,
                    meta,
                    subclass_spec=config.subclass_spec or block_subclass_spec,
                    bat_metric=operative_bat,
                )
                if ok and check_result is not None:
                    check_result = _maybe_downgrade_bat_near_zero(
                        check_result,
                        cost_scope=config.cost_scope,
                        residual_allocation_delivery=config.residual_allocation_delivery,
                        residual_allocation_supply=config.residual_allocation_supply,
                    )
                    _record(check_result, run_num=run_num)

            bat_summary, summary_ok = _safe_execute(
                f"summarize_bat_by_subclass(run {run_num})",
                summarize_bat_by_subclass,
                bat,
                meta,
                config.subclass_spec or block_subclass_spec,
            )
            if summary_ok and bat_summary is not None:
                try:
                    bat_summary.write_csv(run_dir / "bat_summary.csv")
                except Exception as e:
                    print(f"    ERROR writing bat_summary.csv for run {run_num}: {e}")
                try:
                    _save(
                        plot_bat_by_subclass(
                            bat_summary, f"Per-Customer BAT — Run {run_num}"
                        ),
                        plots / "cross_subsidy" / f"bat_by_subclass_run{run_num}.png",
                    )
                    _save(
                        plot_bat_heatmap(bat_summary, f"BAT Heatmap — Run {run_num}"),
                        plots / "cross_subsidy" / f"bat_heatmap_run{run_num}.png",
                    )
                except Exception as e:
                    print(f"    ERROR creating BAT plots for run {run_num}: {e}")

    # --- Seasonal rate ordering: winter rate < summer rate (seasonal precalc blocks) ---
    _seasonal_types = {"seasonal", "seasonalTOU", "seasonalTOU_flex"}
    if block.bat_relevant and any(
        c.tariff_type in _seasonal_types for c in block.configs
    ):
        print("\n    Seasonal rate ordering check — period→season mapping:")
        for run_num, run_dir, config, _, _ in runs:
            if config.tariff_type not in _seasonal_types:
                continue
            tariff, tariff_ok = _safe_execute(
                f"load_tariff_config(run {run_num})", load_tariff_config, run_dir
            )
            if tariff_ok and tariff is not None:
                check_result, ok = _safe_execute(
                    f"check_seasonal_winter_below_summer(run {run_num})",
                    check_seasonal_winter_below_summer,
                    tariff,
                    run_num,
                )
                if ok and check_result is not None:
                    _record(check_result, run_num=run_num)

    # --- Tariff stability + bill deltas (all default paired blocks) ---
    if block.tariff_should_be_unchanged:
        for run_num, run_dir, config, meta, bills in runs:
            prev_num = _find_matching_precalc_run(
                configs, run_num=run_num, config=config
            )
            if prev_num is None:
                print(
                    f"    WARNING: No matching precalc run found for default run {run_num}"
                )
                continue
            if (prev_dir := run_dirs.get(prev_num)) is None:
                continue

            run_output_dir = block_dir / f"run_{run_num}"
            run_output_dir.mkdir(parents=True, exist_ok=True)
            print(
                f"    Run {run_num}: Saving tariff and bill delta diagnostics to {run_output_dir.relative_to(output_dir)}/"
            )
            in_tariff, in_ok = _safe_execute(
                f"load_tariff_config(run {prev_num})", load_tariff_config, prev_dir
            )
            out_tariff, out_ok = _safe_execute(
                f"load_tariff_config(run {run_num})", load_tariff_config, run_dir
            )
            if not in_ok or not out_ok or in_tariff is None or out_tariff is None:
                print(
                    f"    ERROR: Failed to load tariffs for run {run_num}, skipping tariff checks"
                )
                continue

            in_rates, in_rates_ok = _safe_execute(
                f"summarize_tariff_rates(run {prev_num})",
                summarize_tariff_rates,
                in_tariff,
            )
            out_rates, out_rates_ok = _safe_execute(
                f"summarize_tariff_rates(run {run_num})",
                summarize_tariff_rates,
                out_tariff,
            )
            if (
                in_rates_ok
                and out_rates_ok
                and in_rates is not None
                and out_rates is not None
            ):
                check_result, ok = _safe_execute(
                    f"check_tariff_unchanged(run {run_num})",
                    check_tariff_unchanged,
                    in_tariff,
                    out_tariff,
                )
                if ok and check_result is not None:
                    _record(check_result, run_num=run_num)
                try:
                    in_rates.write_csv(
                        run_output_dir / f"tariff_rates_input_run{prev_num}.csv"
                    )
                    out_rates.write_csv(run_output_dir / "tariff_rates_output.csv")
                except Exception as e:
                    print(f"    ERROR writing tariff rates CSV for run {run_num}: {e}")
                try:
                    _save(
                        plot_tariff_comparison(
                            in_rates, out_rates, f"Tariff: Run {prev_num} -> {run_num}"
                        ),
                        plots / "tariff_diffs" / f"tariff_comparison_run{run_num}.png",
                    )
                    _save(
                        plot_tariff_stability(
                            in_rates,
                            out_rates,
                            f"Tariff Diff — Run {run_num} vs {prev_num}",
                        ),
                        plots / "tariff_diffs" / f"tariff_stability_run{run_num}.png",
                    )
                except Exception as e:
                    print(f"    ERROR creating tariff plots for run {run_num}: {e}")

            prev_bills, prev_bills_ok = _safe_execute(
                f"load_bills(run {prev_num})", load_bills, prev_dir, "elec"
            )
            if prev_bills_ok and prev_bills is not None:
                delta, delta_ok = _safe_execute(
                    f"compute_bill_deltas(run {run_num})",
                    compute_bill_deltas,
                    prev_bills,
                    bills["elec"],
                    meta,
                    configs[prev_num].subclass_spec or block_subclass_spec,
                )
                if delta_ok and delta is not None:
                    try:
                        delta.write_csv(run_output_dir / "bill_deltas.csv")
                    except Exception as e:
                        print(
                            f"    ERROR writing bill_deltas.csv for run {run_num}: {e}"
                        )
                    try:
                        _save(
                            plot_bill_deltas(
                                delta, f"Bill Change — Run {run_num} vs {prev_num}"
                            ),
                            plots / f"bill_deltas_run{run_num}.png",
                        )
                    except Exception as e:
                        print(
                            f"    ERROR creating bill deltas plot for run {run_num}: {e}"
                        )

    # --- Non-HP composition (all upgrade-02 blocks) ---
    if block.configs[0].upgrade == "2":
        for run_num, _, _, meta, _ in runs:
            run_dir = block_dir / f"run_{run_num}"
            run_dir.mkdir(parents=True, exist_ok=True)
            print(
                f"    Run {run_num}: Saving non-HP composition diagnostics to {run_dir.relative_to(output_dir)}/"
            )
            comp, comp_ok = _safe_execute(
                f"summarize_nonhp_composition(run {run_num})",
                summarize_nonhp_composition,
                meta,
            )
            if comp_ok and comp is not None:
                try:
                    comp.write_csv(run_dir / "nonhp_composition.csv")
                except Exception as e:
                    print(
                        f"    ERROR writing nonhp_composition.csv for run {run_num}: {e}"
                    )
                check_result, ok = _safe_execute(
                    f"check_nonhp_customers_in_upgrade02(run {run_num})",
                    check_nonhp_customers_in_upgrade02,
                    meta,
                )
                if ok and check_result is not None:
                    _record(check_result, run_num=run_num)
                try:
                    _save(
                        plot_nonhp_composition(
                            comp, f"Non-HP Composition — Run {run_num}"
                        ),
                        plots / f"nonhp_composition_run{run_num}.png",
                    )
                except Exception as e:
                    print(
                        f"    ERROR creating non-HP composition plot for run {run_num}: {e}"
                    )

    # --- Non-HP rate calibrated above original (subclass precalc runs 5-6) ---
    if (
        block.configs[0].has_subclasses
        and block.revenue_neutral
        and block_subclass_spec is not None
        and "non-hp" in block_subclass_spec.aliases
    ):
        for run_num, run_dir, config, _, _ in runs:
            orig_num = _find_matching_original_flat_run(
                configs, run_num=run_num, config=config
            )
            if orig_num is None:
                print(
                    f"    WARNING: No matching original flat run found for subclass run {run_num}"
                )
                continue
            if (orig_dir := run_dirs.get(orig_num)) is None:
                continue
            run_tariff, run_tariff_ok = _safe_execute(
                f"load_tariff_config(run {run_num})", load_tariff_config, run_dir
            )
            orig_tariff, orig_tariff_ok = _safe_execute(
                f"load_tariff_config(run {orig_num})", load_tariff_config, orig_dir
            )
            if (
                run_tariff_ok
                and orig_tariff_ok
                and run_tariff is not None
                and orig_tariff is not None
            ):
                check_result, ok = _safe_execute(
                    f"check_nonhp_calibrated_above_original(run {run_num})",
                    check_nonhp_calibrated_above_original,
                    run_tariff,
                    orig_tariff,
                )
                if ok and check_result is not None:
                    _record(check_result, run_num=run_num)

    # Always write checks summary, even if empty
    try:
        pl.DataFrame(
            [
                {
                    "check": r.name,
                    "status": r.status,
                    "message": r.message,
                    "block_name": r.details.get("block_name"),
                    "run_num": r.details.get("run_num"),
                    "run_nums": ",".join(map(str, r.details.get("run_nums", []))),
                }
                for r in results
            ]
        ).write_csv(block_dir / "checks_summary.csv")
    except Exception as e:
        print(f"    ERROR writing checks_summary.csv: {e}")
    return results


def main() -> None:
    args = _parse_args()
    state, utility = args.state.lower(), args.utility.lower()
    output_dir = Path(args.output_dir or f"validation_outputs/{utility}")
    output_dir.mkdir(parents=True, exist_ok=True)

    run_nums: list[int] | None = None
    if args.runs:
        run_nums = sorted({int(n) for n in args.runs.split(",") if n.strip()})

    configs = load_run_configs_from_yaml(state, utility, run_nums)
    resolved_run_nums = sorted(configs)

    print(f"Validating runs {resolved_run_nums} for {state.upper()} / {utility}")
    print(f"Output: {output_dir}")
    run_names = {n: c.run_name for n, c in configs.items()}

    if args.batch_name:
        batch_name = args.batch_name
        run_dirs = resolve_batch(state, utility, batch_name, run_names)
    else:
        batch_name, run_dirs = find_latest_complete_batch(state, utility, run_names)
        print(f"  Batch: {batch_name}")

    if missing := sorted(set(resolved_run_nums) - run_dirs.keys()):
        print(f"  WARNING: Missing run dirs for runs: {missing}")

    # --- Preprocessing block (always) ---
    print("\n  preprocessing: Customer weight statistics and validation")
    preprocess_dir = output_dir / "preprocessing"
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_plots_dir = preprocess_dir / "plots"
    preprocess_plots_dir.mkdir(parents=True, exist_ok=True)

    # Load run-1 metadata for preprocessing (upgrade 00)
    if 1 in run_dirs:
        meta_run1, meta_ok = _safe_execute(
            "load_metadata(run 1)", load_metadata, run_dirs[1]
        )
        if meta_ok and meta_run1 is not None:
            # Summarize customer weight stats
            weight_stats, stats_ok = _safe_execute(
                "summarize_customer_weight_stats",
                summarize_customer_weight_stats,
                meta_run1,
            )
            if stats_ok and weight_stats is not None:
                try:
                    weight_stats.write_csv(preprocess_dir / "customer_weight_stats.csv")
                except Exception as e:
                    print(f"    ERROR writing customer_weight_stats.csv: {e}")

            # Check weights sum to N customers
            weight_check, check_ok = _safe_execute(
                "check_weights_sum_to_n_customers",
                check_weights_sum_to_n_customers,
                meta_run1,
            )
            if check_ok and weight_check is not None:
                _emit(weight_check)
                try:
                    pl.DataFrame(
                        [
                            {
                                "check": weight_check.name,
                                "status": weight_check.status,
                                "message": weight_check.message,
                                "block_name": "preprocessing",
                                "run_num": 1,
                                "run_nums": "1",
                            }
                        ]
                    ).write_csv(preprocess_dir / "checks.csv")
                except Exception as e:
                    print(f"    ERROR writing preprocessing checks.csv: {e}")

            # Plot weighted customer counts (upgrade 00, once)
            counts, counts_ok = _safe_execute(
                "summarize_customer_counts", summarize_customer_counts, meta_run1
            )
            if counts_ok and counts is not None:
                try:
                    _save(
                        plot_weighted_customer_counts(
                            counts, "Weighted Customer Counts by Subclass (Upgrade 00)"
                        ),
                        preprocess_plots_dir / "customer_counts.png",
                    )
                except Exception as e:
                    print(f"    ERROR creating customer counts plot: {e}")
    else:
        print("  WARNING: Run 1 not found, skipping preprocessing")

    # --- Load utility loads once per utility (if not skip_loads) ---
    # All runs use the same upgrade-00 loads, so collect once for all buildings
    # that appear in any run for this utility
    loads_df: pl.DataFrame | None = None
    if not args.skip_loads:
        if 1 in configs:
            run1_config = configs[1]
            # Extract upgrade-00 path from run1's path_resstock_loads
            # Path format: /ebs/data/.../load_curve_hourly/state=NY/upgrade=00/
            path_loads = run1_config.path_resstock_loads
            if path_loads:
                print(f"  Loading ResStock loads from: {path_loads}")
                loads_lf, loads_lf_ok = _safe_execute(
                    "scan_utility_loads", scan_utility_loads, path_loads
                )
                if loads_lf_ok and loads_lf is not None:
                    # Collect all metadata from all runs to get all bldg_ids for this utility
                    all_bldg_ids: set[int] = set()
                    for run_num in resolved_run_nums:
                        if run_num in run_dirs:
                            meta, meta_ok = _safe_execute(
                                f"load_metadata(run {run_num})",
                                load_metadata,
                                run_dirs[run_num],
                            )
                            if meta_ok and meta is not None:
                                try:
                                    meta_collected = (
                                        meta.select("bldg_id").unique().collect()
                                    )
                                    if isinstance(meta_collected, pl.DataFrame):
                                        bldg_ids = meta_collected.to_series().to_list()
                                        all_bldg_ids.update(bldg_ids)
                                except Exception as e:
                                    print(
                                        f"    ERROR collecting bldg_ids from run {run_num}: {e}"
                                    )

                    # Collect loads once for all buildings across all runs
                    if all_bldg_ids:
                        print(
                            f"  Collecting loads for {len(all_bldg_ids)} buildings (once per utility)"
                        )
                        try:
                            loads_df_collected = loads_lf.filter(
                                pl.col("bldg_id")
                                .cast(pl.Int64)
                                .is_in(list(all_bldg_ids))
                            ).collect()
                            if isinstance(loads_df_collected, pl.DataFrame):
                                loads_df = loads_df_collected
                        except Exception as e:
                            print(f"    ERROR collecting loads: {e}")
            else:
                print("  WARNING: path_resstock_loads not found in run 1 config")
        else:
            print("  WARNING: Run 1 config not found, skipping load scan")

    # --- Load MC components once from run 2 (if not skip_loads) ---
    mc_components: dict[str, pl.Series] | None = None
    if not args.skip_loads:
        if 2 in configs:
            run2_config = configs[2]
            print("  Loading marginal cost components from run 2")
            mc_components, mc_ok = _safe_execute(
                "load_all_mc_components", load_all_mc_components, run2_config
            )
            if not mc_ok:
                mc_components = None
        else:
            print("  WARNING: Run 2 config not found, skipping MC component load")

    # --- Generate load-related outputs once per utility (if not skip_loads) ---
    if not args.skip_loads and loads_df is not None:
        # Use run 1 metadata (upgrade 00) for load outputs - same loads used by all runs
        if 1 in run_dirs:
            print("\n  Generating load-related outputs (once per utility)")
            loads_output_dir = output_dir / "loads"
            loads_output_dir.mkdir(parents=True, exist_ok=True)
            loads_plots_dir = loads_output_dir / "plots"
            loads_plots_dir.mkdir(parents=True, exist_ok=True)

            meta_run1, meta_ok = _safe_execute(
                "load_metadata(run 1)", load_metadata, run_dirs[1]
            )
            if meta_ok and meta_run1 is not None:
                try:
                    meta_run1_collected = meta_run1.collect()
                except Exception as e:
                    print(f"    ERROR collecting metadata: {e}")
                    meta_run1_collected = None

                # Compute weighted loads by subclass
                if (
                    isinstance(meta_run1_collected, pl.DataFrame)
                    and loads_df is not None
                ):
                    loads_by_subclass_df, loads_ok = _safe_execute(
                        "compute_weighted_loads_by_subclass_from_collected",
                        compute_weighted_loads_by_subclass_from_collected,
                        loads_df,
                        meta_run1_collected,
                    )
                    if loads_ok and loads_by_subclass_df is not None:
                        try:
                            loads_by_subclass_df.write_csv(
                                loads_output_dir / "loads_by_subclass.csv"
                            )
                        except Exception as e:
                            print(f"    ERROR writing loads_by_subclass.csv: {e}")
                        try:
                            _save(
                                plot_hourly_loads_by_subclass(
                                    loads_by_subclass_df, "Hourly Loads by Subclass"
                                ),
                                loads_plots_dir / "hourly_loads_by_subclass.png",
                            )
                        except Exception as e:
                            print(f"    ERROR creating hourly loads plot: {e}")

                        # Three cost-of-service plots (if MC components available)
                        if mc_components is not None:
                            try:
                                # Delivery: dist_sub_tx + bulk_tx
                                mc_delivery = (
                                    mc_components["dist_sub_tx"]
                                    + mc_components["bulk_tx"]
                                )
                                cos_delivery, cos_ok = _safe_execute(
                                    "compute_hourly_cost_of_service (delivery)",
                                    compute_hourly_cost_of_service,
                                    loads_by_subclass_df,
                                    mc_delivery,
                                )
                                if cos_ok and cos_delivery is not None:
                                    try:
                                        _save(
                                            plot_hourly_cost_of_service(
                                                cos_delivery,
                                                "Hourly Cost of Service (Delivery)",
                                            ),
                                            loads_plots_dir / "hourly_cos_delivery.png",
                                        )
                                    except Exception as e:
                                        print(
                                            f"    ERROR creating delivery COS plot: {e}"
                                        )

                                # Supply: supply_energy + supply_capacity
                                mc_supply = (
                                    mc_components["supply_energy"]
                                    + mc_components["supply_capacity"]
                                )
                                cos_supply, cos_ok = _safe_execute(
                                    "compute_hourly_cost_of_service (supply)",
                                    compute_hourly_cost_of_service,
                                    loads_by_subclass_df,
                                    mc_supply,
                                )
                                if cos_ok and cos_supply is not None:
                                    try:
                                        _save(
                                            plot_hourly_cost_of_service(
                                                cos_supply,
                                                "Hourly Cost of Service (Supply)",
                                            ),
                                            loads_plots_dir / "hourly_cos_supply.png",
                                        )
                                    except Exception as e:
                                        print(
                                            f"    ERROR creating supply COS plot: {e}"
                                        )

                                # Combined: all four MC components
                                mc_combined = (
                                    mc_components["dist_sub_tx"]
                                    + mc_components["bulk_tx"]
                                    + mc_components["supply_energy"]
                                    + mc_components["supply_capacity"]
                                )
                                cos_combined, cos_ok = _safe_execute(
                                    "compute_hourly_cost_of_service (combined)",
                                    compute_hourly_cost_of_service,
                                    loads_by_subclass_df,
                                    mc_combined,
                                )
                                if cos_ok and cos_combined is not None:
                                    try:
                                        _save(
                                            plot_hourly_cost_of_service(
                                                cos_combined,
                                                "Hourly Cost of Service (Combined)",
                                            ),
                                            loads_plots_dir / "hourly_cos_combined.png",
                                        )
                                    except Exception as e:
                                        print(
                                            f"    ERROR creating combined COS plot: {e}"
                                        )
                            except Exception as e:
                                print(f"    ERROR processing MC components: {e}")
                else:
                    print(
                        "  WARNING: Could not collect metadata or loads, skipping load outputs"
                    )

    # --- Validate blocks ---
    all_results: list[CheckResult] = []
    for block in define_run_blocks(configs):
        try:
            block_results = _validate_block(
                block,
                run_dirs,
                state,
                utility,
                output_dir,
                configs,
                skip_loads=args.skip_loads,
                mc_components=mc_components,
            )
            all_results.extend(block_results)
        except Exception as e:
            print(f"\n  ERROR validating block {block.name}: {type(e).__name__}: {e}")
            print(f"  Traceback:\n{traceback.format_exc()}")
            print("  Continuing with next block...")

    if args.comparison_profile == NY_HP_ONLY_VS_ELECTRIFIED:
        print(f"\n  comparison profile: {args.comparison_profile}")
        comparison_results = run_ny_hp_only_vs_electrified_comparison(
            state=state,
            utility=utility,
            output_dir=output_dir,
            configs=configs,
            run_dirs=run_dirs,
        )
        all_results.extend(
            _annotate_result(result, block_name=args.comparison_profile)
            for result in comparison_results
        )

    by_status = {
        s: sum(1 for r in all_results if r.status == s)
        for s in ("PASS", "WARN", "FAIL")
    }
    print(f"\n{'=' * 60}\nVALIDATION SUMMARY\n{'=' * 60}")
    for status, count in by_status.items():
        print(f"  {status}: {count}")
    if by_status["FAIL"]:
        print("\nFailed checks:")
        for r in all_results:
            if r.status == "FAIL":
                print(f"  ✗ {_format_result_context(r)}{r.name}: {r.message}")
    print(f"\nResults saved to: {output_dir}")
    if by_status["FAIL"]:
        print("\n  WARNING: Some validation checks failed, but execution completed.")
        print("  Review the failed checks above and the output files for details.")


if __name__ == "__main__":
    main()

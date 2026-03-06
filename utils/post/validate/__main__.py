"""CLI entrypoint for CAIRO run validation.

Orchestrates validation blocks (runs 1-2, 3-4, 5-6, 7-8), runs checks, builds
summary tables, generates plots, and saves outputs to a structured directory.

Usage::

    uv run python -m utils.post.validate \\
        --state ny --utility coned \\
        [--timestamp 20260305T211404Z] \\
        [--runs 1,2,3,4,5,6,7,8] \\
        [--output-dir validation_outputs/coned]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import polars as pl

from utils.post.validate import (
    CheckResult,
    check_bat_direction,
    check_bat_near_zero,
    check_nonhp_calibrated_above_original,
    check_nonhp_customers_in_upgrade02,
    check_output_completeness,
    check_revenue_neutrality,
    check_subclass_revenue_neutrality,
    check_subclass_rr_sums_to_total,
    check_tariff_unchanged,
    check_weights_sum_to_n_customers,
    compute_bill_deltas,
    compute_hourly_cost_of_service,
    compute_weighted_loads_by_subclass,
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
from utils.post.validate.config import (
    RunBlock,
    define_run_blocks,
    load_run_configs_from_yaml,
)
from utils.post.validate.discover import find_latest_complete_batch, resolve_batch


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate CAIRO runs for a utility")
    p.add_argument(
        "--state", required=True, help="State abbreviation (e.g. 'ny', 'ri')"
    )
    p.add_argument(
        "--utility", required=True, help="Utility identifier (e.g. 'coned', 'rie')"
    )
    p.add_argument(
        "--timestamp",
        default=None,
        help="Batch name in {state}_{YYYYMMDD}_{letter} format (e.g. 'ny_20250115_a'); "
        "omit to use latest complete batch",
    )
    p.add_argument(
        "--runs", default="1,2,3,4,5,6,7,8", help="Comma-separated run numbers"
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
    return p.parse_args()


def _save(plot: Any, path: Path) -> None:
    """Save a plotnine plot to PNG, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    plot.save(str(path), dpi=150, width=10, height=7, verbose=False)


def _emit(result: CheckResult) -> CheckResult:
    """Print a check result and return it for chaining."""
    print(f"    {result.name}: {result.status} — {result.message}")
    return result


def _validate_block(
    block: RunBlock,
    run_dirs: dict[int, str],
    state: str,
    utility: str,
    output_dir: Path,
    *,
    skip_loads: bool = False,
    loads_lf: pl.LazyFrame | None = None,
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

    print(f"\n  {block.name}: {block.description}")
    block_dir = output_dir / block.name
    block_dir.mkdir(parents=True, exist_ok=True)
    plots = block_dir / "plots"
    results: list[CheckResult] = []

    metas = [load_metadata(d) for d in dirs]
    all_bills = [
        {
            "elec": load_bills(d, "elec"),
            "gas": load_bills(d, "gas"),
            "comb": load_bills(d, "comb"),
        }
        for d in dirs
    ]
    runs = list(zip(nums, dirs, block.configs, metas, all_bills))

    # --- Output completeness ---
    for run_num, s3_dir, *_ in runs:
        results.append(_emit(check_output_completeness(s3_dir)))

    # --- Bills summary + plots ---
    for run_num, _, _, meta, bills in runs:
        summary = summarize_bills_by_subclass(bills, meta)
        summary.write_csv(block_dir / f"bills_summary_run{run_num}.csv")
        _save(
            plot_avg_bills_by_subclass(
                summary, f"Annual Bills by Subclass — Run {run_num}"
            ),
            plots / "bills" / f"bills_by_subclass_run{run_num}.png",
        )

    # --- Revenue neutrality (precalc runs 1-2, 5-6) ---
    if block.revenue_neutral:
        has_sub = block.configs[0].has_subclasses
        total_rr = load_revenue_requirement(state, utility)
        subclass_rr = (
            load_revenue_requirement(state, utility, f"{utility}_hp_vs_nonhp.yaml")
            if has_sub
            else None
        )

        for run_num, _, config, meta, bills in runs:
            results.append(
                _emit(
                    check_revenue_neutrality(
                        bills["elec"], meta, total_rr, config.cost_scope
                    )
                )
            )

            if has_sub and subclass_rr is not None:
                results.append(
                    _emit(
                        check_subclass_revenue_neutrality(
                            bills["elec"], meta, subclass_rr, config.cost_scope
                        )
                    )
                )
                results.append(
                    _emit(
                        check_subclass_rr_sums_to_total(
                            subclass_rr, total_rr, config.cost_scope
                        )
                    )
                )

                sub_rrs = subclass_rr["subclass_revenue_requirements"]
                rr_key = (
                    "total" if config.cost_scope == "delivery+supply" else "delivery"
                )
                rr_vals = {
                    "HP": float(sub_rrs["hp"][rr_key]),
                    "Non-HP": float(sub_rrs["non-hp"][rr_key]),
                }
                total_rr_val = float(
                    total_rr[
                        "total_delivery_and_supply_revenue_requirement"
                        if config.cost_scope == "delivery+supply"
                        else "total_delivery_revenue_requirement"
                    ]
                )

                rev = summarize_revenue(bills["elec"], meta)
                rev.write_csv(block_dir / f"revenue_summary_run{run_num}.csv")
                _save(
                    plot_revenue_vs_rr(rev, rr_vals, f"Revenue vs RR — Run {run_num}"),
                    plots / "revenue_neutrality" / f"revenue_vs_rr_run{run_num}.png",
                )
                _save(
                    plot_subclass_rr_stacked(
                        rr_vals, total_rr_val, f"Subclass RR vs Total — Run {run_num}"
                    ),
                    plots
                    / "revenue_neutrality"
                    / f"subclass_rr_stacked_run{run_num}.png",
                )

    # --- BAT direction and magnitude (precalc runs 1-2, 5-6) ---
    if block.bat_relevant:
        for run_num, s3_dir, config, meta, _ in runs:
            bat = load_bat(s3_dir)
            results.append(_emit(check_bat_direction(bat, meta)))
            if config.has_subclasses:
                results.append(_emit(check_bat_near_zero(bat, meta)))
            bat_summary = summarize_bat_by_subclass(bat, meta)
            bat_summary.write_csv(block_dir / f"bat_summary_run{run_num}.csv")
            _save(
                plot_bat_by_subclass(bat_summary, f"Per-Customer BAT — Run {run_num}"),
                plots / "cross_subsidy" / f"bat_by_subclass_run{run_num}.png",
            )
            _save(
                plot_bat_heatmap(bat_summary, f"BAT Heatmap — Run {run_num}"),
                plots / "cross_subsidy" / f"bat_heatmap_run{run_num}.png",
            )

    # --- Tariff stability + bill deltas (default runs 3-4, 7-8) ---
    if block.tariff_should_be_unchanged:
        for run_num, run_dir, _, meta, bills in runs:
            prev_num = run_num - 2  # 3→1, 4→2, 7→5, 8→6
            if (prev_dir := run_dirs.get(prev_num)) is None:
                continue

            in_tariff, out_tariff = (
                load_tariff_config(prev_dir),
                load_tariff_config(run_dir),
            )
            in_rates, out_rates = (
                summarize_tariff_rates(in_tariff),
                summarize_tariff_rates(out_tariff),
            )
            results.append(_emit(check_tariff_unchanged(in_tariff, out_tariff)))
            in_rates.write_csv(block_dir / f"tariff_rates_input_run{prev_num}.csv")
            out_rates.write_csv(block_dir / f"tariff_rates_output_run{run_num}.csv")
            _save(
                plot_tariff_comparison(
                    in_rates, out_rates, f"Tariff: Run {prev_num} → {run_num}"
                ),
                plots / "tariff_diffs" / f"tariff_comparison_run{run_num}.png",
            )
            _save(
                plot_tariff_stability(
                    in_rates, out_rates, f"Tariff Diff — Run {run_num} vs {prev_num}"
                ),
                plots / "tariff_diffs" / f"tariff_stability_run{run_num}.png",
            )

            delta = compute_bill_deltas(
                load_bills(prev_dir, "elec"), bills["elec"], meta
            )
            delta.write_csv(block_dir / f"bill_deltas_run{run_num}.csv")
            _save(
                plot_bill_deltas(delta, f"Bill Change — Run {run_num} vs {prev_num}"),
                plots / f"bill_deltas_run{run_num}.png",
            )

    # --- Non-HP composition (upgrade 02 runs 3-4, 7-8) ---
    if block.configs[0].upgrade == "2":
        for run_num, _, _, meta, _ in runs:
            comp = summarize_nonhp_composition(meta)
            comp.write_csv(block_dir / f"nonhp_composition_run{run_num}.csv")
            results.append(_emit(check_nonhp_customers_in_upgrade02(meta)))
            _save(
                plot_nonhp_composition(comp, f"Non-HP Composition — Run {run_num}"),
                plots / f"nonhp_composition_run{run_num}.png",
            )

    # --- Non-HP rate calibrated above original (subclass precalc runs 5-6) ---
    if block.configs[0].has_subclasses and block.revenue_neutral:
        for run_num, run_dir, config, _, _ in runs:
            orig_num = 1 if config.cost_scope == "delivery" else 2
            if (orig_dir := run_dirs.get(orig_num)) is not None:
                results.append(
                    _emit(
                        check_nonhp_calibrated_above_original(
                            load_tariff_config(run_dir),
                            load_tariff_config(orig_dir),
                        )
                    )
                )

    # --- Hourly loads by subclass + cost-of-service plots (when not skip_loads) ---
    if not skip_loads and loads_lf is not None:
        for run_num, _, _, meta, _ in runs:
            meta_collected = meta.collect()
            loads_df = compute_weighted_loads_by_subclass(loads_lf, meta_collected)
            loads_df.write_csv(block_dir / f"loads_by_subclass_run{run_num}.csv")
            _save(
                plot_hourly_loads_by_subclass(
                    loads_df, f"Hourly Loads by Subclass — Run {run_num}"
                ),
                plots / "loads" / f"hourly_loads_run{run_num}.png",
            )

            # Three cost-of-service plots per run (if MC components available)
            if mc_components is not None:
                # Delivery: dist_sub_tx + bulk_tx
                mc_delivery = mc_components["dist_sub_tx"] + mc_components["bulk_tx"]
                cos_delivery = compute_hourly_cost_of_service(loads_df, mc_delivery)
                _save(
                    plot_hourly_cost_of_service(
                        cos_delivery,
                        f"Hourly Cost of Service (Delivery) — Run {run_num}",
                    ),
                    plots / "loads" / f"hourly_cos_delivery_run{run_num}.png",
                )

                # Supply: supply_energy + supply_capacity
                mc_supply = (
                    mc_components["supply_energy"] + mc_components["supply_capacity"]
                )
                cos_supply = compute_hourly_cost_of_service(loads_df, mc_supply)
                _save(
                    plot_hourly_cost_of_service(
                        cos_supply, f"Hourly Cost of Service (Supply) — Run {run_num}"
                    ),
                    plots / "loads" / f"hourly_cos_supply_run{run_num}.png",
                )

                # Combined: all four MC components
                mc_combined = (
                    mc_components["dist_sub_tx"]
                    + mc_components["bulk_tx"]
                    + mc_components["supply_energy"]
                    + mc_components["supply_capacity"]
                )
                cos_combined = compute_hourly_cost_of_service(loads_df, mc_combined)
                _save(
                    plot_hourly_cost_of_service(
                        cos_combined,
                        f"Hourly Cost of Service (Combined) — Run {run_num}",
                    ),
                    plots / "loads" / f"hourly_cos_combined_run{run_num}.png",
                )

    pl.DataFrame(
        [{"check": r.name, "status": r.status, "message": r.message} for r in results]
    ).write_csv(block_dir / "checks_summary.csv")
    return results


def main() -> None:
    args = _parse_args()
    state, utility = args.state.lower(), args.utility.lower()
    run_nums = sorted({int(n) for n in args.runs.split(",") if n.strip()})
    output_dir = Path(args.output_dir or f"validation_outputs/{utility}")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Validating runs {run_nums} for {state.upper()} / {utility}")
    print(f"Output: {output_dir}")

    configs = load_run_configs_from_yaml(state, utility, run_nums)
    run_names = {n: c.run_name for n, c in configs.items()}

    if args.timestamp:
        execution_time = args.timestamp
        run_dirs = resolve_batch(state, utility, execution_time, run_names)
    else:
        execution_time, run_dirs = find_latest_complete_batch(state, utility, run_names)
        print(f"  Batch: {execution_time}")

    if missing := sorted(set(run_nums) - run_dirs.keys()):
        print(f"  WARNING: Missing run dirs for runs: {missing}")

    # --- Preprocessing block (always) ---
    print("\n  preprocessing: Customer weight statistics and validation")
    preprocess_dir = output_dir / "preprocessing"
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_plots_dir = preprocess_dir / "plots"
    preprocess_plots_dir.mkdir(parents=True, exist_ok=True)

    # Load run-1 metadata for preprocessing (upgrade 00)
    if 1 in run_dirs:
        meta_run1 = load_metadata(run_dirs[1])

        # Summarize customer weight stats
        weight_stats = summarize_customer_weight_stats(meta_run1)
        weight_stats.write_csv(preprocess_dir / "customer_weight_stats.csv")

        # Check weights sum to N customers
        weight_check = check_weights_sum_to_n_customers(meta_run1)
        _emit(weight_check)
        pl.DataFrame(
            [
                {
                    "check": weight_check.name,
                    "status": weight_check.status,
                    "message": weight_check.message,
                }
            ]
        ).write_csv(preprocess_dir / "checks.csv")

        # Plot weighted customer counts (upgrade 00, once)
        counts = summarize_customer_counts(meta_run1)
        _save(
            plot_weighted_customer_counts(
                counts, "Weighted Customer Counts by Subclass (Upgrade 00)"
            ),
            preprocess_plots_dir / "customer_counts.png",
        )
    else:
        print("  WARNING: Run 1 not found, skipping preprocessing")

    # --- Load utility loads once (if not skip_loads) ---
    loads_lf: pl.LazyFrame | None = None
    if not args.skip_loads:
        if 1 in configs:
            run1_config = configs[1]
            # Extract upgrade-00 path from run1's path_resstock_loads
            # Path format: /ebs/data/.../load_curve_hourly/state=NY/upgrade=00/
            path_loads = run1_config.path_resstock_loads
            if path_loads:
                print(f"  Loading ResStock loads from: {path_loads}")
                loads_lf = scan_utility_loads(path_loads)
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
            mc_components = load_all_mc_components(run2_config)
        else:
            print("  WARNING: Run 2 config not found, skipping MC component load")

    # --- Validate blocks ---
    all_results: list[CheckResult] = []
    for block in define_run_blocks(configs):
        all_results.extend(
            _validate_block(
                block,
                run_dirs,
                state,
                utility,
                output_dir,
                skip_loads=args.skip_loads,
                loads_lf=loads_lf,
                mc_components=mc_components,
            )
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
                print(f"  ✗ {r.name}: {r.message}")
    print(f"\nResults saved to: {output_dir}")
    if by_status["FAIL"]:
        sys.exit(1)


if __name__ == "__main__":
    main()

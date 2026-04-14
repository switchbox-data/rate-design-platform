"""NY-specific comparison profiles for validation outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from utils.post.validate.checks import CheckResult, bat_col_for_allocation
from utils.post.validate.config import RunConfig
from utils.post.validate.load import (
    load_bat,
    load_bills,
    load_metadata,
    load_revenue_requirement,
    load_tariff_config,
)
from utils.post.validate.tables import (
    compute_bill_deltas,
    summarize_bat_by_subclass,
)
from utils.scenario_config import resolve_subclass_rr_for_validation

NY_HP_ONLY_VS_ELECTRIFIED = "ny_hp_only_vs_electrified"


@dataclass(frozen=True, slots=True)
class ComparisonFamily:
    """Run-family pairing for the NY HP-only vs electrified comparison."""

    name: str
    hp_only_runs: tuple[int, int, int, int]
    electrified_runs: tuple[int, int, int, int]


_FAMILIES: tuple[ComparisonFamily, ...] = (
    ComparisonFamily(
        name="flat_epmc",
        hp_only_runs=(17, 18, 19, 20),
        electrified_runs=(29, 30, 31, 32),
    ),
    ComparisonFamily(
        name="seasonal_epmc",
        hp_only_runs=(21, 22, 23, 24),
        electrified_runs=(33, 34, 35, 36),
    ),
)


def resolve_ny_hp_only_vs_electrified_pairs(
    configs: dict[int, RunConfig],
) -> list[ComparisonFamily]:
    """Return all NY comparison families present in ``configs``."""
    available = set(configs)
    return [
        family
        for family in _FAMILIES
        if set(family.hp_only_runs).issubset(available)
        and set(family.electrified_runs).issubset(available)
    ]


def _sign(value: float, *, tolerance: float = 1e-9) -> int:
    if abs(value) <= tolerance:
        return 0
    return 1 if value > 0 else -1


def _check_role_directionality(
    *,
    subject: str,
    metric_frame_hp: pl.DataFrame,
    metric_frame_elec: pl.DataFrame,
    hp_alias_map: dict[str, str],
    elec_alias_map: dict[str, str],
    metric_cols: list[str],
    run_nums: list[int],
) -> CheckResult:
    failures: list[str] = []
    warnings: list[str] = []
    details: list[dict[str, Any]] = []

    for role, hp_alias in hp_alias_map.items():
        elec_alias = elec_alias_map[role]
        hp_row = metric_frame_hp.filter(pl.col("subclass") == hp_alias)
        elec_row = metric_frame_elec.filter(pl.col("subclass") == elec_alias)
        if hp_row.is_empty() or elec_row.is_empty():
            failures.append(f"missing {role} subclass rows")
            continue
        hp_vals = hp_row.to_dicts()[0]
        elec_vals = elec_row.to_dicts()[0]

        for metric in metric_cols:
            hp_val = float(hp_vals[metric])
            elec_val = float(elec_vals[metric])
            hp_sign = _sign(hp_val)
            elec_sign = _sign(elec_val)
            details.append(
                {
                    "role": role,
                    "metric": metric,
                    "hp_only_value": hp_val,
                    "electrified_value": elec_val,
                    "hp_only_sign": hp_sign,
                    "electrified_sign": elec_sign,
                }
            )
            if hp_sign == elec_sign:
                continue
            if hp_sign == 0 or elec_sign == 0:
                warnings.append(
                    f"{role} {metric} touches zero ({hp_val:+.3f} vs {elec_val:+.3f})"
                )
            else:
                failures.append(
                    f"{role} {metric} flips sign ({hp_val:+.3f} vs {elec_val:+.3f})"
                )

    status = "PASS"
    if failures:
        status = "FAIL"
    elif warnings:
        status = "WARN"
    message_parts = failures or warnings or ["direction preserved"]
    return CheckResult(
        name=f"{subject}_directionality",
        status=status,
        message=f"{subject} directionality — " + "; ".join(message_parts),
        details={"comparisons": details, "run_nums": run_nums},
    )


def _check_gap_directionality(
    *,
    subject: str,
    metric_frame_hp: pl.DataFrame,
    metric_frame_elec: pl.DataFrame,
    hp_focus: str,
    hp_other: str,
    elec_focus: str,
    elec_other: str,
    metric_cols: list[str],
    run_nums: list[int],
) -> CheckResult:
    failures: list[str] = []
    warnings: list[str] = []
    details: list[dict[str, Any]] = []

    hp_focus_row = metric_frame_hp.filter(pl.col("subclass") == hp_focus)
    hp_other_row = metric_frame_hp.filter(pl.col("subclass") == hp_other)
    elec_focus_row = metric_frame_elec.filter(pl.col("subclass") == elec_focus)
    elec_other_row = metric_frame_elec.filter(pl.col("subclass") == elec_other)
    if (
        hp_focus_row.is_empty()
        or hp_other_row.is_empty()
        or elec_focus_row.is_empty()
        or elec_other_row.is_empty()
    ):
        return CheckResult(
            name=f"{subject}_gap_directionality",
            status="FAIL",
            message=f"{subject} gap directionality — missing subclass rows",
            details={"run_nums": run_nums},
        )

    hp_focus_vals = hp_focus_row.to_dicts()[0]
    hp_other_vals = hp_other_row.to_dicts()[0]
    elec_focus_vals = elec_focus_row.to_dicts()[0]
    elec_other_vals = elec_other_row.to_dicts()[0]

    for metric in metric_cols:
        hp_gap = float(hp_focus_vals[metric]) - float(hp_other_vals[metric])
        elec_gap = float(elec_focus_vals[metric]) - float(elec_other_vals[metric])
        hp_sign = _sign(hp_gap)
        elec_sign = _sign(elec_gap)
        details.append(
            {
                "metric": metric,
                "hp_only_gap": hp_gap,
                "electrified_gap": elec_gap,
                "hp_only_sign": hp_sign,
                "electrified_sign": elec_sign,
            }
        )
        if hp_sign == elec_sign:
            continue
        if hp_sign == 0 or elec_sign == 0:
            warnings.append(
                f"{metric} gap touches zero ({hp_gap:+.3f} vs {elec_gap:+.3f})"
            )
        else:
            failures.append(
                f"{metric} gap flips sign ({hp_gap:+.3f} vs {elec_gap:+.3f})"
            )

    status = "PASS"
    if failures:
        status = "FAIL"
    elif warnings:
        status = "WARN"
    message_parts = failures or warnings or ["ordering preserved"]
    return CheckResult(
        name=f"{subject}_gap_directionality",
        status=status,
        message=f"{subject} gap directionality — " + "; ".join(message_parts),
        details={"comparisons": details, "run_nums": run_nums},
    )


def _subclass_rr_summary(
    state: str,
    utility: str,
    config: RunConfig,
) -> pl.DataFrame:
    if config.revenue_requirement_filename is None:
        raise ValueError(f"run {config.run_num} has no revenue requirement filename")

    rr_raw = load_revenue_requirement(
        state,
        utility,
        config.revenue_requirement_filename,
    )
    resolved = resolve_subclass_rr_for_validation(
        rr_raw,
        config.cost_scope,
        residual_allocation_delivery=config.residual_allocation_delivery
        or "percustomer",
        residual_allocation_supply=config.residual_allocation_supply or "passthrough",
    )
    key = "total" if config.cost_scope == "delivery+supply" else "delivery"
    return pl.DataFrame(
        [
            {
                "run_num": config.run_num,
                "cost_scope": config.cost_scope,
                "subclass": alias,
                "rr_value": float(values[key]),
            }
            for alias, values in resolved.items()
        ]
    )


def _tariff_key_for_alias(config: RunConfig, alias: str) -> str | None:
    key = config.tariff_keys_by_alias.get(alias)
    if key is not None:
        return key
    if alias == "all":
        return config.tariff_keys_by_alias.get("all")
    return None


def _summarize_tariff_run(
    config: RunConfig,
    tariff_config: dict[str, Any],
    *,
    aliases: tuple[str, ...],
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for alias in aliases:
        tariff_key = _tariff_key_for_alias(config, alias)
        if tariff_key is None:
            continue
        entry = tariff_config.get(tariff_key) or tariff_config.get(
            f"{tariff_key}_calibrated"
        )
        if entry is None:
            continue
        tou_rows = entry.get("ur_ec_tou_mat", [])
        rates = [float(row[4]) for row in tou_rows]
        rows.append(
            {
                "run_num": config.run_num,
                "cost_scope": config.cost_scope,
                "subclass": alias,
                "mean_rate_per_kwh": sum(rates) / len(rates) if rates else float("nan"),
                "max_rate_per_kwh": max(rates) if rates else float("nan"),
                "fixed_charge_per_month": float(
                    entry.get("ur_monthly_fixed_charge", 0.0)
                ),
            }
        )
    return pl.DataFrame(rows)


def run_ny_hp_only_vs_electrified_comparison(
    *,
    state: str,
    utility: str,
    output_dir: Path,
    configs: dict[int, RunConfig],
    run_dirs: dict[int, str],
) -> list[CheckResult]:
    """Run the NY EPMC comparison profile and write side-by-side CSVs."""
    if state != "ny":
        raise ValueError("ny_hp_only_vs_electrified is only supported for NY")

    profile_dir = output_dir / "comparisons" / NY_HP_ONLY_VS_ELECTRIFIED
    profile_dir.mkdir(parents=True, exist_ok=True)
    results: list[CheckResult] = []

    role_hp = {"focus": "hp", "other": "non-hp"}
    role_elec = {"focus": "electric_heating", "other": "non_electric_heating"}

    families = resolve_ny_hp_only_vs_electrified_pairs(configs)
    for family in families:
        family_dir = profile_dir / family.name
        family_dir.mkdir(parents=True, exist_ok=True)

        (
            hp_precalc_delivery,
            hp_precalc_supply,
            hp_default_delivery,
            hp_default_supply,
        ) = family.hp_only_runs
        (
            elec_precalc_delivery,
            elec_precalc_supply,
            elec_default_delivery,
            elec_default_supply,
        ) = family.electrified_runs

        hp_spec = configs[hp_precalc_delivery].subclass_spec
        elec_spec = configs[elec_precalc_delivery].subclass_spec
        if hp_spec is None or elec_spec is None:
            results.append(
                CheckResult(
                    name=f"{family.name}_comparison_ready",
                    status="FAIL",
                    message="Missing subclass metadata for comparison family",
                    details={"family": family.name},
                )
            )
            continue

        bat_frames: list[pl.DataFrame] = []
        rr_frames: list[pl.DataFrame] = []
        tariff_frames: list[pl.DataFrame] = []
        bill_delta_frames: list[pl.DataFrame] = []

        precalc_pairs = (
            (
                "delivery",
                hp_precalc_delivery,
                elec_precalc_delivery,
                hp_spec,
                elec_spec,
            ),
            (
                "delivery+supply",
                hp_precalc_supply,
                elec_precalc_supply,
                hp_spec,
                elec_spec,
            ),
        )
        for cost_scope, hp_run, elec_run, hp_run_spec, elec_run_spec in precalc_pairs:
            hp_bat = summarize_bat_by_subclass(
                load_bat(run_dirs[hp_run]),
                load_metadata(run_dirs[hp_run]),
                hp_run_spec,
            ).with_columns(
                pl.lit("hp_only").alias("scenario_family"),
                pl.lit(cost_scope).alias("cost_scope"),
            )
            elec_bat = summarize_bat_by_subclass(
                load_bat(run_dirs[elec_run]),
                load_metadata(run_dirs[elec_run]),
                elec_run_spec,
            ).with_columns(
                pl.lit("electrified").alias("scenario_family"),
                pl.lit(cost_scope).alias("cost_scope"),
            )
            bat_frames.extend([hp_bat, elec_bat])

            # Use the operative BAT metric for the allocation method.
            operative_bat = bat_col_for_allocation(
                configs[hp_run].residual_allocation_delivery
            )
            operative_wavg = f"{operative_bat}_wavg"
            bat_metric_candidates = (operative_wavg, "BAT_vol_wavg", "BAT_peak_wavg")
            bat_metrics = [
                m
                for m in bat_metric_candidates
                if m in hp_bat.columns and m in elec_bat.columns
            ]

            results.append(
                _check_role_directionality(
                    subject=f"{family.name}_{cost_scope}_bat",
                    metric_frame_hp=hp_bat,
                    metric_frame_elec=elec_bat,
                    hp_alias_map=role_hp,
                    elec_alias_map=role_elec,
                    metric_cols=bat_metrics,
                    run_nums=[hp_run, elec_run],
                )
            )
            results.append(
                _check_gap_directionality(
                    subject=f"{family.name}_{cost_scope}_bat",
                    metric_frame_hp=hp_bat,
                    metric_frame_elec=elec_bat,
                    hp_focus=role_hp["focus"],
                    hp_other=role_hp["other"],
                    elec_focus=role_elec["focus"],
                    elec_other=role_elec["other"],
                    metric_cols=bat_metrics,
                    run_nums=[hp_run, elec_run],
                )
            )

            hp_rr = _subclass_rr_summary(state, utility, configs[hp_run]).with_columns(
                pl.lit("hp_only").alias("scenario_family")
            )
            elec_rr = _subclass_rr_summary(
                state, utility, configs[elec_run]
            ).with_columns(pl.lit("electrified").alias("scenario_family"))
            rr_frames.extend([hp_rr, elec_rr])
            results.append(
                _check_gap_directionality(
                    subject=f"{family.name}_{cost_scope}_revenue_requirement",
                    metric_frame_hp=hp_rr,
                    metric_frame_elec=elec_rr,
                    hp_focus=role_hp["focus"],
                    hp_other=role_hp["other"],
                    elec_focus=role_elec["focus"],
                    elec_other=role_elec["other"],
                    metric_cols=["rr_value"],
                    run_nums=[hp_run, elec_run],
                )
            )

            hp_tariffs = _summarize_tariff_run(
                configs[hp_run],
                load_tariff_config(run_dirs[hp_run]),
                aliases=(role_hp["focus"], role_hp["other"]),
            ).with_columns(
                pl.lit("precalc").alias("stage"),
                pl.lit("hp_only").alias("scenario_family"),
            )
            elec_tariffs = _summarize_tariff_run(
                configs[elec_run],
                load_tariff_config(run_dirs[elec_run]),
                aliases=(role_elec["focus"], role_elec["other"]),
            ).with_columns(
                pl.lit("precalc").alias("stage"),
                pl.lit("electrified").alias("scenario_family"),
            )
            tariff_frames.extend([hp_tariffs, elec_tariffs])
            results.append(
                _check_gap_directionality(
                    subject=f"{family.name}_{cost_scope}_tariff",
                    metric_frame_hp=hp_tariffs,
                    metric_frame_elec=elec_tariffs,
                    hp_focus=role_hp["focus"],
                    hp_other=role_hp["other"],
                    elec_focus=role_elec["focus"],
                    elec_other=role_elec["other"],
                    metric_cols=[
                        "mean_rate_per_kwh",
                        "max_rate_per_kwh",
                        "fixed_charge_per_month",
                    ],
                    run_nums=[hp_run, elec_run],
                )
            )

        default_pairs = (
            ("delivery", hp_precalc_delivery, hp_default_delivery, hp_spec, "hp_only"),
            (
                "delivery+supply",
                hp_precalc_supply,
                hp_default_supply,
                hp_spec,
                "hp_only",
            ),
            (
                "delivery",
                elec_precalc_delivery,
                elec_default_delivery,
                elec_spec,
                "electrified",
            ),
            (
                "delivery+supply",
                elec_precalc_supply,
                elec_default_supply,
                elec_spec,
                "electrified",
            ),
        )
        delta_by_family: dict[str, dict[str, pl.DataFrame]] = {
            "hp_only": {},
            "electrified": {},
        }
        for cost_scope, base_run, default_run, spec, scenario_family in default_pairs:
            delta = compute_bill_deltas(
                load_bills(run_dirs[base_run], "elec"),
                load_bills(run_dirs[default_run], "elec"),
                load_metadata(run_dirs[default_run]),
                spec,
            ).with_columns(
                pl.lit(cost_scope).alias("cost_scope"),
                pl.lit(scenario_family).alias("scenario_family"),
                pl.lit(default_run).alias("run_num"),
            )
            bill_delta_frames.append(delta)
            delta_by_family[scenario_family][cost_scope] = delta

            default_tariff = _summarize_tariff_run(
                configs[default_run],
                load_tariff_config(run_dirs[default_run]),
                aliases=("all",),
            ).with_columns(
                pl.lit("default").alias("stage"),
                pl.lit(scenario_family).alias("scenario_family"),
            )
            tariff_frames.append(default_tariff)

        for cost_scope in ("delivery", "delivery+supply"):
            hp_delta = delta_by_family["hp_only"][cost_scope]
            elec_delta = delta_by_family["electrified"][cost_scope]
            results.append(
                _check_role_directionality(
                    subject=f"{family.name}_{cost_scope}_bill_delta",
                    metric_frame_hp=hp_delta,
                    metric_frame_elec=elec_delta,
                    hp_alias_map=role_hp,
                    elec_alias_map=role_elec,
                    metric_cols=["bill_delta"],
                    run_nums=[
                        hp_delta["run_num"][0],
                        elec_delta["run_num"][0],
                    ],
                )
            )
            results.append(
                _check_gap_directionality(
                    subject=f"{family.name}_{cost_scope}_bill_delta",
                    metric_frame_hp=hp_delta,
                    metric_frame_elec=elec_delta,
                    hp_focus=role_hp["focus"],
                    hp_other=role_hp["other"],
                    elec_focus=role_elec["focus"],
                    elec_other=role_elec["other"],
                    metric_cols=["bill_delta"],
                    run_nums=[
                        hp_delta["run_num"][0],
                        elec_delta["run_num"][0],
                    ],
                )
            )

        if bat_frames:
            pl.concat(bat_frames, how="diagonal").write_csv(
                family_dir / "bat_comparison.csv"
            )
        if rr_frames:
            pl.concat(rr_frames, how="diagonal").write_csv(
                family_dir / "revenue_requirement_comparison.csv"
            )
        if tariff_frames:
            pl.concat(tariff_frames, how="diagonal").write_csv(
                family_dir / "tariff_comparison.csv"
            )
        if bill_delta_frames:
            pl.concat(bill_delta_frames, how="diagonal").write_csv(
                family_dir / "bill_delta_comparison.csv"
            )

    if results:
        pl.DataFrame(
            [
                {
                    "check": result.name,
                    "status": result.status,
                    "message": result.message,
                }
                for result in results
            ]
        ).write_csv(profile_dir / "checks_summary.csv")

    return results

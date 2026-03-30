#!/usr/bin/env python3
"""Compute topped-up delivery and supply revenue requirements and write rev_requirement/<utility>.yaml.

Reads monthly_rates YAML (per charge, with decision), and the existing rate-case delivery
revenue requirement; filters charges by decision (add_to_drr, add_to_srr); computes
total_budget per charge; writes the new schema with delivery and supply top-ups and
derived totals.

Two modes for volumetric ($/kWh) charges:

  **EIA mode** (default): day-weighted avg rate × EIA-861 total residential kWh.
  **ResStock mode** (``--use-resstock-loads``): monthly rate × monthly utility-level kWh
  from ResStock, scaled to EIA-861 customer count.  Captures the covariance between
  monthly rate variation and monthly load variation.

Fixed charges ($/day, $/month) always use EIA-861 customer count in both modes.

Every charge carries a ``charge_unit``: ``$/kWh`` (volumetric), ``$/day`` or ``$/month``
(fixed per-customer), or ``%`` (percentage-of-bill; skipped here, handled elsewhere).
"""

from __future__ import annotations

import argparse
import calendar
import re
from collections import defaultdict
from pathlib import Path

import logging

import yaml

from utils.scenario_config import (
    get_residential_customer_count_from_utility_stats,
    get_residential_sales_kwh_from_utility_stats,
)


def _parse_month(month_key: str) -> tuple[int, int]:
    """Parse 'YYYY-MM' -> (year, month)."""
    y_str, m_str = month_key.split("-")
    return int(y_str), int(m_str)


def _months_in_range(start_month: str, end_month: str) -> list[tuple[int, int]]:
    """Return list of (year, month) from start_month through end_month inclusive."""
    y_start, m_start = _parse_month(start_month)
    y_end, m_end = _parse_month(end_month)
    out: list[tuple[int, int]] = []
    y, m = y_start, m_start
    while (y, m) <= (y_end, m_end):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _day_weighted_avg_rate(
    monthly_rates: dict[str, float], month_list: list[tuple[int, int]]
) -> float:
    """Compute sum(rate_m * days_m) / total_days for the given months."""
    if not month_list:
        return 0.0
    weighted_sum = 0.0
    total_days = 0
    for y, m in month_list:
        month_key = f"{y:04d}-{m:02d}"
        rate = monthly_rates.get(month_key, 0.0)
        days = calendar.monthrange(y, m)[1]
        weighted_sum += rate * days
        total_days += days
    return weighted_sum / total_days if total_days else 0.0


def _fixed_charge_annual_budget(
    monthly_rates: dict[str, float],
    month_list: list[tuple[int, int]],
    charge_unit: str,
    customer_count: int,
) -> float:
    """Compute annual revenue from a fixed per-customer charge.

    For $/day charges, each month contributes rate * days_in_month * customer_count.
    For $/month charges, each month contributes rate * customer_count.
    """
    total = 0.0
    for y, m in month_list:
        month_key = f"{y:04d}-{m:02d}"
        rate = monthly_rates.get(month_key, 0.0)
        if charge_unit == "$/day":
            days = calendar.monthrange(y, m)[1]
            total += rate * days
        elif charge_unit == "$/month":
            total += rate
        else:
            logging.warning("Unknown charge_unit %r; treating as $/month", charge_unit)
            total += rate
    return total * customer_count


ELEC_CONSUMPTION_COL = "out.electricity.total.energy_consumption"
RESSTOCK_UPGRADE = "00"


def _compute_monthly_kwh_from_resstock(
    path_resstock_release: str,
    state: str,
    utility: str,
    customer_count: int,
) -> tuple[dict[int, float], float]:
    """Load ResStock monthly loads and return utility-level kWh per month, scaled to EIA customer count.

    Uses the "small data, collect once" strategy: builds a single lazy pipeline
    (scan loads → join weights → group by month), collects once into a 12-row
    DataFrame, then validates eagerly.

    Returns (monthly_kwh dict {1: ..., 12: ...}, scale_factor).
    """
    from typing import cast

    import polars as pl

    from utils.post.io import scan_load_curves_for_utility

    base = path_resstock_release.rstrip("/")
    meta_path = f"{base}/metadata_utility/state={state}/utility_assignment.parquet"

    weights_df = cast(
        pl.DataFrame,
        pl.scan_parquet(meta_path)
        .filter(pl.col("sb.electric_utility") == utility)
        .select("bldg_id", "weight")
        .collect(),
    )
    if weights_df.is_empty():
        raise ValueError(f"No buildings for utility {utility!r} in {meta_path}.")
    weight_sum = float(weights_df["weight"].sum())
    if weight_sum <= 0:
        raise ValueError(
            f"ResStock weight sum is {weight_sum} for utility {utility!r}; "
            "cannot scale to customer count."
        )
    scale_factor = customer_count / weight_sum

    loads_lf = scan_load_curves_for_utility(
        path_resstock_release=path_resstock_release,
        state=state,
        upgrade=RESSTOCK_UPGRADE,
        utility=utility,
        load_curve_type="monthly",
    )

    monthly_df = cast(
        pl.DataFrame,
        loads_lf.join(weights_df.lazy(), on="bldg_id", how="inner")
        .group_by("month")
        .agg(
            (pl.col(ELEC_CONSUMPTION_COL) * pl.col("weight"))
            .sum()
            .alias("weighted_kwh")
        )
        .sort("month")
        .collect(),
    )

    if monthly_df.height != 12:
        raise ValueError(
            f"Expected 12 months from ResStock, got {monthly_df.height}. "
            f"Months present: {sorted(monthly_df['month'].to_list())}"
        )
    if monthly_df["weighted_kwh"].null_count() > 0:
        raise ValueError("Null weighted kWh values in ResStock monthly aggregation.")

    monthly_kwh: dict[int, float] = {}
    for row in monthly_df.iter_rows(named=True):
        monthly_kwh[int(row["month"])] = float(row["weighted_kwh"]) * scale_factor

    return monthly_kwh, scale_factor


def _resstock_monthly_budget(
    monthly_rates: dict[str, float],
    month_list: list[tuple[int, int]],
    monthly_kwh: dict[int, float],
) -> float:
    """Compute sum(rate_m * utility_kwh_m) for each month in the range."""
    total = 0.0
    for y, m in month_list:
        month_key = f"{y:04d}-{m:02d}"
        rate = monthly_rates.get(month_key, 0.0)
        kwh = monthly_kwh.get(m, 0.0)
        total += rate * kwh
    return total


def _resolve_customer_count_with_year_fallback(
    path_template: str,
    year: int,
    utility: str,
    storage_options: dict[str, str] | None,
) -> tuple[int, int]:
    """Try path for year, then year-1, ...; return (customer_count, actual_year_used)."""
    pattern = re.compile(r"year=\d{4}")
    match = pattern.search(path_template)
    if not match:
        count = get_residential_customer_count_from_utility_stats(
            path_template, utility, storage_options=storage_options
        )
        return count, year

    for offset in range(0, 21):
        try_year = year - offset
        path = pattern.sub(f"year={try_year}", path_template, count=1)
        try:
            count = get_residential_customer_count_from_utility_stats(
                path, utility, storage_options=storage_options
            )
            if offset > 0:
                logging.warning(
                    "EIA-861 customer count for year %s not found; used year %s.",
                    year,
                    try_year,
                )
            return count, try_year
        except (OSError, FileNotFoundError):
            continue
    raise FileNotFoundError(
        f"Could not read EIA-861 customer count for utility={utility} "
        f"for year {year} or prior 20 years."
    )


def _resolve_path_eia_with_year_fallback(
    path_template: str,
    year: int,
    utility: str,
    storage_options: dict[str, str] | None,
) -> tuple[float, int]:
    """Try path for year, then year-1, ...; return (kwh, actual_year_used)."""
    pattern = re.compile(r"year=\d{4}")
    match = pattern.search(path_template)
    if not match:
        kwh = get_residential_sales_kwh_from_utility_stats(
            path_template, utility, storage_options=storage_options
        )
        return kwh, year

    for offset in range(0, 21):
        try_year = year - offset
        path = pattern.sub(f"year={try_year}", path_template, count=1)
        try:
            kwh = get_residential_sales_kwh_from_utility_stats(
                path, utility, storage_options=storage_options
            )
            if offset > 0:
                logging.warning(
                    "EIA-861 data for year %s not found; used year %s (path %s).",
                    year,
                    try_year,
                    path,
                )
            return kwh, try_year
        except (OSError, FileNotFoundError):
            continue
    raise FileNotFoundError(
        f"Could not read EIA-861 for utility={utility} for year {year} or prior 20 years."
    )


def _warn_potential_zonal_duplicates(charges: dict[str, dict], utility: str) -> None:
    """Warn when multiple active charges share the same master_charge and decision.

    This catches zone-specific variants (e.g. ConEd supply commodity for Zones H/I/J)
    that should not all be multiplied by total utility kWh.  Distinct additive
    subcomponents (e.g. CenHud MFC allocation/base/admin) also trigger the warning;
    the operator should verify and suppress by excluding the duplicates in
    charge_decisions.json.
    """
    groups: dict[tuple[str, str], list[tuple[str, dict]]] = defaultdict(list)
    for slug, data in charges.items():
        decision = data.get("decision", "")
        if decision not in ("add_to_drr", "add_to_srr"):
            continue
        master = data.get("master_charge", slug)
        groups[(master, decision)].append((slug, data))

    for (master, decision), entries in groups.items():
        if len(entries) <= 1:
            continue
        slugs_info = []
        for slug, data in entries:
            vrk = data.get("variableRateKey", "")
            rate_name = data.get("rate_name", "")
            label = vrk or rate_name or "(no key)"
            slugs_info.append(f"  {slug} ({label})")
        logging.warning(
            "utility=%s has %d charges with master_charge=%r and decision=%r:\n%s\n"
            "  → If these are zone-specific variants, only one should be active; "
            "exclude the rest in charge_decisions.",
            utility,
            len(entries),
            master,
            decision,
            "\n".join(slugs_info),
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute topped-up delivery and supply revenue requirements from EIA kWh and monthly charge rates."
    )
    parser.add_argument(
        "--utility", required=True, help="Utility shortcode (e.g. coned, rie)"
    )
    parser.add_argument(
        "--path-monthly-rates",
        type=Path,
        required=True,
        help="Path to <utility>_monthly_rates.yaml from fetch_monthly_rates.py",
    )
    parser.add_argument(
        "--path-electric-utility-stats",
        type=str,
        required=True,
        help="Path to EIA-861 parquet (may contain year=YYYY for fallback)",
    )
    parser.add_argument(
        "--path-rate-case-rr",
        type=Path,
        required=True,
        help="Path to rate_case_delivery_rr.yaml (utility_code: amount mapping)",
    )
    parser.add_argument(
        "--path-supply-base",
        type=Path,
        default=None,
        help=(
            "Optional YAML mapping utility -> supply base ($). "
            "If provided and the file has an entry for the current utility, "
            "the value is added to supply top-ups. Use for utilities with TOU "
            "supply rates that can't be summed directly."
        ),
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="Output rev_requirement YAML path"
    )
    parser.add_argument(
        "--use-resstock-loads",
        action="store_true",
        default=False,
        help=(
            "Use ResStock monthly loads × monthly rates for $/kWh charges "
            "instead of day-weighted avg rate × EIA-861 annual kWh."
        ),
    )
    parser.add_argument(
        "--path-resstock-release",
        type=str,
        default=None,
        help=(
            "Root of the ResStock release (local or s3://). "
            "Required when --use-resstock-loads is set."
        ),
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help=(
            "Two-letter state code (uppercase, e.g. RI). "
            "Required when --use-resstock-loads is set."
        ),
    )
    args = parser.parse_args()

    if args.use_resstock_loads:
        if not args.path_resstock_release:
            parser.error(
                "--path-resstock-release is required when --use-resstock-loads is set."
            )
        if not args.state:
            parser.error("--state is required when --use-resstock-loads is set.")

    utility = args.utility.lower()

    storage_options = None
    if args.path_electric_utility_stats.startswith("s3://"):
        try:
            from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

            storage_options = get_aws_storage_options()
        except ImportError:
            pass

    with open(args.path_monthly_rates) as f:
        monthly_rates_data = yaml.safe_load(f)
    if not isinstance(monthly_rates_data, dict):
        raise SystemExit(f"{args.path_monthly_rates} must be a YAML object.")
    start_month = monthly_rates_data.get("start_month")
    end_month = monthly_rates_data.get("end_month")
    if not start_month or not end_month:
        raise SystemExit(
            f"{args.path_monthly_rates} must have start_month and end_month."
        )
    # Read charges from the decision-grouped YAML format.
    # Each decision section has {rate_structure, charges}.  We only process
    # flat sections; non-flat sections (seasonal_tiered, seasonal_tou) are
    # skipped with a warning — use supply_base_overrides for those utilities.
    charges: dict[str, dict] = {}
    for decision_key in ("add_to_drr", "add_to_srr"):
        section = monthly_rates_data.get(decision_key) or {}
        if isinstance(section, dict) and "rate_structure" in section:
            rs = section.get("rate_structure", "flat")
            section_charges = section.get("charges") or {}
            if rs != "flat":
                logging.warning(
                    "Skipping %d %s charge(s) for %s: rate_structure is %r, "
                    "not 'flat'. Use supply_base_overrides or equivalent for "
                    "this utility's %s revenue.",
                    len(section_charges),
                    decision_key,
                    utility,
                    rs,
                    decision_key,
                )
                continue
            for slug, data in section_charges.items():
                charges[slug] = {**data, "decision": decision_key}
        else:
            # Backwards compatibility: old format with decision field per charge
            for slug, data in section.items():
                if isinstance(data, dict):
                    charges[slug] = data

    year_for_eia = _parse_month(start_month)[0]

    eia_total_residential_kwh, eia_year = _resolve_path_eia_with_year_fallback(
        args.path_electric_utility_stats,
        year_for_eia,
        utility,
        storage_options,
    )

    resstock_monthly_kwh: dict[int, float] | None = None
    resstock_scale_factor: float | None = None

    if args.use_resstock_loads:
        residential_customer_count_for_scaling, _ = (
            _resolve_customer_count_with_year_fallback(
                args.path_electric_utility_stats,
                year_for_eia,
                utility,
                storage_options,
            )
        )
        resstock_monthly_kwh, resstock_scale_factor = (
            _compute_monthly_kwh_from_resstock(
                path_resstock_release=args.path_resstock_release,
                state=args.state,
                utility=utility,
                customer_count=residential_customer_count_for_scaling,
            )
        )
        total_residential_kwh = sum(resstock_monthly_kwh.values())
        logging.info(
            "ResStock total kWh: %s (scale factor %.4f) vs EIA %s",
            f"{total_residential_kwh:,.0f}",
            resstock_scale_factor,
            f"{eia_total_residential_kwh:,.0f}",
        )
    else:
        total_residential_kwh = eia_total_residential_kwh

    with open(args.path_rate_case_rr) as f:
        rate_case_data = yaml.safe_load(f)
    if not isinstance(rate_case_data, dict):
        raise SystemExit(f"{args.path_rate_case_rr} must be a YAML mapping.")
    if utility not in rate_case_data:
        raise SystemExit(
            f"No entry for utility {utility!r} in {args.path_rate_case_rr}. "
            f"Available: {[k for k in rate_case_data if not str(k).startswith('#')]}"
        )
    delivery_revenue_requirement_from_rate_case = float(rate_case_data[utility])

    month_list = _months_in_range(start_month, end_month)

    delivery_top_ups: dict[str, dict] = {}
    supply_top_ups: dict[str, dict] = {}
    delivery_budgets_sum = 0.0
    supply_budgets_sum = 0.0
    residential_customer_count: int | None = None

    for slug, data in charges.items():
        decision = data.get("decision")
        if not decision:
            continue
        monthly = data.get("monthly_rates") or {}
        if not monthly:
            continue

        charge_unit = data.get("charge_unit", "$/kWh")
        if charge_unit in ("$/day", "$/month"):
            if residential_customer_count is None:
                residential_customer_count, _ = (
                    _resolve_customer_count_with_year_fallback(
                        args.path_electric_utility_stats,
                        year_for_eia,
                        utility,
                        storage_options,
                    )
                )
            total_budget = _fixed_charge_annual_budget(
                monthly, month_list, charge_unit, residential_customer_count
            )
            entry: dict = {
                "charge_unit": charge_unit,
                "customer_count": residential_customer_count,
                "total_budget": round(total_budget, 2),
            }
        elif charge_unit == "$/kWh":
            if resstock_monthly_kwh is not None:
                total_budget = _resstock_monthly_budget(
                    monthly, month_list, resstock_monthly_kwh
                )
                entry = {
                    "charge_unit": charge_unit,
                    "budget_method": "resstock",
                    "total_budget": round(total_budget, 2),
                }
            else:
                avg_rate = _day_weighted_avg_rate(monthly, month_list)
                total_budget = avg_rate * total_residential_kwh
                entry = {
                    "charge_unit": charge_unit,
                    "budget_method": "eia",
                    "avg_monthly_rate": round(avg_rate, 10),
                    "total_budget": round(total_budget, 2),
                }
        else:
            logging.warning(
                "Skipping %s: unsupported charge_unit %r", slug, charge_unit
            )
            continue

        if decision == "add_to_drr":
            delivery_top_ups[slug] = entry
            delivery_budgets_sum += total_budget
        else:
            supply_top_ups[slug] = entry
            supply_budgets_sum += total_budget

    # Optional supply base override (for utilities with TOU supply rates).
    # Expressed as an entry in supply_top_ups so the output shape is consistent
    # across all utilities.
    if args.path_supply_base and args.path_supply_base.exists():
        with open(args.path_supply_base) as f:
            supply_base_data = yaml.safe_load(f)
        if isinstance(supply_base_data, dict) and utility in supply_base_data:
            supply_base_value = float(supply_base_data[utility])
            supply_top_ups["supply_base_from_budget"] = {
                "source": str(args.path_supply_base.name),
                "total_budget": round(supply_base_value, 2),
            }
            supply_budgets_sum += supply_base_value
            logging.info(
                "Added supply base from budget for %s: $%s",
                utility,
                f"{supply_base_value:,.0f}",
            )

    delivery_revenue_requirement_topups = round(delivery_budgets_sum, 2)
    supply_revenue_requirement_topups = round(supply_budgets_sum, 2)
    total_delivery_revenue_requirement = round(
        delivery_revenue_requirement_from_rate_case
        + delivery_revenue_requirement_topups,
        2,
    )
    total_delivery_and_supply_revenue_requirement = round(
        total_delivery_revenue_requirement + supply_revenue_requirement_topups, 2
    )

    load_method = "resstock" if args.use_resstock_loads else "eia"

    out: dict[str, object] = {
        "utility": utility,
        "load_method": load_method,
        "delivery_revenue_requirement_from_rate_case": delivery_revenue_requirement_from_rate_case,
        "delivery_revenue_requirement_topups": delivery_revenue_requirement_topups,
        "supply_revenue_requirement_topups": supply_revenue_requirement_topups,
        "total_delivery_revenue_requirement": total_delivery_revenue_requirement,
        "total_delivery_and_supply_revenue_requirement": total_delivery_and_supply_revenue_requirement,
        "total_residential_kwh": round(total_residential_kwh, 2),
        "eia_total_residential_kwh": round(eia_total_residential_kwh, 2),
        "eia_year": eia_year,
        "delivery_top_ups": delivery_top_ups,
        "supply_top_ups": supply_top_ups,
    }

    if args.use_resstock_loads and resstock_monthly_kwh is not None:
        out["resstock_monthly_kwh"] = {
            m: round(kwh, 2) for m, kwh in sorted(resstock_monthly_kwh.items())
        }
        out["resstock_scale_factor"] = (
            round(resstock_scale_factor, 6)
            if resstock_scale_factor is not None
            else None
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    yaml_str = yaml.safe_dump(
        out, default_flow_style=False, sort_keys=False, allow_unicode=True
    )
    if args.use_resstock_loads:
        inline_comments: dict[str, str] = {
            "total_residential_kwh:": "# sum(resstock_monthly_kwh) * resstock_scale_factor",
            "delivery_top_ups:": "# total_budgets are inflated by total_residential_kwh / eia_total_residential_kwh",
            "resstock_scale_factor:": "# eia_customer_count / resstock_customer_count_for_utility",
        }
        lines = yaml_str.splitlines(keepends=True)
        for i, line in enumerate(lines):
            stripped = line.lstrip()
            for key, comment in inline_comments.items():
                if stripped.startswith(key):
                    lines[i] = line.rstrip("\n") + f" {comment}\n"
                    break
        yaml_str = "".join(lines)
    with open(args.output, "w") as f:
        f.write(yaml_str)


if __name__ == "__main__":
    main()

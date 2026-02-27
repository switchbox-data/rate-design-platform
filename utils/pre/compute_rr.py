#!/usr/bin/env python3
"""Compute topped-up delivery and supply revenue requirements and write rev_requirement/<utility>.yaml.

Reads EIA-861 residential kWh, monthly_rates YAML (per charge, with decision), and the
existing rate-case delivery revenue requirement; filters charges by decision (add_to_drr,
add_to_srr); computes day-weighted avg_monthly_rate per charge and total_budget =
avg_monthly_rate * total_residential_kwh; writes the new schema with delivery and
supply top-ups and derived totals.
"""

from __future__ import annotations

import argparse
import calendar
import re
from pathlib import Path

import logging

import yaml

from utils.scenario_config import get_residential_sales_kwh_from_utility_stats


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
        "--output", type=Path, required=True, help="Output rev_requirement YAML path"
    )
    args = parser.parse_args()

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
    charges = monthly_rates_data.get("charges") or {}
    year_for_eia = _parse_month(start_month)[0]

    total_residential_kwh, eia_year = _resolve_path_eia_with_year_fallback(
        args.path_electric_utility_stats,
        year_for_eia,
        utility,
        storage_options,
    )

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

    delivery_top_ups: dict[str, dict[str, float]] = {}
    supply_top_ups: dict[str, dict[str, float]] = {}
    delivery_budgets_sum = 0.0
    supply_budgets_sum = 0.0

    for slug, data in charges.items():
        decision = data.get("decision")
        if decision not in ("add_to_drr", "add_to_srr"):
            continue
        monthly = data.get("monthly_rates") or {}
        if not monthly:
            continue
        avg_rate = _day_weighted_avg_rate(monthly, month_list)
        total_budget = avg_rate * total_residential_kwh
        entry = {
            "avg_monthly_rate": round(avg_rate, 10),
            "total_budget": round(total_budget, 2),
        }
        if decision == "add_to_drr":
            delivery_top_ups[slug] = entry
            delivery_budgets_sum += total_budget
        else:
            supply_top_ups[slug] = entry
            supply_budgets_sum += total_budget

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

    out: dict[str, object] = {
        "utility": utility,
        "delivery_revenue_requirement_from_rate_case": delivery_revenue_requirement_from_rate_case,
        "delivery_revenue_requirement_topups": delivery_revenue_requirement_topups,
        "supply_revenue_requirement_topups": supply_revenue_requirement_topups,
        "total_delivery_revenue_requirement": total_delivery_revenue_requirement,
        "total_delivery_and_supply_revenue_requirement": total_delivery_and_supply_revenue_requirement,
        "total_residential_kwh": round(total_residential_kwh, 2),
        "eia_year": eia_year,
        "delivery_top_ups": delivery_top_ups,
        "supply_top_ups": supply_top_ups,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        yaml.safe_dump(
            out, f, default_flow_style=False, sort_keys=False, allow_unicode=True
        )


if __name__ == "__main__":
    main()

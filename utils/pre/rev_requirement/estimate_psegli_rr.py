#!/usr/bin/env python3
"""Estimate PSEG-LI residential delivery and supply revenue requirements from LIPA budget data.

PSEG-LI (on behalf of LIPA) does not participate in traditional PSC rate cases,
so the delivery revenue requirement cannot come from a filing. Additionally, LIPA's
residential tariff uses TOU rates for both delivery and supply, which means the
standard ``compute_rr.py`` top-up logic (rate x total kWh) overcounts by 4x.

Instead, this script uses a **bill-proportional method**: the LIPA budget one-pager
publishes a typical residential monthly bill broken down by component. Each component's
share of the total bill, applied to EIA-861 residential sales revenue, gives an
estimate of the revenue collected by that component across all residential customers.

Source document:
    context/sources/lipa_2025_2026_budget_one_pager.md
    (extracted from LIPA's "Fact Sheet: LIPA 2026 Budget as Compared to 2025")

The script parses the "Average Residential Monthly Bill Impact" table and extracts:
    - "Delivery & System" (2025 Budget column)  -> delivery share
    - "Power Supply" (2025 Budget column)        -> supply share
    - "Typical Average Residential Bill Assuming No Change in Customer Usage" -> total

Formula:
    delivery_rr = (delivery_system / total_bill) * EIA_residential_sales_revenue
    supply_base = (power_supply / total_bill) * EIA_residential_sales_revenue
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import yaml

from utils.scenario_config import get_residential_sales_revenue_from_utility_stats

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Markdown table parsing
# ---------------------------------------------------------------------------


def _parse_dollar_amount(text: str) -> float | None:
    """Parse a dollar amount from a markdown table cell.

    Handles: $96.55, 85.20, (1.56), ($1.56), **$193.98**, **(4.89)**
    Returns None if no numeric content found.
    """
    cleaned = text.strip().replace("*", "").replace(",", "")
    negative = "(" in cleaned and ")" in cleaned
    cleaned = cleaned.replace("(", "").replace(")", "").replace("$", "").strip()
    if not cleaned or cleaned == "—" or cleaned == "-":
        return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return -value if negative else value


def _find_section_rows(lines: list[str], heading_fragment: str) -> list[list[str]]:
    """Find a markdown table under a ## heading containing heading_fragment.

    Returns a list of rows, each row being a list of cell strings (stripped).
    """
    in_section = False
    rows: list[list[str]] = []
    for line in lines:
        if line.startswith("## "):
            if in_section:
                break
            if heading_fragment.lower() in line.lower():
                in_section = True
            continue
        if in_section and "|" in line:
            cells = [c.strip() for c in line.split("|")]
            # skip separator rows like |---|---|
            if cells and all(set(c) <= {"-", ":", " ", ""} for c in cells):
                continue
            rows.append(cells)
    return rows


def parse_bill_components(
    md_path: Path,
) -> dict[str, float]:
    """Parse the bill impact table from the LIPA budget one-pager markdown.

    Returns a dict mapping row label -> 2025 Budget column value.
    """
    text = md_path.read_text()
    lines = text.splitlines()

    rows = _find_section_rows(lines, "Average Residential Monthly Bill Impact")
    if not rows:
        raise SystemExit(
            f"Could not find 'Average Residential Monthly Bill Impact' table in {md_path}"
        )

    # First row is the header — find the "2025 Budget" column index
    header = rows[0]
    budget_col = None
    for i, cell in enumerate(header):
        if "2025 Budget" in cell:
            budget_col = i
            break
    if budget_col is None:
        raise SystemExit(
            f"Could not find '2025 Budget' column in bill impact table header: {header}"
        )

    result: dict[str, float] = {}
    for row in rows[1:]:
        if len(row) <= budget_col:
            continue
        label = row[1] if len(row) > 1 else ""
        label = label.replace("*", "").strip()
        if not label:
            continue
        value = _parse_dollar_amount(row[budget_col])
        if value is not None:
            result[label] = value

    return result


# ---------------------------------------------------------------------------
# YAML I/O
# ---------------------------------------------------------------------------


def _read_yaml_mapping(path: Path) -> dict:
    """Read a YAML file, returning an empty dict if the file doesn't exist."""
    if not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def _write_yaml_mapping(path: Path, data: dict, header_comment: str) -> None:
    """Write a YAML mapping with a header comment, preserving key order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for line in header_comment.strip().splitlines():
            f.write(f"# {line}\n")
        f.write(yaml.dump(data, default_flow_style=False, sort_keys=False))


# ---------------------------------------------------------------------------
# EIA-861 with year fallback
# ---------------------------------------------------------------------------


def _resolve_revenue_with_year_fallback(
    path_template: str,
    year: int,
    utility: str,
    storage_options: dict[str, str] | None,
) -> tuple[float, int]:
    """Try path for year, then year-1, ...; return (revenue_dollars, actual_year)."""
    pattern = re.compile(r"year=\d{4}")
    match = pattern.search(path_template)
    if not match:
        rev = get_residential_sales_revenue_from_utility_stats(
            path_template, utility, storage_options=storage_options
        )
        return rev, year

    for offset in range(0, 21):
        try_year = year - offset
        path = pattern.sub(f"year={try_year}", path_template, count=1)
        try:
            rev = get_residential_sales_revenue_from_utility_stats(
                path, utility, storage_options=storage_options
            )
            if offset > 0:
                log.warning(
                    "EIA-861 revenue for year %s not found; used year %s.",
                    year,
                    try_year,
                )
            return rev, try_year
        except (OSError, FileNotFoundError):
            continue
    raise FileNotFoundError(
        f"Could not read EIA-861 revenue for utility={utility} "
        f"for year {year} or prior 20 years."
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Estimate PSEG-LI residential delivery and supply revenue requirements "
            "from the LIPA budget one-pager and EIA-861 residential sales revenue."
        )
    )
    parser.add_argument(
        "--path-budget-md",
        type=Path,
        required=True,
        help="Path to extracted LIPA budget one-pager markdown",
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
        help="Path to delivery_rev_requirements_from_rate_cases.yaml (will be updated)",
    )
    parser.add_argument(
        "--path-supply-base",
        type=Path,
        required=True,
        help="Path to supply_base_overrides.yaml (will be created/updated)",
    )
    parser.add_argument(
        "--output-key",
        required=True,
        help="Utility key to write (e.g. 'psegli')",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="EIA-861 year to use (default 2025)",
    )
    args = parser.parse_args()

    # --- Parse bill components from budget markdown ---
    components = parse_bill_components(args.path_budget_md)
    log.info("Parsed bill components from %s:", args.path_budget_md)
    for label, value in components.items():
        log.info("  %-60s  $%10.2f", label, value)

    delivery_system = components.get("Delivery & System")
    power_supply = components.get("Power Supply")
    total_bill = components.get(
        "Typical Average Residential Bill Assuming No Change in Customer Usage"
    )

    if delivery_system is None or total_bill is None:
        raise SystemExit(
            "Could not find 'Delivery & System' and/or total bill row in the "
            f"bill impact table. Found labels: {list(components.keys())}"
        )
    if power_supply is None:
        raise SystemExit(
            "Could not find 'Power Supply' row in the bill impact table. "
            f"Found labels: {list(components.keys())}"
        )

    delivery_share = delivery_system / total_bill
    supply_share = power_supply / total_bill

    # --- Read EIA-861 residential sales revenue ---
    storage_options = None
    if args.path_electric_utility_stats.startswith("s3://"):
        try:
            from data.eia.hourly_loads.eia_region_config import (
                get_aws_storage_options,
            )

            storage_options = get_aws_storage_options()
        except ImportError:
            pass

    eia_revenue, eia_year = _resolve_revenue_with_year_fallback(
        args.path_electric_utility_stats,
        args.year,
        args.output_key,
        storage_options,
    )

    # --- Compute estimates ---
    delivery_rr = delivery_share * eia_revenue
    supply_base = supply_share * eia_revenue

    log.info("")
    log.info("=== PSEG-LI Revenue Requirement Estimates ===")
    log.info(
        "Delivery & System:  $%.2f/mo  (%.2f%% of bill)",
        delivery_system,
        delivery_share * 100,
    )
    log.info(
        "Power Supply:       $%.2f/mo  (%.2f%% of bill)",
        power_supply,
        supply_share * 100,
    )
    log.info("Total bill:         $%.2f/mo", total_bill)
    log.info(
        "EIA-861 residential sales revenue (year %d): $%s",
        eia_year,
        f"{eia_revenue:,.0f}",
    )
    log.info("")
    log.info("Estimated delivery RR:  $%s", f"{delivery_rr:,.0f}")
    log.info("Estimated supply base:  $%s", f"{supply_base:,.0f}")

    # --- Write delivery RR to rate-case YAML ---
    rr_data = _read_yaml_mapping(args.path_rate_case_rr)
    rr_data[args.output_key] = round(delivery_rr, 2)
    _write_yaml_mapping(
        args.path_rate_case_rr,
        rr_data,
        (
            "Rate-case delivery revenue requirements (from PUC filings / revenue_requirements CSV).\n"
            "One entry per utility. Used as input to compute_rr.py; do not overwrite with computed values.\n"
            f"Note: {args.output_key} is estimated from LIPA budget via estimate_psegli_rr.py\n"
            "Source: s3://data.sb/switchbox/revenue_requirements/ny/revenue_requirements_dummy.csv"
        ),
    )
    log.info(
        "Wrote %s = %.2f to %s", args.output_key, delivery_rr, args.path_rate_case_rr
    )

    # --- Write supply base to overrides YAML ---
    supply_data = _read_yaml_mapping(args.path_supply_base)
    supply_data[args.output_key] = round(supply_base, 2)
    _write_yaml_mapping(
        args.path_supply_base,
        supply_data,
        (
            "Supply base overrides for utilities with TOU supply rates that can't be\n"
            "summed directly. Estimated from budget documents.\n"
            "See utils/pre/rev_requirement/estimate_psegli_rr.py for methodology."
        ),
    )
    log.info(
        "Wrote %s = %.2f to %s", args.output_key, supply_base, args.path_supply_base
    )


if __name__ == "__main__":
    main()

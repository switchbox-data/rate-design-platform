#!/usr/bin/env python3
"""Complete a partial *_rate_case_test_year.yaml with top-ups and ResStock scaling.

Track 2 revenue requirement pipeline: for utilities whose rate-case filings
provide test-year kWh and customer-count determinants (superseding EIA-861).

Reads:
  1. A hand-seeded partial YAML with base DRR, test-year kWh, and customer counts
     sourced from rate-case testimony.
  2. A monthly-rates YAML (from fetch_monthly_rates.py) with per-charge monthly
     $/kWh or $/month rates grouped by decision (add_to_drr, add_to_srr,
     already_in_drr).
  3. ResStock monthly loads from S3 (12 rows/building) to derive the
     resstock_kwh_scale_factor and supply commodity budget.

Writes a completed YAML with delivery_top_ups, supply_top_ups, the ResStock
scaling block, supply_commodity_bundled_rates, and all summed totals.

See utils/pre/rev_requirement/README.md for Track 1 vs Track 2 details and the
grid_cons / electricity_net floor explanation.
"""

from __future__ import annotations

import argparse
import calendar
import logging
import re
from pathlib import Path
from typing import Any, cast

import polars as pl
import yaml

from utils.loads import (
    BLDG_ID_COL,
    ELECTRIC_LOAD_COL,
    ELECTRIC_PV_COL,
    grid_consumption_expr,
)
from utils.post.io import scan_load_curves_for_utility

log = logging.getLogger(__name__)

RESSTOCK_UPGRADE = "00"


# ---------------------------------------------------------------------------
# Helpers (same logic as compute_rr.py; duplicated to keep scripts standalone)
# ---------------------------------------------------------------------------


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
    customer_count: float,
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
            log.warning("Unknown charge_unit %r; treating as $/month", charge_unit)
            total += rate
    return total * customer_count


# ---------------------------------------------------------------------------
# ResStock load aggregation
# ---------------------------------------------------------------------------


def _load_utility_weights(
    path_resstock_release: str,
    state: str,
    utility: str,
) -> pl.DataFrame:
    """Load bldg_id + weight for a utility, handling both metadata layouts.

    Some ResStock releases store weight in utility_assignment.parquet (e.g. RI);
    others store only the utility mapping there and keep weight in
    metadata-sb.parquet (e.g. MD).  This function handles both.
    """
    base = path_resstock_release.rstrip("/")
    ua_path = f"{base}/metadata_utility/state={state}/utility_assignment.parquet"

    ua_df = cast(
        pl.DataFrame,
        pl.scan_parquet(ua_path)
        .filter(pl.col("sb.electric_utility") == utility)
        .collect(),
    )
    if ua_df.is_empty():
        raise ValueError(f"No buildings for utility {utility!r} in {ua_path}.")

    if "weight" in ua_df.columns:
        return ua_df.select(BLDG_ID_COL, "weight")

    bldg_ids = ua_df[BLDG_ID_COL].to_list()
    meta_path = (
        f"{base}/metadata/state={state}/upgrade={RESSTOCK_UPGRADE}/metadata-sb.parquet"
    )
    weights_df = cast(
        pl.DataFrame,
        pl.scan_parquet(meta_path)
        .filter(pl.col(BLDG_ID_COL).is_in(bldg_ids))
        .select(BLDG_ID_COL, "weight")
        .collect(),
    )
    if weights_df.is_empty():
        raise ValueError(
            f"No weights found in {meta_path} for {len(bldg_ids)} building IDs."
        )
    return weights_df


def _aggregate_resstock_monthly_grid_cons(
    path_resstock_release: str,
    state: str,
    utility: str,
) -> tuple[dict[int, float], float, float]:
    """Aggregate ResStock monthly grid_cons for a utility.

    Uses ``grid_cons = max(total_load - abs(pv), 0)`` from ``utils.loads``
    so the kWh total matches what CAIRO bills against (after the
    electricity_net floor in run_scenario.py).

    Returns
    -------
    monthly_kwh : dict[int, float]
        Weighted grid_cons per calendar month {1: ..., 12: ...}.
    total_kwh : float
        Sum of monthly_kwh.
    customer_count : float
        Sum of ResStock building weights for this utility.
    """
    weights_df = _load_utility_weights(path_resstock_release, state, utility)

    customer_count = float(weights_df["weight"].sum())
    n_bldgs = weights_df.height
    log.info(
        "ResStock: %d buildings, %.2f weighted customers for %s",
        n_bldgs,
        customer_count,
        utility,
    )

    loads_lf = scan_load_curves_for_utility(
        path_resstock_release=path_resstock_release,
        state=state,
        upgrade=RESSTOCK_UPGRADE,
        utility=utility,
        load_curve_type="monthly",
    )

    monthly_df = cast(
        pl.DataFrame,
        loads_lf.with_columns(
            grid_consumption_expr(ELECTRIC_LOAD_COL, ELECTRIC_PV_COL).alias("grid_cons")
        )
        .join(weights_df.lazy(), on=BLDG_ID_COL, how="inner")
        .group_by("month")
        .agg(
            (pl.col("grid_cons") * pl.col("weight")).sum().alias("weighted_kwh"),
        )
        .sort("month")
        .collect(),
    )

    if monthly_df.height != 12:
        raise ValueError(
            f"Expected 12 months from ResStock, got {monthly_df.height}. "
            f"Months present: {sorted(monthly_df['month'].to_list())}"
        )

    monthly_kwh: dict[int, float] = {}
    for row in monthly_df.iter_rows(named=True):
        monthly_kwh[int(row["month"])] = float(row["weighted_kwh"])

    total_kwh = sum(monthly_kwh.values())
    return monthly_kwh, total_kwh, customer_count


# ---------------------------------------------------------------------------
# Budget computation
# ---------------------------------------------------------------------------


def _compute_charge_budget(
    slug: str,
    charge_data: dict[str, Any],
    month_list: list[tuple[int, int]],
    test_year_kwh: float,
    test_year_customer_count: float,
    *,
    scaled_monthly_kwh: dict[int, float] | None = None,
) -> dict[str, Any] | None:
    """Compute the annual budget for a single charge.

    For supply_commodity_bundled, uses scaled_monthly_kwh × monthly rates
    (budget_method: resstock).  For other $/kWh charges, uses
    avg_rate × test_year_kwh (budget_method: rate_case).
    """
    monthly = charge_data.get("monthly_rates") or {}
    if not monthly:
        return None

    charge_unit = charge_data.get("charge_unit", "$/kWh")

    if charge_unit in ("$/day", "$/month"):
        total_budget = _fixed_charge_annual_budget(
            monthly, month_list, charge_unit, test_year_customer_count
        )
        return {
            "charge_unit": charge_unit,
            "budget_method": "rate_case",
            "total_budget": round(total_budget, 2),
        }

    if charge_unit == "$/kWh":
        if slug == "supply_commodity_bundled" and scaled_monthly_kwh is not None:
            total_budget = 0.0
            for y, m in month_list:
                month_key = f"{y:04d}-{m:02d}"
                rate = monthly.get(month_key, 0.0)
                kwh = scaled_monthly_kwh.get(m, 0.0)
                total_budget += rate * kwh
            return {
                "charge_unit": charge_unit,
                "budget_method": "resstock",
                "total_budget": round(total_budget, 2),
            }

        avg_rate = _day_weighted_avg_rate(monthly, month_list)
        total_budget = avg_rate * test_year_kwh
        return {
            "charge_unit": charge_unit,
            "budget_method": "rate_case",
            "total_budget": round(total_budget, 2),
        }

    log.warning("Skipping %s: unsupported charge_unit %r", slug, charge_unit)
    return None


# ---------------------------------------------------------------------------
# YAML output
# ---------------------------------------------------------------------------


class _InputComments:
    """Comments captured from the hand-maintained input YAML.

    Retains testimony provenance (header block, docket links, source notes on
    top-level keys) so it can be re-injected into the regenerated output.  This
    is idempotent: re-reading the generated output captures the same comments
    and re-injects them identically.
    """

    def __init__(self) -> None:
        self.header: list[str] = []
        self.preceding: dict[str, list[str]] = {}
        self.inline: dict[str, str] = {}


_TOP_LEVEL_KEY = re.compile(r"^([A-Za-z0-9_]+):")


def _extract_input_comments(raw_text: str) -> _InputComments:
    """Parse comment lines from the input YAML, keyed by top-level key.

    Captures:
    - ``header``: contiguous comment lines before the first key.
    - ``preceding[key]``: comment lines immediately before a top-level key.
    - ``inline[key]``: a trailing ``# ...`` comment on a top-level key line.

    Only top-level (unindented) keys are tracked; nested keys are ignored so
    recurring keys like ``total_budget`` never collide.
    """
    result = _InputComments()
    pending: list[str] = []
    seen_first_key = False

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            # Blank line: drop any dangling comment buffer so unattached
            # comments don't leak onto the next key.
            pending = []
            continue
        if stripped.startswith("#"):
            pending.append(stripped)
            continue

        match = _TOP_LEVEL_KEY.match(line)
        if match:
            key = match.group(1)
            if not seen_first_key:
                result.header = pending
                seen_first_key = True
            elif pending:
                result.preceding[key] = pending
            pending = []

            # Inline comment: split on the first ' #' outside of quotes. Values
            # here are plain numbers/strings, so a simple split is safe.
            if " #" in line:
                inline = "#" + line.split(" #", 1)[1]
                result.inline[key] = inline.strip()
        else:
            # Indented / non-top-level line: reset the comment buffer.
            pending = []

    return result


def _build_output_yaml(
    partial: dict[str, Any],
    delivery_rr_breakdown: dict[str, dict],
    delivery_top_ups: dict[str, dict],
    supply_top_ups: dict[str, dict],
    resstock_monthly_kwh: dict[int, float],
    resstock_total_kwh: float,
    resstock_customer_count: float,
    scaled_monthly_kwh: dict[int, float],
    supply_commodity_rates: dict[str, Any],
    customer_scale_factor: float,
    kwh_scale_factor: float,
    scaled_customer_total: float,
    input_comments: _InputComments | None = None,
) -> str:
    """Build the completed YAML string with inline comments.

    ``input_comments`` (from :func:`_extract_input_comments`) preserves the
    hand-maintained testimony provenance from the input file so it survives
    regeneration.  Input comments take precedence over the script's generated
    inline comments for any key they cover.
    """
    utility = partial["utility"]
    base_drr = float(partial["delivery_revenue_requirement_from_rate_case"])
    test_year_kwh = float(partial["test_year_residential_kwh"])
    test_year_cc = float(partial["test_year_customer_count"])

    delivery_topups_sum = sum(e["total_budget"] for e in delivery_top_ups.values())
    total_drr = base_drr + delivery_topups_sum
    supply_topups_sum = sum(e["total_budget"] for e in supply_top_ups.values())
    total_drr_and_srr = total_drr + supply_topups_sum

    out: dict[str, Any] = {"utility": utility}

    out["delivery_revenue_requirement_from_rate_case"] = base_drr
    out["delivery_revenue_requirement_topups"] = round(delivery_topups_sum, 2)
    out["total_delivery_revenue_requirement"] = round(total_drr, 2)
    out["supply_revenue_requirement_topups"] = round(supply_topups_sum, 2)
    out["total_delivery_and_supply_revenue_requirement"] = round(total_drr_and_srr, 2)

    out["delivery_revenue_requirement"] = delivery_rr_breakdown
    out["delivery_top_ups"] = delivery_top_ups
    out["supply_top_ups"] = supply_top_ups

    out["test_year_residential_kwh"] = test_year_kwh

    # Per-schedule detail (preserve from partial if present)
    for key in (
        "test_year_residential_kwh_schedule_r",
        "test_year_residential_kwh_schedule_rl",
        "test_year_customer_bills_schedule_r",
        "test_year_customer_bills_schedule_rl",
    ):
        if key in partial:
            out[key] = partial[key]

    out["test_year_customer_count"] = test_year_cc

    # Durable testimony references (hand-maintained, preserved across re-runs).
    if "test_year_fixed_charge_from_rate_case" in partial:
        out["test_year_fixed_charge_from_rate_case"] = partial[
            "test_year_fixed_charge_from_rate_case"
        ]

    out["resstock_total_residential_kwh"] = round(resstock_total_kwh, 2)
    out["resstock_customer_count"] = round(resstock_customer_count, 2)
    out["resstock_customer_scale_factor"] = customer_scale_factor
    out["resstock_total_residential_kwh_scaled_customer"] = round(
        scaled_customer_total, 2
    )
    out["resstock_kwh_scale_factor"] = kwh_scale_factor
    out["resstock_total_residential_kwh_scaled_customer_kwh"] = round(
        sum(scaled_monthly_kwh.values()), 2
    )
    out["resstock_monthly_kwh_scaled_customer_kwh"] = {
        m: round(v, 2) for m, v in sorted(scaled_monthly_kwh.items())
    }

    out["supply_commodity_bundled_rates"] = supply_commodity_rates

    yaml_str = yaml.safe_dump(
        out, default_flow_style=False, sort_keys=False, allow_unicode=True
    )

    # Add inline comments for key computed fields
    inline_comments: dict[str, str] = {
        "delivery_revenue_requirement_from_rate_case:": ("# from rate case testimony"),
        "delivery_revenue_requirement_topups:": (
            "# sum(delivery_top_ups.total_budget)"
        ),
        "total_delivery_revenue_requirement:": (
            "# delivery_revenue_requirement_from_rate_case + delivery_revenue_requirement_topups"
        ),
        "supply_revenue_requirement_topups:": "# sum(supply_top_ups.total_budget)",
        "total_delivery_and_supply_revenue_requirement:": (
            "# total_delivery_revenue_requirement + supply_revenue_requirement_topups"
        ),
        "resstock_total_residential_kwh:": (
            f"# sum(grid_cons * weight) where utility = {utility}"
        ),
        "resstock_customer_count:": (f"# sum(weight) where utility = {utility}"),
        "resstock_customer_scale_factor:": (
            "# test_year_customer_count / resstock_customer_count"
        ),
        "resstock_total_residential_kwh_scaled_customer:": (
            "# resstock_total_residential_kwh * resstock_customer_scale_factor"
        ),
        "resstock_kwh_scale_factor:": (
            "# test_year_residential_kwh / resstock_total_residential_kwh_scaled_customer"
        ),
        "resstock_total_residential_kwh_scaled_customer_kwh:": (
            "# = test_year_residential_kwh (sanity check)"
        ),
    }
    ic = input_comments or _InputComments()

    out_lines: list[str] = []
    for line in yaml_str.splitlines(keepends=True):
        match = _TOP_LEVEL_KEY.match(line)
        key = match.group(1) if match else None

        # Re-inject preceding provenance comments captured from the input.
        if key is not None and key in ic.preceding:
            for c in ic.preceding[key]:
                out_lines.append(c + "\n")

        # Inline comment: input provenance takes precedence over the script's
        # generated inline comment for the same key.
        inline = ic.inline.get(key) if key is not None else None
        if inline is None and key is not None:
            inline = inline_comments.get(f"{key}:")
        if inline is not None:
            out_lines.append(line.rstrip("\n") + f" {inline}\n")
        else:
            out_lines.append(line)

    body = "".join(out_lines)

    # Prepend the input's header comment block (testimony source / docket).
    if ic.header:
        header_block = "\n".join(ic.header) + "\n"
        return header_block + body
    return body


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description=(
            "Complete a partial *_rate_case_test_year.yaml with delivery/supply "
            "top-ups and the ResStock scaling block (Track 2 pipeline)."
        )
    )
    parser.add_argument(
        "--path-partial-yaml",
        type=Path,
        required=True,
        help="Path to partial *_rate_case_test_year.yaml with testimony determinants.",
    )
    parser.add_argument(
        "--path-monthly-rates",
        type=Path,
        required=True,
        help="Path to <utility>_monthly_rates.yaml from fetch_monthly_rates.py.",
    )
    parser.add_argument(
        "--path-resstock-release",
        type=str,
        required=True,
        help="Root of the ResStock release (S3 or local).",
    )
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        help="Two-letter state code (uppercase, e.g. MD).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output path for the completed YAML.",
    )
    args = parser.parse_args()

    # --- Step 1: Read partial YAML ---
    raw_partial_text = Path(args.path_partial_yaml).read_text()
    partial = yaml.safe_load(raw_partial_text)
    if not isinstance(partial, dict):
        raise SystemExit(f"{args.path_partial_yaml} must be a YAML mapping.")
    input_comments = _extract_input_comments(raw_partial_text)

    required_keys = (
        "utility",
        "delivery_revenue_requirement_from_rate_case",
        "test_year_residential_kwh",
        "test_year_customer_count",
    )
    for key in required_keys:
        if key not in partial:
            raise SystemExit(
                f"Missing required key {key!r} in {args.path_partial_yaml}"
            )

    utility = partial["utility"]
    base_drr = float(partial["delivery_revenue_requirement_from_rate_case"])
    test_year_kwh = float(partial["test_year_residential_kwh"])
    test_year_cc = float(partial["test_year_customer_count"])

    log.info("Utility: %s", utility)
    log.info("Base DRR: $%s", f"{base_drr:,.0f}")
    log.info("Test-year kWh: %s", f"{test_year_kwh:,.0f}")
    log.info("Test-year customer count: %s", f"{test_year_cc:,.2f}")

    # --- Step 2: Read monthly rates YAML ---
    with open(args.path_monthly_rates) as f:
        rates_data = yaml.safe_load(f)
    if not isinstance(rates_data, dict):
        raise SystemExit(f"{args.path_monthly_rates} must be a YAML mapping.")

    start_month = rates_data.get("start_month")
    end_month = rates_data.get("end_month")
    if not start_month or not end_month:
        raise SystemExit(
            f"{args.path_monthly_rates} must have start_month and end_month."
        )
    month_list = _months_in_range(start_month, end_month)

    def _extract_charges(section_key: str) -> dict[str, dict]:
        section = rates_data.get(section_key) or {}
        if isinstance(section, dict) and "charges" in section:
            return dict(section["charges"])
        return {}

    drr_charges = _extract_charges("add_to_drr")
    srr_charges = _extract_charges("add_to_srr")
    base_charges = _extract_charges("already_in_drr")

    # --- Step 3: Compute delivery top-ups ---
    delivery_top_ups: dict[str, dict] = {}
    for slug, data in drr_charges.items():
        entry = _compute_charge_budget(
            slug, data, month_list, test_year_kwh, test_year_cc
        )
        if entry is not None:
            delivery_top_ups[slug] = entry

    delivery_topups_sum = sum(e["total_budget"] for e in delivery_top_ups.values())
    log.info("Delivery top-ups total: $%s", f"{delivery_topups_sum:,.2f}")

    # --- Step 4: Compute delivery_revenue_requirement breakdown ---
    # Informational breakdown of the base DRR into already_in_drr charges;
    # used only for the sanity check below (not summed into any budget total).
    delivery_rr_breakdown: dict[str, dict] = {}
    for slug, data in base_charges.items():
        entry = _compute_charge_budget(
            slug, data, month_list, test_year_kwh, test_year_cc
        )
        if entry is not None:
            delivery_rr_breakdown[slug] = entry

    base_breakdown_sum = sum(e["total_budget"] for e in delivery_rr_breakdown.values())
    pct_diff = abs(base_breakdown_sum - base_drr) / base_drr * 100 if base_drr else 0
    if pct_diff > 2:
        log.warning(
            "already_in_drr breakdown ($%s) diverges from base DRR ($%s) by %.1f%%",
            f"{base_breakdown_sum:,.0f}",
            f"{base_drr:,.0f}",
            pct_diff,
        )
    else:
        log.info(
            "already_in_drr breakdown ($%s) matches base DRR ($%s) within %.1f%%",
            f"{base_breakdown_sum:,.0f}",
            f"{base_drr:,.0f}",
            pct_diff,
        )

    # --- Step 5: Aggregate ResStock monthly loads ---
    log.info(
        "Aggregating ResStock monthly grid_cons for %s in %s...",
        utility,
        args.state.upper(),
    )
    resstock_monthly_kwh, resstock_total_kwh, resstock_cc = (
        _aggregate_resstock_monthly_grid_cons(
            path_resstock_release=args.path_resstock_release,
            state=args.state.upper(),
            utility=utility,
        )
    )
    log.info(
        "ResStock total grid_cons: %s kWh, customer_count: %.2f",
        f"{resstock_total_kwh:,.0f}",
        resstock_cc,
    )

    # --- Step 6: Compute scaling block ---
    customer_scale_factor = test_year_cc / resstock_cc
    scaled_customer_total = resstock_total_kwh * customer_scale_factor
    kwh_scale_factor = test_year_kwh / scaled_customer_total

    scaled_monthly_kwh: dict[int, float] = {
        m: v * customer_scale_factor * kwh_scale_factor
        for m, v in resstock_monthly_kwh.items()
    }
    scaled_total_check = sum(scaled_monthly_kwh.values())
    if abs(scaled_total_check - test_year_kwh) > 1.0:
        log.warning(
            "Scaled monthly total (%s) != test_year_kwh (%s); FP drift = %.2f",
            f"{scaled_total_check:,.0f}",
            f"{test_year_kwh:,.0f}",
            scaled_total_check - test_year_kwh,
        )

    log.info("customer_scale_factor: %.10f", customer_scale_factor)
    log.info("kwh_scale_factor: %.16f", kwh_scale_factor)

    # --- Step 7: Compute supply top-ups ---
    supply_top_ups: dict[str, dict] = {}
    for slug, data in srr_charges.items():
        entry = _compute_charge_budget(
            slug,
            data,
            month_list,
            test_year_kwh,
            test_year_cc,
            scaled_monthly_kwh=scaled_monthly_kwh,
        )
        if entry is not None:
            supply_top_ups[slug] = entry

    supply_topups_sum = sum(e["total_budget"] for e in supply_top_ups.values())
    log.info("Supply top-ups total: $%s", f"{supply_topups_sum:,.2f}")

    # --- Step 8: Build supply_commodity_bundled_rates for output ---
    supply_commodity_data = srr_charges.get("supply_commodity_bundled", {})
    supply_commodity_rates: dict[str, Any] = {
        "charge_unit": "$/kWh",
        "monthly_rates": dict(supply_commodity_data.get("monthly_rates", {})),
    }

    # --- Step 9: Write completed YAML ---
    total_drr = base_drr + delivery_topups_sum
    total_all = total_drr + supply_topups_sum
    log.info("Total DRR: $%s", f"{total_drr:,.2f}")
    log.info("Total DRR+SRR: $%s", f"{total_all:,.2f}")

    yaml_str = _build_output_yaml(
        partial=partial,
        delivery_rr_breakdown=delivery_rr_breakdown,
        delivery_top_ups=delivery_top_ups,
        supply_top_ups=supply_top_ups,
        resstock_monthly_kwh=resstock_monthly_kwh,
        resstock_total_kwh=resstock_total_kwh,
        resstock_customer_count=resstock_cc,
        scaled_monthly_kwh=scaled_monthly_kwh,
        supply_commodity_rates=supply_commodity_rates,
        customer_scale_factor=customer_scale_factor,
        kwh_scale_factor=kwh_scale_factor,
        scaled_customer_total=scaled_customer_total,
        input_comments=input_comments,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        f.write(yaml_str)
    log.info("Wrote %s", args.output)


if __name__ == "__main__":
    main()

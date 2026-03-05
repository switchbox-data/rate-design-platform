"""Generate flat URDB v7 tariffs for all utilities in a state.

For each utility discovered in the monthly-rates directory, produces two files:
  - <utility>_flat.json        (delivery only)
  - <utility>_flat_supply.json (delivery + supply)

Fixed charges are extracted from the monthly_rates YAML (sum/average of $/month
and $/day charges in already_in_drr + add_to_drr).  Volumetric rates are derived
top-down from the revenue requirement: delivery_vol = (total_delivery_rr -
fixed_revenue) / total_kwh.  This gives CAIRO precalc a starting point close to
the calibrated result, since calibration adjusts the volumetric rate to match the
revenue requirement anyway.
"""

from __future__ import annotations

import argparse
import calendar
import logging
import re
from pathlib import Path

import yaml

from utils.pre.create_tariff import create_default_flat_tariff, write_tariff_json
from utils.scenario_config import get_residential_customer_count_from_utility_stats

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EIA-861 helpers (same year-fallback as compute_rr.py)
# ---------------------------------------------------------------------------


def _resolve_customer_count(eia_path: str, year: int, utility: str) -> tuple[int, int]:
    """Return (customer_count, actual_year) with year-fallback."""
    pattern = re.compile(r"year=\d{4}")
    if not pattern.search(eia_path):
        count = get_residential_customer_count_from_utility_stats(eia_path, utility)
        return count, year

    for offset in range(21):
        try_year = year - offset
        path = pattern.sub(f"year={try_year}", eia_path, count=1)
        try:
            count = get_residential_customer_count_from_utility_stats(path, utility)
            if offset > 0:
                log.warning(
                    "EIA-861 year %d unavailable for %s; fell back to %d",
                    year,
                    utility,
                    try_year,
                )
            return count, try_year
        except (OSError, FileNotFoundError):
            continue
    raise FileNotFoundError(
        f"No EIA-861 data for utility={utility} in years {year}..{year - 20}"
    )


# ---------------------------------------------------------------------------
# Monthly-rates extraction
# ---------------------------------------------------------------------------

DAYS_PER_MONTH = {m: calendar.monthrange(2025, m)[1] for m in range(1, 13)}


def _monthly_to_monthly_dollar(
    monthly_rates: dict[str, float], charge_unit: str
) -> dict[str, float]:
    """Normalise a charge's monthly_rates to $/month equivalents.

    $/month -> passthrough.  $/day -> multiply by days in that month.
    """
    if charge_unit == "$/month":
        return monthly_rates
    if charge_unit == "$/day":
        return {
            k: v * DAYS_PER_MONTH[int(k.split("-")[1])]
            for k, v in monthly_rates.items()
        }
    raise ValueError(f"Cannot convert {charge_unit} to $/month")


def _extract_fixed_charges(section: dict) -> dict[str, float]:
    """Extract per-month fixed-charge totals ($/month) from a YAML section.

    Sums $/month and $/day charges (converted to monthly equivalents).
    Skips nested seasonal/TOU structures (which shouldn't appear for fixed
    charges but are guarded against defensively).
    """
    charges: dict = section.get("charges", {})
    fixed_by_month: dict[str, float] = {}

    for _slug, data in charges.items():
        unit = data.get("charge_unit", "$/kWh")
        if unit not in ("$/month", "$/day"):
            continue
        mr = data.get("monthly_rates")
        if not isinstance(mr, dict) or not mr:
            continue
        if isinstance(next(iter(mr.values())), dict):
            continue
        normalised = _monthly_to_monthly_dollar(mr, unit)
        for k, v in normalised.items():
            fixed_by_month[k] = fixed_by_month.get(k, 0.0) + v

    return fixed_by_month


def _avg(vals: dict[str, float]) -> float:
    if not vals:
        return 0.0
    return sum(vals.values()) / len(vals)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def _process_utility(
    utility: str,
    monthly_rates_path: Path,
    rev_req_path: Path,
    eia_path: str,
    output_dir: Path,
) -> None:
    """Generate flat.json and flat_supply.json for one utility."""
    with open(monthly_rates_path) as f:
        mr = yaml.safe_load(f)

    with open(rev_req_path) as f:
        rr = yaml.safe_load(f)

    already = mr.get("already_in_drr", {})
    add_drr = mr.get("add_to_drr", {})

    # --- Fixed charge from monthly_rates YAML ---
    fixed_already = _extract_fixed_charges(already)
    fixed_add_drr = _extract_fixed_charges(add_drr)
    combined_fixed: dict[str, float] = {}
    for d in (fixed_already, fixed_add_drr):
        for k, v in d.items():
            combined_fixed[k] = combined_fixed.get(k, 0.0) + v
    fixed_charge = _avg(combined_fixed)

    # --- Top-down volumetric rates from revenue requirement ---
    total_delivery_rr = rr["total_delivery_revenue_requirement"]
    total_kwh = rr["total_residential_kwh"]
    supply_rr = rr["supply_revenue_requirement_topups"]
    eia_year = rr.get("eia_year", 2024)

    customer_count, actual_year = _resolve_customer_count(eia_path, eia_year, utility)

    fixed_revenue = fixed_charge * customer_count * 12
    delivery_vol = (total_delivery_rr - fixed_revenue) / total_kwh
    supply_vol = supply_rr / total_kwh

    log.info(
        "%s: customers=%s (EIA %d), delivery_rr=$%s, fixed_rev=$%s, kwh=%s",
        utility,
        f"{customer_count:,}",
        actual_year,
        f"{total_delivery_rr:,.0f}",
        f"{fixed_revenue:,.0f}",
        f"{total_kwh:,.0f}",
    )
    log.info(
        "  fixed=$%.2f/mo, delivery_vol=$%.5f/kWh, supply_vol=$%.5f/kWh",
        fixed_charge,
        delivery_vol,
        supply_vol,
    )

    # --- Write flat.json (delivery only) ---
    flat_label = f"{utility}_flat"
    flat_tariff = create_default_flat_tariff(
        label=flat_label,
        volumetric_rate=round(delivery_vol, 8),
        fixed_charge=round(fixed_charge, 2),
        adjustment=0.0,
        utility=utility,
    )
    flat_path = output_dir / f"{flat_label}.json"
    write_tariff_json(flat_tariff, flat_path)
    log.info("  wrote %s", flat_path.name)

    # --- Write flat_supply.json (delivery + supply combined) ---
    flat_supply_label = f"{utility}_flat_supply"
    flat_supply_tariff = create_default_flat_tariff(
        label=flat_supply_label,
        volumetric_rate=round(delivery_vol + supply_vol, 8),
        fixed_charge=round(fixed_charge, 2),
        adjustment=0.0,
        utility=utility,
    )
    flat_supply_path = output_dir / f"{flat_supply_label}.json"
    write_tariff_json(flat_supply_tariff, flat_supply_path)
    log.info("  wrote %s", flat_supply_path.name)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate flat URDB v7 tariffs for all utilities in a state"
    )
    parser.add_argument(
        "--monthly-rates-dir",
        type=Path,
        required=True,
        help="Directory containing *_monthly_rates_<year>.yaml files",
    )
    parser.add_argument(
        "--rev-requirement-dir",
        type=Path,
        required=True,
        help="Directory containing <utility>.yaml rev-requirement files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for flat tariff JSONs",
    )
    parser.add_argument(
        "--eia-path",
        required=True,
        help="Path (local or s3://) to EIA-861 utility stats parquet",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Monthly-rates year (default: 2025)",
    )
    args = parser.parse_args()

    pattern = f"*_monthly_rates_{args.year}.yaml"
    mr_files = sorted(args.monthly_rates_dir.glob(pattern))
    if not mr_files:
        raise FileNotFoundError(
            f"No files matching {pattern} in {args.monthly_rates_dir}"
        )

    suffix = f"_monthly_rates_{args.year}.yaml"
    utilities = [f.name.removesuffix(suffix) for f in mr_files]
    log.info("Found %d utilities: %s", len(utilities), ", ".join(utilities))

    args.output_dir.mkdir(parents=True, exist_ok=True)

    for utility, mr_path in zip(utilities, mr_files):
        rr_path = args.rev_requirement_dir / f"{utility}.yaml"
        if not rr_path.exists():
            log.error(
                "Rev-requirement file not found: %s — skipping %s", rr_path, utility
            )
            continue
        _process_utility(utility, mr_path, rr_path, args.eia_path, args.output_dir)

    log.info("Done — wrote flat tariffs for %d utilities", len(utilities))


if __name__ == "__main__":
    main()

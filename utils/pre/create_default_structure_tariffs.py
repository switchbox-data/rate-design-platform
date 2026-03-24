"""Generate default-structure URDB v7 tariffs from monthly_rates YAML.

For each utility discovered in the monthly-rates directory, produces two files:
  - <utility>_default.json        (delivery only)
  - <utility>_default_supply.json (delivery + supply)

Unlike create_flat_tariffs.py which derives a single volumetric rate top-down
from the revenue requirement, this script builds tariffs bottom-up from the
actual filed rates in the monthly_rates YAML — preserving seasonal variation,
consumption tiers, and TOU structure.  CAIRO precalc then calibrates the rates
to hit the revenue requirement while preserving the relative structure.

Dispatches on ``already_in_drr.rate_structure``:
  - ``flat``:             single-tier, detects period boundaries from monthly
                          rate changes
  - ``seasonal_tiered``:  per-season, per-tier rates (e.g. ConEd, O&R)
  - ``seasonal_tou``:     seasonal + time-of-use with weekday/weekend split
                          (e.g. PSEGLI)
"""

from __future__ import annotations

import argparse
import calendar
import logging
from pathlib import Path
from typing import Any

import yaml

from utils.pre.create_tariff import (
    create_seasonal_tiered_tariff,
    create_seasonal_tou_tariff_direct,
    write_tariff_json,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DAYS_PER_MONTH = {m: calendar.monthrange(2025, m)[1] for m in range(1, 13)}


# ---------------------------------------------------------------------------
# Fixed charge extraction
# ---------------------------------------------------------------------------


def _extract_fixed_charge_avg(section: dict) -> float:
    """Average monthly fixed charge ($/month) from all $/month and $/day charges.

    Converts $/day charges to monthly equivalents using actual days per month,
    then averages across all months.  Fixed charges should be recovered as fixed
    charges in the tariff, not absorbed into volumetric rates by CAIRO
    calibration.
    """
    charges: dict = section.get("charges", {})
    totals: dict[str, float] = {}

    for _slug, data in charges.items():
        unit = data.get("charge_unit", "$/kWh")
        if unit not in ("$/month", "$/day"):
            continue
        mr = data.get("monthly_rates")
        if not isinstance(mr, dict) or not mr:
            continue
        if isinstance(next(iter(mr.values())), dict):
            continue

        for k, v in mr.items():
            monthly_val = v
            if unit == "$/day":
                month_num = int(k.split("-")[1])
                monthly_val = v * DAYS_PER_MONTH[month_num]
            totals[k] = totals.get(k, 0.0) + monthly_val

    if not totals:
        return 0.0
    return sum(totals.values()) / len(totals)


# ---------------------------------------------------------------------------
# Volumetric rate extraction: flat structure
# ---------------------------------------------------------------------------


def _extract_flat_kwh_rates(section: dict) -> dict[str, float]:
    """Sum all $/kWh charges in a flat-structure section, keyed by month."""
    charges: dict = section.get("charges", {})
    totals: dict[str, float] = {}

    for _slug, data in charges.items():
        unit = data.get("charge_unit", "$/kWh")
        if unit != "$/kWh":
            continue
        mr = data.get("monthly_rates")
        if not isinstance(mr, dict) or not mr:
            continue
        if isinstance(next(iter(mr.values())), dict):
            continue
        for k, v in mr.items():
            totals[k] = totals.get(k, 0.0) + v

    return totals


# ---------------------------------------------------------------------------
# Volumetric rate extraction: seasonal_tiered structure
# ---------------------------------------------------------------------------


def _month_key_to_int(key: str) -> int:
    """Convert '2025-03' to 3."""
    return int(key.split("-")[1])


def _season_months(seasons: dict) -> dict[str, list[int]]:
    """Parse season definitions into {season_name: [month_ints]}."""
    result: dict[str, list[int]] = {}
    for name, spec in seasons.items():
        fm, tm = spec["from_month"], spec["to_month"]
        if fm <= tm:
            result[name] = list(range(fm, tm + 1))
        else:
            result[name] = list(range(fm, 13)) + list(range(1, tm + 1))
    return result


def _extract_tiered_delivery(
    section: dict,
) -> tuple[
    dict[str, list[int]],
    list[tuple[float | None, dict[str, float]]],
]:
    """Extract tiered $/kWh delivery rates from a seasonal_tiered section.

    Returns (season_months, tiers) where each tier is
    (upper_limit_kwh, {season_name: rate}).
    """
    seasons_def = section.get("seasons", {})
    sm = _season_months(seasons_def)
    charges: dict = section.get("charges", {})

    tiers: list[tuple[float | None, dict[str, float]]] = []

    for _slug, data in charges.items():
        if data.get("charge_unit") != "$/kWh":
            continue
        tier_list = data.get("tiers")
        if not tier_list:
            continue

        for tier in tier_list:
            upper = tier.get("upper_limit_kwh")
            season_rates: dict[str, float] = {}
            for season_name, month_rates in tier["monthly_rates"].items():
                vals = list(month_rates.values())
                season_rates[season_name] = sum(vals) / len(vals)
            tiers.append((upper, season_rates))

    return sm, tiers


# ---------------------------------------------------------------------------
# Volumetric rate extraction: seasonal_tou structure
# ---------------------------------------------------------------------------


def _extract_tou_delivery(
    section: dict,
) -> tuple[
    dict[str, list[int]],
    dict[str, Any],
    dict[str, float],
]:
    """Extract TOU delivery rates from a seasonal_tou section.

    Returns (season_months, tou_periods_def, slot_rates) where slot_rates
    maps "{season}_{tou_period}" to rate.
    """
    seasons_def = section.get("seasons", {})
    sm = _season_months(seasons_def)
    tou_periods_def = section.get("tou_periods", {})
    charges: dict = section.get("charges", {})

    slot_rates: dict[str, float] = {}

    for _slug, data in charges.items():
        if data.get("charge_unit") != "$/kWh":
            continue
        mr = data.get("monthly_rates")
        if not isinstance(mr, dict):
            continue

        first_val = next(iter(mr.values()))
        if not isinstance(first_val, dict):
            continue

        for slot_key, month_rates in mr.items():
            vals = list(month_rates.values())
            avg = sum(vals) / len(vals)
            slot_rates[slot_key] = slot_rates.get(slot_key, 0.0) + avg

    return sm, tou_periods_def, slot_rates


def _extract_tou_supply_monthly(section: dict) -> dict[str, dict[str, float]]:
    """Extract per-TOU-slot, per-month supply rates from a seasonal_tou add_to_srr.

    Returns {slot_key: {month_key: rate_sum}}.

    Two-pass approach: first collect TOU-structured charges (dict-of-dicts) to
    establish slot keys, then distribute flat charges (simple month→rate dicts)
    across all slots.  This avoids ordering dependence between TOU and flat
    charges in the YAML.
    """
    charges: dict = section.get("charges", {})

    tou_items: list[dict[str, dict[str, float]]] = []
    flat_items: list[dict[str, float]] = []

    for _slug, data in charges.items():
        if data.get("charge_unit") != "$/kWh":
            continue
        mr = data.get("monthly_rates")
        if not isinstance(mr, dict) or not mr:
            continue

        first_val = next(iter(mr.values()))
        if isinstance(first_val, dict):
            tou_items.append(mr)
        else:
            flat_items.append(mr)

    result: dict[str, dict[str, float]] = {}

    for tou_mr in tou_items:
        for slot_key, month_rates in tou_mr.items():
            if slot_key not in result:
                result[slot_key] = {}
            for mk, v in month_rates.items():
                result[slot_key][mk] = result[slot_key].get(mk, 0.0) + v

    if result:
        for flat_mr in flat_items:
            for slot_key in result:
                for mk, v in flat_mr.items():
                    result[slot_key][mk] = result[slot_key].get(mk, 0.0) + v
    else:
        for flat_mr in flat_items:
            if "_flat" not in result:
                result["_flat"] = {}
            for mk, v in flat_mr.items():
                result["_flat"][mk] = result["_flat"].get(mk, 0.0) + v

    return result


# ---------------------------------------------------------------------------
# Period merging
# ---------------------------------------------------------------------------


def _detect_periods_from_monthly_rates(
    monthly_rates: dict[str, float],
) -> list[tuple[list[int], float]]:
    """Group consecutive months with identical rates into periods.

    Returns list of (months, rate) tuples where months are 1-indexed.
    """
    sorted_keys = sorted(monthly_rates.keys())
    if not sorted_keys:
        return []

    periods: list[tuple[list[int], float]] = []
    current_months: list[int] = [_month_key_to_int(sorted_keys[0])]
    current_rate = monthly_rates[sorted_keys[0]]

    for key in sorted_keys[1:]:
        rate = monthly_rates[key]
        month = _month_key_to_int(key)
        if abs(rate - current_rate) < 1e-9:
            current_months.append(month)
        else:
            periods.append((current_months, current_rate))
            current_months = [month]
            current_rate = rate

    periods.append((current_months, current_rate))
    return periods


def _detect_periods_from_multi_rate_monthly(
    rate_vectors: dict[str, dict[str, float]],
) -> list[tuple[list[int], dict[str, float]]]:
    """Group months where ALL rate vectors have identical values.

    *rate_vectors* maps an arbitrary label (e.g. tier or TOU slot name) to
    {month_key: rate}.  Returns [(months, {label: rate}), ...].
    """
    labels = sorted(rate_vectors.keys())
    if not labels:
        return []

    all_month_keys = sorted(next(iter(rate_vectors.values())).keys())
    if not all_month_keys:
        return []

    def _sig(mk: str) -> tuple[float, ...]:
        return tuple(round(rate_vectors[lb][mk], 9) for lb in labels)

    periods: list[tuple[list[int], dict[str, float]]] = []
    cur_months: list[int] = [_month_key_to_int(all_month_keys[0])]
    cur_sig = _sig(all_month_keys[0])

    for mk in all_month_keys[1:]:
        sig = _sig(mk)
        month = _month_key_to_int(mk)
        if sig == cur_sig:
            cur_months.append(month)
        else:
            rates_dict = {lb: rate_vectors[lb][all_month_keys[0]] for lb in labels}
            ref_mk = next(
                k for k in all_month_keys if _month_key_to_int(k) == cur_months[0]
            )
            rates_dict = {lb: rate_vectors[lb][ref_mk] for lb in labels}
            periods.append((cur_months, rates_dict))
            cur_months = [month]
            cur_sig = sig

    ref_mk = next(k for k in all_month_keys if _month_key_to_int(k) == cur_months[0])
    rates_dict = {lb: rate_vectors[lb][ref_mk] for lb in labels}
    periods.append((cur_months, rates_dict))
    return periods


# ---------------------------------------------------------------------------
# Build tariffs: flat
# ---------------------------------------------------------------------------


def _build_flat_tariff(
    utility: str,
    already: dict,
    add_drr: dict,
    add_srr: dict | None,
    fixed_charge: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build delivery-only and delivery+supply tariffs for a flat structure."""
    base_rates = _extract_flat_kwh_rates(already)
    topup_rates = _extract_flat_kwh_rates(add_drr)

    delivery_monthly: dict[str, float] = {}
    for mk in sorted(set(base_rates) | set(topup_rates)):
        delivery_monthly[mk] = base_rates.get(mk, 0.0) + topup_rates.get(mk, 0.0)

    delivery_periods = _detect_periods_from_monthly_rates(delivery_monthly)

    delivery_tariff = create_seasonal_tiered_tariff(
        label=f"{utility}_default",
        periods=[(months, [(rate, None)]) for months, rate in delivery_periods],
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )

    supply_rates: dict[str, float] = {}
    if add_srr:
        supply_rates = _extract_flat_kwh_rates(add_srr)

    combined_monthly: dict[str, float] = {}
    for mk in sorted(set(delivery_monthly) | set(supply_rates)):
        combined_monthly[mk] = delivery_monthly.get(mk, 0.0) + supply_rates.get(mk, 0.0)

    combined_periods = _detect_periods_from_monthly_rates(combined_monthly)

    supply_tariff = create_seasonal_tiered_tariff(
        label=f"{utility}_default_supply",
        periods=[(months, [(rate, None)]) for months, rate in combined_periods],
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )

    return delivery_tariff, supply_tariff


# ---------------------------------------------------------------------------
# Build tariffs: seasonal_tiered
# ---------------------------------------------------------------------------


def _build_seasonal_tiered_tariff(
    utility: str,
    already: dict,
    add_drr: dict,
    add_srr: dict | None,
    fixed_charge: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build tariffs for a seasonal_tiered structure (ConEd, O&R)."""
    sm, tiers = _extract_tiered_delivery(already)
    topup_rates = _extract_flat_kwh_rates(add_drr)

    season_names = sorted(sm.keys())

    def _avg_topup_for_months(months: list[int]) -> float:
        vals = []
        for mk, v in topup_rates.items():
            if _month_key_to_int(mk) in months:
                vals.append(v)
        return sum(vals) / len(vals) if vals else 0.0

    # Build delivery periods: one per season
    delivery_periods: list[tuple[list[int], list[tuple[float, float | None]]]] = []
    for season_name in season_names:
        months = sm[season_name]
        topup = _avg_topup_for_months(months)
        tier_entries: list[tuple[float, float | None]] = []
        for upper_limit, season_rates in tiers:
            rate = season_rates.get(season_name, 0.0) + topup
            tier_entries.append((rate, upper_limit))
        delivery_periods.append((months, tier_entries))

    delivery_tariff = create_seasonal_tiered_tariff(
        label=f"{utility}_default",
        periods=delivery_periods,
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )

    # Supply variant: supply varies monthly, so we need monthly periods
    supply_rates: dict[str, float] = {}
    if add_srr:
        supply_rates = _extract_flat_kwh_rates(add_srr)

    if not supply_rates:
        return delivery_tariff, delivery_tariff

    # Build per-tier monthly rate vectors for period detection
    tier_vectors: dict[str, dict[str, float]] = {}
    for tier_idx, (upper_limit, season_rates) in enumerate(tiers):
        month_rates: dict[str, float] = {}
        for mk in sorted(topup_rates.keys() | supply_rates.keys()):
            month = _month_key_to_int(mk)
            season = next((s for s, ms in sm.items() if month in ms), season_names[0])
            base = season_rates.get(season, 0.0)
            topup = topup_rates.get(mk, 0.0)
            supply = supply_rates.get(mk, 0.0)
            month_rates[mk] = base + topup + supply
        tier_vectors[f"tier_{tier_idx}"] = month_rates

    combined_periods_raw = _detect_periods_from_multi_rate_monthly(tier_vectors)

    combined_periods: list[tuple[list[int], list[tuple[float, float | None]]]] = []
    for months, rates_dict in combined_periods_raw:
        tier_entries = []
        for tier_idx, (upper_limit, _) in enumerate(tiers):
            rate = rates_dict[f"tier_{tier_idx}"]
            tier_entries.append((rate, upper_limit))
        combined_periods.append((months, tier_entries))

    supply_tariff = create_seasonal_tiered_tariff(
        label=f"{utility}_default_supply",
        periods=combined_periods,
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )

    return delivery_tariff, supply_tariff


# ---------------------------------------------------------------------------
# Build tariffs: seasonal_tou
# ---------------------------------------------------------------------------


def _build_seasonal_tou_tariff(
    utility: str,
    already: dict,
    add_drr: dict,
    add_srr: dict | None,
    fixed_charge: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build tariffs for a seasonal_tou structure (PSEGLI)."""
    sm, tou_periods_def, delivery_slot_rates = _extract_tou_delivery(already)
    topup_rates = _extract_flat_kwh_rates(add_drr)
    avg_topup = sum(topup_rates.values()) / len(topup_rates) if topup_rates else 0.0

    season_names = sorted(sm.keys())
    tou_names = sorted(tou_periods_def.keys())

    slot_keys = [f"{s}_{t}" for s in season_names for t in tou_names]

    peak_hours: set[int] = set()
    weekdays_only = False
    on_peak_name = next(
        (t for t in tou_names if "off" not in t and "peak" in t), None
    ) or next((t for t in tou_names if "off" not in t), tou_names[-1])
    if on_peak_name in tou_periods_def:
        tou_spec = tou_periods_def[on_peak_name]
        fh, th = tou_spec["from_hour"], tou_spec["to_hour"]
        if fh < th:
            peak_hours = set(range(fh, th))
        else:
            peak_hours = set(range(fh, 24)) | set(range(0, th))
        weekdays_only = tou_spec.get("weekdays_only", False)

    # Assign period indices: iterate seasons then TOU periods
    slot_to_period: dict[str, int] = {}
    for idx, sk in enumerate(slot_keys):
        slot_to_period[sk] = idx

    # Build schedule matrices
    def _make_schedule(include_peak: bool) -> list[list[int]]:
        schedule: list[list[int]] = []
        for m0 in range(12):
            month = m0 + 1
            season = next((s for s, ms in sm.items() if month in ms), season_names[0])
            off_peak_tou = next(t for t in tou_names if "off" in t)
            on_peak_tou = next(t for t in tou_names if "off" not in t)
            off_period = slot_to_period[f"{season}_{off_peak_tou}"]
            on_period = slot_to_period[f"{season}_{on_peak_tou}"]
            row = []
            for hour in range(24):
                if include_peak and hour in peak_hours:
                    row.append(on_period)
                else:
                    row.append(off_period)
            schedule.append(row)
        return schedule

    weekday_schedule = _make_schedule(include_peak=True)
    weekend_schedule = _make_schedule(include_peak=not weekdays_only)

    # Build rate structure
    rate_structure: list[list[dict[str, Any]]] = []
    for sk in slot_keys:
        rate = delivery_slot_rates.get(sk, 0.0) + avg_topup
        rate_structure.append([{"rate": round(rate, 6), "adj": 0.0, "unit": "kWh"}])

    delivery_tariff = create_seasonal_tou_tariff_direct(
        label=f"{utility}_default",
        weekday_schedule=weekday_schedule,
        weekend_schedule=weekend_schedule,
        rate_structure=rate_structure,
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )

    # Supply variant
    supply_slot_monthly: dict[str, dict[str, float]] = {}
    if add_srr:
        srr_structure = add_srr.get("rate_structure", "flat")
        if srr_structure == "seasonal_tou":
            supply_slot_monthly = _extract_tou_supply_monthly(add_srr)
        else:
            flat_supply = _extract_flat_kwh_rates(add_srr)
            for sk in slot_keys:
                supply_slot_monthly[sk] = dict(flat_supply)

    if not supply_slot_monthly:
        return delivery_tariff, delivery_tariff

    # Check if supply varies monthly — if so, we need per-month periods
    all_month_keys = (
        sorted(next(iter(supply_slot_monthly.values())).keys())
        if supply_slot_monthly
        else []
    )

    # Build combined rate vectors per slot per month
    combined_vectors: dict[str, dict[str, float]] = {}
    for sk in slot_keys:
        base_delivery = delivery_slot_rates.get(sk, 0.0) + avg_topup
        combined_vectors[sk] = {}
        for mk in all_month_keys:
            supply_rate = supply_slot_monthly.get(sk, {}).get(mk, 0.0)
            combined_vectors[sk][mk] = base_delivery + supply_rate

    combined_periods = _detect_periods_from_multi_rate_monthly(combined_vectors)

    # Build supply schedule + rate structure with merged periods.
    # Only allocate energyratestructure entries for TOU slots whose season
    # matches the months in each group — CAIRO's precalc iterates every
    # entry in the rate structure and expects to find a matching row in the
    # revenue calculation; unreferenced "dead" entries cause an assertion
    # failure.
    period_idx = 0
    supply_rate_structure: list[list[dict[str, Any]]] = []
    month_to_period_map: dict[int, dict[str, int]] = {}

    for months, rates_dict in combined_periods:
        group_seasons: set[str] = set()
        for m in months:
            season = next((s for s, ms in sm.items() if m in ms), season_names[0])
            group_seasons.add(season)

        slot_period_map: dict[str, int] = {}
        for s in season_names:
            if s not in group_seasons:
                continue
            for t in tou_names:
                sk = f"{s}_{t}"
                rate = rates_dict[sk]
                supply_rate_structure.append(
                    [{"rate": round(rate, 6), "adj": 0.0, "unit": "kWh"}]
                )
                slot_period_map[sk] = period_idx
                period_idx += 1
        for m in months:
            month_to_period_map[m] = slot_period_map

    def _make_supply_schedule(include_peak: bool) -> list[list[int]]:
        schedule: list[list[int]] = []
        for m0 in range(12):
            month = m0 + 1
            season = next((s for s, ms in sm.items() if month in ms), season_names[0])
            off_peak_tou = next(t for t in tou_names if "off" in t)
            on_peak_tou = next(t for t in tou_names if "off" not in t)
            period_map = month_to_period_map.get(month, {})
            off_period = period_map.get(f"{season}_{off_peak_tou}", 0)
            on_period = period_map.get(f"{season}_{on_peak_tou}", 0)
            row = []
            for hour in range(24):
                if include_peak and hour in peak_hours:
                    row.append(on_period)
                else:
                    row.append(off_period)
            schedule.append(row)
        return schedule

    supply_weekday = _make_supply_schedule(include_peak=True)
    supply_weekend = _make_supply_schedule(include_peak=not weekdays_only)

    supply_tariff = create_seasonal_tou_tariff_direct(
        label=f"{utility}_default_supply",
        weekday_schedule=supply_weekday,
        weekend_schedule=supply_weekend,
        rate_structure=supply_rate_structure,
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )

    return delivery_tariff, supply_tariff


# ---------------------------------------------------------------------------
# Core dispatch
# ---------------------------------------------------------------------------


def process_utility(
    utility: str,
    monthly_rates_path: Path,
    output_dir: Path,
) -> None:
    """Generate default.json and default_supply.json for one utility."""
    with open(monthly_rates_path) as f:
        mr = yaml.safe_load(f)

    already = mr.get("already_in_drr", {})
    add_drr = mr.get("add_to_drr", {})
    add_srr = mr.get("add_to_srr")

    rate_structure = already.get("rate_structure", "flat")

    fixed_charge = _extract_fixed_charge_avg(already) + _extract_fixed_charge_avg(
        add_drr
    )
    log.info(
        "%s: rate_structure=%s, fixed_charge=$%.2f/mo",
        utility,
        rate_structure,
        fixed_charge,
    )

    if rate_structure == "flat":
        delivery, supply = _build_flat_tariff(
            utility, already, add_drr, add_srr, fixed_charge
        )
    elif rate_structure == "seasonal_tiered":
        delivery, supply = _build_seasonal_tiered_tariff(
            utility, already, add_drr, add_srr, fixed_charge
        )
    elif rate_structure == "seasonal_tou":
        delivery, supply = _build_seasonal_tou_tariff(
            utility, already, add_drr, add_srr, fixed_charge
        )
    else:
        log.error(
            "%s: unsupported rate_structure '%s' — skipping", utility, rate_structure
        )
        return

    delivery_path = output_dir / f"{utility}_default.json"
    supply_path = output_dir / f"{utility}_default_supply.json"
    write_tariff_json(delivery, delivery_path)
    write_tariff_json(supply, supply_path)

    n_delivery = len(delivery["items"][0]["energyratestructure"])
    n_supply = len(supply["items"][0]["energyratestructure"])
    log.info(
        "  wrote %s (%d periods) and %s (%d periods)",
        delivery_path.name,
        n_delivery,
        supply_path.name,
        n_supply,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate default-structure URDB v7 tariffs from monthly_rates YAML"
    )
    parser.add_argument(
        "--monthly-rates-dir",
        type=Path,
        required=True,
        help="Directory containing *_monthly_rates_<year>.yaml files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for default tariff JSONs",
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
        process_utility(utility, mr_path, args.output_dir)

    log.info("Done — wrote default-structure tariffs for %d utilities", len(utilities))


if __name__ == "__main__":
    main()

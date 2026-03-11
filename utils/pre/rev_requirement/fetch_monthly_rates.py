#!/usr/bin/env python3
"""Fetch monthly volumetric rates for all classified charges and write to YAML.

For a given utility and month range, calls Genability Get Tariff with effectiveOn
per month and lookupVariableRates=true, matches CONSUMPTION_BASED rates to entries
in the topup charge list (all decisions), resolves day-weighted rates per month,
and writes a YAML with every charge labeled by its decision.

Rider fallback: populateRates=true on the base tariff doesn't always resolve
rider-sourced rates (EV Make Ready, VDER, Arrears, etc.).  When classified
tariffRateIds are missing from the base response and have a ``rider_id`` in
the charge list, the script fetches the rider tariff directly to get the rate.

Zone-specific delivery rates (e.g. ConEd zones H/I/J, NatGrid zones) that have
multiple tariffRateIds for the same master_charge will be day-weighted and
deduplicated.

Discovery mode (--discover): enumerate all active rates for a masterTariffId at
a given effective date and write a rump charge_decisions JSON with expanded
schema but no decision labels.  This is the first step in (re)building
charge_decisions from scratch -- the output needs human review to fill in
``decision``, ``master_charge``, and ``master_type``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

log = logging.getLogger(__name__)

# Utility shortcode (scenario YAML) -> Genability masterTariffId
UTILITY_MASTER_TARIFF_IDS: dict[str, int] = {
    "coned": 809,
    "psegli": 3439408,
    "rge": 2001956,
    "nyseg": 80701,
    "nimo": 803,
    "or": 81134,
    "cenhud": 85880,
    "rie": 859,
}


def _slug(s: str) -> str:
    """Lowercase, replace non-alphanumeric with underscore, collapse underscores."""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _parse_iso(s: str) -> datetime:
    if not s:
        return datetime.min
    s = s.replace("T", " ")
    if "+" in s[10:]:
        s = s[: s.index("+", 10)]
    elif s.count("-") > 2:
        last_dash = s.rindex("-")
        if last_dash > 10:
            s = s[:last_dash]
    return datetime.fromisoformat(s.strip())


def _resolve_effective_rate(
    entries: list[dict], month_start: str, month_end: str
) -> float | None:
    """Day-weighted average rate for the calendar month."""
    if len(entries) == 1:
        return entries[0]["rateAmount"]

    ms = datetime.fromisoformat(month_start)
    me = datetime.fromisoformat(month_end)
    total_days = (me - ms).days
    if total_days <= 0:
        return entries[0]["rateAmount"]

    weighted_sum = 0.0
    counted_days = 0
    for e in entries:
        amt = e["rateAmount"]
        if amt is None:
            continue
        e_from = _parse_iso(e["fromDateTime"])
        e_to = _parse_iso(e["toDateTime"])
        overlap_start = max(ms, e_from)
        overlap_end = min(me, e_to)
        overlap_days = max(0, (overlap_end - overlap_start).days)
        if overlap_days > 0:
            weighted_sum += amt * overlap_days
            counted_days += overlap_days

    if counted_days == 0:
        return entries[0]["rateAmount"]
    return weighted_sum / counted_days


def _month_range(year: int, month: int) -> tuple[str, str]:
    from_dt = f"{year}-{month:02d}-01"
    if month == 12:
        to_dt = f"{year + 1}-01-01"
    else:
        to_dt = f"{year}-{month + 1:02d}-01"
    return from_dt, to_dt


def _extract_season(r: dict) -> dict | None:
    """Extract season metadata from a Genability rate entry."""
    s = r.get("season")
    if not s or not isinstance(s, dict):
        return None
    return {
        "name": s.get("seasonName", ""),
        "from_month": s.get("seasonFromMonth"),
        "from_day": s.get("seasonFromDay"),
        "to_month": s.get("seasonToMonth"),
        "to_day": s.get("seasonToDay"),
    }


def _extract_tou(r: dict) -> dict | None:
    """Extract time-of-use metadata from a Genability rate entry."""
    tou = r.get("timeOfUse")
    if not tou or not isinstance(tou, dict):
        return None
    periods = tou.get("touPeriods", [])
    from_hour = periods[0].get("fromHour") if periods else None
    to_hour = periods[0].get("toHour") if periods else None
    from_dow = periods[0].get("fromDayOfWeek") if periods else None
    to_dow = periods[0].get("toDayOfWeek") if periods else None
    weekdays_only = from_dow == 0 and to_dow == 4 if from_dow is not None else None
    return {
        "name": tou.get("touName", ""),
        "type": tou.get("touType", ""),
        "from_hour": from_hour,
        "to_hour": to_hour,
        "weekdays_only": weekdays_only,
    }


def _extract_bands(r: dict) -> list[dict]:
    """Extract all rate bands from a Genability rate entry."""
    return [
        {
            "rateAmount": b.get("rateAmount"),
            "consumptionUpperLimit": b.get("consumptionUpperLimit"),
            "rateSequenceNumber": b.get("rateSequenceNumber"),
        }
        for b in r.get("rateBands", [])
    ]


def _fetch_tariff_rates(
    base_url: str,
    auth: tuple[str, str],
    master_tariff_id: int,
    from_dt: str,
) -> tuple[dict[int, dict], set[int]]:
    """Fetch tariff with effectiveOn + lookupVariableRates.

    Returns (rate_map, unresolved_rider_ids) where rate_map is keyed by
    tariffRateId and unresolved_rider_ids lists the ``riderId`` values of
    rider placeholders that were NOT resolved into actual rate entries.

    Each rate_map entry includes season/TOU metadata and all rate bands
    (not just the first), so callers can handle tiered and seasonal rates.
    """
    resp = requests.get(
        f"{base_url}/rest/public/tariffs/{master_tariff_id}",
        auth=auth,
        params={
            "populateRates": "true",
            "effectiveOn": from_dt,
            "lookupVariableRates": "true",
        },
    )
    resp.raise_for_status()
    tariff = resp.json()["results"][0]

    rate_map: dict[int, dict] = {}
    placeholder_rider_ids: set[int] = set()
    resolved_rider_ids: set[int] = set()
    for r in tariff["rates"]:
        if r.get("riderId"):
            placeholder_rider_ids.add(r["riderId"])
            continue
        if r.get("riderTariffId"):
            resolved_rider_ids.add(r["riderTariffId"])
        trid = r["tariffRateId"]
        bands = r.get("rateBands", [])
        amt = bands[0].get("rateAmount") if bands else None
        if trid not in rate_map:
            rate_map[trid] = {
                "rateName": r["rateName"],
                "chargeType": r.get("chargeType"),
                "variableRateKey": r.get("variableRateKey"),
                "season": _extract_season(r),
                "timeOfUse": _extract_tou(r),
                "rateBands": _extract_bands(r),
                "entries": [],
            }
        rate_map[trid]["entries"].append(
            {
                "rateAmount": amt,
                "fromDateTime": r.get("fromDateTime", ""),
                "toDateTime": r.get("toDateTime", ""),
            }
        )
    unresolved = placeholder_rider_ids - resolved_rider_ids
    return rate_map, unresolved


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------


def _derive_charge_unit(charge_type: str | None, charge_period: str | None) -> str:
    """Best-effort charge_unit from Genability chargeType + chargePeriod."""
    if charge_type == "CONSUMPTION_BASED":
        return "$/kWh"
    if charge_type == "FIXED_PRICE":
        if charge_period == "DAILY":
            return "$/day"
        return "$/month"
    if charge_type == "QUANTITY":
        return "%"
    if charge_type == "MINIMUM":
        return "$/month"
    return "unknown"


def _discover_tariff_rates(
    base_url: str,
    auth: tuple[str, str],
    master_tariff_id: int,
    effective_date: str,
) -> tuple[dict[int, dict], set[int]]:
    """Fetch all rates for a tariff and return a rich map for discovery.

    Unlike ``_fetch_tariff_rates`` this includes placeholder entries
    (rider stubs with no resolved rate data) and captures extra fields
    (chargeClass, chargePeriod, rateGroupName, masterTariffRateId, etc.)
    needed for building a rump charge_decisions JSON.
    """
    resp = requests.get(
        f"{base_url}/rest/public/tariffs/{master_tariff_id}",
        auth=auth,
        params={
            "populateRates": "true",
            "effectiveOn": effective_date,
            "lookupVariableRates": "true",
            "fields": "ext",
        },
    )
    resp.raise_for_status()
    tariff = resp.json()["results"][0]

    discovered: dict[int, dict] = {}
    placeholder_rider_ids: set[int] = set()
    resolved_rider_ids: set[int] = set()

    for r in tariff["rates"]:
        trid = r["tariffRateId"]
        bands = r.get("rateBands", [])
        charge_type = r.get("chargeType")
        charge_period = r.get("chargePeriod")

        if r.get("riderId"):
            # Rider placeholder stub: the API returns both a resolved entry
            # (with actual rate data, tagged with riderTariffId) AND an
            # unresolved "- SC1" stub (tagged with riderId, no rate data)
            # for the same underlying charge.  We track the riderId so we
            # can fetch unresolved riders separately, but drop the stub
            # itself — including it would create duplicate entries in
            # charge_decisions that shadow the resolved versions.
            placeholder_rider_ids.add(r["riderId"])
            continue

        rider_tariff_id = r.get("riderTariffId")
        if rider_tariff_id:
            resolved_rider_ids.add(rider_tariff_id)

        first_amt = bands[0].get("rateAmount") if bands else None
        source = "rider_resolved" if rider_tariff_id else "base_tariff"

        if trid not in discovered:
            discovered[trid] = {
                "rate_name": r.get("rateName"),
                "charge_type": charge_type,
                "charge_class": r.get("chargeClass"),
                "charge_period": charge_period,
                "variable_rate_key": r.get("variableRateKey"),
                "rate_group_name": r.get("rateGroupName"),
                "rider_tariff_id": rider_tariff_id,
                "master_tariff_rate_id": r.get("masterTariffRateId"),
                "charge_unit": _derive_charge_unit(charge_type, charge_period),
                "sample_rate": first_amt,
                "is_tiered": len(bands) > 1,
                "rate_bands": [
                    {
                        "rate_amount": b.get("rateAmount"),
                        "upper_limit": b.get("consumptionUpperLimit"),
                    }
                    for b in bands
                ],
                "season": _extract_season(r),
                "time_of_use": _extract_tou(r),
                "source": source,
            }

    unresolved = placeholder_rider_ids - resolved_rider_ids
    return discovered, unresolved


def _discover_rider_rates(
    base_url: str,
    auth: tuple[str, str],
    rider_id: int,
    effective_date: str,
) -> dict[int, dict]:
    """Fetch a rider tariff and return discovered entries."""
    resp = requests.get(
        f"{base_url}/rest/public/tariffs/{rider_id}",
        auth=auth,
        params={
            "populateRates": "true",
            "effectiveOn": effective_date,
            "lookupVariableRates": "true",
            "fields": "ext",
        },
    )
    resp.raise_for_status()
    tariff = resp.json()["results"][0]

    entries: dict[int, dict] = {}
    for r in tariff["rates"]:
        trid = r["tariffRateId"]
        if trid in entries:
            continue
        bands = r.get("rateBands", [])
        charge_type = r.get("chargeType")
        charge_period = r.get("chargePeriod")
        first_amt = bands[0].get("rateAmount") if bands else None
        entries[trid] = {
            "rate_name": r.get("rateName"),
            "charge_type": charge_type,
            "charge_class": r.get("chargeClass"),
            "charge_period": charge_period,
            "variable_rate_key": r.get("variableRateKey"),
            "rate_group_name": r.get("rateGroupName"),
            "rider_tariff_id": rider_id,
            "master_tariff_rate_id": r.get("masterTariffRateId"),
            "charge_unit": _derive_charge_unit(charge_type, charge_period),
            "sample_rate": first_amt,
            "is_tiered": len(bands) > 1,
            "source": "rider_fetched",
        }
    return entries


# ---------------------------------------------------------------------------
# Output generation: decision-grouped YAML with rate_structure discriminator
# ---------------------------------------------------------------------------


def _determine_rate_structure(
    entries: list[dict],
) -> str:
    """Determine rate_structure from a group of per-trid entries.

    - seasonal_tou: any entry has TOU metadata
    - seasonal_tiered: any entry has season metadata AND multi-band rates
    - flat: everything else
    """
    has_tou = any(e.get("tou_meta") for e in entries)
    if has_tou:
        return "seasonal_tou"
    has_season = any(e.get("season_meta") for e in entries)
    has_tiers = any(e.get("band_monthly_rates") for e in entries)
    if has_season and has_tiers:
        return "seasonal_tiered"
    return "flat"


def _collect_seasons(entries: list[dict]) -> dict:
    """Extract unique season definitions from a group of entries."""
    seasons: dict[str, dict] = {}
    for e in entries:
        sm = e.get("season_meta")
        if sm and isinstance(sm, dict):
            name = sm.get("name", "").lower()
            if name and name not in seasons:
                seasons[name] = {
                    "from_month": sm.get("from_month"),
                    "from_day": sm.get("from_day"),
                    "to_month": sm.get("to_month"),
                    "to_day": sm.get("to_day"),
                }
    return seasons


def _collect_tou_periods(entries: list[dict]) -> dict:
    """Extract unique TOU period definitions from a group of entries."""
    periods: dict[str, dict] = {}
    for e in entries:
        tm = e.get("tou_meta")
        if tm and isinstance(tm, dict):
            ttype = (tm.get("type") or tm.get("name", "")).lower().replace("-", "_")
            if ttype and ttype not in periods:
                periods[ttype] = {
                    "name": tm.get("name", ""),
                    "from_hour": tm.get("from_hour"),
                    "to_hour": tm.get("to_hour"),
                    "weekdays_only": tm.get("weekdays_only"),
                }
    return periods


def _season_label(entry: dict) -> str:
    """Return the lowercase season name for an entry, or '' if none."""
    sm = entry.get("season_meta")
    if sm and isinstance(sm, dict):
        return (sm.get("name") or "").lower()
    return ""


def _tou_label(entry: dict) -> str:
    """Return a lowercase TOU type label for an entry, or '' if none."""
    tm = entry.get("tou_meta")
    if tm and isinstance(tm, dict):
        return (tm.get("type") or tm.get("name", "")).lower().replace("-", "_")
    return ""


def _build_flat_charge(entries: list[dict]) -> dict:
    """Build a flat charge entry (single monthly_rates dict).

    If multiple entries share the same master_charge, they are additive
    (e.g. two components of the same conceptual charge). Sum them per
    month and warn when more than one entry contributes.
    """
    base = entries[0]
    if len(entries) == 1:
        return {
            "charge_unit": base["charge_unit"],
            "monthly_rates": base["monthly_rates"],
        }
    log.warning(
        "Flat merge: %d entries for master_charge %r — summing per month. "
        "If these should NOT be additive, give them different master_charges.",
        len(entries),
        base.get("master_charge", "?"),
    )
    merged: dict[str, float] = {}
    for e in entries:
        for mo, val in e["monthly_rates"].items():
            merged[mo] = merged.get(mo, 0.0) + val
    return {"charge_unit": base["charge_unit"], "monthly_rates": merged}


def _build_seasonal_tiered_charge(entries: list[dict]) -> dict:
    """Build a seasonal-tiered charge entry.

    Entries are grouped by season; each has band_monthly_rates with
    per-tier monthly rates.  Output shape:
        tiers: [{upper_limit_kwh, monthly_rates: {season: {month: rate}}}]
    """
    base = entries[0]
    bmr = base.get("band_monthly_rates") or []
    num_bands = max(len(e.get("band_monthly_rates") or []) for e in entries)

    tiers: list[dict] = []
    for band_idx in range(num_bands):
        upper = None
        if band_idx < len(bmr) and bmr[band_idx].get("upper_limit") is not None:
            upper = bmr[band_idx]["upper_limit"]
        per_season: dict[str, dict] = {}
        for e in entries:
            season = _season_label(e) or "all"
            ebmr = e.get("band_monthly_rates") or []
            if band_idx < len(ebmr):
                per_season[season] = ebmr[band_idx]["monthly_rates"]
            else:
                per_season[season] = e["monthly_rates"]
        tiers.append({"upper_limit_kwh": upper, "monthly_rates": per_season})

    return {"charge_unit": base["charge_unit"], "tiers": tiers}


def _build_seasonal_tou_charge(entries: list[dict]) -> dict:
    """Build a seasonal-TOU charge entry.

    Entries represent different season x TOU combinations for the same
    conceptual charge.  Output shape:
        monthly_rates: {season_tou_label: {month: rate}}
    """
    base = entries[0]
    per_period: dict[str, dict] = {}
    for e in entries:
        season = _season_label(e) or "all"
        tou = _tou_label(e) or "all"
        label = f"{season}_{tou}" if season != "all" or tou != "all" else "all"
        per_period[label] = e["monthly_rates"]
    return {"charge_unit": base["charge_unit"], "monthly_rates": per_period}


def _build_grouped_output(
    utility: str,
    master_tariff_id: int,
    start_month: str,
    end_month: str,
    charges_data: dict[str, dict],
    excluded_entries: dict[int, dict],
) -> dict:
    """Build the decision-grouped YAML output structure."""

    # Group per-trid entries by (decision, master_charge)
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for _s, info in charges_data.items():
        key = (info["decision"], info["master_charge"])
        groups[key].append(info)

    # Build per-decision sections
    decision_sections: dict[str, dict] = {}
    for (decision, _mc), entries in groups.items():
        decision_sections.setdefault(decision, []).append(entries)  # type: ignore[arg-type]

    result: dict = {
        "utility": utility,
        "master_tariff_id": master_tariff_id,
        "start_month": start_month,
        "end_month": end_month,
    }

    for decision in ("add_to_drr", "add_to_srr", "already_in_drr"):
        entry_groups: list[list[dict]] = decision_sections.get(decision, [])  # type: ignore[assignment]
        if not entry_groups:
            result[decision] = {"rate_structure": "flat", "charges": {}}
            continue

        all_entries = [e for grp in entry_groups for e in grp]
        rate_structure = _determine_rate_structure(all_entries)

        section: dict = {"rate_structure": rate_structure}
        if rate_structure != "flat":
            seasons = _collect_seasons(all_entries)
            if seasons:
                section["seasons"] = seasons
        if rate_structure == "seasonal_tou":
            tou_periods = _collect_tou_periods(all_entries)
            if tou_periods:
                section["tou_periods"] = tou_periods

        charges: dict[str, dict] = {}
        for grp in entry_groups:
            mc = grp[0]["master_charge"]
            slug = _slug(mc) or "charge"
            base_slug = slug
            idx = 0
            while slug in charges:
                idx += 1
                slug = f"{base_slug}_{idx}"

            has_season_or_tou = any(
                e.get("season_meta") or e.get("tou_meta") for e in grp
            )

            if rate_structure == "flat" or not has_season_or_tou:
                charges[slug] = _build_flat_charge(grp)
            elif rate_structure == "seasonal_tiered":
                charges[slug] = _build_seasonal_tiered_charge(grp)
            elif rate_structure == "seasonal_tou":
                charges[slug] = _build_seasonal_tou_charge(grp)

        section["charges"] = charges
        result[decision] = section

    # Excluded charges
    excluded_data: dict[str, dict] = {}
    for _trid, einfo in sorted(excluded_entries.items()):
        rn = einfo["rate_name"]
        slug_key = _slug(rn) if rn else f"excluded_{_trid}"
        base_key = slug_key
        idx = 0
        while slug_key in excluded_data:
            idx += 1
            slug_key = f"{base_key}_{idx}"
        excluded_data[slug_key] = {"decision": einfo["decision"]}
    result["excluded"] = excluded_data

    return result


def discover_charges(
    utility: str,
    effective_date: str,
    output: Path,
) -> None:
    """Discover all active rates and write a rump charge_decisions JSON."""
    load_dotenv(Path(__file__).resolve().parents[3] / ".env")
    app_id = os.environ.get("ARCADIA_APP_ID")
    app_key = os.environ.get("ARCADIA_APP_KEY")
    if not app_id or not app_key:
        raise SystemExit("ARCADIA_APP_ID and ARCADIA_APP_KEY must be set")
    auth = (app_id, app_key)
    base_url = "https://api.genability.com"

    utility = utility.lower()
    if utility not in UTILITY_MASTER_TARIFF_IDS:
        raise SystemExit(
            f"Unknown utility {utility!r}. Known: {sorted(UTILITY_MASTER_TARIFF_IDS)}"
        )
    master_tariff_id = UTILITY_MASTER_TARIFF_IDS[utility]

    log.info(
        "Discovering charges for %s (masterTariffId=%d, effectiveOn=%s)",
        utility,
        master_tariff_id,
        effective_date,
    )

    discovered, unresolved_rider_ids = _discover_tariff_rates(
        base_url, auth, master_tariff_id, effective_date
    )
    log.info(
        "Base tariff: %d rate entries, %d unresolved rider(s)",
        len(discovered),
        len(unresolved_rider_ids),
    )

    for rid in sorted(unresolved_rider_ids):
        try:
            rider_entries = _discover_rider_rates(base_url, auth, rid, effective_date)
        except requests.HTTPError as exc:
            log.warning("Rider %d returned %s; skipping", rid, exc.response.status_code)
            continue
        time.sleep(0.2)
        for trid, entry in rider_entries.items():
            if trid not in discovered:
                discovered[trid] = entry
        log.info("  Rider %d: %d rate entries", rid, len(rider_entries))

    rump: dict[str, dict] = {}
    for trid in sorted(discovered):
        entry = discovered[trid]
        entry["decision"] = None
        entry["master_charge"] = None
        entry["master_type"] = None
        rump[str(trid)] = entry

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        json.dump(rump, f, indent=2, default=str)
        f.write("\n")

    log.info("Wrote %d entries to %s", len(rump), output)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Fetch monthly volumetric rates for all classified charges from Genability."
    )
    parser.add_argument(
        "--utility", required=True, help="Utility shortcode (e.g. coned, rie)"
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Discovery mode: enumerate all active rates and write a rump charge_decisions JSON.",
    )
    parser.add_argument(
        "--effective-date",
        metavar="YYYY-MM-DD",
        help="Effective date for discovery mode (e.g. 2025-01-01)",
    )
    parser.add_argument(
        "--start",
        metavar="YYYY-MM",
        help="Start month inclusive (e.g. 2025-01)",
    )
    parser.add_argument(
        "--end",
        metavar="YYYY-MM",
        help="End month inclusive (e.g. 2025-12)",
    )
    parser.add_argument(
        "--path-charge-list",
        type=Path,
        help="Path to <utility>_charge_decisions.json",
    )
    parser.add_argument("--output", type=Path, required=True, help="Output path")
    args = parser.parse_args()

    if args.discover:
        if not args.effective_date:
            raise SystemExit("--effective-date is required when using --discover")
        discover_charges(args.utility, args.effective_date, args.output)
        return

    if not args.start or not args.end or not args.path_charge_list:
        raise SystemExit(
            "--start, --end, and --path-charge-list are required in normal mode"
        )

    start_parts = args.start.split("-")
    end_parts = args.end.split("-")
    if len(start_parts) != 2 or len(end_parts) != 2:
        raise SystemExit("--start and --end must be YYYY-MM")
    start_year, start_month = int(start_parts[0]), int(start_parts[1])
    end_year, end_month = int(end_parts[0]), int(end_parts[1])
    if (start_year, start_month) > (end_year, end_month):
        raise SystemExit("--start must be <= --end")

    load_dotenv(Path(__file__).resolve().parents[3] / ".env")
    app_id = os.environ.get("ARCADIA_APP_ID")
    app_key = os.environ.get("ARCADIA_APP_KEY")
    if not app_id or not app_key:
        raise SystemExit("ARCADIA_APP_ID and ARCADIA_APP_KEY must be set")
    auth = (app_id, app_key)
    base_url = "https://api.genability.com"

    utility = args.utility.lower()
    if utility not in UTILITY_MASTER_TARIFF_IDS:
        raise SystemExit(
            f"Unknown utility {args.utility!r}. Known: {sorted(UTILITY_MASTER_TARIFF_IDS)}"
        )
    master_tariff_id = UTILITY_MASTER_TARIFF_IDS[utility]

    _ACTIVE_DECISIONS = {"add_to_drr", "add_to_srr", "already_in_drr"}

    with open(args.path_charge_list) as f:
        charge_list_raw = json.load(f)

    # Index ALL entries (active + excluded) for unrecognized-rate detection
    known_trids: set[int] = set()
    known_vrks: set[str] = set()
    known_names: set[str] = set()

    classified_charges: dict[int, dict] = {}
    excluded_entries: dict[int, dict] = {}
    rider_ids_for_trid: dict[int, int] = {}
    for key, info in charge_list_raw.items():
        decision = info.get("decision")
        if not decision:
            continue
        try:
            trid = int(key)
        except ValueError:
            continue
        known_trids.add(trid)
        if info.get("variableRateKey"):
            known_vrks.add(info["variableRateKey"])
        if info.get("variable_rate_key"):
            known_vrks.add(info["variable_rate_key"])
        for field in ("rate_name", "master_charge"):
            val = info.get(field, "")
            if val:
                known_names.add(val.lower().strip())

        if decision in _ACTIVE_DECISIONS:
            classified_charges[trid] = {
                "master_charge": info.get("master_charge", ""),
                "variableRateKey": info.get("variableRateKey")
                or info.get("variable_rate_key"),
                "rate_name": info.get("rate_name"),
                "decision": decision,
                "charge_unit": info.get("charge_unit"),
                "is_tiered": info.get("is_tiered", False),
                "rate_bands": info.get("rate_bands", []),
                "season": info.get("season"),
                "time_of_use": info.get("time_of_use"),
            }
            rider_tid = info.get("rider_id") or info.get("rider_tariff_id")
            if rider_tid:
                rider_ids_for_trid[trid] = int(rider_tid)
        else:
            excluded_entries[trid] = {
                "rate_name": (info.get("rate_name") or "").strip(),
                "decision": decision,
            }

    if not classified_charges:
        raise SystemExit(f"No active charges in {args.path_charge_list}.")

    # Each active trid gets its own slug (allows seasonal variants to coexist)
    slug_to_charge: dict[str, tuple[int, str]] = {}
    for trid, info in classified_charges.items():
        mc = info["master_charge"]
        base_slug = _slug(mc) or f"charge_{trid}"
        slug = base_slug
        idx = 0
        while slug in slug_to_charge:
            idx += 1
            slug = f"{base_slug}_{idx}"
        slug_to_charge[slug] = (trid, mc)

    trid_to_slug: dict[int, str] = {}
    vrk_to_slug: dict[str, str] = {}
    name_candidates: dict[str, list[str]] = {}
    for slug, (trid, _) in slug_to_charge.items():
        trid_to_slug[trid] = slug
        vrk = classified_charges[trid].get("variableRateKey")
        if vrk:
            vrk_to_slug[vrk] = slug
        rn = classified_charges[trid].get("rate_name")
        if rn and not vrk:
            name_candidates.setdefault(rn.lower().strip(), []).append(slug)
    name_to_slug: dict[str, str] = {
        rn: slugs[0] for rn, slugs in name_candidates.items() if len(slugs) == 1
    }

    unrecognized_rates: dict[tuple[str], set[str]] = defaultdict(set)

    # Per-trid tracking: monthly_rates (bands[0]) plus per-band rates for tiered
    charges_data: dict[str, dict] = {}
    for slug, (trid, mc) in slug_to_charge.items():
        ci = classified_charges[trid]
        cu = ci.get("charge_unit") or "$/kWh"
        entry: dict = {
            "tariff_rate_id": trid,
            "master_charge": mc,
            "rate_name": ci.get("rate_name", ""),
            "decision": ci["decision"],
            "charge_unit": cu,
            "monthly_rates": {},
            "band_monthly_rates": None,
            "season_meta": ci.get("season"),
            "tou_meta": ci.get("time_of_use"),
        }
        if ci.get("is_tiered") and ci.get("rate_bands"):
            entry["band_monthly_rates"] = [
                {"upper_limit": b.get("upper_limit"), "monthly_rates": {}}
                for b in ci["rate_bands"]
            ]
        charges_data[slug] = entry

    rider_id_to_slug: dict[int, str] = {}
    for trid, rid in rider_ids_for_trid.items():
        slug = trid_to_slug.get(trid)
        if slug and rid not in rider_id_to_slug:
            rider_id_to_slug[rid] = slug
    rider_ids_needed_set = set(rider_id_to_slug.keys())

    # -----------------------------------------------------------------------
    # Monthly fetch loop
    # -----------------------------------------------------------------------
    cur_year, cur_month = start_year, start_month
    first_month = True
    while (cur_year, cur_month) <= (end_year, end_month):
        from_dt, to_dt = _month_range(cur_year, cur_month)
        month_key = from_dt[:7]
        rate_map, unresolved_riders = _fetch_tariff_rates(
            base_url, auth, master_tariff_id, from_dt
        )

        # --- Match API entries to charge_decisions ---
        def _match_slug(api_trid: int, info: dict) -> str | None:
            slug = trid_to_slug.get(api_trid)
            if slug:
                return slug
            vrk = info.get("variableRateKey")
            if vrk:
                slug = vrk_to_slug.get(vrk)
                if slug:
                    return slug
            rn = info.get("rateName", "").lower().strip()
            if rn:
                return name_to_slug.get(rn)
            return None

        for api_trid, info in rate_map.items():
            if info.get("chargeType") != "CONSUMPTION_BASED":
                continue
            slug = _match_slug(api_trid, info)
            if not slug:
                continue
            resolved = _resolve_effective_rate(info["entries"], from_dt, to_dt)
            if resolved is not None:
                charges_data[slug]["monthly_rates"][month_key] = round(resolved, 10)
            # For tiered entries, capture all band rates
            bmr = charges_data[slug]["band_monthly_rates"]
            if bmr is not None:
                api_bands = info.get("rateBands", [])
                for band_idx, band in enumerate(api_bands):
                    if band_idx < len(bmr):
                        rate = band.get("rateAmount")
                        if rate is not None:
                            bmr[band_idx]["monthly_rates"][month_key] = round(rate, 10)
            # On first API call, capture season/TOU metadata from the live
            # response (enriches whatever was in charge_decisions).
            if first_month:
                cd = charges_data[slug]
                if cd["season_meta"] is None and info.get("season"):
                    cd["season_meta"] = info["season"]
                if cd["tou_meta"] is None and info.get("timeOfUse"):
                    cd["tou_meta"] = info["timeOfUse"]

        # --- Fixed per-customer charges ($/day or $/month) ---
        for api_trid, info in rate_map.items():
            if info.get("chargeType") == "CONSUMPTION_BASED":
                continue
            slug = _match_slug(api_trid, info)
            if not slug:
                continue
            cu = charges_data.get(slug, {}).get("charge_unit")
            if cu not in ("$/day", "$/month"):
                continue
            resolved = _resolve_effective_rate(info["entries"], from_dt, to_dt)
            if resolved is not None:
                charges_data[slug]["monthly_rates"][month_key] = round(resolved, 10)

        # --- Check for CONSUMPTION_BASED rates the charge map doesn't know about ---
        for api_trid, info in rate_map.items():
            if info.get("chargeType") != "CONSUMPTION_BASED":
                continue
            if api_trid in known_trids:
                continue
            vrk = info.get("variableRateKey") or ""
            if vrk and vrk in known_vrks:
                continue
            rn = (info.get("rateName") or "").lower().strip()
            if rn and rn in known_names:
                continue
            unrecognized_rates[(info.get("rateName", "?"),)].add(month_key)

        # --- Rider fallback for slugs not populated by the base tariff ---
        riders_to_fetch = unresolved_riders & rider_ids_needed_set
        fetched_count = 0
        for rid in riders_to_fetch:
            slug = rider_id_to_slug[rid]
            if month_key in charges_data[slug]["monthly_rates"]:
                continue
            try:
                rider_rates, _ = _fetch_tariff_rates(base_url, auth, rid, from_dt)
            except requests.HTTPError as exc:
                if first_month:
                    log.warning(
                        "Rider %d returned %s; skipping", rid, exc.response.status_code
                    )
                continue
            time.sleep(0.2)
            fetched_count += 1
            total = 0.0
            found = False
            for _trid, info in rider_rates.items():
                if info.get("chargeType") != "CONSUMPTION_BASED":
                    continue
                resolved = _resolve_effective_rate(info["entries"], from_dt, to_dt)
                if resolved is not None:
                    total += resolved
                    found = True
            if found:
                charges_data[slug]["monthly_rates"][month_key] = round(total, 10)

        if first_month and fetched_count:
            log.info(
                "Rider fallback: %d rider(s) fetched for unresolved rate(s)",
                fetched_count,
            )

        time.sleep(0.3)
        first_month = False
        cur_month += 1
        if cur_month > 12:
            cur_month = 1
            cur_year += 1

    if unrecognized_rates:
        log.warning(
            "Charge list may be stale: %d CONSUMPTION_BASED rate(s) in the API "
            "response are not in %s (under any decision). These could be charges "
            "added after the snapshot the charge list was built from:",
            len(unrecognized_rates),
            args.path_charge_list.name,
        )
        for (name,), months in sorted(unrecognized_rates.items()):
            log.warning(
                "  %s — seen in %d month(s): %s",
                name,
                len(months),
                ", ".join(sorted(months)),
            )

    # Fill missing months with 0.0 for active entries that the API returned
    # no rate data for.
    all_months: list[str] = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        all_months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1

    for info in charges_data.values():
        if not info["monthly_rates"]:
            info["monthly_rates"] = {mo: 0.0 for mo in all_months}
        if info["band_monthly_rates"]:
            for band in info["band_monthly_rates"]:
                if not band["monthly_rates"]:
                    band["monthly_rates"] = {mo: 0.0 for mo in all_months}

    # -----------------------------------------------------------------------
    # Build grouped YAML output
    # -----------------------------------------------------------------------
    out = _build_grouped_output(
        utility,
        master_tariff_id,
        args.start,
        args.end,
        charges_data,
        excluded_entries,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        yaml.safe_dump(
            out, f, default_flow_style=False, sort_keys=False, allow_unicode=True
        )


if __name__ == "__main__":
    main()

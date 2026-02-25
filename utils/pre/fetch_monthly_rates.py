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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Fetch monthly volumetric rates for all classified charges from Genability."
    )
    parser.add_argument(
        "--utility", required=True, help="Utility shortcode (e.g. coned, rie)"
    )
    parser.add_argument(
        "--start",
        required=True,
        metavar="YYYY-MM",
        help="Start month inclusive (e.g. 2025-01)",
    )
    parser.add_argument(
        "--end",
        required=True,
        metavar="YYYY-MM",
        help="End month inclusive (e.g. 2025-12)",
    )
    parser.add_argument(
        "--path-charge-list",
        type=Path,
        required=True,
        help="Path to <utility>_charge_decisions.json",
    )
    parser.add_argument("--output", type=Path, required=True, help="Output YAML path")
    args = parser.parse_args()

    start_parts = args.start.split("-")
    end_parts = args.end.split("-")
    if len(start_parts) != 2 or len(end_parts) != 2:
        raise SystemExit("--start and --end must be YYYY-MM")
    start_year, start_month = int(start_parts[0]), int(start_parts[1])
    end_year, end_month = int(end_parts[0]), int(end_parts[1])
    if (start_year, start_month) > (end_year, end_month):
        raise SystemExit("--start must be <= --end")

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
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

    with open(args.path_charge_list) as f:
        charge_list_raw = json.load(f)

    # Index all entries for unrecognized-rate detection
    known_trids: set[int] = set()
    known_vrks: set[str] = set()
    known_names: set[str] = set()

    classified_charges: dict[int, dict] = {}
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
        for field in ("rate_name", "master_charge"):
            val = info.get(field, "")
            if val:
                known_names.add(val.lower().strip())
        classified_charges[trid] = {
            "master_charge": info.get("master_charge", ""),
            "variableRateKey": info.get("variableRateKey"),
            "rate_name": info.get("rate_name"),
            "decision": decision,
        }
        if info.get("rider_id"):
            rider_ids_for_trid[trid] = int(info["rider_id"])

    if not classified_charges:
        raise SystemExit(f"No classified charges in {args.path_charge_list}.")

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
        if rn and not vrk and trid not in rider_ids_for_trid:
            name_candidates.setdefault(rn.lower().strip(), []).append(slug)
    name_to_slug: dict[str, str] = {
        rn: slugs[0] for rn, slugs in name_candidates.items() if len(slugs) == 1
    }

    unrecognized_rates: dict[tuple[str], set[str]] = defaultdict(set)

    charges_data: dict[str, dict] = {
        slug: {
            "tariff_rate_id": trid,
            "master_charge": mc,
            "decision": classified_charges[trid]["decision"],
            "monthly_rates": {},
        }
        for slug, (trid, mc) in slug_to_charge.items()
    }

    rider_id_to_slug: dict[int, str] = {}
    for trid, rid in rider_ids_for_trid.items():
        slug = trid_to_slug.get(trid)
        if slug and rid not in rider_id_to_slug:
            rider_id_to_slug[rid] = slug
    rider_ids_needed_set = set(rider_id_to_slug.keys())

    # Iterate over every month in [start, end]
    cur_year, cur_month = start_year, start_month
    first_month = True
    while (cur_year, cur_month) <= (end_year, end_month):
        from_dt, to_dt = _month_range(cur_year, cur_month)
        month_key = from_dt[:7]
        rate_map, unresolved_riders = _fetch_tariff_rates(
            base_url, auth, master_tariff_id, from_dt
        )

        # --- Base tariff: match by tariffRateId, variableRateKey, or rateName ---
        for api_trid, info in rate_map.items():
            if info.get("chargeType") != "CONSUMPTION_BASED":
                continue
            slug = trid_to_slug.get(api_trid)
            if not slug:
                vrk = info.get("variableRateKey")
                if vrk:
                    slug = vrk_to_slug.get(vrk)
            if not slug:
                rn = info.get("rateName", "").lower().strip()
                if rn:
                    slug = name_to_slug.get(rn)
            if not slug:
                continue
            resolved = _resolve_effective_rate(info["entries"], from_dt, to_dt)
            if resolved is None:
                continue
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

        # --- Rider fallback for slugs that were NOT populated by the base tariff ---
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

    # Drop charges that had no volumetric rates or only zero rates
    charges_data = {
        k: v
        for k, v in charges_data.items()
        if v["monthly_rates"] and any(r != 0.0 for r in v["monthly_rates"].values())
    }

    # Deduplicate: when the same master_charge has multiple slugs that have
    # overlapping months with near-identical rates (version-mismatch artifacts),
    # keep the slug with the most months.  Slugs with genuinely different rates
    # (e.g. two SBC program components) are preserved.
    mc_slugs: dict[str, list[str]] = defaultdict(list)
    for slug, info in charges_data.items():
        mc_slugs[info["master_charge"]].append(slug)

    for mc, slugs in mc_slugs.items():
        if len(slugs) <= 1:
            continue
        slugs_sorted = sorted(
            slugs, key=lambda s: -len(charges_data[s]["monthly_rates"])
        )
        to_remove: set[str] = set()
        for i, slug_a in enumerate(slugs_sorted):
            if slug_a in to_remove:
                continue
            rates_a = charges_data[slug_a]["monthly_rates"]
            for slug_b in slugs_sorted[i + 1 :]:
                if slug_b in to_remove:
                    continue
                rates_b = charges_data[slug_b]["monthly_rates"]
                overlap = set(rates_a) & set(rates_b)
                if not overlap:
                    continue
                same = all(abs(rates_a[m] - rates_b[m]) < 1e-8 for m in overlap)
                if same:
                    to_remove.add(slug_b)
        for slug in to_remove:
            del charges_data[slug]

    # Re-key slugs from clean master_charge names now that only the target
    # utility's charges remain.  Suffixes are only added when the same
    # master_charge genuinely has multiple populated entries (e.g. RG&E's
    # two SBC program components).
    clean: dict[str, dict] = {}
    mc_count: dict[str, int] = defaultdict(int)
    for info in charges_data.values():
        mc_count[info["master_charge"]] += 1
    mc_seen: dict[str, int] = defaultdict(int)
    for info in charges_data.values():
        mc = info["master_charge"]
        base = _slug(mc)
        if mc_count[mc] > 1:
            mc_seen[mc] += 1
            key = f"{base}_{mc_seen[mc]}"
        else:
            key = base
        clean[key] = info
    charges_data = clean

    out = {
        "utility": utility,
        "master_tariff_id": master_tariff_id,
        "start_month": args.start,
        "end_month": args.end,
        "charges": charges_data,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        yaml.safe_dump(
            out, f, default_flow_style=False, sort_keys=False, allow_unicode=True
        )


if __name__ == "__main__":
    main()

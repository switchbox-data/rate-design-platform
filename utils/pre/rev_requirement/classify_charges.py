#!/usr/bin/env python3
"""Classify discovered charges into charge_decisions.json format.

Reads a *_discovered.json (output of ``fetch_monthly_rates.py --discover``),
applies pattern-based classification rules derived from
``context/code/data/ny_residential_charges_in_bat.md`` and
   ``context/code/data/ri_residential_charges_in_bat.md``, plus utility-specific
zonal/overlap dedup logic, then writes a hydrated charge_decisions.json.

Supports all NY utilities (CenHud, ConEd, NiMo, NYSEG, O&R, PSEG-LI, RGE)
and RI (RIE).

Classification phases (applied per entry, first match wins):

1. **NiMo MFC special handling** — National Grid's MFC rider contains
   sub-entries whose variableRateKeys duplicate supply commodity or ESR
   rates.  These must be excluded to prevent double-counting.
2. **Zonal supply dedup** — ConEd (H/I/J), NiMo (6 zones), NYSEG
   (Regular/West/LHV) each have zone-specific supply commodity rates.
   One representative zone is kept; others get ``exclude_zonal``.
3. **General name-based matching** — regex rules on ``rate_name``
   map to ``(decision, master_charge, master_type)``.

Any entry that doesn't match a rule keeps ``decision: null`` and a
warning is printed.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ---------------------------------------------------------------------------
# Name-based classification rules
# ---------------------------------------------------------------------------
# Each tuple: (compiled regex on rate_name, decision, master_charge, master_type)
# Order matters: first match wins.  More specific patterns come first.

_NAME_RULES: list[tuple[re.Pattern[str], str, str, str]] = [
    # --- already_in_drr: base delivery ---
    (
        re.compile(r"^Customer Charge$", re.I),
        "already_in_drr",
        "Customer Charge",
        "Base delivery",
    ),
    (
        re.compile(r"^Service Charge$", re.I),
        "already_in_drr",
        "Customer Charge",
        "Base delivery",
    ),
    (
        re.compile(r"^Basic Service Charge$", re.I),
        "already_in_drr",
        "Customer Charge",
        "Base delivery",
    ),
    (
        re.compile(r"Billing and Payment Processing|Bill Issuance", re.I),
        "already_in_drr",
        "Billing & Payment Processing",
        "Base delivery",
    ),
    (
        re.compile(r"Make.Whole Energy", re.I),
        "already_in_drr",
        "Core Delivery Rate",
        "Base delivery",
    ),
    (
        re.compile(r"^Energy Delivery Charge$", re.I),
        "already_in_drr",
        "Core Delivery Rate",
        "Base delivery",
    ),
    (
        re.compile(r"^Energy Charge", re.I),
        "already_in_drr",
        "Core Delivery Rate",
        "Base delivery",
    ),
    (
        re.compile(r"^(Summer|Winter) Rate$", re.I),
        "already_in_drr",
        "Core Delivery Rate",
        "Base delivery",
    ),
    # RI: Distribution Charge (main volumetric delivery rate)
    (
        re.compile(r"^Distribution Charge$", re.I),
        "already_in_drr",
        "Core Delivery Rate",
        "Base delivery",
    ),
    # RI: O&M and CapEx components of base delivery (ISR plan)
    (
        re.compile(r"Operating.*Maintenance", re.I),
        "already_in_drr",
        "O&M Charge",
        "Base delivery",
    ),
    (
        re.compile(r"CapEx Factor", re.I),
        "already_in_drr",
        "CapEx Factor",
        "Base delivery",
    ),
    # RI: Pension Adjustment Factor (labor cost recovery in rate case)
    (
        re.compile(r"Pension Adjustment", re.I),
        "already_in_drr",
        "Pension Adjustment Factor",
        "Base delivery",
    ),
    # RI: Transmission Charge (FERC/ISO-NE OATT pass-through)
    (
        re.compile(r"^Transmission Charge$", re.I),
        "already_in_drr",
        "Transmission Charge",
        "Base delivery",
    ),
    (
        re.compile(r"^Minimum( Charge)?$", re.I),
        "exclude_redundant",
        "Minimum Charge",
        "Bill floor (rarely binds)",
    ),
    # --- exclude_trueup: cost reconciliation + revenue true-ups ---
    (
        re.compile(r"Monthly Adjustment Clause", re.I),
        "exclude_trueup",
        "Delivery cost true-up (MAC / RAM / DSA)",
        "Cost recon",
    ),
    (
        re.compile(r"^Reconciliation Rate$", re.I),
        "exclude_trueup",
        "MAC sub-components",
        "Cost recon",
    ),
    (
        re.compile(r"^Uncollectible Bill Expense$", re.I),
        "exclude_trueup",
        "MAC sub-components",
        "Cost recon",
    ),
    (
        re.compile(r"Transition (Adjustment|Charge)", re.I),
        "exclude_trueup",
        "Transition / Restructuring",
        "Cost recon",
    ),
    (
        re.compile(r"Legacy Transition Charge", re.I),
        "exclude_trueup",
        "Transition / Restructuring",
        "Cost recon",
    ),
    (
        re.compile(r"Rate Adjustment Mechanism", re.I),
        "exclude_trueup",
        "Delivery cost true-up (MAC / RAM / DSA)",
        "Cost recon",
    ),
    (
        re.compile(r"Delivery Service Adjustment", re.I),
        "exclude_trueup",
        "Delivery cost true-up (MAC / RAM / DSA)",
        "Cost recon",
    ),
    (
        re.compile(r"Transmission Revenue Adjustment", re.I),
        "exclude_trueup",
        "Transmission Revenue Adjustment",
        "Cost recon",
    ),
    (
        re.compile(r"Net Utility Plant", re.I),
        "exclude_trueup",
        "Net Utility Plant / Depreciation Recon",
        "Cost recon",
    ),
    (
        re.compile(r"Purchased Power Adjustment", re.I),
        "exclude_trueup",
        "Purchased Power Adjustment",
        "Cost recon",
    ),
    (
        re.compile(r"MSC I+\s*Adjustment", re.I),
        "exclude_trueup",
        "Supply cost true-up",
        "Cost recon",
    ),
    (
        re.compile(r"Energy Cost Adjustment", re.I),
        "exclude_trueup",
        "Supply cost true-up",
        "Cost recon",
    ),
    (
        re.compile(r"Market Price Adjustment", re.I),
        "exclude_trueup",
        "Supply cost true-up",
        "Cost recon",
    ),
    (
        re.compile(r"Electricity Supply Reconciliation", re.I),
        "exclude_trueup",
        "Supply cost true-up",
        "Cost recon",
    ),
    (
        re.compile(r"Reliability Support Service", re.I),
        "exclude_trueup",
        "Reliability Support Service",
        "Cost recon",
    ),
    # RI: O&M / CapEx reconciliation factors (ISR true-ups)
    (
        re.compile(r"O&?M Reconciliation", re.I),
        "exclude_trueup",
        "O&M Reconciliation Factor",
        "Cost recon",
    ),
    (
        re.compile(r"CapEx Reconciliation", re.I),
        "exclude_trueup",
        "CapEx Reconciliation Factor",
        "Cost recon",
    ),
    # RI: Last Resort Adjustment Factor (LRS supply cost true-up)
    (
        re.compile(r"Last Resort Adjustment", re.I),
        "exclude_trueup",
        "Last Resort Adjustment Factor",
        "Cost recon",
    ),
    (
        re.compile(r"Revenue Decoupling|RDM Adjustment", re.I),
        "exclude_trueup",
        "Revenue Decoupling (RDM)",
        "Revenue true-up",
    ),
    (
        re.compile(r"Delivery Revenue Surcharge", re.I),
        "exclude_trueup",
        "Delivery Revenue Surcharge / Electric Bill Credit",
        "Revenue true-up",
    ),
    (
        re.compile(r"Electric Bill Credit", re.I),
        "exclude_trueup",
        "Delivery Revenue Surcharge / Electric Bill Credit",
        "Revenue true-up",
    ),
    # --- exclude_negligible: performance incentive ---
    (
        re.compile(r"Earnings Adjustment Mechanism", re.I),
        "exclude_negligible",
        "Earnings Adjustment Mechanism",
        "Performance incentive",
    ),
    (
        re.compile(r"Performance Incentive", re.I),
        "exclude_negligible",
        "Performance Incentive Factor",
        "Performance incentive",
    ),
    # --- exclude_trueup: tax ($/kWh variants; QUANTITY/% ones hit exclude_percentage first) ---
    (re.compile(r"^GRT\b", re.I), "exclude_trueup", "GRT", "Tax pass-through"),
    (
        re.compile(r"Gross Earnings Tax", re.I),
        "exclude_trueup",
        "Gross Earnings Tax",
        "Tax pass-through",
    ),
    (
        re.compile(r"Rate for Cities and Incorporated Villages", re.I),
        "exclude_trueup",
        "PILOTs (Cities/Villages)",
        "Tax pass-through",
    ),
    # --- exclude_expired ---
    (
        re.compile(r"Tax Sur.Credit", re.I),
        "exclude_expired",
        "Tax Sur-Credit",
        "Expired",
    ),
    # --- add_to_drr: program surcharges ---
    (
        re.compile(r"System Benefits Charge", re.I),
        "add_to_drr",
        "System Benefits Charge",
        "Program surcharge",
    ),
    (
        re.compile(r"Clean Energy Standard Delivery", re.I),
        "add_to_drr",
        "CES Delivery",
        "Program surcharge",
    ),
    # NYSEG calls CES Delivery just "Clean Energy Standard Surcharge" (DISTRIBUTION class)
    (
        re.compile(r"^Clean Energy Standard Surcharge$", re.I),
        "add_to_drr",
        "CES Delivery",
        "Program surcharge",
    ),
    (
        re.compile(r"New York State (Surcharge|Assessment)", re.I),
        "add_to_drr",
        "NY State Surcharge (\u00a718-a)",
        "Program surcharge",
    ),
    (
        re.compile(r"Dynamic Load Management", re.I),
        "add_to_drr",
        "DLM Surcharge",
        "Program surcharge",
    ),
    (
        re.compile(r"Electric Vehicle Make.?Ready", re.I),
        "add_to_drr",
        "EV Make Ready",
        "Program surcharge",
    ),
    (
        re.compile(r"Energy Storage", re.I),
        "add_to_drr",
        "Energy Storage Surcharge",
        "Program surcharge",
    ),
    (
        re.compile(r"Miscellaneous Charges", re.I),
        "add_to_drr",
        "Central Hudson Misc Charges",
        "Program surcharge",
    ),
    # RI: Energy Efficiency Programs Charge (Least Cost Procurement)
    (
        re.compile(r"Energy Efficiency Program", re.I),
        "add_to_drr",
        "Energy Efficiency Programs Charge",
        "Program surcharge",
    ),
    # RI: Net Metering Charge (DER credit recovery)
    (
        re.compile(r"Net Metering", re.I),
        "add_to_drr",
        "Net Metering Charge",
        "DER credit recovery",
    ),
    # RI: Long Term Contracting Charge (offshore wind / renewable PPAs)
    (
        re.compile(r"Long Term Contracting", re.I),
        "add_to_drr",
        "Long Term Contracting Charge",
        "Program surcharge",
    ),
    # RI: RE Growth Charge (distributed gen program)
    (
        re.compile(r"RE Growth", re.I),
        "add_to_drr",
        "RE Growth Charge",
        "Program surcharge",
    ),
    # RI: LIHEAP Enhancement Charge (low-income energy assistance supplement)
    (
        re.compile(r"LIHEAP", re.I),
        "add_to_drr",
        "LIHEAP Enhancement Charge",
        "Program surcharge",
    ),
    # RI: Storm Fund Replenishment Factor
    (
        re.compile(r"Storm Fund", re.I),
        "add_to_drr",
        "Storm Fund Replenishment Factor",
        "Sunk-cost recovery",
    ),
    # --- add_to_drr: sunk-cost recovery ---
    (
        re.compile(r"Arrear(s|age)", re.I),
        "add_to_drr",
        "Arrears / Arrearage recovery",
        "Sunk-cost recovery",
    ),
    (
        re.compile(r"Late Payment.*Waived|Late Payment Charge", re.I),
        "add_to_drr",
        "Late Payment / Waived Fees",
        "Sunk-cost recovery",
    ),
    (
        re.compile(r"Recovery Charge", re.I),
        "add_to_drr",
        "Recovery Charge (storm bonds)",
        "Sunk-cost recovery",
    ),
    (
        re.compile(r"Shoreham Property Tax", re.I),
        "add_to_drr",
        "Shoreham Property Tax Settlement",
        "Sunk-cost recovery",
    ),
    # --- add_to_drr: DER credit recovery ---
    (
        re.compile(
            r"VDER|DER Cost Recovery|Distributed Energy Resources Cost Recovery", re.I
        ),
        "add_to_drr",
        "VDER / DER Cost Recovery",
        "DER credit recovery",
    ),
    # --- add_to_srr: supply commodity ---
    (
        re.compile(r"^MSC Rate", re.I),
        "add_to_srr",
        "Supply commodity (bundled)",
        "Supply commodity",
    ),
    (
        re.compile(r"Market Supply Charge", re.I),
        "add_to_srr",
        "Supply commodity (bundled)",
        "Supply commodity",
    ),
    (
        re.compile(r"Market Price Charge", re.I),
        "add_to_srr",
        "Supply commodity (bundled)",
        "Supply commodity",
    ),
    (
        re.compile(r"Power Supply Charge", re.I),
        "add_to_srr",
        "Supply commodity (bundled)",
        "Supply commodity",
    ),
    (
        re.compile(r"^Supply (Service )?Charge$", re.I),
        "add_to_srr",
        "Supply commodity (bundled)",
        "Supply commodity",
    ),
    (
        re.compile(r"^Electri(city|c) Supply Charge", re.I),
        "add_to_srr",
        "Supply commodity (bundled)",
        "Supply commodity",
    ),
    # RI: Standard Offer Service (Last Resort Service bundled supply)
    (
        re.compile(r"Standard Offer Service", re.I),
        "add_to_srr",
        "Supply commodity (bundled)",
        "Supply commodity",
    ),
    # --- add_to_srr: securitization ---
    (
        re.compile(r"Securitization", re.I),
        "add_to_srr",
        "Securitization Charge / Offset (UDSA)",
        "Sunk-cost recovery",
    ),
    # --- add_to_srr: CES supply ---
    (
        re.compile(r"Clean Energy Standard Supply", re.I),
        "add_to_srr",
        "CES Supply Surcharge",
        "CES supply",
    ),
    (
        re.compile(r"Renewable Energy Credit", re.I),
        "add_to_srr",
        "CES Supply Surcharge",
        "CES supply",
    ),
    (
        re.compile(r"Zero Emission Credit", re.I),
        "add_to_srr",
        "CES Supply Surcharge",
        "CES supply",
    ),
    # RI: Renewable Standard Energy Charge (REC obligation per MWh)
    (
        re.compile(r"Renewable (Standard )?Energy", re.I),
        "add_to_srr",
        "Renewable Standard Energy Charge",
        "RES supply",
    ),
    # --- add_to_srr: merchant function ---
    (
        re.compile(r"Merchant Function Charge", re.I),
        "add_to_srr",
        "Merchant Function Charge",
        "Merchant function",
    ),
    (
        re.compile(r"Allocation of MFC", re.I),
        "add_to_srr",
        "Merchant Function Charge",
        "Merchant function",
    ),
    (
        re.compile(r"Base MFC Supply", re.I),
        "add_to_srr",
        "Merchant Function Charge",
        "Merchant function",
    ),
    (
        re.compile(r"MFC Admin", re.I),
        "add_to_srr",
        "Merchant Function Charge",
        "Merchant function",
    ),
    (
        re.compile(r"Electricity Supply Procurement", re.I),
        "add_to_srr",
        "Merchant Function Charge",
        "Merchant function",
    ),
    (
        re.compile(r"Electricity Supply Credit and Collection", re.I),
        "add_to_srr",
        "Merchant Function Charge",
        "Merchant function",
    ),
    # RI: LRS Administrative Cost Adjustment Factor
    (
        re.compile(r"LRS Administrative|Last Resort.*Admin", re.I),
        "add_to_srr",
        "LRS Administrative Cost",
        "Merchant function",
    ),
    # --- exclude_eligibility: eligibility-gated / optional ($0 for default customer) ---
    (
        re.compile(
            r"Customer Benefit Contribution|Costumer Benefit Contribution", re.I
        ),
        "exclude_eligibility",
        "CBC (solar only)",
        "Eligibility / optional",
    ),
    (
        re.compile(
            r"Low[\s-]Income|Income Eligible|Enhanced Energy Affordability", re.I
        ),
        "exclude_eligibility",
        "Low-income discounts",
        "Eligibility",
    ),
    (
        re.compile(r"Residential Agricultural Discount", re.I),
        "exclude_eligibility",
        "RAD (agricultural)",
        "Eligibility",
    ),
    (
        re.compile(r"GreenUp", re.I),
        "exclude_eligibility",
        "GreenUp",
        "Eligibility / optional",
    ),
]


# ---------------------------------------------------------------------------
# Zonal dedup configuration
# ---------------------------------------------------------------------------
# For each utility with zone-specific supply charges, define:
#   - VRK prefix(es) that identify zonal entries
#   - The substring in the VRK that marks the representative zone
#   - What the non-representative zones should be marked as

_ZONAL_SUPPLY_VRKS: dict[str, list[tuple[str, str]]] = {
    # utility → [(vrk_prefix, representative_zone_substring), ...]
    "coned": [("marketSupplyChargeResidential", "ZoneH")],
    "nimo": [
        ("electricSupplyChargeSC1", "Central"),
        ("electricSupplyChargeSc1DeliveryAdj", "Central"),
    ],
    "nyseg": [
        ("supplyCharge", "Regular"),
    ],
}

# For NiMo delivery adj entries, override the base name-rule classification
_NIMO_DELIVERY_ADJ_VRK = "electricSupplyChargeSc1DeliveryAdj"


# ---------------------------------------------------------------------------
# NiMo MFC overlap configuration
# ---------------------------------------------------------------------------
# NiMo's MFC rider sub-entries that share VRKs with supply commodity or ESR
# rates.  Including both the base rate and the MFC rate would double-count.

_NIMO_MFC_SUPPLY_VRK_PREFIX = "electricSupplyChargeSC1"
_NIMO_MFC_ESR_VRK_PREFIX = "esrMechanismSC1"
_NIMO_MFC_CES_VRK_PREFIX = "cleanEnergyStandardSupply"
_NIMO_REPRESENTATIVE_ZONE = "Central"


def _classify_nimo_mfc(
    entry: dict,
) -> tuple[str, str, str, str | None]:
    """Classify a NiMo MFC rider entry, handling VRK overlaps and zonal dedup.

    Returns (decision, master_charge, master_type, _exclude_reason_or_None).
    """
    vrk = entry.get("variable_rate_key") or ""
    name = (entry.get("rate_name") or "").strip()
    mc = "Merchant Function Charge"
    mt = "Merchant function"

    # MFC entries whose VRK matches supply commodity → exclude_zonal (VRK overlap)
    if vrk.startswith(_NIMO_MFC_SUPPLY_VRK_PREFIX):
        return (
            "exclude_zonal",
            mc,
            mt,
            "MFC electricSupply VRK duplicates supply commodity rates; "
            "excluding to avoid double-count",
        )

    # MFC entries whose VRK matches CES Supply → exclude_zonal (VRK overlap)
    if vrk.startswith(_NIMO_MFC_CES_VRK_PREFIX):
        return (
            "exclude_zonal",
            mc,
            mt,
            "MFC CES Supply VRK duplicates CES Supply base rate; "
            "excluding to avoid double-count",
        )

    # MFC entries whose VRK matches ESR → zonal handling
    if vrk.startswith(_NIMO_MFC_ESR_VRK_PREFIX):
        zone_suffix = vrk[len(_NIMO_MFC_ESR_VRK_PREFIX) :]
        if zone_suffix == _NIMO_REPRESENTATIVE_ZONE:
            if "Working Capital" in name:
                return ("add_to_srr", mc, mt, None)
            # Uncollectible Expense for Central → same VRK as Working Capital
            return (
                "exclude_zonal",
                mc,
                mt,
                "Duplicate rider entry (same VRK as another MFC entry)",
            )
        return ("exclude_zonal", mc, mt, None)

    # Non-overlap MFC entries (Procurement, Credit & Collection) → add_to_srr
    return ("add_to_srr", mc, mt, None)


def _check_zonal_supply(
    entry: dict,
    utility: str,
) -> tuple[str, str, str] | None:
    """If the entry is a zone-specific supply charge, return classification.

    Returns (decision, master_charge, master_type) or None if not zonal.
    """
    vrk = entry.get("variable_rate_key") or ""
    if not vrk or utility not in _ZONAL_SUPPLY_VRKS:
        return None

    for vrk_prefix, rep_zone in _ZONAL_SUPPLY_VRKS[utility]:
        if not vrk.startswith(vrk_prefix):
            continue

        zone_suffix = vrk[len(vrk_prefix) :]
        if not zone_suffix:
            continue

        is_representative = rep_zone in zone_suffix

        # Determine master_charge/master_type based on whether this is a
        # delivery adjustment or a supply charge
        if utility == "nimo" and vrk.startswith(_NIMO_DELIVERY_ADJ_VRK):
            mc, mt = "Delivery Charge Adjustment", "Base delivery"
            base_decision = "already_in_drr"
        else:
            mc, mt = "Supply commodity (bundled)", "Supply commodity"
            base_decision = "add_to_srr"

        if is_representative:
            return (base_decision, mc, mt)
        return ("exclude_zonal", mc, mt)

    return None


def _classify_by_name(entry: dict) -> tuple[str, str, str] | None:
    """Apply name-based regex rules.  Returns first match or None."""
    name = (entry.get("rate_name") or "").strip()
    for pattern, decision, master_charge, master_type in _NAME_RULES:
        if pattern.search(name):
            return (decision, master_charge, master_type)
    return None


def classify_entry(
    entry: dict,
    utility: str,
) -> dict:
    """Classify a single discovered entry and return the hydrated dict."""
    # Phase 0: Percentage-of-bill charges cannot be fetched as monthly rates
    # by the current pipeline (fetch_monthly_rates.py handles $/kWh, $/month,
    # $/day only).  Override any substantive classification so the decision
    # honestly reflects the pipeline limitation.
    if entry.get("charge_type") == "QUANTITY" and entry.get("charge_unit") == "%":
        return {
            **entry,
            "decision": "exclude_percentage",
            "master_charge": (entry.get("rate_name") or "").strip(),
            "master_type": "Percentage of bill",
        }

    rate_group = (entry.get("rate_group_name") or "").strip()

    # Phase 1: NiMo MFC special handling
    if utility == "nimo" and rate_group == "Merchant Function Charge":
        decision, mc, mt, reason = _classify_nimo_mfc(entry)
        result = {**entry, "decision": decision, "master_charge": mc, "master_type": mt}
        if reason:
            result["_exclude_reason"] = reason
        return result

    # Phase 2: Zonal supply dedup
    zonal = _check_zonal_supply(entry, utility)
    if zonal:
        decision, mc, mt = zonal
        return {**entry, "decision": decision, "master_charge": mc, "master_type": mt}

    # Phase 3: General name-based matching
    match = _classify_by_name(entry)
    if match:
        decision, mc, mt = match
        return {**entry, "decision": decision, "master_charge": mc, "master_type": mt}

    # No match — leave decision null, warn
    return {**entry}


def classify_charges(utility: str, input_path: Path, output_path: Path) -> None:
    """Read discovered JSON, classify each entry, write charge_decisions JSON."""
    data = json.loads(input_path.read_text())
    logging.info("Read %d entries from %s", len(data), input_path)

    result: dict[str, dict] = {}
    unclassified = []

    for rate_id, entry in data.items():
        classified = classify_entry(entry, utility)
        result[rate_id] = classified
        if classified.get("decision") is None:
            unclassified.append((rate_id, (entry.get("rate_name") or "").strip()))

    if unclassified:
        logging.warning(
            "%d entries could not be classified for %s:", len(unclassified), utility
        )
        for rate_id, name in unclassified:
            logging.warning("  %s: %s", rate_id, name)

    # Phase 4 (post-classification): zone dedup.  When multiple entries share
    # the same rate_name + decision + charge_class + rate_group_name AND have
    # no distinguishing season or TOU metadata, they are zone duplicates
    # (e.g. ConEd zones H/I/J each having a "Summer Rate" entry).  Keep one
    # representative; mark the rest exclude_zonal.
    #
    # Entries WITH distinct season or TOU metadata are NOT deduped — they
    # represent genuinely different seasonal/TOU variants that the output YAML
    # will merge into a combined charge entry.
    _ACTIVE_DECISIONS = {"add_to_drr", "add_to_srr", "already_in_drr"}
    seen_keys: dict[tuple, str] = {}
    dedup_count = 0
    for rate_id in sorted(result):
        entry = result[rate_id]
        decision = entry.get("decision")
        if decision not in _ACTIVE_DECISIONS:
            continue
        season_name = ""
        if entry.get("season") and isinstance(entry["season"], dict):
            season_name = (entry["season"].get("name") or "").lower()
        tou_type = ""
        if entry.get("time_of_use") and isinstance(entry["time_of_use"], dict):
            tou_type = (
                entry["time_of_use"].get("type")
                or entry["time_of_use"].get("name")
                or ""
            ).lower()
        key = (
            (entry.get("rate_name") or "").strip(),
            decision,
            (entry.get("charge_class") or "").strip(),
            (entry.get("rate_group_name") or "").strip(),
            season_name,
            tou_type,
        )
        if key in seen_keys:
            entry["decision"] = "exclude_zonal"
            entry["_exclude_reason"] = (
                f"Duplicate of tariffRateId {seen_keys[key]} "
                f"(same rate_name/decision/charge_class/rate_group_name"
                f"/season/tou)"
            )
            dedup_count += 1
        else:
            seen_keys[key] = rate_id

    if dedup_count:
        logging.info(
            "Post-classification dedup: %d entries marked exclude_zonal for %s",
            dedup_count,
            utility,
        )

    from collections import Counter

    counts = Counter(v.get("decision") for v in result.values())
    logging.info(
        "Classification for %s: %s (total %d)",
        utility,
        dict(sorted(counts.items(), key=lambda x: x[0] or "zzz")),
        len(result),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    logging.info("Wrote %s", output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--utility", required=True, help="Utility key (e.g. coned)")
    parser.add_argument(
        "--input", required=True, type=Path, help="Path to *_discovered.json"
    )
    parser.add_argument(
        "--output", required=True, type=Path, help="Path to write charge_decisions.json"
    )
    args = parser.parse_args()

    if not args.input.exists():
        logging.error("Input file not found: %s", args.input)
        sys.exit(1)

    classify_charges(args.utility, args.input, args.output)


if __name__ == "__main__":
    main()

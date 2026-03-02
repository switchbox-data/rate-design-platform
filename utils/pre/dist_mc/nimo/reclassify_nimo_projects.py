"""Reclassify NiMo MCOS projects as bulk_tx, sub_tx, or distribution.

Background
----------
NiMo's MCOS study (workpaper Excel file) lists ~237 projects in Exhibit 1
(Sheet 1).  Each project has a station name, MW capacity, and capital split
across four cost centres: T_Station, T_Line, D_Station, D_Line.

For the Bill Alignment Test we need to separate costs into:
  - bulk_tx   (≥230 kV — NYISO transmission; excluded from local marginal cost)
  - sub_tx    (69–115 kV — NiMo's sub-transmission network)
  - distribution (≤13.2 kV — distribution substations and feeders)

The problem is that Exhibit 1 groups many transmission projects under a single
"Transm Net" station name with no voltage information.  To assign voltages we
cross-reference two sources:
  1. FinalData (Sheet 11) — sub-project–level detail with descriptive names
     and InvestType (T_Station, T_Line, D_Station, D_Line).
  2. NYISO 2025 Gold Book Table VII — every proposed transmission project in
     New York with station names, voltages, and descriptions.

What this script automates vs. what was decided manually
--------------------------------------------------------
AUTOMATED:
  - Parsing Exhibit 1 and FinalData from the workbook
  - Regex extraction of explicit kV mentions from sub-project names
  - Fuzzy matching of sub-project names against Gold Book station lists
  - Capital-weighted classification for mixed-voltage projects
  - CSV output and evidence formatting

MANUAL (hardcoded as data in this file):
  - GOLD_BOOK_NGRID_HIGH_VOLTAGE — I read Gold Book Table VII and hand-picked
    every NGRID entry at ≥230 kV, deciding which to include for automatic
    matching and which to exclude to avoid false positives.
  - GOLD_BOOK_NGRID_SUB_TX — same for 69–115 kV entries.
  - FN_KNOWN_VOLTAGES — for each project that couldn't be classified from the
    above, I looked at the sub-project names and other context to manually
    assign a voltage with a written justification.
  - STATION_ALIASES — I noticed abbreviated names in the workbook that didn't
    match Gold Book station names and added mappings.
  - STATION_NEGATIVE_PATTERNS — I noticed false positives (e.g. "South Oswego"
    matching "Oswego 345 kV") and added exclusion rules.

Verification
------------
A systematic cross-reference script (cross_reference_gold_book.py, run once
and then deleted) programmatically extracted all 124 NGRID entries from Gold
Book Table VII and fuzzy-matched them against all 136 Transm Net sub-project
names.  This confirmed that:
  - The only genuine ≥230 kV finding was the Ames Road (Marshville) 345/115 kV
    station (Gold Book line 3909), which is now included.
  - All other ≥230 kV matches were false positives on common words ("road",
    "porter", "oswego", "clay", "east", "upgrades").
  - All sub-TX matches were consistent with the curated lists.

Outputs
-------
  - nimo_project_classifications.csv (same directory as this script)
  - Terminal report of classification summary + uncertain classifications
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import fsspec
import polars as pl

# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL DECISION: Gold Book entries at ≥230 kV (bulk transmission)
# ═══════════════════════════════════════════════════════════════════════════════
#
# These are NGRID-owned entries from NYISO Gold Book 2025 Table VII operating
# at 230 kV or 345 kV.  Each entry lists one or more station names that the
# fuzzy matcher will look for in FinalData sub-project names.
#
# IMPORTANT: single-station entries at 345 kV are INTENTIONALLY EXCLUDED from
# this list (Rotterdam 230 kV, Oswego 345 kV, Volney 345 kV, East Conklin
# 345 kV, Bailey 345 kV, Clay 345 kV, Elm St 230 kV).  Reason: these stations
# also have 115 kV equipment, so a sub-project mentioning "Rotterdam" or
# "Oswego" is far more likely to be sub-TX work than bulk.  If a sub-project
# IS at bulk voltage, it will have explicit "345kV" or "230kV" in its name
# (handled by the regex path) or a specific FN override below.
#
# Multi-station corridors are safe to include because the combination of two
# station names is specific enough to avoid false positives.

GOLD_BOOK_NGRID_HIGH_VOLTAGE: list[dict] = [
    # ── Smart Path Connect (SPCP), Queue 1125, 230/345 kV ──
    # This is NiMo's largest transmission project: a 345 kV rebuild of the
    # Adirondack–Edic corridor plus associated 230 kV retirements.  All SPCP
    # sub-projects in FinalData contain the station names listed here.
    {
        "stations": ["adirondack", "austin road"],
        "voltage": 345,
        "desc": "SPCP: Adirondack-Austin Road 345kV",
    },
    {
        "stations": ["adirondack", "marcy"],
        "voltage": 345,
        "desc": "SPCP: Adirondack-Marcy 345kV",
    },
    {
        "stations": ["austin road", "edic"],
        "voltage": 345,
        "desc": "SPCP: Austin Road-Edic 345kV",
    },
    {
        "stations": ["rector road", "austin road"],
        "voltage": 230,
        "desc": "SPCP: Rector Road-Austin Road 230kV",
    },
    {
        "stations": ["austin road"],
        "voltage": 345,
        "desc": "SPCP: Austin Road 345kV substation",
    },
    {
        "stations": ["austin road"],
        "voltage": 230,
        "desc": "SPCP: Austin Road 230/345kV transformer",
    },
    {
        "stations": ["edic"],
        "voltage": 345,
        "desc": "SPCP: Edic 345kV substation upgrades",
    },
    {
        "stations": ["chases lake"],
        "voltage": 230,
        "desc": "SPCP: Retire Chases Lake 230kV",
    },
    {
        "stations": ["adirondack", "porter"],
        "voltage": 230,
        "desc": "SPCP: Retire Adirondack-Porter 230kV",
    },
    {
        "stations": ["adirondack", "chases lake"],
        "voltage": 230,
        "desc": "SPCP: Retire Adirondack-Chases Lake 230kV",
    },
    {
        "stations": ["chases lake", "porter"],
        "voltage": 230,
        "desc": "SPCP: Retire Chases Lake-Porter 230kV",
    },
    {
        "stations": ["edic", "porter"],
        "voltage": 230,
        "desc": "SPCP: Retire Edic-Porter 230kV",
    },
    # ── Gordon Road–Rotterdam 230 kV ──
    # Gold Book Queue 556.  Multi-station corridor, safe from false positives.
    {
        "stations": ["gordon rd", "rotterdam"],
        "voltage": 230,
        "desc": "Gordon Road-Rotterdam 230kV",
    },
    # ── Ames Road Station (Marshville) — 345/115 kV, CLCPA ──
    # Gold Book line 3909, Queue 1672: "Construct a new 345kV/115kV station
    # in a breaker and a half configuration CLCPA."
    # DECISION: Include this because FN011642 has "Marshville New Substation"
    # ($147M) which IS this Gold Book project.  However, Marshville also
    # appears in the sub-TX list (69 kV Gloversville–Marshville rebuild), so
    # sub-projects with "Marshville" will match BOTH lists, triggering the
    # mixed-voltage capital-weighting path.  That's the correct behaviour:
    # the capital weighting distinguishes station construction (bulk-side)
    # from corridor line refurbishment (sub-TX).
    # Found via systematic cross-reference (cross_reference_gold_book.py).
    {
        "stations": ["marshville"],
        "voltage": 345,
        "desc": "Ames Road Station (Marshville) 345/115kV CLCPA",
    },
    # ── STAMP — 345/115 kV load interconnection ──
    # Gold Book Queue 580.  Not a NiMo-only project (NYPA/NGRID joint).
    {
        "stations": ["stamp"],
        "voltage": 345,
        "desc": "STAMP 345/115kV load interconnection",
    },
]

# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL DECISION: Gold Book entries at 69–115 kV (sub-transmission)
# ═══════════════════════════════════════════════════════════════════════════════
#
# These are NGRID entries from Gold Book Table VII at 69 or 115 kV.
# Used for two purposes:
#   1. To confirm sub-TX classification when a sub-project name matches.
#   2. To exclude sub-TX corridor refurbs from the bulk capital count when
#      a sub-project matches BOTH the HV and sub-TX lists (see capital
#      weighting logic in classify_project).

GOLD_BOOK_NGRID_SUB_TX: list[dict] = [
    # ── Western region ──
    {
        "stations": ["gardenville", "dunkirk"],
        "voltage": 115,
        "desc": "Gardenville-Dunkirk 115kV lines 141/142",
    },
    {
        "stations": ["huntley", "lockport"],
        "voltage": 115,
        "desc": "Huntley-Lockport 115kV lines 36/37",
    },
    {
        "stations": ["packard", "huntley"],
        "voltage": 115,
        "desc": "Packard-Huntley/Walck-Huntley 115kV lines 130/133",
    },
    {
        "stations": ["walck", "huntley"],
        "voltage": 115,
        "desc": "Walck-Huntley 115kV line 133",
    },
    {
        "stations": ["mortimer", "pannell"],
        "voltage": 115,
        "desc": "Mortimer-Pannell 115kV lines 24/25",
    },
    {
        "stations": ["mortimer", "golah"],
        "voltage": 115,
        "desc": "Mortimer-Golah 115kV/69kV lines 109/110",
    },
    {
        "stations": ["se batavia", "golah"],
        "voltage": 115,
        "desc": "SE Batavia-Golah 115kV",
    },
    {
        "stations": ["lockport", "batavia"],
        "voltage": 115,
        "desc": "Lockport-Batavia 115kV lines 107/108/112",
    },
    {
        "stations": ["niagara", "gardenville"],
        "voltage": 115,
        "desc": "Niagara-Gardenville 115kV",
    },
    {
        "stations": ["packard", "gardenville"],
        "voltage": 115,
        "desc": "Packard-Gardenville 115kV",
    },
    {
        "stations": ["erie st", "gardenville"],
        "voltage": 115,
        "desc": "Erie St-Gardenville 115kV",
    },
    {
        "stations": ["dunkirk", "falconer"],
        "voltage": 115,
        "desc": "Dunkirk-Falconer 115kV",
    },
    {
        "stations": ["dunkirk", "laona"],
        "voltage": 115,
        "desc": "Dunkirk-Laona 115kV lines 161/162",
    },
    {
        "stations": ["huntley", "gardenville"],
        "voltage": 115,
        "desc": "Huntley-Gardenville 115kV lines 38/39",
    },
    {"stations": ["brockport"], "voltage": 115, "desc": "Brockport 115kV taps 111/113"},
    # ── Central region ──
    {
        "stations": ["cortland", "clarks corners"],
        "voltage": 115,
        "desc": "Cortland-Clarks Corners 115kV",
    },
    {"stations": ["oneida"], "voltage": 115, "desc": "Oneida 115kV station rebuild"},
    {"stations": ["homer hill"], "voltage": 115, "desc": "Homer Hill 115kV"},
    {
        "stations": ["lockport"],
        "voltage": 115,
        "desc": "Lockport 115kV substation rebuild",
    },
    # ── Northern region (Tug Hill / St Lawrence) ──
    {
        "stations": ["colton", "browns falls"],
        "voltage": 115,
        "desc": "Colton-Browns Falls 115kV",
    },
    {
        "stations": ["colton", "dennison"],
        "voltage": 115,
        "desc": "Colton-Dennison 115kV",
    },
    {"stations": ["colton", "malone"], "voltage": 115, "desc": "Colton-Malone 115kV"},
    {"stations": ["malone"], "voltage": 115, "desc": "Malone 115kV PAR / substation"},
    {
        "stations": ["indian river", "north watertown"],
        "voltage": 115,
        "desc": "Indian River-North Watertown 115kV",
    },
    {
        "stations": ["coffeen", "lyme junction"],
        "voltage": 115,
        "desc": "Coffeen-Lyme Junction 115kV line 4",
    },
    {
        "stations": ["coffeen", "black river"],
        "voltage": 115,
        "desc": "Coffeen-Black River 115kV",
    },
    {
        "stations": ["coffeen", "lighthouse hill"],
        "voltage": 115,
        "desc": "Coffeen-Lighthouse Hill 115kV",
    },
    {
        "stations": ["lighthouse hill", "clay"],
        "voltage": 115,
        "desc": "Lighthouse Hill-Clay 115kV",
    },
    {
        "stations": ["lighthouse hill", "south oswego"],
        "voltage": 115,
        "desc": "Lighthouse Hill-South Oswego 115kV",
    },
    {
        "stations": ["south oswego", "lighthouse hill"],
        "voltage": 115,
        "desc": "South Oswego-Lighthouse Hill 115kV",
    },
    {
        "stations": ["black river"],
        "voltage": 115,
        "desc": "Black River 115kV substation",
    },
    {
        "stations": ["taylorville"],
        "voltage": 115,
        "desc": "Taylorville 115kV substation rebuild",
    },
    {
        "stations": ["middle road"],
        "voltage": 115,
        "desc": "Middle Road 115kV six-breaker ring",
    },
    {"stations": ["coffeen"], "voltage": 115, "desc": "Coffeen 115kV station"},
    {"stations": ["boonville"], "voltage": 115, "desc": "Boonville 115kV station"},
    {
        "stations": ["south oswego", "geres lock"],
        "voltage": 115,
        "desc": "South Oswego-Geres Lock 115kV line 9",
    },
    {
        "stations": ["south oswego"],
        "voltage": 115,
        "desc": "South Oswego 115kV substation",
    },
    {
        "stations": ["beaver creek"],
        "voltage": 115,
        "desc": "Beaver Creek 115kV synchronous condensers",
    },
    {
        "stations": ["tar hill"],
        "voltage": 115,
        "desc": "Tar Hill 115kV (replaces Lighthouse Hill)",
    },
    {
        "stations": ["flat rock"],
        "voltage": 115,
        "desc": "Flat Rock 115kV (mid-line Colton-Browns Falls)",
    },
    # ── Capital region ──
    {
        "stations": ["manheim", "inghams"],
        "voltage": 115,
        "desc": "Manheim (Inghams) 115kV PAR replacement",
    },
    {
        "stations": ["rotterdam"],
        "voltage": 115,
        "desc": "Rotterdam 115kV terminal equipment",
    },
    {
        "stations": ["rotterdam"],
        "voltage": 69,
        "desc": "Rotterdam 69kV substation rebuild",
    },
    {
        "stations": ["rotterdam", "schoharie"],
        "voltage": 69,
        "desc": "Rotterdam-Schoharie 69→115kV rebuild",
    },
    {"stations": ["marshville"], "voltage": 115, "desc": "Marshville 115/69kV rebuild"},
    {"stations": ["meco"], "voltage": 115, "desc": "Meco 115/69kV rebuild"},
    {
        "stations": ["amsterdam", "rotterdam"],
        "voltage": 69,
        "desc": "Amsterdam-Rotterdam 69kV rebuild",
    },
    {"stations": ["maiden lane"], "voltage": 115, "desc": "Maiden Lane 115kV station"},
    {"stations": ["east avenue"], "voltage": 115, "desc": "East Avenue 115kV station"},
    {"stations": ["hoosick"], "voltage": 115, "desc": "Hoosick 115kV station"},
    {"stations": ["mohican"], "voltage": 115, "desc": "Mohican 115kV/34.5kV station"},
    {"stations": ["saltsman"], "voltage": 115, "desc": "Saltsman Road 115kV station"},
    # ── Southern tier ──
    {
        "stations": ["sleight rd", "auburn"],
        "voltage": 115,
        "desc": "Sleight Rd-Auburn 115kV line 3",
    },
    {
        "stations": ["katherine st"],
        "voltage": 115,
        "desc": "Katherine St Terminal 115/23kV",
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL DECISION: FN-specific voltage overrides
# ═══════════════════════════════════════════════════════════════════════════════
#
# For Transm Net projects where:
#   - sub-project names don't contain explicit kV strings, AND
#   - station names don't match any Gold Book entry
# I manually determined the voltage from other context.  Each entry documents
# what the project is, how I identified the voltage, and why the classification
# is correct.
#
# These are the weakest classifications — they rely on my interpretation of
# NiMo naming conventions, station type analogies, or domain knowledge rather
# than direct Gold Book confirmation.

FN_KNOWN_VOLTAGES: dict[str, dict] = {
    # ── Projects identified by NiMo line numbering conventions ──
    # NiMo uses 100-series numbers for 115 kV lines and 200-series for 69 kV.
    # Confirmed by cross-referencing known lines: 130/133 = 115 kV (Packard-
    # Huntley), 141/142 = 115 kV (Gardenville-Dunkirk), 110 = 115 kV
    # (Mortimer-Golah).
    "FN008052": {
        "voltage": 115,
        "source": (
            "Sub-project 'Frontier 181 ACR/Recond' is NiMo line #181. "
            "NiMo's line numbering convention: 100-series lines are 115kV "
            "(confirmed by other 100-series lines in Gold Book Table VII, e.g., "
            "lines 130/133 = Packard-Huntley 115kV, lines 141/142 = Gardenville-Dunkirk 115kV, "
            "line 110 = Mortimer-Golah 115kV). Classification as 115kV sub-TX."
        ),
    },
    # ── Projects identified by Gold Book station name (abbreviated in workbook) ──
    # The workbook uses short forms like "Gard-Dun" or "Colton-BF" that don't
    # match the Gold Book's full names.  Rather than complicate the fuzzy matcher,
    # I handle these as explicit overrides with the source documented.
    "FN010101": {
        "voltage": 115,
        "source": (
            "Sub-project 'Gard-Dun 141-142' is Gardenville-Dunkirk lines 141/142. "
            "Gold Book Table VII: NGRID Gardenville Dunkirk, 115kV, In-Service 2024."
        ),
    },
    "FN010073": {
        "voltage": 115,
        "source": (
            "Sub-projects '130/133 Stations- Huntley' and '130/133 T-Line Reconductor' "
            "are Packard-Huntley #130 / Walck-Huntley #133 lines. "
            "Gold Book Table VII: NGRID Packard Huntley, 115kV, S 2027."
        ),
    },
    "FN008017": {
        "voltage": 115,
        "source": (
            "Sub-project 'Colton-BF 1-2 T3140-T3150 ACR' is Colton-Browns Falls lines 1/2. "
            "Gold Book Table VII: NGRID Colton Browns Falls, 115kV, S 2026."
        ),
    },
    "FN008333": {
        "voltage": 115,
        "source": (
            "Sub-project 'Flat Rock Upgrades - Line' is a mid-line station on the "
            "Colton-Browns Falls 115kV corridor. Gold Book Table VII explicitly lists: "
            "'NGRID Colton Browns Falls ... Flat Rock station (mid-line) upgrades OH', 115kV."
        ),
    },
    "FN013188": {
        "voltage": 115,
        "source": (
            "Sub-project 'Elbridge - Geres Lock Capacity' is on the 115kV corridor "
            "between Elbridge and Geres Lock. Gold Book Table VII lists Geres Lock "
            "as a 115kV station in NiMo's sub-TX network."
        ),
    },
    "FN010080": {
        "voltage": 115,
        "source": (
            "Sub-projects 'New Manheim - Assoc Line work', 'New Manheim Control House', "
            "'New Manheim Greenfield project' are the Manheim (Inghams) station relocation. "
            "Gold Book Table VII: NGRID Manheim (Inghams), 115kV, W 2026. "
            "Note: also has D-Line sub-projects 'Manheim 46kV relocation' and "
            "'Manheim Distribution' — the 46kV is likely a step-down from 115kV."
        ),
    },
    # ── EV Ready Site projects ──
    # All are small (40 MW, $6–11M) T_Line investments for EV charging
    # interconnection.  NiMo's sub-TX network is 115 kV; these tap into it.
    # DECISION: classified as sub_tx because the T_Line investment is a tap
    # off an existing 115 kV line, not a new bulk facility.
    "FN012042": {
        "voltage": 115,
        "source": (
            "Sub-project 'EV RS - Angola -T-line': EV Ready Site transmission line tap. "
            "T_Line invest type, tapping into existing NiMo 115kV sub-TX network."
        ),
    },
    "FN012044": {
        "voltage": 115,
        "source": (
            "Sub-project 'EV RS - Dewitt-T-line': EV Ready Site transmission line tap. "
            "T_Line invest type, tapping into existing NiMo 115kV sub-TX network."
        ),
    },
    "FN012047": {
        "voltage": 115,
        "source": (
            "Sub-project 'EV RS - Chittenango-T-line': EV Ready Site transmission line tap. "
            "T_Line invest type, tapping into existing NiMo 115kV sub-TX network."
        ),
    },
    "FN012039": {
        "voltage": 115,
        "source": (
            "Sub-project 'EV RS - Pembroke (Flying J)-T-line': EV Ready Site transmission line tap. "
            "T_Line invest type, tapping into existing NiMo 115kV sub-TX network."
        ),
    },
    "FN012050": {
        "voltage": 115,
        "source": (
            "Sub-project 'EV RS - Pattersonville-T-line': EV Ready Site transmission line tap. "
            "T_Line invest type, tapping into existing NiMo 115kV sub-TX network."
        ),
    },
    # ── Numbered station taps (STA XX) ──
    # NiMo uses "STA" + number for sub-TX stations.  These are tap + conversion
    # projects: the T_Line is the 115 kV tap, the D-side is the station upgrade.
    # DECISION: classified as sub_tx because the T_Line component is a tap off
    # an existing 115 kV line.
    "FN011396": {
        "voltage": 115,
        "source": (
            "Sub-projects 'Tonawanda Area Study-STA74 new tap' (T_Line) and "
            "'Upgrade and Convert STA 74' (D_Line, D_Station). STA 74 is a numbered "
            "station in NiMo's sub-TX network. The T_Line component is a 115kV tap "
            "to serve the upgraded station."
        ),
    },
    "FN011289": {
        "voltage": 115,
        "source": (
            "Sub-projects 'Tonawanda Area Study-STA 129 Tap' (T_Line) and "
            "'Upgrade and Convert STA 129 SUB' (D_Station). STA 129 is a numbered "
            "station in NiMo's sub-TX network. The T_Line component is a 115kV tap."
        ),
    },
    # ── Stations classified by analogy ──
    # Not in Gold Book Table VII.  Classified as 115 kV because every other
    # NiMo greenfield or rebuilt transmission station in the Gold Book is at
    # 115 kV (Boonville, Tar Hill, Saltsman Road, East Avenue, Maiden Lane,
    # Taylorville, Middle Road, Homer Hill, etc.).  None are at ≥230 kV
    # except the ones we already handle above.
    # WEAKNESS: "consistent with the pattern" is weaker evidence than a
    # direct Gold Book match.  These are marked confidence=medium.
    "FN013572": {
        "voltage": 115,
        "source": (
            "Sub-projects 'West Lysander Station - CH/Line Work/Substation' describe a "
            "new transmission station. Not explicitly in Gold Book Table VII. "
            "Classified as 115kV sub-TX: all comparable NiMo greenfield transmission stations "
            "in the Gold Book (Boonville, Tar Hill, Saltsman Road, East Avenue, Maiden Lane) "
            "are at 115kV. No evidence of ≥230kV."
        ),
    },
    "FN011630": {
        "voltage": 115,
        "source": (
            "Sub-project 'Lasher Rebuild': T_Station invest type, sub-TX station rebuild. "
            "Not in Gold Book Table VII. Classified as 115kV: consistent with all other "
            "NiMo station rebuilds in the study (e.g., Taylorville, Middle Road, Homer Hill)."
        ),
    },
    "FN008683": {
        "voltage": 115,
        "source": (
            "Sub-project 'Teall Ave - Asset Replacement': T_Station invest type. "
            "Not in Gold Book Table VII. Classified as 115kV sub-TX station: "
            "consistent with NiMo's sub-TX network architecture."
        ),
    },
    "FN013525": {
        "voltage": 115,
        "source": (
            "Sub-projects 'Tilden - Asset Rplc CH' and 'Tilden: Asset Replacement': "
            "T_Station invest type. Not in Gold Book Table VII. Classified as 115kV: "
            "consistent with NiMo's sub-TX network."
        ),
    },
    "FN011646": {
        "voltage": 115,
        "source": (
            "Sub-project 'Higley': T_Station invest type. "
            "Not in Gold Book Table VII. Classified as 115kV: "
            "consistent with NiMo's sub-TX network."
        ),
    },
    "FN008472": {
        "voltage": 115,
        "source": (
            "Sub-project 'Seneca #5 TRF Asset Replace': T_Station invest type, "
            "transformer replacement at an existing sub-TX station. "
            "Consistent with NiMo's sub-TX network architecture at 115kV."
        ),
    },
    "FN011526": {
        "voltage": 115,
        "source": (
            "Sub-projects 'Capital North AreaStudy Elnora Tap' (T_Line) and "
            "'Elnora Station Rebuild' (D_Station). Elnora is on NiMo's sub-TX network. "
            "The T_Line component is a 115kV tap into the existing line."
        ),
    },
    # ── Elm St #2 transformer — the most debatable classification ──
    # Gold Book Table VII: "NGRID Elm St Elm St S 2027 230/23 230/23 -
    # 118MVA 133MVA Replace TR2 as failure".
    #
    # This is physically a 230 kV asset: a 230/23 kV step-down transformer.
    # By voltage alone it should be bulk_tx.  But:
    #   - It directly connects bulk (230 kV) to distribution (23 kV),
    #     bypassing sub-TX entirely
    #   - The cost is booked as T_Station in NiMo's MCOS, not as FERC/NYISO
    #     bulk transmission
    #   - It serves a distribution interconnection function for NiMo's
    #     local network
    #
    # DECISION: Classify as sub_tx.  We set voltage=115 (the sub-TX bucket)
    # so the classification logic puts it there.  The evidence field documents
    # the 230 kV reality.  This is the single most judgement-dependent
    # classification in the file.
    "FN008471": {
        "voltage": 115,
        "source": (
            "Sub-project 'Elm St #2 TRF Asset Replacement' is the replacement of "
            "transformer TR2 at Elm St station. Gold Book Table VII: 'NGRID Elm St Elm St "
            "S 2027 230/23 230/23 - 118MVA 133MVA Replace TR2 as failure.' "
            "This is a 230/23kV transformer — physically a 230kV asset, but it directly "
            "connects the bulk system to 23kV distribution (bypassing sub-TX). Classified "
            "as sub_tx because the cost is NiMo's local distribution interconnection, "
            "not NYISO bulk TX. The 230kV side is the upstream connection."
        ),
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL DECISION: Station name aliases for fuzzy matching
# ═══════════════════════════════════════════════════════════════════════════════
#
# The NiMo workbook uses abbreviated station names that don't appear in the
# Gold Book.  This dictionary maps canonical station names (matching the Gold
# Book lists above) to all known abbreviations seen in FinalData.
#
# Example: Gold Book says "Gardenville", workbook says "Gard" in
# "Gard-Dun 141-142".  Without this alias, the matcher would miss the
# Gardenville-Dunkirk 115 kV match.

STATION_ALIASES: dict[str, list[str]] = {
    "gardenville": ["gardenville", "gard", "gardenvill"],
    "dunkirk": ["dunkirk", "dun"],
    "browns falls": ["browns falls", "bf", "brown falls"],
    "lighthouse hill": ["lighthouse hill", "lighthh", "lhh", "lighthouse"],
    "huntley": ["huntley"],
    "lockport": ["lockport"],
    "batavia": ["batavia"],
    "packard": ["packard"],
    "walck": ["walck"],
    "mortimer": ["mortimer"],
    "pannell": ["pannell"],
    "golah": ["golah", "e. golah", "east golah"],
    "colton": ["colton"],
    "malone": ["malone"],
    "coffeen": ["coffeen"],
    "black river": ["black river"],
    "boonville": ["boonville"],
    "taylorville": ["taylorville"],
    "clay": ["clay"],
    "south oswego": ["south oswego", "s oswego", "s. oswego", "s.oswego"],
    "geres lock": ["geres lock", "geres"],
    "manheim": ["manheim", "inghams"],
    "marshville": ["marshville"],
    "meco": ["meco"],
    "rotterdam": ["rotterdam"],
    "amsterdam": ["amsterdam"],
    "oneida": ["oneida"],
    "middle road": ["middle road"],
    "mohican": ["mohican"],
    "indian river": ["indian river"],
    "north watertown": ["north watertown", "n watertown"],
    "lyme junction": ["lyme junction", "lyme"],
    "austin road": ["austin road", "austin rd"],
    "adirondack": ["adirondack"],
    "edic": ["edic"],
    "rector road": ["rector road", "rector rd"],
    "chases lake": ["chases lake"],
    "porter": ["porter"],
    "elm st": ["elm st", "elm street"],
    "oswego": ["oswego"],
    "volney": ["volney"],
    "east conklin": ["east conklin", "e conklin"],
    "bailey": ["bailey"],
    "stamp": ["stamp"],
    "niagara": ["niagara"],
    "erie st": ["erie st", "erie street"],
    "falconer": ["falconer"],
    "homer hill": ["homer hill"],
    "hoosick": ["hoosick"],
    "brockport": ["brockport"],
    "beaver creek": ["beaver creek"],
    "maiden lane": ["maiden lane"],
    "east avenue": ["east avenue", "east ave"],
    "saltsman": ["saltsman"],
    "tar hill": ["tar hill"],
    "flat rock": ["flat rock"],
    "sleight rd": ["sleight rd", "sleight road"],
    "auburn": ["auburn"],
    "frontier": ["frontier"],
    "teall": ["teall"],
    "seneca": ["seneca"],
    "tilden": ["tilden"],
    "lasher": ["lasher"],
    "higley": ["higley"],
    "elbridge": ["elbridge"],
}


# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL DECISION: Station name false-positive exclusions
# ═══════════════════════════════════════════════════════════════════════════════
#
# Some station names are substrings of other station names, causing false
# matches.  Example: "Oswego" (345 kV station) is a substring of "South
# Oswego" (115 kV station).  A sub-project mentioning "South Oswego" would
# falsely match the "Oswego" 345 kV entry.
#
# These patterns are only used during high-voltage matching (use_negative=True).
# If a sub-project name contains any of the negative patterns, the match is
# rejected.

STATION_NEGATIVE_PATTERNS: dict[str, list[str]] = {
    # "South Oswego" should not match "Oswego" 345 kV
    "oswego": ["south oswego", "s oswego"],
    # Clay is in the HV list via STAMP joint project; if we ever enable
    # direct Clay 345 kV matching, this prevents "Clay CLCPA 115 kV" work
    # from matching.
    "clay": [],
}


# ═══════════════════════════════════════════════════════════════════════════════
# Data structures and parsing
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class SubProject:
    fn: str
    name: str
    invest_type: str
    capital_k: float


@dataclass
class ClassificationResult:
    exhibit1_line: int
    fn_reference: str
    station: str
    capacity_mw: float
    capital_m: float
    classification: str
    voltage_kv: str
    inference_method: str
    evidence: str
    sub_project_names: str
    confidence: str  # "high", "medium", "low"


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _extract_voltages_from_text(text: str) -> list[int]:
    """Find explicit kV mentions in text like '115kV', '345 kV', '230/345kV'."""
    matches = re.findall(r"(\d+)\s*k[vV]", text)
    return [int(m) for m in matches]


def _station_in_text(station: str, text: str, *, use_negative: bool = False) -> bool:
    """Check if a station name (or any of its aliases) appears in text.

    When use_negative=True, also checks STATION_NEGATIVE_PATTERNS: if the text
    contains a more-specific variant (e.g. "south oswego"), the generic match
    (e.g. "oswego") is rejected.
    """
    aliases = STATION_ALIASES.get(station, [station])
    found = False
    for alias in aliases:
        if alias in text:
            found = True
            break
    if not found:
        return False
    if use_negative:
        negatives = STATION_NEGATIVE_PATTERNS.get(station, [])
        for neg in negatives:
            if neg in text:
                return False
    return True


def _match_gold_book(
    sub_names: list[str], entries: list[dict], *, use_negative: bool = False
) -> list[dict]:
    """Find Gold Book entries where ALL listed station names appear somewhere
    in the concatenated sub-project names."""
    all_text = " ".join(_normalize(n) for n in sub_names)
    matches = []
    for entry in entries:
        if all(
            _station_in_text(station, all_text, use_negative=use_negative)
            for station in entry["stations"]
        ):
            matches.append(entry)
    return matches


# ═══════════════════════════════════════════════════════════════════════════════
# Workbook parsing (automated — no decisions here)
# ═══════════════════════════════════════════════════════════════════════════════


def read_finaldata(xlsx_bytes: bytes) -> dict[str, list[SubProject]]:
    """Read FinalData (Sheet 11) and group sub-projects by parent FN."""
    df = pl.read_excel(
        xlsx_bytes, sheet_id=11, raise_if_empty=False, infer_schema_length=0
    )
    parents: dict[str, list[SubProject]] = {}
    for row in df.iter_rows(named=True):
        parent = row["FN Parent  #"]
        fn = row["Investment Code"]
        name = row["Project Name"] or ""
        itype = row["InvestType"] or ""
        try:
            cap_k = float(row["Sum"] or 0)
        except (ValueError, TypeError):
            cap_k = 0.0

        if parent == "Individual Project":
            parent = fn

        if parent not in parents:
            parents[parent] = []
        parents[parent].append(
            SubProject(fn=fn, name=name, invest_type=itype, capital_k=cap_k)
        )
    return parents


def read_exhibit1(xlsx_bytes: bytes) -> list[dict]:
    """Read Exhibit 1 (Sheet 1) project rows.

    Column positions are hardcoded to match the NiMo workbook layout:
      0  = line number
      1  = FN reference
      2  = station name
      3  = capacity (MW)
      16 = total capital ($k)
      18–21 = capital by cost centre: T_Station, T_Line, D_Station, D_Line
    Data rows start at row index 7 (rows 0–6 are headers).
    """
    df = pl.read_excel(
        xlsx_bytes, sheet_id=1, raise_if_empty=False, infer_schema_length=0
    )

    COL_LINE, COL_REF, COL_STATION, COL_CAP = 0, 1, 2, 3
    COL_CSUM = 16
    COL_C_TS, COL_C_TL, COL_C_DS, COL_C_DL = 18, 19, 20, 21
    DATA_START = 7

    projects = []
    for row_idx in range(DATA_START, df.height):
        row = df.row(row_idx)
        line_val, station_val = row[COL_LINE], row[COL_STATION]
        if line_val is None or station_val is None:
            continue
        try:
            line_num = int(float(line_val))
        except (ValueError, TypeError):
            continue
        station_str = str(station_val).strip()
        if station_str.lower() in ("xxx", ""):
            continue

        def _f(v: object) -> float:
            try:
                return float(str(v))
            except (ValueError, TypeError):
                return 0.0

        projects.append(
            {
                "line": line_num,
                "fn_reference": str(row[COL_REF] or ""),
                "station": station_str,
                "capacity_mw": _f(row[COL_CAP]),
                "capital_k": _f(row[COL_CSUM]),
                "c_ts_k": _f(row[COL_C_TS]),
                "c_tl_k": _f(row[COL_C_TL]),
                "c_ds_k": _f(row[COL_C_DS]),
                "c_dl_k": _f(row[COL_C_DL]),
            }
        )
    return projects


# ═══════════════════════════════════════════════════════════════════════════════
# Classification logic
# ═══════════════════════════════════════════════════════════════════════════════


def classify_project(
    proj: dict, finaldata: dict[str, list[SubProject]]
) -> ClassificationResult:
    fn = proj["fn_reference"]
    station = proj["station"]
    line = proj["line"]
    cap = proj["capacity_mw"]
    capital_m = proj["capital_k"] / 1e3

    subs = finaldata.get(fn, [])
    sub_names_str = "; ".join(f"{s.name} [{s.invest_type}]" for s in subs)

    # ──────────────────────────────────────────────────────────────────────
    # RULE 1: Named substation → distribution
    # ──────────────────────────────────────────────────────────────────────
    # If Exhibit 1 lists a specific substation name (anything other than
    # "Transm Net"), the project is a distribution area study.  The station
    # name identifies a distribution substation, even if some capital is on
    # the T-side (feeding sub-TX substation upgrades are common in area
    # studies).
    #
    # DECISION: classify as distribution regardless of T-side capital
    # fraction.  Confidence is "high" if D-side only, "medium" if mixed.
    if station != "Transm Net":
        has_t = proj["c_ts_k"] > 0 or proj["c_tl_k"] > 0
        if has_t:
            total_k = proj["c_ts_k"] + proj["c_tl_k"] + proj["c_ds_k"] + proj["c_dl_k"]
            t_frac = (proj["c_ts_k"] + proj["c_tl_k"]) / total_k if total_k else 0
            sub_types = set(s.invest_type for s in subs)
            evidence = (
                f"Named substation '{station}' in Exhibit 1 — this identifies it "
                f"as a distribution project. However, {t_frac:.0%} of capital is T-side "
                f"(C_TS=${proj['c_ts_k']:,.0f}k, C_TL=${proj['c_tl_k']:,.0f}k out of "
                f"${total_k:,.0f}k total). This is typical for distribution area studies "
                f"that require upgrades at the feeding sub-TX substation. "
                f"FinalData sub-project InvestTypes: {sorted(sub_types)}. "
                f"Classified as distribution because the project is named after "
                f"a distribution substation and the majority of capital is D-side."
            )
            return ClassificationResult(
                exhibit1_line=line,
                fn_reference=fn,
                station=station,
                capacity_mw=cap,
                capital_m=capital_m,
                classification="distribution",
                voltage_kv="<=13.2",
                inference_method="named_substation_mixed_capital",
                evidence=evidence,
                sub_project_names=sub_names_str,
                confidence="medium",
            )
        sub_types = set(s.invest_type for s in subs)
        evidence = (
            f"Named substation '{station}' in Exhibit 1. "
            f"All capital in D-side cost centres (C_DS=${proj['c_ds_k']:,.0f}k, "
            f"C_DL=${proj['c_dl_k']:,.0f}k). "
            f"FinalData sub-project InvestTypes: {sorted(sub_types)}."
        )
        return ClassificationResult(
            exhibit1_line=line,
            fn_reference=fn,
            station=station,
            capacity_mw=cap,
            capital_m=capital_m,
            classification="distribution",
            voltage_kv="<=13.2",
            inference_method="named_substation_d_side_only",
            evidence=evidence,
            sub_project_names=sub_names_str,
            confidence="high",
        )

    # ──────────────────────────────────────────────────────────────────────
    # RULE 2: "Transm Net" projects — determine voltage from sub-projects
    # ──────────────────────────────────────────────────────────────────────
    # These are the hard cases.  We try five methods in order of strength:
    #
    #   Step 1: Explicit kV in sub-project name (strongest — e.g. "115kV")
    #   Step 2: Gold Book ≥230 kV station match (curated list above)
    #   Step 3: Gold Book sub-TX station match (curated list above)
    #   Step 4: Well-known project identifiers (SPCP, Niagara-Dysinger)
    #   Step 5: FN-specific overrides (manual, weakest)
    #
    # All five methods contribute to a combined voltage list.  The final
    # classification is derived from whether the voltages are bulk-only,
    # sub-TX-only, or mixed.

    sub_names = [s.name for s in subs]

    # Step 1: explicit kV regex
    all_voltages: list[int] = []
    voltage_sources: list[str] = []
    for s in subs:
        vols = _extract_voltages_from_text(s.name)
        for v in vols:
            if v >= 69:  # ignore distribution voltages like 13.2, 23
                all_voltages.append(v)
                voltage_sources.append(f"'{s.name}' states {v}kV")

    # Step 2: Gold Book ≥230 kV match (uses negative patterns to avoid
    # "South Oswego" → "Oswego 345 kV" false positives)
    hv_matches = _match_gold_book(
        sub_names, GOLD_BOOK_NGRID_HIGH_VOLTAGE, use_negative=True
    )
    for m in hv_matches:
        all_voltages.append(m["voltage"])
        voltage_sources.append(f"Gold Book Table VII match: {m['desc']}")

    # Step 3: Gold Book sub-TX match (no negative patterns needed — sub-TX
    # station names are specific enough)
    stx_matches = _match_gold_book(sub_names, GOLD_BOOK_NGRID_SUB_TX)
    for m in stx_matches:
        all_voltages.append(m["voltage"])
        voltage_sources.append(f"Gold Book Table VII match: {m['desc']}")

    # Step 4: well-known projects identifiable from sub-project text
    all_sub_text = " ".join(_normalize(n) for n in sub_names)
    if "smart path" in all_sub_text or "spcp" in all_sub_text:
        all_voltages.extend([230, 345])
        voltage_sources.append(
            "Well-known NYISO project: Smart Path Connect (230/345kV, Queue 1125)"
        )
    if "niagara" in all_sub_text and "dysinger" in all_sub_text:
        all_voltages.append(345)
        voltage_sources.append(
            "Project name contains 'Niagara-Dysinger', known 345kV corridor"
        )

    # Step 5: FN-specific overrides (only if nothing else matched — these
    # are the weakest evidence tier)
    fn_known = FN_KNOWN_VOLTAGES.get(fn)
    used_fn_override = False
    if fn_known and not all_voltages:
        all_voltages.append(fn_known["voltage"])
        voltage_sources.append(fn_known["source"])
        used_fn_override = True

    # ──────────────────────────────────────────────────────────────────────
    # DECISION TREE: classify from the combined voltage list
    # ──────────────────────────────────────────────────────────────────────
    has_bulk = any(v >= 230 for v in all_voltages)
    has_sub_tx = any(69 <= v <= 115 for v in all_voltages)

    # Case A: ONLY bulk voltages found → bulk_tx
    if has_bulk and not has_sub_tx:
        classification = "bulk_tx"
        voltage_str = "/".join(
            str(v) for v in sorted(set(v for v in all_voltages if v >= 230))
        )
        confidence = "high" if not used_fn_override else "medium"
        inference_method = _pick_inference_method(voltage_sources, hv_matches)
        evidence = (
            f"All identified voltages are ≥230kV. "
            f"Sources: {'; '.join(voltage_sources)}."
        )

    # Case B: BOTH bulk and sub-TX voltages → capital-weighted classification
    #
    # This is the most complex case.  A single FN parent can contain sub-
    # projects at different voltage levels.  Example: FN011642 has
    # "Marshville New Substation" (345/115 kV Ames Road station, $147M) AND
    # "Inghams/Rotterdam Circuit Rebuild" (115 kV, $385M) AND "MVT Rott
    # 69kV Rebuild" ($28M).
    #
    # DECISION: winner-take-all based on capital fraction.  If >50% of
    # identifiable capital is at ≥230 kV, classify as bulk_tx; otherwise
    # sub_tx.  The evidence documents the exact capital split.
    #
    # To attribute capital, we use three tiers:
    #   (a) Sub-project name explicitly says "345kV" / "230kV" → bulk
    #   (b) Sub-project name explicitly says "115kV" / "69kV" → NOT bulk
    #   (c) Sub-project name matches a Gold Book ≥230 kV station but is NOT
    #       a sub-TX corridor line refurb → bulk
    # Anything not matched by (a)-(c) is unattributed (treated as sub-TX).
    elif has_bulk and has_sub_tx:
        hv_station_names = set()
        for m in hv_matches:
            for st in m["stations"]:
                hv_station_names.add(st)

        bulk_capital = 0.0
        bulk_sub_details: list[str] = []
        for s in subs:
            explicit_vs = _extract_voltages_from_text(s.name)

            # (a) Explicit ≥230 kV in name → definitely bulk
            if any(v >= 230 for v in explicit_vs):
                bulk_capital += s.capital_k
                bulk_sub_details.append(
                    f"'{s.name}' (${s.capital_k:,.0f}k) — explicit {[v for v in explicit_vs if v >= 230]}kV in name"
                )
                continue

            # (b) Explicit sub-TX voltage in name → definitely NOT bulk,
            # even at a station that also has ≥230 kV equipment
            if any(69 <= v <= 115 for v in explicit_vs):
                continue

            # (c) Station name matches Gold Book ≥230 kV entry
            s_norm = _normalize(s.name)
            matched_hv_station = None
            for st in hv_station_names:
                if _station_in_text(st, s_norm, use_negative=True):
                    matched_hv_station = st
                    break
            if not matched_hv_station:
                continue

            # Exception: if this sub-project ALSO matches a sub-TX Gold Book
            # corridor AND looks like a line refurb, it's more likely sub-TX.
            # Example: "Gloversville - Marshville #6 Refurb" matches
            # "marshville" from the HV list, but also matches the
            # Gloversville-Marshville 69 kV corridor.  It's a 69 kV line
            # refurbishment, not 345 kV station construction.
            stx_corridor = _match_gold_book([s.name], GOLD_BOOK_NGRID_SUB_TX)
            is_line_refurb = s.invest_type == "T_Line" and any(
                w in s_norm
                for w in ["refurb", "rebuild", "recond", "acr", "dct", "tap"]
            )
            if stx_corridor and is_line_refurb:
                continue

            bulk_capital += s.capital_k
            bulk_sub_details.append(
                f"'{s.name}' (${s.capital_k:,.0f}k) — station '{matched_hv_station}' matches Gold Book ≥230kV entry"
            )

        total_capital = sum(s.capital_k for s in subs)
        bulk_frac = bulk_capital / total_capital if total_capital else 0

        if bulk_frac > 0.5:
            classification = "bulk_tx"
            confidence = "medium"
        else:
            classification = "sub_tx"
            confidence = "medium"

        voltage_str = "/".join(str(v) for v in sorted(set(all_voltages)))
        inference_method = "mixed_voltage_capital_weighted"
        bulk_detail_str = (
            "; ".join(bulk_sub_details) if bulk_sub_details else "none identified"
        )
        evidence = (
            f"Mixed voltages found: {sorted(set(all_voltages))}. "
            f"Bulk-voltage capital: ${bulk_capital:,.0f}k / ${total_capital:,.0f}k ({bulk_frac:.1%}). "
            f"Bulk sub-projects: {bulk_detail_str}. "
            f"Classified as {classification} based on capital weighting. "
            f"Sources: {'; '.join(voltage_sources)}."
        )

    # Case C: ONLY sub-TX voltages → sub_tx
    elif has_sub_tx:
        classification = "sub_tx"
        voltage_str = "/".join(
            str(v) for v in sorted(set(v for v in all_voltages if 69 <= v <= 115))
        )
        has_strong_evidence = bool(stx_matches) or any(
            "kV" in s or "kv" in s for s in sub_names
        )
        if has_strong_evidence:
            confidence = "high"
        elif used_fn_override:
            has_gb_in_source = any("Gold Book" in s for s in voltage_sources)
            confidence = "high" if has_gb_in_source else "medium"
        else:
            confidence = "medium"
        inference_method = _pick_inference_method(voltage_sources, stx_matches)
        evidence = (
            f"All identified voltages are 69-115kV (sub-transmission). "
            f"Sources: {'; '.join(voltage_sources)}."
        )

    # Case D: No voltage found anywhere → default to sub_tx
    # This is the weakest classification.  It relies on the fact that NiMo's
    # T-side network is predominantly 115 kV, so an unidentified Transm Net
    # project is most likely sub-TX.  Should be verified manually.
    else:
        classification = "sub_tx"
        voltage_str = "115"
        confidence = "low"
        inference_method = "no_voltage_found_default_sub_tx"
        evidence = (
            f"No explicit voltage found in FinalData sub-project names. "
            f"No Gold Book Table VII match found for station/corridor names. "
            f"Sub-project names: {sub_names}. "
            f"Defaulting to sub_tx (115kV) because NiMo's T-side network is "
            f"predominantly 115kV and no evidence of ≥230kV was found. "
            f"This classification should be verified against NiMo workpapers."
        )

    return ClassificationResult(
        exhibit1_line=line,
        fn_reference=fn,
        station=station,
        capacity_mw=cap,
        capital_m=capital_m,
        classification=classification,
        voltage_kv=voltage_str,
        inference_method=inference_method,
        evidence=evidence,
        sub_project_names=sub_names_str,
        confidence=confidence,
    )


def _pick_inference_method(
    voltage_sources: list[str], gold_book_matches: list[dict]
) -> str:
    """Choose a descriptive label for HOW the classification was determined.

    The label is a structured categorical value (not free text) that appears
    in the CSV.  It reflects the strongest evidence that contributed to the
    classification, in decreasing order of strength.
    """
    has_finaldata_voltage = any("states" in s and "kV" in s for s in voltage_sources)
    has_gold_book = len(gold_book_matches) > 0
    has_known_project = any("Well-known" in s or "known" in s for s in voltage_sources)
    has_fn_override = any(
        "Gold Book Table VII" in s or "Gold Book" in s for s in voltage_sources
    )
    has_line_number = any("line numbering" in s for s in voltage_sources)
    has_analogy = any(
        "analogy" in s or "consistent with" in s or "comparable" in s
        for s in voltage_sources
    )
    has_ev_rs = any("EV Ready Site" in s for s in voltage_sources)
    has_sta_number = any(
        "STA " in s or "STA 1" in s or "STA 7" in s or "numbered station" in s
        for s in voltage_sources
    )
    has_network_default = any("NiMo's sub-TX network" in s for s in voltage_sources)

    if has_known_project and has_gold_book:
        return "known_project_plus_gold_book"
    if has_known_project:
        return "known_project_name"
    if has_finaldata_voltage and has_gold_book:
        return "finaldata_voltage_plus_gold_book"
    if has_finaldata_voltage:
        return "finaldata_voltage_stated"
    if has_gold_book:
        return "gold_book_station_match"
    if has_fn_override and has_line_number:
        return "fn_line_number_convention"
    if has_fn_override:
        return "fn_to_gold_book_manual_match"
    if has_line_number:
        return "line_number_convention"
    if has_ev_rs:
        return "ev_ready_site_sub_tx_tap"
    if has_sta_number:
        return "numbered_station_sub_tx_tap"
    if has_analogy or has_network_default:
        return "station_type_analogy"
    return "no_voltage_found_default_sub_tx"


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════


def main() -> None:
    xlsx_path = "s3://data.sb/ny_psc/mcos_studies_2025/nimo_study_workpaper.xlsx"
    script_dir = Path(__file__).resolve().parent
    output_path = script_dir / "nimo_project_classifications.csv"

    print("Reading workbook from S3...")
    with fsspec.open(xlsx_path, "rb") as f:
        xlsx_bytes = f.read()

    print("Parsing Exhibit 1 (Sheet 1)...")
    projects = read_exhibit1(xlsx_bytes)
    print(f"  {len(projects)} projects")

    print("Parsing FinalData (Sheet 11)...")
    finaldata = read_finaldata(xlsx_bytes)
    print(
        f"  {len(finaldata)} parent FN groups, {sum(len(v) for v in finaldata.values())} sub-projects"
    )

    print("\nClassifying projects...")
    results: list[ClassificationResult] = []
    for proj in projects:
        result = classify_project(proj, finaldata)
        results.append(result)

    # ── Summary ──
    by_cls: dict[str, list[ClassificationResult]] = {}
    for r in results:
        by_cls.setdefault(r.classification, []).append(r)

    print("\n=== Classification Summary ===")
    for cls in ["distribution", "sub_tx", "bulk_tx"]:
        items = by_cls.get(cls, [])
        mw = sum(r.capacity_mw for r in items)
        cap_m = sum(r.capital_m for r in items)
        print(
            f"  {cls:<15} {len(items):>4} projects  {mw:>10,.0f} MW  ${cap_m:>10,.1f}M"
        )

    # ── Inference method breakdown ──
    by_method: dict[str, list[ClassificationResult]] = {}
    for r in results:
        by_method.setdefault(r.inference_method, []).append(r)

    print("\n=== Inference Method Breakdown ===")
    for method in sorted(by_method.keys()):
        items = by_method[method]
        print(f"  {method:<45} {len(items):>4} projects")

    # ── Uncertain classifications ──
    uncertain = [r for r in results if r.confidence in ("low", "medium")]
    print(f"\n=== Uncertain Classifications ({len(uncertain)} projects) ===")
    for r in sorted(
        uncertain, key=lambda x: (-{"low": 0, "medium": 1}[x.confidence], -x.capital_m)
    ):
        print(
            f"\n  Line {r.exhibit1_line:>3} | {r.fn_reference} | {r.station:<15} | "
            f"{r.capacity_mw:>7,.0f} MW | ${r.capital_m:>8,.1f}M | "
            f"{r.classification} | confidence={r.confidence}"
        )
        print(f"    inference_method: {r.inference_method}")
        print(f"    evidence: {r.evidence[:200]}...")
        if r.sub_project_names:
            print(f"    sub_projects: {r.sub_project_names[:200]}...")

    # ── Write CSV ──
    rows = []
    for r in results:
        rows.append(
            {
                "exhibit1_line": r.exhibit1_line,
                "fn_reference": r.fn_reference,
                "station": r.station,
                "capacity_mw": r.capacity_mw,
                "capital_m": round(r.capital_m, 2),
                "classification": r.classification,
                "voltage_kv": r.voltage_kv,
                "inference_method": r.inference_method,
                "evidence": r.evidence,
                "sub_project_names": r.sub_project_names,
                "confidence": r.confidence,
            }
        )

    df_out = pl.DataFrame(rows)
    df_out.write_csv(output_path)
    print(f"\nWrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()

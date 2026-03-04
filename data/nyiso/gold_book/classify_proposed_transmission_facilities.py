"""Classify Gold Book Table VII projects into project_type and project_subtype.

Reads a CSV of proposed transmission facilities (already cleaned for numeric
columns) and writes it with project_type (line | equipment) and project_subtype
populated.

Project type is determined from line_length_miles: rows with a numeric value are
initially classified as line projects; rows without are equipment. The manual
override table corrects misclassified rows — e.g. equipment projects whose
descriptions mention "line" by reference.

Overrides match rows by content (owner, year, description fragment, optional
disambiguator) rather than by row index, so the script works on any extraction
of Table VII regardless of row count or ordering.

Taxonomy
--------
LINE subtypes (project_type = "line"):
  new_line            - New transmission line, circuit, or cable (including HVDC)
  line_rebuild        - Rebuild or refurbish an existing line (new structures/conductor on same ROW)
  reconductor         - Replace conductor on existing line without full rebuild
  retirement          - Remove/retire an existing line (negative line lengths)
  voltage_conversion  - Upgrade operating voltage of existing line (e.g. 69kV -> 115kV)
  line_upgrade        - Generic upgrade to existing line (ratings, minor work)
  reconfiguration     - Loop-in, tap, reroute, or restructure line topology
  restoration         - Return decommissioned line to service

EQUIPMENT subtypes (project_type = "equipment"):
  transformer           - New, replacement, or upgraded transformer (auto-transformers, LTCs)
  new_substation        - Construction of a new substation or station
  substation_rebuild    - Rebuild, expand, or major modification of existing substation
  reactive_compensation - Capacitor banks, shunt reactors, SVCs, STATCOMs, series compensation
  breaker_switch        - Circuit breaker or switch installation/replacement
  terminal_upgrade      - Terminal equipment upgrades (bus work, connections, conductor drops)
  phase_angle_regulator - Phase angle regulators (PARs) and phase shifters
  reconfiguration       - Bus reconfiguration, bay additions, station reconfiguration
  equipment_replacement - Generic equipment replacement (not fitting other categories)
  retirement            - Retire/remove equipment or substation
  protection_controls   - Relay protection, sectionalizing schemes, control upgrades
  interconnection       - Connection to external entity (e.g. MTA/Amtrak)
  feeder                - PAR-regulated feeder or similar feeder-level equipment

Gold Book context
-----------------
Table VII is "Proposed Transmission Facilities." Each row is a single project entry.
Projects are either line projects (building/modifying/retiring a transmission line)
or equipment projects (installing/replacing/upgrading equipment at a station).
No project is both — some equipment projects reference lines they serve, but the
project itself is equipment work.
"""

import argparse
import csv
from collections import Counter
from pathlib import Path


# ---------------------------------------------------------------------------
# Row matching helper
# ---------------------------------------------------------------------------


def _row_text(row: dict) -> str:
    """Concatenate all row values for substring search."""
    return " ".join(row.values()).lower()


def _matches(
    row: dict, owner: str, year: str, desc_frag: str, disambig: str = ""
) -> bool:
    """True if a row matches the given content key."""
    if row["transmission_owner"] != owner:
        return False
    if row["in_service_year"] != year:
        return False
    if desc_frag.lower() not in row["project_description"].lower():
        return False
    if disambig and disambig.lower() not in _row_text(row):
        return False
    return True


# ---------------------------------------------------------------------------
# Manual override table — content-based
# ---------------------------------------------------------------------------
# Each entry: (owner, year, desc_fragment, disambiguator, project_type,
#              project_subtype, reason)
#
# desc_fragment: substring that must appear in project_description
# disambiguator: optional substring that must appear anywhere in the row
#                (used when desc_fragment alone is ambiguous)
#
# The override table is checked BEFORE rule-based classification. If a row
# matches, it gets the specified type/subtype and the reason is logged.

MANUAL_OVERRIDES: list[tuple[str, str, str, str, str, str, str]] = [
    # ===================================================================
    # (A) Equipment projects with numeric line_length_miles
    # ===================================================================
    # The heuristic classifies these as line because they have a numeric
    # length, but the project is actually equipment — the description just
    # references an existing line.
    # ===================================================================
    # New York Transco, Dover Phase Shifter — "Loop Line 398" but installing PARs
    (
        "New York Transco",
        "2025",
        "Loop Line 398",
        "",
        "equipment",
        "phase_angle_regulator",
        "installing PARs at substation; 'Loop Line' refers to the line being looped in",
    ),
    # NYPA Uniondale Hub PAR — "New PAR for Y54 Line"
    (
        "NYPA",
        "2029",
        "New PAR for Y54 Line",
        "",
        "equipment",
        "phase_angle_regulator",
        "new PAR; 'Line' refers to the existing line it controls",
    ),
    # ConEd Gowanus->Greenwood Xfmr/PAR/Feeder (third connection)
    (
        "ConEd",
        "2025",
        "New PAR regulated feeder (third connection)",
        "",
        "equipment",
        "feeder",
        "new PAR regulated feeder connection, not a line build",
    ),
    # ConEd Goethals->Fox Hills Xfmr/PAR/Feeder
    (
        "ConEd",
        "2025",
        "New PAR regulated feeder",
        "Goethals",
        "equipment",
        "feeder",
        "new PAR regulated feeder, not a line build",
    ),
    # ConEd Gowanus->Greenwood Xfmr/PAR/Feeder (fourth connection)
    (
        "ConEd",
        "2026",
        "New PAR regulated feeder (fourth connection)",
        "",
        "equipment",
        "feeder",
        "new PAR regulated feeder connection, not a line build",
    ),
    # LIPA Ocean Avenue->Barrett Series Reactor
    (
        "LIPA",
        "2028",
        "2 ohm series reactor",
        "",
        "equipment",
        "reactive_compensation",
        "series reactor on existing circuit; 'circuit' is a reference",
    ),
    # NGRID Clay->Woodard, "Add 10.5mH reactor on line #17"
    (
        "NGRID",
        "2024",
        "10.5mH reactor on line",
        "",
        "equipment",
        "reactive_compensation",
        "reactor added to existing line; 'line' is a reference",
    ),
    # NGRID Colton->Dennison, "Replace Station connections. Line #4"
    (
        "NGRID",
        "2025",
        "Replace Station connections. Line #4",
        "",
        "equipment",
        "terminal_upgrade",
        "replacing station-side connections for Line #4, not building a line",
    ),
    # NGRID Colton->Dennison, "Replace Station connections. Line #5"
    (
        "NGRID",
        "2025",
        "Replace Station connections. Line #5",
        "",
        "equipment",
        "terminal_upgrade",
        "replacing station-side connections for Line #5, not building a line",
    ),
    # NGRID Colton->Browns Falls, "Flat Rock station (mid-line) upgrades"
    (
        "NGRID",
        "2026",
        "Flat Rock station",
        "",
        "equipment",
        "terminal_upgrade",
        "station upgrades at mid-line tap point; 'line' is locational",
    ),
    # NGRID Malone, "Install PAR on Malone - Willis line 1-910"
    (
        "NGRID",
        "2026",
        "Install PAR on Malone",
        "",
        "equipment",
        "phase_angle_regulator",
        "PAR installation on existing line",
    ),
    # NGRID Maiden Lane Substation, "greenfield station connecting six 115kV lines"
    (
        "NGRID",
        "2030",
        "greenfield 115kV breaker 1/2 station connecting six",
        "",
        "equipment",
        "new_substation",
        "new substation; 'lines' describes what it connects to",
    ),
    # NYSEG Greenidge Capacitor, "breaker for Line 968, 30 MVAR cap bank"
    (
        "NYSEG",
        "2029",
        "circuit breaker for Line 968",
        "",
        "equipment",
        "reactive_compensation",
        "capacitor bank + breaker; 'Line 968' is a reference",
    ),
    # NYSEG Eelpot Expansion, "circuit breaker, line terminal work"
    (
        "NYSEG",
        "2032",
        "new 115 kV circuit breaker,and 115 kV line terminal work",
        "",
        "equipment",
        "breaker_switch",
        "breaker + terminal work; 'line' describes what the terminal serves",
    ),
    # RGE Station 127, "New 115 kV terminals for Trunk 2"
    (
        "RGE",
        "2025",
        "New 115 kV terminals for Trunk 2",
        "",
        "equipment",
        "terminal_upgrade",
        "new terminals at station, not a line build",
    ),
    # RGE Station 127, "2 New 115 kV capacitor banks"
    (
        "RGE",
        "2025",
        "New 115 kV capacitor banks",
        "",
        "equipment",
        "reactive_compensation",
        "capacitor banks at station, not a line build",
    ),
    # NYPA Adirondack Transformer (SPCP)
    (
        "NYPA",
        "2025",
        "SPCP: Adirondack 115/345 kV xfmr",
        "",
        "equipment",
        "transformer",
        "SPCP transformer installation; not a line segment",
    ),
    # NYPA Willis Transformer (SPCP)
    (
        "NYPA",
        "2025",
        "SPCP: Willis 345/230 kV xfmr",
        "",
        "equipment",
        "transformer",
        "SPCP transformer installation; not a line segment",
    ),
    # NGRID Austin Road Transformer (SPCP)
    (
        "NGRID",
        "2025",
        "SPCP: Austin Road 230/345 kV xfmr",
        "",
        "equipment",
        "transformer",
        "SPCP transformer installation; not a line segment",
    ),
    # O&R West Nyack Transformer, desc = "TRANSFORMER"
    (
        "O & R",
        "2026",
        "TRANSFORMER",
        "",
        "equipment",
        "transformer",
        "transformer project; description is literally 'TRANSFORMER'",
    ),
    # O&R Shoemaker Cap Bank, "(2) Capacitor Banks"
    (
        "O & R",
        "2029",
        "(2) Capacitor Banks",
        "",
        "equipment",
        "reactive_compensation",
        "capacitor banks; terminal_to = 'Cap Bank'",
    ),
    # LIPA Moriches Series Reactor
    (
        "LIPA",
        "2027",
        "2 OHM Series reactor on Moriches",
        "",
        "equipment",
        "reactive_compensation",
        "series reactor; 'Circuit' is a reference to existing circuit",
    ),
    # NGRID Whitaker, "line sectionalizing scheme"
    (
        "NGRID",
        "2025",
        "line sectionalizing scheme at Whitaker",
        "",
        "equipment",
        "protection_controls",
        "sectionalizing scheme on existing line; 'line' is a reference",
    ),
    # NGRID Gilbert Mills, "line sectionalizing scheme"
    (
        "NGRID",
        "2025",
        "line sectionalizing scheme at Gilbert Mills",
        "",
        "equipment",
        "protection_controls",
        "sectionalizing scheme on existing line; 'line' is a reference",
    ),
    # NGRID New Krumkill, "automatic line sectionalizing scheme"
    (
        "NGRID",
        "2026",
        "automatic line sectionalizing scheme",
        "",
        "equipment",
        "protection_controls",
        "sectionalizing scheme on existing line; 'line' is a reference",
    ),
    # NYSEG Jennison Rebuild, "rebuild 115kV to 4 bay BAAH. Bring Line 919..."
    (
        "NYSEG",
        "2029",
        "rebuild of 115 kV to 4 bay BAAH",
        "Jennison",
        "equipment",
        "substation_rebuild",
        "substation rebuild; 'Line 919' is routed through it",
    ),
    # ===================================================================
    # (B) Equipment projects the RULE-BASED classifier can't subtype
    # ===================================================================
    # These have no numeric line length (correctly equipment) but their
    # descriptions don't match the keyword rules.
    # ===================================================================
    # NYPA/TRANSCO Barrett, "LI PPTN: New 345 kV Substation"
    (
        "NYPA/TRANSCO",
        "2030",
        "New 345 kV Substation",
        "Barrett",
        "equipment",
        "new_substation",
        "new 345 kV substation (terminal_to has trailing dash not 'Substation')",
    ),
    # NYPA/TRANSCO New Shore Road, "LI PPTN: New 345 kV Substation"
    (
        "NYPA/TRANSCO",
        "2030",
        "New 345 kV Substation",
        "Shore Road",
        "equipment",
        "new_substation",
        "new 345 kV substation",
    ),
    # NYPA/TRANSCO New Rochelle, "LI PPTN: New 345 kV Transition Substation"
    (
        "NYPA/TRANSCO",
        "2030",
        "New 345 kV Transition Substation",
        "",
        "equipment",
        "new_substation",
        "new 345 kV transition substation",
    ),
    # NYPA/TRANSCO New Ruland Road, "LI PPTN: New 345 kV Substation"
    (
        "NYPA/TRANSCO",
        "2030",
        "New 345 kV Substation",
        "Ruland",
        "equipment",
        "new_substation",
        "new 345 kV substation",
    ),
    # NYPA/TRANSCO New Ruland Road, "LI PPTN: New 138 kV Substation"
    (
        "NYPA/TRANSCO",
        "2030",
        "New 138 kV Substation",
        "",
        "equipment",
        "new_substation",
        "new 138 kV substation",
    ),
    # CHGE Hurley Avenue Leeds, "21% Compensation - compensator"
    (
        "CHGE",
        "2025",
        "21% Compensation",
        "",
        "equipment",
        "reactive_compensation",
        "synchronous series compensator",
    ),
    # ConEd Mott Haven, "Connection to MTA/Amtrak"
    (
        "ConEd",
        "2025",
        "Connection to MTA/Amtrak",
        "",
        "equipment",
        "interconnection",
        "connection to external entity (MTA/Amtrak)",
    ),
    # ConEd Parkchester, "Connection to MTA/Amtrak"
    (
        "ConEd",
        "2026",
        "Connection to MTA/Amtrak",
        "",
        "equipment",
        "interconnection",
        "connection to external entity (MTA/Amtrak)",
    ),
    # ConEd Eastern Queens Clean Energy Hub, "New 138 kV Substation"
    (
        "ConEd",
        "2028",
        "New 138 kV Substation",
        "Eastern Queens",
        "equipment",
        "new_substation",
        "new 138 kV substation (clean energy hub)",
    ),
    # LIPA East of Buell, "Convert 23kV System to 34.5 kV System"
    (
        "LIPA",
        "2024",
        "Convert 23kV System to 34.5 kV System",
        "",
        "equipment",
        "substation_rebuild",
        "voltage system conversion at substation (same from/to location)",
    ),
    # NGRID Station 56 Pannell, "Mortimer-Pannell #24 Loop in-and-out"
    (
        "NGRID",
        "2024",
        "Mortimer-Pannell #24 Loop in-and-out",
        "Station 56",
        "equipment",
        "reconfiguration",
        "loop in-and-out reconfiguration at station",
    ),
    # NGRID Browns Falls, "Build new SubT facilities to separate from hydroplant"
    (
        "NGRID",
        "2026",
        "Build new SubT facilities",
        "",
        "equipment",
        "new_substation",
        "new sub-transmission facilities",
    ),
    # NGRID Coffeen, "Coffeen Overvoltage"
    (
        "NGRID",
        "2027",
        "Coffeen Overvoltage",
        "",
        "equipment",
        "reactive_compensation",
        "overvoltage mitigation (reactive compensation)",
    ),
    # NGRID Mcyntyre, "Mcyntyre Overvoltage"
    (
        "NGRID",
        "2028",
        "Mcyntyre Overvoltage",
        "",
        "equipment",
        "reactive_compensation",
        "overvoltage mitigation (reactive compensation)",
    ),
    # NGRID Boonville, "New 115kV station adjacent to existing Boonville sub"
    (
        "NGRID",
        "2028",
        "New 115kV station adjacent to existing Boonville",
        "",
        "equipment",
        "new_substation",
        "new 115kV station",
    ),
    # NGRID Gardenville Ohio, "New Terminal Station"
    (
        "NGRID",
        "2031",
        "New Terminal Station",
        "",
        "equipment",
        "new_substation",
        "new terminal station",
    ),
    # NYSEG Hillside Remove, "Reroute 115 kV lines to new station at Watercure"
    (
        "NYSEG",
        "2029",
        "Reroute the Hillside 115 kV lines to new 115 kV station",
        "",
        "equipment",
        "reconfiguration",
        "rerouting lines to new station; retirement of old station",
    ),
    # Coned Queens, "Queens Clean Energy Hub"
    (
        "Coned",
        "2028",
        "Queens Clean Energy Hub",
        "",
        "equipment",
        "new_substation",
        "new clean energy hub (substation)",
    ),
    # LIPA Wainscott, "New Wainscott substation with 138kV supply"
    (
        "LIPA",
        "2033",
        "New Wainscott substation",
        "",
        "equipment",
        "new_substation",
        "new substation with 138kV transmission supply",
    ),
    # CHGE South Cairo, "Install statcom and cap bank" (not New Baltimore)
    (
        "CHGE",
        "2025",
        "Install statcom and cap bank",
        "South Cairo",
        "equipment",
        "reactive_compensation",
        "statcom and cap bank installation",
    ),
    # ===================================================================
    # (C) Line projects the RULE-BASED classifier can't subtype
    # ===================================================================
    # These are correctly line projects but descriptions are too terse
    # (e.g., just conductor specs) for keyword rules to match.
    # ===================================================================
    # SPCP new lines (descriptions are conductor specs without "new")
    (
        "NYPA",
        "2025",
        "Haverstock Substation. 1",
        "Moses",
        "line",
        "new_line",
        "SPCP new line (desc is just conductor spec: '795 kcmil ACSR')",
    ),
    (
        "NYPA",
        "2025",
        "Haverstock to Adirondack (HA1)",
        "",
        "line",
        "new_line",
        "SPCP new line (desc lists new HA1/HA2 345kV lines)",
    ),
    (
        "NYPA",
        "2026",
        "Haverstock - Willis (HW1)",
        "",
        "line",
        "new_line",
        "SPCP new line (desc lists new HW1 345kV lines)",
    ),
    (
        "NYPA",
        "2025",
        "Two Willis - Patnode 230 kV Lines",
        "",
        "line",
        "new_line",
        "SPCP new line (desc lists new Willis-Patnode 230kV lines)",
    ),
    (
        "NYPA",
        "2026",
        "Two Willis - Ryan 230 kV Lines",
        "",
        "line",
        "new_line",
        "SPCP new line (desc lists new Willis-Ryan 230kV lines)",
    ),
    (
        "NYPA",
        "2026",
        "Two Willis (existing) - Willis (New) 230 kV",
        "",
        "line",
        "new_line",
        "SPCP new line (desc: Willis(existing)-Willis(new) 230kV lines)",
    ),
    (
        "NYPA/NGRID",
        "2025",
        "Adirondack - Austin Road Circuit-1 345 kV",
        "",
        "line",
        "new_line",
        "SPCP new line (desc: Adirondack-Austin Road Circuit-1 345kV)",
    ),
    (
        "NYPA/NGRID",
        "2025",
        "Adirondack - Marcy Circuit-1 345 kV",
        "",
        "line",
        "new_line",
        "SPCP new line (desc: Adirondack-Marcy Circuit-1 345kV)",
    ),
    (
        "NGRID",
        "2025",
        "Austin Road -Edic Circuit-1 345 kV",
        "",
        "line",
        "new_line",
        "SPCP new line (desc: Austin Road-Edic Circuit-1 345kV)",
    ),
    (
        "NGRID",
        "2025",
        "Rector Road - Austin Road Circuit-1 230 kV",
        "",
        "line",
        "new_line",
        "SPCP new line (desc: Rector Road-Austin Road Circuit-1 230kV)",
    ),
    # NGRID Clay->Wetzel, building new short radial line
    (
        "NGRID",
        "2025",
        "breaker at Clay and build approximately 2000 feet",
        "",
        "line",
        "new_line",
        "building new short radial line (~2000 ft) plus breaker at Clay",
    ),
    # O&R/ConEd Ladentown->Lovett 345kV (7.99mi), conductor spec "2-2493 ACAR"
    (
        "O & R/ConEd",
        "2024",
        "2-2493 ACAR",
        "7.99",
        "line",
        "new_line",
        "new 345kV line segment (conductor spec only: '2-2493 ACAR')",
    ),
    # O&R/ConEd Lovett->Buchanan (1.37mi), conductor spec "2-2493 ACAR"
    (
        "O & R/ConEd",
        "2024",
        "2-2493 ACAR",
        "1.37",
        "line",
        "new_line",
        "new 345kV line segment (conductor spec only: '2-2493 ACAR')",
    ),
    # NGRID "Replace 0.25 miles of UG cable on line #15"
    (
        "NGRID",
        "2029",
        "Replace 0.25 miles of UG cable on line #15",
        "",
        "line",
        "line_rebuild",
        "replacing UG cable on existing line route",
    ),
    # NGRID "Replace 0.25 miles of UG cable on line #12"
    (
        "NGRID",
        "2029",
        "Replace 0.25 miles of UG cable on line #12",
        "",
        "line",
        "line_rebuild",
        "replacing UG cable on existing line route",
    ),
    # O&R Burns->West Nyack, desc = "UG Cable"
    (
        "O & R",
        "2026",
        "UG Cable",
        "",
        "line",
        "new_line",
        "new UG cable (description is just 'UG Cable')",
    ),
    # O&R Shoemaker->Pocatello, "Reconductoring with double circuits"
    (
        "O & R",
        "2027",
        "Reconductoring with double circuits",
        "",
        "line",
        "reconductor",
        "reconductoring with double circuits (length unspecified in source)",
    ),
    # O&R West Nyack->Harings Corner, desc = "795 ACSS"
    (
        "O & R",
        "2029",
        "795 ACSS",
        "Harings",
        "line",
        "reconductor",
        "conductor spec only; 69->138 kV voltage change suggests reconductor",
    ),
    # O&R Ramapo->Sugarloaf, desc = "1272 ACSS"
    (
        "O & R",
        "2036",
        "1272 ACSS",
        "",
        "line",
        "new_line",
        "conductor spec only; 17-mile line, assumed new construction",
    ),
    # ===================================================================
    # (D) Line projects with no numeric line_length_miles
    # ===================================================================
    # These have no length in the source data, so the numeric-length
    # heuristic classifies them as equipment. But the description makes
    # it clear they are line work.
    # ===================================================================
    # ConEd Hudson Ave East -> Vinegar Hill, "Reconductoring"
    (
        "ConEd",
        "2025",
        "Reconductoring to accommodate additional capacity",
        "",
        "line",
        "reconductor",
        "reconductoring project; no length in source data",
    ),
    # LIPA Bridgehampton -> Buell, "Installation of New 69kV circuit"
    (
        "LIPA",
        "2025",
        "Installation of New 69kV circuit from Bridgehampton to Buell",
        "",
        "line",
        "new_line",
        "new 69kV circuit installation; no length in source data",
    ),
    # NYPA Moses-St.Lawrence Reynolds -> Back to Service
    (
        "NYPA",
        "2026",
        "MR3 line back to service",
        "",
        "line",
        "restoration",
        "MR3 line returned to service; no length in source data",
    ),
    # RGE "Line Upgrade" rows (desc is literally "line upgrade")
    (
        "RGE",
        "2030",
        "Line Upgrade",
        "Line #942",
        "line",
        "line_upgrade",
        "description says 'line upgrade'; no length in source data",
    ),
    (
        "RGE",
        "2030",
        "Line Upgrade",
        "Line #943",
        "line",
        "line_upgrade",
        "description says 'line upgrade'; no length in source data",
    ),
    (
        "RGE",
        "2030",
        "Line Upgrade",
        "Line #902",
        "line",
        "line_upgrade",
        "description says 'line upgrade'; no length in source data",
    ),
    # O&R Monroe Blooming -> Grove, "Line rebuild"
    (
        "O & R",
        "2027",
        "Line rebuild",
        "",
        "line",
        "line_rebuild",
        "description says 'line rebuild'; no length in source data",
    ),
    # O&R Shoemaker -> Shoemaker, "Transmission lines reconfiguration"
    (
        "O & R",
        "2028",
        "Transmission lines reconfiguration",
        "",
        "line",
        "reconfiguration",
        "transmission lines reconfiguration; no length in source data",
    ),
    # O&R Washington Heights -> Bullville, "New 69kV line"
    (
        "O & R",
        "2028",
        "New 69kV line",
        "",
        "line",
        "new_line",
        "new 69kV line; no length in source data",
    ),
    # O&R Shoemaker -> Cuddebackville, "Reconductor L12 & L13/L131"
    (
        "O & R",
        "2030",
        "Reconductor L12",
        "",
        "line",
        "reconductor",
        "reconductoring of lines L12 & L13/L131; no length in source data",
    ),
]


# ---------------------------------------------------------------------------
# Rule-based classifiers
# ---------------------------------------------------------------------------


def classify_line_subtype(row: dict) -> str:
    """Classify a line project into a subtype based on description and attributes."""
    desc = row["project_description"].lower()
    length = row["line_length_miles"]
    construction = row.get("class_year_or_construction_type", "").strip()
    neg = length != "" and float(length) < 0

    if neg or "retire" in desc or "remove" in desc:
        return "retirement"

    if "reconductor" in desc or "reconducting" in desc:
        return "reconductor"

    if "convert" in desc and "kv" in desc:
        return "voltage_conversion"

    if "rebuild" in desc or "refurbish" in desc:
        return "line_rebuild"

    if any(
        kw in desc
        for kw in [
            "new line",
            "new circuit",
            "new cable",
            "hvdc",
            "new 69kv",
            "new 138kv",
            "new 345kv",
            "new 230kv",
            "new 115kv",
            "new 23kv",
            "installation of new",
        ]
    ):
        return "new_line"

    if any(kw in desc for kw in ["loop", "tap", "tapping", "reroute"]):
        return "reconfiguration"

    if "upgrade" in desc:
        return "line_upgrade"

    if "back to service" in desc:
        return "restoration"

    if construction in ("OH", "UG") and length and float(length) > 0:
        if any(kw in desc for kw in ["replace", "rebuild", "refurbish"]):
            return "line_rebuild"
        return "new_line"

    if construction in ("OH", "UG"):
        return "new_line"

    if "replace" in desc and "mile" in desc:
        return "line_rebuild"

    return "UNCLASSIFIED"


def classify_equipment_subtype(row: dict) -> str:
    """Classify an equipment project into a subtype based on description and terminal."""
    desc = row["project_description"].lower()
    to = row["terminal_to"].lower()

    if any(kw in to for kw in ["transformer", "xfmr"]) or any(
        kw in desc for kw in ["transformer", "xfmr", "xfrm", "autotransformer"]
    ):
        return "transformer"

    if (
        "par" in to.split()
        or "phase shifter" in to
        or "par " in desc
        or "phase angle" in desc
        or "phase shifting" in desc
    ):
        return "phase_angle_regulator"

    if any(kw in to for kw in ["capacitor", "cap bank", "shunt reactor", "svc"]) or any(
        kw in desc
        for kw in [
            "capacitor",
            "cap bank",
            "statcom",
            "svc ",
            "shunt reactor",
            "mvar",
            "compensat",
            "series reactor",
            "reactor",
            "overvoltage",
        ]
    ):
        return "reactive_compensation"

    if ("new" in desc or "construct" in desc or "build" in desc) and (
        "substation" in desc or "station" in desc or "substation" in to
    ):
        return "new_substation"

    if "rebuild" in to or "rebuild" in desc or "expansion" in to or "expand" in desc:
        return "substation_rebuild"
    if "substation" in to and any(
        kw in desc for kw in ["rebuild", "replace", "upgrade"]
    ):
        return "substation_rebuild"

    if any(kw in desc for kw in ["breaker", "switch"]):
        return "breaker_switch"

    if "reconfigur" in to or "reconfigur" in desc:
        return "reconfiguration"

    if "retire" in desc or "remove" in desc or "removal" in to:
        return "retirement"

    if "feeder" in to or "feeder" in desc:
        return "feeder"

    if any(
        kw in desc
        for kw in [
            "terminal",
            "upgrade",
            "replace station",
            "replace conductor",
            "station connection",
        ]
    ):
        return "terminal_upgrade"

    if any(kw in desc for kw in ["sectionaliz", "relay", "protection"]):
        return "protection_controls"

    if "replace" in desc:
        return "equipment_replacement"

    if "substation" in to:
        return "substation_rebuild"

    return "UNCLASSIFIED"


def _has_numeric_line_length(row: dict) -> bool:
    """True if line_length_miles contains a parseable float."""
    val = row.get("line_length_miles", "").strip()
    if not val:
        return False
    try:
        float(val)
        return True
    except ValueError:
        return False


def determine_project_type(row: dict) -> str:
    """Determine whether a project is 'line' or 'equipment'.

    A numeric line_length_miles implies a line project; everything else is
    equipment.  Manual overrides (applied before this is called for overridden
    rows) can flip this for misclassified rows.
    """
    if _has_numeric_line_length(row):
        return "line"
    return "equipment"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify Gold Book Table VII projects into type/subtype."
    )
    parser.add_argument(
        "--path-input",
        type=Path,
        default=Path(__file__).parent / "2025_proposed_transmission_facilities_raw.csv",
        help="Path to the extracted CSV (without project_type/project_subtype).",
    )
    parser.add_argument(
        "--path-output",
        type=Path,
        default=Path(__file__).parent
        / "csv"
        / "2025_proposed_transmission_facilities.csv",
        help="Path to write the classified CSV.",
    )
    args = parser.parse_args()
    path_input: Path = args.path_input
    path_output: Path = args.path_output

    with open(path_input) as f:
        reader = csv.DictReader(f)
        old_fieldnames = list(reader.fieldnames)  # type: ignore[arg-type]
        rows = list(reader)

    # Build override index: map each row to its override (if any)
    override_map: dict[int, tuple[str, str, str]] = {}
    unmatched_overrides: list[tuple[str, str, str, str]] = []
    for owner, year, desc_frag, disambig, ptype, psubtype, reason in MANUAL_OVERRIDES:
        matched = [
            i
            for i, r in enumerate(rows)
            if _matches(r, owner, year, desc_frag, disambig)
        ]
        if len(matched) == 1:
            override_map[matched[0]] = (ptype, psubtype, reason)
        elif len(matched) == 0:
            unmatched_overrides.append((owner, year, desc_frag, disambig))
        else:
            # Ambiguous — try to pick the one not already overridden
            picked = [i for i in matched if i not in override_map]
            if len(picked) >= 1:
                override_map[picked[0]] = (ptype, psubtype, reason)
            else:
                unmatched_overrides.append((owner, year, desc_frag, disambig))

    if unmatched_overrides:
        print(f"WARNING: {len(unmatched_overrides)} overrides did not match any row:")
        for owner, year, desc_frag, disambig in unmatched_overrides:
            print(f"  {owner} | {year} | {desc_frag[:50]} | {disambig}")

    overridden_type = 0
    overridden_subtype = 0
    unclassified = []

    for i, row in enumerate(rows):
        if i in override_map:
            ptype, psubtype, reason = override_map[i]

            heuristic_type = determine_project_type(row)
            if heuristic_type != ptype:
                overridden_type += 1
                existing_notes = row.get("line_length_miles_notes", "")
                note = f"reclassified to {ptype}: {reason}"
                row["line_length_miles_notes"] = (existing_notes + "; " + note).lstrip(
                    "; "
                )

            row["project_type"] = ptype
            row["project_subtype"] = psubtype
            overridden_subtype += 1
        else:
            ptype = determine_project_type(row)
            row["project_type"] = ptype

            if ptype == "line":
                row["project_subtype"] = classify_line_subtype(row)
            else:
                row["project_subtype"] = classify_equipment_subtype(row)

        if row["project_subtype"] == "UNCLASSIFIED":
            unclassified.append((i + 2, row))

    # Build fieldnames: insert project_type/project_subtype after line_length_miles
    # if they're not already present (idempotent on re-runs).
    if "project_type" in old_fieldnames:
        new_fieldnames = old_fieldnames
    else:
        idx = old_fieldnames.index("line_length_miles") + 1
        new_fieldnames = (
            old_fieldnames[:idx]
            + ["project_type", "project_subtype"]
            + old_fieldnames[idx:]
        )

    path_output.parent.mkdir(parents=True, exist_ok=True)
    with open(path_output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # --- Report ---
    line_rows = [r for r in rows if r["project_type"] == "line"]
    equip_rows = [r for r in rows if r["project_type"] == "equipment"]

    print(f"Wrote {len(rows)} rows to {path_output}")
    print(
        f"Manual overrides: {overridden_subtype} subtype, {overridden_type} type flips"
    )
    print()
    print(f"=== LINE projects: {len(line_rows)} ===")
    for st, c in Counter(r["project_subtype"] for r in line_rows).most_common():
        print(f"  {st}: {c}")
    print()
    print(f"=== EQUIPMENT projects: {len(equip_rows)} ===")
    for st, c in Counter(r["project_subtype"] for r in equip_rows).most_common():
        print(f"  {st}: {c}")

    if unclassified:
        print(f"\n!!! UNCLASSIFIED: {len(unclassified)} rows !!!")
        for row_num, r in unclassified:
            print(
                f"  Row {row_num}: {r['transmission_owner']} | "
                f"{r['terminal_from']} -> {r['terminal_to']}"
            )
            print(
                f"    type={r['project_type']} | desc={r['project_description'][:80]}"
            )
    else:
        print("\nAll rows classified successfully.")


if __name__ == "__main__":
    main()

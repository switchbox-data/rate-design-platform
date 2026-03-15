"""Extract Table VII from the NYISO Gold Book markdown into a clean CSV.

Reads the PDF-extracted markdown at context/sources/nyiso_gold_book_2025.md,
parses the messy single-cell rows of Table VII ("Proposed Transmission
Facilities"), and writes a structured CSV with numeric columns cleaned.

The markdown was produced by PDF-to-markdown extraction, which collapsed the
multi-column table into single-cell rows with all fields concatenated.  This
script recovers the column structure by using transmission owner names and
in-service date patterns as parsing anchors.
"""

import argparse
import csv
import re
from pathlib import Path

OWNERS = [
    "CHPE LLC",
    "Clean Path New York LLC",
    "NYPA/TRANSCO",
    "NYPA/NGRID",
    "New York Transco",
    "NGRID/NYSEG",
    "NYSEG/ConEd",
    "O & R/ConEd",
    "LSP/NGRID",
    "LSP",
    "NYPA",
    "NGRID",
    "ConEd",
    "Coned",
    "LIPA",
    "CHGE",
    "NYSEG",
    "RGE",
    "O & R",
]

CATEGORIES = [
    "Class Year Transmission Projects",
    "TIP Projects",
    "Firm Plans",
    "Non-Firm Plans",
]

IN_SERVICE_RE = re.compile(r"(S|W|In-Service|In-service)\s+(20\d{2})")

VOLTAGE_RE = re.compile(r"^[\d/.]+(?:kV)?$")

THERMAL_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(MVA|MW|MVAR|A)?$", re.IGNORECASE)


def find_owner(text: str) -> tuple[str, int, int] | None:
    """Find the transmission owner in the text, return (owner, start, end)."""
    for owner in OWNERS:
        idx = text.find(owner)
        if idx != -1:
            return owner, idx, idx + len(owner)
    return None


def is_category_line(text: str) -> bool:
    return any(cat in text for cat in CATEGORIES)


def parse_thermal(val: str) -> tuple[str, str, str]:
    """Parse a thermal rating value, returning (value, units, notes).

    Per Gold Book footnote 4, bare numbers are in Amperes.
    """
    val = val.strip()
    if not val or val in ("N/A", "NA", "na"):
        return ("", "", "N/A in source")
    if val in ("-", "---", "–"):
        return ("", "", "dash placeholder in source")
    if val == "TBD":
        return ("", "", "TBD in source")

    m = THERMAL_RE.match(val)
    if m:
        num = m.group(1)
        unit = m.group(2) or "A"
        return (num, unit.upper(), "")
    # Bare number
    try:
        float(val)
        return (val, "A", "")
    except ValueError:
        return ("", "", f"unparseable: {val}")


def parse_row(text: str, current_category: str) -> dict | None:
    """Parse a single data row from the concatenated markdown cell."""
    text = text.strip()
    if not text:
        return None

    owner_match = find_owner(text)
    if not owner_match:
        return None

    owner, owner_start, owner_end = owner_match

    # Queue position: everything before the owner
    queue_raw = text[:owner_start].strip().rstrip(",").strip()
    # Clean queue: remove brackets, normalize
    queue = queue_raw.replace("[", "").replace("]", "").strip()

    remainder = text[owner_end:].strip()

    # Find in-service date pattern as the anchor
    date_match = IN_SERVICE_RE.search(remainder)
    if not date_match:
        return None

    season_raw = date_match.group(1)
    year = date_match.group(2)
    date_start = date_match.start()
    date_end = date_match.end()

    # Terminals + line_length are before the date
    before_date = remainder[:date_start].strip()

    # After the date: voltages, circuits, thermal, description, construction
    after_date = remainder[date_end:].strip()

    # Parse line_length: last token before the date
    tokens_before = before_date.rsplit(None, 1)
    if len(tokens_before) == 2:
        terminals_raw, length_candidate = tokens_before
        # Check if it looks like a number or dash
        try:
            float(length_candidate)
            line_length = length_candidate
        except ValueError:
            if length_candidate in ("-", "---", "–"):
                line_length = length_candidate
            else:
                # Not a length — it's part of the terminal name
                terminals_raw = before_date
                line_length = ""
    elif len(tokens_before) == 1:
        # Could be just a terminal or just a length
        try:
            float(tokens_before[0])
            terminals_raw = ""
            line_length = tokens_before[0]
        except ValueError:
            terminals_raw = tokens_before[0]
            line_length = ""
    else:
        terminals_raw = ""
        line_length = ""

    # Split terminals into from and to
    # The terminal_to often ends with a suffix like Transformer, Substation, etc.
    # We'll split on common patterns. The simplest approach: if there are two or
    # more known station names, split between them. For now, use a heuristic:
    # split roughly in the middle, looking for a capital letter start after a space.
    terminal_from, terminal_to = split_terminals(terminals_raw)

    # Parse after_date: voltage_op voltage_design circuits thermal_s thermal_w description [construction]
    after_tokens = after_date.split()

    voltage_op = ""
    voltage_design = ""
    num_circuits = ""
    thermal_summer_raw = ""
    thermal_winter_raw = ""
    description = ""
    construction = ""

    if after_tokens:
        idx = 0
        # Voltages: expect up to 2 voltage tokens (may be dash placeholders)
        if idx < len(after_tokens) and (
            VOLTAGE_RE.match(after_tokens[idx])
            or after_tokens[idx] in ("-", "---", "–")
        ):
            voltage_op = after_tokens[idx]
            idx += 1
        if idx < len(after_tokens) and (
            VOLTAGE_RE.match(after_tokens[idx])
            or after_tokens[idx] in ("-", "---", "–")
        ):
            voltage_design = after_tokens[idx]
            idx += 1

        # Circuits: consume a digit token as circuit count, but NOT if the
        # next token is a thermal unit (MVA/MW/MVAR) — that means the digit is
        # actually a thermal rating, not a circuit count.
        if idx < len(after_tokens):
            circ = after_tokens[idx]
            next_is_unit = idx + 1 < len(after_tokens) and after_tokens[
                idx + 1
            ].upper() in ("MVA", "MW", "MVAR")
            if circ in ("N/A", "-", "---"):
                num_circuits = circ
                idx += 1
            elif circ.isdigit() and not next_is_unit:
                num_circuits = circ
                idx += 1

        # Thermal ratings: can be "N/A", "TBD", a number, or "number MVA/MW/MVAR"
        thermal_summer_raw, idx = consume_thermal(after_tokens, idx)
        thermal_winter_raw, idx = consume_thermal(after_tokens, idx)

        # Remaining is description + optional construction type
        desc_tokens = after_tokens[idx:]
        if desc_tokens and desc_tokens[-1] in ("OH", "UG"):
            construction = desc_tokens[-1]
            desc_tokens = desc_tokens[:-1]
        description = " ".join(desc_tokens).strip().rstrip("-").strip()

    # Clean line_length
    line_length_notes = ""
    if line_length in ("-", "---", "–"):
        line_length_notes = f"originally '{line_length}'"
        line_length = ""
    elif line_length:
        try:
            float(line_length)
        except ValueError:
            line_length_notes = f"non-numeric: {line_length}"
            line_length = ""

    # Clean num_circuits
    circuits_notes = ""
    project_includes_circuits = ""
    if num_circuits in ("N/A", "-", "---"):
        circuits_notes = f"originally '{num_circuits}'"
        num_circuits = ""
        project_includes_circuits = "false"
    elif num_circuits:
        project_includes_circuits = "true"

    # Parse thermal ratings
    ts_val, ts_unit, ts_notes = parse_thermal(thermal_summer_raw)
    tw_val, tw_unit, tw_notes = parse_thermal(thermal_winter_raw)
    thermal_units = ts_unit or tw_unit
    thermal_includes = "true" if (ts_val or tw_val) else "false"
    thermal_notes = "; ".join(filter(None, [ts_notes, tw_notes]))

    # Season normalization
    if season_raw in ("In-Service", "In-service"):
        in_service_season = "In-Service"
    else:
        in_service_season = season_raw

    return {
        "category": current_category,
        "project_queue_position": queue,
        "transmission_owner": owner,
        "terminal_from": terminal_from,
        "terminal_to": terminal_to,
        "line_length_miles": line_length,
        "line_length_miles_notes": line_length_notes,
        "in_service_season": in_service_season,
        "in_service_year": year,
        "voltage_operating_kv": ""
        if voltage_op in ("-", "---", "–")
        else voltage_op.replace("kV", ""),
        "voltage_design_kv": ""
        if voltage_design in ("-", "---", "–")
        else voltage_design.replace("kV", ""),
        "num_circuits": num_circuits,
        "project_includes_circuits": project_includes_circuits,
        "num_circuits_notes": circuits_notes,
        "thermal_rating_summer": ts_val,
        "thermal_rating_winter": tw_val,
        "thermal_rating_units": thermal_units,
        "project_includes_thermal_rating": thermal_includes,
        "thermal_rating_notes": thermal_notes,
        "project_description": description,
        "class_year_or_construction_type": construction,
    }


def consume_thermal(tokens: list[str], idx: int) -> tuple[str, int]:
    """Consume a thermal rating value from the token stream."""
    if idx >= len(tokens):
        return ("", idx)
    tok = tokens[idx]
    if tok in ("N/A", "NA", "TBD"):
        return (tok, idx + 1)
    if tok in ("-", "---", "–"):
        return (tok, idx + 1)
    try:
        float(tok)
        # Check if next token is a unit
        if idx + 1 < len(tokens) and tokens[idx + 1].upper() in ("MVA", "MW", "MVAR"):
            return (f"{tok} {tokens[idx + 1]}", idx + 2)
        return (tok, idx + 1)
    except ValueError:
        # Could be "362MVA" (no space)
        m = re.match(r"^(\d+(?:\.\d+)?)(MVA|MW|MVAR)$", tok, re.IGNORECASE)
        if m:
            return (f"{m.group(1)} {m.group(2)}", idx + 1)
        return ("", idx)


def split_terminals(raw: str) -> tuple[str, str]:
    """Split concatenated terminal names into from and to.

    The terminal_to often has a suffix: Transformer, Substation, PAR, etc.
    We look for these suffixes and common station name patterns to find the
    split point.
    """
    raw = raw.strip()
    if not raw:
        return ("", "")

    # Known terminal_to suffixes that help identify the split
    suffixes = [
        "Transformer",
        "Substation",
        "PAR",
        "Phase Shifter",
        "Shunt Reactor",
        "Shunt reactor",
        "Series Reactor",
        "Series Reactors",
        "SVC",
        "SVC Control",
        "Reconfiguration",
        "Feeder",
        "Cap Bank",
        "Circuit Breakers",
        "Circuit Breaker",
        "Remove",
        "Removal",
        "Rebuild",
        "Upgrades",
        "Upgrade",
        "Expansion",
        "xfmr",
        "Xfmr/PAR/Feeder",
    ]

    # Try to find a suffix near the end to determine terminal_to
    for suffix in suffixes:
        # Look for the suffix pattern: "StationName Suffix"
        pattern = re.compile(r"(.+?)\s+(\S+(?:\s+\S+)*\s+" + re.escape(suffix) + r")$")
        m = pattern.match(raw)
        if m:
            return (m.group(1).strip(), m.group(2).strip())

    # Special case: terminal_to ends with "-" (placeholder)
    if raw.endswith(" -"):
        inner = raw[:-2].strip()
        parts = inner.rsplit(None, 1)
        if len(parts) == 2:
            # If parts look like two station names
            from_part, to_start = parts
            # Check if from_part has multiple words (station name)
            return (from_part, to_start + " -")
        return (inner, "-")

    # Split by looking for repeated station names (from == to)
    words = raw.split()
    n = len(words)
    if n >= 2:
        # Try equal halves
        for split_at in range(1, n):
            candidate_from = " ".join(words[:split_at])
            candidate_to = " ".join(words[split_at:])
            # If the to part starts with the same word(s) as from, good split
            if candidate_to.split()[0] == words[0] and split_at <= n // 2 + 1:
                return (candidate_from, candidate_to)

    # Fallback: split roughly in half
    if n >= 2:
        mid = n // 2
        return (" ".join(words[:mid]), " ".join(words[mid:]))

    return (raw, "")


FIELDNAMES = [
    "category",
    "project_queue_position",
    "transmission_owner",
    "terminal_from",
    "terminal_to",
    "line_length_miles",
    "line_length_miles_notes",
    "in_service_season",
    "in_service_year",
    "voltage_operating_kv",
    "voltage_design_kv",
    "num_circuits",
    "project_includes_circuits",
    "num_circuits_notes",
    "thermal_rating_summer",
    "thermal_rating_winter",
    "thermal_rating_units",
    "project_includes_thermal_rating",
    "thermal_rating_notes",
    "project_description",
    "class_year_or_construction_type",
]


def extract_table_vii(md_path: Path) -> list[dict]:
    """Extract Table VII rows from the Gold Book markdown."""
    text = md_path.read_text()
    lines = text.splitlines()

    # Table VII starts after "## (Untitled table on page 156)" and the header
    # and ends before "## (Untitled table on page 163)" (footnotes)
    in_table = False
    current_category = ""
    rows: list[dict] = []

    for line in lines:
        stripped = line.strip()

        # Detect table boundaries
        if (
            "Untitled table on page 15" in stripped
            or "Untitled table on page 16" in stripped
        ):
            page_match = re.search(r"page (\d+)", stripped)
            if page_match:
                page = int(page_match.group(1))
                if 156 <= page <= 162:
                    in_table = True
                    continue
                elif page == 163:
                    break

        if not in_table:
            continue

        # Skip header rows and separator rows
        if not stripped.startswith("|"):
            continue
        cell = stripped.lstrip("|").rstrip("|").strip()
        if not cell or cell.startswith("-") and not any(c.isalpha() for c in cell):
            continue
        if "Project Queue Position" in cell or "Operating Design" in cell:
            continue
        if cell.startswith("---"):
            continue

        # Category headers
        if is_category_line(cell):
            for cat in CATEGORIES:
                if cat in cell:
                    current_category = cat
                    break
            continue

        # Skip the separator-only rows (all dashes)
        if all(c in "-| " for c in cell):
            continue

        row = parse_row(cell, current_category)
        if row:
            rows.append(row)

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract Table VII from NYISO Gold Book markdown."
    )
    parser.add_argument(
        "--path-gold-book-md",
        type=Path,
        required=True,
        help="Path to the Gold Book markdown file.",
    )
    parser.add_argument(
        "--path-output",
        type=Path,
        required=True,
        help="Path to write the extracted CSV.",
    )
    args = parser.parse_args()

    rows = extract_table_vii(args.path_gold_book_md)
    print(f"Extracted {len(rows)} rows from Table VII")

    args.path_output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.path_output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {args.path_output}")

    # Summary
    categories = {}
    for r in rows:
        cat = r["category"] or "(uncategorized)"
        categories[cat] = categories.get(cat, 0) + 1
    for cat, count in sorted(categories.items()):
        print(f"  {cat}: {count}")


if __name__ == "__main__":
    main()

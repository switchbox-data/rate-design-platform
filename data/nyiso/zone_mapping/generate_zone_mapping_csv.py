"""Generate NY utility → LBMP zone / ICAP locality mapping CSV.

Mapping source: NYISO structure and Table 7-5 (utility gen capacity zone allocations).

Zone letter to LBMP zone name:
  A=WEST, B=GENESE, C=CENTRAL, D=NORTH, E=MHK_VL, F=CAPITL,
  G=HUD_VL, H=MILLWD, I=DUNWOD, J=N.Y.C., K=LONGIL

Output schema (one row per utility × load_zone × icap_locality):
  utility            – standard utility name (cenhud, coned, nimo, nyseg, or, rge, psegli)
  load_zone_letter   – NYISO zone letter (A–K)
  lbmp_zone_name     – NYISO LBMP zone name used in day-ahead price data
  icap_locality      – ICAP locality for capacity pricing (NYCA, GHIJ, NYC, LI)
  gen_capacity_zone  – generation capacity zone (ROS, LHV, NYC, LI)
  capacity_weight    – fraction of utility capacity obligation from this locality
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

# ── Zone letter → LBMP zone name ──────────────────────────────────────────────
ZONE_NAMES: dict[str, str] = {
    "A": "WEST",
    "B": "GENESE",
    "C": "CENTRAL",
    "D": "NORTH",
    "E": "MHK_VL",
    "F": "CAPITL",
    "G": "HUD_VL",
    "H": "MILLWD",
    "I": "DUNWOD",
    "J": "N.Y.C.",
    "K": "LONGIL",
}

# ── Utility mapping rows ─────────────────────────────────────────────────────
# Each tuple: (utility, zone_letters, icap_locality, gen_capacity_zone, capacity_weight)
# ConEd has a split: 87% NYC / 13% GHIJ (LHV).
_MAPPING_ROWS: list[tuple[str, list[str], str, str, float]] = [
    ("cenhud", ["G"], "GHIJ", "LHV", 1.0),
    # ConEd — 87% NYC locality
    ("coned", ["H", "I", "J"], "NYC", "NYC", 0.87),
    # ConEd — 13% GHIJ (Lower Hudson Valley) locality
    ("coned", ["H", "I", "J"], "GHIJ", "LHV", 0.13),
    ("nimo", ["A", "B", "C", "D", "E", "F"], "NYCA", "ROS", 1.0),
    ("nyseg", ["A", "C", "D", "E", "F", "G", "H"], "NYCA", "ROS", 1.0),
    ("or", ["G"], "GHIJ", "LHV", 1.0),
    ("rge", ["B"], "NYCA", "ROS", 1.0),
    ("psegli", ["K"], "LI", "LI", 1.0),
]


def build_zone_mapping() -> pl.DataFrame:
    """Build the zone mapping DataFrame from the hardcoded mapping table."""
    rows: list[dict[str, str | float]] = []
    for utility, zone_letters, icap_locality, gen_cap_zone, cap_weight in _MAPPING_ROWS:
        for letter in zone_letters:
            rows.append(
                {
                    "utility": utility,
                    "load_zone_letter": letter,
                    "lbmp_zone_name": ZONE_NAMES[letter],
                    "icap_locality": icap_locality,
                    "gen_capacity_zone": gen_cap_zone,
                    "capacity_weight": cap_weight,
                }
            )

    return pl.DataFrame(
        rows,
        schema={
            "utility": pl.Utf8,
            "load_zone_letter": pl.Utf8,
            "lbmp_zone_name": pl.Utf8,
            "icap_locality": pl.Utf8,
            "gen_capacity_zone": pl.Utf8,
            "capacity_weight": pl.Float64,
        },
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate NY utility zone mapping CSV."
    )
    parser.add_argument(
        "--path-output-csv",
        type=str,
        required=True,
        help="Local path to write the output CSV (e.g. data/nyiso/zone_mapping/csv/ny_utility_zone_mapping.csv).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path_output = Path(args.path_output_csv)

    # Safeguard: reject uninterpolated Just variables
    if "{{" in str(path_output) or "}}" in str(path_output):
        raise ValueError(
            f"Output path looks like an uninterpolated Just variable: {path_output}"
        )

    path_output.parent.mkdir(parents=True, exist_ok=True)

    df = build_zone_mapping()
    df.write_csv(path_output)

    print(f"✓ Wrote {len(df)} rows to {path_output}")
    print(df)


if __name__ == "__main__":
    main()

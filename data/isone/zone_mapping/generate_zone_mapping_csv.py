"""Generate ISO-NE utility → zone crosswalk mapping CSV.

Each platform utility that operates in ISO-NE territory is mapped to its
ISO-NE load zone.  Zone names match the ``zone`` column written by
``fetch_isone_zone_loads.py`` and the ISO-NE location IDs used by the
Web Services API (``/realtimehourlydemand`` endpoint).

Output schema (one row per utility × zone):
  utility      – platform utility slug (e.g. rie)
  state        – 2-char lowercase analysis state (e.g. ri)
  iso_zone     – ISO-NE load zone abbreviation as used in zone parquet
                 (ME, NH, VT, CT, RI, SEMA, WCMA, NEMA)
  location_id  – ISO-NE Web Services location ID for this zone (4001–4008)

RI maps to a single zone (RI, location 4005).  The schema supports
multi-zone utilities if they are added in the future.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

# ── Mapping rows ──────────────────────────────────────────────────────────────
# Each tuple: (utility, state, iso_zone, location_id)
# iso_zone must match the zone column written by fetch_isone_zone_loads.py.
# location_id must match the ISO-NE Web Services API location IDs:
#   ME=4001, NH=4002, VT=4003, CT=4004, RI=4005,
#   SEMA=4006, WCMA=4007, NEMA=4008
_MAPPING_ROWS: list[tuple[str, str, str, int]] = [
    # RI Electric (National Grid / Narragansett Electric)
    # RI is a single-zone state; all RI load is in the RI load zone.
    ("rie", "ri", "RI", 4005),
]

VALID_ISO_ZONES: dict[str, int] = {
    "ME": 4001,
    "NH": 4002,
    "VT": 4003,
    "CT": 4004,
    "RI": 4005,
    "SEMA": 4006,
    "WCMA": 4007,
    "NEMA": 4008,
}


def build_zone_mapping() -> pl.DataFrame:
    """Build the ISO-NE zone mapping DataFrame from the hardcoded mapping table."""
    rows: list[dict[str, str | int]] = []
    for utility, state, iso_zone, location_id in _MAPPING_ROWS:
        if iso_zone not in VALID_ISO_ZONES:
            raise ValueError(f"iso_zone '{iso_zone}' not in {sorted(VALID_ISO_ZONES)}")
        if VALID_ISO_ZONES[iso_zone] != location_id:
            raise ValueError(
                f"location_id {location_id} does not match expected "
                f"{VALID_ISO_ZONES[iso_zone]} for iso_zone '{iso_zone}'"
            )
        rows.append(
            {
                "utility": utility,
                "state": state,
                "iso_zone": iso_zone,
                "location_id": location_id,
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "utility": pl.String,
            "state": pl.String,
            "iso_zone": pl.String,
            "location_id": pl.Int32,
        },
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate ISO-NE utility zone mapping CSV."
    )
    parser.add_argument(
        "--path-output-csv",
        type=str,
        required=True,
        help=(
            "Local path to write the output CSV "
            "(e.g. data/isone/zone_mapping/csv/isone_utility_zone_mapping.csv)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path_output = Path(args.path_output_csv)

    if "{{" in str(path_output) or "}}" in str(path_output):
        raise ValueError(
            f"Output path looks like an uninterpolated Just variable: {path_output}"
        )

    path_output.parent.mkdir(parents=True, exist_ok=True)
    df = build_zone_mapping()
    df.write_csv(path_output)

    print(f"Wrote {len(df)} row(s) to {path_output}")
    print(df)


if __name__ == "__main__":
    main()

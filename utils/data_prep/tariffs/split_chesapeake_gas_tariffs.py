"""Split Chesapeake Utilities gas tariff CSVs into county-specific URDB v7 JSONs.

The Chesapeake Utilities consolidated tariff (effective 4/19/2025, Case No. 9722)
covers three county groups with different non-fuel energy charges and gas sales
service rates.  RateAcuity produces a single CSV per rate class (RES-1, RES-2)
with all county groups distinguished by the ``location`` column.  The standard
CSV-to-URDB converter averages across locations, producing incorrect blended
tariffs.

This script reads the two Chesapeake CSVs (one per rate class), filters rows by
county group, sums per-therm charges (non-fuel energy + gas sales service +
franchise tax + SIR for Worcester), and writes six county-specific URDB v7 JSONs:

    chesapeake_main_res1.json      chesapeake_main_res2.json
    chesapeake_cecil_res1.json     chesapeake_cecil_res2.json
    chesapeake_worcester_res1.json chesapeake_worcester_res2.json

Tariff structure reference (PSC Md. No. 1, Sheet 7.101-7.104, 7.300, 7.400, 7.404):

  - Customer Charge: fixed monthly, uniform across counties ($8 RES-1, $10 RES-2)
  - Non-Fuel Energy Charge: per-therm delivery, varies by county group
  - Gas Sales Service Rate (GSR): per-therm supply, varies by county and quarter
  - Maryland Franchise Tax Rider: $0.00402/therm, uniform
  - System Improvement Rate (SIR): Worcester County only, $0.052/therm (Dec 2025)
  - Energy Efficiency Rider (EER): $0.000/therm currently (excluded)

Cecil County phased non-fuel energy rates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cecil County (former Elkton Gas) has phased non-fuel energy rates per Case
No. 9722 (PSC Md. No. 1, Sheets 7.101/7.103):

  - Phase 1 (4/19/2025 – 4/18/2026): 1 year
  - Phase 2 (4/19/2026 – 4/18/2030): 4 years
  - Phase 3 (4/19/2030 onward): permanent — the levelized rate

Phase 3 equals the time-weighted average of Phases 1 and 2:
``(1 × Phase1 + 4 × Phase2) / 5``.  This script accepts optional Phase 2 CSVs
(fetched with ``--year 2027``), extracts the Phase 2 non-fuel energy charge for
Cecil County, and computes the levelized rate.  That constant rate is applied for
all 12 months in the Cecil County URDB JSONs.

Worcester County's SIR has two sub-rates (Ocean City vs. rest of county).  This
script uses the non-Ocean-City rate as the default since ResStock does not
distinguish Ocean City from the rest of Worcester County.

Usage::

    uv run python utils/data_prep/tariffs/split_chesapeake_gas_tariffs.py \\
        --path-res1-csv config/tariffs/gas/chesapeake_utilities_res1.csv \\
        --path-res2-csv config/tariffs/gas/chesapeake_utilities_res2.csv \\
        --path-res1-phase2-csv config/tariffs/gas/chesapeake_utilities_res1_phase2.csv \\
        --path-res2-phase2-csv config/tariffs/gas/chesapeake_utilities_res2_phase2.csv \\
        --output-dir config/tariffs/gas
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

NUM_MONTHS = 12
UTILITY_NAME = "Chesapeake Utilities"

# ── County-group definitions ────────────────────────────────────────────────

COUNTY_GROUPS: dict[str, dict[str, object]] = {
    "main": {
        "label": "Caroline, Dorchester, Somerset, and Wicomico Counties",
        "location_matches": [
            "caroline, dorchester, somerset and wicomico counties",
        ],
        "has_sir": False,
        "sir_location": "",
    },
    "cecil": {
        "label": "Cecil County",
        "location_matches": ["cecil county"],
        "has_sir": False,
        "sir_location": "",
    },
    "worcester": {
        "label": "Worcester County",
        "location_matches": ["worcester county"],
        "has_sir": True,
        "sir_location": "worcester county - all areas except ocean city",
    },
}

RATE_CLASS_LABELS = {
    "res1": "RES-1",
    "res2": "RES-2",
}

# Per-therm charge names (from the CSV ``rate`` column) to sum into the
# volumetric rate.  The ``customer charge`` is handled separately as a fixed
# monthly charge.
PER_THERM_RATE_NAMES = frozenset(
    {
        "maryland franchise tax rider",
        "non-fuel energy charge",
        "gas sales service rate",
        "system improvement rate",
    }
)

CUSTOMER_CHARGE_RATE_NAME = "customer charge"


# ── CSV helpers ─────────────────────────────────────────────────────────────


def _parse_float(s: str) -> float | None:
    """Parse a CSV cell as float, returning None for blanks."""
    s = s.strip()
    return float(s) if s else None


def _fill_monthly(values: list[float | None]) -> list[float]:
    """Fill None gaps: backfill leading Nones from the first value, then forward-fill."""
    first = next((i for i, v in enumerate(values) if v is not None), None)
    if first is None:
        msg = "All 12 monthly values are None — cannot build rate"
        raise ValueError(msg)
    filled: list[float] = []
    for i, v in enumerate(values):
        if v is not None:
            filled.append(v)
        elif i < first:
            filled.append(values[first])  # type: ignore[arg-type]
        else:
            filled.append(filled[-1])
    return filled


# ── Row-level types ─────────────────────────────────────────────────────────


class RateRow:
    """One row from the RateAcuity CSV."""

    __slots__ = ("rate_name", "location", "rate_determinant", "monthly")

    def __init__(
        self,
        rate_name: str,
        location: str,
        rate_determinant: str,
        monthly: list[float | None],
    ) -> None:
        self.rate_name = rate_name
        self.location = location
        self.rate_determinant = rate_determinant
        self.monthly = monthly


def _read_csv(path: Path) -> list[RateRow]:
    """Parse a Chesapeake gas tariff CSV into a list of :class:`RateRow`."""
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = [h.strip() for h in next(reader)]

    month_cols: list[int] = []
    for i, col in enumerate(header):
        parts = col.split("/")
        if len(parts) == 3 and all(p.strip().isdigit() for p in parts):
            month_cols.append(i)
    if len(month_cols) != NUM_MONTHS:
        msg = f"Expected {NUM_MONTHS} month columns in {path.name}, found {len(month_cols)}"
        raise ValueError(msg)

    rows: list[RateRow] = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for raw in reader:
            if len(raw) < max(month_cols) + 1:
                continue
            rate_name = raw[0].strip().lower()
            if not rate_name:
                continue
            rate_det_idx = (
                header.index("rate_determinant") if "rate_determinant" in header else 7
            )
            rate_det = (
                raw[rate_det_idx].strip().lower() if len(raw) > rate_det_idx else ""
            )
            if not rate_det:
                continue
            loc_idx = header.index("location") if "location" in header else 6
            location = raw[loc_idx].strip().lower() if len(raw) > loc_idx else ""
            monthly = [_parse_float(raw[i]) for i in month_cols]
            rows.append(RateRow(rate_name, location, rate_det, monthly))

    return rows


# ── Cecil County phased NFEC ─────────────────────────────────────────────────

CECIL_PHASE1_YEARS = 1
CECIL_PHASE2_YEARS = 4
CECIL_TOTAL_YEARS = CECIL_PHASE1_YEARS + CECIL_PHASE2_YEARS
CECIL_LOCATION = "cecil county"
NFEC_RATE_NAME = "non-fuel energy charge"


def _get_nfec_for_location(rows: list[RateRow], location: str) -> float:
    """Extract the non-fuel energy charge value for a specific location.

    The NFEC is a flat per-therm rate (same for all months within a phase),
    so we return the first non-None value.
    """
    for row in rows:
        if row.rate_name == NFEC_RATE_NAME and row.location == location:
            value = next((v for v in row.monthly if v is not None), None)
            if value is None:
                msg = (
                    f"No non-None value in {NFEC_RATE_NAME!r} for location {location!r}"
                )
                raise ValueError(msg)
            return value
    msg = f"No {NFEC_RATE_NAME!r} found for location {location!r}"
    raise ValueError(msg)


def _compute_cecil_nfec_levelized(
    phase1_rows: list[RateRow],
    phase2_rows: list[RateRow],
) -> float:
    """Compute the levelized (Phase 3) non-fuel energy charge for Cecil County.

    Time-weighted average: ``(1yr × Phase1 + 4yr × Phase2) / 5yr``.
    """
    p1 = _get_nfec_for_location(phase1_rows, CECIL_LOCATION)
    p2 = _get_nfec_for_location(phase2_rows, CECIL_LOCATION)
    levelized = (CECIL_PHASE1_YEARS * p1 + CECIL_PHASE2_YEARS * p2) / CECIL_TOTAL_YEARS
    log.info(
        "  Cecil NFEC: Phase 1=$%.5f (1yr), Phase 2=$%.5f (4yr) → levelized=$%.5f",
        p1,
        p2,
        levelized,
    )
    return levelized


# ── County-group filtering ──────────────────────────────────────────────────


def _row_applies_to_group(row: RateRow, group_key: str) -> bool:
    """Return True if *row* should be included in the county group's total."""
    group = COUNTY_GROUPS[group_key]

    if not row.location:
        return True

    if row.rate_name == "system improvement rate":
        if not group["has_sir"]:
            return False
        return row.location == group["sir_location"]

    location_matches: list[str] = group["location_matches"]  # type: ignore[assignment]
    return row.location in location_matches


def _extract_rates_for_group(
    rows: list[RateRow],
    group_key: str,
    *,
    nfec_override: float | None = None,
) -> tuple[list[float], float]:
    """Sum per-therm charges and extract fixed charge for *group_key*.

    When *nfec_override* is set, the non-fuel energy charge from the CSV is
    replaced with this constant value for all 12 months.  Used for Cecil
    County's levelized (time-weighted) non-fuel energy charge.

    Returns ``(monthly_per_therm_totals, fixed_charge)``.
    """
    per_therm_components: list[list[float]] = []
    fixed_charge: float | None = None

    for row in rows:
        if row.rate_name == CUSTOMER_CHARGE_RATE_NAME:
            if not row.location:
                filled = _fill_monthly(row.monthly)
                fixed_charge = filled[0]
            continue

        if row.rate_name not in PER_THERM_RATE_NAMES:
            continue

        if not _row_applies_to_group(row, group_key):
            continue

        if row.rate_name == NFEC_RATE_NAME and nfec_override is not None:
            continue

        filled = _fill_monthly(row.monthly)
        per_therm_components.append(filled)
        log.debug(
            "  %s | %s → %s: avg=%.5f",
            group_key,
            row.rate_name,
            row.location or "(all)",
            sum(filled) / len(filled),
        )

    if nfec_override is not None:
        per_therm_components.append([nfec_override] * NUM_MONTHS)
        log.debug(
            "  %s | %s → override: %.5f (constant)",
            group_key,
            NFEC_RATE_NAME,
            nfec_override,
        )

    if fixed_charge is None:
        msg = f"No customer charge found for group {group_key!r}"
        raise ValueError(msg)
    if not per_therm_components:
        msg = f"No per-therm charges found for group {group_key!r}"
        raise ValueError(msg)

    monthly_totals = [
        sum(comp[m] for comp in per_therm_components) for m in range(NUM_MONTHS)
    ]
    return monthly_totals, fixed_charge


# ── URDB builder ────────────────────────────────────────────────────────────

# The tariff_fetch library (build_urdb) converts gas rates from $/therm to
# $/kWh so that all URDB energy rates share a common unit.  We apply the same
# conversion here so our county-split JSONs are consistent with the other gas
# tariff JSONs in config/tariffs/gas/.
THERMS_PER_KWH = 1 / 29.3001  # 1 therm = 29.3001 kWh


def _build_urdb(
    monthly_rates: list[float],
    fixed_charge: float,
    schedule_name: str,
    county_label: str,
) -> dict[str, object]:
    """Build a URDB v7 JSON dict (bare, no ``items`` envelope).

    Rates are converted from $/therm to $/kWh to match the convention used by
    the tariff_fetch library for all other gas URDB JSONs.
    """
    rate_structure = [
        [{"rate": round(r * THERMS_PER_KWH, 6), "unit": "kWh"}] for r in monthly_rates
    ]

    schedule = [[m] * 24 for m in range(NUM_MONTHS)]

    return {
        "energyratestructure": rate_structure,
        "energyweekdayschedule": schedule,
        "energyweekendschedule": [row[:] for row in schedule],
        "fixedchargefirstmeter": round(fixed_charge, 2),
        "fixedchargeunits": "$/month",
        "utility": UTILITY_NAME,
        "name": f"{schedule_name} ({county_label})",
    }


# ── Main logic ──────────────────────────────────────────────────────────────

RATE_CLASSES = {
    "res1": {
        "schedule_name": "RES-1-RESIDENTIAL SERVICE-1---150",
    },
    "res2": {
        "schedule_name": "RES-2-RESIDENTIAL SERVICE-2--151-",
    },
}


def split_chesapeake_tariffs(
    path_res1_csv: Path,
    path_res2_csv: Path,
    output_dir: Path,
    path_res1_phase2_csv: Path | None = None,
    path_res2_phase2_csv: Path | None = None,
) -> list[Path]:
    """Read the CSVs and write six county-specific URDB JSONs.

    When Phase 2 CSVs are provided, computes the levelized (time-weighted)
    non-fuel energy charge for Cecil County and applies it as a constant.

    Returns the list of output file paths.
    """
    csv_paths = {"res1": path_res1_csv, "res2": path_res2_csv}
    phase2_csv_paths: dict[str, Path | None] = {
        "res1": path_res1_phase2_csv,
        "res2": path_res2_phase2_csv,
    }
    written: list[Path] = []

    for rate_class, csv_path in csv_paths.items():
        log.info("Reading %s → %s", csv_path.name, RATE_CLASS_LABELS[rate_class])
        rows = _read_csv(csv_path)
        log.info("  Parsed %d data rows", len(rows))
        schedule_name = RATE_CLASSES[rate_class]["schedule_name"]

        cecil_nfec: float | None = None
        phase2_path = phase2_csv_paths[rate_class]
        if phase2_path is not None:
            log.info("Reading Phase 2 CSV %s", phase2_path.name)
            phase2_rows = _read_csv(phase2_path)
            cecil_nfec = _compute_cecil_nfec_levelized(rows, phase2_rows)

        for group_key, group_def in COUNTY_GROUPS.items():
            county_label: str = group_def["label"]  # type: ignore[assignment]

            nfec_override = cecil_nfec if group_key == "cecil" else None
            monthly_rates, fixed_charge = _extract_rates_for_group(
                rows, group_key, nfec_override=nfec_override
            )

            avg_rate = sum(monthly_rates) / len(monthly_rates)
            log.info(
                "  %s | %s: fixed=$%.2f, avg volumetric=$%.5f/therm "
                "(%d per-therm components%s)",
                RATE_CLASS_LABELS[rate_class],
                county_label,
                fixed_charge,
                avg_rate,
                sum(
                    1
                    for r in rows
                    if r.rate_name in PER_THERM_RATE_NAMES
                    and _row_applies_to_group(r, group_key)
                ),
                ", NFEC=levelized" if nfec_override is not None else "",
            )

            urdb = _build_urdb(monthly_rates, fixed_charge, schedule_name, county_label)
            out_name = f"chesapeake_{group_key}_{rate_class}.json"
            out_path = output_dir / out_name
            out_path.write_text(json.dumps({"items": [urdb]}, indent=2) + "\n")
            written.append(out_path)
            log.info("  Wrote %s", out_path)

    return written


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Split Chesapeake Utilities gas tariff CSVs into county-specific URDB JSONs."
    )
    parser.add_argument(
        "--path-res1-csv",
        type=Path,
        required=True,
        help="Path to the RES-1 CSV (chesapeake_utilities_res1.csv).",
    )
    parser.add_argument(
        "--path-res2-csv",
        type=Path,
        required=True,
        help="Path to the RES-2 CSV (chesapeake_utilities_res2.csv).",
    )
    parser.add_argument(
        "--path-res1-phase2-csv",
        type=Path,
        default=None,
        help="Path to the Phase 2 RES-1 CSV (fetched with --year 2027).",
    )
    parser.add_argument(
        "--path-res2-phase2-csv",
        type=Path,
        default=None,
        help="Path to the Phase 2 RES-2 CSV (fetched with --year 2027).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for the six URDB JSONs.",
    )
    args = parser.parse_args()

    for p in (args.path_res1_csv, args.path_res2_csv):
        if not p.is_file():
            raise SystemExit(f"CSV not found: {p}")
    phase2_csvs = [
        p for p in (args.path_res1_phase2_csv, args.path_res2_phase2_csv) if p
    ]
    if phase2_csvs:
        for p in phase2_csvs:
            if not p.is_file():
                raise SystemExit(f"Phase 2 CSV not found: {p}")
        if len(phase2_csvs) != 2:
            raise SystemExit(
                "Either provide both --path-res1-phase2-csv and --path-res2-phase2-csv, or neither."
            )
    args.output_dir.mkdir(parents=True, exist_ok=True)

    written = split_chesapeake_tariffs(
        args.path_res1_csv,
        args.path_res2_csv,
        args.output_dir,
        path_res1_phase2_csv=args.path_res1_phase2_csv,
        path_res2_phase2_csv=args.path_res2_phase2_csv,
    )
    log.info("Done: wrote %d URDB JSONs to %s", len(written), args.output_dir)


if __name__ == "__main__":
    main()

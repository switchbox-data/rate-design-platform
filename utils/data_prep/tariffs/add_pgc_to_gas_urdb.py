"""Add commodity (PGC) charges to distribution-only gas URDB JSONs.

UGI Central Penn and Easton Muni gas tariffs from RateAcuity contain only
distribution rates. Their commodity (Purchased Gas Cost / Price to Compare) is
published separately and stored in PGC CSVs. This script reads the PGC CSV,
computes the annual-average $/kWh commodity rate for the target year, and adds
it uniformly to all energy tiers in every period of the URDB JSON.

The CCF-to-kWh conversion uses the standard factor: 1 CCF ≈ 1 therm =
29.3001 kWh.

Usage:
    # UGI Central Penn (price_to_compare_per_ccf column)
    uv run python utils/data_prep/tariffs/add_pgc_to_gas_urdb.py \\
        --path-urdb rate_design/hp_rates/md/config/tariffs/gas/ugi_central_penn_residential.json \\
        --path-pgc  rate_design/hp_rates/md/config/tariffs/gas/ugi_central_penn_pgc.csv \\
        --pgc-col   price_to_compare_per_ccf \\
        --year      2025

    # Easton Muni (total_supply_per_ccf column; Dec 2025 missing → Jan-Nov avg)
    uv run python utils/data_prep/tariffs/add_pgc_to_gas_urdb.py \\
        --path-urdb rate_design/hp_rates/md/config/tariffs/gas/easton_muni_residential.json \\
        --path-pgc  rate_design/hp_rates/md/config/tariffs/gas/easton_muni_pgc.csv \\
        --pgc-col   total_supply_per_ccf \\
        --year      2025
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# 1 CCF ≈ 1 therm = 29.3001 kWh (standard thermal conversion)
CCF_TO_KWH = 29.3001


def _load_pgc_annual_avg(path_pgc: Path, pgc_col: str, year: int) -> float:
    """Return annual-average PGC in $/kWh for *year*.

    Reads monthly $/ccf values from the CSV, filters to the requested year,
    averages all available months, and converts to $/kWh. Missing months
    (e.g. Easton's December) are filled with the mean of the available months
    before averaging — equivalent to carrying the last known rate forward.
    """
    monthly: dict[int, float] = {}
    with path_pgc.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            month_str = row["month"]
            row_year, row_month = month_str.split("-")
            if int(row_year) != year:
                continue
            try:
                val = float(row[pgc_col])
            except (KeyError, ValueError) as exc:
                raise ValueError(
                    f"Column '{pgc_col}' not found or non-numeric in {path_pgc} "
                    f"at row {month_str}"
                ) from exc
            monthly[int(row_month)] = val

    if not monthly:
        raise ValueError(
            f"No PGC data found for year {year} in {path_pgc}. "
            f"Available months: check the CSV."
        )

    found = sorted(monthly.keys())
    missing = [m for m in range(1, 13) if m not in monthly]
    if missing:
        fill = sum(monthly.values()) / len(monthly)
        log.warning(
            "%s: months %s not found for year %d — filling with mean of "
            "available months (%.4f $/ccf)",
            path_pgc.name,
            missing,
            year,
            fill,
        )
        for m in missing:
            monthly[m] = fill
        found = list(range(1, 13))

    log.info(
        "%s: using %d months for year %d (months %s)",
        path_pgc.name,
        len(found),
        year,
        found,
    )

    avg_per_ccf = sum(monthly.values()) / 12
    avg_per_kwh = avg_per_ccf / CCF_TO_KWH
    log.info(
        "Annual-average PGC: %.5f $/ccf → %.6f $/kWh (÷ %.4f CCF/kWh)",
        avg_per_ccf,
        avg_per_kwh,
        CCF_TO_KWH,
    )
    return avg_per_kwh


def add_pgc_to_urdb(path_urdb: Path, commodity_per_kwh: float) -> dict:
    """Return a new URDB dict with *commodity_per_kwh* added to every tier."""
    with path_urdb.open() as f:
        data = json.load(f)

    if "items" not in data:
        raise ValueError(
            f'{path_urdb}: expected top-level {{"items": [...]}} envelope.'
        )

    item: dict = data["items"][0]
    energy_rates = item.get("energyratestructure")
    if not energy_rates:
        raise ValueError(f"{path_urdb}: no 'energyratestructure' found.")

    updated_rates = []
    for period in energy_rates:
        updated_period = []
        for tier in period:
            new_tier = {**tier, "rate": round(tier["rate"] + commodity_per_kwh, 7)}
            updated_period.append(new_tier)
        updated_rates.append(updated_period)

    new_item = {**item, "energyratestructure": updated_rates}
    return {"items": [new_item]}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add annual-average PGC commodity rate to a distribution-only gas URDB JSON."
    )
    parser.add_argument(
        "--path-urdb",
        type=Path,
        required=True,
        help="Path to the distribution-only URDB JSON file to update.",
    )
    parser.add_argument(
        "--path-pgc",
        type=Path,
        required=True,
        help="Path to the PGC CSV file (columns: month YYYY-MM, <pgc-col>).",
    )
    parser.add_argument(
        "--pgc-col",
        required=True,
        help="CSV column name containing the $/ccf commodity rate.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Target year to average PGC rates over (default: 2025).",
    )
    args = parser.parse_args()

    commodity_per_kwh = _load_pgc_annual_avg(args.path_pgc, args.pgc_col, args.year)
    log.info(
        "Adding %.6f $/kWh to all tiers in %s", commodity_per_kwh, args.path_urdb.name
    )

    updated = add_pgc_to_urdb(args.path_urdb, commodity_per_kwh)

    # Log before/after for first period
    orig_tiers = json.load(args.path_urdb.open())["items"][0]["energyratestructure"][0]
    new_tiers = updated["items"][0]["energyratestructure"][0]
    log.info("Period 1 tiers before: %s", [round(t["rate"], 6) for t in orig_tiers])
    log.info("Period 1 tiers after:  %s", [round(t["rate"], 6) for t in new_tiers])

    args.path_urdb.write_text(json.dumps(updated, indent=2) + "\n")
    log.info("Wrote updated URDB to %s", args.path_urdb)


if __name__ == "__main__":
    main()

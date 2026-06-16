"""Generate PJM utility → zone crosswalk mapping CSV.

PJM publishes the same zone under different labels depending on the data source:

  - **Data Miner 2** feeds (e.g. ``hrl_load_metered``) use legacy transmission
    zone codes: ``BC`` for BGE, ``PEP`` for Pepco, ``AP`` for APS, etc.
  - **5CP peaks PDFs** ("Summer YYYY Peaks and 5CPs") and **RPM auction
    Excel files** use modern zone labels: ``BGE``, ``PEPCO``, ``APS``, etc.

This mapping CSV is the single place that crosswalk lives. MC scripts filter
region-wide PJM datasets down to one utility through these rows.

Output schema (one row per utility × zone; weights sum to 1.0 per utility):
  utility           – platform utility slug (lowercase, e.g. bge)
  state             – 2-char lowercase analysis state (e.g. md); drives the MC
                      output base s3://data.sb/switchbox/marginal_costs/{state}/supply/
  dataminer_zone    – PJM Data Miner legacy zone code (forward-looking for the
                      deferred hourly-loads pipeline)
  fivecp_zone_label – canonical zone label as it appears in fivecp_peaks.csv
  price_zone        – canonical zone label as it appears in rpm_capacity_prices.csv
  capacity_weight   – fraction of utility capacity obligation from this row

Note: zone ≠ retail territory. The PEPCO and DPL zones span MD + DC/DE; the
``state`` column is the analysis state, and customer-level territory filtering
happens upstream via ResStock utility assignment.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

# ── Utility mapping rows ──────────────────────────────────────────────────────
# Each tuple: (utility, state, dataminer_zone, fivecp_zone_label, price_zone,
# capacity_weight). Single-zone utilities have weight 1.0; the schema supports
# multi-row weighted utilities later (ConEd analog in NY).
#
# Source of truth for MD zone assignments:
#   - PJM Energy Credits utility↔zone table (pjmenergycredits.com/About-PJM)
#   - MD PSC Ten-Year Plan (2024–2033 and 2025–2034 editions)
#   - ODEC Wholesale Power Contract (SEC filing): confirms Choptank and A&N
#     are in the DPL zone; SMECO is in the PEPCO zone (SMECO website + PJM
#     Tariff Attachment H-9C).
#   - Somerset REC: APS in MD, PENELEC in PA (PJM Energy Credits confirms).
#
# All MD utilities map entirely (1.0) to a single PJM zone — no split-zone
# cases exist among MD electric utilities.
_MAPPING_ROWS: list[tuple[str, str, str, str, str, float]] = [
    # ── Investor-Owned Utilities ──────────────────────────────────────────────
    # BGE — Baltimore Gas & Electric; only PJM zone entirely within Maryland.
    ("bge", "md", "BC", "BGE", "BGE", 1.0),
    # Pepco — Potomac Electric Power Co.; zone also covers DC.
    ("pepco", "md", "PEP", "PEPCO", "PEPCO", 1.0),
    # Delmarva Power & Light; zone also covers DE and parts of VA.
    ("dpl", "md", "DPL", "DPL", "DPL", 1.0),
    # Potomac Edison — FirstEnergy subsidiary; operates within the APS zone
    # (Allegheny Power Systems), which also covers parts of WV, PA, VA.
    ("potomac-edison", "md", "AP", "APS", "APS", 1.0),
    # ── Cooperatives ──────────────────────────────────────────────────────────
    # SMECO — Southern Maryland Electric Cooperative; distribution co-op whose
    # 6 interties connect to the PEPCO transmission zone (per SMECO website
    # and PJM Tariff Attachment H-9C).
    ("smeco", "md", "PEP", "PEPCO", "PEPCO", 1.0),
    # Choptank Electric Cooperative — ODEC member serving MD Eastern Shore;
    # interconnected to DPL transmission system (ODEC Wholesale Contract, SEC).
    ("choptank", "md", "DPL", "DPL", "DPL", 1.0),
    # A&N Electric Cooperative — ODEC member; serves Smith Island (MD) and VA
    # Eastern Shore; interconnected to DPL transmission (ODEC Wholesale Contract).
    ("an-electric", "md", "DPL", "DPL", "DPL", 1.0),
    # Somerset Rural Electric Cooperative — Allegheny Electric Cooperative
    # member; serves Garrett County MD within the APS zone. (In PA it is in
    # the PENELEC zone, but its MD territory is APS.)
    ("somerset-rec", "md", "AP", "APS", "APS", 1.0),
    # ── Municipal Utilities ───────────────────────────────────────────────────
    # Hagerstown Light Department — municipal utility within the APS zone.
    ("hagerstown", "md", "AP", "APS", "APS", 1.0),
    # Thurmont Municipal Light Company — municipal utility within the APS zone.
    ("thurmont", "md", "AP", "APS", "APS", 1.0),
    # Town of Williamsport — municipal utility within the APS zone.
    ("williamsport", "md", "AP", "APS", "APS", 1.0),
    # Easton Utilities Commission — municipal utility within the DPL zone.
    ("easton", "md", "DPL", "DPL", "DPL", 1.0),
    # Town of Berlin Municipal Electric Plant — municipal within the DPL zone.
    ("berlin", "md", "DPL", "DPL", "DPL", 1.0),
]

# Valid Data Miner legacy transmission-zone codes (hrl_load_metered vocabulary).
VALID_DATAMINER_ZONES = frozenset(
    {
        "AE",
        "AEP",
        "AP",
        "ATSI",
        "BC",
        "CE",
        "DAY",
        "DEOK",
        "DOM",
        "DPL",
        "DUQ",
        "EKPC",
        "JC",
        "ME",
        "PE",
        "PEP",
        "PL",
        "PN",
        "PS",
        "RECO",
        "UGI",
    }
)


def build_zone_mapping() -> pl.DataFrame:
    """Build the PJM zone mapping DataFrame from the hardcoded mapping table."""
    rows: list[dict[str, str | float]] = []
    for (
        utility,
        state,
        dataminer_zone,
        fivecp_zone_label,
        price_zone,
        capacity_weight,
    ) in _MAPPING_ROWS:
        assert dataminer_zone in VALID_DATAMINER_ZONES, (
            f"dataminer_zone '{dataminer_zone}' not in {sorted(VALID_DATAMINER_ZONES)}"
        )
        rows.append(
            {
                "utility": utility,
                "state": state,
                "dataminer_zone": dataminer_zone,
                "fivecp_zone_label": fivecp_zone_label,
                "price_zone": price_zone,
                "capacity_weight": capacity_weight,
            }
        )

    return pl.DataFrame(
        rows,
        schema={
            "utility": pl.Utf8,
            "state": pl.Utf8,
            "dataminer_zone": pl.Utf8,
            "fivecp_zone_label": pl.Utf8,
            "price_zone": pl.Utf8,
            "capacity_weight": pl.Float64,
        },
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate PJM utility zone mapping CSV."
    )
    parser.add_argument(
        "--path-output-csv",
        type=str,
        required=True,
        help=(
            "Local path to write the output CSV "
            "(e.g. data/pjm/zone_mapping/csv/pjm_utility_zone_mapping.csv)."
        ),
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

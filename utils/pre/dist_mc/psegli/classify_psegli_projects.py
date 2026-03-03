"""Classify PSEG-LI MCOS projects as sub_tx or distribution for BAT.

Background
----------
PSEG-LI's 2025 MCOS filing lists 30 projects in Exhibit 2.  Each project has
an asset_class: T-Substation (15 projects), D-Substation (2), or D-Feeders (13).

The filing labels T-Substation as "Transmission" — but LIPA's system operates
at only five voltage levels (LIPA Transmission Planning Criteria, Rev 1, Dec 2022):

    Bulk Electric System (BES):  138 kV, 345 kV  (NYISO-jurisdictional)
    Sub-transmission:            23 kV, 34.5 kV, 69 kV  (local)

For BAT we need to separate bulk_tx (excluded) from sub_tx (included, like
distribution).  This script classifies each of the 15 T-Substation projects
by cross-referencing their operating voltage against public sources.

Evidence tiers
--------------
TIER 1 — Direct voltage evidence (high confidence):
    The project's operating voltage was confirmed from a public source
    (PSEG-LI reliability page, LIPA environmental assessment, NYISO
    interconnection queue, or Southampton Town filing).

TIER 2 — Systemic inference (medium confidence):
    No direct voltage evidence found, but:
    (a) No NY PSL Article VII filing exists for this project (Article VII
        is required for transmission lines ≥100 kV)
    (b) All 10 T-Substation projects with confirmed voltages are 69 kV or
        below (sub-transmission)
    (c) LIPA has no intermediate voltages — projects are either 138/345 kV
        (BES, with extensive public record) or 69 kV and below

Key finding: ZERO of the 15 T-Substation projects operate at 138 kV or above.
All are classified as sub_tx and included in BAT.  The Deerfield project is an
edge case: the Southampton-to-Deerfield cable is rated for 138 kV but will
operate at 69 kV during the study period (FY2025-2032).

Outputs
-------
  - psegli_project_classifications.csv (same directory as this script)
  - Terminal report of classification summary
"""

from __future__ import annotations

import argparse
from pathlib import Path

import fsspec
import polars as pl


# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL DECISION: Per-project voltage evidence
# ═══════════════════════════════════════════════════════════════════════════════
#
# Each entry documents:
#   - The confirmed or inferred voltage
#   - The public source used to determine the voltage
#   - A confidence level: "high" (direct evidence) or "medium" (systemic inference)
#
# TIER 1 (high confidence) — voltage confirmed from a specific public source.
# TIER 2 (medium confidence) — no direct evidence; classified by systemic
# reasoning (see Background above).
#
# The Deerfield entries (SOS 1822/2370) are the only borderline case: the cable
# is rated 138 kV (Article VII Case 24-T-0113) but will operate at 69 kV during
# the MCOS study period.  Classified as sub_tx because the operating voltage
# during the study period is 69 kV, which is sub-transmission by LIPA's own
# planning criteria.

T_SUBSTATION_EVIDENCE: dict[int, dict] = {
    # ── TIER 1: Direct voltage evidence ──────────────────────────────────
    1545: {
        "voltage_kv": "23→33",
        "confidence": "high",
        "evidence": (
            "Hither Hills: 23kV→33kV substation conversion. "
            "Source: LIPA environmental assessment (April 2024). "
            "Both voltages are well below LIPA's BES threshold (138kV). "
            "LIPA Transmission Planning Criteria Section 5: "
            "'sub-transmission system consists of 23, 34.5 kV and 69 kV.'"
        ),
    },
    2143: {
        "voltage_kv": "33→69",
        "confidence": "high",
        "evidence": (
            "Belmont: 33kV→69kV substation conversion with two new underground "
            "circuits to Lake Success and Whiteside substations. "
            "Source: psegliny.com/reliability/belmont (Jan 2025 construction start). "
            "69kV = sub-transmission per LIPA Transmission Planning Criteria Section 5."
        ),
    },
    1940: {
        "voltage_kv": "≤69",
        "confidence": "high",
        "evidence": (
            "Miller Place: distribution feeders, distribution poles, and substation "
            "expansion. PSEG LI's project page (psegliny.com/reliability/millerplace) "
            "describes 'installing new underground and overhead distribution feeders "
            "and expanding the existing substation on Route 25A.' All described work "
            "is distribution-level (new wood/steel poles, 13kV feeders). The T-Substation "
            "classification in the MCOS refers to the substation's transmission-side "
            "equipment, which on LIPA's system is 69kV or below."
        ),
    },
    1476: {
        "voltage_kv": "69",
        "confidence": "high",
        "evidence": (
            "Bridgehampton: 69kV substation with 69/13kV transformer banks. "
            "Source: PSEG LI Bridgehampton project page and Southampton Town "
            "archive (2007-2008 Article VII filing for Southampton-Bridgehampton "
            "69kV transmission line; Bridgehampton substation expansion with "
            "'new 69/13kV 33MVA transformer bank and switchgear'). "
            "Bridgehampton-Buell 69kV underground cable also confirms 69kV bus."
        ),
    },
    2588: {
        "voltage_kv": "69",
        "confidence": "high",
        "evidence": (
            "Lindbergh (Uniondale): 69kV underground transmission cables and "
            "13kV distribution feeders. Source: psegliny.com/reliability/lindbergh. "
            "Project includes 'Installation of two new underground 69kV transmission "
            "cables' and 'Installation of new underground 13kV distribution feeders.' "
            "Driven by Nassau Coliseum redevelopment area load growth."
        ),
    },
    1986: {
        "voltage_kv": "69",
        "confidence": "high",
        "evidence": (
            "Southampton: 69kV substation on the South Fork 69kV sub-transmission "
            "network. The Southampton substation is the origin point for: "
            "(a) Southampton-Bridgehampton 69kV line (Article VII, 2007), "
            "(b) Southampton-Deerfield 69kV-operated line (Article VII Case "
            "24-T-0113, construction Jan 2026). All connected substations operate "
            "at 69kV."
        ),
    },
    # ── Deerfield: edge case (138kV-rated, 69kV-operated) ───────────────
    #
    # DECISION: Classify as sub_tx.  The cable is physically 138kV-rated and
    # went through Article VII (Case 24-T-0113), but it will operate at 69kV
    # during the entire MCOS study period (FY2025-2032).  The filing includes
    # it as a T-Substation project for local load growth on the South Fork.
    # When LIPA eventually energizes it at 138kV, it will become BES — but
    # that is outside the study period.
    1822: {
        "voltage_kv": "69 (138-rated)",
        "confidence": "high",
        "evidence": (
            "Deerfield (phase 1, ISD 2027): Southampton-to-Deerfield underground "
            "transmission cable, 4.5 miles.  RATED at 138kV but OPERATED initially "
            "at 69kV.  Source: southampton2deerfield.com and DPS Case 24-T-0113 "
            "(Article VII certificate).  Project serves South Fork load growth. "
            "Classified as sub_tx because the operating voltage during the MCOS "
            "study period (FY2025-2032) is 69kV = sub-transmission per LIPA "
            "Transmission Planning Criteria Section 5.  When eventually energized "
            "at 138kV, this will become BES."
        ),
    },
    2370: {
        "voltage_kv": "69 (138-rated)",
        "confidence": "high",
        "evidence": (
            "Deerfield (phase 2, ISD 2032): same Southampton-to-Deerfield corridor "
            "as SOS 1822.  Continued sub-transmission function at 69kV operating "
            "voltage.  See SOS 1822 evidence for full details."
        ),
    },
    # ── TIER 1 via NYISO Gold Book interconnection queue ─────────────────
    #
    # The NYISO 2025 Gold Book Table VII lists proposed generator
    # interconnections at specific substations.  The interconnection voltage
    # confirms the substation's bus voltage.
    2583: {
        "voltage_kv": "69",
        "confidence": "high",
        "evidence": (
            "Moriches (phase 1, ISD 2027): 69kV substation confirmed by NYISO "
            "Gold Book 2025 Table VII interconnection queue entry C24-061: "
            "'Hanwha Q CELLS USA Corp. Moriches 69kV K 12-2027 75.3MW Energy "
            "Storage.'  The interconnection voltage of 69kV confirms the "
            "substation operates at 69kV = sub-transmission."
        ),
    },
    2358: {
        "voltage_kv": "69",
        "confidence": "high",
        "evidence": (
            "Moriches (phase 2, ISD 2031): same substation as SOS 2583. "
            "See SOS 2583 evidence (NYISO queue C24-061 confirms 69kV)."
        ),
    },
    2512: {
        "voltage_kv": "69",
        "confidence": "high",
        "evidence": (
            "Quogue Substation: 69kV confirmed by NYISO Gold Book 2025 Table VII "
            "entry C24-158: 'KCE NY 27, LLC Quogue - Tiana 69kV K 01-2028 "
            "51.5MW Energy Storage.'  Quogue-Tiana 69kV interconnection confirms "
            "the substation's bus voltage."
        ),
    },
    1102: {
        "voltage_kv": "69",
        "confidence": "high",
        "evidence": (
            "Peconic: 69kV substation confirmed by NYISO Gold Book 2025 Table VII "
            "entry C24-023: 'KCE NY 26, LLC Jamesport - Peconic 69kV K 01-2028 "
            "62.0MW Energy Storage.'  The Jamesport-Peconic 69kV interconnection "
            "confirms the substation operates at 69kV."
        ),
    },
    # ── TIER 2: Systemic inference ───────────────────────────────────────
    #
    # No direct voltage evidence found for these projects.  Classified as
    # sub_tx based on systemic reasoning:
    #   (a) No NY PSL Article VII filing (required for ≥100kV facilities)
    #   (b) 12 of 15 T-Substation projects have confirmed 69kV or below
    #   (c) LIPA has no intermediate voltages: 138/345kV = BES, ≤69kV = sub-TX
    #   (d) 138kV projects generate extensive public record (environmental
    #       review, NYISO filings); absence of such record is informative
    1540: {
        "voltage_kv": "≤69 (inferred)",
        "confidence": "medium",
        "evidence": (
            "North Bellmore (ISD 2026): no direct voltage evidence found. "
            "No NY PSL Article VII filing exists for this project (required for "
            "≥100kV transmission lines).  Classified as sub_tx by systemic "
            "inference: all 12 T-Substation projects with confirmed voltages are "
            "69kV or below; LIPA's system has no intermediate voltages between "
            "69kV (sub-TX) and 138kV (BES); and 138kV projects on LIPA's system "
            "generate extensive public record that is absent here."
        ),
    },
    1041: {
        "voltage_kv": "≤69 (inferred)",
        "confidence": "medium",
        "evidence": (
            "New South Road (ISD 2028): no direct voltage evidence found. "
            "No public project page, no Article VII filing. "
            "Classified as sub_tx by systemic inference (see SOS 1540 reasoning). "
            "Capacity of 33 MVA is consistent with a 69kV sub-TX substation "
            "(comparable to Miller Place, North Bellmore at 33 MVA each)."
        ),
    },
    2330: {
        "voltage_kv": "≤69 (inferred)",
        "confidence": "medium",
        "evidence": (
            "Arverne (ISD 2030): no direct voltage confirmed, but the Rockaway "
            "peninsula sub-transmission system connects Far Rockaway, Arverne, and "
            "Rockaway Beach substations (per MFS Engineers project reference for "
            "'2H Far Rockaway Substation - 2AR Arverne Substation').  PSEG LI's "
            "completed Arverne Reliability Project (2018-2019) upgraded underground "
            "cable and distribution infrastructure in the area.  No Article VII "
            "filing.  Classified as sub_tx by systemic inference."
        ),
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# Classification logic
# ═══════════════════════════════════════════════════════════════════════════════

ASSET_CLASS_TO_BAT_CLASS = {
    "T-Substation": "sub_tx",
    "D-Substation": "distribution",
    "D-Feeders": "distribution",
}


def classify_project(row: dict) -> dict:
    """Classify a single project, returning extra columns to append."""
    sos_id = int(row["sos_id"])
    asset_class = row["asset_class"]

    classification = ASSET_CLASS_TO_BAT_CLASS[asset_class]

    if asset_class == "T-Substation":
        ev = T_SUBSTATION_EVIDENCE.get(sos_id)
        if ev is None:
            msg = f"No evidence entry for T-Substation SOS {sos_id} ({row['location']})"
            raise ValueError(msg)
        return {
            "classification": classification,
            "bat_included": True,
            "voltage_kv": ev["voltage_kv"],
            "confidence": ev["confidence"],
            "inference_method": "tier1_direct"
            if ev["confidence"] == "high"
            else "tier2_systemic",
            "evidence": ev["evidence"],
        }

    return {
        "classification": classification,
        "bat_included": True,
        "voltage_kv": "≤13.2",
        "confidence": "high",
        "inference_method": "asset_class_distribution",
        "evidence": (
            f"{asset_class} project at {row['location']}. "
            f"Distribution assets (D-Substation, D-Feeders) are unambiguously "
            f"below sub-transmission voltage and included in BAT."
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify PSEG-LI MCOS projects as sub_tx or distribution"
    )
    parser.add_argument(
        "--path-csv",
        required=True,
        help="Path to raw project list CSV (local or S3)",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    output_path = script_dir / "psegli_project_classifications.csv"

    path_csv: str = args.path_csv
    print(f"Reading project list from {path_csv}")
    if path_csv.startswith("s3://"):
        fs = fsspec.filesystem("s3")
        with fs.open(path_csv, "rb") as f:
            df = pl.read_csv(f)
    else:
        df = pl.read_csv(path_csv)
    print(f"  {len(df)} projects loaded")

    # Classify each project
    extra_rows: list[dict] = []
    for row in df.iter_rows(named=True):
        extra = classify_project(row)
        extra_rows.append(extra)

    df_extra = pl.DataFrame(extra_rows)
    df_out = pl.concat([df, df_extra], how="horizontal")

    # ── Summary ──
    print("\n=== Classification Summary ===")
    summary = (
        df_out.group_by("classification")
        .agg(
            pl.len().alias("n_projects"),
            pl.col("load_capacity_mva").sum().alias("total_mva"),
        )
        .sort("classification")
    )
    for row in summary.iter_rows(named=True):
        print(
            f"  {row['classification']:<15} "
            f"{row['n_projects']:>3} projects  "
            f"{row['total_mva']:>10,.1f} MVA"
        )

    # ── Confidence breakdown ──
    print("\n=== Confidence Breakdown (T-Substation only) ===")
    t_sub = df_out.filter(pl.col("asset_class") == "T-Substation")
    conf_summary = (
        t_sub.group_by("confidence")
        .agg(
            pl.len().alias("n"),
            pl.col("load_capacity_mva").sum().alias("mva"),
        )
        .sort("confidence")
    )
    for row in conf_summary.iter_rows(named=True):
        print(
            f"  {row['confidence']:<8} {row['n']:>3} projects  {row['mva']:>10,.1f} MVA"
        )

    # ── Per-project detail for T-Substations ──
    print("\n=== T-Substation Classifications ===")
    for row in t_sub.iter_rows(named=True):
        print(
            f"\n  SOS {row['sos_id']:>4} | {row['location']:<20} | "
            f"{row['load_capacity_mva']:>7,.1f} MVA | "
            f"voltage={row['voltage_kv']:<16} | "
            f"confidence={row['confidence']}"
        )
        ev = row["evidence"]
        print(f"    {ev[:120]}{'...' if len(ev) > 120 else ''}")

    # ── Write CSV ──
    df_out.write_csv(output_path)
    print(f"\nWrote {len(df_out)} rows to {output_path}")


if __name__ == "__main__":
    main()

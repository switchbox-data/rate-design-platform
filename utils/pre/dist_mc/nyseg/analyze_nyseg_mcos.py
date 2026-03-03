"""Compute marginal costs from a CRA-prepared NYSEG MCOS workbook.

Reads per-project data from **W2 (Investment Location Detail)** and
applies NERA-style project-level aggregation for cross-utility
consistency.  This replaces the earlier approach of reading pre-computed
MC tables from T1A/T2, which used CRA's location-specific growth
factors, demand-loss-adjusted capacities, and within-division
adjustments.  By aggregating project-level capital and capacity directly,
NYSEG/RG&E now follow the same methodology as NiMo, ConEd, O&R, and
CenHud (see context/domain/ny_mcos_studies_comparison.md §9).

Cost centers (at primary voltage level):
  1. Upstream — substation (115 kV/46 kV) + feeder (115 kV/34.5 kV)
  2. Distribution Substation (12.5 kV)
  3. Primary Feeder (12.5 kV/4 kV)

Four MC variants (8 CSVs):
  1. Cumulative diluted    — costs for projects in service ≤ Y ÷ system peak
  2. Incremental diluted   — costs for projects entering in Y ÷ system peak
  3. Cumulative undiluted  — capacity-weighted avg for projects in service ≤ Y
  4. Incremental undiluted — capacity-weighted avg for projects entering in Y

Data sources within the workbook:
  W2  — Per-project investment, capacity, and in-service date
  W4  — Demand-related loss factors (for "Total at Primary" column)
  T4  — System peak (row 20, used for verification only; peak is a CLI arg)

Usage (via Justfile):
    just analyze-nyseg
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fsspec
import openpyxl
import polars as pl


# ── Study parameters ─────────────────────────────────────────────────────────

YEARS = list(range(2026, 2036))
N_YEARS = len(YEARS)
INFLATION_RATE = 0.02

COST_CENTERS = ["upstream", "dist_sub", "primary_feeder"]
BUCKET_KEYS = [*COST_CENTERS, "total"]
BUCKET_LABELS = {
    "upstream": "Upstream (Sub + Feeder)",
    "dist_sub": "Distribution Substation",
    "primary_feeder": "Primary Feeder",
    "total": "Total at Primary",
}

SUB_COST_CENTERS: dict[str, list[str]] = {
    "upstream": ["ups_sub", "ups_feed"],
    "dist_sub": ["dist_sub"],
    "primary_feeder": ["primary_feeder"],
}

VARIANT_NAMES = [
    "cumulative_diluted",
    "incremental_diluted",
    "cumulative_undiluted",
    "incremental_undiluted",
]


# ── W2 column layout (identical for NYSEG and RG&E) ─────────────────────────

W2_DATA_START_ROW = 15
W2_COL_DIVISION = 3
W2_COL_SEGMENT = 5
W2_COL_SUBSTATION = 6
W2_COL_EQUIPMENT = 7
W2_COL_ISD = 9
W2_COL_TOTAL_CAPITAL = 20
W2_COL_LOAD_CARRYING = 63
W2_COL_INVEST_KW_START = 31  # $/kW investment, 2026..2035
W2_COL_FINAL_KW_START = 51  # fully loaded annualized $/kW-yr


# ── Data structures ──────────────────────────────────────────────────────────


@dataclass
class ProjectData:
    name: str
    division: str
    segment: str
    equipment: str
    cost_center: str  # ups_sub | ups_feed | dist_sub | primary_feeder
    isd: int
    total_capital_k: float  # $000s
    final_capacity_mva: float
    capital_per_kw: float  # $/kW = total_capital_k / final_capacity_mva
    annualized_per_kw: float  # $/kW-yr at ISD prices


@dataclass
class LossFactors:
    upstream: float = 1.0
    dist_sub: float = 1.0
    primary_feeder: float = 1.0


@dataclass
class CRAConfig:
    """Configuration for a CRA-style NYSEG/RGE MCOS workbook."""

    utility: str
    display_name: str
    system_peak_mw: float
    divisions: list[str] = field(default_factory=list)


# ── Parsing: W2 (projects) ──────────────────────────────────────────────────


def _classify_cost_center(segment: str, equipment: str) -> str:
    seg = segment.strip().lower()
    equip = equipment.strip().lower()
    if seg == "upstream":
        return "ups_sub" if "sub" in equip else "ups_feed"
    return "dist_sub" if "sub" in equip else "primary_feeder"


def _derive_composite_rate(ws: Any, equipment_keyword: str) -> float:
    """Derive the composite rate (ECC × O&M/A&G loading) for an equipment type.

    Finds the first project of the given type that has nonzero values at its
    ISD year and returns col_51(ISD) / col_31(ISD).
    """
    for r in range(W2_DATA_START_ROW, ws.max_row + 1):
        equip = ws.cell(r, W2_COL_EQUIPMENT).value
        isd = ws.cell(r, W2_COL_ISD).value
        if not equip or not isd:
            continue
        if equipment_keyword not in str(equip).lower():
            continue
        isd_yr = int(isd)
        if isd_yr < YEARS[0] or isd_yr > YEARS[-1]:
            continue
        offset = isd_yr - YEARS[0]
        inv_kw = ws.cell(r, W2_COL_INVEST_KW_START + offset).value
        final_kw = ws.cell(r, W2_COL_FINAL_KW_START + offset).value
        if inv_kw and final_kw and float(inv_kw) > 0 and float(final_kw) > 0:
            return float(final_kw) / float(inv_kw)
    msg = f"Could not derive composite rate for equipment '{equipment_keyword}'"
    raise ValueError(msg)


def parse_w2_projects(
    ws: Any,
) -> tuple[list[ProjectData], dict[str, float]]:
    """Parse all projects from W2 and derive composite rates.

    Returns ``(projects, composite_rates)`` where ``composite_rates`` maps
    ``"substation"`` and ``"feeder"`` to their annualization rates.
    """
    sub_rate = _derive_composite_rate(ws, "sub")
    feed_rate = _derive_composite_rate(ws, "feed")

    projects: list[ProjectData] = []
    for r in range(W2_DATA_START_ROW, ws.max_row + 1):
        name = ws.cell(r, W2_COL_SUBSTATION).value
        if not name:
            continue
        seg = ws.cell(r, W2_COL_SEGMENT).value
        equip = ws.cell(r, W2_COL_EQUIPMENT).value
        isd = ws.cell(r, W2_COL_ISD).value
        total_k = ws.cell(r, W2_COL_TOTAL_CAPITAL).value
        final_cap = ws.cell(r, W2_COL_LOAD_CARRYING).value

        if not all([seg, equip, isd]):
            continue
        if not total_k or not final_cap or float(final_cap) <= 0:
            continue

        isd_yr = int(isd)
        if isd_yr < YEARS[0] or isd_yr > YEARS[-1]:
            continue

        cc = _classify_cost_center(str(seg), str(equip))
        rate = sub_rate if "sub" in str(equip).lower() else feed_rate
        cap_per_kw = float(total_k) / float(final_cap)  # $000s/MVA = $/kW
        annualized = cap_per_kw * rate

        projects.append(
            ProjectData(
                name=str(name),
                division=str(ws.cell(r, W2_COL_DIVISION).value or ""),
                segment=str(seg),
                equipment=str(equip),
                cost_center=cc,
                isd=isd_yr,
                total_capital_k=float(total_k),
                final_capacity_mva=float(final_cap),
                capital_per_kw=cap_per_kw,
                annualized_per_kw=annualized,
            )
        )

    rates = {"substation": sub_rate, "feeder": feed_rate}
    return projects, rates


# ── Parsing: W4 (loss factors) ──────────────────────────────────────────────


def parse_w4_loss_factors(ws: Any) -> LossFactors:
    """Extract demand-related loss factors from W4.

    Finds the "Primary" column dynamically (position differs between NYSEG
    and RG&E) and reads the upstream→primary, dist-sub→primary, and
    primary→primary factors.
    """
    primary_col: int | None = None
    for c in range(1, ws.max_column + 1):
        v = ws.cell(10, c).value
        if v and "primary" in str(v).lower():
            primary_col = c
            break
    if primary_col is None:
        return LossFactors()

    def _read(row: int) -> float:
        v = ws.cell(row, primary_col).value
        if v is not None and str(v).strip() != "-----":
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
        return 1.0

    return LossFactors(upstream=_read(12), dist_sub=_read(13), primary_feeder=_read(14))


# ── Computation ──────────────────────────────────────────────────────────────


def _cc_of_project(p: ProjectData) -> str:
    """Return the aggregated cost-center bucket for a project."""
    for cc, subs in SUB_COST_CENTERS.items():
        if p.cost_center in subs:
            return cc
    return "primary_feeder"


def compute_variants(
    projects: list[ProjectData],
    system_peak_mw: float,
    loss: LossFactors,
) -> dict[str, dict[str, list[float]]]:
    """Compute all four MC variants from project-level data."""
    loss_map = {
        "upstream": loss.upstream,
        "dist_sub": loss.dist_sub,
        "primary_feeder": loss.primary_feeder,
    }

    def _total_from_cc(per_cc: dict[str, list[float]], yi: int) -> float:
        return sum(per_cc[cc][yi] * loss_map[cc] for cc in COST_CENTERS)

    # Pre-group projects by ISD and cost center for efficiency
    by_isd: dict[int, list[ProjectData]] = {yr: [] for yr in YEARS}
    for p in projects:
        if p.isd in by_isd:
            by_isd[p.isd].append(p)

    # ── Incremental diluted ──────────────────────────────────────────────
    inc_dil: dict[str, list[float]] = {k: [] for k in BUCKET_KEYS}
    for yi, yr in enumerate(YEARS):
        for cc in COST_CENTERS:
            subs = SUB_COST_CENTERS[cc]
            yr_p = [p for p in by_isd[yr] if p.cost_center in subs]
            cost = sum(p.annualized_per_kw * p.final_capacity_mva for p in yr_p)
            inc_dil[cc].append(cost / system_peak_mw if system_peak_mw > 0 else 0.0)
        inc_dil["total"].append(_total_from_cc(inc_dil, yi))

    # ── Cumulative diluted ───────────────────────────────────────────────
    cum_dil: dict[str, list[float]] = {k: [] for k in BUCKET_KEYS}
    for yi in range(N_YEARS):
        for cc in COST_CENTERS:
            val = sum(
                inc_dil[cc][ti] * (1 + INFLATION_RATE) ** (yi - ti)
                for ti in range(yi + 1)
            )
            cum_dil[cc].append(val)
        cum_dil["total"].append(_total_from_cc(cum_dil, yi))

    # ── Incremental undiluted ────────────────────────────────────────────
    inc_und: dict[str, list[float]] = {k: [] for k in BUCKET_KEYS}
    for yr in YEARS:
        for cc in COST_CENTERS:
            subs = SUB_COST_CENTERS[cc]
            yr_p = [p for p in by_isd[yr] if p.cost_center in subs]
            num = sum(p.annualized_per_kw * p.final_capacity_mva for p in yr_p)
            den = sum(p.final_capacity_mva for p in yr_p)
            inc_und[cc].append(num / den if den > 0 else 0.0)
        all_yr = by_isd[yr]
        num_t = sum(
            p.annualized_per_kw * p.final_capacity_mva * loss_map[_cc_of_project(p)]
            for p in all_yr
        )
        den_t = sum(p.final_capacity_mva for p in all_yr)
        inc_und["total"].append(num_t / den_t if den_t > 0 else 0.0)

    # ── Cumulative undiluted ─────────────────────────────────────────────
    cum_und: dict[str, list[float]] = {k: [] for k in BUCKET_KEYS}
    for yi, yr in enumerate(YEARS):
        for cc in COST_CENTERS:
            subs = SUB_COST_CENTERS[cc]
            scope = [p for p in projects if p.isd <= yr and p.cost_center in subs]
            num = sum(
                p.annualized_per_kw
                * p.final_capacity_mva
                * (1 + INFLATION_RATE) ** (yr - p.isd)
                for p in scope
            )
            den = sum(p.final_capacity_mva for p in scope)
            cum_und[cc].append(num / den if den > 0 else 0.0)
        scope_all = [p for p in projects if p.isd <= yr]
        num_t = sum(
            p.annualized_per_kw
            * p.final_capacity_mva
            * loss_map[_cc_of_project(p)]
            * (1 + INFLATION_RATE) ** (yr - p.isd)
            for p in scope_all
        )
        den_t = sum(p.final_capacity_mva for p in scope_all)
        cum_und["total"].append(num_t / den_t if den_t > 0 else 0.0)

    return {
        "incremental_diluted": inc_dil,
        "cumulative_diluted": cum_dil,
        "incremental_undiluted": inc_und,
        "cumulative_undiluted": cum_und,
    }


def to_real(nominal: list[float]) -> list[float]:
    """Convert nominal values to base-year (2026) real dollars."""
    return [v / (1 + INFLATION_RATE) ** i for i, v in enumerate(nominal)]


# ── CSV export ───────────────────────────────────────────────────────────────


def export_annualized_csv(mc_nominal: dict[str, list[float]], path: Path) -> None:
    rows: list[dict[str, object]] = []
    for yi, yr in enumerate(YEARS):
        nom = mc_nominal["total"][yi]
        real_val = nom / (1 + INFLATION_RATE) ** yi
        rows.append(
            {
                "year": yr,
                "bulk_tx_nominal": 0.0,
                "bulk_tx_real": 0.0,
                "sub_tx_and_dist_nominal": round(nom, 4),
                "sub_tx_and_dist_real": round(real_val, 4),
            }
        )
    pl.DataFrame(rows).write_csv(path)
    print(f"  Wrote {path}")


def export_levelized_csv(mc_nominal: dict[str, list[float]], path: Path) -> None:
    total_nominal = mc_nominal["total"]
    total_real = to_real(total_nominal)
    lev_real = sum(total_real) / len(total_real) if total_real else 0.0
    final_year_real = total_real[-1] if total_real else 0.0
    final_year_nominal = total_nominal[-1] if total_nominal else 0.0
    rows: list[dict[str, object]] = [
        {
            "bucket": "bulk_tx",
            "label": "Bulk TX",
            "levelized_mc_kw_yr": 0.0,
            "final_year_real_mc_kw_yr": 0.0,
            "final_year_nominal_mc_kw_yr": 0.0,
        },
        {
            "bucket": "sub_tx_and_dist",
            "label": "Sub-TX + Distribution",
            "levelized_mc_kw_yr": round(lev_real, 4),
            "final_year_real_mc_kw_yr": round(final_year_real, 4),
            "final_year_nominal_mc_kw_yr": round(final_year_nominal, 4),
        },
    ]
    pl.DataFrame(rows).write_csv(path)
    print(f"  Wrote {path}")


# ── Terminal report ──────────────────────────────────────────────────────────


def print_report(
    config: CRAConfig,
    projects: list[ProjectData],
    composite_rates: dict[str, float],
    loss: LossFactors,
    variants: dict[str, dict[str, list[float]]],
) -> None:
    W = 80
    print("=" * W)
    print(f"{config.display_name} MCOS Analysis — NERA-style project-level aggregation")
    print("=" * W)

    print(f"\n── System {'─' * (W - 11)}")
    print(f"  System peak (2035 forecast):  {config.system_peak_mw:,.2f} MW")
    print(f"  Study period:                 {YEARS[0]}–{YEARS[-1]} ({N_YEARS} years)")
    print(f"  Inflation:                    {INFLATION_RATE * 100:.1f}%/yr")
    print(f"  Projects parsed:              {len(projects)}")
    print(f"  Composite rate (substation):  {composite_rates['substation']:.5f}")
    print(f"  Composite rate (feeder):      {composite_rates['feeder']:.5f}")
    print(f"  Loss factor (upstream→pri):   {loss.upstream:.4f}")
    print(f"  Loss factor (dist_sub→pri):   {loss.dist_sub:.4f}")
    print(f"  Loss factor (pf→pri):         {loss.primary_feeder:.4f}")

    # Project summary by cost center and ISD
    from collections import Counter

    by_cc = Counter(p.cost_center for p in projects)
    print(f"\n── Projects by cost center {'─' * max(1, W - 28)}")
    for cc in ["ups_sub", "ups_feed", "dist_sub", "primary_feeder"]:
        print(f"  {cc:<18} {by_cc[cc]:>3} projects")

    by_isd = Counter(p.isd for p in projects)
    print(f"\n── Projects by ISD year {'─' * max(1, W - 25)}")
    for yr in YEARS:
        if by_isd[yr]:
            print(f"  {yr}  {by_isd[yr]:>3} projects")
        else:
            print(f"  {yr}    — (no projects)")

    # Levelized MC
    print(f"\n── Levelized MC (real $/kW-yr, simple avg) {'─' * max(1, W - 43)}")
    header = f"  {'Variant':<28}"
    for k in BUCKET_KEYS:
        header += f" {BUCKET_LABELS[k]:>14}"
    print(header)
    print(f"  {'─' * (W - 4)}")
    for vname in VARIANT_NAMES:
        data = variants[vname]
        real_values = {k: to_real(data[k]) for k in BUCKET_KEYS}
        levs = {k: sum(real_values[k]) / N_YEARS for k in BUCKET_KEYS}
        line = f"  {vname:<28}"
        for k in BUCKET_KEYS:
            line += f" ${levs[k]:>12.2f}"
        print(line)

    # Year-by-year incremental diluted
    print(
        f"\n── Year-by-year incremental diluted (nominal $/kW-yr) "
        f"{'─' * max(1, W - 52)}"
    )
    inc = variants["incremental_diluted"]
    header2 = f"  {'Year':>4}"
    for k in BUCKET_KEYS:
        header2 += f" {k:>12}"
    print(header2)
    for yi in range(N_YEARS):
        line = f"  {YEARS[yi]:>4}"
        for k in BUCKET_KEYS:
            line += f" ${inc[k][yi]:>10.2f}"
        print(line)

    print("\n" + "=" * W)


# ── Pipeline ─────────────────────────────────────────────────────────────────


def run_analysis(
    xlsx_path: str,
    config: CRAConfig,
    output_dir: Path,
) -> None:
    """Run the full NERA-style project-level MCOS analysis."""
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {config.display_name} workbook from {xlsx_path} ...")
    if xlsx_path.startswith("s3://"):
        with fsspec.open(xlsx_path, "rb") as f:
            wb = openpyxl.load_workbook(f, data_only=True)
    else:
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)

    # 1. Parse projects from W2
    ws_w2 = _find_sheet(wb, "W2")
    projects, composite_rates = parse_w2_projects(ws_w2)
    print(f"  W2: {len(projects)} projects parsed")

    # 2. Parse loss factors from W4
    ws_w4 = _find_sheet(wb, "W4")
    loss = parse_w4_loss_factors(ws_w4)
    print(
        f"  W4: loss factors — ups={loss.upstream:.4f}, "
        f"ds={loss.dist_sub:.4f}, pf={loss.primary_feeder:.4f}"
    )

    # 3. Compute all four MC variants
    variants = compute_variants(projects, config.system_peak_mw, loss)
    print("  Computed 4 MC variants")

    # 4. Export CSVs
    print("\nExporting CSVs:")
    for vname in VARIANT_NAMES:
        prefix = f"{config.utility}_{vname}"
        export_annualized_csv(variants[vname], output_dir / f"{prefix}_annualized.csv")
        export_levelized_csv(variants[vname], output_dir / f"{prefix}_levelized.csv")

    # 5. Report
    print()
    print_report(config, projects, composite_rates, loss, variants)


def _find_sheet(wb: openpyxl.Workbook, pattern: str) -> Any:
    for name in wb.sheetnames:
        if pattern in name:
            return wb[name]
    msg = f"No sheet matching '{pattern}' in {wb.sheetnames}"
    raise ValueError(msg)


NYSEG_DIVISIONS = [
    "Auburn",
    "Binghamton",
    "Brewster",
    "Elmira",
    "Geneva",
    "Hornell",
    "Ithaca",
    "Lancaster",
    "Liberty",
    "Lockport",
    "Mechanicville",
    "Oneonta",
    "Plattsburgh",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="NYSEG MCOS analysis")
    parser.add_argument("--path-xlsx", required=True)
    parser.add_argument("--system-peak-mw", type=float, required=True)
    parser.add_argument("--path-output-dir", required=True)
    args = parser.parse_args()

    config = CRAConfig(
        utility="nyseg",
        display_name="NYSEG",
        system_peak_mw=args.system_peak_mw,
        divisions=NYSEG_DIVISIONS,
    )

    run_analysis(args.path_xlsx, config, Path(args.path_output_dir))


if __name__ == "__main__":
    main()

"""Compute marginal costs from PSEG-LI's 2025 MCOS filing.

Reads the classified project list (CSV, output of classify_psegli_projects.py)
and combines with Exhibit 1's aggregate ECCR+O&M rates to compute marginal costs
by cost center in four variants:

  1. Cumulative diluted    — all in-service capacity ÷ system peak
  2. Incremental diluted   — new capacity entering in year Y ÷ system peak (BAT input)
  3. Cumulative undiluted  — all in-service capacity ÷ cumulative capacity
  4. Incremental undiluted — new capacity entering in year Y ÷ annual capacity

Cost centers (from voltage-based classification):
  1. Sub-TX (T-Substation) — all 15 T-Substation projects are ≤69kV local
     sub-transmission, INCLUDED in BAT
  2. Distribution (D-Substation + D-Feeders) — included in BAT

All projects are included in BAT.  See classify_psegli_projects.py for the
per-project voltage evidence that determined zero T-Substation projects are
bulk (138kV+) transmission.

The filing's Exhibit 1 reports aggregate capital/kW rates ($563.17 TX, $721.12
Dist) that include a 2.9% General Plant Adder and Handy-Whitman escalation to
2025 — adjustments we cannot replicate from project-level data alone.  We
therefore use the Exhibit 1 rates for per-kW MC computation and the project
list only for (a) classification and (b) actual in-service year timing.

We apply a 2.1%/yr GDP deflator (base year 2025) for nominal values,
matching the deflator used by CenHud, NiMo, ConEd, and O&R.

Usage (via Justfile):
    just analyze-psegli
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import polars as pl


# ── Study parameters ─────────────────────────────────────────────────────────

YEARS = list(range(2025, 2033))
N_YEARS = len(YEARS)
BASE_YEAR = 2025
GDP_DEFLATOR_RATE = 0.021

COST_CENTERS = ["sub_tx", "distribution"]
BUCKET_KEYS = [*COST_CENTERS, "total"]

BUCKET_LABELS = {
    "sub_tx": "Sub-TX (T-Substation, ≤69kV)",
    "distribution": "Distribution (D-Substation + D-Feeders)",
    "total": "Total (Sub-TX + Dist)",
}

VARIANT_NAMES = [
    "cumulative_diluted",
    "incremental_diluted",
    "cumulative_undiluted",
    "incremental_undiluted",
]


# ── Filing parameters (Exhibit 1) ────────────────────────────────────────────
# These aggregate rates include General Plant Adder and Handy-Whitman
# escalation that cannot be derived from Exhibit 2 project costs alone.
# The "TX" rate applies to all T-Substation projects (now classified as sub_tx),
# and the "Dist" rate applies to D-Substation and D-Feeders projects.


@dataclass
class FilingComponent:
    """Aggregate parameters for one cost component from Exhibit 1."""

    name: str
    capital_per_kw: float  # $/kW of component capacity (Exhibit 1, cols 4-5)
    eccr_om_rate: float  # combined ECCR + O&M rate (Exhibit 1, cols 6-7)

    @property
    def undiluted_mc(self) -> float:
        """Annual MC per kW of component capacity (base-year real)."""
        return self.capital_per_kw * self.eccr_om_rate


SUB_TX_PARAMS = FilingComponent(
    name="Sub-TX (T-Substation)",
    capital_per_kw=563.17,
    eccr_om_rate=0.082,
)

DIST_PARAMS = FilingComponent(
    name="Distribution",
    capital_per_kw=721.12,
    eccr_om_rate=0.139,
)

COMPONENT_PARAMS = {"sub_tx": SUB_TX_PARAMS, "distribution": DIST_PARAMS}


# ── Classification → cost center mapping ─────────────────────────────────────

CLASSIFICATION_TO_COST_CENTER = {
    "sub_tx": "sub_tx",
    "distribution": "distribution",
}


# ── Data structures ──────────────────────────────────────────────────────────


@dataclass
class MCRow:
    """One year's MC for a bucket."""

    year: int
    cost_real_k: float
    cost_nominal_k: float
    denominator_mw: float
    nominal_mc: float
    real_mc: float


# ── CSV reading ──────────────────────────────────────────────────────────────


def read_classified_projects(path_csv: str) -> pl.DataFrame:
    """Read the classified project list CSV (local path)."""
    return pl.read_csv(path_csv)


def group_capacity_by_year(df: pl.DataFrame) -> dict[str, dict[int, float]]:
    """Group capacity (MVA, treated as MW with PF=1) by cost center and ISD year.

    Pre-2025 ISDs are clamped to 2025 (the first study year).
    """
    df = df.with_columns(
        pl.col("classification")
        .replace_strict(CLASSIFICATION_TO_COST_CENTER)
        .alias("cost_center"),
        pl.col("in_service_date")
        .str.to_date("%m/%d/%Y")
        .dt.year()
        .clip(lower_bound=BASE_YEAR)
        .alias("isd_year"),
    )

    grouped = (
        df.group_by("cost_center", "isd_year")
        .agg(pl.col("load_capacity_mva").sum())
        .sort("cost_center", "isd_year")
    )

    result: dict[str, dict[int, float]] = {cc: {} for cc in COST_CENTERS}
    for row in grouped.iter_rows(named=True):
        cc = row["cost_center"]
        yr = row["isd_year"]
        result[cc][yr] = row["load_capacity_mva"]

    return result


def get_component_totals(
    cap_by_year: dict[str, dict[int, float]],
) -> dict[str, float]:
    return {cc: sum(years.values()) for cc, years in cap_by_year.items()}


# ── Computation ──────────────────────────────────────────────────────────────


def _escalation(year: int) -> float:
    return (1 + GDP_DEFLATOR_RATE) ** (year - BASE_YEAR)


def compute_component_mc(
    params: FilingComponent,
    cap_by_year: dict[int, float],
    system_peak_mw: float,
    *,
    cumulative: bool,
    diluted: bool,
) -> list[MCRow]:
    mc_real = params.undiluted_mc
    system_peak_kw = system_peak_mw * 1000
    rows: list[MCRow] = []

    cumulative_cap_mw = 0.0

    for year in YEARS:
        esc = _escalation(year)
        entering_mw = cap_by_year.get(year, 0.0)
        cumulative_cap_mw += entering_mw

        if cumulative:
            cap_in_scope_mw = cumulative_cap_mw
        else:
            cap_in_scope_mw = entering_mw

        cap_in_scope_kw = cap_in_scope_mw * 1000
        annual_cost_real_k = mc_real * cap_in_scope_mw
        annual_cost_nominal_k = annual_cost_real_k * esc

        if diluted:
            denom_kw = system_peak_kw
        else:
            denom_kw = cap_in_scope_kw

        real_mc_val = annual_cost_real_k * 1000 / denom_kw if denom_kw > 0 else 0.0
        nominal_mc_val = real_mc_val * esc

        rows.append(
            MCRow(
                year=year,
                cost_real_k=annual_cost_real_k,
                cost_nominal_k=annual_cost_nominal_k,
                denominator_mw=denom_kw / 1000,
                nominal_mc=nominal_mc_val,
                real_mc=real_mc_val,
            )
        )

    return rows


def compute_total_mc(
    component_mc: dict[str, list[MCRow]],
    system_peak_mw: float,
    *,
    diluted: bool,
) -> list[MCRow]:
    """Combine Sub-TX + Dist into total.

    Diluted: additive (costs sum, denominator is system peak).
    Undiluted: capacity-weighted average.
    """
    rows: list[MCRow] = []
    for yi in range(N_YEARS):
        cost_real_k = sum(component_mc[cc][yi].cost_real_k for cc in COST_CENTERS)
        cost_nominal_k = sum(component_mc[cc][yi].cost_nominal_k for cc in COST_CENTERS)

        if diluted:
            denom_mw = system_peak_mw
        else:
            denom_mw = sum(component_mc[cc][yi].denominator_mw for cc in COST_CENTERS)

        denom_kw = denom_mw * 1000
        real_mc_val = cost_real_k * 1000 / denom_kw if denom_kw > 0 else 0.0
        esc = _escalation(YEARS[yi])
        nominal_mc_val = real_mc_val * esc

        rows.append(
            MCRow(
                year=YEARS[yi],
                cost_real_k=cost_real_k,
                cost_nominal_k=cost_nominal_k,
                denominator_mw=denom_mw,
                nominal_mc=nominal_mc_val,
                real_mc=real_mc_val,
            )
        )

    return rows


def levelized(rows: list[MCRow]) -> float:
    return sum(r.real_mc for r in rows) / len(rows) if rows else 0.0


# ── CSV export ───────────────────────────────────────────────────────────────


def export_levelized_csv(
    mc_data: dict[str, list[MCRow]],
    component_totals: dict[str, float],
    n_projects_by_cc: dict[str, int],
    path: Path,
) -> None:
    rows = []
    for key in BUCKET_KEYS:
        mc_rows = mc_data[key]
        lev = levelized(mc_rows)
        final_real = mc_rows[-1].real_mc
        final_nom = mc_rows[-1].nominal_mc

        if key in component_totals:
            cap = component_totals[key]
            n_proj = n_projects_by_cc[key]
        else:
            cap = sum(component_totals.values())
            n_proj = sum(n_projects_by_cc.values())

        rows.append(
            {
                "bucket": key,
                "label": BUCKET_LABELS[key],
                "n_projects": n_proj,
                "capacity_mw": round(cap, 1),
                "levelized_mc_kw_yr": round(lev, 2),
                "final_year_real_mc_kw_yr": round(final_real, 2),
                "final_year_nominal_mc_kw_yr": round(final_nom, 2),
            }
        )
    pl.DataFrame(rows).write_csv(path)
    print(f"  Wrote {path}")


def export_annualized_csv(mc_data: dict[str, list[MCRow]], path: Path) -> None:
    rows = []
    for yi in range(N_YEARS):
        row: dict[str, object] = {"year": YEARS[yi]}
        for key in BUCKET_KEYS:
            row[f"{key}_nominal"] = round(mc_data[key][yi].nominal_mc, 2)
            row[f"{key}_real"] = round(mc_data[key][yi].real_mc, 2)
        rows.append(row)
    pl.DataFrame(rows).write_csv(path)
    print(f"  Wrote {path}")


# ── Terminal report ──────────────────────────────────────────────────────────


def print_report(
    variants: dict[str, dict[str, list[MCRow]]],
    system_peak_mw: float,
    cap_by_year: dict[str, dict[int, float]],
    component_totals: dict[str, float],
    n_projects_by_cc: dict[str, int],
) -> None:
    W = 80
    print("=" * W)
    print("PSEG-LI MCOS Analysis — Classified Project Data")
    print("=" * W)

    total_cap = sum(component_totals.values())
    total_proj = sum(n_projects_by_cc.values())
    print(f"\n── System {'─' * (W - 11)}")
    print(f"  System peak (2024 actual):  {system_peak_mw:,.0f} MW")
    print(f"  Study period:               {YEARS[0]}–{YEARS[-1]} ({N_YEARS} years)")
    print(
        f"  Applied escalation:         {GDP_DEFLATOR_RATE * 100:.1f}%/yr GDP deflator (base {BASE_YEAR})"
    )
    print(f"  Total projects:             {total_proj}")
    print(f"  Total capacity:             {total_cap:,.1f} MW")
    for cc in COST_CENTERS:
        print(
            f"    {BUCKET_LABELS[cc]:40s}  {component_totals[cc]:>8,.1f} MW  ({n_projects_by_cc[cc]} projects)"
        )

    print(f"\n── Classification {'─' * max(1, W - 20)}")
    print("  ALL T-Substation projects classified as sub_tx (≤69kV).")
    print("  BAT input = Sub-TX + Distribution = Total")

    print(f"\n── Capacity entry by year {'─' * max(1, W - 27)}")
    header = (
        f"  {'Year':>4}  {'Sub-TX (MW)':>12}  {'Dist (MW)':>10}  {'Total (MW)':>10}"
    )
    print(header)
    for year in YEARS:
        tx_cap = cap_by_year["sub_tx"].get(year, 0.0)
        dist_cap = cap_by_year["distribution"].get(year, 0.0)
        print(
            f"  {year:>4}  {tx_cap:>12.1f}  {dist_cap:>10.1f}  {tx_cap + dist_cap:>10.1f}"
        )

    print(f"\n── Filing inputs (Exhibit 1) {'─' * max(1, W - 30)}")
    for cc, params in COMPONENT_PARAMS.items():
        mc = params.undiluted_mc
        cap = component_totals[cc]
        print(
            f"  {params.name:30s}  capital=${params.capital_per_kw:,.2f}/kW  "
            f"cap={cap:,.1f} MW  "
            f"ECCR+O&M={params.eccr_om_rate * 100:.1f}%  "
            f"MC=${mc:.2f}/kW-yr"
        )

    print(f"\n── Levelized MC ($/kW-yr, base-year real) {'─' * max(1, W - 43)}")
    for vname in VARIANT_NAMES:
        mc_data = variants[vname]
        print(f"\n  {vname}:")
        for key in BUCKET_KEYS:
            lev = levelized(mc_data[key])
            print(f"    {BUCKET_LABELS[key]:40s}  ${lev:7.2f}/kW-yr")

    inc_dil = variants["incremental_diluted"]
    print(
        f"\n── Year-by-year incremental diluted (nominal $/kW-yr) "
        f"{'─' * max(1, W - 52)}"
    )
    header = f"  {'Year':>4}"
    for k in BUCKET_KEYS:
        header += f" {k:>14}"
    print(header)
    for yi in range(N_YEARS):
        line = f"  {YEARS[yi]:>4}"
        for k in BUCKET_KEYS:
            line += f" ${inc_dil[k][yi].nominal_mc:>12.2f}"
        print(line)

    print(f"\n── BAT input (total diluted = Sub-TX + Dist) {'─' * max(1, W - 47)}")
    total_dil_lev = levelized(inc_dil["total"])
    subtx_dil_lev = levelized(inc_dil["sub_tx"])
    dist_dil_lev = levelized(inc_dil["distribution"])
    print(f"  Sub-TX diluted (levelized):   ${subtx_dil_lev:.2f}/kW-yr")
    print(f"  Dist diluted (levelized):     ${dist_dil_lev:.2f}/kW-yr")
    print(f"  Total diluted (BAT input):    ${total_dil_lev:.2f}/kW-yr")

    print("\n" + "=" * W)


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="PSEG-LI MCOS analysis")
    parser.add_argument(
        "--path-classifications",
        required=True,
        help="Path to classified project list CSV (output of classify script)",
    )
    parser.add_argument("--system-peak-mw", type=float, required=True)
    parser.add_argument("--path-output-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.path_output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    system_peak_mw = args.system_peak_mw

    print(
        f"PSEG-LI MCOS — reading classified projects from {args.path_classifications}"
    )
    df = read_classified_projects(args.path_classifications)
    print(f"  {len(df)} projects loaded")

    # Validate: all projects must have a classification
    missing = df.filter(pl.col("classification").is_null())
    if len(missing) > 0:
        msg = f"{len(missing)} projects missing classification"
        raise ValueError(msg)

    # Validate: no bulk_tx should exist
    bulk = df.filter(pl.col("classification") == "bulk_tx")
    if len(bulk) > 0:
        msg = f"Unexpected bulk_tx classifications: {bulk['sos_id'].to_list()}"
        raise ValueError(msg)

    cap_by_year = group_capacity_by_year(df)
    component_totals = get_component_totals(cap_by_year)

    n_projects_by_cc: dict[str, int] = {}
    for cc in COST_CENTERS:
        mapping = {"sub_tx": "sub_tx", "distribution": "distribution"}
        cls_values = [c for c, m in mapping.items() if m == cc]
        n_projects_by_cc[cc] = len(
            df.filter(pl.col("classification").is_in(cls_values))
        )

    print(
        f"  Sub-TX capacity:   {component_totals['sub_tx']:,.1f} MW ({n_projects_by_cc['sub_tx']} projects)"
    )
    print(
        f"  Dist capacity:     {component_totals['distribution']:,.1f} MW ({n_projects_by_cc['distribution']} projects)"
    )
    print(f"  System peak:       {system_peak_mw:,.0f} MW")

    assert abs(component_totals["sub_tx"] - 1027.1) < 0.2, (
        f"Sub-TX capacity mismatch: {component_totals['sub_tx']}"
    )
    assert abs(component_totals["distribution"] - 183.0) < 0.2, (
        f"Dist capacity mismatch: {component_totals['distribution']}"
    )

    variants: dict[str, dict[str, list[MCRow]]] = {}
    for vname in VARIANT_NAMES:
        cumulative = "cumulative" in vname
        diluted = "undiluted" not in vname

        mc_data: dict[str, list[MCRow]] = {}
        for cc in COST_CENTERS:
            mc_data[cc] = compute_component_mc(
                COMPONENT_PARAMS[cc],
                cap_by_year[cc],
                system_peak_mw,
                cumulative=cumulative,
                diluted=diluted,
            )
        mc_data["total"] = compute_total_mc(mc_data, system_peak_mw, diluted=diluted)
        variants[vname] = mc_data

    print("\nExporting CSVs:")
    for vname in VARIANT_NAMES:
        prefix = f"psegli_{vname}"
        export_levelized_csv(
            variants[vname],
            component_totals,
            n_projects_by_cc,
            out_dir / f"{prefix}_levelized.csv",
        )
        export_annualized_csv(variants[vname], out_dir / f"{prefix}_annualized.csv")

    print()
    print_report(
        variants, system_peak_mw, cap_by_year, component_totals, n_projects_by_cc
    )


if __name__ == "__main__":
    main()

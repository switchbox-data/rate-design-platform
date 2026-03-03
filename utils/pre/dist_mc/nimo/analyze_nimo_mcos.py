"""Compute marginal costs from NiMo's MCOS Exhibit 1 workbook.

Parses NiMo's MCOS workbook (Exhibit 1), applies project-level classifications
(bulk_tx / sub_tx / distribution from Gold Book cross-referencing), and computes
MC values per bucket in four variants:

  1. Cumulative diluted    — all in-service projects ÷ system peak
  2. Incremental diluted   — new projects in year Y ÷ system peak (BAT input)
  3. Cumulative undiluted  — all in-service projects ÷ in-service capacity
  4. Incremental undiluted — new projects in year Y ÷ new capacity in year Y

Each variant is exported in both annualized (year-by-year) and levelized form,
for a total of 8 CSVs.

NiMo's workbook differs from ConEd/O&R: instead of a composite rate × escalation
formula, each project has pre-computed ECCR values:
  - F26(p): base-year (FY2026) annual cost/MW ($000s/MW)
  - F_Y(p): nominal annual cost/MW in year Y ($000s/MW)

So MC(Y) = sum(F × capacity) / Denominator, where F = F26 for real, F_Y for
nominal.

Outputs (8 CSVs):
  - nimo_{cumulative,incremental}_{diluted,undiluted}_{levelized,annualized}.csv
  - Terminal report (always printed)

Usage (via Justfile):
    just analyze-nimo
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import fsspec
import polars as pl

# ── Column indices for NiMo Exhibit 1 (sheet_id=1) ───────────────────────────
# Row 4 = header names, Row 5 = column codes, data starts at row 7 (0-indexed)

COL_LINE = 0
COL_REF = 1
COL_STATION = 2
COL_CAP_MW = 3
COL_CSUM = 16  # Total capital spending ($000s)
COL_C_TS = 18  # Capital: T-Station ($000s)
COL_C_TL = 19  # Capital: T-Line ($000s)
COL_C_DS = 20  # Capital: D-Station ($000s)
COL_C_DL = 21  # Capital: D-Line ($000s)
COL_E_TS = 30  # Annual ECCR cost/MW: T-Station ($000s/MW)
COL_E_TL = 31  # Annual ECCR cost/MW: T-Line ($000s/MW)
COL_E_DS = 32  # Annual ECCR cost/MW: D-Station ($000s/MW)
COL_E_DL = 33  # Annual ECCR cost/MW: D-Line ($000s/MW)
COL_E = 34  # Annual ECCR cost/MW: Total ($000s/MW)
COL_IN_SVC = 29  # In-service year
COL_F26 = 39  # Marginal cost in FY2026 ($000s/MW)
DATA_START_ROW = 7

# Year-by-year F columns: FY2026 (col 39) through FY2036 (col 49)
FISCAL_YEARS = list(range(2026, 2037))  # [2026, 2027, ..., 2036]
N_YEARS = len(FISCAL_YEARS)
F_COL_BY_YEAR: dict[int, int] = {yr: COL_F26 + (yr - 2026) for yr in FISCAL_YEARS}

VALID_CLASSIFICATIONS = {"bulk_tx", "sub_tx", "distribution"}

BUCKET_KEYS = ["total", "bulk_tx", "sub_tx", "distribution", "sub_tx_plus_dist"]
BUCKET_LABELS = {
    "total": "All projects",
    "bulk_tx": "Bulk TX (≥230kV)",
    "sub_tx": "Sub-TX (69–115kV)",
    "distribution": "Distribution (≤13.2kV)",
    "sub_tx_plus_dist": "Sub-TX + Distribution",
}

VARIANT_NAMES = [
    "cumulative_diluted",
    "incremental_diluted",
    "cumulative_undiluted",
    "incremental_undiluted",
]


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class McosProject:
    """One row from the MCOS Exhibit 1 workbook."""

    line: int
    reference: str
    station: str
    capacity_mw: float
    capital_k: float  # Total capital ($000s)
    c_ts_k: float  # T-Station capital ($000s)
    c_tl_k: float  # T-Line capital ($000s)
    c_ds_k: float  # D-Station capital ($000s)
    c_dl_k: float  # D-Line capital ($000s)
    e_ts: float  # Annual ECCR cost/MW: T-Station ($000s/MW)
    e_tl: float  # Annual ECCR cost/MW: T-Line ($000s/MW)
    e_ds: float  # Annual ECCR cost/MW: D-Station ($000s/MW)
    e_dl: float  # Annual ECCR cost/MW: D-Line ($000s/MW)
    e_total: float  # Annual ECCR cost/MW: Total ($000s/MW)
    f26: float  # Marginal cost in FY2026 ($000s/MW)
    f_by_year: dict[int, float]  # FY → $000s/MW for FY2026–FY2036
    in_service_year: int | None
    classification: str = ""  # bulk_tx | sub_tx | distribution


@dataclass
class MCRow:
    """One year's MC for a bucket (used for all variants)."""

    fy: int
    cost_real_k: float  # numerator, base-year ($000s)
    cost_nominal_k: float  # numerator, year-Y ($000s)
    denominator_mw: float
    nominal_mc: float  # $/kW-yr, year-Y dollars
    real_mc: float  # $/kW-yr, FY2026 dollars


# ── Parse MCOS workbook ──────────────────────────────────────────────────────


def _float_or_zero(val: object) -> float:
    if val is None:
        return 0.0
    try:
        return float(str(val))
    except (ValueError, TypeError):
        return 0.0


def _int_or_none(val: object) -> int | None:
    if val is None:
        return None
    try:
        return int(float(str(val)))
    except (ValueError, TypeError):
        return None


def _read_excel_from_path_or_s3(source: str) -> pl.DataFrame:
    """Read an Excel file from a local path or S3 URL."""
    if source.startswith("s3://"):
        with fsspec.open(source, "rb") as f:
            return pl.read_excel(
                f.read(), sheet_id=1, raise_if_empty=False, infer_schema_length=0
            )
    return pl.read_excel(
        source, sheet_id=1, raise_if_empty=False, infer_schema_length=0
    )


def parse_mcos_workbook(xlsx_path: str) -> list[McosProject]:
    """Parse Exhibit 1 from the NiMo MCOS workbook."""
    df = _read_excel_from_path_or_s3(xlsx_path)

    projects: list[McosProject] = []
    for row_idx in range(DATA_START_ROW, df.height):
        row = df.row(row_idx)
        line_val = row[COL_LINE]
        station_val = row[COL_STATION]

        if line_val is None or station_val is None:
            continue

        try:
            line_num = int(float(line_val))
        except (ValueError, TypeError):
            continue

        station_str = str(station_val).strip()
        if station_str.lower() in ("xxx", ""):
            continue

        f_by_year = {yr: _float_or_zero(row[col]) for yr, col in F_COL_BY_YEAR.items()}

        projects.append(
            McosProject(
                line=line_num,
                reference=str(row[COL_REF] or ""),
                station=station_str,
                capacity_mw=_float_or_zero(row[COL_CAP_MW]),
                capital_k=_float_or_zero(row[COL_CSUM]),
                c_ts_k=_float_or_zero(row[COL_C_TS]),
                c_tl_k=_float_or_zero(row[COL_C_TL]),
                c_ds_k=_float_or_zero(row[COL_C_DS]),
                c_dl_k=_float_or_zero(row[COL_C_DL]),
                e_ts=_float_or_zero(row[COL_E_TS]),
                e_tl=_float_or_zero(row[COL_E_TL]),
                e_ds=_float_or_zero(row[COL_E_DS]),
                e_dl=_float_or_zero(row[COL_E_DL]),
                e_total=_float_or_zero(row[COL_E]),
                f26=_float_or_zero(row[COL_F26]),
                f_by_year=f_by_year,
                in_service_year=_int_or_none(row[COL_IN_SVC]),
            )
        )
    return projects


def load_classifications(csv_path: Path) -> dict[str, str]:
    """Load fn_reference → classification mapping from the classifications CSV."""
    df = pl.read_csv(
        csv_path,
        schema_overrides={"fn_reference": pl.Utf8, "classification": pl.Utf8},
    )
    result: dict[str, str] = {}
    for row in df.iter_rows(named=True):
        fn = row["fn_reference"]
        cls = row["classification"]
        if cls not in VALID_CLASSIFICATIONS:
            msg = f"Invalid classification '{cls}' for {fn} in {csv_path}"
            raise ValueError(msg)
        result[fn] = cls
    return result


def apply_classifications(
    projects: list[McosProject], classifications: dict[str, str]
) -> None:
    """Assign classification to each project from the CSV lookup."""
    for p in projects:
        cls = classifications.get(p.reference)
        if cls is None:
            msg = f"No classification for {p.reference} ({p.station}, line {p.line})"
            raise ValueError(msg)
        p.classification = cls


# ── MC computation ───────────────────────────────────────────────────────────


def _projects_by_bucket(
    projects: list[McosProject],
) -> dict[str, list[McosProject]]:
    by_cls: dict[str, list[McosProject]] = {c: [] for c in VALID_CLASSIFICATIONS}
    for p in projects:
        by_cls[p.classification].append(p)
    return {
        "total": list(projects),
        "bulk_tx": by_cls["bulk_tx"],
        "sub_tx": by_cls["sub_tx"],
        "distribution": by_cls["distribution"],
        "sub_tx_plus_dist": by_cls["sub_tx"] + by_cls["distribution"],
    }


def compute_bucket_mc(
    projects: list[McosProject],
    system_peak_mw: float,
    *,
    cumulative: bool,
    diluted: bool,
) -> list[MCRow]:
    """Year-by-year MC for a set of projects under one variant.

    cumulative=True:  in-scope = projects with in_service_year <= fy
    cumulative=False: in-scope = projects with in_service_year == fy
    diluted=True:     denominator = system_peak_mw
    diluted=False:    denominator = sum(capacity) of in-scope projects
    """
    rows: list[MCRow] = []
    for fy in FISCAL_YEARS:
        if cumulative:
            scope = [
                p
                for p in projects
                if p.in_service_year is not None and p.in_service_year <= fy
            ]
        else:
            scope = [p for p in projects if p.in_service_year == fy]

        cost_real_k = sum(p.f26 * p.capacity_mw for p in scope)
        cost_nominal_k = sum(p.f_by_year.get(fy, 0.0) * p.capacity_mw for p in scope)

        if diluted:
            denom = system_peak_mw
        else:
            denom = sum(p.capacity_mw for p in scope)

        real_mc = cost_real_k / denom if denom > 0 else 0.0
        nominal_mc = cost_nominal_k / denom if denom > 0 else 0.0

        rows.append(
            MCRow(
                fy=fy,
                cost_real_k=cost_real_k,
                cost_nominal_k=cost_nominal_k,
                denominator_mw=denom,
                nominal_mc=nominal_mc,
                real_mc=real_mc,
            )
        )
    return rows


def levelized(rows: list[MCRow]) -> float:
    """Average of real (base-year) MC across all study years."""
    return sum(r.real_mc for r in rows) / len(rows) if rows else 0.0


# ── CSV export (harmonized 2-bucket schema) ──────────────────────────────────

EXPORT_BUCKETS: list[tuple[str, str, str]] = [
    ("bulk_tx", "bulk_tx", "Bulk TX"),
    ("sub_tx_and_dist", "sub_tx_plus_dist", "Sub-TX + Distribution"),
]
"""(export_key, internal_key, label) for the harmonized output schema."""


def export_levelized_csv(
    mc_data: dict[str, list[MCRow]],
    bucket_info: dict[str, dict],
    path: Path,
) -> None:
    rows = []
    for export_key, internal_key, label in EXPORT_BUCKETS:
        mc_rows = mc_data[internal_key]
        lev = levelized(mc_rows)
        final_real = mc_rows[-1].real_mc
        final_nom = mc_rows[-1].nominal_mc
        rows.append(
            {
                "bucket": export_key,
                "label": label,
                "levelized_mc_kw_yr": round(lev, 2),
                "final_year_real_mc_kw_yr": round(final_real, 2),
                "final_year_nominal_mc_kw_yr": round(final_nom, 2),
            }
        )
    pl.DataFrame(rows).write_csv(path)
    print(f"  Wrote {path}")


def export_annualized_csv(mc_data: dict[str, list[MCRow]], path: Path) -> None:
    rows = []
    for yi, fy in enumerate(FISCAL_YEARS):
        row: dict[str, object] = {"year": fy}
        for export_key, internal_key, _label in EXPORT_BUCKETS:
            row[f"{export_key}_nominal"] = round(
                mc_data[internal_key][yi].nominal_mc, 2
            )
            row[f"{export_key}_real"] = round(mc_data[internal_key][yi].real_mc, 2)
        rows.append(row)
    pl.DataFrame(rows).write_csv(path)
    print(f"  Wrote {path}")


# ── Terminal report ──────────────────────────────────────────────────────────


def print_report(
    bucket_info: dict[str, dict],
    variants: dict[str, dict[str, list[MCRow]]],
    system_peak_mw: float,
    undiluted_mc_per_kw: float,
) -> None:
    W = 80
    print("=" * W)
    print("NiMo (National Grid) MCOS Analysis — Cumulative vs. Incremental")
    print("=" * W)

    print(f"\n── System {'─' * (W - 11)}")
    print(f"  System peak (2024 actual):     {system_peak_mw:,.0f} MW")
    print(f"  Undiluted MC (MCOS headline):  ${undiluted_mc_per_kw:.2f}/kW-yr")

    cum_dil = variants["cumulative_diluted"]
    inc_dil = variants["incremental_diluted"]
    cum_undil = variants["cumulative_undiluted"]
    inc_undil = variants["incremental_undiluted"]

    print(f"\n── Levelized MC ($/kW-yr, real FY2026) {'─' * (W - 40)}")
    print(
        f"  {'Bucket':<28} {'Cum.Dil':>8} {'Inc.Dil':>8} {'Cum.Und':>8} {'Inc.Und':>8}"
    )
    print(f"  {'─' * (W - 4)}")
    for key in BUCKET_KEYS:
        label = BUCKET_LABELS[key]
        vals = [
            levelized(cum_dil[key]),
            levelized(inc_dil[key]),
            levelized(cum_undil[key]),
            levelized(inc_undil[key]),
        ]
        parts = "  ".join(f"${v:>6.2f}" for v in vals)
        print(f"  {label:<28} {parts}")

    print(
        f"\n── Year-by-year sub_tx_plus_dist diluted ($/kW-yr, nominal) "
        f"{'─' * max(1, W - 61)}"
    )
    print(f"  {'FY':>4}  {'Cumulative':>12}  {'Incremental':>12}  {'Match?':>8}")
    print(f"  {'─' * 42}")
    for yi, fy in enumerate(FISCAL_YEARS):
        cum_v = cum_dil["sub_tx_plus_dist"][yi].nominal_mc
        inc_v = inc_dil["sub_tx_plus_dist"][yi].nominal_mc
        match = "  ✓" if fy == FISCAL_YEARS[0] and abs(cum_v - inc_v) < 0.01 else ""
        print(f"  {fy:>4}  ${cum_v:>10.2f}  ${inc_v:>10.2f}  {match}")

    total_cap = bucket_info["total"]["capacity_mw"]
    ratio = total_cap / system_peak_mw if system_peak_mw else 0
    print(
        f"\n  Capacity added / system peak:  {ratio:.2f}x "
        f"{'(> 1.0)' if ratio > 1 else ''}"
    )
    print()


# ── CLI ──────────────────────────────────────────────────────────────────────


def _bucket_info(
    buckets: dict[str, list[McosProject]],
) -> dict[str, dict]:
    """Summary statistics per bucket (for CSV metadata columns)."""
    info: dict[str, dict] = {}
    for key, projects in buckets.items():
        info[key] = {
            "n_projects": len(projects),
            "n_unique_stations": len({p.station.upper() for p in projects}),
            "capacity_mw": sum(p.capacity_mw for p in projects),
            "capital_b": sum(p.capital_k for p in projects) / 1e6,
        }
    return info


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute MC variants from NiMo MCOS workbook"
    )
    parser.add_argument(
        "--path-xlsx",
        type=str,
        required=True,
        help="Path or S3 URL to NiMo MCOS Exhibit 1 workbook (.xlsx)",
    )
    parser.add_argument(
        "--path-classifications",
        type=Path,
        required=True,
        help="Path to project classifications CSV",
    )
    parser.add_argument(
        "--system-peak-mw",
        type=float,
        required=True,
        help="System peak demand in MW (e.g. 6616)",
    )
    parser.add_argument(
        "--undiluted-mc-per-kw",
        type=float,
        required=True,
        help="Undiluted MC headline from MCOS ($/kW-yr, e.g. 71.524)",
    )
    parser.add_argument(
        "--path-output-dir",
        type=Path,
        required=True,
        help="Directory for output CSVs",
    )
    args = parser.parse_args()

    path_xlsx: str = args.path_xlsx
    path_classifications: Path = args.path_classifications
    path_output_dir: Path = args.path_output_dir
    system_peak_mw: float = args.system_peak_mw
    undiluted_mc: float = args.undiluted_mc_per_kw

    # 1. Parse MCOS workbook
    print(f"Parsing: {path_xlsx}")
    all_projects = parse_mcos_workbook(path_xlsx)
    print(f"  Projects parsed: {len(all_projects)}")

    # 2. Load and apply classifications
    print(f"Classifications: {path_classifications.name}")
    classifications = load_classifications(path_classifications)
    apply_classifications(all_projects, classifications)
    for cls in sorted(VALID_CLASSIFICATIONS):
        n = sum(1 for p in all_projects if p.classification == cls)
        print(f"  {cls}: {n}")

    # 3. Group projects into buckets
    buckets = _projects_by_bucket(all_projects)
    info = _bucket_info(buckets)

    # 4. Compute all 4 variants
    variants: dict[str, dict[str, list[MCRow]]] = {}
    for variant_name in VARIANT_NAMES:
        cumulative = "cumulative" in variant_name
        diluted = "undiluted" not in variant_name
        variants[variant_name] = {
            key: compute_bucket_mc(
                buckets[key], system_peak_mw, cumulative=cumulative, diluted=diluted
            )
            for key in BUCKET_KEYS
        }

    # 5. Export CSVs
    path_output_dir.mkdir(parents=True, exist_ok=True)
    print("\nExporting CSVs:")
    for variant_name in VARIANT_NAMES:
        prefix = f"nimo_{variant_name}"
        export_levelized_csv(
            variants[variant_name], info, path_output_dir / f"{prefix}_levelized.csv"
        )
        export_annualized_csv(
            variants[variant_name], path_output_dir / f"{prefix}_annualized.csv"
        )

    # 6. Terminal report
    print()
    print_report(info, variants, system_peak_mw, undiluted_mc)


if __name__ == "__main__":
    main()

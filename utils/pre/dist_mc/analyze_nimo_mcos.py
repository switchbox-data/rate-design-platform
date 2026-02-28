"""Compute diluted marginal costs from NiMo's MCOS Exhibit 1 workbook.

Parses NiMo's MCOS workbook (Exhibit 1), applies project-level classifications
(bulk_tx / sub_tx / distribution from Gold Book cross-referencing), and computes
diluted MC values per bucket — both levelized (present-value-equivalent at FY2026
prices) and year-by-year (cumulative annual bill as projects enter service).

Outputs:
  - Levelized summary CSV: one row per bucket with total costs and diluted $/kW-yr
  - Annualized CSV: one row per (bucket, fiscal year) with year-by-year diluted MC
  - Terminal report (always printed)

Usage (via Justfile):
    just analyze
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
F_COL_BY_YEAR: dict[int, int] = {yr: COL_F26 + (yr - 2026) for yr in FISCAL_YEARS}

VALID_CLASSIFICATIONS = {"bulk_tx", "sub_tx", "distribution"}

BUCKET_KEYS = ["total", "bulk_tx", "sub_tx", "distribution", "sub_tx_plus_dist"]


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

    @property
    def annual_cost_nominal_k(self) -> float:
        """Nominal annual MC = E * capacity ($000s/yr). At in-service-year prices."""
        return self.e_total * self.capacity_mw

    @property
    def annual_cost_discounted_k(self) -> float:
        """Discounted annual MC = F26 * capacity ($000s/yr). At FY2026 prices."""
        return self.f26 * self.capacity_mw

    def annual_cost_in_year_k(self, fy: int) -> float:
        """Cost this project contributes in fiscal year fy ($000s).

        Zero if the project isn't yet in service.
        """
        if self.in_service_year is not None and fy < self.in_service_year:
            return 0.0
        return self.f_by_year.get(fy, 0.0) * self.capacity_mw


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


# ── Dilution ──────────────────────────────────────────────────────────────────


@dataclass
class BucketSummary:
    """Cost summary for one classification bucket."""

    key: str
    label: str
    n_projects: int
    n_unique_stations: int
    capacity_mw: float
    capital_b: float
    annual_cost_nominal_m: float  # sum(E × cap), in-service-year prices
    annual_cost_discounted_m: float  # sum(F26 × cap), FY2026 prices
    diluted_per_kw: float  # discounted annual / system peak


@dataclass
class DilutionResult:
    system_peak_mw: float
    undiluted_mc_per_kw: float

    total: BucketSummary
    bulk_tx: BucketSummary
    sub_tx: BucketSummary
    distribution: BucketSummary
    sub_tx_plus_dist: BucketSummary  # What DRV should cover

    def buckets(self) -> list[BucketSummary]:
        return [
            self.total,
            self.bulk_tx,
            self.sub_tx,
            self.distribution,
            self.sub_tx_plus_dist,
        ]


def _bucket(
    key: str, label: str, projects: list[McosProject], system_peak_mw: float
) -> BucketSummary:
    nom_k = sum(p.annual_cost_nominal_k for p in projects)
    disc_k = sum(p.annual_cost_discounted_k for p in projects)
    unique_stations = len({p.station.upper() for p in projects})
    return BucketSummary(
        key=key,
        label=label,
        n_projects=len(projects),
        n_unique_stations=unique_stations,
        capacity_mw=sum(p.capacity_mw for p in projects),
        capital_b=sum(p.capital_k for p in projects) / 1e6,
        annual_cost_nominal_m=nom_k / 1e3,
        annual_cost_discounted_m=disc_k / 1e3,
        diluted_per_kw=disc_k / system_peak_mw if system_peak_mw else 0,
    )


def compute_dilution(
    projects: list[McosProject],
    *,
    system_peak_mw: float,
    undiluted_mc_per_kw: float,
) -> DilutionResult:
    by_cls: dict[str, list[McosProject]] = {c: [] for c in VALID_CLASSIFICATIONS}
    for p in projects:
        by_cls[p.classification].append(p)

    return DilutionResult(
        system_peak_mw=system_peak_mw,
        undiluted_mc_per_kw=undiluted_mc_per_kw,
        total=_bucket("total", "All projects", projects, system_peak_mw),
        bulk_tx=_bucket(
            "bulk_tx", "Bulk TX (≥230kV)", by_cls["bulk_tx"], system_peak_mw
        ),
        sub_tx=_bucket("sub_tx", "Sub-TX (69–115kV)", by_cls["sub_tx"], system_peak_mw),
        distribution=_bucket(
            "distribution",
            "Distribution (≤13.2kV)",
            by_cls["distribution"],
            system_peak_mw,
        ),
        sub_tx_plus_dist=_bucket(
            "sub_tx_plus_dist",
            "Sub-TX + Distribution (DRV-relevant)",
            by_cls["sub_tx"] + by_cls["distribution"],
            system_peak_mw,
        ),
    )


# ── Annualized (year-by-year) dilution ────────────────────────────────────────


@dataclass
class AnnualizedRow:
    """One fiscal year's worth of costs for a bucket of projects."""

    fy: int
    new_capacity_mw: float  # capacity entering service this year
    cumulative_capacity_mw: float  # total in-service capacity through this year
    annual_cost_m: float  # cumulative annual bill ($M, year-Y dollars)
    diluted_per_kw: float  # annual bill / system peak ($/kW-yr)


@dataclass
class AnnualizedTable:
    """Year-by-year diluted marginal costs for one classification bucket."""

    key: str
    label: str
    system_peak_mw: float
    rows: list[AnnualizedRow]


def compute_annualized_table(
    key: str,
    label: str,
    projects: list[McosProject],
    system_peak_mw: float,
) -> AnnualizedTable:
    """Build a year-by-year table of cumulative diluted MC for a set of projects.

    For each fiscal year Y, the annual bill is the sum of (F_Y × capacity) for
    all projects in service by year Y. The diluted MC divides that bill by
    system peak.
    """
    rows: list[AnnualizedRow] = []
    for fy in FISCAL_YEARS:
        new_cap = sum(p.capacity_mw for p in projects if p.in_service_year == fy)
        cum_cap = sum(
            p.capacity_mw
            for p in projects
            if p.in_service_year is not None and p.in_service_year <= fy
        )
        annual_cost_k = sum(p.annual_cost_in_year_k(fy) for p in projects)
        diluted = annual_cost_k / system_peak_mw if system_peak_mw else 0.0
        rows.append(
            AnnualizedRow(
                fy=fy,
                new_capacity_mw=new_cap,
                cumulative_capacity_mw=cum_cap,
                annual_cost_m=annual_cost_k / 1e3,
                diluted_per_kw=diluted,
            )
        )
    return AnnualizedTable(
        key=key, label=label, system_peak_mw=system_peak_mw, rows=rows
    )


def compute_all_annualized(
    projects: list[McosProject], system_peak_mw: float
) -> dict[str, AnnualizedTable]:
    by_cls: dict[str, list[McosProject]] = {c: [] for c in VALID_CLASSIFICATIONS}
    for p in projects:
        by_cls[p.classification].append(p)

    return {
        "total": compute_annualized_table(
            "total", "All projects", projects, system_peak_mw
        ),
        "bulk_tx": compute_annualized_table(
            "bulk_tx", "Bulk TX (≥230kV)", by_cls["bulk_tx"], system_peak_mw
        ),
        "sub_tx": compute_annualized_table(
            "sub_tx", "Sub-TX (69–115kV)", by_cls["sub_tx"], system_peak_mw
        ),
        "distribution": compute_annualized_table(
            "distribution",
            "Distribution (≤13.2kV)",
            by_cls["distribution"],
            system_peak_mw,
        ),
        "sub_tx_plus_dist": compute_annualized_table(
            "sub_tx_plus_dist",
            "Sub-TX + Distribution (DRV-relevant)",
            by_cls["sub_tx"] + by_cls["distribution"],
            system_peak_mw,
        ),
    }


# ── CSV export ────────────────────────────────────────────────────────────────


def export_levelized_csv(result: DilutionResult, path: Path) -> None:
    rows = []
    for b in result.buckets():
        rows.append(
            {
                "bucket": b.key,
                "label": b.label,
                "n_projects": b.n_projects,
                "n_unique_stations": b.n_unique_stations,
                "capacity_mw": round(b.capacity_mw, 1),
                "capital_b": round(b.capital_b, 2),
                "annual_cost_nominal_m": round(b.annual_cost_nominal_m, 1),
                "annual_cost_discounted_m": round(b.annual_cost_discounted_m, 1),
                "diluted_per_kw_yr": round(b.diluted_per_kw, 2),
            }
        )
    df = pl.DataFrame(rows)
    df.write_csv(path)
    print(f"  Wrote {path}")


def export_annualized_csv(tables: dict[str, AnnualizedTable], path: Path) -> None:
    rows = []
    for key in BUCKET_KEYS:
        t = tables[key]
        for r in t.rows:
            rows.append(
                {
                    "bucket": key,
                    "label": t.label,
                    "fy": r.fy,
                    "new_capacity_mw": round(r.new_capacity_mw, 1),
                    "cumulative_capacity_mw": round(r.cumulative_capacity_mw, 1),
                    "annual_cost_m": round(r.annual_cost_m, 1),
                    "diluted_per_kw_yr": round(r.diluted_per_kw, 2),
                }
            )
    df = pl.DataFrame(rows)
    df.write_csv(path)
    print(f"  Wrote {path}")


# ── Terminal report ───────────────────────────────────────────────────────────


def _print_annualized_table(table: AnnualizedTable) -> None:
    hdr = (
        f"  {'FY':>6}  {'New MW':>8}  {'Cum MW':>8}  {'Bill ($M)':>10}  {'Diluted':>12}"
    )
    print(f"  {table.label}")
    print(f"  System peak: {table.system_peak_mw:,.0f} MW")
    print(hdr)
    print(f"  {'─' * (len(hdr) - 2)}")
    for r in table.rows:
        print(
            f"  {r.fy:>6}  {r.new_capacity_mw:>8,.1f}  "
            f"{r.cumulative_capacity_mw:>8,.1f}  "
            f"${r.annual_cost_m:>9,.1f}  "
            f"${r.diluted_per_kw:>8,.2f}/kW-yr"
        )


def _print_bucket(b: BucketSummary, indent: str = "  ") -> None:
    print(f"{indent}{b.label}")
    print(
        f"{indent}  {b.n_projects} projects across "
        f"{b.n_unique_stations} stations, "
        f"{b.capacity_mw:,.1f} MW, "
        f"${b.capital_b:,.2f}B capital"
    )
    print(f"{indent}  Nominal:    ${b.annual_cost_nominal_m:,.1f}M/yr  (sum of E×cap)")
    print(
        f"{indent}  Discounted: ${b.annual_cost_discounted_m:,.1f}M/yr  "
        f"(sum of F26×cap, FY2026)"
    )
    print(f"{indent}  Diluted:    ${b.diluted_per_kw:,.2f}/kW-yr")


def print_report(
    result: DilutionResult,
    annualized: dict[str, AnnualizedTable] | None = None,
) -> None:
    W = 78
    print("=" * W)
    print("NiMo (National Grid) MCOS Dilution Analysis")
    print("=" * W)

    print(f"\n── System {'─' * (W - 11)}")
    print(f"  System peak (2024 actual):     {result.system_peak_mw:,.0f} MW")
    print(f"  Undiluted MC (MCOS headline):  ${result.undiluted_mc_per_kw:.2f}/kW-yr")

    print(f"\n── Three-bucket breakdown {'─' * (W - 26)}")
    _print_bucket(result.total)
    print()
    _print_bucket(result.bulk_tx)
    print()
    _print_bucket(result.sub_tx)
    print()
    _print_bucket(result.distribution)

    ratio = result.total.capacity_mw / result.system_peak_mw
    print(
        f"\n  Capacity added / system peak:  {ratio:.2f}x "
        f"{'(⚠ > 1.0)' if ratio > 1 else ''}"
    )

    print(f"\n── DRV-relevant composite {'─' * (W - 26)}")
    _print_bucket(result.sub_tx_plus_dist)

    print(f"\n── Dilution summary {'─' * (W - 20)}")
    print(f"  All projects:           ${result.total.diluted_per_kw:,.2f}/kW-yr")
    print(f"  Bulk TX only:           ${result.bulk_tx.diluted_per_kw:,.2f}/kW-yr")
    print(f"  Sub-TX only:            ${result.sub_tx.diluted_per_kw:,.2f}/kW-yr")
    print(f"  Distribution only:      ${result.distribution.diluted_per_kw:,.2f}/kW-yr")
    d = result.sub_tx_plus_dist.diluted_per_kw
    print(f"  Sub-TX + Distribution:  ${d:,.2f}/kW-yr  ← DRV-relevant")
    if result.total.diluted_per_kw > result.undiluted_mc_per_kw:
        print(
            f"\n  ⚠  Diluted total > undiluted because project capacity "
            f"({result.total.capacity_mw:,.0f} MW)"
        )
        print(
            f"     exceeds system peak ({result.system_peak_mw:,.0f} MW) — "
            f"driven by bulk TX"
        )

    print(f"\n── Cross-utility comparison (diluted) {'─' * (W - 38)}")
    print("  CenHud:                 $12/kW-yr  (levelized total, no bulk TX)")
    print("  NYSEG:                  $23/kW-yr  (levelized total, no bulk TX)")
    print("  PSEG-LI:               $34/kW-yr  (estimated total, includes TX)")
    print("  RG&E:                   $39/kW-yr  (levelized total, no bulk TX)")
    print(f"  NiMo sub-TX + dist:     ${d:.0f}/kW-yr  (DRV-relevant, excl bulk TX)")
    print(
        f"  NiMo all projects:      ${result.total.diluted_per_kw:.0f}/kW-yr  (incl bulk TX)"
    )

    if annualized:
        print(f"\n── Year-by-year diluted MC {'─' * (W - 27)}")
        print("  As projects enter service, the annual infrastructure bill grows.")
        print("  Each year's bill ÷ system peak = that year's diluted MC.\n")
        for key in BUCKET_KEYS:
            if key in annualized:
                _print_annualized_table(annualized[key])
                print()
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute diluted MC from NiMo MCOS workbook"
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

    # 3. Compute levelized dilution
    result = compute_dilution(
        all_projects,
        system_peak_mw=system_peak_mw,
        undiluted_mc_per_kw=undiluted_mc,
    )

    # 4. Compute year-by-year annualized tables
    annualized = compute_all_annualized(all_projects, system_peak_mw)

    # 5. Export CSVs
    path_output_dir.mkdir(parents=True, exist_ok=True)
    print("\nExporting CSVs:")
    export_levelized_csv(result, path_output_dir / "nimo_diluted_levelized.csv")
    export_annualized_csv(annualized, path_output_dir / "nimo_diluted_annualized.csv")

    # 6. Terminal report
    print()
    print_report(result, annualized=annualized)


if __name__ == "__main__":
    main()

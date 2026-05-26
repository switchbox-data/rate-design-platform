"""Inspect CT GIS boundary data produced by fetch_ct_gis_boundaries.py.

Prints human-readable summaries of three datasets:

  1. CT electric utility territories — name, type, customers, city, …
  2. CT 2020 PUMA districts — code, full name (includes county reference)
  3. Utility × PUMA coverage — intersection area fractions from the overlay parquet

All input files are auto-discovered by finding the most-recently-modified
date-stamped file that matches each expected glob in the staging directories.

Usage::

    uv run python data/resstock/utility/assign_utility_ct.py

    # Override the default staging-directory roots
    uv run python data/resstock/utility/assign_utility_ct.py \\
        --path-local-csv        data/resstock/utility/csv \\
        --path-local-shapefiles data/resstock/utility/shapefiles \\
        --path-local-parquet    data/resstock/utility/parquet

    # Skip sections that are not available yet (e.g. if you ran --no-overlay)
    uv run python data/resstock/utility/assign_utility_ct.py --no-overlay
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import polars as pl
import shapely.wkt


# ── File discovery ──────────────────────────────────────────────────────────


def _latest(directory: Path, glob: str) -> Path | None:
    """Return the most-recently modified file matching *glob* inside *directory*."""
    candidates = sorted(
        directory.glob(glob), key=lambda p: p.stat().st_mtime, reverse=True
    )
    return candidates[0] if candidates else None


# ── Section 1: CT electric utilities ───────────────────────────────────────

# Columns present in HIFLD Electric Retail Service Territories shapefile that
# are interesting for a quick inspection.  The script falls back gracefully if
# any column is absent.
_UTILITY_DISPLAY_COLS = [
    "comp_full",  # renamed from HIFLD NAME
    "ID",
    "TYPE",
    "CITY",
    "ZIP",
    "CNTRL_AREA",  # NERC control area
    "HOLDING_CO",  # parent holding company
    "CUSTOMERS",  # number of retail customers
    "NAICSDESC",  # NAICS sector description
    "REGULATED",
    "SOURCE",
    "YEAR",
]


def _show_utilities(csv_path: Path) -> None:
    df = pl.read_csv(str(csv_path))

    present = [c for c in _UTILITY_DISPLAY_COLS if c in df.columns]
    display = df.select(present).sort("comp_full") if present else df.drop("the_geom")

    print(f"\n{'=' * 80}")
    print(f"CT ELECTRIC UTILITY TERRITORIES  ({len(df)} utilities)")
    print(f"Source: {csv_path.name}")
    print(f"{'=' * 80}")
    print(display.to_pandas().to_string(index=False))

    other_cols = [c for c in df.columns if c not in present and c != "the_geom"]
    if other_cols:
        print(f"\nOther columns (not shown): {', '.join(other_cols)}")


# ── Section 2: CT PUMA districts ────────────────────────────────────────────

# TIGER 2020 PUMA column names
_PUMA_CODE_COL = "PUMACE20"
_PUMA_NAME_COL = "NAMELSAD20"
_PUMA_GEOID_COL = "GEOID20"


def _show_pumas(shp_path: Path) -> None:
    gdf = gpd.read_file(str(shp_path))

    key = [
        c for c in [_PUMA_CODE_COL, _PUMA_GEOID_COL, _PUMA_NAME_COL] if c in gdf.columns
    ]

    print(f"\n{'=' * 80}")
    print(f"CT 2020 PUMA DISTRICTS  ({len(gdf)} PUMAs)")
    print(f"Source: {shp_path.name}")
    print(f"{'=' * 80}")

    if key:
        print(gdf[key].sort_values(_PUMA_CODE_COL).to_string(index=False))
        other_cols = [c for c in gdf.columns if c not in key and c != "geometry"]
        if other_cols:
            print(f"\nOther columns (not shown): {', '.join(other_cols)}")
    else:
        # Fallback: expected 2020-vintage column names absent — show everything and warn.
        all_data_cols = [c for c in gdf.columns if c != "geometry"]
        print(gdf[all_data_cols].to_string(index=False))
        print(
            f"\nWARNING: expected columns {[_PUMA_CODE_COL, _PUMA_GEOID_COL, _PUMA_NAME_COL]} "
            "not found — check that fetch used year=2024 (2020-definition PUMAs)."
        )


# ── Section 3: Utility × PUMA overlay ──────────────────────────────────────


def _compute_area_sqft(wkt_series: pl.Series) -> pl.Series:
    """Return a Float64 Series of shapely polygon areas (in CRS native units)."""
    return pl.Series(
        [shapely.wkt.loads(g).area if g is not None else 0.0 for g in wkt_series],
        dtype=pl.Float64,
    )


def _show_overlay(parquet_path: Path) -> None:
    df = pl.read_parquet(str(parquet_path))

    util_col = next((c for c in ["comp_full", "NAME"] if c in df.columns), None)
    puma_code = _PUMA_CODE_COL if _PUMA_CODE_COL in df.columns else None
    puma_name = _PUMA_NAME_COL if _PUMA_NAME_COL in df.columns else None
    geom_col = "geometry_wkt" if "geometry_wkt" in df.columns else None

    print(f"\n{'=' * 80}")
    print(f"UTILITY × PUMA SPATIAL OVERLAY  ({len(df)} intersection polygons)")
    print(f"Source: {parquet_path.name}")
    print("Geometry CRS: EPSG:2234 (CT state plane, feet)")
    print(f"{'=' * 80}")

    # ── Compute area fractions ────────────────────────────────────────────
    if geom_col and puma_code:
        df = df.with_columns(area_sqft=_compute_area_sqft(df[geom_col]))
        puma_totals = df.group_by(puma_code).agg(
            pl.col("area_sqft").sum().alias("puma_area_sqft")
        )
        df = df.join(puma_totals, on=puma_code).with_columns(
            pct_of_puma=(pl.col("area_sqft") / pl.col("puma_area_sqft") * 100).round(1)
        )

    # ── Tabular view ──────────────────────────────────────────────────────
    show_cols = [c for c in [util_col, puma_code, puma_name, "pct_of_puma"] if c]
    if show_cols:
        table = df.select(show_cols)
        if puma_code:
            table = table.sort([puma_code, util_col] if util_col else [puma_code])
        print("\nDetailed intersection rows:")
        print(table.to_pandas().to_string(index=False))

    # ── Summary: PUMA → utilities ─────────────────────────────────────────
    if util_col and (puma_code or puma_name):
        group_col = puma_name or puma_code
        assert group_col is not None

        print(f"\n{'─' * 80}")
        print("PUMA-LEVEL UTILITY ASSIGNMENT SUMMARY")
        print(f"{'─' * 80}")

        summary_cols = [
            c
            for c in [group_col, puma_code, util_col, "pct_of_puma"]
            if c and c in df.columns
        ]
        summary = df.select(summary_cols).sort(
            [group_col, util_col] if util_col else [group_col]
        )

        for puma_val in summary[group_col].unique(maintain_order=True).sort():
            rows = summary.filter(pl.col(group_col) == puma_val)
            code_str = ""
            if puma_code and puma_code in rows.columns and puma_code != group_col:
                code_str = f"  [{rows[puma_code][0]}]"
            print(f"\n  {puma_val}{code_str}")
            for row in rows.iter_rows(named=True):
                pct_str = (
                    f"  ({row['pct_of_puma']:.1f}% of PUMA)"
                    if "pct_of_puma" in row
                    else ""
                )
                print(f"    • {row[util_col]}{pct_str}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Inspect CT GIS boundary data from fetch_ct_gis_boundaries outputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--path-local-csv",
        default="data/resstock/utility/csv",
        metavar="DIR",
        help="Directory containing ct_electric_utilities_*.csv files.",
    )
    p.add_argument(
        "--path-local-shapefiles",
        default="data/resstock/utility/shapefiles",
        metavar="DIR",
        help="Directory containing the pumas/ shapefile subdirectory.",
    )
    p.add_argument(
        "--path-local-parquet",
        default="data/resstock/utility/parquet",
        metavar="DIR",
        help="Directory containing ct_puma_elec_utility_overlay_*.parquet files.",
    )
    p.add_argument(
        "--no-overlay",
        action="store_true",
        help="Skip the overlay section (use if fetch was run with --no-overlay).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    errors: list[str] = []

    # ── Section 1: Utilities ──────────────────────────────────────────────
    csv_dir = Path(args.path_local_csv)
    csv_path = (
        _latest(csv_dir, "ct_electric_utilities_*.csv") if csv_dir.exists() else None
    )
    if csv_path:
        _show_utilities(csv_path)
    else:
        msg = f"No ct_electric_utilities_*.csv found in {csv_dir}. Run fetch first."
        print(f"\nSKIPPING utilities: {msg}", file=sys.stderr)
        errors.append(msg)

    # ── Section 2: PUMAs ─────────────────────────────────────────────────
    puma_shp = Path(args.path_local_shapefiles) / "pumas" / "tl_2024_09_puma20.shp"
    if puma_shp.exists():
        _show_pumas(puma_shp)
    else:
        msg = f"PUMA shapefile not found: {puma_shp}. Run fetch first."
        print(f"\nSKIPPING PUMAs: {msg}", file=sys.stderr)
        errors.append(msg)

    # ── Section 3: Overlay ────────────────────────────────────────────────
    if not args.no_overlay:
        parquet_dir = Path(args.path_local_parquet)
        parquet_path = (
            _latest(parquet_dir, "ct_puma_elec_utility_overlay_*.parquet")
            if parquet_dir.exists()
            else None
        )
        if parquet_path:
            _show_overlay(parquet_path)
        else:
            msg = (
                f"No ct_puma_elec_utility_overlay_*.parquet found in {parquet_dir}. "
                "Run fetch without --no-overlay, or pass --no-overlay to skip this section."
            )
            print(f"\nSKIPPING overlay: {msg}", file=sys.stderr)
            errors.append(msg)

    if errors:
        print(f"\n{'─' * 80}", file=sys.stderr)
        print(
            f"{len(errors)} section(s) skipped due to missing files.", file=sys.stderr
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

"""Fetch Connecticut electric utility territory and PUMA shapefiles for ResStock utility assignment.

Downloads CT 2020 PUMA boundaries from Census TIGER/Line and the HIFLD electric
retail service territories (nationwide), clips the latter to Connecticut, writes
the utility territories as a WKT-geometry CSV compatible with
``data.resstock.assign_utility.read_csv_to_gdf_from_s3``, and optionally
computes a PUMA × utility territory spatial overlay.

All outputs are written to local staging dirs (no S3 writes here); upload is a
separate step handled by the Justfile ``upload`` recipe.

Usage::

    uv run python data/resstock/utility/fetch_ct_gis_boundaries.py \\
        --path-local-zip        data/resstock/utility/zips \\
        --path-local-shapefiles data/resstock/utility/shapefiles \\
        --path-local-csv        data/resstock/utility/csv \\
        --path-local-parquet    data/resstock/utility/parquet

    # Skip the PUMA x utility overlay
    uv run python ... --no-overlay

    # Re-process without re-downloading (ZIPs already present locally)
    uv run python ... --no-download

NOTE on the HIFLD URL: the default URL embeds a ``jsessionid`` that may expire.
If the download fails with a 4xx error, obtain a fresh URL from
https://www.datalumos.org/datalumos/project/239091/version/V2/view
and pass it via ``--hifld-url``.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

import geopandas as gpd
import polars as pl
import requests

CT_STATE_CRS = 2234  # NAD83 / Connecticut State Plane
CT_STATE_ABBR = "CT"

# Census TIGER/Line 2024 PUMA boundaries for Connecticut (FIPS 09)
TIGER_PUMA_URL = "https://www2.census.gov/geo/tiger/TIGER2024/PUMA/tl_2024_09_puma20.zip"
TIGER_PUMA_ZIP = "tl_2024_09_puma20.zip"

# HIFLD Electric Retail Service Territories (nationwide shapefile ZIP)
# The jsessionid may expire; pass a fresh URL via --hifld-url if needed.
HIFLD_URL_DEFAULT = (
    "https://www.datalumos.org/datalumos/project/239091/version/V2/view"
    ";jsessionid=03A2D27C2281D3006379D44CC91C4B78"
    "?path=%2Fdatalumos%2F239091%2Ffcr%3Aversions%2FV2%2F"
    "electric-retail-service-territories-shapefile.zip&type=file"
)
HIFLD_ZIP = "electric-retail-service-territories.zip"


# ── Download helper ────────────────────────────────────────────────────────────


def _download(url: str, dest: Path, label: str) -> None:
    """Stream-download ``url`` to ``dest``, printing MB progress."""
    print(f"  Downloading {label} ...", flush=True)
    print(f"    URL: {url}", flush=True)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(
                        f"\r    {downloaded / 1e6:.1f} / {total / 1e6:.1f} MB  ({pct:.0f}%)",
                        end="",
                        flush=True,
                    )
    print(flush=True)
    print(f"    Saved → {dest}", flush=True)


# ── Step 1: CT PUMAs ──────────────────────────────────────────────────────────


def fetch_pumas(
    path_local_zip: Path,
    path_local_shapefiles: Path,
    *,
    skip_download: bool,
) -> gpd.GeoDataFrame:
    """Download and extract the CT 2020 PUMA shapefile; return as GeoDataFrame."""
    zip_path = path_local_zip / TIGER_PUMA_ZIP
    puma_dir = path_local_shapefiles / "pumas"
    puma_dir.mkdir(parents=True, exist_ok=True)

    if not skip_download:
        _download(TIGER_PUMA_URL, zip_path, "CT PUMA boundaries (Census TIGER/Line 2024)")
    elif not zip_path.exists():
        raise FileNotFoundError(
            f"--no-download requested but {zip_path} not found. "
            "Run without --no-download to fetch it first."
        )

    print("  Extracting PUMA shapefile ...", flush=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(puma_dir)

    shp = puma_dir / "tl_2024_09_puma20.shp"
    if not shp.exists():
        raise FileNotFoundError(f"Expected shapefile not found after extraction: {shp}")

    pumas = gpd.read_file(str(shp))
    print(f"    {len(pumas)} PUMA polygons loaded (CRS: {pumas.crs}).", flush=True)
    return pumas


# ── Step 2: CT electric utility territories ───────────────────────────────────


def fetch_ct_utilities(
    path_local_zip: Path,
    path_local_shapefiles: Path,
    hifld_url: str,
    *,
    skip_download: bool,
) -> gpd.GeoDataFrame:
    """Download the HIFLD nationwide shapefile ZIP, extract, and filter to CT."""
    zip_path = path_local_zip / HIFLD_ZIP
    util_dir = path_local_shapefiles / "utilities"
    util_dir.mkdir(parents=True, exist_ok=True)

    if not skip_download:
        _download(
            hifld_url,
            zip_path,
            "HIFLD electric retail service territories (nationwide)",
        )
    elif not zip_path.exists():
        raise FileNotFoundError(
            f"--no-download requested but {zip_path} not found. "
            "Run without --no-download to fetch it first."
        )

    print("  Extracting HIFLD shapefile ...", flush=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(util_dir)

    shp_files = sorted(util_dir.rglob("*.shp"))
    if not shp_files:
        raise RuntimeError(
            f"No .shp file found under {util_dir} after extracting {zip_path}. "
            f"Contents: {sorted(p.name for p in util_dir.iterdir())}"
        )
    nationwide_shp = shp_files[0]
    print(f"    Reading {nationwide_shp.name} ...", flush=True)

    nationwide = gpd.read_file(str(nationwide_shp))
    print(f"    {len(nationwide)} utility territories nationwide.", flush=True)

    ct = nationwide[nationwide["STATE"] == CT_STATE_ABBR].copy()
    if len(ct) == 0:
        sample = sorted(nationwide["STATE"].dropna().unique()[:10].tolist())
        raise RuntimeError(
            f"No rows with STATE='{CT_STATE_ABBR}' in {nationwide_shp.name}. "
            f"Sample STATE values present: {sample}"
        )
    print(f"    {len(ct)} Connecticut utility territories extracted.", flush=True)
    return ct


def write_utilities_csv(utilities: gpd.GeoDataFrame, out_path: Path) -> None:
    """Write CT utility GeoDataFrame as a WKT-geometry CSV.

    Column layout matches what ``read_csv_to_gdf_from_s3`` expects for
    ``utility_type='electric'``:

    - ``comp_full`` — utility name (renamed from HIFLD's ``NAME`` column)
    - ``the_geom``  — WKT geometry in EPSG:4326

    The GeoDataFrame is reprojected to WGS-84 before export so the CSV
    is portable and readable by ``read_csv_to_gdf_from_s3(..., crs='EPSG:4326')``.
    """
    df = utilities.copy().to_crs(epsg=4326)
    df["the_geom"] = df.geometry.to_wkt()
    df = df.drop(columns="geometry")
    if "NAME" in df.columns and "comp_full" not in df.columns:
        df = df.rename(columns={"NAME": "comp_full"})
    df.to_csv(str(out_path), index=False)
    print(f"    CT utilities CSV: {out_path} ({len(df)} rows).", flush=True)


# ── Step 3: PUMA × utility overlay ───────────────────────────────────────────


def compute_overlay(
    pumas: gpd.GeoDataFrame,
    utilities: gpd.GeoDataFrame,
    state_crs: int,
) -> gpd.GeoDataFrame:
    """Return the spatial intersection of CT PUMAs and CT utility territories.

    Both inputs are reprojected to ``state_crs`` (CT state plane) before the
    overlay so that area calculations are in metres/feet rather than degrees.
    """
    print(f"  Reprojecting to EPSG:{state_crs} (CT state plane) ...", flush=True)
    pumas_proj = pumas.to_crs(epsg=state_crs)
    utils_proj = utilities.to_crs(epsg=state_crs)

    print("  Computing PUMA × utility intersection ...", flush=True)
    overlay = gpd.overlay(utils_proj, pumas_proj, how="intersection")
    print(f"    {len(overlay)} overlay polygons produced.", flush=True)
    return overlay


def write_overlay_parquet(overlay: gpd.GeoDataFrame, out_path: Path) -> None:
    """Write the overlay GeoDataFrame as a zstd-compressed parquet file.

    The geometry is serialised as a WKT string in a ``geometry_wkt`` column
    (CRS: CT state plane, EPSG:2234) so the file is readable by both
    GeoPandas (``gpd.GeoDataFrame.from_wkt``) and Polars.
    """
    df = overlay.copy()
    df["geometry_wkt"] = df.geometry.to_wkt()
    df_plain = df.drop(columns="geometry")
    pl.from_pandas(df_plain).write_parquet(str(out_path), compression="zstd")
    print(f"    Overlay parquet: {out_path} ({len(df_plain)} rows).", flush=True)


# ── S3 helpers ────────────────────────────────────────────────────────────────


def _s3_cp(local: Path, s3_dest: str) -> None:
    rc = subprocess.run(["aws", "s3", "cp", str(local), s3_dest], check=False).returncode
    if rc != 0:
        print(f"  WARNING: aws s3 cp exited with code {rc} for {local.name}.", flush=True)


def _s3_sync(local_dir: Path, s3_dest: str) -> None:
    rc = subprocess.run(["aws", "s3", "sync", str(local_dir), s3_dest], check=False).returncode
    if rc != 0:
        print(f"  WARNING: aws s3 sync exited with code {rc} for {local_dir}.", flush=True)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch CT electric utility territory and PUMA shapefiles.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--path-local-zip",
        required=True,
        metavar="DIR",
        help="Directory for downloaded ZIP files.",
    )
    p.add_argument(
        "--path-local-shapefiles",
        required=True,
        metavar="DIR",
        help="Directory for extracted shapefiles (pumas/ and utilities/ subdirs are created here).",
    )
    p.add_argument(
        "--path-local-csv",
        required=True,
        metavar="DIR",
        help="Directory for the CT utilities WKT-geometry CSV.",
    )
    p.add_argument(
        "--path-local-parquet",
        required=True,
        metavar="DIR",
        help="Directory for the PUMA × utility overlay parquet.",
    )
    p.add_argument(
        "--path-s3-pumas",
        default="s3://data.sb/gis/pumas",
        metavar="S3_URI",
        help="S3 base URI for PUMA shapefiles.",
    )
    p.add_argument(
        "--path-s3-utility",
        default="s3://data.sb/gis/utility_boundaries",
        metavar="S3_URI",
        help="S3 base URI for utility territory files.",
    )
    p.add_argument(
        "--hifld-url",
        default=HIFLD_URL_DEFAULT,
        metavar="URL",
        help=(
            "Override the HIFLD download URL. "
            "The default embeds a jsessionid that may expire; "
            "obtain a fresh URL from datalumos.org if the download fails."
        ),
    )
    p.add_argument(
        "--no-overlay",
        action="store_true",
        help="Skip the PUMA × utility territory overlay computation.",
    )
    p.add_argument(
        "--no-download",
        action="store_true",
        help="Skip HTTP downloads; re-use existing ZIPs in --path-local-zip.",
    )
    p.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip uploading outputs to S3 (local-only run).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    today = date.today().strftime("%Y%m%d")

    # Safeguard: reject uninterpolated Just variables
    for attr in (
        "path_local_zip",
        "path_local_shapefiles",
        "path_local_csv",
        "path_local_parquet",
    ):
        val: str = getattr(args, attr)
        if "{{" in val or "}}" in val:
            print(
                f"ERROR: --{attr.replace('_', '-')} contains an uninterpolated "
                f"Just variable: {val!r}",
                flush=True,
            )
            sys.exit(1)

    path_local_zip = Path(args.path_local_zip)
    path_local_shapefiles = Path(args.path_local_shapefiles)
    path_local_csv = Path(args.path_local_csv)
    path_local_parquet = Path(args.path_local_parquet)

    for d in (path_local_zip, path_local_shapefiles, path_local_csv, path_local_parquet):
        d.mkdir(parents=True, exist_ok=True)

    # ── 1. CT PUMAs ───────────────────────────────────────────────────────
    print("Step 1: CT PUMA boundaries (Census TIGER/Line 2024) ...", flush=True)
    pumas = fetch_pumas(
        path_local_zip,
        path_local_shapefiles,
        skip_download=args.no_download,
    )

    # ── 2. CT electric utility territories ───────────────────────────────
    print("Step 2: HIFLD electric utility territories ...", flush=True)
    ct_utilities = fetch_ct_utilities(
        path_local_zip,
        path_local_shapefiles,
        args.hifld_url,
        skip_download=args.no_download,
    )
    utils_csv = path_local_csv / f"ct_electric_utilities_{today}.csv"
    write_utilities_csv(ct_utilities, utils_csv)

    # ── 3. PUMA × utility overlay ─────────────────────────────────────────
    overlay_parquet: Path | None = None
    if not args.no_overlay:
        print("Step 3: PUMA × utility overlay ...", flush=True)
        overlay = compute_overlay(pumas, ct_utilities, CT_STATE_CRS)
        overlay_parquet = path_local_parquet / f"ct_puma_utility_overlay_{today}.parquet"
        write_overlay_parquet(overlay, overlay_parquet)
    else:
        print("Step 3: Overlay skipped (--no-overlay).", flush=True)

    # ── 4. Upload to S3 ───────────────────────────────────────────────────
    if not args.no_upload:
        print("Step 4: Uploading to S3 ...", flush=True)

        puma_s3 = f"{args.path_s3_pumas.rstrip('/')}/state=CT/"
        print(f"  Syncing PUMA shapefile → {puma_s3}", flush=True)
        _s3_sync(path_local_shapefiles / "pumas", puma_s3)

        util_s3 = f"{args.path_s3_utility.rstrip('/')}/{utils_csv.name}"
        print(f"  Uploading CT utilities CSV → {util_s3}", flush=True)
        _s3_cp(utils_csv, util_s3)

        if overlay_parquet is not None:
            ov_s3 = f"{args.path_s3_utility.rstrip('/')}/{overlay_parquet.name}"
            print(f"  Uploading overlay → {ov_s3}", flush=True)
            _s3_cp(overlay_parquet, ov_s3)
    else:
        print("Step 4: Upload skipped (--no-upload).", flush=True)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()

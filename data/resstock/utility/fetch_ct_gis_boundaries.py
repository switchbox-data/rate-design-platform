"""Fetch Connecticut electric and gas utility territory and PUMA shapefiles.

Downloads CT 2020 PUMA boundaries from Census TIGER/Line, HIFLD electric retail
service territories, and HIFLD natural gas LDC service territories; clips each to
Connecticut; writes utility territories as WKT-geometry CSVs compatible with
``data.resstock.assign_utility.read_csv_to_gdf_from_s3``; and optionally computes
PUMA × utility territory spatial overlays.

All outputs are written to local staging dirs (no S3 writes here); upload is a
separate step handled by the Justfile ``upload`` recipe.

Usage::

    uv run python data/resstock/utility/fetch_ct_gis_boundaries.py \\
        --path-local-zip        data/resstock/utility/zips \\
        --path-local-shapefiles data/resstock/utility/shapefiles \\
        --path-local-csv        data/resstock/utility/csv \\
        --path-local-parquet    data/resstock/utility/parquet

    # Skip the PUMA x utility overlays
    uv run python ... --no-overlay

    # Skip gas utility fetching
    uv run python ... --no-gas

    # Re-process without re-downloading (shapefiles already cached locally)
    uv run python ... --no-download

For data source details and alternatives, see:
  context/code/data/ct_utility_gis_data_sources.md
"""

from __future__ import annotations

import argparse
import io
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

# Census TIGER/Line 2024 PUMA (2020-definition) boundaries for Connecticut (FIPS 09).
# NOTE: For year >= 2022, Census hosts these under /PUMA20/ (not /PUMA/).  pygris has a
# bug (same as R tigris — github.com/walkerke/tigris/issues/213) that constructs the
# wrong path, so we download directly rather than using pygris.
TIGER_PUMA_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/tl_2024_09_puma20.zip"
)
TIGER_PUMA_ZIP = "tl_2024_09_puma20.zip"

# ── HIFLD Electric Retail Service Territories ─────────────────────────────────
#
# The DHS HIFLD Open portal was shut down on 2025-08-26.  The dataset is
# mirrored by multiple hosts; we try them in order.  Data vintage: 2022.
# These are actual surveyed utility boundary polygons, not county approximations.
#
# For context on these sources and alternatives, see:
#   context/code/data/ct_utility_gis_data_sources.md
#
# Override via --hifld-elec-url to bypass the fallback chain.
HIFLD_ELEC_URLS = [
    # 1. NASA NCCS mirror — government facility with energy/climate mandate.
    (
        "https://maps.nccs.nasa.gov/mapping/rest/services/"
        "hifld_open/energy/MapServer/26/query"
        "?where=STATE%3D%27CT%27&outFields=*&outSR=4326&f=geojson"
    ),
    # 2. ArcGIS org mirror — org ID OYP7N6mAJJCyH6hd appears to be maintained
    #    by the original HIFLD/ORNL data team post-portal-shutdown.
    (
        "https://services3.arcgis.com/OYP7N6mAJJCyH6hd/ArcGIS/rest/services/"
        "Electric_Retail_Service_Territories_HIFLD/FeatureServer/0/query"
        "?where=STATE%3D%27CT%27&outFields=*&outSR=4326&f=geojson"
    ),
]

# ── HIFLD Natural Gas LDC Service Territories ─────────────────────────────────
#
# Same HIFLD vintage (2022), same shutdown context.  Unlike electric, no ArcGIS
# FeatureServer mirror is known for the gas layer — only the NASA NCCS MapServer
# hosts it programmatically.  If that fails, manual download from the Data Rescue
# Project (DataLumos) is the fallback; see HIFLD_GAS_DATALUMOS_URL below.
#
# Override via --hifld-gas-url to bypass the fallback chain.
HIFLD_GAS_URLS = [
    # 1. NASA NCCS mirror (Layer 29 = natural_gas_service_territories).
    (
        "https://maps.nccs.nasa.gov/mapping/rest/services/"
        "hifld_open/energy/MapServer/29/query"
        "?where=STATE%3D%27CT%27&outFields=*&outSR=4326&f=geojson"
    ),
]

# DataLumos archive of the HIFLD gas territories (downloaded 2025-08-26).
# Behind Cloudflare — cannot be fetched programmatically; manual download only.
# If NASA NCCS is down, visit this URL, download the shapefile ZIP, extract it,
# and re-run with --no-download (the cached shapefile will be used instead).
HIFLD_GAS_DATALUMOS_URL = (
    "https://www.datalumos.org/datalumos/project/240245/version/V1/view"
)


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
                else:
                    # No Content-Length header — print running total so there is
                    # visible activity rather than apparent silence.
                    print(
                        f"\r    {downloaded / 1e6:.1f} MB received …",
                        end="",
                        flush=True,
                    )
    print(flush=True)
    print(f"    Saved → {dest} ({dest.stat().st_size / 1e6:.1f} MB)", flush=True)


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
        _download(
            TIGER_PUMA_URL, zip_path, "CT PUMA boundaries (Census TIGER/Line 2024)"
        )
    elif not zip_path.exists():
        raise FileNotFoundError(
            f"--no-download requested but {zip_path} not found. "
            "Run without --no-download to fetch it first."
        )

    print("  Extracting PUMA shapefile …", flush=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(puma_dir)

    shp = puma_dir / "tl_2024_09_puma20.shp"
    if not shp.exists():
        raise FileNotFoundError(f"Expected shapefile not found after extraction: {shp}")

    pumas = gpd.read_file(str(shp))

    print(f"    {len(pumas)} PUMA polygons loaded (CRS: {pumas.crs}).", flush=True)
    return pumas


# ── Step 2a: CT electric utility territories ──────────────────────────────────


def _fetch_geojson(
    urls: list[str],
    manual_fallback: str | None = None,
) -> gpd.GeoDataFrame:
    """Try each URL in order; return GeoDataFrame from the first that succeeds.

    Uses ``requests.get`` instead of ``gpd.read_file(url)`` because some ArcGIS
    MapServer endpoints reject the ``Range: bytes=0-1`` probe that geopandas
    sends.

    If all URLs fail and ``manual_fallback`` is provided, its text is appended
    to the error message to guide the user toward a manual download.
    """
    errors: list[tuple[str, Exception]] = []
    for url in urls:
        print(f"    Trying: {url}", flush=True)
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            gdf = gpd.read_file(io.BytesIO(resp.content))
            if len(gdf) == 0:
                raise RuntimeError("Query returned 0 features.")
            print(f"    ✓ Success ({len(gdf)} features).", flush=True)
            return gdf
        except Exception as exc:  # noqa: BLE001
            print(f"    ✗ Failed: {exc}", flush=True)
            errors.append((url, exc))

    failed = "\n".join(f"  {u}: {e}" for u, e in errors)
    extra = f"\n{manual_fallback}" if manual_fallback else ""
    raise RuntimeError(
        f"All {len(urls)} endpoints failed:\n{failed}"
        "\nPass the relevant --hifld-*-url flag with a working URL, or use "
        f"--no-download with a previously cached shapefile.{extra}"
    )


def fetch_ct_utilities(
    path_local_shapefiles: Path,
    hifld_urls: list[str],
    *,
    skip_download: bool,
) -> gpd.GeoDataFrame:
    """Fetch CT electric utility territories from HIFLD mirrors.

    Tries each URL in ``hifld_urls`` in order until one succeeds. The result is
    saved as a shapefile in the utilities/ subdirectory so it can be reused with
    ``--no-download``.
    """
    util_dir = path_local_shapefiles / "utilities"
    util_dir.mkdir(parents=True, exist_ok=True)
    local_shp = util_dir / "ct_electric_utilities.shp"

    if skip_download:
        if not local_shp.exists():
            raise FileNotFoundError(
                f"--no-download requested but {local_shp} not found. "
                "Run without --no-download to fetch it first."
            )
        print("  Re-using cached CT utility shapefile (--no-download).", flush=True)
        ct = gpd.read_file(str(local_shp))
    else:
        print("  Fetching CT utility territories from HIFLD …", flush=True)
        ct = _fetch_geojson(hifld_urls)
        ct.to_file(local_shp)
        print(f"    Cached → {local_shp}", flush=True)

    print(f"    {len(ct)} Connecticut utility territories loaded.", flush=True)
    return ct


# ── Step 2b: CT gas utility territories ───────────────────────────────────────


def fetch_ct_gas_utilities(
    path_local_shapefiles: Path,
    hifld_gas_urls: list[str],
    *,
    skip_download: bool,
) -> gpd.GeoDataFrame:
    """Fetch CT natural gas LDC service territories from HIFLD mirrors.

    Tries each URL in ``hifld_gas_urls`` in order until one succeeds.  Unlike
    the electric dataset, no ArcGIS FeatureServer mirror is known for the gas
    layer — only the NASA NCCS MapServer (Layer 29) hosts it programmatically.
    If all automated sources fail, the error message includes instructions to
    manually download from the Data Rescue Project DataLumos archive.

    The result is cached as a shapefile so ``--no-download`` works on reruns.
    """
    gas_dir = path_local_shapefiles / "gas_utilities"
    gas_dir.mkdir(parents=True, exist_ok=True)
    local_shp = gas_dir / "ct_gas_utilities.shp"

    if skip_download:
        if not local_shp.exists():
            raise FileNotFoundError(
                f"--no-download requested but {local_shp} not found. "
                "Run without --no-download to fetch it first, or manually "
                f"download from {HIFLD_GAS_DATALUMOS_URL} and extract the "
                f"CT features into {local_shp}."
            )
        print("  Re-using cached CT gas utility shapefile (--no-download).", flush=True)
        ct = gpd.read_file(str(local_shp))
    else:
        print("  Fetching CT gas utility territories from HIFLD …", flush=True)
        manual = (
            "Manual fallback: if all automated sources are unavailable, download "
            "the archived HIFLD gas territories shapefile from the Data Rescue "
            f"Project (DataLumos) at:\n  {HIFLD_GAS_DATALUMOS_URL}\n"
            "Then extract CT features, save as a shapefile, and re-run with "
            "--no-download."
        )
        ct = _fetch_geojson(hifld_gas_urls, manual_fallback=manual)
        ct.to_file(local_shp)
        print(f"    Cached → {local_shp}", flush=True)

    print(f"    {len(ct)} Connecticut gas utility territories loaded.", flush=True)
    return ct


def write_utilities_csv(
    utilities: gpd.GeoDataFrame, out_path: Path, label: str = "utilities"
) -> None:
    """Write a utility GeoDataFrame as a WKT-geometry CSV.

    Column layout matches what ``read_csv_to_gdf_from_s3`` expects:

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
    print(f"    CT {label} CSV: {out_path} ({len(df)} rows).", flush=True)


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
    rc = subprocess.run(
        ["aws", "s3", "cp", str(local), s3_dest], check=False
    ).returncode
    if rc != 0:
        print(
            f"  WARNING: aws s3 cp exited with code {rc} for {local.name}.", flush=True
        )


def _s3_sync(local_dir: Path, s3_dest: str) -> None:
    rc = subprocess.run(
        ["aws", "s3", "sync", str(local_dir), s3_dest], check=False
    ).returncode
    if rc != 0:
        print(
            f"  WARNING: aws s3 sync exited with code {rc} for {local_dir}.", flush=True
        )


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
        "--hifld-elec-url",
        default=None,
        metavar="URL",
        help=(
            "Override the electric utility territory REST API URL (skips the fallback "
            "chain). By default, the script tries NASA NCCS then the ORNL ArcGIS "
            "mirror. See context/code/data/ct_utility_gis_data_sources.md."
        ),
    )
    p.add_argument(
        "--hifld-gas-url",
        default=None,
        metavar="URL",
        help=(
            "Override the gas utility territory REST API URL (skips the fallback "
            "chain). By default, the script tries the NASA NCCS MapServer Layer 29. "
            "If unavailable, see context/code/data/ct_utility_gis_data_sources.md "
            f"for the DataLumos manual-download fallback ({HIFLD_GAS_DATALUMOS_URL})."
        ),
    )
    p.add_argument(
        "--no-gas",
        action="store_true",
        help="Skip gas utility territory fetching (Step 2b and gas overlay).",
    )
    p.add_argument(
        "--no-overlay",
        action="store_true",
        help="Skip the PUMA × utility territory overlay computations.",
    )
    p.add_argument(
        "--no-download",
        action="store_true",
        help="Skip HTTP downloads; re-use previously cached shapefiles.",
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

    for d in (
        path_local_zip,
        path_local_shapefiles,
        path_local_csv,
        path_local_parquet,
    ):
        d.mkdir(parents=True, exist_ok=True)

    # ── 1. CT PUMAs ───────────────────────────────────────────────────────
    print("Step 1: CT PUMA boundaries (Census TIGER/Line 2024) ...", flush=True)
    pumas = fetch_pumas(
        path_local_zip,
        path_local_shapefiles,
        skip_download=args.no_download,
    )

    # ── 2a. CT electric utility territories ──────────────────────────────
    print("Step 2a: HIFLD electric utility territories ...", flush=True)
    elec_urls = [args.hifld_elec_url] if args.hifld_elec_url else HIFLD_ELEC_URLS
    ct_elec = fetch_ct_utilities(
        path_local_shapefiles,
        elec_urls,
        skip_download=args.no_download,
    )
    elec_csv = path_local_csv / f"ct_electric_utilities_{today}.csv"
    write_utilities_csv(ct_elec, elec_csv, label="electric utilities")

    # ── 2b. CT gas utility territories ───────────────────────────────────
    ct_gas: gpd.GeoDataFrame | None = None
    gas_csv: Path | None = None
    if not args.no_gas:
        print("Step 2b: HIFLD natural gas utility territories ...", flush=True)
        gas_urls = [args.hifld_gas_url] if args.hifld_gas_url else HIFLD_GAS_URLS
        ct_gas = fetch_ct_gas_utilities(
            path_local_shapefiles,
            gas_urls,
            skip_download=args.no_download,
        )
        gas_csv = path_local_csv / f"ct_gas_utilities_{today}.csv"
        write_utilities_csv(ct_gas, gas_csv, label="gas utilities")
    else:
        print("Step 2b: Gas utilities skipped (--no-gas).", flush=True)

    # ── 3. PUMA × utility overlays ────────────────────────────────────────
    elec_overlay_parquet: Path | None = None
    gas_overlay_parquet: Path | None = None
    if not args.no_overlay:
        print("Step 3: PUMA × utility overlays ...", flush=True)

        print("  Electric overlay ...", flush=True)
        elec_overlay = compute_overlay(pumas, ct_elec, CT_STATE_CRS)
        elec_overlay_parquet = (
            path_local_parquet / f"ct_puma_elec_utility_overlay_{today}.parquet"
        )
        write_overlay_parquet(elec_overlay, elec_overlay_parquet)

        if ct_gas is not None:
            print("  Gas overlay ...", flush=True)
            gas_overlay = compute_overlay(pumas, ct_gas, CT_STATE_CRS)
            gas_overlay_parquet = (
                path_local_parquet / f"ct_puma_gas_utility_overlay_{today}.parquet"
            )
            write_overlay_parquet(gas_overlay, gas_overlay_parquet)
    else:
        print("Step 3: Overlays skipped (--no-overlay).", flush=True)

    # ── 4. Upload to S3 ───────────────────────────────────────────────────
    if not args.no_upload:
        print("Step 4: Uploading to S3 ...", flush=True)

        puma_s3 = f"{args.path_s3_pumas.rstrip('/')}/state=CT/"
        print(f"  Syncing PUMA shapefile → {puma_s3}", flush=True)
        _s3_sync(path_local_shapefiles / "pumas", puma_s3)

        elec_s3 = f"{args.path_s3_utility.rstrip('/')}/{elec_csv.name}"
        print(f"  Uploading electric utilities CSV → {elec_s3}", flush=True)
        _s3_cp(elec_csv, elec_s3)

        if gas_csv is not None:
            gas_s3 = f"{args.path_s3_utility.rstrip('/')}/{gas_csv.name}"
            print(f"  Uploading gas utilities CSV → {gas_s3}", flush=True)
            _s3_cp(gas_csv, gas_s3)

        if elec_overlay_parquet is not None:
            ov_s3 = f"{args.path_s3_utility.rstrip('/')}/{elec_overlay_parquet.name}"
            print(f"  Uploading electric overlay → {ov_s3}", flush=True)
            _s3_cp(elec_overlay_parquet, ov_s3)

        if gas_overlay_parquet is not None:
            ov_s3 = f"{args.path_s3_utility.rstrip('/')}/{gas_overlay_parquet.name}"
            print(f"  Uploading gas overlay → {ov_s3}", flush=True)
            _s3_cp(gas_overlay_parquet, ov_s3)
    else:
        print("Step 4: Upload skipped (--no-upload).", flush=True)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()

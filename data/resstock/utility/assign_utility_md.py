"""Utility assignment for ResStock buildings (MD).

Thin wrapper around the state-generic helpers in
``data.resstock.utility.utils``, providing MD-specific configuration
(HIFLD fetch/cache loaders, CRS, PUMA year).

The public entry point is ``assign_utility()`` — called by the dynamic
dispatch in ``data.resstock.utility.assign_utility`` with kwargs from
``state_configs.yaml``.  The lower-level ``assign_utility_md()`` takes
pre-loaded GeoDataFrames and is used directly when GIS data is already
in memory.

GIS data fetch / cache helpers (``load_pumas``, ``load_electric_utilities``,
``load_gas_utilities``) are also exported so callers can fetch and cache
boundary files locally before running the assignment.  On first use, these
pull data from HIFLD mirrors (electric/gas) and pygris (PUMAs), write dated
WKT CSVs to a local cache directory, upload to S3, and write the filenames
back to ``state_configs.yaml``.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import geopandas as gpd
import polars as pl
from cloudpathlib import S3Path
from pygris import pumas as pygris_get_pumas

from data.resstock.utils import (
    load_state_configs,
    select_puma_and_heating_fuel_metadata,
)
from data.resstock.utility.utils import (
    HIFLD_ELEC_URLS,
    HIFLD_GAS_DATALUMOS_URL,
    HIFLD_GAS_DATALUMOS_ZIP,
    HIFLD_GAS_URLS,
    HIFLD_GAS_ZIP_SHP,
    S3_GIS_DIR,
    create_hh_utilities,
    latest_utility_csv_name,
    load_pumas as _load_pumas,
    load_utility_boundaries,
    read_csv_to_gdf_from_s3,
    update_state_config_filenames,
)
from utils import get_aws_region

# ── MD-specific constants ─────────────────────────────────────────────────────

_STATE = "MD"
_STATE_CONFIGS = load_state_configs()
_MD_CFG = _STATE_CONFIGS[_STATE]["utility_assignment"]["kwargs"]

MD_STATE_CRS: int = _MD_CFG["state_crs"]
MD_PUMA_YEAR: int = _MD_CFG["puma_year"]

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


# ── Pipeline entry point ──────────────────────────────────────────────────────


def assign_utility(
    metadata: pl.LazyFrame,
    *,
    state_crs: int,
    puma_year: int,
    electric_poly_filename: str,
    gas_poly_filename: str,
    path_s3_gis_dir: str | None = None,
    excluded_gas_utilities: list[str] | None = None,
) -> pl.LazyFrame:
    """Entry point for dynamic dispatch from ``assign_utility.py``.

    Loads GIS data (polygon CSVs from S3, Census PUMAs via pygris) and
    delegates to :func:`assign_utility_md`.

    Args:
        metadata: ResStock metadata LazyFrame.
        state_crs: EPSG code for MD projected CRS (2248 = NAD83 / Maryland
            State Plane feet).
        puma_year: Census TIGER/Line PUMA vintage year (2019 for 2010-def).
        electric_poly_filename: Filename of the electric utility polygon CSV
            in ``path_s3_gis_dir``.
        gas_poly_filename: Filename of the gas utility polygon CSV in
            ``path_s3_gis_dir``.
        path_s3_gis_dir: S3 directory containing the polygon CSV files.
            Defaults to the ``paths.s3_gis_dir`` value in ``config.yaml``.
        excluded_gas_utilities: Standardised gas utility names whose PUMA
            probabilities are zeroed before sampling (default: none).
    """
    gis_base = S3Path((path_s3_gis_dir or S3_GIS_DIR).rstrip("/"))

    electric_polygons = read_csv_to_gdf_from_s3(
        gis_base / electric_poly_filename,
        utility_type="electric",
        state_crs=state_crs,
    )
    gas_polygons = read_csv_to_gdf_from_s3(
        gis_base / gas_poly_filename,
        utility_type="gas",
        state_crs=state_crs,
    )

    print("    Loading MD Census PUMA shapefiles via pygris ...", flush=True)
    pumas = cast(
        gpd.GeoDataFrame,
        pygris_get_pumas(state=_STATE, year=puma_year, cb=True),
    )
    pumas = pumas.to_crs(epsg=state_crs)

    return assign_utility_md(
        input_metadata=metadata,
        electric_polygons=electric_polygons,
        gas_polygons=gas_polygons,
        pumas=pumas,
        state_crs=state_crs,
        excluded_gas_utilities=frozenset(excluded_gas_utilities)
        if excluded_gas_utilities is not None
        else frozenset(),
    )


# ── Core assignment logic ─────────────────────────────────────────────────────


def assign_utility_md(
    input_metadata: pl.LazyFrame,
    electric_polygons: gpd.GeoDataFrame,
    gas_polygons: gpd.GeoDataFrame,
    pumas: gpd.GeoDataFrame,
    state_crs: int,
    excluded_gas_utilities: frozenset[str] = frozenset(),
) -> pl.LazyFrame:
    """Assign electric and gas utilities to ResStock buildings in MD.

    Computes PUMA-level overlap probabilities from the supplied utility
    service territory polygons, then samples one electric and one gas utility
    per building according to those probabilities.

    MD uses HIFLD utility names directly (no name-crosswalk map needed); an
    empty ``utility_name_map`` is passed so names flow through unchanged.

    Args:
        input_metadata: ResStock metadata LazyFrame.
        electric_polygons: GeoDataFrame of MD electric utility territories.
        gas_polygons: GeoDataFrame of MD gas utility territories.
        pumas: GeoDataFrame of MD Census PUMAs projected to ``state_crs``.
        state_crs: EPSG code for the MD projected CRS.
        excluded_gas_utilities: Gas utility names whose PUMA probabilities are
            zeroed before sampling (default: empty).

    Returns:
        LazyFrame with all original metadata columns plus
        ``sb.electric_utility`` and ``sb.gas_utility``.
    """
    puma_and_heating_fuel = select_puma_and_heating_fuel_metadata(input_metadata)

    # MD uses HIFLD source names directly; no crosswalk needed.
    utility_name_map = pl.DataFrame(
        {
            "state_name": pl.Series([], dtype=pl.Utf8),
            "std_name": pl.Series([], dtype=pl.Utf8),
        }
    ).lazy()

    building_utilities = create_hh_utilities(
        puma_and_heating_fuel=puma_and_heating_fuel,
        electric_polygons=electric_polygons,
        gas_polygons=gas_polygons,
        pumas=pumas,
        utility_name_map=utility_name_map,
        state_crs=state_crs,
        excluded_gas_utilities=excluded_gas_utilities,
    )

    input_metadata = input_metadata.drop(
        ["sb.electric_utility", "sb.gas_utility"], strict=False
    )

    counts_df = cast(
        pl.DataFrame,
        input_metadata.select(pl.lit(1).sum().alias("input_count"))
        .join(
            building_utilities.select(pl.lit(1).sum().alias("building_count")),
            how="cross",
        )
        .collect(),
    )
    input_count = cast(int, counts_df["input_count"][0])
    building_count = cast(int, counts_df["building_count"][0])
    if input_count != building_count:
        raise ValueError(
            f"Row count mismatch: input_metadata has {input_count} rows, "
            f"but building_utilities has {building_count} rows"
        )

    return input_metadata.join(
        building_utilities.select(["bldg_id", "sb.electric_utility", "sb.gas_utility"]),
        on="bldg_id",
        how="left",
    )


# ── GIS data fetch / cache helpers ────────────────────────────────────────────


def load_pumas(cache_dir: Path) -> gpd.GeoDataFrame:
    """Load MD 2010-definition Census PUMA boundaries.

    Checks ``cache_dir/pumas/md_pumas.shp`` first.  If absent, fetches via
    pygris, saves locally, and syncs to S3.
    """
    return _load_pumas(state=_STATE, puma_year=MD_PUMA_YEAR, cache_dir=cache_dir)


def load_electric_utilities(cache_dir: Path) -> gpd.GeoDataFrame:
    """Load MD electric utility service territory boundaries.

    Checks the cached filename in ``state_configs.yaml`` first.  If absent,
    fetches from HIFLD mirrors, writes a dated WKT CSV, uploads to S3, and
    records the new filename in ``state_configs.yaml``.
    """
    cached_name: str = _MD_CFG.get("electric_poly_filename") or ""

    gdf = load_utility_boundaries(
        state=_STATE,
        utility_type="electric",
        cache_dir=cache_dir,
        cached_filename=cached_name,
        hifld_urls=HIFLD_ELEC_URLS,
    )

    if not cached_name or not (cache_dir / cached_name).exists():
        new_name = latest_utility_csv_name(cache_dir, _STATE, "electric")
        if new_name:
            update_state_config_filenames(_STATE, electric_filename=new_name)

    return gdf


def load_gas_utilities(cache_dir: Path) -> gpd.GeoDataFrame:
    """Load MD natural gas LDC service territory boundaries.

    Same cache-then-fetch pattern as :func:`load_electric_utilities`.
    Falls back to a locally cached DataLumos ZIP if all HIFLD mirrors fail.
    """
    cached_name: str = _MD_CFG.get("gas_poly_filename") or ""

    def _datalumos_fallback() -> gpd.GeoDataFrame:
        zip_abs = HIFLD_GAS_DATALUMOS_ZIP.resolve()
        if not zip_abs.exists():
            raise RuntimeError(
                f"Manual fallback: download the archived HIFLD gas territories "
                f"shapefile from DataLumos at:\n  {HIFLD_GAS_DATALUMOS_URL}\n"
                f"Save it to {HIFLD_GAS_DATALUMOS_ZIP} and retry."
            )
        print(f"    Falling back to local DataLumos ZIP: {zip_abs}", flush=True)
        all_gas = gpd.read_file(f"/vsizip/{zip_abs}/{HIFLD_GAS_ZIP_SHP}")
        md_slice = all_gas[all_gas["STATE"] == _STATE].copy()
        print(
            f"    Read {len(all_gas)} total features from ZIP; "
            f"{len(md_slice)} in {_STATE}.",
            flush=True,
        )
        return gpd.GeoDataFrame(md_slice, crs=all_gas.crs)

    gdf = load_utility_boundaries(
        state=_STATE,
        utility_type="gas",
        cache_dir=cache_dir,
        cached_filename=cached_name,
        hifld_urls=HIFLD_GAS_URLS,
        hifld_fallback_fn=_datalumos_fallback,
    )

    if not cached_name or not (cache_dir / cached_name).exists():
        new_name = latest_utility_csv_name(cache_dir, _STATE, "gas")
        if new_name:
            update_state_config_filenames(_STATE, gas_filename=new_name)

    return gdf

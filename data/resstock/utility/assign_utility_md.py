"""Utility assignment for ResStock buildings (MD).

Thin wrapper around the state-generic helpers in
``data.resstock.utility.utils``, providing MD-specific configuration
(CRS, PUMA year, utility name mapping).

The public entry point is ``assign_utility()`` — called by the dynamic
dispatch in ``data.resstock.utility.assign_utility`` with kwargs from
``state_configs.yaml``.  The lower-level ``assign_utility_md()`` takes
pre-loaded GeoDataFrames and is used directly when GIS data is already
in memory.

Electric utility assignment
---------------------------
MD uses EIA Form 861 county-level service territory data (via PUDL) instead
of HIFLD utility polygons.  HIFLD is missing Pepco, Potomac Edison, and
Delmarva Power — three of the five major MD investor-owned utilities — because
those utilities never submitted their boundary shapes to the HIFLD portal.

The approach: Census county polygons are used as proxy utility boundaries.
For counties served by a single utility, the county polygon maps 1-to-1 to
that utility.  For counties served by multiple utilities (e.g. Montgomery
County has BGE, Pepco, and Potomac Edison all reporting in EIA-861), the
county's PUMA-overlap area is split proportionally using statewide residential
customer counts as a proxy for each utility's share of the county.

This gives sub-county precision where PUMAs cross county lines: a PUMA in
western Frederick County that also overlaps Washington County (Potomac Edison
only) accumulates overlap weight toward Potomac Edison, while a PUMA in
eastern Frederick that overlaps Howard County (BGE only) tilts toward BGE.

Gas utility assignment
----------------------
Gas utilities continue to use HIFLD-derived polygon CSVs from S3 (loaded
via ``read_csv_to_gdf_from_s3``), the same approach as NY and RI.

Service territory data
----------------------
Pre-computed county weights parquet produced by::

    just -f data/eia/861/Justfile fetch-service-territory MD
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import geopandas as gpd
import polars as pl
from cloudpathlib import S3Path

from data.eia.constants import service_territory_s3_path
from data.resstock.utils import (
    load_state_configs,
    select_puma_and_heating_fuel_metadata,
)
from data.resstock.utility.utils import (
    GIS_CACHE_DIR,
    S3_GIS_DIR,
    calculate_prior_distributions,
    calculate_puma_county_utility_overlap,
    calculate_puma_utility_overlap,
    calculate_utility_probabilities,
    fill_missing_puma_probabilities,
    load_county_boundaries,
    load_county_utility_weights,
    load_pumas,
    print_comparison_summary,
    read_csv_to_gdf_from_s3,
    sample_utility_per_building,
    zero_excluded_gas_utilities_and_renormalize,
)
from utils.utility_codes import get_eia_utility_id_to_std_name

# ── MD-specific constants ─────────────────────────────────────────────────────

_STATE = "MD"
_STATE_CONFIGS = load_state_configs()
_MD_CFG = _STATE_CONFIGS[_STATE]["utility_assignment"]["kwargs"]

MD_STATE_CRS: int = _MD_CFG["state_crs"]
MD_PUMA_YEAR: int = _MD_CFG["puma_year"]


# ── Pipeline entry point ──────────────────────────────────────────────────────


def assign_utility(
    metadata: pl.LazyFrame,
    *,
    state_crs: int,
    puma_year: int,
    gas_poly_filename: str,
    path_s3_gis_dir: str | None = None,
    excluded_gas_utilities: list[str] | None = None,
    puma_cache_dir: str | None = None,
) -> pl.LazyFrame:
    """Entry point for dynamic dispatch from ``assign_utility.py``.

    Electric utilities: loaded from EIA-861 county service territory
    (county polygons + per-county utility weights from S3).
    Gas utilities: loaded from HIFLD-derived polygon CSV on S3.

    Args:
        metadata: ResStock metadata LazyFrame.
        state_crs: EPSG code for MD projected CRS (2248 = NAD83 / Maryland
            State Plane feet).
        puma_year: Census TIGER/Line PUMA vintage year (2019 for 2010-def).
        gas_poly_filename: Filename of the gas utility polygon CSV in
            ``path_s3_gis_dir``.
        path_s3_gis_dir: S3 directory containing the gas polygon CSV.
            Defaults to ``paths.s3_gis_dir`` in ``config.yaml``.
        excluded_gas_utilities: Standardised gas utility names whose PUMA
            probabilities are zeroed before sampling (default: none).
        puma_cache_dir: Root local directory for the PUMA shapefile cache.
            Defaults to ``paths.gis_cache_dir`` in ``config.yaml``.
    """
    gis_base = S3Path((path_s3_gis_dir or S3_GIS_DIR).rstrip("/"))
    elec_st_path = service_territory_s3_path(_STATE)
    eia_id_map = get_eia_utility_id_to_std_name(_STATE)

    # ── Electric: county-based approach ───────────────────────────────────────
    print("    Loading MD county service territory weights from S3 ...", flush=True)
    county_utility_weights = load_county_utility_weights(elec_st_path, eia_id_map)

    print("    Loading MD Census county shapefiles ...", flush=True)
    county_gdf = load_county_boundaries(
        state=_STATE, year=puma_year, state_crs=state_crs
    )

    # ── Gas: HIFLD-derived polygon CSV ────────────────────────────────────────
    gas_polygons = read_csv_to_gdf_from_s3(
        gis_base / gas_poly_filename,
        utility_type="gas",
        state_crs=state_crs,
    )

    # ── PUMA shapefiles ───────────────────────────────────────────────────────
    print("    Loading MD Census PUMA shapefiles ...", flush=True)
    pumas = load_pumas(
        state=_STATE,
        puma_year=puma_year,
        cache_dir=Path(puma_cache_dir or GIS_CACHE_DIR),
    )
    pumas = pumas.to_crs(epsg=state_crs)

    return assign_utility_md(
        input_metadata=metadata,
        county_gdf=county_gdf,
        county_utility_weights=county_utility_weights,
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
    county_gdf: gpd.GeoDataFrame,
    county_utility_weights: pl.DataFrame,
    gas_polygons: gpd.GeoDataFrame,
    pumas: gpd.GeoDataFrame,
    state_crs: int,
    excluded_gas_utilities: frozenset[str] = frozenset(),
) -> pl.LazyFrame:
    """Assign electric and gas utilities to ResStock buildings in MD.

    Electric utilities are assigned via PUMA-county area overlap weighted by
    per-county utility shares (from EIA-861 service territory data).
    Gas utilities are assigned via PUMA-polygon overlap on HIFLD shapes.

    Args:
        input_metadata: ResStock metadata LazyFrame.
        county_gdf: Census county GeoDataFrame for MD (with GEOID column).
        county_utility_weights: DataFrame with county_id_fips, utility,
            weight columns (weights sum to 1.0 per county).
        gas_polygons: GeoDataFrame of MD gas utility territories (HIFLD).
        pumas: GeoDataFrame of MD Census PUMAs projected to ``state_crs``.
        state_crs: EPSG code for the MD projected CRS.
        excluded_gas_utilities: Gas utility names whose PUMA probabilities
            are zeroed before sampling (default: empty).

    Returns:
        LazyFrame with all original metadata columns plus
        ``sb.electric_utility`` and ``sb.gas_utility``.
    """
    puma_and_heating_fuel = select_puma_and_heating_fuel_metadata(input_metadata)

    # Electric: county-weighted PUMA overlap.
    puma_elec_overlap = calculate_puma_county_utility_overlap(
        pumas=pumas,
        county_gdf=county_gdf,
        county_utility_weights=county_utility_weights,
        state_crs=state_crs,
    )

    # Gas: standard HIFLD polygon overlap.
    utility_name_map = pl.DataFrame(
        {
            "state_name": pl.Series([], dtype=pl.Utf8),
            "std_name": pl.Series([], dtype=pl.Utf8),
        }
    ).lazy()

    puma_gas_overlap = calculate_puma_utility_overlap(pumas, gas_polygons, state_crs)

    puma_elec_probs = calculate_utility_probabilities(
        puma_elec_overlap,
        utility_name_map,
        handle_municipal=False,
        filter_none=True,
    )

    puma_gas_probs = calculate_utility_probabilities(
        puma_gas_overlap,
        utility_name_map,
        handle_municipal=False,
        filter_none=False,
    )

    puma_elec_probs = fill_missing_puma_probabilities(
        puma_elec_probs, pumas, label="electric"
    )
    puma_gas_probs = fill_missing_puma_probabilities(puma_gas_probs, pumas, label="gas")

    if excluded_gas_utilities:
        puma_gas_probs = zero_excluded_gas_utilities_and_renormalize(
            puma_gas_probs,
            excluded_utilities=excluded_gas_utilities,
            pumas=pumas,
            puma_and_heating_fuel=puma_and_heating_fuel,
        )

    building_elec = sample_utility_per_building(
        puma_and_heating_fuel, puma_elec_probs, "sb.electric_utility"
    )

    building_gas = sample_utility_per_building(
        puma_and_heating_fuel,
        puma_gas_probs,
        "sb.gas_utility",
    )

    elec_prior_weighted, gas_prior_weighted = calculate_prior_distributions(
        puma_elec_probs, puma_gas_probs, puma_and_heating_fuel=puma_and_heating_fuel
    )

    print_comparison_summary(
        building_elec,
        building_gas,
        elec_prior_weighted,
        gas_prior_weighted,
        puma_and_heating_fuel=puma_and_heating_fuel,
    )

    building_utilities = building_elec.join(
        building_gas.select(["bldg_id", "sb.gas_utility"]),
        on="bldg_id",
        how="left",
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

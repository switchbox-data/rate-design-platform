"""Utility assignment for ResStock buildings (MD).

Thin wrapper around the state-generic helpers in
``data.resstock.utility.utils``, providing MD-specific configuration
(CRS, PUMA year, excluded utilities).

The public entry point is ``assign_utility()`` — called by the dynamic
dispatch in ``data.resstock.utility.assign_utility`` with kwargs from
``state_configs.yaml``.  The lower-level ``assign_utility_md()`` takes
pre-loaded GeoDataFrames and is used directly when GIS data is already
in memory.

Electric and gas utility assignment
------------------------------------
Both electric and gas utilities are assigned from the full national HIFLD
shapefiles (Electric Retail Service Territories and Natural Gas LDC Service
Territories).  The national data is downloaded once, cached locally and on
S3, and then filtered to the valid utilities for MD (defined in
``utils/utility_codes.py``).

This avoids the STATE attribute filter problem where multi-state utilities
(Pepco filed under DC, Potomac Edison under PA, Delmarva under DE) were
silently dropped when querying ``WHERE STATE='MD'``.

The filtered polygons are spatially intersected with Census PUMAs to compute
per-PUMA utility probability distributions, which are then used to sample a
utility assignment for each ResStock building.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import geopandas as gpd
import polars as pl

from data.resstock.utils import (
    load_state_configs,
    select_puma_and_heating_fuel_metadata,
)
from data.resstock.utility.utils import (
    GIS_CACHE_DIR,
    calculate_prior_distributions,
    calculate_puma_utility_overlap,
    calculate_utility_probabilities,
    fill_missing_puma_probabilities,
    filter_hifld_for_state,
    load_national_hifld,
    load_pumas,
    print_comparison_summary,
    sample_utility_per_building,
    zero_excluded_utilities_and_renormalize,
)

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
    excluded_gas_utilities: list[str] | None = None,
    excluded_electric_utilities: list[str] | None = None,
    puma_cache_dir: str | None = None,
    hifld_cache_dir: str | None = None,
    **_kwargs: object,
) -> pl.LazyFrame:
    """Entry point for dynamic dispatch from ``assign_utility.py``.

    Loads national HIFLD polygons, filters to valid MD utilities, loads
    Census PUMAs, and delegates to :func:`assign_utility_md`.

    Args:
        metadata: ResStock metadata LazyFrame.
        state_crs: EPSG code for MD projected CRS (2248 = NAD83 / Maryland
            State Plane feet).
        puma_year: Census TIGER/Line PUMA vintage year (2019 for 2010-def).
        excluded_gas_utilities: Standardised gas utility names whose PUMA
            probabilities are zeroed before sampling (default: none).
        excluded_electric_utilities: Standardised electric utility names
            whose PUMA probabilities are zeroed before sampling (default: none).
        puma_cache_dir: Root local directory for the PUMA shapefile cache.
            Defaults to ``paths.gis_cache_dir`` in ``config.yaml``.
        hifld_cache_dir: Root local directory for national HIFLD parquets.
            Defaults to ``paths.gis_cache_dir`` in ``config.yaml``.
    """
    cache_dir = Path(hifld_cache_dir or GIS_CACHE_DIR)

    # ── Load national HIFLD and filter to valid MD utilities ───────────────
    print("    Loading national HIFLD electric territories ...", flush=True)
    elec_national = load_national_hifld("electric", cache_dir)
    elec_md = filter_hifld_for_state(elec_national, _STATE, "electric")
    elec_md = elec_md.to_crs(epsg=state_crs)

    print("    Loading national HIFLD gas territories ...", flush=True)
    gas_national = load_national_hifld("gas", cache_dir)
    gas_md = filter_hifld_for_state(gas_national, _STATE, "gas")
    gas_md = gas_md.to_crs(epsg=state_crs)

    # ── PUMA shapefiles ───────────────────────────────────────────────────
    print("    Loading MD Census PUMA shapefiles ...", flush=True)
    pumas = load_pumas(
        state=_STATE,
        puma_year=puma_year,
        cache_dir=Path(puma_cache_dir or GIS_CACHE_DIR),
    )
    pumas = pumas.to_crs(epsg=state_crs)

    return assign_utility_md(
        input_metadata=metadata,
        electric_polygons=elec_md,
        gas_polygons=gas_md,
        pumas=pumas,
        state_crs=state_crs,
        excluded_gas_utilities=frozenset(excluded_gas_utilities)
        if excluded_gas_utilities is not None
        else frozenset(),
        excluded_electric_utilities=frozenset(excluded_electric_utilities)
        if excluded_electric_utilities is not None
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
    excluded_electric_utilities: frozenset[str] = frozenset(),
) -> pl.LazyFrame:
    """Assign electric and gas utilities to ResStock buildings in MD.

    Both electric and gas utilities are assigned via PUMA-polygon area
    overlap on HIFLD service territory shapes.

    Args:
        input_metadata: ResStock metadata LazyFrame.
        electric_polygons: GeoDataFrame of MD electric utility territories
            (filtered from national HIFLD, with ``utility`` column containing
            std_names).
        gas_polygons: GeoDataFrame of MD gas utility territories (filtered
            from national HIFLD, with ``utility`` column containing
            std_names).
        pumas: GeoDataFrame of MD Census PUMAs projected to ``state_crs``.
        state_crs: EPSG code for the MD projected CRS.
        excluded_gas_utilities: Gas utility names whose PUMA probabilities
            are zeroed before sampling (default: empty).
        excluded_electric_utilities: Electric utility names whose PUMA
            probabilities are zeroed before sampling (default: empty).

    Returns:
        LazyFrame with all original metadata columns plus
        ``sb.electric_utility`` and ``sb.gas_utility``.
    """
    puma_and_heating_fuel = select_puma_and_heating_fuel_metadata(input_metadata)

    # Utility name map — HIFLD names are already mapped to std_names by
    # filter_hifld_for_state, so we pass an empty map.
    utility_name_map = pl.DataFrame(
        {
            "state_name": pl.Series([], dtype=pl.Utf8),
            "std_name": pl.Series([], dtype=pl.Utf8),
        }
    ).lazy()

    puma_elec_overlap = calculate_puma_utility_overlap(
        pumas, electric_polygons, state_crs
    )
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

    if excluded_electric_utilities:
        puma_elec_probs = zero_excluded_utilities_and_renormalize(
            puma_elec_probs,
            excluded_utilities=excluded_electric_utilities,
            pumas=pumas,
            puma_and_heating_fuel=puma_and_heating_fuel,
            label="electric",
        )

    if excluded_gas_utilities:
        puma_gas_probs = zero_excluded_utilities_and_renormalize(
            puma_gas_probs,
            excluded_utilities=excluded_gas_utilities,
            pumas=pumas,
            puma_and_heating_fuel=puma_and_heating_fuel,
            label="gas",
        )

    building_elec = sample_utility_per_building(
        puma_and_heating_fuel, puma_elec_probs, "sb.electric_utility"
    )
    building_gas = sample_utility_per_building(
        puma_and_heating_fuel,
        puma_gas_probs,
        "sb.gas_utility",
        only_when_fuel="Natural Gas",
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

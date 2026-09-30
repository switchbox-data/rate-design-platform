"""Utility assignment for ResStock buildings (CT).

Thin wrapper around the state-generic helpers in
``data.resstock.utility.utils``, providing CT-specific configuration
(CRS, PUMA year).

The public entry point is ``assign_utility()`` — called by the dynamic
dispatch in ``data.resstock.utility.assign_utility`` with kwargs from
``state_configs.yaml``.  The lower-level ``assign_utility_ct()`` takes
pre-loaded GeoDataFrames and is used directly when GIS data is already
in memory.

Electric and gas utility assignment
------------------------------------
Both electric and gas utilities are assigned from the full national HIFLD
shapefiles (Electric Retail Service Territories and Natural Gas LDC Service
Territories).  The national data is downloaded once, cached locally and on
S3, and then filtered to the valid utilities for CT (defined in
``utils/utility_codes.py``).

This avoids the STATE attribute filter problem where multi-state utilities
may be filed under adjacent states in the HIFLD data.

The filtered polygons are spatially intersected with Census PUMAs to compute
per-PUMA utility probability distributions, which are then used to sample a
utility assignment for each ResStock building.

Electric probabilities are each utility's share of the PUMA's land area. Gas
probabilities are each utility's share of the PUMA's homes heated with utility
gas (ACS 2016-2020 table B25040 by block group, spread to 2020 census blocks
by housing units): every block is placed, by its internal point, in a PUMA and
a gas territory. The same gas probabilities are used for every building with a
gas connection, including ones that do not heat with gas, since gas-heated
homes mark where the mains are. Land area and total housing units both
over-assign Yankee Gas, whose territory covers much of the state but has a low
share of homes on gas.

CT electric utilities (IOUs + municipals):
  - Eversource Energy CT (std_name ``ct_eversource``; HIFLD still
    ``CONNECTICUT LIGHT & POWER CO``)
  - United Illuminating / Avangrid (std_name ``ct_ui``; HIFLD
    ``UNITED ILLUMINATING CO``)
  - Farmington River Power Company
  - Bozrah Light & Power, City of Jewett City, City of Norwich,
    City of South Norwalk, Groton Dept of Utilities,
    Mohegan Tribal Utility Authority, Norwalk Third Taxing District,
    Town of Wallingford

CT gas utilities:
  - Connecticut Natural Gas Corp (Eversource, typo in HIFLD: "CONNETICUT")
  - Yankee Gas Service Co. (Eversource)
  - Southern Connecticut Gas (Avangrid)
  - Norwich Public Utilities (municipal)
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import geopandas as gpd
import polars as pl

from data.resstock.utility.utils import (
    GIS_CACHE_DIR,
    add_block_gas_heated_homes,
    calculate_prior_distributions,
    calculate_puma_utility_housing_overlap,
    calculate_puma_utility_overlap,
    calculate_utility_probabilities,
    fetch_acs_gas_heated_homes_by_block_group,
    fill_missing_puma_probabilities,
    filter_hifld_for_state,
    load_census_blocks,
    load_national_hifld,
    load_pumas,
    print_comparison_summary,
    sample_utility_per_building,
    zero_excluded_gas_utilities_and_renormalize,
)
from data.resstock.utils import (
    load_state_configs,
    select_puma_and_heating_fuel_metadata,
)

# ── CT-specific constants ─────────────────────────────────────────────────────

_STATE = "CT"
_STATE_CONFIGS = load_state_configs()
_CT_CFG = _STATE_CONFIGS[_STATE]["utility_assignment"]["kwargs"]

CT_STATE_CRS: int = _CT_CFG["state_crs"]
CT_PUMA_YEAR: int = _CT_CFG["puma_year"]
CT_STATE_FIPS: str = _STATE_CONFIGS[_STATE]["state_fips"]


# ── Pipeline entry point ──────────────────────────────────────────────────────


def assign_utility(
    metadata: pl.LazyFrame,
    *,
    state_crs: int,
    puma_year: int,
    excluded_gas_utilities: list[str] | None = None,
    puma_cache_dir: str | None = None,
    hifld_cache_dir: str | None = None,
    **_kwargs: object,
) -> pl.LazyFrame:
    """Entry point for dynamic dispatch from ``assign_utility.py``.

    Loads national HIFLD polygons, filters to valid CT utilities, loads
    Census PUMAs, and delegates to :func:`assign_utility_ct`.

    Args:
        metadata: ResStock metadata LazyFrame.
        state_crs: EPSG code for CT projected CRS (2234 = NAD83 / Connecticut
            State Plane feet).
        puma_year: Census TIGER/Line PUMA vintage year (2019 for 2010-def).
        excluded_gas_utilities: Standardised gas utility names whose PUMA
            probabilities are zeroed before sampling (default: none).
        puma_cache_dir: Root local directory for the PUMA shapefile cache.
            Defaults to ``paths.gis_cache_dir`` in ``config.yaml``.
        hifld_cache_dir: Root local directory for national HIFLD parquets.
            Defaults to ``paths.gis_cache_dir`` in ``config.yaml``.
    """
    cache_dir = Path(hifld_cache_dir or GIS_CACHE_DIR)

    # ── Load national HIFLD and filter to valid CT utilities ───────────────
    print("    Loading national HIFLD electric territories ...", flush=True)
    elec_national = load_national_hifld("electric", cache_dir)
    elec_ct = filter_hifld_for_state(elec_national, _STATE, "electric")
    elec_ct = elec_ct.to_crs(epsg=state_crs)

    print("    Loading national HIFLD gas territories ...", flush=True)
    gas_national = load_national_hifld("gas", cache_dir)
    gas_ct = filter_hifld_for_state(gas_national, _STATE, "gas")
    gas_ct = gas_ct.to_crs(epsg=state_crs)

    # ── PUMA shapefiles ───────────────────────────────────────────────────
    print("    Loading CT Census PUMA shapefiles ...", flush=True)
    pumas = load_pumas(
        state=_STATE,
        puma_year=puma_year,
        cache_dir=Path(puma_cache_dir or GIS_CACHE_DIR),
    )
    pumas = pumas.to_crs(epsg=state_crs)

    print("    Loading CT 2020 Census blocks and ACS gas-heated homes ...", flush=True)
    blocks = add_block_gas_heated_homes(
        load_census_blocks(_STATE),
        fetch_acs_gas_heated_homes_by_block_group(CT_STATE_FIPS),
    )

    return assign_utility_ct(
        input_metadata=metadata,
        electric_polygons=elec_ct,
        gas_polygons=gas_ct,
        pumas=pumas,
        state_crs=state_crs,
        gas_blocks=blocks,
        excluded_gas_utilities=frozenset(excluded_gas_utilities)
        if excluded_gas_utilities is not None
        else frozenset(),
    )


# ── Core assignment logic ─────────────────────────────────────────────────────


def assign_utility_ct(
    input_metadata: pl.LazyFrame,
    electric_polygons: gpd.GeoDataFrame,
    gas_polygons: gpd.GeoDataFrame,
    pumas: gpd.GeoDataFrame,
    state_crs: int,
    excluded_gas_utilities: frozenset[str] = frozenset(),
    gas_blocks: gpd.GeoDataFrame | None = None,
) -> pl.LazyFrame:
    """Assign electric and gas utilities to ResStock buildings in CT.

    Electric utilities are assigned via PUMA-polygon area overlap on HIFLD
    service territory shapes. Gas utilities use gas-heated-home overlap when
    ``gas_blocks`` is given, and area overlap otherwise.

    Args:
        input_metadata: ResStock metadata LazyFrame.
        electric_polygons: GeoDataFrame of CT electric utility territories
            (filtered from national HIFLD, with ``utility`` column containing
            std_names).
        gas_polygons: GeoDataFrame of CT gas utility territories (filtered
            from national HIFLD, with ``utility`` column containing
            std_names).
        pumas: GeoDataFrame of CT Census PUMAs projected to ``state_crs``.
        state_crs: EPSG code for the CT projected CRS.
        excluded_gas_utilities: Gas utility names whose PUMA probabilities
            are zeroed before sampling (default: empty).
        gas_blocks: Census block internal points with ``gas_heated_homes``
            (from :func:`load_census_blocks` and
            :func:`add_block_gas_heated_homes`).

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
    if gas_blocks is not None:
        puma_gas_overlap = calculate_puma_utility_housing_overlap(
            pumas, gas_polygons, gas_blocks, state_crs, weight_col="gas_heated_homes"
        )
    else:
        puma_gas_overlap = calculate_puma_utility_overlap(
            pumas, gas_polygons, state_crs
        )

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

    counts_df = (
        input_metadata.select(pl.lit(1).sum().alias("input_count"))
        .join(
            building_utilities.select(pl.lit(1).sum().alias("building_count")),
            how="cross",
        )
        .collect()
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

"""Utility assignment orchestration for the ResStock pipeline.

Routes building-level utility assignment to state-specific implementations
and handles all state-specific data loading (GIS polygon CSVs, Census PUMAs).

Callers (e.g. ``data/resstock/main.py``) should import only ``assign_utility``
and ``SUPPORTED_UTILITY_STATES`` from here; state-specific functions are an
implementation detail of this module.
"""

from __future__ import annotations

from typing import cast

import geopandas as gpd
import polars as pl
from cloudpathlib import S3Path
from pygris import pumas as get_pumas

from data.resstock.utils import load_state_configs
from data.resstock.utility.assign_utility_ny import (
    assign_utility_ny,
    read_csv_to_gdf_from_s3,
)
from data.resstock.utility.assign_utility_ri import assign_utility_ri

# States that have a utility assignment implementation — derived from
# state_configs.yaml.  Any state whose config entry contains both
# ``electric_poly_filename`` and ``gas_poly_filename`` keys (even if the values
# are null) is considered supported.  Adding a new state to state_configs.yaml
# automatically includes it here.
_STATE_CONFIGS = load_state_configs()
SUPPORTED_UTILITY_STATES: frozenset[str] = frozenset(
    state
    for state, cfg in _STATE_CONFIGS.items()
    if "electric_poly_filename" in cfg and "gas_poly_filename" in cfg
)


def assign_utility(
    state: str,
    metadata: pl.LazyFrame,
    *,
    path_s3_gis_dir: str | None = None,
    electric_poly_filename: str | None = None,
    gas_poly_filename: str | None = None,
) -> pl.LazyFrame:
    """Assign electric and gas utilities to ResStock buildings for a given state.

    Looks up the state's configuration from ``state_configs.yaml`` internally.
    CLI values for ``electric_poly_filename`` / ``gas_poly_filename`` override
    the config defaults when provided.  When both filenames resolve to empty,
    uses a rule-based assignment; otherwise performs GIS-based assignment
    (loading polygon CSVs from S3 and Census PUMAs via pygris).

    Returns a LazyFrame with all original metadata columns plus
    ``sb.electric_utility`` and ``sb.gas_utility``.

    Args:
        state: 2-letter state code (e.g. ``"NY"``, ``"RI"``).
        metadata: ResStock metadata LazyFrame (from ``metadata-sb.parquet``).
        path_s3_gis_dir: S3 directory containing utility polygon CSV files.
            Required for GIS-based states.
        electric_poly_filename: CLI override for the electric polygon CSV
            filename.  Falls back to ``state_configs.yaml`` when not provided.
        gas_poly_filename: CLI override for the gas polygon CSV filename.
            Falls back to ``state_configs.yaml`` when not provided.

    Raises:
        ValueError: If ``state`` is not in ``SUPPORTED_UTILITY_STATES``, or if
            a required input for the state is not provided.
    """
    if state not in SUPPORTED_UTILITY_STATES:
        raise ValueError(
            f"Utility assignment is not implemented for state {state!r}. "
            f"Supported states: {sorted(SUPPORTED_UTILITY_STATES)}."
        )

    config = _STATE_CONFIGS[state]
    # CLI values override state_configs.yaml defaults.
    electric_poly_filename = electric_poly_filename or config.get(
        "electric_poly_filename"
    )
    gas_poly_filename = gas_poly_filename or config.get("gas_poly_filename")

    needs_gis = bool(electric_poly_filename or gas_poly_filename)

    if not needs_gis:
        # Rule-based assignment (e.g. RI): no GIS polygon data required.
        if state == "RI":
            return assign_utility_ri(metadata)
        raise ValueError(
            f"State {state!r} has no polygon filenames configured in "
            f"state_configs.yaml and no GIS-free assignment implementation."
        )

    # GIS-based assignment (e.g. NY): requires both polygon files and a
    # projected CRS.
    if not path_s3_gis_dir:
        raise ValueError(
            f"--path-s3-gis-dir is required for {state} utility assignment."
        )
    if not electric_poly_filename:
        raise ValueError(
            f"--electric-poly-filename (or state_configs.yaml "
            f"electric_poly_filename) is required for {state} utility assignment."
        )
    if not gas_poly_filename:
        raise ValueError(
            f"--gas-poly-filename (or state_configs.yaml "
            f"gas_poly_filename) is required for {state} utility assignment."
        )

    gis_base = S3Path(path_s3_gis_dir.rstrip("/"))
    electric_polygons = read_csv_to_gdf_from_s3(
        gis_base / electric_poly_filename,
        utility_type="electric",
        state_crs=config["state_crs"],
    )
    gas_polygons = read_csv_to_gdf_from_s3(
        gis_base / gas_poly_filename,
        utility_type="gas",
        state_crs=config["state_crs"],
    )
    print("    Loading Census PUMA shapefiles via pygris...", flush=True)
    pumas = cast(
        gpd.GeoDataFrame,
        get_pumas(state=state, year=config["puma_year"], cb=True),
    )
    pumas = pumas.to_crs(epsg=config["state_crs"])

    if state == "NY":
        return assign_utility_ny(
            input_metadata=metadata,
            electric_polygons=electric_polygons,
            gas_polygons=gas_polygons,
            pumas=pumas,
            config=config,
        )

    raise ValueError(
        f"State {state!r} has polygon filenames configured but no "
        f"GIS-based assignment implementation."
    )


__all__ = [
    "SUPPORTED_UTILITY_STATES",
    "assign_utility",
]

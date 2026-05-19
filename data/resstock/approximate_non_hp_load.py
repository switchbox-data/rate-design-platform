"""Re-export non-HP load approximation utilities from ``utils.pre``.

This thin wrapper keeps ``data.resstock.main`` imports within the
``data.resstock`` package.  All logic lives in
``utils.pre.approximate_non_hp_load``; this module just surfaces the
symbols that the pipeline orchestrator needs.
"""

from utils.pre.approximate_non_hp_load import (
    STORAGE_OPTIONS,
    _find_nearest_neighbors,
    _identify_non_hp_mf,
    _identify_other_fuel_types,
    update_load_curve_hourly,
    update_metadata,
)

__all__ = [
    "STORAGE_OPTIONS",
    "_find_nearest_neighbors",
    "_identify_non_hp_mf",
    "_identify_other_fuel_types",
    "update_load_curve_hourly",
    "update_metadata",
]

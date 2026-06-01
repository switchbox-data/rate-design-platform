"""Re-export utility assignment functions from state-specific modules.

Import from here inside main.py instead of directly from the state modules
so the pattern mirrors load_curve/adjust_mf_electricity.py and load_curve/approximate_non_hp_load.py.
"""

from data.resstock.utility.assign_utility_ny import (
    assign_utility_ny,
    read_csv_to_gdf_from_s3,
)
from data.resstock.utility.assign_utility_ri import assign_utility_ri

# States that have a utility assignment implementation.
SUPPORTED_UTILITY_STATES: frozenset[str] = frozenset({"NY", "RI"})

__all__ = [
    "SUPPORTED_UTILITY_STATES",
    "assign_utility_ny",
    "assign_utility_ri",
    "read_csv_to_gdf_from_s3",
]

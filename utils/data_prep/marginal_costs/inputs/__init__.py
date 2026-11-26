"""Registry of config ``from:`` loaders.

Each loader turns one config entry into the value a method function takes.
None of them allocates.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from utils.data_prep.marginal_costs.inputs.context import InputContext, Resolved
from utils.data_prep.marginal_costs.inputs.generic import cpi, hive_load, literal, mc_table, parquet, periods_yaml
from utils.data_prep.marginal_costs.inputs.isone import isone_zone_load
from utils.data_prep.marginal_costs.inputs.nyiso import nyiso_scr_components
from utils.data_prep.marginal_costs.inputs.pjm import pjm_hive_load, pjm_nits

Loader = Callable[[Mapping[str, Any], InputContext, str], Resolved]

INPUT_LOADERS: dict[str, Loader] = {
    "literal": literal,
    "mc_table": mc_table,
    "cpi": cpi,
    "parquet": parquet,
    "hive_load": hive_load,
    "periods_yaml": periods_yaml,
    "isone_zone_load": isone_zone_load,
    "nyiso_scr_components": nyiso_scr_components,
    "pjm_nits": pjm_nits,
    "pjm_hive_load": pjm_hive_load,
}


def resolve_input(name: str, entry: Mapping[str, Any], ctx: InputContext, *, role: str) -> Resolved:
    """Call the loader named ``name``. An unknown name names the known loaders."""
    loader = INPUT_LOADERS.get(name)
    if loader is None:
        known = ", ".join(sorted(INPUT_LOADERS))
        raise ValueError(f"Unknown input loader {name!r}. Known loaders: {known}")
    return loader(entry, ctx, role)


__all__ = ["INPUT_LOADERS", "InputContext", "Resolved", "resolve_input"]

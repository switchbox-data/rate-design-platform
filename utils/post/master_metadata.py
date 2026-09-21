"""Building metadata for the master tables.

Attributes are always read from the **baseline** ResStock upgrade, on every
segment.  ResStock marks every building in the heat-pump upgrade as
``has_hp = true`` with ``heating_type = heat_pump``, so taking a calibrated
segment's own upgrade would erase what the home heated with before the
retrofit — the dimension most analyses slice on.  The ``upgrade`` column in the
master tables identifies the stage instead.

``utility_assignment.parquet`` carries only the utility mapping in newer
ResStock releases (older ones repeat the full upgrade-0 metadata on it), so the
attributes come from the baseline upgrade's ``metadata-sb.parquet`` for every
state.
"""

from __future__ import annotations

import polars as pl

from utils.post.io import BLDG_ID

METADATA_UPGRADE = "00"

UTILITY_COLS = ["sb.electric_utility", "sb.gas_utility"]

ATTR_COLS = [
    "postprocess_group.has_hp",
    "postprocess_group.heating_type",
    "heats_with_electricity",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
    "in.representative_income",
    "in.hvac_cooling_partial_space_conditioning",
]


def load_metadata(path_resstock_base: str, state_upper: str) -> pl.DataFrame:
    """Utility assignment joined to baseline building attributes, one row per building."""
    base = path_resstock_base.rstrip("/")
    assignment = pl.scan_parquet(
        f"{base}/metadata_utility/state={state_upper}/utility_assignment.parquet"
    ).select(BLDG_ID, *UTILITY_COLS)
    attributes = pl.scan_parquet(
        f"{base}/metadata/state={state_upper}/upgrade={METADATA_UPGRADE}/metadata-sb.parquet"
    ).select(BLDG_ID, *ATTR_COLS)
    return assignment.join(attributes, on=BLDG_ID, how="inner").collect()


def heating_type_v2() -> pl.Expr:
    """Coarser heating classification derived from HP status and fuel flags."""
    return (
        pl.when(pl.col("postprocess_group.has_hp"))
        .then(pl.lit("heat_pump"))
        .when(pl.col("heats_with_electricity"))
        .then(pl.lit("electrical_resistance"))
        .when(pl.col("heats_with_natgas"))
        .then(pl.lit("natgas"))
        .when(pl.col("heats_with_oil") | pl.col("heats_with_propane"))
        .then(pl.lit("delivered_fuels"))
        .otherwise(pl.lit("other"))
        .alias("postprocess_group.heating_type_v2")
    )

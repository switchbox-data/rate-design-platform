"""NYISO bulk-transmission inputs.

``nyiso_scr_components`` returns one ``{load, annual_cost_kw_year}`` per utility
ICAP row. The runner calls seasonal SCR exceedance once per row and sums the
cost column. This module does not allocate.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl

from utils.data_prep.marginal_costs.inputs.context import InputContext, Resolved, resolve_path
from utils.data_prep.marginal_costs.mc_manifest import input_file, input_partition
from utils.data_prep.marginal_costs.supply_utils import (
    load_zone_loads,
    load_zone_mapping,
    remap_year_if_needed,
)
from utils.file_io import read_csv_local_or_s3
from utils.pre.season_config import DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS

DEFAULT_SCR_WINTER_MONTHS: tuple[int, ...] = DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS

NESTED_LOCALITY_ZONES: dict[str, list[str]] = {
    "NYCA": [
        "WEST",
        "GENESE",
        "CENTRAL",
        "NORTH",
        "MHK_VL",
        "CAPITL",
        "HUD_VL",
        "MILLWD",
        "DUNWOD",
        "N.Y.C.",
        "LONGIL",
    ],
    "LHV": ["HUD_VL", "MILLWD", "DUNWOD", "N.Y.C."],
    "NYC": ["N.Y.C."],
    "LI": ["LONGIL"],
}
VALID_NESTED_LOCALITIES = frozenset(NESTED_LOCALITY_ZONES)
VALID_PAYING_LOCALITIES = frozenset({"ROS", "LHV", "NYC", "LI"})
ICAP_RAW_TO_NESTED_LOCALITY = {
    "NYCA": "NYCA",
    "GHIJ": "LHV",
    "NYC": "NYC",
    "LI": "LI",
}


def load_constraint_group_table(path: str) -> pl.DataFrame:
    """Load ``ny_bulk_tx_constraint_groups.csv`` from local or S3."""
    df = read_csv_local_or_s3(path)
    required = {
        "nested_localities_str",
        "constraint_group",
        "v_constraint_group_kw_yr",
        "tightest_nested_locality",
        "paying_localities",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Constraint-group table missing columns: {sorted(missing)}")

    bad_nested = sorted(set(df["tightest_nested_locality"].cast(pl.String).to_list()) - set(VALID_NESTED_LOCALITIES))
    if bad_nested:
        raise ValueError(
            f"Invalid tightest_nested_locality values: {bad_nested}. Expected {sorted(VALID_NESTED_LOCALITIES)}"
        )

    df = df.with_columns(
        pl.col("constraint_group").cast(pl.String),
        pl.col("nested_localities_str").cast(pl.String),
        pl.col("tightest_nested_locality").cast(pl.String),
        pl.col("paying_localities").cast(pl.String),
        pl.col("v_constraint_group_kw_yr").cast(pl.Float64),
    )
    print(f"Loaded constraint-group table with {len(df)} rows from {path}")
    return df


def compute_paying_locality_costs(constraint_group_df: pl.DataFrame) -> dict[str, float]:
    """Mean annual cost per paying locality from constraint groups."""
    exploded = (
        constraint_group_df.with_columns(pl.col("paying_localities").str.split("|").alias("paying_locality_list"))
        .explode("paying_locality_list", empty_as_null=True)
        .rename({"paying_locality_list": "paying_locality"})
    )
    invalid = sorted(set(exploded["paying_locality"].drop_nulls().to_list()) - set(VALID_PAYING_LOCALITIES))
    if invalid:
        raise ValueError(
            f"Invalid paying_locality values in constraint groups: {invalid}. "
            f"Expected {sorted(VALID_PAYING_LOCALITIES)}"
        )
    locality_costs = (
        exploded.group_by("paying_locality")
        .agg(pl.col("v_constraint_group_kw_yr").mean().alias("mean_cost_kw_yr"))
        .sort("paying_locality")
    )
    return {str(row["paying_locality"]): float(row["mean_cost_kw_yr"]) for row in locality_costs.iter_rows(named=True)}


def build_nested_locality_load_profiles(
    zone_loads_df: pl.DataFrame,
    nested_localities: list[str],
) -> dict[str, pl.DataFrame]:
    """Sum zone load into one profile per nested locality."""
    profiles: dict[str, pl.DataFrame] = {}
    for locality in nested_localities:
        zone_names = NESTED_LOCALITY_ZONES[locality]
        profile = (
            zone_loads_df.filter(pl.col("zone").is_in(zone_names))
            .group_by("timestamp")
            .agg(pl.col("load_mw").sum().alias("load_mw"))
            .sort("timestamp")
        )
        if profile.is_empty():
            raise ValueError(f"No zone loads found for nested locality {locality} (zones={zone_names})")
        profiles[locality] = profile
    return profiles


def icap_components(
    utility_icap_rows: pl.DataFrame,
    paying_locality_costs: dict[str, float],
    locality_profiles: dict[str, pl.DataFrame],
) -> list[dict[str, Any]]:
    """One ``{load, annual_cost_kw_year}`` per utility ICAP row.

    The annual cost is the paying locality's ``$/kW-yr`` times ``capacity_weight``.
    The load is that row's nested-locality profile.
    """
    components: list[dict[str, Any]] = []
    for row in utility_icap_rows.iter_rows(named=True):
        icap_locality_raw = str(row["icap_locality"])
        gen_capacity_zone = str(row["gen_capacity_zone"])
        capacity_weight = float(row["capacity_weight"])
        if icap_locality_raw not in ICAP_RAW_TO_NESTED_LOCALITY:
            raise ValueError(
                f"Unknown icap_locality {icap_locality_raw!r}. Expected one of {sorted(ICAP_RAW_TO_NESTED_LOCALITY)}."
            )
        nested_locality = ICAP_RAW_TO_NESTED_LOCALITY[icap_locality_raw]
        if gen_capacity_zone not in paying_locality_costs:
            raise ValueError(
                f"No cost available for paying locality {gen_capacity_zone!r}. "
                f"Available: {sorted(paying_locality_costs)}"
            )
        if nested_locality not in locality_profiles:
            raise ValueError(
                f"No load profile for nested locality {nested_locality!r}. Available: {sorted(locality_profiles)}"
            )
        components.append(
            {
                "load": locality_profiles[nested_locality],
                "annual_cost_kw_year": paying_locality_costs[gen_capacity_zone] * capacity_weight,
            }
        )
    if not components:
        raise ValueError("No ICAP locality components found for utility")
    return components


def nyiso_scr_components(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """Constraint-group costs and nested-locality loads for one utility."""
    del role
    constraint_path = resolve_path(str(entry["constraint_groups"]))
    mapping_path = resolve_path(str(entry["zone_mapping"]))
    zone_loads_base = str(entry["zone_loads"])
    load_year = int(entry.get("load_year", ctx.load_year))

    mapping_df = load_zone_mapping(mapping_path, ctx.storage_options)
    constraint_group_df = load_constraint_group_table(constraint_path)
    paying_locality_costs = compute_paying_locality_costs(constraint_group_df)
    utility_icap_rows = (
        mapping_df.filter(pl.col("utility") == ctx.utility)
        .select("icap_locality", "gen_capacity_zone", "capacity_weight")
        .unique()
    )
    if utility_icap_rows.is_empty():
        available = sorted(mapping_df["utility"].unique().to_list())
        raise ValueError(f"Utility {ctx.utility!r} not found in zone mapping. Available: {available}")

    nested_localities = sorted(
        {ICAP_RAW_TO_NESTED_LOCALITY[str(locality)] for locality in utility_icap_rows["icap_locality"].to_list()}
    )
    zone_names = sorted({zone for locality in nested_localities for zone in NESTED_LOCALITY_ZONES[str(locality)]})
    zone_loads_df = load_zone_loads(zone_loads_base, zone_names, load_year, ctx.storage_options)
    profiles = build_nested_locality_load_profiles(zone_loads_df, [str(name) for name in nested_localities])
    components = icap_components(utility_icap_rows, paying_locality_costs, profiles)
    if load_year != ctx.year:
        for component in components:
            component["load"] = remap_year_if_needed(component["load"], "timestamp", load_year, ctx.year)

    zone_root = zone_loads_base.rstrip("/")
    inputs = [
        input_file("constraint_groups", constraint_path),
        input_file("zone_mapping", mapping_path),
        *[input_partition(f"load_zone_{zone}", f"{zone_root}/zone={zone}/year={load_year}") for zone in zone_names],
    ]
    return Resolved(
        components,
        inputs,
        {"nested_localities": [str(name) for name in nested_localities], "load_year": load_year, "zones": zone_names},
    )

"""NYISO bulk transmission marginal cost logic using constraint-group SCR allocation.

Methodology
-----------
Derives scalar costs for paying localities (ROS/LHV/NYC/LI) from NYISO
constraint groups, then allocates each utility's weighted locality costs onto
SCR hours of that utility's mapped ICAP localities.

The hourly formula is seasonal SCR exceedance
(``methods/seasonal_scr_exceedance.py``). This module chooses the ``$/kW-yr``
and the nested-locality load, calls that function once per utility ICAP row,
and sums the hourly costs. See
``context/methods/marginal_costs/seasonal_scr_exceedance_allocation.md``.

For ISO-NE bulk transmission, see ``bulk_tx_isone.py``.
For the CLI entrypoint, see ``generate_bulk_tx_mc.py``.
"""

from __future__ import annotations

import io

import polars as pl
from cloudpathlib import S3Path

from utils.data_prep.marginal_costs.methods.seasonal_scr_exceedance import (
    seasonal_scr_exceedance,
)
from utils.data_prep.marginal_costs.supply_utils import (
    build_cairo_8760_timestamps,
)
from utils.pre.season_config import DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS

# ── NYISO constants ───────────────────────────────────────────────────────────

DEFAULT_NYISO_BULK_TX_CONSTRAINT_GROUP_TABLE_PATH = "s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_constraint_groups.csv"

N_SCR_HOURS_PER_SEASON = 40
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


# ── Constraint-group table ───────────────────────────────────────────────────


def load_constraint_group_table(path: str) -> pl.DataFrame:
    """Load ny_bulk_tx_constraint_groups.csv from local or S3."""
    if path.startswith("s3://"):
        csv_bytes = S3Path(path).read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(path)

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


# ── Constraint-group engine ──────────────────────────────────────────────────


def compute_paying_locality_costs(
    constraint_group_df: pl.DataFrame,
) -> dict[str, float]:
    """Compute mean annual cost per paying locality from constraint groups."""
    exploded = (
        constraint_group_df.with_columns(pl.col("paying_localities").str.split("|").alias("paying_locality_list"))
        .explode("paying_locality_list")
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

    result: dict[str, float] = {}
    for row in locality_costs.iter_rows(named=True):
        result[str(row["paying_locality"])] = float(row["mean_cost_kw_yr"])

    return result


def build_nested_locality_load_profiles(
    zone_loads_df: pl.DataFrame,
    nested_localities: list[str],
) -> dict[str, pl.DataFrame]:
    """Build load profile per nested locality from NYISO zone loads."""
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


def compute_utility_bulk_tx_signal(
    utility_icap_rows: pl.DataFrame,
    paying_locality_costs: dict[str, float],
    locality_profiles: dict[str, pl.DataFrame],
    n_scr: int,
    winter_months: list[int],
) -> pl.DataFrame:
    """Compute utility bulk TX hourly signal from utility ICAP locality rows.

    Each row is one call to :func:`seasonal_scr_exceedance`: the paying
    locality's ``$/kW-yr`` times ``capacity_weight``, on that row's
    nested-locality load. The hourly costs are summed.
    """
    component_frames: list[pl.DataFrame] = []

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

        component_value = paying_locality_costs[gen_capacity_zone] * capacity_weight
        load_profile = locality_profiles[nested_locality]
        component_hourly = seasonal_scr_exceedance(
            load_profile,
            component_value,
            n_scr,
            winter_months,
            output_column="bulk_tx_cost_enduse",
        )
        component_frames.append(component_hourly)

    if not component_frames:
        raise ValueError("No ICAP locality components found for utility")

    return (
        pl.concat(component_frames)
        .group_by("timestamp")
        .agg(pl.col("bulk_tx_cost_enduse").sum().alias("bulk_tx_cost_enduse"))
        .sort("timestamp")
    )


# ── Output assembly ──────────────────────────────────────────────────────────


def prepare_output(allocated_df: pl.DataFrame, year: int) -> pl.DataFrame:
    """Prepare output on CAIRO 8760 timestamp index."""
    ref_8760 = build_cairo_8760_timestamps(year)

    hourly = allocated_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("timestamp"))
    hourly = hourly.group_by("timestamp").agg(pl.col("bulk_tx_cost_enduse").mean()).sort("timestamp")

    output = ref_8760.join(hourly, on="timestamp", how="left").with_columns(
        pl.col("bulk_tx_cost_enduse").fill_null(0.0).alias("bulk_tx_cost_enduse")
    )

    if output.height != 8760:
        raise ValueError(f"Output has {output.height} rows, expected 8760")
    if output.filter(pl.col("bulk_tx_cost_enduse").is_null()).height > 0:
        raise ValueError("Output has null bulk_tx_cost_enduse values")

    return output.select("timestamp", "bulk_tx_cost_enduse")


def save_output(
    output_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write bulk Tx MC parquet to S3 (hive-style utility/year partition path)."""
    output_s3_base = output_s3_base.rstrip("/") + "/"
    output_path = f"{output_s3_base}utility={utility}/year={year}/data.parquet"
    output_df.write_parquet(output_path, storage_options=storage_options)

    print(f"\n✓ Saved bulk Tx MC to {output_path}")
    print(f"  Rows: {len(output_df):,}")


def print_summary(output_df: pl.DataFrame) -> None:
    """Print top-10 hours and summary statistics for a bulk TX output DataFrame."""
    from typing import cast as _cast

    print("\n" + "=" * 60)
    print("SAMPLE: Top 10 hours by bulk Tx cost")
    print("=" * 60)
    print(output_df.sort("bulk_tx_cost_enduse", descending=True).head(10))

    avg_cost = _cast(float, output_df["bulk_tx_cost_enduse"].mean())
    max_cost = _cast(float, output_df["bulk_tx_cost_enduse"].max())
    n_nonzero = output_df.filter(pl.col("bulk_tx_cost_enduse") > 0).height
    print("\nOutput summary:")
    print(f"  avg = ${avg_cost:.6f}/kWh, max = ${max_cost:.6f}/kWh")
    print(f"  {n_nonzero} non-zero hours out of 8760")

"""ResStock load scanning with hive partitions and optional building ID filter.

Use the base S3 path and partition columns (state, upgrade) so the engine
discovers and prunes partitions instead of globbing. Optionally filter to
specific building IDs for alignment with CAIRO sample runs.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

# Load column used for hourly electric consumption (ResStock schema)
ELECTRIC_LOAD_COL = "out.electricity.net.energy_consumption"

# ResStock load_curve_hourly layout: .../load_curve_hourly/state=XX/upgrade=YY/*.parquet
LOAD_CURVE_HOURLY_SUBDIR = "load_curve_hourly/"
BLDG_ID_COL = "bldg_id"

# Timezone used for hourly load index; must match marginal cost data (see utils/cairo.py).
HOURLY_LOAD_TZ = "EST"


def scan_resstock_loads(
    resstock_base: str,
    state: str,
    upgrade: str,
    *,
    building_ids: list[int] | None = None,
    storage_options: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """Scan ResStock hourly loads via hive partitions; optionally filter to building IDs.

    Uses the base ResStock path (e.g. s3://.../res_2024_amy2018_2) and
    state/upgrade so the reader discovers partitions instead of globbing.
    No file listing or glob pattern is used.

    Args:
        resstock_base: Base S3 or local path to the ResStock release
            (e.g. s3://data.sb/nrel/resstock/res_2024_amy2018_2).
        state: State partition value (e.g. "NY", "RI").
        upgrade: Upgrade partition value (e.g. "00").
        building_ids: If set, filter to these building IDs so the scan aligns
            with the CAIRO sample.
        storage_options: Passed to scan_parquet for S3 (e.g. aws_region).

    Returns:
        LazyFrame of hourly load data with partition columns state, upgrade
        and optionally filtered to building_ids.
    """
    base = resstock_base.rstrip("/")
    loads_root = f"{base}/{LOAD_CURVE_HOURLY_SUBDIR}"
    kwargs: dict = {}
    if storage_options is not None:
        kwargs["storage_options"] = storage_options

    # Hive-partition root: parse partition columns as strings so filters are plain equality.
    lf = pl.scan_parquet(
        loads_root,
        hive_partitioning=True,
        hive_schema={"state": pl.String, "upgrade": pl.String},
        **kwargs,
    )

    # Partition columns from hive.
    lf = lf.filter(
        pl.col("state") == str(state),
        pl.col("upgrade") == str(upgrade),
        pl.col("bldg_id").is_in(building_ids),
    )
    return lf


def hourly_system_load_from_resstock(
    loads_lf: pl.LazyFrame,
    weights: pl.DataFrame,
    *,
    load_col: str = ELECTRIC_LOAD_COL,
    timestamp_col: str = "timestamp",
    bldg_id_col: str = BLDG_ID_COL,
    weight_col: str = "weight",
) -> pd.Series:
    """Compute weighted hourly system load (one value per hour) from loads and weights.

    Joins loads with weights on bldg_id, multiplies load by weight, and sums by
    timestamp. Returns a pandas Series indexed by datetime (tz-aware EST, matching
    marginal cost data) for use in TOU derivation.
    """
    weights_lf = weights.select(
        pl.col(bldg_id_col).cast(pl.Int64),
        pl.col(weight_col).cast(pl.Float64),
    ).lazy()
    schema_names = loads_lf.collect_schema().names()
    if load_col not in schema_names:
        raise ValueError(
            f"Load column '{load_col}' not found; available: {schema_names[:15]}"
        )
    aggregated = (
        loads_lf.join(weights_lf, on=bldg_id_col, how="inner")
        .with_columns(
            pl.col(timestamp_col)
            .cast(pl.String, strict=False)
            .str.to_datetime(strict=False)
            .alias("_ts"),
            (pl.col(load_col).cast(pl.Float64) * pl.col(weight_col)).alias("_wload"),
        )
        .group_by("_ts")
        .agg(pl.col("_wload").sum().alias("load"))
        .sort("_ts")
    )
    df = aggregated.collect()
    index = df["_ts"].to_pandas()
    series = df["load"].to_pandas()
    series.index = index
    series.index.name = "time"
    # Match marginal cost index (EST) so TOU derivation can align load with MC.
    if series.index.tz is None:
        series.index = series.index.tz_localize(HOURLY_LOAD_TZ, ambiguous="infer")
    return series

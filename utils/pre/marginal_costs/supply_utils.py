"""Shared utility helpers for NY supply marginal cost pipelines."""

from __future__ import annotations

import calendar
import io
from datetime import datetime, timedelta

import polars as pl
from cloudpathlib import S3Path

DEFAULT_LBMP_S3_BASE = "s3://data.sb/nyiso/lbmp/real_time/zones/"
DEFAULT_ICAP_S3_BASE = "s3://data.sb/nyiso/icap/"
DEFAULT_ZONE_LOADS_S3_BASE = "s3://data.sb/nyiso/hourly_demand/zones/"
DEFAULT_ZONE_MAPPING_PATH = (
    "s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv"
)
DEFAULT_OUTPUT_S3_BASE = "s3://data.sb/switchbox/marginal_costs/ny/supply/"

VALID_UTILITIES = frozenset({"cenhud", "coned", "nimo", "nyseg", "or", "rge", "psegli"})


def load_zone_mapping(path: str, storage_options: dict[str, str]) -> pl.DataFrame:
    """Load utility zone mapping CSV from local path or S3."""
    if path.startswith("s3://"):
        csv_bytes = S3Path(path).read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(path)

    required = {
        "utility",
        "load_zone_letter",
        "lbmp_zone_name",
        "icap_locality",
        "gen_capacity_zone",
        "capacity_weight",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Zone mapping CSV missing columns: {sorted(missing)}")

    print(f"Loaded zone mapping with {len(df)} rows from {path}")
    return df


def get_utility_mapping(mapping_df: pl.DataFrame, utility: str) -> pl.DataFrame:
    """Filter zone mapping to a single utility."""
    rows = mapping_df.filter(pl.col("utility") == utility)
    if rows.is_empty():
        available = sorted(mapping_df["utility"].unique().to_list())
        raise ValueError(
            f"Utility '{utility}' not found in zone mapping. Available: {available}"
        )
    return rows


def strip_tz_if_needed(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Strip timezone from datetime column when present."""
    ts_dtype = df.schema[col]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        return df.with_columns(pl.col(col).dt.replace_time_zone(None).alias(col))
    return df


def load_zone_loads(
    zone_loads_s3_base: str,
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load NYISO zone-level hourly loads for selected zones and year."""
    base = zone_loads_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("zone").is_in(zone_names), pl.col("year") == year)
        .select("timestamp", "zone", "load_mw")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone loads collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No zone load data found for zones={zone_names}, year={year} under {base}"
        )

    collected = strip_tz_if_needed(collected, "timestamp")
    zones_found = sorted(collected["zone"].unique().to_list())
    print(
        f"Loaded zone loads: {len(collected):,} rows for zones {zones_found}, year {year}"
    )
    return collected


def remap_year_if_needed(
    df: pl.DataFrame, timestamp_col: str, from_year: int, to_year: int
) -> pl.DataFrame:
    """Offset timestamps by integer years when years differ."""
    if from_year == to_year:
        return df
    offset = f"{to_year - from_year}y"
    return df.with_columns(
        pl.col(timestamp_col).dt.offset_by(offset).alias(timestamp_col)
    )


def build_cairo_8760_timestamps(year: int) -> pl.DataFrame:
    """Build Cairo-compatible 8760 naive timestamps (drops Dec 31 in leap years)."""
    start = datetime(year, 1, 1, 0, 0, 0)
    end = datetime(year, 12, 31, 23, 0, 0)
    timestamps: list[datetime] = []
    cur = start
    while cur <= end:
        timestamps.append(cur)
        cur += timedelta(hours=1)

    if calendar.isleap(year):
        timestamps = [t for t in timestamps if not (t.month == 12 and t.day == 31)]

    if len(timestamps) != 8760:
        raise ValueError(
            f"Expected 8760 timestamps, got {len(timestamps)} for year {year}"
        )
    return pl.DataFrame({"timestamp": timestamps})


def prepare_component_output(
    df: pl.DataFrame,
    year: int,
    input_col: str,
    output_col: str,
    scale: float,
) -> pl.DataFrame:
    """Prepare a single hourly component as Cairo-compatible 8760 output."""
    ref_8760 = build_cairo_8760_timestamps(year)
    hourly = (
        df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("timestamp"))
        .group_by("timestamp")
        .agg(pl.col(input_col).mean().alias(input_col))
        .sort("timestamp")
    )
    output = (
        ref_8760.join(hourly, on="timestamp", how="left")
        .with_columns((pl.col(input_col).fill_null(0.0) * scale).alias(output_col))
        .select("timestamp", output_col)
    )

    if output.height != 8760:
        raise ValueError(f"{output_col} output has {output.height} rows, expected 8760")
    if output.filter(pl.col(output_col).is_null()).height > 0:
        raise ValueError(f"{output_col} output has nulls")
    return output


def assemble_output(
    energy_df: pl.DataFrame,
    capacity_df: pl.DataFrame,
    year: int,
) -> pl.DataFrame:
    """Assemble final combined output matching Cambium-compatible schema."""
    ref_8760 = build_cairo_8760_timestamps(year)
    energy_hourly = (
        energy_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("timestamp"))
        .group_by("timestamp")
        .agg(pl.col("energy_cost_enduse").mean())
        .sort("timestamp")
    )
    capacity_hourly = (
        capacity_df.with_columns(
            pl.col("timestamp").dt.truncate("1h").alias("timestamp")
        )
        .group_by("timestamp")
        .agg(pl.col("capacity_cost_per_kw").mean())
        .sort("timestamp")
    )

    output = (
        ref_8760.join(energy_hourly, on="timestamp", how="left")
        .join(capacity_hourly, on="timestamp", how="left")
        .with_columns(
            pl.col("energy_cost_enduse").fill_null(0.0).alias("energy_cost_enduse"),
            (pl.col("capacity_cost_per_kw").fill_null(0.0) * 1000.0).alias(
                "capacity_cost_enduse"
            ),
        )
        .select("timestamp", "energy_cost_enduse", "capacity_cost_enduse")
    )

    if output.height != 8760:
        raise ValueError(f"Output has {output.height} rows, expected 8760")

    energy_nulls = output.filter(pl.col("energy_cost_enduse").is_null()).height
    capacity_nulls = output.filter(pl.col("capacity_cost_enduse").is_null()).height
    if energy_nulls > 0 or capacity_nulls > 0:
        raise ValueError(
            f"Output has nulls: energy={energy_nulls}, capacity={capacity_nulls}"
        )

    avg_energy = output["energy_cost_enduse"].mean()
    avg_capacity = output["capacity_cost_enduse"].mean()
    max_capacity = output["capacity_cost_enduse"].max()
    nonzero_cap = output.filter(pl.col("capacity_cost_enduse") > 0).height
    print("\nOutput summary (8760 rows):")
    print(f"  Energy:   avg=${avg_energy:.2f}/MWh")
    print(f"  Capacity: avg=${avg_capacity:.2f}/MWh, max=${max_capacity:.2f}/MWh")
    print(f"  Capacity: {nonzero_cap} non-zero hours out of 8760")
    return output


def save_component_output(
    component_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
    component: str,
) -> None:
    """Write a single component parquet to S3 with Hive-style partitioning."""
    base = output_s3_base.rstrip("/") + f"/{component}/"
    partitioned = component_df.with_columns(
        pl.lit(utility).alias("utility"),
        pl.lit(year).alias("year"),
    )
    partitioned.write_parquet(
        base,
        partition_by=["utility", "year"],
        storage_options=storage_options,
    )

    output_path = f"{base}utility={utility}/year={year}/data.parquet"
    print(f"\n✓ Saved {component} MC to {output_path}")
    print(f"  Rows: {len(component_df):,}")
    print(f"  Columns: {', '.join(component_df.columns)}")

"""PJM bulk-transmission inputs.

``pjm_nits`` is the day-weighted January/June NITS ``$/kW-yr``. ``pjm_hive_load``
is the PJM utility-load reader: interpolate onto the Cairo clock, then fill a
remaining null with 0. That fill is what the PJM bulk-tx writer did.
``hive_load`` forward-fills instead, so the two are not the same function.
"""

from __future__ import annotations

import calendar
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import polars as pl

from utils.data_prep.marginal_costs.inputs.context import InputContext, Resolved, resolve_path
from utils.data_prep.marginal_costs.mc_manifest import input_file, input_partition, partition_file_path
from utils.data_prep.marginal_costs.supply_utils import (
    PJM_UTILITY_ZONES,
    build_cairo_8760_timestamps,
    remap_year_if_needed,
)

# Jun 1 – Dec 31 is 214 days in both leap and non-leap years.
_DAYS_JUN_TO_DEC: int = 214
DEFAULT_K_PEAK_HOURS: int = 150

NITS_CSV_PATH = Path(__file__).resolve().parents[4] / "data/pjm/bulk_tx/nits/nits_rates.csv"

# APS is a multi-state zone. The full zone rate and the full zone load are the
# marginal cost a Maryland kW in APS pays. No state scaling is applied.
UTILITY_TO_NITS_ZONE: dict[str, str] = PJM_UTILITY_ZONES
VALID_PJM_UTILITIES: frozenset[str] = frozenset(UTILITY_TO_NITS_ZONE)


def load_nits_rates(nits_csv_path: Path | str = NITS_CSV_PATH) -> pl.DataFrame:
    """Load the NITS rates CSV, skipping comment lines."""
    return pl.read_csv(str(nits_csv_path), comment_prefix="#")


def compute_blended_nits_rate(nits_df: pl.DataFrame, zone: str, year: int) -> float:
    """Day-weighted blend of the January and June NITS rates, in ``$/kW-yr``.

    Non-leap: ``(151 * jan + 214 * jun) / 365``. Leap: ``(152 * jan + 214 * jun) / 366``.
    """
    year_rows = nits_df.filter((pl.col("year") == year) & (pl.col("zone") == zone))
    if year_rows.height == 0:
        available_years = sorted(nits_df["year"].unique().to_list())
        raise ValueError(f"No NITS data for zone={zone}, year={year}. Available years: {available_years}")

    jan_rows = year_rows.filter(pl.col("effective_date").str.contains("-01-01"))
    jun_rows = year_rows.filter(pl.col("effective_date").str.contains("-06-01"))
    if jan_rows.height == 0 or jun_rows.height == 0:
        raise ValueError(
            f"Missing Jan or Jun rate for zone={zone}, year={year}. "
            f"Found effective_dates: {year_rows['effective_date'].to_list()}"
        )

    jan_rate_kw = float(jan_rows["nits_rate_kw_yr"][0])
    jun_rate_kw = float(jun_rows["nits_rate_kw_yr"][0])
    is_leap = calendar.isleap(year)
    days_jan = 152 if is_leap else 151
    days_year = 366 if is_leap else 365
    return (days_jan * jan_rate_kw + _DAYS_JUN_TO_DEC * jun_rate_kw) / days_year


def load_hourly_demand(
    utility: str,
    year: int,
    s3_base: str,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """PJM utility load for ``year``, aligned to the Cairo 8760.

    Duplicate timestamps are averaged. Gaps are interpolated, and a null that
    remains is filled with 0.
    """
    base = s3_base.rstrip("/") + "/"
    scan_kwargs: dict[str, Any] = {"hive_partitioning": True}
    if base.startswith("s3://") and storage_options:
        scan_kwargs["storage_options"] = storage_options
    raw = (
        pl.scan_parquet(base, **scan_kwargs)
        .filter(pl.col("utility") == utility, pl.col("year") == year)
        .select("timestamp", "load_mw")
        .collect()
    )
    if raw.is_empty():
        raise FileNotFoundError(
            f"No PJM hourly demand data for utility={utility!r}, year={year} under {base}. "
            "Check that the data has been uploaded."
        )

    ts_dtype = raw.schema["timestamp"]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        raw = raw.with_columns(pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp"))
    raw = raw.with_columns(pl.col("timestamp").cast(pl.Datetime("us")))

    deduped = raw.group_by("timestamp").agg(pl.col("load_mw").mean()).sort("timestamp")
    ref_8760 = build_cairo_8760_timestamps(year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    aligned = (
        ref_8760.join(deduped, on="timestamp", how="left")
        .sort("timestamp")
        .with_columns(pl.col("load_mw").interpolate())
    )
    if aligned.filter(pl.col("load_mw").is_null()).height > 0:
        aligned = aligned.with_columns(pl.col("load_mw").fill_null(0.0))
    return aligned.select("timestamp", "load_mw")


def pjm_nits(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """Blended NITS ``$/kW-yr`` for the utility's zone in ``year``."""
    del role
    if ctx.utility not in VALID_PJM_UTILITIES:
        raise ValueError(f"Invalid utility {ctx.utility!r}. Valid: {sorted(VALID_PJM_UTILITIES)}")
    path = resolve_path(str(entry.get("path", NITS_CSV_PATH)))
    year = int(entry.get("year", ctx.year))
    zone = UTILITY_TO_NITS_ZONE[ctx.utility]
    rate = compute_blended_nits_rate(load_nits_rates(path), zone, year)
    print(f"\n  NITS zone: {zone}")
    print(f"  Blended rate: ${rate:.4f}/kW-yr")
    return Resolved(rate, [input_file("nits_rates", path)], {"nits_zone": zone, "nits_dollar_year": year})


def pjm_hive_load(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """PJM utility load on the output year's clock."""
    base = str(entry["path"])
    load_year = int(entry.get("load_year", ctx.load_year))
    frame = load_hourly_demand(ctx.utility, load_year, base, ctx.storage_options)
    frame = remap_year_if_needed(frame, "timestamp", load_year, ctx.year)
    partition = partition_file_path(base, ctx.utility, load_year, "")
    return Resolved(
        frame,
        [input_partition(role, partition)],
        {"load_year": load_year, "load_source": "pjm_utility"},
    )

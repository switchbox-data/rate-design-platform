"""Aggregate hour-by-hour grid electricity (kWh) for buildings on one electric utility.

Scans a ``load_curve_hourly`` parquet partition (local or S3), joins to
``utility_assignment.parquet`` on ``bldg_id``, filters to ``sb.electric_utility``,
and sums grid consumption per timestamp across all matching buildings.

Grid consumption follows CAIRO: ``max(total - abs(pv), 0)`` (see ``utils.loads``).
The pipeline stays lazy until the final ``collect``.
"""

from __future__ import annotations

import argparse
import logging
from typing import cast

import polars as pl

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.loads import (
    BLDG_ID_COL,
    ELECTRIC_LOAD_COL,
    ELECTRIC_PV_COL,
    grid_consumption_expr,
)

log = logging.getLogger(__name__)

_UA_UTIL_COL = "sb.electric_utility"


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _parquet_scan_kwargs(path: str) -> dict:
    if _is_s3(path):
        return {"storage_options": get_aws_storage_options()}
    return {}


def hourly_totals_by_utility_lazy(
    path_load_curve_hourly: str,
    path_utility_assignment: str,
    electric_utility: str,
) -> pl.LazyFrame:
    """Return a LazyFrame: one row per timestamp, column ``total_grid_kwh``."""
    scan_kw = _parquet_scan_kwargs(path_load_curve_hourly)
    scan_kw_ua = _parquet_scan_kwargs(path_utility_assignment)

    ua_buildings = (
        pl.scan_parquet(path_utility_assignment, **scan_kw_ua)
        .filter(pl.col(_UA_UTIL_COL) == electric_utility)
        .select(pl.col(BLDG_ID_COL).cast(pl.Int64))
        .unique()
    )

    n_bldg = cast(
        pl.DataFrame,
        ua_buildings.select(pl.len().alias("_n")).collect(),
    )["_n"][0]
    log.info("Buildings for %s: %s (utility_assignment)", electric_utility, n_bldg)

    loads = pl.scan_parquet(path_load_curve_hourly, **scan_kw).with_columns(
        pl.col(BLDG_ID_COL).cast(pl.Int64)
    )

    joined = loads.join(ua_buildings, on=BLDG_ID_COL, how="inner")

    return (
        joined.select(
            pl.col("timestamp"),
            grid_consumption_expr(ELECTRIC_LOAD_COL, ELECTRIC_PV_COL).alias(
                "_grid_kwh"
            ),
        )
        .with_columns(
            pl.col("timestamp")
            .cast(pl.String, strict=False)
            .str.to_datetime(strict=False)
            .alias("_ts")
        )
        .group_by("_ts")
        .agg(pl.col("_grid_kwh").sum().alias("total_grid_kwh"))
        .sort("_ts")
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Sum hour-by-hour grid kWh for all buildings assigned to an electric "
            "utility (lazy scan + single collect)."
        )
    )
    p.add_argument(
        "--path-load-curve-hourly",
        type=str,
        required=True,
        help=(
            "Directory or hive root of hourly load parquet files "
            "(e.g. .../load_curve_hourly/state=NY/upgrade=00 or S3 equivalent)."
        ),
    )
    p.add_argument(
        "--path-utility-assignment",
        type=str,
        required=True,
        help="Path to utility_assignment.parquet (local or s3://).",
    )
    p.add_argument(
        "--electric-utility",
        type=str,
        required=True,
        help="Value of sb.electric_utility to keep (e.g. psegli, coned).",
    )
    p.add_argument(
        "--path-output",
        type=str,
        default=None,
        help="Optional path to write hourly totals as Parquet (local path only).",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = _parse_args()

    lf = hourly_totals_by_utility_lazy(
        args.path_load_curve_hourly,
        args.path_utility_assignment,
        args.electric_utility,
    )
    df = cast(pl.DataFrame, lf.collect())

    if df.height == 0:
        log.warning(
            "No rows after join/filter — check paths, upgrade partition, and "
            "sb.electric_utility value %r",
            args.electric_utility,
        )
        return

    log.info("Hours aggregated: %d", df.height)
    log.info(
        "Total annual grid kWh (sum of hourly totals): %.6g",
        df["total_grid_kwh"].sum(),
    )
    log.info("First row: %s", df.head(1))
    log.info("Last row: %s", df.tail(1))

    """if args.path_output:
        out = Path(args.path_output)
        if _is_s3(str(out)):
            raise SystemExit("--path-output must be a local file path, not s3://")
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out)
        log.info("Wrote %s", out)"""
    print(df.head(10))


if __name__ == "__main__":
    main()

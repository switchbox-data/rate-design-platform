"""Scale ResStock hourly natural gas load curves to EIA-176 residential deliveries.

The scale factor is statewide: EIA-176 residential sales plus transportation
(all operators in the state, Mcf converted to kWh with each operator's
delivered-gas heat content) divided by weighted upgrade-00 ResStock gas use. The same factor is applied to
every requested upgrade so that retrofit gas savings are scaled consistently
with the baseline.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from data.eia.constants import (
    PUDL_EIA176_BY_CONSUMER_URL,
    PUDL_EIA176_DISPOSITION_URL,
)
from data.eia.utils import load_eia176_residential_by_operator

BLDG_ID_COL = "bldg_id"
WEIGHT_COL = "weight"
ANNUAL_GAS_TOTAL_COL = "out.natural_gas.total.energy_consumption.kwh"

GAS_END_USES = (
    "clothes_dryer",
    "fireplace",
    "grill",
    "heating",
    "heating_hp_bkup",
    "hot_water",
    "lighting",
    "permanent_spa_heat",
    "pool_heater",
    "range_oven",
    "total",
)
HOURLY_GAS_COLS = tuple(
    f"out.natural_gas.{end_use}.{suffix}"
    for end_use in GAS_END_USES
    for suffix in ("energy_consumption", "energy_consumption_intensity")
)


def load_eia176_residential_kwh(
    state: str,
    year: int,
    path_by_consumer: str = PUDL_EIA176_BY_CONSUMER_URL,
    path_disposition: str = PUDL_EIA176_DISPOSITION_URL,
) -> float:
    """Statewide EIA-176 residential deliveries (sales + transportation) in kWh.

    Operators with no delivered-gas heat content get the state median; see
    ``load_eia176_residential_by_operator``.
    """
    by_operator = load_eia176_residential_by_operator(
        state,
        year,
        path_by_consumer=path_by_consumer,
        path_disposition=path_disposition,
    )
    imputed = by_operator.filter(pl.col("heat_content_imputed"))
    if imputed.height:
        share = (
            float(imputed["volume_mcf"].sum())
            / float(by_operator["volume_mcf"].sum())
            * 100
        )
        print(
            f"  WARNING: EIA-176 {state} {year} has no delivered-gas heat content for "
            f"operator(s) {imputed['operator_id_eia'].to_list()} ({share:.1f}% of "
            "residential volume); using the state median.",
            flush=True,
        )
    return float(by_operator["eia_kwh"].sum())


def resstock_weighted_gas_kwh(load_curve_annual: pl.LazyFrame) -> float:
    """Weighted sum of annual total natural gas kWh across all buildings."""
    schema = load_curve_annual.collect_schema().names()
    for col in (ANNUAL_GAS_TOTAL_COL, WEIGHT_COL):
        if col not in schema:
            raise ValueError(f"Load curve annual is missing column {col!r}.")
    return float(
        load_curve_annual.select(
            (pl.col(ANNUAL_GAS_TOTAL_COL).fill_null(0) * pl.col(WEIGHT_COL)).sum()
        )
        .collect()
        .item()
    )


def scale_gas_hourly(load_curve_hourly: pl.LazyFrame, factor: float) -> pl.LazyFrame:
    """Multiply every natural gas consumption and intensity column by ``factor``."""
    schema = load_curve_hourly.collect_schema().names()
    missing = [c for c in HOURLY_GAS_COLS if c not in schema]
    if missing:
        raise ValueError(f"Hourly load curve is missing gas columns {missing}.")
    return load_curve_hourly.with_columns(
        (pl.col(c) * factor).alias(c) for c in HOURLY_GAS_COLS
    )


def adjust_gas_usage_hourly_dir(
    load_curve_hourly_dir: Path, factor: float, max_workers: int = 128
) -> int:
    """Scale gas columns in every hourly parquet in the directory, in place.

    Returns the number of files rewritten.
    """
    paths = sorted(load_curve_hourly_dir.glob("*.parquet"))

    def _scale_one(path: Path) -> None:
        # Collect before writing: source and sink are the same file.
        scale_gas_hourly(pl.scan_parquet(str(path)), factor).collect().write_parquet(
            str(path)
        )

    n_files = len(paths)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, _ in enumerate(executor.map(_scale_one, paths), 1):
            if i % 500 == 0 or i == n_files:
                print(f"    {i} out of {n_files} files scaled", flush=True)
    return n_files

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

from data.eia.constants import PUDL_STABLE_VERSION

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

# 1 MMBtu = 1e6 Btu; 1 kWh = 3412.141633 Btu.
KWH_PER_MMBTU = 1_000_000 / 3412.141633

_PUDL_PARQUET = (
    f"https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/{PUDL_STABLE_VERSION}"
)
EIA176_BY_CONSUMER = (
    f"{_PUDL_PARQUET}/core_eia176__yearly_gas_disposition_by_consumer.parquet"
)
EIA176_DISPOSITION = f"{_PUDL_PARQUET}/core_eia176__yearly_gas_disposition.parquet"


def load_eia176_residential_kwh(
    state: str,
    year: int,
    path_by_consumer: str = EIA176_BY_CONSUMER,
    path_disposition: str = EIA176_DISPOSITION,
) -> float:
    """Statewide EIA-176 residential deliveries (sales + transportation) in kWh.

    Transportation volumes are gas the utility delivers to residential customers
    who buy the commodity from a marketer; ResStock models their use too.
    """
    sales = (
        pl.scan_parquet(path_by_consumer)
        .filter(
            (pl.col("operating_state") == state)
            & (pl.col("report_year") == year)
            & (pl.col("customer_class") == "residential")
            & pl.col("revenue_class").cast(pl.String).is_in(["sales", "transport"])
        )
        .group_by("operator_id_eia")
        .agg(pl.col("volume_mcf").cast(pl.Float64).sum())
    )
    heat = (
        pl.scan_parquet(path_disposition)
        .filter((pl.col("operating_state") == state) & (pl.col("report_year") == year))
        .select(
            "operator_id_eia",
            pl.col("delivered_gas_heat_content_mmbtu_per_mcf").cast(pl.Float64),
        )
    )
    joined = sales.join(heat, on="operator_id_eia", how="left").collect()
    if joined.height == 0:
        raise ValueError(f"EIA-176 has no residential deliveries for {state} {year}.")
    missing_heat = joined.filter(
        pl.col("delivered_gas_heat_content_mmbtu_per_mcf").is_null()
    )["operator_id_eia"].to_list()
    if missing_heat:
        raise ValueError(
            f"EIA-176 is missing delivered-gas heat content for {state} {year} "
            f"operator(s) {missing_heat}."
        )
    return float(
        joined.select(
            (
                pl.col("volume_mcf")
                * pl.col("delivered_gas_heat_content_mmbtu_per_mcf")
                * KWH_PER_MMBTU
            ).sum()
        ).item()
    )


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

"""Add delivered fuel (oil, propane) costs to combined bills using ResStock consumption and EIA prices.

Combined bills from CAIRO include electric + natural gas only. This module tops up
bill_level with oil and propane costs for buildings that heat with those fuels,
using consumption from load_curve_annual and prices from EIA heating fuel data on S3.
"""

from __future__ import annotations

from typing import cast

import polars as pl

from utils.post.io import ANNUAL_MONTH, BLDG_ID, BILL_LEVEL

# kWh per gallon: 138,500 BTU/gal heating oil / 3,412 BTU/kWh ≈ 40.6; 91,500 BTU/gal propane ≈ 26.8
KWH_PER_GAL_HEATING_OIL = 40.6
KWH_PER_GAL_PROPANE = 26.8

OIL_CONSUMPTION_COL = "out.fuel_oil.total.energy_consumption.kwh"
PROPANE_CONSUMPTION_COL = "out.propane.total.energy_consumption.kwh"

EIA_HEATING_FUEL_BASE = "s3://data.sb/eia/heating_fuel_prices"


def load_avg_annual_fuel_price(
    product: str,
    state: str,
    year: int,
    price_type: str = "residential",
    storage_options: dict[str, str] | None = None,
) -> float:
    """Load EIA heating fuel prices for product/state/year and return average price_per_gallon.

    Reads Hive-partitioned parquet at s3://data.sb/eia/heating_fuel_prices/product={product}/,
    filters to the given year and state, and averages price_per_gallon across months
    for the given price_type (e.g. residential).
    """
    path = f"{EIA_HEATING_FUEL_BASE}/product={product}/year={year}/"
    if storage_options is None:
        df = pl.scan_parquet(path)
    else:
        df = pl.scan_parquet(path, storage_options=storage_options)
    df = df.filter(
        (pl.col("state") == state) & (pl.col("price_type") == price_type)
    ).select("price_per_gallon")
    result = cast(pl.DataFrame, df.select(pl.col("price_per_gallon").mean()).collect())
    if result.is_empty() or result["price_per_gallon"][0] is None:
        raise ValueError(
            f"No EIA heating fuel price found for product={product}, state={state}, year={year}"
        )
    return float(result["price_per_gallon"][0])


def add_delivered_fuel_bills(
    comb_bills: pl.LazyFrame,
    annual_results: pl.LazyFrame,
    oil_price_per_gallon: float,
    propane_price_per_gallon: float,
) -> pl.LazyFrame:
    """Add annual oil and propane costs to comb_bills bill_level for the Annual row.

    comb_bills: LazyFrame from comb_bills_year_target.csv (bldg_id, weight, month, bill_level, dollar_year).
    annual_results: LazyFrame from load_curve_annual RI_upgrade{XX}_metadata_and_annual_results.parquet
        with columns bldg_id, out.fuel_oil.total.energy_consumption.kwh, out.propane.total.energy_consumption.kwh.
    Oil/propane consumption (kWh) is converted to gallons and multiplied by the given prices;
    the resulting annual fuel cost is added to bill_level only where month == "Annual".
    """
    fuel = (
        annual_results.select(
            pl.col(BLDG_ID),
            pl.col(OIL_CONSUMPTION_COL).fill_null(0),
            pl.col(PROPANE_CONSUMPTION_COL).fill_null(0),
        )
        .with_columns(
            (
                pl.col(OIL_CONSUMPTION_COL)
                / KWH_PER_GAL_HEATING_OIL
                * oil_price_per_gallon
            ).alias("oil_bill"),
            (
                pl.col(PROPANE_CONSUMPTION_COL)
                / KWH_PER_GAL_PROPANE
                * propane_price_per_gallon
            ).alias("propane_bill"),
        )
        .with_columns(
            (pl.col("oil_bill") + pl.col("propane_bill")).alias("delivered_fuel_bill")
        )
        .select(pl.col(BLDG_ID), pl.col("delivered_fuel_bill"))
    )

    combined = comb_bills.join(fuel, on=BLDG_ID, how="left").with_columns(
        pl.when(pl.col("month") == ANNUAL_MONTH)
        .then(pl.col(BILL_LEVEL) + pl.col("delivered_fuel_bill").fill_null(0))
        .otherwise(pl.col(BILL_LEVEL))
        .alias(BILL_LEVEL)
    )
    return combined.drop("delivered_fuel_bill")

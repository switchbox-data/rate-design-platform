"""Add delivered fuel (oil, propane) costs to combined bills using monthly ResStock consumption and EIA prices.

Combined bills from CAIRO include electric + natural gas only. This module tops up
bill_level with oil and propane costs for buildings that heat with those fuels,
using monthly consumption from load_curve_monthly and monthly prices from EIA
heating fuel data on S3.
"""

from __future__ import annotations

from typing import cast

import polars as pl

from utils.post.io import ANNUAL_MONTH, BLDG_ID, BILL_LEVEL, scan

# kWh per gallon: 138,500 BTU/gal heating oil / 3,412 BTU/kWh ≈ 40.6; 91,500 BTU/gal propane ≈ 26.8
KWH_PER_GAL_HEATING_OIL = 40.6
KWH_PER_GAL_PROPANE = 26.8

OIL_CONSUMPTION_COL = "out.fuel_oil.total.energy_consumption"
PROPANE_CONSUMPTION_COL = "out.propane.total.energy_consumption"

MONTH_INT_TO_STR: dict[int, str] = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def _assert_both_products(pivoted: pl.DataFrame, state: str, year: int) -> None:
    """Raise if the pivot is missing a heating_oil or propane column."""
    for col in ("heating_oil", "propane"):
        if col not in pivoted.columns:
            raise ValueError(
                f"EIA fuel prices for state={state}, year={year} missing product '{col}'. "
                f"Available columns after pivot: {pivoted.columns}"
            )


def _assert_no_null_prices(df: pl.DataFrame, state: str, year: int) -> None:
    """Raise if any month is missing an oil or propane price."""
    nulls = df.filter(
        pl.col("oil_price_per_gallon").is_null()
        | pl.col("propane_price_per_gallon").is_null()
    )
    if not nulls.is_empty():
        missing = nulls.select(
            "month", "oil_price_per_gallon", "propane_price_per_gallon"
        )
        raise ValueError(
            f"Missing EIA fuel prices for state={state}, year={year}. "
            f"Null months:\n{missing}"
        )


def load_monthly_fuel_prices(
    path_heating_fuel_prices: str,
    state: str,
    year: int,
) -> pl.DataFrame:
    """Load monthly residential fuel prices (oil + propane) for a state/year.

    Scans the Hive-partitioned root (e.g. s3://data.sb/eia/heating_fuel_prices/) and
    pivots to one row per month with oil and propane price columns.
    Months without price data get 0 (buildings barely consume delivered fuels in summer).
    """
    root = path_heating_fuel_prices.rstrip("/") + "/"
    df = cast(
        pl.DataFrame,
        scan(root, "parquet")
        .filter(
            (pl.col("year") == year)
            & (pl.col("state") == state)
            & (pl.col("price_type") == "residential")
        )
        .select(
            pl.col("month").cast(
                pl.Int8
            ),  # Hive partitions infer as Int64; cast to match load_curve_monthly's Int8
            pl.col("product"),
            pl.col("price_per_gallon"),
        )
        .collect(),
    )

    pivoted = df.pivot(on="product", index="month", values="price_per_gallon")

    _assert_both_products(pivoted, state, year)

    result = pivoted.select(
        pl.col("month"),
        pl.col("heating_oil").alias("oil_price_per_gallon"),
        pl.col("propane").alias("propane_price_per_gallon"),
    )
    _assert_no_null_prices(result, state, year)
    return result


def _assert_no_null_fuel_bills(fuel: pl.LazyFrame) -> pl.LazyFrame:
    """Raise if any rows have null delivered_fuel_bill (price join failed to match)."""
    null_count = cast(
        pl.DataFrame,
        fuel.filter(pl.col("delivered_fuel_bill").is_null()).select(pl.len()).collect(),
    ).item()
    if null_count > 0:
        raise ValueError(
            f"{null_count} rows have null delivered_fuel_bill after joining "
            "load_curve_monthly to monthly_prices. Check that all months in "
            "load_curve_monthly (1-12) have matching price data."
        )
    return fuel


def _assert_fuel_join_complete(combined: pl.LazyFrame) -> None:
    """Raise if any comb_bills rows lack a matching fuel record from load_curve_monthly."""
    null_count = cast(
        pl.DataFrame,
        combined.filter(pl.col("delivered_fuel_bill").is_null())
        .select(pl.len())
        .collect(),
    ).item()
    if null_count > 0:
        raise ValueError(
            f"{null_count} rows in comb_bills have no matching fuel record after joining "
            "all_fuel. Every (bldg_id, month) in comb_bills should exist in load_curve_monthly."
        )


def add_delivered_fuel_bills(
    comb_bills: pl.LazyFrame,
    load_curve_monthly: pl.LazyFrame,
    monthly_prices: pl.DataFrame,
) -> pl.LazyFrame:
    """Add per-month oil and propane costs to comb_bills bill_level.

    Joins monthly consumption (from load_curve_monthly) to monthly prices,
    computes fuel cost per building per month, then adds it to the corresponding
    monthly and Annual rows in comb_bills.

    comb_bills: LazyFrame with (bldg_id, weight, month [str: "Jan"..."Dec","Annual"], bill_level, dollar_year).
    load_curve_monthly: LazyFrame from load_curve_monthly with (bldg_id, month [Int8: 1..12],
        out.fuel_oil.total.energy_consumption, out.propane.total.energy_consumption).
    monthly_prices: DataFrame from load_monthly_fuel_prices with (month [Int8],
        oil_price_per_gallon, propane_price_per_gallon).
    """
    fuel_with_prices = (
        load_curve_monthly.select(
            pl.col(BLDG_ID),
            pl.col("month"),
            pl.col(OIL_CONSUMPTION_COL).fill_null(0),
            pl.col(PROPANE_CONSUMPTION_COL).fill_null(0),
        )
        .join(monthly_prices.lazy(), on="month", how="left")
        .with_columns(
            (
                pl.col(OIL_CONSUMPTION_COL)
                / KWH_PER_GAL_HEATING_OIL
                * pl.col("oil_price_per_gallon")
            ).alias("oil_bill"),
            (
                pl.col(PROPANE_CONSUMPTION_COL)
                / KWH_PER_GAL_PROPANE
                * pl.col("propane_price_per_gallon")
            ).alias("propane_bill"),
        )
        .with_columns(
            (pl.col("oil_bill") + pl.col("propane_bill")).alias("delivered_fuel_bill")
        )
    )
    _assert_no_null_fuel_bills(fuel_with_prices)

    # comb_bills has 13 rows per building: "Jan"..."Dec" + "Annual", each with its own
    # bill_level. We need fuel costs for every row so the join on (bldg_id, month) matches:
    #   bldg_id | month  | bill_level
    #   134     | Jan    | 355.78
    #   134     | Feb    | 207.37
    #   ...     | ...    | ...
    #   134     | Annual | 1995.31
    fuel_monthly = fuel_with_prices.select(
        pl.col(BLDG_ID),
        pl.col("month").replace_strict(
            MONTH_INT_TO_STR, return_dtype=pl.String
        ),  # CAIRO comb_bills uses "Jan"..."Dec", not Int8
        pl.col("delivered_fuel_bill"),
    )

    fuel_annual = (
        fuel_monthly.group_by(BLDG_ID)
        .agg(pl.col("delivered_fuel_bill").sum())
        .with_columns(pl.lit(ANNUAL_MONTH).alias("month"))
        .select(BLDG_ID, "month", "delivered_fuel_bill")
    )

    all_fuel = pl.concat([fuel_monthly, fuel_annual])

    combined = comb_bills.join(all_fuel, on=[BLDG_ID, "month"], how="left")
    _assert_fuel_join_complete(combined)

    result = combined.with_columns(
        (pl.col(BILL_LEVEL) + pl.col("delivered_fuel_bill")).alias(BILL_LEVEL)
    )
    return result.drop("delivered_fuel_bill")

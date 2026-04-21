"""Load baseline electric passthrough components from master ``run_1+2`` comb bills.

``passthrough_delivery`` = ``elec_fixed_charge + elec_delivery_bill`` and
``passthrough_supply`` = ``elec_supply_bill`` on the merged delivery+supply master
table (same definitions as ``validate_passthrough_rr_master_vs_yaml`` / RR YAML
passthrough blocks). Rows are restricted to ``upgrade == 0`` to match that
validation and subclass RR generation.
"""

from __future__ import annotations

from typing import cast

import polars as pl

from utils.post.io import ANNUAL_MONTH, BLDG_ID


REFERENCE_COMB_RUN_PAIR = "1+2"


def comb_bills_passthrough_ref_root(*, state_lower: str, output_batch: str) -> str:
    """Hive root for master ``comb_bills_year_target`` at ``run_1+2``."""
    return (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state_lower}/"
        f"all_utilities/{output_batch}/run_{REFERENCE_COMB_RUN_PAIR}/comb_bills_year_target/"
    )


def load_passthrough_reference_monthly(
    *,
    state_lower: str,
    output_batch: str,
    utility: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """Per ``(bldg_id, month)``: baseline passthrough columns for one electric utility."""
    util_col = "sb.electric_utility"
    root = comb_bills_passthrough_ref_root(
        state_lower=state_lower, output_batch=output_batch
    )
    lf = pl.scan_parquet(
        root,
        hive_partitioning=True,
        storage_options=storage_options,
    )
    q = lf.filter(
        pl.col(util_col) == pl.lit(utility),
        pl.col("upgrade").cast(pl.Int64) == 0,
    ).select(
        pl.col(BLDG_ID),
        pl.col("month"),
        (pl.col("elec_fixed_charge") + pl.col("elec_delivery_bill")).alias(
            "passthrough_delivery"
        ),
        pl.col("elec_supply_bill").alias("passthrough_supply"),
    )
    return cast(pl.DataFrame, q.collect())


def load_passthrough_reference_annual(
    *,
    state_lower: str,
    output_batch: str,
    utility: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """One row per ``bldg_id`` (``month == Annual``, ``upgrade == 0``)."""
    return (
        load_passthrough_reference_monthly(
            state_lower=state_lower,
            output_batch=output_batch,
            utility=utility,
            storage_options=storage_options,
        )
        .filter(pl.col("month") == pl.lit(ANNUAL_MONTH))
        .drop("month")
    )

"""Compute HP and non-HP revenue requirements from CAIRO outputs.

For each customer group (HP vs non-HP):
  RR_group = sum(annual_target_bills) - sum(cross_subsidy_per_customer)

Inputs (under --run-dir):
  - bills/elec_bills_year_target.csv
  - cross_subsidization/cross_subsidization_BAT_values.csv
  - customer_metadata.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

# CAIRO output column names
BLDG_ID_COL = "bldg_id"
HAS_HP_COL = "postprocess_group.has_hp"
CROSS_SUBSIDY_COL = "customer_level_residual_share_percustomer"

# Output constants
GROUP_COL = "customer_group"
HP_GROUP = "HP"
NONHP_GROUP = "NonHP"
ANNUAL_MONTH_VALUE = "Annual"


def _csv_path(run_dir: S3Path | Path, relative: str) -> str:
    return str(run_dir / relative)


def _load_customer_metadata(
    run_dir: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _csv_path(run_dir, "customer_metadata.csv"),
            storage_options=storage_options,
        )
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(HAS_HP_COL).cast(pl.Boolean, strict=False).alias("has_hp"),
        )
        .with_columns(pl.col("has_hp").fill_null(False))
        .unique(subset=[BLDG_ID_COL], keep="first")
    )


def _load_annual_target_bills(
    run_dir: S3Path | Path,
    annual_month: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _csv_path(run_dir, "bills/elec_bills_year_target.csv"),
            storage_options=storage_options,
        )
        .filter(pl.col("month") == annual_month)
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col("bill_level").cast(pl.Float64).alias("annual_bill"),
        )
        .group_by(BLDG_ID_COL)
        .agg(pl.col("annual_bill").sum())
    )


def _load_cross_subsidy(
    run_dir: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _csv_path(
                run_dir, "cross_subsidization/cross_subsidization_BAT_values.csv"
            ),
            storage_options=storage_options,
        )
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(CROSS_SUBSIDY_COL).cast(pl.Float64).alias("cross_subsidy"),
        )
        .group_by(BLDG_ID_COL)
        .agg(pl.col("cross_subsidy").sum())
    )


def compute_hp_nonhp_rr(
    run_dir: S3Path | Path,
    annual_month: str = ANNUAL_MONTH_VALUE,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Return per-group (HP / NonHP) revenue requirement breakdown.

    Columns: customer_group, sum_bills, sum_cross_subsidy, revenue_requirement
    """
    metadata = _load_customer_metadata(run_dir, storage_options)
    bills = _load_annual_target_bills(run_dir, annual_month, storage_options)
    cross_sub = _load_cross_subsidy(run_dir, storage_options)

    joined = (
        metadata.join(bills, on=BLDG_ID_COL, how="left")
        .join(cross_sub, on=BLDG_ID_COL, how="left")
        .collect()
    )
    if joined.is_empty():
        msg = "No customers found in customer_metadata.csv."
        raise ValueError(msg)

    nulls_bills = joined.filter(pl.col("annual_bill").is_null()).height
    if nulls_bills:
        msg = (
            f"Missing annual target bills for {nulls_bills} buildings "
            f"(month={annual_month})."
        )
        raise ValueError(msg)

    nulls_cs = joined.filter(pl.col("cross_subsidy").is_null()).height
    if nulls_cs:
        msg = f"Missing cross-subsidy values for {nulls_cs} buildings."
        raise ValueError(msg)

    breakdown = (
        joined.with_columns(
            pl.when(pl.col("has_hp"))
            .then(pl.lit(HP_GROUP))
            .otherwise(pl.lit(NONHP_GROUP))
            .alias(GROUP_COL)
        )
        .group_by(GROUP_COL)
        .agg(
            pl.col("annual_bill").sum().alias("sum_bills"),
            pl.col("cross_subsidy").sum().alias("sum_cross_subsidy"),
        )
        .with_columns(
            (pl.col("sum_bills") - pl.col("sum_cross_subsidy")).alias(
                "revenue_requirement"
            )
        )
    )

    # Ensure both groups appear even if one has no customers
    groups = pl.DataFrame({GROUP_COL: [HP_GROUP, NONHP_GROUP]})
    return groups.join(breakdown, on=GROUP_COL, how="left").with_columns(
        pl.col("sum_bills").fill_null(0.0),
        pl.col("sum_cross_subsidy").fill_null(0.0),
        pl.col("revenue_requirement").fill_null(0.0),
    )


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Compute HP and non-HP revenue requirements from CAIRO outputs."
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to CAIRO output directory (local or s3://...)",
    )
    parser.add_argument(
        "--annual-month",
        default=ANNUAL_MONTH_VALUE,
        help="Month label for annual bill (default: Annual)",
    )
    parser.add_argument(
        "--output-csv",
        help="Write breakdown CSV to this path",
    )
    args = parser.parse_args()

    run_dir: S3Path | Path = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    storage_options = get_aws_storage_options() if isinstance(run_dir, S3Path) else None

    breakdown = compute_hp_nonhp_rr(
        run_dir=run_dir,
        annual_month=args.annual_month,
        storage_options=storage_options,
    )
    print(breakdown)

    if args.output_csv:
        breakdown.write_csv(args.output_csv)


if __name__ == "__main__":
    main()

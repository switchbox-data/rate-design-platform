"""Compute HP and non-HP revenue requirements from CAIRO outputs.

For each customer group (HP vs non-HP), this script computes:
  RR_group = sum(annual electric target bills) - sum(cross_subsidy_per_customer)

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

BLDG_ID_COL = "bldg_id"
HAS_HP_COL = "postprocess_group.has_hp"
HAS_HP_FLAG_COL = "has_hp"
ANNUAL_BILL_COL = "annual_bill"
CROSS_SUBSIDY_COL = "customer_level_residual_share_percustomer"
CROSS_SUBSIDY_PER_CUSTOMER_COL = "cross_subsidy_per_customer"
GROUP_COL = "customer_group"
SUM_BILLS_COL = "sum_bills"
SUM_CROSS_SUBSIDY_COL = "sum_cross_subsidy_per_customer"
RR_COL = "revenue_requirement"

HP_GROUP = "HP"
NONHP_GROUP = "NonHP"
ANNUAL_MONTH_VALUE = "Annual"


def _storage_opts() -> dict[str, str]:
    return get_aws_storage_options()


def _run_file(run_dir: S3Path | Path, relative_path: str) -> str:
    return str(run_dir / relative_path)


def _load_customer_metadata(
    run_dir: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _run_file(run_dir, "customer_metadata.csv"),
            storage_options=storage_options,
        )
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(HAS_HP_COL).cast(pl.Boolean, strict=False).alias(HAS_HP_FLAG_COL),
        )
        .with_columns(pl.col(HAS_HP_FLAG_COL).fill_null(False))
        .unique(subset=[BLDG_ID_COL], keep="first")
    )


def _load_annual_target_bills(
    run_dir: S3Path | Path,
    annual_month: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _run_file(run_dir, "bills/elec_bills_year_target.csv"),
            storage_options=storage_options,
        )
        .filter(pl.col("month") == annual_month)
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col("bill_level").cast(pl.Float64).alias(ANNUAL_BILL_COL),
        )
        .group_by(BLDG_ID_COL)
        .agg(pl.col(ANNUAL_BILL_COL).sum().alias(ANNUAL_BILL_COL))
    )


def _load_cross_subsidy_per_customer(
    run_dir: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _run_file(
                run_dir,
                "cross_subsidization/cross_subsidization_BAT_values.csv",
            ),
            storage_options=storage_options,
        )
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(CROSS_SUBSIDY_COL)
            .cast(pl.Float64)
            .alias(CROSS_SUBSIDY_PER_CUSTOMER_COL),
        )
        .group_by(BLDG_ID_COL)
        .agg(
            pl.col(CROSS_SUBSIDY_PER_CUSTOMER_COL)
            .sum()
            .alias(CROSS_SUBSIDY_PER_CUSTOMER_COL)
        )
    )


def _validate_joined_inputs(joined: pl.DataFrame, annual_month: str) -> None:
    missing_bills = joined.filter(pl.col(ANNUAL_BILL_COL).is_null()).height
    if missing_bills > 0:
        raise ValueError(
            f"Missing annual target bills for {missing_bills} buildings "
            f"(month={annual_month})."
        )

    missing_cross_subsidy = joined.filter(
        pl.col(CROSS_SUBSIDY_PER_CUSTOMER_COL).is_null()
    ).height
    if missing_cross_subsidy > 0:
        raise ValueError(
            "Missing per-customer cross-subsidy values for "
            f"{missing_cross_subsidy} buildings."
        )


def compute_hp_nonhp_rr(
    run_dir: S3Path | Path,
    annual_month: str = ANNUAL_MONTH_VALUE,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Return HP/non-HP sums and revenue requirement by group."""
    metadata = _load_customer_metadata(run_dir, storage_options)
    annual_bills = _load_annual_target_bills(run_dir, annual_month, storage_options)
    cross_subsidy = _load_cross_subsidy_per_customer(run_dir, storage_options)

    joined = (
        metadata.join(annual_bills, on=BLDG_ID_COL, how="left")
        .join(cross_subsidy, on=BLDG_ID_COL, how="left")
        .collect()
    )
    if joined.is_empty():
        raise ValueError("No customers found in customer_metadata.csv.")

    _validate_joined_inputs(joined, annual_month)

    breakdown = (
        joined.with_columns(
            pl.when(pl.col(HAS_HP_FLAG_COL))
            .then(pl.lit(HP_GROUP))
            .otherwise(pl.lit(NONHP_GROUP))
            .alias(GROUP_COL)
        )
        .group_by(GROUP_COL)
        .agg(
            pl.col(ANNUAL_BILL_COL).sum().alias(SUM_BILLS_COL),
            pl.col(CROSS_SUBSIDY_PER_CUSTOMER_COL).sum().alias(SUM_CROSS_SUBSIDY_COL),
        )
        .with_columns((pl.col(SUM_BILLS_COL) - pl.col(SUM_CROSS_SUBSIDY_COL)).alias(RR_COL))
    )

    groups = pl.DataFrame({GROUP_COL: [HP_GROUP, NONHP_GROUP]})
    return groups.join(breakdown, on=GROUP_COL, how="left").with_columns(
        pl.col(SUM_BILLS_COL).fill_null(0.0).cast(pl.Float64),
        pl.col(SUM_CROSS_SUBSIDY_COL).fill_null(0.0).cast(pl.Float64),
        pl.col(RR_COL).fill_null(0.0).cast(pl.Float64),
    )


def compute_rr_wide(breakdown: pl.DataFrame) -> pl.DataFrame:
    """Return single-row summary with RR_HP and RR_NonHP columns."""
    return breakdown.select(
        pl.col(SUM_BILLS_COL)
        .filter(pl.col(GROUP_COL) == HP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_bills_hp"),
        pl.col(SUM_CROSS_SUBSIDY_COL)
        .filter(pl.col(GROUP_COL) == HP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_cross_subsidy_hp"),
        pl.col(RR_COL)
        .filter(pl.col(GROUP_COL) == HP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("RR_HP"),
        pl.col(SUM_BILLS_COL)
        .filter(pl.col(GROUP_COL) == NONHP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_bills_nonhp"),
        pl.col(SUM_CROSS_SUBSIDY_COL)
        .filter(pl.col(GROUP_COL) == NONHP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_cross_subsidy_nonhp"),
        pl.col(RR_COL)
        .filter(pl.col(GROUP_COL) == NONHP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("RR_NonHP"),
    )


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description=(
            "Compute RR_HP and RR_NonHP from CAIRO outputs using annual target "
            "electric bills and per-customer cross-subsidy."
        )
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to CAIRO output directory (local path or s3://...)",
    )
    parser.add_argument(
        "--annual-month",
        default=ANNUAL_MONTH_VALUE,
        help="Month label in bills file to treat as annual bill (default: Annual)",
    )
    parser.add_argument(
        "--output-csv",
        help="Optional path to write single-row RR summary CSV",
    )
    parser.add_argument(
        "--output-breakdown-csv",
        help="Optional path to write HP/non-HP breakdown CSV",
    )
    args = parser.parse_args()

    run_dir = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    storage_options = _storage_opts() if isinstance(run_dir, S3Path) else None

    breakdown = compute_hp_nonhp_rr(
        run_dir=run_dir,
        annual_month=args.annual_month,
        storage_options=storage_options,
    )
    rr_summary = compute_rr_wide(breakdown)

    if args.output_breakdown_csv:
        breakdown.write_csv(args.output_breakdown_csv)
    if args.output_csv:
        rr_summary.write_csv(args.output_csv)

    print("HP/non-HP breakdown:")
    print(breakdown)
    print("\nRR summary:")
    print(rr_summary)


if __name__ == "__main__":
    main()

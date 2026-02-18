"""Compute subclass revenue requirements from CAIRO outputs.

For each customer subclass in a selected metadata grouping column:
  RR_subclass = sum(annual_target_bills) - sum(selected_BAT_metric)

Inputs (under --run-dir):
  - bills/elec_bills_year_target.csv
  - cross_subsidization/cross_subsidization_BAT_values.csv
  - customer_metadata.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

# CAIRO output column names
BLDG_ID_COL = "bldg_id"
DEFAULT_GROUP_COL = "has_hp"
BAT_METRIC_CHOICES = ("BAT_vol", "BAT_peak", "BAT_percustomer")
DEFAULT_BAT_METRIC = "BAT_percustomer"

# Output constants
GROUP_VALUE_COL = "subclass"
ANNUAL_MONTH_VALUE = "Annual"
DEFAULT_OUTPUT_FILENAME = "subclass_revenue_requirement.csv"


def _csv_path(run_dir: S3Path | Path, relative: str) -> str:
    return str(run_dir / relative)


def _load_group_values(
    run_dir: S3Path | Path,
    group_col: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    metadata = pl.scan_csv(
        _csv_path(run_dir, "customer_metadata.csv"),
        storage_options=storage_options,
    )
    schema = metadata.collect_schema().names()
    group_col_candidates = (
        [group_col, f"postprocess_group.{group_col}"]
        if "." not in group_col
        else [group_col]
    )
    resolved_group_col = next((cn for cn in group_col_candidates if cn in schema), None)
    if resolved_group_col is None:
        msg = (
            f"Grouping column '{group_col}' not found in customer_metadata.csv. "
            f"Tried: {group_col_candidates}"
        )
        raise ValueError(msg)

    return (
        metadata.select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(resolved_group_col)
            .cast(pl.String, strict=False)
            .alias(GROUP_VALUE_COL),
        )
        .with_columns(pl.col(GROUP_VALUE_COL).fill_null("Unknown"))
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
    cross_subsidy_col: str,
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
            pl.col(cross_subsidy_col).cast(pl.Float64).alias("cross_subsidy"),
        )
        .group_by(BLDG_ID_COL)
        .agg(pl.col("cross_subsidy").sum())
    )


def compute_subclass_rr(
    run_dir: S3Path | Path,
    group_col: str = DEFAULT_GROUP_COL,
    cross_subsidy_col: str = DEFAULT_BAT_METRIC,
    annual_month: str = ANNUAL_MONTH_VALUE,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Return subclass revenue requirement breakdown for the selected grouping.

    Columns: subclass, sum_bills, sum_cross_subsidy, revenue_requirement
    """
    group_values = _load_group_values(run_dir, group_col, storage_options)
    bills = _load_annual_target_bills(run_dir, annual_month, storage_options)
    cross_sub = _load_cross_subsidy(run_dir, cross_subsidy_col, storage_options)

    joined = cast(
        pl.DataFrame,
        group_values.join(bills, on=BLDG_ID_COL, how="left")
        .join(cross_sub, on=BLDG_ID_COL, how="left")
        .collect(),
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

    return (
        joined.group_by(GROUP_VALUE_COL)
        .agg(
            pl.col("annual_bill").sum().alias("sum_bills"),
            pl.col("cross_subsidy").sum().alias("sum_cross_subsidy"),
        )
        .with_columns(
            (pl.col("sum_bills") - pl.col("sum_cross_subsidy")).alias(
                "revenue_requirement"
            )
        )
        .sort(GROUP_VALUE_COL)
    )


def _write_breakdown_csv(
    breakdown: pl.DataFrame,
    run_dir: S3Path | Path,
    output_dir: S3Path | Path | None = None,
) -> str:
    target_dir = output_dir if output_dir is not None else run_dir
    output_path = str(target_dir / DEFAULT_OUTPUT_FILENAME)
    csv_text = breakdown.write_csv(None)
    if isinstance(csv_text, str):
        if isinstance(target_dir, S3Path):
            S3Path(output_path).write_text(csv_text)
        else:
            Path(output_path).write_text(csv_text, encoding="utf-8")
        return output_path

    msg = "Failed to render CSV output text."
    raise ValueError(msg)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Compute subclass revenue requirements from CAIRO outputs."
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to CAIRO output directory (local or s3://...)",
    )
    parser.add_argument(
        "--group-col",
        default=DEFAULT_GROUP_COL,
        help=(
            "Grouping column from customer_metadata.csv "
            "(default: has_hp; utility will also resolve postprocess_group.has_hp)"
        ),
    )
    parser.add_argument(
        "--cross-subsidy-col",
        default=DEFAULT_BAT_METRIC,
        choices=BAT_METRIC_CHOICES,
        help="BAT column in cross_subsidization_BAT_values.csv to use.",
    )
    parser.add_argument(
        "--annual-month",
        default=ANNUAL_MONTH_VALUE,
        help="Month label for annual bill (default: Annual)",
    )
    parser.add_argument(
        "--output-dir",
        help=("Optional output directory override. If omitted, writes to --run-dir."),
    )
    args = parser.parse_args()

    run_dir: S3Path | Path = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    output_dir: S3Path | Path | None = None
    if args.output_dir:
        output_dir = (
            S3Path(args.output_dir)
            if args.output_dir.startswith("s3://")
            else Path(args.output_dir)
        )
    storage_options = get_aws_storage_options() if isinstance(run_dir, S3Path) else None

    breakdown = compute_subclass_rr(
        run_dir=run_dir,
        group_col=args.group_col,
        cross_subsidy_col=args.cross_subsidy_col,
        annual_month=args.annual_month,
        storage_options=storage_options,
    )
    print(breakdown)

    output_path = _write_breakdown_csv(
        breakdown=breakdown,
        run_dir=run_dir,
        output_dir=output_dir,
    )
    print(f"Wrote CSV: {output_path}")


if __name__ == "__main__":
    main()

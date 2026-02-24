"""Compare ResStock total load per utility to EIA-861 residential sales.

Uses ResStock **annual** load curves (load_curve_annual: one parquet per state/upgrade
with one row per building and annual kWh columns). Multiplies each building's
electricity kWh by sample weight (each building represents ~252 dwellings), joins
to utility assignment, then sums weighted kWh by utility. Compares to EIA-861
residential_sales_mwh (converted to kWh) and outputs ratio, pct difference.

Annual data: s3://data.sb/nrel/resstock/<release>/load_curve_annual/state=<state>/upgrade=<upgrade>/<state>_upgrade<upgrade>_metadata_and_annual_results.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

import polars as pl

from utils import get_aws_region

BLDG_ID_COL = "bldg_id"
ELECTRIC_UTILITY_COL = "sb.electric_utility"
WEIGHT_COL = "weight"
# Annual parquet: one row per bldg, column = annual electricity kWh
ANNUAL_ELECTRICITY_COL = "out.electricity.total.energy_consumption.kwh"
RESSTOCK_TOTAL_KWH = "resstock_total_kwh"
RESSTOCK_CUSTOMERS = "resstock_customers"
EIA_RESIDENTIAL_KWH = "eia_residential_kwh"
EIA_RESIDENTIAL_CUSTOMERS = "eia_residential_customers"
MWH_TO_KWH = 1000

DEFAULT_RESSTOCK_RELEASE = "res_2024_amy2018_2"
S3_BASE_RESSTOCK = "s3://data.sb/nrel/resstock"
S3_BASE_EIA861 = "s3://data.sb/eia/861/electric_utility_stats"


def _storage_options() -> dict[str, str]:
    return {"aws_region": get_aws_region()}


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _default_annual_path(release: str, state: str, upgrade: str) -> str:
    """Default path to the single load_curve_annual parquet for state/upgrade."""
    base = f"{S3_BASE_RESSTOCK}/{release}/load_curve_annual/state={state}/upgrade={upgrade}"
    return f"{base}/{state}_upgrade{upgrade}_metadata_and_annual_results.parquet"


def _default_utility_assignment_path(release: str, state: str) -> str:
    return f"{S3_BASE_RESSTOCK}/{release}/metadata_utility/state={state}/utility_assignment.parquet"


def _default_eia861_path(state: str, year: int = 2018) -> str:
    """Default path: year-partitioned EIA-861 (year=2018 aligns with ResStock AMY 2018)."""
    return f"{S3_BASE_EIA861}/year={year}/state={state}/data.parquet"


def _load_resstock_total_kwh_per_utility_from_annual(
    path_annual: str,
    path_utility_assignment: str,
    storage_options: dict[str, str] | None,
    electricity_column_override: str | None = None,
) -> pl.DataFrame:
    """Read annual parquet (one row per bldg), join utility assignment, sum weighted kWh by utility."""
    opts_annual = storage_options if _is_s3(path_annual) else None
    annual_schema = (
        pl.scan_parquet(path_annual, storage_options=opts_annual)
        .collect_schema()
        .names()
    )

    elec_col = electricity_column_override or ANNUAL_ELECTRICITY_COL
    if elec_col not in annual_schema:
        raise ValueError(
            f"Annual parquet at {path_annual!r} is missing column {elec_col!r}. "
            f"Use --load-column to specify. Available (first 30): {annual_schema[:30]!r}"
        )
    if BLDG_ID_COL not in annual_schema:
        raise ValueError(
            f"Annual parquet at {path_annual!r} is missing column {BLDG_ID_COL!r}."
        )
    if WEIGHT_COL not in annual_schema:
        raise ValueError(
            f"Annual parquet at {path_annual!r} is missing column {WEIGHT_COL!r} (sample weight)."
        )

    annual_df = cast(
        pl.DataFrame,
        pl.scan_parquet(path_annual, storage_options=opts_annual)
        .select(
            pl.col(BLDG_ID_COL),
            pl.col(elec_col).alias("annual_kwh"),
            pl.col(WEIGHT_COL),
        )
        .collect(),
    )

    opts_ua = storage_options if _is_s3(path_utility_assignment) else None
    ua_lf = pl.scan_parquet(path_utility_assignment, storage_options=opts_ua)
    ua_schema = ua_lf.collect_schema().names()
    if ELECTRIC_UTILITY_COL not in ua_schema:
        raise ValueError(
            f"Utility assignment at {path_utility_assignment!r} is missing "
            f"required column {ELECTRIC_UTILITY_COL!r}"
        )
    ua_df = cast(
        pl.DataFrame,
        ua_lf.select([BLDG_ID_COL, ELECTRIC_UTILITY_COL]).collect(),
    )

    joined = annual_df.join(ua_df, on=BLDG_ID_COL, how="inner")
    joined = joined.with_columns(
        (pl.col("annual_kwh") * pl.col(WEIGHT_COL)).alias("weighted_kwh")
    )
    out = joined.group_by(ELECTRIC_UTILITY_COL).agg(
        pl.col("weighted_kwh").sum().alias(RESSTOCK_TOTAL_KWH),
        pl.col(WEIGHT_COL).sum().alias(RESSTOCK_CUSTOMERS),
    )
    return out


def _load_eia861_residential(
    path_eia861: str, storage_options: dict[str, str] | None
) -> pl.DataFrame:
    """Load EIA-861 state parquet: residential sales (kWh) and customer counts."""
    opts = storage_options if _is_s3(path_eia861) else None
    out = (
        pl.scan_parquet(path_eia861, storage_options=opts)
        .select(
            pl.col("utility_code"),
            (pl.col("residential_sales_mwh") * MWH_TO_KWH).alias(EIA_RESIDENTIAL_KWH),
            pl.col("residential_customers").alias(EIA_RESIDENTIAL_CUSTOMERS),
        )
        .collect()
    )
    return cast(pl.DataFrame, out)


def compare_resstock_eia861(
    path_annual: str,
    path_utility_assignment: str,
    path_eia861: str,
    storage_options: dict[str, str] | None = None,
    load_column: str | None = None,
) -> pl.DataFrame:
    """Compute comparison table with kWh and customer count metrics per utility."""
    resstock = _load_resstock_total_kwh_per_utility_from_annual(
        path_annual,
        path_utility_assignment,
        storage_options=storage_options,
        electricity_column_override=load_column,
    )
    eia = _load_eia861_residential(path_eia861, storage_options)

    comparison = resstock.join(
        eia,
        left_on=ELECTRIC_UTILITY_COL,
        right_on="utility_code",
        how="inner",
    ).select(
        pl.col(ELECTRIC_UTILITY_COL).alias("utility_code"),
        pl.col(RESSTOCK_TOTAL_KWH),
        pl.col(EIA_RESIDENTIAL_KWH),
        (pl.col(RESSTOCK_TOTAL_KWH) / pl.col(EIA_RESIDENTIAL_KWH)).alias("kwh_ratio"),
        (
            (pl.col(RESSTOCK_TOTAL_KWH) - pl.col(EIA_RESIDENTIAL_KWH))
            / pl.col(EIA_RESIDENTIAL_KWH)
            * 100
        ).alias("kwh_pct_diff"),
        pl.col(RESSTOCK_CUSTOMERS),
        pl.col(EIA_RESIDENTIAL_CUSTOMERS),
        (pl.col(RESSTOCK_CUSTOMERS) / pl.col(EIA_RESIDENTIAL_CUSTOMERS)).alias(
            "customers_ratio"
        ),
        (
            (pl.col(RESSTOCK_CUSTOMERS) - pl.col(EIA_RESIDENTIAL_CUSTOMERS))
            / pl.col(EIA_RESIDENTIAL_CUSTOMERS)
            * 100
        ).alias("customers_pct_diff"),
    )
    return comparison


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare ResStock total load per utility to EIA-861 residential sales (uses load_curve_annual)."
    )
    parser.add_argument(
        "--state",
        required=True,
        help="State abbreviation (e.g. RI, NY)",
    )
    parser.add_argument(
        "--resstock-release",
        default=DEFAULT_RESSTOCK_RELEASE,
        help="ResStock release name",
    )
    parser.add_argument(
        "--upgrade",
        default="00",
        help="ResStock upgrade ID (zero-padded)",
    )
    parser.add_argument(
        "--path-annual",
        default=None,
        help="Path to load_curve_annual parquet (default: S3 <release>/load_curve_annual/state=<state>/upgrade=<upgrade>/<state>_upgrade<upgrade>_metadata_and_annual_results.parquet)",
    )
    parser.add_argument(
        "--path-utility-assignment",
        default=None,
        help="ResStock utility assignment parquet path (default: S3 from release/state)",
    )
    parser.add_argument(
        "--eia-year",
        type=int,
        default=2018,
        help="EIA-861 report year for default path (default: 2018, to align with ResStock AMY 2018)",
    )
    parser.add_argument(
        "--path-eia861",
        default=None,
        help="EIA-861 state parquet path (default: S3 year=<eia-year>/state=<state>/data.parquet)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write comparison CSV here; if omitted, print to stdout",
    )
    parser.add_argument(
        "--load-column",
        default=None,
        help="Annual parquet column for electricity kWh (default: %s)"
        % ANNUAL_ELECTRICITY_COL,
    )
    args = parser.parse_args()

    state = args.state.strip().upper()
    path_annual = args.path_annual or _default_annual_path(
        args.resstock_release, state, args.upgrade
    )
    path_utility_assignment = (
        args.path_utility_assignment
        or _default_utility_assignment_path(args.resstock_release, state)
    )
    path_eia861 = args.path_eia861 or _default_eia861_path(state, args.eia_year)

    opts = (
        _storage_options()
        if (
            _is_s3(path_annual)
            or _is_s3(path_utility_assignment)
            or _is_s3(path_eia861)
        )
        else None
    )

    comparison = compare_resstock_eia861(
        path_annual=path_annual,
        path_utility_assignment=path_utility_assignment,
        path_eia861=path_eia861,
        storage_options=opts,
        load_column=args.load_column or None,
    )

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        comparison.write_csv(args.output)
        print(f"Wrote {args.output}")
    else:
        print(comparison.write_csv())


if __name__ == "__main__":
    main()

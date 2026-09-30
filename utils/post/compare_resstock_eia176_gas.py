"""Compare ResStock natural gas use per gas utility to EIA-176 residential deliveries.

ResStock ``res_2024_amy2018_2`` is simulated on Actual Meteorological Year 2018,
so the default EIA year is 2018 (parsed from the release name). Each building's
annual gas kWh (``out.natural_gas.total.energy_consumption.kwh``) is multiplied
by its sample weight and summed by ``sb.gas_utility``.

EIA-176 (via the PUDL release pinned in ``data.eia.constants``) reports company-
and state-level deliveries. The comparison uses residential sales plus
residential transportation: ResStock is a residential building sample, and in a
retail-choice state the transportation lines are gas the utility delivers for a
marketer. Volumes are Mcf, converted to kWh with that company's delivered-gas
heat content.

Connecticut operator ids are the EIA-176 company ids from the 2018 company list:
Yankee Gas, Connecticut Natural Gas, Southern Connecticut Gas, and Norwich
Public Utilities.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import polars as pl

from data.eia.constants import PUDL_STABLE_VERSION
from utils import get_aws_region

BLDG_ID_COL = "bldg_id"
GAS_UTILITY_COL = "sb.gas_utility"
WEIGHT_COL = "weight"
ANNUAL_GAS_COL = "out.natural_gas.total.energy_consumption.kwh"
RESSTOCK_TOTAL_KWH = "resstock_total_kwh"
RESSTOCK_CUSTOMERS = "resstock_customers"
EIA_RESIDENTIAL_KWH = "eia_residential_kwh"
EIA_RESIDENTIAL_CUSTOMERS = "eia_residential_customers"

# 1 MMBtu = 1e6 Btu; 1 kWh = 3412.141633 Btu.
KWH_PER_MMBTU = 1_000_000 / 3412.141633

DEFAULT_RESSTOCK_RELEASE = "res_2024_amy2018_2_sb"
S3_BASE_RESSTOCK = "s3://data.sb/nrel/resstock"
_PUDL_PARQUET = (
    f"https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/{PUDL_STABLE_VERSION}"
)
EIA176_BY_CONSUMER = (
    f"{_PUDL_PARQUET}/core_eia176__yearly_gas_disposition_by_consumer.parquet"
)
EIA176_DISPOSITION = f"{_PUDL_PARQUET}/core_eia176__yearly_gas_disposition.parquet"

# EIA-176 operator_id_eia -> platform gas utility, by state.
# Connecticut names are from eia176_2018_company_list.csv.
GAS_OPERATORS_BY_STATE: dict[str, dict[str, str]] = {
    "CT": {
        "17602792CT": "yankee_gas",  # YANKEE GAS SVC CO
        "17619018CT": "ct_natural_gas",  # CONNECTICUT NAT GAS CORP
        "17619865CT": "southern_ct_gas",  # SOUTHERN CONNECTICUT GAS COMPANY
        "17610359CT": "norwich_muni",  # NORWICH PUBLIC UTILITIES
    },
}

_AMY_YEAR = re.compile(r"amy(\d{4})", re.IGNORECASE)


def eia_year_for_resstock_release(release: str) -> int:
    """Return the AMY weather year embedded in a ResStock release name."""
    match = _AMY_YEAR.search(release)
    if match is None:
        raise ValueError(
            f"Cannot infer the EIA year from ResStock release {release!r}. "
            "Expected a name containing amyYYYY, such as res_2024_amy2018_2_sb. "
            "Pass --eia-year."
        )
    return int(match.group(1))


def _storage_options() -> dict[str, str]:
    return {"aws_region": get_aws_region()}


def _scan(path: str, storage_options: dict[str, str] | None) -> pl.LazyFrame:
    opts = storage_options if path.startswith("s3://") else None
    return pl.scan_parquet(path, storage_options=opts)


def _default_annual_path(release: str, state: str, upgrade: str) -> str:
    base = (
        f"{S3_BASE_RESSTOCK}/{release}/load_curve_annual/"
        f"state={state}/upgrade={upgrade}"
    )
    return f"{base}/{state}_upgrade{upgrade}_metadata_and_annual_results.parquet"


def _default_utility_assignment_path(release: str, state: str) -> str:
    return (
        f"{S3_BASE_RESSTOCK}/{release}/metadata_utility/"
        f"state={state}/utility_assignment.parquet"
    )


def load_resstock_gas_kwh_per_utility(
    path_annual: str,
    path_utility_assignment: str,
    storage_options: dict[str, str] | None,
    gas_column: str = ANNUAL_GAS_COL,
) -> pl.DataFrame:
    """Sum weighted annual gas kWh and sample weights by gas utility."""
    annual_schema = _scan(path_annual, storage_options).collect_schema().names()
    for column in (gas_column, BLDG_ID_COL, WEIGHT_COL):
        if column not in annual_schema:
            raise ValueError(
                f"Annual parquet at {path_annual!r} is missing column {column!r}."
            )
    annual = (
        _scan(path_annual, storage_options)
        .select(
            pl.col(BLDG_ID_COL),
            pl.col(gas_column).alias("annual_kwh"),
            pl.col(WEIGHT_COL),
        )
        .collect()
    )
    assignment_schema = (
        _scan(path_utility_assignment, storage_options).collect_schema().names()
    )
    if GAS_UTILITY_COL not in assignment_schema:
        raise ValueError(
            f"Utility assignment at {path_utility_assignment!r} is missing "
            f"{GAS_UTILITY_COL!r}."
        )
    assignment = (
        _scan(path_utility_assignment, storage_options)
        .select(BLDG_ID_COL, GAS_UTILITY_COL)
        .collect()
    )
    joined = annual.join(assignment, on=BLDG_ID_COL, how="inner").filter(
        pl.col(GAS_UTILITY_COL).is_not_null()
    )
    return joined.group_by(GAS_UTILITY_COL).agg(
        (pl.col("annual_kwh").fill_null(0) * pl.col(WEIGHT_COL))
        .sum()
        .alias(RESSTOCK_TOTAL_KWH),
        pl.col(WEIGHT_COL).sum().alias(RESSTOCK_CUSTOMERS),
    )


def load_eia176_residential(
    path_by_consumer: str,
    path_disposition: str,
    state: str,
    year: int,
    operator_to_utility: dict[str, str],
) -> pl.DataFrame:
    """Residential deliveries (sales + transport) in kWh and customer counts."""
    operator_ids = list(operator_to_utility)
    deliveries = (
        _scan(path_by_consumer, None)
        .filter(
            (pl.col("operating_state") == state)
            & (pl.col("report_year") == year)
            & (pl.col("customer_class") == "residential")
            & pl.col("operator_id_eia").is_in(operator_ids)
        )
        .group_by("operator_id_eia")
        .agg(
            pl.col("volume_mcf").sum().alias("volume_mcf"),
            pl.col("consumers").sum().alias(EIA_RESIDENTIAL_CUSTOMERS),
        )
        .collect()
    )
    if not isinstance(deliveries, pl.DataFrame):
        raise TypeError("Expected DataFrame from EIA-176 deliveries collect()")
    missing = sorted(set(operator_ids) - set(deliveries["operator_id_eia"].to_list()))
    if missing:
        raise ValueError(
            f"EIA-176 has no residential deliveries for {state} {year} "
            f"operator(s) {missing}."
        )
    heat = (
        _scan(path_disposition, None)
        .filter(
            (pl.col("operating_state") == state)
            & (pl.col("report_year") == year)
            & pl.col("operator_id_eia").is_in(operator_ids)
        )
        .select("operator_id_eia", "delivered_gas_heat_content_mmbtu_per_mcf")
        .collect()
    )
    if not isinstance(heat, pl.DataFrame):
        raise TypeError("Expected DataFrame from EIA-176 heat content collect()")
    mapping = pl.DataFrame(
        {
            "operator_id_eia": operator_ids,
            "utility_code": [operator_to_utility[i] for i in operator_ids],
        }
    )
    out = deliveries.join(heat, on="operator_id_eia", how="left").join(
        mapping, on="operator_id_eia", how="left"
    )
    if out["delivered_gas_heat_content_mmbtu_per_mcf"].null_count() > 0:
        raise ValueError(
            f"EIA-176 is missing delivered-gas heat content for {state} {year}."
        )
    return out.select(
        "utility_code",
        (
            pl.col("volume_mcf")
            * pl.col("delivered_gas_heat_content_mmbtu_per_mcf")
            * KWH_PER_MMBTU
        ).alias(EIA_RESIDENTIAL_KWH),
        pl.col(EIA_RESIDENTIAL_CUSTOMERS),
    )


def compare_resstock_eia176_gas(
    path_annual: str,
    path_utility_assignment: str,
    path_by_consumer: str,
    path_disposition: str,
    state: str,
    year: int,
    storage_options: dict[str, str] | None = None,
    gas_column: str = ANNUAL_GAS_COL,
) -> pl.DataFrame:
    """Build the per-utility ResStock vs EIA-176 comparison table."""
    operators = GAS_OPERATORS_BY_STATE.get(state)
    if operators is None:
        supported = ", ".join(sorted(GAS_OPERATORS_BY_STATE))
        raise ValueError(
            f"No EIA-176 gas-utility map for state {state}. Supported: {supported}."
        )
    resstock = load_resstock_gas_kwh_per_utility(
        path_annual,
        path_utility_assignment,
        storage_options,
        gas_column=gas_column,
    ).rename({GAS_UTILITY_COL: "utility_code"})
    eia = load_eia176_residential(
        path_by_consumer, path_disposition, state, year, operators
    )
    return (
        resstock.join(eia, on="utility_code", how="left")
        .with_columns(
            (pl.col(RESSTOCK_TOTAL_KWH) / pl.col(EIA_RESIDENTIAL_KWH)).alias(
                "kwh_ratio"
            ),
            (
                (pl.col(RESSTOCK_TOTAL_KWH) - pl.col(EIA_RESIDENTIAL_KWH))
                / pl.col(EIA_RESIDENTIAL_KWH)
                * 100
            ).alias("kwh_pct_diff"),
            (pl.col(RESSTOCK_CUSTOMERS) / pl.col(EIA_RESIDENTIAL_CUSTOMERS)).alias(
                "customers_ratio"
            ),
            (
                (pl.col(RESSTOCK_CUSTOMERS) - pl.col(EIA_RESIDENTIAL_CUSTOMERS))
                / pl.col(EIA_RESIDENTIAL_CUSTOMERS)
                * 100
            ).alias("customers_pct_diff"),
        )
        .sort("utility_code")
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare weighted ResStock natural gas use by gas utility to "
            "EIA-176 residential deliveries for the ResStock weather year."
        )
    )
    parser.add_argument("--state", required=True, help="State abbreviation, e.g. CT.")
    parser.add_argument(
        "--resstock-release",
        default=DEFAULT_RESSTOCK_RELEASE,
        help="ResStock release (default: res_2024_amy2018_2_sb).",
    )
    parser.add_argument("--upgrade", default="00", help="Zero-padded upgrade id.")
    parser.add_argument("--path-annual", default=None)
    parser.add_argument("--path-utility-assignment", default=None)
    parser.add_argument(
        "--eia-year",
        type=int,
        default=None,
        help="EIA-176 report year. Default: AMY year parsed from the release name.",
    )
    parser.add_argument("--path-eia176-by-consumer", default=EIA176_BY_CONSUMER)
    parser.add_argument("--path-eia176-disposition", default=EIA176_DISPOSITION)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write comparison CSV here. Prints CSV to stdout when omitted.",
    )
    parser.add_argument(
        "--load-column",
        default=ANNUAL_GAS_COL,
        help=f"Annual gas kWh column (default: {ANNUAL_GAS_COL}).",
    )
    args = parser.parse_args()

    state = args.state.strip().upper()
    year = (
        args.eia_year
        if args.eia_year is not None
        else eia_year_for_resstock_release(args.resstock_release)
    )
    path_annual = args.path_annual or _default_annual_path(
        args.resstock_release, state, args.upgrade
    )
    path_utility_assignment = (
        args.path_utility_assignment
        or _default_utility_assignment_path(args.resstock_release, state)
    )
    needs_aws = path_annual.startswith("s3://") or path_utility_assignment.startswith(
        "s3://"
    )
    comparison = compare_resstock_eia176_gas(
        path_annual=path_annual,
        path_utility_assignment=path_utility_assignment,
        path_by_consumer=args.path_eia176_by_consumer,
        path_disposition=args.path_eia176_disposition,
        state=state,
        year=year,
        storage_options=_storage_options() if needs_aws else None,
        gas_column=args.load_column,
    )
    print(
        f"ResStock {args.resstock_release} upgrade {args.upgrade} vs EIA-176 {year} "
        f"residential deliveries (sales + transport) in {state}.",
        file=sys.stderr,
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        comparison.write_csv(args.output)
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        print(comparison.write_csv(), end="")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

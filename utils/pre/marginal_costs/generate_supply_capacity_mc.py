"""Generate utility-level NY supply capacity marginal costs from ICAP."""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.pre.marginal_costs.supply_capacity import (
    N_PEAK_HOURS_PER_MONTH,
    compute_supply_capacity_mc,
)
from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ICAP_S3_BASE,
    DEFAULT_OUTPUT_S3_BASE,
    DEFAULT_ZONE_LOADS_S3_BASE,
    DEFAULT_ZONE_MAPPING_PATH,
    VALID_UTILITIES,
    get_utility_mapping,
    load_zone_mapping,
    prepare_component_output,
    save_component_output,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate utility-level supply capacity marginal costs (ICAP MCOS)."
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        choices=sorted(VALID_UTILITIES),
        help="Utility short name (e.g. nyseg, coned, rge).",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for supply MC generation (e.g. 2025).",
    )
    parser.add_argument(
        "--capacity-load-year",
        type=int,
        default=None,
        help=(
            "Year of zone load profile for ICAP peak identification. "
            "Defaults to --year."
        ),
    )
    parser.add_argument(
        "--zone-mapping-path",
        type=str,
        default=DEFAULT_ZONE_MAPPING_PATH,
        help=f"Path to zone mapping CSV (default: {DEFAULT_ZONE_MAPPING_PATH}).",
    )
    parser.add_argument(
        "--icap-s3-base",
        type=str,
        default=DEFAULT_ICAP_S3_BASE,
        help=f"S3 base for ICAP data (default: {DEFAULT_ICAP_S3_BASE}).",
    )
    parser.add_argument(
        "--zone-loads-s3-base",
        type=str,
        default=DEFAULT_ZONE_LOADS_S3_BASE,
        help=f"S3 base for NYISO zone loads (default: {DEFAULT_ZONE_LOADS_S3_BASE}).",
    )
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default=DEFAULT_OUTPUT_S3_BASE,
        help=f"S3 base for output (default: {DEFAULT_OUTPUT_S3_BASE}).",
    )
    parser.add_argument(
        "--peak-hours",
        type=int,
        default=N_PEAK_HOURS_PER_MONTH,
        help=f"Peak hours per month for ICAP allocation (default: {N_PEAK_HOURS_PER_MONTH}).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: inspect only).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    load_dotenv()
    storage_options = get_aws_storage_options()

    utility = args.utility
    price_year = args.year
    capacity_load_year = args.capacity_load_year or price_year

    print("=" * 60)
    print("SUPPLY CAPACITY MARGINAL COST GENERATION (ICAP)")
    print("=" * 60)
    print(f"  Utility:              {utility}")
    print(f"  Price year:           {price_year}")
    print(f"  Capacity load year:   {capacity_load_year}")
    print(f"  Peak hours:           {args.peak_hours}/month")
    print(f"  Upload to S3:         {'Yes' if args.upload else 'No (inspect only)'}")
    print("=" * 60)

    print("\n── Zone Mapping ──")
    mapping_df = load_zone_mapping(args.zone_mapping_path, storage_options)
    utility_mapping = get_utility_mapping(mapping_df, utility)
    print(utility_mapping)

    print("\n── Capacity MC (ICAP MCOS) ──")
    capacity_df = compute_supply_capacity_mc(
        utility_mapping=utility_mapping,
        utility=utility,
        icap_s3_base=args.icap_s3_base,
        zone_loads_s3_base=args.zone_loads_s3_base,
        price_year=price_year,
        storage_options=storage_options,
        peak_hours=args.peak_hours,
        capacity_load_year=(
            capacity_load_year if capacity_load_year != price_year else None
        ),
    )
    capacity_output = prepare_component_output(
        df=capacity_df,
        year=price_year,
        input_col="capacity_cost_per_kw",
        output_col="capacity_cost_enduse",
        scale=1000.0,
    )

    print("\n── Output Preparation ──")
    print("\nSAMPLE: Top 10 hours by capacity cost")
    print("=" * 60)
    sample = capacity_output.sort("capacity_cost_enduse", descending=True).head(10)
    print(sample)

    if args.upload:
        save_component_output(
            component_df=capacity_output,
            utility=utility,
            year=price_year,
            output_s3_base=args.output_s3_base,
            storage_options=storage_options,
            component="capacity",
        )
        print("\n" + "=" * 60)
        print("✓ Supply capacity marginal cost generation completed and uploaded")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✓ Supply capacity marginal cost generation completed (inspect only)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print("=" * 60)


if __name__ == "__main__":
    main()

"""Generate utility-level ISO-NE supply ancillary service marginal costs."""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.pre.marginal_costs.supply_ancillary import compute_supply_ancillary_mc
from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ISONE_ANCILLARY_S3_BASE,
    DEFAULT_ISONE_OUTPUT_S3_BASE,
    ISONE_UTILITY_ZONES,
    VALID_ISONE_UTILITIES,
    save_component_output,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level supply ancillary service marginal costs "
            "from ISO-NE regulation clearing prices."
        )
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        choices=sorted(VALID_ISONE_UTILITIES),
        help=f"Utility short name. One of: {sorted(VALID_ISONE_UTILITIES)}.",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for ancillary MC generation (e.g. 2025).",
    )
    parser.add_argument(
        "--zone",
        type=str,
        default=None,
        help=(
            "Load zone (informational; ancillary data is not zone-partitioned). "
            "Defaults to the zone mapped to --utility in ISONE_UTILITY_ZONES."
        ),
    )
    parser.add_argument(
        "--ancillary-s3-base",
        type=str,
        default=DEFAULT_ISONE_ANCILLARY_S3_BASE,
        help=f"S3 base for ISO-NE ancillary data (default: {DEFAULT_ISONE_ANCILLARY_S3_BASE}).",
    )
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default=DEFAULT_ISONE_OUTPUT_S3_BASE,
        help=f"S3 base for output (default: {DEFAULT_ISONE_OUTPUT_S3_BASE}).",
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
    zone = args.zone or ISONE_UTILITY_ZONES.get(utility, "unknown")

    print("=" * 60)
    print("SUPPLY ANCILLARY MARGINAL COST GENERATION (ISO-NE)")
    print("=" * 60)
    print(f"  Utility:              {utility}")
    print(f"  Zone:                 {zone}")
    print(f"  Price year:           {price_year}")
    print(f"  Ancillary S3 base:    {args.ancillary_s3_base}")
    print(f"  Upload to S3:         {'Yes' if args.upload else 'No (inspect only)'}")
    print("=" * 60)

    print("\n── Ancillary MC (Regulation Service + Capacity) ──")
    ancillary_output = compute_supply_ancillary_mc(
        year=price_year,
        storage_options=storage_options,
        ancillary_s3_base=args.ancillary_s3_base,
    )

    print("\n── Output Preparation ──")
    print("\nSAMPLE: Top 10 hours by ancillary cost")
    print("=" * 60)
    sample = ancillary_output.sort("ancillary_cost_enduse", descending=True).head(10)
    print(sample)

    if args.upload:
        save_component_output(
            component_df=ancillary_output,
            utility=utility,
            year=price_year,
            output_s3_base=args.output_s3_base,
            storage_options=storage_options,
            component="ancillary",
        )
        print("\n" + "=" * 60)
        print("✓ Supply ancillary marginal cost generation completed and uploaded")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✓ Supply ancillary marginal cost generation completed (inspect only)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print("=" * 60)


if __name__ == "__main__":
    main()

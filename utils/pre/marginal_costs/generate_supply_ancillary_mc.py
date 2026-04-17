"""Generate utility-level ISO-NE or NYISO supply ancillary service marginal costs."""

from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

from utils.pre.marginal_costs.supply_ancillary import (
    AncillaryIso,
    compute_supply_ancillary_mc,
)
from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ISONE_ANCILLARY_S3_BASE,
    DEFAULT_ISONE_OUTPUT_S3_BASE,
    DEFAULT_NYISO_ANCILLARY_S3_BASE,
    DEFAULT_NYISO_OUTPUT_S3_BASE,
    ISONE_UTILITY_ZONES,
    VALID_ISONE_UTILITIES,
    VALID_NYISO_UTILITIES,
    save_component_output,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level supply ancillary service marginal costs "
            "from ISO-NE or NYISO regulation-related clearing prices."
        )
    )
    parser.add_argument(
        "--iso",
        type=str,
        choices=("isone", "nyiso"),
        default="isone",
        help="Market: isone (default) or nyiso.",
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        help="Utility short name (valid set depends on --iso).",
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
            "Load zone (informational for ISO-NE; ancillary data is not zone-partitioned). "
            "Defaults to the zone mapped to --utility when using ISO-NE."
        ),
    )
    parser.add_argument(
        "--ancillary-s3-base",
        type=str,
        default="",
        help=(
            "S3 base for ancillary parquet (default: ISO-NE or NYISO canonical path "
            "matching --iso)."
        ),
    )
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default="",
        help="S3 base for output (default: RI supply vs NY supply matching --iso).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: inspect only).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    iso: AncillaryIso = "isone" if args.iso == "isone" else "nyiso"

    if iso == "isone" and args.utility not in VALID_ISONE_UTILITIES:
        print(
            f"ERROR: --utility must be one of {sorted(VALID_ISONE_UTILITIES)} "
            "for --iso isone",
            file=sys.stderr,
        )
        sys.exit(1)
    if iso == "nyiso" and args.utility not in VALID_NYISO_UTILITIES:
        print(
            f"ERROR: --utility must be one of {sorted(VALID_NYISO_UTILITIES)} "
            "for --iso nyiso",
            file=sys.stderr,
        )
        sys.exit(1)

    ancillary_s3_base = (
        args.ancillary_s3_base
        if args.ancillary_s3_base
        else (
            DEFAULT_ISONE_ANCILLARY_S3_BASE
            if iso == "isone"
            else DEFAULT_NYISO_ANCILLARY_S3_BASE
        )
    )
    output_s3_base = (
        args.output_s3_base
        if args.output_s3_base
        else (
            DEFAULT_ISONE_OUTPUT_S3_BASE
            if iso == "isone"
            else DEFAULT_NYISO_OUTPUT_S3_BASE
        )
    )

    load_dotenv()
    from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

    storage_options = get_aws_storage_options()

    utility = args.utility
    price_year = args.year
    zone = (
        args.zone
        if args.zone is not None
        else (ISONE_UTILITY_ZONES.get(utility, "unknown") if iso == "isone" else "n/a")
    )

    iso_title = "ISO-NE" if iso == "isone" else "NYISO"
    print("=" * 60)
    print(f"SUPPLY ANCILLARY MARGINAL COST GENERATION ({iso_title})")
    print("=" * 60)
    print(f"  ISO:                  {iso}")
    print(f"  Utility:              {utility}")
    print(f"  Zone:                 {zone}")
    print(f"  Price year:           {price_year}")
    print(f"  Ancillary S3 base:    {ancillary_s3_base}")
    print(f"  Upload to S3:         {'Yes' if args.upload else 'No (inspect only)'}")
    print("=" * 60)

    print("\n── Ancillary MC ──")
    ancillary_output = compute_supply_ancillary_mc(
        year=price_year,
        storage_options=storage_options,
        ancillary_s3_base=ancillary_s3_base,
        iso=iso,
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
            output_s3_base=output_s3_base,
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

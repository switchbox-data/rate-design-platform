"""Generate utility-level supply capacity marginal costs (NYISO ICAP or ISO-NE FCA)."""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.pre.marginal_costs.supply_capacity_nyiso import (
    N_PEAK_HOURS_PER_MONTH,
    compute_supply_capacity_mc,
)
from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ICAP_S3_BASE,
    DEFAULT_ISONE_FCA_S3_PATH,
    DEFAULT_ISONE_OUTPUT_S3_BASE,
    DEFAULT_ISONE_ZONE_LOADS_S3_BASE,
    DEFAULT_OUTPUT_S3_BASE,
    DEFAULT_ZONE_LOADS_S3_BASE,
    DEFAULT_ZONE_MAPPING_PATH,
    ISONE_UTILITY_CAPACITY_ZONES,
    VALID_ISONE_UTILITIES,
    VALID_UTILITIES,
    generate_zero_capacity_mc,
    get_utility_mapping,
    load_zone_mapping,
    prepare_component_output,
    save_component_output,
    save_zero_capacity_mc,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level supply capacity marginal costs from "
            "NYISO ICAP or ISO-NE FCA."
        )
    )
    parser.add_argument(
        "--iso",
        type=str,
        default="nyiso",
        choices=["nyiso", "isone"],
        help="ISO to use as source: 'nyiso' (default) or 'isone'.",
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        help=(
            "Utility short name. NYISO: one of "
            f"{sorted(VALID_UTILITIES)}. "
            f"ISO-NE: one of {sorted(VALID_ISONE_UTILITIES)}."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for supply MC generation (e.g. 2025).",
    )
    # NYISO-only args
    parser.add_argument(
        "--capacity-load-year",
        type=int,
        default=None,
        help=(
            "[NYISO only] Year of zone load profile for ICAP peak identification. "
            "Defaults to --year."
        ),
    )
    parser.add_argument(
        "--zone-mapping-path",
        type=str,
        default=DEFAULT_ZONE_MAPPING_PATH,
        help=f"[NYISO only] Path to zone mapping CSV (default: {DEFAULT_ZONE_MAPPING_PATH}).",
    )
    parser.add_argument(
        "--icap-s3-base",
        type=str,
        default=DEFAULT_ICAP_S3_BASE,
        help=f"[NYISO only] S3 base for ICAP data (default: {DEFAULT_ICAP_S3_BASE}).",
    )
    parser.add_argument(
        "--zone-loads-s3-base",
        type=str,
        default=DEFAULT_ZONE_LOADS_S3_BASE,
        help=f"[NYISO only] S3 base for NYISO zone loads (default: {DEFAULT_ZONE_LOADS_S3_BASE}).",
    )
    parser.add_argument(
        "--peak-hours",
        type=int,
        default=N_PEAK_HOURS_PER_MONTH,
        help=f"[NYISO only] Peak hours per month for ICAP allocation (default: {N_PEAK_HOURS_PER_MONTH}).",
    )
    # ISO-NE-only args
    parser.add_argument(
        "--zone",
        type=str,
        default=None,
        help="[ISO-NE only] Load zone for aggregate peak identification (e.g. 'RI').",
    )
    parser.add_argument(
        "--fca-s3-path",
        type=str,
        default=DEFAULT_ISONE_FCA_S3_PATH,
        help=f"[ISO-NE only] S3 path to FCA parquet (default: {DEFAULT_ISONE_FCA_S3_PATH}).",
    )
    parser.add_argument(
        "--capacity-zone-id",
        type=int,
        default=None,
        help=(
            "[ISO-NE only] FCA capacity zone ID (e.g. 8506 for SENE). "
            "Defaults to ISONE_UTILITY_CAPACITY_ZONES[utility]."
        ),
    )
    # Shared args
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default=None,
        help=(
            "S3 base for output. "
            f"Defaults to {DEFAULT_OUTPUT_S3_BASE!r} (NYISO) "
            f"or {DEFAULT_ISONE_OUTPUT_S3_BASE!r} (ISO-NE)."
        ),
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

    iso = args.iso
    utility = args.utility
    price_year = args.year

    # Validate utility against the correct ISO's set
    if iso == "nyiso":
        if utility not in VALID_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for NYISO. "
                f"Valid choices: {sorted(VALID_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_OUTPUT_S3_BASE
    else:  # isone
        if utility not in VALID_ISONE_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for ISO-NE. "
                f"Valid choices: {sorted(VALID_ISONE_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_ISONE_OUTPUT_S3_BASE

    print("=" * 60)
    print(f"SUPPLY CAPACITY MARGINAL COST GENERATION ({iso.upper()})")
    print("=" * 60)
    print(f"  ISO:                  {iso.upper()}")
    print(f"  Utility:              {utility}")
    print(f"  Price year:           {price_year}")
    print(f"  Upload to S3:         {'Yes' if args.upload else 'No (inspect only)'}")

    if iso == "nyiso":
        capacity_load_year = args.capacity_load_year or price_year
        print(f"  Capacity load year:   {capacity_load_year}")
        print(f"  Peak hours:           {args.peak_hours}/month")
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
    else:  # isone
        capacity_zone_id = args.capacity_zone_id or ISONE_UTILITY_CAPACITY_ZONES.get(
            utility
        )
        if capacity_zone_id is None:
            raise SystemExit(
                f"Error: no capacity_zone_id mapping found for utility {utility!r}. "
                "Provide --capacity-zone-id explicitly."
            )
        print(f"  Capacity zone ID:     {capacity_zone_id}")
        print("=" * 60)

        from utils.pre.marginal_costs.supply_capacity_isone import (
            compute_isone_supply_capacity_mc,
        )

        print("\n── Capacity MC (FCA) ──")
        capacity_df = compute_isone_supply_capacity_mc(
            utility=utility,
            year=price_year,
            storage_options=storage_options,
            fca_s3_path=args.fca_s3_path,
            zone_loads_s3_base=DEFAULT_ISONE_ZONE_LOADS_S3_BASE,
            capacity_zone_id=capacity_zone_id,
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
            output_s3_base=output_s3_base,
            storage_options=storage_options,
            component="capacity",
        )
        # Generate zero-filled capacity parquet for delivery-only runs
        # Note: This is ONLY a placeholder for delivery-only runs.
        # For supply runs, actual supply MCs should be loaded.
        print("\n── Zero-Filled Capacity MC (Placeholder for delivery-only runs) ──")
        zero_capacity_output = generate_zero_capacity_mc(year=price_year)
        save_zero_capacity_mc(
            capacity_df=zero_capacity_output,
            utility=utility,
            year=price_year,
            output_s3_base=output_s3_base,
            storage_options=storage_options,
        )
        print("\n" + "=" * 60)
        print("✓ Supply capacity marginal cost generation completed and uploaded")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✓ Supply capacity marginal cost generation completed (inspect only)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print(
            "\nNote: When uploading, zero-filled capacity parquet "
            "(placeholder for delivery-only runs) will also be generated."
        )
        print("=" * 60)


if __name__ == "__main__":
    main()

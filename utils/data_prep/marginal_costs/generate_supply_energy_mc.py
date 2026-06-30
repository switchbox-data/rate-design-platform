"""Generate utility-level supply energy marginal costs from LBMP (NYISO) or LMP (ISO-NE / PJM)."""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from utils.file_io import get_aws_storage_options
from utils.data_prep.marginal_costs.supply_energy import (
    compute_isone_supply_energy_mc,
    compute_pjm_supply_energy_mc,
    compute_supply_energy_mc,
)
from utils.data_prep.marginal_costs.supply_utils import (
    DEFAULT_ISONE_LMP_S3_BASE,
    DEFAULT_ISONE_OUTPUT_S3_BASE,
    DEFAULT_LBMP_S3_BASE,
    DEFAULT_OUTPUT_S3_BASE,
    DEFAULT_PJM_LMP_S3_BASE,
    DEFAULT_PJM_OUTPUT_S3_BASE,
    DEFAULT_ZONE_LOADS_S3_BASE,
    DEFAULT_ZONE_MAPPING_PATH,
    ISONE_UTILITY_ZONES,
    PJM_UTILITY_ZONES,
    VALID_ISONE_UTILITIES,
    VALID_PJM_UTILITIES,
    VALID_UTILITIES,
    generate_zero_energy_mc,
    get_utility_mapping,
    load_zone_mapping,
    prepare_component_output,
    save_component_output,
    save_zero_energy_mc,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level supply energy marginal costs from "
            "NYISO LBMP or ISO-NE LMP."
        )
    )
    parser.add_argument(
        "--iso",
        type=str,
        default="nyiso",
        choices=["nyiso", "isone", "pjm"],
        help="ISO to use as source: 'nyiso' (default), 'isone', or 'pjm'.",
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        help=(
            "Utility short name. NYISO: one of "
            f"{sorted(VALID_UTILITIES)}. "
            f"ISO-NE: one of {sorted(VALID_ISONE_UTILITIES)}. "
            f"PJM (MD): one of {sorted(VALID_PJM_UTILITIES)}."
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
        "--energy-load-year",
        type=int,
        default=None,
        help=(
            "[NYISO only] Year of zone load profile for LBMP weighting "
            "(multi-zone utilities). Defaults to --year. "
            "Single-zone utilities ignore this."
        ),
    )
    parser.add_argument(
        "--zone-mapping-path",
        type=str,
        default=DEFAULT_ZONE_MAPPING_PATH,
        help=f"[NYISO only] Path to zone mapping CSV (default: {DEFAULT_ZONE_MAPPING_PATH}).",
    )
    parser.add_argument(
        "--lbmp-s3-base",
        type=str,
        default=DEFAULT_LBMP_S3_BASE,
        help=f"[NYISO only] S3 base for LBMP data (default: {DEFAULT_LBMP_S3_BASE}).",
    )
    parser.add_argument(
        "--zone-loads-s3-base",
        type=str,
        default=DEFAULT_ZONE_LOADS_S3_BASE,
        help=f"[NYISO only] S3 base for NYISO zone loads (default: {DEFAULT_ZONE_LOADS_S3_BASE}).",
    )
    # ISO-NE / PJM args
    parser.add_argument(
        "--zone",
        type=str,
        default=None,
        help=(
            "[ISO-NE / PJM] LMP zone to use (e.g. 'RI' or 'BGE'). "
            "Defaults to the zone mapped to --utility in the ISO's utility-zone table."
        ),
    )
    parser.add_argument(
        "--lmp-s3-base",
        type=str,
        default=None,
        help=(
            "[ISO-NE / PJM] S3 base for LMP data. "
            f"ISO-NE default: {DEFAULT_ISONE_LMP_S3_BASE}. "
            f"PJM default: {DEFAULT_PJM_LMP_S3_BASE}."
        ),
    )
    # Shared args
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default=None,
        help=(
            "S3 base for output. "
            f"Defaults to {DEFAULT_OUTPUT_S3_BASE!r} (NYISO), "
            f"{DEFAULT_ISONE_OUTPUT_S3_BASE!r} (ISO-NE), "
            f"or {DEFAULT_PJM_OUTPUT_S3_BASE!r} (PJM)."
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

    # Validate utility and resolve defaults per ISO
    if iso == "nyiso":
        if utility not in VALID_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for NYISO. "
                f"Valid choices: {sorted(VALID_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_OUTPUT_S3_BASE
    elif iso == "isone":
        if utility not in VALID_ISONE_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for ISO-NE. "
                f"Valid choices: {sorted(VALID_ISONE_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_ISONE_OUTPUT_S3_BASE
    elif iso == "pjm":  # pjm
        if utility not in VALID_PJM_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for PJM. "
                f"Valid choices: {sorted(VALID_PJM_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_PJM_OUTPUT_S3_BASE
    else:
        raise SystemExit(
            f"Error: invalid ISO {iso!r}. Valid choices: ['nyiso', 'isone', 'pjm']"
        )

    print("=" * 60)
    print(f"SUPPLY ENERGY MARGINAL COST GENERATION ({iso.upper()})")
    print("=" * 60)
    print(f"  ISO:                  {iso.upper()}")
    print(f"  Utility:              {utility}")
    print(f"  Price year:           {price_year}")
    print(f"  Upload to S3:         {'Yes' if args.upload else 'No (inspect only)'}")

    if iso == "nyiso":
        energy_load_year = args.energy_load_year or price_year
        print(f"  Energy load year:     {energy_load_year}")
        print("=" * 60)

        print("\n── Zone Mapping ──")
        mapping_df = load_zone_mapping(args.zone_mapping_path, storage_options)
        utility_mapping = get_utility_mapping(mapping_df, utility)
        print(utility_mapping)

        print("\n── Energy MC (LBMP) ──")
        energy_df = compute_supply_energy_mc(
            utility_mapping,
            args.lbmp_s3_base,
            args.zone_loads_s3_base,
            price_year,
            storage_options,
            zone_load_year=energy_load_year if energy_load_year != price_year else None,
        )
    elif iso == "isone":
        zone = args.zone or ISONE_UTILITY_ZONES.get(utility)
        if zone is None:
            raise SystemExit(
                f"Error: no zone mapping found for utility {utility!r}. "
                "Provide --zone explicitly."
            )
        lmp_s3_base = args.lmp_s3_base or DEFAULT_ISONE_LMP_S3_BASE
        print(f"  Zone:                 {zone}")
        print("=" * 60)

        print("\n── Energy MC (LMP) ──")
        energy_df = compute_isone_supply_energy_mc(
            zone=zone,
            year=price_year,
            storage_options=storage_options,
            lmp_s3_base=lmp_s3_base,
        )
    elif iso == "pjm":  # pjm
        zone = args.zone or PJM_UTILITY_ZONES.get(utility)
        if zone is None:
            raise SystemExit(
                f"Error: no zone mapping found for utility {utility!r}. "
                "Provide --zone explicitly."
            )
        lmp_s3_base = args.lmp_s3_base or DEFAULT_PJM_LMP_S3_BASE
        print(f"  Zone:                 {zone}")
        print("=" * 60)

        print("\n── Energy MC (LMP) ──")
        energy_df = compute_pjm_supply_energy_mc(
            zone=zone,
            year=price_year,
            storage_options=storage_options,
            lmp_s3_base=lmp_s3_base,
        )
    else:
        raise SystemExit(
            f"Error: invalid ISO {iso!r}. Valid choices: ['nyiso', 'isone', 'pjm']"
        )

    energy_output = prepare_component_output(
        df=energy_df,
        year=price_year,
        input_col="energy_cost_enduse",
        output_col="energy_cost_enduse",
        scale=1.0,
    )

    print("\n── Output Preparation ──")
    print("\nSAMPLE: Top 10 hours by energy cost")
    print("=" * 60)
    sample = energy_output.sort("energy_cost_enduse", descending=True).head(10)
    print(sample)

    if args.upload:
        save_component_output(
            component_df=energy_output,
            utility=utility,
            year=price_year,
            output_s3_base=output_s3_base,
            storage_options=storage_options,
            component="energy",
        )
        # Generate zero-filled energy parquet for delivery-only runs
        # Note: This is ONLY a placeholder for delivery-only runs.
        # For supply runs, actual supply MCs should be loaded.
        print("\n── Zero-Filled Energy MC (Placeholder for delivery-only runs) ──")
        zero_energy_output = generate_zero_energy_mc(year=price_year)
        save_zero_energy_mc(
            energy_df=zero_energy_output,
            utility=utility,
            year=price_year,
            output_s3_base=output_s3_base,
            storage_options=storage_options,
        )
        print("\n" + "=" * 60)
        print("✓ Supply energy marginal cost generation completed and uploaded")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✓ Supply energy marginal cost generation completed (inspect only)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print(
            "\nNote: When uploading, zero-filled energy parquet "
            "(placeholder for delivery-only runs) will also be generated."
        )
        print("=" * 60)


if __name__ == "__main__":
    main()

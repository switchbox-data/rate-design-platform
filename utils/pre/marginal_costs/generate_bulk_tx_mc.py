"""Generate utility-level bulk transmission marginal costs (NYISO SCR or ISO-NE AESC PTF).

Usage
-----
    # ISO-NE / RI (inspect only)
    uv run python utils/pre/marginal_costs/generate_bulk_tx_mc.py \\
        --iso isone --utility rie --year 2025 --load-year 2025

    # ISO-NE / RI (upload)
    uv run python utils/pre/marginal_costs/generate_bulk_tx_mc.py \\
        --iso isone --utility rie --year 2025 --load-year 2025 --upload

    # ISO-NE with custom AESC PTF override
    uv run python utils/pre/marginal_costs/generate_bulk_tx_mc.py \\
        --iso isone --utility rie --year 2025 --aesc-ptf-kw-year 184.0 --upload

    # NYISO (inspect only)
    uv run python utils/pre/marginal_costs/generate_bulk_tx_mc.py \\
        --iso nyiso --utility nyseg --year 2025 --load-year 2024

    # NYISO (upload)
    uv run python utils/pre/marginal_costs/generate_bulk_tx_mc.py \\
        --iso nyiso --utility nyseg --year 2025 --load-year 2024 --upload
"""

from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.pre.marginal_costs.bulk_tx_isone import (
    AESC_2024_AVOIDED_PTF_KW_YEAR,
    DEFAULT_N_PEAK_HOURS,
)
from utils.pre.marginal_costs.bulk_tx_nyiso import (
    DEFAULT_NYISO_BULK_TX_CONSTRAINT_GROUP_TABLE_PATH,
    DEFAULT_SCR_WINTER_MONTHS,
    N_SCR_HOURS_PER_SEASON,
)
from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ISONE_BULK_TX_OUTPUT_S3_BASE,
    DEFAULT_ISONE_ZONE_LOADS_S3_BASE,
    DEFAULT_NYISO_BULK_TX_OUTPUT_S3_BASE,
    DEFAULT_NYISO_ZONE_LOADS_S3_BASE,
    DEFAULT_NYISO_ZONE_MAPPING_PATH,
    ISONE_ALL_LOAD_ZONES,
    VALID_ISONE_UTILITIES,
    VALID_NYISO_UTILITIES,
    remap_year_if_needed,
)
from utils.pre.season_config import (
    get_utility_periods_yaml_path,
    load_winter_months_from_periods,
    parse_months_arg,
    resolve_winter_summer_months,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level bulk transmission marginal costs from "
            "NYISO SCR constraint groups or ISO-NE AESC avoided PTF."
        )
    )
    parser.add_argument(
        "--iso",
        type=str,
        required=True,
        choices=["nyiso", "isone"],
        help="ISO to use as source: 'nyiso' or 'isone'.",
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        help=(
            "Utility short name. "
            f"NYISO: one of {sorted(VALID_NYISO_UTILITIES)}. "
            f"ISO-NE: one of {sorted(VALID_ISONE_UTILITIES)}."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for bulk Tx MC generation (e.g. 2025).",
    )
    parser.add_argument(
        "--load-year",
        type=int,
        default=None,
        help="Year of zone loads for peak identification (defaults to --year).",
    )
    # NYISO-only args
    parser.add_argument(
        "--constraint-group-table-path",
        type=str,
        default=DEFAULT_NYISO_BULK_TX_CONSTRAINT_GROUP_TABLE_PATH,
        help=(
            "[NYISO only] Path to ny_bulk_tx_constraint_groups.csv "
            f"(default: {DEFAULT_NYISO_BULK_TX_CONSTRAINT_GROUP_TABLE_PATH})."
        ),
    )
    parser.add_argument(
        "--zone-mapping-path",
        type=str,
        default=DEFAULT_NYISO_ZONE_MAPPING_PATH,
        help=(
            "[NYISO only] Path to zone mapping CSV "
            f"(default: {DEFAULT_NYISO_ZONE_MAPPING_PATH})."
        ),
    )
    parser.add_argument(
        "--zone-loads-s3-base",
        type=str,
        default=None,
        help=(
            "S3 base for zone loads. "
            f"Defaults to {DEFAULT_NYISO_ZONE_LOADS_S3_BASE!r} (NYISO) "
            f"or {DEFAULT_ISONE_ZONE_LOADS_S3_BASE!r} (ISO-NE)."
        ),
    )
    parser.add_argument(
        "--scr-hours-per-season",
        type=int,
        default=N_SCR_HOURS_PER_SEASON,
        help=f"[NYISO only] SCR hours per season (default: {N_SCR_HOURS_PER_SEASON}).",
    )
    parser.add_argument(
        "--periods-yaml",
        type=str,
        default=None,
        help=(
            "[NYISO only] Path to periods YAML containing winter_months. "
            "When omitted, resolves NY utility periods YAML from the repo."
        ),
    )
    parser.add_argument(
        "--winter-months",
        type=str,
        default=None,
        help=(
            "[NYISO only] Comma-separated winter months (e.g. 10,11,12,1,2,3). "
            "Overrides --periods-yaml."
        ),
    )
    # ISO-NE-only args
    parser.add_argument(
        "--aesc-ptf-kw-year",
        type=float,
        default=AESC_2024_AVOIDED_PTF_KW_YEAR,
        help=(
            "[ISO-NE only] AESC avoided PTF cost in $/kW-year "
            f"(default: {AESC_2024_AVOIDED_PTF_KW_YEAR} from AESC 2024)."
        ),
    )
    parser.add_argument(
        "--n-peak-hours",
        type=int,
        default=DEFAULT_N_PEAK_HOURS,
        help=(
            "[ISO-NE only] Number of top NE system-load hours for exceedance "
            f"allocation (default: {DEFAULT_N_PEAK_HOURS})."
        ),
    )
    # Shared args
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default=None,
        help=(
            "S3 base for output. "
            f"Defaults to {DEFAULT_NYISO_BULK_TX_OUTPUT_S3_BASE!r} (NYISO) "
            f"or {DEFAULT_ISONE_BULK_TX_OUTPUT_S3_BASE!r} (ISO-NE)."
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
    year = args.year
    load_year = args.load_year if args.load_year else year

    # Validate utility against the correct ISO's set
    if iso == "nyiso":
        if utility not in VALID_NYISO_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for NYISO. "
                f"Valid choices: {sorted(VALID_NYISO_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_NYISO_BULK_TX_OUTPUT_S3_BASE
        zone_loads_s3_base = args.zone_loads_s3_base or DEFAULT_NYISO_ZONE_LOADS_S3_BASE
    else:  # isone
        if utility not in VALID_ISONE_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for ISO-NE. "
                f"Valid choices: {sorted(VALID_ISONE_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_ISONE_BULK_TX_OUTPUT_S3_BASE
        zone_loads_s3_base = args.zone_loads_s3_base or DEFAULT_ISONE_ZONE_LOADS_S3_BASE

    print("=" * 60)
    print(f"BULK TRANSMISSION MARGINAL COST GENERATION ({iso.upper()})")
    print("=" * 60)
    print(f"  ISO:                  {iso.upper()}")
    print(f"  Utility:              {utility}")
    print(f"  Year:                 {year}")
    print(f"  Load year:            {load_year}")
    print(f"  Upload to S3:         {'Yes' if args.upload else 'No (inspect only)'}")

    if iso == "nyiso":
        _run_nyiso(
            args,
            utility,
            year,
            load_year,
            output_s3_base,
            zone_loads_s3_base,
            storage_options,
        )
    else:
        _run_isone(
            args,
            utility,
            year,
            load_year,
            output_s3_base,
            zone_loads_s3_base,
            storage_options,
        )


def _run_nyiso(
    args: argparse.Namespace,
    utility: str,
    year: int,
    load_year: int,
    output_s3_base: str,
    zone_loads_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    from utils.pre.marginal_costs.bulk_tx_nyiso import (
        ICAP_RAW_TO_NESTED_LOCALITY,
        NESTED_LOCALITY_ZONES,
        compute_paying_locality_costs,
        compute_utility_bulk_tx_signal,
        load_constraint_group_table,
        prepare_output,
        print_summary,
        save_output,
    )
    from utils.pre.marginal_costs.supply_utils import load_zone_loads, load_zone_mapping

    n_scr = args.scr_hours_per_season

    project_root = Path(__file__).resolve().parents[3]
    periods_yaml_path = (
        Path(args.periods_yaml)
        if args.periods_yaml
        else get_utility_periods_yaml_path(
            project_root=project_root,
            state="ny",
            utility=utility,
        )
    )

    if periods_yaml_path.exists():
        yaml_winter = load_winter_months_from_periods(
            periods_yaml_path,
            default_winter_months=DEFAULT_SCR_WINTER_MONTHS,
        )
    else:
        yaml_winter = list(DEFAULT_SCR_WINTER_MONTHS)

    winter_months, summer_months = resolve_winter_summer_months(
        parse_months_arg(args.winter_months) if args.winter_months else None,
        default_winter_months=yaml_winter,
    )

    print(f"  Constraint-group table:  {args.constraint_group_table_path}")
    print(f"  SCR hours/season:        {n_scr}")
    print(f"  Winter months:           {winter_months}")
    print(f"  Summer months:           {summer_months}")
    print("=" * 60)

    mapping_df = load_zone_mapping(args.zone_mapping_path, storage_options)
    constraint_group_df = load_constraint_group_table(args.constraint_group_table_path)

    paying_locality_costs = compute_paying_locality_costs(constraint_group_df)
    utility_icap_rows = (
        mapping_df.filter(mapping_df["utility"] == utility)
        .select("icap_locality", "gen_capacity_zone", "capacity_weight")
        .unique()
    )
    if utility_icap_rows.is_empty():
        available = sorted(mapping_df["utility"].unique().to_list())
        raise ValueError(
            f"Utility '{utility}' not found in zone mapping. Available: {available}"
        )

    nested_localities = sorted(
        {
            ICAP_RAW_TO_NESTED_LOCALITY[str(locality)]
            for locality in utility_icap_rows["icap_locality"].to_list()
        }
    )
    zone_names_needed = sorted(
        {
            zone
            for locality in nested_localities
            for zone in NESTED_LOCALITY_ZONES[str(locality)]
        }
    )

    print(f"\n── Locality Load Profiles (year={load_year}) ──")
    print(f"Loading zone loads for zones={zone_names_needed}")
    zone_loads_df = load_zone_loads(
        zone_loads_s3_base,
        zone_names_needed,
        load_year,
        storage_options,
    )

    from utils.pre.marginal_costs.bulk_tx_nyiso import (
        build_nested_locality_load_profiles,
    )

    locality_profiles = build_nested_locality_load_profiles(
        zone_loads_df,
        [str(x) for x in nested_localities],
    )

    utility_hourly = compute_utility_bulk_tx_signal(
        utility_icap_rows=utility_icap_rows,
        paying_locality_costs=paying_locality_costs,
        locality_profiles=locality_profiles,
        n_scr=n_scr,
        winter_months=winter_months,
    )

    if load_year != year:
        print(f"\n  Remapping timestamps: {load_year} → {year}")
        utility_hourly = utility_hourly.with_columns(
            utility_hourly["timestamp"].dt.offset_by(f"{year - load_year}y")
        )

    output_df = prepare_output(utility_hourly, year)
    print_summary(output_df)

    if args.upload:
        save_output(output_df, utility, year, output_s3_base, storage_options)
        print("\n✓ Bulk transmission MC generation completed and uploaded")
    else:
        print("\n✓ Bulk transmission MC generation completed (inspect only)")


def _run_isone(
    args: argparse.Namespace,
    utility: str,
    year: int,
    load_year: int,
    output_s3_base: str,
    zone_loads_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    from utils.pre.marginal_costs.bulk_tx_isone import (
        compute_isone_bulk_tx_signal,
        prepare_output,
        print_summary,
        save_output,
        validate_allocation,
    )
    from utils.pre.marginal_costs.supply_capacity_isone import load_isone_zone_loads

    aesc_ptf = args.aesc_ptf_kw_year
    n_peak = args.n_peak_hours

    print(f"  AESC avoided PTF:     ${aesc_ptf:.2f}/kW-yr")
    print(f"  Peak hours:           {n_peak}")
    print(f"  NE zones:             {ISONE_ALL_LOAD_ZONES}")
    print("=" * 60)

    # Load all New England zone loads (aggregate to NE system load)
    print(f"\n── NE System Load (year={load_year}) ──")
    ne_load_df = load_isone_zone_loads(
        zone_loads_s3_base=zone_loads_s3_base,
        zone_names=ISONE_ALL_LOAD_ZONES,
        year=load_year,
        storage_options=storage_options,
    )

    # Load RI zone load separately for informational RNS share display
    print(f"\n── RI Zone Load (year={load_year}) ──")
    ri_zone_load_df = load_isone_zone_loads(
        zone_loads_s3_base=zone_loads_s3_base,
        zone_names=["RI"],
        year=load_year,
        storage_options=storage_options,
    )

    # Remap timestamps if load year differs from target year
    ne_load_df = remap_year_if_needed(ne_load_df, "timestamp", load_year, year)
    ri_zone_load_df = remap_year_if_needed(
        ri_zone_load_df, "timestamp", load_year, year
    )

    # Compute ISO-NE bulk TX signal
    bulk_tx_hourly = compute_isone_bulk_tx_signal(
        ne_load_df=ne_load_df,
        aesc_ptf_kw_year=aesc_ptf,
        n_peak_hours=n_peak,
        ri_zone_load_df=ri_zone_load_df,
    )

    # Expand to full 8760 and validate
    output_df = prepare_output(bulk_tx_hourly, year)
    validate_allocation(output_df, aesc_ptf)
    print_summary(output_df)

    if args.upload:
        save_output(output_df, utility, year, output_s3_base, storage_options)
        print("\n✓ ISO-NE bulk transmission MC generation completed and uploaded")
    else:
        print("\n✓ ISO-NE bulk transmission MC generation completed (inspect only)")


if __name__ == "__main__":
    main()

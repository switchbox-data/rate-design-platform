"""Generate utility-level bulk transmission marginal costs.

Supports PJM (MD), NYISO (NY), and ISO-NE (RI, CT).

Usage
-----
    # PJM / MD (inspect only)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso pjm --utility bge --year 2025

    # PJM / MD (upload)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso pjm --utility bge --year 2025 --upload

    # PJM / MD: 2025 NITS $/kW-yr, 2018 load for PCAF (sibling bulk_tx_load2018/)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso pjm --utility bge --year 2025 --load-year 2018 --upload

    # ISO-NE / RI (inspect only)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso isone --utility rie --year 2025 --load-year 2025

    # ISO-NE / RI (upload)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso isone --utility rie --year 2025 --load-year 2025 --upload

    # ISO-NE / CT exceedance, NE system load. This is the CLI default when
    # --method is omitted. The CT Justfile default is month_hour (below).
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso isone --utility ct_eversource --year 2025 --load-year 2025

    # ISO-NE / CT exceedance using CT zone load for peak identification
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso isone --utility ct_eversource --year 2025 --load-year 2025 \\
        --allocation-load utility_zone

    # ISO-NE / CT Eversource: month-hour from OCC-863 probability of peak
    # (the CT Justfile default)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso isone --method month_hour --utility ct_eversource --year 2025 \\
        --path-weekday-pop month_hour_probability_of_peak_weekday.parquet \\
        --path-weekend-pop month_hour_probability_of_peak_weekend_holidays.parquet

    # ISO-NE with custom AESC PTF override
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso isone --utility rie --year 2025 --aesc-ptf-kw-year 184.0 --upload

    # NYISO (inspect only)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso nyiso --utility nyseg --year 2025 --load-year 2024

    # NYISO (upload)
    uv run python utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py \\
        --iso nyiso --utility nyseg --year 2025 --load-year 2024 --upload
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_isone import (
    AESC_2024_AVOIDED_PTF_KW_YEAR,
    AESC_2024_DOLLAR_YEAR,
    DEFAULT_N_PEAK_HOURS,
)
from utils.data_prep.marginal_costs.sub_tx_and_dist.generate_utility_tx_dx_mc import (
    _parse_bool,
    expand_month_hour_mc_to_8760,
    inflate_annual_mc,
    month_hour_costs_from_probability,
    scale_8760_to_annual,
)
from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_nyiso import (
    DEFAULT_NYISO_BULK_TX_CONSTRAINT_GROUP_TABLE_PATH,
    DEFAULT_SCR_WINTER_MONTHS,
    N_SCR_HOURS_PER_SEASON,
)
from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_pjm import (
    DEFAULT_K_PEAK_HOURS,
    PJM_HOURLY_DEMAND_S3_BASE,
    VALID_PJM_UTILITIES,
)
from utils.data_prep.marginal_costs.mc_manifest import (
    McSpec,
    input_file,
    input_partition,
    partition_file_path,
    record_mc_write,
    require_published_code,
)
from utils.data_prep.marginal_costs.supply_utils import (
    DEFAULT_ISONE_ZONE_LOADS_S3_BASE,
    DEFAULT_NYISO_BULK_TX_OUTPUT_S3_BASE,
    DEFAULT_NYISO_ZONE_LOADS_S3_BASE,
    DEFAULT_NYISO_ZONE_MAPPING_PATH,
    DEFAULT_PJM_BULK_TX_OUTPUT_S3_BASE,
    ISONE_ALL_LOAD_ZONES,
    ISONE_UTILITY_ZONES,
    VALID_ISONE_UTILITIES,
    VALID_NYISO_UTILITIES,
    output_base_for_load_year,
    remap_year_if_needed,
)
from utils.file_io import get_aws_storage_options
from utils.pre.season_config import (
    get_utility_periods_yaml_path,
    load_winter_months_from_periods,
    parse_months_arg,
    resolve_winter_summer_months,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level bulk transmission marginal costs. "
            "Supports PJM (MD), NYISO (NY), and ISO-NE (RI)."
        )
    )
    parser.add_argument(
        "--iso",
        type=str,
        required=True,
        choices=["pjm", "nyiso", "isone"],
        help="ISO to use as source: 'pjm', 'nyiso', or 'isone'.",
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        help=(
            "Utility short name. "
            f"PJM: one of {sorted(VALID_PJM_UTILITIES)}. "
            f"NYISO: one of {sorted(VALID_NYISO_UTILITIES)}. "
            f"ISO-NE: one of {sorted(VALID_ISONE_UTILITIES)}."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help=(
            "Output year for partition key and timestamps, and the dollar year "
            "of the annual $/kW-yr (PJM NITS year, ISO-NE AESC year, NYISO "
            "constraint-group costs). Example: 2025."
        ),
    )
    parser.add_argument(
        "--load-year",
        type=int,
        default=None,
        help=(
            "Year of the hourly load used to identify and weight peak hours "
            "(defaults to --year). Distinct from the NITS/AESC dollar year."
        ),
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
    # PJM-only args
    parser.add_argument(
        "--k-peak-hours",
        type=int,
        default=DEFAULT_K_PEAK_HOURS,
        help=(
            "[PJM only] Number of top full-year hours for PCAF load-share allocation "
            f"(default: {DEFAULT_K_PEAK_HOURS}, following E3 ICC-VDER Appendix C)."
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
            "[ISO-NE only] Number of top allocation-load hours for exceedance "
            f"allocation (default: {DEFAULT_N_PEAK_HOURS})."
        ),
    )
    parser.add_argument(
        "--method",
        type=str,
        default="isone_aesc_ptf",
        choices=["isone_aesc_ptf", "month_hour"],
        help=(
            "[ISO-NE only] isone_aesc_ptf (this CLI's default; RI and the "
            "CT exceedance path): top-N exceedance on the allocation load. "
            "month_hour (the CT Justfile default, ct_eversource only): weekday "
            "vs weekend-and-holiday lookup of $/kWh built from the AESC "
            "$/kW-yr times a probability of peak, divided by that month's day "
            "count, then CPI-adjusted to --target-dollar-year."
        ),
    )
    parser.add_argument(
        "--path-weekday-pop",
        type=str,
        default=None,
        help=(
            "[ISO-NE month_hour] Weekday probability-of-peak parquet "
            "(month, hour, num_days, probability_of_peak)."
        ),
    )
    parser.add_argument(
        "--path-weekend-pop",
        type=str,
        default=None,
        help=("[ISO-NE month_hour] Weekend-and-holiday probability-of-peak parquet."),
    )
    parser.add_argument(
        "--include-holidays",
        type=_parse_bool,
        default=True,
        metavar="{true,false}",
        help=(
            "[ISO-NE month_hour] true: federal holidays take the weekend table. "
            "false: only Saturday and Sunday do."
        ),
    )
    parser.add_argument(
        "--calendar-year",
        type=int,
        default=None,
        help=(
            "[ISO-NE month_hour] Calendar that classifies weekdays, weekends, "
            "and holidays (default: --year). Restamped onto --year by position."
        ),
    )
    parser.add_argument(
        "--table-source",
        type=str,
        default="occ863att2",
        help="[ISO-NE month_hour] Short label for the probability tables in the key.",
    )
    parser.add_argument(
        "--ptf-dollar-year",
        type=int,
        default=AESC_2024_DOLLAR_YEAR,
        help=(
            "[ISO-NE month_hour] Dollar year of --aesc-ptf-kw-year "
            f"(default: {AESC_2024_DOLLAR_YEAR}, the AESC 2024 study year)."
        ),
    )
    parser.add_argument(
        "--target-dollar-year",
        type=int,
        default=None,
        help=(
            "[ISO-NE month_hour] Dollar year to inflate the AESC $/kW-yr to "
            "(default: --year). No CPI when this matches --ptf-dollar-year."
        ),
    )
    parser.add_argument(
        "--cpi-s3-base",
        type=str,
        default="s3://data.sb/fred/cpi/",
        help=(
            "[ISO-NE month_hour] Base S3 path for CPIAUCSL annual parquets "
            "(default: s3://data.sb/fred/cpi/)."
        ),
    )
    parser.add_argument(
        "--allocation-load",
        type=str,
        default="ne_system",
        choices=["ne_system", "utility_zone"],
        help=(
            "[ISO-NE only] Load series used to identify peak hours for exceedance "
            "allocation. 'ne_system' (default) sums all 8 ISO-NE zones — consistent "
            "with how bulk TX peaks are driven regionally. 'utility_zone' uses only "
            "the utility's own zone load — closer to embedded-cost RNS allocation "
            "mechanics but defensible for marginal cost work."
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
            "or 's3://data.sb/switchbox/marginal_costs/<state>/bulk_tx/' (ISO-NE)."
        ),
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help=(
            "Citation recorded in the manifest. Default: a per-ISO description "
            "of the $/kW-yr source."
        ),
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: inspect only).",
    )
    return parser.parse_args()


def _zone_partition_inputs(
    zone_loads_s3_base: str, zones: list[str], load_year: int
) -> list[dict]:
    base = zone_loads_s3_base.rstrip("/")
    return [
        input_partition(f"load_zone_{zone}", f"{base}/zone={zone}/year={load_year}")
        for zone in sorted(zones)
    ]


def _save_with_manifest(
    save_output: Callable[[pl.DataFrame, str, int, str, dict[str, str]], None],
    output_df: pl.DataFrame,
    spec: McSpec,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    save_output(output_df, spec.utility, spec.year, output_s3_base, storage_options)
    path_file = partition_file_path(output_s3_base, spec.utility, spec.year)
    record_mc_write(path_file, spec, output_df, "bulk_tx_cost_enduse")


def main() -> None:
    args = _parse_args()
    if args.upload:
        require_published_code()
    load_dotenv()
    storage_options = get_aws_storage_options()

    iso = args.iso
    utility = args.utility
    year = args.year
    load_year = args.load_year if args.load_year else year

    # Validate utility against the correct ISO's set and resolve defaults
    if iso == "pjm":
        if utility not in VALID_PJM_UTILITIES:
            raise SystemExit(
                f"Error: utility {utility!r} is not valid for PJM. "
                f"Valid choices: {sorted(VALID_PJM_UTILITIES)}"
            )
        output_s3_base = args.output_s3_base or DEFAULT_PJM_BULK_TX_OUTPUT_S3_BASE
        zone_loads_s3_base = args.zone_loads_s3_base or PJM_HOURLY_DEMAND_S3_BASE
    elif iso == "nyiso":
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
        state = ISONE_UTILITY_ZONES[utility].lower()
        output_s3_base = (
            args.output_s3_base
            or f"s3://data.sb/switchbox/marginal_costs/{state}/bulk_tx/"
        )
        zone_loads_s3_base = args.zone_loads_s3_base or DEFAULT_ISONE_ZONE_LOADS_S3_BASE

    if args.method != "isone_aesc_ptf" and iso != "isone":
        raise SystemExit(f"--method {args.method} is only defined for --iso isone")
    if args.method == "month_hour":
        if args.load_year is not None and args.load_year != year:
            raise SystemExit(
                "--method month_hour does not use a load year; "
                f"--load-year {args.load_year} differs from --year {year}"
            )
    else:
        output_s3_base = output_base_for_load_year(output_s3_base, year, load_year)

    print("=" * 60)
    print(f"BULK TRANSMISSION MARGINAL COST GENERATION ({iso.upper()})")
    print("=" * 60)
    print(f"  ISO:                  {iso.upper()}")
    print(f"  Utility:              {utility}")
    print(f"  Output / dollar year: {year}")
    print(f"  Load year:            {load_year}")
    print(f"  Output S3 base:       {output_s3_base}")
    print(f"  Upload to S3:         {'Yes' if args.upload else 'No (inspect only)'}")

    if iso == "pjm":
        _run_pjm(
            args,
            utility,
            year,
            load_year,
            output_s3_base,
            zone_loads_s3_base,
            storage_options,
        )
    elif iso == "nyiso":
        _run_nyiso(
            args,
            utility,
            year,
            load_year,
            output_s3_base,
            zone_loads_s3_base,
            storage_options,
        )
    elif args.method == "month_hour":
        _run_isone_month_hour(args, utility, year, output_s3_base, storage_options)
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


def _run_pjm(
    args: argparse.Namespace,
    utility: str,
    year: int,
    load_year: int,
    output_s3_base: str,
    utility_loads_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_pjm import (
        compute_pjm_bulk_tx_mc,
        print_summary,
        save_output,
    )

    k_peak_hours = args.k_peak_hours
    print(f"  K peak hours:         {k_peak_hours}")
    print("  Method:               PCAF load-share, full-year (E3 ICC-VDER)")
    print("=" * 60)

    output_df = compute_pjm_bulk_tx_mc(
        utility=utility,
        year=year,
        load_year=load_year,
        k_peak_hours=k_peak_hours,
        s3_base=utility_loads_s3_base,
        storage_options=storage_options,
    )
    print_summary(output_df)

    if args.upload:
        from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_pjm import NITS_CSV_PATH

        spec = McSpec(
            mc_type="bulk_tx",
            method="pjm_nits_pcaf",
            utility=utility,
            year=year,
            params={
                "k_peak_hours": k_peak_hours,
                "load_year": load_year,
                "nits_dollar_year": year,
            },
            source=args.source or "PJM NITS network service rates, blended by zone",
            inputs=[
                input_file("nits_rates", str(NITS_CSV_PATH)),
                input_partition(
                    "load",
                    partition_file_path(utility_loads_s3_base, utility, load_year, ""),
                ),
            ],
        )
        _save_with_manifest(
            save_output, output_df, spec, output_s3_base, storage_options
        )
        print("\n✓ PJM bulk transmission MC generation completed and uploaded")
    else:
        print("\n✓ PJM bulk transmission MC generation completed (inspect only)")


def _run_nyiso(
    args: argparse.Namespace,
    utility: str,
    year: int,
    load_year: int,
    output_s3_base: str,
    zone_loads_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_nyiso import (
        ICAP_RAW_TO_NESTED_LOCALITY,
        NESTED_LOCALITY_ZONES,
        compute_paying_locality_costs,
        compute_utility_bulk_tx_signal,
        load_constraint_group_table,
        prepare_output,
        print_summary,
        save_output,
    )
    from utils.data_prep.marginal_costs.supply_utils import (
        load_zone_loads,
        load_zone_mapping,
    )

    n_scr = args.scr_hours_per_season

    project_root = Path(__file__).resolve().parents[4]
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

    from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_nyiso import (
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
        spec = McSpec(
            mc_type="bulk_tx",
            method="nyiso_scr",
            utility=utility,
            year=year,
            params={
                "scr_hours_per_season": n_scr,
                "load_year": load_year,
                "winter_months": list(winter_months),
                "summer_months": list(summer_months),
                "nested_localities": [str(x) for x in nested_localities],
            },
            source=args.source or "NYISO bulk TX constraint-group costs",
            inputs=[
                input_file("constraint_groups", args.constraint_group_table_path),
                input_file("zone_mapping", args.zone_mapping_path),
                *_zone_partition_inputs(
                    zone_loads_s3_base, zone_names_needed, load_year
                ),
            ],
        )
        _save_with_manifest(
            save_output, output_df, spec, output_s3_base, storage_options
        )
        print("\n✓ Bulk transmission MC generation completed and uploaded")
    else:
        print("\n✓ Bulk transmission MC generation completed (inspect only)")


def _read_parquet_any(path: str, storage_options: dict[str, str]) -> pl.DataFrame:
    if path.startswith("s3://"):
        return pl.read_parquet(path, storage_options=storage_options)
    return pl.read_parquet(path)


def _run_isone_month_hour(
    args: argparse.Namespace,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Weekday/weekend lookup of AESC $/kW-yr × PoP / num_days, scaled to the annual."""
    from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_isone import (
        print_summary,
        save_output,
        validate_allocation,
    )

    if not (args.path_weekday_pop and args.path_weekend_pop):
        raise ValueError(
            "--method month_hour needs --path-weekday-pop and --path-weekend-pop"
        )
    as_filed = args.aesc_ptf_kw_year
    source_dollar_year = args.ptf_dollar_year
    target_dollar_year = args.target_dollar_year or year
    resolved = inflate_annual_mc(
        as_filed,
        source_dollar_year,
        target_dollar_year,
        args.cpi_s3_base,
        storage_options,
    )
    calendar_year = args.calendar_year or year
    print("  Method:               month_hour")
    print(
        f"  AESC avoided PTF:     ${as_filed:.2f}/kW-yr "
        f"({source_dollar_year}$, as filed)"
    )
    print(f"  Target dollar year:   {target_dollar_year}")
    print(f"  CPI factor:           {resolved.cpi_factor:.6f}")
    print(f"  Allocated annual:     ${resolved.allocated:.4f}/kW-yr")
    print(f"  Calendar year:        {calendar_year}")
    print(f"  Include holidays:     {args.include_holidays}")
    print(f"  Weekday PoP:          {args.path_weekday_pop}")
    print(f"  Weekend PoP:          {args.path_weekend_pop}")
    print("=" * 60)

    weekday_pop = _read_parquet_any(args.path_weekday_pop, storage_options)
    weekend_pop = _read_parquet_any(args.path_weekend_pop, storage_options)
    weekday = month_hour_costs_from_probability(
        weekday_pop, resolved.allocated, label="weekday"
    )
    weekend = month_hour_costs_from_probability(
        weekend_pop, resolved.allocated, label="weekend"
    )
    expanded = expand_month_hour_mc_to_8760(
        weekday,
        weekend,
        year,
        utility,
        include_holidays=args.include_holidays,
        calendar_year=calendar_year,
    )
    annual = float(expanded["mc_total_per_kwh"].sum())
    expanded, scale = scale_8760_to_annual(expanded, resolved.allocated)
    output_df = expanded.select(
        "timestamp", pl.col("mc_total_per_kwh").alias("bulk_tx_cost_enduse")
    )
    print(f"\n  Sum before scale (flat 1 kW, $/kW-yr): ${annual:.4f}")
    print(f"  Uniform scale: {scale:.6f}")
    validate_allocation(output_df, resolved.allocated)
    print_summary(output_df)

    if args.upload:
        spec = McSpec(
            mc_type="bulk_tx",
            method="month_hour",
            utility=utility,
            year=year,
            params={
                "table_source": args.table_source,
                "ptf_kw_year": as_filed,
                "ptf_dollar_year": source_dollar_year,
                "target_dollar_year": target_dollar_year,
                "cpi_factor": resolved.cpi_factor,
                "include_holidays": args.include_holidays,
                "holiday_rule": "5 USC 6103, Sat->Fri, Sun->Mon",
                "calendar_year": calendar_year,
                "timestamp_year": year,
                "calendar_to_timestamp_mapping": (
                    "identity"
                    if calendar_year == year
                    else f"by position: hour i of {calendar_year} -> hour i of {year}"
                ),
                "mc_kw_yr_as_filed": as_filed,
                "mc_kw_yr_target": resolved.allocated,
                "sum_before_scale": annual,
                "scale_factor": scale,
            },
            source=args.source
            or (
                "AESC 2024 avoided PTF "
                f"(${as_filed:.2f}/kW-yr, {source_dollar_year}$) × probability of "
                f"peak / num_days, CPI to {target_dollar_year}$, scaled so the "
                "8760 sums to that inflated annual total"
            ),
            inputs=[
                input_file("weekday_pop", args.path_weekday_pop),
                input_file("weekend_holiday_pop", args.path_weekend_pop),
                *(
                    [input_partition("cpi", args.cpi_s3_base)]
                    if resolved.cpi_factor != 1.0
                    else []
                ),
            ],
        )
        _save_with_manifest(
            save_output, output_df, spec, output_s3_base, storage_options
        )
        print("\n✓ ISO-NE month-hour bulk TX MC completed and uploaded")
    else:
        print("\n✓ ISO-NE month-hour bulk TX MC completed (inspect only)")


def _run_isone(
    args: argparse.Namespace,
    utility: str,
    year: int,
    load_year: int,
    output_s3_base: str,
    zone_loads_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    from utils.data_prep.marginal_costs.bulk_tx.bulk_tx_isone import (
        compute_isone_bulk_tx_signal,
        prepare_output,
        print_summary,
        save_output,
        validate_allocation,
    )
    from utils.data_prep.marginal_costs.supply.supply_capacity_isone import (
        load_isone_zone_loads,
    )

    aesc_ptf = args.aesc_ptf_kw_year
    n_peak = args.n_peak_hours
    allocation_load = args.allocation_load

    # Resolve utility-specific zone (e.g. "RI" for rie, "CT" for ct_eversource/ct_ui)
    utility_zone = ISONE_UTILITY_ZONES[utility]
    # Zones used for allocation load
    allocation_zones = (
        ISONE_ALL_LOAD_ZONES if allocation_load == "ne_system" else [utility_zone]
    )

    print(f"  AESC avoided PTF:     ${aesc_ptf:.2f}/kW-yr")
    print(f"  Peak hours:           {n_peak}")
    print(
        f"  Allocation load:      {allocation_load} ({', '.join(sorted(allocation_zones))})"
    )
    print("=" * 60)

    # Load the allocation load (NE system or utility zone)
    print(f"\n── Allocation Load (year={load_year}) ──")
    alloc_load_df = load_isone_zone_loads(
        zone_loads_s3_base=zone_loads_s3_base,
        zone_names=allocation_zones,
        year=load_year,
        storage_options=storage_options,
    )

    # When using NE system load, also load the utility's own zone for informational
    # load-share display.  When using utility zone, skip — it's already the alloc load.
    utility_zone_load_df = None
    if allocation_load == "ne_system":
        print(f"\n── {utility_zone} Zone Load (year={load_year}) ──")
        utility_zone_load_df = load_isone_zone_loads(
            zone_loads_s3_base=zone_loads_s3_base,
            zone_names=[utility_zone],
            year=load_year,
            storage_options=storage_options,
        )

    # Remap timestamps if load year differs from target year
    alloc_load_df = remap_year_if_needed(alloc_load_df, "timestamp", load_year, year)
    if utility_zone_load_df is not None:
        utility_zone_load_df = remap_year_if_needed(
            utility_zone_load_df, "timestamp", load_year, year
        )

    # Compute ISO-NE bulk TX signal
    bulk_tx_hourly = compute_isone_bulk_tx_signal(
        load_df=alloc_load_df,
        aesc_ptf_kw_year=aesc_ptf,
        n_peak_hours=n_peak,
        utility_zone_load_df=utility_zone_load_df,
        utility_label=utility_zone,
    )

    # Expand to full 8760 and validate
    output_df = prepare_output(bulk_tx_hourly, year)
    validate_allocation(output_df, aesc_ptf)
    print_summary(output_df)

    if args.upload:
        spec = McSpec(
            mc_type="bulk_tx",
            method="isone_aesc_ptf",
            utility=utility,
            year=year,
            params={
                "ptf_kw_year": aesc_ptf,
                "allocation_load": allocation_load,
                "allocation_zones": sorted(allocation_zones),
                "n_peak_hours": n_peak,
                "weighting": "exceedance_top_n",
                "load_year": load_year,
            },
            source=args.source
            or (
                "AESC 2024 avoided PTF cost"
                if aesc_ptf == AESC_2024_AVOIDED_PTF_KW_YEAR
                else "AESC avoided PTF cost, overridden via --aesc-ptf-kw-year"
            ),
            inputs=_zone_partition_inputs(
                zone_loads_s3_base, allocation_zones, load_year
            ),
        )
        _save_with_manifest(
            save_output, output_df, spec, output_s3_base, storage_options
        )
        print("\n✓ ISO-NE bulk transmission MC generation completed and uploaded")
    else:
        print("\n✓ ISO-NE bulk transmission MC generation completed (inspect only)")


if __name__ == "__main__":
    main()

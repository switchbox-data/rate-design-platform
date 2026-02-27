"""Derive a seasonal TOU tariff and derivation spec from marginal costs.

Standalone script that loads bulk (Cambium) and distribution marginal costs,
computes seasonal TOU peak windows and cost-causation ratios, and writes:

1. **URDB v7 tariff JSON** — ready for CAIRO ``_initialize_tariffs``.
2. **Derivation spec JSON** — per-season peak windows, base rates, and
   cost-causation ratios, consumed by ``run_scenario.py`` for demand shifting.

The load-shifting step stays in ``run_scenario.py`` (Phase 2.5).  This script
replaces the old Phase 2 that was inlined inside ``run_scenario.run()``.

Usage (via Justfile)::

    just derive-seasonal-tou \\
        --cambium-path s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet \\
        --state RI --utility rie --year 2025 \\
        --path-td-marginal-costs s3://data.sb/switchbox/marginal_costs/ri/region=isone/utility=rie/year=2025/0000000.parquet \\
        --resstock-metadata-path /data.sb/nrel/resstock/.../metadata-sb.parquet \\
        --resstock-loads-path   /data.sb/nrel/resstock/.../load_curve_hourly/state=RI/upgrade=00 \\
        --path-electric-utility-stats s3://data.sb/eia/861/electric_utility_stats/state=RI/data.parquet \\
        --reference-tariff config/tariffs/electric/rie_flat_calibrated.json \\
        --tou-tariff-key rie_seasonal_tou_hp

Or directly::

    uv run python -m utils.pre.derive_seasonal_tou <args>
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import polars as pl
from cairo.rates_tool.loads import return_buildingstock

from utils import get_aws_region
from utils.loads import (
    ELECTRIC_LOAD_COL,
    hourly_system_load_from_resstock,
    scan_resstock_loads,
)
from utils.cairo import (
    _load_cambium_marginal_costs,
    load_distribution_marginal_costs,
)
from utils.mid.data_parsing import get_residential_customer_count_from_utility_stats
from utils.pre.compute_tou import (
    SeasonTouSpec,
    combine_marginal_costs,
    compute_seasonal_base_rates,
    compute_tou_cost_causation_ratio,
    find_tou_peak_window,
    make_winter_summer_seasons,
    save_season_specs,
    season_mask,
)
from utils.pre.create_tariff import (
    SeasonalTouTariffSpec,
    create_seasonal_tou_tariff,
    extract_base_rate_and_fixed_charge,
    write_tariff_json,
)
from utils.pre.season_config import (
    DEFAULT_TOU_WINTER_MONTHS,
    get_utility_periods_yaml_path,
    load_tou_window_hours_from_periods,
    load_winter_months_from_periods,
    parse_months_arg,
    resolve_winter_summer_months,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core derivation function
# ---------------------------------------------------------------------------


def derive_seasonal_tou(
    bulk_marginal_costs: pd.DataFrame,
    distribution_marginal_costs: pd.Series,
    hourly_system_load: pd.Series,
    *,
    winter_months: list[int] | None = None,
    tou_window_hours: int = 4,
    tou_base_rate: float = 0.06,
    tou_fixed_charge: float = 6.75,
    tou_tariff_key: str = "seasonal_tou_hp",
    utility: str = "GenericUtility",
) -> tuple[dict, list[SeasonTouSpec]]:
    """Derive a seasonal TOU tariff from marginal costs and system load.

    This is the pure-computation core — it takes pre-loaded data and
    returns the tariff and per-season derivation specs.

    Args:
        bulk_marginal_costs: Cambium energy + capacity MCs ($/kWh),
            indexed by time.
        distribution_marginal_costs: Distribution MCs ($/kWh) Series,
            indexed by time.
        hourly_system_load: Hourly aggregate system load (kW or kWh)
            for demand-weighting.
        winter_months: 1-indexed months defining winter. Summer months are
            derived as the complement.
        tou_window_hours: Width of the peak window in hours.
        tou_base_rate: Nominal base rate ($/kWh) — seasonal ratios are
            preserved but precalc will calibrate the absolute level.
        tou_fixed_charge: Fixed monthly charge ($).
        tou_tariff_key: Tariff key for HP customers.
        utility: Utility name for the tariff label.

    Returns:
        ``(tou_tariff, season_specs)`` where:

        - *tou_tariff* is a URDB v7 dict ready to write as JSON.
        - *season_specs* is a list of :class:`SeasonTouSpec` for
          downstream demand shifting.
    """
    combined_mc = combine_marginal_costs(
        bulk_marginal_costs, distribution_marginal_costs
    )

    # Align load index to MC index so multiply in find_tou_peak_window is 1:1 (MC is
    # target_year e.g. 2025; ResStock load may be a different year).
    if not hourly_system_load.index.equals(combined_mc.index):
        if len(hourly_system_load) == len(combined_mc):
            hourly_system_load = pd.Series(
                hourly_system_load.values,
                index=combined_mc.index,
                name=hourly_system_load.name,
            )
        else:
            hourly_system_load = hourly_system_load.reindex(
                combined_mc.index, method="ffill"
            )

    seasons = make_winter_summer_seasons(winter_months)
    season_rates = compute_seasonal_base_rates(
        combined_mc, hourly_system_load, seasons, tou_base_rate
    )

    season_specs: list[SeasonTouSpec] = []
    mc_index = pd.DatetimeIndex(combined_mc.index)

    for s in seasons:
        mask = season_mask(mc_index, s)
        peak_hours = find_tou_peak_window(
            combined_mc[mask],
            hourly_system_load[mask],
            tou_window_hours,
        )
        ratio = compute_tou_cost_causation_ratio(
            combined_mc[mask],
            hourly_system_load[mask],
            peak_hours,
        )
        season_specs.append(
            SeasonTouSpec(
                season=s,
                base_rate=season_rates[s.name],
                peak_hours=peak_hours,
                peak_offpeak_ratio=ratio,
            )
        )

    tou_tariff = create_seasonal_tou_tariff(
        label=tou_tariff_key,
        specs=[
            SeasonalTouTariffSpec(
                months=spec.season.months,
                base_rate=spec.base_rate,
                peak_hours=spec.peak_hours,
                peak_offpeak_ratio=spec.peak_offpeak_ratio,
            )
            for spec in season_specs
        ],
        fixed_charge=tou_fixed_charge,
        utility=utility,
    )

    return tou_tariff, season_specs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Derive a seasonal TOU tariff from Cambium + distribution marginal "
            "costs.  Writes tariff JSON and derivation spec JSON."
        ),
    )

    # -- Marginal cost inputs ------------------------------------------------
    p.add_argument(
        "--cambium-path",
        required=True,
        help="Path (local or s3://) to Cambium bulk marginal cost file.",
    )
    p.add_argument("--state", required=True, help="State code (e.g. RI).")
    p.add_argument(
        "--utility",
        required=True,
        help="Utility short name (e.g. rie).",
    )
    p.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for marginal cost data.",
    )
    p.add_argument(
        "--path-td-marginal-costs",
        required=True,
        help="Path (local or s3://) to T&D marginal cost parquet.",
    )

    # -- ResStock inputs (for system load weighting) --------------------------
    p.add_argument(
        "--resstock-metadata-path",
        required=True,
        help="Path to ResStock metadata parquet (metadata-sb.parquet).",
    )
    p.add_argument(
        "--resstock-base",
        required=True,
        help="Base path to ResStock release (e.g. s3://.../res_2024_amy2018_2).",
    )
    p.add_argument(
        "--upgrade",
        default="00",
        help="Upgrade partition for loads (e.g. 00).",
    )
    p.add_argument(
        "--path-electric-utility-stats",
        required=True,
        help="Path (local or s3://) to EIA-861 utility stats parquet for customer count lookup.",
    )

    # -- TOU derivation parameters -------------------------------------------
    p.add_argument(
        "--winter-months",
        default=None,
        help=("Comma-separated 1-indexed winter months (default: 1,2,3,4,5,10,11,12)."),
    )
    p.add_argument(
        "--summer-months",
        default=None,
        help=(
            "Deprecated: comma-separated summer months. "
            "If both are provided, --winter-months takes precedence."
        ),
    )
    p.add_argument(
        "--tou-window-hours",
        type=int,
        default=None,
        help="Peak window width in hours. Falls back to periods YAML or 4.",
    )
    p.add_argument(
        "--reference-tariff",
        default=None,
        help=(
            "Path to a URDB v7 tariff JSON from which base rate and fixed "
            "charge are inferred. Overridden by explicit --tou-base-rate / "
            "--tou-fixed-charge when both are provided."
        ),
    )
    p.add_argument(
        "--tou-base-rate",
        type=float,
        default=None,
        help="Nominal base rate $/kWh. Overrides value from --reference-tariff.",
    )
    p.add_argument(
        "--tou-fixed-charge",
        type=float,
        default=None,
        help="Fixed monthly charge $. Overrides value from --reference-tariff.",
    )

    # -- Tariff keys ---------------------------------------------------------
    p.add_argument(
        "--tou-tariff-key",
        required=True,
        help="Tariff key for HP customers (e.g. rie_seasonal_tou_hp).",
    )
    # -- Output --------------------------------------------------------------
    p.add_argument(
        "--output-dir",
        required=True,
        help="Output directory (tariffs/electric and tou_derivation subdirs will be created).",
    )
    p.add_argument(
        "--periods-yaml",
        default=None,
        help=(
            "Optional periods YAML containing `winter_months` and "
            "`tou_window_hours`. When omitted, resolves "
            "rate_design/hp_rates/<state>/config/periods/<utility>.yaml."
        ),
    )
    p.add_argument(
        "--run-dir",
        default=None,
        help=(
            "Optional path (local or s3://) to a CAIRO output directory. "
            "When provided, building IDs are read from "
            "run_dir/customer_metadata.csv and passed as building_stock_sample "
            "to return_buildingstock, restricting the TOU derivation to the "
            "exact customer set from that run."
        ),
    )

    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(name)s %(levelname)s: %(message)s"
    )
    args = _parse_args()

    project_root = Path(__file__).resolve().parents[2]
    periods_yaml_path = (
        Path(args.periods_yaml)
        if args.periods_yaml
        else get_utility_periods_yaml_path(
            project_root=project_root,
            state=args.state,
            utility=args.utility,
        )
    )

    default_winter_months = list(DEFAULT_TOU_WINTER_MONTHS)
    default_tou_window_hours = 4
    if periods_yaml_path.exists():
        default_winter_months = load_winter_months_from_periods(
            periods_yaml_path,
            default_winter_months=DEFAULT_TOU_WINTER_MONTHS,
        )
        default_tou_window_hours = load_tou_window_hours_from_periods(
            periods_yaml_path,
            default_tou_window_hours=default_tou_window_hours,
        )

    if args.summer_months:
        summer_months = parse_months_arg(args.summer_months)
        fallback_winter = [
            month for month in range(1, 13) if month not in summer_months
        ]
    else:
        fallback_winter = default_winter_months

    winter_months, summer_months = resolve_winter_summer_months(
        parse_months_arg(args.winter_months) if args.winter_months else None,
        default_winter_months=fallback_winter,
    )
    tou_window_hours = (
        args.tou_window_hours
        if args.tou_window_hours is not None
        else default_tou_window_hours
    )
    output_dir = Path(args.output_dir)

    # -- Resolve base rate and fixed charge ----------------------------------
    ref_base_rate: float | None = None
    ref_fixed_charge: float | None = None
    if args.reference_tariff is not None:
        ref_base_rate, ref_fixed_charge = extract_base_rate_and_fixed_charge(
            Path(args.reference_tariff)
        )
        log.info(
            "Reference tariff %s: base_rate=%.6f, fixed_charge=%.2f",
            args.reference_tariff,
            ref_base_rate,
            ref_fixed_charge,
        )

    tou_base_rate = (
        args.tou_base_rate if args.tou_base_rate is not None else ref_base_rate
    )
    tou_fixed_charge = (
        args.tou_fixed_charge if args.tou_fixed_charge is not None else ref_fixed_charge
    )

    if tou_base_rate is None or tou_fixed_charge is None:
        raise SystemExit(
            "Either --reference-tariff or both --tou-base-rate and "
            "--tou-fixed-charge must be provided."
        )

    # -- 1. Load data --------------------------------------------------------
    log.info("Loading Cambium bulk marginal costs from %s", args.cambium_path)
    bulk_mc = _load_cambium_marginal_costs(args.cambium_path, args.year)

    log.info(
        "Loading distribution marginal costs from %s",
        args.path_td_marginal_costs,
    )
    dist_mc = load_distribution_marginal_costs(args.path_td_marginal_costs)

    log.info(
        "Looking up residential customer count from %s for utility=%s",
        args.path_electric_utility_stats,
        args.utility,
    )
    storage_options = {"aws_region": get_aws_region()}
    customer_count = get_residential_customer_count_from_utility_stats(
        args.path_electric_utility_stats,
        args.utility,
        storage_options=storage_options,
    )
    log.info("Residential customer count: %d", customer_count)

    building_stock_sample: list[int] | None = None
    if args.run_dir:
        run_dir_path = args.run_dir.rstrip("/")
        cm_path = f"{run_dir_path}/customer_metadata.csv"
        log.info("Reading building IDs from %s", cm_path)
        if cm_path.startswith("s3://"):
            try:
                # Avoid get_aws_storage_options() here: its "region" key is not
                # accepted by some fsspec/s3fs code paths used by read_csv.
                run_bldg_ids = pl.read_csv(cm_path, columns=["bldg_id"])[
                    "bldg_id"
                ].to_list()
            except TypeError:
                # Compatibility fallback for older fsspec/s3fs stacks.
                run_bldg_ids = pl.read_csv(
                    cm_path,
                    columns=["bldg_id"],
                    storage_options={
                        "client_kwargs": {"region_name": get_aws_region()}
                    },
                )["bldg_id"].to_list()
        else:
            run_bldg_ids = pl.read_csv(cm_path, columns=["bldg_id"])[
                "bldg_id"
            ].to_list()
        building_stock_sample = run_bldg_ids
        log.info("Restricting to %d buildings from run output", len(run_bldg_ids))

    log.info("Loading ResStock metadata from %s", args.resstock_metadata_path)
    customer_metadata = return_buildingstock(
        load_scenario=Path(args.resstock_metadata_path),
        customer_count=customer_count,
        columns=["applicability", "postprocess_group.has_hp"],
        building_stock_sample=building_stock_sample,
    )

    building_ids = customer_metadata["bldg_id"].tolist()
    weights_df = pl.from_pandas(customer_metadata[["bldg_id", "weight"]].copy())
    log.info(
        "Scanning ResStock loads (base=%s, state=%s, upgrade=%s, n_buildings=%d)",
        args.resstock_base,
        args.state,
        args.upgrade,
        len(building_ids),
    )
    loads_lf = scan_resstock_loads(
        args.resstock_base,
        args.state,
        args.upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    log.info("Computing system load from building loads")
    hourly_system_load = hourly_system_load_from_resstock(
        loads_lf,
        weights_df,
        load_col=ELECTRIC_LOAD_COL,
    )

    # -- 2. Derive seasonal TOU ----------------------------------------------
    log.info(
        "Deriving seasonal TOU (winter=%s, summer=%s, window=%d h, base_rate=%.4f)",
        winter_months,
        summer_months,
        tou_window_hours,
        tou_base_rate,
    )
    tou_tariff, season_specs = derive_seasonal_tou(
        bulk_marginal_costs=bulk_mc,
        distribution_marginal_costs=dist_mc,
        hourly_system_load=hourly_system_load,
        winter_months=winter_months,
        tou_window_hours=tou_window_hours,
        tou_base_rate=tou_base_rate,
        tou_fixed_charge=tou_fixed_charge,
        tou_tariff_key=args.tou_tariff_key,
        utility=args.utility,
    )

    # -- 3. Write outputs ----------------------------------------------------
    tariff_path = output_dir / "tariffs" / "electric" / f"{args.tou_tariff_key}.json"
    tariff_path.parent.mkdir(parents=True, exist_ok=True)
    write_tariff_json(tou_tariff, tariff_path)
    log.info("Wrote TOU tariff JSON: %s", tariff_path)

    derivation_path = (
        output_dir / "tou_derivation" / f"{args.tou_tariff_key}_derivation.json"
    )
    save_season_specs(season_specs, derivation_path)
    log.info("Wrote derivation spec JSON: %s", derivation_path)

    # -- 4. Summary ----------------------------------------------------------
    for spec in season_specs:
        log.info(
            "  %s: base_rate=$%.6f, peak_hours=%s, ratio=%.4f",
            spec.season.name,
            spec.base_rate,
            spec.peak_hours,
            spec.peak_offpeak_ratio,
        )
    log.info("Done.")


if __name__ == "__main__":
    main()

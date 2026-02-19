"""Derive a seasonal TOU tariff, tariff map, and derivation spec from marginal costs.

Standalone script that loads bulk (Cambium) and distribution marginal costs,
computes seasonal TOU peak windows and cost-causation ratios, and writes:

1. **URDB v7 tariff JSON** — ready for CAIRO ``_initialize_tariffs``.
2. **Tariff map CSV** — HP customers → TOU tariff, everyone else → flat.
3. **Derivation spec JSON** — per-season peak windows, base rates, and
   cost-causation ratios, consumed by ``run_scenario.py`` for demand shifting.

The load-shifting step stays in ``run_scenario.py`` (Phase 2.5).  This script
replaces the old Phase 2 that was inlined inside ``run_scenario.run()``.

Usage (via Justfile)::

    just derive-seasonal-tou \\
        --cambium-path s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet \\
        --state RI --region isone --utility rie --year 2025 \\
        --resstock-metadata-path /data.sb/nrel/resstock/.../metadata-sb.parquet \\
        --resstock-loads-path   /data.sb/nrel/resstock/.../load_curve_hourly/state=RI/upgrade=00 \\
        --customer-count 451381 \\
        --tou-tariff-key rie_seasonal_tou_hp --flat-tariff-key rie_a16

Or directly::

    uv run python -m utils.pre.derive_seasonal_tou <args>
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
from cairo.rates_tool.loads import (
    _return_load,
    process_residential_hourly_demand,
    return_buildingstock,
)

from utils.cairo import (
    _load_cambium_marginal_costs,
    build_bldg_id_to_load_filepath,
    load_distribution_marginal_costs,
)
from utils.pre.compute_tou import (
    SeasonTouSpec,
    combine_marginal_costs,
    compute_seasonal_base_rates,
    compute_tou_cost_causation_ratio,
    find_tou_peak_window,
    generate_tou_tariff_map,
    make_winter_summer_seasons,
    save_season_specs,
    season_mask,
)
from utils.pre.create_tariff import (
    SeasonalTouTariffSpec,
    create_seasonal_tou_tariff,
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
    customer_metadata: pd.DataFrame,
    *,
    winter_months: list[int] | None = None,
    tou_window_hours: int = 4,
    tou_base_rate: float = 0.06,
    tou_fixed_charge: float = 6.75,
    tou_tariff_key: str = "seasonal_tou_hp",
    flat_tariff_key: str = "flat",
    utility: str = "GenericUtility",
) -> tuple[dict, pd.DataFrame, list[SeasonTouSpec]]:
    """Derive a seasonal TOU tariff from marginal costs and system load.

    This is the pure-computation core — it takes pre-loaded data and
    returns the tariff, tariff map, and per-season derivation specs.

    Args:
        bulk_marginal_costs: Cambium energy + capacity MCs ($/kWh),
            indexed by time.
        distribution_marginal_costs: Distribution MCs ($/kWh) Series,
            indexed by time.
        hourly_system_load: Hourly aggregate system load (kW or kWh)
            for demand-weighting.
        customer_metadata: ResStock metadata with ``bldg_id`` and
            ``postprocess_group.has_hp`` columns.
        winter_months: 1-indexed months defining winter. Summer months are
            derived as the complement.
        tou_window_hours: Width of the peak window in hours.
        tou_base_rate: Nominal base rate ($/kWh) — seasonal ratios are
            preserved but precalc will calibrate the absolute level.
        tou_fixed_charge: Fixed monthly charge ($).
        tou_tariff_key: Tariff key for HP customers.
        flat_tariff_key: Tariff key for non-HP customers.
        utility: Utility name for the tariff label.

    Returns:
        ``(tou_tariff, tariff_map_df, season_specs)`` where:

        - *tou_tariff* is a URDB v7 dict ready to write as JSON.
        - *tariff_map_df* is a ``bldg_id, tariff_key`` DataFrame.
        - *season_specs* is a list of :class:`SeasonTouSpec` for
          downstream demand shifting.
    """
    combined_mc = combine_marginal_costs(
        bulk_marginal_costs, distribution_marginal_costs
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

    tariff_map_df = generate_tou_tariff_map(
        customer_metadata=customer_metadata,
        tou_tariff_key=tou_tariff_key,
        flat_tariff_key=flat_tariff_key,
    )

    return tou_tariff, tariff_map_df, season_specs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Derive a seasonal TOU tariff from Cambium + distribution marginal "
            "costs.  Writes tariff JSON, tariff map CSV, and derivation spec JSON."
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
        "--region",
        required=True,
        help="ISO region for distribution MC (e.g. isone).",
    )
    p.add_argument(
        "--utility",
        required=True,
        help="Utility short name for distribution MC (e.g. rie).",
    )
    p.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for marginal cost data.",
    )

    # -- ResStock inputs (for system load + tariff map) ----------------------
    p.add_argument(
        "--resstock-metadata-path",
        required=True,
        help="Path to ResStock metadata parquet (metadata-sb.parquet).",
    )
    p.add_argument(
        "--resstock-loads-path",
        required=True,
        help="Directory containing per-building load parquet files.",
    )
    p.add_argument(
        "--customer-count",
        type=int,
        required=True,
        help="Utility customer count for metadata weighting.",
    )

    # -- TOU derivation parameters -------------------------------------------
    p.add_argument(
        "--winter-months",
        default=None,
        help=(
            "Comma-separated 1-indexed winter months "
            "(default: 1,2,3,4,5,10,11,12)."
        ),
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
        "--tou-base-rate",
        type=float,
        default=0.06,
        help="Nominal base rate $/kWh (default 0.06).",
    )
    p.add_argument(
        "--tou-fixed-charge",
        type=float,
        default=6.75,
        help="Fixed monthly charge $ (default 6.75).",
    )

    # -- Tariff keys ---------------------------------------------------------
    p.add_argument(
        "--tou-tariff-key",
        required=True,
        help="Tariff key for HP customers (e.g. rie_seasonal_tou_hp).",
    )
    p.add_argument(
        "--flat-tariff-key",
        required=True,
        help="Tariff key for non-HP customers (e.g. rie_a16).",
    )

    # -- Output --------------------------------------------------------------
    p.add_argument(
        "--output-dir",
        required=True,
        help="Output directory (tariffs/electric, tariff_maps/electric, "
        "tou_derivation subdirs will be created).",
    )
    p.add_argument(
        "--periods-yaml",
        default=None,
        help=(
            "Optional periods YAML containing `winter_months` and "
            "`tou_window_hours`. When omitted, resolves "
            "rate_design/<state>/hp_rates/config/periods/<utility>.yaml."
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
        fallback_winter = [month for month in range(1, 13) if month not in summer_months]
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

    # -- 1. Load data --------------------------------------------------------
    log.info("Loading Cambium bulk marginal costs from %s", args.cambium_path)
    bulk_mc = _load_cambium_marginal_costs(args.cambium_path, args.year)

    log.info(
        "Loading distribution marginal costs (state=%s, region=%s, utility=%s, year=%d)",
        args.state,
        args.region,
        args.utility,
        args.year,
    )
    dist_mc = load_distribution_marginal_costs(
        state=args.state,
        region=args.region,
        utility=args.utility,
        year_run=args.year,
    )

    log.info("Loading ResStock metadata from %s", args.resstock_metadata_path)
    customer_metadata = return_buildingstock(
        load_scenario=Path(args.resstock_metadata_path),
        customer_count=args.customer_count,
        columns=["applicability", "postprocess_group.has_hp"],
    )

    log.info("Loading building loads from %s", args.resstock_loads_path)
    bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
        path_resstock_loads=Path(args.resstock_loads_path),
    )
    raw_load_elec = _return_load(
        load_type="electricity",
        target_year=args.year,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )

    log.info("Computing system load from building loads")
    hourly_system_load = process_residential_hourly_demand(
        bldg_load=raw_load_elec,
        sample_weights=customer_metadata[["bldg_id", "weight"]],
    )

    # -- 2. Derive seasonal TOU ----------------------------------------------
    log.info(
        "Deriving seasonal TOU (winter=%s, summer=%s, window=%d h, base_rate=%.4f)",
        winter_months,
        summer_months,
        tou_window_hours,
        args.tou_base_rate,
    )
    tou_tariff, tariff_map_df, season_specs = derive_seasonal_tou(
        bulk_marginal_costs=bulk_mc,
        distribution_marginal_costs=dist_mc,
        hourly_system_load=hourly_system_load,
        customer_metadata=customer_metadata,
        winter_months=winter_months,
        tou_window_hours=tou_window_hours,
        tou_base_rate=args.tou_base_rate,
        tou_fixed_charge=args.tou_fixed_charge,
        tou_tariff_key=args.tou_tariff_key,
        flat_tariff_key=args.flat_tariff_key,
        utility=args.utility,
    )

    # -- 3. Write outputs ----------------------------------------------------
    tariff_path = output_dir / "tariffs" / "electric" / f"{args.tou_tariff_key}.json"
    tariff_path.parent.mkdir(parents=True, exist_ok=True)
    write_tariff_json(tou_tariff, tariff_path)
    log.info("Wrote TOU tariff JSON: %s", tariff_path)

    map_path = (
        output_dir
        / "tariff_maps"
        / "electric"
        / f"{args.tou_tariff_key}_tariff_map.csv"
    )
    map_path.parent.mkdir(parents=True, exist_ok=True)
    tariff_map_df.to_csv(map_path, index=False)
    log.info("Wrote tariff map CSV: %s", map_path)

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

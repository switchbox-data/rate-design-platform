"""Derive a seasonal TOU tariff and derivation spec from marginal costs.

Standalone script that loads bulk (Cambium) and dist+sub-tx marginal costs,
computes seasonal TOU peak windows and cost-causation ratios, and writes:

1. **URDB v7 tariff JSON** — ready for CAIRO ``_initialize_tariffs``.
2. **Derivation spec JSON** — per-season peak windows, base rates, and
   cost-causation ratios, consumed by ``run_scenario.py`` for demand shifting.

The load-shifting step stays in ``run_scenario.py`` (Phase 2.5).  This script
replaces the old Phase 2 that was inlined inside ``run_scenario.run()``.

Usage (via Justfile)::

    just derive-seasonal-tou \\
        --path-supply-energy-mc s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet \\
        --path-supply-capacity-mc s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet \\
        --state RI --utility rie --year 2025 \\
        --path-dist-and-sub-tx-mc s3://data.sb/switchbox/marginal_costs/ri/dist_and_sub_tx/utility=rie/year=2025/data.parquet \\
        --path-bulk-tx-mc s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility=nyseg/year=2025/data.parquet \\
        --path-utility-assignment /data.sb/nrel/resstock/.../metadata_utility \\
        --path-electric-utility-stats s3://data.sb/eia/861/electric_utility_stats/state=RI/data.parquet \\
        --reference-tariff config/tariffs/electric/rie_flat_calibrated.json \\
        --tou-tariff-key rie_seasonal_tou_hp

Note: ``--path-supply-energy-mc`` and ``--path-supply-capacity-mc`` accept either
separate NYISO-style parquets or the same Cambium path for both (detected
automatically when the path contains ``cambium``).

Or directly::

    uv run python -m utils.pre.derive_seasonal_tou <args>
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import polars as pl

from utils import get_aws_region
from utils.cairo import (
    _load_supply_marginal_costs,  # noqa: PLC2701
    load_bulk_tx_marginal_costs,
    load_dist_and_sub_tx_marginal_costs,
)
from utils.loads import (
    ELECTRIC_LOAD_COL,
    hourly_resstock_load_from_parquet,
    scan_resstock_loads,
)
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
from utils.scenario_config import get_residential_customer_count_from_utility_stats

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_tou_inputs(
    path_supply_energy_mc: str,
    path_supply_capacity_mc: str,
    year: int,
    path_dist_and_sub_tx_mc: str,
    path_utility_assignment: str,
    resstock_base: str,
    state: str,
    upgrade: str,
    path_electric_utility_stats: str,
    utility: str,
    *,
    path_bulk_tx_mc: str | None = None,
    run_dir: str | None = None,
    has_hp_filter: set[bool] | None = None,
) -> tuple[pd.DataFrame, pd.Series, pd.Series | None, pd.Series]:
    """Load marginal costs and hourly ResStock load for TOU derivation.

    Building metadata always comes from ``utility_assignment.parquet``:

    1. Filtered by ``sb.electric_utility == utility``.
    2. Weights are rescaled to the EIA residential customer count (on
       **all** utility buildings, before any subclass filter).
    3. Optionally filtered by ``postprocess_group.has_hp`` via
       *has_hp_filter*.  Because weight rescaling happens first, filtered
       buildings keep weights representing their actual share of the
       residential population, not the entire class.
    4. If *run_dir* is provided, further restricted to ``bldg_id`` values
       in ``{run_dir}/customer_metadata.csv`` (CAIRO's actual sample).

    Returns:
        ``(bulk_mc, dist_mc, bulk_tx_mc, hourly_resstock_load)`` where:

        - *bulk_mc* is a DataFrame of supply energy + capacity MCs.
        - *dist_mc* is a Series of dist+sub-tx MCs.
        - *bulk_tx_mc* is an optional Series of bulk-tx MCs (or ``None``).
        - *hourly_resstock_load* is a Series of weighted aggregate load.
    """
    log.info(
        "Loading supply marginal costs (energy=%s, capacity=%s)",
        path_supply_energy_mc,
        path_supply_capacity_mc,
    )
    bulk_mc = _load_supply_marginal_costs(
        path_supply_energy_mc,
        path_supply_capacity_mc,
        year,
    )

    log.info("Loading dist+sub-tx marginal costs from %s", path_dist_and_sub_tx_mc)
    dist_mc = load_dist_and_sub_tx_marginal_costs(path_dist_and_sub_tx_mc)

    bulk_tx_mc: pd.Series | None = None
    if path_bulk_tx_mc:
        log.info("Loading bulk transmission marginal costs from %s", path_bulk_tx_mc)
        bulk_tx_mc = load_bulk_tx_marginal_costs(path_bulk_tx_mc)

    # -- Building metadata from utility_assignment.parquet -------------------
    # Weight rescaling must happen on ALL utility buildings before any
    # subclass or run_dir filter, so that filtered subsets keep weights
    # representing their actual share of the residential population (not
    # the entire class).
    log.info("Loading utility assignment from %s", path_utility_assignment)
    storage_options = {"aws_region": get_aws_region()}
    customer_count = get_residential_customer_count_from_utility_stats(
        path_electric_utility_stats,
        utility,
        storage_options=storage_options,
    )
    log.info("Residential customer count: %d", customer_count)

    ua_lf = pl.scan_parquet(path_utility_assignment)
    ua_lf = ua_lf.filter(pl.col("sb.electric_utility") == utility)
    ua_df: pl.DataFrame = ua_lf.select(  # type: ignore[assignment]
        "bldg_id", "weight", "postprocess_group.has_hp"
    ).collect()
    log.info("All buildings for utility=%s: %d", utility, len(ua_df))

    raw_weight_sum = float(ua_df["weight"].sum())
    if raw_weight_sum > 0:
        ua_df = ua_df.with_columns(
            (pl.col("weight") / raw_weight_sum * customer_count).alias("weight")
        )

    if has_hp_filter is not None:
        ua_df = ua_df.filter(
            pl.col("postprocess_group.has_hp").is_in(list(has_hp_filter))
        )
        log.info("Filtered to has_hp in %s: %d buildings", has_hp_filter, len(ua_df))

    if run_dir:
        run_dir_path = run_dir.rstrip("/")
        cm_path = f"{run_dir_path}/customer_metadata.csv"
        log.info("Reading building IDs from %s", cm_path)
        if cm_path.startswith("s3://"):
            try:
                run_bldg_ids = pl.read_csv(cm_path, columns=["bldg_id"])[
                    "bldg_id"
                ].to_list()
            except TypeError:
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
        ua_df = ua_df.filter(pl.col("bldg_id").is_in(run_bldg_ids))
        log.info("Restricted to %d buildings from run output", len(run_bldg_ids))

    ua_df = ua_df.select("bldg_id", "weight")
    log.info("Selected %d buildings for utility=%s", len(ua_df), utility)

    building_ids = ua_df["bldg_id"].to_list()
    weights_df = ua_df.select(
        pl.col("bldg_id").cast(pl.Int64),
        pl.col("weight").cast(pl.Float64),
    )

    log.info(
        "Scanning ResStock loads (base=%s, state=%s, upgrade=%s, n_buildings=%d)",
        resstock_base,
        state,
        upgrade,
        len(building_ids),
    )
    loads_lf = scan_resstock_loads(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    log.info("Computing hourly ResStock load from building loads")
    hourly_resstock_load = hourly_resstock_load_from_parquet(
        loads_lf,
        weights_df,
        load_col=ELECTRIC_LOAD_COL,
    )

    return bulk_mc, dist_mc, bulk_tx_mc, hourly_resstock_load


# ---------------------------------------------------------------------------
# Core derivation function
# ---------------------------------------------------------------------------


def derive_seasonal_tou(
    bulk_marginal_costs: pd.DataFrame,
    dist_and_sub_tx_marginal_costs: pd.Series,
    hourly_load: pd.Series,
    *,
    bulk_tx_marginal_costs: pd.Series | None = None,
    winter_months: list[int] | None = None,
    tou_window_hours: int = 4,
    tou_base_rate: float = 0.06,
    tou_fixed_charge: float = 6.75,
    tou_tariff_key: str = "seasonal_tou_hp",
    utility: str = "GenericUtility",
) -> tuple[dict, list[SeasonTouSpec]]:
    """Derive a seasonal TOU tariff from marginal costs and load.

    This is the pure-computation core — it takes pre-loaded data and
    returns the tariff and per-season derivation specs.

    Args:
        bulk_marginal_costs: Cambium energy + capacity MCs ($/kWh),
            indexed by time.
        dist_and_sub_tx_marginal_costs: Dist+sub-tx MCs ($/kWh) Series,
            indexed by time.
        hourly_load: Hourly aggregate load (kW or kWh)
            for demand-weighting.
        bulk_tx_marginal_costs: Optional utility-level bulk transmission
            MCs ($/kWh) Series indexed by time.  When provided, this is
            added to the combined MC so that TOU peak windows and
            cost-causation ratios reflect all delivery-side costs.
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
        bulk_marginal_costs,
        dist_and_sub_tx_marginal_costs,
        bulk_tx_marginal_costs,
    )

    # Align load index to MC index so multiply in find_tou_peak_window is 1:1 (MC is
    # target_year e.g. 2025; ResStock load may be a different year).
    if not hourly_load.index.equals(combined_mc.index):
        if len(hourly_load) == len(combined_mc):
            hourly_load = pd.Series(
                hourly_load.values,
                index=combined_mc.index,
                name=hourly_load.name,
            )
        else:
            hourly_load = hourly_load.reindex(combined_mc.index, method="ffill")

    seasons = make_winter_summer_seasons(winter_months)
    season_rates = compute_seasonal_base_rates(
        combined_mc, hourly_load, seasons, tou_base_rate
    )

    season_specs: list[SeasonTouSpec] = []
    mc_index = pd.DatetimeIndex(combined_mc.index)

    for s in seasons:
        mask = season_mask(mc_index, s)
        peak_hours = find_tou_peak_window(
            combined_mc[mask],
            hourly_load[mask],
            tou_window_hours,
        )
        ratio = compute_tou_cost_causation_ratio(
            combined_mc[mask],
            hourly_load[mask],
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
            "Derive a seasonal TOU tariff from supply (energy + capacity) and "
            "dist+sub-tx marginal costs.  Writes tariff JSON and derivation spec JSON."
        ),
    )

    # -- Marginal cost inputs ------------------------------------------------
    p.add_argument(
        "--path-supply-energy-mc",
        required=True,
        help=(
            "Path (local or s3://) to supply energy MC parquet.  "
            "Pass a Cambium path here (and also to --path-supply-capacity-mc) "
            "for Cambium-based states; the loader detects 'cambium' in the path "
            "and routes to the combined Cambium loader automatically."
        ),
    )
    p.add_argument(
        "--path-supply-capacity-mc",
        required=True,
        help=(
            "Path (local or s3://) to supply capacity MC parquet.  "
            "For Cambium-based states, set to the same Cambium path as "
            "--path-supply-energy-mc."
        ),
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
        "--path-dist-and-sub-tx-mc",
        required=True,
        help="Path (local or s3://) to dist+sub-tx marginal cost parquet.",
    )
    p.add_argument(
        "--path-bulk-tx-mc",
        default=None,
        help=(
            "Optional path (local or s3://) to utility-level bulk transmission "
            "marginal cost parquet.  When provided, bulk-tx costs are included "
            "in the combined MC used to derive TOU peak windows and ratios."
        ),
    )

    # -- ResStock inputs (for load weighting) ----------------------------------
    p.add_argument(
        "--path-utility-assignment",
        required=True,
        help="Path to utility_assignment.parquet (bldg_id, weight, sb.electric_utility, has_hp).",
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
            "When provided, building IDs from run_dir/customer_metadata.csv "
            "further restrict the building set (on top of utility filtering)."
        ),
    )
    p.add_argument(
        "--has-hp",
        default="true",
        help=(
            "Filter buildings by HP status: 'true' (HP only, default), "
            "'false' (non-HP only), or 'all' (no filter)."
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

    # -- Parse has_hp filter ---------------------------------------------------
    has_hp_raw = args.has_hp.strip().lower()
    if has_hp_raw == "all":
        has_hp_filter: set[bool] | None = None
    elif has_hp_raw == "true":
        has_hp_filter = {True}
    elif has_hp_raw == "false":
        has_hp_filter = {False}
    else:
        raise SystemExit(
            f"--has-hp must be 'true', 'false', or 'all', got '{args.has_hp}'"
        )

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
    bulk_mc, dist_mc, bulk_tx_mc, hourly_load = load_tou_inputs(
        path_supply_energy_mc=args.path_supply_energy_mc,
        path_supply_capacity_mc=args.path_supply_capacity_mc,
        year=args.year,
        path_dist_and_sub_tx_mc=args.path_dist_and_sub_tx_mc,
        path_utility_assignment=args.path_utility_assignment,
        resstock_base=args.resstock_base,
        state=args.state,
        upgrade=args.upgrade,
        path_electric_utility_stats=args.path_electric_utility_stats,
        utility=args.utility,
        path_bulk_tx_mc=args.path_bulk_tx_mc,
        run_dir=args.run_dir,
        has_hp_filter=has_hp_filter,
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
        dist_and_sub_tx_marginal_costs=dist_mc,
        hourly_load=hourly_load,
        bulk_tx_marginal_costs=bulk_tx_mc,
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

"""Entrypoint for running heat pump rate scenarios - Residential (non-LMI)."""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import random
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import dask
import pandas as pd
import polars as pl
import yaml
from cairo.rates_tool.loads import (
    return_buildingstock,
)
from cairo.rates_tool.systemsimulator import (
    MeetRevenueSufficiencySystemWide,
    _initialize_tariffs,
    _return_export_compensation_rate,
    _return_revenue_requirement_target,
)

from utils import get_aws_region
from utils.cairo import (
    _fetch_prototype_ids_by_electric_util,
    _load_cambium_marginal_costs,
    _load_supply_marginal_costs,
    build_bldg_id_to_load_filepath,
    load_distribution_marginal_costs,
)
from utils.demand_flex import apply_demand_flex, split_revenue_requirement_by_tou
from utils.scenario_config import (
    RevenueRequirementConfig,
    _parse_bool,
    _parse_float,
    _parse_int,
    _parse_path_tariffs,
    _parse_path_tariffs_gas,
    _parse_utility_revenue_requirement,
    _resolve_path,
    _resolve_path_or_uri,
    get_residential_customer_count_from_utility_stats,
)
from utils.mid.patches import _return_loads_combined
from utils.pre.generate_precalc_mapping import generate_default_precalc_mapping
from utils.types import ElectricUtility

log = logging.getLogger("rates_analysis").getChild("run_scenario")


def _storage_options() -> dict[str, str]:
    return {"aws_region": get_aws_region()}


# Resolve paths relative to this script so the scenario can be run from any CWD.
# Config lives under rate_design/hp_rates/{state}/config/.
PATH_PROJECT = Path(__file__).resolve().parent


def _state_config(state: str) -> Path:
    return PATH_PROJECT / state.lower() / "config"


@contextlib.contextmanager
def _timed(label: str) -> Iterator[None]:
    t0 = time.perf_counter()
    yield
    log.info("TIMING %s: %.1fs", label, time.perf_counter() - t0)


def _scenario_config_from_utility(state: str, utility: str) -> Path:
    return _state_config(state) / "scenarios" / f"scenarios_{utility}.yaml"


@dataclass(slots=True)
class ScenarioSettings:
    """All inputs needed to run one scenario."""

    run_name: str
    run_type: str
    state: str
    utility: str
    path_results: Path
    path_resstock_metadata: Path
    path_resstock_loads: Path
    path_utility_assignment: str | Path
    path_td_marginal_costs: str | Path
    path_tariff_maps_electric: Path
    path_tariff_maps_gas: Path
    path_tariffs_electric: dict[str, Path]
    path_tariffs_gas: dict[str, Path]
    rr_total: float
    subclass_rr: dict[str, float] | None
    run_includes_subclasses: bool
    path_electric_utility_stats: str | Path
    year_run: int
    year_dollar_conversion: int
    process_workers: int
    path_supply_marginal_costs: str | Path | None = None
    path_supply_energy_mc: str | Path | None = None
    path_supply_capacity_mc: str | Path | None = None
    solar_pv_compensation: str = "net_metering"
    run_includes_supply: bool = False
    sample_size: int | None = None
    elasticity: float = 0.0
    path_tou_supply_mc: str | Path | None = None


def apply_prototype_sample(
    prototype_ids: list[int],
    sample_size: int | None,
    *,
    rng: random.Random | None = None,
) -> list[int]:
    """Optionally take a uniform random sample of prototype IDs.

    When sample_size is None, returns prototype_ids unchanged.
    When sample_size is set, returns a random sample of that size without replacement.
    Raises ValueError if sample_size is not positive or exceeds the number of prototype IDs.
    """
    if sample_size is None:
        return prototype_ids
    if sample_size <= 0:
        raise ValueError(f"sample_size must be positive, got {sample_size}")
    if sample_size > len(prototype_ids):
        raise ValueError(
            f"sample_size {sample_size} exceeds number of prototype IDs ({len(prototype_ids)})"
        )
    gen = rng if rng is not None else random
    return gen.sample(prototype_ids, sample_size)


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected mapping for `{field_name}`")
    return value


def _require_value(run: dict[str, Any], field_name: str) -> Any:
    value = run.get(field_name)
    if value is None:
        raise ValueError(f"Missing required field `{field_name}` in run config")
    return value


def _load_run_from_yaml(scenario_config: Path, run_num: int) -> dict[str, Any]:
    with scenario_config.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(
            f"Invalid YAML format in {scenario_config}: expected top-level map"
        )
    runs = cast(
        dict[str | int, Any],
        _require_mapping(data.get("runs"), "runs"),
    )
    run = runs.get(run_num) or runs.get(str(run_num))
    if run is None:
        raise ValueError(f"Run {run_num} not found in {scenario_config}")
    return _require_mapping(run, f"runs[{run_num}]")


def _resolve_output_dir(
    run: dict[str, Any],
    run_num: int,
    output_dir_override: Path | None,
) -> Path:
    """Determine the CAIRO output root directory.

    When *output_dir_override* is provided (CLI ``--output-dir``), it wins.
    Otherwise ``path_outputs`` from the YAML run dict is required:
    CAIRO creates ``{output_dir}/{timestamp}_{run_name}/``, so we pass
    ``parent(path_outputs)`` as the output root.
    """
    if output_dir_override is not None:
        return output_dir_override
    path_outputs_raw = run.get("path_outputs")
    if not path_outputs_raw:
        raise ValueError(
            f"runs[{run_num}] is missing required key 'path_outputs' "
            "and no --output-dir was provided on the CLI."
        )
    return Path(str(path_outputs_raw)).parent


def _build_settings_from_yaml_run(
    run: dict[str, Any],
    run_num: int,
    state: str,
    output_dir_override: Path | None,
    run_name_override: str | None,
) -> ScenarioSettings:
    """Build runtime settings from repo YAML scenario config."""
    state = state.upper()
    path_config = _state_config(state)
    utility = str(_require_value(run, "utility")).lower()
    mode = str(run.get("run_type", "precalc"))
    year_run = _parse_int(_require_value(run, "year_run"), "year_run")
    year_dollar_conversion = _parse_int(
        _require_value(run, "year_dollar_conversion"),
        "year_dollar_conversion",
    )
    process_workers = _parse_int(
        _require_value(run, "process_workers"), "process_workers"
    )
    path_electric_utility_stats = _resolve_path_or_uri(
        str(_require_value(run, "path_electric_utility_stats")),
        path_config,
    )
    solar_pv_compensation = str(_require_value(run, "solar_pv_compensation"))

    path_tariff_maps_electric = _resolve_path(
        str(_require_value(run, "path_tariff_maps_electric")),
        path_config,
    )
    raw_path_tariffs_electric = _require_value(run, "path_tariffs_electric")
    path_tariffs_electric = _parse_path_tariffs(
        raw_path_tariffs_electric,
        path_tariff_maps_electric,
        path_config,
        "electric",
    )
    run_includes_supply = _parse_bool(
        _require_value(run, "run_includes_supply"),
        "run_includes_supply",
    )
    run_includes_subclasses = _parse_bool(
        _require_value(run, "run_includes_subclasses"),
        "run_includes_subclasses",
    )
    rr_config: RevenueRequirementConfig = _parse_utility_revenue_requirement(
        _require_value(run, "utility_delivery_revenue_requirement"),
        path_config,
        raw_path_tariffs_electric,
        add_supply=run_includes_supply,
        run_includes_subclasses=run_includes_subclasses,
    )
    path_tariff_maps_gas = _resolve_path(
        str(_require_value(run, "path_tariff_maps_gas")),
        path_config,
    )
    path_tariffs_gas = _parse_path_tariffs_gas(
        _require_value(run, "path_tariffs_gas"),
        path_tariff_maps_gas,
        path_config,
    )
    elasticity = _parse_float(run.get("elasticity", 0.0), "elasticity")
    sample_size = (
        _parse_int(run["sample_size"], "sample_size") if "sample_size" in run else None
    )
    path_tou_supply_mc: str | Path | None = None
    if "path_tou_supply_mc" in run:
        path_tou_supply_mc = _resolve_path_or_uri(
            str(run["path_tou_supply_mc"]), path_config
        )
    output_dir = _resolve_output_dir(run, run_num, output_dir_override)
    run_name = run_name_override or run.get("run_name") or f"run_{run_num}"
    return ScenarioSettings(
        run_name=run_name,
        run_type=mode,
        state=state,
        utility=utility,
        path_results=output_dir,
        path_utility_assignment=_resolve_path_or_uri(
            str(_require_value(run, "path_utility_assignment")),
            path_config,
        ),
        path_resstock_metadata=_resolve_path(
            str(_require_value(run, "path_resstock_metadata")),
            path_config,
        ),
        path_resstock_loads=_resolve_path(
            str(_require_value(run, "path_resstock_loads")),
            path_config,
        ),
        path_supply_marginal_costs=_resolve_path_or_uri(
            str(run.get("path_supply_marginal_costs", "")), path_config
        )
        if run.get("path_supply_marginal_costs")
        else None,
        path_supply_energy_mc=_resolve_path_or_uri(
            str(run.get("path_supply_energy_mc", "")), path_config
        )
        if run.get("path_supply_energy_mc")
        else None,
        path_supply_capacity_mc=_resolve_path_or_uri(
            str(run.get("path_supply_capacity_mc", "")), path_config
        )
        if run.get("path_supply_capacity_mc")
        else None,
        path_td_marginal_costs=_resolve_path_or_uri(
            str(_require_value(run, "path_td_marginal_costs")),
            path_config,
        ),
        path_tariff_maps_electric=path_tariff_maps_electric,
        path_tariff_maps_gas=path_tariff_maps_gas,
        path_tariffs_electric=path_tariffs_electric,
        path_tariffs_gas=path_tariffs_gas,
        rr_total=rr_config.rr_total,
        subclass_rr=rr_config.subclass_rr,
        run_includes_subclasses=rr_config.run_includes_subclasses,
        path_electric_utility_stats=path_electric_utility_stats,
        year_run=year_run,
        year_dollar_conversion=year_dollar_conversion,
        process_workers=process_workers,
        solar_pv_compensation=solar_pv_compensation,
        sample_size=sample_size,
        run_includes_supply=run_includes_supply,
        elasticity=elasticity,
        path_tou_supply_mc=path_tou_supply_mc,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Run heat-pump scenario using YAML config.")
    )
    parser.add_argument(
        "--scenario-config",
        type=Path,
        default=None,
        help=("Path to YAML scenario config. Required unless --utility is provided."),
    )
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        help="Two-letter state code (e.g. ri, ny). Determines config directory.",
    )
    parser.add_argument(
        "--utility",
        type=str,
        default=None,
        help=(
            "Utility code used to derive scenario config as "
            "{state}/config/scenarios/scenarios_<utility>.yaml. "
            "Required unless --scenario-config is provided."
        ),
    )
    parser.add_argument(
        "--run-num",
        type=int,
        required=True,
        help="Run number in the YAML `runs` mapping (e.g. 1 or 2).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Output root dir passed to CAIRO. When omitted, uses "
            "parent(path_outputs) from the scenario YAML."
        ),
    )
    parser.add_argument(
        "--run-name",
        help="Optional override for run name.",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=None,
        dest="num_workers",
        help=(
            "Number of Dask process workers. When provided, overrides process_workers "
            "from the scenario YAML. When omitted, uses "
            "min(process_workers, os.cpu_count())."
        ),
    )
    args = parser.parse_args()
    if args.scenario_config is None and args.utility is None:
        parser.error("Provide either --scenario-config or --utility.")
    return args


def _resolve_settings(args: argparse.Namespace) -> ScenarioSettings:
    state = args.state
    scenario_config = args.scenario_config
    if scenario_config is None:
        if args.utility is None:
            raise ValueError("Provide either --scenario-config or --utility.")
        scenario_config = _scenario_config_from_utility(state, args.utility.lower())
    run = _load_run_from_yaml(scenario_config, args.run_num)
    return _build_settings_from_yaml_run(
        run=run,
        run_num=args.run_num,
        state=state,
        output_dir_override=args.output_dir,
        run_name_override=args.run_name,
    )


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------


def _load_prototype_ids_for_run(
    path_utility_assignment: str | Path,
    utility: str,
    sample_size: int | None,
) -> list[int]:
    """Load utility assignment, fetch prototype IDs for the utility, optionally sample."""

    path_ua = path_utility_assignment
    storage_opts = (
        _storage_options()
        if isinstance(path_ua, str) and path_ua.startswith("s3://")
        else None
    )
    utility_assignment = pl.scan_parquet(str(path_ua), storage_options=storage_opts)
    prototype_ids = _fetch_prototype_ids_by_electric_util(
        cast(ElectricUtility, utility), utility_assignment
    )
    log.info(
        ".... Found %s bldgs for utility %s",
        len(prototype_ids),
        utility,
    )
    if sample_size is not None:
        rng = random.Random(42)
        try:
            prototype_ids = apply_prototype_sample(prototype_ids, sample_size, rng=rng)
        except ValueError as e:
            log.warning("%s; exiting.", e)
            sys.exit(1)
        log.info(
            ".... Selected a sample of %s of these for simulation (sample_size=%s)",
            len(prototype_ids),
            sample_size,
        )
    return prototype_ids


def _build_precalc_period_mapping(
    path_tariffs_electric: dict[str, Path],
) -> pd.DataFrame:
    """Build precalc mapping by concatenating all configured electric tariffs."""
    if not path_tariffs_electric:
        raise ValueError("path_tariffs_electric must contain at least one tariff")
    precalc_parts = [
        generate_default_precalc_mapping(tariff_path=tariff_path, tariff_key=tariff_key)
        for tariff_key, tariff_path in path_tariffs_electric.items()
    ]
    return pd.concat(precalc_parts, ignore_index=True)


def run(settings: ScenarioSettings, num_workers: int | None = None) -> None:
    log.info(
        ".... Beginning %s residential (non-LMI) rate scenario simulation: %s",
        settings.state,
        settings.run_name,
    )

    _effective_workers = (
        num_workers
        if num_workers is not None
        else min(settings.process_workers, os.cpu_count() or 1)
    )
    dask.config.set(scheduler="processes", num_workers=_effective_workers)
    log.info(
        "TIMING workers: %d (yaml=%d, cpu_count=%d)",
        _effective_workers,
        settings.process_workers,
        os.cpu_count() or 1,
    )

    # Phase 1 ---------------------------------------------------------------
    with _timed("_load_prototype_ids_for_run"):
        prototype_ids = _load_prototype_ids_for_run(
            settings.path_utility_assignment,
            settings.utility,
            settings.sample_size,
        )

    with _timed("_initialize_tariffs"):
        tariffs_params, tariff_map_df = _initialize_tariffs(
            tariff_map=settings.path_tariff_maps_electric,
            building_stock_sample=prototype_ids,
            tariff_paths=settings.path_tariffs_electric,
        )

    with _timed("_build_precalc_period_mapping"):
        precalc_mapping = _build_precalc_period_mapping(settings.path_tariffs_electric)

    with _timed("return_buildingstock"):
        customer_count = get_residential_customer_count_from_utility_stats(
            settings.path_electric_utility_stats,
            settings.utility,
            storage_options=_storage_options(),
        )
        customer_metadata = return_buildingstock(
            load_scenario=settings.path_resstock_metadata,
            building_stock_sample=prototype_ids,
            customer_count=customer_count,
            columns=[
                "applicability",
                "postprocess_group.has_hp",
                "postprocess_group.heating_type",
                "in.vintage_acs",
            ],
        )

    with _timed("build_bldg_id_to_load_filepath"):
        bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
            path_resstock_loads=settings.path_resstock_loads,
            building_ids=prototype_ids,
        )

    with _timed("_return_loads_combined"):
        raw_load_elec, raw_load_gas = _return_loads_combined(
            target_year=settings.year_run,
            building_ids=prototype_ids,
            load_filepath_key=bldg_id_to_load_filepath,
            force_tz="EST",
        )

    # Phase 2 ---------------------------------------------------------------
    # ------------------------------------------------------------------
    # Load marginal costs (needed before demand-flex and revenue calc)
    # ------------------------------------------------------------------
    # MC prices are exogenous (Cambium + distribution); load shifting
    # changes total MC dollars, not the prices themselves.

    # Load supply MCs: use separate energy/capacity files if provided, else fallback to combined
    if settings.path_supply_energy_mc and settings.path_supply_capacity_mc:
        bulk_marginal_costs = _load_supply_marginal_costs(
            settings.path_supply_energy_mc,
            settings.path_supply_capacity_mc,
            settings.year_run,
        )
    elif settings.path_supply_marginal_costs:
        bulk_marginal_costs = _load_cambium_marginal_costs(
            settings.path_supply_marginal_costs,
            settings.year_run,
        )
    else:
        raise ValueError(
            "Must provide either path_supply_marginal_costs (combined) or "
            "both path_supply_energy_mc and path_supply_capacity_mc (separate)"
        )
    distribution_marginal_costs = load_distribution_marginal_costs(
        settings.path_td_marginal_costs,
    )
    if not bulk_marginal_costs.index.equals(distribution_marginal_costs.index):
        if len(bulk_marginal_costs) == len(distribution_marginal_costs):
            # T&D MC parquet can carry a different in-file year (e.g. 2025) than
            # run year (e.g. 2026). Align by hour position onto bulk MC index so
            # CAIRO system-cost merge does not collapse to empty.
            distribution_marginal_costs = pd.Series(
                distribution_marginal_costs.values,
                index=bulk_marginal_costs.index,
                name=distribution_marginal_costs.name,
            )
            log.info(
                ".... Aligned distribution MC index to bulk MC index "
                "(bulk_year=%s, dist_year=%s)",
                bulk_marginal_costs.index[0].year,
                distribution_marginal_costs.index[0].year,
            )
        else:
            distribution_marginal_costs = distribution_marginal_costs.reindex(
                bulk_marginal_costs.index
            )
            log.info(
                ".... Reindexed distribution MC to bulk MC index "
                "(bulk_rows=%s, dist_rows=%s)",
                len(bulk_marginal_costs),
                len(distribution_marginal_costs),
            )
    log.info(
        ".... Loaded distribution marginal costs rows=%s",
        len(distribution_marginal_costs),
    )
    sell_rate = _return_export_compensation_rate(
        year_run=settings.year_run,
        solar_pv_compensation=settings.solar_pv_compensation,
        solar_pv_export_import_ratio=1.0,
        tariff_dict=tariffs_params,
    )

    # FIND TOTAL RESIDUAL AND HOURLY MC's
    # Decomposes the revenue requirement into total marginal costs and residual.
    # RR = total_MC + residual, where total_MC = sum of hourly (MC_price × load)
    # and residual = RR − total_MC (embedded infrastructure costs).
    demand_flex_enabled = settings.elasticity != 0.0

    if not demand_flex_enabled:
        (
            revenue_requirement,
            marginal_system_prices,
            _marginal_system_costs,
            costs_by_type,
        ) = _return_revenue_requirement_target(
            building_load=raw_load_elec,
            sample_weight=customer_metadata[["bldg_id", "weight"]],
            revenue_requirement_target=settings.rr_total,
            residual_cost=None,
            residual_cost_frac=None,
            bulk_marginal_costs=bulk_marginal_costs,
            distribution_marginal_costs=distribution_marginal_costs,
            low_income_strategy=None,
        )
        effective_load_elec = raw_load_elec
        elasticity_tracker = pd.DataFrame()
        if settings.run_includes_subclasses:
            revenue_requirement = settings.subclass_rr
    else:
        flex = apply_demand_flex(
            elasticity=settings.elasticity,
            run_type=settings.run_type,
            year_run=settings.year_run,
            path_tariffs_electric=settings.path_tariffs_electric,
            path_tou_supply_mc=settings.path_tou_supply_mc,
            tou_derivation_dir=_state_config(settings.state) / "tou_derivation",
            raw_load_elec=raw_load_elec,
            customer_metadata=customer_metadata,
            tariff_map_df=tariff_map_df,
            precalc_mapping=precalc_mapping,
            rr_total=settings.rr_total,
            bulk_marginal_costs=bulk_marginal_costs,
            distribution_marginal_costs=distribution_marginal_costs,
        )

        revenue_requirement = flex.revenue_requirement_raw
        marginal_system_prices = flex.marginal_system_prices
        _marginal_system_costs = flex.marginal_system_costs
        costs_by_type = flex.costs_by_type

        effective_load_elec = flex.effective_load_elec
        elasticity_tracker = flex.elasticity_tracker
        precalc_mapping = flex.precalc_mapping
        if settings.run_includes_subclasses and settings.subclass_rr is not None:
            revenue_requirement = split_revenue_requirement_by_tou(
                revenue_requirement_total=revenue_requirement,
                subclass_baseline=settings.subclass_rr,
                tou_tariff_keys=flex.tou_tariff_keys,
            )

    # Phase 3 ---------------------------------------------------------------
    # FIND RATE(S) THAT MEET REVENUE REQUIREMENT(S)
    # THEN CALCULATE CUSTOMER-LEVEL BILLS USING THE RATE,
    # CUSTOMER-LEVEL COSTS USING HOURLY MARGINAL COSTS AND RESIDUAL.
    with _timed("bs.simulate"):
        bs = MeetRevenueSufficiencySystemWide(
            run_type=settings.run_type,
            year_run=settings.year_run,
            year_dollar_conversion=settings.year_dollar_conversion,
            process_workers=settings.process_workers,
            building_stock_sample=prototype_ids,
            run_name=settings.run_name,
            output_dir=settings.path_results,
        )
        bs.simulate(
            revenue_requirement=revenue_requirement,
            tariffs_params=tariffs_params,
            tariff_map=tariff_map_df,
            precalc_period_mapping=precalc_mapping,
            customer_metadata=customer_metadata,
            customer_electricity_load=effective_load_elec,
            customer_gas_load=raw_load_gas,
            gas_tariff_map=settings.path_tariff_maps_gas,
            gas_tariff_str_loc=settings.path_tariffs_gas,
            load_cols="total_fuel_electricity",
            marginal_system_prices=marginal_system_prices,
            costs_by_type=costs_by_type,
            solar_pv_compensation=None,
            sell_rate=sell_rate,
            low_income_strategy=None,
            low_income_participation_target=None,
            low_income_bill_assistance_program=None,
        )

    save_file_loc = getattr(bs, "save_file_loc", None)
    if save_file_loc is not None:
        distribution_mc_path = Path(save_file_loc) / "distribution_marginal_costs.csv"
        distribution_marginal_costs.to_csv(distribution_mc_path, index=True)
        log.info(".... Saved distribution marginal costs: %s", distribution_mc_path)
        if demand_flex_enabled:
            tracker_path = Path(save_file_loc) / "demand_flex_elasticity_tracker.csv"
            elasticity_tracker.to_csv(tracker_path, index=True)
            log.info(".... Saved demand-flex elasticity tracker: %s", tracker_path)

    log.info(
        ".... Completed %s residential (non-LMI) rate scenario simulation",
        settings.state,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args()
    settings = _resolve_settings(args)
    run(settings, num_workers=args.num_workers)


if __name__ == "__main__":
    main()

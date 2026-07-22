"""Prefect pipeline for heat pump rate design scenario runs.

Replaces the Justfile shell orchestration with a Python-native Prefect pipeline.
Each scenario (identified by tariff name) expands into a "quartet" subflow:
  precalc_task (delivery + supply in parallel) → calibrated_task (delivery + supply in parallel)

Configuration is loaded from a compact pipeline YAML that declares utility-level
defaults and a scenario (tariff name). All file paths are derived from naming
conventions — no per-run path duplication.
"""

from __future__ import annotations

import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from prefect import flow, task
from prefect.cache_policies import CacheKeyFnPolicy
from prefect.context import TaskRunContext

from rate_design.hp_rates.run_scenario import (
    ScenarioSettings,
    run,
)
from utils.mid.copy_calibrated_tariff_from_run import (
    copy_calibrated_tariff_from_run_dir,
)
from utils.scenario_config import (
    _parse_path_tariffs,
    _parse_path_tariffs_gas,
    _parse_utility_revenue_requirement,
)

log = logging.getLogger(__name__)

HP_RATES_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StageResult:
    """Output of one stage (precalc or calibrated) covering both delivery and supply."""

    delivery_output_dir: Path
    supply_output_dir: Path
    calibrated_tariff_paths: list[Path] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class QuartetResult:
    """Full output of a scenario quartet (precalc + calibrated)."""

    precalc: StageResult
    calibrated: StageResult


# ---------------------------------------------------------------------------
# Pipeline config (loaded from YAML)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """Parsed pipeline YAML — all config needed to run a scenario."""

    state: str
    utility: str
    year: int
    solar_pv_compensation: str
    process_workers: int
    scenario: str

    resstock_base: str
    upgrade_precalc: str
    upgrade_calibrated: str

    mc_dist_and_sub_tx: str
    mc_bulk_tx: str
    mc_supply_energy: str
    mc_supply_capacity: str

    rr_precalc_path: str
    rr_calibrated_path: str

    residual_allocation_delivery: str
    residual_allocation_supply: str

    @property
    def state_config_dir(self) -> Path:
        return HP_RATES_DIR / self.state.lower() / "config"


def load_pipeline_config(yaml_path: Path) -> PipelineConfig:
    """Load the compact pipeline YAML into a PipelineConfig."""
    with yaml_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    resstock = data["resstock"]
    mc = data["marginal_costs"]
    rr = data["revenue_requirement"]
    ra = data["residual_allocation"]

    return PipelineConfig(
        state=data["state"],
        utility=data["utility"],
        year=int(data["year"]),
        solar_pv_compensation=data.get("solar_pv_compensation", "net_metering"),
        process_workers=int(data.get("process_workers", 8)),
        scenario=data["scenario"],
        resstock_base=resstock["base"],
        upgrade_precalc=resstock["upgrade_precalc"],
        upgrade_calibrated=resstock["upgrade_calibrated"],
        mc_dist_and_sub_tx=mc["dist_and_sub_tx"],
        mc_bulk_tx=mc["bulk_tx"],
        mc_supply_energy=mc["supply_energy"],
        mc_supply_capacity=mc["supply_capacity"],
        rr_precalc_path=rr["precalc"],
        rr_calibrated_path=rr["calibrated"],
        residual_allocation_delivery=ra["delivery"],
        residual_allocation_supply=ra["supply"],
    )


# ---------------------------------------------------------------------------
# Path derivation from tariff name
# ---------------------------------------------------------------------------


def _tariff_map_path(config: PipelineConfig, *, supply: bool, calibrated: bool) -> Path:
    """Derive tariff map CSV path from naming convention."""
    name = f"{config.utility}_{config.scenario}"
    if calibrated:
        name += "_calibrated"
    if supply:
        name += "_supply"
    return config.state_config_dir / "tariff_maps" / "electric" / f"{name}.csv"


def _tariff_json_path(
    config: PipelineConfig, *, supply: bool, calibrated: bool
) -> Path:
    """Derive tariff JSON path from naming convention."""
    name = f"{config.utility}_{config.scenario}"
    if supply:
        name += "_supply"
    if calibrated:
        name += "_calibrated"
    return config.state_config_dir / "tariffs" / "electric" / f"{name}.json"


def _gas_tariff_map_path(config: PipelineConfig, *, calibrated: bool) -> Path:
    """Derive gas tariff map path."""
    upgrade = config.upgrade_calibrated if calibrated else config.upgrade_precalc
    return (
        config.state_config_dir
        / "tariff_maps"
        / "gas"
        / f"{config.utility}_u{upgrade}.csv"
    )


def _supply_mc_path(base_path: str, *, include_supply: bool) -> str:
    """Return real MC path if supply run, else swap data.parquet for zero.parquet."""
    if include_supply:
        return base_path
    return base_path.replace("/data.parquet", "/zero.parquet")


# ---------------------------------------------------------------------------
# Settings derivation
# ---------------------------------------------------------------------------


def derive_settings(
    config: PipelineConfig,
    batch: str,
    *,
    supply: bool,
    calibrated: bool,
) -> ScenarioSettings:
    """Expand compact PipelineConfig into a full ScenarioSettings for one CAIRO run."""
    state_upper = config.state.upper()
    upgrade = config.upgrade_calibrated if calibrated else config.upgrade_precalc
    run_type = "default" if calibrated else "precalc"

    variant_suffix = "supply" if supply else "delivery"
    stage_suffix = "calibrated" if calibrated else "precalc"
    run_name = f"{config.state}_{config.utility}_{config.scenario}_{stage_suffix}_{variant_suffix}"

    path_config = config.state_config_dir
    outputs_base = Path(
        os.environ.get(
            "RDP_OUTPUT_BASE",
            f"/data.sb/switchbox/cairo/outputs/hp_rates/{config.state}/{config.utility}",
        )
    )
    output_dir = outputs_base / batch

    rr_yaml_rel = config.rr_calibrated_path if calibrated else config.rr_precalc_path
    raw_path_tariffs_electric: dict[str, Any] = {
        "all": str(
            _tariff_json_path(config, supply=supply, calibrated=calibrated).relative_to(
                path_config
            )
        )
    }

    rr_config = _parse_utility_revenue_requirement(
        rr_yaml_rel,
        path_config,
        raw_path_tariffs_electric,
        add_supply=supply,
        run_includes_subclasses=False,
        residual_allocation_delivery=config.residual_allocation_delivery,
        residual_allocation_supply=config.residual_allocation_supply,
    )

    path_tariff_maps_electric = _tariff_map_path(
        config, supply=supply, calibrated=calibrated
    )
    path_tariffs_electric = _parse_path_tariffs(
        raw_path_tariffs_electric,
        path_tariff_maps_electric,
        path_config,
        "electric",
    )

    path_tariff_maps_gas = _gas_tariff_map_path(config, calibrated=calibrated)
    path_tariffs_gas = _parse_path_tariffs_gas(
        "tariffs/gas",
        path_tariff_maps_gas,
        path_config,
    )

    resstock_base = config.resstock_base
    path_resstock_metadata = Path(
        f"{resstock_base}/metadata/state={state_upper}/upgrade={upgrade}/metadata-sb.parquet"
    )
    path_resstock_loads = Path(
        f"{resstock_base}/load_curve_hourly/state={state_upper}/upgrade={upgrade}/"
    )
    path_utility_assignment = Path(
        f"{resstock_base}/metadata_utility/state={state_upper}/utility_assignment.parquet"
    )

    eia_year = config.year - 1
    path_electric_utility_stats = (
        f"s3://data.sb/eia/861/electric_utility_stats/year={eia_year}"
        f"/state={state_upper}/data.parquet"
    )

    return ScenarioSettings(
        run_name=run_name,
        run_type=run_type,
        state=state_upper,
        utility=config.utility,
        path_results=output_dir,
        path_resstock_metadata=path_resstock_metadata,
        path_resstock_loads=path_resstock_loads,
        path_utility_assignment=path_utility_assignment,
        path_dist_and_sub_tx_mc=config.mc_dist_and_sub_tx,
        path_tariff_maps_electric=path_tariff_maps_electric,
        path_tariff_maps_gas=path_tariff_maps_gas,
        path_tariffs_electric=path_tariffs_electric,
        path_tariffs_gas=path_tariffs_gas,
        rr_total=rr_config.rr_total,
        subclass_rr=rr_config.subclass_rr,
        run_includes_subclasses=False,
        residual_allocation_delivery=rr_config.residual_allocation_delivery,
        residual_allocation_supply=rr_config.residual_allocation_supply,
        path_electric_utility_stats=path_electric_utility_stats,
        path_supply_energy_mc=_supply_mc_path(
            config.mc_supply_energy, include_supply=supply
        ),
        path_supply_capacity_mc=_supply_mc_path(
            config.mc_supply_capacity, include_supply=supply
        ),
        year_run=config.year,
        year_dollar_conversion=config.year,
        process_workers=config.process_workers,
        solar_pv_compensation=config.solar_pv_compensation,
        run_includes_supply=supply,
        path_bulk_tx_mc=config.mc_bulk_tx,
        elasticity=0.0,
        customer_count_override=rr_config.customer_count_override,
        kwh_scale_factor=rr_config.kwh_scale_factor,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _git_metadata() -> dict[str, str]:
    """Collect git commit and dirty status."""
    try:
        commit = (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        commit = "unknown"
    try:
        dirty = len(
            subprocess.check_output(
                ["git", "status", "--porcelain"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
            .splitlines()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        dirty = -1
    return {
        "git_commit": commit,
        "git_dirty": f"{dirty} files",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _write_log(utility: str, scenario: str, stage: str, batch: str) -> None:
    """Write git metadata log for this run."""
    log_dir = Path.home() / "rdp_run_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{utility}_{scenario}_{stage}_{batch}.log"
    meta = _git_metadata()
    with log_path.open("w") as f:
        for k, v in meta.items():
            f.write(f"{k}: {v}\n")
    log.info("Wrote metadata to %s", log_path)


def _run_single(settings: ScenarioSettings, *, billing_kwh: bool) -> Path:
    """Run a single CAIRO invocation and return the output directory."""
    result_dir = run(settings, billing_kwh=billing_kwh)
    if result_dir is None:
        raise RuntimeError(
            f"CAIRO returned None for {settings.run_name} — no output directory produced."
        )
    return result_dir


def _run_pair(
    config: PipelineConfig,
    batch: str,
    *,
    calibrated: bool,
) -> tuple[Path, Path]:
    """Run delivery + supply in parallel, return (delivery_dir, supply_dir)."""
    delivery_settings = derive_settings(
        config, batch, supply=False, calibrated=calibrated
    )
    supply_settings = derive_settings(config, batch, supply=True, calibrated=calibrated)

    workers = max(1, config.process_workers // 2)
    delivery_settings = _with_workers(delivery_settings, workers)
    supply_settings = _with_workers(supply_settings, workers)

    results: dict[str, Path] = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(_run_single, delivery_settings, billing_kwh=True): "delivery",
            pool.submit(_run_single, supply_settings, billing_kwh=False): "supply",
        }
        for future in as_completed(futures):
            variant = futures[future]
            results[variant] = future.result()

    return results["delivery"], results["supply"]


def _with_workers(settings: ScenarioSettings, workers: int) -> ScenarioSettings:
    """Return a copy of settings with adjusted process_workers."""
    import dataclasses

    return dataclasses.replace(settings, process_workers=workers)


def _extract_calibrated_tariffs(
    output_dir: Path,
    config: PipelineConfig,
) -> list[Path]:
    """Extract calibrated tariffs from a precalc output dir (if present)."""
    tariff_final = output_dir / "tariff_final_config.json"
    if not tariff_final.exists():
        return []
    return copy_calibrated_tariff_from_run_dir(output_dir, state=config.state)


# ---------------------------------------------------------------------------
# Prefect cache policy
# ---------------------------------------------------------------------------


def _stage_cache_key(context: TaskRunContext, parameters: dict[str, Any]) -> str:
    """Cache key = state/utility/batch/scenario/stage."""
    config: PipelineConfig = parameters["config"]
    return (
        f"{config.state}/{config.utility}/{parameters['batch']}"
        f"/{config.scenario}/{parameters['stage']}"
    )


_cache_policy = CacheKeyFnPolicy(cache_key_fn=_stage_cache_key)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------


@task(log_prints=True, persist_result=True, cache_policy=_cache_policy)
def precalc_task(
    config: PipelineConfig, batch: str, stage: str = "precalc"
) -> StageResult:
    """Run precalc delivery + supply in parallel, extract calibrated tariffs."""
    _write_log(config.utility, config.scenario, "precalc", batch)

    delivery_dir, supply_dir = _run_pair(config, batch, calibrated=False)

    calibrated_paths: list[Path] = []
    calibrated_paths.extend(_extract_calibrated_tariffs(delivery_dir, config))
    calibrated_paths.extend(_extract_calibrated_tariffs(supply_dir, config))

    log.info(
        "Precalc complete: delivery=%s, supply=%s, tariffs=%s",
        delivery_dir,
        supply_dir,
        [p.name for p in calibrated_paths],
    )
    return StageResult(
        delivery_output_dir=delivery_dir,
        supply_output_dir=supply_dir,
        calibrated_tariff_paths=calibrated_paths,
    )


@task(log_prints=True, persist_result=True, cache_policy=_cache_policy)
def calibrated_task(
    config: PipelineConfig,
    batch: str,
    precalc_tariffs: list[Path],
    stage: str = "calibrated",
) -> StageResult:
    """Run calibrated delivery + supply in parallel.

    The precalc_tariffs argument encodes the Prefect dependency on precalc_task —
    by the time this task executes, precalc has completed and written tariff files.
    """
    _write_log(config.utility, config.scenario, "calibrated", batch)

    delivery_dir, supply_dir = _run_pair(config, batch, calibrated=True)

    log.info("Calibrated complete: delivery=%s, supply=%s", delivery_dir, supply_dir)
    return StageResult(
        delivery_output_dir=delivery_dir,
        supply_output_dir=supply_dir,
    )


# ---------------------------------------------------------------------------
# Prefect subflow (quartet)
# ---------------------------------------------------------------------------


@flow(name="quartet")
def quartet(config: PipelineConfig, batch: str) -> QuartetResult:
    """Run a full scenario quartet: precalc → calibrated.

    This is a Prefect subflow so it can be wired as a dependency unit
    when composing multiple scenarios in a parent flow.
    """
    precalc = precalc_task(config, batch)
    calibrated = calibrated_task(config, batch, precalc.calibrated_tariff_paths)
    return QuartetResult(precalc=precalc, calibrated=calibrated)


# ---------------------------------------------------------------------------
# Prefect parent flow
# ---------------------------------------------------------------------------


@flow(name="hp-rates-pipeline")
def hp_rates_pipeline(yaml_path: str, batch: str) -> QuartetResult:
    """Top-level flow: load config, run the default scenario quartet.

    Future scenarios (hp_seasonal, etc.) would be additional quartet() calls
    wired together via return values.
    """
    config = load_pipeline_config(Path(yaml_path))
    return quartet(config, batch)


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Run hp_rates Prefect pipeline")
    parser.add_argument("--yaml", required=True, help="Path to pipeline YAML config")
    parser.add_argument(
        "--batch", required=True, help="Batch name (e.g. md_20260722_r1-4)"
    )
    args = parser.parse_args()

    result = hp_rates_pipeline(yaml_path=args.yaml, batch=args.batch)
    print(f"Precalc delivery:    {result.precalc.delivery_output_dir}")
    print(f"Precalc supply:      {result.precalc.supply_output_dir}")
    print(f"Calibrated delivery: {result.calibrated.delivery_output_dir}")
    print(f"Calibrated supply:   {result.calibrated.supply_output_dir}")
    if result.precalc.calibrated_tariff_paths:
        print("Calibrated tariffs:")
        for p in result.precalc.calibrated_tariff_paths:
            print(f"  {p}")

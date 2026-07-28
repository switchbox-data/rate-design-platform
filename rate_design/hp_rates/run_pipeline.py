"""Prefect pipeline for heat pump rate design scenario runs.

Orchestrates CAIRO runs for one or more scenarios defined in a pipeline YAML
(e.g. ``pipeline_bge.yaml``).  Each scenario is evaluated as a **quartet** of
four CAIRO runs (2 stages × 2 variants).  Multi-rate scenarios may depend on a
prior single-rate scenario's outputs.

Architecture
------------
* **preflight** generates scenario YAMLs, tariff maps, and validates inputs.
* **run_quartet** runs any scenario's four CAIRO invocations with tariff
  promotion between stages.
* **derive_tariffs** computes subclass revenue requirements and derived tariffs
  for multi-rate scenarios that depend on a prior scenario.
* **run_batch** (master flow) ties everything together in dependency order.

CAIRO is invoked as a subprocess (``run_scenario.py``) for full memory
isolation.  Output directories are discovered via run index files written by
``run_scenario.py`` on success.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import threading
from pathlib import Path

import yaml
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from rate_design.hp_rates.pipeline_config import (
    ALLOCATION_TO_BAT_COL,
    PipelineConfig,
    ScenarioConfig,
    multi_rate_rr_path,
    tariff_stem,
)
from utils.mid.compute_subclass_rr import (
    SUBCLASS_RR_ALLOCATION_METHODS,
    _write_revenue_requirement_yamls,
    compute_subclass_rr,
)
from utils.mid.copy_calibrated_tariff_from_run import (
    copy_calibrated_tariff_from_run_dir,
)
from utils.pre.season_config import load_winter_months_from_periods

from rate_design.hp_rates.pipeline_derive import DeriveContext, derive_subgroup_tariff


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Concurrency gate
# ---------------------------------------------------------------------------

_cairo_semaphore: threading.Semaphore | None = None


def _init_semaphore(max_concurrent: int) -> None:
    """Initialise the module-level CAIRO concurrency semaphore.

    Called once by ``run_batch`` before any tasks are submitted.  Using a
    module-level semaphore (rather than passing it through Prefect) avoids
    serialisation issues — the semaphore lives in the Prefect worker process
    and gates ``subprocess.run`` calls from ``cairo_run`` tasks that execute
    in the same process via ``ThreadPoolTaskRunner``.
    """
    global _cairo_semaphore  # noqa: PLW0603
    _cairo_semaphore = threading.Semaphore(max_concurrent)


# ---------------------------------------------------------------------------
# Run index helpers
# ---------------------------------------------------------------------------


def read_run_output_dir(batch_dir: Path, run_name: str) -> Path:
    """Read a completed run's output directory from the run index.

    The run index is written by ``run_scenario.py`` on successful completion.
    Each index file is a one-line text file containing the absolute path to
    the timestamped output directory that CAIRO created.
    """
    index_file = batch_dir / ".runs" / f"{run_name}.path"
    return Path(index_file.read_text().strip())


def _run_is_complete(batch_dir: Path, run_name: str) -> bool:
    """Check whether a run has already completed (index file exists)."""
    index_file = batch_dir / ".runs" / f"{run_name}.path"
    return index_file.is_file()


# ---------------------------------------------------------------------------
# Canonical run name helpers
# ---------------------------------------------------------------------------


def canonical_run_name(
    batch: str,
    scenario_name: str,
    stage: str,
    variant: str,
) -> str:
    """Build the canonical run name for one CAIRO invocation.

    Format: ``{batch}_{scenario_name}_{stage}_{variant}``.
    The batch already encodes ``{state}_{utility}_...``.

    Example: ``md_bge_default_precalc_delivery``.
    """
    return f"{batch}_{scenario_name}_{stage}_{variant}"


# ---------------------------------------------------------------------------
# Core CAIRO task
# ---------------------------------------------------------------------------


@task(log_prints=True)
def cairo_run(
    run_id: str,
    *,
    yaml_path: Path,
    batch_dir: Path,
    state: str,
    is_delivery: bool,
) -> Path:
    """Execute one CAIRO run as a subprocess.  Returns the output directory.

    This is the atomic unit of work in the pipeline.  Each invocation shells
    out to ``run_scenario.py`` for full memory isolation — CAIRO + Dask +
    pandas consume several GB per run and Python's allocator does not return
    memory to the OS, so in-process execution caused OOM on larger utilities.

    **Resume**: if the run index file already exists, the run is skipped and
    the previously recorded output directory is returned.

    **Concurrency**: gated by ``_cairo_semaphore`` (initialised by
    ``run_batch``).  The semaphore limits how many CAIRO subprocesses run
    simultaneously across the whole pipeline.

    Args:
        run_id: Canonical run name (e.g. ``md_bge_default_precalc_delivery``).
            Passed as ``--run-num`` to ``run_scenario.py``.
        yaml_path: Path to the generated scenario YAML
            (e.g. ``scenarios_bge.yaml``).
        batch_dir: Batch output directory (contains ``.runs/`` index).
        state: Two-letter state code (e.g. ``md``).
        is_delivery: Whether this is a delivery variant (adds ``--billing-kwh``).

    Returns:
        Absolute path to the CAIRO output directory for this run.
    """
    if _run_is_complete(batch_dir, run_id):
        output_dir = read_run_output_dir(batch_dir, run_id)
        log.info("Skipping %s — already completed at %s", run_id, output_dir)
        return output_dir

    assert _cairo_semaphore is not None, (
        "Semaphore not initialised — call _init_semaphore() before submitting tasks."
    )

    cmd = [
        sys.executable,
        "-m",
        "rate_design.hp_rates.run_scenario",
        "--state",
        state,
        "--scenario-config",
        str(yaml_path),
        "--run-num",
        run_id,
    ]
    if is_delivery:
        cmd.append("--billing-kwh")

    log.info("Acquiring semaphore for %s", run_id)
    with _cairo_semaphore:
        log.info("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, check=False)

    if result.returncode != 0:
        raise RuntimeError(
            f"CAIRO subprocess failed for {run_id} (exit code {result.returncode})"
        )

    if not _run_is_complete(batch_dir, run_id):
        raise RuntimeError(
            f"CAIRO completed for {run_id} but no run index file found at "
            f"{batch_dir / '.runs' / f'{run_id}.path'}"
        )

    return read_run_output_dir(batch_dir, run_id)


# ---------------------------------------------------------------------------
# Tariff promotion (precalc -> calibrated seam)
# ---------------------------------------------------------------------------


def _promote_calibrated_tariffs(
    output_dirs: list[Path],
    state: str,
) -> list[Path]:
    """Extract calibrated tariffs from precalc output dirs.

    Reads ``tariff_final_config.json`` from each output directory, converts
    each tariff key to URDB format, and writes ``<key>_calibrated.json`` to
    the state's config tariffs directory.  Returns all written paths.

    This is the **tariff promotion seam** — the handoff between precalc and
    calibrated stages.  The generated scenario YAML already references the
    ``*_calibrated.json`` paths for calibrated-stage inputs, so promotion
    simply ensures those files exist on disk.
    """
    written: list[Path] = []
    for output_dir in output_dirs:
        if (output_dir / "tariff_final_config.json").exists():
            written.extend(copy_calibrated_tariff_from_run_dir(output_dir, state=state))
    return written


# ---------------------------------------------------------------------------
# Derive tariffs (between dependency and dependent quartets)
# ---------------------------------------------------------------------------


def _group_value_to_subclass(scenario: ScenarioConfig) -> dict[str, str]:
    """Build the ``raw group value -> alias`` inverse mapping.

    E.g. ``{"true": "hp", "false": "non-hp"}`` for a ``has_hp`` split.
    """
    assert scenario.subclass_config is not None
    mapping: dict[str, str] = {}
    for sg in scenario.subclass_config.subgroups:
        for value in sg.values:
            mapping[value] = sg.alias
    return mapping


def _compute_subclass_rr(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    dep_precalc: dict[str, Path],
) -> Path:
    """Compute and write the differentiated (per-subgroup) revenue requirement YAML.

    Uses the dependency scenario's precalc delivery and supply output dirs to
    compute revenue requirement breakdowns for each allocation method, then
    writes a single YAML with delivery/supply blocks keyed by allocation.

    Returns the path to the written RR YAML.
    """
    assert scenario.subclass_config is not None
    rd = config.run_defaults

    delivery_dir = dep_precalc["precalc_delivery"]
    supply_dir = dep_precalc["precalc_supply"]
    group_col = scenario.subclass_config.group_col
    cols = tuple(SUBCLASS_RR_ALLOCATION_METHODS)

    delivery_breakdowns = compute_subclass_rr(
        run_dir=delivery_dir, group_col=group_col, cross_subsidy_cols=cols
    )
    total_breakdowns = compute_subclass_rr(
        run_dir=supply_dir, group_col=group_col, cross_subsidy_cols=cols
    )

    base_rr_path = config.state_config_dir / rd.rr_single_rate
    base_rr = yaml.safe_load(base_rr_path.read_text(encoding="utf-8"))
    total_delivery_rr = base_rr.get("total_delivery_revenue_requirement")
    if total_delivery_rr is None:
        raise ValueError(
            f"Missing 'total_delivery_revenue_requirement' in {base_rr_path}"
        )
    total_delivery_and_supply_rr = base_rr.get(
        "total_delivery_and_supply_revenue_requirement"
    )
    if total_delivery_and_supply_rr is None:
        raise ValueError(
            f"Missing 'total_delivery_and_supply_revenue_requirement' in {base_rr_path}"
        )
    customer_count = base_rr.get("test_year_customer_count")
    kwh_scale_factor = base_rr.get("resstock_kwh_scale_factor")

    out_path = config.state_config_dir / multi_rate_rr_path(config, scenario)
    gv2s = _group_value_to_subclass(scenario)

    differentiated_yaml_path, _ = _write_revenue_requirement_yamls(
        delivery_breakdowns=delivery_breakdowns,
        run_dir=delivery_dir,
        group_col=group_col,
        utility=config.utility,
        default_revenue_requirement=float(total_delivery_rr),
        differentiated_yaml_path=out_path,
        default_yaml_path=base_rr_path,
        group_value_to_subclass=gv2s,
        total_breakdowns=total_breakdowns,
        total_delivery_rr=total_delivery_rr,
        total_delivery_and_supply_rr=total_delivery_and_supply_rr,
        customer_count_override=customer_count,
        kwh_scale_factor=kwh_scale_factor,
    )
    log.info("compute_subclass_rr: wrote %s", differentiated_yaml_path)
    return differentiated_yaml_path


def _derive_subgroup_tariffs(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    dep_precalc: dict[str, Path],
) -> list[Path]:
    """Create each subgroup's precalc tariff input based on its structure.

    For each subgroup:
    - ``structure == "base"``: copy the dependency's calibrated tariff and
      relabel it with the subgroup's tariff stem.
    - ``structure == "seasonal"``: compute seasonal discount inputs from the
      dependency's precalc outputs and ResStock loads, then create a 2-period
      seasonal tariff.
    - ``structure == "flat"``: compute a flat discount rate and create a
      single-period flat tariff.

    Both delivery and supply variants are written for each subgroup.

    Returns all written tariff JSON paths.
    """
    assert scenario.subclass_config is not None
    assert scenario.depends_on is not None
    rd = config.run_defaults

    dep_scenario = config.scenario(scenario.depends_on)
    group_col = scenario.subclass_config.group_col
    gv2s = _group_value_to_subclass(scenario)

    if scenario.residual_allocation_delivery is None:
        raise ValueError(
            f"Scenario {scenario.name!r} requires 'residual_allocation_delivery' "
            f"for tariff derivation but it is not set."
        )
    allocation = scenario.residual_allocation_delivery
    bat_col = ALLOCATION_TO_BAT_COL[allocation]

    json_dir = config.state_config_dir / "tariffs" / "electric"

    # Dependency's calibrated tariff paths (for structure: base copies)
    dep_delivery_cal = json_dir / (
        tariff_stem(config.utility, dep_scenario, supply=False, calibrated=True)
        + ".json"
    )
    dep_supply_cal = json_dir / (
        tariff_stem(config.utility, dep_scenario, supply=True, calibrated=True)
        + ".json"
    )

    written: list[Path] = []

    periods_path = config.state_config_dir / rd.periods_yaml
    winter_months = tuple(load_winter_months_from_periods(periods_path))

    for sg in scenario.subclass_config.subgroups:
        stem_d = tariff_stem(
            config.utility, scenario, sg.alias, supply=False, calibrated=False
        )
        stem_s = tariff_stem(
            config.utility, scenario, sg.alias, supply=True, calibrated=False
        )
        out_d = json_dir / f"{stem_d}.json"
        out_s = json_dir / f"{stem_s}.json"

        if sg.structure == "base":
            _relabel_tariff_copy(dep_delivery_cal, out_d, stem_d)
            _relabel_tariff_copy(dep_supply_cal, out_s, stem_s)
            log.info("derive_tariffs: relabel-copied %s (base)", sg.alias)
            written.extend([out_d, out_s])
            continue

        for out_path, stem, run_dir, base_tariff_path in (
            (out_d, stem_d, dep_precalc["precalc_delivery"], dep_delivery_cal),
            (out_s, stem_s, dep_precalc["precalc_supply"], dep_supply_cal),
        ):
            ctx = DeriveContext(
                run_dir=run_dir,
                base_tariff_path=base_tariff_path,
                stem=stem,
                out_path=out_path,
                resstock_base=rd.resstock_base,
                state=config.state.upper(),
                upgrade=rd.upgrade_precalc,
                group_col=group_col,
                subclass_value=sg.alias,
                bat_col=bat_col,
                group_value_to_subclass=gv2s,
                utility=config.utility,
                winter_months=winter_months,
            )
            written.append(derive_subgroup_tariff(sg.structure, ctx))

    return written


def _relabel_tariff_copy(source: Path, destination: Path, label: str) -> Path:
    """Copy a URDB tariff JSON to ``destination`` with label/name set to ``label``."""
    payload = json.loads(source.read_text(encoding="utf-8"))
    items = payload.get("items")
    if isinstance(items, list) and items:
        items[0]["label"] = label
        items[0]["name"] = label
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return destination


def check_dependency(
    batch_dir: Path,
    batch: str,
    dep_scenario_name: str,
) -> dict[str, Path]:
    """Verify that a dependency scenario's quartet has completed.

    Checks that all four run index files exist.  Returns the output dirs
    keyed by ``{stage}_{variant}``.

    Raises ``RuntimeError`` if any index file is missing.
    """
    results: dict[str, Path] = {}
    for stage in ("precalc", "calibrated"):
        for variant in ("delivery", "supply"):
            run_id = canonical_run_name(batch, dep_scenario_name, stage, variant)
            if not _run_is_complete(batch_dir, run_id):
                raise RuntimeError(
                    f"Dependency {dep_scenario_name!r} run {run_id!r} has not "
                    f"completed — no index file at "
                    f"{batch_dir / '.runs' / f'{run_id}.path'}"
                )
            results[f"{stage}_{variant}"] = read_run_output_dir(batch_dir, run_id)
    return results


def derive_tariffs(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    dep_precalc: dict[str, Path],
) -> Path:
    """Compute subclass RR and create derived tariffs for a multi-rate scenario.

    This runs between a dependency scenario completing and the dependent
    scenario's quartet.  Steps:

    1. Compute differentiated revenue requirements (subclass RR YAML).
    2. Create each subgroup's tariff input (dispatched by structure).

    Args:
        config: Loaded pipeline config.
        scenario: The multi-rate scenario with ``depends_on`` set.
        dep_precalc: The dependency's precalc output dirs
            (``{"precalc_delivery": Path, "precalc_supply": Path}``).

    Returns:
        Path to the written subclass RR YAML.
    """
    rr_path = _compute_subclass_rr(config, scenario, dep_precalc)
    tariffs = _derive_subgroup_tariffs(config, scenario, dep_precalc)
    log.info(
        "derive_tariffs[%s]: wrote RR at %s, %d tariff JSONs",
        scenario.name,
        rr_path,
        len(tariffs),
    )
    return rr_path


# ---------------------------------------------------------------------------
# Quartet flow
# ---------------------------------------------------------------------------


@flow(  # type: ignore[no-matching-overload]  # ty: ignore[no-matching-overload]
    name="run-quartet",
    task_runner=ThreadPoolTaskRunner(max_workers=2),
)
def run_quartet(
    scenario_name: str,
    *,
    batch: str,
    yaml_path: Path,
    batch_dir: Path,
    state: str,
) -> dict[str, Path]:
    """Run one scenario's full quartet: 2 stages × 2 variants = 4 CAIRO runs.

    Steps:

    1. **Precalc stage** — submit delivery and supply ``cairo_run`` tasks
       concurrently (gated by the semaphore).
    2. **Tariff promotion** — extract ``*_calibrated.json`` from precalc
       outputs so they are available as inputs for the calibrated stage.
    3. **Calibrated stage** — submit delivery and supply ``cairo_run`` tasks
       concurrently.

    All run configurations are already materialised in the scenario YAML by
    preflight — this flow only constructs canonical run names and dispatches.

    Args:
        scenario_name: Scenario key (e.g. ``default``).
        batch: Batch prefix for canonical names (e.g. ``md_bge``).
        yaml_path: Path to the generated scenario YAML.
        batch_dir: Batch output directory (contains ``.runs/`` index).
        state: Two-letter state code (e.g. ``md``).

    Returns:
        Mapping of ``{stage}_{variant}`` to the output directory path for
        each of the four runs.
    """
    results: dict[str, Path] = {}

    # --- Stage 1: precalc (delivery + supply in parallel) ---
    precalc_d_name = canonical_run_name(batch, scenario_name, "precalc", "delivery")
    precalc_s_name = canonical_run_name(batch, scenario_name, "precalc", "supply")

    precalc_d_future = cairo_run.submit(
        precalc_d_name,
        yaml_path=yaml_path,
        batch_dir=batch_dir,
        state=state,
        is_delivery=True,
    )
    precalc_s_future = cairo_run.submit(
        precalc_s_name,
        yaml_path=yaml_path,
        batch_dir=batch_dir,
        state=state,
        is_delivery=False,
    )

    precalc_d_dir = precalc_d_future.result()
    precalc_s_dir = precalc_s_future.result()
    results["precalc_delivery"] = precalc_d_dir
    results["precalc_supply"] = precalc_s_dir

    # --- Tariff promotion seam ---
    promoted = _promote_calibrated_tariffs([precalc_d_dir, precalc_s_dir], state=state)
    log.info(
        "run_quartet[%s]: promoted %d calibrated tariffs: %s",
        scenario_name,
        len(promoted),
        [p.name for p in promoted],
    )

    # --- Stage 2: calibrated (delivery + supply in parallel) ---
    cal_d_name = canonical_run_name(batch, scenario_name, "calibrated", "delivery")
    cal_s_name = canonical_run_name(batch, scenario_name, "calibrated", "supply")

    cal_d_future = cairo_run.submit(
        cal_d_name,
        yaml_path=yaml_path,
        batch_dir=batch_dir,
        state=state,
        is_delivery=True,
    )
    cal_s_future = cairo_run.submit(
        cal_s_name,
        yaml_path=yaml_path,
        batch_dir=batch_dir,
        state=state,
        is_delivery=False,
    )

    results["calibrated_delivery"] = cal_d_future.result()
    results["calibrated_supply"] = cal_s_future.result()

    return results

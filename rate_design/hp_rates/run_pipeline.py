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

import logging
import subprocess
import sys
import threading
from pathlib import Path

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from utils.mid.copy_calibrated_tariff_from_run import (
    copy_calibrated_tariff_from_run_dir,
)


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

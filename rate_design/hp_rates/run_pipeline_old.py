"""Prefect pipeline for heat pump rate design scenario runs.

Replaces the Justfile shell orchestration with a Python-native Prefect pipeline.

Vocabulary
----------
The pipeline is described entirely in terms of the following canonical concepts.
None of them are tied to the legacy "run 1..8" numbering — a scenario is
identified by its name and its ``quartet`` kind, not by a position in a run list.

* **Scenario** — one rate design to evaluate (e.g. ``default``,
  ``hp_seasonal_percustomer``). Declared in the pipeline YAML with a ``quartet``
  kind (see below).
* **Variant** — the cost scope of a single CAIRO run. Exactly two:
    - ``delivery`` — delivery charges only; supply marginal costs zeroed;
      billed with ``billing_kwh=True``.
    - ``supply`` — adds the real supply marginal costs; ``billing_kwh=False``.
* **Stage** — where a scenario sits in the calibration lifecycle. Exactly two,
  run in order:
    - ``precalc`` — pre-calibration on the baseline population (ResStock
      ``upgrade_precalc``). CAIRO solves for revenue-neutral tariffs and emits
      ``tariff_final_config.json``.
    - ``calibrated`` — the calibrated tariffs evaluated on the target population
      (ResStock ``upgrade_calibrated``). **Always consumes stage-1 outputs
      (`*_calibrated.json`), never stage-1 inputs.**
* **Rate mode** — how many distinct tariffs a stage runs: ``single`` (one tariff
  for the whole population) or ``multi`` (per-subgroup tariffs + a bldg→tariff map).
* **Run** — one CAIRO invocation = one (stage, variant) pair. The atomic unit
  of work (``cairo_run``).
* **Quartet** — the four runs that fully evaluate one scenario: the cross
  product of the two stages and two variants. A ``quartet_evaluator`` owns one.
* **tariff_promotion seam** — the handoff that joins a quartet's two stages: the
  precalc stage's calibrated tariffs (``*_calibrated.json``) become the calibrated
  stage's inputs (``promote_subgroup_tariff`` -> ``PromotionResult``).

Quartet kinds
-------------
A scenario picks exactly one ``quartet`` kind, which fixes both stages' rate modes
and the derived promotion policy (there is no separate promotion knob):

===================== ========= ========== ===================== ================
quartet               precalc   calibrated tariff_promotion       needs subgroups
===================== ========= ========== ===================== ================
``single_rate``       single    single     ``identity``          no
``multi_rate_collapsed`` multi   single     ``collapse_to_derived`` yes
``multi_rate_preserved`` multi   multi      ``keep_subgroups``    yes
===================== ========= ========== ===================== ================

* ``single_rate`` — one tariff for the whole population through both stages.
* ``multi_rate_collapsed`` — calibrate N per-subgroup tariffs, then evaluate the
  *target* population on a single promoted subgroup's calibrated tariff (which
  subgroup is ``promote_subgroup``, defaulting to the sole ``source: derived``
  subgroup). Right for HP-adoption scenarios: the target world is all-HP.
* ``multi_rate_preserved`` — keep every subgroup's calibrated tariff through the
  calibrated stage. Right when the grouping dimension persists across the
  precalc→target population change (income tier, geography, building type).

Anatomy of a quartet
--------------------
::

    quartet(scenario)
      ├─ precalc stage      (run_stage; single or multi per quartet kind)
      │     ├─ run: precalc · delivery      cairo_run.submit(billing_kwh=True)
      │     └─ run: precalc · supply        cairo_run.submit(billing_kwh=False)
      │           └─ writes tariff_final_config.json → *_calibrated.json
      ├─ tariff_promotion seam  (promote_subgroup_tariff → PromotionResult)
      └─ calibrated stage   (run_stage; single or multi per quartet kind)
            ├─ run: calibrated · delivery   (tariffs = *_calibrated.json)
            └─ run: calibrated · supply

The two variants within a stage run concurrently; the two stages run in sequence
(the calibrated stage consumes the precalc stage's ``*_calibrated.json`` across
the tariff_promotion seam). ``quartet_evaluator`` runs the whole quartet for any
kind — both stages always execute.

Architecture (three tiers)
--------------------------
* **Plain functions** — ``run_stage`` (runs one stage = its variant pair),
  ``derive_settings`` (compact config -> ScenarioSettings),
  ``promote_subgroup_tariff`` (the tariff_promotion seam).
* **Tasks** — ``cairo_run`` (one run, global-gated + cached),
  ``resolve_subgroups`` (data-driven N-subgroup split + stage-2 maps),
  ``compute_subclass_rr`` (differentiated revenue requirements),
  ``derive_seasonal_tariff`` (rate designer).
* **Flows** — ``quartet_evaluator`` subflow (one quartet, any kind),
  ``default_flow`` / ``hp_seasonal_flow`` scenario flows, and the
  ``hp_rates_pipeline`` master flow that wires them with a sequential fan-out.

Concurrency: every run is a ``@task`` submitted via ``.submit()`` inside the
evaluator subflow (``ThreadPoolTaskRunner(max_workers=2)`` expresses the
delivery ∥ supply variant pair). A global ``cairo-runs`` concurrency limit caps
the number of CAIRO processes actually resident in memory regardless of how wide
the scenario fan-out grows.

Configuration is loaded from a compact multi-scenario pipeline YAML; all file
paths are derived from naming conventions — no per-run path duplication.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import polars as pl
import yaml
from prefect import flow, tags, task
from prefect.cache_policies import CacheKeyFnPolicy
from prefect.concurrency.sync import concurrency
from prefect.context import TaskRunContext
from prefect.task_runners import ThreadPoolTaskRunner

from rate_design.hp_rates.run_scenario import ScenarioSettings, run
from utils.mid.compute_subclass_rr import (
    SUBCLASS_RR_ALLOCATION_METHODS,
    _resolve_winter_months,
    _write_revenue_requirement_yamls,
    compute_subclass_rr,
    compute_subclass_seasonal_discount_inputs,
)
from utils.mid.copy_calibrated_tariff_from_run import (
    copy_calibrated_tariff_from_run_dir,
)
from utils.pre.create_tariff import create_seasonal_rate
from utils.pre.electric_tariff_mapper import generate_tariff_map_from_scenario_keys
from utils.scenario_config import (
    _parse_path_tariffs,
    _parse_path_tariffs_gas,
    _parse_utility_revenue_requirement,
)

log = logging.getLogger(__name__)

HP_RATES_DIR = Path(__file__).resolve().parent

CAIRO_CONCURRENCY_LIMIT = "cairo-runs"

# Residual allocation method -> BAT cross-subsidy column in CAIRO outputs.
ALLOCATION_TO_BAT_COL: dict[str, str] = {
    "percustomer": "BAT_percustomer",
    "epmc": "BAT_epmc",
    "volumetric": "BAT_vol",
}


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StageResult:
    """Outputs of one stage of a quartet: its ``delivery`` and ``supply`` runs.

    ``calibrated_tariff_paths`` holds the URDB tariffs extracted from the runs'
    ``tariff_final_config.json`` (populated for the precalc stage, whose whole
    purpose is to calibrate tariffs; empty for the calibrated stage).
    """

    delivery_output_dir: Path
    supply_output_dir: Path
    calibrated_tariff_paths: list[Path] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class EvalResult:
    """The outputs of one scenario's quartet, produced by ``quartet_evaluator``.

    ``quartet_evaluator`` always runs both stages, so both ``precalc`` and
    ``calibrated`` are populated. ``calibrated`` remains ``Optional`` only to keep
    the type honest for callers that construct partial results.
    """

    precalc: StageResult
    calibrated: StageResult | None = None


@dataclass(frozen=True, slots=True)
class SubgroupSpec:
    """One subgroup of a multi-rate scenario's population split.

    Attributes:
        alias: Short label for the subgroup (e.g. ``"hp"``); becomes part of the
            tariff filename stem and the differentiated-RR keys.
        values: The raw ``postprocess_group.<group_col>`` values that belong to
            this subgroup (e.g. ``["true"]``). May list several values that map
            to one subgroup.
        structure: The tariff structure to give this subgroup (e.g. ``"seasonal"``,
            ``"default"``); part of the tariff filename stem.
        source: How the subgroup's tariff input is produced —
            ``"derived"`` (a designer task computes it, e.g. a seasonal discount)
            or ``"default_calibrated"`` (relabel-copy of the calibrated single-rate
            default tariff).
    """

    alias: str
    values: list[str]
    structure: str
    source: str


@dataclass(frozen=True, slots=True)
class SubclassConfig:
    """A population grouping column plus its ordered subgroups (arbitrary N).

    ``group_col`` is the ``postprocess_group`` column suffix that partitions the
    population (e.g. ``"has_hp"``); ``subgroups`` enumerates the partitions.
    """

    group_col: str
    subgroups: list[SubgroupSpec]


@dataclass(frozen=True, slots=True)
class SubclassResolution:
    """The concrete multi-rate layout produced by ``resolve_subgroups``.

    Turns an abstract ``SubclassConfig`` into the exact tariff stems, file paths,
    tariff maps (for **both** stages), and value->subgroup mapping that the
    downstream designer, revenue-requirement, and evaluator steps consume.

    Precalc-stage tariffs are the *inputs* (``<stem>.json``); calibrated-stage
    tariffs are the *outputs* (``<stem>_calibrated.json``), matched by maps whose
    ``tariff_key`` equals the calibrated file stem.

    Attributes:
        group_col: The grouping column suffix (from ``SubclassConfig``).
        aliases: Subgroup aliases in declaration order.
        promoted_alias: For ``multi_rate_collapsed``, the subgroup whose calibrated
            tariff is promoted to the single-rate calibrated stage (from the
            scenario's ``promote_subgroup``, else the sole ``source: derived``
            subgroup). Empty string when promotion does not collapse to one alias
            (i.e. ``multi_rate_preserved``).
        selectors: ``alias -> "v1,v2,..."`` — the CAIRO/tariff-mapper form of the
            per-subgroup ``values``.
        alias_to_stem / alias_to_stem_supply: ``alias -> precalc tariff filename
            stem`` for the delivery and supply variants. Append ``_calibrated`` for
            the calibrated-stage stems.
        group_value_to_subclass: ``raw group value -> alias`` (the inverse of
            ``selectors``), used to key differentiated revenue requirements.
        path_tariffs_electric / path_tariffs_electric_supply: ``alias -> precalc
            tariff JSON path`` for each variant.
        path_tariff_map / path_tariff_map_supply: precalc-stage bldg_id->tariff_key
            map CSV per variant (keys = precalc stems).
        path_tariff_map_calibrated / path_tariff_map_calibrated_supply:
            calibrated-stage map CSV per variant (keys = calibrated stems). For
            ``multi_rate_collapsed`` every building maps to the single promoted
            calibrated stem; for ``multi_rate_preserved`` each building maps to its
            subgroup's calibrated stem.
    """

    group_col: str
    aliases: list[str]
    promoted_alias: str
    selectors: dict[str, str]
    alias_to_stem: dict[str, str]
    alias_to_stem_supply: dict[str, str]
    group_value_to_subclass: dict[str, str]
    path_tariffs_electric: dict[str, Path]
    path_tariffs_electric_supply: dict[str, Path]
    path_tariff_map: Path
    path_tariff_map_supply: Path
    path_tariff_map_calibrated: Path
    path_tariff_map_calibrated_supply: Path


@dataclass(frozen=True, slots=True)
class PromotionResult:
    """Calibrated-stage tariff inputs chosen by the ``tariff_promotion`` seam.

    The single source of truth for what the calibrated stage runs. It **always**
    references stage-1 *outputs* (``*_calibrated.json``), never stage-1 inputs.

    Attributes:
        policy: The promotion policy applied (``identity`` / ``collapse_to_derived``
            / ``keep_subgroups``).
        mode: Calibrated-stage rate mode — ``"single"`` (one ``all`` tariff) or
            ``"multi"`` (per-subgroup tariffs).
        tariffs_delivery / tariffs_supply: ``key -> calibrated tariff JSON path``
            for each variant (``{"all": ...}`` for single mode; ``{alias: ...}``
            for multi mode).
        map_delivery / map_supply: the calibrated-stage bldg->tariff_key map per
            variant (keys = calibrated file stems).
    """

    policy: str
    mode: str
    tariffs_delivery: dict[str, Path]
    tariffs_supply: dict[str, Path]
    map_delivery: Path
    map_supply: Path


@dataclass(frozen=True, slots=True)
class DefaultFlowResult:
    """Output of ``default_flow``.

    Bundles the default scenario's quartet outputs (``eval_result``) with the
    differentiated revenue-requirement YAML (``subclass_rr_path``) that downstream
    multi-rate scenarios depend on.
    """

    eval_result: EvalResult
    subclass_rr_path: Path


# ---------------------------------------------------------------------------
# Pipeline config (loaded from YAML)
# ---------------------------------------------------------------------------


# Quartet kinds and the (precalc mode, calibrated mode, promotion policy) each
# fixes. The user picks one ``quartet`` per scenario; everything else is derived.
QUARTET_KINDS: frozenset[str] = frozenset(
    {"single_rate", "multi_rate_collapsed", "multi_rate_preserved"}
)
_QUARTET_TO_PROMOTION: dict[str, str] = {
    "single_rate": "identity",
    "multi_rate_collapsed": "collapse_to_derived",
    "multi_rate_preserved": "keep_subgroups",
}
# Quartet kinds whose precalc stage runs per-subgroup (multi) tariffs.
_MULTI_PRECALC_QUARTETS: frozenset[str] = frozenset(
    {"multi_rate_collapsed", "multi_rate_preserved"}
)
# Quartet kinds whose calibrated stage runs per-subgroup (multi) tariffs.
_MULTI_EVAL_QUARTETS: frozenset[str] = frozenset({"multi_rate_preserved"})


@dataclass(frozen=True, slots=True)
class ScenarioConfig:
    """One scenario declaration from the pipeline YAML.

    A scenario is fully characterized by its ``quartet`` kind (one of
    ``QUARTET_KINDS``), which determines the precalc/calibrated rate modes and the
    derived ``tariff_promotion`` policy. ``subclass_config`` (and typically
    ``residual_allocation``) are required for the two multi-precalc kinds and
    absent for ``single_rate``.

    Attributes:
        name: Scenario key from the YAML (e.g. ``"default"``).
        quartet: One of ``QUARTET_KINDS`` — ``single_rate``,
            ``multi_rate_collapsed``, or ``multi_rate_preserved``.
        promote_subgroup: For ``multi_rate_collapsed`` only, the subgroup alias
            whose calibrated tariff is promoted to the single-rate calibrated
            stage. ``None`` means "use the sole ``source: derived`` subgroup".
        depends_on: Optional name of a scenario this one depends on (for graph
            provenance; the ``default`` dependency is wired structurally).
        residual_allocation_delivery / residual_allocation_supply: Residual cost
            allocation method per variant (e.g. ``percustomer``, ``passthrough``).
        subclass_config: The population split; required iff ``precalc_is_multi``.
    """

    name: str
    quartet: str
    promote_subgroup: str | None = None
    depends_on: str | None = None
    residual_allocation_delivery: str | None = None
    residual_allocation_supply: str | None = None
    subclass_config: SubclassConfig | None = None

    @property
    def tariff_promotion(self) -> str:
        """The seam policy implied by the quartet kind (never user-specified)."""
        return _QUARTET_TO_PROMOTION[self.quartet]

    @property
    def precalc_is_multi(self) -> bool:
        """True if the precalc stage runs per-subgroup (multi-rate) tariffs."""
        return self.quartet in _MULTI_PRECALC_QUARTETS

    @property
    def eval_is_multi(self) -> bool:
        """True if the calibrated stage runs per-subgroup (multi-rate) tariffs."""
        return self.quartet in _MULTI_EVAL_QUARTETS


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """Parsed pipeline YAML — utility-level defaults plus a scenario dict."""

    state: str
    utility: str
    year: int
    solar_pv_compensation: str
    process_workers: int
    max_concurrent_cairo_runs: int

    resstock_base: str
    upgrade_precalc: str
    upgrade_calibrated: str

    mc_dist_and_sub_tx: str
    mc_bulk_tx: str
    mc_supply_energy: str
    mc_supply_capacity: str

    rr_single_rate: str
    rr_single_rate_calibrated: str
    rr_multi_rate: str
    rr_multi_rate_calibrated: str

    scenarios: dict[str, ScenarioConfig]

    @property
    def state_config_dir(self) -> Path:
        return HP_RATES_DIR / self.state.lower() / "config"

    def scenario(self, name: str) -> ScenarioConfig:
        if name not in self.scenarios:
            raise KeyError(
                f"Scenario {name!r} not found in pipeline config "
                f"(available: {sorted(self.scenarios)})"
            )
        return self.scenarios[name]


# Legacy ``type`` values mapped onto quartet kinds (back-compat for older YAMLs).
_LEGACY_TYPE_TO_QUARTET: dict[str, str] = {
    "single_rate": "single_rate",
    "multi_rate": "multi_rate_collapsed",
}


def _parse_quartet(name: str, raw: dict[str, Any]) -> str:
    """Read a scenario's ``quartet`` kind, tolerating the legacy ``type`` field.

    Prefers the ``quartet`` key. If absent, maps the legacy ``type``
    (``single_rate`` -> ``single_rate``; ``multi_rate`` -> ``multi_rate_collapsed``,
    which reproduces the historical collapse-to-derived behavior).
    """
    if "quartet" in raw:
        return str(raw["quartet"])
    legacy = raw.get("type")
    if legacy is not None and str(legacy) in _LEGACY_TYPE_TO_QUARTET:
        return _LEGACY_TYPE_TO_QUARTET[str(legacy)]
    raise ValueError(
        f"Scenario {name!r} must declare a 'quartet' (one of {sorted(QUARTET_KINDS)})."
    )


def _validate_scenario(scenario: ScenarioConfig) -> None:
    """Validate a scenario's quartet/subclass/promotion invariants."""
    if scenario.quartet not in QUARTET_KINDS:
        raise ValueError(
            f"Scenario {scenario.name!r}: unknown quartet {scenario.quartet!r} "
            f"(expected one of {sorted(QUARTET_KINDS)})."
        )
    if scenario.precalc_is_multi and scenario.subclass_config is None:
        raise ValueError(
            f"Scenario {scenario.name!r} ({scenario.quartet}) requires a "
            "'subclass_config' for its multi-rate precalc stage."
        )
    if not scenario.precalc_is_multi and scenario.subclass_config is not None:
        raise ValueError(
            f"Scenario {scenario.name!r} ({scenario.quartet}) is single-rate and "
            "must not declare a 'subclass_config'."
        )
    if (
        scenario.promote_subgroup is not None
        and scenario.quartet != "multi_rate_collapsed"
    ):
        raise ValueError(
            f"Scenario {scenario.name!r}: 'promote_subgroup' only applies to "
            f"'multi_rate_collapsed' (got quartet {scenario.quartet!r})."
        )


def _parse_subclass_config(raw: dict[str, Any] | None) -> SubclassConfig | None:
    if raw is None:
        return None
    subgroups_raw = raw["subgroups"]
    subgroups = [
        SubgroupSpec(
            alias=alias,
            values=[str(v) for v in spec["values"]],
            structure=str(spec["structure"]),
            source=str(spec["source"]),
        )
        for alias, spec in subgroups_raw.items()
    ]
    return SubclassConfig(group_col=str(raw["group_col"]), subgroups=subgroups)


def load_pipeline_config(yaml_path: Path) -> PipelineConfig:
    """Load the compact multi-scenario pipeline YAML into a PipelineConfig."""
    with yaml_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    resstock = data["resstock"]
    mc = data["marginal_costs"]
    rr = data["revenue_requirement"]

    scenarios: dict[str, ScenarioConfig] = {}
    for name, raw in data["scenarios"].items():
        ra = raw.get("residual_allocation") or {}
        scenario = ScenarioConfig(
            name=name,
            quartet=_parse_quartet(name, raw),
            promote_subgroup=raw.get("promote_subgroup"),
            depends_on=raw.get("depends_on"),
            residual_allocation_delivery=ra.get("delivery"),
            residual_allocation_supply=ra.get("supply"),
            subclass_config=_parse_subclass_config(raw.get("subclass_config")),
        )
        _validate_scenario(scenario)
        scenarios[name] = scenario

    return PipelineConfig(
        state=data["state"],
        utility=data["utility"],
        year=int(data["year"]),
        solar_pv_compensation=data.get("solar_pv_compensation", "net_metering"),
        process_workers=int(data.get("process_workers", 8)),
        max_concurrent_cairo_runs=int(data.get("max_concurrent_cairo_runs", 2)),
        resstock_base=resstock["base"],
        upgrade_precalc=resstock["upgrade_precalc"],
        upgrade_calibrated=resstock["upgrade_calibrated"],
        mc_dist_and_sub_tx=mc["dist_and_sub_tx"],
        mc_bulk_tx=mc["bulk_tx"],
        mc_supply_energy=mc["supply_energy"],
        mc_supply_capacity=mc["supply_capacity"],
        rr_single_rate=rr["single_rate"],
        rr_single_rate_calibrated=rr["single_rate_calibrated"],
        rr_multi_rate=rr["multi_rate"],
        rr_multi_rate_calibrated=rr["multi_rate_calibrated"],
        scenarios=scenarios,
    )


# ---------------------------------------------------------------------------
# Naming derivation
# ---------------------------------------------------------------------------


def _single_rate_stem(config: PipelineConfig, *, supply: bool, calibrated: bool) -> str:
    """Single-rate tariff JSON stem: ``{utility}_default[_supply][_calibrated]``."""
    name = f"{config.utility}_default"
    if supply:
        name += "_supply"
    if calibrated:
        name += "_calibrated"
    return name


def _multi_rate_stem(
    config: PipelineConfig,
    alias: str,
    structure: str,
    allocation: str,
    *,
    supply: bool,
    calibrated: bool,
) -> str:
    """Multi-rate stem: ``{utility}_{alias}_{structure}_{allocation}[_supply][_cal]``."""
    name = f"{config.utility}_{alias}_{structure}_{allocation}"
    if supply:
        name += "_supply"
    if calibrated:
        name += "_calibrated"
    return name


def _tariff_json_dir(config: PipelineConfig) -> Path:
    return config.state_config_dir / "tariffs" / "electric"


def _tariff_map_dir(config: PipelineConfig) -> Path:
    return config.state_config_dir / "tariff_maps" / "electric"


def _single_rate_map_path(
    config: PipelineConfig, *, supply: bool, calibrated: bool
) -> Path:
    """Single-rate map: ``{utility}_default[_calibrated][_supply].csv``."""
    name = f"{config.utility}_default"
    if calibrated:
        name += "_calibrated"
    if supply:
        name += "_supply"
    return _tariff_map_dir(config) / f"{name}.csv"


def _scenario_map_path(
    config: PipelineConfig, scenario: str, *, supply: bool, calibrated: bool
) -> Path:
    """Multi-rate scenario map: ``{utility}_{scenario}[_calibrated][_supply].csv``."""
    name = f"{config.utility}_{scenario}"
    if calibrated:
        name += "_calibrated"
    if supply:
        name += "_supply"
    return _tariff_map_dir(config) / f"{name}.csv"


def _gas_tariff_map_path(config: PipelineConfig, *, calibrated: bool) -> Path:
    upgrade = config.upgrade_calibrated if calibrated else config.upgrade_precalc
    return (
        config.state_config_dir
        / "tariff_maps"
        / "gas"
        / f"{config.utility}_u{upgrade}.csv"
    )


def _supply_mc_path(base_path: str, *, include_supply: bool) -> str:
    """Return the real MC path for supply runs, else swap data.parquet -> zero.parquet."""
    if include_supply:
        return base_path
    return base_path.replace("/data.parquet", "/zero.parquet")


# ---------------------------------------------------------------------------
# Settings derivation
# ---------------------------------------------------------------------------


def _resstock_paths(
    config: PipelineConfig, *, calibrated: bool
) -> tuple[Path, Path, Path]:
    """Return (metadata, loads, utility_assignment) resstock paths for the stage."""
    state_upper = config.state.upper()
    upgrade = config.upgrade_calibrated if calibrated else config.upgrade_precalc
    base = config.resstock_base
    metadata = Path(
        f"{base}/metadata/state={state_upper}/upgrade={upgrade}/metadata-sb.parquet"
    )
    loads = Path(f"{base}/load_curve_hourly/state={state_upper}/upgrade={upgrade}/")
    utility_assignment = Path(
        f"{base}/metadata_utility/state={state_upper}/utility_assignment.parquet"
    )
    return metadata, loads, utility_assignment


def _electric_utility_stats_path(config: PipelineConfig) -> str:
    eia_year = config.year - 1
    return (
        f"s3://data.sb/eia/861/electric_utility_stats/year={eia_year}"
        f"/state={config.state.upper()}/data.parquet"
    )


def derive_settings(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    batch: str,
    *,
    supply: bool,
    calibrated: bool,
    resolution: SubclassResolution | None = None,
    promotion: PromotionResult | None = None,
) -> ScenarioSettings:
    """Expand the compact config into a full ``ScenarioSettings`` for one run.

    Builds the settings for a single (stage, variant) run of a scenario's quartet.
    All file paths are derived from naming conventions rather than duplicated in
    the YAML.

    The precalc stage's rate mode follows the scenario's quartet kind
    (``precalc_is_multi``). The calibrated stage's tariffs and map come **only**
    from the ``tariff_promotion`` seam (``promotion``), so the calibrated stage
    always consumes stage-1 outputs (``*_calibrated.json``).

    Args:
        config: The loaded pipeline config (state/utility/batch-wide inputs).
        scenario: The scenario being evaluated.
        batch: Batch name; appended to the S3/output path.
        supply: Variant selector — ``True`` builds the ``supply`` run (real supply
            marginal costs, ``billing_kwh`` implicitly ``False``); ``False`` builds
            the ``delivery`` run (supply MC zeroed, ``billing_kwh`` ``True``).
        calibrated: Stage selector — ``False`` builds the ``precalc`` stage (baseline
            population, tariffs to be solved); ``True`` builds the ``calibrated``
            stage (target population, promoted calibrated tariffs as input).
        resolution: Required for multi-rate precalc runs and multi-mode calibrated
            runs; the resolved per-subgroup layout.
        promotion: Required for the calibrated stage; the ``PromotionResult`` from
            ``promote_subgroup_tariff`` that fixes the stage's tariffs and map.

    Returns:
        A fully-populated ``ScenarioSettings`` whose ``run_name`` encodes the
        scenario, stage, and variant (``{state}_{utility}_{scenario}_{stage}_{variant}``).
    """
    state_upper = config.state.upper()
    run_type = "default" if calibrated else "precalc"
    variant_suffix = "supply" if supply else "delivery"
    stage_suffix = "calibrated" if calibrated else "precalc"
    run_name = (
        f"{config.state}_{config.utility}_{scenario.name}"
        f"_{stage_suffix}_{variant_suffix}"
    )

    path_config = config.state_config_dir
    output_dir = (
        Path(
            f"/data.sb/switchbox/cairo/outputs/hp_rates/{config.state}/{config.utility}"
        )
        / batch
    )

    metadata, loads, utility_assignment = _resstock_paths(config, calibrated=calibrated)

    if calibrated:
        if promotion is None:
            raise ValueError(
                f"derive_settings for the calibrated stage of {scenario.name!r} "
                "requires a PromotionResult."
            )
        settings = _derive_promoted_settings(
            config, scenario, promotion, resolution, supply=supply
        )
    elif scenario.precalc_is_multi:
        settings = _derive_multi_rate_settings(
            config,
            scenario,
            resolution=_require_resolution(resolution, scenario),
            supply=supply,
            calibrated=False,
        )
    else:
        settings = _derive_single_rate_settings(config, supply=supply)

    path_tariff_maps_gas = _gas_tariff_map_path(config, calibrated=calibrated)
    path_tariffs_gas = _parse_path_tariffs_gas(
        "tariffs/gas", path_tariff_maps_gas, path_config
    )

    return ScenarioSettings(
        run_name=run_name,
        run_type=run_type,
        state=state_upper,
        utility=config.utility,
        path_results=output_dir,
        path_resstock_metadata=metadata,
        path_resstock_loads=loads,
        path_utility_assignment=utility_assignment,
        path_dist_and_sub_tx_mc=config.mc_dist_and_sub_tx,
        path_tariff_maps_electric=settings.path_tariff_maps_electric,
        path_tariff_maps_gas=path_tariff_maps_gas,
        path_tariffs_electric=settings.path_tariffs_electric,
        path_tariffs_gas=path_tariffs_gas,
        rr_total=settings.rr_total,
        subclass_rr=settings.subclass_rr,
        run_includes_subclasses=settings.run_includes_subclasses,
        residual_allocation_delivery=settings.residual_allocation_delivery,
        residual_allocation_supply=settings.residual_allocation_supply,
        path_electric_utility_stats=_electric_utility_stats_path(config),
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
        customer_count_override=settings.customer_count_override,
        kwh_scale_factor=settings.kwh_scale_factor,
        subclass_config=settings.subclass_config,
    )


@dataclass(frozen=True, slots=True)
class _ResolvedTariffSettings:
    """Intermediate: tariff/RR fields resolved for one CAIRO run (single or multi)."""

    path_tariff_maps_electric: Path
    path_tariffs_electric: dict[str, Path]
    rr_total: float
    subclass_rr: dict[str, float] | None
    run_includes_subclasses: bool
    residual_allocation_delivery: str | None
    residual_allocation_supply: str | None
    customer_count_override: float | None
    kwh_scale_factor: float | None
    subclass_config: dict[str, Any] | None


def _derive_single_rate_settings(
    config: PipelineConfig, *, supply: bool
) -> _ResolvedTariffSettings:
    """Precalc-stage settings for a single-rate scenario (one ``all`` tariff)."""
    path_config = config.state_config_dir
    stem = _single_rate_stem(config, supply=supply, calibrated=False)
    json_path = _tariff_json_dir(config) / f"{stem}.json"
    raw_path_tariffs_electric = {"all": str(json_path.relative_to(path_config))}
    map_path = _single_rate_map_path(config, supply=supply, calibrated=False)
    return _single_rate_resolved(
        config,
        config.rr_single_rate,
        raw_path_tariffs_electric,
        map_path,
        supply=supply,
    )


def _single_rate_resolved(
    config: PipelineConfig,
    rr_yaml_rel: str,
    raw_path_tariffs_electric: dict[str, str],
    map_path: Path,
    *,
    supply: bool,
) -> _ResolvedTariffSettings:
    """Resolve a single-rate ``_ResolvedTariffSettings`` from tariff/map/RR inputs."""
    path_config = config.state_config_dir
    rr_config = _parse_utility_revenue_requirement(
        rr_yaml_rel,
        path_config,
        raw_path_tariffs_electric,
        add_supply=supply,
        run_includes_subclasses=False,
    )
    path_tariffs_electric = _parse_path_tariffs(
        raw_path_tariffs_electric, map_path, path_config, "electric"
    )
    return _ResolvedTariffSettings(
        path_tariff_maps_electric=map_path,
        path_tariffs_electric=path_tariffs_electric,
        rr_total=rr_config.rr_total,
        subclass_rr=None,
        run_includes_subclasses=False,
        residual_allocation_delivery=None,
        residual_allocation_supply=None,
        customer_count_override=rr_config.customer_count_override,
        kwh_scale_factor=rr_config.kwh_scale_factor,
        subclass_config=None,
    )


def _derive_multi_rate_settings(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    *,
    resolution: SubclassResolution,
    supply: bool,
    calibrated: bool,
) -> _ResolvedTariffSettings:
    """Precalc-stage settings for a multi-rate scenario (per-subgroup tariffs).

    Only ever called for the precalc stage (``calibrated=False``); the calibrated
    stage is built by ``_derive_promoted_settings`` from the promotion seam.
    """
    if calibrated:
        raise ValueError(
            "_derive_multi_rate_settings is precalc-only; the calibrated stage "
            "flows through _derive_promoted_settings."
        )
    path_config = config.state_config_dir
    tariffs = (
        resolution.path_tariffs_electric_supply
        if supply
        else resolution.path_tariffs_electric
    )
    raw_path_tariffs_electric = {
        alias: str(path.relative_to(path_config)) for alias, path in tariffs.items()
    }
    map_path = (
        resolution.path_tariff_map_supply if supply else resolution.path_tariff_map
    )
    return _multi_rate_resolved(
        config,
        scenario,
        resolution,
        config.rr_multi_rate,
        raw_path_tariffs_electric,
        map_path,
        supply=supply,
    )


def _multi_rate_resolved(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    resolution: SubclassResolution,
    rr_yaml_rel: str,
    raw_path_tariffs_electric: dict[str, str],
    map_path: Path,
    *,
    supply: bool,
) -> _ResolvedTariffSettings:
    """Resolve a multi-rate ``_ResolvedTariffSettings`` from tariff/map/RR inputs."""
    path_config = config.state_config_dir
    rr_config = _parse_utility_revenue_requirement(
        rr_yaml_rel,
        path_config,
        raw_path_tariffs_electric,
        add_supply=supply,
        run_includes_subclasses=True,
        residual_allocation_delivery=scenario.residual_allocation_delivery,
        residual_allocation_supply=scenario.residual_allocation_supply,
    )
    path_tariffs_electric = _parse_path_tariffs(
        raw_path_tariffs_electric, map_path, path_config, "electric"
    )
    subclass_config = {
        "group_col": resolution.group_col,
        "selectors": resolution.selectors,
    }
    return _ResolvedTariffSettings(
        path_tariff_maps_electric=map_path,
        path_tariffs_electric=path_tariffs_electric,
        rr_total=rr_config.rr_total,
        subclass_rr=rr_config.subclass_rr,
        run_includes_subclasses=True,
        residual_allocation_delivery=rr_config.residual_allocation_delivery,
        residual_allocation_supply=rr_config.residual_allocation_supply,
        customer_count_override=rr_config.customer_count_override,
        kwh_scale_factor=rr_config.kwh_scale_factor,
        subclass_config=subclass_config,
    )


def _derive_promoted_settings(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    promotion: PromotionResult,
    resolution: SubclassResolution | None,
    *,
    supply: bool,
) -> _ResolvedTariffSettings:
    """Calibrated-stage settings built from the ``tariff_promotion`` seam.

    The promotion result supplies the tariffs (always ``*_calibrated.json``) and
    the calibrated-stage map. ``single`` mode uses the single-rate calibrated RR;
    ``multi`` mode (``multi_rate_preserved``) uses the multi calibrated RR and the
    resolution's subgroup selectors. The multi calibrated RR YAML must carry
    per-subgroup revenue requirements keyed by subgroup alias.
    """
    path_config = config.state_config_dir
    tariffs = promotion.tariffs_supply if supply else promotion.tariffs_delivery
    map_path = promotion.map_supply if supply else promotion.map_delivery
    raw_path_tariffs_electric = {
        key: str(path.relative_to(path_config)) for key, path in tariffs.items()
    }
    if promotion.mode == "single":
        return _single_rate_resolved(
            config,
            config.rr_single_rate_calibrated,
            raw_path_tariffs_electric,
            map_path,
            supply=supply,
        )
    if resolution is None:
        raise ValueError(
            f"Scenario {scenario.name!r}: multi-mode promotion requires a "
            "SubclassResolution for its subgroup selectors."
        )
    return _multi_rate_resolved(
        config,
        scenario,
        resolution,
        config.rr_multi_rate_calibrated,
        raw_path_tariffs_electric,
        map_path,
        supply=supply,
    )


def _require_resolution(
    resolution: SubclassResolution | None, scenario: ScenarioConfig
) -> SubclassResolution:
    if resolution is None:
        raise ValueError(
            f"Scenario {scenario.name!r} is multi_rate but no SubclassResolution "
            "was supplied to derive_settings."
        )
    return resolution


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _git_short_sha() -> str:
    """Return the short git commit hash, or 'unknown' if unavailable."""
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _with_workers(settings: ScenarioSettings, workers: int) -> ScenarioSettings:
    """Return a copy of settings with adjusted process_workers."""
    return dataclasses.replace(settings, process_workers=workers)


def _extract_calibrated_tariffs(output_dir: Path, config: PipelineConfig) -> list[Path]:
    """Convert + copy calibrated tariffs from a run dir (if it produced any)."""
    if not (output_dir / "tariff_final_config.json").exists():
        return []
    return copy_calibrated_tariff_from_run_dir(output_dir, state=config.state)


def _scan_parquet_any(path: str) -> pl.LazyFrame:
    """Scan a local, /data.sb/, or s3:// parquet path (lazy)."""
    if path.startswith("s3://") or path.startswith("/data.sb/"):
        from utils.file_io import get_aws_storage_options

        uri = path if path.startswith("s3://") else "s3://" + path.lstrip("/")
        return pl.scan_parquet(uri, storage_options=get_aws_storage_options())
    return pl.scan_parquet(path)


def _relabel_tariff_copy(source: Path, destination: Path, label: str) -> Path:
    """Copy a URDB tariff JSON to ``destination`` with item label/name set to ``label``.

    Naming-agnostic: the caller owns ``destination`` and ``label`` (both derived
    from the ``SubclassResolution`` stems). Pass ``label`` equal to the
    destination's filename stem (the tariff key), since CAIRO matches tariffs by
    label; the ``{utility}_{alias}_{structure}_{allocation}`` convention lives in
    ``_multi_rate_stem`` / ``resolve_subgroups``, not here.
    """
    payload = json.loads(source.read_text(encoding="utf-8"))
    items = payload.get("items")
    if isinstance(items, list) and items:
        items[0]["label"] = label
        items[0]["name"] = label
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return destination


# ---------------------------------------------------------------------------
# Prefect cache policy
# ---------------------------------------------------------------------------


def _cairo_cache_key(context: TaskRunContext, parameters: dict[str, Any]) -> str:
    """Cache key = run_name + output base (which encodes the batch)."""
    settings: ScenarioSettings = parameters["settings"]
    return f"{settings.run_name}|{settings.path_results}"


_cairo_cache_policy = CacheKeyFnPolicy(cache_key_fn=_cairo_cache_key)


# ---------------------------------------------------------------------------
# Core CAIRO task + stage helper
# ---------------------------------------------------------------------------


@task(log_prints=True, persist_result=True, cache_policy=_cairo_cache_policy)
def cairo_run(settings: ScenarioSettings, *, billing_kwh: bool) -> Path:
    """Execute one run — a single (stage, variant) pair — as a Prefect task.

    The atomic unit of work in a quartet. ``settings`` already encodes the stage
    (precalc vs calibrated, via the population upgrade and tariff inputs) and the
    ``billing_kwh`` flag encodes the variant (``True`` = delivery, ``False`` =
    supply).

    Gated by the global ``cairo-runs`` concurrency limit, which bounds the number
    of CAIRO processes resident in memory across the whole pipeline, independent
    of how many runs are submitted concurrently. The limit is non-strict: if it
    has not been created (``prefect concurrency-limit create cairo-runs N``)
    Prefect logs a warning and proceeds ungated.

    Cached on ``run_name|path_results`` so a re-submitted identical run is skipped.

    Returns:
        The run's output directory. Raises if CAIRO returns no directory.
    """
    with concurrency(CAIRO_CONCURRENCY_LIMIT, occupy=1):
        result_dir = run(settings, billing_kwh=billing_kwh)
    if result_dir is None:
        raise RuntimeError(
            f"CAIRO returned None for {settings.run_name} — no output directory."
        )
    return result_dir


def run_stage(
    delivery_settings: ScenarioSettings,
    supply_settings: ScenarioSettings,
) -> tuple[Path, Path]:
    """Run one stage of a quartet: its ``delivery`` and ``supply`` variant pair.

    Submits the two variant runs as ``cairo_run`` tasks and returns their output
    directories once both finish. Delivery runs with ``billing_kwh=True``; supply
    with ``billing_kwh=False``. The stage (precalc vs calibrated) is whatever the
    two ``ScenarioSettings`` encode; this helper is stage-agnostic.

    Must be called inside a flow (an evaluator subflow) so ``.submit()`` has a
    task runner. The two variant runs execute concurrently (the evaluator's
    ``ThreadPoolTaskRunner(max_workers=2)``), subject to the global ``cairo-runs``
    gate. Each run's ``process_workers`` is halved so the pair does not oversubscribe.

    Returns:
        ``(delivery_output_dir, supply_output_dir)``.
    """
    workers = max(1, delivery_settings.process_workers // 2)
    delivery_future = cairo_run.submit(
        _with_workers(delivery_settings, workers), billing_kwh=True
    )
    supply_future = cairo_run.submit(
        _with_workers(supply_settings, workers), billing_kwh=False
    )
    return delivery_future.result(), supply_future.result()


# ---------------------------------------------------------------------------
# resolve_subgroups task
# ---------------------------------------------------------------------------


def _resolve_promoted_alias(
    scenario: ScenarioConfig, subclass_config: SubclassConfig
) -> str:
    """Resolve which subgroup alias is promoted to the calibrated stage.

    Only ``multi_rate_collapsed`` collapses to one alias. If the scenario sets
    ``promote_subgroup`` it must name a real alias; otherwise exactly one
    ``source: derived`` subgroup must exist and it is used. Returns ``""`` for
    kinds that do not collapse (``multi_rate_preserved``).
    """
    if scenario.quartet != "multi_rate_collapsed":
        return ""
    aliases = [sg.alias for sg in subclass_config.subgroups]
    if scenario.promote_subgroup is not None:
        if scenario.promote_subgroup not in aliases:
            raise ValueError(
                f"Scenario {scenario.name!r}: promote_subgroup "
                f"{scenario.promote_subgroup!r} is not a declared subgroup "
                f"(available: {aliases})."
            )
        return scenario.promote_subgroup
    derived = [sg.alias for sg in subclass_config.subgroups if sg.source == "derived"]
    if len(derived) != 1:
        raise ValueError(
            f"Scenario {scenario.name!r} is multi_rate_collapsed without an explicit "
            f"'promote_subgroup'; expected exactly one 'source: derived' subgroup to "
            f"promote, found {derived}. Set 'promote_subgroup' to disambiguate."
        )
    return derived[0]


def _write_tariff_map(
    map_path: Path,
    stem_paths: dict[str, str],
    bldg_data: pl.DataFrame,
    subclass_config: dict[str, Any] | None,
) -> None:
    """Build and write one bldg_id->tariff_key map CSV.

    ``stem_paths`` maps each subclass key to a tariff filename (its stem becomes
    the map's ``tariff_key``). ``subclass_config`` is ``None`` for single-key
    (``{"all": ...}``) maps and the ``{group_col, selectors}`` dict otherwise.
    """
    result = generate_tariff_map_from_scenario_keys(
        stem_paths, bldg_data, subclass_config
    )
    map_path.parent.mkdir(parents=True, exist_ok=True)
    result.write_csv(map_path)
    log.info("resolve_subgroups: wrote %s (%d rows)", map_path, len(result))


@task(log_prints=True)
def resolve_subgroups(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    batch: str,
) -> SubclassResolution:
    """Turn a scenario's abstract subclass split into a concrete ``SubclassResolution``.

    The first step of a multi-rate scenario flow. Takes the scenario's declarative
    ``subclass_config`` (a grouping column + N subgroups) and produces everything
    the downstream designer, revenue-requirement, and evaluator steps need: tariff
    filename stems, JSON/map paths per variant, the raw-value->subgroup mapping,
    and the delivery + supply bldg_id->tariff_key map CSVs (written to disk here).

    Reuses ``generate_tariff_map_from_scenario_keys`` (already supports N
    subclasses). ``bldg_data`` is assembled by filtering the utility assignment to
    ``sb.electric_utility`` and joining ``postprocess_group.{group_col}`` from
    ResStock metadata.

    Args:
        config: Loaded pipeline config (supplies ResStock paths and naming inputs).
        scenario: The multi_rate scenario; must carry a ``subclass_config``.
        batch: Batch name (part of output paths).

    Returns:
        The fully-populated ``SubclassResolution`` for this scenario.
    """
    subclass_config = scenario.subclass_config
    if subclass_config is None:
        raise ValueError(
            f"Scenario {scenario.name!r} is multi_rate but has no subclass_config."
        )
    group_col = subclass_config.group_col
    aliases = [sg.alias for sg in subclass_config.subgroups]

    # Which subgroup's calibrated tariff is promoted to a single-rate calibrated
    # stage. Only meaningful for ``multi_rate_collapsed``; empty otherwise.
    promoted_alias = _resolve_promoted_alias(scenario, subclass_config)

    allocation = scenario.residual_allocation_delivery or "percustomer"

    selectors = {sg.alias: ",".join(sg.values) for sg in subclass_config.subgroups}
    group_value_to_subclass: dict[str, str] = {}
    for sg in subclass_config.subgroups:
        for value in sg.values:
            group_value_to_subclass[value] = sg.alias

    alias_to_stem: dict[str, str] = {}
    alias_to_stem_supply: dict[str, str] = {}
    path_tariffs_electric: dict[str, Path] = {}
    path_tariffs_electric_supply: dict[str, Path] = {}
    json_dir = _tariff_json_dir(config)
    for sg in subclass_config.subgroups:
        stem = _multi_rate_stem(
            config, sg.alias, sg.structure, allocation, supply=False, calibrated=False
        )
        stem_supply = _multi_rate_stem(
            config, sg.alias, sg.structure, allocation, supply=True, calibrated=False
        )
        alias_to_stem[sg.alias] = stem
        alias_to_stem_supply[sg.alias] = stem_supply
        path_tariffs_electric[sg.alias] = json_dir / f"{stem}.json"
        path_tariffs_electric_supply[sg.alias] = json_dir / f"{stem_supply}.json"

    path_tariff_map = _scenario_map_path(
        config, scenario.name, supply=False, calibrated=False
    )
    path_tariff_map_supply = _scenario_map_path(
        config, scenario.name, supply=True, calibrated=False
    )
    path_tariff_map_calibrated = _scenario_map_path(
        config, scenario.name, supply=False, calibrated=True
    )
    path_tariff_map_calibrated_supply = _scenario_map_path(
        config, scenario.name, supply=True, calibrated=True
    )

    # Assemble bldg_data: utility buildings joined with postprocess_group.{group_col}.
    _, _, utility_assignment = _resstock_paths(config, calibrated=False)
    metadata, _, _ = _resstock_paths(config, calibrated=False)
    meta_col = f"postprocess_group.{group_col}"
    bldg_ids = (
        _scan_parquet_any(str(utility_assignment))
        .filter(pl.col("sb.electric_utility") == config.utility)
        .select("bldg_id")
    )
    bldg_data = cast(
        pl.DataFrame,
        _scan_parquet_any(str(metadata))
        .select("bldg_id", meta_col)
        .join(bldg_ids, on="bldg_id")
        .collect(),
    )
    if bldg_data.is_empty():
        raise ValueError(
            f"No buildings for utility {config.utility!r} in {utility_assignment}"
        )

    map_subclass_config = {"group_col": group_col, "selectors": selectors}
    # Precalc maps: each building -> its subgroup's precalc stem. Delivery + supply
    # share the bldg->alias assignment but differ by stem.
    for map_path, alias_stems in (
        (path_tariff_map, alias_to_stem),
        (path_tariff_map_supply, alias_to_stem_supply),
    ):
        stem_paths = {alias: f"{stem}.json" for alias, stem in alias_stems.items()}
        _write_tariff_map(map_path, stem_paths, bldg_data, map_subclass_config)

    # Calibrated-stage maps: keys are the *_calibrated stems (CAIRO matches map
    # tariff_key to tariff file stem). ``collapse`` routes every building to the
    # single promoted subgroup; ``preserve`` keeps the per-subgroup split.
    collapse = scenario.quartet == "multi_rate_collapsed"
    for map_path, alias_stems in (
        (path_tariff_map_calibrated, alias_to_stem),
        (path_tariff_map_calibrated_supply, alias_to_stem_supply),
    ):
        if collapse:
            promoted_stem = alias_stems[promoted_alias]
            cal_stem_paths = {"all": f"{promoted_stem}_calibrated.json"}
            _write_tariff_map(map_path, cal_stem_paths, bldg_data, None)
        else:  # multi_rate_preserved
            cal_stem_paths = {
                alias: f"{stem}_calibrated.json" for alias, stem in alias_stems.items()
            }
            _write_tariff_map(map_path, cal_stem_paths, bldg_data, map_subclass_config)

    return SubclassResolution(
        group_col=group_col,
        aliases=aliases,
        promoted_alias=promoted_alias,
        selectors=selectors,
        alias_to_stem=alias_to_stem,
        alias_to_stem_supply=alias_to_stem_supply,
        group_value_to_subclass=group_value_to_subclass,
        path_tariffs_electric=path_tariffs_electric,
        path_tariffs_electric_supply=path_tariffs_electric_supply,
        path_tariff_map=path_tariff_map,
        path_tariff_map_supply=path_tariff_map_supply,
        path_tariff_map_calibrated=path_tariff_map_calibrated,
        path_tariff_map_calibrated_supply=path_tariff_map_calibrated_supply,
    )


# ---------------------------------------------------------------------------
# compute_subclass_rr task
# ---------------------------------------------------------------------------


@task(log_prints=True)
def compute_subclass_rr_task(
    config: PipelineConfig,
    precalc: StageResult,
    resolution: SubclassResolution,
) -> Path:
    """Write the differentiated (per-subgroup) revenue-requirement YAML.

    Splits the utility's total revenue requirement across the population subgroups
    so that multi-rate scenarios calibrate against a subgroup-consistent baseline.
    Delivery breakdowns come from the default scenario's precalc *delivery* run and
    total (delivery+supply) breakdowns from its *supply* run — computed for every
    allocation method — then combined into one YAML with delivery/supply blocks
    keyed by allocation.

    Args:
        config: Loaded pipeline config (supplies the single-rate base RR YAML and
            the destination multi-rate RR path).
        precalc: The default scenario's precalc ``StageResult`` (source of the
            delivery and supply run directories).
        resolution: Supplies the grouping column that defines the subgroups.

    Returns:
        Path to the written differentiated revenue-requirement YAML.
    """
    delivery_dir = precalc.delivery_output_dir
    supply_dir = precalc.supply_output_dir
    group_col = resolution.group_col
    cols = tuple(SUBCLASS_RR_ALLOCATION_METHODS)

    delivery_breakdowns = compute_subclass_rr(
        run_dir=delivery_dir, group_col=group_col, cross_subsidy_cols=cols
    )
    total_breakdowns = compute_subclass_rr(
        run_dir=supply_dir, group_col=group_col, cross_subsidy_cols=cols
    )

    # Base totals + scaling from the single-rate base rate case YAML.
    base_rr_path = config.state_config_dir / config.rr_single_rate
    base_rr = yaml.safe_load(base_rr_path.read_text(encoding="utf-8"))
    total_delivery_rr = base_rr.get("total_delivery_revenue_requirement")
    total_delivery_and_supply_rr = base_rr.get(
        "total_delivery_and_supply_revenue_requirement"
    )
    customer_count = base_rr.get("test_year_customer_count")
    kwh_scale_factor = base_rr.get("resstock_kwh_scale_factor")

    out_path = config.state_config_dir / config.rr_multi_rate
    differentiated_yaml_path, _ = _write_revenue_requirement_yamls(
        delivery_breakdowns=delivery_breakdowns,
        run_dir=delivery_dir,
        group_col=group_col,
        utility=config.utility,
        default_revenue_requirement=float(total_delivery_rr or 0.0),
        differentiated_yaml_path=out_path,
        default_yaml_path=base_rr_path,
        group_value_to_subclass=resolution.group_value_to_subclass,
        total_breakdowns=total_breakdowns,
        total_delivery_rr=total_delivery_rr,
        total_delivery_and_supply_rr=total_delivery_and_supply_rr,
        customer_count_override=customer_count,
        kwh_scale_factor=kwh_scale_factor,
    )
    log.info("compute_subclass_rr: wrote %s", differentiated_yaml_path)
    return differentiated_yaml_path


# ---------------------------------------------------------------------------
# derive_seasonal_tariff designer task
# ---------------------------------------------------------------------------


@task(log_prints=True)
def derive_seasonal_tariff(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    default_eval: EvalResult,
    resolution: SubclassResolution,
) -> SubclassResolution:
    """Rate designer: produce each subgroup's tariff *input* for the multi-rate quartet.

    For every subgroup in ``resolution``, writes the precalc-stage tariff input
    according to the subgroup's ``source``:

    * ``source: derived`` — compute a seasonal-discount rate from the default
      scenario's precalc bills/BAT and ResStock loads, keyed on the allocation's
      BAT column.
    * ``source: default_calibrated`` — relabel-copy the calibrated single-rate
      default tariff (its calibrated value already reflects the allocation once
      CAIRO knows the subclass revenue requirements).

    Both variants (delivery + supply) are written to the paths declared by
    ``resolution``.

    Args:
        config: Loaded pipeline config (state/utility, winter months, tariff dir).
        scenario: The multi_rate scenario (supplies subgroups and residual allocation).
        default_eval: The default scenario's quartet result; its precalc stage
            seeds the derived rates and its calibrated tariffs are relabel-copied.
        resolution: The resolved layout whose per-subgroup paths/stems are the
            write destinations.

    Returns:
        The same ``resolution`` (returned so callers get an explicit dependency
        edge from designer to evaluator in the flow graph).
    """
    subclass_config = scenario.subclass_config
    if subclass_config is None:
        raise ValueError(f"Scenario {scenario.name!r} has no subclass_config.")

    allocation = scenario.residual_allocation_delivery or "percustomer"
    bat_col = ALLOCATION_TO_BAT_COL.get(allocation, "BAT_percustomer")

    winter_months = _resolve_winter_months(state=config.state, utility=config.utility)

    json_dir = _tariff_json_dir(config)
    default_delivery_calibrated = json_dir / f"{config.utility}_default_calibrated.json"
    default_supply_calibrated = (
        json_dir / f"{config.utility}_default_supply_calibrated.json"
    )

    precalc = default_eval.precalc
    for sg in subclass_config.subgroups:
        delivery_out = resolution.path_tariffs_electric[sg.alias]
        supply_out = resolution.path_tariffs_electric_supply[sg.alias]
        delivery_stem = resolution.alias_to_stem[sg.alias]
        supply_stem = resolution.alias_to_stem_supply[sg.alias]

        if sg.source == "default_calibrated":
            _relabel_tariff_copy(
                default_delivery_calibrated, delivery_out, delivery_stem
            )
            _relabel_tariff_copy(default_supply_calibrated, supply_out, supply_stem)
            log.info("derive_seasonal_tariff: relabel-copied %s", sg.alias)
            continue

        if sg.source != "derived":
            raise ValueError(
                f"Unknown subgroup source {sg.source!r} for alias {sg.alias!r}."
            )
        if sg.structure != "seasonal":
            raise ValueError(
                f"derive_seasonal_tariff only supports structure 'seasonal'; "
                f"got {sg.structure!r} for alias {sg.alias!r}."
            )

        for out_path, stem, run_dir, base_tariff_path in (
            (
                delivery_out,
                delivery_stem,
                precalc.delivery_output_dir,
                default_delivery_calibrated,
            ),
            (
                supply_out,
                supply_stem,
                precalc.supply_output_dir,
                default_supply_calibrated,
            ),
        ):
            inputs = compute_subclass_seasonal_discount_inputs(
                run_dir=run_dir,
                resstock_base=config.resstock_base,
                state=config.state.upper(),
                upgrade=config.upgrade_precalc,
                group_col=resolution.group_col,
                subclass_value=sg.alias,
                cross_subsidy_col=bat_col,
                group_value_to_subclass=resolution.group_value_to_subclass,
                base_tariff_json_path=base_tariff_path,
                winter_months=winter_months,
            )
            row = inputs.row(0, named=True)
            base_tariff = json.loads(base_tariff_path.read_text(encoding="utf-8"))
            seasonal = create_seasonal_rate(
                base_tariff=base_tariff,
                label=stem,
                winter_rate=float(row["winter_rate"]),
                summer_rate=float(row["summer_rate"]),
                winter_months=list(winter_months),
            )
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(seasonal, indent=2) + "\n", encoding="utf-8")
            log.info("derive_seasonal_tariff: wrote %s", out_path)

    return resolution


# ---------------------------------------------------------------------------
# tariff_promotion seam
# ---------------------------------------------------------------------------


def _calibrated_tariff_path(tariff_dir: Path, stem: str) -> Path:
    """Path to a subgroup's calibrated tariff output (``<stem>_calibrated.json``)."""
    return tariff_dir / f"{stem}_calibrated.json"


def promote_subgroup_tariff(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    resolution: SubclassResolution | None,
    precalc: StageResult,
) -> PromotionResult:
    """Cross the ``tariff_promotion`` seam: choose the calibrated stage's inputs.

    The single source of truth for what the calibrated stage runs. The returned
    tariffs **always** reference stage-1 *outputs* (``*_calibrated.json``), never
    stage-1 inputs. The policy is derived from the scenario's quartet kind:

    * ``identity`` (``single_rate``) — the whole population runs the single
      calibrated default tariff (``{utility}_default_calibrated.json``) with the
      pre-generated single-rate calibrated map. ``resolution`` is ``None``.
    * ``collapse_to_derived`` (``multi_rate_collapsed``) — the target population
      runs one promoted subgroup's calibrated tariff (``promote_subgroup``, else
      the sole ``source: derived`` subgroup), with the collapse map that routes
      every building to it. All N subgroups' calibrated tariffs and bills/BAT are
      still persisted for post-processing; only one is promoted forward.
    * ``keep_subgroups`` (``multi_rate_preserved``) — every subgroup keeps its own
      calibrated tariff, with the per-subgroup calibrated map.

    Args:
        config: Loaded pipeline config (supplies naming/paths for ``identity``).
        scenario: The scenario being evaluated (supplies the quartet kind).
        resolution: The multi-rate layout for the two multi kinds; ``None`` for
            ``single_rate``.
        precalc: The precalc stage's result. Not read for its contents (the
            tariffs are already on disk); accepted to make the stage-to-stage data
            dependency explicit in the flow graph.

    Returns:
        A ``PromotionResult`` describing the calibrated stage's tariffs and maps.
    """
    _ = precalc  # tariffs already persisted by the precalc stage; kept for the data edge
    policy = scenario.tariff_promotion

    if policy == "identity":
        json_dir = _tariff_json_dir(config)
        stem_d = _single_rate_stem(config, supply=False, calibrated=True)
        stem_s = _single_rate_stem(config, supply=True, calibrated=True)
        return PromotionResult(
            policy=policy,
            mode="single",
            tariffs_delivery={"all": json_dir / f"{stem_d}.json"},
            tariffs_supply={"all": json_dir / f"{stem_s}.json"},
            map_delivery=_single_rate_map_path(config, supply=False, calibrated=True),
            map_supply=_single_rate_map_path(config, supply=True, calibrated=True),
        )

    if resolution is None:
        raise ValueError(
            f"Scenario {scenario.name!r} ({scenario.quartet}) requires a "
            "SubclassResolution to promote calibrated tariffs."
        )
    tariff_dir = next(iter(resolution.path_tariffs_electric.values())).parent

    if policy == "collapse_to_derived":
        promoted = resolution.promoted_alias
        stem_d = resolution.alias_to_stem[promoted]
        stem_s = resolution.alias_to_stem_supply[promoted]
        return PromotionResult(
            policy=policy,
            mode="single",
            tariffs_delivery={"all": _calibrated_tariff_path(tariff_dir, stem_d)},
            tariffs_supply={"all": _calibrated_tariff_path(tariff_dir, stem_s)},
            map_delivery=resolution.path_tariff_map_calibrated,
            map_supply=resolution.path_tariff_map_calibrated_supply,
        )

    if policy == "keep_subgroups":
        tariffs_delivery = {
            alias: _calibrated_tariff_path(tariff_dir, resolution.alias_to_stem[alias])
            for alias in resolution.aliases
        }
        tariffs_supply = {
            alias: _calibrated_tariff_path(
                tariff_dir, resolution.alias_to_stem_supply[alias]
            )
            for alias in resolution.aliases
        }
        return PromotionResult(
            policy=policy,
            mode="multi",
            tariffs_delivery=tariffs_delivery,
            tariffs_supply=tariffs_supply,
            map_delivery=resolution.path_tariff_map_calibrated,
            map_supply=resolution.path_tariff_map_calibrated_supply,
        )

    raise ValueError(f"Unknown tariff_promotion policy {policy!r}.")


# ---------------------------------------------------------------------------
# Evaluator subflows
# ---------------------------------------------------------------------------


@flow(name="quartet-evaluator", task_runner=ThreadPoolTaskRunner(max_workers=2))  # type: ignore[no-matching-overload]
def quartet_evaluator(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    batch: str,
    resolution: SubclassResolution | None = None,
    subclass_rr_path: Path | None = None,
) -> EvalResult:
    """Run one scenario's full quartet, for any ``quartet`` kind.

    Owns the four runs (two stages × two variants) of a single scenario and always
    executes both stages. The precalc and calibrated rate modes follow the
    scenario's quartet kind; the calibrated stage's tariffs come exclusively from
    the ``tariff_promotion`` seam (so it always consumes ``*_calibrated.json``).

    Steps:

    1. **precalc stage** — ``run_stage`` on the precalc settings (single or
       per-subgroup per ``precalc_is_multi``); the delivery+supply variant pair
       runs concurrently. Every subgroup's ``tariff_final_config.json`` is
       converted to a URDB ``*_calibrated.json`` and retained on the
       ``StageResult`` (so, e.g., a non-promoted subgroup's final rate is kept).
    2. **tariff_promotion seam** — ``promote_subgroup_tariff`` selects the
       calibrated stage's tariffs and map (``identity`` / ``collapse_to_derived`` /
       ``keep_subgroups``).
    3. **calibrated stage** — ``run_stage`` on the calibrated settings built from
       the seam.

    Args:
        resolution: The resolved multi-rate layout; required for the multi-precalc
            quartet kinds, ``None`` for ``single_rate``.
        subclass_rr_path: Differentiated revenue-requirement YAML from
            ``compute_subclass_rr``. Not read directly; accepted to make the
            dependency explicit in the flow graph (its contents are consumed by
            ``derive_settings`` via ``config.rr_multi_rate`` on disk).

    Returns:
        The quartet's ``EvalResult`` with both stages populated.
    """
    _ = subclass_rr_path  # dependency edge; consumed via config.rr_multi_rate on disk

    # Stage 1: precalc.
    precalc_delivery = derive_settings(
        config, scenario, batch, supply=False, calibrated=False, resolution=resolution
    )
    precalc_supply = derive_settings(
        config, scenario, batch, supply=True, calibrated=False, resolution=resolution
    )
    delivery_dir, supply_dir = run_stage(precalc_delivery, precalc_supply)

    # Retain every subgroup's calibrated tariff (not only the promoted one).
    calibrated_tariffs: list[Path] = []
    calibrated_tariffs.extend(_extract_calibrated_tariffs(delivery_dir, config))
    calibrated_tariffs.extend(_extract_calibrated_tariffs(supply_dir, config))
    precalc = StageResult(delivery_dir, supply_dir, calibrated_tariffs)

    # Cross the tariff_promotion seam.
    promotion = promote_subgroup_tariff(config, scenario, resolution, precalc)
    log.info(
        "quartet_evaluator[%s]: promotion=%s mode=%s -> %s",
        scenario.name,
        promotion.policy,
        promotion.mode,
        {k: str(v) for k, v in promotion.tariffs_delivery.items()},
    )

    # Stage 2: calibrated (always runs; tariffs come from the seam).
    calibrated_delivery = derive_settings(
        config,
        scenario,
        batch,
        supply=False,
        calibrated=True,
        resolution=resolution,
        promotion=promotion,
    )
    calibrated_supply = derive_settings(
        config,
        scenario,
        batch,
        supply=True,
        calibrated=True,
        resolution=resolution,
        promotion=promotion,
    )
    cal_delivery_dir, cal_supply_dir = run_stage(calibrated_delivery, calibrated_supply)
    calibrated = StageResult(cal_delivery_dir, cal_supply_dir)

    return EvalResult(precalc=precalc, calibrated=calibrated)


# ---------------------------------------------------------------------------
# Scenario flows
# ---------------------------------------------------------------------------


@flow(name="default-flow")
def default_flow(config: PipelineConfig, batch: str) -> DefaultFlowResult:
    """Scenario flow for the ``default`` single-rate scenario.

    The root of the pipeline's dependency graph. It:

    1. Runs the ``default`` scenario's quartet via ``quartet_evaluator``
       (``single_rate`` kind).
    2. Computes the differentiated (per-subgroup) revenue requirements from that
       quartet's precalc stage, so downstream multi-rate scenarios inherit a
       consistent RR baseline.

    Returns:
        A ``DefaultFlowResult`` bundling the quartet's ``EvalResult`` and the
        subclass RR YAML path — the two things every ``hp_seasonal_flow`` needs.
    """
    scenario = config.scenario("default")
    eval_result = quartet_evaluator(config, scenario, batch)

    # Subclass RR is a population-wide split needing only a group_col; the first
    # multi-precalc scenario supplies it (all currently share has_hp).
    resolution = _default_resolution_for_rr(config, batch)
    subclass_rr_path = compute_subclass_rr_task(config, eval_result.precalc, resolution)
    return DefaultFlowResult(eval_result=eval_result, subclass_rr_path=subclass_rr_path)


def _default_resolution_for_rr(
    config: PipelineConfig, batch: str
) -> SubclassResolution:
    """Resolve subgroups for the first multi-precalc scenario, for subclass RR.

    The differentiated revenue requirement is a population-wide split, so it only
    needs the grouping column and subgroups — any multi-precalc scenario's config
    supplies these (they currently share ``has_hp``). Raises if none is defined.
    """
    for scenario in config.scenarios.values():
        if scenario.precalc_is_multi and scenario.subclass_config is not None:
            return resolve_subgroups(config, scenario, batch)
    raise ValueError(
        "No multi-rate scenario with subclass_config found; cannot compute "
        "subclass revenue requirements."
    )


@flow(name="hp-seasonal-flow")
def hp_seasonal_flow(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    batch: str,
    default_out: DefaultFlowResult,
) -> EvalResult:
    """Scenario flow for a seasonal multi-rate scenario (depends on ``default_flow``).

    Runs the three multi-rate steps in order:

    1. ``resolve_subgroups`` — turn the scenario's ``subclass_config`` into the
       concrete per-subgroup tariff stems, paths, and maps.
    2. ``derive_seasonal_tariff`` — the rate designer; produces each subgroup's
       tariff input (seasonal HP rate, relabeled default for non-HP, etc.),
       consuming the default quartet's calibrated tariffs.
    3. ``quartet_evaluator`` — run the scenario's full quartet (both stages).

    Args:
        default_out: The ``default_flow`` result. Its calibrated tariffs seed the
            designer; its subclass RR YAML feeds the evaluator. This argument is
            what makes ``hp_seasonal_flow`` depend on ``default_flow`` in the graph.

    Returns:
        The scenario's quartet ``EvalResult``.
    """
    resolution = resolve_subgroups(config, scenario, batch)
    resolution = derive_seasonal_tariff(
        config, scenario, default_out.eval_result, resolution
    )
    return quartet_evaluator(
        config, scenario, batch, resolution, default_out.subclass_rr_path
    )


# ---------------------------------------------------------------------------
# Master flow
# ---------------------------------------------------------------------------


@flow(name="hp-rates-pipeline")
def hp_rates_pipeline(yaml_path: str, batch: str) -> dict[str, EvalResult]:
    """Master flow: run the ``default`` scenario, then fan out to multi-rate ones.

    Wires the scenario flows into one dependency graph:

    * ``default_flow`` runs first (the ``default`` single-rate quartet plus the
      subclass RR computation).
    * Every multi-rate scenario then runs via ``hp_seasonal_flow``, each taking
      the ``default_flow`` result as input (the fan-out edge).

    Provenance is attached as Prefect tags (``batch``, ``utility``, ``commit``,
    and per-scenario ``scenario``) for filtering runs in the Prefect UI.

    Args:
        yaml_path: Path to the compact multi-scenario pipeline YAML.
        batch: Human-readable batch name; used in output paths and tags.

    Returns:
        Mapping of scenario name -> its quartet ``EvalResult`` (``"default"`` plus
        one entry per multi_rate scenario).
    """
    config = load_pipeline_config(Path(yaml_path))
    results: dict[str, EvalResult] = {}
    with tags(
        f"batch:{batch}",
        f"utility:{config.utility}",
        f"commit:{_git_short_sha()}",
    ):
        default_out = default_flow(config, batch)
        results["default"] = default_out.eval_result

        for name, scenario in config.scenarios.items():
            if not scenario.precalc_is_multi:
                continue
            with tags(f"scenario:{name}"):
                results[name] = hp_seasonal_flow(config, scenario, batch, default_out)
    return results


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
        "--batch", required=True, help="Batch name (e.g. md_20260722_r1-6)"
    )
    args = parser.parse_args()

    all_results = hp_rates_pipeline(yaml_path=args.yaml, batch=args.batch)
    for scenario_name, eval_result in all_results.items():
        print(f"[{scenario_name}]")
        print(f"  precalc delivery:    {eval_result.precalc.delivery_output_dir}")
        print(f"  precalc supply:      {eval_result.precalc.supply_output_dir}")
        if eval_result.calibrated is not None:
            print(
                f"  calibrated delivery: {eval_result.calibrated.delivery_output_dir}"
            )
            print(f"  calibrated supply:   {eval_result.calibrated.supply_output_dir}")

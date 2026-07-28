"""Pipeline configuration: dataclasses, YAML loader, and naming derivation.

This is the sole config module for the Prefect pipeline.  It defines the
vocabulary (quartet kinds, structure values), the two dataclasses
(``PipelineConfig`` and ``ScenarioConfig``), and pure functions for deriving
tariff stems, paths, and canonical run names from a loaded config.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HP_RATES_DIR = Path(__file__).resolve().parent

QUARTET_KINDS: frozenset[str] = frozenset(
    {"single_rate", "multi_rate_collapsed", "multi_rate_preserved"}
)

# Quartet kinds whose precalc stage runs per-subgroup (multi) tariffs.
_MULTI_PRECALC_QUARTETS: frozenset[str] = frozenset(
    {"multi_rate_collapsed", "multi_rate_preserved"}
)

# Quartet kinds whose calibrated stage runs per-subgroup (multi) tariffs.
_MULTI_EVAL_QUARTETS: frozenset[str] = frozenset({"multi_rate_preserved"})

# Residual allocation method -> BAT cross-subsidy column in CAIRO outputs.
ALLOCATION_TO_BAT_COL: dict[str, str] = {
    "percustomer": "BAT_percustomer",
    "epmc": "BAT_epmc",
    "volumetric": "BAT_vol",
}

# Structure values that trigger a derivation script.  Anything not in this
# set (currently only ``"base"``) means "copy from the dependency's calibrated
# tariff and rename."
DERIVED_STRUCTURES: frozenset[str] = frozenset({"seasonal", "flat", "tou"})

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SubgroupSpec:
    """One subgroup of a multi-rate scenario's population split.

    Attributes:
        alias: Short label (e.g. ``"hp"``); becomes part of the tariff stem.
        values: The ``postprocess_group.<group_col>`` values that belong to
            this subgroup (e.g. ``["true"]``).
        structure: The tariff structure for this subgroup.  One of the keys in
            ``DERIVED_STRUCTURES`` (triggers derivation) or ``"base"`` (copy
            the dependency's calibrated tariff and rename).
    """

    alias: str
    values: list[str]
    structure: str


@dataclass(frozen=True, slots=True)
class SubclassConfig:
    """A population grouping column plus its ordered subgroups.

    ``group_col`` is the ``postprocess_group`` column suffix that partitions
    the population (e.g. ``"has_hp"``).
    """

    group_col: str
    subgroups: list[SubgroupSpec]


@dataclass(frozen=True, slots=True)
class ScenarioConfig:
    """One scenario declaration from the pipeline YAML.

    A scenario is identified by its ``name`` and ``quartet`` kind.  Multi-rate
    scenarios additionally carry a ``subclass_config`` (population split),
    ``residual_allocation`` (delivery + supply), and ``promote`` (which
    subgroup's calibrated tariff is promoted for collapsed quartets).

    ``tariff_base`` is the base component of the tariff filename stem
    (e.g. ``"default"``).  For single-rate scenarios this produces stems like
    ``bge_default[_supply][_calibrated]``.  Required only for single-rate scenarios.

    ``depends_on`` names the scenario whose outputs feed into this one's
    derive flow (subclass RR, tariff derivation).
    """

    name: str
    quartet: str
    promote: str | None = None
    tariff_base: str | None = None
    depends_on: str | None = None
    residual_allocation_delivery: str | None = None
    residual_allocation_supply: str | None = None
    subclass_config: SubclassConfig | None = None

    @property
    def precalc_is_multi(self) -> bool:
        """True if the precalc stage runs per-subgroup tariffs."""
        return self.quartet in _MULTI_PRECALC_QUARTETS

    @property
    def eval_is_multi(self) -> bool:
        """True if the calibrated stage keeps per-subgroup tariffs."""
        return self.quartet in _MULTI_EVAL_QUARTETS

    @property
    def is_single_rate(self) -> bool:
        return self.quartet == "single_rate"


@dataclass(frozen=True, slots=True)
class RunDefaults:
    """Batch-level fields passed through to the generated scenario YAML.

    The pipeline itself does not use these directly — they are consumed by
    the scenario YAML generator (preflight) and ultimately by
    ``run_scenario.py``.  The derive flow also reads ``resstock_base`` and
    ``upgrade_*`` for seasonal discount input computation, and MC paths for
    TOU derivation.
    """

    resstock_base: str
    upgrade_precalc: str
    upgrade_calibrated: str

    mc_dist_and_sub_tx: str
    mc_bulk_tx: str
    mc_supply_energy: str
    mc_supply_capacity: str
    mc_supply_ancillary: str

    rr_single_rate: str
    rr_single_rate_calibrated: str
    rr_multi_rate_calibrated: str

    solar_pv_compensation: str
    sample_size: int | None = None
    elasticity: float = 0.0


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """Parsed pipeline YAML — orchestration fields plus scenario dict.

    Fields the pipeline uses directly for orchestration, naming, and resource
    management live here.  Fields that are passed through to the generated
    scenario YAML (or consumed by the derive flow) live in ``run_defaults``.
    """

    state: str
    utility: str
    year: int
    process_workers: int
    max_concurrent_cairo_runs: int

    scenarios: dict[str, ScenarioConfig]
    run_defaults: RunDefaults

    @property
    def state_config_dir(self) -> Path:
        return HP_RATES_DIR / self.state.lower() / "config"

    def scenario(self, name: str) -> ScenarioConfig:
        if name not in self.scenarios:
            raise KeyError(
                f"Unknown scenario {name!r}; "
                f"available: {sorted(self.scenarios)}"
            )
        return self.scenarios[name]


# ---------------------------------------------------------------------------
# Naming derivation (pure functions)
# ---------------------------------------------------------------------------


def tariff_stem(
    utility: str,
    scenario: ScenarioConfig,
    alias: str | None = None,
    *,
    supply: bool = False,
    calibrated: bool = False,
) -> str:
    """Build a tariff filename stem from naming conventions.

    Single-rate:
        ``{utility}_{tariff_base}[_supply][_calibrated]``

    Multi-rate (per subgroup):
        ``{utility}_{alias}_{structure}_{delivery_alloc}_{supply_alloc}[_supply][_calibrated]``

    ``_supply`` always precedes ``_calibrated``.
    """
    if scenario.is_single_rate:
        if scenario.tariff_base is None:
            raise ValueError(
                f"Scenario {scenario.name!r} is single-rate but has no tariff_base."
            )
        name = f"{utility}_{scenario.tariff_base}"
    else:
        if alias is None:
            raise ValueError("alias required for multi-rate tariff stems")
        sg = _find_subgroup(scenario, alias)
        name = (
            f"{utility}_{alias}_{sg.structure}"
            f"_{scenario.residual_allocation_delivery}"
            f"_{scenario.residual_allocation_supply}"
        )
    if supply:
        name += "_supply"
    if calibrated:
        name += "_calibrated"
    return name


def tariff_json_path(config: PipelineConfig, stem: str) -> Path:
    """Path to a tariff JSON in the state config dir."""
    return config.state_config_dir / "tariffs" / "electric" / f"{stem}.json"


def tariff_map_path(config: PipelineConfig, stem: str) -> Path:
    """Path to a tariff map CSV in the state config dir."""
    return config.state_config_dir / "tariff_maps" / "electric" / f"{stem}.csv"


def gas_tariff_map_path(config: PipelineConfig, *, calibrated: bool) -> Path:
    """Path to the gas tariff map CSV (scenario-independent)."""
    rd = config.run_defaults
    upgrade = rd.upgrade_calibrated if calibrated else rd.upgrade_precalc
    return (
        config.state_config_dir
        / "tariff_maps"
        / "gas"
        / f"{config.utility}_u{upgrade}.csv"
    )


def canonical_run_name(
    state: str,
    utility: str,
    scenario_name: str,
    stage: str,
    variant: str,
) -> str:
    """Build the canonical run name used as YAML key and run index filename.

    Examples:
        ``md_bge_default_precalc_delivery``
        ``md_bge_hp_seasonal_percustomer_passthrough_calibrated_supply``
    """
    return f"{state}_{utility}_{scenario_name}_{stage}_{variant}"


def multi_rate_rr_path(config: PipelineConfig, scenario: ScenarioConfig) -> str:
    """Derive the multi-rate RR YAML path from utility + subgroup aliases.

    Format: ``rev_requirement/{utility}_{alias1}_vs_{alias2}.yaml``

    Uses exact subgroup alias strings in declaration order.
    """
    if scenario.subclass_config is None:
        raise ValueError(
            f"Scenario {scenario.name!r} has no subclass_config; "
            "cannot derive multi-rate RR path."
        )
    aliases = [sg.alias for sg in scenario.subclass_config.subgroups]
    joined = "_vs_".join(aliases)
    return f"rev_requirement/{config.utility}_{joined}.yaml"


def supply_mc_path(base_path: str, *, include_supply: bool) -> str:
    """Real MC path for supply runs; swap ``data.parquet`` -> ``zero.parquet`` for delivery."""
    if include_supply:
        return base_path
    return base_path.replace("/data.parquet", "/zero.parquet")


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------


def load_pipeline_config(yaml_path: Path) -> PipelineConfig:
    """Load a pipeline YAML (e.g. ``pipeline_bge.yaml``) into a ``PipelineConfig``."""
    with yaml_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    resstock = data["resstock"]
    mc = data["marginal_costs"]
    rr = data["revenue_requirement"]

    scenarios: dict[str, ScenarioConfig] = {}
    for name, raw in data["scenarios"].items():
        scenario = _parse_scenario(name, raw)
        _validate_scenario(scenario)
        scenarios[name] = scenario

    run_defaults = RunDefaults(
        resstock_base=resstock["base"],
        upgrade_precalc=str(resstock["upgrade_precalc"]),
        upgrade_calibrated=str(resstock["upgrade_calibrated"]),
        mc_dist_and_sub_tx=mc["dist_and_sub_tx"],
        mc_bulk_tx=mc["bulk_tx"],
        mc_supply_energy=mc["supply_energy"],
        mc_supply_capacity=mc["supply_capacity"],
        mc_supply_ancillary=mc.get("supply_ancillary", ""),
        rr_single_rate=rr["single_rate"],
        rr_single_rate_calibrated=rr["single_rate_calibrated"],
        rr_multi_rate_calibrated=rr["multi_rate_calibrated"],
        solar_pv_compensation=data.get("solar_pv_compensation", "net_metering"),
        sample_size=_parse_optional_int(data.get("sample_size")),
        elasticity=float(data.get("elasticity", 0.0)),
    )

    return PipelineConfig(
        state=data["state"],
        utility=data["utility"],
        year=int(data["year"]),
        process_workers=int(data.get("process_workers", 8)),
        max_concurrent_cairo_runs=int(data.get("max_concurrent_cairo_runs", 2)),
        scenarios=scenarios,
        run_defaults=run_defaults,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_scenario(name: str, raw: dict[str, Any]) -> ScenarioConfig:
    """Parse one scenario entry from the pipeline YAML."""
    quartet = str(raw.get("quartet", ""))
    if not quartet:
        raise ValueError(
            f"Scenario {name!r} must declare a 'quartet' "
            f"(one of {sorted(QUARTET_KINDS)})."
        )
    ra = raw.get("residual_allocation") or {}
    return ScenarioConfig(
        name=name,
        quartet=quartet,
        promote=raw.get("promote"),
        tariff_base=raw.get("tariff_base"),
        depends_on=raw.get("depends_on"),
        residual_allocation_delivery=ra.get("delivery"),
        residual_allocation_supply=ra.get("supply"),
        subclass_config=_parse_subclass_config(raw.get("subclass_config")),
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
        )
        for alias, spec in subgroups_raw.items()
    ]
    return SubclassConfig(group_col=str(raw["group_col"]), subgroups=subgroups)


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
            f"Scenario {scenario.name!r} ({scenario.quartet}) is single-rate "
            "and must not declare a 'subclass_config'."
        )
    if scenario.is_single_rate and scenario.tariff_base is None:
        raise ValueError(
            f"Scenario {scenario.name!r}: single-rate scenarios require a "
            "'tariff_base' (e.g. 'default')."
        )
    if scenario.promote is not None and scenario.quartet != "multi_rate_collapsed":
        raise ValueError(
            f"Scenario {scenario.name!r}: 'promote' only applies to "
            f"'multi_rate_collapsed' (got quartet {scenario.quartet!r})."
        )
    if scenario.quartet == "multi_rate_collapsed" and scenario.promote is None:
        raise ValueError(
            f"Scenario {scenario.name!r}: 'multi_rate_collapsed' requires an "
            "explicit 'promote' field naming the subgroup to promote."
        )
    if scenario.subclass_config is not None:
        for sg in scenario.subclass_config.subgroups:
            if sg.structure not in DERIVED_STRUCTURES and sg.structure != "base":
                raise ValueError(
                    f"Scenario {scenario.name!r}, subgroup {sg.alias!r}: "
                    f"unknown structure {sg.structure!r}. Expected one of "
                    f"{sorted(DERIVED_STRUCTURES)} or 'base'."
                )


def _find_subgroup(scenario: ScenarioConfig, alias: str) -> SubgroupSpec:
    """Look up a subgroup by alias; raise if not found."""
    if scenario.subclass_config is None:
        raise ValueError(f"Scenario {scenario.name!r} has no subclass_config.")
    for sg in scenario.subclass_config.subgroups:
        if sg.alias == alias:
            return sg
    aliases = [sg.alias for sg in scenario.subclass_config.subgroups]
    raise KeyError(
        f"Subgroup {alias!r} not found in scenario {scenario.name!r}; "
        f"available: {aliases}"
    )


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)

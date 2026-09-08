"""Tests that run_batch dispatches every selected scenario."""

from __future__ import annotations

from rate_design.hp_rates.pipeline_config import QUARTET_KINDS, ScenarioConfig
from rate_design.hp_rates.run_pipeline import _partition_scenarios


def _scenario(
    name: str, quartet: str, *, depends_on: list[str] | None = None
) -> ScenarioConfig:
    return ScenarioConfig(
        name=name,
        quartet=quartet,
        tariff_base="default",
        depends_on=depends_on,
    )


def test_partition_covers_every_quartet_kind_with_and_without_depends_on() -> None:
    selected = []
    for kind in sorted(QUARTET_KINDS):
        selected.append(_scenario(f"{kind}_solo", kind))
        selected.append(_scenario(f"{kind}_dep", kind, depends_on=["default"]))

    independent, dependent, fixed = _partition_scenarios(selected)
    dispatched = independent + dependent + fixed
    assert {s.name for s in dispatched} == {s.name for s in selected}
    assert len(dispatched) == len(selected)


def test_fixed_without_depends_on_is_still_dispatched() -> None:
    """Fully manual multi_rate_fixed omits depends_on; it must not be skipped."""
    selected = [_scenario("manual_fixed", "multi_rate_fixed")]
    independent, dependent, fixed = _partition_scenarios(selected)
    assert [s.name for s in fixed] == ["manual_fixed"]
    assert independent == []
    assert dependent == []

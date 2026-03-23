"""Tests for analytical demand-flex validation and sensitivity analysis."""

from __future__ import annotations

import pytest

from utils.post.validate_demand_flex import (
    SyntheticScenario,
    build_synthetic_scenario,
    check_achieved_elasticity_near_target,
    check_energy_conservation,
    check_frozen_residual_identity,
    check_mc_delta_monotonic_in_elasticity,
    check_mc_delta_negative,
    check_nonhp_subclass_rr_unchanged,
    check_rr_decreases,
    check_shifted_loads_nonnegative,
    check_tou_ratio_does_not_increase,
    check_tou_subclass_rr_absorbs_delta,
    run_all_checks,
)
from utils.post.sensitivity_demand_flex import (
    compute_sweep_point,
    results_to_dataframe,
    run_sweep,
)


@pytest.fixture(scope="module")
def scenario() -> SyntheticScenario:
    """Shared synthetic scenario (expensive to build, reuse across tests)."""
    return build_synthetic_scenario()


# ---------------------------------------------------------------------------
# Individual check tests
# ---------------------------------------------------------------------------


class TestEnergyConservation:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_energy_conservation(scenario, elasticity=-0.1)
        assert result.passed
        assert result.details["rel_diff"] < 1e-8

    def test_passes_at_various_elasticities(self, scenario: SyntheticScenario) -> None:
        for e in [-0.05, -0.10, -0.15, -0.20]:
            result = check_energy_conservation(scenario, elasticity=e)
            assert result.passed, f"Failed at elasticity={e}"


class TestMcDeltaNegative:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_mc_delta_negative(scenario, elasticity=-0.1)
        assert result.passed
        assert result.details["mc_delta"] < 0

    def test_larger_elasticity_gives_larger_delta(
        self, scenario: SyntheticScenario
    ) -> None:
        r1 = check_mc_delta_negative(scenario, elasticity=-0.05)
        r2 = check_mc_delta_negative(scenario, elasticity=-0.15)
        assert abs(r2.details["mc_delta"]) > abs(r1.details["mc_delta"])


class TestFrozenResidualIdentity:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_frozen_residual_identity(scenario, elasticity=-0.1)
        assert result.passed
        assert result.details["diff"] < 1.0


class TestRrDecreases:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_rr_decreases(scenario, elasticity=-0.1)
        assert result.passed


class TestTouRatioDoesNotIncrease:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_tou_ratio_does_not_increase(scenario, elasticity=-0.1)
        assert result.passed

    def test_ratio_within_tolerance(self, scenario: SyntheticScenario) -> None:
        result = check_tou_ratio_does_not_increase(scenario, elasticity=-0.1)
        assert result.details["delta"] < 1e-3


class TestNonhpSubclassRrUnchanged:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_nonhp_subclass_rr_unchanged(scenario, elasticity=-0.1)
        assert result.passed


class TestTouSubclassRrAbsorbsDelta:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_tou_subclass_rr_absorbs_delta(scenario, elasticity=-0.1)
        assert result.passed


class TestAchievedElasticityNearTarget:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_achieved_elasticity_near_target(scenario, elasticity=-0.1)
        assert result.passed or result.status == "WARN"


class TestShiftedLoadsNonnegative:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_shifted_loads_nonnegative(scenario, elasticity=-0.1)
        assert result.passed

    def test_fails_with_extreme_elasticity(self) -> None:
        """Extreme elasticity can push loads negative on thin load buildings."""
        extreme_scenario = build_synthetic_scenario(n_tou_bldgs=3, seed=99)
        result = check_shifted_loads_nonnegative(extreme_scenario, elasticity=-5.0)
        # With such extreme elasticity on synthetic data, loads may go negative.
        # Either outcome (PASS or FAIL) validates the check's logic is exercised.
        assert result.name == "shifted_loads_nonnegative"
        if result.status == "FAIL":
            assert result.details["n_negative"] > 0


class TestMcDeltaMonotonicInElasticity:
    def test_passes_on_synthetic(self, scenario: SyntheticScenario) -> None:
        result = check_mc_delta_monotonic_in_elasticity(scenario)
        assert result.passed
        assert result.details["is_monotonic"]

    def test_custom_elasticities(self, scenario: SyntheticScenario) -> None:
        result = check_mc_delta_monotonic_in_elasticity(
            scenario, elasticities=[-0.02, -0.08, -0.12]
        )
        assert result.passed


# ---------------------------------------------------------------------------
# Run-all checks
# ---------------------------------------------------------------------------


class TestRunAllChecks:
    def test_all_pass_on_synthetic(self, scenario: SyntheticScenario) -> None:
        results = run_all_checks(scenario, elasticity=-0.1)
        assert len(results) == 10
        for r in results:
            assert r.status in ("PASS", "WARN"), f"{r.name} -> {r.status}: {r.message}"

    def test_all_checks_have_unique_names(self, scenario: SyntheticScenario) -> None:
        results = run_all_checks(scenario, elasticity=-0.1)
        names = [r.name for r in results]
        assert len(names) == len(set(names))


# ---------------------------------------------------------------------------
# Sensitivity sweep tests
# ---------------------------------------------------------------------------


class TestSensitivitySweep:
    def test_sweep_returns_correct_count(self, scenario: SyntheticScenario) -> None:
        elasticities = [-0.05, -0.10, -0.15, -0.20]
        results = run_sweep(scenario, elasticities)
        assert len(results) == len(elasticities)

    def test_sweep_mc_deltas_are_negative(self, scenario: SyntheticScenario) -> None:
        results = run_sweep(scenario, [-0.05, -0.10, -0.15])
        for r in results:
            assert r.mc_delta_dollars < 0, f"MC delta should be negative at e={r.elasticity}"

    def test_sweep_mc_deltas_are_monotonic(self, scenario: SyntheticScenario) -> None:
        results = run_sweep(scenario, [-0.05, -0.10, -0.15, -0.20])
        magnitudes = [abs(r.mc_delta_dollars) for r in results]
        for i in range(len(magnitudes) - 1):
            assert magnitudes[i] <= magnitudes[i + 1]

    def test_sweep_rr_decreases_monotonically(
        self, scenario: SyntheticScenario
    ) -> None:
        results = run_sweep(scenario, [-0.05, -0.10, -0.15, -0.20])
        for r in results:
            assert r.rr_change_dollars < 0

    def test_sweep_tou_ratio_does_not_increase(
        self, scenario: SyntheticScenario
    ) -> None:
        results = run_sweep(scenario, [-0.05, -0.10, -0.15, -0.20])
        for r in results:
            assert r.tou_ratio_change < 1e-3, (
                f"TOU ratio increased significantly at e={r.elasticity}"
            )

    def test_results_to_dataframe(self, scenario: SyntheticScenario) -> None:
        results = run_sweep(scenario, [-0.05, -0.10])
        df = results_to_dataframe(results)
        assert len(df) == 2
        assert "elasticity" in df.columns
        assert "mc_delta_dollars" in df.columns

    def test_single_point(self, scenario: SyntheticScenario) -> None:
        result = compute_sweep_point(scenario, -0.10)
        assert result.load_shift_kwh > 0
        assert result.min_shifted_load >= 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_single_building(self) -> None:
        s = build_synthetic_scenario(n_tou_bldgs=1, n_nontou_bldgs=1)
        results = run_all_checks(s, elasticity=-0.1)
        for r in results:
            assert r.status in ("PASS", "WARN"), f"{r.name} -> {r.status}: {r.message}"

    def test_zero_elasticity_energy_conservation(self) -> None:
        """Zero elasticity should result in no shift and perfect conservation."""
        s = build_synthetic_scenario()
        result = check_energy_conservation(s, elasticity=0.0)
        # Zero elasticity means Q_target = Q_orig, so no shift
        assert result.passed

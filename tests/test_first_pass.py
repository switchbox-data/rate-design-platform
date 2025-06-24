"""
Tests for the first-pass TOU HPWH scheduling simulation

This includes both basic functionality tests and dynamic behavior tests
that should FAIL initially with static controllers and PASS once real
controllers are implemented.
"""

import contextlib

import numpy as np
import pytest

from rate_design_platform.first_pass import (
    TOUParameters,
    building_simulation_controller,
    calculate_annual_metrics,
    calculate_comfort_penalty,
    calculate_monthly_bill,
    create_operation_schedule,
    create_tou_rates,
    define_peak_hours,
    human_controller,
    load_input_data,
    run_full_simulation,
    simulate_annual_cycle,
    simulate_single_month,
)


class TestTOUParameters:
    """Test TOU parameter initialization"""

    def test_default_parameters(self):
        params = TOUParameters()
        assert params.r_on == 0.28
        assert params.r_off == 0.12
        assert params.c_switch == 35.0
        assert params.alpha == 0.15
        assert params.cop == 3.0
        assert params.peak_start_hour == 14
        assert params.peak_end_hour == 20


class TestPeakHours:
    """Test peak hour definition and TOU rate creation"""

    def test_define_peak_hours_default(self):
        peak_hours = define_peak_hours()
        assert len(peak_hours) == 96  # 24 hours * 4 intervals/hour
        assert np.sum(peak_hours) == 24  # 6 hours * 4 intervals/hour (14:00-20:00)

        # Check specific peak intervals (14:00-20:00)
        assert peak_hours[56]  # 14:00 (56 = 14 * 4)
        assert peak_hours[79]  # 19:45 (79 = 19 * 4 + 3)
        assert not peak_hours[80]  # 20:00 (end of peak)
        assert not peak_hours[0]  # Midnight
        assert not peak_hours[55]  # 13:45

    def test_create_tou_rates(self):
        rates = create_tou_rates(96 * 2)  # Two days
        assert len(rates) == 192

        # Count peak and off-peak rates
        peak_count = np.sum(rates == 0.28)
        offpeak_count = np.sum(rates == 0.12)

        assert peak_count == 48  # 24 peak intervals * 2 days
        assert offpeak_count == 144  # 72 off-peak intervals * 2 days
        assert peak_count + offpeak_count == 192


class TestOperationSchedules:
    """Test operational schedule creation"""

    def test_default_schedule(self):
        schedule = create_operation_schedule(1, 96)  # Default
        assert len(schedule) == 96
        assert np.all(schedule == 1)  # Always allowed

    def test_tou_schedule(self):
        schedule = create_operation_schedule(0, 96)  # TOU
        assert len(schedule) == 96
        assert np.sum(schedule) == 72  # 72 off-peak intervals allowed

        # Peak hours should be restricted
        peak_hours = define_peak_hours()
        assert np.all(schedule[peak_hours] == 0)  # Restricted during peak
        assert np.all(schedule[~peak_hours] == 1)  # Allowed during off-peak


class TestBillCalculations:
    """Test bill and penalty calculations"""

    def test_calculate_monthly_bill(self):
        consumption = np.array([0.5, 0.6, 0.4])  # kWh/15min
        rates = np.array([0.12, 0.28, 0.12])  # $/kWh

        expected_bill = 0.5 * 0.12 + 0.6 * 0.28 + 0.4 * 0.12
        bill = calculate_monthly_bill(consumption, rates)

        assert abs(bill - expected_bill) < 1e-6

    def test_calculate_comfort_penalty(self):
        # Test with zero unmet demand
        unmet_demand_watts = np.zeros(96)
        penalty = calculate_comfort_penalty(unmet_demand_watts, 0.15)
        assert penalty == 0.0

        # Test with non-zero unmet demand
        unmet_demand_watts = np.full(96, 1000.0)  # 1000 W each interval
        penalty = calculate_comfort_penalty(unmet_demand_watts, 0.15)

        # Expected: 96 * 1000 W * 0.25 hours / 1000 W/kW * 0.15 $/kWh = 96 * 0.25 * 0.15 = 3.6
        expected_penalty = 96 * 1000.0 * 0.25 / 1000.0 * 0.15
        assert abs(penalty - expected_penalty) < 1e-6


class TestInputLoading:
    """Test input data loading (will fail if files not present)"""

    def test_load_input_data_file_not_found(self):
        # Test with non-existent file
        with pytest.raises((FileNotFoundError, ValueError)):
            load_input_data("nonexistent_file.csv")

    def test_run_full_simulation_basic_structure(self):
        # This test will fail gracefully if input files aren't available
        # but tests the function signature and error handling
        try:
            monthly_results, annual_metrics = run_full_simulation()

            # If it succeeds, check structure
            assert isinstance(monthly_results, list)
            assert isinstance(annual_metrics, dict)
            assert len(monthly_results) == 12

        except (FileNotFoundError, ValueError) as e:
            # Expected if input files not available
            assert "not found" in str(e).lower() or "hot_water_fixtures" in str(e)


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_zero_intervals(self):
        # Test that zero intervals returns empty array (current behavior)
        rates = create_tou_rates(0)
        assert len(rates) == 0

    def test_empty_hot_water_data(self):
        # Test that empty data is handled gracefully
        empty_data = np.array([])
        with contextlib.suppress(ValueError, IndexError, ZeroDivisionError):
            # Expected behavior - division by zero when splitting into months
            simulate_annual_cycle(empty_data)

    def test_mismatched_data_lengths(self):
        hot_water = np.ones(50)  # Short data
        rates = np.ones(100)  # Longer rates

        # Should handle gracefully or raise appropriate error
        with pytest.raises((ValueError, IndexError)):
            calculate_monthly_bill(hot_water, rates)


class TestBuildingSimulation:
    """Test building physics behavior (will fail with static controller)"""

    def test_tou_schedule_reduces_peak_consumption(self):
        """TOU schedule should shift consumption away from peak hours"""
        hot_water = np.ones(96) * 0.1  # Constant hot water usage

        # Default schedule (unrestricted)
        default_schedule = create_operation_schedule(1, 96)
        default_results = building_simulation_controller(1, default_schedule, hot_water)

        # TOU schedule (peak restricted)
        tou_schedule = create_operation_schedule(0, 96)
        tou_results = building_simulation_controller(1, tou_schedule, hot_water)

        # Peak hours: intervals 56-79 (14:00-20:00)
        peak_intervals = slice(56, 80)

        # TOU should have lower peak consumption due to restrictions
        default_peak_consumption = np.sum(default_results.E_mt[peak_intervals])
        tou_peak_consumption = np.sum(tou_results.E_mt[peak_intervals])

        assert tou_peak_consumption < default_peak_consumption, "TOU schedule should reduce peak consumption"

    def test_tank_temperature_responds_to_restrictions(self):
        """Tank temperature should drop when HPWH operation is restricted"""
        hot_water = np.ones(96) * 0.2  # Moderate hot water usage

        # Default schedule allows heating
        default_schedule = create_operation_schedule(1, 96)
        default_results = building_simulation_controller(1, default_schedule, hot_water)

        # TOU schedule restricts heating during peaks
        tou_schedule = create_operation_schedule(0, 96)
        tou_results = building_simulation_controller(1, tou_schedule, hot_water)

        # Average tank temperature should be lower under TOU restrictions
        default_avg_temp = np.mean(default_results.T_tank_mt)
        tou_avg_temp = np.mean(tou_results.T_tank_mt)

        assert tou_avg_temp < default_avg_temp, "TOU restrictions should result in lower average tank temperature"


class TestHumanDecisions:
    """Test human decision-making controller (will fail with static controller)"""

    def test_switches_to_tou_with_high_savings(self):
        """Consumer should switch to TOU when anticipated savings are high"""
        # Test case where TOU saves significant money
        realized_savings = 0.0  # Not used for default→TOU decision
        unrealized_savings = 1000.0  # High anticipated savings

        decision = human_controller(
            current_state=1,  # Default schedule
            realized_savings=realized_savings,
            unrealized_savings=unrealized_savings,
        )

        assert decision == 1, "Should switch to TOU when anticipated savings exceed switching cost"

    def test_stays_on_default_with_low_savings(self):
        """Consumer should not switch to TOU when anticipated savings are low"""
        realized_savings = 0.0
        unrealized_savings = 1.0  # Low savings

        decision = human_controller(
            current_state=1,  # Default schedule
            realized_savings=realized_savings,
            unrealized_savings=unrealized_savings,
        )

        assert decision == 0, "Should not switch to TOU when anticipated savings are below switching cost"

    def test_switches_back_with_poor_performance(self):
        """Consumer should switch back to default when TOU performance is poor"""
        realized_savings = -10.0  # Negative savings (costs more than expected)
        unrealized_savings = 0.0  # Not used for TOU→default decision

        decision = human_controller(
            current_state=0,  # TOU schedule
            realized_savings=realized_savings,
            unrealized_savings=unrealized_savings,
        )

        assert decision == 1, "Should switch back to default when TOU results in higher costs"

    def test_stays_on_tou_with_good_performance(self):
        """Consumer should continue TOU when performance meets expectations"""
        realized_savings = 25.0  # Positive net savings after comfort penalty
        unrealized_savings = 0.0

        decision = human_controller(
            current_state=0,  # TOU schedule
            realized_savings=realized_savings,
            unrealized_savings=unrealized_savings,
        )

        assert decision == 0, "Should continue TOU when realized savings are positive"


class TestMonthlySimulation:
    """Test monthly simulation"""

    def test_monthly_switching_behavior(self):
        """Test that monthly simulation produces realistic switching patterns"""
        # Create scenario with varying hot water usage and rates
        hot_water = np.random.rand(2920) * 0.3  # Random usage pattern
        rates = create_tou_rates(2920)
        params = TOUParameters()

        # Simulate from default state
        result_default = simulate_single_month(1, 1, hot_water, rates, params)

        # Should have realistic unrealized savings calculation
        assert result_default.unrealized_savings != 0.0, "Should calculate non-zero unrealized savings potential"

        # Simulate from TOU state
        result_tou = simulate_single_month(1, 0, hot_water, rates, params)

        # Should have realistic realized savings calculation
        assert result_tou.realized_savings != 0.0, "Should calculate non-zero realized savings"


class TestDynamicAnnualPatterns:
    """Test annual behavior patterns"""

    def test_annual_adoption_patterns(self):
        """Test that some consumers adopt TOU over the year"""
        # Create full year of realistic data
        hot_water_data = np.random.rand(35040) * 0.2  # Realistic usage levels
        params = TOUParameters()

        monthly_results = simulate_annual_cycle(hot_water_data, params)
        annual_metrics = calculate_annual_metrics(monthly_results)

        # Should see some TOU adoption over the year
        assert annual_metrics["tou_adoption_rate_percent"] > 0, (
            "Should see some TOU adoption with realistic decision-making"
        )

        # Should see some switching activity
        assert annual_metrics["annual_switches"] > 0, "Should see switching decisions with dynamic human controller"

    def test_seasonal_switching_patterns(self):
        """Test that switching patterns vary by season/month"""
        # Create seasonal usage pattern (higher in winter)
        hot_water_data = np.random.rand(35040) * 0.2
        # Increase winter usage (months 11, 12, 1, 2)
        for month in [0, 1, 10, 11]:  # 0-indexed months
            start_idx = month * 2920
            end_idx = (month + 1) * 2920
            hot_water_data[start_idx:end_idx] *= 1.5

        monthly_results = simulate_annual_cycle(hot_water_data)

        # Extract switching decisions by month
        switches_by_month = [r.switching_decision for r in monthly_results]

        # Should see different switching patterns (not all zeros)
        assert any(switch == 1 for switch in switches_by_month), "Should see switching activity with seasonal patterns"


class TestComfortPenalties:
    """Test that comfort penalties reflect unmet demand changes"""

    def test_comfort_penalty_scales_with_unmet_demand(self):
        """Higher unmet demand should result in higher comfort penalty"""
        # Low unmet demand
        low_unmet = np.full(96, 100.0)  # 100W each interval
        low_penalty = calculate_comfort_penalty(low_unmet, 0.15)

        # High unmet demand
        high_unmet = np.full(96, 500.0)  # 500W each interval
        high_penalty = calculate_comfort_penalty(high_unmet, 0.15)

        assert high_penalty > low_penalty, "Higher unmet demand should result in higher comfort penalty"

        # Penalty should scale proportionally
        assert abs(high_penalty / low_penalty - 5.0) < 0.1, (
            "Comfort penalty should scale proportionally with unmet demand"
        )

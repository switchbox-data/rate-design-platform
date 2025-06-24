"""
Tests for the first-pass TOU HPWH scheduling simulation
"""

import numpy as np
import pytest

from src.first_pass import (
    MonthlyResults,
    SimulationResults,
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
    simulate_month_both_schedules,
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


class TestStaticControllers:
    """Test static controller implementations"""

    def test_building_simulation_controller(self):
        hot_water = np.ones(96) * 0.1  # L/15min
        operation_schedule = np.ones(96)

        results = building_simulation_controller(1, operation_schedule, hot_water)

        assert isinstance(results, SimulationResults)
        assert len(results.E_mt) == 96
        assert len(results.T_tank_mt) == 96
        assert len(results.Q_unmet_mt) == 96

        # Check static values
        assert np.all(results.E_mt == 0.5)  # kWh/15min
        assert np.all(results.T_tank_mt == 55.0)  # °C
        assert np.all(results.Q_unmet_mt == 0.0)  # J/15min

    def test_human_controller(self):
        # Static controller should always return 0 (no switching)
        assert human_controller(1, 0.0, 10.0) == 0  # Default state with positive savings
        assert human_controller(0, -5.0, 0.0) == 0  # TOU state with negative savings
        assert human_controller(1, 0.0, -10.0) == 0  # Default state with negative savings


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
        unmet_demand = np.zeros(96)
        penalty = calculate_comfort_penalty(unmet_demand, 0.15, 3.0)
        assert penalty == 0.0

        # Test with non-zero unmet demand
        unmet_demand = np.full(96, 1e6)  # 1 MJ/15min each interval
        penalty = calculate_comfort_penalty(unmet_demand, 0.15, 3.0)

        # Expected: 96 * 1e6 J / (3.0 * 3.6e6 J/kWh) * 4 intervals/hour * 0.15 $/kW
        expected_penalty = 96 * 1e6 / (3.0 * 3.6e6) * 4 * 0.15
        assert abs(penalty - expected_penalty) < 1e-6


class TestMonthlySimulation:
    """Test monthly simulation logic"""

    def test_simulate_month_both_schedules(self):
        hot_water = np.ones(96) * 0.1  # L/15min
        rates = create_tou_rates(96)
        params = TOUParameters()

        default_results, tou_results, default_bill, tou_bill = simulate_month_both_schedules(
            1, hot_water, rates, params
        )

        # Both should return results
        assert isinstance(default_results, SimulationResults)
        assert isinstance(tou_results, SimulationResults)
        assert isinstance(default_bill, float)
        assert isinstance(tou_bill, float)

        # Bills should be positive
        assert default_bill > 0
        assert tou_bill > 0

    def test_simulate_single_month_default_state(self):
        hot_water = np.ones(96) * 0.1
        rates = create_tou_rates(96)
        params = TOUParameters()

        result = simulate_single_month(1, 1, hot_water, rates, params)  # Default state

        assert isinstance(result, MonthlyResults)
        assert result.current_state == 1
        assert result.bill > 0
        assert result.comfort_penalty >= 0
        assert result.switching_decision == 0  # Static controller returns 0
        assert result.realized_savings == 0.0  # No realized savings when on default
        assert isinstance(result.unrealized_savings, float)  # Should have unrealized calculation

    def test_simulate_single_month_tou_state(self):
        hot_water = np.ones(96) * 0.1
        rates = create_tou_rates(96)
        params = TOUParameters()

        result = simulate_single_month(1, 0, hot_water, rates, params)  # TOU state

        assert isinstance(result, MonthlyResults)
        assert result.current_state == 0
        assert result.bill > 0
        assert result.comfort_penalty >= 0
        assert result.switching_decision == 0  # Static controller returns 0
        assert isinstance(result.realized_savings, float)  # Should have realized calculation
        assert result.unrealized_savings == 0.0  # No unrealized savings when on TOU


class TestAnnualSimulation:
    """Test annual simulation cycle"""

    def test_simulate_annual_cycle_basic(self):
        # Create test data for one year
        hot_water_data = np.ones(35040) * 0.1  # ~35,040 intervals per year
        params = TOUParameters()

        results = simulate_annual_cycle(hot_water_data, params)

        assert len(results) == 12  # 12 months
        assert all(isinstance(r, MonthlyResults) for r in results)
        assert all(r.month == i for i, r in enumerate(results, 1))  # Months 1-12

        # First month should start on default schedule
        assert results[0].current_state == 1

    def test_calculate_annual_metrics(self):
        # Create mock monthly results
        monthly_results = []
        for month in range(1, 13):
            result = MonthlyResults(
                month=month,
                current_state=1 if month % 2 == 1 else 0,  # Alternate states
                bill=100.0,
                comfort_penalty=5.0,
                switching_decision=1 if month == 6 else 0,  # One switch
                realized_savings=10.0 if month % 2 == 0 else 0.0,
                unrealized_savings=15.0 if month % 2 == 1 else 0.0,
            )
            monthly_results.append(result)

        metrics = calculate_annual_metrics(monthly_results)

        assert metrics["total_annual_bills"] == 1200.0  # 12 * 100
        assert metrics["total_comfort_penalty"] == 60.0  # 12 * 5
        assert metrics["annual_switches"] == 1
        assert metrics["total_switching_costs"] == 35.0  # 1 * 35
        assert metrics["average_monthly_bill"] == 100.0
        assert metrics["tou_adoption_rate_percent"] == 50.0  # 6 months TOU
        assert metrics["total_realized_savings"] == 60.0  # 6 TOU months * 10


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
        with pytest.raises(ValueError):
            create_tou_rates(0)

    def test_empty_hot_water_data(self):
        empty_data = np.array([])
        with pytest.raises((ValueError, IndexError)):
            simulate_annual_cycle(empty_data)

    def test_mismatched_data_lengths(self):
        hot_water = np.ones(50)  # Short data
        rates = np.ones(100)  # Longer rates

        # Should handle gracefully or raise appropriate error
        with pytest.raises((ValueError, IndexError)):
            calculate_monthly_bill(hot_water, rates)


if __name__ == "__main__":
    # Run basic smoke tests
    print("Running basic smoke tests...")

    # Test TOU parameters
    params = TOUParameters()
    print(f"✓ TOU parameters initialized: peak=${params.r_on}, off-peak=${params.r_off}")

    # Test peak hours
    peak_hours = define_peak_hours()
    print(f"✓ Peak hours defined: {np.sum(peak_hours)} intervals per day")

    # Test rate creation
    rates = create_tou_rates(96)
    print(f"✓ TOU rates created: {len(rates)} intervals")

    # Test controllers
    hot_water = np.ones(96) * 0.1
    operation_schedule = np.ones(96)

    results = building_simulation_controller(1, operation_schedule, hot_water)
    print(f"✓ Building controller: returned {len(results.E_mt)} consumption values")

    decision = human_controller(1, 0.0, 10.0)
    print(f"✓ Human controller: returned decision {decision}")

    print("All smoke tests passed!")

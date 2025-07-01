"""
Tests for rate_design_platform.first_pass module

Each function in first_pass.py has a corresponding test_ function here.
Dynamic behavior tests (marked with # DYNAMIC) will fail with static controllers.
"""

import contextlib

import numpy as np
import pytest

from rate_design_platform.first_pass import (
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


def test_define_peak_hours():
    """Test define_peak_hours function"""
    peak_hours = define_peak_hours()
    assert len(peak_hours) == 96  # 24 hours * 4 intervals/hour
    assert np.sum(peak_hours) == 24  # 6 hours * 4 intervals/hour (14:00-20:00)

    # Check specific peak intervals (14:00-20:00)
    assert peak_hours[56]  # 14:00 (56 = 14 * 4)
    assert peak_hours[79]  # 19:45 (79 = 19 * 4 + 3)
    assert not peak_hours[80]  # 20:00 (end of peak)
    assert not peak_hours[0]  # Midnight
    assert not peak_hours[55]  # 13:45


def test_create_tou_rates():
    """Test create_tou_rates function"""
    rates = create_tou_rates(96 * 2)  # Two days
    assert len(rates) == 192

    # Count peak and off-peak rates
    peak_count = np.sum(rates == 0.28)
    offpeak_count = np.sum(rates == 0.12)

    assert peak_count == 48  # 24 peak intervals * 2 days
    assert offpeak_count == 144  # 72 off-peak intervals * 2 days
    assert peak_count + offpeak_count == 192

    # Test edge case
    empty_rates = create_tou_rates(0)
    assert len(empty_rates) == 0


def test_create_operation_schedule():
    """Test create_operation_schedule function"""
    # Default schedule
    default_schedule = create_operation_schedule(1, 96)
    assert len(default_schedule) == 96
    assert np.all(default_schedule == 1)  # Always allowed

    # TOU schedule
    tou_schedule = create_operation_schedule(0, 96)
    assert len(tou_schedule) == 96
    assert np.sum(tou_schedule) == 72  # 72 off-peak intervals allowed

    # Peak hours should be restricted
    peak_hours = define_peak_hours()
    assert np.all(tou_schedule[peak_hours] == 0)  # Restricted during peak
    assert np.all(tou_schedule[~peak_hours] == 1)  # Allowed during off-peak


def test_building_simulation_controller():
    """Test building_simulation_controller function"""
    hot_water = np.ones(96) * 0.1  # L/15min
    operation_schedule = np.ones(96)

    results = building_simulation_controller(1, operation_schedule, hot_water)

    assert isinstance(results, SimulationResults)
    assert len(results.E_mt) == 96
    assert len(results.T_tank_mt) == 96
    assert len(results.D_unmet_mt) == 96

    # Check static values (will change when real controller implemented)
    assert np.all(results.E_mt == 0.5)  # kWh/15min
    assert np.all(results.T_tank_mt == 55.0)  # °C
    assert np.all(results.D_unmet_mt == 0.0)  # W


def test_building_simulation_controller_dynamic():
    """Test building_simulation_controller dynamic behavior (DYNAMIC - will fail)"""
    # DYNAMIC: TOU schedule should reduce peak consumption
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


def test_human_controller():
    """Test human_controller function"""
    # Test static behavior (will change when real controller implemented)
    assert human_controller(1, 0.0, 10.0) == 0  # Default state with positive savings
    assert human_controller(0, -5.0, 0.0) == 0  # TOU state with negative savings
    assert human_controller(1, 0.0, -10.0) == 0  # Default state with negative savings


def test_human_controller_dynamic():
    """Test human_controller dynamic behavior (DYNAMIC - will fail)"""
    # DYNAMIC: Should switch to TOU when anticipated savings are high
    decision = human_controller(
        current_state=1,  # Default schedule
        realized_savings=0.0,  # Not used for default→TOU decision
        unrealized_savings=1000.0,  # High anticipated savings
    )
    assert decision == 1, "Should switch to TOU when anticipated savings exceed switching cost"

    # DYNAMIC: Should not switch when savings are low
    decision = human_controller(
        current_state=1,  # Default schedule
        realized_savings=0.0,
        unrealized_savings=1.0,  # Low savings
    )
    assert decision == 0, "Should not switch to TOU when anticipated savings are below switching cost"

    # DYNAMIC: Should switch back when TOU performance is poor
    decision = human_controller(
        current_state=0,  # TOU schedule
        realized_savings=-10.0,  # Negative savings
        unrealized_savings=0.0,  # Not used for TOU→default decision
    )
    assert decision == 1, "Should switch back to default when TOU results in higher costs"

    # DYNAMIC: Should continue TOU when performance is good
    decision = human_controller(
        current_state=0,  # TOU schedule
        realized_savings=25.0,  # Positive net savings
        unrealized_savings=0.0,
    )
    assert decision == 0, "Should continue TOU when realized savings are positive"


def test_calculate_monthly_bill():
    """Test calculate_monthly_bill function"""
    consumption = np.array([0.5, 0.6, 0.4])  # kWh/15min
    rates = np.array([0.12, 0.28, 0.12])  # $/kWh

    expected_bill = 0.5 * 0.12 + 0.6 * 0.28 + 0.4 * 0.12
    bill = calculate_monthly_bill(consumption, rates)

    assert abs(bill - expected_bill) < 1e-6

    # Test edge case with mismatched lengths
    hot_water = np.ones(50)  # Short data
    rates_long = np.ones(100)  # Longer rates
    with pytest.raises((ValueError, IndexError)):
        calculate_monthly_bill(hot_water, rates_long)


def test_calculate_comfort_penalty():
    """Test calculate_comfort_penalty function"""
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

    # Test proportional scaling
    low_unmet = np.full(96, 100.0)  # 100W each interval
    low_penalty = calculate_comfort_penalty(low_unmet, 0.15)

    high_unmet = np.full(96, 500.0)  # 500W each interval
    high_penalty = calculate_comfort_penalty(high_unmet, 0.15)

    assert high_penalty > low_penalty, "Higher unmet demand should result in higher comfort penalty"
    assert abs(high_penalty / low_penalty - 5.0) < 0.1, "Comfort penalty should scale proportionally"


def test_simulate_month_both_schedules():
    """Test simulate_month_both_schedules function"""
    hot_water = np.ones(96) * 0.1  # L/15min
    rates = create_tou_rates(96)
    params = TOUParameters()

    default_results, tou_results, default_bill, tou_bill = simulate_month_both_schedules(1, hot_water, rates, params)

    # Both should return results
    assert isinstance(default_results, SimulationResults)
    assert isinstance(tou_results, SimulationResults)
    assert isinstance(default_bill, float)
    assert isinstance(tou_bill, float)

    # Bills should be positive
    assert default_bill > 0
    assert tou_bill > 0


def test_simulate_single_month():
    """Test simulate_single_month function"""
    hot_water = np.ones(96) * 0.1
    rates = create_tou_rates(96)
    params = TOUParameters()

    # Test from default state
    result_default = simulate_single_month(1, 1, hot_water, rates, params)
    assert isinstance(result_default, MonthlyResults)
    assert result_default.current_state == 1
    assert result_default.bill > 0
    assert result_default.comfort_penalty >= 0
    assert result_default.switching_decision == 0  # Static controller returns 0
    assert result_default.realized_savings == 0.0  # No realized savings when on default
    assert isinstance(result_default.unrealized_savings, float)

    # Test from TOU state
    result_tou = simulate_single_month(1, 0, hot_water, rates, params)
    assert isinstance(result_tou, MonthlyResults)
    assert result_tou.current_state == 0
    assert result_tou.bill > 0
    assert result_tou.comfort_penalty >= 0
    assert result_tou.switching_decision == 0  # Static controller returns 0
    assert isinstance(result_tou.realized_savings, float)
    assert result_tou.unrealized_savings == 0.0  # No unrealized savings when on TOU


def test_simulate_single_month_dynamic():
    """Test simulate_single_month dynamic behavior (DYNAMIC - will fail)"""
    # DYNAMIC: Should calculate realistic non-zero savings
    hot_water = np.random.rand(2920) * 0.3  # Random usage pattern
    rates = create_tou_rates(2920)
    params = TOUParameters()

    # Simulate from default state
    result_default = simulate_single_month(1, 1, hot_water, rates, params)
    assert result_default.unrealized_savings != 0.0, "Should calculate non-zero unrealized savings potential"

    # Simulate from TOU state
    result_tou = simulate_single_month(1, 0, hot_water, rates, params)
    assert result_tou.realized_savings != 0.0, "Should calculate non-zero realized savings"


def test_simulate_annual_cycle():
    """Test simulate_annual_cycle function"""
    # Create test data for one year
    hot_water_data = np.ones(35040) * 0.1  # ~35,040 intervals per year
    params = TOUParameters()

    results = simulate_annual_cycle(hot_water_data, params)

    assert len(results) == 12  # 12 months
    assert all(isinstance(r, MonthlyResults) for r in results)
    assert all(r.month == i for i, r in enumerate(results, 1))  # Months 1-12

    # First month should start on default schedule
    assert results[0].current_state == 1

    # Test edge case with empty data
    empty_data = np.array([])
    with contextlib.suppress(ValueError, IndexError, ZeroDivisionError):
        simulate_annual_cycle(empty_data)


def test_simulate_annual_cycle_dynamic():
    """Test simulate_annual_cycle dynamic behavior (DYNAMIC - will fail)"""
    # DYNAMIC: Should see some TOU adoption over the year
    hot_water_data = np.random.rand(35040) * 0.2  # Realistic usage levels
    params = TOUParameters()

    monthly_results = simulate_annual_cycle(hot_water_data, params)
    annual_metrics = calculate_annual_metrics(monthly_results)

    assert annual_metrics["tou_adoption_rate_percent"] > 0, (
        "Should see some TOU adoption with realistic decision-making"
    )
    assert annual_metrics["annual_switches"] > 0, "Should see switching decisions with dynamic human controller"

    # DYNAMIC: Should see seasonal switching patterns
    hot_water_seasonal = np.random.rand(35040) * 0.2
    for month in [0, 1, 10, 11]:  # Winter months
        start_idx = month * 2920
        end_idx = (month + 1) * 2920
        hot_water_seasonal[start_idx:end_idx] *= 1.5

    monthly_results = simulate_annual_cycle(hot_water_seasonal)
    switches_by_month = [r.switching_decision for r in monthly_results]
    assert any(switch == 1 for switch in switches_by_month), "Should see switching activity with seasonal patterns"


def test_calculate_annual_metrics():
    """Test calculate_annual_metrics function"""
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


def test_load_input_data():
    """Test load_input_data function"""
    # Test with non-existent file
    with pytest.raises((FileNotFoundError, ValueError)):
        load_input_data("nonexistent_file.csv")

    # Test with default path (may pass or fail depending on file availability)
    try:
        data = load_input_data()
        assert isinstance(data, np.ndarray)
        assert len(data) > 0
    except (FileNotFoundError, ValueError):
        # Expected if input files not available in test environment
        pass


def test_run_full_simulation():
    """Test run_full_simulation function"""
    # Test function signature and error handling
    try:
        monthly_results, annual_metrics = run_full_simulation()

        # If it succeeds, check structure
        assert isinstance(monthly_results, list)
        assert isinstance(annual_metrics, dict)
        assert len(monthly_results) == 12

    except (FileNotFoundError, ValueError) as e:
        # Expected if input files not available
        assert "not found" in str(e).lower() or "hot_water_fixtures" in str(e)

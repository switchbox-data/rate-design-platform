from datetime import datetime

import numpy as np

from rate_design_platform.Analysis import (
    MonthlyMetrics,
    MonthlyResults,
    SimulationResults,
    calculate_monthly_bill,
    calculate_monthly_bill_and_comfort_penalty,
    calculate_monthly_comfort_penalty,
    calculate_monthly_metrics,
)
from rate_design_platform.utils.rates import MonthlyRateStructure, TOUParameters


def test_calculate_monthly_bill():
    simulation_results = SimulationResults(
        Time=np.array([datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)]),
        E_mt=np.array([10, 20]),
        T_tank_mt=np.array([50, 60]),
        D_unmet_mt=np.array([0, 0]),
    )
    monthly_rate_structure = [
        MonthlyRateStructure(year=2024, month=1, intervals=2, rates=np.array([0.1, 0.2])),
    ]
    monthly_metrics = calculate_monthly_bill(simulation_results, monthly_rate_structure)
    assert monthly_metrics == [MonthlyMetrics(year=2024, month=1, bill=5)]

    simulation_results = SimulationResults(
        Time=np.array([
            datetime(2022, 1, 1, 0, 0, 0),
            datetime(2022, 1, 15, 0, 0, 0),
            datetime(2022, 2, 1, 0, 0, 0),
            datetime(2022, 2, 15, 0, 0, 0),
        ]),
        E_mt=np.array([10, 20, 30, 40]),
        T_tank_mt=np.array([50, 60, 70, 80]),
        D_unmet_mt=np.array([0, 0, 0, 0]),
    )
    monthly_rate_structure = [
        MonthlyRateStructure(year=2022, month=1, intervals=2, rates=np.array([0.1, 0.2])),
        MonthlyRateStructure(year=2022, month=2, intervals=2, rates=np.array([0.1, 0.2])),
    ]
    monthly_metrics = calculate_monthly_bill(simulation_results, monthly_rate_structure)
    assert monthly_metrics == [MonthlyMetrics(year=2022, month=1, bill=5), MonthlyMetrics(year=2022, month=2, bill=11)]


def test_calculate_monthly_comfort_penalty():
    # Test case 1: No unmet demand
    simulation_results = SimulationResults(
        Time=np.array([datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)]),
        E_mt=np.array([10, 20]),
        T_tank_mt=np.array([50, 60]),
        D_unmet_mt=np.array([0, 0]),
    )
    TOU_params = TOUParameters(alpha=0.1)
    monthly_metrics = calculate_monthly_comfort_penalty(simulation_results, TOU_params)
    assert monthly_metrics == [MonthlyMetrics(year=2024, month=1, comfort_penalty=0)]

    # Test case 2: Some unmet demand
    simulation_results = SimulationResults(
        Time=np.array([datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)]),
        E_mt=np.array([10, 20]),
        T_tank_mt=np.array([50, 60]),
        D_unmet_mt=np.array([10, 20]),
    )
    TOU_params = TOUParameters(alpha=0.1)
    monthly_metrics = calculate_monthly_comfort_penalty(simulation_results, TOU_params)
    assert monthly_metrics == [MonthlyMetrics(year=2024, month=1, comfort_penalty=3)]

    # Test case 3: more unmet demands
    simulation_results = SimulationResults(
        Time=np.array([datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0, 0)]),
        E_mt=np.array([10, 20, 30]),
        T_tank_mt=np.array([50, 60, 70]),
        D_unmet_mt=np.array([10, 20, 30]),
    )
    TOU_params = TOUParameters(alpha=0.1)
    monthly_metrics = calculate_monthly_comfort_penalty(simulation_results, TOU_params)
    assert monthly_metrics == [MonthlyMetrics(year=2024, month=1, comfort_penalty=6)]


def test_calculate_monthly_bill_and_comfort_penalty():
    simulation_results = SimulationResults(
        Time=np.array([datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)]),
        E_mt=np.array([10, 20]),
        T_tank_mt=np.array([50, 60]),
        D_unmet_mt=np.array([0, 0]),
    )
    monthly_rate_structure = [MonthlyRateStructure(year=2024, month=1, intervals=2, rates=np.array([0.1, 0.2]))]
    TOU_params = TOUParameters(alpha=0.1)
    monthly_metrics = calculate_monthly_bill_and_comfort_penalty(simulation_results, monthly_rate_structure, TOU_params)
    assert monthly_metrics == [MonthlyMetrics(year=2024, month=1, bill=5, comfort_penalty=0)]

    simulation_results = SimulationResults(
        Time=np.array([datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 1, 0, 0)]),
        E_mt=np.array([10, 20]),
        T_tank_mt=np.array([50, 60]),
        D_unmet_mt=np.array([10, 20]),
    )
    monthly_rate_structure = [MonthlyRateStructure(year=2024, month=1, intervals=2, rates=np.array([0.1, 0.2]))]
    TOU_params = TOUParameters(alpha=0.1)
    monthly_metrics = calculate_monthly_bill_and_comfort_penalty(simulation_results, monthly_rate_structure, TOU_params)
    assert monthly_metrics == [MonthlyMetrics(year=2024, month=1, bill=5, comfort_penalty=3)]


def test_calculate_monthly_metrics():
    """Test calculate_monthly_metrics function"""
    year_months = [(2018, 1), (2018, 2)]
    decisions = ["switch", "stay"]
    states = ["default", "tou"]
    default_bills = [100.0, 110.0]
    tou_bills = [80.0, 90.0]
    default_penalties = [2.0, 3.0]
    tou_penalties = [5.0, 6.0]

    results = calculate_monthly_metrics(
        year_months, decisions, states, default_bills, tou_bills, default_penalties, tou_penalties
    )

    assert len(results) == 2
    assert all(isinstance(result, MonthlyResults) for result in results)

    # Check first month (default state)
    assert results[0].year == 2018
    assert results[0].month == 1
    assert results[0].current_state == "default"
    assert results[0].bill == 100.0  # Uses default bill
    assert results[0].comfort_penalty == 2.0  # Uses default penalty
    assert results[0].switching_decision == "switch"
    assert results[0].realized_savings == 0  # No realized savings on default
    assert results[0].unrealized_savings == 20.0  # 100 - 80

    # Check second month (TOU state)
    assert results[1].current_state == "tou"
    assert results[1].bill == 90.0  # Uses TOU bill
    assert results[1].comfort_penalty == 6.0  # Uses TOU penalty
    assert results[1].realized_savings == 20.0  # 110 - 90
    assert results[1].unrealized_savings == 0  # No unrealized savings on TOU

    # Test  case: empty inputs
    empty_results = calculate_monthly_metrics([], [], [], [], [], [], [])
    assert len(empty_results) == 0

    # Test  case: single month
    single_results = calculate_monthly_metrics([(2018, 3)], ["stay"], ["default"], [120.0], [95.0], [1.0], [4.0])
    assert len(single_results) == 1
    assert single_results[0].month == 3
    assert single_results[0].unrealized_savings == 25.0  # 120 - 95

from datetime import datetime

import numpy as np

from rate_design_platform.Analysis import (
    MonthlyMetrics,
    SimulationResults,
    calculate_monthly_bill,
    calculate_monthly_comfort_penalty,
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

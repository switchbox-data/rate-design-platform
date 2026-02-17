"""Utility function to reweight customer counts for rate design analysis.

This module provides a function to reweight ResStock building samples to match
utility-specific customer counts from rate cases using the CAIRO reweighting formula.

TODO: Delete this module once NY run_scenario is updated to use
cairo.rates_tool.loads.return_buildingstock(customer_count=...) directly.
"""

import pandas as pd


def reweight_customer_counts(
    customer_metadata: pd.DataFrame,
    target_count: int,
) -> pd.DataFrame:
    """Reweight customer metadata to match target customer count.

    Applies CAIRO reweighting formula: new_weight = old_weight * (target / sum_old)

    Args:
        customer_metadata: DataFrame with bldg_id and weight columns (from return_buildingstock)
        target_count: Target total customer count for the utility

    Returns:
        Full customer_metadata DataFrame with reweighted weight column
    """
    df = customer_metadata.copy()

    current_sum = df["weight"].sum()
    scale_factor = target_count / current_sum

    df["weight"] = df["weight"] * scale_factor

    return df

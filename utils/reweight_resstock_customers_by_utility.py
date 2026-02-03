"""Utility function to reweight customer counts for rate design analysis.

This module provides a function to reweight ResStock building samples to match
utility-specific customer counts from rate cases using the CAIRO reweighting formula.
"""

from pathlib import Path

import pandas as pd


def reweight_customer_counts(
    customer_metadata: pd.DataFrame,
    target_count: int,
    output_path: Path,
) -> pd.DataFrame:
    """Reweight customer metadata to match target customer count and save to CSV.

    Applies CAIRO reweighting formula: new_weight = old_weight * (target / sum_old)

    Args:
        customer_metadata: DataFrame with bldg_id and weight columns (from return_buildingstock)
        target_count: Target total customer count for the utility
        output_path: Path for output CSV file

    Returns:
        Full customer_metadata DataFrame with reweighted weight column
    """
    df = customer_metadata.copy()

    current_sum = df["weight"].sum()
    scale_factor = target_count / current_sum

    df["weight"] = df["weight"] * scale_factor

    # Save weights to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df[["bldg_id", "weight"]].to_csv(output_path, index=False)

    return df

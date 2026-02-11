"""Utility functions for Cairo-related operations."""

import pandas as pd
from pathlib import Path


def patch_postprocessor_peak_allocation():
    """
    Patch CAIRO's _allocate_residual_peak to identify peak hours using
    Marginal Distribution Costs + Marginal Capacity Costs > 0, instead of
    only Marginal Capacity Costs > 0.

    This makes the peak residual allocation robust to zero Cambium capacity
    costs (e.g., dummy data), since distribution cost peak hours (~100 hours
    from add_distribution_costs) will still define peak periods.

    TODO: Remove once CAIRO branch sz/peak-residual-sum-costs is merged to main.
    """
    from cairo.rates_tool.postprocessing import InternalCrossSubsidizationProcessor

    def _patched_allocate_residual_peak(
        self, building_metadata, raw_hourly_load, marginal_system_prices, costs_by_type
    ):
        # Identify peak periods using distribution + capacity costs (not just capacity)
        peak_cost_cols = []
        if "Marginal Capacity Costs ($/kWh)" in marginal_system_prices.columns:
            peak_cost_cols.append("Marginal Capacity Costs ($/kWh)")
        if "Marginal Distribution Costs ($/kWh)" in marginal_system_prices.columns:
            peak_cost_cols.append("Marginal Distribution Costs ($/kWh)")

        combined_peak_signal = marginal_system_prices[peak_cost_cols].sum(axis=1)
        peak_hours_mask = combined_peak_signal > 0.0

        if not peak_hours_mask.any():
            return pd.Series(
                0.0,
                index=building_metadata["bldg_id"],
                name="customer_level_residual_share_peak",
            )

        peak_hours_index = marginal_system_prices.index[peak_hours_mask]

        # Sum all load during peak periods by customer
        peak_demand_contribution = (
            raw_hourly_load.reset_index("bldg_id")
            .loc[peak_hours_index, ["bldg_id", "electricity_net"]]
            .groupby(["bldg_id"])["electricity_net"]
            .sum()
        )
        peak_demand_contribution = pd.merge(
            peak_demand_contribution,
            building_metadata.set_index("bldg_id")[["weight"]],
            right_index=True,
            left_index=True,
            how="left",
        )

        denominator = peak_demand_contribution.prod(axis=1).sum().squeeze()
        if denominator == 0:
            return pd.Series(
                0.0,
                index=building_metadata["bldg_id"],
                name="customer_level_residual_share_peak",
            )

        peak_charge_residual = costs_by_type["Residual Costs ($)"] / denominator

        annual_customer_residual_share = peak_demand_contribution[
            "electricity_net"
        ].mul(peak_charge_residual)
        annual_customer_residual_share.name = "customer_level_residual_share_peak"

        return annual_customer_residual_share

    InternalCrossSubsidizationProcessor._allocate_residual_peak = (
        _patched_allocate_residual_peak
    )


def build_bldg_id_to_load_filepath(
    path_resstock_loads: Path,
    building_ids: list[int] | None = None,
    return_path_base: Path | None = None,
) -> dict[int, Path]:
    """
    Build a dictionary mapping building IDs to their load file paths.

    Args:
        path_resstock_loads: Directory containing parquet load files to scan
        building_ids: Optional list of building IDs to include. If None, includes all.
        return_path_base: Base directory for returned paths.
            If None, returns actual file paths from path_resstock_loads.
            If Path, returns paths as return_path_base / filename.

    Returns:
        Dictionary mapping building ID (int) to full file path (Path)

    Raises:
        FileNotFoundError: If path_resstock_loads does not exist
    """
    if not path_resstock_loads.exists():
        raise FileNotFoundError(f"Load directory not found: {path_resstock_loads}")

    building_ids_set = set(building_ids) if building_ids is not None else None

    bldg_id_to_load_filepath = {}
    for parquet_file in path_resstock_loads.glob("*.parquet"):
        try:
            bldg_id = int(parquet_file.stem.split("-")[0])
        except ValueError:
            continue  # Skip files that don't match expected pattern

        if building_ids_set is not None and bldg_id not in building_ids_set:
            continue

        if return_path_base is None:
            filepath = parquet_file
        else:
            filepath = return_path_base / parquet_file.name

        bldg_id_to_load_filepath[bldg_id] = filepath

    return bldg_id_to_load_filepath

"""Utility functions for Cairo-related operations."""

import json
import pandas as pd
import polars as pl
from pathlib import Path, PurePath

import dask

from cairo.rates_tool import config, lookups
from cairo.rates_tool.loads import __timeshift__, _load_worker
from cairo.rates_tool.tariffs import URDBv7_to_ElectricityRates

# Default columns to load from buildingstock metadata parquet files.
# Mirrors cairo.rates_tool.lookups.summary_buildings_cols_keep
def _load_cambium_marginal_costs(path: Path | PurePath, target_year: int):
    """Load Cambium marginal costs using engine='c' so columns are numpy-backed.
    Avoids Arrow-backed in-place division failure in pandas (LossySetitemError / Arrow _arith_method).
    """
    df = pd.read_csv(
        path, skiprows=5, index_col="timestamp", parse_dates=True, engine="c"
    )
    keep_cols = {
        "energy_cost_enduse": "Marginal Energy Costs ($/kWh)",
        "capacity_cost_enduse": "Marginal Capacity Costs ($/kWh)",
    }
    df = df.loc[:, list(keep_cols)].rename(columns=keep_cols)
    df.loc[:, [c for c in df.columns if "/kWh" in c]] /= 1000  # $/MWh -> $/kWh
    df.index.name = "time"
    common_years = [2017, 2023, 2034, 2045, 2051]
    year_diff = [abs(y - target_year) for y in common_years]
    common_year = common_years[year_diff.index(min(year_diff))]
    df.index = pd.DatetimeIndex(
        [t.replace(year=common_year) for t in df.index], name="time"
    )
    df = __timeshift__(df, target_year)
    df.index = df.index.tz_localize("EST")
    return df


DEFAULT_BUILDINGSTOCK_COLUMNS: list[str] = [
    "bldg_id",
    "weight",
    "applicability",
    "upgrade",
    "postprocess_group.has_hp",
    "in.income",
    "in.ashrae_iecc_climate_zone_2004",
    "in.census_region",
    "in.county_and_puma",
    "in.federal_poverty_level",
    "in.geometry_building_type_recs",
    "in.geometry_building_type_acs",
    "in.has_pv",
    "in.pv_orientation",
    "in.pv_system_size",
    "in.occupants",
    "in.tenure",
    "in.vintage",
    "in.vintage_acs",
    "out.electricity.total.energy_consumption.kwh",
    "out.natural_gas.total.energy_consumption.kwh",
]


def patch_postprocessor_weight_handling():
    """
    Patch CAIRO's calculate_bill_stats_by_group to handle missing 'weight' column.

    CAIRO's _postprocess_bill_results merges elec and gas bills with
    suffixes=("_elec", "_gas"), renaming 'weight' to 'weight_elec'/'weight_gas'
    in combined bills. This patch restores 'weight' from metadata_df when missing.
    """
    from cairo.rates_tool.postprocessing import InternalCustomerBillProcessor

    _original = InternalCustomerBillProcessor.calculate_bill_stats_by_group

    def _patched(self, metadata_df, bill_type, bill_df, gcn):
        if "weight" not in bill_df.columns:
            weight_map = metadata_df[["bldg_id", "weight"]].drop_duplicates()
            bill_df = bill_df.merge(weight_map, on="bldg_id", how="left")
        return _original(self, metadata_df, bill_type, bill_df, gcn)

    InternalCustomerBillProcessor.calculate_bill_stats_by_group = _patched


def patch_postprocessor_peak_allocation():
    """
    Patch CAIRO's _allocate_residual_peak to identify peak hours using
    Marginal Distribution Costs + Marginal Capacity Costs > 0, instead of
    only Marginal Capacity Costs > 0.

    This makes the peak residual allocation robust to zero Cambium capacity
    costs (e.g., dummy data), since distribution cost peak hours (~100 hours
    from add_distribution_costs) will still define peak periods.
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


def _load_tariff_json(tariff_path: Path) -> dict:
    """
    Load and extract tariff structure from URDB v7 JSON file.

    Args:
        tariff_path: Path to tariff JSON file

    Returns:
        Tariff dictionary (first item from URDB "items" array)

    Raises:
        FileNotFoundError: If tariff file does not exist
        KeyError: If JSON structure is invalid (missing "items" key)
    """
    if not tariff_path.exists():
        raise FileNotFoundError(f"Tariff file not found: {tariff_path}")

    with open(tariff_path) as f:
        tariff_data = json.load(f)

    if "items" not in tariff_data or not tariff_data["items"]:
        raise KeyError(f"Invalid tariff JSON structure in: {tariff_path}")

    return tariff_data["items"][0]


def get_default_tariff_structures(tariff_paths: dict[str, Path]) -> dict[str, object]:
    """
    Load tariff JSON files and convert to ElectricityRates objects.

    Args:
        tariff_paths: Dict mapping tariff key name to JSON file path.
                      Keys must match tariff_key values in tariff map CSV.

    Returns:
        Dictionary mapping tariff key (str) to ElectricityRates object

    Raises:
        FileNotFoundError: If any tariff file does not exist
    """
    tariff_structures = {}
    for tariff_key, tariff_path in tariff_paths.items():
        tariff_dict = _load_tariff_json(tariff_path)
        tariff_structures[tariff_key] = URDBv7_to_ElectricityRates(tariff_dict)

    return tariff_structures


def _initialize_tariffs(
    tariff_map: str | Path | pd.DataFrame,
    tariff_paths: dict[str, Path],
    building_stock_sample: list[int] | None = None,
    tariff_map_dir: Path | None = None,
) -> tuple[dict, pd.DataFrame]:
    """
    Initialize tariff parameters and mapping for bill calculation.

    Args:
        tariff_map: Tariff map identifier. Can be:
            - str: Name prefix (loads "tariff_map_{name}.csv" from tariff_map_dir)
            - Path: Direct path to CSV file
            - DataFrame: Pre-loaded tariff mapping
        tariff_paths: Dict mapping tariff key name to JSON file path.
                      Keys must match tariff_key values in tariff map CSV.
        building_stock_sample: Optional list of building IDs to filter
        tariff_map_dir: Directory for string-based tariff map lookup.
            If None and tariff_map is str, falls back to config.TARIFFSSTOCKMAP.

    Returns:
        Tuple of (param_grid dict, filtered tariff_map DataFrame)

    Raises:
        FileNotFoundError: If tariff map file does not exist
        ValueError: If tariff_map type is unsupported
    """
    if isinstance(tariff_map, str):
        base_dir = (
            tariff_map_dir if tariff_map_dir is not None else config.TARIFFSSTOCKMAP
        )
        tariff_map_fp = base_dir / f"tariff_map_{tariff_map}.csv"
        if not tariff_map_fp.exists():
            raise FileNotFoundError(f"Tariff map not found: {tariff_map_fp}")
        tariff_map_df = pd.read_csv(tariff_map_fp)
    elif isinstance(tariff_map, PurePath):
        if not tariff_map.exists():
            raise FileNotFoundError(f"Tariff map not found: {tariff_map}")
        tariff_map_df = pd.read_csv(tariff_map)
    elif isinstance(tariff_map, pd.DataFrame):
        tariff_map_df = tariff_map
    else:
        raise ValueError(
            f"tariff_map must be str, Path, or DataFrame, got {type(tariff_map)}"
        )

    if building_stock_sample is not None:
        tariff_map_df = tariff_map_df.loc[
            tariff_map_df.bldg_id.isin(building_stock_sample)
        ]

    param_grid = get_default_tariff_structures(tariff_paths)

    return param_grid, tariff_map_df


def return_buildingstock(
    metadata_path: Path,
    columns: list[str] | None = None,
    building_ids: list[int] | None = None,
    default_weight: float = 1.0,
) -> pd.DataFrame:
    """
    Load buildingstock metadata from parquet with custom column selection.

    Alternative to cairo.rates_tool.loads.return_buildingstock with:
    - Custom column selection (only loads columns that exist)
    - Default weight values when 'weight' column is missing

    Args:
        metadata_path: Path to the buildingstock metadata parquet file
        columns: List of columns to load. If None, uses DEFAULT_BUILDINGSTOCK_COLUMNS.
                 Columns that don't exist are silently skipped.
                 'bldg_id' is always included.
        building_ids: Optional list of building IDs to filter to.
        default_weight: Default weight value when 'weight' column is missing (default: 1.0)

    Returns:
        pd.DataFrame with 'bldg_id' and 'weight' columns guaranteed

    Raises:
        FileNotFoundError: If metadata_path does not exist
        ValueError: If 'bldg_id' column is not in the parquet file
    """
    if not metadata_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {metadata_path}")

    if columns is None:
        columns = DEFAULT_BUILDINGSTOCK_COLUMNS

    schema = pl.read_parquet_schema(metadata_path)
    available_cols = set(schema.keys())

    if "bldg_id" not in available_cols:
        raise ValueError(
            f"Required column 'bldg_id' not found in {metadata_path}. "
            f"Available columns: {sorted(available_cols)}"
        )

    columns_to_load = ["bldg_id"] + [
        c for c in columns if c in available_cols and c != "bldg_id"
    ]

    df = pl.read_parquet(metadata_path, columns=columns_to_load)

    if building_ids is not None:
        df = df.filter(pl.col("bldg_id").is_in(building_ids))

    if "weight" not in df.columns:
        df = df.with_columns(pl.lit(default_weight).alias("weight"))

    return df.to_pandas()


# Column renames from ResStock parquet names to CAIRO internal names
_LOAD_COL_RENAMES = {
    "timestamp": "time",
    "out.electricity.total.energy_consumption": "load_data",
    "out.natural_gas.total.energy_consumption": "load_data",
    "out.electricity.net.energy_consumption": "electricity_net",
    "out.electricity.pv.energy_consumption": "pv_generation",
}


def _return_load(
    load_type: str,
    target_year: int,
    load_filepath_key: dict[int, Path],
    force_tz: str | None = None,
) -> pd.DataFrame:
    """
    Load hourly building load profiles from per-building parquet files.

    Alternative to cairo.rates_tool.loads._return_load that handles
    per-building files where bldg_id is not the parquet index.
    Reads each file, sets bldg_id as index, renames columns to CAIRO
    conventions, then delegates to CAIRO's _load_worker pipeline for
    time correction, year shifting, and timezone localization.

    Args:
        load_type: 'electricity' or 'gas'
        target_year: Target year for timestamp alignment
        load_filepath_key: Dict mapping bldg_id -> parquet file path
        force_tz: Timezone string to apply (e.g. 'EST'), or None

    Returns:
        pd.DataFrame with MultiIndex ['bldg_id', 'time'], 8760 rows per building
    """
    assert load_type in ["electricity", "gas"], (
        "load_type must be 'electricity' or 'gas'"
    )
    load_col_key = (
        "total_fuel_electricity" if load_type == "electricity" else "total_fuel_gas"
    )

    delayed_results = []
    for bldg_id, filepath in load_filepath_key.items():
        # Read parquet, set bldg_id as index, rename cols to CAIRO conventions
        df = pd.read_parquet(
            filepath, engine="pyarrow", columns=lookups.load_types_map[load_col_key]
        )
        df.index = pd.Index([bldg_id] * len(df), name="bldg_id")
        df = df.rename(columns=_LOAD_COL_RENAMES)

        # Handle electricity_net when PV data is present
        if (
            "electricity_net" not in df.columns
            and "pv_generation" in df.columns
            and load_col_key != "total_fuel_gas"
        ):
            if all(df["pv_generation"] == 0.0):
                df["electricity_net"] = df["load_data"]
            elif any(df["pv_generation"] < 0.0):
                df["electricity_net"] = df["load_data"] + df["pv_generation"]
            else:
                df["electricity_net"] = df["load_data"] - df["pv_generation"]

        delayed_results.append(
            dask.delayed(_load_worker)(bldg_id, df, load_col_key, target_year, force_tz)
        )

    results = dask.compute(delayed_results)[0]
    return pd.concat(results)

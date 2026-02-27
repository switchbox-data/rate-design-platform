"""Utility functions for Cairo-related operations."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import polars as pl
from cairo.rates_tool import config
from cairo.rates_tool.loads import __timeshift__
from cloudpathlib import S3Path

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.types import ElectricUtility

CambiumPathLike = str | Path | S3Path
log = logging.getLogger(__name__)


def _is_cambium_path(path: CambiumPathLike) -> bool:
    """Check if a path is a Cambium file path (contains 'cambium' in the path string)."""
    return "cambium" in str(path).lower()


def _normalize_cambium_path(cambium_scenario: CambiumPathLike):  # noqa: ANN201
    """Return a single path-like (Path or S3Path) for Cambium CSV or Parquet."""
    if isinstance(cambium_scenario, S3Path):
        return cambium_scenario
    if isinstance(cambium_scenario, Path):
        return cambium_scenario
    if isinstance(cambium_scenario, str):
        if cambium_scenario.startswith("s3://"):
            return S3Path(cambium_scenario)
        if "/" in cambium_scenario or cambium_scenario.endswith((".csv", ".parquet")):
            return Path(cambium_scenario)
        return config.MARGINALCOST_DIR / f"{cambium_scenario}.csv"
    raise TypeError(
        f"cambium_scenario must be str, Path, or S3Path; got {type(cambium_scenario)}"
    )


def _load_cambium_marginal_costs(
    cambium_scenario: CambiumPathLike, target_year: int
) -> pd.DataFrame:
    """
    Load Cambium marginal costs from CSV or Parquet (local or S3). Returns costs in $/kWh.

    Accepts: scenario name (str → CSV under config dir), local path (str or Path),
    or S3 URI (str or S3Path). Example S3 Parquet:
    s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet

    Assumptions (verified against S3 Parquet and repo CSV example_marginal_costs.csv):
    - CSV: first 5 rows are metadata; row 6 is header; data has columns timestamp,
      energy_cost_enduse, capacity_cost_enduse. Costs are in $/MWh.
    - Parquet: columns timestamp (datetime), energy_cost_enduse, capacity_cost_enduse
      (float). Costs are in $/MWh. Exactly 8760 rows (hourly). No partition columns
      in the DataFrame (single-file read).
    - Both: we divide cost columns by 1000 to get $/kWh; then common_year alignment,
      __timeshift__ to target_year, and tz_localize("EST") so output matches CAIRO.
    """
    path = _normalize_cambium_path(cambium_scenario)
    if not path.exists():
        raise FileNotFoundError(f"Cambium marginal cost file {path} does not exist")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        if isinstance(path, S3Path):
            raw = path.read_bytes()
            df = pd.read_csv(
                io.BytesIO(raw),
                skiprows=5,
                index_col="timestamp",
                parse_dates=True,
            )
        else:
            df = pd.read_csv(
                path,
                skiprows=5,
                index_col="timestamp",
                parse_dates=True,
            )
    elif suffix == ".parquet":
        if isinstance(path, S3Path):
            # Read as bytes and pass BytesIO so PyArrow reads a single file. If we pass
            # the S3 path (even with explicit S3FileSystem), PyArrow infers a partitioned
            # dataset from path segments like scenario=MidCase/... and raises
            # ArrowTypeError when merging partition schemas.
            raw = path.read_bytes()
            df = pd.read_parquet(io.BytesIO(raw), engine="pyarrow")
        else:
            df = pd.read_parquet(path)
        if "timestamp" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            df = df.set_index("timestamp")
        if df.index.name != "time":
            df.index.name = "time"
    else:
        raise ValueError(
            f"Cambium file must be .csv or .parquet; got {path} (suffix {suffix})"
        )

    keep_cols = {
        "energy_cost_enduse": "Marginal Energy Costs ($/kWh)",
        "capacity_cost_enduse": "Marginal Capacity Costs ($/kWh)",
    }
    numeric_input_cols = list(keep_cols.keys())
    for col in numeric_input_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=numeric_input_cols, how="any")
    if df.empty:
        raise ValueError(
            f"Cambium marginal cost file {path} has no valid numeric rows in required "
            f"columns: {numeric_input_cols}"
        )

    df = df.loc[:, list(keep_cols.keys())].rename(columns=keep_cols)
    df.loc[:, [c for c in df.columns if "/kWh" in c]] /= 1000  # $/MWh → $/kWh

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.loc[~df.index.isna()].copy()
    if df.empty:
        raise ValueError(f"Cambium marginal cost file {path} has no valid timestamps")

    common_years = [2017, 2023, 2034, 2045, 2051]
    year_diff = [abs(y - target_year) for y in common_years]
    common_year = common_years[year_diff.index(min(year_diff))]
    df.index = pd.DatetimeIndex(
        [t.replace(year=common_year) for t in df.index],
        name="time",
    )
    df = __timeshift__(df, target_year)
    df.index = df.index.tz_localize("EST")
    df.index.name = "time"
    return df


def _load_supply_energy_mc(
    energy_path: CambiumPathLike, target_year: int
) -> pd.DataFrame:
    """
    Load supply energy marginal costs (LBMP) from parquet (local or S3). Returns costs in $/kWh.

    Accepts: local path (str or Path) or S3 URI (str or S3Path).
    Example S3 Parquet:
    s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=nyseg/year=2025/data.parquet

    Assumptions:
    - Parquet: columns timestamp (datetime), energy_cost_enduse (float).
      Costs are in $/MWh. Exactly 8760 rows (hourly).
    - We divide by 1000 to get $/kWh; then common_year alignment,
      __timeshift__ to target_year, and tz_localize("EST") so output matches CAIRO.
    """
    path = _normalize_cambium_path(energy_path)
    if not path.exists():
        raise FileNotFoundError(f"Supply energy MC file {path} does not exist")

    if path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Supply energy MC file must be .parquet; got {path} (suffix {path.suffix})"
        )

    if isinstance(path, S3Path):
        # Read as bytes and pass BytesIO so PyArrow reads a single file
        raw = path.read_bytes()
        df = pd.read_parquet(io.BytesIO(raw), engine="pyarrow")
    else:
        df = pd.read_parquet(path)

    if "timestamp" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        df = df.set_index("timestamp")
    if df.index.name != "time":
        df.index.name = "time"

    # Keep only energy column
    if "energy_cost_enduse" not in df.columns:
        raise ValueError(
            f"Supply energy MC file {path} missing required column: energy_cost_enduse"
        )

    df = df.loc[:, ["energy_cost_enduse"]].copy()
    df["energy_cost_enduse"] = pd.to_numeric(df["energy_cost_enduse"], errors="coerce")
    df = df.dropna(subset=["energy_cost_enduse"], how="any")

    if df.empty:
        raise ValueError(
            f"Supply energy MC file {path} has no valid numeric rows in energy_cost_enduse"
        )

    # Rename to match CAIRO convention
    df = df.rename(columns={"energy_cost_enduse": "Marginal Energy Costs ($/kWh)"})
    df.loc[:, "Marginal Energy Costs ($/kWh)"] /= 1000  # $/MWh → $/kWh

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.loc[~df.index.isna()].copy()
    if df.empty:
        raise ValueError(f"Supply energy MC file {path} has no valid timestamps")

    # Align to common_year (first year in index) and timeshift to target_year
    common_year = df.index[0].year
    if common_year != target_year:
        df = __timeshift__(df, target_year)

    # Set timezone to EST (CAIRO convention)
    if df.index.tz is None:
        df.index = df.index.tz_localize("EST")
    else:
        df.index = df.index.tz_convert("EST")
    df.index.name = "time"

    return df


def _load_supply_capacity_mc(
    capacity_path: CambiumPathLike, target_year: int
) -> pd.DataFrame:
    """
    Load supply capacity marginal costs (ICAP) from parquet (local or S3). Returns costs in $/kWh.

    Accepts: local path (str or Path) or S3 URI (str or S3Path).
    Example S3 Parquet:
    s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility=nyseg/year=2025/data.parquet

    Assumptions:
    - Parquet: columns timestamp (datetime), capacity_cost_enduse (float).
      Costs are in $/MWh. Exactly 8760 rows (hourly).
    - We divide by 1000 to get $/kWh; then common_year alignment,
      __timeshift__ to target_year, and tz_localize("EST") so output matches CAIRO.
    """
    path = _normalize_cambium_path(capacity_path)
    if not path.exists():
        raise FileNotFoundError(f"Supply capacity MC file {path} does not exist")

    if path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Supply capacity MC file must be .parquet; got {path} (suffix {path.suffix})"
        )

    if isinstance(path, S3Path):
        # Read as bytes and pass BytesIO so PyArrow reads a single file
        raw = path.read_bytes()
        df = pd.read_parquet(io.BytesIO(raw), engine="pyarrow")
    else:
        df = pd.read_parquet(path)

    if "timestamp" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        df = df.set_index("timestamp")
    if df.index.name != "time":
        df.index.name = "time"

    # Keep only capacity column
    if "capacity_cost_enduse" not in df.columns:
        raise ValueError(
            f"Supply capacity MC file {path} missing required column: capacity_cost_enduse"
        )

    df = df.loc[:, ["capacity_cost_enduse"]].copy()
    df["capacity_cost_enduse"] = pd.to_numeric(
        df["capacity_cost_enduse"], errors="coerce"
    )
    df = df.dropna(subset=["capacity_cost_enduse"], how="any")

    if df.empty:
        raise ValueError(
            f"Supply capacity MC file {path} has no valid numeric rows in capacity_cost_enduse"
        )

    # Rename to match CAIRO convention
    df = df.rename(columns={"capacity_cost_enduse": "Marginal Capacity Costs ($/kWh)"})
    df.loc[:, "Marginal Capacity Costs ($/kWh)"] /= 1000  # $/MWh → $/kWh

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.loc[~df.index.isna()].copy()
    if df.empty:
        raise ValueError(f"Supply capacity MC file {path} has no valid timestamps")

    # Align to common_year (first year in index) and timeshift to target_year
    common_year = df.index[0].year
    if common_year != target_year:
        df = __timeshift__(df, target_year)

    # Set timezone to EST (CAIRO convention)
    if df.index.tz is None:
        df.index = df.index.tz_localize("EST")
    else:
        df.index = df.index.tz_convert("EST")
    df.index.name = "time"

    return df


def _load_supply_marginal_costs(
    energy_path: CambiumPathLike | None,
    capacity_path: CambiumPathLike | None,
    target_year: int,
) -> pd.DataFrame:
    """
    Load supply marginal costs: Energy + Capacity (bulk supply).

    Architecture:
    - Supply MCs = Energy + Capacity (bulk supply, from Cambium or NYISO)
    - Delivery MCs = Bulk Tx + Dist+Sub-Tx (handled separately)
    All MCs are index-aligned to a common 8760-hour DatetimeIndex.

    If both energy_path and capacity_path are provided:
    - If either path contains "cambium", treat it as a Cambium file (combined energy+capacity)
      and use _load_cambium_marginal_costs with the cambium path.
    - Otherwise, load them separately and combine.

    If only one is provided, raises ValueError.
    If both are None, raises ValueError (caller should use _load_cambium_marginal_costs instead).

    For delivery-only runs (add_supply_revenue_requirement=false), supply MCs may be set to
    zero but are still loaded to provide the alignment target index.

    Returns DataFrame with columns matching CAIRO convention:
    - Marginal Energy Costs ($/kWh)
    - Marginal Capacity Costs ($/kWh)

    Args:
        energy_path: Path to energy MC parquet (or None). If path contains "cambium",
                     treated as combined Cambium file.
        capacity_path: Path to capacity MC parquet (or None). If path contains "cambium",
                       treated as combined Cambium file.
        target_year: Target year for timeshifting.

    Returns:
        Combined DataFrame with both energy and capacity costs, indexed by DatetimeIndex
        (EST-localized, 8760 rows).
    """
    if energy_path is None and capacity_path is None:
        raise ValueError(
            "Both energy_path and capacity_path are None. "
            "Use _load_cambium_marginal_costs for combined files."
        )
    if energy_path is None:
        raise ValueError("energy_path is required when using separate supply MC files")
    if capacity_path is None:
        raise ValueError(
            "capacity_path is required when using separate supply MC files"
        )

    # Check if either path contains "cambium" - if so, treat as Cambium file (backward compatibility)
    if _is_cambium_path(energy_path) or _is_cambium_path(capacity_path):
        # Use the cambium path (prefer energy_path if both contain cambium, otherwise use the one that does)
        cambium_path = energy_path if _is_cambium_path(energy_path) else capacity_path
        log.info(
            "Detected Cambium path in supply MC: %s. Using _load_cambium_marginal_costs for backward compatibility.",
            cambium_path,
        )
        return _load_cambium_marginal_costs(cambium_path, target_year)

    # Both paths are separate files (not Cambium)
    energy_df = _load_supply_energy_mc(energy_path, target_year)
    capacity_df = _load_supply_capacity_mc(capacity_path, target_year)

    # Combine on index (should align perfectly as both are 8760 rows)
    combined = pd.concat([energy_df, capacity_df], axis=1)

    if len(combined) != 8760:
        raise ValueError(f"Combined supply MC has {len(combined)} rows, expected 8760")

    return combined


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


def _fetch_prototype_ids_by_electric_util(
    electric_utility: ElectricUtility, utility_assignment: pl.LazyFrame
) -> list[int]:
    """
    Fetch all building ID's assigned to the given electric utility.

    Args:
        electric_utility: The electric utility to fetch prototype IDs for.
        utility_assignment: The utility assignment LazyFrame.

    Returns:
        A list of building IDs assigned to the given electric utility.
    """
    if "sb.electric_utility" not in utility_assignment.collect_schema().names():
        raise ValueError("sb.electric_utility column not found in utility assignment")
    utility_assignment = utility_assignment.filter(
        pl.col("sb.electric_utility") == electric_utility
    )
    bldg_ids = cast(
        pl.DataFrame,
        utility_assignment.select("bldg_id").collect(),
    )
    if bldg_ids.height == 0:
        raise ValueError(f"No buildings assigned to {electric_utility}")
    return cast(list[int], bldg_ids["bldg_id"].to_list())


def load_dist_and_sub_tx_marginal_costs(
    path: str | Path,
) -> pd.Series:
    """Load *distribution + upstream (Tx/Sub-Tx)* marginal costs and return a tz-aware Series.

    The parquet this function loads is produced by `utils/pre/generate_utility_tx_dx_mc.py`
    and contains:
    - `mc_upstream_per_kwh` (Tx/Sub-Tx component, $/kWh)
    - `mc_dist_per_kwh` (distribution component, $/kWh)
    - `mc_total_per_kwh` (= upstream + distribution, $/kWh)

    We load `mc_total_per_kwh` and pass it to CAIRO as the delivery-side MC trace.
    Bulk transmission is handled separately via `load_bulk_tx_marginal_costs`.
    """
    path_str = str(path)
    if path_str.startswith("s3://"):
        dist_and_sub_tx_mc_scan: pl.LazyFrame = pl.scan_parquet(
            path_str,
            storage_options=get_aws_storage_options(),
        )
    else:
        dist_and_sub_tx_mc_scan = pl.scan_parquet(path_str)
    dist_and_sub_tx_mc_df = cast(pl.DataFrame, dist_and_sub_tx_mc_scan.collect())
    dist_and_sub_tx_marginal_costs = dist_and_sub_tx_mc_df.to_pandas()
    required_cols = {"timestamp", "mc_total_per_kwh"}
    missing_cols = required_cols.difference(dist_and_sub_tx_marginal_costs.columns)
    if missing_cols:
        raise ValueError(
            "Dist+sub-tx marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )
    dist_and_sub_tx_marginal_costs = dist_and_sub_tx_marginal_costs.set_index(
        "timestamp"
    )["mc_total_per_kwh"]
    dist_and_sub_tx_marginal_costs.index = pd.DatetimeIndex(
        dist_and_sub_tx_marginal_costs.index
    ).tz_localize("EST")
    dist_and_sub_tx_marginal_costs.index.name = "time"
    dist_and_sub_tx_marginal_costs.name = "Marginal Dist+Sub-Tx Costs ($/kWh)"
    return dist_and_sub_tx_marginal_costs


def load_bulk_tx_marginal_costs(
    path: str | Path,
) -> pd.Series:
    """Load bulk transmission marginal costs from a parquet path and return a tz-aware Series.

    Reads parquet with columns ``timestamp`` and ``bulk_tx_cost_enduse`` ($/MWh),
    converts to $/kWh (÷ 1000), TZ-localizes to EST, and returns a Series named
    ``"Marginal Bulk Transmission Costs ($/kWh)"``.

    The output mirrors the shape of :func:`load_dist_and_sub_tx_marginal_costs` so the
    caller can sum the two into a single delivery MC trace for CAIRO.

    Args:
        path: Local or S3 path to the bulk transmission MC parquet file.
            Expected schema: ``timestamp`` (datetime), ``bulk_tx_cost_enduse``
            (float, $/MWh), 8760 rows.

    Returns:
        ``pd.Series`` indexed by ``DatetimeIndex`` (EST-localized, name ``"time"``),
        values in $/kWh, series name ``"Marginal Bulk Transmission Costs ($/kWh)"``.

    Raises:
        FileNotFoundError: If the path does not exist.
        ValueError: If required columns are missing, data has nulls, or row count
            is not 8760.
    """
    path_str = str(path)
    if path_str.startswith("s3://"):
        bulk_tx_mc_scan: pl.LazyFrame = pl.scan_parquet(
            path_str,
            storage_options=get_aws_storage_options(),
        )
    else:
        bulk_tx_mc_scan = pl.scan_parquet(path_str)
    bulk_tx_mc_df = cast(pl.DataFrame, bulk_tx_mc_scan.collect())
    bulk_tx_mc_pd = bulk_tx_mc_df.to_pandas()

    required_cols = {"timestamp", "bulk_tx_cost_enduse"}
    missing_cols = required_cols.difference(bulk_tx_mc_pd.columns)
    if missing_cols:
        raise ValueError(
            "Bulk transmission marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )

    if bulk_tx_mc_pd["bulk_tx_cost_enduse"].isna().any():
        raise ValueError(
            "Bulk transmission marginal costs contain null values in bulk_tx_cost_enduse"
        )

    if len(bulk_tx_mc_pd) != 8760:
        log.warning(
            "Bulk transmission MC has %d rows (expected 8760); proceeding anyway.",
            len(bulk_tx_mc_pd),
        )

    bulk_tx_series = bulk_tx_mc_pd.set_index("timestamp")["bulk_tx_cost_enduse"]

    # $/MWh → $/kWh
    bulk_tx_series = bulk_tx_series / 1000.0

    bulk_tx_series.index = pd.DatetimeIndex(bulk_tx_series.index).tz_localize("EST")
    bulk_tx_series.index.name = "time"
    bulk_tx_series.name = "Marginal Bulk Transmission Costs ($/kWh)"

    # Validate all values >= 0
    if (bulk_tx_series < 0).any():
        n_neg = int((bulk_tx_series < 0).sum())
        log.warning(
            "Bulk transmission MC has %d negative values; check input data.", n_neg
        )

    # Log summary for inspection
    annual_sum_kwh = float(bulk_tx_series.sum())
    log.info(
        "Loaded bulk transmission MC: %d rows, annual sum = %.4f $/kW-yr, "
        "avg = %.6f $/kWh, max = %.6f $/kWh",
        len(bulk_tx_series),
        annual_sum_kwh,
        float(bulk_tx_series.mean()),
        float(bulk_tx_series.max()),
    )

    return bulk_tx_series


def _align_mc_to_index(
    mc_series: pd.Series,
    target_index: pd.DatetimeIndex,
    mc_type: str = "MC",
) -> pd.Series:
    """Align a marginal cost Series to a target DatetimeIndex.

    Handles index alignment when MC has a different year or length than the target.
    Preserves values by position when lengths match (common when MC file year differs
    from run year), otherwise reindexes.

    Args:
        mc_series: MC Series with DatetimeIndex to align.
        target_index: Target DatetimeIndex (typically from bulk MC).
        mc_type: Label for logging (e.g., "dist_and_sub_tx", "bulk_tx").

    Returns:
        MC Series aligned to target_index, preserving original name.
    """
    if mc_series.index.equals(target_index):
        return mc_series

    if len(mc_series) == len(target_index):
        # Same length but different timestamps: align by position
        aligned = pd.Series(
            mc_series.values,
            index=target_index,
            name=mc_series.name,
        )
        log.info(
            "Aligned %s MC index to target (mc_year=%s, target_year=%s)",
            mc_type,
            mc_series.index[0].year,
            target_index[0].year,
        )
        return aligned

    # Different lengths: reindex
    aligned = mc_series.reindex(target_index)
    log.info(
        "Reindexed %s MC to target (mc_rows=%s, target_rows=%s)",
        mc_type,
        len(mc_series),
        len(target_index),
    )
    return aligned


def add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
    path_dist_and_sub_tx_mc: str | Path,
    path_bulk_tx_mc: str | Path | None,
    target_index: pd.DatetimeIndex,
) -> pd.Series:
    """Load and combine delivery marginal costs: Bulk Tx + Dist+Sub-Tx.

    Architecture:
    - Delivery MCs = Bulk Tx + Dist+Sub-Tx (both are delivery charges)
    - Supply MCs = Energy + Capacity (bulk supply, handled separately)
    All MCs must share the same DatetimeIndex for CAIRO compatibility.

    This function loads dist+sub-tx MC (required) and bulk transmission MC (optional),
    aligns both to the target_index, and sums them into a single delivery MC Series.

    Args:
        path_dist_and_sub_tx_mc: Path to dist+sub-tx MC parquet file.
        path_bulk_tx_mc: Optional path to bulk transmission MC parquet file.
        target_index: Target DatetimeIndex to align both MCs to (typically from supply MC index
            to ensure all MCs share the same 8760-hour index).

    Returns:
        Combined delivery MC Series (dist+sub-tx + bulk transmission) aligned to target_index,
        named "Marginal Delivery Costs ($/kWh)".

    Raises:
        ValueError: If combined series contains nulls after alignment.
    """
    # Load and align dist+sub-tx MC (distribution + upstream Tx/Sub-Tx)
    dist_and_sub_tx_mc = load_dist_and_sub_tx_marginal_costs(path_dist_and_sub_tx_mc)
    dist_and_sub_tx_mc = _align_mc_to_index(
        dist_and_sub_tx_mc, target_index, "dist_and_sub_tx"
    )

    # Load and align bulk transmission MC if provided, then combine
    # Handle None, empty string, or blank path
    if path_bulk_tx_mc is not None and str(path_bulk_tx_mc).strip():
        bulk_tx_mc = load_bulk_tx_marginal_costs(path_bulk_tx_mc)
        bulk_tx_mc = _align_mc_to_index(bulk_tx_mc, target_index, "bulk_tx")

        # Log pre-merge statistics
        dist_sum = float(dist_and_sub_tx_mc.sum())
        bulk_tx_sum = float(bulk_tx_mc.sum())
        log.info(
            "Pre-merge delivery MC: Dist+Sub-Tx=%.4f $/kW-yr, Bulk Tx=%.4f $/kW-yr",
            dist_sum,
            bulk_tx_sum,
        )

        # Sum bulk transmission into dist+sub-tx (both are delivery charges)
        delivery_mc = dist_and_sub_tx_mc + bulk_tx_mc

        # Validate and log
        if delivery_mc.isna().any():
            raise ValueError(
                "Combined delivery MC (dist+sub-tx + bulk transmission) contains null values"
            )

        log.info(
            "Combined delivery MC: Total=%.4f $/kW-yr (Dist+Sub-Tx=%.4f + Bulk Tx=%.4f)",
            float(delivery_mc.sum()),
            dist_sum,
            bulk_tx_sum,
        )
    else:
        delivery_mc = dist_and_sub_tx_mc

    delivery_mc.name = "Marginal Delivery Costs ($/kWh)"
    return delivery_mc


def extract_tou_period_rates(tou_tariff: dict) -> pd.DataFrame:
    """Extract period-level TOU rates from a URDB-style tariff.

    Args:
        tou_tariff: URDB v7 tariff dictionary with `energyratestructure`.

    Returns:
        DataFrame with columns:
        - `energy_period` (int)
        - `tier` (1-based int)
        - `rate` ($/kWh, including `adj`)
    """
    tariff_item = tou_tariff["items"][0]
    rate_structure = tariff_item["energyratestructure"]
    rows: list[dict[str, object]] = []
    for period_idx, tiers in enumerate(rate_structure):
        for tier_idx, tier_data in enumerate(tiers):
            rate = float(tier_data["rate"]) + float(tier_data.get("adj", 0.0))
            rows.append(
                {
                    "energy_period": period_idx,
                    "tier": tier_idx + 1,
                    "rate": rate,
                }
            )
    return pd.DataFrame(rows)


def assign_hourly_periods(
    hourly_index: pd.DatetimeIndex,
    tou_tariff: dict,
) -> pd.Series:
    """Map hourly timestamps to TOU `energy_period` values.

    Args:
        hourly_index: Hourly DatetimeIndex (typically one full year).
        tou_tariff: URDB v7 tariff dictionary with weekday/weekend schedules.

    Returns:
        Series indexed by `hourly_index` containing integer `energy_period`.
    """
    tariff_item = tou_tariff["items"][0]
    weekday_schedule = np.array(tariff_item["energyweekdayschedule"])
    weekend_schedule = np.array(tariff_item["energyweekendschedule"])

    months = np.asarray(hourly_index.month) - 1  # type: ignore[attr-defined]
    hours = np.asarray(hourly_index.hour)  # type: ignore[attr-defined]
    is_weekday = np.asarray(hourly_index.dayofweek) < 5  # type: ignore[attr-defined]

    periods = np.where(
        is_weekday, weekday_schedule[months, hours], weekend_schedule[months, hours]
    )
    return pd.Series(periods, index=hourly_index, name="energy_period", dtype=int)


def _build_period_consumption(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate baseline consumption by building and tariff period.

    Args:
        hourly_df: DataFrame with `bldg_id`, `energy_period`, `electricity_net`.

    Returns:
        DataFrame with one row per `(bldg_id, energy_period)` and `Q_orig`.
        Missing period combinations are zero-filled for stable downstream math.
    """
    observed_periods = sorted(hourly_df["energy_period"].dropna().unique())
    bldg_ids = pd.Index(hourly_df["bldg_id"].unique(), name="bldg_id")
    scaffold = pd.MultiIndex.from_product(
        [bldg_ids, pd.Index(observed_periods, name="energy_period")],
        names=["bldg_id", "energy_period"],
    )
    return (
        hourly_df.groupby(["bldg_id", "energy_period"])["electricity_net"]
        .sum()
        .reindex(scaffold, fill_value=0.0)
        .rename("Q_orig")
        .reset_index()
    )


def _compute_equivalent_flat_tariff(
    period_consumption: pd.DataFrame,
    period_rate: pd.Series,
) -> float:
    """Compute endogenous equivalent flat rate for one customer-class slice.

    Args:
        period_consumption: Building-period baseline demand with `Q_orig`.
        period_rate: Series mapping `energy_period -> rate`.

    Returns:
        Endogenous flat comparator price for the active customer slice:
        `sum_t(Q_t * P_t) / sum_t(Q_t)`.
    """
    rates = period_rate.rename("rate").reset_index()
    class_period = (
        period_consumption.groupby("energy_period", as_index=False)["Q_orig"].sum()
    ).merge(rates, on="energy_period", how="left")
    total_demand = float(class_period["Q_orig"].sum())
    if total_demand <= 0:
        raise ValueError("Cannot compute equivalent flat tariff with zero demand.")
    return float((class_period["Q_orig"] * class_period["rate"]).sum() / total_demand)


def _build_period_shift_targets(
    period_consumption: pd.DataFrame,
    period_rate: pd.Series,
    demand_elasticity: float,
    equivalent_flat_tariff: float,
    receiver_period: int | None,
) -> pd.DataFrame:
    """Build period-level shift targets under constant elasticity.

    Args:
        period_consumption: Building-period demand with `Q_orig`.
        period_rate: Series mapping `energy_period -> rate`.
        demand_elasticity: Constant demand elasticity parameter.
        equivalent_flat_tariff: Comparator flat rate for elasticity response.
        receiver_period: Optional sink period; if omitted, lowest-rate period used.

    Returns:
        DataFrame with per-building/period target demand and `load_shift`.
    """
    targets = period_consumption.merge(
        period_rate.rename("rate").reset_index(), on="energy_period", how="left"
    )
    targets["Q_target"] = targets["Q_orig"] * (
        (targets["rate"] / equivalent_flat_tariff) ** demand_elasticity
    )
    targets["load_shift"] = targets["Q_target"] - targets["Q_orig"]
    # Zero-sum: receiver period absorbs the negated sum of donor shifts.
    recv_period = (
        int(targets.loc[targets["rate"].idxmin(), "energy_period"])
        if receiver_period is None
        else receiver_period
    )
    donor_shift = (
        targets[targets["energy_period"] != recv_period]
        .groupby("bldg_id")["load_shift"]
        .sum()
    )
    recv_mask = targets["energy_period"] == recv_period
    targets.loc[recv_mask, "load_shift"] = (
        targets.loc[recv_mask, "bldg_id"].map(-donor_shift).fillna(0.0)
    )
    return targets


def _shift_building_hourly_demand(
    bldg_hourly_df: pd.DataFrame,
    period_targets: pd.DataFrame,
    equivalent_flat_tariff: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """CAIRO-style worker: allocate period shifts to hourly rows for one building.

    Args:
        bldg_hourly_df: One building's hourly load rows with `energy_period`.
        period_targets: Building-level period shift targets.
        equivalent_flat_tariff: Comparator flat rate used in elasticity math.

    Returns:
        Tuple of:
        - shifted hourly DataFrame (`shifted_net`, `hourly_shift`)
        - period-level elasticity tracker DataFrame
    """
    shifted = bldg_hourly_df.merge(
        period_targets[["energy_period", "Q_orig", "load_shift", "rate"]],
        on="energy_period",
        how="left",
    )
    shifted["hour_share"] = np.where(
        shifted["Q_orig"].abs() > 0,
        shifted["electricity_net"] / shifted["Q_orig"],
        0.0,
    )
    shifted["hourly_shift"] = shifted["load_shift"].fillna(0.0) * shifted["hour_share"]
    shifted["shifted_net"] = shifted["electricity_net"] + shifted["hourly_shift"]

    tracker = shifted.groupby(["bldg_id", "energy_period"], as_index=False).agg(
        Q_orig=("electricity_net", "sum"),
        Q_new=("shifted_net", "sum"),
        rate=("rate", "first"),
    )
    valid = (
        (tracker["Q_new"] > 0)
        & (tracker["Q_orig"] > 0)
        & (tracker["rate"] != equivalent_flat_tariff)
    )
    tracker["epsilon"] = np.nan
    tracker.loc[valid, "epsilon"] = np.log(
        tracker.loc[valid, "Q_new"] / tracker.loc[valid, "Q_orig"]
    ) / np.log(tracker.loc[valid, "rate"] / equivalent_flat_tariff)
    return shifted, tracker


def process_residential_hourly_demand_response_shift(
    hourly_load_df: pd.DataFrame,
    period_rate: pd.Series,
    demand_elasticity: float,
    equivalent_flat_tariff: float | None = None,
    receiver_period: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """CAIRO-style parent function for demand-response load shifting.

    Args:
        hourly_load_df: Hourly TOU-cohort load with `bldg_id`, `energy_period`,
            and `electricity_net`.
        period_rate: Series mapping `energy_period -> rate`.
        demand_elasticity: Constant demand elasticity parameter.
        equivalent_flat_tariff: Optional comparator flat rate. If omitted,
            computed endogenously from the active slice.
        receiver_period: Optional sink period for zero-sum balancing.

    Returns:
        Tuple of:
        - shifted hourly DataFrame for the slice
        - period-level elasticity tracker DataFrame
    """
    if hourly_load_df.empty:
        return hourly_load_df.copy(), pd.DataFrame()

    period_consumption = _build_period_consumption(hourly_load_df)
    flat_tariff = (
        _compute_equivalent_flat_tariff(period_consumption, period_rate)
        if equivalent_flat_tariff is None
        else equivalent_flat_tariff
    )
    period_targets = _build_period_shift_targets(
        period_consumption=period_consumption,
        period_rate=period_rate,
        demand_elasticity=demand_elasticity,
        equivalent_flat_tariff=flat_tariff,
        receiver_period=receiver_period,
    )

    shifted_chunks: list[pd.DataFrame] = []
    tracker_chunks: list[pd.DataFrame] = []
    for bldg_id, bldg_hourly in hourly_load_df.groupby("bldg_id", sort=False):
        bldg_targets = period_targets[period_targets["bldg_id"] == bldg_id]
        shifted_bldg, tracker_bldg = _shift_building_hourly_demand(
            bldg_hourly_df=bldg_hourly,
            period_targets=bldg_targets,
            equivalent_flat_tariff=flat_tariff,
        )
        shifted_chunks.append(shifted_bldg)
        tracker_chunks.append(tracker_bldg)

    return (
        pd.concat(shifted_chunks, ignore_index=True),
        pd.concat(tracker_chunks, ignore_index=True),
    )


def _infer_season_groups_from_tariff(
    period_map: pd.Series,
) -> list[dict[str, object]]:
    """Infer season groups from month-specific period signatures in the tariff.

    Args:
        period_map: Series indexed by time with integer `energy_period`.

    Returns:
        List of season group dictionaries (`name`, `months`). Returns an empty
        list when tariff structure is effectively full-year.
    """
    month_periods = (
        period_map.to_frame(name="energy_period")
        .assign(month=lambda frame: frame.index.month)
        .groupby("month")["energy_period"]
        .unique()
        .apply(lambda values: tuple(sorted(int(v) for v in values)))
    )
    grouped: dict[tuple[int, ...], list[int]] = {}
    for month, period_tuple in month_periods.items():
        key = cast(tuple[int, ...], period_tuple)
        grouped.setdefault(key, []).append(int(cast(int, month)))
    if len(grouped) <= 1:
        return []

    groups: list[dict[str, object]] = []
    ordered_groups = sorted(grouped.items(), key=lambda item: min(item[1]))
    for idx, (periods, months) in enumerate(ordered_groups):
        if not periods:
            continue
        groups.append(
            {
                "name": f"season_{idx + 1}",
                "months": sorted(months),
            }
        )
    return groups


def apply_runtime_tou_demand_response(
    raw_load_elec: pd.DataFrame,
    tou_bldg_ids: list[int],
    tou_tariff: dict,
    demand_elasticity: float,
    season_specs: list | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply runtime TOU demand response to the assigned TOU customer cohort.

    Args:
        raw_load_elec: Full electric load DataFrame indexed by `(bldg_id, time)`.
        tou_bldg_ids: Building IDs assigned to the TOU tariff.
        tou_tariff: URDB v7 tariff dictionary.
        demand_elasticity: Constant demand elasticity parameter.
        season_specs: Optional season definitions for seasonal slicing.

    Returns:
        Tuple of:
        - full shifted load DataFrame (`raw_load_elec` shape preserved)
        - pivoted elasticity tracker DataFrame by building
    """
    if not tou_bldg_ids:
        return raw_load_elec.copy(), pd.DataFrame()

    # Only TOU-assigned buildings are shifted; others pass through unchanged.
    bldg_level = raw_load_elec.index.get_level_values("bldg_id")
    tou_mask = bldg_level.isin(set(tou_bldg_ids))
    if not tou_mask.any():
        log.warning("No TOU buildings found in load data; skipping demand response.")
        return raw_load_elec.copy(), pd.DataFrame()

    rate_df = extract_tou_period_rates(tou_tariff)
    period_rate = cast(pd.Series, rate_df.groupby("energy_period")["rate"].first())
    time_idx = pd.DatetimeIndex(
        raw_load_elec.index.get_level_values("time").unique().sort_values()
    )
    period_map = assign_hourly_periods(time_idx, tou_tariff)

    tou_df = raw_load_elec.loc[tou_mask, ["electricity_net"]].copy().reset_index()
    period_df = period_map.reset_index()
    period_df.columns = ["time", "energy_period"]
    tou_df = tou_df.merge(period_df, on="time", how="left")
    tou_df["month"] = tou_df["time"].dt.month

    # Shift per-season so energy conservation holds within each slice.
    # Season groups come from explicit specs, tariff-inferred months, or
    # fall back to full-year as a single group.
    shifted_chunks: list[pd.DataFrame] = []
    trackers: list[pd.DataFrame] = []
    season_groups: list[dict[str, object]] = []
    if season_specs:
        for spec in season_specs:
            season_groups.append(
                {
                    "name": str(spec.season.name),
                    "months": list(spec.season.months),
                }
            )
    else:
        # For seasonal+TOU tariffs without explicit derivation specs, infer
        # month groups directly from tariff month->period structure.
        season_groups = _infer_season_groups_from_tariff(period_map)

    if season_groups:
        for season_group in season_groups:
            season_name = str(season_group["name"])
            season_months = set(cast(list[int], season_group["months"]))
            season_df = tou_df[tou_df["month"].isin(season_months)].copy()
            if season_df.empty:
                continue
            season_periods = sorted(season_df["energy_period"].dropna().unique())
            if not season_periods:
                continue
            shifted_season, tracker = process_residential_hourly_demand_response_shift(
                hourly_load_df=season_df,
                period_rate=period_rate,
                demand_elasticity=demand_elasticity,
            )
            tracker["season"] = season_name
            shifted_chunks.append(shifted_season)
            trackers.append(tracker)
    else:
        # Non-seasonal tariff: shift across the full year as one group.
        if not tou_df["energy_period"].dropna().empty:
            shifted_year, tracker = process_residential_hourly_demand_response_shift(
                hourly_load_df=tou_df,
                period_rate=period_rate,
                demand_elasticity=demand_elasticity,
            )
            tracker["season"] = "all_year"
            shifted_chunks.append(shifted_year)
            trackers.append(tracker)

    # Merge shifted TOU rows back; non-TOU buildings are untouched.
    shifted_load_elec = raw_load_elec.copy()
    if shifted_chunks:
        shifted = pd.concat(shifted_chunks, ignore_index=True).set_index(
            ["bldg_id", "time"]
        )
        shifted = shifted.sort_index()
        shifted_load_elec.loc[shifted.index, "electricity_net"] = shifted[
            "shifted_net"
        ].to_numpy()
        if "load_data" in shifted_load_elec.columns:
            shifted_load_elec.loc[shifted.index, "load_data"] = (
                shifted_load_elec.loc[shifted.index, "load_data"]
                + shifted["hourly_shift"].to_numpy()
            )

    if trackers:
        tracker_df = pd.concat(trackers, ignore_index=True)
        tracker_df["period_label"] = tracker_df.apply(
            lambda row: f"{row['season']}_period_{int(row['energy_period'])}", axis=1
        )
        elasticity_tracker = tracker_df.pivot(
            index="bldg_id", columns="period_label", values="epsilon"
        )
    else:
        elasticity_tracker = pd.DataFrame()

    original_total = raw_load_elec.loc[tou_mask, "electricity_net"].sum()
    shifted_total = shifted_load_elec.loc[tou_mask, "electricity_net"].sum()
    log.info(
        "Runtime demand response complete: bldgs=%d, elasticity=%.3f, original=%.0f, shifted=%.0f, diff=%.2f",
        len(tou_bldg_ids),
        demand_elasticity,
        original_total,
        shifted_total,
        shifted_total - original_total,
    )
    return shifted_load_elec, elasticity_tracker

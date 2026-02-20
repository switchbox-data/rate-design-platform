"""Utility functions for Cairo-related operations."""

from __future__ import annotations

import io
import logging
from copy import deepcopy
from pathlib import Path, PurePath
from typing import cast

import pandas as pd
import polars as pl
from cairo.rates_tool import config
from cairo.rates_tool.loads import __timeshift__
from cloudpathlib import S3Path

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

CambiumPathLike = str | Path | S3Path
_CAIRO_MULTITARIFF_WORKAROUND_APPLIED = False
log = logging.getLogger("rates_analysis").getChild("utils.cairo")


def enable_cairo_multitariff_mapping_workaround() -> None:
    """Patch CAIRO to support multi-tariff mappings with preinitialized tariff dicts.

    CAIRO currently re-initializes tariffs from disk when more than one tariff key is
    present in a map, even when a ready-to-use `tariff_base` dict is already passed.
    In some code paths (notably gas post-processing), this drops explicit path mapping
    and fails with "tariff path build failed".

    This workaround reuses the provided `tariff_base` values directly when:
    - `tariff_base` is a dict with more than one key, and
    - keys referenced by `tariff_map` are all present in `tariff_base`.
    """

    global _CAIRO_MULTITARIFF_WORKAROUND_APPLIED
    if _CAIRO_MULTITARIFF_WORKAROUND_APPLIED:
        return

    from cairo.rates_tool import tariffs as tariff_funcs

    original_load_base_tariffs = tariff_funcs._load_base_tariffs

    def _patched_load_base_tariffs(
        tariff_base: dict[str, dict],
        tariff_map: str | PurePath | pd.DataFrame | None,
        prototype_ids: list[int] | None = None,
    ) -> tuple[dict[int, str], dict[str, dict]]:
        should_try_workaround = (
            isinstance(tariff_base, dict)
            and len(tariff_base.keys()) > 1
            and tariff_map is not None
        )
        if not should_try_workaround:
            return original_load_base_tariffs(tariff_base, tariff_map, prototype_ids)

        if isinstance(tariff_map, (str, PurePath)):
            tariff_map_df = tariff_funcs.__load_tariff_maps__(tariff_map)
        else:
            if not isinstance(tariff_map, pd.DataFrame):
                return original_load_base_tariffs(
                    tariff_base, tariff_map, prototype_ids
                )
            tariff_map_df = tariff_map
            if "bldg_id" in tariff_map_df.columns:
                tariff_map_df = tariff_map_df.set_index("bldg_id")
            elif tariff_map_df.index.name != "bldg_id":
                return original_load_base_tariffs(
                    tariff_base, tariff_map, prototype_ids
                )

        if "tariff_key" not in tariff_map_df.columns:
            return original_load_base_tariffs(tariff_base, tariff_map, prototype_ids)

        prototype_tariff_map = tariff_map_df["tariff_key"].to_dict()
        unique_tariffs = set(prototype_tariff_map.values())
        if not unique_tariffs.issubset(set(tariff_base.keys())):
            return original_load_base_tariffs(tariff_base, tariff_map, prototype_ids)

        # Reuse already initialized tariff dicts instead of reloading by default path.
        tariff_dict = {k: deepcopy(tariff_base[k]) for k in unique_tariffs}
        return prototype_tariff_map, tariff_dict

    tariff_funcs._load_base_tariffs = _patched_load_base_tariffs
    _CAIRO_MULTITARIFF_WORKAROUND_APPLIED = True
    log.info(".... Enabled CAIRO multi-tariff mapping workaround")


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


def load_distribution_marginal_costs(
    path: str | Path,
) -> pd.Series:
    """Load distribution marginal costs from a parquet path and return a tz-aware Series."""
    path_str = str(path)
    if path_str.startswith("s3://"):
        distribution_mc_scan: pl.LazyFrame = pl.scan_parquet(
            path_str,
            storage_options=get_aws_storage_options(),
        )
    else:
        distribution_mc_scan = pl.scan_parquet(path_str)
    distribution_mc_df = cast(pl.DataFrame, distribution_mc_scan.collect())
    distribution_marginal_costs = distribution_mc_df.to_pandas()
    required_cols = {"timestamp", "mc_total_per_kwh"}
    missing_cols = required_cols.difference(distribution_marginal_costs.columns)
    if missing_cols:
        raise ValueError(
            "Distribution marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )
    distribution_marginal_costs = distribution_marginal_costs.set_index("timestamp")[
        "mc_total_per_kwh"
    ]
    distribution_marginal_costs.index = pd.DatetimeIndex(
        distribution_marginal_costs.index
    ).tz_localize("EST")
    distribution_marginal_costs.index.name = "time"
    distribution_marginal_costs.name = "Marginal Distribution Costs ($/kWh)"
    return distribution_marginal_costs

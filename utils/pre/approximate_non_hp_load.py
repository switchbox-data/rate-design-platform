import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import cast
import threading

import numpy as np
import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


def _parquet_storage_options(load_curve_hourly_dir: S3Path | Path) -> dict:
    """Return storage_options for scan_parquet/sink_parquet: use AWS options for S3, none for local Path."""
    if isinstance(load_curve_hourly_dir, S3Path):
        return STORAGE_OPTIONS
    return {}


BLDG_TYPE_COLUMN = "in.geometry_building_type_height"
STORIES_COLUMN = "in.geometry_story_bin"
HEATING_TYPE_COLUMN = "in.hvac_heating_type"
WEATHER_FILE_CITY_COLUMN = "in.weather_file_city"
HEATS_WITH_COLUMNS = (
    "heats_with_electricity",
    "heats_with_natgas",
    "heats_with_oil",
    "heats_with_propane",
)

# Columns to change in metadata
UPGRADE_COOLING_EFFICIENCY_COLUMN = "upgrade.hvac_cooling_efficiency"
UPGRADE_PARTIAL_CONDITIONING_COLUMN = "upgrade.hvac_cooling_partial_space_conditioning"
UPGRADE_HEATING_EFFICIENCY_COLUMN = "upgrade.hvac_heating_efficiency"
POSTPROCESS_HAS_HP_COLUMN = "postprocess_group.has_hp"
POSTPROCESS_HEATING_TYPE_COLUMN = "postprocess_group.heating_type"
HAS_NATGAS_CONNECTION_COLUMN = "has_natgas_connection"


# Columns in load_curve_hourly parquet.
TIMESTAMP_COLUMN = (
    "timestamp"  # chronological order; sort by this after joins to restore row order
)
COOLING_LOAD_COLUMN = "out.load.cooling.energy_delivered.kbtu"
HEATING_LOAD_COLUMN = "out.load.heating.energy_delivered.kbtu"

HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS = (
    "out.electricity.heating.energy_consumption",
    "out.electricity.heating.energy_consumption_intensity",
    "out.electricity.heating_fans_pumps.energy_consumption",
    "out.electricity.heating_fans_pumps.energy_consumption_intensity",
    "out.electricity.heating_hp_bkup.energy_consumption",
    "out.electricity.heating_hp_bkup.energy_consumption_intensity",
    "out.electricity.heating_hp_bkup_fa.energy_consumption",
    "out.electricity.heating_hp_bkup_fa.energy_consumption_intensity",
)
COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS = (
    "out.electricity.cooling.energy_consumption",
    "out.electricity.cooling.energy_consumption_intensity",
    "out.electricity.cooling_fans_pumps.energy_consumption",
    "out.electricity.cooling_fans_pumps.energy_consumption_intensity",
)
TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS = (
    "out.electricity.total.energy_consumption",
    "out.electricity.total.energy_consumption_intensity",
)

# Heating/Cooling energy consumption columns for all fuels.
HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS = (
    "out.natural_gas.heating.energy_consumption",
    "out.natural_gas.heating.energy_consumption_intensity",
    "out.natural_gas.heating_hp_bkup.energy_consumption",
    "out.natural_gas.heating_hp_bkup.energy_consumption_intensity",
)
HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS = (
    "out.fuel_oil.heating.energy_consumption",
    "out.fuel_oil.heating.energy_consumption_intensity",
    "out.fuel_oil.heating_hp_bkup.energy_consumption",
    "out.fuel_oil.heating_hp_bkup.energy_consumption_intensity",
)
HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS = (
    "out.propane.heating.energy_consumption",
    "out.propane.heating.energy_consumption_intensity",
    "out.propane.heating_hp_bkup.energy_consumption",
    "out.propane.heating_hp_bkup.energy_consumption_intensity",
)

# Total energy consumption columns for all fuels.
TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS = (
    "out.natural_gas.total.energy_consumption",
    "out.natural_gas.total.energy_consumption_intensity",
)
TOTAL_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS = (
    "out.fuel_oil.total.energy_consumption",
    "out.fuel_oil.total.energy_consumption_intensity",
)

TOTAL_ENERGY_CONSUMPTION_PROPANE_COLUMNS = (
    "out.propane.total.energy_consumption",
    "out.propane.total.energy_consumption_intensity",
)


def _identify_non_hp_mf_highrise(metadata: pl.LazyFrame) -> pl.LazyFrame:
    is_non_hp_mf_highrise = (
        ~pl.col(POSTPROCESS_HAS_HP_COLUMN)
        & pl.col(BLDG_TYPE_COLUMN).str.contains("Multifamily", literal=True)
        & pl.col(STORIES_COLUMN).str.contains("8+", literal=True)
    )
    return metadata.filter(is_non_hp_mf_highrise).select(
        "bldg_id", WEATHER_FILE_CITY_COLUMN
    )


def _identify_other_fuel_types(metadata: pl.LazyFrame) -> pl.LazyFrame:
    is_other_fuel_types = pl.col(POSTPROCESS_HAS_HP_COLUMN) & pl.all_horizontal(
        pl.col(c).eq(False) for c in HEATS_WITH_COLUMNS
    )
    return metadata.filter(is_other_fuel_types).select(
        "bldg_id", WEATHER_FILE_CITY_COLUMN
    )


def group_by_weather_station_id(metadata: pl.LazyFrame) -> dict[str, list[int]]:
    """
    Group non-HP MF highrise bldg_ids by weather_station_id.
    Single lazy pipeline: semi-join metadata to restrict to those bldg_ids, then group_by/agg; one collect.
    """
    unique_df = cast(
        pl.DataFrame,
        metadata.select(WEATHER_FILE_CITY_COLUMN).unique().collect(),
    )
    unique_weather_station_ids = unique_df[WEATHER_FILE_CITY_COLUMN].to_list()
    return {
        weather_station_id: cast(
            pl.DataFrame,
            metadata.filter(pl.col(WEATHER_FILE_CITY_COLUMN) == weather_station_id)
            .select("bldg_id")
            .collect(),
        )["bldg_id"].to_list()
        for weather_station_id in unique_weather_station_ids
    }


def _load_one_total_building_load_curve(
    load_curve_hourly_dir: S3Path | Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, compute total building load (heating + cooling); returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    opts = _parquet_storage_options(load_curve_hourly_dir)
    try:
        lf = pl.scan_parquet(str(path), storage_options=opts)
    except Exception:
        raise ValueError(
            f"Failed to load load curve for bldg_id: {bldg_id} from {path}"
        )
    schema = lf.collect_schema()
    if (
        HEATING_LOAD_COLUMN not in schema.names()
        or COOLING_LOAD_COLUMN not in schema.names()
    ):
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing required columns: {HEATING_LOAD_COLUMN}, {COOLING_LOAD_COLUMN}"
        )
    df = cast(
        pl.DataFrame,
        lf.with_columns(
            (pl.col(HEATING_LOAD_COLUMN) + pl.col(COOLING_LOAD_COLUMN)).alias("_summed")
        )
        .select("_summed")
        .collect(),
    )
    vec = df["_summed"].to_numpy().astype(np.float64, copy=False)
    if len(vec) != 8760:
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} has incorrect length: {len(vec)}"
        )
    return (bldg_id, vec)


def _load_one_total_heating_cooling_energy_consumption_curve(
    load_curve_hourly_dir: S3Path | Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, compute summed heating + cooling energy consumption columns; returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    opts = _parquet_storage_options(load_curve_hourly_dir)
    try:
        lf = pl.scan_parquet(str(path), storage_options=opts)
    except Exception:
        raise ValueError(
            f"Failed to load load curve for bldg_id: {bldg_id} from {path}"
        )
    schema = lf.collect_schema()
    if any(
        col not in schema.names()
        for col in HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
    ) or any(
        col not in schema.names()
        for col in COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
    ):
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing required columns: {HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS} {COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS}"
        )
    heating_consumption_cols = [
        c
        for c in HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
        if "consumption_intensity" not in c
    ]
    cooling_consumption_cols = [
        c
        for c in COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
        if "consumption_intensity" not in c
    ]
    df = cast(
        pl.DataFrame,
        lf.with_columns(
            pl.sum_horizontal([pl.col(c) for c in heating_consumption_cols]).alias(
                "_heat"
            ),
            pl.sum_horizontal([pl.col(c) for c in cooling_consumption_cols]).alias(
                "_cool"
            ),
        )
        .with_columns((pl.col("_heat") + pl.col("_cool")).alias("_summed"))
        .select("_summed")
        .collect(),
    )
    vec = df["_summed"].to_numpy().astype(np.float64, copy=False)
    if len(vec) != 8760:
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} has incorrect length: {len(vec)}"
        )
    return (bldg_id, vec)


def _load_one_heating_building_load_curve(
    load_curve_hourly_dir: S3Path | Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, heating load column only; returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    opts = _parquet_storage_options(load_curve_hourly_dir)
    try:
        lf = pl.scan_parquet(str(path), storage_options=opts)
    except Exception:
        raise ValueError(
            f"Failed to load load curve for bldg_id: {bldg_id} from {path}"
        )
    schema = lf.collect_schema()
    if HEATING_LOAD_COLUMN not in schema.names():
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing column: {HEATING_LOAD_COLUMN}"
        )
    df = cast(
        pl.DataFrame,
        lf.select(pl.col(HEATING_LOAD_COLUMN).alias("_summed")).collect(),
    )
    vec = df["_summed"].to_numpy().astype(np.float64, copy=False)
    if len(vec) != 8760:
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} has incorrect length: {len(vec)}"
        )
    return (bldg_id, vec)


def _load_one_cooling_building_load_curve(
    load_curve_hourly_dir: S3Path | Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, cooling load column only; returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    opts = _parquet_storage_options(load_curve_hourly_dir)
    try:
        lf = pl.scan_parquet(str(path), storage_options=opts)
    except Exception:
        raise ValueError(
            f"Failed to load load curve for bldg_id: {bldg_id} from {path}"
        )
    schema = lf.collect_schema()
    if COOLING_LOAD_COLUMN not in schema.names():
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing column: {COOLING_LOAD_COLUMN}"
        )
    df = cast(
        pl.DataFrame,
        lf.select(pl.col(COOLING_LOAD_COLUMN).alias("_summed")).collect(),
    )
    vec = df["_summed"].to_numpy().astype(np.float64, copy=False)
    if len(vec) != 8760:
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} has incorrect length: {len(vec)}"
        )
    return (bldg_id, vec)


def _load_one_heating_energy_consumption_curve(
    load_curve_hourly_dir: S3Path | Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, heating energy consumption columns only (summed); returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    opts = _parquet_storage_options(load_curve_hourly_dir)
    try:
        lf = pl.scan_parquet(str(path), storage_options=opts)
    except Exception:
        raise ValueError(
            f"Failed to load load curve for bldg_id: {bldg_id} from {path}"
        )
    schema = lf.collect_schema()
    if any(
        col not in schema.names()
        for col in HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
    ):
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing required columns: {HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS}"
        )
    heating_cols = [
        c
        for c in HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
        if "consumption_intensity" not in c
    ]
    df = cast(
        pl.DataFrame,
        lf.with_columns(
            pl.sum_horizontal([pl.col(c) for c in heating_cols]).alias("_summed")
        )
        .select("_summed")
        .collect(),
    )
    vec = df["_summed"].to_numpy().astype(np.float64, copy=False)
    if len(vec) != 8760:
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} has incorrect length: {len(vec)}"
        )
    return (bldg_id, vec)


def _load_one_cooling_energy_consumption_curve(
    load_curve_hourly_dir: S3Path | Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, cooling energy consumption columns only (summed); returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    opts = _parquet_storage_options(load_curve_hourly_dir)
    try:
        lf = pl.scan_parquet(str(path), storage_options=opts)
    except Exception:
        raise ValueError(
            f"Failed to load load curve for bldg_id: {bldg_id} from {path}"
        )
    schema = lf.collect_schema()
    if any(
        col not in schema.names()
        for col in COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
    ):
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing required columns: {COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS}"
        )
    cooling_cols = [
        c
        for c in COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS
        if "consumption_intensity" not in c
    ]
    df = cast(
        pl.DataFrame,
        lf.with_columns(
            pl.sum_horizontal([pl.col(c) for c in cooling_cols]).alias("_summed")
        )
        .select("_summed")
        .collect(),
    )
    vec = df["_summed"].to_numpy().astype(np.float64, copy=False)
    if len(vec) != 8760:
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} has incorrect length: {len(vec)}"
        )
    return (bldg_id, vec)


def _load_all_total_load_curves_for_bldg_ids(
    load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    bldg_ids: list[int],
    max_workers: int = 256,
) -> dict[int, np.ndarray]:
    """Load total (heating + cooling) load curves for the given bldg_ids. Returns bldg_id -> 8760-point array."""
    bldg_id_to_load_curve: dict[int, np.ndarray] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _load_one_total_building_load_curve,
                load_curve_hourly_dir,
                bldg_id,
                upgrade_id,
            ): bldg_id
            for bldg_id in bldg_ids
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                bldg_id, vec = result
                bldg_id_to_load_curve[bldg_id] = vec
    return bldg_id_to_load_curve


def _load_all_heating_load_curves_for_bldg_ids(
    load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    bldg_ids: list[int],
    max_workers: int = 256,
) -> dict[int, np.ndarray]:
    """Load heating-only load curves for the given bldg_ids. Returns bldg_id -> 8760-point array."""
    bldg_id_to_load_curve: dict[int, np.ndarray] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _load_one_heating_building_load_curve,
                load_curve_hourly_dir,
                bldg_id,
                upgrade_id,
            ): bldg_id
            for bldg_id in bldg_ids
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                bldg_id, vec = result
                bldg_id_to_load_curve[bldg_id] = vec
    return bldg_id_to_load_curve


def _rmse_8760(x: np.ndarray, y: np.ndarray, smooth: bool = False) -> float:
    """RMSE between two 8760-point load curves: sqrt(mean((x - y)^2)).
    If smooth is True, apply a 3-point moving average to both series first.
    """
    if smooth:
        kernel = np.ones(3) / 3
        x = np.convolve(x, kernel, mode="same")
        y = np.convolve(y, kernel, mode="same")
    return float(np.sqrt(np.mean((x - y) ** 2)))


def _find_nearest_neighbors(
    metadata: pl.LazyFrame,
    non_hp_bldg_metadata: pl.LazyFrame,
    load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    *,
    k: int = 5,
    max_workers_load_curves: int = 256,
    max_workers_neighbors: int = 256,
    include_cooling: bool = False,
) -> dict[int, list[tuple[int, float]]]:
    """For each non-HP MF highrise bldg, find k nearest same-weather bldgs by lowest RMSE on load curves.
    Returns non_hp_bldg_id -> [(neighbor_bldg_id, rmse), ...].
    """
    weather_station_bldg_id_map_non_hp = group_by_weather_station_id(
        non_hp_bldg_metadata
    )
    weather_station_bldg_id_map_total = group_by_weather_station_id(metadata)

    k_nearest_bldg_id_map: dict[int, list[tuple[int, float]]] = {}
    for i, (weather_station, station_bldg_ids) in enumerate(
        weather_station_bldg_id_map_non_hp.items()
    ):
        print(
            f"Processing weather station {i + 1} of {len(weather_station_bldg_id_map_non_hp)}: {weather_station}"
        )
        k_nearest_bldg_id_map.update({bldg_id: [] for bldg_id in station_bldg_ids})
        # Neighbors = all bldgs at this station except the non-HP ones we're finding neighbors for.
        neighbor_bldg_ids = [
            x
            for x in weather_station_bldg_id_map_total[weather_station]
            if x not in station_bldg_ids
        ]
        # If include_cooling, fit based on total (heating+cooling) load curves; otherwise, fit based on heating load curves.
        if include_cooling:
            non_hp_load_curves = _load_all_total_load_curves_for_bldg_ids(
                load_curve_hourly_dir,
                upgrade_id,
                station_bldg_ids,
                max_workers=max_workers_load_curves,
            )
        else:
            non_hp_load_curves = _load_all_heating_load_curves_for_bldg_ids(
                load_curve_hourly_dir,
                upgrade_id,
                station_bldg_ids,
                max_workers=max_workers_load_curves,
            )
        # Download load curves for each neighbor (in parallel).
        with ThreadPoolExecutor(max_workers=max_workers_neighbors) as executor:
            # If include_cooling, fit based on total load curves; otherwise, fit based on heating load curves.
            if include_cooling:
                futures = {
                    executor.submit(
                        _load_one_total_building_load_curve,
                        load_curve_hourly_dir,
                        bldg_id,
                        upgrade_id,
                    ): bldg_id
                    for bldg_id in neighbor_bldg_ids
                }
            else:
                futures = {
                    executor.submit(
                        _load_one_heating_building_load_curve,
                        load_curve_hourly_dir,
                        bldg_id,
                        upgrade_id,
                    ): bldg_id
                    for bldg_id in neighbor_bldg_ids
                }
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                if result is None:
                    continue
                neighbor_bldg_id, neighbor_load_curve_vec = result
                # For each non-HP station bldg, compute RMSE to each neighbor; iteratively keep only top k (lowest RMSE).
                for station_bldg_id in station_bldg_ids:
                    station_bldg_load_curve_vec = non_hp_load_curves[station_bldg_id]
                    rmse = _rmse_8760(
                        station_bldg_load_curve_vec, neighbor_load_curve_vec
                    )
                    if len(k_nearest_bldg_id_map[station_bldg_id]) < k:
                        k_nearest_bldg_id_map[station_bldg_id].append(
                            (neighbor_bldg_id, rmse)
                        )
                        k_nearest_bldg_id_map[station_bldg_id].sort(key=lambda p: p[1])
                    else:
                        if rmse < k_nearest_bldg_id_map[station_bldg_id][-1][1]:
                            k_nearest_bldg_id_map[station_bldg_id][-1] = (
                                neighbor_bldg_id,
                                rmse,
                            )
                            k_nearest_bldg_id_map[station_bldg_id].sort(
                                key=lambda p: p[1]
                            )
    return k_nearest_bldg_id_map


def _replace_electricity_columns(
    original_load_curve_hourly: pl.LazyFrame,
    neighbors_load_curve_hourly: list[pl.LazyFrame],
) -> pl.LazyFrame:
    """Replace heating and cooling electricity columns with neighbor averages and adjust total electricity columns accordingly."""
    heating_cols = list(HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS)
    cooling_cols = list(COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS)
    hc_cols = heating_cols + cooling_cols
    n_neighbors = len(neighbors_load_curve_hourly)

    # Join each neighbor on timestamp so row order aligns by time
    out = original_load_curve_hourly
    for i, neighbor in enumerate(neighbors_load_curve_hourly):
        neighbor_select = neighbor.select(
            [pl.col(TIMESTAMP_COLUMN)]
            + [pl.col(c).alias(f"_n{i}_{c}") for c in hc_cols]
        )
        out = out.join(neighbor_select, on=TIMESTAMP_COLUMN, how="left")

    # Average each heating/cooling column: (_n0_c + _n1_c + ... + _n{k-1}_c) / k
    def avg_expr(col: str) -> pl.Expr:
        total = pl.col(f"_n0_{col}")
        for j in range(1, n_neighbors):
            total = total + pl.col(f"_n{j}_{col}")
        return total / n_neighbors

    replace_cols: list[pl.Expr] = [avg_expr(c).alias(c) for c in hc_cols]

    # Column sets for total adjustment
    heating_consumption = [c for c in heating_cols if "consumption_intensity" not in c]
    cooling_consumption = [c for c in cooling_cols if "consumption_intensity" not in c]
    heating_intensity = [c for c in heating_cols if "consumption_intensity" in c]
    cooling_intensity = [c for c in cooling_cols if "consumption_intensity" in c]

    # Original sums (from original frame, still in out)
    orig_heating_consumption = pl.col(heating_consumption[0])
    for c in heating_consumption[1:]:
        orig_heating_consumption = orig_heating_consumption + pl.col(c)
    orig_cooling_consumption = pl.col(cooling_consumption[0])
    for c in cooling_consumption[1:]:
        orig_cooling_consumption = orig_cooling_consumption + pl.col(c)
    orig_heating_intensity = pl.col(heating_intensity[0])
    for c in heating_intensity[1:]:
        orig_heating_intensity = orig_heating_intensity + pl.col(c)
    orig_cooling_intensity = pl.col(cooling_intensity[0])
    for c in cooling_intensity[1:]:
        orig_cooling_intensity = orig_cooling_intensity + pl.col(c)

    # Averaged sums: (1/n) * sum over neighbors of (sum of cols)
    def avg_sum(cols: list[str]) -> pl.Expr:
        neighbor_sums = pl.col(f"_n0_{cols[0]}")
        for c in cols[1:]:
            neighbor_sums = neighbor_sums + pl.col(f"_n0_{c}")
        for i in range(1, n_neighbors):
            s = pl.col(f"_n{i}_{cols[0]}")
            for c in cols[1:]:
                s = s + pl.col(f"_n{i}_{c}")
            neighbor_sums = neighbor_sums + s
        return neighbor_sums / n_neighbors

    avg_heating_consumption = avg_sum(heating_consumption)
    avg_cooling_consumption = avg_sum(cooling_consumption)
    avg_heating_intensity = avg_sum(heating_intensity)
    avg_cooling_intensity = avg_sum(cooling_intensity)

    total_consumption_col = TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[0]
    total_intensity_col = TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[1]
    new_total_consumption = (
        pl.col(total_consumption_col)
        - orig_heating_consumption
        - orig_cooling_consumption
        + avg_heating_consumption
        + avg_cooling_consumption
    )
    new_total_intensity = (
        pl.col(total_intensity_col)
        - orig_heating_intensity
        - orig_cooling_intensity
        + avg_heating_intensity
        + avg_cooling_intensity
    )
    replace_cols.extend(
        [
            new_total_consumption.alias(total_consumption_col),
            new_total_intensity.alias(total_intensity_col),
        ]
    )

    drop_cols = [f"_n{i}_{c}" for i in range(n_neighbors) for c in hc_cols]
    return out.with_columns(replace_cols).drop(drop_cols)


def _replace_heating_cooling_load_columns(
    original_load_curve_hourly: pl.LazyFrame,
    neighbors_load_curve_hourly: list[pl.LazyFrame],
) -> pl.LazyFrame:
    """Replace heating and cooling load columns with neighbor averages."""
    hc_cols = [HEATING_LOAD_COLUMN, COOLING_LOAD_COLUMN]
    n_neighbors = len(neighbors_load_curve_hourly)

    out = original_load_curve_hourly
    for i, neighbor in enumerate(neighbors_load_curve_hourly):
        neighbor_select = neighbor.select(
            [pl.col(TIMESTAMP_COLUMN)]
            + [pl.col(c).alias(f"_n{i}_{c}") for c in hc_cols]
        )
        out = out.join(neighbor_select, on=TIMESTAMP_COLUMN, how="left")

    def avg_expr(col: str) -> pl.Expr:
        total = pl.col(f"_n0_{col}")
        for j in range(1, n_neighbors):
            total = total + pl.col(f"_n{j}_{col}")
        return total / n_neighbors

    replace_cols = [avg_expr(c).alias(c) for c in hc_cols]
    drop_cols = [f"_n{i}_{c}" for i in range(n_neighbors) for c in hc_cols]
    return out.with_columns(replace_cols).drop(drop_cols)


def _replace_natural_gas_columns(
    original_load_curve_hourly: pl.LazyFrame,
    neighbors_load_curve_hourly: list[pl.LazyFrame],
) -> tuple[pl.LazyFrame, bool]:
    """Replace natural gas heating columns with neighbor averages and adjust total natural gas columns accordingly."""
    heating_cols = list(HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS)
    n_neighbors = len(neighbors_load_curve_hourly)

    out = original_load_curve_hourly
    for i, neighbor in enumerate(neighbors_load_curve_hourly):
        neighbor_select = neighbor.select(
            [pl.col(TIMESTAMP_COLUMN)]
            + [pl.col(c).alias(f"_n{i}_{c}") for c in heating_cols]
        )
        out = out.join(neighbor_select, on=TIMESTAMP_COLUMN, how="left")

    def avg_expr(col: str) -> pl.Expr:
        total = pl.col(f"_n0_{col}")
        for j in range(1, n_neighbors):
            total = total + pl.col(f"_n{j}_{col}")
        return total / n_neighbors

    replace_cols: list[pl.Expr] = [avg_expr(c).alias(c) for c in heating_cols]

    heating_consumption = [c for c in heating_cols if "consumption_intensity" not in c]
    heating_intensity = [c for c in heating_cols if "consumption_intensity" in c]

    orig_heating_consumption = pl.col(heating_consumption[0])
    for c in heating_consumption[1:]:
        orig_heating_consumption = orig_heating_consumption + pl.col(c)
    orig_heating_intensity = pl.col(heating_intensity[0])
    for c in heating_intensity[1:]:
        orig_heating_intensity = orig_heating_intensity + pl.col(c)

    def avg_sum(cols: list[str]) -> pl.Expr:
        neighbor_sums = pl.col(f"_n0_{cols[0]}")
        for c in cols[1:]:
            neighbor_sums = neighbor_sums + pl.col(f"_n0_{c}")
        for i in range(1, n_neighbors):
            s = pl.col(f"_n{i}_{cols[0]}")
            for c in cols[1:]:
                s = s + pl.col(f"_n{i}_{c}")
            neighbor_sums = neighbor_sums + s
        return neighbor_sums / n_neighbors

    avg_heating_consumption = avg_sum(heating_consumption)
    avg_heating_intensity = avg_sum(heating_intensity)

    total_consumption_col = TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[0]
    total_intensity_col = TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[1]
    new_total_consumption = (
        pl.col(total_consumption_col)
        - orig_heating_consumption
        + avg_heating_consumption
    )
    new_total_intensity = (
        pl.col(total_intensity_col) - orig_heating_intensity + avg_heating_intensity
    )
    replace_cols.extend(
        [
            new_total_consumption.alias(total_consumption_col),
            new_total_intensity.alias(total_intensity_col),
        ]
    )

    drop_cols = [f"_n{i}_{c}" for i in range(n_neighbors) for c in heating_cols]
    total_sum_df = cast(pl.DataFrame, out.select(new_total_consumption.sum()).collect())
    total_sum = float(total_sum_df.to_series().item())
    if np.isclose(total_sum, 0.0, atol=1e-6):
        uses_natural_gas = False
    else:
        uses_natural_gas = True
    return out.with_columns(replace_cols).drop(drop_cols), uses_natural_gas


def _replace_fuel_oil_columns(
    original_load_curve_hourly: pl.LazyFrame,
    neighbors_load_curve_hourly: list[pl.LazyFrame],
) -> pl.LazyFrame:
    """Replace fuel oil heating columns with neighbor averages and adjust total fuel oil columns accordingly."""
    heating_cols = list(HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS)
    n_neighbors = len(neighbors_load_curve_hourly)

    out = original_load_curve_hourly
    for i, neighbor in enumerate(neighbors_load_curve_hourly):
        neighbor_select = neighbor.select(
            [pl.col(TIMESTAMP_COLUMN)]
            + [pl.col(c).alias(f"_n{i}_{c}") for c in heating_cols]
        )
        out = out.join(neighbor_select, on=TIMESTAMP_COLUMN, how="left")

    def avg_expr(col: str) -> pl.Expr:
        total = pl.col(f"_n0_{col}")
        for j in range(1, n_neighbors):
            total = total + pl.col(f"_n{j}_{col}")
        return total / n_neighbors

    replace_cols: list[pl.Expr] = [avg_expr(c).alias(c) for c in heating_cols]

    heating_consumption = [c for c in heating_cols if "consumption_intensity" not in c]
    heating_intensity = [c for c in heating_cols if "consumption_intensity" in c]

    orig_heating_consumption = pl.col(heating_consumption[0])
    for c in heating_consumption[1:]:
        orig_heating_consumption = orig_heating_consumption + pl.col(c)
    orig_heating_intensity = pl.col(heating_intensity[0])
    for c in heating_intensity[1:]:
        orig_heating_intensity = orig_heating_intensity + pl.col(c)

    def avg_sum(cols: list[str]) -> pl.Expr:
        neighbor_sums = pl.col(f"_n0_{cols[0]}")
        for c in cols[1:]:
            neighbor_sums = neighbor_sums + pl.col(f"_n0_{c}")
        for i in range(1, n_neighbors):
            s = pl.col(f"_n{i}_{cols[0]}")
            for c in cols[1:]:
                s = s + pl.col(f"_n{i}_{c}")
            neighbor_sums = neighbor_sums + s
        return neighbor_sums / n_neighbors

    avg_heating_consumption = avg_sum(heating_consumption)
    avg_heating_intensity = avg_sum(heating_intensity)

    total_consumption_col = TOTAL_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[0]
    total_intensity_col = TOTAL_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[1]
    new_total_consumption = (
        pl.col(total_consumption_col)
        - orig_heating_consumption
        + avg_heating_consumption
    )
    new_total_intensity = (
        pl.col(total_intensity_col) - orig_heating_intensity + avg_heating_intensity
    )
    replace_cols.extend(
        [
            new_total_consumption.alias(total_consumption_col),
            new_total_intensity.alias(total_intensity_col),
        ]
    )

    drop_cols = [f"_n{i}_{c}" for i in range(n_neighbors) for c in heating_cols]
    return out.with_columns(replace_cols).drop(drop_cols)


def _replace_propane_columns(
    original_load_curve_hourly: pl.LazyFrame,
    neighbors_load_curve_hourly: list[pl.LazyFrame],
) -> pl.LazyFrame:
    """Replace propane heating columns with neighbor averages and adjust total propane columns accordingly."""
    heating_cols = list(HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS)
    n_neighbors = len(neighbors_load_curve_hourly)

    out = original_load_curve_hourly
    for i, neighbor in enumerate(neighbors_load_curve_hourly):
        neighbor_select = neighbor.select(
            [pl.col(TIMESTAMP_COLUMN)]
            + [pl.col(c).alias(f"_n{i}_{c}") for c in heating_cols]
        )
        out = out.join(neighbor_select, on=TIMESTAMP_COLUMN, how="left")

    def avg_expr(col: str) -> pl.Expr:
        total = pl.col(f"_n0_{col}")
        for j in range(1, n_neighbors):
            total = total + pl.col(f"_n{j}_{col}")
        return total / n_neighbors

    replace_cols: list[pl.Expr] = [avg_expr(c).alias(c) for c in heating_cols]

    heating_consumption = [c for c in heating_cols if "consumption_intensity" not in c]
    heating_intensity = [c for c in heating_cols if "consumption_intensity" in c]

    orig_heating_consumption = pl.col(heating_consumption[0])
    for c in heating_consumption[1:]:
        orig_heating_consumption = orig_heating_consumption + pl.col(c)
    orig_heating_intensity = pl.col(heating_intensity[0])
    for c in heating_intensity[1:]:
        orig_heating_intensity = orig_heating_intensity + pl.col(c)

    def avg_sum(cols: list[str]) -> pl.Expr:
        neighbor_sums = pl.col(f"_n0_{cols[0]}")
        for c in cols[1:]:
            neighbor_sums = neighbor_sums + pl.col(f"_n0_{c}")
        for i in range(1, n_neighbors):
            s = pl.col(f"_n{i}_{cols[0]}")
            for c in cols[1:]:
                s = s + pl.col(f"_n{i}_{c}")
            neighbor_sums = neighbor_sums + s
        return neighbor_sums / n_neighbors

    avg_heating_consumption = avg_sum(heating_consumption)
    avg_heating_intensity = avg_sum(heating_intensity)

    total_consumption_col = TOTAL_ENERGY_CONSUMPTION_PROPANE_COLUMNS[0]
    total_intensity_col = TOTAL_ENERGY_CONSUMPTION_PROPANE_COLUMNS[1]
    new_total_consumption = (
        pl.col(total_consumption_col)
        - orig_heating_consumption
        + avg_heating_consumption
    )
    new_total_intensity = (
        pl.col(total_intensity_col) - orig_heating_intensity + avg_heating_intensity
    )
    replace_cols.extend(
        [
            new_total_consumption.alias(total_consumption_col),
            new_total_intensity.alias(total_intensity_col),
        ]
    )

    drop_cols = [f"_n{i}_{c}" for i in range(n_neighbors) for c in heating_cols]
    return out.with_columns(replace_cols).drop(drop_cols)


def replace_hvac_columns(
    original_load_curve_hourly: pl.LazyFrame,
    neighbors_load_curve_hourly: list[pl.LazyFrame],
) -> tuple[pl.LazyFrame, bool]:
    """Replace hvac columns in the original load curve with neighbor averages. Frames must already be loaded. Returns (replaced LazyFrame, uses_natural_gas)."""
    out = _replace_electricity_columns(
        original_load_curve_hourly, neighbors_load_curve_hourly
    )
    out = _replace_heating_cooling_load_columns(out, neighbors_load_curve_hourly)
    out, uses_natural_gas = _replace_natural_gas_columns(
        out, neighbors_load_curve_hourly
    )
    out = _replace_fuel_oil_columns(out, neighbors_load_curve_hourly)
    out = _replace_propane_columns(out, neighbors_load_curve_hourly)
    return out.sort(TIMESTAMP_COLUMN), uses_natural_gas


def update_load_curve_hourly(
    nearest_neighbor_map: dict[int, list[tuple[int, float]]],
    input_load_curve_hourly_dir: S3Path | Path,
    output_load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    *,
    max_workers: int = 150,
) -> list[int]:
    """Load original and neighbor parquets concurrently, replace hvac columns, sink back to original path. Returns list of bldg_ids that use natural gas after replacement."""
    upgrade_int = int(upgrade_id)
    scan_opts = _parquet_storage_options(input_load_curve_hourly_dir)
    sink_opts = _parquet_storage_options(output_load_curve_hourly_dir)
    natural_gas_usage: list[int] = []
    usage_lock = threading.Lock()

    def process_one(
        bldg_id: int, k_nearest_bldg_ids_rmse: list[tuple[int, float]]
    ) -> None:
        input_path = input_load_curve_hourly_dir / f"{bldg_id}-{upgrade_int}.parquet"
        neighbor_ids = [nid for nid, _ in k_nearest_bldg_ids_rmse]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            original_future = executor.submit(
                pl.scan_parquet,
                str(input_path),
                storage_options=scan_opts,
            )
            neighbor_futures = [
                executor.submit(
                    pl.scan_parquet,
                    str(input_load_curve_hourly_dir / f"{nid}-{upgrade_int}.parquet"),
                    storage_options=scan_opts,
                )
                for nid in neighbor_ids
            ]
            original_lf = original_future.result()
            neighbors_lf = [f.result() for f in neighbor_futures]
        replaced, uses_natural_gas = replace_hvac_columns(original_lf, neighbors_lf)
        with usage_lock:
            if uses_natural_gas:
                natural_gas_usage.append(bldg_id)
        replaced.sink_parquet(
            str(output_load_curve_hourly_dir / f"{bldg_id}-{upgrade_int}.parquet"),
            storage_options=sink_opts,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_one, bldg_id, k_nearest_bldg_ids_rmse)
            for bldg_id, k_nearest_bldg_ids_rmse in nearest_neighbor_map.items()
        ]
        for i, future in enumerate(as_completed(futures)):
            print(f"Processed {i + 1} of {len(nearest_neighbor_map)} buildings")
            future.result()
    return natural_gas_usage


def update_metadata(
    non_hp_bldg_metadata: pl.LazyFrame,
    input_metadata: pl.LazyFrame,
    natural_gas_usage: list[int] | None = None,
) -> pl.LazyFrame:
    """Update metadata for non-HP bldg_id's."""
    non_hp_df = cast(
        pl.DataFrame,
        non_hp_bldg_metadata.select("bldg_id").collect(),
    )
    non_hp_bldg_ids = non_hp_df["bldg_id"].to_list()
    # Update postprocess_group.has_hp column
    replaced_metadata = input_metadata.with_columns(
        pl.when(pl.col("bldg_id").is_in(non_hp_bldg_ids))
        .then(True)
        .otherwise(pl.col(POSTPROCESS_HAS_HP_COLUMN))
        .alias(POSTPROCESS_HAS_HP_COLUMN)
    )
    # For non-HP bldg_id's only: set has_natgas_connection from natural_gas_usage (True/False); other bldg_id's unchanged
    if natural_gas_usage:
        replaced_metadata = replaced_metadata.with_columns(
            pl.when(
                pl.col("bldg_id").is_in(non_hp_bldg_ids)
                & pl.col("bldg_id").is_in(natural_gas_usage)
            )
            .then(True)
            .when(
                pl.col("bldg_id").is_in(non_hp_bldg_ids)
                & ~pl.col("bldg_id").is_in(natural_gas_usage)
            )
            .then(False)
            .otherwise(pl.col(HAS_NATGAS_CONNECTION_COLUMN))
            .alias(HAS_NATGAS_CONNECTION_COLUMN)
        )
    # Update heats_with columns
    for heats_with in HEATS_WITH_COLUMNS:
        if heats_with == "heats_with_electricity":
            replaced_metadata = replaced_metadata.with_columns(
                pl.when(pl.col("bldg_id").is_in(non_hp_bldg_ids))
                .then(True)
                .otherwise(pl.col(heats_with))
                .alias(heats_with)
            )
        else:
            replaced_metadata = replaced_metadata.with_columns(
                pl.when(pl.col("bldg_id").is_in(non_hp_bldg_ids))
                .then(False)
                .otherwise(pl.col(heats_with))
                .alias(heats_with)
            )
    # Update postprocess_group.heating_type column
    replaced_metadata = replaced_metadata.with_columns(
        pl.when(pl.col("bldg_id").is_in(non_hp_bldg_ids))
        .then("heat_pump")
        .otherwise(pl.col(POSTPROCESS_HEATING_TYPE_COLUMN))
        .alias(POSTPROCESS_HEATING_TYPE_COLUMN)
    )
    # Update upgrade.hvac columns
    replaced_metadata = replaced_metadata.with_columns(
        pl.when(
            pl.col("bldg_id").is_in(non_hp_bldg_ids)
            & pl.col(HEATING_TYPE_COLUMN).str.contains("Non-Ducted Heating")
        )
        .then(pl.lit("MSHP, SEER 20, 11 HSPF, CCHP, Max Load"))
        .when(
            pl.col("bldg_id").is_in(non_hp_bldg_ids)
            & ~pl.col(HEATING_TYPE_COLUMN).str.contains("Non-Ducted Heating")
        )
        .then(pl.lit("ASHP, SEER 20, 11 HSPF, CCHP, Max Load"))
        .otherwise(pl.col(HEATING_TYPE_COLUMN))
        .alias(UPGRADE_HEATING_EFFICIENCY_COLUMN)
    )
    replaced_metadata = replaced_metadata.with_columns(
        pl.when(
            pl.col("bldg_id").is_in(non_hp_bldg_ids)
            & pl.col(HEATING_TYPE_COLUMN).str.contains("Non-Ducted Heating")
        )
        .then(pl.lit("Non-Ducted Heat Pump"))
        .when(
            pl.col("bldg_id").is_in(non_hp_bldg_ids)
            & ~pl.col(HEATING_TYPE_COLUMN).str.contains("Non-Ducted Heating")
        )
        .then(pl.lit("Ducted Heat Pump"))
        .otherwise(pl.col(UPGRADE_COOLING_EFFICIENCY_COLUMN))
        .alias(UPGRADE_COOLING_EFFICIENCY_COLUMN)
    )
    replaced_metadata = replaced_metadata.with_columns(
        pl.when(pl.col("bldg_id").is_in(non_hp_bldg_ids))
        .then("100% Conditioned")
        .otherwise(pl.col(UPGRADE_PARTIAL_CONDITIONING_COLUMN))
        .alias(UPGRADE_PARTIAL_CONDITIONING_COLUMN)
    )
    return replaced_metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Approximate non-HP MF highrise load curves using k-nearest neighbors; optionally run validation.",
    )
    parser.add_argument(
        "--path_s3",
        type=str,
        required=True,
        help="S3 base path for ResStock data (e.g. s3://data.sb/nrel/resstock)",
    )
    parser.add_argument(
        "--state", type=str, required=True, help="State abbreviation (e.g. NY)"
    )
    parser.add_argument(
        "--upgrade_id",
        type=str,
        required=True,
        help="Upgrade id (e.g. 02)",
    )
    parser.add_argument(
        "--input_release",
        type=str,
        required=True,
        help="Input release name (e.g. res_2024_amy2018_2)",
    )
    parser.add_argument(
        "--output_release",
        type=str,
        required=True,
        help="Output release name for approximated curves (e.g. res_2024_amy2018_2_sb)",
    )
    parser.add_argument(
        "--k",
        type=int,
        required=True,
        help="Number of nearest neighbors per building",
    )
    parser.add_argument(
        "--update_MF_highrise",
        type=bool,
        required=True,
        help="Whether to update MF highrise load curves and metadata",
    )
    parser.add_argument(
        "--update_other_fuel_types",
        type=bool,
        required=True,
        help="Whether to update bldg_id's with other heating fuel types' load curves and metadata",
    )

    args = parser.parse_args()

    path_s3 = S3Path(args.path_s3)
    input_metadata_path = (
        path_s3
        / args.input_release
        / "metadata"
        / f"state={args.state}"
        / f"upgrade={args.upgrade_id}"
        / "metadata-sb.parquet"
    )
    input_metadata = pl.scan_parquet(
        str(input_metadata_path), storage_options=STORAGE_OPTIONS
    )
    input_load_curve_hourly_dir = (
        path_s3
        / args.input_release
        / "load_curve_hourly"
        / f"state={args.state}"
        / f"upgrade={args.upgrade_id}"
    )
    output_metadata_path = (
        path_s3
        / args.output_release
        / "metadata"
        / f"state={args.state}"
        / f"upgrade={args.upgrade_id}"
        / "metadata-sb.parquet"
    )
    output_load_curve_hourly_dir = (
        path_s3
        / args.output_release
        / "load_curve_hourly"
        / f"state={args.state}"
        / f"upgrade={args.upgrade_id}"
    )
    if args.update_MF_highrise:
        non_hp_mf_highrise_bldg_metadata = _identify_non_hp_mf_highrise(input_metadata)
    if args.update_other_fuel_types:
        non_hp_other_fuel_types_bldg_metadata = _identify_other_fuel_types(
            input_metadata
        )
    non_hp_bldg_metadata = pl.concat(
        [non_hp_mf_highrise_bldg_metadata, non_hp_other_fuel_types_bldg_metadata]
    )

    nearest_neighbor_map = _find_nearest_neighbors(
        input_metadata,
        non_hp_bldg_metadata,
        input_load_curve_hourly_dir,
        args.upgrade_id,
        k=int(args.k),
        include_cooling=False,
    )
    natural_gas_usage = update_load_curve_hourly(
        nearest_neighbor_map,
        input_load_curve_hourly_dir,
        output_load_curve_hourly_dir,
        args.upgrade_id,
    )
    updated_metadata = update_metadata(
        non_hp_bldg_metadata,
        input_metadata,
        natural_gas_usage=natural_gas_usage,
    )
    updated_metadata.sink_parquet(
        str(output_metadata_path), storage_options=STORAGE_OPTIONS
    )

    # Validation. Uncomment to run validation.
    """validate_nearest_neighbor_approximation(
        input_metadata,
        input_load_curve_hourly_dir,
        args.upgrade_id,
        k=args.k,
        include_cooling=False,
        n_validation=100,
    )"""

########################################################
# Validation functions
########################################################


def _validate_one_building_load(
    load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    bldg_id: int,
    k_nearest_bldg_ids_rmse: list[tuple[int, float]],
) -> tuple[float, float, float, float, float, float] | None:
    """Compute validation metrics for one building (total, heating, cooling load). Returns (total_rmse, total_peak, heating_rmse, heating_peak, cooling_rmse, cooling_peak) or None on load failure."""
    try:
        real_total_result = _load_one_total_building_load_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
        real_heating_result = _load_one_heating_building_load_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
        real_cooling_result = _load_one_cooling_building_load_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
    except (ValueError, OSError) as e:
        raise ValueError(f"Error loading load curve for bldg_id: {bldg_id}: {e}")
    if (
        real_total_result is None
        or real_heating_result is None
        or real_cooling_result is None
    ):
        raise ValueError(
            f"Error loading load curve for bldg_id: {bldg_id}: one or more curves failed to load"
        )
    _, real_total_vec = real_total_result
    _, real_heating_vec = real_heating_result
    _, real_cooling_vec = real_cooling_result
    average_neighbor_total = np.zeros(8760)
    average_neighbor_heating = np.zeros(8760)
    average_neighbor_cooling = np.zeros(8760)
    for neighbor_id, _ in k_nearest_bldg_ids_rmse:
        neighbor_total_result = _load_one_total_building_load_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id
        )
        neighbor_heating_result = _load_one_heating_building_load_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id
        )
        neighbor_cooling_result = _load_one_cooling_building_load_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id
        )
        if (
            neighbor_total_result is None
            or neighbor_heating_result is None
            or neighbor_cooling_result is None
        ):
            raise ValueError(
                f"Error loading load curve for neighbor_id: {neighbor_id}: one or more curves failed to load"
            )
        _, neighbor_total_vec = neighbor_total_result
        _, neighbor_heating_vec = neighbor_heating_result
        _, neighbor_cooling_vec = neighbor_cooling_result
        average_neighbor_total += neighbor_total_vec
        average_neighbor_heating += neighbor_heating_vec
        average_neighbor_cooling += neighbor_cooling_vec
    n = len(k_nearest_bldg_ids_rmse)
    average_neighbor_total /= n
    average_neighbor_heating /= n
    average_neighbor_cooling /= n
    total_rmse = _rmse_8760(real_total_vec, average_neighbor_total, smooth=False)
    total_peak = np.abs(np.max(real_total_vec) - np.max(average_neighbor_total))
    heating_rmse = _rmse_8760(real_heating_vec, average_neighbor_heating, smooth=False)
    heating_peak = np.abs(np.max(real_heating_vec) - np.max(average_neighbor_heating))
    cooling_rmse = _rmse_8760(real_cooling_vec, average_neighbor_cooling, smooth=False)
    cooling_peak = np.abs(np.max(real_cooling_vec) - np.max(average_neighbor_cooling))
    return (
        total_rmse,
        total_peak,
        heating_rmse,
        heating_peak,
        cooling_rmse,
        cooling_peak,
    )


def _validate_nearest_neighbors_building_load(
    load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    approximated_yes_hp_mf_highrise_bldg_ids: dict[int, list[tuple[int, float]]],
    max_workers: int = 64,
) -> None:
    """Validate the approximated yes-HP MF highrise load curves.
    Reports total (heating+cooling), heating-only, and cooling-only metrics for all cases.
    One task per (bldg_id, k_nearest); main thread collects results and prints combined summary.
    """
    total_rmse_results: list[float] = []
    total_peak_difference_results: list[float] = []
    heating_rmse_results: list[float] = []
    heating_peak_difference_results: list[float] = []
    cooling_rmse_results: list[float] = []
    cooling_peak_difference_results: list[float] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _validate_one_building_load,
                load_curve_hourly_dir,
                upgrade_id,
                bldg_id,
                k_nearest_bldg_ids_rmse,
            ): bldg_id
            for bldg_id, k_nearest_bldg_ids_rmse in approximated_yes_hp_mf_highrise_bldg_ids.items()
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                (
                    total_rmse,
                    total_peak,
                    heating_rmse,
                    heating_peak,
                    cooling_rmse,
                    cooling_peak,
                ) = result
                total_rmse_results.append(total_rmse)
                total_peak_difference_results.append(total_peak)
                heating_rmse_results.append(heating_rmse)
                heating_peak_difference_results.append(heating_peak)
                cooling_rmse_results.append(cooling_rmse)
                cooling_peak_difference_results.append(cooling_peak)

    print("================================================")
    print("Total (heating + cooling) building load:")
    print(f"  Mean RMSE: {np.mean(total_rmse_results):.3f} kW")
    print(f"  Median RMSE: {np.median(total_rmse_results):.3f} kW")
    print(f"  Mean peak difference: {np.mean(total_peak_difference_results):.3f} kW")
    print(
        f"  Median peak difference: {np.median(total_peak_difference_results):.3f} kW"
    )
    print("================================================")
    print("Heating-only building load:")
    print(f"  Mean RMSE: {np.mean(heating_rmse_results):.3f} kW")
    print(f"  Median RMSE: {np.median(heating_rmse_results):.3f} kW")
    print(f"  Mean peak difference: {np.mean(heating_peak_difference_results):.3f} kW")
    print(
        f"  Median peak difference: {np.median(heating_peak_difference_results):.3f} kW"
    )
    print("================================================")
    print("Cooling-only building load:")
    print(f"  Mean RMSE: {np.mean(cooling_rmse_results):.3f} kW")
    print(f"  Median RMSE: {np.median(cooling_rmse_results):.3f} kW")
    print(f"  Mean peak difference: {np.mean(cooling_peak_difference_results):.3f} kW")
    print(
        f"  Median peak difference: {np.median(cooling_peak_difference_results):.3f} kW"
    )


def _validate_one_building_energy_consumption(
    load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    bldg_id: int,
    k_nearest_bldg_ids_rmse: list[tuple[int, float]],
) -> tuple[float, float, float, float, float, float, float, float, float, float] | None:
    """Compute validation metrics for one building (total, heating, cooling energy consumption). Returns (total_rmse, total_peak, total_diff, total_diff_pct, heating_rmse, heating_peak, heating_diff, cooling_rmse, cooling_peak, cooling_diff) or None on load failure. total_diff_pct = total_diff / sum(real_total_vec)."""
    try:
        real_total_result = _load_one_total_heating_cooling_energy_consumption_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
        real_heating_result = _load_one_heating_energy_consumption_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
        real_cooling_result = _load_one_cooling_energy_consumption_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
    except (ValueError, OSError) as e:
        raise ValueError(
            f"Error loading energy consumption curve for bldg_id: {bldg_id}: {e}"
        )
    if (
        real_total_result is None
        or real_heating_result is None
        or real_cooling_result is None
    ):
        raise ValueError(
            f"Error loading energy consumption curve for bldg_id: {bldg_id}: one or more curves failed to load"
        )
    _, real_total_vec = real_total_result
    _, real_heating_vec = real_heating_result
    _, real_cooling_vec = real_cooling_result
    average_neighbor_total = np.zeros(8760)
    average_neighbor_heating = np.zeros(8760)
    average_neighbor_cooling = np.zeros(8760)
    for neighbor_id, _ in k_nearest_bldg_ids_rmse:
        neighbor_total_result = (
            _load_one_total_heating_cooling_energy_consumption_curve(
                load_curve_hourly_dir, neighbor_id, upgrade_id
            )
        )
        neighbor_heating_result = _load_one_heating_energy_consumption_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id
        )
        neighbor_cooling_result = _load_one_cooling_energy_consumption_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id
        )
        if (
            neighbor_total_result is None
            or neighbor_heating_result is None
            or neighbor_cooling_result is None
        ):
            raise ValueError(
                f"Error loading energy consumption curve for neighbor_id: {neighbor_id}: one or more curves failed to load"
            )
        _, neighbor_total_vec = neighbor_total_result
        _, neighbor_heating_vec = neighbor_heating_result
        _, neighbor_cooling_vec = neighbor_cooling_result
        average_neighbor_total += neighbor_total_vec
        average_neighbor_heating += neighbor_heating_vec
        average_neighbor_cooling += neighbor_cooling_vec
    n = len(k_nearest_bldg_ids_rmse)
    average_neighbor_total /= n
    average_neighbor_heating /= n
    average_neighbor_cooling /= n
    total_rmse = _rmse_8760(real_total_vec, average_neighbor_total, smooth=False)
    total_peak = np.abs(np.max(real_total_vec) - np.max(average_neighbor_total))
    total_diff = np.abs(np.sum(real_total_vec) - np.sum(average_neighbor_total))
    real_total_sum = np.sum(real_total_vec)
    total_diff_pct = (total_diff / real_total_sum) if real_total_sum != 0 else 0.0
    heating_rmse = _rmse_8760(real_heating_vec, average_neighbor_heating, smooth=False)
    heating_peak = np.abs(np.max(real_heating_vec) - np.max(average_neighbor_heating))
    heating_diff = np.abs(np.sum(real_heating_vec) - np.sum(average_neighbor_heating))
    cooling_rmse = _rmse_8760(real_cooling_vec, average_neighbor_cooling, smooth=False)
    cooling_peak = np.abs(np.max(real_cooling_vec) - np.max(average_neighbor_cooling))
    cooling_diff = np.abs(np.sum(real_cooling_vec) - np.sum(average_neighbor_cooling))
    return (
        total_rmse,
        total_peak,
        total_diff,
        total_diff_pct,
        heating_rmse,
        heating_peak,
        heating_diff,
        cooling_rmse,
        cooling_peak,
        cooling_diff,
    )


def _validate_nearest_neighbors_heating_cooling_energy_consumption(
    load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    approximated_yes_hp_mf_highrise_bldg_ids: dict[int, list[tuple[int, float]]],
    max_workers: int = 64,
) -> None:
    """Validate the approximated yes-HP MF highrise heating/cooling energy consumption curves.
    Reports total (heating+cooling), heating-only, and cooling-only metrics for all cases.
    One task per (bldg_id, k_nearest); main thread collects results and prints combined summary.
    """
    total_rmse_results: list[float] = []
    total_peak_difference_results: list[float] = []
    total_energy_consumption_diff_results: list[float] = []
    total_diff_pct_results: list[float] = []
    heating_rmse_results: list[float] = []
    heating_peak_difference_results: list[float] = []
    heating_total_diff_results: list[float] = []
    cooling_rmse_results: list[float] = []
    cooling_peak_difference_results: list[float] = []
    cooling_total_diff_results: list[float] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _validate_one_building_energy_consumption,
                load_curve_hourly_dir,
                upgrade_id,
                bldg_id,
                k_nearest_bldg_ids_rmse,
            ): bldg_id
            for bldg_id, k_nearest_bldg_ids_rmse in approximated_yes_hp_mf_highrise_bldg_ids.items()
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                (
                    total_rmse,
                    total_peak,
                    total_diff,
                    total_diff_pct,
                    heating_rmse,
                    heating_peak,
                    heating_diff,
                    cooling_rmse,
                    cooling_peak,
                    cooling_diff,
                ) = result
                total_rmse_results.append(total_rmse)
                total_peak_difference_results.append(total_peak)
                total_energy_consumption_diff_results.append(total_diff)
                total_diff_pct_results.append(total_diff_pct)
                heating_rmse_results.append(heating_rmse)
                heating_peak_difference_results.append(heating_peak)
                heating_total_diff_results.append(heating_diff)
                cooling_rmse_results.append(cooling_rmse)
                cooling_peak_difference_results.append(cooling_peak)
                cooling_total_diff_results.append(cooling_diff)

    if not total_rmse_results:
        return
    print("================================================")
    print("Total (heating + cooling) energy consumption:")
    print(f"  Mean RMSE: {np.mean(total_rmse_results):.3f} kWh")
    print(f"  Median RMSE: {np.median(total_rmse_results):.3f} kWh")
    print(f"  Mean peak difference: {np.mean(total_peak_difference_results):.3f} kW")
    print(
        f"  Median peak difference: {np.median(total_peak_difference_results):.3f} kW"
    )
    print(
        f"  Mean total energy consumption difference: {np.mean(total_energy_consumption_diff_results):.3f} kWh"
    )
    print(
        f"  Median total energy consumption difference: {np.median(total_energy_consumption_diff_results):.3f} kWh"
    )
    print(
        f"  Mean total diff as % of building total: {100 * np.mean(total_diff_pct_results):.2f}%"
    )
    print(
        f"  Median total diff as % of building total: {100 * np.median(total_diff_pct_results):.2f}%"
    )
    print("================================================")
    print("Heating-only energy consumption:")
    print(f"  Mean RMSE: {np.mean(heating_rmse_results):.3f} kWh")
    print(f"  Median RMSE: {np.median(heating_rmse_results):.3f} kWh")
    print(f"  Mean peak difference: {np.mean(heating_peak_difference_results):.3f} kW")
    print(
        f"  Median peak difference: {np.median(heating_peak_difference_results):.3f} kW"
    )
    print(
        f"  Mean total heating energy consumption difference: {np.mean(heating_total_diff_results):.3f} kWh"
    )
    print(
        f"  Median total heating energy consumption difference: {np.median(heating_total_diff_results):.3f} kWh"
    )
    print("================================================")
    print("Cooling-only energy consumption:")
    print(f"  Mean RMSE: {np.mean(cooling_rmse_results):.3f} kWh")
    print(f"  Median RMSE: {np.median(cooling_rmse_results):.3f} kWh")
    print(f"  Mean peak difference: {np.mean(cooling_peak_difference_results):.3f} kW")
    print(
        f"  Median peak difference: {np.median(cooling_peak_difference_results):.3f} kW"
    )
    print(
        f"  Mean total cooling energy consumption difference: {np.mean(cooling_total_diff_results):.3f} kWh"
    )
    print(
        f"  Median total cooling energy consumption difference: {np.median(cooling_total_diff_results):.3f} kWh"
    )


def validate_nearest_neighbor_approximation(
    metadata: pl.LazyFrame,
    input_load_curve_hourly_dir: S3Path | Path,
    upgrade_id: str,
    *,
    k: int = 20,
    include_cooling: bool = False,
    n_validation: int = 100,
) -> None:
    """Run nearest-neighbor approximation validation: find k nearest for a sample of HP MF highrise bldgs and report load/energy metrics."""
    validation_bldg_ids = (
        metadata.filter(
            (
                pl.col(POSTPROCESS_HAS_HP_COLUMN)
                & pl.col(BLDG_TYPE_COLUMN).str.contains("Multifamily", literal=True)
                & pl.col(STORIES_COLUMN).str.contains("8+", literal=True)
            )
        )
        .select("bldg_id", WEATHER_FILE_CITY_COLUMN)
        .head(n_validation)
    )
    validation_bldg_id_nearest_neighbors = _find_nearest_neighbors(
        metadata,
        validation_bldg_ids,
        input_load_curve_hourly_dir,
        upgrade_id,
        k=k,
        include_cooling=include_cooling,
    )
    _validate_nearest_neighbors_building_load(
        input_load_curve_hourly_dir, upgrade_id, validation_bldg_id_nearest_neighbors
    )
    _validate_nearest_neighbors_heating_cooling_energy_consumption(
        input_load_curve_hourly_dir, upgrade_id, validation_bldg_id_nearest_neighbors
    )

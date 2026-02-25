from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import cast

import numpy as np
import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}
BLDG_TYPE_COLUMN = "in.geometry_building_type_height"
STORIES_COLUMN = "in.geometry_story_bin"
WEATHER_FILE_CITY_COLUMN = "in.weather_file_city"
HAS_HP_COLUMN = "postprocess_group.has_hp"

# Columns in load_curve_hourly parquet.
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
)
TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS = (
    "out.electricity.total.energy_consumption",
    "out.electricity.total.energy_consumption_intensity",
    "out.electricity.cooling_fans_pumps.energy_consumption",
    "out.electricity.cooling_fans_pumps.energy_consumption_intensity",
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
        ~pl.col(HAS_HP_COLUMN)
        & pl.col(BLDG_TYPE_COLUMN).str.contains("Multifamily", literal=True)
        & pl.col(STORIES_COLUMN).str.contains("8+", literal=True)
    )
    return metadata.filter(is_non_hp_mf_highrise).select(
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
    load_curve_hourly_dir: S3Path,
    bldg_id: int,
    upgrade_id: str,
    include_cooling: bool = False,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, compute summed column; returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    try:
        lf = pl.scan_parquet(str(path), storage_options=STORAGE_OPTIONS)
    except Exception:
        raise ValueError(
            f"Failed to load load curve for bldg_id: {bldg_id} from {path}"
        )
    schema = lf.collect_schema()
    if (
        include_cooling and COOLING_LOAD_COLUMN not in schema.names()
    ) or HEATING_LOAD_COLUMN not in schema.names():
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing required columns: {COOLING_LOAD_COLUMN if include_cooling else ''} {HEATING_LOAD_COLUMN}"
        )
    df = cast(
        pl.DataFrame,
        lf.with_columns(
            (
                (pl.col(COOLING_LOAD_COLUMN) if include_cooling else pl.lit(0.0))
                + pl.col(HEATING_LOAD_COLUMN)
            ).alias("_summed")
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
    load_curve_hourly_dir: S3Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, compute summed heating + cooling energy consumption columns; returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    try:
        lf = pl.scan_parquet(str(path), storage_options=STORAGE_OPTIONS)
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
    load_curve_hourly_dir: S3Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, heating load column only; returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    try:
        lf = pl.scan_parquet(str(path), storage_options=STORAGE_OPTIONS)
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
    load_curve_hourly_dir: S3Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, cooling load column only; returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    try:
        lf = pl.scan_parquet(str(path), storage_options=STORAGE_OPTIONS)
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
    load_curve_hourly_dir: S3Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, heating energy consumption columns only (summed); returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    try:
        lf = pl.scan_parquet(str(path), storage_options=STORAGE_OPTIONS)
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
    load_curve_hourly_dir: S3Path,
    bldg_id: int,
    upgrade_id: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, cooling energy consumption columns only (summed); returns (bldg_id, vec) or None."""
    path = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    try:
        lf = pl.scan_parquet(str(path), storage_options=STORAGE_OPTIONS)
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
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    bldg_ids: list[int],
    max_workers: int = 256,
    include_cooling: bool = False,
) -> dict[int, np.ndarray]:
    """Load load curves for the given bldg_ids. Returns bldg_id -> 8760-point array."""
    bldg_id_to_load_curve: dict[int, np.ndarray] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _load_one_total_building_load_curve,
                load_curve_hourly_dir,
                bldg_id,
                upgrade_id,
                include_cooling=include_cooling,
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
    load_curve_hourly_dir: S3Path,
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
    non_hp_mf_highrise_bldg_metadata: pl.LazyFrame,
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    *,
    k: int = 5,
    max_workers_load_curves: int = 256,
    max_workers_neighbors: int = 256,
    include_cooling: bool = False,
) -> dict[int, list[int]]:
    """For each non-HP MF highrise bldg, find k nearest same-weather bldgs by lowest RMSE on load curves.
    Returns non_hp_bldg_id -> [k nearest neighbor bldg_ids].
    """
    weather_station_bldg_id_map_non_hp = group_by_weather_station_id(
        non_hp_mf_highrise_bldg_metadata
    )
    weather_station_bldg_id_map_total = group_by_weather_station_id(metadata)

    k_nearest_bldg_id_map: dict[int, list[tuple[int, float]]] = {}
    for weather_station, station_bldg_ids in weather_station_bldg_id_map_non_hp.items():
        k_nearest_bldg_id_map.update({bldg_id: [] for bldg_id in station_bldg_ids})
        # Neighbors = all bldgs at this station except the non-HP ones we're finding neighbors for.
        neighbor_bldg_ids = [
            x
            for x in weather_station_bldg_id_map_total[weather_station]
            if x not in station_bldg_ids
        ]
        # If include_cooling, fit based on total load curves; otherwise, fit based on heating load curves.
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
                    else:
                        if rmse < k_nearest_bldg_id_map[station_bldg_id][-1][1]:
                            k_nearest_bldg_id_map[station_bldg_id][-1] = (
                                neighbor_bldg_id,
                                rmse,
                            )
                            k_nearest_bldg_id_map[station_bldg_id].sort(
                                key=lambda p: p[1]
                            )
                print(
                    f"Processed {i + 1} of {len(neighbor_bldg_ids)} neighbor bldgs for weather station {weather_station}"
                )
    return k_nearest_bldg_id_map


def _find_nearest_neighbors_ignore_weather_station(
    metadata: pl.LazyFrame,
    non_hp_mf_highrise_bldg_metadata: pl.LazyFrame,
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    *,
    k: int = 5,
    max_workers_load_curves: int = 256,
    max_workers_neighbors: int = 256,
) -> dict[int, list[int]]:
    """For each non-HP MF highrise bldg, find k nearest same-weather bldgs by lowest RMSE on load curves.
    Returns non_hp_bldg_id -> [k nearest neighbor bldg_ids].
    """
    all_bldg_ids = cast(
        pl.DataFrame,
        metadata.select("bldg_id").collect(),
    )["bldg_id"].to_list()
    non_hp_bldg_ids = cast(
        pl.DataFrame,
        non_hp_mf_highrise_bldg_metadata.select("bldg_id").collect(),
    )["bldg_id"].to_list()
    neighbor_bldg_ids = [x for x in all_bldg_ids if x not in non_hp_bldg_ids]
    k_nearest_bldg_id_map: dict[int, list[tuple[int, float]]] = {
        bldg_id: [] for bldg_id in non_hp_bldg_ids
    }
    non_hp_load_curves = _load_all_total_load_curves_for_bldg_ids(
        load_curve_hourly_dir,
        upgrade_id,
        non_hp_bldg_ids,
        max_workers=max_workers_load_curves,
    )
    # Download load curves for each neighbor (in parallel).
    with ThreadPoolExecutor(max_workers=max_workers_neighbors) as executor:
        futures = {
            executor.submit(
                _load_one_total_building_load_curve,
                load_curve_hourly_dir,
                bldg_id,
                upgrade_id,
            ): bldg_id
            for bldg_id in neighbor_bldg_ids
        }
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            neighbor_bldg_id, neighbor_load_curve_vec = result
            # For each non-HP station bldg, compute RMSE to each neighbor; iteratively keep only top k (lowest RMSE).
            for non_hp_bldg_id in non_hp_bldg_ids:
                non_hp_bldg_load_curve_vec = non_hp_load_curves[non_hp_bldg_id]
                rmse = _rmse_8760(non_hp_bldg_load_curve_vec, neighbor_load_curve_vec)
                if len(k_nearest_bldg_id_map[non_hp_bldg_id]) < k:
                    k_nearest_bldg_id_map[non_hp_bldg_id].append(
                        (neighbor_bldg_id, rmse)
                    )
                else:
                    if rmse < k_nearest_bldg_id_map[non_hp_bldg_id][-1][1]:
                        k_nearest_bldg_id_map[non_hp_bldg_id][-1] = (
                            neighbor_bldg_id,
                            rmse,
                        )
                        k_nearest_bldg_id_map[non_hp_bldg_id].sort(key=lambda p: p[1])
            print(f"Processed {i + 1} of {len(neighbor_bldg_ids)} neighbor bldgs")
    return k_nearest_bldg_id_map


def _validate_one_building_load(
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    bldg_id: int,
    k_nearest_bldg_ids_rmse: list[tuple[int, float]],
) -> tuple[float, float, float, float, float, float] | None:
    """Compute validation metrics for one building (total, heating, cooling load). Returns (total_rmse, total_peak, heating_rmse, heating_peak, cooling_rmse, cooling_peak) or None on load failure."""
    try:
        real_total_result = _load_one_total_building_load_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id, include_cooling=True
        )
        real_heating_result = _load_one_heating_building_load_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
        real_cooling_result = _load_one_cooling_building_load_curve(
            load_curve_hourly_dir, bldg_id, upgrade_id
        )
    except (ValueError, OSError) as e:
        raise ValueError(f"Error loading load curve for bldg_id: {bldg_id}: {e}")
    _, real_total_vec = real_total_result
    _, real_heating_vec = real_heating_result
    _, real_cooling_vec = real_cooling_result
    average_neighbor_total = np.zeros(8760)
    average_neighbor_heating = np.zeros(8760)
    average_neighbor_cooling = np.zeros(8760)
    for neighbor_id, _ in k_nearest_bldg_ids_rmse:
        neighbor_total_result = _load_one_total_building_load_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id, include_cooling=True
        )
        neighbor_heating_result = _load_one_heating_building_load_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id
        )
        neighbor_cooling_result = _load_one_cooling_building_load_curve(
            load_curve_hourly_dir, neighbor_id, upgrade_id
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
    load_curve_hourly_dir: S3Path,
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
    load_curve_hourly_dir: S3Path,
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
    load_curve_hourly_dir: S3Path,
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


if __name__ == "__main__":
    # Load files, define paths
    path_s3 = S3Path("s3://data.sb/nrel/resstock")
    state = "NY"
    upgrade_id = "02"
    release = "res_2024_amy2018_2_sb"
    metadata_path = (
        path_s3
        / release
        / "metadata"
        / f"state={state}"
        / f"upgrade={upgrade_id}"
        / "metadata-sb.parquet"
    )
    metadata = pl.scan_parquet(str(metadata_path), storage_options=STORAGE_OPTIONS)
    load_curve_hourly_dir = (
        path_s3
        / release
        / "load_curve_hourly"
        / f"state={state}"
        / f"upgrade={upgrade_id}"
    )

    # Identify non-HP MF highrise bldg_ids
    non_hp_mf_highrise_bldg_ids = _identify_non_hp_mf_highrise(metadata).head(10)

    # Identify validation bldg_ids (MF highrise, with HP in upgrade 2)
    validation_bldg_ids = (
        metadata.filter(
            (
                pl.col(HAS_HP_COLUMN)
                & pl.col(BLDG_TYPE_COLUMN).str.contains("Multifamily", literal=True)
                & pl.col(STORIES_COLUMN).str.contains("8+", literal=True)
            )
        )
        .select("bldg_id", WEATHER_FILE_CITY_COLUMN)
        .head(300)
    )
    validation_bldg_id_nearest_neighbors = _find_nearest_neighbors(
        metadata,
        validation_bldg_ids,
        load_curve_hourly_dir,
        upgrade_id,
        k=3,
        include_cooling=True,  # True means find nearest neighbor based on total load curves; otherwise, only based on heating load curves.
    )
    print(validation_bldg_id_nearest_neighbors)
    _validate_nearest_neighbors_building_load(
        load_curve_hourly_dir, upgrade_id, validation_bldg_id_nearest_neighbors
    )
    _validate_nearest_neighbors_heating_cooling_energy_consumption(
        load_curve_hourly_dir, upgrade_id, validation_bldg_id_nearest_neighbors
    )

    """_approximate_non_hp_load(
        metadata,
        non_hp_mf_highrise_bldg_ids,
        load_curve_hourly_dir,
        load_curve_upgrade,
        k=1,
    )"""

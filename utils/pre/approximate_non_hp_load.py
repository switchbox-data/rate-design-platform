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
)
COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS = (
    "out.electricity.cooling.energy_consumption",
    "out.electricity.cooling.energy_consumption_intensity",
)
TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS = (
    "out.electricity.total.energy_consumption",
    "out.electricity.total.energy_consumption_intensity",
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
        COOLING_LOAD_COLUMN not in schema.names()
        or HEATING_LOAD_COLUMN not in schema.names()
    ):
        raise ValueError(
            f"Load curve for bldg_id: {bldg_id} from {path} is missing required columns: {COOLING_LOAD_COLUMN} and {HEATING_LOAD_COLUMN}"
        )
    df = cast(
        pl.DataFrame,
        lf.with_columns(
            (pl.col(COOLING_LOAD_COLUMN) + pl.col(HEATING_LOAD_COLUMN)).alias("_summed")
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


def _load_all_load_curves_for_bldg_ids(
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    bldg_ids: list[int],
    max_workers: int = 256,
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
            ): bldg_id
            for bldg_id in bldg_ids
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                bldg_id, vec = result
                bldg_id_to_load_curve[bldg_id] = vec
    return bldg_id_to_load_curve


def _rmse_8760(x: np.ndarray, y: np.ndarray) -> float:
    """RMSE between two 8760-point load curves: sqrt(mean((x - y)^2))."""
    return float(np.sqrt(np.mean((x - y) ** 2)))


def _find_nearest_neighbors(
    metadata: pl.LazyFrame,
    non_hp_mf_highrise_bldg_metadata: pl.LazyFrame,
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    *,
    k: int = 5,
    max_workers: int = 256,
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
        non_hp_load_curves = _load_all_load_curves_for_bldg_ids(
            load_curve_hourly_dir,
            upgrade_id,
            station_bldg_ids,
            max_workers=max_workers,
        )
        # Download load curves for each neighbor (in parallel).
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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


def _validate_yes_hp_mf_highrise_load(
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    approximated_yes_hp_mf_highrise_bldg_ids: dict[int, list[int]],
) -> None:
    """Validate the approximated yes-HP MF highrise load curves."""
    for bldg_id, k_nearest_bldg_ids in approximated_yes_hp_mf_highrise_bldg_ids.items():
        real_result = _load_one_total_building_load_curve(
            load_curve_hourly_dir,
            bldg_id,
            upgrade_id,
        )
        if real_result is None:
            continue
        real_load_curve_vec = real_result[1]
        average_neighbor_load_curves = np.zeros(8760)
        count = 0

        # save comparison results
        rmse_results = []
        peak_difference_results = []
        for neighbor_id in k_nearest_bldg_ids:
            neighbor_result = _load_one_total_building_load_curve(
                load_curve_hourly_dir,
                neighbor_id,
                upgrade_id,
            )
            if neighbor_result is None:
                continue
            neighbor_load_curve_vec = neighbor_result[1]
            average_neighbor_load_curves += neighbor_load_curve_vec
            count += 1
        if count == 0:
            continue
        average_neighbor_load_curves /= count
        rmse = _rmse_8760(real_load_curve_vec, average_neighbor_load_curves)
        peak_difference = np.abs(
            np.max(real_load_curve_vec) - np.max(average_neighbor_load_curves)
        )
        rmse_results.append(rmse)
        peak_difference_results.append(peak_difference)
    print(f"Mean RMSE: {np.mean(rmse_results)}")
    print(f"Median RMSE: {np.median(rmse_results)}")
    print(f"Mean peak difference: {np.mean(peak_difference_results)}")
    print(f"Median peak difference: {np.median(peak_difference_results)}")


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
        .head(1)
    )
    validation_bldg_id_nearest_neighbors = _find_nearest_neighbors(
        metadata,
        validation_bldg_ids,
        load_curve_hourly_dir,
        upgrade_id,
        k=1,
    )
    _validate_yes_hp_mf_highrise_load(
        load_curve_hourly_dir,
        upgrade_id,
        validation_bldg_id_nearest_neighbors,
    )

    """_approximate_non_hp_load(
        metadata,
        non_hp_mf_highrise_bldg_ids,
        load_curve_hourly_dir,
        load_curve_upgrade,
        k=1,
    )"""

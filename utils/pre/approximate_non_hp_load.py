from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
from cloudpathlib import S3Path
from sklearn.neighbors import NearestNeighbors

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}
BLDG_TYPE_COLUMN = pl.col("in.geometry_building_type_height")
STORIES_COLUMN = pl.col("in.geometry_story_bin")
WEATHER_FILE_CITY_COLUMN = "in.weather_file_city"

# Columns in load_curve_hourly parquet to use for KNN (8760-point feature vector).
COOLING_LOAD_COLUMN = "out.load.cooling.energy_delivered.kbtu"
HEATING_LOAD_COLUMN = "out.load.heating.energy_delivered.kbtu"


def _identify_non_hp_mf_highrise(metadata: pl.LazyFrame) -> pl.LazyFrame:
    is_non_hp_mf_highrise = (
        ~pl.col("postprocess_group.has_hp")
        & BLDG_TYPE_COLUMN.str.contains("Multifamily", literal=True)
        & STORIES_COLUMN.str.contains("8+", literal=True)
    )
    return metadata.filter(is_non_hp_mf_highrise).select(
        "bldg_id", WEATHER_FILE_CITY_COLUMN
    )


def _extract_weather_station_ids(
    metadata: pl.LazyFrame, bldg_ids: list[int]
) -> list[str]:
    """Filter metadata to rows where bldg_id is in bldg_ids; return list of weather station values."""
    df = cast(
        pl.DataFrame,
        metadata.filter(pl.col("bldg_id").is_in(bldg_ids))
        .select(WEATHER_FILE_CITY_COLUMN)
        .collect(),
    )
    return df[WEATHER_FILE_CITY_COLUMN].to_list()


def group_by_weather_station_id(
    metadata: pl.LazyFrame, non_hp_mf_highrise_bldg_ids: pl.LazyFrame
) -> dict[str, list[int]]:
    """
    Group non-HP MF highrise bldg_ids by weather_station_id.
    Single lazy pipeline: semi-join metadata to restrict to those bldg_ids, then group_by/agg; one collect.
    """
    df = cast(
        pl.DataFrame,
        metadata.join(non_hp_mf_highrise_bldg_ids, on="bldg_id", how="semi")
        .select(WEATHER_FILE_CITY_COLUMN, "bldg_id")
        .group_by(WEATHER_FILE_CITY_COLUMN)
        .agg(pl.col("bldg_id"))
        .collect(),
    )
    return dict(zip(df[WEATHER_FILE_CITY_COLUMN], df["bldg_id"]))


def _load_one_curve(
    base_path: str,
    bldg_id: int,
    c1: str,
    c2: str,
) -> tuple[int, np.ndarray] | None:
    """Load one parquet, compute summed column; returns (bldg_id, vec) or None."""
    path = f"{base_path.rstrip('/')}/{bldg_id}-0.parquet"
    try:
        lf = pl.scan_parquet(path, storage_options=STORAGE_OPTIONS)
    except Exception:
        return None
    schema = lf.collect_schema()
    if c1 not in schema.names() or c2 not in schema.names():
        return None
    df = cast(
        pl.DataFrame,
        lf.with_columns((pl.col(c1) + pl.col(c2)).alias("_summed"))
        .select("_summed")
        .collect(),
    )
    vec = df["_summed"].to_numpy().astype(np.float64, copy=False)
    if len(vec) != 8760:
        return None
    return (bldg_id, vec)


def _load_curves_for_bldg_ids(
    load_curve_hourly_dir: S3Path | Path | str,
    upgrade_id: str,
    bldg_ids: list[int],
    *,
    cols: tuple[str, str] = (COOLING_LOAD_COLUMN, HEATING_LOAD_COLUMN),
    max_workers: int = 128,
) -> tuple[np.ndarray, list[int]]:
    """
    Load hourly load curves for the given bldg_ids only (one parquet per bldg).
    Reads parquets in parallel; extracts the two columns, sums them element-wise (8760 rows),
    and returns (summed, bldg_ids) where summed has shape (len(bldg_ids), 8760) mapped to each bldg_id.
    """
    base_path = str(load_curve_hourly_dir)
    c1, c2 = cols
    summed_rows: list[np.ndarray] = []
    valid_ids: list[int] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_load_one_curve, base_path, bldg_id, c1, c2): bldg_id
            for bldg_id in bldg_ids
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                bldg_id, vec = result
                valid_ids.append(bldg_id)
                summed_rows.append(vec)
            print(
                f"Read bldg_id: {bldg_id} with shape: {vec.shape} cumulative read ids: {len(valid_ids)} out of {len(bldg_ids)}"
            )
    if not summed_rows:
        return np.array([]).reshape(0, 8760), []
    return np.stack(summed_rows), valid_ids


def _rmse_8760(x: np.ndarray, y: np.ndarray) -> float:
    """RMSE between two 8760-point load curves: sqrt(mean((x - y)^2))."""
    return float(np.sqrt(np.mean((x - y) ** 2)))


def _k_nearest_same_weather(
    target_load_curve: np.ndarray,
    candidate_bldg_ids: list[int],
    candidate_load_curves: np.ndarray,
    k: int,
) -> tuple[list[int], np.ndarray]:
    """
    Find k nearest neighbors of the target by smallest RMSE over 8760-point load curves.
    Expects pre-loaded curves: target_load_curve (8760,), candidate_load_curves (n, 8760)
    with candidate_bldg_ids length n. Returns (k_nearest_bldg_ids, k_rmse_values).
    """
    if candidate_load_curves.shape[0] == 0 or k <= 0:
        return [], np.array([])
    k = min(k, candidate_load_curves.shape[0])
    nn = NearestNeighbors(n_neighbors=k, metric=_rmse_8760, algorithm="brute")
    nn.fit(candidate_load_curves)
    target_vec = np.asarray(target_load_curve, dtype=np.float64).reshape(1, -1)
    distances, indices = nn.kneighbors(target_vec)
    k_nearest_ids = [candidate_bldg_ids[i] for i in indices[0]]
    return k_nearest_ids, distances[0]


def _approximate_non_hp_load(
    metadata: pl.LazyFrame,
    non_hp_mf_highrise_bldg_ids: pl.LazyFrame,
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    *,
    k: int = 5,
) -> dict[int, list[int]]:
    """For each non-HP MF highrise bldg, find k nearest same-weather bldgs via sklearn KNN on load curves."""
    bldg_ids_df = cast(pl.DataFrame, non_hp_mf_highrise_bldg_ids.collect())
    bldg_ids = bldg_ids_df["bldg_id"].to_list()
    weather_station_ids = _extract_weather_station_ids(metadata, bldg_ids)
    unique_weather_station_ids = list(set(weather_station_ids))
    print(f"unique_weather_station_ids: {unique_weather_station_ids}")
    weather_station_bldg_id_map = group_by_weather_station_id(
        metadata, non_hp_mf_highrise_bldg_ids
    )
    print(weather_station_bldg_id_map)
    print("Total number of bldg_ids: ", len(bldg_ids))
    print(
        "Total number of mapped bldg_ids: ",
        sum(len(bldg_ids) for bldg_ids in weather_station_bldg_id_map.values()),
    )

    k_nearest_bldg_id_map: dict[int, list[int]] = {}
    load_columns = (COOLING_LOAD_COLUMN, HEATING_LOAD_COLUMN)
    for weather_station_id, station_bldg_ids in weather_station_bldg_id_map.items():
        candidate_df = cast(
            pl.DataFrame,
            metadata.filter(
                pl.col(WEATHER_FILE_CITY_COLUMN).eq(weather_station_id)
                & ~pl.col("bldg_id").is_in(station_bldg_ids)
            )
            .select("bldg_id")
            .collect(),
        )
        candidate_bldg_ids = candidate_df["bldg_id"].to_list()
        # Load curves once for all buildings at this weather station
        all_ids = list(station_bldg_ids) + candidate_bldg_ids
        summed, loaded_ids = _load_curves_for_bldg_ids(
            load_curve_hourly_dir,
            upgrade_id,
            all_ids,
            cols=load_columns,
        )
        if summed.shape[0] == 0:
            continue
        # Index by bldg_id for lookups
        id_to_idx = {bid: i for i, bid in enumerate(loaded_ids)}
        candidate_set = set(candidate_bldg_ids)
        candidate_indices = [
            i for i, bid in enumerate(loaded_ids) if bid in candidate_set
        ]
        candidate_load_curves = summed[candidate_indices]
        candidate_ids_for_knn = [loaded_ids[i] for i in candidate_indices]
        for target_bldg_id in station_bldg_ids:
            if target_bldg_id not in id_to_idx:
                continue
            target_load_curve = summed[id_to_idx[target_bldg_id]]
            k_nearest_bldg_ids, _ = _k_nearest_same_weather(
                target_load_curve,
                candidate_ids_for_knn,
                candidate_load_curves,
                k=k,
            )
            k_nearest_bldg_id_map[target_bldg_id] = k_nearest_bldg_ids
        print(f"k_nearest_bldg_id_map: {k_nearest_bldg_id_map}")
    return k_nearest_bldg_id_map


def _validate_yes_hp_mf_highrise_load(
    load_curve_hourly_dir: S3Path,
    upgrade_id: str,
    approximated_yes_hp_mf_highrise_bldg_ids: dict[int, list[int]],
) -> None:
    """Validate the approximated yes-HP MF highrise load curves."""
    load_columns = (COOLING_LOAD_COLUMN, HEATING_LOAD_COLUMN)
    for bldg_id, k_nearest_bldg_ids in approximated_yes_hp_mf_highrise_bldg_ids.items():
        summed_all, loaded_ids = _load_curves_for_bldg_ids(
            load_curve_hourly_dir,
            upgrade_id,
            [bldg_id] + k_nearest_bldg_ids,
            cols=load_columns,
        )
        if summed_all.shape[0] == 0:
            continue
        id_to_idx = {bid: i for i, bid in enumerate(loaded_ids)}
        if bldg_id not in id_to_idx:
            continue
        real_load_curve = summed_all[id_to_idx[bldg_id]]
        k_nearest_indices = [
            id_to_idx[bid] for bid in k_nearest_bldg_ids if bid in id_to_idx
        ]
        if not k_nearest_indices:
            continue
        approximated_load_curve = np.mean(summed_all[k_nearest_indices], axis=0)
        print(
            f"RMSE between approximated and real load curves: {_rmse_8760(approximated_load_curve, real_load_curve)}"
        )
        print(
            f"Max absolute difference between approximated and real load curves: {np.max(np.abs(approximated_load_curve - real_load_curve))}"
        )


if __name__ == "__main__":
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
    non_hp_mf_highrise_bldg_ids = _identify_non_hp_mf_highrise(metadata).head(10)
    print(non_hp_mf_highrise_bldg_ids.collect())

    yes_hp_mf_highrise_bldg_ids = (
        metadata.filter(
            (
                pl.col("postprocess_group.has_hp")
                & BLDG_TYPE_COLUMN.str.contains("Multifamily", literal=True)
                & STORIES_COLUMN.str.contains("8+", literal=True)
            )
        )
        .select("bldg_id", WEATHER_FILE_CITY_COLUMN)
        .head(1)
    )

    # Load curve hourly files live under upgrade=00 with filenames {bldg_id}-0.parquet
    load_curve_upgrade = "00"
    load_curve_hourly_dir = (
        path_s3
        / release
        / "load_curve_hourly"
        / f"state={state}"
        / f"upgrade={load_curve_upgrade}"
    )
    approximated_yes_hp_mf_highrise_bldg_ids = _approximate_non_hp_load(
        metadata,
        yes_hp_mf_highrise_bldg_ids,
        load_curve_hourly_dir,
        load_curve_upgrade,
        k=1,
    )
    print(approximated_yes_hp_mf_highrise_bldg_ids)
    _validate_yes_hp_mf_highrise_load(
        load_curve_hourly_dir,
        load_curve_upgrade,
        approximated_yes_hp_mf_highrise_bldg_ids,
    )

    """_approximate_non_hp_load(
        metadata,
        non_hp_mf_highrise_bldg_ids,
        load_curve_hourly_dir,
        load_curve_upgrade,
        k=1,
    )"""

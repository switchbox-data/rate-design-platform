"""Tests for approximate_non_hp_load: find k nearest neighbors and replace * columns."""

from pathlib import Path
from typing import cast
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest

from utils.pre.approximate_non_hp_load import (
    COOLING_LOAD_COLUMN,
    HEATING_LOAD_COLUMN,
    _find_nearest_neighbors,
    _identify_non_hp_mf_highrise,
    _replace_electricity_columns,
    _replace_fuel_oil_columns,
    _replace_heating_cooling_load_columns,
    _replace_natural_gas_columns,
    _replace_propane_columns,
)

BLDG_TYPE_COLUMN = "in.geometry_building_type_height"
STORIES_COLUMN = "in.geometry_story_bin"
WEATHER_FILE_CITY_COLUMN = "in.weather_file_city"
HAS_HP_COLUMN = "postprocess_group.has_hp"

# Column name tuples from the module (for building minimal frames)
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
HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS = (
    "out.natural_gas.heating.energy_consumption",
    "out.natural_gas.heating.energy_consumption_intensity",
    "out.natural_gas.heating_hp_bkup.energy_consumption",
    "out.natural_gas.heating_hp_bkup.energy_consumption_intensity",
)
TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS = (
    "out.natural_gas.total.energy_consumption",
    "out.natural_gas.total.energy_consumption_intensity",
)
HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS = (
    "out.fuel_oil.heating.energy_consumption",
    "out.fuel_oil.heating.energy_consumption_intensity",
    "out.fuel_oil.heating_hp_bkup.energy_consumption",
    "out.fuel_oil.heating_hp_bkup.energy_consumption_intensity",
)
TOTAL_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS = (
    "out.fuel_oil.total.energy_consumption",
    "out.fuel_oil.total.energy_consumption_intensity",
)
HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS = (
    "out.propane.heating.energy_consumption",
    "out.propane.heating.energy_consumption_intensity",
    "out.propane.heating_hp_bkup.energy_consumption",
    "out.propane.heating_hp_bkup.energy_consumption_intensity",
)
TOTAL_ENERGY_CONSUMPTION_PROPANE_COLUMNS = (
    "out.propane.total.energy_consumption",
    "out.propane.total.energy_consumption_intensity",
)


def _make_metadata_df(
    bldg_ids: list[int],
    weather_stations: list[str],
    has_hp: list[bool],
) -> pl.DataFrame:
    return pl.DataFrame({
        "bldg_id": bldg_ids,
        WEATHER_FILE_CITY_COLUMN: weather_stations,
        HAS_HP_COLUMN: has_hp,
        BLDG_TYPE_COLUMN: ["Multifamily (2-4 units)"] * len(bldg_ids),
        STORIES_COLUMN: ["8+"] * len(bldg_ids),
    })


def _write_load_parquet(path: Path, heating: np.ndarray, cooling: np.ndarray | None = None) -> None:
    data = {HEATING_LOAD_COLUMN: heating.tolist()}
    if cooling is not None:
        data[COOLING_LOAD_COLUMN] = cooling.tolist()
    pl.DataFrame(data).write_parquet(path)


def _heating_curve(n_hours: int, base: float, amplitude: float, phase: float, seed: int | None = None) -> np.ndarray:
    """Hourly heating load: base + amplitude * sin(2*pi*hour/8760 + phase) + small noise."""
    if seed is not None:
        np.random.seed(seed)
    t = np.arange(n_hours, dtype=np.float64)
    seasonal = base + amplitude * np.sin(2 * np.pi * t / 8760 + phase)
    noise = np.random.uniform(-0.5, 0.5, size=n_hours) if seed is not None else 0.0
    return (seasonal + noise).astype(np.float64)


def _cooling_curve(n_hours: int, base: float, amplitude: float, phase: float, seed: int | None = None) -> np.ndarray:
    """Cooling peaks in summer (phase shifted vs heating)."""
    if seed is not None:
        np.random.seed(seed)
    t = np.arange(n_hours, dtype=np.float64)
    seasonal = base + amplitude * np.sin(2 * np.pi * t / 8760 + phase + np.pi)
    seasonal = np.maximum(seasonal, 0.0)
    noise = np.random.uniform(-0.2, 0.2, size=n_hours) if seed is not None else 0.0
    return (seasonal + noise).astype(np.float64)


def test_find_nearest_neighbors_heating_only(tmp_path: Path) -> None:
    """Find k nearest neighbors with include_cooling=False; multiple non-HP bldgs and neighbors, structured curves."""
    # Two weather stations: WS1 has non-HP 1,2 and neighbors 3,4,5; WS2 has non-HP 10 and neighbors 11,12
    bldg_ids = [1, 2, 3, 4, 5, 10, 11, 12]
    weather_stations = ["WS1", "WS1", "WS1", "WS1", "WS1", "WS2", "WS2", "WS2"]
    has_hp = [False, False, True, True, True, False, True, True]
    metadata_df = _make_metadata_df(bldg_ids, weather_stations, has_hp)
    metadata = metadata_df.lazy()
    non_hp = _identify_non_hp_mf_highrise(metadata)
    upgrade_id = "02"
    n_hours = 8760

    # Build heating curves: bldg 1 and 2 (non-HP) with distinct shapes; 3,4,5 similar to 1/2 so RMSE is meaningful
    heating_curves = {
        1: _heating_curve(n_hours, base=5.0, amplitude=3.0, phase=0.0, seed=101),
        2: _heating_curve(n_hours, base=6.0, amplitude=2.5, phase=0.2, seed=102),
        3: _heating_curve(n_hours, base=5.1, amplitude=2.9, phase=0.05, seed=103),  # close to 1
        4: _heating_curve(n_hours, base=6.2, amplitude=2.4, phase=0.18, seed=104),  # close to 2
        5: _heating_curve(n_hours, base=8.0, amplitude=1.0, phase=1.0, seed=105),  # different
        10: _heating_curve(n_hours, base=4.0, amplitude=4.0, phase=0.5, seed=110),
        11: _heating_curve(n_hours, base=4.1, amplitude=3.8, phase=0.52, seed=111),
        12: _heating_curve(n_hours, base=10.0, amplitude=0.5, phase=0.0, seed=112),
    }
    for bldg_id, heating in heating_curves.items():
        _write_load_parquet(tmp_path / f"{bldg_id}-{int(upgrade_id)}.parquet", heating)

    with patch("utils.pre.approximate_non_hp_load.STORAGE_OPTIONS", {}):
        result = _find_nearest_neighbors(
            metadata,
            non_hp,
            tmp_path,
            upgrade_id,
            k=2,
            max_workers_load_curves=4,
            max_workers_neighbors=4,
            include_cooling=False,
        )

    # WS1: non-HP are 1, 2; neighbors 3,4,5. Each of 1,2 gets 2 nearest from {3,4,5}
    assert 1 in result
    assert 2 in result
    assert 10 in result
    assert len(result[1]) == 2
    assert len(result[2]) == 2
    assert len(result[10]) == 2
    assert result[1][0][1] <= result[1][1][1]
    assert result[2][0][1] <= result[2][1][1]
    assert result[10][0][1] <= result[10][1][1]
    assert set(result[1][i][0] for i in range(2)) <= {3, 4, 5}
    assert set(result[2][i][0] for i in range(2)) <= {3, 4, 5}
    assert set(result[10][i][0] for i in range(2)) <= {11, 12}


def test_find_nearest_neighbors_include_cooling(tmp_path: Path) -> None:
    """Find k nearest neighbors with include_cooling=True; heating + cooling with distinct patterns."""
    bldg_ids = [1, 2, 3, 4, 5]
    weather_stations = ["WS1"] * 5
    has_hp = [False, False, True, True, True]
    metadata_df = _make_metadata_df(bldg_ids, weather_stations, has_hp)
    metadata = metadata_df.lazy()
    non_hp = _identify_non_hp_mf_highrise(metadata)
    upgrade_id = "02"
    n_hours = 8760

    for bldg_id in bldg_ids:
        h = _heating_curve(n_hours, base=3.0 + bldg_id, amplitude=2.0, phase=bldg_id * 0.1, seed=200 + bldg_id)
        c = _cooling_curve(n_hours, base=1.0 + bldg_id * 0.5, amplitude=1.5, phase=bldg_id * 0.15, seed=300 + bldg_id)
        _write_load_parquet(tmp_path / f"{bldg_id}-{int(upgrade_id)}.parquet", h, c)

    with patch("utils.pre.approximate_non_hp_load.STORAGE_OPTIONS", {}):
        result = _find_nearest_neighbors(
            metadata,
            non_hp,
            tmp_path,
            upgrade_id,
            k=2,
            max_workers_load_curves=4,
            max_workers_neighbors=4,
            include_cooling=True,
        )

    assert 1 in result
    assert 2 in result
    assert len(result[1]) == 2
    assert len(result[2]) == 2
    assert set(result[1][i][0] for i in range(2)) <= {3, 4, 5}
    assert set(result[2][i][0] for i in range(2)) <= {3, 4, 5}
    assert result[1][0][1] <= result[1][1][1]
    assert result[2][0][1] <= result[2][1][1]


def _make_electricity_frame_constant(
    n_rows: int, heating_vals: float, cooling_vals: float, total_consumption: float, total_intensity: float
) -> pl.LazyFrame:
    heating_cols = list(HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS)
    cooling_cols = list(COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS)
    data = {}
    for c in heating_cols:
        data[c] = [heating_vals] * n_rows
    for c in cooling_cols:
        data[c] = [cooling_vals] * n_rows
    data[TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[0]] = [total_consumption] * n_rows
    data[TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[1]] = [total_intensity] * n_rows
    return pl.DataFrame(data).lazy()


def _make_electricity_frame_time_varying(
    n_rows: int,
    heating_per_row: list[float],
    cooling_per_row: list[float],
    total_consumption_per_row: list[float],
    total_intensity_per_row: list[float],
) -> pl.LazyFrame:
    assert len(heating_per_row) == n_rows and len(cooling_per_row) == n_rows
    assert len(total_consumption_per_row) == n_rows and len(total_intensity_per_row) == n_rows
    heating_cols = list(HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS)
    cooling_cols = list(COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS)
    data = {}
    for c in heating_cols:
        data[c] = heating_per_row
    for c in cooling_cols:
        data[c] = cooling_per_row
    data[TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[0]] = total_consumption_per_row
    data[TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[1]] = total_intensity_per_row
    return pl.DataFrame(data).lazy()


def test_replace_electricity_columns_constant() -> None:
    """Replace electricity columns (constant values) with neighbor average and adjust totals."""
    n_rows = 4
    original = _make_electricity_frame_constant(n_rows, 10.0, 5.0, 100.0, 1.0)
    neighbor1 = _make_electricity_frame_constant(n_rows, 2.0, 1.0, 50.0, 0.5)
    neighbor2 = _make_electricity_frame_constant(n_rows, 4.0, 2.0, 60.0, 0.6)

    out = _replace_electricity_columns(original, [neighbor1, neighbor2])
    df = cast(pl.DataFrame, out.collect())

    assert df.shape[0] == n_rows
    assert df["out.electricity.heating.energy_consumption"].to_list() == [3.0, 3.0, 3.0, 3.0]
    assert df[TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[0]].to_list() == [65.0, 65.0, 65.0, 65.0]


def test_replace_electricity_columns_time_varying() -> None:
    """Replace electricity columns with time-varying heating/cooling; multiple neighbors, verify row-wise average and total adjustment."""
    n_rows = 24  # 24 hours
    hour = np.arange(n_rows, dtype=np.float64)
    # Original: heating 10+sin(h), cooling 5+0.5*cos(h), total = 100 + 2*sin(h)
    orig_heating = (10.0 + np.sin(hour)).tolist()
    orig_cooling = (5.0 + 0.5 * np.cos(hour)).tolist()
    orig_total = (100.0 + 2.0 * np.sin(hour)).tolist()
    orig_intensity = (1.0 + 0.01 * np.cos(hour)).tolist()

    n1_heating = (2.0 + 0.2 * np.sin(hour)).tolist()
    n1_cooling = (1.0 + 0.1 * np.cos(hour)).tolist()
    n2_heating = (4.0 + 0.3 * np.sin(hour)).tolist()
    n2_cooling = (2.0 + 0.15 * np.cos(hour)).tolist()

    original = _make_electricity_frame_time_varying(n_rows, orig_heating, orig_cooling, orig_total, orig_intensity)
    neighbor1 = _make_electricity_frame_time_varying(
        n_rows, n1_heating, n1_cooling, [50.0] * n_rows, [0.5] * n_rows
    )
    neighbor2 = _make_electricity_frame_time_varying(
        n_rows, n2_heating, n2_cooling, [60.0] * n_rows, [0.6] * n_rows
    )

    out = _replace_electricity_columns(original, [neighbor1, neighbor2])
    df = cast(pl.DataFrame, out.collect())

    # Heating consumption (first col) = (n1_heating + n2_heating) / 2 per row
    expected_heating = [(a + b) / 2 for a, b in zip(n1_heating, n2_heating)]
    assert df["out.electricity.heating.energy_consumption"].to_list() == pytest.approx(expected_heating)
    # Cooling first col
    expected_cooling = [(a + b) / 2 for a, b in zip(n1_cooling, n2_cooling)]
    assert df["out.electricity.cooling.energy_consumption"].to_list() == pytest.approx(expected_cooling)

    # Total: orig_total - sum(orig heating consumption cols) - sum(orig cooling consumption cols) + avg_heating_sum + avg_cooling_sum
    # 4 heating consumption cols, 2 cooling. orig_heating_sum = 4*orig_heating, orig_cooling_sum = 2*orig_cooling
    # avg_heating_sum = 4 * expected_heating, avg_cooling_sum = 2 * expected_cooling
    orig_heating_sum = [4 * x for x in orig_heating]
    orig_cooling_sum = [2 * x for x in orig_cooling]
    avg_heating_sum = [4 * x for x in expected_heating]
    avg_cooling_sum = [2 * x for x in expected_cooling]
    expected_total = [
        o - oh - oc + ah + ac
        for o, oh, oc, ah, ac in zip(orig_total, orig_heating_sum, orig_cooling_sum, avg_heating_sum, avg_cooling_sum)
    ]
    assert df[TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS[0]].to_list() == pytest.approx(expected_total)


def test_replace_heating_cooling_load_columns() -> None:
    """Replace heating and cooling load columns with neighbor average; time-varying loads."""
    n_rows = 168  # one week hourly
    hour = np.arange(n_rows, dtype=np.float64)
    original = pl.DataFrame({
        HEATING_LOAD_COLUMN: (10.0 + 3.0 * np.sin(2 * np.pi * hour / 24)).tolist(),
        COOLING_LOAD_COLUMN: (5.0 + 2.0 * np.cos(2 * np.pi * hour / 24)).tolist(),
    }).lazy()
    neighbor1 = pl.DataFrame({
        HEATING_LOAD_COLUMN: (2.0 + 0.5 * np.sin(2 * np.pi * hour / 24)).tolist(),
        COOLING_LOAD_COLUMN: (1.0 + 0.3 * np.cos(2 * np.pi * hour / 24)).tolist(),
    }).lazy()
    neighbor2 = pl.DataFrame({
        HEATING_LOAD_COLUMN: (4.0 + 1.0 * np.sin(2 * np.pi * hour / 24)).tolist(),
        COOLING_LOAD_COLUMN: (3.0 + 0.8 * np.cos(2 * np.pi * hour / 24)).tolist(),
    }).lazy()
    neighbor3 = pl.DataFrame({
        HEATING_LOAD_COLUMN: (1.0 + 0.2 * np.sin(2 * np.pi * hour / 24)).tolist(),
        COOLING_LOAD_COLUMN: (2.0 + 0.5 * np.cos(2 * np.pi * hour / 24)).tolist(),
    }).lazy()

    out = _replace_heating_cooling_load_columns(original, [neighbor1, neighbor2, neighbor3])
    df = cast(pl.DataFrame, out.collect())

    n1_h = (2.0 + 0.5 * np.sin(2 * np.pi * hour / 24)).tolist()
    n2_h = (4.0 + 1.0 * np.sin(2 * np.pi * hour / 24)).tolist()
    n3_h = (1.0 + 0.2 * np.sin(2 * np.pi * hour / 24)).tolist()
    expected_heating = [(a + b + c) / 3 for a, b, c in zip(n1_h, n2_h, n3_h)]
    assert df[HEATING_LOAD_COLUMN].to_list() == pytest.approx(expected_heating)

    n1_c = (1.0 + 0.3 * np.cos(2 * np.pi * hour / 24)).tolist()
    n2_c = (3.0 + 0.8 * np.cos(2 * np.pi * hour / 24)).tolist()
    n3_c = (2.0 + 0.5 * np.cos(2 * np.pi * hour / 24)).tolist()
    expected_cooling = [(a + b + c) / 3 for a, b, c in zip(n1_c, n2_c, n3_c)]
    assert df[COOLING_LOAD_COLUMN].to_list() == pytest.approx(expected_cooling)


def _make_natural_gas_frame(n_rows: int, h_consumption: float, h_intensity: float, total_consumption: float, total_intensity: float) -> pl.LazyFrame:
    data = {
        HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[0]: [h_consumption] * n_rows,
        HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[1]: [h_intensity] * n_rows,
        HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[2]: [0.0] * n_rows,
        HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[3]: [0.0] * n_rows,
        TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[0]: [total_consumption] * n_rows,
        TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[1]: [total_intensity] * n_rows,
    }
    return pl.DataFrame(data).lazy()


def test_replace_natural_gas_columns() -> None:
    """Replace natural gas heating columns with neighbor average and adjust totals."""
    n_rows = 4
    original = _make_natural_gas_frame(n_rows, h_consumption=20.0, h_intensity=2.0, total_consumption=80.0, total_intensity=3.0)
    neighbor1 = _make_natural_gas_frame(n_rows, h_consumption=4.0, h_intensity=0.4, total_consumption=30.0, total_intensity=0.3)
    neighbor2 = _make_natural_gas_frame(n_rows, h_consumption=6.0, h_intensity=0.6, total_consumption=40.0, total_intensity=0.4)

    out = _replace_natural_gas_columns(original, [neighbor1, neighbor2])
    df = cast(pl.DataFrame, out.collect())

    assert df[HEATING_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[0]].to_list() == [5.0, 5.0, 5.0, 5.0]
    assert df[TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[0]].to_list() == [65.0, 65.0, 65.0, 65.0]
    assert df[TOTAL_ENERGY_CONSUMPTION_NATURAL_GAS_COLUMNS[1]].to_list() == [1.5, 1.5, 1.5, 1.5]


def _make_fuel_oil_frame(n_rows: int, h_consumption: float, h_intensity: float, total_consumption: float, total_intensity: float) -> pl.LazyFrame:
    data = {
        HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[0]: [h_consumption] * n_rows,
        HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[1]: [h_intensity] * n_rows,
        HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[2]: [0.0] * n_rows,
        HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[3]: [0.0] * n_rows,
        TOTAL_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[0]: [total_consumption] * n_rows,
        TOTAL_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[1]: [total_intensity] * n_rows,
    }
    return pl.DataFrame(data).lazy()


def test_replace_fuel_oil_columns() -> None:
    """Replace fuel oil heating columns with neighbor average and adjust totals."""
    n_rows = 4
    original = _make_fuel_oil_frame(n_rows, h_consumption=15.0, h_intensity=1.5, total_consumption=50.0, total_intensity=0.5)
    neighbor1 = _make_fuel_oil_frame(n_rows, h_consumption=3.0, h_intensity=0.3, total_consumption=20.0, total_intensity=0.2)

    out = _replace_fuel_oil_columns(original, [neighbor1])
    df = cast(pl.DataFrame, out.collect())

    assert df[HEATING_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[0]].to_list() == [3.0, 3.0, 3.0, 3.0]
    assert df[TOTAL_ENERGY_CONSUMPTION_FUEL_OIL_COLUMNS[0]].to_list() == [38.0, 38.0, 38.0, 38.0]


def _make_propane_frame(n_rows: int, h_consumption: float, h_intensity: float, total_consumption: float, total_intensity: float) -> pl.LazyFrame:
    data = {
        HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS[0]: [h_consumption] * n_rows,
        HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS[1]: [h_intensity] * n_rows,
        HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS[2]: [0.0] * n_rows,
        HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS[3]: [0.0] * n_rows,
        TOTAL_ENERGY_CONSUMPTION_PROPANE_COLUMNS[0]: [total_consumption] * n_rows,
        TOTAL_ENERGY_CONSUMPTION_PROPANE_COLUMNS[1]: [total_intensity] * n_rows,
    }
    return pl.DataFrame(data).lazy()


def test_replace_propane_columns() -> None:
    """Replace propane heating columns with neighbor average and adjust totals."""
    n_rows = 4
    original = _make_propane_frame(n_rows, h_consumption=12.0, h_intensity=1.2, total_consumption=45.0, total_intensity=0.45)
    neighbor1 = _make_propane_frame(n_rows, h_consumption=2.0, h_intensity=0.2, total_consumption=15.0, total_intensity=0.15)
    neighbor2 = _make_propane_frame(n_rows, h_consumption=4.0, h_intensity=0.4, total_consumption=25.0, total_intensity=0.25)

    out = _replace_propane_columns(original, [neighbor1, neighbor2])
    df = cast(pl.DataFrame, out.collect())

    assert df[HEATING_ENERGY_CONSUMPTION_PROPANE_COLUMNS[0]].to_list() == [3.0, 3.0, 3.0, 3.0]
    assert df[TOTAL_ENERGY_CONSUMPTION_PROPANE_COLUMNS[0]].to_list() == [36.0, 36.0, 36.0, 36.0]

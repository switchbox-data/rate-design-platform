"""Tests for NY utility assignment helpers (deterministic)."""

from typing import cast

import numpy as np
import pandas as pd
import polars as pl
import pytest

import geopandas as gpd
from shapely.geometry import box

from data.resstock.assign_utility_ny import (
    SMALL_GAS_UTILITIES,
    _calculate_prior_distributions,
    _calculate_utility_probabilities,
    _puma_id_series_for_join,
    _sample_utility_per_building,
    _zero_small_gas_utilities_and_renormalize,
)
from utils.utility_codes import get_ny_open_data_to_std_name


def _utility_name_map_lazy() -> pl.LazyFrame:
    """Build utility_name_map from central crosswalk (matches data.resstock.assign_utility_ny)."""
    return pl.DataFrame(
        [
            {"state_name": k, "std_name": v}
            for k, v in get_ny_open_data_to_std_name().items()
        ]
    ).lazy()


def test_calculate_utility_probabilities_basic():
    """Std_name mapping and pivot shape are correct; filter_none drops 'none' without renormalizing."""
    puma_overlap = pl.LazyFrame(
        {
            "puma_id": ["00100", "00100", "00100", "00200", "00200"],
            "utility": [
                "Consolidated Edison",
                "National Grid",
                "None",
                "Consolidated Edison",
                "NYS Electric and Gas",
            ],
            "pct_overlap": [60.0, 30.0, 10.0, 50.0, 50.0],
        }
    )
    utility_name_map = _utility_name_map_lazy()

    result = _calculate_utility_probabilities(
        puma_overlap,
        utility_name_map,
        handle_municipal=False,
        filter_none=True,
    )
    df = cast(pl.DataFrame, result.collect())

    assert "puma_id" in df.columns
    assert set(df.columns) >= {"puma_id", "coned", "nimo", "nyseg"}
    # filter_none=True removes "none" but does not renormalize: 00100 has coned+nimo = 0.6+0.3 = 0.9
    assert "none" not in df.columns
    row_00100 = df.filter(pl.col("puma_id") == "00100").to_dicts()[0]
    assert abs(row_00100["coned"] + row_00100["nimo"] - 0.9) < 1e-9
    # 00200 had no "none", so it still sums to 1
    row_00200 = df.filter(pl.col("puma_id") == "00200").to_dicts()[0]
    utility_cols = [c for c in df.columns if c != "puma_id"]
    assert abs(sum(row_00200[c] for c in utility_cols) - 1.0) < 1e-9


def test_calculate_utility_probabilities_filter_none_false():
    """With filter_none=False, 'none' utility is kept and gets probability."""
    puma_overlap = pl.LazyFrame(
        {
            "puma_id": ["00100", "00100"],
            "utility": ["Consolidated Edison", "None"],
            "pct_overlap": [70.0, 30.0],
        }
    )
    utility_name_map = _utility_name_map_lazy()

    result = _calculate_utility_probabilities(
        puma_overlap,
        utility_name_map,
        handle_municipal=False,
        filter_none=False,
    )
    df = cast(pl.DataFrame, result.collect())

    assert "none" in df.columns
    row = df.filter(pl.col("puma_id") == "00100").to_dicts()[0]
    assert row["coned"] == 0.7 and row["none"] == 0.3


def test_calculate_utility_probabilities_handle_municipal():
    """Municipal Utility: names are rewritten to muni-<lower> when include_muni=True."""
    puma_overlap = pl.LazyFrame(
        {
            "puma_id": ["00100", "00100"],
            "utility": ["Municipal Utility: Some City", "Consolidated Edison"],
            "pct_overlap": [40.0, 60.0],
        }
    )
    utility_name_map = _utility_name_map_lazy()

    result = _calculate_utility_probabilities(
        puma_overlap,
        utility_name_map,
        handle_municipal=True,
        filter_none=False,
        include_municipal=True,
    )
    df = cast(pl.DataFrame, result.collect())

    assert "muni-some city" in df.columns
    assert "coned" in df.columns


def test_calculate_prior_distributions_sums_to_one():
    """Electric and gas prior weighted dicts sum to 1 (when all PUMAs have buildings)."""
    puma_elec_probs = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200"],
            "coned": [1.0, 0.0],
            "nimo": [0.0, 1.0],
        }
    )
    puma_gas_probs = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200"],
            "coned": [1.0, 0.0],
            "none": [0.0, 1.0],
        }
    )
    # Gas prior is weighted by has_natgas_connection (3 with connection: bldgs 1,2,4)
    puma_and_heating_fuel = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "puma": ["00100", "00100", "00200", "00200"],
            "heating_fuel": ["Natural Gas", "Natural Gas", "Electric", "Natural Gas"],
            "has_natgas_connection": [True, True, False, True],
        }
    )

    elec_prior, gas_prior = _calculate_prior_distributions(
        puma_elec_probs, puma_gas_probs, puma_and_heating_fuel
    )

    assert abs(sum(elec_prior.values()) - 1.0) < 1e-9
    assert abs(sum(gas_prior.values()) - 1.0) < 1e-9
    assert "coned" in elec_prior and "nimo" in elec_prior
    # Gas: 3 buildings with has_natgas_connection; 2 in 00100 (coned), 1 in 00200 (none)
    assert "coned" in gas_prior and "none" in gas_prior


def test_calculate_prior_distributions_gas_only_has_natgas_connection():
    """Gas prior is weighted only by buildings with has_natgas_connection."""
    puma_elec_probs = pl.LazyFrame({"puma_id": ["00100"], "coned": [1.0]})
    puma_gas_probs = pl.LazyFrame({"puma_id": ["00100"], "coned": [1.0]})
    # Only one building and it has a gas connection
    puma_and_heating_fuel = pl.LazyFrame(
        {
            "bldg_id": [1],
            "puma": ["00100"],
            "heating_fuel": ["Natural Gas"],
            "has_natgas_connection": [True],
        }
    )

    _, gas_prior = _calculate_prior_distributions(
        puma_elec_probs, puma_gas_probs, puma_and_heating_fuel
    )
    assert gas_prior == {"coned": 1.0}

    # No buildings with gas connection -> gas_prior empty; total gas bldgs = 0 so weighted_prob is 0
    puma_and_heating_fuel_no_gas = pl.LazyFrame(
        {
            "bldg_id": [1],
            "puma": ["00100"],
            "heating_fuel": ["Electric"],
            "has_natgas_connection": [False],
        }
    )
    _, gas_prior_no_gas = _calculate_prior_distributions(
        puma_elec_probs, puma_gas_probs, puma_and_heating_fuel_no_gas
    )
    assert gas_prior_no_gas == {}


def test_sample_utility_per_building_deterministic():
    """Same inputs and seed yield the same assignments (deterministic)."""
    bldgs = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "puma": ["00100", "00100", "00200"],
            "heating_fuel": ["Natural Gas", "Natural Gas", "Natural Gas"],
        }
    )
    puma_probs = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200"],
            "coned": [0.7, 0.0],
            "nimo": [0.3, 0.0],
            "nyseg": [0.0, 1.0],
        }
    )

    out1 = cast(
        pl.DataFrame,
        _sample_utility_per_building(
            bldgs, puma_probs, "sb.electric_utility", only_when_fuel=None
        ).collect(),
    )
    out2 = cast(
        pl.DataFrame,
        _sample_utility_per_building(
            bldgs, puma_probs, "sb.electric_utility", only_when_fuel=None
        ).collect(),
    )

    assert out1.equals(out2)
    assert out1.columns == ["bldg_id", "sb.electric_utility"]
    assert out1.height == 3
    # All assigned (no only_when_fuel filter)
    assert out1["sb.electric_utility"].null_count() == 0
    # Values must be in the utility set
    assert set(out1["sb.electric_utility"].to_list()) <= {"coned", "nimo", "nyseg"}


def test_sample_utility_per_building_only_when_fuel():
    """With only_when_fuel='Natural Gas', buildings without has_natgas_connection get null utility."""
    bldgs = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "puma": ["00100", "00100"],
            "heating_fuel": ["Natural Gas", "Electric"],
            "has_natgas_connection": [True, False],
        }
    )
    puma_probs = pl.LazyFrame(
        {
            "puma_id": ["00100"],
            "coned": [1.0],
        }
    )

    out = cast(
        pl.DataFrame,
        _sample_utility_per_building(
            bldgs,
            puma_probs,
            "sb.gas_utility",
            only_when_fuel="Natural Gas",
        ).collect(),
    )

    assert out.columns == ["bldg_id", "sb.gas_utility"]
    # Building 1 (has_natgas_connection) gets a utility; building 2 (no connection) gets null
    assigned = out.filter(pl.col("sb.gas_utility").is_not_null())
    unassigned = out.filter(pl.col("sb.gas_utility").is_null())
    assert assigned.height == 1 and unassigned.height == 1
    assert assigned["bldg_id"][0] == 1 and unassigned["bldg_id"][0] == 2


def test_sample_utility_per_building_all_zero_probs_returns_none():
    """When a PUMA has no overlap (all zero probs), building gets null utility."""
    bldgs = pl.LazyFrame(
        {
            "bldg_id": [1],
            "puma": ["99999"],
            "heating_fuel": ["Natural Gas"],
        }
    )
    puma_probs = pl.LazyFrame(
        {
            "puma_id": ["00100"],
            "coned": [1.0],
        }
    )
    # Building in 99999 has no row in puma_probs (left join) -> null probs -> null utility
    out = cast(
        pl.DataFrame,
        _sample_utility_per_building(
            bldgs, puma_probs, "sb.electric_utility", only_when_fuel=None
        ).collect(),
    )
    assert out["sb.electric_utility"][0] is None


def test_sample_utility_per_building_exact_assignments():
    """With deterministic probs (1.0 per PUMA) and seed 42, exact bldg_id -> electric/gas is asserted."""
    # Each PUMA has a single utility with prob 1.0 so assignment is deterministic.
    # Gas assignment uses has_natgas_connection (bldg 2 has no connection -> null).
    bldgs = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "puma": ["00100", "00100", "00200", "00300"],
            "heating_fuel": ["Natural Gas", "Electric", "Natural Gas", "Natural Gas"],
            "has_natgas_connection": [True, False, True, True],
        }
    )
    puma_elec = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200", "00300"],
            "coned": [1.0, 0.0, 0.0],
            "nimo": [0.0, 1.0, 0.0],
            "nyseg": [0.0, 0.0, 1.0],
        }
    )
    puma_gas = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200", "00300"],
            "coned": [1.0, 0.0, 0.0],
            "nfg": [0.0, 1.0, 0.0],
            "none": [0.0, 0.0, 1.0],
        }
    )

    elec = cast(
        pl.DataFrame,
        _sample_utility_per_building(
            bldgs, puma_elec, "sb.electric_utility", only_when_fuel=None
        ).collect(),
    )
    gas = cast(
        pl.DataFrame,
        _sample_utility_per_building(
            bldgs, puma_gas, "sb.gas_utility", only_when_fuel="Natural Gas"
        ).collect(),
    )

    # Electric: every building gets the single utility for its PUMA
    elec_by_bldg = {
        row["bldg_id"]: row["sb.electric_utility"] for row in elec.iter_rows(named=True)
    }
    assert elec_by_bldg[1] == "coned"
    assert elec_by_bldg[2] == "coned"
    assert elec_by_bldg[3] == "nimo"
    assert elec_by_bldg[4] == "nyseg"

    # Gas: only buildings with has_natgas_connection get an assignment
    gas_by_bldg = {
        row["bldg_id"]: row["sb.gas_utility"] for row in gas.iter_rows(named=True)
    }
    assert gas_by_bldg[1] == "coned"  # PUMA 00100, has_natgas_connection
    assert gas_by_bldg[2] is None  # no gas connection -> no gas utility
    assert gas_by_bldg[3] == "nfg"  # PUMA 00200, has_natgas_connection
    assert gas_by_bldg[4] == "none"  # PUMA 00300, has_natgas_connection


def test_sample_utility_per_building_deterministic_with_varying_probs():
    """With varying probabilities (sum to 1) and seed 42, in-test assignment matches _sample_utility_per_building."""
    # More complex setup: 6 buildings across 3 PUMAs with 5 different utilities
    bldgs = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5, 6],
            "puma": ["00100", "00100", "00100", "00200", "00200", "00300"],
            "heating_fuel": [
                "Natural Gas",
                "Natural Gas",
                "Electric",
                "Natural Gas",
                "Natural Gas",
                "Natural Gas",
            ],
        }
    )
    # Complex probability distributions: PUMA 00100 has 4 utilities, 00200 has 3, 00300 has 2
    # Probabilities are more realistic (not simple fractions) and sum to 1 per PUMA
    puma_probs = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200", "00300"],
            "coned": [0.234, 0.0, 0.0],
            "nimo": [0.456, 0.0, 0.0],
            "nyseg": [0.189, 0.523, 0.0],
            "rge": [0.121, 0.312, 0.0],
            "or": [0.0, 0.165, 0.678],
            "psegli": [0.0, 0.0, 0.322],
        }
    )

    # Perform assignment directly in test with seed 42 (same logic as _sample_utility_per_building)
    bldgs_joined_df = cast(
        pl.DataFrame,
        bldgs.join(
            puma_probs, left_on="puma", right_on="puma_id", how="left"
        ).collect(),
    )
    bldgs_pd = bldgs_joined_df.to_pandas().sort_values("bldg_id").reset_index(drop=True)
    puma_probs_df = cast(pl.DataFrame, puma_probs.collect())
    utility_cols = sorted([c for c in puma_probs_df.columns if c != "puma_id"])

    np.random.seed(42)

    def sample_utility(row: pd.Series) -> str | None:
        probs = pd.to_numeric(row[utility_cols].values, errors="coerce").astype(float)
        if np.all(np.isnan(probs)) or np.sum(probs) == 0:
            return None
        probs = np.nan_to_num(probs, nan=0.0)
        probs = probs / np.sum(probs)
        return np.random.choice(utility_cols, size=1, replace=False, p=probs)[0]

    expected_utility = bldgs_pd.apply(sample_utility, axis=1)
    expected = dict(zip(bldgs_pd["bldg_id"], expected_utility, strict=True))

    # Function uses the same seed 42 internally; result must match
    out = cast(
        pl.DataFrame,
        _sample_utility_per_building(
            bldgs, puma_probs, "sb.electric_utility", only_when_fuel=None
        ).collect(),
    )
    actual = {
        row["bldg_id"]: row["sb.electric_utility"] for row in out.iter_rows(named=True)
    }

    assert actual == expected, f"Expected {expected}, got {actual}"


# ---------------------------------------------------------------------------
# SMALL_GAS_UTILITIES and _zero_small_gas_utilities_and_renormalize
# ---------------------------------------------------------------------------


def test_small_gas_utilities_constant():
    """SMALL_GAS_UTILITIES is the expected frozenset of small gas utility std names."""
    expected = {"bath", "chautauqua", "corning", "fillmore", "reserve", "stlaw"}
    assert SMALL_GAS_UTILITIES == frozenset(expected)


def test_puma_id_series_for_join_pumace10():
    """_puma_id_series_for_join returns 5-char zero-padded ids from PUMACE10."""
    gdf = gpd.GeoDataFrame(
        {"PUMACE10": [100, 200, 3600], "geometry": [box(0, 0, 1, 1)] * 3}
    )
    out = _puma_id_series_for_join(gdf)
    assert out is not None
    assert list(out) == ["00100", "00200", "03600"]


def test_puma_id_series_for_join_geoid():
    """_puma_id_series_for_join uses last 5 chars of GEOID when PUMACE10 missing."""
    gdf = gpd.GeoDataFrame(
        {"GEOID": ["3600100", "3600200"], "geometry": [box(0, 0, 1, 1)] * 2}
    )
    out = _puma_id_series_for_join(gdf)
    assert out is not None
    assert list(out) == ["00100", "00200"]


def test_puma_id_series_for_join_no_id_column_returns_none():
    """_puma_id_series_for_join returns None when neither PUMACE10 nor GEOID present."""
    gdf = gpd.GeoDataFrame({"other": [1], "geometry": [box(0, 0, 1, 1)]})
    assert _puma_id_series_for_join(gdf) is None


def test_zero_small_gas_utilities_no_small_cols_unchanged():
    """When gas probs have no columns in SMALL_GAS_UTILITIES, result is unchanged."""
    puma_gas_probs = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200"],
            "coned": [0.5, 0.0],
            "nyseg": [0.5, 1.0],
        }
    )
    out = _zero_small_gas_utilities_and_renormalize(puma_gas_probs)
    df = cast(pl.DataFrame, out.collect())
    assert df.shape == (2, 3)
    assert df.filter(pl.col("puma_id") == "00100").to_dicts()[0]["coned"] == 0.5
    assert df.filter(pl.col("puma_id") == "00100").to_dicts()[0]["nyseg"] == 0.5


def test_zero_small_gas_utilities_renormalize():
    """Small gas columns are zeroed and rows renormalized so each row sums to 1."""
    puma_gas_probs = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200"],
            "stlaw": [0.4, 0.0],
            "nyseg": [0.4, 0.7],
            "nimo": [0.2, 0.3],
        }
    )
    out = _zero_small_gas_utilities_and_renormalize(puma_gas_probs)
    df = cast(pl.DataFrame, out.collect())
    # 00100: stlaw zeroed, nyseg+nimo renormalized from 0.4+0.2 to sum 1
    row1 = df.filter(pl.col("puma_id") == "00100").to_dicts()[0]
    assert row1["stlaw"] == 0.0
    assert abs(row1["nyseg"] + row1["nimo"] - 1.0) < 1e-9
    assert abs(row1["nyseg"] - 0.4 / 0.6) < 1e-9
    # 00200: stlaw already 0, nyseg+nimo renormalized
    row2 = df.filter(pl.col("puma_id") == "00200").to_dicts()[0]
    assert row2["stlaw"] == 0.0
    assert abs(row2["nyseg"] + row2["nimo"] - 1.0) < 1e-9


def test_zero_small_gas_utilities_bad_puma_raises_without_pumas():
    """When a PUMA has only small gas utilities and pumas is None, raises ValueError."""
    puma_gas_probs = pl.LazyFrame(
        {
            "puma_id": ["00100"],
            "stlaw": [1.0],
        }
    )
    with pytest.raises(ValueError) as exc_info:
        _zero_small_gas_utilities_and_renormalize(puma_gas_probs, pumas=None)
    assert "00100" in str(exc_info.value)
    assert "small gas utilities" in str(exc_info.value).lower()


def test_zero_small_gas_utilities_bad_puma_uses_donor_with_pumas():
    """When a PUMA has only small gas utils, donor PUMA distribution is used (adjacent)."""
    # Two PUMAs: 00100 only stlaw (bad), 00200 has nyseg (good). Geometries touch.
    puma_gas_probs = pl.LazyFrame(
        {
            "puma_id": ["00100", "00200"],
            "stlaw": [1.0, 0.0],
            "nyseg": [0.0, 1.0],
        }
    )
    # Two boxes that share an edge (x=1): box(0,0,1,1) and box(1,0,2,1)
    pumas = gpd.GeoDataFrame(
        {
            "PUMACE10": ["00100", "00200"],
            "geometry": [box(0, 0, 1, 1), box(1, 0, 2, 1)],
        }
    )
    out = _zero_small_gas_utilities_and_renormalize(
        puma_gas_probs, pumas=pumas, puma_and_heating_fuel=None
    )
    df = cast(pl.DataFrame, out.collect())
    # 00100 should get donor 00200's row: stlaw=0, nyseg=1
    row_bad = df.filter(pl.col("puma_id") == "00100").to_dicts()[0]
    assert row_bad["stlaw"] == 0.0
    assert row_bad["nyseg"] == 1.0
    # Each row sums to 1
    for row in df.iter_rows(named=True):
        utility_cols = [c for c in row if c != "puma_id"]
        assert abs(sum(row[c] for c in utility_cols) - 1.0) < 1e-9

"""State-generic helpers for GIS-based utility assignment.

Functions in this module are reusable across any state that uses PUMA-level
probability tables to assign electric/gas utilities to ResStock buildings.
State-specific logic (e.g. excluded gas utilities, utility name maps) stays
in the state-specific modules (``assign_utility_ny.py``, etc.).

This module is intentionally *below* the orchestration layer
(``assign_utility.py``) and the state-specific modules in the import
hierarchy, so there are no circular-import concerns.

In addition to the probability/assignment helpers, this module provides
state-generic *fetch and cache* utilities for GIS data (PUMA shapefiles,
HIFLD utility boundaries).  State-specific fetch scripts (e.g.
``fetch_utility_boundary_md.py``) import these and pass state-specific
constants (state code, HIFLD URLs, CRS, etc.).
"""

from __future__ import annotations

import io
import os
import subprocess
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import cast

import geopandas as gpd
import httpx
import numpy as np
import pandas as pd
import polars as pl
import yaml
from cloudpathlib import S3Path
from pygris import pumas as pygris_pumas
from ruamel.yaml import YAML

from pygris import counties as pygris_counties

from data.resstock.constants import CONFIG_PATH, STATE_CONFIGS_PATH
from utils import get_aws_region


# ---------------------------------------------------------------------------
# GIS / I/O helpers
# ---------------------------------------------------------------------------


def read_csv_to_gdf_from_s3(
    s3_path: S3Path,
    utility_type: str,
    state_crs: int,
    geometry_col: str = "the_geom",
    crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """Read a CSV with WKT geometry from S3 and return a projected GeoDataFrame.

    Parameters
    ----------
    s3_path : S3Path
        S3Path to the CSV file (e.g., S3Path('s3://bucket/path/file.csv'))
    utility_type : str
        'electric' or 'gas'
    state_crs : int
        EPSG code of the state-specific projected CRS used for accurate area
        calculations (e.g. 2260 for NY State Plane).
    geometry_col : str, default 'the_geom'
        Name of the column containing WKT geometry
    crs : str, default 'EPSG:4326'
        Coordinate reference system for the source geometries
    """
    df = pd.read_csv(str(s3_path), low_memory=False)

    # Both casings appear in practice: NY source CSVs use COMP_FULL (uppercase);
    # HIFLD-fetched CSVs written by write_utilities_csv use comp_full (lowercase).
    _GAS_NAME_COL = "COMP_FULL" if "COMP_FULL" in df.columns else "comp_full"

    if utility_type == "electric":
        string_columns = {"comp_full", "utility", "state_name", "std_name"}
    elif utility_type == "gas":
        string_columns = {_GAS_NAME_COL, "utility", "state_name", "std_name"}
    else:
        string_columns: set[str] = set()

    for col in df.columns:
        if col != geometry_col and col not in string_columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except (ValueError, TypeError):
                pass

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.GeoSeries.from_wkt(df[geometry_col]), crs=crs
    )

    gdf = gdf.to_crs(epsg=state_crs)
    if utility_type == "electric":
        gdf = cast(gpd.GeoDataFrame, gdf.rename(columns={"comp_full": "utility"}))
    elif utility_type == "gas":
        gdf = cast(gpd.GeoDataFrame, gdf.rename(columns={_GAS_NAME_COL: "utility"}))

    return gdf


def load_county_boundaries(state: str, year: int, state_crs: int) -> gpd.GeoDataFrame:
    """Fetch Census county boundaries for ``state`` via pygris.

    Args:
        state: 2-letter state abbreviation (e.g. ``"MD"``).
        year: TIGER/Line vintage year (e.g. ``2019``).
        state_crs: EPSG code to project the result into.

    Returns:
        GeoDataFrame with a ``GEOID`` column (5-char county FIPS) projected
        to ``state_crs``.
    """
    return cast(
        gpd.GeoDataFrame,
        pygris_counties(state=state, year=year).to_crs(epsg=state_crs),
    )


def load_county_utility_weights(
    s3_path: str,
    eia_id_to_std_name: dict[int, str],
) -> pl.DataFrame:
    """Load pre-computed county service territory weights from S3.

    Applies an EIA utility ID -> standardised name mapping so downstream code
    works with short names (e.g. ``"bge"``) instead of full EIA names.
    Utilities not in the map fall back to their ``utility_name_eia``.

    Args:
        s3_path: Full S3 path to the service territory parquet
            (e.g. ``s3://data.sb/eia/861/service_territory/state=MD/data.parquet``).
        eia_id_to_std_name: Mapping from ``utility_id_eia`` (int) to
            standardised short name (str).

    Returns:
        DataFrame with columns ``county_id_fips``, ``utility`` (std name),
        and ``weight`` (float, sums to 1.0 per county).

    Raises:
        RuntimeError: If the S3 path cannot be read (likely the data hasn't
            been fetched yet).
    """
    region = get_aws_region()
    opts = {"region": region, "default_region": region}

    try:
        df = pl.read_parquet(s3_path, storage_options=opts)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read county service territory from {s3_path!r}. "
            "This data must be fetched before running utility assignment.\n"
            "  Run:  just -f data/eia/861/Justfile fetch-service-territory <STATE>"
        ) from exc

    name_map = pl.DataFrame(
        {
            "utility_id_eia": pl.Series(
                list(eia_id_to_std_name.keys()), dtype=pl.Int32
            ),
            "utility": pl.Series(list(eia_id_to_std_name.values()), dtype=pl.Utf8),
        }
    )

    df = (
        df.join(name_map, on="utility_id_eia", how="left")
        .with_columns(
            pl.col("utility").fill_null(pl.col("utility_name_eia")).alias("utility")
        )
        .select(["county_id_fips", "utility", "weight"])
    )

    return df


# ---------------------------------------------------------------------------
# PUMA–utility overlap
# ---------------------------------------------------------------------------


def calculate_puma_utility_overlap(
    pumas: gpd.GeoDataFrame,
    utility_gdf: gpd.GeoDataFrame,
    state_crs: int,
) -> pl.LazyFrame:
    """Calculate overlap between PUMAs and utility service territories.

    Returns a Polars LazyFrame with ``puma_id``, ``pct_overlap``, and all
    utility columns.
    """
    puma_overlap = pumas.to_crs(epsg=state_crs).copy()
    puma_overlap["puma_area"] = puma_overlap.geometry.area

    utility_polygons_transformed = utility_gdf.to_crs(epsg=state_crs)

    puma_overlap = gpd.overlay(
        puma_overlap, utility_polygons_transformed, how="intersection"
    )

    puma_overlap["overlap_area"] = puma_overlap.geometry.area
    puma_overlap["pct_overlap"] = (
        puma_overlap["overlap_area"] / puma_overlap["puma_area"] * 100
    ).astype(float)

    puma_overlap = puma_overlap.drop(columns=["geometry"])
    utility_cols = [col for col in puma_overlap.columns if "utility" in col.lower()]
    puma_overlap = puma_overlap[["PUMACE10", "pct_overlap"] + utility_cols].rename(
        columns={"PUMACE10": "puma_id"}
    )

    return pl.from_pandas(puma_overlap).lazy()


def calculate_puma_county_utility_overlap(
    pumas: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    county_utility_weights: pl.DataFrame,
    state_crs: int,
) -> pl.LazyFrame:
    """Calculate PUMA-utility overlap using county polygons with utility weights.

    An alternative to :func:`calculate_puma_utility_overlap` for states where
    sub-county utility boundary shapefiles are unavailable.  County polygons
    (Census TIGER/Line, e.g. from ``pygris.counties()``) are used as proxy
    boundaries.  When a county is served by multiple utilities, the
    PUMA–county intersection area is split proportionally using
    ``county_utility_weights``.

    The returned LazyFrame has the same columns as
    :func:`calculate_puma_utility_overlap` — ``puma_id``, ``utility``, and
    ``pct_overlap`` — so it plugs directly into
    :func:`calculate_utility_probabilities`.

    Args:
        pumas: Census PUMA GeoDataFrame (must contain a ``PUMACE10`` column).
        county_gdf: Census county GeoDataFrame (must contain a ``GEOID``
            column with 5-char FIPS strings, e.g. from
            ``pygris.counties(state="MD", year=2019)``).
        county_utility_weights: DataFrame with columns ``county_id_fips``
            (5-char FIPS string), ``utility`` (utility name), and ``weight``
            (float, sums to 1.0 per county).
        state_crs: EPSG code for the projected CRS used for area calculations.

    Returns:
        LazyFrame with columns ``puma_id`` (str), ``utility`` (str), and
        ``pct_overlap`` (float).  Values reflect weighted intersections;
        a PUMA whose area falls entirely within a single-utility county has
        ``pct_overlap ≈ 100`` for that utility.
    """
    pumas_proj = pumas[["PUMACE10", "geometry"]].to_crs(epsg=state_crs).copy()
    pumas_proj["puma_area"] = pumas_proj.geometry.area

    counties_proj = county_gdf[["GEOID", "geometry"]].to_crs(epsg=state_crs)

    # One row per (PUMA × county) intersection.
    intersection = gpd.overlay(pumas_proj, counties_proj, how="intersection")
    intersection["overlap_area"] = intersection.geometry.area

    overlap_df = pl.from_pandas(
        intersection[["PUMACE10", "GEOID", "puma_area", "overlap_area"]].rename(
            columns={"PUMACE10": "puma_id", "GEOID": "county_id_fips"}
        )
    )

    # Expand: each (PUMA, county) row fans out to one row per utility serving
    # that county, weighted by the utility's share of the county.
    weighted = overlap_df.join(
        county_utility_weights.select(["county_id_fips", "utility", "weight"]),
        on="county_id_fips",
        how="left",
    )

    weighted = weighted.with_columns(
        (pl.col("overlap_area") * pl.col("weight") / pl.col("puma_area") * 100).alias(
            "pct_overlap"
        )
    )

    # Sum across counties: a PUMA spanning multiple counties accumulates
    # weighted overlap area from each.
    result = (
        weighted.group_by(["puma_id", "utility"])
        .agg(pl.col("pct_overlap").sum())
        .sort("puma_id")
    )

    return result.lazy()


# ---------------------------------------------------------------------------
# Probability computation
# ---------------------------------------------------------------------------


def calculate_utility_probabilities(
    puma_overlap: pl.LazyFrame,
    utility_name_map: pl.LazyFrame,
    handle_municipal: bool = True,
    filter_none: bool = False,
    include_municipal: bool = False,
) -> pl.LazyFrame:
    """Calculate utility probabilities for each PUMA based on overlap percentages.

    Returns a wide-format LazyFrame with ``puma_id`` and one probability column
    per utility.
    """
    probs = puma_overlap.join(
        utility_name_map, left_on="utility", right_on="state_name", how="left"
    )
    probs = probs.with_columns(
        pl.col("std_name").fill_null(pl.col("utility")).alias("utility")
    ).drop("std_name")

    if handle_municipal:
        probs = probs.with_columns(
            pl.when(pl.col("utility").str.starts_with("Municipal Utility:"))
            .then(
                pl.concat_str(
                    [
                        pl.lit("muni-"),
                        pl.col("utility")
                        .str.replace("Municipal Utility:", "")
                        .str.strip_chars()
                        .str.to_lowercase(),
                    ]
                )
            )
            .otherwise(pl.col("utility"))
            .alias("utility")
        )

    probs = probs.group_by(["puma_id", "utility"]).agg(pl.col("pct_overlap").sum())

    probs = probs.with_columns(
        (pl.col("pct_overlap") / pl.col("pct_overlap").sum().over("puma_id")).alias(
            "probability"
        )
    )

    probs = probs.select(["puma_id", "utility", "probability"])

    if not include_municipal:
        probs = probs.filter(~pl.col("utility").str.contains("muni-"))

    if filter_none:
        probs = probs.filter(pl.col("utility") != "none")

    probs_collected = cast(pl.DataFrame, probs.collect())
    probs_pivoted = probs_collected.pivot(
        index="puma_id", on="utility", values="probability", aggregate_function=None
    )
    probs = probs_pivoted.fill_null(0).lazy()

    return probs


def calculate_prior_distributions(
    puma_elec_probs: pl.LazyFrame,
    puma_gas_probs: pl.LazyFrame,
    puma_and_heating_fuel: pl.LazyFrame,
) -> tuple[dict[str, float], dict[str, float]]:
    """Calculate building-count-weighted prior probability distributions.

    Returns ``(elec_prior_weighted, gas_prior_weighted)`` dictionaries mapping
    utility names to weighted probabilities (for later comparison with the
    posterior).
    """
    puma_counts = (
        puma_and_heating_fuel.group_by("puma").agg(pl.len().alias("count")).collect()
    )
    puma_elec_probs_df = cast(pl.DataFrame, puma_elec_probs.collect())
    elec_prior = cast(
        pl.DataFrame,
        (
            pl.LazyFrame(puma_elec_probs_df)
            .join(
                pl.LazyFrame(puma_counts),
                left_on="puma_id",
                right_on="puma",
                how="left",
            )
            .with_columns(pl.col("count").fill_null(0))
            .collect()
        ),
    )
    utility_cols_elec = [c for c in puma_elec_probs_df.columns if c != "puma_id"]
    elec_prior_weighted: dict[str, float] = {}
    total_bldgs = elec_prior["count"].sum()
    for util in utility_cols_elec:
        weighted_prob = (
            (elec_prior[util] * elec_prior["count"]).sum() / total_bldgs
            if total_bldgs > 0
            else 0
        )
        if weighted_prob > 0:
            elec_prior_weighted[util] = weighted_prob

    gas_bldgs = puma_and_heating_fuel.filter(pl.col("has_natgas_connection"))
    gas_puma_counts = gas_bldgs.group_by("puma").agg(pl.len().alias("count")).collect()
    puma_gas_probs_df = cast(pl.DataFrame, puma_gas_probs.collect())
    gas_prior = cast(
        pl.DataFrame,
        (
            pl.LazyFrame(puma_gas_probs_df)
            .join(
                pl.LazyFrame(gas_puma_counts),
                left_on="puma_id",
                right_on="puma",
                how="left",
            )
            .with_columns(pl.col("count").fill_null(0))
            .collect()
        ),
    )
    utility_cols_gas = [c for c in puma_gas_probs_df.columns if c != "puma_id"]
    gas_prior_weighted: dict[str, float] = {}
    total_gas_bldgs = gas_prior["count"].sum()
    for util in utility_cols_gas:
        weighted_prob = (
            (gas_prior[util] * gas_prior["count"]).sum() / total_gas_bldgs
            if total_gas_bldgs > 0
            else 0
        )
        if weighted_prob > 0:
            gas_prior_weighted[util] = weighted_prob

    return elec_prior_weighted, gas_prior_weighted


# ---------------------------------------------------------------------------
# PUMA ID helpers
# ---------------------------------------------------------------------------


def puma_id_series_for_join(pumas: gpd.GeoDataFrame) -> pd.Series | None:
    """Return a Series of 5-char zero-padded PUMA IDs suitable for joining."""
    if "PUMACE10" in pumas.columns:
        return pumas["PUMACE10"].astype(str).str.zfill(5)
    if "GEOID" in pumas.columns:
        return pumas["GEOID"].astype(str).str[-5:]
    return None


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


def sample_utility_per_building(
    bldgs: pl.LazyFrame,
    puma_probs: pl.LazyFrame,
    utility_col_name: str,
    only_when_fuel: str | None = None,
    seed: int = 42,
) -> pl.LazyFrame:
    """For each building, sample one utility from its PUMA's probability distribution.

    Args:
        bldgs: LazyFrame with ``bldg_id``, ``puma``, ``heating_fuel``.
        puma_probs: Wide-format LazyFrame with ``puma_id`` and probability
            columns for each utility.
        utility_col_name: Name for the output utility column.
        only_when_fuel: If provided, only sample when the building matches this
            fuel.  ``"Natural Gas"`` is treated specially: assignment is gated
            on ``has_natgas_connection`` rather than ``heating_fuel``.
        seed: Random seed for deterministic sampling.
    """
    bldgs_joined = bldgs.join(
        puma_probs, left_on="puma", right_on="puma_id", how="left"
    )

    puma_probs_df = cast(pl.DataFrame, puma_probs.collect())
    utility_cols = sorted([c for c in puma_probs_df.columns if c != "puma_id"])

    bldgs_joined_df = cast(pl.DataFrame, bldgs_joined.collect())
    bldgs_pd = bldgs_joined_df.to_pandas()

    bldgs_pd = bldgs_pd.sort_values("bldg_id").reset_index(drop=True)

    np.random.seed(seed)

    def _sample(row: pd.Series) -> str | None:
        if only_when_fuel is not None:
            if only_when_fuel == "Natural Gas":
                if not row["has_natgas_connection"]:
                    return None
            else:
                if row["heating_fuel"] != only_when_fuel:
                    return None

        probs = pd.to_numeric(row[utility_cols].values, errors="coerce").astype(float)

        if np.all(np.isnan(probs)) or np.sum(probs) == 0:
            return None

        probs = np.nan_to_num(probs, nan=0.0)
        probs = probs / np.sum(probs)

        sampled_utility = np.random.choice(
            utility_cols, size=1, replace=False, p=probs
        )[0]
        return sampled_utility

    bldgs_pd[utility_col_name] = bldgs_pd.apply(_sample, axis=1)

    result = pl.from_pandas(bldgs_pd[["bldg_id", utility_col_name]]).lazy()

    return result


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def print_comparison_summary(
    building_elec: pl.LazyFrame,
    building_gas: pl.LazyFrame,
    elec_prior_weighted: dict[str, float],
    gas_prior_weighted: dict[str, float],
    puma_and_heating_fuel: pl.LazyFrame,
) -> None:
    """Print prior-vs-posterior distribution comparison tables.

    Compares the expected probability distribution (prior, from PUMA overlap)
    with the actual assignment distribution (posterior, from sampling) to
    verify that sampling matches the intended probabilities.
    """

    def _table(
        utility_type: str,
        building_df: pl.LazyFrame,
        utility_col: str,
        prior_weighted: dict[str, float],
        filter_fuel_type: str | None = None,
    ) -> None:
        if filter_fuel_type is not None:
            if filter_fuel_type == "Natural Gas":
                building_df_filtered = building_df.join(
                    puma_and_heating_fuel.select("bldg_id", "has_natgas_connection"),
                    on="bldg_id",
                    how="left",
                ).filter(pl.col("has_natgas_connection"))
            else:
                building_df_filtered = building_df.join(
                    puma_and_heating_fuel.select("bldg_id", "heating_fuel"),
                    on="bldg_id",
                    how="left",
                ).filter(pl.col("heating_fuel") == filter_fuel_type)
        else:
            building_df_filtered = building_df

        building_filtered_df = cast(pl.DataFrame, building_df_filtered.collect())
        posterior_df = (
            building_filtered_df.group_by(utility_col)
            .agg(pl.len().alias("count"))
            .with_columns((pl.col("count") / pl.col("count").sum()).alias("proportion"))
            .sort("proportion", descending=True)
        )

        all_utils = sorted(
            set(
                list(prior_weighted.keys())
                + [r[utility_col] for r in posterior_df.iter_rows(named=True)]
            ),
            key=lambda x: (x is None, str(x) if x is not None else ""),
        )

        total_buildings = building_filtered_df.height
        assigned_buildings = building_filtered_df.filter(
            pl.col(utility_col).is_not_null()
        ).height
        unassigned_buildings = building_filtered_df.filter(
            pl.col(utility_col).is_null()
        ).height

        if filter_fuel_type is not None:
            print(
                f"\nNote: Comparing only {filter_fuel_type} buildings "
                f"({assigned_buildings} assigned, {unassigned_buildings} unassigned out of {total_buildings} total)"
            )
        else:
            print(
                f"\nAssignment Statistics: {assigned_buildings} assigned, "
                f"{unassigned_buildings} unassigned (null) out of {total_buildings} total buildings"
            )

        comparisons = []
        differences: list[float] = []
        for util in all_utils:
            prior_prob = prior_weighted.get(util, 0.0) if util is not None else 0.0
            prior_pct = prior_prob * 100
            expected_count = prior_prob * total_buildings

            post_row = None
            for r in posterior_df.iter_rows(named=True):
                if util is None and r[utility_col] is None:
                    post_row = r
                    break
                elif util is not None and r[utility_col] == util:
                    post_row = r
                    break

            posterior_pct = post_row["proportion"] * 100 if post_row else 0.0
            actual_count = post_row["count"] if post_row else 0

            util_display = "None (unassigned)" if util is None else util
            diff_pct = posterior_pct - prior_pct
            diff_count = actual_count - expected_count
            differences.append(abs(diff_pct))
            comparisons.append(
                {
                    "utility": util_display,
                    "prior_pct": prior_pct,
                    "posterior_pct": posterior_pct,
                    "diff_pct": diff_pct,
                    "expected_count": expected_count,
                    "actual_count": actual_count,
                    "diff_count": diff_count,
                }
            )

        print(
            f"\n{utility_type} Utilities - Prior vs Posterior Comparison (Percentages):"
        )
        print("This table compares the expected probability distribution (prior) with")
        print(
            "the actual assignment distribution (posterior) to verify sampling accuracy.\n"
        )

        print(
            f"{'Utility':<30} {'Prior %':>12} {'Posterior %':>14} {'Difference %':>15}"
        )
        print("-" * 73)

        for comp in comparisons:
            diff_str = f"{comp['diff_pct']:+.2f}"
            print(
                f"{comp['utility']:<30} "
                f"{comp['prior_pct']:>11.2f}% "
                f"{comp['posterior_pct']:>13.2f}% "
                f"{diff_str:>14}"
            )

        print(f"\n{utility_type} Utilities - Expected vs Actual Building Counts:")
        print(f"Total buildings: {total_buildings}")
        print(f"{'Utility':<30} {'Expected':>12} {'Actual':>12} {'Difference':>15}")
        print("-" * 73)

        for comp in comparisons:
            diff_str = f"{comp['diff_count']:+.0f}"
            print(
                f"{comp['utility']:<30} "
                f"{comp['expected_count']:>11.0f} "
                f"{comp['actual_count']:>11.0f} "
                f"{diff_str:>14}"
            )

        if differences:
            max_diff = max(differences)
            avg_diff = sum(differences) / len(differences)
            mean_diff = avg_diff
            variance = sum((d - mean_diff) ** 2 for d in differences) / len(differences)
            std_diff = variance**0.5

            print("\nDifference Statistics (Percentages):")
            print(f"  Maximum absolute difference: {max_diff:.2f}%")
            print(f"  Average absolute difference: {avg_diff:.2f}%")
            print(f"  Standard deviation of differences: {std_diff:.2f}%")
            if max_diff > 5.0:
                print(
                    "\n  WARNING: Maximum absolute difference exceeds 5%. "
                    "Prior and posterior distributions may not match well."
                )

    print("\n" + "=" * 80)
    print("PRIOR vs POSTERIOR COMPARISON SUMMARY")
    print("=" * 80)

    _table("Electric", building_elec, "sb.electric_utility", elec_prior_weighted)

    _table(
        "Gas",
        building_gas,
        "sb.gas_utility",
        gas_prior_weighted,
        filter_fuel_type="Natural Gas",
    )

    print("\n" + "=" * 80 + "\n")


# ---------------------------------------------------------------------------
# Excluded-utility zeroing and renormalization
# ---------------------------------------------------------------------------


def fill_missing_puma_probabilities(
    puma_probs: pl.LazyFrame,
    pumas: gpd.GeoDataFrame,
    label: str = "utility",
) -> pl.LazyFrame:
    """Add rows for PUMAs that are absent from ``puma_probs`` due to coverage gaps.

    When a PUMA has no intersection with any utility polygon it receives no row
    in ``puma_probs``, which causes every building in that PUMA to be assigned
    ``None``.  For each such PUMA, the nearest donor PUMA that *does* have
    coverage is found (adjacent first; centroid distance as fallback) and its
    probability distribution is copied.

    Args:
        puma_probs: Wide-format LazyFrame with ``puma_id`` and one column per
            utility, as returned by :func:`calculate_utility_probabilities`.
        pumas: Census PUMA GeoDataFrame (any CRS; should match the one used to
            build ``puma_probs`` so distances are meaningful).
        label: Short descriptor used in log messages (e.g. ``"electric"``).

    Returns:
        ``puma_probs`` augmented with rows for previously missing PUMAs.
    """
    probs_df = cast(pl.DataFrame, puma_probs.collect())
    covered_ids: set[str] = set(
        probs_df["puma_id"].cast(pl.Utf8).str.zfill(5).to_list()
    )

    puma_id_col = puma_id_series_for_join(pumas)
    if puma_id_col is None:
        raise ValueError(
            "pumas GeoDataFrame has no PUMACE10 or GEOID column; "
            "cannot identify missing PUMAs."
        )
    pumas = pumas.copy()
    pumas["_puma_id"] = puma_id_col.values

    all_puma_ids: list[str] = pumas["_puma_id"].astype(str).str.zfill(5).tolist()
    missing_ids = sorted(set(all_puma_ids) - covered_ids)

    if not missing_ids:
        return puma_probs

    utility_cols = [c for c in probs_df.columns if c != "puma_id"]
    good_ids = list(covered_ids)

    pumas_geom = pumas.set_geometry("geometry")
    pumas_geom = cast(gpd.GeoDataFrame, pumas_geom[pumas_geom.geometry.notna()].copy())
    centroids = pumas_geom.geometry.centroid

    print(
        f"  {len(missing_ids)} PUMA(s) have no {label} coverage; "
        "applying nearest-neighbor fill ...",
        flush=True,
    )

    new_rows: list[pl.DataFrame] = []
    for missing_str in missing_ids:
        missing_idx = pumas_geom["_puma_id"].astype(str).str.zfill(5) == missing_str
        if not missing_idx.any():
            continue

        missing_geom = pumas_geom.loc[missing_idx, "geometry"].iloc[0]
        missing_centroid = centroids[missing_idx].iloc[0]

        # Prefer adjacent (touching boundary) donor; fall back to pure centroid dist.
        adjacent: list[str] = []
        for gid in good_ids:
            good_str = str(gid).zfill(5)
            good_idx = pumas_geom["_puma_id"].astype(str).str.zfill(5) == good_str
            if not good_idx.any():
                continue
            if missing_geom.touches(pumas_geom.loc[good_idx, "geometry"].iloc[0]):
                adjacent.append(good_str)

        candidates = adjacent if adjacent else [str(g).zfill(5) for g in good_ids]
        nn_note = "adjacent" if adjacent else "nearest by centroid"

        best_donor: str | None = None
        best_dist = float("inf")
        for cand in candidates:
            cand_idx = pumas_geom["_puma_id"].astype(str).str.zfill(5) == cand
            if not cand_idx.any():
                continue
            d = missing_centroid.distance(centroids[cand_idx].iloc[0])
            if d < best_dist:
                best_dist, best_donor = d, cand

        if best_donor is None:
            print(
                f"    WARNING: no donor found for PUMA {missing_str!r}; skipping.",
                flush=True,
            )
            continue

        # Flexible donor lookup — puma_id may be stored with or without zero-padding.
        donor_row = probs_df.filter(
            pl.col("puma_id").cast(pl.Utf8).str.zfill(5) == best_donor
        )
        if donor_row.height == 0:
            donor_row = probs_df.filter(pl.col("puma_id").cast(pl.Utf8) == best_donor)
        if donor_row.height == 0:
            print(
                f"    WARNING: donor PUMA {best_donor!r} not found in probs; skipping.",
                flush=True,
            )
            continue

        donor_vals = donor_row.select(utility_cols).row(0)
        new_rows.append(
            pl.DataFrame(
                {
                    "puma_id": [missing_str],
                    **{c: [donor_vals[i]] for i, c in enumerate(utility_cols)},
                }
            )
        )
        print(
            f"    PUMA {missing_str!r} → donor {best_donor!r} "
            f"({nn_note}, dist={best_dist:.0f})",
            flush=True,
        )

    if not new_rows:
        return puma_probs

    return pl.concat([probs_df, pl.concat(new_rows)]).lazy()


def zero_excluded_gas_utilities_and_renormalize(
    puma_gas_probs: pl.LazyFrame,
    excluded_utilities: frozenset[str],
    pumas: gpd.GeoDataFrame | None = None,
    puma_and_heating_fuel: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Set prior probability to zero for excluded gas utilities; renormalize.

    For each PUMA row, columns corresponding to *excluded_utilities* are set
    to 0, then the row is renormalized so probabilities sum to 1.  If any PUMA
    would have all gas utilities zeroed (no gas utility left with positive
    probability), either raises ``ValueError`` (when *pumas* is ``None``) or
    uses the nearest-neighbor PUMA's gas probability distribution (when *pumas*
    is provided).  When *puma_and_heating_fuel* is provided, prints how many
    gas buildings (``has_natgas_connection``) are in each affected PUMA.

    Args:
        puma_gas_probs: Wide-format LazyFrame with ``puma_id`` and gas
            utility probability columns.
        excluded_utilities: Set of utility column names whose prior
            probability should be zeroed.
        pumas: Census PUMA GeoDataFrame (needed for nearest-neighbor
            donor resolution).
        puma_and_heating_fuel: LazyFrame with ``bldg_id``, ``puma``,
            ``heating_fuel``, ``has_natgas_connection`` (for debug counts).
    """
    gas_probs_df = cast(pl.DataFrame, puma_gas_probs.collect())
    utility_cols = [c for c in gas_probs_df.columns if c != "puma_id"]
    excluded_cols = [c for c in utility_cols if c in excluded_utilities]

    if not excluded_cols:
        return puma_gas_probs

    gas_probs_df_before_zero = gas_probs_df.clone()

    gas_probs_df = gas_probs_df.with_columns(
        [pl.lit(0.0).alias(c) for c in excluded_cols]
    )

    row_sums = gas_probs_df.select(pl.sum_horizontal(utility_cols)).to_series()
    gas_probs_df = gas_probs_df.with_columns(row_sums.alias("_row_sum"))
    bad_mask = row_sums == 0

    if bad_mask.any():
        bad_puma_ids = gas_probs_df.filter(bad_mask)["puma_id"].to_list()
        if pumas is None:
            raise ValueError(
                "Zero gas probability for all utilities in PUMA(s) after excluding "
                f"excluded gas utilities {sorted(excluded_utilities)}. Affected puma_id(s): {bad_puma_ids}. "
                "Excluding these utilities leaves no gas utility with positive probability."
            )

        gas_bldg_counts: dict[str, int] = {}
        if puma_and_heating_fuel is not None:
            counts_df = cast(
                pl.DataFrame,
                puma_and_heating_fuel.filter(pl.col("has_natgas_connection"))
                .group_by("puma")
                .agg(pl.len().alias("n_bldgs"))
                .collect(),
            )
            for row in counts_df.iter_rows(named=True):
                puma_val = row["puma"]
                key = str(puma_val).zfill(5) if puma_val is not None else ""
                gas_bldg_counts[key] = int(row["n_bldgs"])

        puma_id_in_pumas = puma_id_series_for_join(pumas)
        if puma_id_in_pumas is None:
            raise ValueError(
                "pumas GeoDataFrame has no PUMACE10 or GEOID column; cannot find nearest neighbor."
            )
        pumas = pumas.copy()
        pumas["_puma_id"] = puma_id_in_pumas.values

        good_df = gas_probs_df.filter(~bad_mask)
        good_puma_ids = good_df["puma_id"].to_list()
        if not good_puma_ids:
            raise ValueError(
                "No PUMA has non-zero gas probability after excluding small utilities; "
                "cannot assign nearest neighbor."
            )

        pumas_geom = pumas.set_geometry("geometry")
        pumas_geom = pumas_geom[pumas_geom.geometry.notna()].copy()
        centroids = pumas_geom.geometry.centroid

        fixed_rows = []
        for bad_puma_id in bad_puma_ids:
            bad_str = str(bad_puma_id).zfill(5)
            bad_idx = pumas_geom["_puma_id"].astype(str) == bad_str
            if not bad_idx.any():
                raise ValueError(
                    f"Bad PUMA id {bad_puma_id!r} not found in pumas GeoDataFrame."
                )
            bad_geom = pumas_geom.loc[bad_idx, "geometry"].iloc[0]
            bad_centroid = centroids[bad_idx].iloc[0]

            adjacent_good: list[str] = []
            for good_puma_id in good_puma_ids:
                good_str = str(good_puma_id).zfill(5)
                good_idx = pumas_geom["_puma_id"].astype(str) == good_str
                if not good_idx.any():
                    continue
                good_geom = pumas_geom.loc[good_idx, "geometry"].iloc[0]
                if bad_geom.touches(good_geom):
                    adjacent_good.append(good_str)

            if adjacent_good:
                best_donor = None
                best_dist = float("inf")
                for good_str in adjacent_good:
                    good_idx = pumas_geom["_puma_id"].astype(str) == good_str
                    good_centroid = centroids[good_idx].iloc[0]
                    d = bad_centroid.distance(good_centroid)
                    if d < best_dist:
                        best_dist = d
                        best_donor = good_str
                nn_note = "adjacent (touching boundary)"
            else:
                best_donor = None
                best_dist = float("inf")
                for good_puma_id in good_puma_ids:
                    good_str = str(good_puma_id).zfill(5)
                    good_idx = pumas_geom["_puma_id"].astype(str) == good_str
                    if not good_idx.any():
                        continue
                    good_centroid = centroids[good_idx].iloc[0]
                    d = bad_centroid.distance(good_centroid)
                    if d < best_dist:
                        best_dist = d
                        best_donor = good_str
                nn_note = (
                    "no adjacent PUMA with gas; using nearest by centroid (fallback)"
                )

            if best_donor is None:
                raise ValueError(f"No donor PUMA found for bad PUMA {bad_puma_id!r}.")
            donor_row = good_df.filter(
                pl.col("puma_id").cast(pl.Utf8).str.zfill(5) == best_donor
            )
            if donor_row.height == 0:
                donor_row = good_df.filter(
                    pl.col("puma_id").cast(pl.Utf8) == best_donor
                )
            if donor_row.height == 0:
                donor_row = good_df.filter(pl.col("puma_id") == best_donor)
            if donor_row.height == 0:
                raise ValueError(f"Donor PUMA {best_donor!r} not found in good_df.")
            donor_vals = donor_row.select(utility_cols).row(0)
            fixed_rows.append(
                pl.DataFrame(
                    {
                        "puma_id": [bad_puma_id],
                        **{c: [donor_vals[i]] for i, c in enumerate(utility_cols)},
                        "_row_sum": [donor_row["_row_sum"][0]],
                    }
                )
            )

            orig_row_df = gas_probs_df_before_zero.filter(
                pl.col("puma_id").cast(pl.Utf8).str.zfill(5) == bad_str
            )
            if orig_row_df.height == 0:
                orig_row_df = gas_probs_df_before_zero.filter(
                    pl.col("puma_id").cast(pl.Utf8) == bad_str
                )
            if orig_row_df.height == 0:
                orig_row_df = gas_probs_df_before_zero.filter(
                    pl.col("puma_id") == bad_puma_id
                )
            orig_vals = (
                orig_row_df.select(utility_cols).row(0) if orig_row_df.height else None
            )
            orig_probs = (
                {utility_cols[i]: orig_vals[i] for i in range(len(utility_cols))}
                if orig_vals
                else {}
            )
            excluded_that_had_prob = [
                c for c in excluded_cols if orig_probs.get(c, 0.0) > 0
            ]
            small_probs_before = {c: orig_probs[c] for c in excluded_that_had_prob}
            donor_probs = {
                utility_cols[i]: donor_vals[i] for i in range(len(utility_cols))
            }
            prior_before_str = ", ".join(
                f"{u}={orig_probs.get(u, 0):.3f}"
                for u in utility_cols
                if orig_probs.get(u, 0) > 0
            )
            after_nn_str = ", ".join(
                f"{u}={donor_probs.get(u, 0):.3f}"
                for u in utility_cols
                if donor_probs.get(u, 0) > 0
            )
            row_sum_before = sum(orig_probs.get(u, 0) for u in utility_cols)

            n_bldgs = gas_bldg_counts.get(bad_str, 0)
            print(
                f"PUMA {bad_puma_id!r} had zero gas probability "
                f"after excluding utilities; using donor PUMA {best_donor!r} "
                f"({nn_note}; distance={best_dist:.0f} m). "
                f"Affected bldg_ids (gas buildings in this PUMA): {n_bldgs}."
            )
            print(
                f"  Excluded gas utilities zeroed in this PUMA: {excluded_that_had_prob}; "
                f"their prior probs before removal: {small_probs_before}."
            )
            print(
                f"  Prior (before removal): {prior_before_str} (row sum={row_sum_before:.3f})"
            )
            if set(excluded_that_had_prob) == set(
                u for u in utility_cols if orig_probs.get(u, 0) > 0
            ):
                print(
                    "  → So before the fix, all gas bldg_ids in this PUMA would have been "
                    f"assigned to one of {excluded_that_had_prob}."
                )
            print(f"  After nearest-neighbor approximation: {after_nn_str}")

        gas_probs_df = pl.concat([good_df, pl.concat(fixed_rows)])

    gas_probs_df = gas_probs_df.drop("_row_sum")

    row_sums = gas_probs_df.select(pl.sum_horizontal(utility_cols)).to_series()
    gas_probs_df = gas_probs_df.with_columns(
        [(pl.col(c) / row_sums).alias(c) for c in utility_cols]
    )
    return gas_probs_df.lazy()


# ---------------------------------------------------------------------------
# Full GIS-based utility assignment pipeline
# ---------------------------------------------------------------------------


def create_hh_utilities(
    puma_and_heating_fuel: pl.LazyFrame,
    electric_polygons: gpd.GeoDataFrame,
    gas_polygons: gpd.GeoDataFrame,
    pumas: gpd.GeoDataFrame,
    utility_name_map: pl.LazyFrame,
    state_crs: int,
    excluded_gas_utilities: frozenset[str] = frozenset(),
    fill_missing_pumas: bool = False,
) -> pl.LazyFrame:
    """Create a LazyFrame of households with their associated utilities.

    Uses Census PUMAs and utility service territory polygons to compute
    overlap, then assigns electric/gas utilities to ResStock buildings by
    sampling from PUMA-level probabilities.

    Args:
        puma_and_heating_fuel: LazyFrame with ``bldg_id``, ``puma``,
            ``heating_fuel``, ``has_natgas_connection``.
        electric_polygons: GeoDataFrame of electric utility service territories.
        gas_polygons: GeoDataFrame of gas utility service territories.
        pumas: GeoDataFrame of Census PUMAs.
        utility_name_map: LazyFrame mapping ``state_name`` → ``std_name``
            for standardising utility names from the polygon CSVs.
        state_crs: EPSG code of the projected CRS for area calculations.
        excluded_gas_utilities: Gas utility names whose prior probability
            should be zeroed before sampling (default: empty).
        fill_missing_pumas: When ``True``, PUMAs with no utility polygon
            coverage (absent from the overlap table) are filled using the
            nearest covered PUMA's distribution.  Use for states where the
            utility boundary data does not fully cover all PUMAs.
    """
    puma_elec_overlap = calculate_puma_utility_overlap(
        pumas, electric_polygons, state_crs
    )

    puma_gas_overlap = calculate_puma_utility_overlap(pumas, gas_polygons, state_crs)

    puma_elec_probs = calculate_utility_probabilities(
        puma_elec_overlap,
        utility_name_map,
        handle_municipal=True,
        filter_none=True,
    )

    puma_gas_probs = calculate_utility_probabilities(
        puma_gas_overlap,
        utility_name_map,
        handle_municipal=False,
        filter_none=False,
    )

    if fill_missing_pumas:
        puma_elec_probs = fill_missing_puma_probabilities(
            puma_elec_probs, pumas, label="electric"
        )
        puma_gas_probs = fill_missing_puma_probabilities(
            puma_gas_probs, pumas, label="gas"
        )

    if excluded_gas_utilities:
        puma_gas_probs = zero_excluded_gas_utilities_and_renormalize(
            puma_gas_probs,
            excluded_utilities=excluded_gas_utilities,
            pumas=pumas,
            puma_and_heating_fuel=puma_and_heating_fuel,
        )

    elec_prior_weighted, gas_prior_weighted = calculate_prior_distributions(
        puma_elec_probs, puma_gas_probs, puma_and_heating_fuel=puma_and_heating_fuel
    )

    building_elec = sample_utility_per_building(
        puma_and_heating_fuel, puma_elec_probs, "sb.electric_utility"
    )

    building_gas = sample_utility_per_building(
        puma_and_heating_fuel,
        puma_gas_probs,
        "sb.gas_utility",
        only_when_fuel="Natural Gas",
    )

    print_comparison_summary(
        building_elec,
        building_gas,
        elec_prior_weighted,
        gas_prior_weighted,
        puma_and_heating_fuel=puma_and_heating_fuel,
    )

    building_utilities = building_elec.join(
        building_gas.select("bldg_id", "sb.gas_utility"), on="bldg_id", how="left"
    )

    return building_utilities


# ---------------------------------------------------------------------------
# Configuration loaders
# ---------------------------------------------------------------------------

_CONFIG = yaml.safe_load(CONFIG_PATH.open())
S3_GIS_DIR: str = _CONFIG["paths"]["s3_gis_dir"]
S3_PUMAS_DIR: str = _CONFIG["paths"]["s3_pumas_dir"]
GIS_CACHE_DIR: str = _CONFIG["paths"]["gis_cache_dir"]

_USER_CACHE_DIR = Path.home() / ".cache" / "switchbox" / "gis"


def _resolve_writable_cache_dir(configured: Path) -> Path:
    """Return *configured* if it (or an ancestor) is writable, else a per-user fallback.

    The GIS cache directory in config.yaml may live on a shared EBS volume
    owned by a different user.  Rather than failing on ``mkdir``, we
    transparently fall back to ``~/.cache/switchbox/gis`` so PUMA shapefiles
    and utility CSVs are still cached across runs.
    """
    check = configured
    while not check.exists():
        check = check.parent
    if os.access(check, os.W_OK):
        return configured
    print(
        f"  NOTE: configured cache dir {configured} is not writable; "
        f"using {_USER_CACHE_DIR} instead.",
        flush=True,
    )
    return _USER_CACHE_DIR


# ---------------------------------------------------------------------------
# HIFLD source URLs
# ---------------------------------------------------------------------------
#
# The DHS HIFLD Open portal was shut down on 2025-08-26.  Datasets are
# mirrored by multiple hosts; callers try them in order.  Data vintage: 2022.
# These endpoints accept a STATE filter via paginate_arcgis_geojson, so they
# are fully state-generic.

# Electric Retail Service Territories
HIFLD_ELEC_URLS: list[str] = [
    (
        "https://maps.nccs.nasa.gov/mapping/rest/services/"
        "hifld_open/energy/MapServer/26/query"
    ),
    (
        "https://services3.arcgis.com/OYP7N6mAJJCyH6hd/ArcGIS/rest/services/"
        "Electric_Retail_Service_Territories_HIFLD/FeatureServer/0/query"
    ),
]

# Natural Gas LDC Service Territories
HIFLD_GAS_URLS: list[str] = [
    (
        "https://maps.nccs.nasa.gov/mapping/rest/services/"
        "hifld_open/energy/MapServer/29/query"
    ),
]

# DataLumos mirror for the HIFLD gas dataset (Cloudflare-protected; manual
# download only).  The ZIP contains a single shapefile covering all states.
HIFLD_GAS_DATALUMOS_URL: str = (
    "https://www.datalumos.org/datalumos/project/240245/version/V1/view"
)
HIFLD_GAS_ZIP_SHP: str = "NG_Service_Terr.shp"
# Expected local path for the manually downloaded DataLumos ZIP.
HIFLD_GAS_DATALUMOS_ZIP: Path = Path(
    "data/resstock/utility/zips/natural-gas-service-territories-shapefile.zip"
)


# ---------------------------------------------------------------------------
# HTTP / retry / ArcGIS pagination
# ---------------------------------------------------------------------------

_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF = [1.0, 3.0, 9.0]


def get_with_retry(url: str, params: dict) -> httpx.Response:
    """GET ``url`` with ``params``, retrying on network errors and 5xx."""
    last_exc: Exception | None = None
    for attempt in range(_RETRY_ATTEMPTS):
        try:
            resp = httpx.get(url, params=params, timeout=120)
            if resp.status_code < 500:
                resp.raise_for_status()
                return resp
            print(
                f"    [{attempt + 1}/{_RETRY_ATTEMPTS}] HTTP {resp.status_code} — "
                f"retrying in {_RETRY_BACKOFF[attempt]:.0f}s ...",
                flush=True,
            )
            last_exc = httpx.HTTPStatusError(
                f"HTTP {resp.status_code}", request=resp.request, response=resp
            )
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            print(
                f"    [{attempt + 1}/{_RETRY_ATTEMPTS}] {exc} — "
                f"retrying in {_RETRY_BACKOFF[attempt]:.0f}s ...",
                flush=True,
            )
            last_exc = exc
        if attempt < _RETRY_ATTEMPTS - 1:
            time.sleep(_RETRY_BACKOFF[attempt])
    raise RuntimeError(f"All {_RETRY_ATTEMPTS} attempts failed for {url}") from last_exc


def paginate_arcgis_geojson(
    base_url: str,
    state: str,
    record_count: int = 1000,
) -> gpd.GeoDataFrame:
    """Fetch all features for ``state`` from an ArcGIS query endpoint.

    Pages through results using ``resultOffset`` / ``resultRecordCount`` until
    a page returns fewer features than ``record_count``.
    """
    frames: list[gpd.GeoDataFrame] = []
    offset = 0
    while True:
        params: dict[str, str | int] = {
            "where": f"STATE='{state}'",
            "outFields": "*",
            "outSR": "4326",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": record_count,
        }
        resp = get_with_retry(base_url, params)
        chunk = gpd.read_file(io.BytesIO(resp.content))
        n = len(chunk)
        print(f"    Page offset={offset}: {n} features retrieved.", flush=True)
        if n > 0:
            frames.append(chunk)
        if n < record_count:
            break
        offset += record_count

    if not frames:
        raise RuntimeError(f"No features returned for STATE='{state}' from {base_url}")
    if len(frames) == 1:
        return frames[0]
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)


def fetch_geojson_with_fallback(
    urls: list[str],
    state: str,
    manual_fallback: str | None = None,
) -> gpd.GeoDataFrame:
    """Try each ArcGIS URL in order; return from the first that succeeds."""
    errors: list[tuple[str, Exception]] = []
    for url in urls:
        print(f"    Trying: {url}", flush=True)
        try:
            gdf = paginate_arcgis_geojson(url, state)
            print(f"    ✓ Success ({len(gdf)} features).", flush=True)
            return gdf
        except Exception as exc:  # noqa: BLE001
            print(f"    ✗ Failed: {exc}", flush=True)
            errors.append((url, exc))

    failed = "\n".join(f"  {u}: {e}" for u, e in errors)
    extra = f"\n{manual_fallback}" if manual_fallback else ""
    raise RuntimeError(f"All {len(urls)} endpoints failed:\n{failed}{extra}")


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def s3_cp(local: Path, s3_dest: str) -> None:
    """Copy a single local file to S3."""
    rc = subprocess.run(
        ["aws", "s3", "cp", str(local), s3_dest], check=False
    ).returncode
    if rc != 0:
        print(
            f"  WARNING: aws s3 cp exited with code {rc} for {local.name}.", flush=True
        )


def _s3_sync(src: str, dest: str) -> bool:
    """Sync *src* to *dest* using ``aws s3 sync`` (either can be an s3:// URI).

    Returns ``True`` on success, ``False`` on failure (prints a warning but
    does not raise).
    """
    rc = subprocess.run(["aws", "s3", "sync", src, dest], check=False).returncode
    if rc != 0:
        print(
            f"  WARNING: aws s3 sync {src} → {dest} exited with code {rc}.", flush=True
        )
    return rc == 0


# ---------------------------------------------------------------------------
# PUMA loading (local cache → S3 → pygris fallback)
# ---------------------------------------------------------------------------


def fetch_and_sync_pumas(
    state: str,
    puma_year: int,
    cache_dir: Path,
) -> gpd.GeoDataFrame:
    """Fetch PUMA boundaries via pygris, save locally, and sync to S3.

    Always fetches fresh from the Census API via pygris, writes the shapefile
    to ``cache_dir/pumas/<state>/<state_lower>_pumas.shp``, and syncs that
    directory to ``S3_PUMAS_DIR/state=<state>/``.

    Call this once when setting up a new state (alongside
    :func:`load_utility_boundaries`).  Subsequent runs should call
    :func:`load_pumas`, which reads the cached file rather than hitting Census.

    Args:
        state: 2-letter state abbreviation (e.g. ``"MD"``).
        puma_year: TIGER/Line vintage year (e.g. ``2019`` for 2010-def PUMAs).
        cache_dir: Root local GIS cache directory (e.g. ``Path(GIS_CACHE_DIR)``).

    Returns:
        GeoDataFrame of PUMA boundaries (unprojected; caller reprojects to
        the desired CRS).
    """
    puma_dir = cache_dir / "pumas" / state
    local_shp = puma_dir / f"{state.lower()}_pumas.shp"

    print(
        f"  Fetching {state} PUMA boundaries via pygris (year={puma_year}) ...",
        flush=True,
    )
    pumas = cast(gpd.GeoDataFrame, pygris_pumas(state=state, year=puma_year, cb=True))
    puma_dir.mkdir(parents=True, exist_ok=True)
    pumas.to_file(local_shp)
    print(f"    Saved → {local_shp}", flush=True)

    s3_dest = f"{S3_PUMAS_DIR.rstrip('/')}/state={state}/"
    print(f"    Syncing → {s3_dest}", flush=True)
    if not _s3_sync(str(puma_dir), s3_dest):
        raise RuntimeError(
            f"Failed to sync {state} PUMA shapefiles to S3 ({s3_dest}). "
            "The local cache was written but S3 was not updated. "
            "Fix AWS credentials or permissions and re-run fetch_and_sync_pumas()."
        )

    print(f"    {len(pumas)} PUMA polygons loaded.", flush=True)
    return pumas


def load_pumas(
    state: str,
    puma_year: int,
    cache_dir: Path,
) -> gpd.GeoDataFrame:
    """Load PUMA boundaries with fallback chain: local cache → S3 → pygris.

    1. **Local cache** — if ``cache_dir/pumas/<state>/<state_lower>_pumas.shp``
       exists, reads and returns it immediately.
    2. **S3** — syncs ``S3_PUMAS_DIR/state=<state>/`` down to the local cache
       directory; if the shapefile is present after the sync, reads and returns
       it.
    3. **pygris fallback** — fetches fresh via :func:`fetch_and_sync_pumas`,
       which also repopulates both local cache and S3 for future calls.

    Args:
        state: 2-letter state abbreviation (e.g. ``"MD"``).
        puma_year: TIGER/Line vintage year (e.g. ``2019`` for 2010-def PUMAs).
        cache_dir: Root local GIS cache directory (e.g. ``Path(GIS_CACHE_DIR)``).

    Returns:
        GeoDataFrame of PUMA boundaries (unprojected; caller reprojects to
        the desired CRS).
    """
    cache_dir = _resolve_writable_cache_dir(cache_dir)
    puma_dir = cache_dir / "pumas" / state
    local_shp = puma_dir / f"{state.lower()}_pumas.shp"

    # 1. Local cache
    if local_shp.exists():
        print(f"  Re-using cached {state} PUMA shapefile: {local_shp}", flush=True)
        pumas = cast(gpd.GeoDataFrame, gpd.read_file(str(local_shp)))
        print(f"    {len(pumas)} PUMA polygons loaded.", flush=True)
        return pumas

    # 2. S3
    s3_src = f"{S3_PUMAS_DIR.rstrip('/')}/state={state}/"
    print(f"  Local PUMA cache missing; trying S3: {s3_src}", flush=True)
    puma_dir.mkdir(parents=True, exist_ok=True)
    if _s3_sync(s3_src, str(puma_dir)) and local_shp.exists():
        print(f"    Downloaded from S3 → {local_shp}", flush=True)
        pumas = cast(gpd.GeoDataFrame, gpd.read_file(str(local_shp)))
        print(f"    {len(pumas)} PUMA polygons loaded.", flush=True)
        return pumas

    # 3. pygris fallback
    print("  S3 sync failed or empty; falling back to pygris ...", flush=True)
    return fetch_and_sync_pumas(state, puma_year, cache_dir)


# ---------------------------------------------------------------------------
# Utility boundary CSV loading (with HIFLD fetch + local cache)
# ---------------------------------------------------------------------------


def load_utility_boundaries(
    state: str,
    utility_type: str,
    cache_dir: Path,
    cached_filename: str,
    hifld_urls: list[str],
    hifld_fallback_fn: Callable[[], gpd.GeoDataFrame] | None = None,
) -> gpd.GeoDataFrame:
    """Load utility boundary polygons, fetching from HIFLD if not cached.

    Checks ``cache_dir/<cached_filename>`` first.  If ``cached_filename`` is
    empty or the file doesn't exist, fetches from HIFLD mirrors, writes a
    dated WKT CSV, uploads to S3, and returns the GeoDataFrame.

    Args:
        state: 2-letter state abbreviation.
        utility_type: ``"electric"`` or ``"gas"`` (used in log messages and
            the generated filename).
        cache_dir: Local directory for utility CSV files.
        cached_filename: Filename from state_configs.yaml (may be empty).
        hifld_urls: Ordered list of ArcGIS REST API URLs to try.
        hifld_fallback_fn: Optional callable that returns a GeoDataFrame if
            all HIFLD URLs fail (e.g. DataLumos ZIP reader for gas).

    Returns:
        GeoDataFrame with utility boundary polygons.
    """
    if cached_filename and (cache_dir / cached_filename).exists():
        print(
            f"  Re-using cached {state} {utility_type} utilities: {cached_filename}",
            flush=True,
        )
        return load_csv_as_gdf(cache_dir / cached_filename)

    if cached_filename:
        print(
            f"  Cached {state} {utility_type} filename {cached_filename!r} not found "
            "locally — fetching from HIFLD ...",
            flush=True,
        )
    else:
        print(
            f"  Fetching {state} {utility_type} utility territories from HIFLD ...",
            flush=True,
        )

    try:
        gdf = fetch_geojson_with_fallback(hifld_urls, state)
    except RuntimeError:
        if hifld_fallback_fn is not None:
            gdf = hifld_fallback_fn()
        else:
            raise

    today = date.today().strftime("%Y%m%d")
    csv_name = f"{state.lower()}_{utility_type}_utilities_{today}.csv"
    csv_path = cache_dir / csv_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    write_utilities_csv(gdf, csv_path, label=f"{state} {utility_type} utilities")

    s3_dest = f"{S3_GIS_DIR.rstrip('/')}/{csv_name}"
    print(f"    Uploading → {s3_dest}", flush=True)
    s3_cp(csv_path, s3_dest)

    return gdf


def load_csv_as_gdf(csv_path: Path) -> gpd.GeoDataFrame:
    """Load a WKT-geometry CSV as a GeoDataFrame (EPSG:4326)."""
    df = pd.read_csv(str(csv_path))
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.GeoSeries.from_wkt(df["the_geom"]),
        crs="EPSG:4326",
    )
    return cast(gpd.GeoDataFrame, gdf.drop(columns="the_geom"))


def write_utilities_csv(
    utilities: gpd.GeoDataFrame, out_path: Path, label: str = "utilities"
) -> None:
    """Write a utility GeoDataFrame as a WKT-geometry CSV.

    Column layout matches what ``read_csv_to_gdf_from_s3`` expects:
    ``comp_full`` (utility name) and ``the_geom`` (WKT in EPSG:4326).
    """
    df = utilities.copy().to_crs(epsg=4326)
    df["the_geom"] = df.geometry.to_wkt()
    df = df.drop(columns="geometry")
    if "NAME" in df.columns and "comp_full" not in df.columns:
        df = df.rename(columns={"NAME": "comp_full"})
    df.to_csv(str(out_path), index=False)
    print(f"    {label} CSV: {out_path} ({len(df)} rows).", flush=True)


# ---------------------------------------------------------------------------
# State config write-back
# ---------------------------------------------------------------------------


def update_state_config_filenames(
    state: str,
    electric_filename: str | None = None,
    gas_filename: str | None = None,
) -> None:
    """Write updated electric/gas polygon filenames into ``state_configs.yaml``.

    Uses ``ruamel.yaml`` to preserve existing comments and formatting.
    Only updates the fields that are provided (non-None).
    """
    ry = YAML()
    ry.preserve_quotes = True
    with STATE_CONFIGS_PATH.open() as f:
        doc = ry.load(f)

    kwargs = doc[state]["utility_assignment"]["kwargs"]
    if electric_filename is not None:
        kwargs["electric_poly_filename"] = electric_filename
    if gas_filename is not None:
        kwargs["gas_poly_filename"] = gas_filename

    with STATE_CONFIGS_PATH.open("w") as f:
        ry.dump(doc, f)

    updates = []
    if electric_filename is not None:
        updates.append(f"electric={electric_filename!r}")
    if gas_filename is not None:
        updates.append(f"gas={gas_filename!r}")
    print(f"  Updated state_configs.yaml [{state}]: {', '.join(updates)}", flush=True)


def latest_utility_csv_name(
    cache_dir: Path, state: str, utility_type: str
) -> str | None:
    """Return the name of the most recently written utility CSV in ``cache_dir``.

    Matches files of the form ``<state_lower>_<utility_type>_utilities_YYYYMMDD.csv``.
    Returns ``None`` if no matching file exists.
    """
    prefix = f"{state.lower()}_{utility_type}_utilities_"
    matches = sorted(cache_dir.glob(f"{prefix}*.csv"), reverse=True)
    return matches[0].name if matches else None

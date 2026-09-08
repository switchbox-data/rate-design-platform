"""Correct ResStock hourly load curves for ACH50 infiltration overestimation.

Uses the Chan et al. (2013) regression to compute a per-building benchmark ACH50
from vintage, climate zone, floor area, and building height. Then fits a weighted
least-squares regression within ResStock itself (kWh/sqft ~ ACH50, per heating-type
group × end-use) to measure the marginal kWh impact of each unit of ACH50. Finally,
scales each building's hourly end-use columns by ``(1 - frac)``, where:

    frac = slope × sqft × delta_ach50 / annual_enduse_kwh
    delta_ach50 = resstock_ach50 - chan_predicted_ach50

This is a non-destructive correction: it reads from an input release and writes
corrected hourly parquet to a new output release. Metadata and utility assignment
files are copied unchanged to the output release.

The correction addresses the systematic ACH50 overestimation in ResStock's LBNL
ResDB-derived assignments, which inflates simulated heating and cooling loads.

Configuration files
-------------------
1. ``data/resstock/config/chan_2013_coefficients.yaml``
   Full regression from Chan et al. Table 3: year_built, climate_zone,
   floor_area, and house_height coefficients, plus the NL→ACH50 constant.

2. ``data/resstock/config/ach50_correction/<state>.yaml``
   State-specific context: climate zone code, vintage→Chan year_built mapping,
   stories→height mapping, group classification conditions, and per-group
   end-use column mappings (fully config-driven).

Usage (from project root)::

    uv run python data/resstock/load_curve/correct_ach50_infiltration.py \\
        --path-local /ebs/data/nrel/resstock \\
        --path-s3 s3://data.sb/nrel/resstock \\
        --input-release res_2024_amy2018_2_sb \\
        --output-release res_2024_amy2018_2_sb_ach \\
        --state MD --upgrade-ids "00 01" \\
        --path-chan-coefficients data/resstock/config/chan_2013_coefficients.yaml \\
        --path-context-config data/resstock/config/ach50_correction/md.yaml \\
        --workers 50

References
----------
Chan, W.R., Joh, J., & Sherman, M.H. (2013). Analysis of air leakage measurements
of US houses. *Energy and Buildings*, 66, 616-625.
"""

from __future__ import annotations

import argparse
import logging
import math
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import yaml
from cloudpathlib import S3Path

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}

logger = logging.getLogger(__name__)


def _storage_options_for_path(path: Path | S3Path) -> dict[str, str]:
    """Return STORAGE_OPTIONS for S3 paths, empty dict for local Path."""
    return STORAGE_OPTIONS if isinstance(path, S3Path) else {}


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


@dataclass
class ChanCoefficients:
    """Chan et al. (2013) Table 3 regression coefficients."""

    year_built: dict[str, float]
    climate_zone: dict[str, float]
    floor_area_per_m2: float
    house_height_per_m: float
    nl_to_ach50_constant: float


@dataclass
class EndUseGroup:
    """One end-use group: the hourly columns to scale and the annual columns for WLS."""

    name: str
    hourly_consumption_cols: list[str]
    annual_kwh_cols: list[str]


@dataclass
class GroupDefinition:
    """A classification group with filter conditions and end-use mappings.

    ``conditions`` is a list of dicts (OR-of-AND): each dict maps a metadata
    column name to a required boolean value. A building matches if *any* dict
    in the list has *all* of its conditions satisfied.
    """

    name: str
    conditions: list[dict[str, bool]]
    enduse_groups: list[EndUseGroup]


@dataclass
class ContextConfig:
    """State-specific ACH50 correction context."""

    climate_zone: str
    vintage_mapping: dict[str, str]
    stories_to_height_m: dict[str, float]
    groups: list[GroupDefinition]
    ach50_column: str
    sqft_column: str
    vintage_column: str
    stories_column: str
    weight_column: str
    frac_clip_lower: float
    frac_clip_upper: float


def _load_chan_coefficients(path: str | Path) -> ChanCoefficients:
    """Load Chan et al. coefficients from YAML.

    The YAML stores full regression detail (beta, se, ci_95) per category.
    We extract only the beta values needed for prediction.
    """
    with open(path) as f:
        raw = yaml.safe_load(f)

    # year_built and climate_zone are dicts of {category: {beta, se, ci_95}}
    year_built_betas = {k: v["beta"] for k, v in raw["year_built"].items()}
    climate_zone_betas = {k: v["beta"] for k, v in raw["climate_zone"].items()}

    # Calibrated from Chan Section 5: NL ≈ 0.055 × ACH50 at H=3m (1-story).
    # C = 3.0^0.7 / 0.055 ≈ 39.3.  See resstock_ach.qmd for derivation.
    nl_to_ach50_constant = 3.0**0.7 / 0.055

    return ChanCoefficients(
        year_built=year_built_betas,
        climate_zone=climate_zone_betas,
        floor_area_per_m2=raw["floor_area"]["beta_area_per_m2"],
        house_height_per_m=raw["house_height"]["beta_h_per_m"],
        nl_to_ach50_constant=nl_to_ach50_constant,
    )


def _parse_group_conditions(raw_conditions: Any) -> list[dict[str, bool]]:
    """Parse group conditions from YAML into a list of AND-condition dicts.

    YAML formats supported:
      - dict ``{col: val, ...}`` → single AND clause → ``[{col: val, ...}]``
      - list ``[{col: val}, {col: val}]`` → OR of AND clauses
    """
    if isinstance(raw_conditions, dict):
        return [raw_conditions]
    if isinstance(raw_conditions, list):
        return [d for d in raw_conditions if isinstance(d, dict)]
    msg = f"Unexpected conditions type: {type(raw_conditions)}"
    raise ValueError(msg)


def _load_context_config(path: str | Path) -> ContextConfig:
    """Load state-specific ACH50 correction context from YAML."""
    with open(path) as f:
        raw = yaml.safe_load(f)

    groups: list[GroupDefinition] = []
    for group_name, group_raw in raw["groups"].items():
        conditions = _parse_group_conditions(group_raw["conditions"])

        enduse_groups: list[EndUseGroup] = []
        for eu_name, eu_raw in group_raw["enduse_groups"].items():
            enduse_groups.append(
                EndUseGroup(
                    name=eu_name,
                    hourly_consumption_cols=eu_raw["hourly_consumption_cols"],
                    annual_kwh_cols=eu_raw["annual_kwh_cols"],
                )
            )
        groups.append(GroupDefinition(name=group_name, conditions=conditions, enduse_groups=enduse_groups))

    return ContextConfig(
        climate_zone=raw["climate_zone"],
        vintage_mapping=raw["vintage_mapping"],
        stories_to_height_m={str(k): float(v) for k, v in raw["stories_to_height_m"].items()},
        groups=groups,
        ach50_column=raw.get("ach50_column", "in.infiltration"),
        sqft_column=raw.get("sqft_column", "in.sqft"),
        vintage_column=raw.get("vintage_column", "in.vintage"),
        stories_column=raw.get("stories_column", "in.geometry_stories"),
        weight_column=raw.get("weight_column", "weight"),
        frac_clip_lower=raw.get("frac_clip_lower", -1.0),
        frac_clip_upper=raw.get("frac_clip_upper", 1.0),
    )


# ---------------------------------------------------------------------------
# Group classification
# ---------------------------------------------------------------------------


def _matches_one_clause(row: dict[str, Any], clause: dict[str, bool]) -> bool:
    """Return True if *row* satisfies all key=value checks in *clause* (AND)."""
    for col, expected in clause.items():
        val = row.get(col)
        if val is None or bool(val) != bool(expected):
            return False
    return True


def _classify_building(row: dict[str, Any], groups: list[GroupDefinition]) -> str | None:
    """Classify a single building into a group. First match wins.

    ``conditions`` is OR-of-AND: if *any* clause dict is fully satisfied, the
    building belongs to that group.
    """
    for group in groups:
        for clause in group.conditions:
            if _matches_one_clause(row, clause):
                return group.name
    return None


def _classify_all_buildings(
    meta: pl.DataFrame,
    groups: list[GroupDefinition],
) -> pl.Series:
    """Return a string Series of group names (or None) aligned to *meta* rows."""
    condition_cols: set[str] = set()
    for g in groups:
        for clause in g.conditions:
            condition_cols.update(clause.keys())

    available = [c for c in condition_cols if c in meta.columns]
    rows = meta.select(available).to_dicts()

    assignments = [_classify_building(row, groups) for row in rows]
    return pl.Series("_group", assignments, dtype=pl.String)


# ---------------------------------------------------------------------------
# Chan et al. ACH50 prediction
# ---------------------------------------------------------------------------


def predict_ach50_chan(
    vintage_chan: str,
    sqft: float,
    height_m: float,
    climate_zone_beta: float,
    chan: ChanCoefficients,
) -> float:
    """Predict ACH50 for a single building using Chan et al. (2013) regression.

    ln(NL) = β_year + β_cz + β_area × A_m2 + β_h × H_m

    ACH50 ≈ C × NL / H^0.7
    """
    beta_year = chan.year_built.get(vintage_chan)
    if beta_year is None:
        return float("nan")

    area_m2 = sqft / 10.764
    ln_nl = beta_year + climate_zone_beta + chan.floor_area_per_m2 * area_m2 + chan.house_height_per_m * height_m
    nl = math.exp(ln_nl)
    return chan.nl_to_ach50_constant * nl / (height_m**0.7)


# ---------------------------------------------------------------------------
# WLS regression
# ---------------------------------------------------------------------------


@dataclass
class WLSResult:
    """Result of a weighted least-squares regression: y = intercept + slope * x."""

    intercept: float
    slope: float
    r_squared: float
    n: int


def weighted_least_squares(
    x: np.ndarray, y: np.ndarray, w: np.ndarray
) -> WLSResult:
    """Fit y = a + b*x by weighted least squares.

    Parameters
    ----------
    x, y : array-like
        Predictor and response (same length).
    w : array-like
        Non-negative weights (same length as x, y).

    Returns
    -------
    WLSResult with intercept, slope, r_squared, n.
    """
    sw = w.sum()
    if sw == 0 or len(x) < 2:
        return WLSResult(intercept=0.0, slope=0.0, r_squared=0.0, n=len(x))

    sx = (w * x).sum() / sw
    sy = (w * y).sum() / sw
    sxx = (w * x * x).sum() / sw - sx * sx
    sxy = (w * x * y).sum() / sw - sx * sy
    syy = (w * y * y).sum() / sw - sy * sy

    if sxx == 0:
        return WLSResult(intercept=sy, slope=0.0, r_squared=0.0, n=len(x))

    slope = sxy / sxx
    intercept = sy - slope * sx
    r_squared = (sxy**2) / (sxx * syy) if syy > 0 else 0.0
    return WLSResult(intercept=intercept, slope=slope, r_squared=r_squared, n=len(x))


# ---------------------------------------------------------------------------
# Building-level computation
# ---------------------------------------------------------------------------


@dataclass
class BuildingCorrection:
    """Per-building correction data for one end-use."""

    enduse_name: str
    annual_kwh: float
    frac: float
    delta_ach50: float
    slope: float


def _parse_ach50(val: Any) -> float:
    """Parse ACH50 from ResStock metadata.

    Values may be strings like '10 ACH50' or numeric.
    """
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(" ACH50", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return float("nan")


def _parse_sqft(val: Any) -> float:
    """Parse sqft from ResStock metadata.

    Handles numeric values and string representations.
    """
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return float("nan")


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------


def _build_building_table(
    meta: pl.DataFrame,
    annual: pl.DataFrame,
    chan: ChanCoefficients,
    ctx: ContextConfig,
) -> pl.DataFrame:
    """Build per-building table with ACH50, chan-predicted ACH50, delta, group, sqft, weight, and annual end-use kWh.

    If the metadata contains an ``approximated_hp_load`` column (set by the
    ``approximate_non_hp_load`` pipeline step), it is carried through as
    ``_approximated``.  Buildings flagged True had their HVAC load curves
    replaced with nearest-neighbour HP profiles, so their kWh↔ACH50
    relationship is artificial.  ``_fit_regressions`` excludes them from
    regression fitting but ``_compute_correction_fractions`` still applies
    the correction to them.
    """
    cz_beta = chan.climate_zone.get(ctx.climate_zone, 0.0)

    # Classify buildings into groups using config-driven conditions
    group_series = _classify_all_buildings(meta, ctx.groups)

    # Collect all annual columns we need (from all groups)
    annual_cols_needed: set[str] = set()
    for group_def in ctx.groups:
        for eu in group_def.enduse_groups:
            annual_cols_needed.update(eu.annual_kwh_cols)

    # Select and join metadata + annual
    meta_cols = [
        "bldg_id",
        ctx.ach50_column,
        ctx.sqft_column,
        ctx.vintage_column,
        ctx.stories_column,
    ]
    if ctx.weight_column in meta.columns:
        meta_cols.append(ctx.weight_column)
    if "approximated_hp_load" in meta.columns:
        meta_cols.append("approximated_hp_load")
    meta_select = meta.select([c for c in meta_cols if c in meta.columns])

    # Add the group assignment column
    meta_select = meta_select.with_columns(group_series)

    # Carry the approximated flag (default False when column is absent)
    if "approximated_hp_load" in meta_select.columns:
        meta_select = meta_select.rename({"approximated_hp_load": "_approximated"})
    else:
        meta_select = meta_select.with_columns(pl.lit(False).alias("_approximated"))

    annual_select_cols = ["bldg_id"] + [c for c in annual_cols_needed if c in annual.columns]
    annual_select = annual.select(annual_select_cols)

    df = meta_select.join(annual_select, on="bldg_id", how="inner")

    # Parse ACH50 and sqft
    ach50_parsed = [_parse_ach50(v) for v in df[ctx.ach50_column].to_list()]
    sqft_parsed = [_parse_sqft(v) for v in df[ctx.sqft_column].to_list()]
    vintage_list = df[ctx.vintage_column].to_list()
    stories_list = [str(v) for v in df[ctx.stories_column].to_list()]

    # Compute Chan-predicted ACH50 per building
    chan_ach50 = []
    for i in range(len(df)):
        vintage_str = str(vintage_list[i])
        chan_vintage = ctx.vintage_mapping.get(vintage_str)
        # Cap at 3 stories — Chan model was developed for single-family homes.
        # Formula: 2.5 * stories + 0.5 (matches notebook's stories_to_height_m).
        try:
            s = min(int(stories_list[i]), 3)
        except (ValueError, TypeError):
            s = 1
        height_m = 2.5 * s + 0.5
        if chan_vintage is None:
            chan_ach50.append(float("nan"))
        else:
            chan_ach50.append(predict_ach50_chan(chan_vintage, sqft_parsed[i], height_m, cz_beta, chan))

    df = df.with_columns(
        pl.Series("ach50", ach50_parsed),
        pl.Series("sqft", sqft_parsed),
        pl.Series("chan_ach50", chan_ach50),
    ).with_columns(
        # Sign convention: delta > 0 means ResStock overestimates infiltration.
        # Downstream: frac = slope * sqft * delta / kwh, factor = 1 - frac.
        # Positive delta → positive frac → factor < 1 → loads scaled DOWN.
        (pl.col("ach50") - pl.col("chan_ach50")).alias("delta_ach50"),
    )

    # Sanity check: Chan's thesis is that ResStock overestimates infiltration
    # for the existing housing stock, so the median delta should be positive.
    finite_deltas = df.filter(pl.col("delta_ach50").is_finite())["delta_ach50"]
    if finite_deltas.len() > 0:
        median_val = finite_deltas.median()
        median_delta = median_val if isinstance(median_val, (int, float)) else 0.0
        if median_delta <= 0:
            logger.warning(
                "Median delta_ach50 = %.2f (expected positive). "
                "Check sign convention: delta = resstock - chan.",
                median_delta,
            )

    return df


def _sum_annual_kwh_cols(
    sub: pl.DataFrame, annual_kwh_cols: list[str]
) -> np.ndarray:
    """Sum multiple annual kWh columns into a single array, skipping missing columns."""
    available = [c for c in annual_kwh_cols if c in sub.columns]
    if not available:
        return np.zeros(sub.height, dtype=np.float64)
    total = np.zeros(sub.height, dtype=np.float64)
    for c in available:
        total += sub[c].to_numpy().astype(np.float64)
    return total


def _fit_regressions(
    building_table: pl.DataFrame,
    ctx: ContextConfig,
    min_group_size: int = 30,
) -> dict[tuple[str, str], WLSResult]:
    """Fit WLS regression (kWh/sqft ~ ACH50) for each (group, enduse_name).

    Buildings with ``_approximated == True`` are excluded from regression
    fitting because their load curves were copied from HP neighbours (by
    ``approximate_non_hp_load``), so their kWh↔ACH50 relationship is
    artificial.  The correction is still *applied* to them via
    ``_compute_correction_fractions``.

    Groups with fewer than *min_group_size* usable observations get a
    zero-slope result and a log warning.

    Returns dict mapping (group_name, enduse_name) -> WLSResult.
    """
    results: dict[tuple[str, str], WLSResult] = {}

    for group_def in ctx.groups:
        sub = building_table.filter(pl.col("_group") == group_def.name)
        if sub.height == 0:
            continue

        # Exclude approximated buildings from regression fitting
        n_approx = sub.filter(pl.col("_approximated")).height
        sub_real = sub.filter(~pl.col("_approximated"))
        if n_approx > 0:
            logger.info(
                "  Group %r: %d/%d buildings are approximated (excluded from regression)",
                group_def.name,
                n_approx,
                sub.height,
            )

        ach50_arr = sub_real["ach50"].to_numpy().astype(np.float64)
        sqft_arr = sub_real["sqft"].to_numpy().astype(np.float64)
        weight_arr = (
            sub_real[ctx.weight_column].to_numpy().astype(np.float64)
            if ctx.weight_column in sub_real.columns
            else np.ones(sub_real.height, dtype=np.float64)
        )

        for eu in group_def.enduse_groups:
            annual_kwh = _sum_annual_kwh_cols(sub_real, eu.annual_kwh_cols)

            # kWh per sqft
            with np.errstate(divide="ignore", invalid="ignore"):
                kwh_per_sqft = np.where(sqft_arr > 0, annual_kwh / sqft_arr, 0.0)

            # Filter to finite, positive kWh/sqft values
            mask = np.isfinite(kwh_per_sqft) & (kwh_per_sqft > 0) & np.isfinite(ach50_arr) & (weight_arr > 0)
            if mask.sum() < min_group_size:
                logger.warning(
                    "  Group %r / %s: only %d usable observations (min %d) — skipping regression",
                    group_def.name,
                    eu.name,
                    int(mask.sum()),
                    min_group_size,
                )
                results[(group_def.name, eu.name)] = WLSResult(
                    intercept=0.0, slope=0.0, r_squared=0.0, n=int(mask.sum())
                )
                continue

            results[(group_def.name, eu.name)] = weighted_least_squares(
                ach50_arr[mask], kwh_per_sqft[mask], weight_arr[mask]
            )

    return results


def _compute_correction_fractions(
    building_table: pl.DataFrame,
    regressions: dict[tuple[str, str], WLSResult],
    ctx: ContextConfig,
) -> dict[int, list[BuildingCorrection]]:
    """Compute per-building correction fractions for each end-use.

    frac = clip(slope × sqft × delta_ach50 / annual_kwh, lower, upper)

    Returns dict mapping bldg_id -> list of BuildingCorrection.
    """
    # Build a lookup from group name to GroupDefinition for fast access
    group_lookup: dict[str, GroupDefinition] = {g.name: g for g in ctx.groups}

    bldg_ids = building_table["bldg_id"].to_list()
    groups_col = building_table["_group"].to_list()
    sqft_arr = building_table["sqft"].to_numpy().astype(np.float64)
    delta_ach50_arr = building_table["delta_ach50"].to_numpy().astype(np.float64)

    corrections: dict[int, list[BuildingCorrection]] = {}

    for i in range(len(bldg_ids)):
        bldg_id = bldg_ids[i]
        group_name = groups_col[i]
        sqft = sqft_arr[i]
        delta = delta_ach50_arr[i]
        bldg_corrections: list[BuildingCorrection] = []

        group_def = group_lookup.get(group_name) if group_name else None
        if group_def is None:
            corrections[bldg_id] = bldg_corrections
            continue

        for eu in group_def.enduse_groups:
            # Sum annual kWh for this enduse group
            annual_kwh = 0.0
            for col in eu.annual_kwh_cols:
                if col in building_table.columns:
                    annual_kwh += float(building_table[col][i])

            reg = regressions.get((group_name, eu.name))
            slope = reg.slope if reg is not None else 0.0

            if annual_kwh > 0 and np.isfinite(delta) and np.isfinite(sqft):
                raw_frac = slope * sqft * delta / annual_kwh
                frac = float(np.clip(raw_frac, ctx.frac_clip_lower, ctx.frac_clip_upper))
            else:
                frac = 0.0

            bldg_corrections.append(BuildingCorrection(
                enduse_name=eu.name,
                annual_kwh=annual_kwh,
                frac=frac,
                delta_ach50=delta,
                slope=slope,
            ))

        corrections[bldg_id] = bldg_corrections

    return corrections


# ---------------------------------------------------------------------------
# Hourly file processing
# ---------------------------------------------------------------------------


def _hourly_consumption_col_to_intensity(col: str) -> str:
    """Convert an energy_consumption column name to its intensity counterpart."""
    return col.replace(".energy_consumption", ".energy_consumption_intensity")


_FUEL_PREFIXES = ("out.electricity.", "out.natural_gas.", "out.fuel_oil.", "out.propane.")

# Columns to exclude when recomputing fuel totals
_ELECTRICITY_EXCLUDE_SUFFIXES = (".total.", ".net.", ".pv.")
_OTHER_FUEL_EXCLUDE_SUFFIX = ".total."


def _get_fuel_consumption_cols(
    hourly_schema: list[str], fuel_prefix: str
) -> list[str]:
    """Return all individual (non-total/net/pv) energy_consumption columns for a fuel."""
    if fuel_prefix == "out.electricity.":
        return [
            c for c in hourly_schema
            if c.startswith(fuel_prefix)
            and c.endswith(".energy_consumption")
            and not any(ex in c for ex in _ELECTRICITY_EXCLUDE_SUFFIXES)
        ]
    return [
        c for c in hourly_schema
        if c.startswith(fuel_prefix)
        and c.endswith(".energy_consumption")
        and _OTHER_FUEL_EXCLUDE_SUFFIX not in c
    ]


def _build_col_to_frac(
    bldg_corrections: list[BuildingCorrection],
    group_def: GroupDefinition | None,
) -> dict[str, float]:
    """Map each hourly consumption column to its correction fraction."""
    col_to_frac: dict[str, float] = {}
    if group_def is None:
        return col_to_frac
    for corr in bldg_corrections:
        for eu in group_def.enduse_groups:
            if eu.name == corr.enduse_name:
                for col in eu.hourly_consumption_cols:
                    col_to_frac[col] = corr.frac
                break
    return col_to_frac


def _recompute_fuel_totals(
    df: pl.DataFrame, schema_names: list[str]
) -> pl.DataFrame:
    """Recompute total columns for all fuels and site_energy."""
    for fuel_prefix in _FUEL_PREFIXES:
        total_col = f"{fuel_prefix}total.energy_consumption"
        total_intensity_col = f"{fuel_prefix}total.energy_consumption_intensity"
        individual_cols = _get_fuel_consumption_cols(schema_names, fuel_prefix)

        if total_col in schema_names and individual_cols:
            df = df.with_columns(
                pl.sum_horizontal([pl.col(c) for c in individual_cols]).alias(total_col)
            )
        if total_intensity_col in schema_names:
            individual_intensity = [
                _hourly_consumption_col_to_intensity(c)
                for c in individual_cols
                if _hourly_consumption_col_to_intensity(c) in schema_names
            ]
            if individual_intensity:
                df = df.with_columns(
                    pl.sum_horizontal([pl.col(c) for c in individual_intensity]).alias(total_intensity_col)
                )

    # Recompute site_energy.total = sum of all fuel totals
    site_total_col = "out.site_energy.total.energy_consumption"
    site_total_intensity = "out.site_energy.total.energy_consumption_intensity"
    fuel_total_cols = [f"{p}total.energy_consumption" for p in _FUEL_PREFIXES]
    fuel_total_cols = [c for c in fuel_total_cols if c in schema_names]

    if site_total_col in schema_names and fuel_total_cols:
        df = df.with_columns(
            pl.sum_horizontal([pl.col(c) for c in fuel_total_cols]).alias(site_total_col)
        )
    fuel_total_intensity_cols = [f"{p}total.energy_consumption_intensity" for p in _FUEL_PREFIXES]
    fuel_total_intensity_cols = [c for c in fuel_total_intensity_cols if c in schema_names]
    if site_total_intensity in schema_names and fuel_total_intensity_cols:
        df = df.with_columns(
            pl.sum_horizontal([pl.col(c) for c in fuel_total_intensity_cols]).alias(site_total_intensity)
        )

    return df


def _process_one_building(
    bldg_id: int,
    upgrade_id: str,
    input_hourly_dir: Path,
    output_hourly_dir: Path,
    bldg_corrections: list[BuildingCorrection],
    group_def: GroupDefinition | None,
) -> int:
    """Read one building's hourly parquet, apply corrections, write to output release.

    Returns the bldg_id on success.
    """
    input_path = input_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
    output_path = output_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"

    col_to_frac = _build_col_to_frac(bldg_corrections, group_def)

    if not col_to_frac:
        shutil.copy2(str(input_path), str(output_path))
        return bldg_id

    df = pl.read_parquet(str(input_path))
    schema_names = df.columns

    # Scale end-use columns by (1 - frac) and their intensity counterparts
    scale_exprs: list[pl.Expr] = []
    for col, frac in col_to_frac.items():
        factor = 1.0 - frac
        if col in schema_names:
            scale_exprs.append((pl.col(col) * factor).alias(col))
            intensity_col = _hourly_consumption_col_to_intensity(col)
            if intensity_col not in schema_names:
                msg = (
                    f"Building {bldg_id}: consumption column '{col}' has no matching "
                    f"intensity column '{intensity_col}'. This indicates data corruption."
                )
                raise ValueError(msg)
            scale_exprs.append((pl.col(intensity_col) * factor).alias(intensity_col))

    if scale_exprs:
        df = df.with_columns(scale_exprs)

    df = _recompute_fuel_totals(df, schema_names)

    # Drop out.electricity.net.* — it becomes stale after scaling end-uses
    # and CAIRO doesn't read it (computes grid_con = max(total - abs(pv), 0)).
    net_cols = [c for c in df.columns if c.startswith("out.electricity.net.")]
    if net_cols:
        df = df.drop(net_cols)

    df.write_parquet(str(output_path))
    return bldg_id


def _copy_metadata_and_utility(
    input_base: Path,
    output_base: Path,
    state: str,
    upgrade_id: str,
) -> None:
    """Copy metadata and utility assignment files from input to output release."""
    # Metadata
    input_meta_dir = input_base / "metadata" / f"state={state}" / f"upgrade={upgrade_id}"
    output_meta_dir = output_base / "metadata" / f"state={state}" / f"upgrade={upgrade_id}"
    if input_meta_dir.exists():
        output_meta_dir.mkdir(parents=True, exist_ok=True)
        for f in input_meta_dir.iterdir():
            if f.is_file():
                shutil.copy2(str(f), str(output_meta_dir / f.name))

    # Utility assignment (shared across upgrades)
    input_ua = input_base / "metadata_utility" / f"state={state}" / "utility_assignment.parquet"
    output_ua_dir = output_base / "metadata_utility" / f"state={state}"
    if input_ua.exists() and not (output_ua_dir / "utility_assignment.parquet").exists():
        output_ua_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(input_ua), str(output_ua_dir / "utility_assignment.parquet"))


# ---------------------------------------------------------------------------
# Report YAML
# ---------------------------------------------------------------------------


def _build_report(
    state: str,
    upgrade_id: str,
    input_release: str,
    output_release: str,
    regressions: dict[tuple[str, str], WLSResult],
    building_table: pl.DataFrame,
    corrections: dict[int, list[BuildingCorrection]],
    elapsed_s: float,
    ctx: ContextConfig,
) -> dict[str, Any]:
    """Build a lean methods-focused report for writeup reference.

    Three sections: sample sizes, regression coefficients, and aggregate
    correction fractions (keyed by group × enduse).
    """
    # ── Sample sizes ──────────────────────────────────────────────────────
    group_counts: dict[str, int] = {}
    for g in ctx.groups:
        sub = building_table.filter(pl.col("_group") == g.name)
        group_counts[g.name] = sub.height
        n_approx = sub.filter(pl.col("_approximated")).height
        if n_approx > 0:
            group_counts[f"{g.name}__approximated"] = n_approx
    group_counts["unclassified"] = building_table.filter(pl.col("_group").is_null()).height

    # ── Regressions ───────────────────────────────────────────────────────
    regression_report: dict[str, dict[str, object]] = {}
    for (group, enduse), reg in regressions.items():
        regression_report[f"{group}__{enduse}"] = {
            "slope": round(reg.slope, 6),
            "intercept": round(reg.intercept, 6),
            "r_squared": round(reg.r_squared, 4),
            "n": reg.n,
        }

    # ── Correction fractions (by group × enduse) ─────────────────────────
    # Bucket each building's correction by (group, enduse) so each group
    # gets its own summary rather than pooling across heating types.
    grouped_fracs: dict[str, list[float]] = {}
    bldg_groups = building_table.select("bldg_id", "_group").to_dict()
    bldg_to_group: dict[int, str | None] = dict(
        zip(bldg_groups["bldg_id"], bldg_groups["_group"])
    )
    for bldg_id, bldg_corrections in corrections.items():
        group_name = bldg_to_group.get(bldg_id)
        if group_name is None:
            continue
        for corr in bldg_corrections:
            key = f"{group_name}__{corr.enduse_name}"
            grouped_fracs.setdefault(key, []).append(corr.frac)

    frac_summary: dict[str, dict[str, object]] = {}
    for key, fracs in sorted(grouped_fracs.items()):
        arr = np.array(fracs)
        frac_summary[key] = {
            "mean": round(float(arr.mean()), 6),
            "median": round(float(np.median(arr)), 6),
            "std": round(float(arr.std()), 6),
            "min": round(float(arr.min()), 6),
            "max": round(float(arr.max()), 6),
        }

    return {
        "state": state,
        "upgrade_id": upgrade_id,
        "input_release": input_release,
        "output_release": output_release,
        "climate_zone": ctx.climate_zone,
        "elapsed_seconds": round(elapsed_s, 1),
        "n_buildings_total": len(building_table),
        "group_counts": group_counts,
        "regressions": regression_report,
        "correction_fractions": frac_summary,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_correction(
    *,
    path_local: Path,
    path_s3: str,
    input_release: str,
    output_release: str,
    state: str,
    upgrade_ids: list[str],
    path_chan_coefficients: str | Path,
    path_context_config: str | Path,
    workers: int = 50,
) -> None:
    """Run the full ACH50 correction pipeline."""
    chan = _load_chan_coefficients(path_chan_coefficients)
    ctx = _load_context_config(path_context_config)

    # Build group lookup for per-building processing
    group_lookup: dict[str, GroupDefinition] = {g.name: g for g in ctx.groups}

    state_upper = state.upper()

    for upgrade_id in upgrade_ids:
        print(f"\n{'='*70}")
        print(f"Processing state={state_upper} upgrade={upgrade_id}")
        print(f"{'='*70}")
        t0 = time.time()

        # ------------------------------------------------------------------
        # 1. Resolve input paths: metadata and annual from S3 or local
        # ------------------------------------------------------------------
        input_local = path_local / input_release
        output_local = path_local / output_release

        # Read metadata — prefer local, fall back to S3
        meta_dir = f"metadata/state={state_upper}/upgrade={upgrade_id}"
        meta_path_local = input_local / meta_dir / "metadata-sb.parquet"
        meta_path_s3 = f"{path_s3}/{input_release}/{meta_dir}/metadata-sb.parquet"
        if meta_path_local.exists():
            print(f"  Reading metadata from local: {meta_path_local}")
            meta = pl.read_parquet(str(meta_path_local))
        else:
            print(f"  Reading metadata from S3: {meta_path_s3}")
            meta = pl.read_parquet(meta_path_s3, storage_options=STORAGE_OPTIONS)

        # Read annual loads — single file per upgrade, prefer local
        annual_dir = f"load_curve_annual/state={state_upper}/upgrade={upgrade_id}"
        annual_filename = f"{state_upper}_upgrade{upgrade_id}_metadata_and_annual_results.parquet"
        annual_path_local = input_local / annual_dir / annual_filename
        annual_path_s3 = f"{path_s3}/{input_release}/{annual_dir}/{annual_filename}"
        if annual_path_local.exists():
            print(f"  Reading annual loads from local: {annual_path_local}")
            annual = pl.read_parquet(str(annual_path_local))
        else:
            print(f"  Reading annual loads from S3: {annual_path_s3}")
            annual = pl.read_parquet(annual_path_s3, storage_options=STORAGE_OPTIONS)

        print(f"  Metadata: {meta.height:,} buildings")
        print(f"  Annual loads: {annual.height:,} buildings")

        # ------------------------------------------------------------------
        # 2. Build per-building table with ACH50, chan prediction, etc.
        # ------------------------------------------------------------------
        building_table = _build_building_table(meta, annual, chan, ctx)
        finite_count = building_table.filter(pl.col("delta_ach50").is_finite()).height
        print(f"  Building table: {building_table.height:,} rows ({finite_count:,} with finite delta_ach50)")

        # Report group classification counts
        for g in ctx.groups:
            g_count = building_table.filter(pl.col("_group") == g.name).height
            print(f"    {g.name:25s}: {g_count:,}")
        unclassified = building_table.filter(pl.col("_group").is_null()).height
        if unclassified > 0:
            print(f"    {'unclassified':25s}: {unclassified:,}")

        # ------------------------------------------------------------------
        # 3. Fit WLS regressions
        # ------------------------------------------------------------------
        regressions = _fit_regressions(building_table, ctx)
        print(f"  Fitted {len(regressions)} regressions:")
        for (group, enduse), reg in sorted(regressions.items()):
            print(f"    {group:25s}  {enduse:20s}  slope={reg.slope:.5f}  R²={reg.r_squared:.3f}  n={reg.n}")

        # ------------------------------------------------------------------
        # 4. Compute per-building correction fractions
        # ------------------------------------------------------------------
        corrections = _compute_correction_fractions(building_table, regressions, ctx)

        # ------------------------------------------------------------------
        # 5. Process hourly files in parallel
        # ------------------------------------------------------------------
        input_hourly_dir = input_local / "load_curve_hourly" / f"state={state_upper}" / f"upgrade={upgrade_id}"
        output_hourly_dir = output_local / "load_curve_hourly" / f"state={state_upper}" / f"upgrade={upgrade_id}"
        output_hourly_dir.mkdir(parents=True, exist_ok=True)

        if not input_hourly_dir.exists():
            print(f"  ERROR: Input hourly directory does not exist: {input_hourly_dir}")
            sys.exit(1)

        bldg_ids_with_corrections = list(corrections.keys())

        # Find orphan buildings: present on disk but not in the building table
        # (e.g. buildings that were filtered out during the metadata/annual join).
        all_hourly_files = set(input_hourly_dir.glob(f"*-{int(upgrade_id)}.parquet"))
        hourly_bldg_ids_on_disk: set[int] = set()
        for f in all_hourly_files:
            try:
                bid = int(f.stem.split("-")[0])
                hourly_bldg_ids_on_disk.add(bid)
            except (ValueError, IndexError):
                pass

        orphan_bldg_ids = sorted(hourly_bldg_ids_on_disk - set(bldg_ids_with_corrections))
        n_orphans = len(orphan_bldg_ids)
        if n_orphans > 0:
            print(f"  WARNING: {n_orphans} orphan buildings on disk but not in building table. "
                  f"They will be copied unchanged.")
            print(f"    Orphan bldg_ids: {orphan_bldg_ids}")

        # Build bldg_id → group_name mapping for hourly processing
        bldg_group_map: dict[int, str | None] = dict(
            zip(
                building_table["bldg_id"].to_list(),
                building_table["_group"].to_list(),
            )
        )

        n_total = len(bldg_ids_with_corrections) + n_orphans
        print(f"  Processing {len(bldg_ids_with_corrections):,} buildings with corrections, "
              f"{n_orphans:,} copied unchanged ({n_total:,} total)")

        n_processed = 0

        def _process_corrected(bid: int) -> int:
            g_name = bldg_group_map.get(bid)
            return _process_one_building(
                bldg_id=bid,
                upgrade_id=upgrade_id,
                input_hourly_dir=input_hourly_dir,
                output_hourly_dir=output_hourly_dir,
                bldg_corrections=corrections[bid],
                group_def=group_lookup.get(g_name) if g_name else None,
            )

        def _copy_uncorrected(bid: int) -> int:
            src = input_hourly_dir / f"{bid}-{int(upgrade_id)}.parquet"
            dst = output_hourly_dir / f"{bid}-{int(upgrade_id)}.parquet"
            shutil.copy2(str(src), str(dst))
            return bid

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process_corrected, bid): bid for bid in bldg_ids_with_corrections
            }
            futures.update({
                executor.submit(_copy_uncorrected, bid): bid for bid in orphan_bldg_ids
            })

            for future in as_completed(futures):
                n_processed += 1
                future.result()
                if n_processed % 500 == 0 or n_processed == n_total:
                    print(f"    {n_processed:,} / {n_total:,} hourly files processed")

        # ------------------------------------------------------------------
        # 6. Copy metadata and utility assignment
        # ------------------------------------------------------------------
        _copy_metadata_and_utility(input_local, output_local, state_upper, upgrade_id)
        print(f"  Copied metadata and utility assignment to output release")

        # ------------------------------------------------------------------
        # 7. Write report YAML
        # ------------------------------------------------------------------
        elapsed_s = time.time() - t0
        report = _build_report(
            state=state_upper,
            upgrade_id=upgrade_id,
            input_release=input_release,
            output_release=output_release,
            regressions=regressions,
            building_table=building_table,
            corrections=corrections,
            elapsed_s=elapsed_s,
            ctx=ctx,
        )

        # Write to rate_design/hp_rates/{state}/config/load_adj/
        project_root = Path(__file__).resolve().parents[3]
        report_dir = project_root / "rate_design" / "hp_rates" / state_upper.lower() / "config" / "load_adj"
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"ach50_correction_report_u{upgrade_id}.yaml"
        with open(report_path, "w") as f:
            yaml.dump(report, f, default_flow_style=False, sort_keys=False)
        print(f"  Report written to: {report_path}")
        print(f"  Completed in {elapsed_s:.1f}s")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Correct ResStock hourly load curves for ACH50 infiltration overestimation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--path-local",
        type=Path,
        required=True,
        help="Local base path for ResStock data (e.g. /ebs/data/nrel/resstock)",
    )
    parser.add_argument(
        "--path-s3",
        type=str,
        required=True,
        help="S3 base path for ResStock data (e.g. s3://data.sb/nrel/resstock)",
    )
    parser.add_argument(
        "--input-release",
        type=str,
        required=True,
        help="Input release name (e.g. res_2024_amy2018_2_sb)",
    )
    parser.add_argument(
        "--output-release",
        type=str,
        required=True,
        help="Output release name (e.g. res_2024_amy2018_2_sb_ach)",
    )
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        help="State abbreviation (e.g. MD)",
    )
    parser.add_argument(
        "--upgrade-ids",
        type=str,
        required=True,
        help="Space-separated upgrade IDs (e.g. '00 01')",
    )
    parser.add_argument(
        "--path-chan-coefficients",
        type=str,
        required=True,
        help="Path to chan_2013_coefficients.yaml",
    )
    parser.add_argument(
        "--path-context-config",
        type=str,
        required=True,
        help="Path to state-specific ach50_correction/<state>.yaml",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Number of parallel workers for hourly file processing (default: 50)",
    )

    args = parser.parse_args()

    run_correction(
        path_local=args.path_local,
        path_s3=args.path_s3,
        input_release=args.input_release,
        output_release=args.output_release,
        state=args.state,
        upgrade_ids=args.upgrade_ids.split(),
        path_chan_coefficients=args.path_chan_coefficients,
        path_context_config=args.path_context_config,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()

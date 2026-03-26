"""Per-utility demand-flex elasticity calibration for heat-pump rate design.

For each utility, loads real TOU derivation data, ResStock HP building loads,
and marginal cost profiles to compute:
  - Demand shift at each candidate elasticity (kWh moved, peak reduction %)
  - Rate arbitrage savings (the primary bill savings mechanism)
  - MC-based RR reduction savings (secondary, small)
  - Arcturus "no enabling tech" comparison
  - Recommended elasticity per utility

Operates purely analytically (no CAIRO run required). Results can be validated
against actual CAIRO bill outputs from batch ny_20260325b_r1-16.

Usage:
    uv run python -m utils.pre.calibrate_demand_flex_elasticity \
        --state ny \
        --output-dir /tmp/demand_flex_diagnostic

    # Fast dev iteration with sampled buildings:
    uv run python -m utils.pre.calibrate_demand_flex_elasticity \
        --state ny --sample-size 50 --output-dir /tmp/demand_flex_diagnostic
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import polars as pl
import yaml

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.cairo import (
    _build_period_consumption,
    _build_period_shift_targets,
    _compute_equivalent_flat_tariff,
    assign_hourly_periods,
    extract_tou_period_rates,
)
from utils.pre.compute_tou import SeasonTouSpec, load_season_specs
from utils.scenario_config import get_residential_customer_count_from_utility_stats

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

WINTER_MONTHS = {1, 2, 3, 10, 11, 12}
SUMMER_MONTHS = {4, 5, 6, 7, 8, 9}

ARCTURUS_NO_TECH_INTERCEPT = -0.011
ARCTURUS_NO_TECH_SLOPE = -0.065

ARCTURUS_WITH_TECH_INTERCEPT = -0.011
ARCTURUS_WITH_TECH_SLOPE = -0.111


def _build_epsilon_range(start: float, end: float, step: float) -> list[float]:
    """Build a list of epsilon values from start to end (inclusive) by step."""
    values: list[float] = []
    current = start
    while current >= end - 1e-9:
        values.append(round(current, 4))
        current += step
    return values


STATE_CONFIGS: dict[str, dict] = {
    "ny": {
        "utilities": ("cenhud", "coned", "nimo", "nyseg", "or", "psegli", "rge"),
        "path_metadata": Path(
            "/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/metadata/state=NY/"
            "upgrade=00/metadata-sb.parquet"
        ),
        "path_utility_assignment": Path(
            "/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/metadata_utility/"
            "state=NY/utility_assignment.parquet"
        ),
        "path_loads_dir": Path(
            "/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/load_curve_hourly/"
            "state=NY/upgrade=00"
        ),
        "path_tou_derivation_dir": Path(
            "rate_design/hp_rates/ny/config/tou_derivation"
        ),
        "path_tariffs_electric_dir": Path(
            "rate_design/hp_rates/ny/config/tariffs/electric"
        ),
        "path_rev_requirement_dir": Path(
            "rate_design/hp_rates/ny/config/rev_requirement"
        ),
        "mc_base": "s3://data.sb/switchbox/marginal_costs/ny",
        "mc_year": 2025,
        "path_electric_utility_stats": "s3://data.sb/eia/861/electric_utility_stats/year=2024/state=NY/data.parquet",
        "path_periods_dir": Path("rate_design/hp_rates/ny/config/periods"),
    },
}


# ── Data structures ───────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class UtilityContext:
    """All data needed to diagnose one utility."""

    utility: str
    season_specs: list[SeasonTouSpec]
    tou_tariff: dict
    hp_bldg_ids: list[int]
    hp_weights: dict[int, float]
    total_weighted_customers: float
    hp_weighted_customers: float
    delivery_rr: float = 0.0


@dataclass(slots=True)
class SeasonResult:
    """Diagnostic results for one utility × season × elasticity."""

    utility: str
    season: str
    elasticity: float
    price_ratio: float
    base_rate: float
    peak_hours: list[int]
    n_peak_hours_per_day: int
    hp_bldg_count: int
    hp_weighted_share: float
    hp_peak_kwh_orig: float
    hp_peak_kwh_shifted: float
    peak_reduction_pct: float
    kwh_shifted: float
    system_peak_reduction_pct: float
    p_flat: float
    arcturus_peak_reduction_pct: float
    delta_vs_arcturus_pct: float
    rate_arbitrage_savings_per_hp: float
    delivery_mc_savings_total: float
    delivery_mc_savings_per_hp: float
    delivery_mc_nonzero_hours: int
    delivery_mc_peak_overlap_hours: int


@dataclass(slots=True)
class SeasonRecommendation:
    """Per-season elasticity recommendation."""

    season: str
    recommended_elasticity: float
    arcturus_target_pct: float
    peak_reduction_pct: float
    price_ratio: float
    savings_per_hp: float


@dataclass(slots=True)
class UtilityResult:
    """Aggregated diagnostic results for one utility."""

    utility: str
    season_results: list[SeasonResult] = field(default_factory=list)
    annual_weighted_ratio: float = 0.0
    annual_arcturus_pct: float = 0.0
    recommended_elasticity: float = 0.0
    recommended_annual_savings: float = 0.0
    seasonal_recommendations: list[SeasonRecommendation] = field(default_factory=list)
    seasonal_recommendations_with_tech: list[SeasonRecommendation] = field(
        default_factory=list
    )
    delivery_rr: float = 0.0
    rr_decrease_by_elasticity: dict[float, float] = field(default_factory=dict)


# ── Arcturus model ────────────────────────────────────────────────────────────


def arcturus_peak_reduction(price_ratio: float) -> float:
    """Arcturus 2.0 'no enabling technology' peak reduction prediction.

    Returns a positive fraction (e.g. 0.056 for 5.6% reduction).
    """
    if price_ratio <= 1.0:
        return 0.0
    return abs(
        ARCTURUS_NO_TECH_INTERCEPT + ARCTURUS_NO_TECH_SLOPE * math.log(price_ratio)
    )


def arcturus_peak_reduction_with_tech(price_ratio: float) -> float:
    """Arcturus 2.0 'with enabling technology' peak reduction prediction.

    Returns a positive fraction (e.g. 0.13 for 13% reduction).
    Roughly 2x the no-tech response at the same ratio.
    """
    if price_ratio <= 1.0:
        return 0.0
    return abs(
        ARCTURUS_WITH_TECH_INTERCEPT + ARCTURUS_WITH_TECH_SLOPE * math.log(price_ratio)
    )


# ── Data loading ──────────────────────────────────────────────────────────────


def load_hp_metadata(
    path_metadata: Path,
    path_utility_assignment: Path,
) -> pl.DataFrame:
    """Load metadata and return HP buildings with utility assignment and weight."""
    meta = pl.read_parquet(path_metadata).select(
        "bldg_id", "postprocess_group.has_hp", "weight"
    )
    util = pl.read_parquet(path_utility_assignment).select(
        "bldg_id", "sb.electric_utility"
    )
    joined = meta.join(util, on="bldg_id", how="inner")
    hp = joined.filter(pl.col("postprocess_group.has_hp") == True)  # noqa: E712
    log.info(
        "Loaded %d HP buildings out of %d total",
        hp.height,
        joined.height,
    )
    return hp


def load_building_loads(
    bldg_ids: list[int],
    loads_dir: Path,
    sample_size: int | None = None,
) -> pd.DataFrame:
    """Load hourly electric net load for a set of buildings from local parquet.

    Returns a pandas DataFrame with MultiIndex (bldg_id, time) and column
    'electricity_net' in kWh, matching CAIRO's expected format.
    """
    if sample_size and sample_size < len(bldg_ids):
        rng = np.random.default_rng(42)
        bldg_ids = list(rng.choice(bldg_ids, size=sample_size, replace=False))

    frames: list[pd.DataFrame] = []
    missing = 0
    for bid in bldg_ids:
        path = loads_dir / f"{bid}-0.parquet"
        if not path.exists():
            missing += 1
            continue
        df = pl.read_parquet(
            path,
            columns=["timestamp", "out.electricity.net.energy_consumption"],
        ).to_pandas()
        df = df.rename(
            columns={
                "timestamp": "time",
                "out.electricity.net.energy_consumption": "electricity_net",
            }
        )
        df["bldg_id"] = bid
        frames.append(df)

    if missing:
        log.warning("Missing load files for %d of %d buildings", missing, len(bldg_ids))
    if not frames:
        raise FileNotFoundError(
            f"No load files found in {loads_dir} for {len(bldg_ids)} buildings"
        )

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.set_index(["bldg_id", "time"]).sort_index()
    log.info(
        "Loaded %d buildings x %d hours = %d rows",
        len(frames),
        8760,
        len(combined),
    )
    return combined


def load_tou_context(
    utility: str,
    tou_derivation_dir: Path,
    tariffs_electric_dir: Path,
    *,
    use_calibrated: bool = False,
) -> tuple[list[SeasonTouSpec], dict]:
    """Load TOU derivation specs and the TOU tariff for one utility.

    When *use_calibrated* is True, loads the calibrated tariff
    (``*_calibrated.json``) whose rate **levels** match the revenue
    requirement. The pre-calibration tariff has correct peak/off-peak
    ratios but absolute $/kWh set at MC levels (~1.6× higher than
    revenue-neutral for the HP subclass).
    """
    deriv_path = tou_derivation_dir / f"{utility}_hp_seasonalTOU_derivation.json"
    specs = load_season_specs(deriv_path)

    if use_calibrated:
        tariff_path = (
            tariffs_electric_dir / f"{utility}_hp_seasonalTOU_flex_calibrated.json"
        )
        if not tariff_path.exists():
            tariff_path = (
                tariffs_electric_dir / f"{utility}_hp_seasonalTOU_calibrated.json"
            )
        if not tariff_path.exists():
            raise FileNotFoundError(
                f"No calibrated tariff found for {utility}. "
                f"Looked for *_flex_calibrated.json and *_calibrated.json in "
                f"{tariffs_electric_dir}. Run a precalc CAIRO scenario first "
                f"to generate calibrated tariffs, or use --no-calibrated."
            )
    else:
        tariff_path = tariffs_electric_dir / f"{utility}_hp_seasonalTOU_flex.json"
        if not tariff_path.exists():
            tariff_path = tariffs_electric_dir / f"{utility}_hp_seasonalTOU.json"

    log.info("  Tariff: %s", tariff_path.name)
    with open(tariff_path) as f:
        tou_tariff = json.load(f)
    return specs, tou_tariff


def load_delivery_mc(
    utility: str,
    mc_base: str,
    mc_year: int,
    storage_options: dict[str, str],
) -> pd.DataFrame:
    """Load delivery-only MC (dist+sub-tx + bulk_tx) for one utility.

    Returns DataFrame with columns ['timestamp', 'mc_kwh'] in $/kWh.
    """
    dist_path = (
        f"{mc_base}/dist_and_sub_tx/utility={utility}/year={mc_year}/data.parquet"
    )
    bulk_path = f"{mc_base}/bulk_tx/utility={utility}/year={mc_year}/data.parquet"

    dist = pl.read_parquet(dist_path, storage_options=storage_options)
    if "timestamp" in dist.columns:
        ts_dtype = dist.schema["timestamp"]
        if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
            dist = dist.with_columns(
                pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp")
            )
    dist = dist.select(
        "timestamp",
        pl.col("mc_total_per_kwh").alias("mc_dist"),
    )

    bulk = pl.read_parquet(bulk_path, storage_options=storage_options)
    if "timestamp" in bulk.columns:
        ts_dtype = bulk.schema["timestamp"]
        if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
            bulk = bulk.with_columns(
                pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp")
            )
    bulk = bulk.select(
        "timestamp",
        pl.col("bulk_tx_cost_enduse").alias("mc_bulk"),
    )

    mc = dist.join(bulk, on="timestamp", how="full", coalesce=True).sort("timestamp")
    mc = mc.with_columns(
        (pl.col("mc_dist").fill_null(0.0) + pl.col("mc_bulk").fill_null(0.0)).alias(
            "mc_kwh"
        )
    )
    return mc.select("timestamp", "mc_kwh").to_pandas()


# ── Core diagnostic logic ─────────────────────────────────────────────────────


def diagnose_season(
    utility: str,
    season_spec: SeasonTouSpec,
    loads_df: pd.DataFrame,
    period_rate: pd.Series,
    period_map: pd.Series,
    elasticity: float,
    hp_weights: dict[int, float],
    total_weighted_customers: float,
    hp_weighted_customers: float,
    mc_df: pd.DataFrame | None,
) -> SeasonResult:
    """Run diagnostic for one utility × season × elasticity."""
    season_months = set(season_spec.season.months)
    season_name = season_spec.season.name

    time_level = pd.DatetimeIndex(loads_df.index.get_level_values("time"))
    season_mask = time_level.to_series().dt.month.isin(season_months).to_numpy()
    season_df = loads_df.loc[season_mask, ["electricity_net"]].copy().reset_index()

    period_lookup = period_map.reset_index()
    period_lookup.columns = pd.Index(["time", "energy_period"])
    season_df = season_df.merge(period_lookup, on="time", how="left")

    period_consumption = _build_period_consumption(season_df)
    p_flat = _compute_equivalent_flat_tariff(period_consumption, period_rate)

    targets = _build_period_shift_targets(
        period_consumption, period_rate, elasticity, p_flat, receiver_period=None
    )

    peak_periods = set()
    offpeak_periods = set()
    for _, row in targets.drop_duplicates("energy_period").iterrows():
        ep = int(row["energy_period"])
        rate_val = float(row["rate"])
        if rate_val > p_flat:
            peak_periods.add(ep)
        else:
            offpeak_periods.add(ep)

    peak_orig = float(
        targets.loc[targets["energy_period"].isin(peak_periods), "Q_orig"].sum()
    )
    peak_shifted = float(
        targets.loc[targets["energy_period"].isin(peak_periods), "Q_target"].sum()
    )
    peak_reduction_kwh = peak_orig - peak_shifted
    peak_reduction_pct = (peak_reduction_kwh / peak_orig * 100) if peak_orig > 0 else 0

    total_shift = float(targets.loc[targets["load_shift"] < 0, "load_shift"].sum())

    peak_rates = targets.loc[
        targets["energy_period"].isin(peak_periods), "rate"
    ].unique()
    offpeak_rates = targets.loc[
        targets["energy_period"].isin(offpeak_periods), "rate"
    ].unique()
    peak_rate = float(peak_rates.mean()) if len(peak_rates) > 0 else 0.0
    offpeak_rate = float(offpeak_rates.mean()) if len(offpeak_rates) > 0 else 0.0
    rate_spread = peak_rate - offpeak_rate
    rate_arb_savings_total = abs(total_shift) * rate_spread

    n_bldgs = len(targets["bldg_id"].unique())
    savings_per_hp = rate_arb_savings_total / n_bldgs if n_bldgs > 0 else 0

    arcturus_pct = arcturus_peak_reduction(season_spec.peak_offpeak_ratio) * 100
    delta_vs_arcturus = peak_reduction_pct - arcturus_pct

    system_peak_pct = (
        peak_reduction_pct * (hp_weighted_customers / total_weighted_customers)
        if total_weighted_customers > 0
        else 0
    )

    delivery_mc_savings = 0.0
    mc_nonzero = 0
    mc_peak_overlap = 0
    if mc_df is not None:
        mc_season = mc_df[mc_df["timestamp"].dt.month.isin(season_months)].copy()
        mc_nonzero = int((mc_season["mc_kwh"].abs() > 1e-9).sum())

        peak_hours_set = set(season_spec.peak_hours)
        mc_in_peak = mc_season[mc_season["timestamp"].dt.hour.isin(peak_hours_set)]
        mc_peak_overlap = int((mc_in_peak["mc_kwh"].abs() > 1e-9).sum())

        hourly_shift = season_df.copy()
        hourly_shift = hourly_shift.merge(
            targets[["bldg_id", "energy_period", "load_shift", "Q_orig"]],
            on=["bldg_id", "energy_period"],
            how="left",
        )
        period_sums = hourly_shift.groupby(["bldg_id", "energy_period"])[
            "electricity_net"
        ].transform("sum")
        hour_share = np.where(
            period_sums != 0,
            hourly_shift["electricity_net"] / period_sums,
            0.0,
        )
        hourly_shift["hourly_shift_kwh"] = hourly_shift["load_shift"] * hour_share

        weights_series = hourly_shift["bldg_id"].map(hp_weights).fillna(1.0)
        hourly_shift["weighted_shift"] = (
            hourly_shift["hourly_shift_kwh"] * weights_series
        )

        system_hourly = (
            hourly_shift.groupby("time")["weighted_shift"].sum().reset_index()
        )
        system_hourly.columns = ["timestamp", "sys_shift_kwh"]
        system_hourly["timestamp"] = pd.to_datetime(system_hourly["timestamp"])

        # At runtime, CAIRO re-indexes ResStock AMY2018 loads to the MC year
        # (year_run, e.g. 2025) via _return_loads_combined. Our analytical
        # loads are still in the source year, so align to MC year here.
        mc_year = int(mc_season["timestamp"].dt.year.iloc[0])
        shift_year = int(system_hourly["timestamp"].dt.year.iloc[0])
        if mc_year != shift_year:
            year_offset = pd.Timestamp(f"{mc_year}-01-01") - pd.Timestamp(
                f"{shift_year}-01-01"
            )
            system_hourly["timestamp"] = system_hourly["timestamp"] + year_offset

        mc_merge = mc_season.merge(system_hourly, on="timestamp", how="left")
        mc_merge["sys_shift_kwh"] = mc_merge["sys_shift_kwh"].fillna(0.0)
        delivery_mc_savings = -float(
            (mc_merge["mc_kwh"] * mc_merge["sys_shift_kwh"]).sum()
        )

    return SeasonResult(
        utility=utility,
        season=season_name,
        elasticity=elasticity,
        price_ratio=season_spec.peak_offpeak_ratio,
        base_rate=season_spec.base_rate,
        peak_hours=list(season_spec.peak_hours),
        n_peak_hours_per_day=len(season_spec.peak_hours),
        hp_bldg_count=len(targets["bldg_id"].unique()),
        hp_weighted_share=hp_weighted_customers / total_weighted_customers * 100
        if total_weighted_customers > 0
        else 0,
        hp_peak_kwh_orig=peak_orig,
        hp_peak_kwh_shifted=peak_shifted,
        peak_reduction_pct=peak_reduction_pct,
        kwh_shifted=abs(total_shift),
        system_peak_reduction_pct=system_peak_pct,
        p_flat=p_flat,
        arcturus_peak_reduction_pct=arcturus_pct,
        delta_vs_arcturus_pct=delta_vs_arcturus,
        rate_arbitrage_savings_per_hp=savings_per_hp,
        delivery_mc_savings_total=delivery_mc_savings,
        delivery_mc_savings_per_hp=delivery_mc_savings / n_bldgs if n_bldgs > 0 else 0,
        delivery_mc_nonzero_hours=mc_nonzero,
        delivery_mc_peak_overlap_hours=mc_peak_overlap,
    )


def diagnose_utility(
    ctx: UtilityContext,
    loads_df: pd.DataFrame,
    elasticities: list[float],
    mc_df: pd.DataFrame | None,
) -> UtilityResult:
    """Run full diagnostic for one utility across all seasons and elasticities."""
    result = UtilityResult(utility=ctx.utility, delivery_rr=ctx.delivery_rr)

    rate_df = extract_tou_period_rates(ctx.tou_tariff)
    period_rate = cast(pd.Series, rate_df.groupby("energy_period")["rate"].first())
    time_idx = pd.DatetimeIndex(
        loads_df.index.get_level_values("time").unique().sort_values()
    )
    period_map = assign_hourly_periods(time_idx, ctx.tou_tariff)

    for eps in elasticities:
        for spec in ctx.season_specs:
            sr = diagnose_season(
                utility=ctx.utility,
                season_spec=spec,
                loads_df=loads_df,
                period_rate=period_rate,
                period_map=period_map,
                elasticity=eps,
                hp_weights=ctx.hp_weights,
                total_weighted_customers=ctx.total_weighted_customers,
                hp_weighted_customers=ctx.hp_weighted_customers,
                mc_df=mc_df,
            )
            result.season_results.append(sr)

    # RR decrease by elasticity: sum delivery MC savings across seasons
    if ctx.delivery_rr > 0:
        for eps in elasticities:
            total_mc_savings = sum(
                sr.delivery_mc_savings_total
                for sr in result.season_results
                if sr.elasticity == eps
            )
            result.rr_decrease_by_elasticity[eps] = total_mc_savings / ctx.delivery_rr

    winter_specs = [s for s in ctx.season_specs if s.season.name == "winter"]
    summer_specs = [s for s in ctx.season_specs if s.season.name == "summer"]
    if winter_specs and summer_specs:
        winter_kwh = sum(
            sr.hp_peak_kwh_orig + sr.kwh_shifted
            for sr in result.season_results
            if sr.season == "winter" and sr.elasticity == elasticities[0]
        )
        summer_kwh = sum(
            sr.hp_peak_kwh_orig + sr.kwh_shifted
            for sr in result.season_results
            if sr.season == "summer" and sr.elasticity == elasticities[0]
        )
        total_kwh = winter_kwh + summer_kwh
        if total_kwh > 0:
            w_ratio = winter_specs[0].peak_offpeak_ratio
            s_ratio = summer_specs[0].peak_offpeak_ratio
            result.annual_weighted_ratio = (
                w_ratio * winter_kwh + s_ratio * summer_kwh
            ) / total_kwh
        else:
            result.annual_weighted_ratio = (
                winter_specs[0].peak_offpeak_ratio + summer_specs[0].peak_offpeak_ratio
            ) / 2
    elif ctx.season_specs:
        result.annual_weighted_ratio = ctx.season_specs[0].peak_offpeak_ratio

    result.annual_arcturus_pct = (
        arcturus_peak_reduction(result.annual_weighted_ratio) * 100
    )

    # Per-season recommendations: each season has its own TOU ratio and
    # therefore its own Arcturus target. Find the elasticity that best matches
    # the Arcturus peak reduction for that season's ratio.
    season_names = sorted({sr.season for sr in result.season_results})
    for sn in season_names:
        season_srs = [sr for sr in result.season_results if sr.season == sn]
        if not season_srs:
            continue
        season_ratio = season_srs[0].price_ratio
        season_arcturus = arcturus_peak_reduction(season_ratio) * 100

        best_season_eps = elasticities[0]
        best_season_delta = float("inf")
        for eps in elasticities:
            matching = [sr for sr in season_srs if sr.elasticity == eps]
            if not matching:
                continue
            peak_red = matching[0].peak_reduction_pct
            delta = abs(peak_red - season_arcturus)
            if delta < best_season_delta:
                best_season_delta = delta
                best_season_eps = eps

        best_sr = next(
            (sr for sr in season_srs if sr.elasticity == best_season_eps), None
        )
        result.seasonal_recommendations.append(
            SeasonRecommendation(
                season=sn,
                recommended_elasticity=best_season_eps,
                arcturus_target_pct=season_arcturus,
                peak_reduction_pct=best_sr.peak_reduction_pct if best_sr else 0.0,
                price_ratio=season_ratio,
                savings_per_hp=best_sr.rate_arbitrage_savings_per_hp
                if best_sr
                else 0.0,
            )
        )

    # Per-season recommendations: with enabling technology
    for sn in season_names:
        season_srs = [sr for sr in result.season_results if sr.season == sn]
        if not season_srs:
            continue
        season_ratio = season_srs[0].price_ratio
        season_arcturus_wt = arcturus_peak_reduction_with_tech(season_ratio) * 100

        best_wt_eps = elasticities[0]
        best_wt_delta = float("inf")
        for eps in elasticities:
            matching = [sr for sr in season_srs if sr.elasticity == eps]
            if not matching:
                continue
            delta = abs(matching[0].peak_reduction_pct - season_arcturus_wt)
            if delta < best_wt_delta:
                best_wt_delta = delta
                best_wt_eps = eps

        best_wt_sr = next(
            (sr for sr in season_srs if sr.elasticity == best_wt_eps), None
        )
        result.seasonal_recommendations_with_tech.append(
            SeasonRecommendation(
                season=sn,
                recommended_elasticity=best_wt_eps,
                arcturus_target_pct=season_arcturus_wt,
                peak_reduction_pct=best_wt_sr.peak_reduction_pct if best_wt_sr else 0.0,
                price_ratio=season_ratio,
                savings_per_hp=best_wt_sr.rate_arbitrage_savings_per_hp
                if best_wt_sr
                else 0.0,
            )
        )

    # Annual recommendation: average peak reduction across seasons
    best_eps = elasticities[0]
    best_delta = float("inf")
    for eps in elasticities:
        annual_peak_red = np.mean(
            [
                sr.peak_reduction_pct
                for sr in result.season_results
                if sr.elasticity == eps
            ]
        )
        delta = abs(annual_peak_red - result.annual_arcturus_pct)
        if delta < best_delta:
            best_delta = delta
            best_eps = eps

    result.recommended_elasticity = best_eps
    result.recommended_annual_savings = sum(
        sr.rate_arbitrage_savings_per_hp
        for sr in result.season_results
        if sr.elasticity == best_eps
    )
    return result


# ── Output formatting ─────────────────────────────────────────────────────────


def results_to_dataframe(results: list[UtilityResult]) -> pl.DataFrame:
    """Convert all season-level results to a flat Polars DataFrame."""
    rows = []
    for ur in results:
        for sr in ur.season_results:
            rows.append(
                {
                    "utility": sr.utility,
                    "season": sr.season,
                    "elasticity": sr.elasticity,
                    "price_ratio": round(sr.price_ratio, 4),
                    "base_rate_kwh": round(sr.base_rate, 6),
                    "peak_rate_kwh": round(sr.base_rate * sr.price_ratio, 6),
                    "n_peak_hours_per_day": sr.n_peak_hours_per_day,
                    "hp_bldg_count": sr.hp_bldg_count,
                    "hp_weighted_share_pct": round(sr.hp_weighted_share, 2),
                    "hp_peak_kwh_orig": round(sr.hp_peak_kwh_orig, 1),
                    "hp_peak_kwh_shifted": round(sr.hp_peak_kwh_shifted, 1),
                    "peak_reduction_pct": round(sr.peak_reduction_pct, 3),
                    "kwh_shifted": round(sr.kwh_shifted, 1),
                    "system_peak_reduction_pct": round(sr.system_peak_reduction_pct, 4),
                    "p_flat": round(sr.p_flat, 6),
                    "arcturus_peak_reduction_pct": round(
                        sr.arcturus_peak_reduction_pct, 3
                    ),
                    "delta_vs_arcturus_pct": round(sr.delta_vs_arcturus_pct, 3),
                    "rate_arbitrage_savings_per_hp": round(
                        sr.rate_arbitrage_savings_per_hp, 2
                    ),
                    "delivery_mc_savings_total": round(sr.delivery_mc_savings_total, 2),
                    "delivery_mc_savings_per_hp": round(
                        sr.delivery_mc_savings_per_hp, 2
                    ),
                    "delivery_mc_nonzero_hours": sr.delivery_mc_nonzero_hours,
                    "delivery_mc_peak_overlap_hours": sr.delivery_mc_peak_overlap_hours,
                    "annual_weighted_ratio": round(ur.annual_weighted_ratio, 4),
                    "annual_arcturus_pct": round(ur.annual_arcturus_pct, 3),
                    "recommended_elasticity": ur.recommended_elasticity,
                    "recommended_annual_savings": round(
                        ur.recommended_annual_savings, 2
                    ),
                    "seasonal_recommended_elasticity": _seasonal_rec_field(
                        ur, sr.season, "recommended_elasticity"
                    ),
                    "seasonal_arcturus_target_pct": _seasonal_rec_field(
                        ur, sr.season, "arcturus_target_pct"
                    ),
                    "seasonal_recommended_savings": _seasonal_rec_field(
                        ur, sr.season, "savings_per_hp"
                    ),
                    "seasonal_recommended_elasticity_with_tech": _seasonal_rec_field(
                        ur, sr.season, "recommended_elasticity", with_tech=True
                    ),
                    "seasonal_arcturus_target_pct_with_tech": _seasonal_rec_field(
                        ur, sr.season, "arcturus_target_pct", with_tech=True
                    ),
                    "seasonal_recommended_savings_with_tech": _seasonal_rec_field(
                        ur, sr.season, "savings_per_hp", with_tech=True
                    ),
                }
            )
    return pl.DataFrame(rows)


def _seasonal_rec_field(
    ur: UtilityResult, season: str, field_name: str, *, with_tech: bool = False
) -> float:
    """Look up a seasonal recommendation field for the output DataFrame."""
    recs = (
        ur.seasonal_recommendations_with_tech
        if with_tech
        else ur.seasonal_recommendations
    )
    for rec in recs:
        if rec.season == season:
            return round(getattr(rec, field_name), 4)
    return 0.0


S3_OUTPUTS_BASE = "s3://data.sb/switchbox/cairo/outputs/hp_rates"

COMPARISON_PAIRS: list[tuple[str, str, str, str]] = [
    ("In-sample delivery", "run9", "run13", "precalc u00, subclasses"),
    ("In-sample supply", "run10", "run14", "precalc u00, subclasses + supply"),
    ("Out-of-sample delivery", "run11", "run15", "default u02, all-HP"),
    ("Out-of-sample supply", "run12", "run16", "default u02, all-HP + supply"),
]


def _find_run_dir(batch_prefix: str, run_fragment: str) -> str | None:
    """Find the timestamped run directory under a batch prefix on S3."""
    result = subprocess.run(
        ["aws", "s3", "ls", batch_prefix],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.strip().splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and "PRE" in parts:
            dirname = parts[-1].rstrip("/")
            if run_fragment in dirname:
                return f"{batch_prefix}{dirname}"
    return None


def _read_annual_bills(csv_path: str) -> pl.DataFrame | None:
    """Read a CAIRO bill CSV and return Annual rows only."""
    try:
        return cast(
            pl.DataFrame,
            pl.scan_csv(csv_path).filter(pl.col("month") == "Annual").collect(),
        )
    except Exception as e:
        log.warning("  Failed to read %s: %s", csv_path, e)
        return None


@dataclass(frozen=True, slots=True)
class PairResult:
    """Savings stats for one comparison pair."""

    label: str
    baseline_run: str
    flex_run: str
    description: str
    mean_savings: float
    median_savings: float
    n_bldgs: int


@dataclass(frozen=True, slots=True)
class UtilityBillComparison:
    """All comparison pairs for one utility."""

    utility: str
    analytical_savings: float
    analytical_savings_seasonal: float
    pairs: list[PairResult]


def compare_batch_bills(
    state: str,
    batch: str,
    results: list[UtilityResult],
    hp_meta: pl.DataFrame,
    *,
    with_tech: bool = False,
) -> list[UtilityBillComparison]:
    """Compare analytical savings against CAIRO bill outputs across run pairs."""
    comparisons: list[UtilityBillComparison] = []

    for ur in results:
        utility = ur.utility
        batch_prefix = f"{S3_OUTPUTS_BASE}/{state}/{utility}/{batch}/"

        ls_result = subprocess.run(
            ["aws", "s3", "ls", batch_prefix],
            capture_output=True,
            text=True,
            check=False,
        )
        if ls_result.returncode != 0:
            log.warning("  %s: batch not found at %s", utility, batch_prefix)
            continue

        run_dirs: dict[str, str] = {}
        for line in ls_result.stdout.strip().splitlines():
            parts = line.strip().split()
            if len(parts) >= 2 and "PRE" in parts:
                dirname = parts[-1].rstrip("/")
                run_dirs[dirname] = f"{batch_prefix}{dirname}"

        hp_bldg_ids = hp_meta.filter(pl.col("sb.electric_utility") == utility)[
            "bldg_id"
        ].to_list()

        pairs: list[PairResult] = []
        for label, base_frag, flex_frag, desc in COMPARISON_PAIRS:
            base_dir = next((v for k, v in run_dirs.items() if base_frag in k), None)
            flex_dir = next((v for k, v in run_dirs.items() if flex_frag in k), None)
            if not base_dir or not flex_dir:
                log.warning(
                    "  %s: %s or %s not found, skipping %s",
                    utility,
                    base_frag,
                    flex_frag,
                    label,
                )
                continue

            base_df = _read_annual_bills(f"{base_dir}/bills/elec_bills_year_run.csv")
            flex_df = _read_annual_bills(f"{flex_dir}/bills/elec_bills_year_run.csv")
            if base_df is None or flex_df is None:
                continue

            joined = (
                base_df.select(
                    pl.col("bldg_id"),
                    pl.col("bill_level").alias("bill_baseline"),
                )
                .join(
                    flex_df.select(
                        pl.col("bldg_id"),
                        pl.col("bill_level").alias("bill_flex"),
                    ),
                    on="bldg_id",
                    how="inner",
                )
                .with_columns(
                    (pl.col("bill_baseline") - pl.col("bill_flex")).alias("savings"),
                )
            )

            is_in_sample = "In-sample" in label
            if is_in_sample:
                joined = joined.filter(pl.col("bldg_id").is_in(hp_bldg_ids))

            if joined.is_empty():
                log.warning("  %s: no buildings for %s", utility, label)
                continue

            pairs.append(
                PairResult(
                    label=label,
                    baseline_run=base_frag,
                    flex_run=flex_frag,
                    description=desc,
                    mean_savings=float(joined["savings"].mean()),  # type: ignore[arg-type]
                    median_savings=float(joined["savings"].median()),  # type: ignore[arg-type]
                    n_bldgs=joined.height,
                )
            )

        if pairs:
            recs = (
                ur.seasonal_recommendations_with_tech
                if with_tech
                else ur.seasonal_recommendations
            )
            seasonal_savings = sum(sr.savings_per_hp for sr in recs)
            comparisons.append(
                UtilityBillComparison(
                    utility=utility,
                    analytical_savings=ur.recommended_annual_savings,
                    analytical_savings_seasonal=seasonal_savings,
                    pairs=pairs,
                )
            )

    return comparisons


def print_batch_comparison(
    batch: str, comparisons: list[UtilityBillComparison]
) -> None:
    """Print analytical vs CAIRO bill comparison across all run pairs."""
    print("\n" + "=" * 80)
    print(f"CAIRO BILL COMPARISON (batch: {batch})")
    print("=" * 80)

    for uc in comparisons:
        ref = uc.analytical_savings_seasonal or uc.analytical_savings
        label_suffix = "seasonal" if uc.analytical_savings_seasonal else "annual"
        print(
            f"\n  {uc.utility.upper()}  "
            f"(analytical: ${ref:.2f}/HP {label_suffix}"
            f", annual=${uc.analytical_savings:.2f}/HP)"
        )
        print(
            f"  {'Comparison':26s}  {'Runs':>10s}  {'Mean sav':>9s}"
            f"  {'Med sav':>8s}  {'Δ(mean)':>8s}  {'N':>5s}  Description"
        )
        print(
            f"  {'─' * 26}  {'─' * 10}  {'─' * 9}"
            f"  {'─' * 8}  {'─' * 8}  {'─' * 5}  {'─' * 28}"
        )
        for p in uc.pairs:
            delta = p.mean_savings - ref
            print(
                f"  {p.label:26s}  {p.baseline_run:>4s}→{p.flex_run:<4s}"
                f"  ${p.mean_savings:>8.2f}"
                f"  ${p.median_savings:>7.2f}"
                f"  {delta:>+8.2f}"
                f"  {p.n_bldgs:>5d}  {p.description}"
            )
    print()


def _print_seasonal_table(
    title: str, results: list[UtilityResult], variant: str
) -> None:
    """Print a seasonal recommendation table (no_tech or with_tech)."""
    print(f"  {title}\n")
    print(
        f"  {'Utility':8s}  {'Season':8s}  {'ε':>6s}  {'Savings/HP':>11s}"
        f"  {'Ratio':>7s}  {'Peak red':>8s}  {'Arcturus':>8s}"
    )
    print(
        f"  {'─' * 8}  {'─' * 8}  {'─' * 6}  {'─' * 11}"
        f"  {'─' * 7}  {'─' * 8}  {'─' * 8}"
    )
    for ur in results:
        recs = (
            ur.seasonal_recommendations
            if variant == "no_tech"
            else ur.seasonal_recommendations_with_tech
        )
        for rec in recs:
            print(
                f"  {ur.utility:8s}  {rec.season:8s}  {rec.recommended_elasticity:>6.2f}"
                f"  ${rec.savings_per_hp:>9.2f}"
                f"  {rec.price_ratio:>7.3f}"
                f"  {rec.peak_reduction_pct:>7.1f}%"
                f"  {rec.arcturus_target_pct:>7.1f}%"
            )
    print()


def print_summary(results: list[UtilityResult], *, verbose: bool = False) -> None:
    """Print a human-readable summary to stdout."""
    print("\n" + "=" * 80)
    print("DEMAND-FLEX ELASTICITY CALIBRATION")
    print("=" * 80)

    # ── Annual recommendation table ───────────────────────────────────────
    print("\n  RECOMMENDED ELASTICITIES (annual)\n")
    print(
        f"  {'Utility':8s}  {'ε':>6s}  {'Savings/HP':>11s}"
        f"  {'Peak win':>8s}  {'Wt ratio':>8s}  {'Arcturus':>8s}"
    )
    print(f"  {'─' * 8}  {'─' * 6}  {'─' * 11}  {'─' * 8}  {'─' * 8}  {'─' * 8}")
    for ur in results:
        n_peak = 0
        for sr in ur.season_results:
            if sr.elasticity == ur.recommended_elasticity:
                n_peak = sr.n_peak_hours_per_day
                break
        print(
            f"  {ur.utility:8s}  {ur.recommended_elasticity:>6.2f}"
            f"  ${ur.recommended_annual_savings:>9.2f}"
            f"  {n_peak:>5d} hr"
            f"  {ur.annual_weighted_ratio:>8.3f}"
            f"  {ur.annual_arcturus_pct:>7.1f}%"
        )
    print()

    # ── Seasonal recommendation table ─────────────────────────────────────
    _print_seasonal_table(
        "RECOMMENDED ELASTICITIES — no enabling technology (seasonal)",
        results,
        "no_tech",
    )
    _print_seasonal_table(
        "RECOMMENDED ELASTICITIES — with enabling technology (seasonal)",
        results,
        "with_tech",
    )

    # ── RR decrease table ──────────────────────────────────────────────────
    has_rr = any(ur.rr_decrease_by_elasticity for ur in results)
    if has_rr:
        all_eps = sorted(
            {e for ur in results for e in ur.rr_decrease_by_elasticity},
        )
        print("  DELIVERY RR DECREASE BY ELASTICITY (delivery MC savings only)\n")
        header = f"  {'Utility':8s}  {'Del. RR':>14s}"
        for eps in all_eps:
            header += f"  {eps:>7.2f}"
        print(header)
        print(f"  {'─' * 8}  {'─' * 14}" + "".join(f"  {'─' * 7}" for _ in all_eps))
        for ur in results:
            if not ur.rr_decrease_by_elasticity:
                continue
            row = f"  {ur.utility:8s}  ${ur.delivery_rr / 1e6:>10.1f}M"
            for eps in all_eps:
                pct = ur.rr_decrease_by_elasticity.get(eps, 0.0) * 100
                row += f"  {pct:>7.4f}%"
            print(row)
        # Also show absolute dollar savings for the recommended elasticity
        print()
        print(f"  {'Utility':8s}  {'ε':>6s}  {'Del MC savings':>14s}  Note")
        print(f"  {'─' * 8}  {'─' * 6}  {'─' * 14}  {'─' * 40}")
        for ur in results:
            if not ur.rr_decrease_by_elasticity:
                continue
            rec_eps = ur.recommended_elasticity
            total_mc = sum(
                sr.delivery_mc_savings_total
                for sr in ur.season_results
                if sr.elasticity == rec_eps
            )
            print(
                f"  {ur.utility:8s}  {rec_eps:>6.2f}"
                f"  ${total_mc:>13,.0f}"
                f"  delivery only; supply MC savings not included"
            )
        print()

    # ── Caveats ───────────────────────────────────────────────────────────
    print("  CAVEATS:")
    print("  - Arcturus uses aggregate heterogeneous response; our model applies")
    print("    uniform constant elasticity. Recommended epsilon is approximate.")
    print("  - Arcturus log-linear form differs from our power-law model;")
    print("    comparison valid at specific ratios, not across full curve.")

    # ── Per-utility detail (verbose only) ────────────────────────────────
    if not verbose:
        return

    for ur in results:
        print(f"\n{'─' * 80}")
        print(f"UTILITY: {ur.utility.upper()}")
        print(
            f"  Annual weighted ratio: {ur.annual_weighted_ratio:.3f}"
            f"  │  Arcturus target: {ur.annual_arcturus_pct:.1f}% peak reduction"
        )
        print(
            f"  RECOMMENDED ELASTICITY: {ur.recommended_elasticity}"
            f"  │  Annual savings/HP customer: ${ur.recommended_annual_savings:.2f}"
        )

        for eps in sorted({sr.elasticity for sr in ur.season_results}):
            print(f"\n  ε = {eps}:")
            for sr in ur.season_results:
                if sr.elasticity != eps:
                    continue
                print(
                    f"    {sr.season:8s}  ratio={sr.price_ratio:.2f}"
                    f"  peak_red={sr.peak_reduction_pct:5.2f}%"
                    f"  (arcturus={sr.arcturus_peak_reduction_pct:.2f}%"
                    f"  Δ={sr.delta_vs_arcturus_pct:+.2f}%)"
                )
                print(
                    f"              shifted={sr.kwh_shifted:,.0f} kWh"
                    f"  arb_savings=${sr.rate_arbitrage_savings_per_hp:.2f}/hp"
                    f"  mc_savings=${sr.delivery_mc_savings_per_hp:.2f}/hp"
                )
                print(
                    f"              mc_nonzero_hrs={sr.delivery_mc_nonzero_hours}"
                    f"  mc_peak_overlap={sr.delivery_mc_peak_overlap_hours}"
                    f"  sys_peak_impact={sr.system_peak_reduction_pct:.3f}%"
                )


# ── Main ──────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", required=True, choices=list(STATE_CONFIGS.keys()))
    p.add_argument(
        "--epsilon-start",
        type=float,
        default=-0.04,
        help="Start of elasticity sweep range (default: -0.04)",
    )
    p.add_argument(
        "--epsilon-end",
        type=float,
        default=-0.50,
        help="End of elasticity sweep range, inclusive (default: -0.50)",
    )
    p.add_argument(
        "--epsilon-step",
        type=float,
        default=-0.02,
        help="Step size for elasticity sweep (default: -0.02)",
    )
    p.add_argument("--output-dir", type=Path, default=None)
    p.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Limit HP buildings per utility for faster dev iteration",
    )
    p.add_argument(
        "--utilities",
        default=None,
        help="Comma-separated utility subset (default: all)",
    )
    p.add_argument(
        "--no-calibrated",
        dest="calibrated",
        action="store_false",
        default=True,
        help="Use pre-calibration (MC-level) tariff rates instead of calibrated",
    )
    p.add_argument(
        "--compare-batch",
        default=None,
        help="CAIRO batch name to compare analytical predictions against actual bills",
    )
    p.add_argument(
        "--with-tech",
        action="store_true",
        help="Use with-tech recommendations for batch comparison (match CAIRO runs that used elasticity_with_tech)",
    )
    p.add_argument(
        "--write-periods",
        action="store_true",
        help="Write seasonal elasticities into each utility's config/periods YAML",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    cfg = STATE_CONFIGS[args.state]
    elasticities = _build_epsilon_range(
        args.epsilon_start, args.epsilon_end, args.epsilon_step
    )
    utilities = tuple(args.utilities.split(",")) if args.utilities else cfg["utilities"]
    storage_options = get_aws_storage_options()

    project_root = Path(__file__).resolve().parents[2]

    log.info("Loading HP metadata...")
    hp_meta = load_hp_metadata(cfg["path_metadata"], cfg["path_utility_assignment"])

    all_meta = pl.read_parquet(cfg["path_utility_assignment"]).select(
        "bldg_id", "weight", "sb.electric_utility"
    )

    results: list[UtilityResult] = []
    for utility in utilities:
        log.info("=" * 60)
        log.info("Diagnosing %s...", utility)

        util_hp = hp_meta.filter(pl.col("sb.electric_utility") == utility)
        hp_bldg_ids = util_hp["bldg_id"].to_list()

        util_all = all_meta.filter(pl.col("sb.electric_utility") == utility)
        raw_weight_sum = float(util_all["weight"].sum())

        eia_customer_count = get_residential_customer_count_from_utility_stats(
            cfg["path_electric_utility_stats"],
            utility,
            storage_options=storage_options,
        )
        scale = eia_customer_count / raw_weight_sum if raw_weight_sum > 0 else 1.0
        log.info(
            "  EIA customer count: %d, raw weight sum: %.0f, scale factor: %.4f",
            eia_customer_count,
            raw_weight_sum,
            scale,
        )

        hp_weights = {
            bid: w * scale
            for bid, w in zip(util_hp["bldg_id"].to_list(), util_hp["weight"].to_list())
        }
        hp_weighted = float(util_hp["weight"].sum()) * scale
        total_weighted = raw_weight_sum * scale  # == eia_customer_count

        log.info(
            "  %d HP buildings (%.0f weighted, %.1f%% of %.0f total)",
            len(hp_bldg_ids),
            hp_weighted,
            hp_weighted / total_weighted * 100 if total_weighted else 0,
            total_weighted,
        )

        if not hp_bldg_ids:
            log.warning("  No HP buildings for %s, skipping", utility)
            continue

        tou_deriv_dir = project_root / cfg["path_tou_derivation_dir"]
        tariffs_dir = project_root / cfg["path_tariffs_electric_dir"]
        specs, tou_tariff = load_tou_context(
            utility, tou_deriv_dir, tariffs_dir, use_calibrated=args.calibrated
        )
        log.info(
            "  TOU specs: %s",
            [(s.season.name, f"ratio={s.peak_offpeak_ratio:.2f}") for s in specs],
        )

        log.info("  Loading HP building loads...")
        loads_df = load_building_loads(
            hp_bldg_ids, cfg["path_loads_dir"], sample_size=args.sample_size
        )

        log.info("  Loading delivery MC data...")
        try:
            mc_df = load_delivery_mc(
                utility, cfg["mc_base"], cfg["mc_year"], storage_options
            )
            log.info("  MC data loaded: %d rows", len(mc_df))
        except Exception as e:
            log.warning("  Failed to load MC data for %s: %s", utility, e)
            mc_df = None

        delivery_rr = 0.0
        rr_dir = project_root / cfg.get("path_rev_requirement_dir", "")
        rr_path = rr_dir / f"{utility}.yaml"
        if rr_path.exists():
            with open(rr_path) as f:
                rr_data = yaml.safe_load(f)
            delivery_rr = float(rr_data.get("total_delivery_revenue_requirement", 0))
            log.info("  Delivery RR: $%.0fM", delivery_rr / 1e6)

        ctx = UtilityContext(
            utility=utility,
            season_specs=specs,
            tou_tariff=tou_tariff,
            hp_bldg_ids=hp_bldg_ids,
            hp_weights=hp_weights,
            total_weighted_customers=total_weighted,
            hp_weighted_customers=hp_weighted,
            delivery_rr=delivery_rr,
        )

        ur = diagnose_utility(ctx, loads_df, elasticities, mc_df)
        results.append(ur)
        log.info(
            "  Recommended ε=%.2f, annual savings=$%.2f/hp",
            ur.recommended_elasticity,
            ur.recommended_annual_savings,
        )

    print_summary(results, verbose=args.verbose)

    if args.write_periods:
        periods_dir = project_root / cfg["path_periods_dir"]
        for ur in results:
            if not ur.seasonal_recommendations:
                log.warning("No seasonal recommendations for %s, skipping", ur.utility)
                continue
            periods_path = periods_dir / f"{ur.utility}.yaml"
            if periods_path.exists():
                with open(periods_path) as f:
                    periods_data = yaml.safe_load(f) or {}
            else:
                periods_data = {}
            periods_data["elasticity"] = {
                sr.season: sr.recommended_elasticity
                for sr in ur.seasonal_recommendations
            }
            if ur.seasonal_recommendations_with_tech:
                periods_data["elasticity_with_tech"] = {
                    sr.season: sr.recommended_elasticity
                    for sr in ur.seasonal_recommendations_with_tech
                }
            with open(periods_path, "w") as f:
                yaml.dump(periods_data, f, default_flow_style=False, sort_keys=False)
            log.info(
                "Wrote elasticity %s and elasticity_with_tech %s to %s",
                periods_data["elasticity"],
                periods_data.get("elasticity_with_tech"),
                periods_path,
            )

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        df = results_to_dataframe(results)
        csv_path = args.output_dir / "demand_flex_diagnostic.csv"
        df.write_csv(csv_path)
        log.info("Wrote diagnostic CSV: %s", csv_path)

        parquet_path = args.output_dir / "demand_flex_diagnostic.parquet"
        df.write_parquet(parquet_path)
        log.info("Wrote diagnostic parquet: %s", parquet_path)

    if args.compare_batch:
        log.info("Comparing against CAIRO batch: %s", args.compare_batch)
        comparisons = compare_batch_bills(
            state=args.state,
            batch=args.compare_batch,
            results=results,
            hp_meta=hp_meta,
            with_tech=args.with_tech,
        )
        if comparisons:
            print_batch_comparison(args.compare_batch, comparisons)
        else:
            log.warning("No comparisons could be made for batch %s", args.compare_batch)


if __name__ == "__main__":
    main()

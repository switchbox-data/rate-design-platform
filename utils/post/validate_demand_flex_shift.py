"""Validate demand-flex load shifting and produce diagnostic plots.

Reproduces the constant-elasticity demand-response shift analytically using the
same functions CAIRO uses, then:
  1. Runs validation checks (energy conservation, direction, magnitude)
  2. Cross-checks against CAIRO's saved elasticity tracker
  3. Produces diagnostic plots (daily profiles, heatmaps, distributions)
  4. Writes data outputs for report reuse

Invoke via the Justfile:

    # One utility, scalar elasticity (no CAIRO cross-check):
    just -f rate_design/hp_rates/ny/Justfile validate-demand-flex cenhud

    # One utility with seasonal elasticities (matching periods YAML):
    just -f rate_design/hp_rates/ny/Justfile validate-demand-flex cenhud "winter=-0.12,summer=-0.14"

    # With CAIRO tracker cross-check (no-tech batch):
    just -f rate_design/hp_rates/ny/Justfile validate-demand-flex cenhud "winter=-0.12,summer=-0.14" ny_20260325b_r1-16

    # With CAIRO tracker cross-check (with-tech batch):
    just -f rate_design/hp_rates/ny/Justfile validate-demand-flex cenhud "winter=-0.18,summer=-0.22" ny_20260326_elast_seasonal_tech

    # All utilities (scalar, no batch):
    just -f rate_design/hp_rates/ny/Justfile validate-demand-flex-all

Or directly:

    # Scalar elasticity:
    uv run python -m utils.post.validate_demand_flex_shift \
        --utility coned --elasticity -0.10 \
        --output-dir dev_plots/flex/coned

    # Seasonal elasticities (no-tech values from periods YAML):
    uv run python -m utils.post.validate_demand_flex_shift \
        --utility cenhud --elasticity winter=-0.12,summer=-0.14 \
        --output-dir dev_plots/flex/cenhud

    # Seasonal elasticities (with-tech values):
    uv run python -m utils.post.validate_demand_flex_shift \
        --utility cenhud --elasticity winter=-0.18,summer=-0.22 \
        --output-dir dev_plots/flex/cenhud

    # With CAIRO tracker cross-check:
    uv run python -m utils.post.validate_demand_flex_shift \
        --utility cenhud --elasticity winter=-0.12,summer=-0.14 \
        --batch ny_20260326_elast_seasonal_tech \
        --output-dir dev_plots/flex/cenhud
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl

from utils.cairo import (
    _build_period_consumption,
    _build_period_shift_targets,
    _compute_equivalent_flat_tariff,
    assign_hourly_periods,
    extract_tou_period_rates,
)
from utils.pre.compute_tou import SeasonTouSpec, load_season_specs

log = logging.getLogger(__name__)

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams.update(
    {
        "figure.dpi": 150,
        "savefig.dpi": 150,
        "savefig.bbox": "tight",
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.labelsize": 10,
    }
)

WINTER_MONTHS = {1, 2, 3, 10, 11, 12}
SUMMER_MONTHS = {4, 5, 6, 7, 8, 9}

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
    },
}


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
    log.info("Loaded %d HP buildings out of %d total", hp.height, joined.height)
    return hp


def load_building_loads(bldg_ids: list[int], loads_dir: Path) -> pd.DataFrame:
    """Load hourly electric net load for buildings from local parquet.

    Returns pandas DataFrame with MultiIndex (bldg_id, time) and column
    'electricity_net' in kWh.
    """
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
        raise FileNotFoundError(f"No load files found in {loads_dir}")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.set_index(["bldg_id", "time"]).sort_index()
    log.info("Loaded %d buildings, %d rows", len(frames), len(combined))
    return combined


def load_tou_context(
    utility: str,
    tou_derivation_dir: Path,
    tariffs_electric_dir: Path,
) -> tuple[list[SeasonTouSpec], dict]:
    """Load TOU derivation specs and the flex tariff for one utility."""
    deriv_path = tou_derivation_dir / f"{utility}_hp_seasonalTOU_derivation.json"
    specs = load_season_specs(deriv_path)
    tariff_path = tariffs_electric_dir / f"{utility}_hp_seasonalTOU_flex.json"
    if not tariff_path.exists():
        tariff_path = tariffs_electric_dir / f"{utility}_hp_seasonalTOU.json"
    with open(tariff_path) as f:
        tou_tariff = json.load(f)
    return specs, tou_tariff


# ── Shift reproduction ────────────────────────────────────────────────────────


def reproduce_shift(
    loads_df: pd.DataFrame,
    period_rate: pd.Series,
    period_map: pd.Series,
    season_specs: list[SeasonTouSpec],
    elasticity: float | dict[str, float],
) -> pd.DataFrame:
    """Apply constant-elasticity shifting to all buildings, return hourly results.

    Args:
        elasticity: Scalar applied to all seasons, or a ``{season_name: value}``
            dict for per-season elasticity.

    Returns a DataFrame indexed like loads_df with columns:
        electricity_net_orig, electricity_net_shifted, hourly_shift_kw,
        energy_period, hour_of_day, month, season, tou_rate
    """
    time_level = pd.DatetimeIndex(loads_df.index.get_level_values("time"))

    result_parts: list[pd.DataFrame] = []

    for spec in season_specs:
        season_months = set(spec.season.months)
        season_name = spec.season.name

        season_eps = (
            elasticity.get(season_name, 0.0)
            if isinstance(elasticity, dict)
            else elasticity
        )
        if season_eps == 0.0:
            continue

        season_mask = time_level.to_series().dt.month.isin(season_months).to_numpy()
        season_df = loads_df.loc[season_mask].copy().reset_index()

        period_lookup = period_map.reset_index()
        period_lookup.columns = pd.Index(["time", "energy_period"])
        season_df = season_df.merge(period_lookup, on="time", how="left")

        period_consumption = _build_period_consumption(season_df)
        p_flat = _compute_equivalent_flat_tariff(period_consumption, period_rate)

        targets = _build_period_shift_targets(
            period_consumption, period_rate, season_eps, p_flat, receiver_period=None
        )

        # Distribute period-level shifts to hourly rows proportionally
        hourly = season_df.merge(
            targets[["bldg_id", "energy_period", "load_shift", "Q_orig"]],
            on=["bldg_id", "energy_period"],
            how="left",
        )
        period_sums = hourly.groupby(["bldg_id", "energy_period"])[
            "electricity_net"
        ].transform("sum")
        hour_share = np.where(
            period_sums != 0, hourly["electricity_net"] / period_sums, 0.0
        )
        hourly["hourly_shift_kw"] = hourly["load_shift"] * hour_share
        hourly["electricity_net_shifted"] = (
            hourly["electricity_net"] + hourly["hourly_shift_kw"]
        )
        hourly["season"] = season_name
        hourly["hour_of_day"] = hourly["time"].dt.hour
        hourly["month"] = hourly["time"].dt.month

        rate_map = period_rate.to_dict()
        hourly["tou_rate"] = hourly["energy_period"].map(rate_map)

        hourly = hourly.rename(columns={"electricity_net": "electricity_net_orig"})
        result_parts.append(
            hourly[
                [
                    "bldg_id",
                    "time",
                    "electricity_net_orig",
                    "electricity_net_shifted",
                    "hourly_shift_kw",
                    "energy_period",
                    "hour_of_day",
                    "month",
                    "season",
                    "tou_rate",
                ]
            ]
        )

    result = pd.concat(result_parts, ignore_index=True).sort_values(["bldg_id", "time"])
    log.info("Reproduced shift: %d hourly rows", len(result))
    return result


# ── Validation checks ─────────────────────────────────────────────────────────


def run_validation_checks(
    hourly: pd.DataFrame,
    season_specs: list[SeasonTouSpec],
    period_rate: pd.Series,
) -> dict:
    """Run validation checks on the shifted hourly data. Returns summary dict."""
    results: dict = {"energy_conservation": {}, "direction": {}, "magnitude": {}}
    p_flat_map: dict[str, float] = {}

    for spec in season_specs:
        sn = spec.season.name
        sh = hourly[hourly["season"] == sn]

        # Energy conservation: per building
        bldg_totals = sh.groupby("bldg_id").agg(
            orig_kwh=("electricity_net_orig", "sum"),
            shifted_kwh=("electricity_net_shifted", "sum"),
        )
        bldg_totals["diff_kwh"] = bldg_totals["shifted_kwh"] - bldg_totals["orig_kwh"]
        max_diff = bldg_totals["diff_kwh"].abs().max()
        violations = (bldg_totals["diff_kwh"].abs() > 0.01).sum()
        results["energy_conservation"][sn] = {
            "max_abs_diff_kwh": float(max_diff),
            "violations": int(violations),
            "pass": violations == 0,
        }

        # Direction: peak kWh should decrease, off-peak should increase
        rates = period_rate.to_dict()
        period_consumption_all = sh.groupby("energy_period").agg(
            orig=("electricity_net_orig", "sum"),
            shifted=("electricity_net_shifted", "sum"),
        )
        period_consumption_all["shift"] = (
            period_consumption_all["shifted"] - period_consumption_all["orig"]
        )

        global_p_flat = (
            period_consumption_all["orig"] * pd.Series(rates)
        ).sum() / period_consumption_all["orig"].sum()
        p_flat_map[sn] = float(global_p_flat)

        direction_ok = True
        direction_details = {}
        for ep, row in period_consumption_all.iterrows():
            ep_rate = rates.get(int(ep), 0.0)
            if ep_rate > global_p_flat:
                expected_sign = "decrease"
                ok = row["shift"] <= 0.01
            else:
                expected_sign = "increase"
                ok = row["shift"] >= -0.01
            direction_ok = direction_ok and ok
            direction_details[int(ep)] = {
                "rate": float(ep_rate),
                "is_peak": ep_rate > global_p_flat,
                "shift_kwh": float(row["shift"]),
                "expected": expected_sign,
                "pass": ok,
            }
        results["direction"][sn] = {
            "pass": direction_ok,
            "periods": direction_details,
        }

        # Magnitude: aggregate peak reduction %
        peak_eps = [
            ep
            for ep, r in rates.items()
            if r > global_p_flat and ep in period_consumption_all.index
        ]
        if peak_eps:
            peak_orig = period_consumption_all.loc[peak_eps, "orig"].sum()
            peak_shifted = period_consumption_all.loc[peak_eps, "shifted"].sum()
            peak_red_pct = (
                (peak_orig - peak_shifted) / peak_orig * 100 if peak_orig > 0 else 0.0
            )
        else:
            peak_red_pct = 0.0
        results["magnitude"][sn] = {
            "peak_reduction_pct": float(peak_red_pct),
            "ratio": spec.peak_offpeak_ratio,
        }

    return results


def print_validation_report(checks: dict) -> None:
    """Print a human-readable validation report."""
    print("\n" + "=" * 70)
    print("DEMAND-FLEX SHIFT VALIDATION")
    print("=" * 70)

    for sn in sorted(checks["energy_conservation"]):
        ec = checks["energy_conservation"][sn]
        status = "PASS" if ec["pass"] else "FAIL"
        print(f"\n  [{status}] Energy conservation ({sn}):")
        print(f"         max |orig - shifted| = {ec['max_abs_diff_kwh']:.6f} kWh")
        if not ec["pass"]:
            print(f"         {ec['violations']} buildings exceeded 0.01 kWh tolerance")

    for sn in sorted(checks["direction"]):
        dr = checks["direction"][sn]
        status = "PASS" if dr["pass"] else "FAIL"
        print(f"\n  [{status}] Direction check ({sn}):")
        for ep, info in sorted(dr["periods"].items()):
            peak_label = "PEAK" if info["is_peak"] else "offpk"
            ep_status = "ok" if info["pass"] else "FAIL"
            print(
                f"         period {ep} ({peak_label}, ${info['rate']:.4f}/kWh):"
                f" shift={info['shift_kwh']:+,.0f} kWh [{ep_status}]"
            )

    for sn in sorted(checks["magnitude"]):
        mg = checks["magnitude"][sn]
        print(
            f"\n  Peak reduction ({sn}): {mg['peak_reduction_pct']:.2f}%"
            f"  (TOU ratio={mg['ratio']:.2f})"
        )


# ── Diagnostic plots ──────────────────────────────────────────────────────────


def _get_peak_hours(spec: SeasonTouSpec) -> set[int]:
    return set(spec.peak_hours)


def _season_spec_by_name(specs: list[SeasonTouSpec], name: str) -> SeasonTouSpec | None:
    for s in specs:
        if s.season.name == name:
            return s
    return None


def plot_building_daily_profile(
    hourly: pd.DataFrame,
    bldg_id: int,
    season_specs: list[SeasonTouSpec],
    utility: str,
    output_dir: Path,
) -> None:
    """Plot 1: Per-building daily load profile with TOU overlay for one building."""
    bldg = hourly[hourly["bldg_id"] == bldg_id]
    if bldg.empty:
        return

    seasons = ["summer", "winter"]
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

    for ax, sn in zip(axes, seasons):
        spec = _season_spec_by_name(season_specs, sn)
        if spec is None:
            continue
        peak_hours = _get_peak_hours(spec)

        sdata = bldg[bldg["season"] == sn]
        if sdata.empty:
            ax.set_title(f"{sn.title()} — no data")
            continue

        # Pick a representative weekday (middle of season)
        sdata_ts = pd.to_datetime(sdata["time"])
        weekday_mask = sdata_ts.dt.dayofweek < 5
        sdata_wd = sdata[weekday_mask.values]
        if sdata_wd.empty:
            continue

        dates = sdata_wd["time"].dt.date.unique()
        mid_date = dates[len(dates) // 2]
        day = sdata_wd[sdata_wd["time"].dt.date == mid_date]
        if day.empty:
            continue

        hours = day["hour_of_day"].values
        orig = day["electricity_net_orig"].values
        shifted = day["electricity_net_shifted"].values
        rates = day["tou_rate"].values

        # Shade peak hours
        for h in range(24):
            if h in peak_hours:
                ax.axvspan(h - 0.5, h + 0.5, alpha=0.12, color="red", zorder=0)

        ax.plot(
            hours,
            orig,
            "o-",
            color="#2c3e50",
            linewidth=1.8,
            markersize=3,
            label="Original",
            zorder=3,
        )
        ax.plot(
            hours,
            shifted,
            "s--",
            color="#e74c3c",
            linewidth=1.8,
            markersize=3,
            label="Shifted",
            zorder=3,
        )

        ax2 = ax.twinx()
        ax2.step(
            hours,
            rates,
            where="mid",
            color="#7f8c8d",
            linewidth=1.0,
            alpha=0.6,
            label="TOU rate",
        )
        ax2.set_ylabel("TOU rate ($/kWh)", color="#7f8c8d")
        ax2.tick_params(axis="y", labelcolor="#7f8c8d")

        shift_kwh = float(day["hourly_shift_kw"].sum())
        peak_orig_kwh = float(
            day.loc[day["hour_of_day"].isin(peak_hours), "electricity_net_orig"].sum()
        )
        peak_shifted_kwh = float(
            day.loc[
                day["hour_of_day"].isin(peak_hours), "electricity_net_shifted"
            ].sum()
        )
        peak_red = (
            (peak_orig_kwh - peak_shifted_kwh) / peak_orig_kwh * 100
            if peak_orig_kwh > 0
            else 0
        )

        ax.set_title(f"{sn.title()} ({mid_date})")
        ax.set_xlabel("Hour of day")
        ax.set_xlim(-0.5, 23.5)
        ax.set_xticks(range(0, 24, 3))
        textstr = (
            f"Peak red: {peak_red:.1f}%\n"
            f"Net shift: {shift_kwh:+.2f} kWh\n"
            f"Ratio: {spec.peak_offpeak_ratio:.2f}"
        )
        ax.text(
            0.02,
            0.98,
            textstr,
            transform=ax.transAxes,
            fontsize=8,
            verticalalignment="top",
            bbox={"boxstyle": "round,pad=0.3", "facecolor": "wheat", "alpha": 0.5},
        )

    axes[0].set_ylabel("Load (kWh)")
    axes[0].legend(loc="upper right", fontsize=8)

    fig.suptitle(
        f"{utility.upper()} — Building {bldg_id} daily profile",
        fontsize=13,
        fontweight="bold",
    )
    fig.tight_layout()
    path = output_dir / f"building_daily_profile_{bldg_id}.png"
    fig.savefig(path)
    plt.close(fig)
    log.info("Saved %s", path)


def plot_aggregate_daily_profile(
    hourly: pd.DataFrame,
    season_specs: list[SeasonTouSpec],
    utility: str,
    output_dir: Path,
) -> None:
    """Plot 2: Aggregate average daily load profile by season."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
    seasons = ["summer", "winter"]

    for ax, sn in zip(axes, seasons):
        spec = _season_spec_by_name(season_specs, sn)
        if spec is None:
            continue
        peak_hours = _get_peak_hours(spec)

        sdata = hourly[hourly["season"] == sn]
        if sdata.empty:
            continue

        profile = sdata.groupby("hour_of_day").agg(
            orig=("electricity_net_orig", "mean"),
            shifted=("electricity_net_shifted", "mean"),
        )
        hours = profile.index.values
        orig = profile["orig"].values
        shifted = profile["shifted"].values

        for h in range(24):
            if h in peak_hours:
                ax.axvspan(h - 0.5, h + 0.5, alpha=0.10, color="red", zorder=0)

        ax.plot(
            hours,
            orig,
            "o-",
            color="#2c3e50",
            linewidth=2,
            markersize=3,
            label="Original",
        )
        ax.plot(
            hours,
            shifted,
            "s-",
            color="#e74c3c",
            linewidth=2,
            markersize=3,
            label="Shifted",
        )

        ax.fill_between(
            hours,
            orig,
            shifted,
            where=orig > shifted,
            interpolate=True,
            alpha=0.25,
            color="#e74c3c",
            label="Load removed (peak)",
        )
        ax.fill_between(
            hours,
            orig,
            shifted,
            where=shifted > orig,
            interpolate=True,
            alpha=0.25,
            color="#3498db",
            label="Load added (off-peak)",
        )

        total_orig_peak = float(
            sdata.loc[
                sdata["hour_of_day"].isin(peak_hours), "electricity_net_orig"
            ].sum()
        )
        total_shifted_peak = float(
            sdata.loc[
                sdata["hour_of_day"].isin(peak_hours), "electricity_net_shifted"
            ].sum()
        )
        n_bldgs = sdata["bldg_id"].nunique()
        peak_red = (
            (total_orig_peak - total_shifted_peak) / total_orig_peak * 100
            if total_orig_peak > 0
            else 0
        )
        total_shift = float(sdata["hourly_shift_kw"].sum()) / n_bldgs

        ax.set_title(f"{sn.title()} (mean of {n_bldgs} HP buildings)")
        ax.set_xlabel("Hour of day")
        ax.set_xlim(-0.5, 23.5)
        ax.set_xticks(range(0, 24, 3))

        textstr = (
            f"Peak red: {peak_red:.1f}%\n"
            f"kWh shifted/bldg: {total_shift:+,.1f}\n"
            f"Ratio: {spec.peak_offpeak_ratio:.2f}"
        )
        ax.text(
            0.02,
            0.98,
            textstr,
            transform=ax.transAxes,
            fontsize=8,
            verticalalignment="top",
            bbox={"boxstyle": "round,pad=0.3", "facecolor": "wheat", "alpha": 0.5},
        )

    axes[0].set_ylabel("Mean load (kWh)")
    axes[0].legend(loc="upper right", fontsize=7)
    fig.suptitle(
        f"{utility.upper()} — Aggregate daily load profile",
        fontsize=13,
        fontweight="bold",
    )
    fig.tight_layout()
    path = output_dir / "aggregate_daily_profile.png"
    fig.savefig(path)
    plt.close(fig)
    log.info("Saved %s", path)


def plot_net_shift_by_hour(
    hourly: pd.DataFrame,
    season_specs: list[SeasonTouSpec],
    utility: str,
    output_dir: Path,
) -> None:
    """Plot 3: Net kW shift by hour-of-day, bars colored by sign."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
    seasons = ["summer", "winter"]

    for ax, sn in zip(axes, seasons):
        spec = _season_spec_by_name(season_specs, sn)
        if spec is None:
            continue
        peak_hours = _get_peak_hours(spec)

        sdata = hourly[hourly["season"] == sn]
        if sdata.empty:
            continue

        n_bldgs = sdata["bldg_id"].nunique()
        mean_shift = sdata.groupby("hour_of_day")["hourly_shift_kw"].mean()

        colors = ["#e74c3c" if v < 0 else "#3498db" for v in mean_shift.values]
        ax.bar(
            mean_shift.index,
            mean_shift.values,
            color=colors,
            width=0.8,
            edgecolor="white",
            linewidth=0.5,
        )

        for h in range(24):
            if h in peak_hours:
                ax.axvspan(h - 0.5, h + 0.5, alpha=0.06, color="red", zorder=0)

        ax.axhline(0, color="black", linewidth=0.5)
        ax.set_title(f"{sn.title()} (mean/building, n={n_bldgs})")
        ax.set_xlabel("Hour of day")
        ax.set_xlim(-0.5, 23.5)
        ax.set_xticks(range(0, 24, 3))

        net_check = mean_shift.sum()
        ax.text(
            0.98,
            0.02,
            f"Net: {net_check:+.4f} kWh\n(should be ~0)",
            transform=ax.transAxes,
            fontsize=7,
            ha="right",
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "lightyellow",
                "alpha": 0.7,
            },
        )

    axes[0].set_ylabel("Mean shift (kWh)")
    fig.suptitle(
        f"{utility.upper()} — Net load shift by hour",
        fontsize=13,
        fontweight="bold",
    )
    fig.tight_layout()
    path = output_dir / "net_shift_by_hour.png"
    fig.savefig(path)
    plt.close(fig)
    log.info("Saved %s", path)


def plot_shift_heatmap(
    hourly: pd.DataFrame,
    season_specs: list[SeasonTouSpec],
    utility: str,
    output_dir: Path,
) -> None:
    """Plot 4: Month x hour heatmap of mean kW shift per building."""
    pivot = hourly.groupby(["month", "hour_of_day"])["hourly_shift_kw"].mean()
    heatmap_data = pivot.unstack(level="hour_of_day").sort_index()

    fig, ax = plt.subplots(figsize=(14, 5))
    vmax = max(abs(heatmap_data.min().min()), abs(heatmap_data.max().max()))
    im = ax.imshow(
        heatmap_data.values,
        aspect="auto",
        cmap="RdBu",
        vmin=-vmax,
        vmax=vmax,
        interpolation="nearest",
    )

    ax.set_yticks(range(len(heatmap_data.index)))
    month_names = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    ax.set_yticklabels([month_names[m - 1] for m in heatmap_data.index])
    ax.set_xticks(range(24))
    ax.set_xticklabels([str(h) for h in range(24)], fontsize=8)
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Month")

    # Annotate peak hours per season
    for spec in season_specs:
        peak_hours = _get_peak_hours(spec)
        for m in spec.season.months:
            row_idx = (
                list(heatmap_data.index).index(m) if m in heatmap_data.index else None
            )
            if row_idx is None:
                continue
            for h in peak_hours:
                ax.plot(h, row_idx, "k.", markersize=2, alpha=0.4)

    cbar = fig.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label("Mean shift (kWh/building)")
    ax.set_title(
        f"{utility.upper()} — Hourly load shift heatmap"
        f"  (red=removed, blue=added, dots=peak hours)",
        fontsize=12,
        fontweight="bold",
    )
    fig.tight_layout()
    path = output_dir / "shift_heatmap.png"
    fig.savefig(path)
    plt.close(fig)
    log.info("Saved %s", path)


def plot_peak_reduction_distribution(
    hourly: pd.DataFrame,
    season_specs: list[SeasonTouSpec],
    utility: str,
    output_dir: Path,
) -> None:
    """Plot 5: Histogram of per-building peak reduction % by season."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    seasons = ["summer", "winter"]

    for ax, sn in zip(axes, seasons):
        spec = _season_spec_by_name(season_specs, sn)
        if spec is None:
            continue
        peak_hours = _get_peak_hours(spec)

        sdata = hourly[hourly["season"] == sn]
        if sdata.empty:
            continue

        peak_data = sdata[sdata["hour_of_day"].isin(peak_hours)]
        bldg_peak = peak_data.groupby("bldg_id").agg(
            orig=("electricity_net_orig", "sum"),
            shifted=("electricity_net_shifted", "sum"),
        )
        bldg_peak["reduction_pct"] = np.where(
            bldg_peak["orig"] > 0,
            (bldg_peak["orig"] - bldg_peak["shifted"]) / bldg_peak["orig"] * 100,
            0.0,
        )

        data_range = bldg_peak["reduction_pct"].max() - bldg_peak["reduction_pct"].min()
        n_bins = max(5, min(30, int(data_range / 0.3))) if data_range > 0.01 else 5
        ax.hist(
            bldg_peak["reduction_pct"],
            bins=n_bins,
            color="#3498db",
            edgecolor="white",
            alpha=0.8,
        )

        mean_red = bldg_peak["reduction_pct"].mean()
        median_red = bldg_peak["reduction_pct"].median()
        ax.axvline(
            mean_red,
            color="#e74c3c",
            linewidth=2,
            linestyle="-",
            label=f"Mean: {mean_red:.1f}%",
        )
        ax.axvline(
            median_red,
            color="#2ecc71",
            linewidth=2,
            linestyle="--",
            label=f"Median: {median_red:.1f}%",
        )

        ax.set_title(f"{sn.title()} (n={len(bldg_peak)} buildings)")
        ax.set_xlabel("Peak reduction (%)")
        ax.set_ylabel("Count")
        ax.legend(fontsize=8)

        textstr = (
            f"Std: {bldg_peak['reduction_pct'].std():.1f}%\n"
            f"Range: [{bldg_peak['reduction_pct'].min():.1f}%,"
            f" {bldg_peak['reduction_pct'].max():.1f}%]"
        )
        ax.text(
            0.98,
            0.98,
            textstr,
            transform=ax.transAxes,
            fontsize=8,
            ha="right",
            va="top",
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "lightyellow",
                "alpha": 0.7,
            },
        )

    fig.suptitle(
        f"{utility.upper()} — Per-building peak reduction distribution",
        fontsize=13,
        fontweight="bold",
    )
    fig.tight_layout()
    path = output_dir / "peak_reduction_distribution.png"
    fig.savefig(path)
    plt.close(fig)
    log.info("Saved %s", path)


# ── Data outputs ──────────────────────────────────────────────────────────────


def write_data_outputs(
    hourly: pd.DataFrame,
    season_specs: list[SeasonTouSpec],
    utility: str,
    output_dir: Path,
    n_example_buildings: int = 100,
) -> None:
    """Write data files for report reuse."""
    # Aggregate hourly profile CSV
    agg = (
        hourly.groupby(["season", "hour_of_day"])
        .agg(
            mean_load_orig_kw=("electricity_net_orig", "mean"),
            mean_load_shifted_kw=("electricity_net_shifted", "mean"),
            mean_shift_kw=("hourly_shift_kw", "mean"),
        )
        .reset_index()
    )
    agg_path = output_dir / f"{utility}_aggregate_hourly_profile.csv"
    agg.to_csv(agg_path, index=False)
    log.info("Wrote %s", agg_path)

    # Shift heatmap data CSV
    hm = (
        hourly.groupby(["month", "hour_of_day"])["hourly_shift_kw"].mean().reset_index()
    )
    hm.columns = ["month", "hour_of_day", "mean_shift_kw"]
    hm_path = output_dir / f"{utility}_shift_heatmap_data.csv"
    hm.to_csv(hm_path, index=False)
    log.info("Wrote %s", hm_path)

    # Per-building peak reduction CSV
    reduction_rows = []
    for spec in season_specs:
        sn = spec.season.name
        peak_hours = _get_peak_hours(spec)
        sdata = hourly[hourly["season"] == sn]
        peak_data = sdata[sdata["hour_of_day"].isin(peak_hours)]
        bldg_peak = peak_data.groupby("bldg_id").agg(
            orig=("electricity_net_orig", "sum"),
            shifted=("electricity_net_shifted", "sum"),
        )
        bldg_peak["reduction_pct"] = np.where(
            bldg_peak["orig"] > 0,
            (bldg_peak["orig"] - bldg_peak["shifted"]) / bldg_peak["orig"] * 100,
            0.0,
        )
        bldg_peak["season"] = sn
        reduction_rows.append(bldg_peak.reset_index())
    red_df = pd.concat(reduction_rows, ignore_index=True)
    red_path = output_dir / f"{utility}_building_peak_reduction.csv"
    red_df.to_csv(red_path, index=False)
    log.info("Wrote %s", red_path)

    # Per-building hourly data for a subset
    bldg_ids = sorted(hourly["bldg_id"].unique())
    if len(bldg_ids) > n_example_buildings:
        rng = np.random.default_rng(42)
        bldg_ids = sorted(rng.choice(bldg_ids, size=n_example_buildings, replace=False))
    subset = hourly[hourly["bldg_id"].isin(bldg_ids)]
    sub_path = output_dir / f"{utility}_building_hourly_shifted.parquet"
    subset.to_parquet(sub_path, index=False)
    log.info("Wrote %s (%d buildings)", sub_path, len(bldg_ids))


# ── CAIRO tracker cross-check ────────────────────────────────────────────────


def crosscheck_cairo_tracker(
    hourly: pd.DataFrame,
    batch: str,
    utility: str,
    period_rate: pd.Series,
    season_specs: list[SeasonTouSpec],
    output_dir: Path,
) -> None:
    """Download CAIRO elasticity tracker and compare to our analytical results."""
    import subprocess
    import tempfile

    s3_prefix = f"s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/{utility}/{batch}/"

    # List run dirs to find the run13 tracker
    result = subprocess.run(
        ["aws", "s3", "ls", s3_prefix, "--recursive"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    tracker_s3 = None
    for line in result.stdout.splitlines():
        if "run13" in line and "demand_flex_elasticity_tracker.csv" in line:
            parts = line.strip().split()
            key = parts[-1]  # relative to bucket root
            tracker_s3 = f"s3://data.sb/{key}"
            break

    if tracker_s3 is None:
        log.warning("Could not find CAIRO tracker for %s/%s", utility, batch)
        return

    log.info("Downloading CAIRO tracker: %s", tracker_s3)
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name
    subprocess.run(
        ["aws", "s3", "cp", tracker_s3, tmp_path],
        capture_output=True,
        timeout=30,
    )
    cairo_tracker = pd.read_csv(tmp_path)
    Path(tmp_path).unlink(missing_ok=True)

    print(f"\n  CAIRO tracker cross-check ({utility}, run13):")
    print(f"    CAIRO tracker: {len(cairo_tracker)} buildings")
    print(f"    CAIRO columns: {list(cairo_tracker.columns)}")

    # Our analytical tracker: compute per-building per-season-period epsilon
    rates = period_rate.to_dict()
    our_rows = []
    for spec in season_specs:
        sn = spec.season.name
        sdata = hourly[hourly["season"] == sn]
        if sdata.empty:
            continue

        bldg_period = (
            sdata.groupby(["bldg_id", "energy_period"])
            .agg(
                orig=("electricity_net_orig", "sum"),
                shifted=("electricity_net_shifted", "sum"),
            )
            .reset_index()
        )
        bldg_period["rate"] = bldg_period["energy_period"].map(rates)

        global_p_flat = (
            sdata["electricity_net_orig"] * sdata["tou_rate"]
        ).sum() / sdata["electricity_net_orig"].sum()

        valid = (
            (bldg_period["orig"] > 0)
            & (bldg_period["shifted"] > 0)
            & (bldg_period["rate"] != global_p_flat)
        )
        bldg_period["epsilon"] = np.nan
        bldg_period.loc[valid, "epsilon"] = np.log(
            bldg_period.loc[valid, "shifted"] / bldg_period.loc[valid, "orig"]
        ) / np.log(bldg_period.loc[valid, "rate"] / global_p_flat)

        bldg_period["season"] = sn
        our_rows.append(bldg_period)

    our_tracker = pd.concat(our_rows, ignore_index=True)

    # Pivot our tracker to match CAIRO format: bldg_id, season_period columns
    our_pivot = our_tracker.pivot_table(
        index="bldg_id",
        columns=["season", "energy_period"],
        values="epsilon",
    )
    our_pivot.columns = [f"{s}_period_{p}" for s, p in our_pivot.columns]

    # Compare to CAIRO
    common_bldgs = set(our_pivot.index) & set(cairo_tracker["bldg_id"])
    if not common_bldgs:
        print("    WARNING: no common buildings between analytical and CAIRO tracker")
        return

    print(f"    Common buildings: {len(common_bldgs)}")
    cairo_indexed = cairo_tracker.set_index("bldg_id")
    common_cols = set(our_pivot.columns) & set(cairo_indexed.columns)
    if not common_cols:
        print(f"    Our columns: {list(our_pivot.columns)}")
        print(f"    CAIRO columns: {list(cairo_indexed.columns)}")
        print("    WARNING: no matching column names")
        return

    diffs = []
    for col in sorted(common_cols):
        ours = our_pivot.loc[list(common_bldgs), col].dropna()
        theirs = cairo_indexed.loc[list(common_bldgs), col].dropna()
        common_idx = ours.index.intersection(theirs.index)
        if common_idx.empty:
            continue
        diff = (ours.loc[common_idx] - theirs.loc[common_idx]).abs()
        diffs.append(diff)
        print(
            f"    {col}: max|Δε|={diff.max():.6f}, "
            f"mean|Δε|={diff.mean():.6f}, n={len(common_idx)}"
        )

    if diffs:
        all_diffs = pd.concat(diffs)
        print(
            f"    Overall: max|Δε|={all_diffs.max():.6f}, "
            f"mean|Δε|={all_diffs.mean():.6f}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────


def _parse_elasticity(value: str) -> float | dict[str, float]:
    """Parse elasticity as a scalar or ``season=value,...`` dict.

    Examples: ``-0.12``, ``winter=-0.12,summer=-0.14``.
    """
    try:
        return float(value)
    except ValueError:
        pass
    pairs = {}
    for part in value.split(","):
        k, _, v = part.partition("=")
        k = k.strip()
        v = v.strip()
        if not k or not v:
            raise argparse.ArgumentTypeError(
                f"Invalid elasticity format: {value!r}. "
                "Use a number (e.g. -0.12) or season=value pairs "
                "(e.g. winter=-0.12,summer=-0.14)."
            )
        pairs[k] = float(v)
    return pairs


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--utility", required=True)
    p.add_argument("--state", default="ny", choices=list(STATE_CONFIGS.keys()))
    p.add_argument("--elasticity", type=_parse_elasticity, default=-0.10)
    p.add_argument("--batch", default=None, help="S3 batch for CAIRO cross-check")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument(
        "--n-example-buildings",
        type=int,
        default=100,
        help="Buildings to include in hourly parquet (default 100)",
    )
    p.add_argument(
        "--n-profile-buildings",
        type=int,
        default=5,
        help="Buildings to generate individual profile plots for (default 5)",
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
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    project_root = Path(__file__).resolve().parents[2]

    # Load metadata
    log.info("Loading HP metadata...")
    hp_meta = load_hp_metadata(cfg["path_metadata"], cfg["path_utility_assignment"])
    util_hp = hp_meta.filter(pl.col("sb.electric_utility") == args.utility)
    hp_bldg_ids = util_hp["bldg_id"].to_list()
    log.info("%d HP buildings for %s", len(hp_bldg_ids), args.utility)

    if not hp_bldg_ids:
        log.error("No HP buildings for %s", args.utility)
        sys.exit(1)

    # Load TOU context
    tou_deriv_dir = project_root / cfg["path_tou_derivation_dir"]
    tariffs_dir = project_root / cfg["path_tariffs_electric_dir"]
    specs, tou_tariff = load_tou_context(args.utility, tou_deriv_dir, tariffs_dir)
    log.info(
        "TOU specs: %s",
        [(s.season.name, f"ratio={s.peak_offpeak_ratio:.2f}") for s in specs],
    )

    rate_df = extract_tou_period_rates(tou_tariff)
    period_rate = cast(pd.Series, rate_df.groupby("energy_period")["rate"].first())
    log.info("Period rates:\n%s", period_rate)

    # Load building loads
    log.info("Loading HP building loads...")
    loads_df = load_building_loads(hp_bldg_ids, cfg["path_loads_dir"])
    time_idx = pd.DatetimeIndex(
        loads_df.index.get_level_values("time").unique().sort_values()
    )
    period_map = assign_hourly_periods(time_idx, tou_tariff)

    # Reproduce the shift
    log.info("Reproducing demand-flex shift (ε=%s)...", args.elasticity)
    hourly = reproduce_shift(loads_df, period_rate, period_map, specs, args.elasticity)

    # Validation checks
    log.info("Running validation checks...")
    checks = run_validation_checks(hourly, specs, period_rate)
    print_validation_report(checks)

    # Diagnostic plots
    log.info("Generating diagnostic plots...")

    # Plot 2: Aggregate daily profile
    plot_aggregate_daily_profile(hourly, specs, args.utility, output_dir)

    # Plot 3: Net shift by hour
    plot_net_shift_by_hour(hourly, specs, args.utility, output_dir)

    # Plot 4: Shift heatmap
    plot_shift_heatmap(hourly, specs, args.utility, output_dir)

    # Plot 5: Peak reduction distribution
    plot_peak_reduction_distribution(hourly, specs, args.utility, output_dir)

    # Plot 1: Per-building profiles for illustrative buildings
    annual_load = hourly.groupby("bldg_id")["electricity_net_orig"].sum()
    quantiles = annual_load.quantile([0.10, 0.25, 0.50, 0.75, 0.90])
    illustrative_bldgs = []
    for q_val in quantiles.values:
        closest = (annual_load - q_val).abs().idxmin()
        if closest not in illustrative_bldgs:
            illustrative_bldgs.append(closest)

    if len(illustrative_bldgs) < args.n_profile_buildings:
        remaining = [b for b in annual_load.index if b not in illustrative_bldgs]
        rng = np.random.default_rng(42)
        extra = rng.choice(
            remaining,
            size=min(
                args.n_profile_buildings - len(illustrative_bldgs), len(remaining)
            ),
            replace=False,
        )
        illustrative_bldgs.extend(extra)

    for bid in illustrative_bldgs:
        plot_building_daily_profile(hourly, int(bid), specs, args.utility, output_dir)

    # Data outputs
    log.info("Writing data outputs...")
    write_data_outputs(
        hourly,
        specs,
        args.utility,
        output_dir,
        n_example_buildings=args.n_example_buildings,
    )

    # CAIRO tracker cross-check
    if args.batch:
        log.info("Cross-checking against CAIRO tracker...")
        try:
            crosscheck_cairo_tracker(
                hourly, args.batch, args.utility, period_rate, specs, output_dir
            )
        except Exception as e:
            log.warning("CAIRO cross-check failed: %s", e)

    # Validation summary JSON
    import json as json_mod

    def _make_serializable(obj: object) -> object:
        if isinstance(obj, (np.bool_, np.integer)):
            return obj.item()
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, dict):
            return {k: _make_serializable(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_make_serializable(v) for v in obj]
        return obj

    summary_path = output_dir / f"{args.utility}_validation_summary.json"
    with open(summary_path, "w") as f:
        json_mod.dump(
            _make_serializable(
                {
                    "utility": args.utility,
                    "elasticity": args.elasticity,
                    "n_buildings": len(hp_bldg_ids),
                    "checks": checks,
                }
            ),
            f,
            indent=2,
        )
    log.info("Wrote %s", summary_path)

    all_pass = all(
        checks["energy_conservation"][s]["pass"] and checks["direction"][s]["pass"]
        for s in checks["energy_conservation"]
    )
    print(f"\n{'=' * 70}")
    print(f"Overall: {'ALL CHECKS PASSED' if all_pass else 'SOME CHECKS FAILED'}")
    print(f"Plots and data written to: {output_dir}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()

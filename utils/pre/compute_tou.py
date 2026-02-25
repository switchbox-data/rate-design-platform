"""Derive seasonal TOU tariffs from marginal-cost data.

Provides composable building blocks for rate derivation:

1. **Primitives** — ``find_tou_peak_window``, ``compute_tou_cost_causation_ratio``
   work on any hourly MC/load slice (full year, single season, etc.).
2. **Season helpers** — ``Season``, ``season_mask``, ``make_winter_summer_seasons``,
   ``compute_seasonal_base_rates`` let callers iterate over seasons.
3. **Tariff builders** — handled by ``utils.pre.create_tariff``:

   * ``create_seasonal_tariff`` — N-period seasonal flat (one rate per season).
   * ``create_seasonal_tou_tariff`` — 2N-period seasonal+TOU (off-peak + peak per
    season).

The caller (e.g. ``run_scenario.py`` or ``derive_seasonal_tou.py``) iterates
``for season in seasons`` and calls the primitives to derive per-season peak
windows and ratios, then passes those to tariff constructors from
``utils.pre.create_tariff``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from utils.pre.season_config import (
    DEFAULT_TOU_WINTER_MONTHS,
    resolve_winter_summer_months,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Season:
    """A named season defined by a set of calendar months."""

    name: str
    months: list[int]  # 1-indexed (January = 1)


@dataclass(slots=True)
class SeasonTouSpec:
    """Per-season TOU derivation result.

    Produced by the caller after iterating over seasons and calling
    ``find_tou_peak_window`` + ``compute_tou_cost_causation_ratio`` on each
    season's MC/load slice. Consumed by tariff constructors in
    ``utils.pre.create_tariff``.
    """

    season: Season
    base_rate: float  # off-peak (seasonal flat) rate in $/kWh
    peak_hours: list[int]  # hour-of-day integers for the TOU peak window
    peak_offpeak_ratio: float  # peak rate / off-peak rate


def save_season_specs(specs: list[SeasonTouSpec], path: Path) -> None:
    """Serialize a list of :class:`SeasonTouSpec` to a JSON file.

    The output file is consumed by :func:`load_season_specs` (e.g. in
    ``run_scenario.py`` for the demand-shifting phase).
    """
    data = [
        {
            "name": s.season.name,
            "months": s.season.months,
            "base_rate": s.base_rate,
            "peak_hours": s.peak_hours,
            "peak_offpeak_ratio": s.peak_offpeak_ratio,
        }
        for s in specs
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_season_specs(path: Path) -> list[SeasonTouSpec]:
    """Deserialize a derivation JSON produced by :func:`save_season_specs`.

    Returns:
        List of :class:`SeasonTouSpec` in the same order they were saved.
    """
    with open(path) as f:
        data = json.load(f)
    return [
        SeasonTouSpec(
            season=Season(name=d["name"], months=d["months"]),
            base_rate=d["base_rate"],
            peak_hours=d["peak_hours"],
            peak_offpeak_ratio=d["peak_offpeak_ratio"],
        )
        for d in data
    ]


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------


def season_mask(index: pd.DatetimeIndex, s: Season) -> np.ndarray:
    """Return a boolean array — ``True`` for timestamps in *s*'s months."""
    months = np.asarray(index.month)  # type: ignore[union-attr]
    return np.isin(months, s.months)


def make_winter_summer_seasons(
    winter_months: list[int] | None = None,
) -> list[Season]:
    """Return ``[Season("winter", …), Season("summer", …)]``.

    Args:
        winter_months: 1-indexed month list. Defaults to the winter
            complement of legacy TOU summer months (June-September).
    """
    winter_months, summer_months = resolve_winter_summer_months(
        winter_months,
        default_winter_months=DEFAULT_TOU_WINTER_MONTHS,
    )
    return [
        Season("winter", winter_months),
        Season("summer", summer_months),
    ]


# ---------------------------------------------------------------------------
# 1. Marginal-cost combination
# ---------------------------------------------------------------------------


def combine_marginal_costs(
    bulk_marginal_costs: pd.DataFrame,
    distribution_marginal_costs: pd.Series,
) -> pd.Series:
    """Merge bulk (energy + capacity) and distribution MCs into a single $/kWh series.

    Args:
        bulk_marginal_costs: DataFrame indexed by time with columns
            ``Marginal Energy Costs ($/kWh)`` and
            ``Marginal Capacity Costs ($/kWh)``.
        distribution_marginal_costs: Series indexed by time with name
            ``Marginal Distribution Costs ($/kWh)``.

    Returns:
        Series of total marginal cost per kWh, indexed by time.
    """
    total_bulk = bulk_marginal_costs.sum(axis=1).sort_index()
    total_bulk.name = "bulk_mc"
    dist_mc = distribution_marginal_costs.sort_index()
    bulk_index = pd.DatetimeIndex(total_bulk.index)
    bulk_tz = bulk_index.tz

    if not total_bulk.index.equals(dist_mc.index):
        # Some MC sources are partitioned by year but carry a different timestamp
        # year in-file (e.g., partition year=2026 with 2025 timestamps). Normalize
        # distribution timestamps to the bulk MC year before fallback alignment.
        if len(total_bulk) == len(dist_mc):
            try:
                target_year = int(total_bulk.index[0].year)
                dist_index = pd.DatetimeIndex(
                    [ts.replace(year=target_year) for ts in dist_mc.index]
                )
                dist_index = (
                    dist_index.tz_localize(bulk_tz)
                    if dist_index.tz is None
                    else dist_index.tz_convert(bulk_tz)
                )
                dist_mc = pd.Series(
                    dist_mc.values,
                    index=dist_index,
                    name=dist_mc.name,
                ).sort_index()
            except ValueError:
                # If year replacement ever fails (e.g. leap-day mismatch), use
                # positional alignment as a last resort.
                dist_mc = pd.Series(
                    dist_mc.values,
                    index=total_bulk.index,
                    name=dist_mc.name,
                )

        if not total_bulk.index.equals(dist_mc.index):
            dist_mc = dist_mc.reindex(total_bulk.index)

    combined = pd.concat([total_bulk, dist_mc], axis=1).dropna(how="any")
    if combined.empty:
        raise ValueError(
            "Unable to align bulk and distribution marginal costs: no overlapping "
            "timestamps after index alignment."
        )

    total: pd.Series = combined.sum(axis=1)
    total.name = "total_mc_per_kwh"
    total.index.name = "time"
    return total


# ---------------------------------------------------------------------------
# 2. Peak-window detection & cost-causation ratio (single-responsibility
#    primitives — work on any slice of the year)
# ---------------------------------------------------------------------------


def find_tou_peak_window(
    combined_mc: pd.Series,
    hourly_system_load: pd.Series,
    window_hours: int = 4,
) -> list[int]:
    """Find the contiguous *window_hours*-wide block with the highest
    demand-weighted average marginal cost across the 24-hour day.

    This is a **primitive**: it operates on whatever hourly slice is passed
    in.  For seasonal TOU, callers filter to a single season's hours first.

    Args:
        combined_mc: Hourly Series of total MC ($/kWh) indexed by time.
        hourly_system_load: Matching hourly Series of system load.
        window_hours: Width of the peak window (default 4).

    Returns:
        Sorted list of hour-of-day integers (0-23) forming the peak window.
    """
    if window_hours < 1 or window_hours > 23:
        raise ValueError("window_hours must be between 1 and 23")

    # Demand-weighted MC: MC_h * load_h (caller must pass aligned indices)
    dw_mc = combined_mc * hourly_system_load

    # Build a 24-hour profile: average demand-weighted MC by hour-of-day
    hour_of_day = combined_mc.index.hour  # type: ignore[union-attr]
    profile = (
        pd.DataFrame({"dw_mc": dw_mc.values, "hour": hour_of_day})
        .groupby("hour")["dw_mc"]
        .mean()
    )

    # Slide a contiguous window (wrapping around midnight)
    best_sum = -np.inf
    best_start = 0
    for start in range(24):
        hours = [(start + i) % 24 for i in range(window_hours)]
        window_sum = profile.loc[hours].sum()
        if window_sum > best_sum:
            best_sum = window_sum
            best_start = start

    peak_hours = sorted((best_start + i) % 24 for i in range(window_hours))
    log.info(
        "TOU peak window (hours %d–%d): %s", peak_hours[0], peak_hours[-1], peak_hours
    )
    return peak_hours


def compute_tou_cost_causation_ratio(
    combined_mc: pd.Series,
    hourly_system_load: pd.Series,
    peak_hours: list[int],
) -> float:
    """Compute the demand-weighted MC ratio: peak / off-peak.

    This is a **primitive**: it operates on whatever hourly slice is passed
    in.  For seasonal TOU, callers filter to a single season's hours first.

    Args:
        combined_mc: Hourly total MC series.
        hourly_system_load: Matching hourly system load series.
        peak_hours: Hour-of-day integers defining the peak window.

    Returns:
        Peak-to-off-peak cost-causation ratio (always >= 1.0).
    """
    hour_of_day = combined_mc.index.hour  # type: ignore[union-attr]
    is_peak = np.isin(hour_of_day, peak_hours)

    peak_dw = (combined_mc[is_peak] * hourly_system_load[is_peak]).sum()
    peak_load = hourly_system_load[is_peak].sum()

    offpeak_dw = (combined_mc[~is_peak] * hourly_system_load[~is_peak]).sum()
    offpeak_load = hourly_system_load[~is_peak].sum()

    if peak_load == 0 or offpeak_load == 0:
        raise ValueError("Peak or off-peak load is zero; cannot compute ratio")

    peak_avg = peak_dw / peak_load
    offpeak_avg = offpeak_dw / offpeak_load

    if offpeak_avg <= 0:
        raise ValueError("Off-peak demand-weighted MC is non-positive")

    ratio = float(peak_avg / offpeak_avg)
    log.info(
        "Cost-causation ratio: %.4f  (peak avg MC=%.6f, off-peak avg MC=%.6f)",
        ratio,
        peak_avg,
        offpeak_avg,
    )
    return ratio


# ---------------------------------------------------------------------------
# 3. Seasonal base-rate derivation
# ---------------------------------------------------------------------------


def compute_seasonal_base_rates(
    combined_mc: pd.Series,
    hourly_system_load: pd.Series,
    seasons: list[Season],
    base_rate: float,
) -> dict[str, float]:
    """Derive a per-season flat rate from demand-weighted MC.

    The ratio of demand-weighted average MC across seasons scales
    ``base_rate`` so that the load-weighted average of the seasonal rates
    equals ``base_rate``.

    Args:
        combined_mc: 8760-row Series of total MC ($/kWh) indexed by time.
        hourly_system_load: 8760-row Series of system load indexed by time.
        seasons: List of :class:`Season` objects covering all 12 months.
        base_rate: Nominal annual average rate ($/kWh); precalc will
            calibrate the absolute level but the *seasonal ratios* are
            preserved.

    Returns:
        ``{season.name: rate}`` for each season.
    """
    idx = pd.DatetimeIndex(combined_mc.index)
    mc_vals = combined_mc.values
    load_vals = hourly_system_load.values

    # Demand-weighted avg MC per season
    dw_avgs: dict[str, float] = {}
    season_loads: dict[str, float] = {}
    for s in seasons:
        mask = season_mask(idx, s)
        mc_s = mc_vals[mask]
        load_s = load_vals[mask]
        total_load = float(load_s.sum())
        if total_load == 0:
            raise ValueError(f"Load is zero for season '{s.name}'")
        dw_avgs[s.name] = float((mc_s * load_s).sum() / total_load)
        season_loads[s.name] = total_load

    # Scale so load-weighted mean of seasonal rates == base_rate
    total_load = sum(season_loads.values())
    total_mc = sum(dw_avgs[s.name] * season_loads[s.name] for s in seasons)
    scale = base_rate * total_load / total_mc if total_mc != 0 else 1.0

    rates: dict[str, float] = {}
    for s in seasons:
        rates[s.name] = round(dw_avgs[s.name] * scale, 6)
        log.info(
            "Seasonal base rate %s: $%.6f/kWh (dw avg MC=%.6f)",
            s.name,
            rates[s.name],
            dw_avgs[s.name],
        )
    return rates

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

4. **Tariff-map helper** — ``generate_tou_tariff_map`` assigns HP customers to the
   TOU tariff and everyone else to a flat tariff.

The caller (e.g. ``run_scenario.py`` or ``derive_seasonal_tou.py``) iterates
``for season in seasons`` and calls the primitives to derive per-season peak
windows and ratios, then passes those to tariff constructors from
``utils.pre.create_tariff``.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Default summer months (1-indexed).  June–September is the standard New
# England utility summer season.
DEFAULT_SUMMER_MONTHS: list[int] = [6, 7, 8, 9]


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
    summer_months: list[int] | None = None,
) -> list[Season]:
    """Return ``[Season("winter", …), Season("summer", …)]``.

    Args:
        summer_months: 1-indexed month list.  Defaults to
            ``DEFAULT_SUMMER_MONTHS`` (June–September).
    """
    if summer_months is None:
        summer_months = list(DEFAULT_SUMMER_MONTHS)
    winter_months = [m for m in range(1, 13) if m not in summer_months]
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
    total_bulk = bulk_marginal_costs.sum(axis=1)
    total_bulk.name = "bulk_mc"
    combined = pd.merge(
        total_bulk, distribution_marginal_costs, left_index=True, right_index=True
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

    # Demand-weighted MC: MC_h * load_h
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


# ---------------------------------------------------------------------------
# Tariff-map helper
# ---------------------------------------------------------------------------


def generate_tou_tariff_map(
    customer_metadata: pd.DataFrame,
    tou_tariff_key: str,
    flat_tariff_key: str,
) -> pd.DataFrame:
    """Assign HP customers to the TOU tariff and everyone else to flat.

    Args:
        customer_metadata: DataFrame with ``bldg_id`` and
            ``postprocess_group.has_hp`` columns.
        tou_tariff_key: Tariff key for heat-pump customers.
        flat_tariff_key: Tariff key for non-HP customers.

    Returns:
        DataFrame with columns ``bldg_id``, ``tariff_key``.
    """
    has_hp = customer_metadata["postprocess_group.has_hp"].fillna(False).astype(bool)
    tariff_key = has_hp.map({True: tou_tariff_key, False: flat_tariff_key})
    return pd.DataFrame(
        {"bldg_id": customer_metadata["bldg_id"], "tariff_key": tariff_key}
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a TOU tariff map from ResStock metadata."
    )
    parser.add_argument(
        "--metadata-path",
        required=True,
        help="Path (local or s3://) to ResStock metadata parquet.",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. RI).")
    parser.add_argument("--upgrade-id", default="00", help="Upgrade id (e.g. 00).")
    parser.add_argument(
        "--tou-tariff-key",
        required=True,
        help="Tariff key for HP customers (e.g. rie_tou_mc).",
    )
    parser.add_argument(
        "--flat-tariff-key",
        required=True,
        help="Tariff key for non-HP customers (e.g. rie_a16).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for the tariff map CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

    metadata_base = args.metadata_path
    is_s3 = metadata_base.startswith("s3://")
    storage_opts = get_aws_storage_options() if is_s3 else None

    metadata_path = f"{metadata_base}/state={args.state}/upgrade={args.upgrade_id}/metadata-sb.parquet"

    if is_s3:
        metadata_df = pd.read_parquet(metadata_path, storage_options=storage_opts)
    else:
        metadata_df = pd.read_parquet(metadata_path)

    tariff_map_df = generate_tou_tariff_map(
        customer_metadata=metadata_df,
        tou_tariff_key=args.tou_tariff_key,
        flat_tariff_key=args.flat_tariff_key,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.tou_tariff_key}_tariff_map.csv"
    tariff_map_df.to_csv(output_path, index=False)
    print(f"Created TOU tariff map: {output_path}")


if __name__ == "__main__":
    main()

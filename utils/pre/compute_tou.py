"""Derive a TOU tariff from marginal-cost data.

Given hourly bulk (Cambium) and distribution marginal costs together with
system load, this module:

1. Finds the contiguous N-hour peak window with the highest demand-weighted
   average combined marginal cost across the 24-hour day.
2. Computes the peak / off-peak cost-causation ratio (demand-weighted MC).
3. Builds a two-period URDB v7 tariff JSON whose rate ratio equals the
   cost-causation ratio.
4. Generates a tariff map that assigns heat-pump customers to the TOU tariff
   and everyone else to a flat tariff.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Peak-window detection
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


def find_tou_peak_window(
    combined_mc: pd.Series,
    hourly_system_load: pd.Series,
    window_hours: int = 4,
) -> list[int]:
    """Find the contiguous *window_hours*-wide block with the highest
    demand-weighted average marginal cost across the 24-hour day.

    Args:
        combined_mc: 8760-row Series of total MC ($/kWh) indexed by time.
        hourly_system_load: 8760-row Series of system load (kW or kWh)
            indexed by time.
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
    log.info("TOU peak window (hours %d–%d): %s", peak_hours[0], peak_hours[-1], peak_hours)
    return peak_hours


# ---------------------------------------------------------------------------
# 2. Cost-causation ratio
# ---------------------------------------------------------------------------


def compute_tou_cost_causation_ratio(
    combined_mc: pd.Series,
    hourly_system_load: pd.Series,
    peak_hours: list[int],
) -> float:
    """Compute the demand-weighted MC ratio: peak / off-peak.

    Args:
        combined_mc: 8760-row total MC series.
        hourly_system_load: 8760-row system load series.
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
# 3. URDB v7 tariff builder
# ---------------------------------------------------------------------------


def create_tou_tariff(
    label: str,
    peak_hours: list[int],
    peak_offpeak_ratio: float,
    base_rate: float = 0.06,
    fixed_charge: float = 6.75,
    adjustment: float = 0.0,
    utility: str = "GenericUtility",
) -> dict:
    """Build a two-period URDB v7 tariff JSON.

    Period 0 = off-peak, period 1 = peak.  The ratio of effective rates
    (rate + adj) between peak and off-peak equals *peak_offpeak_ratio*.

    Args:
        label: Tariff label / identifier.
        peak_hours: Hour-of-day integers (0-23) for the peak period.
        peak_offpeak_ratio: Peak rate / off-peak rate.
        base_rate: Off-peak volumetric rate in $/kWh (precalc will calibrate).
        fixed_charge: Fixed monthly charge in $.
        adjustment: Rate adjustment applied to both periods ($/kWh).
        utility: Utility name.

    Returns:
        Dictionary in URDB v7 ``{"items": [...]}`` format.
    """
    peak_rate = base_rate * peak_offpeak_ratio
    peak_hours_set = set(peak_hours)

    # 12 months × 24 hours schedule: 0 = off-peak, 1 = peak
    schedule = [
        [1 if h in peak_hours_set else 0 for h in range(24)] for _ in range(12)
    ]

    return {
        "items": [
            {
                "label": label,
                "uri": "",
                "sector": "Residential",
                "energyweekdayschedule": schedule,
                "energyweekendschedule": schedule,
                "energyratestructure": [
                    # period 0 – off-peak
                    [{"rate": round(base_rate, 6), "adj": adjustment, "unit": "kWh"}],
                    # period 1 – peak
                    [{"rate": round(peak_rate, 6), "adj": adjustment, "unit": "kWh"}],
                ],
                "fixedchargefirstmeter": fixed_charge,
                "fixedchargeunits": "$/month",
                "mincharge": 0.0,
                "minchargeunits": "$/month",
                "utility": utility,
                "servicetype": "Bundled",
                "name": label,
                "is_default": False,
                "country": "USA",
                "demandunits": "kW",
                "demandrateunit": "kW",
            }
        ]
    }


# ---------------------------------------------------------------------------
# 4. Tariff map generator
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

    import polars as pl

    from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

    metadata_base = args.metadata_path
    is_s3 = metadata_base.startswith("s3://")
    storage_opts = get_aws_storage_options() if is_s3 else None

    metadata_path = (
        f"{metadata_base}/state={args.state}/upgrade={args.upgrade_id}/metadata-sb.parquet"
    )
    lf = (
        pl.scan_parquet(metadata_path, storage_options=storage_opts)
        if storage_opts
        else pl.scan_parquet(metadata_path)
    )
    metadata_pd = lf.select("bldg_id", "postprocess_group.has_hp").collect().to_pandas()

    tariff_map = generate_tou_tariff_map(
        customer_metadata=metadata_pd,
        tou_tariff_key=args.tou_tariff_key,
        flat_tariff_key=args.flat_tariff_key,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.tou_tariff_key}_tariff_map.csv"
    tariff_map.to_csv(output_path, index=False)
    print(f"Created TOU tariff map: {output_path}")


if __name__ == "__main__":
    main()

"""ISO-NE zone-load reader.

``isone_zone_load`` reads the ISO-NE zone hive (``interval_start_et``, ``zone=``,
``year=``). One zone is a list of length one. ``history_years`` stacks several
years for the KDE probability of peak. It does not allocate.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl

from utils.data_prep.marginal_costs.inputs.context import InputContext, Resolved
from utils.data_prep.marginal_costs.mc_manifest import input_partition
from utils.data_prep.marginal_costs.supply_utils import (
    ISONE_ALL_LOAD_ZONES,
    build_cairo_8760_timestamps,
    remap_year_if_needed,
    strip_tz_if_needed,
)

# AESC 2024 avoided Pool Transmission Facility cost, as filed. The same value
# applies in every New England state. CPI, when a config asks for it, starts
# from AESC_2024_DOLLAR_YEAR.
AESC_2024_AVOIDED_PTF_KW_YEAR = 69.0
AESC_2024_DOLLAR_YEAR = 2024
DEFAULT_N_PEAK_HOURS = 100
DEFAULT_POP_KDE_HISTORY_YEARS: tuple[int, ...] = (2022, 2023, 2024, 2025)


def parse_history_years(text: str) -> tuple[int, ...]:
    """Parse ``2022-2025`` or ``2022,2023,2024,2025`` into sorted years."""
    text = text.strip()
    if "-" in text and "," not in text:
        first, last = (int(part) for part in text.split("-", 1))
        if last < first:
            raise ValueError(f"history years {text!r}: end before start")
        return tuple(range(first, last + 1))
    years = tuple(sorted({int(part) for part in text.split(",") if part.strip()}))
    if not years:
        raise ValueError("history years must name at least one year")
    return years


def _as_years(value: str | Sequence[int]) -> tuple[int, ...]:
    if isinstance(value, str):
        return parse_history_years(value)
    years = tuple(int(year) for year in value)
    if not years:
        raise ValueError("history years must name at least one year")
    return years


def _load_single_zone(
    base: str,
    zone: str,
    year: int,
    ref_8760: pl.DataFrame,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load one ISO-NE zone's hourly load for a year, returning exactly 8760 rows.

    DST fall-back hours that share a naive timestamp are averaged. The missing
    spring-forward hour is interpolated, and a gap that interpolation cannot
    fill becomes 0.
    """
    scan_kwargs: dict[str, Any] = {"hive_partitioning": True}
    if base.startswith("s3://"):
        scan_kwargs["storage_options"] = storage_options
    raw = (
        pl.scan_parquet(base, **scan_kwargs)
        .filter(pl.col("zone") == zone, pl.col("year") == year)
        .select("interval_start_et", "load_mw")
        .collect()
    )
    if not isinstance(raw, pl.DataFrame) or raw.is_empty():
        raise FileNotFoundError(f"No ISO-NE zone load data found for zone={zone!r}, year={year} under {base}")

    raw = strip_tz_if_needed(raw, "interval_start_et").rename({"interval_start_et": "timestamp"})
    raw = raw.with_columns(pl.col("timestamp").cast(pl.Datetime("us")))

    months_present = raw.select(pl.col("timestamp").dt.month().unique()).to_series().to_list()
    missing_months = sorted(set(range(1, 13)) - set(months_present))
    if missing_months:
        raise ValueError(
            f"ISO-NE zone load data for zone={zone!r}, year={year} is missing months: "
            f"{missing_months}. Backfill source data before generating marginal costs."
        )

    deduped = raw.group_by("timestamp").agg(pl.col("load_mw").mean()).sort("timestamp")
    n_raw, n_deduped = raw.height, deduped.height
    if n_raw != n_deduped:
        print(f"    {zone}: DST fall-back — {n_raw - n_deduped} duplicate timestamp(s) averaged")

    zone_8760 = (
        ref_8760.join(deduped, on="timestamp", how="left")
        .sort("timestamp")
        .with_columns(pl.col("load_mw").interpolate())
    )
    n_gaps = zone_8760.filter(pl.col("load_mw").is_null()).height
    if n_gaps > 0:
        zone_8760 = zone_8760.with_columns(pl.col("load_mw").fill_null(0.0))
        print(f"    {zone}: {n_gaps} hour(s) zero-filled (could not interpolate)")
    n_interpolated = 8760 - n_deduped
    if n_interpolated > 0:
        print(f"    {zone}: {n_interpolated} gap(s) interpolated (DST spring-forward)")
    return zone_8760


def load_isone_zone_loads(
    zone_loads_s3_base: str,
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load ISO-NE zone loads and sum them into an 8760-row aggregate.

    Each zone is loaded on its own, DST-handled, and aligned to the Cairo clock
    before the loads are added.
    """
    if not zone_names:
        raise ValueError("isone_zone_load needs at least one zone")
    base = zone_loads_s3_base.rstrip("/") + "/"
    ref_8760 = build_cairo_8760_timestamps(year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))

    aggregate: pl.DataFrame | None = None
    for zone in sorted(zone_names):
        zone_df = _load_single_zone(base, zone, year, ref_8760, storage_options)
        if aggregate is None:
            aggregate = zone_df
        else:
            aggregate = (
                aggregate.join(zone_df.rename({"load_mw": "_load_other"}), on="timestamp", how="left")
                .with_columns((pl.col("load_mw") + pl.col("_load_other")).alias("load_mw"))
                .select("timestamp", "load_mw")
            )
    assert aggregate is not None
    print(
        f"Loaded ISO-NE zone load: {len(zone_names)} zone(s) "
        f"[{', '.join(sorted(zone_names))}] summed → {aggregate.height:,} hours"
    )
    return aggregate


def load_isone_zone_load_history(
    zone_loads_s3_base: str,
    zone_names: list[str],
    years: tuple[int, ...],
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Several calendar years of summed ISO-NE zone load, one row per hour.

    Timestamps stay on their own years. The KDE fitter needs that.
    """
    frames = []
    for year in years:
        print(f"\n── Allocation load history (year={year}) ──")
        frames.append(
            load_isone_zone_loads(
                zone_loads_s3_base=zone_loads_s3_base,
                zone_names=zone_names,
                year=year,
                storage_options=storage_options,
            )
        )
    return pl.concat(frames).sort("timestamp")


def _allocation_load(zones: Sequence[str]) -> str:
    if set(zones) == set(ISONE_ALL_LOAD_ZONES):
        return "ne_system"
    return "utility_zone"


def _zone_inputs(base: str, zones: Sequence[str], year: int) -> list[dict]:
    root = base.rstrip("/")
    return [input_partition(f"load_zone_{zone}", f"{root}/zone={zone}/year={year}") for zone in sorted(zones)]


def isone_zone_load(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """Sum named ISO-NE zones for one year, or stack ``history_years``.

    A single year is remapped onto the output year. A history is not: each
    year keeps the timestamps the KDE fitter groups on.
    """
    del role
    zones = [str(zone) for zone in entry["zones"]]
    path = str(entry["path"])
    if "history_years" in entry:
        years = _as_years(entry["history_years"])
        frame = load_isone_zone_load_history(path, zones, years, ctx.storage_options)
        inputs: list[dict] = []
        for year in years:
            inputs.extend(_zone_inputs(path, zones, year))
        return Resolved(
            frame,
            inputs,
            {
                "zones": sorted(zones),
                "history_years": list(years),
                "allocation_load": _allocation_load(zones),
                "load_year": years[-1],
            },
        )
    load_year = int(entry["year"]) if "year" in entry else ctx.load_year
    frame = load_isone_zone_loads(path, zones, load_year, ctx.storage_options)
    frame = remap_year_if_needed(frame, "timestamp", load_year, ctx.year)
    return Resolved(
        frame,
        _zone_inputs(path, zones, load_year),
        {
            "zones": sorted(zones),
            "load_year": load_year,
            "allocation_load": _allocation_load(zones),
        },
    )

"""Plot HP adoption trajectory loads and Cambium busbar loads across scenario years.

Produces six diagnostic charts saved as PNGs:

1. **loads_by_year.png** — weighted aggregate hourly residential electric load
   (kWh) from materialized ResStock, one line per adoption year.

2. **cambium_busbar_by_year.png** — Cambium ``busbar_load`` (MWh) across all
   adoption years, one line per year.

3. **cambium_seasonal_peaks_by_year.png** — Cambium winter and summer peak
   busbar loads (MWh) by calendar year, with crossover annotation when winter
   and summer peaks swap ordering.

4. **load_vs_cambium_peaks_by_year.png** — normalized year-over-year comparison
   of the residential HP load peak and the Cambium busbar peak (winter vs summer)
   by adoption year.

5. **cambium_energy_mc_by_year.png** — Cambium ``energy_cost_enduse`` ($/kWh)
   hourly timeseries, one line per adoption year, with winter/summer peak markers.

6. **cambium_capacity_mc_by_year.png** — Cambium ``capacity_cost_enduse``
   ($/kWh) hourly timeseries, one line per adoption year, with winter/summer
   peak markers.

7. **demand_weighted_mc_by_year.png** — demand-weighted supply MC ($/kWh) by
   customer type (HP vs non-HP) and season (Winter / Summer), as a grouped
   bar chart evolving across adoption years. Supply MC = energy + capacity MC,
   load-weighted: Σ(load_h × supply_mc_h) / Σ(load_h).

Usage::

    uv run python utils/pre/plot_adoption_trajectory.py \\
        --adoption-config rate_design/hp_rates/ny/config/adoption/nyca_electrification.yaml \\
        --materialized-dir /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/adoption/nyca_electrification \\
        --path-utility-assignment /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/metadata_utility/state=NY/utility_assignment.parquet \\
        --state NY \\
        --cambium-path "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t={year}/gea=NYISO/r=p127/data.parquet" \\
        --output-dir rate_design/hp_rates/ny/config/adoption/trajectory_plots
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import yaml
from plotnine import (
    aes,
    element_line,
    element_text,
    geom_hline,
    geom_line,
    geom_point,
    geom_vline,
    ggplot,
    labs,
    scale_color_manual,
    scale_linetype_manual,
    scale_x_datetime,
    theme,
    theme_minimal,
)

from utils import get_aws_region
from utils.loads import ELECTRIC_LOAD_COL, scan_resstock_loads
from utils.pre.season_config import parse_months_arg, resolve_winter_summer_months

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# Default winter months (Dec–Mar inclusive) for peak detection.
_DEFAULT_WINTER_MONTHS = [12, 1, 2, 3]

# Palette for adoption years (sequential blue→red).
_YEAR_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
]


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------


def _load_adoption_config(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_run_years(config: dict[str, Any]) -> list[int]:
    """Return the calendar run years from an adoption config YAML."""
    year_labels: list[int] = [int(y) for y in config["year_labels"]]
    run_years_raw: list[int] | None = config.get("run_years")
    if run_years_raw is None:
        return year_labels
    # Snap each run_year to the nearest year_label entry.
    snapped: list[int] = []
    for yr in run_years_raw:
        nearest = min(year_labels, key=lambda y: abs(y - int(yr)))
        snapped.append(nearest)
    return snapped


_HEATING_TYPE_COL = "postprocess_group.heating_type"
_HEATING_TYPE_ALL = "all"


def _load_metadata_slim(year_dir: str) -> pl.DataFrame:
    """Load only bldg_id, heating_type, and weight from the year's metadata + utility assignment.

    Reads ``<year_dir>/metadata-sb.parquet`` for ``postprocess_group.heating_type``.
    Weight comes from utility_assignment.parquet passed separately.
    """
    meta_path = f"{year_dir.rstrip('/')}/metadata-sb.parquet"
    return pl.read_parquet(
        meta_path,
        columns=["bldg_id", _HEATING_TYPE_COL],
    )


def _load_weights(path_utility_assignment: str) -> pl.DataFrame:
    """Load building weights from utility_assignment.parquet."""
    storage_options = {"aws_region": get_aws_region()}
    if path_utility_assignment.startswith("s3://"):
        df = pl.read_parquet(path_utility_assignment, storage_options=storage_options)
    else:
        df = pl.read_parquet(path_utility_assignment)
    if "weight" not in df.columns:
        raise ValueError(
            f"utility_assignment.parquet missing 'weight' column; found: {df.columns}"
        )
    return df.select(["bldg_id", "weight"])


def _agg_cache_path(cache_dir: Path, year: int) -> Path:
    return cache_dir / f"year={year}" / "data.parquet"


def _load_or_build_agg_loads(
    materialized_dir: str,
    year: int,
    state: str,
    upgrade: str,
    weights: pl.DataFrame,
    cache_dir: Path,
) -> pl.DataFrame:
    """Return cached aggregated loads, building+writing the cache if absent.

    Cache schema: timestamp (Datetime), heating_type (String), load_kwh (Float64).
    One row per (timestamp, heating_type) — 8760 rows × n heating types + "all".

    On a cache miss the function scans all per-building parquets via the hive
    partition, joins metadata for heating_type and utility-assignment weights,
    then streams a weighted group-by so only the tiny aggregated result ever
    lives in memory.
    """
    cache_path = _agg_cache_path(cache_dir, year)
    if cache_path.exists():
        log.info("  year %d: reading cached agg loads from %s", year, cache_path)
        return pl.read_parquet(cache_path)

    log.info("  year %d: cache miss — scanning hive loads (streaming)", year)
    year_dir = f"{materialized_dir.rstrip('/')}/year={year}"

    # Slim metadata: bldg_id → heating_type
    meta = _load_metadata_slim(year_dir).with_columns(
        pl.col(_HEATING_TYPE_COL).fill_null(_HEATING_TYPE_ALL).alias("heating_type")
    )

    # weights: bldg_id → weight
    weights_slim = weights.select(
        pl.col("bldg_id").cast(pl.Int64),
        pl.col("weight").cast(pl.Float64),
    )

    # Join metadata + weights on bldg_id (both small DataFrames — one join is fine)
    bldg_attrs = meta.join(weights_slim, on="bldg_id", how="inner").select(
        pl.col("bldg_id").cast(pl.Int64),
        "heating_type",
        "weight",
    )

    # Lazy scan of hive-partitioned loads for this year's mixed-upgrade snapshot.
    # scan_resstock_loads appends load_curve_hourly/ internally.
    loads_lf = scan_resstock_loads(year_dir, state=state, upgrade=upgrade)

    # Stream: select only what we need, join attrs, weighted load, aggregate.
    agg_lf = (
        loads_lf.select(
            pl.col("bldg_id").cast(pl.Int64),
            pl.col("timestamp"),
            pl.col(ELECTRIC_LOAD_COL).cast(pl.Float64),
        )
        .join(bldg_attrs.lazy(), on="bldg_id", how="inner")
        .with_columns(
            (pl.col(ELECTRIC_LOAD_COL) * pl.col("weight")).alias("_wload"),
        )
        .group_by("timestamp", "heating_type")
        .agg(pl.col("_wload").sum().alias("load_kwh"))
        .sort("timestamp", "heating_type")
    )

    # Also compute "all" heating types combined.
    all_lf = (
        loads_lf.select(
            pl.col("bldg_id").cast(pl.Int64),
            pl.col("timestamp"),
            pl.col(ELECTRIC_LOAD_COL).cast(pl.Float64),
        )
        .join(weights_slim.lazy(), on="bldg_id", how="inner")
        .with_columns(
            (pl.col(ELECTRIC_LOAD_COL) * pl.col("weight")).alias("_wload"),
        )
        .group_by("timestamp")
        .agg(pl.col("_wload").sum().alias("load_kwh"))
        .with_columns(pl.lit(_HEATING_TYPE_ALL).alias("heating_type"))
        .sort("timestamp")
    )

    _cols = ["timestamp", "heating_type", "load_kwh"]
    _agg_collected: pl.DataFrame = agg_lf.collect(engine="streaming")  # type: ignore[assignment]
    _all_collected: pl.DataFrame = all_lf.collect(engine="streaming")  # type: ignore[assignment]
    combined = pl.concat(
        [_agg_collected.select(_cols), _all_collected.select(_cols)]
    ).sort("heating_type", "timestamp")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(cache_path)
    log.info(
        "  year %d: wrote agg loads cache (%d rows) → %s",
        year,
        len(combined),
        cache_path,
    )
    return combined


def _load_cambium_for_year(
    cambium_path_template: str,
    year: int,
) -> pd.DataFrame:
    """Return a DataFrame of Cambium columns for a given year (indexed by time).

    Columns returned (after unit conversion):
    - ``busbar_load`` (MWh)
    - ``energy_cost_enduse`` ($/kWh, converted from $/MWh in source)
    - ``capacity_cost_enduse`` ($/kWh, converted from $/MWh in source)

    The template must contain ``{year}``, e.g.
    ``s3://data.sb/nrel/cambium/2024/scenario=MidCase/t={year}/gea=NYISO/r=p127/data.parquet``.
    """
    path = cambium_path_template.replace("{year}", str(year))
    log.info("loading Cambium data: %s", path)
    storage_options = {"aws_region": get_aws_region()}
    if path.startswith("s3://"):
        df = pl.read_parquet(path, storage_options=storage_options)
    else:
        df = pl.read_parquet(path)

    required = {
        "timestamp",
        "busbar_load",
        "energy_cost_enduse",
        "capacity_cost_enduse",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Cambium parquet missing columns {missing}; available: {df.columns[:10]}"
        )

    pdf = df.select(list(required)).to_pandas()
    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"], errors="coerce")
    pdf = pdf.dropna(subset=["timestamp"]).set_index("timestamp")
    pdf.index.name = "time"
    if pdf.index.tz is None:  # type: ignore[union-attr]
        pdf.index = pdf.index.tz_localize("EST", ambiguous="infer")  # type: ignore[union-attr]

    # Cambium stores MC columns in $/MWh; convert to $/kWh.
    pdf["energy_cost_enduse"] = (
        pd.to_numeric(pdf["energy_cost_enduse"], errors="coerce") / 1000
    )
    pdf["capacity_cost_enduse"] = (
        pd.to_numeric(pdf["capacity_cost_enduse"], errors="coerce") / 1000
    )
    return pdf


def _cambium_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Extract a named column from a Cambium DataFrame as a Series."""
    return df[col]


def _peak_hour_in_season(series: pd.Series, month_list: list[int]) -> pd.Timestamp:
    """Return the timestamp of the maximum value in the given months."""
    mask = series.index.month.isin(month_list)  # type: ignore[union-attr]
    subset = series[mask]
    return subset.idxmax()


def _peak_value_in_season(series: pd.Series, month_list: list[int]) -> float:
    """Return the maximum value in the given months."""
    mask = series.index.month.isin(month_list)  # type: ignore[union-attr]
    return float(series[mask].max())


# ---------------------------------------------------------------------------
# Build tidy DataFrames for plotting
# ---------------------------------------------------------------------------


def _extract_load_series(agg_df: pl.DataFrame, heating_type: str) -> pd.Series:
    """Extract a single heating_type's load as a pandas Series indexed by timestamp."""
    sub = (
        agg_df.filter(pl.col("heating_type") == heating_type)
        .select(["timestamp", "load_kwh"])
        .sort("timestamp")
        .to_pandas()
    )
    sub["timestamp"] = pd.to_datetime(sub["timestamp"])
    series = sub.set_index("timestamp")["load_kwh"]
    series.index.name = "time"
    if series.index.tz is None:
        series.index = series.index.tz_localize("EST", ambiguous="infer")
    return series


# Reference year used to normalize all timeseries to a common 8760 x-axis.
# AMY2018 is the ResStock weather year; Cambium data is also keyed to this calendar.
_REF_YEAR = 2018


def _normalize_ts(ts: pd.Timestamp, ref_year: int = _REF_YEAR) -> pd.Timestamp:
    """Replace the year of a timestamp with ref_year, preserving month/day/hour."""
    return ts.replace(year=ref_year)


def _normalize_index(
    index: pd.DatetimeIndex, ref_year: int = _REF_YEAR
) -> pd.DatetimeIndex:
    """Shift a DatetimeIndex to ref_year so all adoption years share a common x-axis."""
    normalized = pd.DatetimeIndex(
        [t.replace(year=ref_year) for t in index],
        tz=index.tz,
        name=index.name,
    )
    # Sort to handle Dec→Jan wrap: Dec timestamps become Dec 2018, Jan→Nov stay in 2018.
    return normalized


def _build_loads_df(
    loads_by_year: dict[int, pl.DataFrame],
    heating_type: str = _HEATING_TYPE_ALL,
) -> pd.DataFrame:
    """Build a tidy pandas DataFrame of hourly loads for one heating_type across years.

    Timestamps are normalized to ``_REF_YEAR`` so all years share a common 8760
    x-axis and overlay correctly in plots.
    """
    rows = []
    for year, agg_df in loads_by_year.items():
        sub = (
            agg_df.filter(pl.col("heating_type") == heating_type)
            .select(["timestamp", "load_kwh"])
            .sort("timestamp")
            .to_pandas()
        )
        sub["timestamp"] = pd.to_datetime(sub["timestamp"]).dt.tz_localize(None)
        # Normalize year → ref year for overlay.
        sub["time"] = sub["timestamp"].apply(lambda t: t.replace(year=_REF_YEAR))
        sub["year"] = str(year)
        rows.append(sub[["time", "load_kwh", "year"]])
    return pd.concat(rows, ignore_index=True)


def _build_cambium_col_df(
    cambium_by_year: dict[int, pd.DataFrame],
    col: str,
    value_name: str,
) -> pd.DataFrame:
    """Melt a single Cambium column across years into a tidy DataFrame.

    Timestamps are normalized to ``_REF_YEAR`` so all years share a common 8760
    x-axis and overlay correctly in plots.
    """
    rows = []
    for year, df in cambium_by_year.items():
        tmp = df[[col]].copy()
        # Strip tz and normalize to ref year.
        tmp.index = pd.DatetimeIndex(
            [t.replace(year=_REF_YEAR) for t in tmp.index],
            name="time",
        )
        tmp = tmp.reset_index()
        tmp.columns = pd.Index(["time", value_name])
        tmp["year"] = str(year)
        rows.append(tmp)
    return pd.concat(rows, ignore_index=True)


def _build_seasonal_peaks_df(
    cambium_by_year: dict[int, pd.DataFrame],
    loads_by_year: dict[int, pl.DataFrame],
    winter_months: list[int],
    summer_months: list[int],
) -> pd.DataFrame:
    """Build a year-level summary of Cambium and HP load seasonal peaks.

    HP load peaks use the ``"all"`` heating_type aggregate.
    """
    rows = []
    for year in sorted(cambium_by_year):
        cam = _cambium_series(cambium_by_year[year], "busbar_load")
        cam_winter_peak = _peak_value_in_season(cam, winter_months)
        cam_summer_peak = _peak_value_in_season(cam, summer_months)
        cam_winter_ts = _peak_hour_in_season(cam, winter_months)
        cam_summer_ts = _peak_hour_in_season(cam, summer_months)

        row: dict[str, Any] = {
            "year": year,
            "cambium_winter_peak_mwh": cam_winter_peak,
            "cambium_summer_peak_mwh": cam_summer_peak,
            "cambium_winter_peak_ts": cam_winter_ts,
            "cambium_summer_peak_ts": cam_summer_ts,
        }

        if year in loads_by_year:
            hp = _extract_load_series(loads_by_year[year], _HEATING_TYPE_ALL)
            row["hp_winter_peak_kwh"] = _peak_value_in_season(hp, winter_months)
            row["hp_summer_peak_kwh"] = _peak_value_in_season(hp, summer_months)
            row["hp_winter_peak_ts"] = _peak_hour_in_season(hp, winter_months)
            row["hp_summer_peak_ts"] = _peak_hour_in_season(hp, summer_months)
        else:
            row["hp_winter_peak_kwh"] = float("nan")
            row["hp_summer_peak_kwh"] = float("nan")
            row["hp_winter_peak_ts"] = pd.NaT
            row["hp_summer_peak_ts"] = pd.NaT

        rows.append(row)
    return pd.DataFrame(rows)


def _detect_crossover_year(
    peaks_df: pd.DataFrame,
    col_winter: str,
    col_summer: str,
) -> int | None:
    """Return the first year where winter peak surpasses summer peak, or None."""
    for _, row in peaks_df.iterrows():
        w = row[col_winter]
        s = row[col_summer]
        w_val = w.item() if hasattr(w, "item") else w
        s_val = s.item() if hasattr(s, "item") else s
        if pd.notna(w_val) and pd.notna(s_val) and float(w_val) > float(s_val):
            return int(row["year"])
    return None


# ---------------------------------------------------------------------------
# Plotting functions
# ---------------------------------------------------------------------------


def _year_color_map(years: list[int]) -> dict[str, str]:
    """Map string year labels to colors (used for summary/crossover plots)."""
    return {str(yr): _YEAR_COLORS[i % len(_YEAR_COLORS)] for i, yr in enumerate(years)}


_SEASON_COLORS = {"winter": "#4393c3", "summer": "#d6604d"}
_SEASON_LINETYPES = {"winter": "dashed", "summer": "dotted"}


def plot_loads_by_year(
    loads_by_year: dict[int, pl.DataFrame],
    winter_months: list[int],
    summer_months: list[int],
    path_plot: Path,
) -> None:
    """Save two faceted grid plots of weighted hourly HP adoption load.

    - ``<path_plot>`` — all buildings combined, one panel per year (ncol=2).
    - ``<stem>_by_heating_type<suffix>`` — facet grid: rows=heating_type, cols=year.
    """
    from plotnine import facet_wrap  # noqa: PLC0415

    years = sorted(loads_by_year)

    # ---- Peak marker DataFrame (shared by both plots) ----
    peak_rows = []
    for year, agg_df in loads_by_year.items():
        series = _extract_load_series(agg_df, _HEATING_TYPE_ALL)
        for season, months in [("winter", winter_months), ("summer", summer_months)]:
            ts = _peak_hour_in_season(series, months)
            peak_rows.append(
                {
                    "time": ts.replace(year=_REF_YEAR, tzinfo=None),
                    "year": str(year),
                    "season": season,
                }
            )
    peaks_df = pd.DataFrame(peak_rows)
    peaks_df["year"] = pd.Categorical(peaks_df["year"], categories=[str(y) for y in years])

    # ---- Plot 1: "all" aggregate — faceted by year ----
    df_all = _build_loads_df(loads_by_year, heating_type=_HEATING_TYPE_ALL)
    df_all["year"] = pd.Categorical(df_all["year"], categories=[str(y) for y in years])

    p = (
        ggplot(df_all, aes(x="time", y="load_kwh"))
        + geom_line(size=0.5, color="#2166ac", alpha=0.9)
        + geom_vline(
            data=peaks_df,
            mapping=aes(xintercept="time", color="season", linetype="season"),
            size=0.6,
            alpha=0.8,
        )
        + facet_wrap("year", ncol=2, scales="free_y")
        + scale_color_manual(values=_SEASON_COLORS)
        + scale_linetype_manual(values=_SEASON_LINETYPES)
        + scale_x_datetime(date_breaks="2 months", date_labels="%b")
        + labs(
            title="HP adoption trajectory — weighted aggregate residential load (all buildings)",
            x="Month (AMY2018 weather year)",
            y="Weighted aggregate load (kWh)",
            color="Season peak",
            linetype="Season peak",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11),
            axis_title=element_text(size=10),
            strip_text=element_text(size=10),
            legend_title=element_text(size=9),
            legend_text=element_text(size=9),
            panel_grid_minor=element_line(size=0),
        )
    )
    path_plot.parent.mkdir(parents=True, exist_ok=True)
    n_rows = -(-len(years) // 2)  # ceiling div
    p.save(str(path_plot), dpi=150, width=14, height=4 * n_rows)
    log.info("wrote %s", path_plot)

    # ---- Plot 2: by heating_type — faceted by year, one panel per (year × heating_type) ----
    ht_rows = []
    for year, agg_df in loads_by_year.items():
        heating_types = (
            agg_df.filter(pl.col("heating_type") != _HEATING_TYPE_ALL)["heating_type"]
            .unique()
            .sort()
            .to_list()
        )
        for ht in heating_types:
            sub = (
                agg_df.filter(pl.col("heating_type") == ht)
                .select(["timestamp", "load_kwh"])
                .sort("timestamp")
                .to_pandas()
            )
            sub["timestamp"] = pd.to_datetime(sub["timestamp"]).apply(
                lambda t: t.replace(year=_REF_YEAR)
            )
            sub = sub.rename(columns={"timestamp": "time"})
            sub["year"] = str(year)
            sub["heating_type"] = ht
            ht_rows.append(sub)

    if ht_rows:
        from plotnine import facet_grid  # noqa: PLC0415

        df_ht = pd.concat(ht_rows, ignore_index=True)
        df_ht["year"] = pd.Categorical(df_ht["year"], categories=[str(y) for y in years])
        # Peak markers per (year × heating_type) — use same winter/summer per year
        ht_peak_rows = []
        for year, agg_df in loads_by_year.items():
            heating_types_present = (
                agg_df.filter(pl.col("heating_type") != _HEATING_TYPE_ALL)["heating_type"]
                .unique()
                .to_list()
            )
            for ht in heating_types_present:
                ht_series = _extract_load_series(agg_df, ht)
                for season, months in [("winter", winter_months), ("summer", summer_months)]:
                    ts = _peak_hour_in_season(ht_series, months)
                    ht_peak_rows.append(
                        {
                            "time": ts.replace(year=_REF_YEAR, tzinfo=None),
                            "year": str(year),
                            "heating_type": ht,
                            "season": season,
                        }
                    )
        ht_peaks_df = pd.DataFrame(ht_peak_rows)
        ht_peaks_df["year"] = pd.Categorical(
            ht_peaks_df["year"], categories=[str(y) for y in years]
        )

        p2 = (
            ggplot(df_ht, aes(x="time", y="load_kwh"))
            + geom_line(size=0.4, color="#2166ac", alpha=0.9)
            + geom_vline(
                data=ht_peaks_df,
                mapping=aes(xintercept="time", color="season", linetype="season"),
                size=0.6,
                alpha=0.8,
            )
            + facet_grid("heating_type ~ year", scales="free_y")
            + scale_color_manual(values=_SEASON_COLORS)
            + scale_linetype_manual(values=_SEASON_LINETYPES)
            + scale_x_datetime(date_breaks="3 months", date_labels="%b")
            + labs(
                title="HP adoption trajectory — weighted load by heating type and year",
                x="Month (AMY2018 weather year)",
                y="Weighted aggregate load (kWh)",
                color="Season peak",
                linetype="Season peak",
            )
            + theme_minimal()
            + theme(
                plot_title=element_text(size=11),
                axis_title=element_text(size=10),
                strip_text=element_text(size=9),
                legend_title=element_text(size=9),
                legend_text=element_text(size=9),
                panel_grid_minor=element_line(size=0),
            )
        )
        p2_path = path_plot.with_name(
            path_plot.stem + "_by_heating_type" + path_plot.suffix
        )
        n_ht = len(set(df_ht["heating_type"]))
        p2.save(str(p2_path), dpi=150, width=4 * len(years), height=3 * n_ht)
        log.info("wrote %s", p2_path)


def _plot_cambium_timeseries(
    cambium_by_year: dict[int, pd.DataFrame],
    col: str,
    value_name: str,
    winter_months: list[int],
    summer_months: list[int],
    title: str,
    y_label: str,
    path_plot: Path,
    line_color: str = "#2166ac",
    free_y: bool = False,
) -> None:
    """Generic helper: plot a Cambium hourly column faceted by year with seasonal peak markers."""
    from plotnine import facet_wrap  # noqa: PLC0415

    years = sorted(cambium_by_year)
    df = _build_cambium_col_df(cambium_by_year, col=col, value_name=value_name)
    df["year"] = pd.Categorical(df["year"], categories=[str(y) for y in years])

    peak_rows = []
    for year, cam_df in cambium_by_year.items():
        series = _cambium_series(cam_df, col)
        for season, months in [("winter", winter_months), ("summer", summer_months)]:
            ts = _peak_hour_in_season(series, months)
            peak_rows.append(
                {
                    "time": ts.replace(year=_REF_YEAR, tzinfo=None),
                    "year": str(year),
                    "season": season,
                }
            )
    peaks_df = pd.DataFrame(peak_rows)
    peaks_df["year"] = pd.Categorical(peaks_df["year"], categories=[str(y) for y in years])

    scales_val = "free_y" if free_y else "fixed"
    p = (
        ggplot(df, aes(x="time", y=value_name))
        + geom_line(size=0.5, color=line_color, alpha=0.9)
        + geom_vline(
            data=peaks_df,
            mapping=aes(xintercept="time", color="season", linetype="season"),
            size=0.6,
            alpha=0.8,
        )
        + facet_wrap("year", ncol=2, scales=scales_val)
        + scale_color_manual(values=_SEASON_COLORS)
        + scale_linetype_manual(values=_SEASON_LINETYPES)
        + scale_x_datetime(date_breaks="2 months", date_labels="%b")
        + labs(
            title=title,
            x="Month (AMY2018 weather year)",
            y=y_label,
            color="Season peak",
            linetype="Season peak",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11),
            axis_title=element_text(size=10),
            strip_text=element_text(size=10),
            legend_title=element_text(size=9),
            legend_text=element_text(size=9),
            panel_grid_minor=element_line(size=0),
        )
    )
    path_plot.parent.mkdir(parents=True, exist_ok=True)
    n_rows = -(-len(years) // 2)
    p.save(str(path_plot), dpi=150, width=14, height=4 * n_rows)
    log.info("wrote %s", path_plot)


def plot_cambium_busbar_by_year(
    cambium_by_year: dict[int, pd.DataFrame],
    winter_months: list[int],
    summer_months: list[int],
    path_plot: Path,
) -> None:
    """Save a plot of Cambium busbar_load (MWh) for each year."""
    _plot_cambium_timeseries(
        cambium_by_year=cambium_by_year,
        col="busbar_load",
        value_name="busbar_mwh",
        winter_months=winter_months,
        summer_months=summer_months,
        title="Cambium busbar load by adoption year",
        y_label="Busbar load (MWh)",
        path_plot=path_plot,
        line_color="#2166ac",
        free_y=False,
    )


def plot_cambium_energy_mc_by_year(
    cambium_by_year: dict[int, pd.DataFrame],
    winter_months: list[int],
    summer_months: list[int],
    path_plot: Path,
) -> None:
    """Save a plot of Cambium energy_cost_enduse ($/kWh) for each year."""
    _plot_cambium_timeseries(
        cambium_by_year=cambium_by_year,
        col="energy_cost_enduse",
        value_name="energy_mc_per_kwh",
        winter_months=winter_months,
        summer_months=summer_months,
        title="Cambium marginal energy cost (energy_cost_enduse) by adoption year",
        y_label="Marginal energy cost ($/kWh)",
        path_plot=path_plot,
        line_color="#4d9221",
        free_y=False,
    )


def plot_cambium_capacity_mc_by_year(
    cambium_by_year: dict[int, pd.DataFrame],
    winter_months: list[int],
    summer_months: list[int],
    path_plot: Path,
) -> None:
    """Save a plot of Cambium capacity_cost_enduse ($/kWh) for each year."""
    _plot_cambium_timeseries(
        cambium_by_year=cambium_by_year,
        col="capacity_cost_enduse",
        value_name="capacity_mc_per_kwh",
        winter_months=winter_months,
        summer_months=summer_months,
        title="Cambium marginal capacity cost (capacity_cost_enduse) by adoption year",
        y_label="Marginal capacity cost ($/kWh)",
        path_plot=path_plot,
        line_color="#8b2252",
        free_y=True,
    )


def plot_cambium_seasonal_peaks(
    peaks_df: pd.DataFrame,
    path_plot: Path,
) -> None:
    """Save a year-by-year chart of Cambium winter vs summer busbar peaks.

    Marks any year where winter peak crosses above summer peak.
    """
    crossover_year = _detect_crossover_year(
        peaks_df, "cambium_winter_peak_mwh", "cambium_summer_peak_mwh"
    )

    melt = peaks_df[
        ["year", "cambium_winter_peak_mwh", "cambium_summer_peak_mwh"]
    ].melt(id_vars="year", var_name="season_col", value_name="peak_mwh")
    melt["season"] = melt["season_col"].map(
        {
            "cambium_winter_peak_mwh": "Winter peak",
            "cambium_summer_peak_mwh": "Summer peak",
        }
    )

    p = (
        ggplot(melt, aes(x="year", y="peak_mwh", color="season"))
        + geom_line(size=1.2)
        + geom_point(size=3)
        + scale_color_manual(
            values={"Winter peak": "#4393c3", "Summer peak": "#d6604d"}
        )
        + labs(
            title="Cambium busbar load — winter vs summer peak by year",
            subtitle=(
                f"Crossover year (winter > summer): {crossover_year}"
                if crossover_year
                else "No winter-over-summer crossover in adoption window"
            ),
            x="Year",
            y="Busbar peak load (MWh)",
            color="Season",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11),
            plot_subtitle=element_text(size=9),
            axis_title=element_text(size=10),
            legend_title=element_text(size=9),
            legend_text=element_text(size=9),
            panel_grid_minor=element_line(size=0),
        )
    )
    if crossover_year is not None:
        p = p + geom_vline(
            xintercept=crossover_year,
            color="#333333",
            linetype="dashed",
            size=0.7,
        )
    path_plot.parent.mkdir(parents=True, exist_ok=True)
    p.save(str(path_plot), dpi=150, width=10, height=5)
    log.info("wrote %s", path_plot)


def plot_hp_load_vs_cambium_peaks(
    peaks_df: pd.DataFrame,
    path_plot: Path,
) -> None:
    """Save a chart comparing HP residential peak load vs Cambium busbar peaks.

    Both series are normalized (index=1.0 at the first adoption year) so the
    relative trajectory — and any divergence or convergence — is visible despite
    the different units (kWh vs MWh).
    """
    df = peaks_df.dropna(subset=["hp_winter_peak_kwh", "hp_summer_peak_kwh"]).copy()
    if df.empty:
        log.warning("No HP load data in peaks_df; skipping hp_vs_cambium plot")
        return

    first_year_row = df.iloc[0]

    def _norm(col: str, ref_col: str | None = None) -> pd.Series:
        ref = float(first_year_row[ref_col or col])
        if ref == 0:
            return df[col]
        return df[col] / ref

    df["hp_winter_norm"] = _norm("hp_winter_peak_kwh")
    df["hp_summer_norm"] = _norm("hp_summer_peak_kwh")
    df["cam_winter_norm"] = _norm("cambium_winter_peak_mwh")
    df["cam_summer_norm"] = _norm("cambium_summer_peak_mwh")

    melt_rows = []
    series_defs = [
        ("hp_winter_norm", "HP load — winter peak"),
        ("hp_summer_norm", "HP load — summer peak"),
        ("cam_winter_norm", "Cambium busbar — winter peak"),
        ("cam_summer_norm", "Cambium busbar — summer peak"),
    ]
    for col, label in series_defs:
        for _, row in df.iterrows():
            melt_rows.append(
                {"year": int(row["year"]), "value": float(row[col]), "series": label}
            )
    melt = pd.DataFrame(melt_rows)

    color_map = {
        "HP load — winter peak": "#4393c3",
        "HP load — summer peak": "#d6604d",
        "Cambium busbar — winter peak": "#92c5de",
        "Cambium busbar — summer peak": "#f4a582",
    }
    linetype_map = {
        "HP load — winter peak": "solid",
        "HP load — summer peak": "solid",
        "Cambium busbar — winter peak": "dashed",
        "Cambium busbar — summer peak": "dashed",
    }

    # Detect crossover in HP load (winter vs summer).
    hp_crossover = _detect_crossover_year(
        df,
        col_winter="hp_winter_peak_kwh",
        col_summer="hp_summer_peak_kwh",
    )

    p = (
        ggplot(melt, aes(x="year", y="value", color="series", linetype="series"))
        + geom_hline(yintercept=1.0, color="#aaaaaa", linetype="dotted", size=0.5)
        + geom_line(size=1.1)
        + geom_point(size=2.5)
        + scale_color_manual(values=color_map)
        + scale_linetype_manual(values=linetype_map)
        + labs(
            title="HP adoption load vs Cambium busbar peaks — normalized to first year",
            subtitle=(
                f"HP load winter-over-summer crossover: year {hp_crossover}"
                if hp_crossover
                else "HP load: no winter-over-summer crossover in adoption window"
            ),
            x="Year",
            y="Normalized peak (index = 1.0 at first year)",
            color="Series",
            linetype="Series",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11),
            plot_subtitle=element_text(size=9),
            axis_title=element_text(size=10),
            legend_title=element_text(size=9),
            legend_text=element_text(size=9),
            panel_grid_minor=element_line(size=0),
        )
    )
    if hp_crossover is not None:
        p = p + geom_vline(
            xintercept=hp_crossover,
            color="#333333",
            linetype="dashed",
            size=0.7,
        )
    path_plot.parent.mkdir(parents=True, exist_ok=True)
    p.save(str(path_plot), dpi=150, width=10, height=5)
    log.info("wrote %s", path_plot)


# ---------------------------------------------------------------------------
# Demand-weighted marginal cost ("cost of service") plot
# ---------------------------------------------------------------------------

# Heating types to treat as "HP customer" vs "non-HP customer".
_HP_HEATING_TYPES = {"heat_pump"}
_NONHP_HEATING_TYPES = {"fossil_fuel", "electrical_resistance"}


def _demand_weighted_mc(
    load_series: pd.Series,
    mc_series: pd.Series,
    month_list: list[int],
) -> float:
    """Return load-weighted average MC ($/kWh) for the given season months.

    Σ(load_h × mc_h) / Σ(load_h) — the effective $/kWh a customer type
    would pay if billed at marginal cost each hour.
    """
    mask = load_series.index.month.isin(month_list)
    l = load_series[mask]
    m = mc_series.reindex(l.index)
    total_load = float(l.sum())
    if total_load == 0:
        return float("nan")
    return float((l * m).sum() / total_load)


def _build_demand_weighted_mc_df(
    loads_by_year: dict[int, pl.DataFrame],
    cambium_by_year: dict[int, pd.DataFrame],
    winter_months: list[int],
    summer_months: list[int],
) -> pd.DataFrame:
    """Build a tidy long DataFrame of demand-weighted supply MC by (year, customer_type, season).

    Supply MC = energy_cost_enduse + capacity_cost_enduse, demand-weighted:
      Σ(load_h × (energy_mc_h + capacity_mc_h)) / Σ(load_h)

    Columns: year, customer_type, season, dw_mc_per_kwh
    """
    rows = []
    for year in sorted(set(loads_by_year) & set(cambium_by_year)):
        cam = cambium_by_year[year]
        agg_df = loads_by_year[year]

        # Normalize Cambium index: replace calendar year → _REF_YEAR and strip
        # timezone so it aligns with the tz-naive load cache timestamps.
        cam_norm = cam.copy()
        cam_norm.index = pd.DatetimeIndex(
            [t.replace(year=_REF_YEAR).replace(tzinfo=None) for t in cam_norm.index],
            name="time",
        )
        supply_mc: pd.Series = (
            pd.to_numeric(cam_norm["energy_cost_enduse"], errors="coerce").fillna(0.0)
            + pd.to_numeric(cam_norm["capacity_cost_enduse"], errors="coerce").fillna(0.0)
        )

        # All distinct heating types present (excluding the "all" aggregate).
        heating_types = (
            agg_df.filter(pl.col("heating_type") != _HEATING_TYPE_ALL)["heating_type"]
            .unique()
            .to_list()
        )

        # Map heating types → customer group label.
        type_to_group: dict[str, str] = {}
        for ht in heating_types:
            if ht in _HP_HEATING_TYPES:
                type_to_group[ht] = "HP customers"
            elif ht in _NONHP_HEATING_TYPES:
                type_to_group[ht] = "Non-HP customers"

        # Pool load × MC across all heating types in a group (can't average DWMCs).
        group_season_load: dict[tuple[str, str], float] = {}
        group_season_wmc: dict[tuple[str, str], float] = {}

        for ht, group in type_to_group.items():
            load_s = _extract_load_series(agg_df, ht)
            # load_s is indexed in _REF_YEAR with tz from tz_localize; strip tz for alignment.
            if load_s.index.tz is not None:
                load_s = load_s.copy()
                load_s.index = load_s.index.tz_localize(None)

            for season, months in [("Winter", winter_months), ("Summer", summer_months)]:
                key = (group, season)
                mask = load_s.index.month.isin(months)
                l = load_s[mask]
                mc = supply_mc.reindex(l.index)
                total_load = float(l.sum())
                group_season_load[key] = group_season_load.get(key, 0.0) + total_load
                group_season_wmc[key] = group_season_wmc.get(key, 0.0) + float(
                    (l * mc).sum()
                )

        for (group, season), total_load in group_season_load.items():
            if total_load == 0:
                continue
            rows.append(
                {
                    "year": year,
                    "customer_type": group,
                    "season": season,
                    "dw_mc_per_kwh": group_season_wmc[(group, season)] / total_load,
                }
            )

    return pd.DataFrame(rows)


def plot_demand_weighted_mc(
    loads_by_year: dict[int, pl.DataFrame],
    cambium_by_year: dict[int, pd.DataFrame],
    winter_months: list[int],
    summer_months: list[int],
    path_plot: Path,
) -> None:
    """Save a grouped bar chart of demand-weighted supply MC by year.

    Supply MC = energy_cost_enduse + capacity_cost_enduse, load-weighted:
      Σ(load_h × supply_mc_h) / Σ(load_h)

    Layout: facet rows = customer_type (HP / non-HP), x = year,
    bars filled by season (Winter / Summer) — side-by-side within each year.
    """
    from plotnine import (  # noqa: PLC0415
        facet_wrap,
        geom_bar,
        position_dodge,
        scale_fill_manual,
    )

    df = _build_demand_weighted_mc_df(
        loads_by_year=loads_by_year,
        cambium_by_year=cambium_by_year,
        winter_months=winter_months,
        summer_months=summer_months,
    )
    if df.empty:
        log.warning("No demand-weighted MC data; skipping plot")
        return

    years = sorted(df["year"].unique())
    df["year"] = pd.Categorical(df["year"], categories=years)
    df["season"] = pd.Categorical(df["season"], categories=["Winter", "Summer"])
    df["customer_type"] = pd.Categorical(
        df["customer_type"], categories=["HP customers", "Non-HP customers"]
    )

    season_colors = {"Winter": "#4393c3", "Summer": "#d6604d"}

    dodge = position_dodge(width=0.8)

    p = (
        ggplot(df, aes(x="factor(year)", y="dw_mc_per_kwh", fill="season"))
        + geom_bar(stat="identity", position=dodge, width=0.7, alpha=0.9)
        + facet_wrap("customer_type", ncol=1, scales="fixed")
        + scale_fill_manual(values=season_colors)
        + labs(
            title="Demand-weighted supply marginal cost by customer type and adoption year",
            subtitle="Supply MC = energy + capacity MC; load-weighted avg (Σ load·MC / Σ load) — Cambium MidCase",
            x="Adoption year",
            y="Demand-weighted supply MC ($/kWh)",
            fill="Season",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11),
            plot_subtitle=element_text(size=9),
            axis_title=element_text(size=10),
            strip_text=element_text(size=11),
            legend_title=element_text(size=9),
            legend_text=element_text(size=9),
            panel_grid_minor=element_line(size=0),
            axis_text_x=element_text(size=9),
        )
    )

    path_plot.parent.mkdir(parents=True, exist_ok=True)
    p.save(str(path_plot), dpi=150, width=12, height=7)
    log.info("wrote %s", path_plot)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Plot HP adoption trajectory loads and Cambium busbar loads "
            "across scenario years."
        ),
    )
    p.add_argument(
        "--adoption-config",
        required=True,
        metavar="PATH",
        dest="path_adoption_config",
        help="Adoption trajectory YAML (e.g. config/adoption/nyca_electrification.yaml).",
    )
    p.add_argument(
        "--materialized-dir",
        required=True,
        metavar="PATH",
        dest="path_materialized_dir",
        help=(
            "Root of materialized ResStock adoption data with year=YYYY/ subdirs "
            "(e.g. .../adoption/nyca_electrification)."
        ),
    )
    p.add_argument(
        "--path-utility-assignment",
        required=True,
        metavar="PATH",
        dest="path_utility_assignment",
        help="Path to utility_assignment.parquet providing bldg_id→weight mapping.",
    )
    p.add_argument(
        "--state",
        required=True,
        help="Two-letter state abbreviation (e.g. NY).",
    )
    p.add_argument(
        "--upgrade",
        default="00",
        help="Upgrade partition value for materialized loads (default: 00).",
    )
    p.add_argument(
        "--cambium-path",
        required=True,
        metavar="TEMPLATE",
        dest="cambium_path_template",
        help=(
            "Cambium parquet path template with {year} placeholder, e.g. "
            "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t={year}/gea=NYISO/r=p127/data.parquet"
        ),
    )
    p.add_argument(
        "--winter-months",
        default=",".join(str(m) for m in _DEFAULT_WINTER_MONTHS),
        metavar="M,M,...",
        help=(
            "Comma-separated winter month numbers for peak detection "
            f"(default: {','.join(str(m) for m in _DEFAULT_WINTER_MONTHS)})."
        ),
    )
    p.add_argument(
        "--output-dir",
        required=True,
        metavar="PATH",
        dest="path_output_dir",
        help="Directory where PNG plots are written.",
    )
    p.add_argument(
        "--agg-cache-dir",
        metavar="PATH",
        dest="path_agg_cache_dir",
        default=None,
        help=(
            "Directory for per-year aggregated load cache parquets "
            "(default: <output-dir>/agg_loads/). "
            "If a year's cache exists it is read directly, skipping the full scan."
        ),
    )
    return p


def main() -> None:
    args = build_parser().parse_args()

    path_adoption_config = Path(args.path_adoption_config)
    path_output_dir = Path(args.path_output_dir)

    log.info("loading adoption config: %s", path_adoption_config)
    config = _load_adoption_config(path_adoption_config)
    run_years = _parse_run_years(config)
    log.info("adoption run years: %s", run_years)

    winter_months_raw = parse_months_arg(args.winter_months)
    winter_months, summer_months = resolve_winter_summer_months(
        winter_months_raw,
        default_winter_months=_DEFAULT_WINTER_MONTHS,
    )
    log.info("winter months: %s  |  summer months: %s", winter_months, summer_months)

    log.info("loading building weights from: %s", args.path_utility_assignment)
    weights = _load_weights(args.path_utility_assignment)
    log.info("loaded %d buildings with weights", len(weights))

    cache_dir = Path(
        args.path_agg_cache_dir
        if args.path_agg_cache_dir
        else path_output_dir / "agg_loads"
    )
    log.info("aggregated load cache dir: %s", cache_dir)

    # ------------------------------------------------------------------
    # Load per-year data (one year at a time to minimise peak memory)
    # ------------------------------------------------------------------

    loads_by_year: dict[int, pl.DataFrame] = {}
    cambium_by_year: dict[int, pd.DataFrame] = {}

    for year in run_years:
        log.info("--- year %d ---", year)

        log.info("  loading HP adoption loads for year=%d", year)
        try:
            agg_df = _load_or_build_agg_loads(
                materialized_dir=args.path_materialized_dir,
                year=year,
                state=args.state,
                upgrade=args.upgrade,
                weights=weights,
                cache_dir=cache_dir,
            )
            loads_by_year[year] = agg_df
            total_rows = len(agg_df.filter(pl.col("heating_type") == _HEATING_TYPE_ALL))
            _peak_val = agg_df.filter(pl.col("heating_type") == _HEATING_TYPE_ALL)[
                "load_kwh"
            ].max()
            peak_all = float(_peak_val) if isinstance(_peak_val, (int, float)) else 0.0
            log.info(
                "  year %d: %d hourly records (all), peak=%.1f kWh, heating_types=%s",
                year,
                total_rows,
                peak_all,
                sorted(agg_df["heating_type"].unique().to_list()),
            )
        except Exception as exc:
            log.warning("  skipping HP loads for year %d: %s", year, exc)

        log.info("  loading Cambium data for year=%d", year)
        try:
            cambium_by_year[year] = _load_cambium_for_year(
                cambium_path_template=args.cambium_path_template,
                year=year,
            )
            cam_df = cambium_by_year[year]
            log.info(
                "  year %d: %d Cambium rows, busbar peak=%.1f MWh, "
                "energy_mc peak=%.4f $/kWh, capacity_mc peak=%.4f $/kWh",
                year,
                len(cam_df),
                float(cam_df["busbar_load"].max()),
                float(cam_df["energy_cost_enduse"].max()),
                float(cam_df["capacity_cost_enduse"].max()),
            )
        except Exception as exc:
            log.warning("  skipping Cambium data for year %d: %s", year, exc)

    if not cambium_by_year:
        raise RuntimeError(
            "No Cambium data loaded for any year; cannot produce plots. "
            "Check --cambium-path template and network access."
        )

    # ------------------------------------------------------------------
    # Build peak summary
    # ------------------------------------------------------------------

    peaks_df = _build_seasonal_peaks_df(
        cambium_by_year=cambium_by_year,
        loads_by_year=loads_by_year,
        winter_months=winter_months,
        summer_months=summer_months,
    )
    log.info("peak summary:\n%s", peaks_df.to_string(index=False))

    # ------------------------------------------------------------------
    # Save plots
    # ------------------------------------------------------------------

    if loads_by_year:
        log.info("plotting HP adoption loads by year")
        plot_loads_by_year(
            loads_by_year=loads_by_year,
            winter_months=winter_months,
            summer_months=summer_months,
            path_plot=path_output_dir / "loads_by_year.png",
        )

    log.info("plotting Cambium busbar load by year")
    plot_cambium_busbar_by_year(
        cambium_by_year=cambium_by_year,
        winter_months=winter_months,
        summer_months=summer_months,
        path_plot=path_output_dir / "cambium_busbar_by_year.png",
    )

    log.info("plotting Cambium seasonal peaks by year")
    plot_cambium_seasonal_peaks(
        peaks_df=peaks_df,
        path_plot=path_output_dir / "cambium_seasonal_peaks_by_year.png",
    )

    if loads_by_year:
        log.info("plotting HP load vs Cambium peaks by year")
        plot_hp_load_vs_cambium_peaks(
            peaks_df=peaks_df,
            path_plot=path_output_dir / "load_vs_cambium_peaks_by_year.png",
        )

    log.info("plotting Cambium energy marginal cost by year")
    plot_cambium_energy_mc_by_year(
        cambium_by_year=cambium_by_year,
        winter_months=winter_months,
        summer_months=summer_months,
        path_plot=path_output_dir / "cambium_energy_mc_by_year.png",
    )

    log.info("plotting Cambium capacity marginal cost by year")
    plot_cambium_capacity_mc_by_year(
        cambium_by_year=cambium_by_year,
        winter_months=winter_months,
        summer_months=summer_months,
        path_plot=path_output_dir / "cambium_capacity_mc_by_year.png",
    )

    if loads_by_year and cambium_by_year:
        log.info("plotting demand-weighted marginal cost by customer type")
        plot_demand_weighted_mc(
            loads_by_year=loads_by_year,
            cambium_by_year=cambium_by_year,
            winter_months=winter_months,
            summer_months=summer_months,
            path_plot=path_output_dir / "demand_weighted_mc_by_year.png",
        )

    log.info("done — plots written to %s", path_output_dir)


if __name__ == "__main__":
    main()

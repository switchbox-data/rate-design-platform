"""Load a sample of ResStock hourly load curves for NY, aggregate by hour, and plot.

Reads from the local EBS copy of ResStock. Samples N building IDs from metadata,
scans the load_curve_hourly partition for those buildings, selects only the total
electricity column, sums across buildings per timestamp, and plots as a heatmap.

Usage:
    uv run python utils/post/view_resstock_hourly_loads.py
    uv run python utils/post/view_resstock_hourly_loads.py --n-sample 2000 --output-dir /tmp
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl
from plotnine import (
    aes,
    element_text,
    geom_line,
    geom_tile,
    ggplot,
    labs,
    scale_fill_cmap,
    scale_x_continuous,
    scale_x_datetime,
    scale_y_reverse,
    theme,
    theme_minimal,
)

RESSTOCK_LOCAL = "/ebs/data/nrel/resstock/res_2024_amy2018_2_sb"
LOAD_COL = "out.electricity.total.energy_consumption"


ELECTRIC_RESISTANCE_TYPES = {
    "Electricity Baseboard",
    "Electricity Electric Furnace",
    "Electricity Electric Wall Furnace",
    "Electricity Electric Boiler",
    "Electricity Shared Heating",
}


def sample_bldg_ids(n: int) -> list[int]:
    """Sample N building IDs from the NY upgrade=00 metadata.

    Excludes buildings with electric resistance heating (baseboard, electric
    furnace, electric wall furnace, electric boiler, shared electric).
    """
    all_ids = (
        pl.scan_parquet(
            f"{RESSTOCK_LOCAL}/metadata/state=NY/upgrade=00/metadata-sb.parquet",
        )
        .filter(
            ~pl.col("in.hvac_heating_type_and_fuel").is_in(ELECTRIC_RESISTANCE_TYPES)
        )
        .select("bldg_id")
        .collect()
    )
    assert isinstance(all_ids, pl.DataFrame)
    return all_ids.sample(min(n, all_ids.height), seed=42)["bldg_id"].to_list()


def load_aggregate_hourly(bldg_ids: list[int]) -> pl.DataFrame:
    """Scan local load curves for sampled buildings, sum total electricity per hour."""
    result = (
        pl.scan_parquet(
            f"{RESSTOCK_LOCAL}/load_curve_hourly/state=NY/upgrade=00/",
            hive_partitioning=False,
        )
        .filter(pl.col("bldg_id").is_in(bldg_ids))
        .select("timestamp", LOAD_COL)
        .group_by("timestamp")
        .agg(pl.col(LOAD_COL).sum())
        .sort("timestamp")
        .collect()
    )
    assert isinstance(result, pl.DataFrame)
    return result


def _prepare_heatmap_df(df: pl.DataFrame) -> pl.DataFrame:
    """Add hour-of-day and day-of-year columns for heatmap plotting."""
    return df.with_columns(
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
    )


def _month_breaks(year: int) -> tuple[list[int], list[str]]:
    """Return (day-of-year breaks, month abbreviation labels) for the 1st of each month."""
    import datetime

    breaks: list[int] = []
    labels: list[str] = []
    for m in range(1, 13):
        dt = datetime.date(year, m, 1)
        breaks.append(dt.timetuple().tm_yday)
        labels.append(dt.strftime("%b"))
    return breaks, labels


def plot_hourly_load(df: pl.DataFrame, n_sample: int) -> ggplot:
    """Hour-of-day x day-of-year heatmap of aggregated electricity load."""
    heatmap_df = _prepare_heatmap_df(df)

    year = df["timestamp"].dt.year()[0]
    breaks, labels = _month_breaks(year)

    vmin = float(heatmap_df[LOAD_COL].min())  # type: ignore[arg-type]
    vmax = float(heatmap_df[LOAD_COL].max())  # type: ignore[arg-type]
    n_intervals = 5
    step = (vmax - vmin) / n_intervals if vmax != vmin else 1.0
    fill_breaks = [vmin + i * step for i in range(n_intervals + 1)]
    fill_labels = [f"{v:,.0f}" for v in fill_breaks]

    return (
        ggplot(heatmap_df, aes(x="day_of_year", y="hour", fill=LOAD_COL))
        + geom_tile()
        + scale_fill_cmap(
            "plasma",
            limits=(vmin, vmax),
            breaks=fill_breaks,
            labels=fill_labels,
        )
        + scale_x_continuous(breaks=breaks, labels=labels)
        + scale_y_reverse()
        + labs(
            x="",
            y="Hour of Day",
            title=f"Aggregate Hourly Electric Load (kWh) — NY ResStock Sample (n={n_sample})",
            fill="kWh",
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 5),
            plot_title=element_text(size=10),
            axis_title=element_text(size=9),
            axis_text=element_text(size=7),
            legend_text=element_text(size=7),
            legend_title=element_text(size=8),
        )
    )


def plot_hourly_load_ts(df: pl.DataFrame, n_sample: int) -> ggplot:
    """Hourly time series line plot of aggregated electricity load."""
    return (
        ggplot(df, aes(x="timestamp", y=LOAD_COL))
        + geom_line(size=0.3, alpha=0.7, color="#440154")
        + scale_x_datetime(date_breaks="1 month", date_labels="%b")
        + labs(
            x="",
            y="Total Electricity (kWh)",
            title=f"Aggregate Hourly Electric Load — NY ResStock Sample (n={n_sample})",
        )
        + theme_minimal()
        + theme(
            figure_size=(14, 4),
            plot_title=element_text(size=10),
            axis_title=element_text(size=9),
            axis_text=element_text(size=7),
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--n-sample", type=int, default=5000, help="Number of buildings to sample."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/ebs/home/alex_switch_box/rate-design-platform/dev_plots",
        help="Directory for PNG output.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    n_sample = args.n_sample
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Sampling {n_sample} building IDs from metadata …")
    bldg_ids = sample_bldg_ids(n_sample)
    print(f"  got {len(bldg_ids)} IDs (first 5: {bldg_ids[:5]})")

    print(f"Loading load curves for {len(bldg_ids)} buildings …")
    df = load_aggregate_hourly(bldg_ids)
    print(f"  aggregated shape: {df.shape}")
    print(df.head(5))

    p_heatmap = plot_hourly_load(df, n_sample)
    path_heatmap = output_dir / "resstock_hourly_load_ny.png"
    p_heatmap.save(str(path_heatmap), width=14, height=5, dpi=150, verbose=False)
    print(f"Saved: {path_heatmap}")

    p_ts = plot_hourly_load_ts(df, n_sample)
    path_ts = output_dir / "resstock_hourly_load_ny_ts.png"
    p_ts.save(str(path_ts), width=14, height=4, dpi=150, verbose=False)
    print(f"Saved: {path_ts}")


if __name__ == "__main__":
    main()

"""Validate marginal cost input datasets used by CAIRO for a state's utilities.

For each utility, loads supply energy, supply capacity, bulk TX, and dist/sub-TX
marginal cost parquets from S3, runs sanity checks (row counts, nulls, non-negative
values, expected nonzero hour counts, year matching), prints a text summary, and
generates:
  1. A 4-quadrant heatmap per utility (hour-of-day x day-of-year, magma colormap,
     independent color scale per MC type).
  2. A faceted histogram of supply energy MC across all utilities.

Usage:
    uv run python utils/post/validate_ny_mc_data.py --state ny --output-dir /tmp/mc_validation
    uv run python utils/post/validate_ny_mc_data.py --state ri --utilities rie --year 2025
    uv run python utils/post/validate_ny_mc_data.py --state ny --year 2025 # all utilities in state
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import polars as pl
from plotnine import (
    aes,
    element_text,
    facet_wrap,
    geom_histogram,
    geom_tile,
    ggplot,
    labs,
    scale_fill_cmap,
    scale_x_continuous,
    scale_y_reverse,
    theme,
    theme_minimal,
)
from plotnine.composition import Compose

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.pre.marginal_costs.supply_utils import (
    build_mc_partition_parquet_path,
    build_mc_partition_path,
    list_partition_parquet_paths,
)

# ── Constants ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class StateValidationConfig:
    utilities: tuple[str, ...]
    default_mc_table_relpath: str


STATE_CONFIGS: dict[str, StateValidationConfig] = {
    "ny": StateValidationConfig(
        utilities=("cenhud", "coned", "nimo", "nyseg", "or", "psegli", "rge"),
        default_mc_table_relpath=(
            "rate_design/hp_rates/ny/config/marginal_costs/"
            "ny_sub_tx_and_dist_mc_levelized.csv"
        ),
    ),
    "ri": StateValidationConfig(
        utilities=("rie",),
        default_mc_table_relpath=(
            "rate_design/hp_rates/ri/config/marginal_costs/ri_marginal_costs_2025.csv"
        ),
    ),
}

MC_TYPE_DEFS: dict[str, dict[str, str]] = {
    "supply_energy": {
        "s3_suffix": "supply/energy/",
        "value_col": "energy_cost_enduse",
        "raw_units": "$/MWh",
        "label": "Supply Energy",
    },
    "supply_capacity": {
        "s3_suffix": "supply/capacity/",
        "value_col": "capacity_cost_enduse",
        "raw_units": "$/MWh",
        "label": "Supply Capacity",
    },
    "bulk_tx": {
        "s3_suffix": "bulk_tx/",
        "value_col": "bulk_tx_cost_enduse",
        "raw_units": "$/kWh",
        "label": "Bulk Transmission",
    },
    "dist_sub_tx": {
        "s3_suffix": "dist_and_sub_tx/",
        "value_col": "mc_total_per_kwh",
        "raw_units": "$/kWh",
        "label": "Dist & Sub-TX",
    },
}

NORMALIZED_COL = "mc_kwh"
NORMALIZED_UNITS = "$/kWh"

EXPECTED_NONZERO: dict[str, int | None] = {
    "supply_energy": None,  # virtually all hours; no hard expectation
    "supply_capacity": 96,  # 8 peak hours/month x 12
    "bulk_tx": 80,  # 40 SCR hours/season x 2
    "dist_sub_tx": 100,  # default N_HOURS for PoP
}

STATE_EXPECTED_NONZERO_OVERRIDES: dict[str, dict[str, int | None]] = {
    "ri": {"bulk_tx": 100},
}

SUMMER_MONTHS = {4, 5, 6, 7, 8, 9}
WINTER_MONTHS = {1, 2, 3, 10, 11, 12}

# Quadrant layout order (row-major): top-left, top-right, bottom-left, bottom-right
QUADRANT_ORDER = ["supply_energy", "supply_capacity", "bulk_tx", "dist_sub_tx"]


# ── Data loading ──────────────────────────────────────────────────────────────


def load_mc(
    mc_key: str,
    state: str,
    utility: str,
    year: int,
    storage_options: dict[str, str],
    mc_base_path: str = "s3://data.sb/switchbox/marginal_costs",
) -> pl.DataFrame:
    """Load a single canonical MC parquet for one utility/year.

    All values are normalized to $/kWh (supply energy and capacity are stored as
    $/MWh on S3, so they are divided by 1000). The value column is renamed to
    ``NORMALIZED_COL`` ("mc_kwh") regardless of MC type.
    """
    info = MC_TYPE_DEFS[mc_key]
    value_col = info["value_col"]
    canonical_path = build_mc_partition_parquet_path(
        state=state,
        component=mc_key,
        utility=utility,
        year=year,
        filename="data.parquet",
        mc_base_path=mc_base_path,
    )
    try:
        df = pl.read_parquet(canonical_path, storage_options=storage_options)
    except Exception as e:
        path_partition = build_mc_partition_path(
            state=state,
            component=mc_key,
            utility=utility,
            year=year,
            mc_base_path=mc_base_path,
        )
        parquet_paths = list_partition_parquet_paths(path_partition)
        if not parquet_paths:
            raise FileNotFoundError(
                f"{mc_key}: canonical file missing at {canonical_path}. "
                f"No parquet files found in partition {path_partition}. "
                "Remediation: generate/upload marginal costs for this "
                "state/utility/year before running validation."
            ) from e

        filenames = ", ".join(Path(p).name for p in parquet_paths)
        raise FileNotFoundError(
            f"{mc_key}: canonical file missing at {canonical_path}. "
            f"Found partition parquet files: {filenames}. "
            "Remediation: write canonical data.parquet (or move stale files out of "
            "the partition) so validation reads a deterministic input."
        ) from e

    if "timestamp" in df.columns:
        ts_dtype = df.schema["timestamp"]
        if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
            df = df.with_columns(
                pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp")
            )

    if value_col not in df.columns:
        raise ValueError(
            f"{mc_key}: expected column '{value_col}' not found. Columns: {df.columns}"
        )

    df = df.select("timestamp", value_col).sort("timestamp")

    # Normalize to $/kWh
    if info["raw_units"] == "$/MWh":
        df = df.with_columns((pl.col(value_col) / 1000.0).alias(NORMALIZED_COL))
    else:
        df = df.rename({value_col: NORMALIZED_COL})

    return df.select("timestamp", NORMALIZED_COL)


# ── Sanity checks ─────────────────────────────────────────────────────────────


def check_mc(
    df: pl.DataFrame,
    mc_key: str,
    utility: str,
    year: int,
    state: str,
) -> dict:
    """Run sanity checks on one MC DataFrame. Returns a results dict."""
    info = MC_TYPE_DEFS[mc_key]
    col = NORMALIZED_COL
    results: dict = {
        "mc_type": mc_key,
        "utility": utility,
        "year": year,
        "label": info["label"],
        "units": NORMALIZED_UNITS,
        "issues": [],
    }

    # Row count
    n_rows = df.height
    results["n_rows"] = n_rows
    if n_rows != 8760:
        results["issues"].append(f"FAIL: expected 8760 rows, got {n_rows}")

    # Nulls
    n_null_ts = df.filter(pl.col("timestamp").is_null()).height
    n_null_val = df.filter(pl.col(col).is_null()).height
    results["n_null_timestamp"] = n_null_ts
    results["n_null_value"] = n_null_val
    if n_null_ts > 0:
        results["issues"].append(f"FAIL: {n_null_ts} null timestamps")
    if n_null_val > 0:
        results["issues"].append(f"FAIL: {n_null_val} null values")

    # Year in timestamps
    if n_rows > 0 and n_null_ts == 0:
        years_found = df.select(pl.col("timestamp").dt.year().unique()).to_series()
        if len(years_found) != 1 or years_found[0] != year:
            results["issues"].append(
                f"WARN: timestamp years {years_found.to_list()}, expected [{year}]"
            )

    # Duplicate timestamps
    n_unique_ts = df.select(pl.col("timestamp").n_unique()).item()
    if n_unique_ts != n_rows:
        results["issues"].append(f"FAIL: {n_rows - n_unique_ts} duplicate timestamps")

    # Value stats
    vals = df[col]
    results["min"] = cast(float, vals.min() or 0.0)
    results["max"] = cast(float, vals.max() or 0.0)
    results["mean"] = cast(float, vals.mean() or 0.0)
    results["median"] = cast(float, vals.median() or 0.0)
    results["p05"] = cast(float, vals.quantile(0.05) or 0.0)
    results["p95"] = cast(float, vals.quantile(0.95) or 0.0)

    # Non-zero hours
    n_nonzero = df.filter(pl.col(col) != 0.0).height
    results["n_nonzero"] = n_nonzero
    expected = _expected_nonzero_hours(state, mc_key)
    if expected is not None and n_nonzero != expected:
        results["issues"].append(
            f"WARN: expected {expected} nonzero hours, got {n_nonzero}"
        )

    # Total cost recovery (sum of hourly values)
    total = float(vals.sum())
    results["total_sum"] = total

    # Non-negative check (skip for supply energy which can be negative)
    if mc_key != "supply_energy":
        n_negative = df.filter(pl.col(col) < 0).height
        results["n_negative"] = n_negative
        if n_negative > 0:
            results["issues"].append(f"FAIL: {n_negative} negative values")

    # Top-10 hours
    top10 = df.sort(col, descending=True).head(10)
    results["top10"] = top10

    # MC-type-specific checks
    if mc_key == "supply_capacity":
        _check_capacity_monthly(df, col, results)
    elif mc_key == "bulk_tx":
        _check_bulk_tx_seasonal(df, col, results)

    return results


def _expected_nonzero_hours(state: str, mc_key: str) -> int | None:
    """Return expected nonzero-hour count with optional state-level overrides."""
    state_override = STATE_EXPECTED_NONZERO_OVERRIDES.get(state.lower(), {})
    if mc_key in state_override:
        return state_override[mc_key]
    return EXPECTED_NONZERO[mc_key]


def _check_capacity_monthly(df: pl.DataFrame, col: str, results: dict) -> None:
    """Check month-by-month nonzero hour counts for supply capacity."""
    monthly = (
        df.filter(pl.col(col) > 0)
        .with_columns(pl.col("timestamp").dt.month().alias("month"))
        .group_by("month")
        .len()
        .sort("month")
    )
    results["monthly_nonzero"] = monthly
    for row in monthly.iter_rows(named=True):
        if row["len"] != 8:
            results["issues"].append(
                f"WARN: month {row['month']} has {row['len']} nonzero hours (expected 8)"
            )


def _check_bulk_tx_seasonal(df: pl.DataFrame, col: str, results: dict) -> None:
    """Check seasonal split for bulk TX."""
    with_season = df.filter(pl.col(col) > 0).with_columns(
        pl.when(pl.col("timestamp").dt.month().is_in(list(SUMMER_MONTHS)))
        .then(pl.lit("summer"))
        .otherwise(pl.lit("winter"))
        .alias("season")
    )
    seasonal = (
        with_season.group_by("season")
        .agg(
            pl.len().alias("n_hours"),
            pl.col(col).sum().alias("cost_sum"),
        )
        .sort("season")
    )
    results["seasonal_split"] = seasonal
    total = float(seasonal["cost_sum"].sum())
    if total > 0:
        for row in seasonal.iter_rows(named=True):
            phi = row["cost_sum"] / total
            results[f"phi_{row['season']}"] = phi


# ── Printing ──────────────────────────────────────────────────────────────────


def print_check_results(r: dict) -> None:
    """Print formatted check results for one MC/utility."""
    label = r["label"]
    utility = r["utility"]
    units = r["units"]
    print(f"\n{'─' * 70}")
    print(f"  {label}  |  utility={utility}  |  {units}")
    print(f"{'─' * 70}")
    print(f"  Rows:       {r['n_rows']}")
    print(f"  Nulls:      ts={r['n_null_timestamp']}, val={r['n_null_value']}")
    print(f"  Nonzero:    {r['n_nonzero']} hours")
    print(
        f"  Stats:      min={r['min']:.4f}  p05={r['p05']:.4f}  "
        f"mean={r['mean']:.4f}  median={r['median']:.4f}  "
        f"p95={r['p95']:.4f}  max={r['max']:.4f}"
    )
    print(f"  Total sum:  {r['total_sum']:.4f} {units}")

    if "monthly_nonzero" in r:
        print("  Monthly nonzero hours:")
        for row in r["monthly_nonzero"].iter_rows(named=True):
            ok = "ok" if row["len"] == 8 else "!!"
            print(f"    month {row['month']:2d}: {row['len']:3d} hours  {ok}")

    if "seasonal_split" in r:
        print("  Seasonal split:")
        for row in r["seasonal_split"].iter_rows(named=True):
            phi_key = f"phi_{row['season']}"
            phi_str = f"  phi={r[phi_key]:.4f}" if phi_key in r else ""
            print(
                f"    {row['season']:>8s}: {row['n_hours']:3d} hours, "
                f"cost_sum={row['cost_sum']:.6f}{phi_str}"
            )

    print("  Top 10 hours:")
    for row in r["top10"].iter_rows(named=True):
        ts = row["timestamp"]
        val_col = [c for c in row if c != "timestamp"][0]
        print(f"    {ts}  {row[val_col]:.6f}")

    if r["issues"]:
        for issue in r["issues"]:
            print(f"  ** {issue}")
    else:
        print("  PASS: all checks passed")


# ── Heatmap data prep ─────────────────────────────────────────────────────────


def prepare_heatmap_df(df: pl.DataFrame) -> pl.DataFrame:
    """Add hour-of-day and day-of-year columns for heatmap plotting.

    Zero values are replaced with null so the heatmap renders them as gray
    (via ``na_value``) rather than mapping them onto the color scale.
    """
    return df.with_columns(
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
        pl.when(pl.col(NORMALIZED_COL) == 0.0)
        .then(None)
        .otherwise(pl.col(NORMALIZED_COL))
        .alias("value"),
    )


# ── Plotting ──────────────────────────────────────────────────────────────────


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


def _make_heatmap(
    heatmap_df: pl.DataFrame,
    title: str,
    year: int,
) -> ggplot:
    """Create a single plotnine heatmap (hour-of-day x day-of-year)."""
    breaks, labels = _month_breaks(year)

    non_null = heatmap_df.filter(pl.col("value").is_not_null())["value"]
    vmin = cast(float, non_null.min()) if non_null.len() > 0 else 0.0
    vmax = cast(float, non_null.max()) if non_null.len() > 0 else 1.0
    n_intervals = 4
    step = (vmax - vmin) / n_intervals if vmax != vmin else 1.0
    fill_breaks = [vmin + i * step for i in range(n_intervals + 1)]
    fill_labels = [f"{v:.3f}" for v in fill_breaks]

    return (
        ggplot(heatmap_df, aes(x="day_of_year", y="hour", fill="value"))
        + geom_tile()
        + scale_fill_cmap(
            "plasma",
            na_value="#cccccc",
            limits=(vmin, vmax),
            breaks=fill_breaks,
            labels=fill_labels,
        )
        + scale_x_continuous(breaks=breaks, labels=labels)
        + scale_y_reverse()
        + labs(x="", y="Hour of Day", title=title, fill="$/kWh")
        + theme_minimal()
        + theme(
            figure_size=(10, 4),
            plot_title=element_text(size=8),
            axis_title=element_text(size=7),
            axis_text=element_text(size=6),
            legend_text=element_text(size=6),
            legend_title=element_text(size=7),
        )
    )


def make_four_quadrant_plot(
    mc_data: dict[str, pl.DataFrame],
    check_results: dict[str, dict],
    utility: str,
    year: int,
) -> Compose:
    """Compose 4 plotnine heatmaps into a 2x2 grid using | and / operators."""
    plots: list[ggplot] = []
    for mc_key in QUADRANT_ORDER:
        info = MC_TYPE_DEFS[mc_key]
        r = check_results[mc_key]
        subtitle = (
            f"{info['label']} ({NORMALIZED_UNITS})  "
            f"nonzero={r['n_nonzero']}h  sum={r['total_sum']:.4f}"
        )
        heatmap_df = prepare_heatmap_df(mc_data[mc_key])
        plots.append(_make_heatmap(heatmap_df, subtitle, year=r["year"]))

    top_row = plots[0] | plots[1]
    bottom_row = plots[2] | plots[3]
    return top_row / bottom_row


def make_energy_histogram(energy_frames: list[pl.DataFrame]) -> ggplot:
    """Faceted histogram of supply energy MC across all utilities."""
    all_energy = pl.concat(energy_frames)
    return (
        ggplot(all_energy, aes(x=NORMALIZED_COL))
        + geom_histogram(bins=80, fill="#440154", alpha=0.85)
        + facet_wrap("utility", scales="free_y")
        + labs(
            x=f"Energy MC ({NORMALIZED_UNITS})",
            y="Count",
            title="Supply Energy MC Distribution by Utility",
        )
        + theme_minimal()
        + theme(figure_size=(16, 10))
    )


# ── Main ──────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate state marginal cost datasets and generate diagnostic plots.",
    )
    parser.add_argument(
        "--state",
        type=str,
        choices=sorted(STATE_CONFIGS.keys()),
        default="ny",
        help="State abbreviation (default: ny).",
    )
    parser.add_argument(
        "--utilities",
        type=str,
        default=None,
        help="Comma-separated utility list (default: all utilities for --state).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Year for supply and bulk TX MC (default: 2025).",
    )
    parser.add_argument(
        "--load-year",
        type=int,
        default=None,
        help="Year for dist MC (default: same as --year).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="dev_plots",
        help="Directory to save PNGs (default: dev_plots/).",
    )
    parser.add_argument(
        "--mc-table-path",
        type=str,
        default=None,
        help=(
            "Path to dist MC CSV for expected totals "
            "(default: state-specific marginal_costs CSV)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    state = args.state.lower()
    state_cfg = STATE_CONFIGS[state]

    utilities_arg = args.utilities or ",".join(state_cfg.utilities)
    utilities = [u.strip() for u in utilities_arg.split(",") if u.strip()]
    unknown_utils = sorted(set(utilities) - set(state_cfg.utilities))
    if unknown_utils:
        raise ValueError(
            f"Utilities {unknown_utils} are not in configured {state.upper()} utilities "
            f"{list(state_cfg.utilities)}"
        )
    year = args.year
    load_year = args.load_year or year
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    storage_options = get_aws_storage_options()

    # Resolve MC table for dist MC expected totals
    mc_table_path = args.mc_table_path
    if mc_table_path is None:
        from utils import get_project_root

        mc_table_path = str(get_project_root() / state_cfg.default_mc_table_relpath)
    expected_dist = pl.read_csv(mc_table_path)

    energy_frames: list[pl.DataFrame] = []
    # summary_rows: list of (utility, mc_key, status, detail) for the final table
    summary_rows: list[tuple[str, str, str, str]] = []

    for utility in utilities:
        print("\n" + "=" * 70)
        print(f"  UTILITY: {utility}")
        print("=" * 70)

        mc_data: dict[str, pl.DataFrame] = {}
        all_checks: dict[str, dict] = {}

        year_for_mc = {
            "supply_energy": year,
            "supply_capacity": year,
            "bulk_tx": year,
            "dist_sub_tx": load_year,
        }

        for mc_key in QUADRANT_ORDER:
            mc_year = year_for_mc[mc_key]
            try:
                df = load_mc(mc_key, state, utility, mc_year, storage_options)
            except Exception as e:
                print(f"\n  ERROR loading {mc_key} for {utility}: {e}")
                summary_rows.append((utility, mc_key, "ERROR", str(e)[:60]))
                continue

            mc_data[mc_key] = df
            r = check_mc(df, mc_key, utility, mc_year, state)

            # Cross-reference dist MC total against expected CSV
            if mc_key == "dist_sub_tx":
                row = expected_dist.filter(pl.col("utility") == utility)
                if not row.is_empty():
                    expected_total = float(row["sub_tx_and_dist_mc_kw_yr"][0])
                    actual_total = r["total_sum"]
                    err_pct = (
                        abs(actual_total - expected_total) / expected_total * 100
                        if expected_total > 0
                        else 0
                    )
                    r["expected_dist_total"] = expected_total
                    r["dist_err_pct"] = err_pct
                    if err_pct > 0.1:
                        r["issues"].append(
                            f"WARN: dist total {actual_total:.4f} vs "
                            f"expected {expected_total:.4f} "
                            f"(err={err_pct:.4f}%)"
                        )

            all_checks[mc_key] = r
            print_check_results(r)

            # Classify status for summary
            fails = [i for i in r["issues"] if i.startswith("FAIL")]
            warns = [i for i in r["issues"] if i.startswith("WARN")]
            if fails:
                summary_rows.append((utility, mc_key, "FAIL", "; ".join(fails)))
            elif warns:
                summary_rows.append((utility, mc_key, "WARN", "; ".join(warns)))
            else:
                summary_rows.append((utility, mc_key, "PASS", ""))

        # Collect energy frames for the faceted histogram
        if "supply_energy" in mc_data:
            energy_frames.append(
                mc_data["supply_energy"]
                .select(NORMALIZED_COL)
                .with_columns(pl.lit(utility).alias("utility"))
            )

        # Timestamp consistency across MC types
        loaded_keys = list(mc_data.keys())
        if len(loaded_keys) > 1:
            ref_ts = set(mc_data[loaded_keys[0]]["timestamp"].to_list())
            for k in loaded_keys[1:]:
                other_ts = set(mc_data[k]["timestamp"].to_list())
                if ref_ts != other_ts:
                    diff = ref_ts.symmetric_difference(other_ts)
                    print(
                        f"\n  WARN: timestamp mismatch between "
                        f"{loaded_keys[0]} and {k}: {len(diff)} differences"
                    )

        # Generate 4-quadrant heatmap
        if len(mc_data) == 4:
            grid = make_four_quadrant_plot(mc_data, all_checks, utility, year)
            path_png = output_dir / f"mc_heatmap_{utility}.png"
            grid.save(str(path_png), dpi=150, verbose=False)
            print(f"\n  Saved heatmap: {path_png}")
        else:
            print(
                f"\n  Skipping heatmap for {utility}: "
                f"only {len(mc_data)}/4 MC types loaded"
            )

    # Faceted energy histogram
    if energy_frames:
        hist = make_energy_histogram(energy_frames)
        path_hist = output_dir / "mc_energy_histogram.png"
        hist.save(str(path_hist), dpi=150, verbose=False)
        print(f"\nSaved energy histogram: {path_hist}")

    # ── Summary table ─────────────────────────────────────────────────────
    print("\n" + "=" * 90)
    print("  SUMMARY")
    print("=" * 90)
    print(f"  {'utility':<10} {'mc_type':<18} {'status':<6}  detail")
    print(f"  {'─' * 10} {'─' * 18} {'─' * 6}  {'─' * 50}")
    for utility_name, mc_key, status, detail in summary_rows:
        print(f"  {utility_name:<10} {mc_key:<18} {status:<6}  {detail}")
    print("=" * 90)

    n_fail = sum(1 for _, _, s, _ in summary_rows if s == "FAIL")
    n_warn = sum(1 for _, _, s, _ in summary_rows if s == "WARN")
    n_error = sum(1 for _, _, s, _ in summary_rows if s == "ERROR")
    n_pass = sum(1 for _, _, s, _ in summary_rows if s == "PASS")
    print(f"  PASS={n_pass}  WARN={n_warn}  FAIL={n_fail}  ERROR={n_error}")

    if n_fail > 0 or n_error > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

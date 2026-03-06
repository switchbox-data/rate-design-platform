"""Plotnine plot functions for CAIRO run validation.

All functions accept ``pl.DataFrame`` inputs (as produced by :mod:`tables`) and
return ``plotnine.ggplot`` objects; callers save with ``p.save(path, dpi=150)``.

Color conventions (colorblind-friendly Wong palette):
  HP customers    → ``#E69F00`` (orange)
  Non-HP customers → ``#0072B2`` (blue)
"""

from __future__ import annotations

import polars as pl
from plotnine import (
    aes,
    element_text,
    geom_col,
    geom_hline,
    geom_line,
    geom_text,
    geom_tile,
    ggplot,
    labs,
    position_dodge,
    scale_color_manual,
    scale_fill_gradient2,
    scale_fill_manual,
    theme,
    theme_minimal,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_HP_COL = "postprocess_group.has_hp"
_HEATING_TYPE_COL = "postprocess_group.heating_type"

# Wong colorblind-friendly palette for HP / Non-HP subclasses.
_HP_COLORS: dict[str, str] = {"HP": "#E69F00", "Non-HP": "#0072B2"}

# Human-readable labels for BAT weighted-average columns (from summarize_bat_by_subclass).
_BAT_LABELS: dict[str, str] = {
    "BAT_percustomer_wavg": "Per Customer",
    "BAT_vol_wavg": "Volumetric",
    "BAT_peak_wavg": "Peak",
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _label_subclass(df: pl.DataFrame) -> pl.DataFrame:
    """Add a ``subclass`` column (``"HP"`` / ``"Non-HP"``) from the boolean HP flag."""
    return df.with_columns(
        pl.when(pl.col(_HP_COL))
        .then(pl.lit("HP"))
        .otherwise(pl.lit("Non-HP"))
        .alias("subclass")
    )


# ---------------------------------------------------------------------------
# Bills
# ---------------------------------------------------------------------------


def plot_avg_bills_by_subclass(
    summary_df: pl.DataFrame,
    title: str = "Average Annual Bills by Subclass",
) -> ggplot:
    """Grouped bar chart of weighted mean annual bills by bill type and HP/non-HP subclass.

    Args:
        summary_df: Output of :func:`~tables.summarize_bills_by_subclass`
            (columns: ``bill_type``, ``postprocess_group.has_hp``,
            ``bill_mean_weighted``, ``customers_weighted``).
        title: Plot title.

    Returns:
        ggplot with x=bill_type, y=weighted-mean-bill, fill=subclass.
    """
    df = _label_subclass(summary_df).to_pandas()
    return (
        ggplot(df, aes("bill_type", "bill_mean_weighted", fill="subclass"))
        + geom_col(position=position_dodge(width=0.8), width=0.7)
        + scale_fill_manual(values=_HP_COLORS)
        + labs(
            x="Bill Type",
            y="Weighted Mean Annual Bill ($)",
            fill="Subclass",
            title=title,
        )
        + theme_minimal()
    )


def plot_bill_deltas(
    delta_df: pl.DataFrame,
    title: str = "Bill Change by Subclass (B − A)",
) -> ggplot:
    """Bar chart of weighted mean bill deltas (B minus A) by HP/non-HP subclass.

    Args:
        delta_df: Output of :func:`~tables.compute_bill_deltas`
            (columns: ``postprocess_group.has_hp``, ``bill_delta``, …).
        title: Plot title.

    Returns:
        ggplot with x=subclass, y=bill_delta, and a zero reference line.
    """
    df = _label_subclass(delta_df).to_pandas()
    return (
        ggplot(df, aes("subclass", "bill_delta", fill="subclass"))
        + geom_col(width=0.6)
        + geom_hline(yintercept=0, linetype="dashed", color="#666666")
        + scale_fill_manual(values=_HP_COLORS)
        + labs(x=None, y="Bill Change ($/yr)", fill="Subclass", title=title)
        + theme_minimal()
    )


# ---------------------------------------------------------------------------
# Cross-subsidy (BAT)
# ---------------------------------------------------------------------------


def plot_bat_by_subclass(
    bat_summary: pl.DataFrame,
    title: str = "Per-Customer BAT by Subclass",
) -> ggplot:
    """Bar chart of weighted-average per-customer BAT by HP/non-HP subclass.

    Args:
        bat_summary: Output of :func:`~tables.summarize_bat_by_subclass`
            (columns: ``postprocess_group.has_hp``, ``BAT_percustomer_wavg``, …).
        title: Plot title.

    Returns:
        ggplot with x=subclass, y=BAT_percustomer_wavg, and a zero reference line.
    """
    df = _label_subclass(bat_summary).to_pandas()
    return (
        ggplot(df, aes("subclass", "BAT_percustomer_wavg", fill="subclass"))
        + geom_col(width=0.6)
        + geom_hline(yintercept=0, linetype="dashed", color="#666666")
        + scale_fill_manual(values=_HP_COLORS)
        + labs(x=None, y="Weighted Avg BAT ($/cust-yr)", fill="Subclass", title=title)
        + theme_minimal()
    )


def plot_bat_heatmap(
    bat_summary: pl.DataFrame,
    title: str = "BAT by Subclass and Benchmark",
) -> ggplot:
    """Tile heatmap of weighted-average BAT across all benchmarks and subclasses.

    Diverging fill: positive BAT (overpaying) → orange, zero → white, negative
    (underpaying) → blue — consistent with the HP/non-HP palette.

    Args:
        bat_summary: Output of :func:`~tables.summarize_bat_by_subclass`
            (columns: ``postprocess_group.has_hp``, zero or more of
            ``BAT_percustomer_wavg``, ``BAT_vol_wavg``, ``BAT_peak_wavg``).
        title: Plot title.

    Returns:
        ggplot tile heatmap with subclass × benchmark cells and ``$±N`` labels.
    """
    bat_wavg_cols = [c for c in _BAT_LABELS if c in bat_summary.columns]
    df = (
        _label_subclass(bat_summary)
        .unpivot(
            on=bat_wavg_cols,
            index="subclass",
            variable_name="bat_col",
            value_name="wavg_usd",
        )
        .with_columns(
            pl.col("bat_col").replace(_BAT_LABELS).alias("benchmark"),
            pl.col("wavg_usd")
            .map_elements(lambda v: f"${v:+,.0f}", return_dtype=pl.String)
            .alias("label"),
        )
        .to_pandas()
    )
    return (
        ggplot(df, aes("benchmark", "subclass", fill="wavg_usd"))
        + geom_tile(color="white", size=0.5)
        + geom_text(aes(label="label"), size=9)
        + scale_fill_gradient2(low="#0072B2", mid="white", high="#E69F00", midpoint=0)
        + labs(x="Benchmark", y=None, fill="$/cust-yr", title=title)
        + theme_minimal()
    )


# ---------------------------------------------------------------------------
# Revenue
# ---------------------------------------------------------------------------


def plot_revenue_vs_rr(
    revenue_df: pl.DataFrame,
    rr_values: dict[str, float],
    title: str = "Revenue vs Revenue Requirement by Subclass",
) -> ggplot:
    """Grouped bar chart comparing billed revenue against revenue requirement targets.

    Args:
        revenue_df: Output of :func:`~tables.summarize_revenue`
            (columns: ``postprocess_group.has_hp``, ``total_revenue_weighted``, …).
        rr_values: Target RR per subclass, keyed by subclass label
            (``"HP"`` / ``"Non-HP"``).  For non-subclass runs, pass a single
            ``{"all": total_rr}`` and pre-aggregate ``revenue_df`` accordingly.
        title: Plot title.

    Returns:
        ggplot grouped bar with ``source`` distinguishing Actual from Target RR.
    """
    actual = (
        _label_subclass(revenue_df)
        .select(pl.col("subclass"), pl.col("total_revenue_weighted").alias("revenue"))
        .with_columns(pl.lit("Actual").alias("source"))
    )
    target = pl.DataFrame(
        [
            {"subclass": k, "revenue": v, "source": "Target RR"}
            for k, v in rr_values.items()
        ]
    )
    df = pl.concat([actual, target]).to_pandas()
    return (
        ggplot(df, aes("subclass", "revenue", fill="source"))
        + geom_col(position=position_dodge(width=0.8), width=0.7)
        + scale_fill_manual(values={"Actual": "#56B4E9", "Target RR": "#CC79A7"})
        + labs(x=None, y="Weighted Revenue ($)", fill=None, title=title)
        + theme_minimal()
    )


def plot_subclass_rr_stacked(
    subclass_rr: dict[str, float],
    total_rr: float,
    title: str = "Subclass Revenue Requirements vs Total",
) -> ggplot:
    """Stacked bar showing per-subclass revenue requirements summing to the total RR.

    A dashed reference line marks ``total_rr``; a gap or overshoot flags an
    inconsistency between the subclass and total YAML files.

    Args:
        subclass_rr: Per-subclass RR values keyed by subclass label
            (e.g. ``{"HP": 1_200_000, "Non-HP": 3_800_000}``).
        total_rr: Total topped-up revenue requirement (reference line).
        title: Plot title.

    Returns:
        ggplot stacked bar with a dashed total RR reference line.
    """
    df = pl.DataFrame(
        [
            {"subclass": k, "rr": v, "group": "Subclasses"}
            for k, v in subclass_rr.items()
        ]
    ).to_pandas()
    colors = {k: _HP_COLORS.get(k, "#999999") for k in subclass_rr}
    return (
        ggplot(df, aes("group", "rr", fill="subclass"))
        + geom_col(position="stack", width=0.4)
        + geom_hline(yintercept=total_rr, linetype="dashed", color="black", size=0.8)
        + scale_fill_manual(values=colors)
        + labs(x=None, y="Revenue Requirement ($)", fill="Subclass", title=title)
        + theme_minimal()
    )


# ---------------------------------------------------------------------------
# Tariffs
# ---------------------------------------------------------------------------


def plot_tariff_comparison(
    input_rates: pl.DataFrame,
    output_rates: pl.DataFrame,
    title: str = "Tariff Rate Comparison (Input vs Output)",
) -> ggplot:
    """Side-by-side bar chart comparing input and output tariff volumetric rates.

    Args:
        input_rates: Output of :func:`~tables.summarize_tariff_rates` for the
            input tariff (e.g. a ``*_calibrated.json`` from the preceding precalc run).
        output_rates: Same structure for the run's ``tariff_final_config.json``.
        title: Plot title.

    Returns:
        ggplot grouped bar with x=rate component (key·period·tier), y=$/kWh, fill=source.
    """

    def _labeled(df: pl.DataFrame, source: str) -> pl.DataFrame:
        return df.with_columns(
            (
                pl.col("tariff_key")
                + " P"
                + pl.col("period").cast(pl.String)
                + "T"
                + pl.col("tier").cast(pl.String)
            ).alias("component"),
            pl.lit(source).alias("source"),
        ).select(["component", "rate_per_kwh", "source"])

    df = pl.concat(
        [_labeled(input_rates, "Input"), _labeled(output_rates, "Output")]
    ).to_pandas()
    return (
        ggplot(df, aes("component", "rate_per_kwh", fill="source"))
        + geom_col(position=position_dodge(width=0.8), width=0.7)
        + scale_fill_manual(values={"Input": "#56B4E9", "Output": "#E69F00"})
        + labs(x="Rate Component", y="Rate ($/kWh)", fill=None, title=title)
        + theme_minimal()
        + theme(axis_text_x=element_text(rotation=45, hjust=1))
    )


def plot_tariff_stability(
    rates_a: pl.DataFrame,
    rates_b: pl.DataFrame,
    title: str = "Tariff Rate Difference (B − A)",
) -> ggplot:
    """Bar chart of per-component rate differences (B minus A).

    Expected to show near-zero bars for default runs (3-4, 7-8) that inherit
    the preceding precalc tariff unchanged.

    Args:
        rates_a: Baseline rate DataFrame from :func:`~tables.summarize_tariff_rates`.
        rates_b: Comparison rate DataFrame (same structure).
        title: Plot title.

    Returns:
        ggplot bar chart with zero reference line; non-zero bars signal unexpected drift.
    """
    df = (
        rates_a.rename({"rate_per_kwh": "rate_a"})
        .join(
            rates_b.rename({"rate_per_kwh": "rate_b"}),
            on=["tariff_key", "period", "tier"],
        )
        .with_columns(
            (pl.col("rate_b") - pl.col("rate_a")).alias("rate_diff"),
            (
                pl.col("tariff_key")
                + " P"
                + pl.col("period").cast(pl.String)
                + "T"
                + pl.col("tier").cast(pl.String)
            ).alias("component"),
        )
        .to_pandas()
    )
    return (
        ggplot(df, aes("component", "rate_diff"))
        + geom_col(fill="#999999", width=0.6)
        + geom_hline(yintercept=0, linetype="dashed", color="black")
        + labs(x="Rate Component", y="Rate Difference ($/kWh)", title=title)
        + theme_minimal()
        + theme(axis_text_x=element_text(rotation=45, hjust=1))
    )


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------


def plot_nonhp_composition(
    composition_df: pl.DataFrame,
    title: str = "Non-HP Customer Composition by Heating Type",
) -> ggplot:
    """Bar chart of weighted non-HP customer counts by heating type.

    Args:
        composition_df: Output of :func:`~tables.summarize_nonhp_composition`
            (columns: ``postprocess_group.heating_type``, ``customers_weighted``, …).
            Already sorted by ``customers_weighted`` descending.
        title: Plot title.

    Returns:
        ggplot bar chart ordered by weighted count (largest on left).
    """
    df = composition_df.to_pandas()
    return (
        ggplot(df, aes(_HEATING_TYPE_COL, "customers_weighted"))
        + geom_col(fill="#0072B2", width=0.7)
        + labs(x="Heating Type", y="Weighted Customers", title=title)
        + theme_minimal()
        + theme(axis_text_x=element_text(rotation=45, hjust=1))
    )


# ---------------------------------------------------------------------------
# Optional (loads / counts) — toggleable via --skip-loads
# ---------------------------------------------------------------------------


def plot_hourly_loads_by_subclass(
    loads_df: pl.DataFrame,
    title: str = "Hourly Loads by Subclass",
) -> ggplot:
    """Line chart of hourly mean loads for HP vs non-HP customers.

    Args:
        loads_df: DataFrame with columns ``hour`` (int 0–8759), ``subclass``
            (``"HP"`` / ``"Non-HP"``), and ``load_kwh`` (weighted mean, float).
        title: Plot title.

    Returns:
        ggplot line chart with color=subclass (thin, semi-transparent lines).
    """
    df = loads_df.to_pandas()
    return (
        ggplot(df, aes("hour", "load_kwh", color="subclass"))
        + geom_line(size=0.3, alpha=0.7)
        + scale_color_manual(values=_HP_COLORS)
        + labs(
            x="Hour of Year",
            y="Weighted Mean Load (kWh)",
            color="Subclass",
            title=title,
        )
        + theme_minimal()
    )


def plot_weighted_customer_counts(
    counts_df: pl.DataFrame,
    title: str = "Weighted Customer Counts by Subclass",
) -> ggplot:
    """Bar chart of weighted customer counts per subclass.

    Args:
        counts_df: DataFrame with columns ``subclass`` (``"HP"`` / ``"Non-HP"``)
            and ``customers_weighted`` (float).
        title: Plot title.

    Returns:
        ggplot bar chart with fill=subclass.
    """
    df = counts_df.to_pandas()
    return (
        ggplot(df, aes("subclass", "customers_weighted", fill="subclass"))
        + geom_col(width=0.6)
        + scale_fill_manual(values=_HP_COLORS)
        + labs(x=None, y="Weighted Customers", fill=None, title=title)
        + theme_minimal()
    )

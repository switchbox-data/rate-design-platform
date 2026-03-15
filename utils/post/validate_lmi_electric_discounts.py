"""Validate and explore LMI electric bill discounts applied to master bills.

Reads integrated master-bills outputs for p100 and p40 participation scenarios,
plus an optional production-source master-bills dataset for pass-through column
comparison. Produces histograms, summary stats, and diagnostics for electric
bills: discount amounts, before/after distributions, tier distribution by
electric utility, and p40 participation weighting. Section 1.5 compares
actualized discounts to the expected EAP/EEAP credits from the NY credit table
(`utils/post/data/ny_eap_credits.yaml`) by utility and tier, with annual
expected-vs-actual plots. Section 1.6 performs the same check month by month.
All plots are saved to ``dev_plots/`` with Agg backend (no display).

See `context/domain/charges/lmi_discounts_in_ny.md` for EAP/EEAP tier and credit details.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.post.lmi_common import get_ny_eap_credits_df

matplotlib.use("Agg")

# Tolerance for expected vs actual discount comparison (rounding / clip at zero).
EXPECTED_VS_ACTUAL_TOL = 0.02
# For capped histogram: treat |diff| < this as zero (perfect match).
CAP_DIFF_ZERO_TOL = 1e-6

PLOTS_DIR = Path(__file__).resolve().parents[2] / "dev_plots"

LMI_S3_PREFIX = "s3://data.sb/switchbox/lmi"
MASTER_S3_PREFIX = "s3://data.sb/switchbox/cairo/outputs/hp_rates"


def _build_lmi_path(state: str, batch: str, run_d: int, run_s: int, pct: int) -> str:
    return (
        f"{LMI_S3_PREFIX}/{state}/{batch}/run_{run_d}+{run_s}/"
        f"p{pct}/comb_bills_year_target/"
    )


def _build_master_path(state: str, batch: str, run_d: int, run_s: int) -> str:
    return (
        f"{MASTER_S3_PREFIX}/{state}/all_utilities/"
        f"{batch}/run_{run_d}+{run_s}/comb_bills_year_target/"
    )


ANNUAL_MONTH = "Annual"


def _load_staging(path: str, opts: dict[str, str]) -> pl.DataFrame:
    return pl.read_parquet(path, hive_partitioning=True, storage_options=opts)


def _section_header(title: str) -> None:
    sep = "=" * 72
    print(f"\n{sep}\n  {title}\n{sep}\n")


# ---------------------------------------------------------------------------
# Section 1: Electric discount EDA (p100)
# ---------------------------------------------------------------------------


def _elec_summary_stats(df: pl.DataFrame) -> None:
    """Print per-electric-utility summary stats for p100 annual electric bills."""
    _section_header(
        "Section 1.1: Electric Discount Summary Stats (p100, Annual, elec_total_bill > 0)"
    )

    stats = (
        df.group_by("sb.electric_utility")
        .agg(
            pl.len().alias("n_buildings"),
            pl.col("is_lmi").sum().alias("n_lmi"),
            pl.col("applied_discount_100").sum().alias("n_discounted"),
            pl.col("elec_total_bill").mean().alias("elec_bill_mean"),
            pl.col("elec_total_bill").median().alias("elec_bill_median"),
            pl.col("elec_total_bill").min().alias("elec_bill_min"),
            pl.col("elec_total_bill").max().alias("elec_bill_max"),
            pl.col("elec_total_bill_lmi_100").mean().alias("elec_lmi_mean"),
            pl.col("elec_total_bill_lmi_100").median().alias("elec_lmi_median"),
            pl.col("elec_total_bill_lmi_100").min().alias("elec_lmi_min"),
            pl.col("elec_total_bill_lmi_100").max().alias("elec_lmi_max"),
            (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100"))
            .filter(pl.col("applied_discount_100"))
            .mean()
            .alias("discount_mean"),
            (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100"))
            .filter(pl.col("applied_discount_100"))
            .median()
            .alias("discount_median"),
        )
        .sort("sb.electric_utility")
    )
    print(stats)


def _elec_discount_histogram(df: pl.DataFrame) -> None:
    """Histogram of electric discount amount for LMI participants, faceted by electric utility."""
    _section_header("Section 1.2: Electric Discount Amount Histogram (p100)")

    participants = df.filter(pl.col("applied_discount_100"))
    utilities = sorted(participants["sb.electric_utility"].unique().to_list())
    n_utils = len(utilities)
    if n_utils == 0:
        print("No participants with applied_discount_100=True — skipping.")
        return

    ncols = min(n_utils, 3)
    nrows = (n_utils + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows), squeeze=False)

    for idx, util in enumerate(utilities):
        ax = axes[idx // ncols][idx % ncols]
        subset = participants.filter(pl.col("sb.electric_utility") == util)
        discount = (
            subset["elec_total_bill"] - subset["elec_total_bill_lmi_100"]
        ).to_numpy()
        ax.hist(discount, bins=40, color="steelblue", edgecolor="white")
        ax.set_title(util)
        ax.set_xlabel("Electric discount ($)")
        ax.set_ylabel("Count")

    for idx in range(n_utils, nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle(
        "Electric Bill Discount Amount — p100 Participants", fontsize=14, y=1.01
    )
    fig.tight_layout()
    out = PLOTS_DIR / "elec_discount_hist_p100.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _elec_before_after_histogram(df: pl.DataFrame) -> None:
    """Overlapping histograms of electric bill before vs after discount, by electric utility."""
    _section_header("Section 1.3: Electric Bill Before vs After Histogram (p100)")

    participants = df.filter(pl.col("applied_discount_100"))
    utilities = sorted(participants["sb.electric_utility"].unique().to_list())
    n_utils = len(utilities)
    if n_utils == 0:
        print("No participants — skipping.")
        return

    ncols = min(n_utils, 3)
    nrows = (n_utils + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows), squeeze=False)

    for idx, util in enumerate(utilities):
        ax = axes[idx // ncols][idx % ncols]
        subset = participants.filter(pl.col("sb.electric_utility") == util)
        before = subset["elec_total_bill"].to_numpy()
        after = subset["elec_total_bill_lmi_100"].to_numpy()
        lo = min(before.min(), after.min())
        hi = max(before.max(), after.max())
        bins = np.linspace(lo, hi, 41)
        ax.hist(
            before,
            bins=bins,
            alpha=0.5,
            color="coral",
            label="Original",
            edgecolor="white",
        )
        ax.hist(
            after,
            bins=bins,
            alpha=0.5,
            color="steelblue",
            label="After LMI",
            edgecolor="white",
        )
        ax.set_title(util)
        ax.set_xlabel("Annual electric bill ($)")
        ax.set_ylabel("Count")
        ax.legend(fontsize=8)

    for idx in range(n_utils, nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle(
        "Electric Bill Before vs After Discount — p100 Participants",
        fontsize=14,
        y=1.01,
    )
    fig.tight_layout()
    out = PLOTS_DIR / "elec_bill_before_after_p100.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _tier_distribution_bar(df: pl.DataFrame) -> None:
    """Bar chart of tier distribution by electric utility."""
    _section_header("Section 1.4: Tier Distribution by Electric Utility")

    tier_counts = (
        df.filter(pl.col("lmi_tier") > 0)
        .group_by("sb.electric_utility", "lmi_tier")
        .agg(pl.len().alias("count"))
        .sort("sb.electric_utility", "lmi_tier")
    )
    utilities = sorted(tier_counts["sb.electric_utility"].unique().to_list())
    tiers = sorted(tier_counts["lmi_tier"].unique().to_list())
    n_utils = len(utilities)

    fig, ax = plt.subplots(figsize=(12, 6))
    width = 0.8 / n_utils
    x = np.arange(len(tiers))

    for i, util in enumerate(utilities):
        util_data = tier_counts.filter(pl.col("sb.electric_utility") == util)
        counts = []
        for t in tiers:
            row = util_data.filter(pl.col("lmi_tier") == t)
            counts.append(int(row["count"][0]) if len(row) > 0 else 0)
        ax.bar(x + i * width, counts, width, label=util)

    ax.set_xticks(x + width * (n_utils - 1) / 2)
    ax.set_xticklabels([str(t) for t in tiers])
    ax.set_xlabel("EAP Tier")
    ax.set_ylabel("Count of buildings")
    ax.set_title("Tier Distribution by Electric Utility (Annual, elec_total_bill > 0)")
    ax.legend(fontsize=8)
    fig.tight_layout()
    out = PLOTS_DIR / "elec_tier_distribution.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


# ---------------------------------------------------------------------------
# Section 1.5: Expected vs actual discount (EAP/EEAP credit table)
# ---------------------------------------------------------------------------


def _merge_expected_electric_credit(
    df: pl.DataFrame, credits_df: pl.DataFrame
) -> pl.DataFrame:
    """Join staging with NY EAP credits; add expected_annual_credit (electric, $/year).

    credits_df has utility, tier, elec_heat, elec_nonheat ($/month). We join on
    sb.electric_utility = utility and lmi_tier = tier, then set expected_annual_credit
    = 12 * (elec_heat if heats_with_electricity else elec_nonheat), fill nulls with 0.
    """
    elec_credits = credits_df.select(
        pl.col("utility").alias("sb.electric_utility"),
        pl.col("tier").alias("lmi_tier"),
        pl.col("elec_heat").fill_null(0.0).alias("_elec_heat"),
        pl.col("elec_nonheat").fill_null(0.0).alias("_elec_nonheat"),
    )
    merged = df.join(elec_credits, on=["sb.electric_utility", "lmi_tier"], how="left")
    merged = merged.with_columns(
        pl.when(pl.col("heats_with_electricity").fill_null(False))
        .then(pl.col("_elec_heat") * 12.0)
        .otherwise(pl.col("_elec_nonheat") * 12.0)
        .fill_null(0.0)
        .alias("expected_annual_credit")
    ).drop(["_elec_heat", "_elec_nonheat"])
    return merged


def _merge_expected_electric_credit_monthly(
    df: pl.DataFrame, credits_df: pl.DataFrame
) -> pl.DataFrame:
    """Join staging with NY EAP credits; add expected_monthly_credit (electric, $/month).

    Same join as annual merge but uses monthly credit as-is (no *12). For use with
    monthly rows (month != Annual).
    """
    elec_credits = credits_df.select(
        pl.col("utility").alias("sb.electric_utility"),
        pl.col("tier").alias("lmi_tier"),
        pl.col("elec_heat").fill_null(0.0).alias("_elec_heat"),
        pl.col("elec_nonheat").fill_null(0.0).alias("_elec_nonheat"),
    )
    merged = df.join(elec_credits, on=["sb.electric_utility", "lmi_tier"], how="left")
    merged = merged.with_columns(
        pl.when(pl.col("heats_with_electricity").fill_null(False))
        .then(pl.col("_elec_heat"))
        .otherwise(pl.col("_elec_nonheat"))
        .fill_null(0.0)
        .alias("expected_monthly_credit")
    ).drop(["_elec_heat", "_elec_nonheat"])
    return merged


def _expected_vs_actual_summary(participants: pl.DataFrame) -> None:
    """Print summary: expected (from EAP table) vs actualized discount by utility and tier."""
    _section_header(
        "Section 1.5a: Expected vs Actual Electric Discount (p100 participants)"
    )

    participants = participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount"
        )
    )

    summary = (
        participants.group_by("sb.electric_utility", "lmi_tier")
        .agg(
            pl.len().alias("n"),
            pl.col("expected_annual_credit").mean().alias("expected_mean"),
            pl.col("expected_annual_credit").median().alias("expected_median"),
            pl.col("actual_discount").mean().alias("actual_mean"),
            pl.col("actual_discount").median().alias("actual_median"),
            (pl.col("actual_discount") - pl.col("expected_annual_credit"))
            .mean()
            .alias("diff_mean"),
            (pl.col("actual_discount") - pl.col("expected_annual_credit"))
            .abs()
            .mean()
            .alias("abs_diff_mean"),
        )
        .sort("sb.electric_utility", "lmi_tier")
    )
    print(summary)
    n_mismatch = participants.filter(
        (pl.col("actual_discount") - pl.col("expected_annual_credit")).abs()
        > EXPECTED_VS_ACTUAL_TOL
    ).height
    n_total = participants.height
    print(
        f"\nRows with |actual - expected| > ${EXPECTED_VS_ACTUAL_TOL}: "
        f"{n_mismatch} / {n_total}"
    )
    if n_total > 0:
        print(
            "  (Small differences are normal when bill < expected credit: "
            "discount is capped at bill amount.)"
        )


def _plot_expected_vs_actual_scatter(participants: pl.DataFrame) -> None:
    """Scatter: expected annual credit (x) vs actual discount (y); 1:1 line."""
    _section_header("Section 1.5b: Expected vs Actual Scatter (p100)")

    participants = participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount"
        )
    )
    exp = participants["expected_annual_credit"].to_numpy()
    act = participants["actual_discount"].to_numpy()

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(exp, act, alpha=0.3, s=8, color="steelblue", label="Participants")
    lim = max(exp.max(), act.max(), 1.0)
    ax.plot([0, lim], [0, lim], "k--", alpha=0.8, label="1:1 (expected = actual)")
    ax.set_xlabel("Expected annual credit ($) — from EAP/EEAP table")
    ax.set_ylabel("Actual discount ($) — elec_total_bill - elec_total_bill_lmi_100")
    ax.set_title(
        "Electric LMI: Expected vs Actualized Discount (p100; points below line = bill cap)"
    )
    ax.legend()
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_aspect("equal")
    fig.tight_layout()
    out = PLOTS_DIR / "elec_expected_vs_actual_scatter.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _plot_expected_vs_actual_by_utility_tier(participants: pl.DataFrame) -> None:
    """Grouped bar: mean expected vs mean actual by electric utility and tier."""
    _section_header("Section 1.5c: Expected vs Actual by Utility and Tier (p100)")

    participants = participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount"
        )
    )
    agg = (
        participants.group_by("sb.electric_utility", "lmi_tier")
        .agg(
            pl.col("expected_annual_credit").mean().alias("expected_mean"),
            pl.col("actual_discount").mean().alias("actual_mean"),
            pl.len().alias("n"),
        )
        .sort("sb.electric_utility", "lmi_tier")
    )
    utilities = agg["sb.electric_utility"].unique().sort().to_list()
    tiers = agg["lmi_tier"].unique().sort().to_list()
    n_utils = len(utilities)
    n_tiers = len(tiers)

    if n_utils == 0 or n_tiers == 0:
        print("No data — skipping.")
        return

    fig, axes = plt.subplots(n_utils, 1, figsize=(10, 4 * n_utils), squeeze=False)
    for idx, util in enumerate(utilities):
        ax = axes[idx][0]
        util_agg = agg.filter(pl.col("sb.electric_utility") == util)
        x = np.arange(n_tiers)
        width = 0.35
        exp_vals = [
            util_agg.filter(pl.col("lmi_tier") == t)["expected_mean"][0]
            if util_agg.filter(pl.col("lmi_tier") == t).height > 0
            else 0.0
            for t in tiers
        ]
        act_vals = [
            util_agg.filter(pl.col("lmi_tier") == t)["actual_mean"][0]
            if util_agg.filter(pl.col("lmi_tier") == t).height > 0
            else 0.0
            for t in tiers
        ]
        ax.bar(
            x - width / 2,
            exp_vals,
            width,
            label="Expected (EAP table)",
            color="gray",
            edgecolor="white",
        )
        ax.bar(
            x + width / 2,
            act_vals,
            width,
            label="Actual (staging)",
            color="steelblue",
            edgecolor="white",
        )
        ax.set_xticks(x)
        ax.set_xticklabels([str(t) for t in tiers])
        ax.set_ylabel("Annual discount ($)")
        ax.set_xlabel("EAP Tier")
        ax.set_title(util)
        ax.legend(fontsize=8)
    fig.suptitle(
        "Electric LMI: Expected vs Actual Mean Discount by Utility and Tier (p100)",
        fontsize=12,
        y=1.01,
    )
    fig.tight_layout()
    out = PLOTS_DIR / "elec_expected_vs_actual_by_utility_tier.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _plot_expected_vs_actual_scatter_with_cap(participants: pl.DataFrame) -> None:
    """Scatter: expected_capped (x) vs actual discount (y); 1:1 line.

    expected_capped = min(expected_annual_credit, elec_total_bill). With correct
    zero-flooring, actual discount should equal this, so points lie on the 1:1 line.
    """
    _section_header("Section 1.5b2: Expected (capped) vs Actual Scatter (p100)")

    participants = participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount"
        )
    ).with_columns(
        pl.min_horizontal("expected_annual_credit", "elec_total_bill").alias(
            "expected_capped"
        )
    )
    exp_cap = participants["expected_capped"].to_numpy()
    act = participants["actual_discount"].to_numpy()

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(exp_cap, act, alpha=0.3, s=8, color="steelblue", label="Participants")
    lim = max(exp_cap.max(), act.max(), 1.0)
    ax.plot(
        [0, lim], [0, lim], "k--", alpha=0.8, label="1:1 (expected_capped = actual)"
    )
    ax.set_xlabel(
        "Expected (capped) ($) — min(expected annual credit, elec_total_bill)"
    )
    ax.set_ylabel("Actual discount ($) — elec_total_bill - elec_total_bill_lmi_100")
    ax.set_title(
        "Electric LMI: Expected-With-Cap vs Actual — on line = correct zero-flooring"
    )
    ax.legend()
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_aspect("equal")
    fig.tight_layout()
    out = PLOTS_DIR / "elec_expected_vs_actual_scatter_with_cap.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _plot_expected_vs_actual_diff_hist(participants: pl.DataFrame) -> None:
    """Histogram of (actual - expected) discount; 0 = perfect match.

    Uses a bin edge at 0 so the full range of differences is visible: negative
    (bill cap), zero (full credit applied), positive (check logic).
    """
    _section_header("Section 1.5d: Distribution of (Actual - Expected) Discount (p100)")

    participants = participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount"
        )
    ).with_columns(
        (pl.col("actual_discount") - pl.col("expected_annual_credit")).alias(
            "discount_diff"
        )
    )
    diff = participants["discount_diff"].to_numpy()
    d_min, d_max = float(diff.min()), float(diff.max())
    n_neg = 35
    n_nonneg = 15
    if d_min < -1e-9:
        bins_neg = np.linspace(d_min, -1e-9, n_neg + 1)
    else:
        bins_neg = np.array([d_min])
    if d_max > 1e-9:
        bins_nonneg = np.linspace(0.0, d_max, n_nonneg + 1)
    else:
        # Need a right edge so the bin [0, right] contains zeros (hist uses left-open right-closed for last bin in some versions; standard is [a,b) so [0, 1) catches 0)
        bins_nonneg = np.array([0.0, max(1.0, abs(d_min) * 0.01 + 1e-9)])
    bins = np.unique(np.concatenate([bins_neg, np.array([0.0]), bins_nonneg]))
    if len(bins) < 2:
        bins = np.array([min(d_min, 0) - 1e-9, 0.0, max(d_max, 0) + 1e-9])

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(diff, bins=bins.tolist(), color="steelblue", edgecolor="white")
    ax.axvline(0, color="black", linestyle="--", linewidth=1, label="0 (perfect match)")
    ax.set_xlabel("Actual discount − Expected annual credit ($)")
    ax.set_ylabel("Count")
    ax.set_title(
        "Electric LMI: (Actual − Expected) Discount — negative = bill cap, positive = check logic"
    )
    ax.legend()
    fig.tight_layout()
    out = PLOTS_DIR / "elec_expected_vs_actual_diff_hist.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _plot_expected_vs_actual_diff_hist_with_cap(participants: pl.DataFrame) -> None:
    """Histogram of actual minus expected-with-cap; 0 = correct zero-flooring.

    Expected-with-cap = min(expected_annual_credit, elec_total_bill). With bill
    floored at zero, actual_discount should equal this. So (actual - expected_capped)
    should be ~0 for all; concentration at 0 means implementation matches the cap rule.
    """
    _section_header(
        "Section 1.5e: Expected vs Actual (with zero cap) — diff should be ~0"
    )

    participants = (
        participants.with_columns(
            (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
                "actual_discount"
            )
        )
        .with_columns(
            pl.min_horizontal("expected_annual_credit", "elec_total_bill").alias(
                "expected_capped"
            )
        )
        .with_columns(
            (pl.col("actual_discount") - pl.col("expected_capped")).alias(
                "diff_vs_capped"
            )
        )
        .with_columns(
            pl.when(pl.col("diff_vs_capped").abs() < CAP_DIFF_ZERO_TOL)
            .then(0.0)
            .otherwise(pl.col("diff_vs_capped"))
            .alias("diff_vs_capped")
        )
    )
    diff = participants["diff_vs_capped"].to_numpy()
    d_min, d_max = float(diff.min()), float(diff.max())
    data_range = d_max - d_min

    # When all diffs are ~0 (correct zero-flooring), use a small symmetric range
    # so we get a visible spike at 0 instead of one wide [0, 1) bin.
    if data_range < 0.01 and d_min >= -1e-6 and d_max <= 1e-6:
        bins = np.linspace(-0.02, 0.02, 41)
    else:
        n_neg = 35
        n_nonneg = 15
        if d_min < -1e-9:
            bins_neg = np.linspace(d_min, -1e-9, n_neg + 1)
        else:
            bins_neg = np.array([d_min])
        if d_max > 1e-9:
            bins_nonneg = np.linspace(0.0, d_max, n_nonneg + 1)
        else:
            bins_nonneg = np.array([0.0, max(1.0, abs(d_min) * 0.01 + 1e-9)])
        bins = np.unique(np.concatenate([bins_neg, np.array([0.0]), bins_nonneg]))
        if len(bins) < 2:
            bins = np.array([min(d_min, 0) - 1e-9, 0.0, max(d_max, 0) + 1e-9])

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(diff, bins=bins.tolist(), color="steelblue", edgecolor="white")
    ax.axvline(0, color="black", linestyle="--", linewidth=1, label="0 (perfect match)")
    ax.set_xlabel(
        "Actual discount − min(expected credit, bill) ($)\n(expected when bill is floored at zero; |diff| < 1e-6 counted as 0)"
    )
    ax.set_ylabel("Count")
    ax.set_title(
        "Electric LMI: Actual vs Expected-With-Cap — centered at 0 = correct zero-flooring"
    )
    ax.legend()
    fig.tight_layout()
    out = PLOTS_DIR / "elec_expected_vs_actual_diff_hist_with_cap.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


# ---------------------------------------------------------------------------
# Section 1.6: Month-by-month expected vs actual (p100)
# ---------------------------------------------------------------------------


def _plot_monthly_expected_vs_actual_scatter(
    monthly_participants: pl.DataFrame,
) -> None:
    """Scatter: expected monthly credit (x) vs actual monthly discount (y); 1:1 line.

    Uses only non-Annual rows; expected = EAP table monthly credit, actual = bill - bill_lmi
    for that month.
    """
    _section_header("Section 1.6a: Month-by-month Expected vs Actual Scatter (p100)")

    monthly_participants = monthly_participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount_monthly"
        )
    )
    exp = monthly_participants["expected_monthly_credit"].to_numpy()
    act = monthly_participants["actual_discount_monthly"].to_numpy()

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(exp, act, alpha=0.2, s=6, color="steelblue", label="Participant-months")
    lim = max(exp.max(), act.max(), 1.0)
    ax.plot([0, lim], [0, lim], "k--", alpha=0.8, label="1:1 (expected = actual)")
    ax.set_xlabel("Expected monthly credit ($) — from EAP/EEAP table")
    ax.set_ylabel(
        "Actual monthly discount ($) — elec_total_bill - elec_total_bill_lmi_100"
    )
    ax.set_title(
        "Electric LMI: Month-by-month Expected vs Actual (p100; below line = bill cap)"
    )
    ax.legend()
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_aspect("equal")
    fig.tight_layout()
    out = PLOTS_DIR / "elec_monthly_expected_vs_actual_scatter.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _plot_monthly_expected_vs_actual_scatter_with_cap(
    monthly_participants: pl.DataFrame,
) -> None:
    """Scatter: expected_capped monthly (x) vs actual monthly discount (y); 1:1 line.

    expected_capped = min(expected_monthly_credit, elec_total_bill) for that month.
    On line = correct zero-flooring at monthly level.
    """
    _section_header(
        "Section 1.6b: Month-by-month Expected (capped) vs Actual Scatter (p100)"
    )

    monthly_participants = monthly_participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount_monthly"
        )
    ).with_columns(
        pl.min_horizontal("expected_monthly_credit", "elec_total_bill").alias(
            "expected_monthly_capped"
        )
    )
    exp_cap = monthly_participants["expected_monthly_capped"].to_numpy()
    act = monthly_participants["actual_discount_monthly"].to_numpy()

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(
        exp_cap, act, alpha=0.2, s=6, color="steelblue", label="Participant-months"
    )
    lim = max(exp_cap.max(), act.max(), 1.0)
    ax.plot(
        [0, lim], [0, lim], "k--", alpha=0.8, label="1:1 (expected_capped = actual)"
    )
    ax.set_xlabel(
        "Expected (capped) monthly ($) — min(expected monthly credit, elec_total_bill)"
    )
    ax.set_ylabel(
        "Actual monthly discount ($) — elec_total_bill - elec_total_bill_lmi_100"
    )
    ax.set_title(
        "Electric LMI: Month-by-month Expected-With-Cap vs Actual — on line = correct zero-flooring"
    )
    ax.legend()
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_aspect("equal")
    fig.tight_layout()
    out = PLOTS_DIR / "elec_monthly_expected_vs_actual_scatter_with_cap.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _plot_monthly_expected_vs_actual_diff_hist(
    monthly_participants: pl.DataFrame,
) -> None:
    """Histogram of (actual - expected) monthly discount; 0 = perfect match.

    Uses a bin edge at 0 so the full range of differences is visible: negative
    (bill cap), zero (full credit applied), positive (check logic).
    """
    _section_header(
        "Section 1.6c: Month-by-month (Actual − Expected) Discount Distribution (p100)"
    )

    monthly_participants = monthly_participants.with_columns(
        (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
            "actual_discount_monthly"
        )
    ).with_columns(
        (pl.col("actual_discount_monthly") - pl.col("expected_monthly_credit")).alias(
            "discount_diff_monthly"
        )
    )
    diff = monthly_participants["discount_diff_monthly"].to_numpy()
    d_min, d_max = float(diff.min()), float(diff.max())
    n_neg = 35
    n_nonneg = 15
    if d_min < -1e-9:
        bins_neg = np.linspace(d_min, -1e-9, n_neg + 1)
    else:
        bins_neg = np.array([d_min])
    if d_max > 1e-9:
        bins_nonneg = np.linspace(0.0, d_max, n_nonneg + 1)
    else:
        bins_nonneg = np.array([0.0, max(1.0, abs(d_min) * 0.01 + 1e-9)])
    bins = np.unique(np.concatenate([bins_neg, np.array([0.0]), bins_nonneg]))
    if len(bins) < 2:
        bins = np.array([min(d_min, 0) - 1e-9, 0.0, max(d_max, 0) + 1e-9])

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(diff, bins=bins.tolist(), color="steelblue", edgecolor="white")
    ax.axvline(0, color="black", linestyle="--", linewidth=1, label="0 (perfect match)")
    ax.set_xlabel("Actual monthly discount − Expected monthly credit ($)")
    ax.set_ylabel("Count (participant-months)")
    ax.set_title(
        "Electric LMI: Month-by-month (Actual − Expected) — negative = bill cap, positive = check logic"
    )
    ax.legend()
    fig.tight_layout()
    out = PLOTS_DIR / "elec_monthly_expected_vs_actual_diff_hist.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _plot_monthly_expected_vs_actual_diff_hist_with_cap(
    monthly_participants: pl.DataFrame,
) -> None:
    """Histogram of actual minus expected-with-cap (monthly); 0 = correct zero-flooring.

    expected_monthly_capped = min(expected_monthly_credit, elec_total_bill).
    Concentration at 0 means implementation matches the cap rule at monthly level.
    """
    _section_header(
        "Section 1.6d: Month-by-month Expected vs Actual (with zero cap) — diff ~0"
    )

    monthly_participants = (
        monthly_participants.with_columns(
            (pl.col("elec_total_bill") - pl.col("elec_total_bill_lmi_100")).alias(
                "actual_discount_monthly"
            )
        )
        .with_columns(
            pl.min_horizontal("expected_monthly_credit", "elec_total_bill").alias(
                "expected_monthly_capped"
            )
        )
        .with_columns(
            (
                pl.col("actual_discount_monthly") - pl.col("expected_monthly_capped")
            ).alias("diff_vs_capped_monthly")
        )
        .with_columns(
            pl.when(pl.col("diff_vs_capped_monthly").abs() < CAP_DIFF_ZERO_TOL)
            .then(0.0)
            .otherwise(pl.col("diff_vs_capped_monthly"))
            .alias("diff_vs_capped_monthly")
        )
    )
    diff = monthly_participants["diff_vs_capped_monthly"].to_numpy()
    d_min, d_max = float(diff.min()), float(diff.max())
    data_range = d_max - d_min

    if data_range < 0.01 and d_min >= -1e-6 and d_max <= 1e-6:
        bins = np.linspace(-0.02, 0.02, 41)
    else:
        n_neg = 35
        n_nonneg = 15
        if d_min < -1e-9:
            bins_neg = np.linspace(d_min, -1e-9, n_neg + 1)
        else:
            bins_neg = np.array([d_min])
        if d_max > 1e-9:
            bins_nonneg = np.linspace(0.0, d_max, n_nonneg + 1)
        else:
            bins_nonneg = np.array([0.0, max(1.0, abs(d_min) * 0.01 + 1e-9)])
        bins = np.unique(np.concatenate([bins_neg, np.array([0.0]), bins_nonneg]))
        if len(bins) < 2:
            bins = np.array([min(d_min, 0) - 1e-9, 0.0, max(d_max, 0) + 1e-9])

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(diff, bins=bins.tolist(), color="steelblue", edgecolor="white")
    ax.axvline(0, color="black", linestyle="--", linewidth=1, label="0 (perfect match)")
    ax.set_xlabel(
        "Actual monthly discount − min(expected monthly credit, bill) ($)\n"
        "(|diff| < 1e-6 counted as 0)"
    )
    ax.set_ylabel("Count (participant-months)")
    ax.set_title(
        "Electric LMI: Month-by-month Actual vs Expected-With-Cap — centered at 0 = correct zero-flooring"
    )
    ax.legend()
    fig.tight_layout()
    out = PLOTS_DIR / "elec_monthly_expected_vs_actual_diff_hist_with_cap.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


# ---------------------------------------------------------------------------
# Section 2: Participation weighting analysis (p40)
# ---------------------------------------------------------------------------


def _p40_overview(df: pl.DataFrame) -> None:
    """Print participation overview for p40 eligible buildings."""
    _section_header("Section 2.1: p40 Participation Overview (Electric)")

    total_eligible = len(df)
    total_participating = int(df["applied_discount_40"].sum())
    rate = total_participating / total_eligible if total_eligible > 0 else 0.0
    print(f"Total eligible (is_lmi=True): {total_eligible}")
    print(f"Total participating (applied_discount_40=True): {total_participating}")
    print(f"Actual participation rate: {rate:.4f} ({rate * 100:.1f}%)")


def _p40_tier_comparison(df: pl.DataFrame) -> None:
    """Per-tier participation counts and rates."""
    _section_header("Section 2.2: p40 Participation by Tier (Electric)")

    tier_stats = (
        df.group_by("lmi_tier")
        .agg(
            pl.len().alias("n_eligible"),
            pl.col("applied_discount_40").sum().alias("n_participating"),
        )
        .with_columns(
            (pl.col("n_participating") / pl.col("n_eligible")).alias(
                "participation_rate"
            )
        )
        .sort("lmi_tier")
    )
    print(tier_stats)
    print(
        "\nExpected: lower tiers (1-3) should have higher participation rates "
        "than higher tiers (5-7)."
    )


def _p40_participant_vs_excluded_hist(df: pl.DataFrame) -> None:
    """Overlapping histograms of electric bill for participants vs non-participants."""
    _section_header("Section 2.3: p40 Participant vs Excluded Electric Bills")

    has_elec = df.filter(pl.col("elec_total_bill") > 0)
    parts = has_elec.filter(pl.col("applied_discount_40"))
    excluded = has_elec.filter(~pl.col("applied_discount_40"))

    if len(parts) == 0 or len(excluded) == 0:
        print("Insufficient data for comparison — skipping.")
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    all_vals = has_elec["elec_total_bill"].to_numpy()
    bins = np.linspace(all_vals.min(), all_vals.max(), 50)
    ax.hist(
        parts["elec_total_bill"].to_numpy(),
        bins=bins,
        alpha=0.5,
        color="steelblue",
        label="Participants",
        edgecolor="white",
    )
    ax.hist(
        excluded["elec_total_bill"].to_numpy(),
        bins=bins,
        alpha=0.5,
        color="coral",
        label="Excluded",
        edgecolor="white",
    )
    ax.set_xlabel("Annual electric bill ($)")
    ax.set_ylabel("Count")
    ax.set_title(
        "p40: Electric Bill Distribution — Participants vs Excluded (LMI eligible)"
    )
    ax.legend()
    fig.tight_layout()
    out = PLOTS_DIR / "p40_participant_vs_excluded_elec_bills.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _p40_participation_rate_by_tier(df: pl.DataFrame) -> None:
    """Bar chart of participation rate by tier."""
    _section_header("Section 2.4: p40 Participation Rate by Tier (Electric)")

    tier_stats = (
        df.group_by("lmi_tier")
        .agg(
            pl.len().alias("n_eligible"),
            pl.col("applied_discount_40").sum().alias("n_participating"),
        )
        .with_columns(
            (pl.col("n_participating") / pl.col("n_eligible")).alias(
                "participation_rate"
            )
        )
        .sort("lmi_tier")
    )
    tiers = tier_stats["lmi_tier"].to_list()
    rates = tier_stats["participation_rate"].to_list()

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar([str(t) for t in tiers], rates, color="steelblue", edgecolor="white")
    ax.set_xlabel("EAP Tier")
    ax.set_ylabel("Participation rate")
    ax.set_title("p40: Participation Rate by Tier (LMI eligible, electric)")
    ax.set_ylim(0, 1.0)
    for i, (t, r) in enumerate(zip(tiers, rates)):
        ax.text(i, r + 0.02, f"{r:.2f}", ha="center", fontsize=9)
    fig.tight_layout()
    out = PLOTS_DIR / "p40_participation_rate_by_tier_elec.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _p40_participation_by_bill_size(df: pl.DataFrame) -> None:
    """Strip plot of electric bill by electric utility, colored by participation status."""
    _section_header(
        "Section 2.5: p40 Participation by Bill Size (Electric, strip plot)"
    )

    has_elec = df.filter(pl.col("elec_total_bill") > 0)
    utilities = sorted(has_elec["sb.electric_utility"].unique().to_list())
    n_utils = len(utilities)
    if n_utils == 0:
        print("No electric utilities — skipping.")
        return

    fig, axes = plt.subplots(1, n_utils, figsize=(5 * n_utils, 6), squeeze=False)

    for idx, util in enumerate(utilities):
        ax = axes[0][idx]
        subset = has_elec.filter(pl.col("sb.electric_utility") == util)
        parts = subset.filter(pl.col("applied_discount_40"))
        excluded = subset.filter(~pl.col("applied_discount_40"))

        rng = np.random.default_rng(42)
        if len(excluded) > 0:
            jitter_e = rng.uniform(-0.15, 0.15, size=len(excluded))
            ax.scatter(
                jitter_e,
                excluded["elec_total_bill"].to_numpy(),
                alpha=0.3,
                s=8,
                color="coral",
                label="Excluded",
            )
        if len(parts) > 0:
            jitter_p = rng.uniform(-0.15, 0.15, size=len(parts))
            ax.scatter(
                jitter_p,
                parts["elec_total_bill"].to_numpy(),
                alpha=0.5,
                s=12,
                color="steelblue",
                label="Participant",
            )
        ax.set_title(util)
        ax.set_ylabel("Annual electric bill ($)")
        ax.set_xticks([])
        ax.legend(fontsize=7, loc="upper right")

    fig.suptitle(
        "p40: Electric Bill by Utility — Participants vs Excluded (LMI eligible)",
        fontsize=13,
    )
    fig.tight_layout()
    out = PLOTS_DIR / "p40_participation_by_bill_size_elec.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Section 3: Cross-run and source integrity checks
# ---------------------------------------------------------------------------


def _cross_check_p100_p40(p100: pl.DataFrame, p40: pl.DataFrame) -> None:
    """Verify lmi_tier and is_lmi are identical between p100 and p40."""
    _section_header("Section 3.1: p100 vs p40 tier/eligibility consistency")

    p100_ann = p100.filter(pl.col("month") == ANNUAL_MONTH).select(
        "bldg_id", "lmi_tier", "is_lmi"
    )
    p40_ann = p40.filter(pl.col("month") == ANNUAL_MONTH).select(
        "bldg_id", "lmi_tier", "is_lmi"
    )

    joined = p100_ann.join(p40_ann, on="bldg_id", suffix="_p40")

    tier_mismatch = joined.filter(pl.col("lmi_tier") != pl.col("lmi_tier_p40")).height
    lmi_mismatch = joined.filter(pl.col("is_lmi") != pl.col("is_lmi_p40")).height

    print(f"Buildings compared: {joined.height}")
    print(f"lmi_tier mismatches: {tier_mismatch}")
    print(f"is_lmi mismatches: {lmi_mismatch}")
    if tier_mismatch == 0 and lmi_mismatch == 0:
        print("PASS: p100 and p40 have identical tier assignments")
    else:
        print("FAIL: tier assignments differ between p100 and p40")


def _check_source_columns(
    staging: pl.DataFrame, prod: pl.DataFrame, label: str
) -> None:
    """Verify pass-through columns in staging match the production source."""
    _section_header(f"Section 3.2: Source column integrity ({label})")

    lmi_cols = {
        "lmi_tier",
        "is_lmi",
        "elec_total_bill_lmi_100",
        "gas_total_bill_lmi_100",
        "applied_discount_100",
        "elec_total_bill_lmi_40",
        "gas_total_bill_lmi_40",
        "applied_discount_40",
    }
    shared_cols = [
        c for c in prod.columns if c in staging.columns and c not in lmi_cols
    ]
    print(f"Checking {len(shared_cols)} pass-through columns: {shared_cols}")

    staging_sub = staging.select(shared_cols).sort("bldg_id", "month")
    prod_sub = prod.select(shared_cols).sort("bldg_id", "month")

    if staging_sub.height != prod_sub.height:
        print(
            f"FAIL: row count mismatch: staging={staging_sub.height}, "
            f"prod={prod_sub.height}"
        )
        return

    n_diffs = 0
    for col in shared_cols:
        if col in ("bldg_id", "month"):
            continue
        if staging_sub[col].dtype.is_float():
            diff = float((staging_sub[col] - prod_sub[col]).abs().max())  # type: ignore[arg-type]
            if diff > 1e-10:
                print(f"  FAIL: {col} max diff = {diff}")
                n_diffs += 1
        else:
            mismatches = int((staging_sub[col] != prod_sub[col]).sum())
            if mismatches > 0:
                print(f"  FAIL: {col} has {mismatches} mismatches")
                n_diffs += 1

    if n_diffs == 0:
        print("PASS: all pass-through columns match production source")
    else:
        print(f"FAIL: {n_diffs} columns differ from production")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and explore LMI electric bill discounts from master-bills outputs.",
    )
    parser.add_argument("--state", default="ny", help="State code (default: ny).")
    parser.add_argument(
        "--batch", required=True, help="Batch name (e.g. ny_20260311b_r1-12)."
    )
    parser.add_argument(
        "--run-delivery", type=int, default=1, help="Delivery run number (default: 1)."
    )
    parser.add_argument(
        "--run-supply", type=int, default=2, help="Supply run number (default: 2)."
    )
    parser.add_argument(
        "--p100-path",
        help="Override: S3/local path to p100 comb_bills_year_target dataset. "
        "If omitted, built from --state/--batch/--run-delivery/--run-supply.",
    )
    parser.add_argument(
        "--p40-path",
        help="Override: S3/local path to p40 comb_bills_year_target dataset. "
        "If omitted, built from batch args. Pass 'skip' to skip p40 sections.",
    )
    parser.add_argument(
        "--prod-path",
        help="Override: S3/local path to the production/source comb_bills_year_target. "
        "If omitted, built from batch args (the master bills path).",
    )
    args = parser.parse_args()

    state = args.state.lower()
    if not args.p100_path:
        args.p100_path = _build_lmi_path(
            state, args.batch, args.run_delivery, args.run_supply, 100
        )
    if not args.p40_path:
        args.p40_path = _build_lmi_path(
            state, args.batch, args.run_delivery, args.run_supply, 40
        )
    if not args.prod_path:
        args.prod_path = _build_master_path(
            state, args.batch, args.run_delivery, args.run_supply
        )
    return args


def main() -> None:
    args = _parse_args()
    load_dotenv()
    opts = get_aws_storage_options()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    skip_p40 = args.p40_path.lower() == "skip"

    print(f"p100 path: {args.p100_path}")
    print(f"p40 path:  {'(skipped)' if skip_p40 else args.p40_path}")
    print(f"prod path: {args.prod_path}")

    # --- Load data ---
    _section_header("Loading p100 staging data")
    p100 = _load_staging(args.p100_path, opts)
    print(f"p100 shape: {p100.shape}")
    print(f"p100 columns: {p100.columns}")

    p40: pl.DataFrame | None = None
    if not skip_p40:
        _section_header("Loading p40 staging data")
        p40 = _load_staging(args.p40_path, opts)
        print(f"p40 shape: {p40.shape}")
        print(f"p40 columns: {p40.columns}")

    # --- Section 1: Electric discount EDA (p100) ---
    p100_annual_elec = p100.filter(
        (pl.col("month") == "Annual") & (pl.col("elec_total_bill") > 0)
    )
    print(f"\np100 annual rows with elec > 0: {len(p100_annual_elec)}")

    _elec_summary_stats(p100_annual_elec)
    _elec_discount_histogram(p100_annual_elec)
    _elec_before_after_histogram(p100_annual_elec)
    _tier_distribution_bar(p100_annual_elec)

    # --- Section 1.5: Expected vs actual discount (EAP/EEAP credit table) ---
    credits_df = get_ny_eap_credits_df()
    p100_participants = p100_annual_elec.filter(pl.col("applied_discount_100"))
    p100_participants_with_expected = _merge_expected_electric_credit(
        p100_participants, credits_df
    )
    if p100_participants_with_expected.height > 0:
        _expected_vs_actual_summary(p100_participants_with_expected)
        _plot_expected_vs_actual_scatter(p100_participants_with_expected)
        _plot_expected_vs_actual_scatter_with_cap(p100_participants_with_expected)
        _plot_expected_vs_actual_by_utility_tier(p100_participants_with_expected)
        _plot_expected_vs_actual_diff_hist(p100_participants_with_expected)
        _plot_expected_vs_actual_diff_hist_with_cap(p100_participants_with_expected)
    else:
        _section_header("Section 1.5: Expected vs Actual (p100)")
        print("No p100 participants — skipping expected vs actual plots.")

    # --- Section 1.6: Month-by-month expected vs actual (p100) ---
    p100_monthly_elec = p100.filter(
        (pl.col("month") != ANNUAL_MONTH) & (pl.col("elec_total_bill") > 0)
    )
    p100_monthly_participants = p100_monthly_elec.filter(pl.col("applied_discount_100"))
    p100_monthly_participants_with_expected = _merge_expected_electric_credit_monthly(
        p100_monthly_participants, credits_df
    )
    if p100_monthly_participants_with_expected.height > 0:
        _plot_monthly_expected_vs_actual_scatter(
            p100_monthly_participants_with_expected
        )
        _plot_monthly_expected_vs_actual_scatter_with_cap(
            p100_monthly_participants_with_expected
        )
        _plot_monthly_expected_vs_actual_diff_hist(
            p100_monthly_participants_with_expected
        )
        _plot_monthly_expected_vs_actual_diff_hist_with_cap(
            p100_monthly_participants_with_expected
        )
    else:
        _section_header("Section 1.6: Month-by-month Expected vs Actual (p100)")
        print("No p100 monthly participant rows — skipping monthly plots.")

    # --- Section 2: Participation weighting (p40) ---
    if p40 is not None:
        p40_annual_lmi = p40.filter((pl.col("month") == "Annual") & (pl.col("is_lmi")))
        print(f"\np40 annual LMI-eligible rows: {len(p40_annual_lmi)}")

        _p40_overview(p40_annual_lmi)
        _p40_tier_comparison(p40_annual_lmi)
        _p40_participant_vs_excluded_hist(p40_annual_lmi)
        _p40_participation_rate_by_tier(p40_annual_lmi)
        _p40_participation_by_bill_size(p40_annual_lmi)

        # --- Section 3: Cross-run and source integrity ---
        _cross_check_p100_p40(p100, p40)
    else:
        _section_header("Section 2 & 3: p40 (skipped)")
        print(
            "p40 path set to 'skip' — skipping participation and cross-check sections."
        )

    # --- Source column integrity ---
    skip_prod = args.prod_path == args.p100_path
    if not skip_prod:
        _section_header("Loading production source data")
        prod = _load_staging(args.prod_path, opts)
        print(f"prod shape: {prod.shape}")
        _check_source_columns(p100, prod, "p100 vs production")
    else:
        _section_header("Source column integrity (skipped)")
        print("prod path == p100 path (integrated mode) — skipping source comparison.")

    _section_header("Done")
    print(f"All plots saved to {PLOTS_DIR}/")


if __name__ == "__main__":
    main()

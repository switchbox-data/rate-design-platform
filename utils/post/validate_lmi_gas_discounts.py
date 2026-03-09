"""Validate and explore LMI gas bill discounts applied to master bills.

Reads staging outputs for p100 and p40 participation scenarios, produces
histograms, summary stats, and diagnostics about the weighted participation
sampling. All plots saved to dev_plots/ with Agg backend (no display).
"""

from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

matplotlib.use("Agg")

PLOTS_DIR = Path(__file__).resolve().parents[2] / "dev_plots"

S3_BASE = "s3://data.sb/switchbox/lmi/ny/ny_20260307_r1-8_gascalcfix/run_1+2"
P100_PATH = f"{S3_BASE}/p100/comb_bills_year_target/"
P40_PATH = f"{S3_BASE}/p40/comb_bills_year_target/"


def _load_staging(path: str, opts: dict[str, str]) -> pl.DataFrame:
    return pl.read_parquet(path, hive_partitioning=True, storage_options=opts)


def _section_header(title: str) -> None:
    sep = "=" * 72
    print(f"\n{sep}\n  {title}\n{sep}\n")


# ---------------------------------------------------------------------------
# Section 1: Gas discount EDA (p100)
# ---------------------------------------------------------------------------


def _gas_summary_stats(df: pl.DataFrame) -> None:
    """Print per-gas-utility summary stats for p100 annual gas bills."""
    _section_header(
        "Section 1.1: Gas Discount Summary Stats (p100, Annual, gas_total_bill > 0)"
    )

    stats = (
        df.group_by("sb.gas_utility")
        .agg(
            pl.len().alias("n_buildings"),
            pl.col("is_lmi").sum().alias("n_lmi"),
            pl.col("applied_discount_100").sum().alias("n_discounted"),
            pl.col("gas_total_bill").mean().alias("gas_bill_mean"),
            pl.col("gas_total_bill").median().alias("gas_bill_median"),
            pl.col("gas_total_bill").min().alias("gas_bill_min"),
            pl.col("gas_total_bill").max().alias("gas_bill_max"),
            pl.col("gas_total_bill_lmi_100").mean().alias("gas_lmi_mean"),
            pl.col("gas_total_bill_lmi_100").median().alias("gas_lmi_median"),
            pl.col("gas_total_bill_lmi_100").min().alias("gas_lmi_min"),
            pl.col("gas_total_bill_lmi_100").max().alias("gas_lmi_max"),
            (pl.col("gas_total_bill") - pl.col("gas_total_bill_lmi_100"))
            .mean()
            .alias("discount_mean"),
            (pl.col("gas_total_bill") - pl.col("gas_total_bill_lmi_100"))
            .median()
            .alias("discount_median"),
        )
        .sort("sb.gas_utility")
    )
    print(stats)


def _gas_discount_histogram(df: pl.DataFrame) -> None:
    """Histogram of gas discount amount for LMI participants, faceted by gas utility."""
    _section_header("Section 1.2: Gas Discount Amount Histogram (p100)")

    participants = df.filter(pl.col("applied_discount_100"))
    utilities = sorted(participants["sb.gas_utility"].unique().to_list())
    n_utils = len(utilities)
    if n_utils == 0:
        print("No participants with applied_discount_100=True — skipping.")
        return

    ncols = min(n_utils, 3)
    nrows = (n_utils + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows), squeeze=False)

    for idx, util in enumerate(utilities):
        ax = axes[idx // ncols][idx % ncols]
        subset = participants.filter(pl.col("sb.gas_utility") == util)
        discount = (
            subset["gas_total_bill"] - subset["gas_total_bill_lmi_100"]
        ).to_numpy()
        ax.hist(discount, bins=40, color="steelblue", edgecolor="white")
        ax.set_title(util)
        ax.set_xlabel("Gas discount ($)")
        ax.set_ylabel("Count")

    for idx in range(n_utils, nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle("Gas Bill Discount Amount — p100 Participants", fontsize=14, y=1.01)
    fig.tight_layout()
    out = PLOTS_DIR / "gas_discount_hist_p100.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _gas_before_after_histogram(df: pl.DataFrame) -> None:
    """Overlapping histograms of gas bill before vs after discount, faceted by gas utility."""
    _section_header("Section 1.3: Gas Bill Before vs After Histogram (p100)")

    participants = df.filter(pl.col("applied_discount_100"))
    utilities = sorted(participants["sb.gas_utility"].unique().to_list())
    n_utils = len(utilities)
    if n_utils == 0:
        print("No participants — skipping.")
        return

    ncols = min(n_utils, 3)
    nrows = (n_utils + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows), squeeze=False)

    for idx, util in enumerate(utilities):
        ax = axes[idx // ncols][idx % ncols]
        subset = participants.filter(pl.col("sb.gas_utility") == util)
        before = subset["gas_total_bill"].to_numpy()
        after = subset["gas_total_bill_lmi_100"].to_numpy()
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
        ax.set_xlabel("Annual gas bill ($)")
        ax.set_ylabel("Count")
        ax.legend(fontsize=8)

    for idx in range(n_utils, nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    fig.suptitle(
        "Gas Bill Before vs After Discount — p100 Participants", fontsize=14, y=1.01
    )
    fig.tight_layout()
    out = PLOTS_DIR / "gas_bill_before_after_p100.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _tier_distribution_bar(df: pl.DataFrame) -> None:
    """Bar chart of tier distribution by gas utility."""
    _section_header("Section 1.4: Tier Distribution by Gas Utility")

    tier_counts = (
        df.filter(pl.col("lmi_tier") > 0)
        .group_by("sb.gas_utility", "lmi_tier")
        .agg(pl.len().alias("count"))
        .sort("sb.gas_utility", "lmi_tier")
    )
    utilities = sorted(tier_counts["sb.gas_utility"].unique().to_list())
    tiers = sorted(tier_counts["lmi_tier"].unique().to_list())
    n_utils = len(utilities)

    fig, ax = plt.subplots(figsize=(12, 6))
    width = 0.8 / n_utils
    x = np.arange(len(tiers))

    for i, util in enumerate(utilities):
        util_data = tier_counts.filter(pl.col("sb.gas_utility") == util)
        counts = []
        for t in tiers:
            row = util_data.filter(pl.col("lmi_tier") == t)
            counts.append(int(row["count"][0]) if len(row) > 0 else 0)
        ax.bar(x + i * width, counts, width, label=util)

    ax.set_xticks(x + width * (n_utils - 1) / 2)
    ax.set_xticklabels([str(t) for t in tiers])
    ax.set_xlabel("EAP Tier")
    ax.set_ylabel("Count of buildings")
    ax.set_title("Tier Distribution by Gas Utility (Annual, gas_total_bill > 0)")
    ax.legend(fontsize=8)
    fig.tight_layout()
    out = PLOTS_DIR / "gas_tier_distribution.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


# ---------------------------------------------------------------------------
# Section 2: Participation weighting analysis (p40)
# ---------------------------------------------------------------------------


def _p40_overview(df: pl.DataFrame) -> None:
    """Print participation overview for p40 eligible buildings."""
    _section_header("Section 2.1: p40 Participation Overview")

    total_eligible = len(df)
    total_participating = int(df["applied_discount_40"].sum())
    rate = total_participating / total_eligible if total_eligible > 0 else 0.0
    print(f"Total eligible (is_lmi=True): {total_eligible}")
    print(f"Total participating (applied_discount_40=True): {total_participating}")
    print(f"Actual participation rate: {rate:.4f} ({rate * 100:.1f}%)")


def _p40_tier_comparison(df: pl.DataFrame) -> None:
    """Per-tier participation counts and rates."""
    _section_header("Section 2.2: p40 Participation by Tier")

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
    """Overlapping histograms of gas bill for participants vs non-participants."""
    _section_header("Section 2.3: p40 Participant vs Excluded Gas Bills")

    has_gas = df.filter(pl.col("gas_total_bill") > 0)
    parts = has_gas.filter(pl.col("applied_discount_40"))
    excluded = has_gas.filter(~pl.col("applied_discount_40"))

    if len(parts) == 0 or len(excluded) == 0:
        print("Insufficient data for comparison — skipping.")
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    all_vals = has_gas["gas_total_bill"].to_numpy()
    bins = np.linspace(all_vals.min(), all_vals.max(), 50)
    ax.hist(
        parts["gas_total_bill"].to_numpy(),
        bins=bins,
        alpha=0.5,
        color="steelblue",
        label="Participants",
        edgecolor="white",
    )
    ax.hist(
        excluded["gas_total_bill"].to_numpy(),
        bins=bins,
        alpha=0.5,
        color="coral",
        label="Excluded",
        edgecolor="white",
    )
    ax.set_xlabel("Annual gas bill ($)")
    ax.set_ylabel("Count")
    ax.set_title("p40: Gas Bill Distribution — Participants vs Excluded (LMI eligible)")
    ax.legend()
    fig.tight_layout()
    out = PLOTS_DIR / "p40_participant_vs_excluded_gas_bills.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _p40_participation_rate_by_tier(df: pl.DataFrame) -> None:
    """Bar chart of participation rate by tier."""
    _section_header("Section 2.4: p40 Participation Rate by Tier")

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
    ax.set_title("p40: Participation Rate by Tier (LMI eligible)")
    ax.set_ylim(0, 1.0)
    for i, (t, r) in enumerate(zip(tiers, rates)):
        ax.text(i, r + 0.02, f"{r:.2f}", ha="center", fontsize=9)
    fig.tight_layout()
    out = PLOTS_DIR / "p40_participation_rate_by_tier.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


def _p40_participation_by_bill_size(df: pl.DataFrame) -> None:
    """Strip plot of gas bill by gas utility, colored by participation status."""
    _section_header("Section 2.5: p40 Participation by Bill Size (strip plot)")

    has_gas = df.filter(pl.col("gas_total_bill") > 0)
    utilities = sorted(has_gas["sb.gas_utility"].unique().to_list())
    n_utils = len(utilities)
    if n_utils == 0:
        print("No gas utilities — skipping.")
        return

    fig, axes = plt.subplots(1, n_utils, figsize=(5 * n_utils, 6), squeeze=False)

    for idx, util in enumerate(utilities):
        ax = axes[0][idx]
        subset = has_gas.filter(pl.col("sb.gas_utility") == util)
        parts = subset.filter(pl.col("applied_discount_40"))
        excluded = subset.filter(~pl.col("applied_discount_40"))

        rng = np.random.default_rng(42)
        if len(excluded) > 0:
            jitter_e = rng.uniform(-0.15, 0.15, size=len(excluded))
            ax.scatter(
                jitter_e,
                excluded["gas_total_bill"].to_numpy(),
                alpha=0.3,
                s=8,
                color="coral",
                label="Excluded",
            )
        if len(parts) > 0:
            jitter_p = rng.uniform(-0.15, 0.15, size=len(parts))
            ax.scatter(
                jitter_p,
                parts["gas_total_bill"].to_numpy(),
                alpha=0.5,
                s=12,
                color="steelblue",
                label="Participant",
            )
        ax.set_title(util)
        ax.set_ylabel("Annual gas bill ($)")
        ax.set_xticks([])
        ax.legend(fontsize=7, loc="upper right")

    fig.suptitle(
        "p40: Gas Bill by Utility — Participants vs Excluded (LMI eligible)",
        fontsize=13,
    )
    fig.tight_layout()
    out = PLOTS_DIR / "p40_participation_by_bill_size.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    load_dotenv()
    opts = get_aws_storage_options()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Load data ---
    _section_header("Loading p100 staging data")
    p100 = _load_staging(P100_PATH, opts)
    print(f"p100 shape: {p100.shape}")
    print(f"p100 columns: {p100.columns}")

    _section_header("Loading p40 staging data")
    p40 = _load_staging(P40_PATH, opts)
    print(f"p40 shape: {p40.shape}")
    print(f"p40 columns: {p40.columns}")

    # --- Section 1: Gas discount EDA (p100) ---
    p100_annual_gas = p100.filter(
        (pl.col("month") == "Annual") & (pl.col("gas_total_bill") > 0)
    )
    print(f"\np100 annual rows with gas > 0: {len(p100_annual_gas)}")

    _gas_summary_stats(p100_annual_gas)
    _gas_discount_histogram(p100_annual_gas)
    _gas_before_after_histogram(p100_annual_gas)
    _tier_distribution_bar(p100_annual_gas)

    # --- Section 2: Participation weighting (p40) ---
    p40_annual_lmi = p40.filter((pl.col("month") == "Annual") & (pl.col("is_lmi")))
    print(f"\np40 annual LMI-eligible rows: {len(p40_annual_lmi)}")

    _p40_overview(p40_annual_lmi)
    _p40_tier_comparison(p40_annual_lmi)
    _p40_participant_vs_excluded_hist(p40_annual_lmi)
    _p40_participation_rate_by_tier(p40_annual_lmi)
    _p40_participation_by_bill_size(p40_annual_lmi)

    _section_header("Done")
    print(f"All plots saved to {PLOTS_DIR}/")


if __name__ == "__main__":
    main()

"""Derive bulk transmission v_z ($/kW-yr) per gen_capacity_zone from NYISO studies.

Implements a three-step derivation:

Step 1 — Discrete marginal values:
    v_i = B_i / (ΔMW_i × 1000) for each project with a published ΔMW.

Step 2 — Average secant of the diminishing-returns curve:
    Within each (locality, scenario_family) group:

    a. Collapse scenario variants: if multiple rows share the same (project,
       delta_mw) — e.g. the same physical project evaluated under Baseline and
       CES+Ret scenarios — average their annual_benefit_m_yr first so each
       physical project contributes one data point.

    b. Sort collapsed projects by ΔMW ascending.  This traces the "supply
       curve" of capacity expansion: smaller-increment projects first.

    c. Compute the cumulative secant at each step:
           secant_i = cum_B_i × 1000 / cum_ΔMW_i   [$/kW-yr]

    d. v_avg = mean(secant_i) across all steps.

    This "average secant" represents the typical cost-effectiveness along the
    diminishing-returns curve.  A single large-ΔMW low-v project only affects
    the last cumulative secant, not every term — so it has less influence than
    in a simple mean(v_i) or MW-weighted average.

Step 3 — Aggregate to gen_capacity_zone via receiving_locality:
    Zone membership is determined by the receiving_locality column in the
    project table (where the benefit actually accrues), not by study locality.

    receiving_locality → gen_capacity_zone(s):
      G-K  → LHV and LI   (G-K spans zones G–K; K = Long Island)
      G-J  → LHV           (G–J corridor; J is part of LHV, not LI)
      J    → NYC            (explicit UPNY→ConEd interface studies, J only)
      ROS  → ROS

    G-J projects go to LHV only (not NYC): the MMU J-only studies are the
    dedicated NYC data source.  Adding G-J projects would dilute NYC with a
    mixed G-through-J corridor value.

    G-K projects contribute to BOTH LHV and LI because G-K includes zone K
    (Long Island).  LI has no projects with receiving_locality=K, so the G-K
    projects are the only basis for LI's v_z.

    LI Export projects (locality=K, direction K→G-J) have receiving_locality
    =G-J and therefore count toward LHV, not LI — the benefit of increased
    export capability accrues to the mainland (G-J) load, not LI load.

    The addendum_optimizer_gj_elim family (G-J Elimination sensitivity) is
    excluded from all zones; it is a scenario sensitivity, not a primary study.

Input:
    ny_bulk_tx_projects.csv — raw project-level data from NYISO studies.

Output:
    ny_bulk_tx_values.csv — v_z per gen_capacity_zone:
        gen_capacity_zone, v_avg_kw_yr

Usage:
    uv run python data/nyiso/transmission/derive_tx_values.py \\
        --path-projects-csv data/nyiso/transmission/csv/ny_bulk_tx_projects.csv \\
        --path-output-csv data/nyiso/transmission/csv/ny_bulk_tx_values.csv
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import polars as pl

# ── Scenario family classification ────────────────────────────────────────────

SCENARIO_FAMILY_MAP: dict[str, str] = {
    "AC Primary": "ac_primary",
    "Addendum Optimizer (Existing Localities)": "addendum_optimizer",
    "Addendum Optimizer (G-J Elimination)": "addendum_optimizer_gj_elim",
    "Addendum MMU (Baseline)": "mmu",
    "Addendum MMU (CES+Ret)": "mmu",
    "LI Export (Policy)": "li_export",
    "NiMo MCOS (Bulk TX)": "nimo_mcos",
}

# ── receiving_locality → gen_capacity_zone(s) ─────────────────────────────────
# Determines which zone(s) each project family contributes to in Step 3.
# G-K spans zones G through K (includes LI = zone K), so it goes to both
# LHV and LI.  G-J spans G through J (no K), so LHV only.  J alone = NYC.

RECEIVING_LOCALITY_ZONES: dict[str, list[str]] = {
    "ROS": ["ROS"],
    "G-K": ["LHV", "LI"],  # includes zone K → contributes to both
    "G-J": ["LHV"],  # corridor only; J is included in LHV, not NYC
    "J": ["NYC"],  # UPNY-ConEd interface; explicitly NYC-only
    "K": ["LI"],  # K-only (no current projects; for completeness)
}

# Scenario families excluded from zone aggregation (sensitivities, not primary)
EXCLUDED_FAMILIES: frozenset[str] = frozenset({"addendum_optimizer_gj_elim"})


# ── Step 1: Discrete marginal values ─────────────────────────────────────────


def compute_discrete_v(df: pl.DataFrame) -> pl.DataFrame:
    """Compute v = B / (ΔMW × 1000) for rows with published ΔMW.

    Args:
        df: Raw projects DataFrame.

    Returns:
        DataFrame filtered to rows with ΔMW, with added v_kw_yr column.
    """
    with_delta = df.filter(pl.col("delta_mw").is_not_null())

    result = with_delta.with_columns(
        (pl.col("annual_benefit_m_yr") * 1_000_000 / (pl.col("delta_mw") * 1000)).alias(
            "v_kw_yr"
        )
    )

    for row in result.iter_rows(named=True):
        v = row["v_kw_yr"]
        if v > 100:
            warnings.warn(
                f"High v = ${v:.1f}/kW-yr for {row['project']} ({row['locality']})",
                stacklevel=2,
            )
        elif v < -20:
            warnings.warn(
                f"Negative v = ${v:.1f}/kW-yr for {row['project']} ({row['locality']})",
                stacklevel=2,
            )

    print("\n" + "=" * 70)
    print("STEP 1: Discrete marginal values (v = B / ΔMW × 1000)")
    print("=" * 70)
    print(
        result.select(
            "scenario",
            "project",
            "locality",
            "receiving_locality",
            "delta_mw",
            "v_kw_yr",
        ).sort("locality", "scenario", "delta_mw")
    )

    return result


# ── Step 2: Average secant of the diminishing-returns curve ──────────────────


def compute_avg_secant(df: pl.DataFrame) -> pl.DataFrame:
    """Compute v_avg per (locality, scenario_family) via cumulative secants.

    Also captures receiving_locality for each group (used in zone aggregation).

    Within each (locality, scenario_family) group:
      1. Collapse scenario variants by (project, delta_mw) → average B.
      2. Sort by ΔMW ascending (diminishing-returns order).
      3. Accumulate; compute secant_i = cum_B_i × 1000 / cum_ΔMW_i.
      4. v_avg = mean(secant_i).

    Args:
        df: Step 1 output (rows with delta_mw only).

    Returns:
        DataFrame with locality, scenario_family, receiving_locality,
        v_avg_kw_yr, n_projects.
    """
    df_with_family = df.with_columns(
        pl.col("scenario").replace_strict(SCENARIO_FAMILY_MAP).alias("scenario_family")
    )

    results: list[dict[str, object]] = []

    for (locality, scenario_family), group_df in df_with_family.group_by(
        "locality", "scenario_family"
    ):
        # Step 2a: collapse scenario variants of the same physical project
        project_df = (
            group_df.group_by("project", "delta_mw")
            .agg(
                pl.col("annual_benefit_m_yr").mean().alias("mean_benefit_m"),
                pl.col("receiving_locality").first().alias("receiving_locality"),
            )
            .sort("delta_mw")
        )

        # Verify receiving_locality is consistent across collapsed project rows
        recv_locs = project_df["receiving_locality"].unique().to_list()
        if len(recv_locs) != 1:
            warnings.warn(
                f"Mixed receiving_locality in {locality}/{scenario_family}: {recv_locs}. "
                "Using first.",
                stacklevel=2,
            )
        receiving_locality = recv_locs[0]

        delta_mws: list[float] = project_df["delta_mw"].to_list()
        benefits: list[float] = project_df["mean_benefit_m"].to_list()
        n = len(delta_mws)

        if n < 2:
            warnings.warn(
                f"Only {n} project(s) for {locality}/{scenario_family}: "
                "secant curve is a single point.",
                stacklevel=2,
            )

        # Steps 2b–2d: cumulative secants, sorted by ΔMW ascending
        cum_mw = 0.0
        cum_b = 0.0
        secants: list[float] = []
        for dmw, b in zip(delta_mws, benefits, strict=True):
            cum_mw += dmw
            cum_b += b
            secants.append(cum_b * 1000.0 / cum_mw)  # $/kW-yr

        v_avg = sum(secants) / len(secants)

        zones = RECEIVING_LOCALITY_ZONES.get(receiving_locality, [])
        excluded = scenario_family in EXCLUDED_FAMILIES
        print(
            f"  {locality}/{scenario_family} (recv={receiving_locality}): "
            f"n={n}, secants=[{', '.join(f'{s:.2f}' for s in secants)}], "
            f"v_avg={v_avg:.2f} $/kW-yr"
            + (f" → zones: {zones}" if not excluded else " → EXCLUDED (sensitivity)")
        )

        results.append(
            {
                "locality": locality,
                "scenario_family": scenario_family,
                "receiving_locality": receiving_locality,
                "v_avg_kw_yr": round(v_avg, 2),
                "n_projects": n,
            }
        )

    result_df = pl.DataFrame(results)

    print("\n" + "=" * 70)
    print("STEP 2: Average secant per (locality, scenario_family)")
    print("=" * 70)
    print(
        result_df.sort("locality", "scenario_family").select(
            "locality",
            "scenario_family",
            "receiving_locality",
            "v_avg_kw_yr",
            "n_projects",
        )
    )

    return result_df


# ── Step 3: Aggregate to gen_capacity_zone ───────────────────────────────────


def aggregate_to_zones(avg_df: pl.DataFrame) -> pl.DataFrame:
    """Map family-level v_avg to gen_capacity_zones via receiving_locality.

    Zone assignment (from receiving_locality column):
      G-K  → LHV and LI   (G-K spans G–K; K = LI zone)
      G-J  → LHV           (G–J corridor; not LI)
      J    → NYC            (UPNY-ConEd interface, J-only studies)
      ROS  → ROS

    Zone v_avg = mean of contributing family v_avg values.
    The addendum_optimizer_gj_elim family is excluded (sensitivity scenario).

    Args:
        avg_df: Step 2 output.

    Returns:
        DataFrame with gen_capacity_zone, v_avg_kw_yr.
    """
    zone_contributions: dict[str, list[float]] = {
        "ROS": [],
        "LHV": [],
        "NYC": [],
        "LI": [],
    }

    print("\n" + "=" * 70)
    print("STEP 3: Zone assignments from receiving_locality")
    print("=" * 70)

    for row in avg_df.iter_rows(named=True):
        family = row["scenario_family"]
        if family in EXCLUDED_FAMILIES:
            print(
                f"  SKIP {row['locality']}/{family} (recv={row['receiving_locality']}): "
                "excluded sensitivity"
            )
            continue

        recv = row["receiving_locality"]
        zones = RECEIVING_LOCALITY_ZONES.get(recv, [])
        v = row["v_avg_kw_yr"]

        if not zones:
            warnings.warn(
                f"No zone mapping for receiving_locality='{recv}' "
                f"({row['locality']}/{family}); skipping.",
                stacklevel=2,
            )
            continue

        for zone in zones:
            if zone in zone_contributions:
                zone_contributions[zone].append(v)

        print(
            f"  {row['locality']}/{family} (recv={recv}): "
            f"v_avg=${v:.2f}/kW-yr → {zones}"
        )

    # Build final zone table
    zone_results: list[dict[str, object]] = []
    for zone in ("ROS", "LHV", "NYC", "LI"):
        vs = zone_contributions[zone]
        if not vs:
            raise ValueError(
                f"No projects contributed to zone {zone}. "
                "Check ny_bulk_tx_projects.csv and RECEIVING_LOCALITY_ZONES."
            )
        v_zone = round(sum(vs) / len(vs), 2)
        zone_results.append({"gen_capacity_zone": zone, "v_avg_kw_yr": v_zone})
        print(
            f"\n  {zone}: mean of [{', '.join(f'${v:.2f}' for v in vs)}] "
            f"= ${v_zone:.2f}/kW-yr"
        )

    result_df = pl.DataFrame(zone_results)

    # Validation
    lhv_val = float(
        result_df.filter(pl.col("gen_capacity_zone") == "LHV")["v_avg_kw_yr"][0]
    )
    nyc_val = float(
        result_df.filter(pl.col("gen_capacity_zone") == "NYC")["v_avg_kw_yr"][0]
    )
    ros_val = float(
        result_df.filter(pl.col("gen_capacity_zone") == "ROS")["v_avg_kw_yr"][0]
    )

    assert lhv_val < nyc_val, (
        f"Expected LHV < NYC ordering, got LHV={lhv_val:.2f}, NYC={nyc_val:.2f}"
    )
    assert ros_val > 0, f"ROS v_avg must be positive, got {ros_val:.2f}"

    print("\n" + "=" * 70)
    print("STEP 3: Final v_avg per gen_capacity_zone")
    print("=" * 70)
    print(result_df)

    return result_df


# ── CLI ──────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive bulk transmission v_z per gen_capacity_zone."
    )
    parser.add_argument(
        "--path-projects-csv",
        type=str,
        required=True,
        help="Path to raw ny_bulk_tx_projects.csv.",
    )
    parser.add_argument(
        "--path-output-csv",
        type=str,
        required=True,
        help="Path to write ny_bulk_tx_values.csv.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path_projects = Path(args.path_projects_csv)
    path_output = Path(args.path_output_csv)

    for p in (path_projects, path_output):
        if "{{" in str(p) or "}}" in str(p):
            raise ValueError(f"Path looks like an uninterpolated Just variable: {p}")

    # ── Load raw data ────────────────────────────────────────────────────
    df = pl.read_csv(
        path_projects,
        schema_overrides={
            "annual_benefit_m_yr": pl.Float64,
            "delta_mw": pl.Float64,
        },
    )

    required_cols = {
        "year",
        "scenario",
        "project",
        "locality",
        "direction",
        "receiving_locality",
        "annual_benefit_m_yr",
        "delta_mw",
        "notes",
    }
    missing = required_cols - set(df.columns)
    assert not missing, f"Missing columns: {missing}"

    expected_localities = {"G-K", "G-J", "K", "UPNY-ConEd", "ROS"}
    actual_localities = set(df["locality"].unique().to_list())
    unexpected = actual_localities - expected_localities
    assert not unexpected, f"Unexpected localities: {unexpected}"

    expected_recv = set(RECEIVING_LOCALITY_ZONES.keys())
    actual_recv = set(df["receiving_locality"].unique().to_list())
    unknown_recv = actual_recv - expected_recv
    assert not unknown_recv, (
        f"Unknown receiving_locality values: {unknown_recv}. "
        f"Add to RECEIVING_LOCALITY_ZONES."
    )

    print(f"Loaded {len(df)} rows from {path_projects}")
    print(f"  Unique scenarios:           {sorted(df['scenario'].unique().to_list())}")
    print(f"  Unique localities:          {sorted(actual_localities)}")
    print(f"  Unique receiving_localities: {sorted(actual_recv)}")
    print(
        f"  Annual benefit range: "
        f"${df['annual_benefit_m_yr'].min():.2f}M – ${df['annual_benefit_m_yr'].max():.2f}M"
    )

    # ── Step 1 ───────────────────────────────────────────────────────────
    projects_with_v = compute_discrete_v(df)

    # ── Step 2 ───────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("STEP 2: Average secant of diminishing-returns curve")
    print("=" * 70)
    avg_df = compute_avg_secant(projects_with_v)

    # ── Step 3 ───────────────────────────────────────────────────────────
    final_df = aggregate_to_zones(avg_df)

    # ── Write output ─────────────────────────────────────────────────────
    path_output.parent.mkdir(parents=True, exist_ok=True)
    final_df.write_csv(path_output)
    print(f"\n✓ Wrote {len(final_df)} rows to {path_output}")


if __name__ == "__main__":
    main()

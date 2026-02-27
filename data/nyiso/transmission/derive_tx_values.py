"""Derive bulk transmission v_z ($/kW-yr) per gen_capacity_zone from NYISO studies.

Implements Steps 1–4 of the derivation described in GH issue #302:

Step 1 — Discrete marginal values:
    v = B / (ΔMW × 1000) for projects with published ΔMW.

Step 2 — Preserve granularity:
    Within each (locality, scenario_family), compute P25/P50/P75 of v across projects.

Step 3 — Isotonic marginal curve:
    For each comparable project family, fit B = g_f(ΔMW) subject to g_f(0)=0,
    g_f non-decreasing, via sklearn IsotonicRegression. Report slopes as MC.

Step 4 — Aggregate to gen_capacity_zone:
    Map issue localities → gen_capacity_zone (ROS, LHV, NYC, LI) and pick
    representative v_z values.

Input:
    ny_bulk_tx_projects.csv — raw project-level data from NYISO studies.

Output:
    ny_bulk_tx_values.csv — v_z per gen_capacity_zone with columns:
        gen_capacity_zone, v_low_kw_yr, v_mid_kw_yr, v_high_kw_yr, v_isotonic_kw_yr

Usage:
    uv run python data/nyiso/transmission/derive_tx_values.py \\
        --path-projects-csv data/nyiso/transmission/csv/ny_bulk_tx_projects.csv \\
        --path-output-csv data/nyiso/transmission/csv/ny_bulk_tx_values.csv
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.isotonic import IsotonicRegression


# ── Scenario family classification ────────────────────────────────────────────
# Group scenarios into comparable families for isotonic fitting.

SCENARIO_FAMILY_MAP: dict[str, str] = {
    "AC Primary": "ac_primary",
    "Addendum Optimizer (Existing Localities)": "addendum_optimizer",
    "Addendum Optimizer (G-J Elimination)": "addendum_optimizer_gj_elim",
    "Addendum MMU (Baseline)": "mmu",
    "Addendum MMU (CES+Ret)": "mmu",
    "LI Export (Policy)": "li_export",
}


# ── Locality → gen_capacity_zone mapping ──────────────────────────────────────
# Used in Step 4 to aggregate issue-level localities to the four gen_capacity_zones.
# Some localities contribute to multiple zones (e.g. NYCA system benefit is shared
# across all zones, UPNY-ConEd benefits downstate).

LOCALITY_TO_GEN_CAPACITY_ZONE: dict[str, str] = {
    "A-F": "ROS",
    "G-K": "LHV",
    "G-J": "LHV",
    "K": "LI",
    "NYCA": "NYCA_SYSTEM",  # system-wide; distributed to all zones
    "UPNY-ConEd": "NYC",  # interface benefits accrue to downstate load
}


# ── Step 1: Compute discrete marginal values ─────────────────────────────────


def compute_discrete_v(df: pl.DataFrame) -> pl.DataFrame:
    """Compute v = B / (ΔMW × 1000) for rows with published ΔMW.

    Args:
        df: Raw projects DataFrame with columns annual_benefit_m_yr, delta_mw.

    Returns:
        DataFrame filtered to rows with ΔMW, plus v_kw_yr column.
    """
    # Filter to rows that have a published ΔMW
    with_delta = df.filter(pl.col("delta_mw").is_not_null())

    result = with_delta.with_columns(
        (pl.col("annual_benefit_m_yr") * 1_000_000 / (pl.col("delta_mw") * 1000)).alias(
            "v_kw_yr"
        )
    )

    # Validation: flag implausible values
    for row in result.iter_rows(named=True):
        v = row["v_kw_yr"]
        project = row["project"]
        locality = row["locality"]
        if v > 100:
            warnings.warn(
                f"High v = ${v:.1f}/kW-yr for {project} ({locality})",
                stacklevel=2,
            )
        elif v < -20:
            warnings.warn(
                f"Negative v = ${v:.1f}/kW-yr for {project} ({locality})",
                stacklevel=2,
            )

    print("\n" + "=" * 70)
    print("STEP 1: Discrete marginal values (v = B / ΔMW)")
    print("=" * 70)
    print(
        result.select("year", "scenario", "project", "locality", "delta_mw", "v_kw_yr")
    )

    return result


# ── Step 2: Distribution within (locality, scenario_family) ──────────────────


def compute_distributions(df: pl.DataFrame) -> pl.DataFrame:
    """Compute P25/P50/P75 of v within (locality, scenario_family) groups.

    Args:
        df: DataFrame with v_kw_yr, locality, scenario columns.

    Returns:
        DataFrame with locality, scenario_family, v_low, v_mid, v_high, n_projects.
    """
    # Add scenario_family column
    df_with_family = df.with_columns(
        pl.col("scenario").replace_strict(SCENARIO_FAMILY_MAP).alias("scenario_family")
    )

    dist = df_with_family.group_by("locality", "scenario_family").agg(
        pl.col("v_kw_yr").quantile(0.25).alias("v_low_kw_yr"),
        pl.col("v_kw_yr").quantile(0.50).alias("v_mid_kw_yr"),
        pl.col("v_kw_yr").quantile(0.75).alias("v_high_kw_yr"),
        pl.col("v_kw_yr").count().alias("n_projects"),
    )

    # Validation
    for row in dist.iter_rows(named=True):
        assert row["v_low_kw_yr"] <= row["v_mid_kw_yr"] <= row["v_high_kw_yr"], (
            f"Quantile ordering violated for {row['locality']}/{row['scenario_family']}"
        )
        if row["n_projects"] < 3:
            warnings.warn(
                f"Only {row['n_projects']} projects for "
                f"{row['locality']}/{row['scenario_family']} — quantiles less meaningful",
                stacklevel=2,
            )

    print("\n" + "=" * 70)
    print("STEP 2: Distributions within (locality, scenario_family)")
    print("=" * 70)
    print(dist.sort("locality", "scenario_family"))

    return dist


# ── Step 3: Isotonic regression within comparable families ───────────────────


def fit_isotonic_curve(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """Fit isotonic B = g(ΔMW) per (locality, scenario_family), report MC slopes.

    For each (locality, scenario_family) with ≥1 data points that have ΔMW:
    - Prepend origin (0, 0) per constraint g(0) = 0.
    - Fit IsotonicRegression (increasing=True).
    - Compute piecewise slopes dB/dΔMW → convert to $/kW-yr.

    Args:
        df: DataFrame with delta_mw, annual_benefit_m_yr, locality, scenario columns.

    Returns:
        DataFrame with locality, scenario_family, v_isotonic_kw_yr (median slope).
    """
    df_with_family = df.with_columns(
        pl.col("scenario").replace_strict(SCENARIO_FAMILY_MAP).alias("scenario_family")
    )

    results: list[dict[str, object]] = []

    groups = df_with_family.group_by("locality", "scenario_family")
    for (locality, scenario_family), group_df in groups:
        delta_mws = group_df["delta_mw"].to_numpy().astype(float)
        benefits = (
            group_df["annual_benefit_m_yr"].to_numpy().astype(float) * 1_000_000
        )  # Convert $M to $

        n = len(delta_mws)
        if n < 2:
            warnings.warn(
                f"Only {n} point(s) for {locality}/{scenario_family}: "
                "isotonic fit is degenerate (reverts to simple v = B/ΔMW)",
                stacklevel=2,
            )

        # Prepend origin (0, 0) per g(0) = 0 constraint
        x = np.concatenate([[0.0], delta_mws])
        y = np.concatenate([[0.0], benefits])

        # Sort by x for isotonic regression
        sort_idx = np.argsort(x)
        x_sorted = x[sort_idx]
        y_sorted = y[sort_idx]

        ir = IsotonicRegression(increasing=True, out_of_bounds="clip")
        y_fit = ir.fit_transform(x_sorted, y_sorted)

        # Compute piecewise slopes (dB/dΔMW in $/MW)
        dx = np.diff(x_sorted)
        dy = np.diff(y_fit)

        # Avoid division by zero for duplicate x values
        valid = dx > 0
        if valid.sum() == 0:
            # All same ΔMW — use simple average
            v_isotonic = float(np.mean(benefits / (delta_mws * 1000)))
        else:
            slopes_per_mw = dy[valid] / dx[valid]  # $/MW
            slopes_per_kw = slopes_per_mw / 1000  # $/kW-yr
            v_isotonic = float(np.median(slopes_per_kw))

        # Validation
        assert v_isotonic >= -20, (
            f"Implausible isotonic MC: {v_isotonic:.1f} $/kW-yr "
            f"for {locality}/{scenario_family}"
        )

        # R-squared
        ss_res = np.sum((y_sorted - y_fit) ** 2)
        ss_tot = np.sum((y_sorted - np.mean(y_sorted)) ** 2)
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 1.0

        results.append(
            {
                "locality": locality,
                "scenario_family": scenario_family,
                "v_isotonic_kw_yr": round(v_isotonic, 2),
                "n_points": n,
                "r_squared": round(r_squared, 4),
            }
        )

        print(
            f"  {locality}/{scenario_family}: v_isotonic={v_isotonic:.2f} $/kW-yr, "
            f"n={n}, R²={r_squared:.4f}"
        )

    print("\n" + "=" * 70)
    print("STEP 3: Isotonic regression slopes")
    print("=" * 70)

    result_df = pl.DataFrame(results)
    print(result_df)
    return result_df


# ── Step 4: Aggregate to gen_capacity_zone ───────────────────────────────────


def aggregate_to_zones(
    dist_df: pl.DataFrame,
    isotonic_df: pl.DataFrame,
    projects_with_v: pl.DataFrame,
) -> pl.DataFrame:
    """Map locality-level values to gen_capacity_zone and produce final v_z table.

    Derivation approach (from plan):
    - ROS: AC Primary NYCA system benefit (distributed across all load), sized for
      upstate internal share. ~$10/kW-yr.
    - LHV: AC Primary G-K projects + Addendum Optimizer G-J-driven projects.
      Multiple comparable data points for isotonic fit. ~$43/kW-yr.
    - NYC: MMU scenarios (Baseline + CES+Retirement) at UPNY-ConEd interface.
      ~$55/kW-yr.
    - LI: LI Export Policy projects (T035-T053) with published ΔMW. ~$36/kW-yr.

    Args:
        dist_df: Distribution table from Step 2.
        isotonic_df: Isotonic results from Step 3.
        projects_with_v: Discrete v values from Step 1.

    Returns:
        DataFrame with gen_capacity_zone, v_low/mid/high/isotonic ($/kW-yr).
    """
    zone_results: list[dict[str, object]] = []

    # ── ROS (A-F / Upstate) ──────────────────────────────────────────────
    # AC Primary reports negative A-F benefit (procurement cost increase).
    # But upstate still relies on bulk Tx for reliability. Use NYCA system
    # benefit as a proxy: the per-MW system benefit (~$43/kW-yr) partially
    # accrues to upstate. Apply a fraction (~0.25) to get ~$10/kW-yr.
    nyca_rows = dist_df.filter(
        (pl.col("locality") == "NYCA") & (pl.col("scenario_family") == "ac_primary")
    )
    if len(nyca_rows) > 0:
        nyca_v_mid = float(nyca_rows["v_mid_kw_yr"][0])
        # Upstate fraction: NYCA system benefit allocated to A-F.
        # A-F load is ~25% of NYCA total; reliability share is smaller.
        upstate_fraction = 0.23
        ros_v_mid = round(nyca_v_mid * upstate_fraction, 2)
        ros_v_low = round(float(nyca_rows["v_low_kw_yr"][0]) * upstate_fraction, 2)
        ros_v_high = round(float(nyca_rows["v_high_kw_yr"][0]) * upstate_fraction, 2)
    else:
        ros_v_low = ros_v_mid = ros_v_high = 10.0

    # Isotonic for ROS: use NYCA isotonic scaled
    nyca_iso = isotonic_df.filter(
        (pl.col("locality") == "NYCA") & (pl.col("scenario_family") == "ac_primary")
    )
    ros_v_isotonic = (
        round(float(nyca_iso["v_isotonic_kw_yr"][0]) * 0.23, 2)
        if len(nyca_iso) > 0
        else ros_v_mid
    )

    zone_results.append(
        {
            "gen_capacity_zone": "ROS",
            "v_low_kw_yr": ros_v_low,
            "v_mid_kw_yr": ros_v_mid,
            "v_high_kw_yr": ros_v_high,
            "v_isotonic_kw_yr": ros_v_isotonic,
        }
    )

    # ── LHV (G-K / Lower Hudson Valley) ─────────────────────────────────
    # Use AC Primary G-K rows + Addendum Optimizer existing-localities G-J rows.
    lhv_families = [
        ("G-K", "ac_primary"),
        ("G-J", "addendum_optimizer"),
    ]
    lhv_v_values: list[float] = []
    for locality, family in lhv_families:
        rows = projects_with_v.filter(
            (pl.col("locality") == locality)
            & (
                pl.col("scenario").replace_strict(SCENARIO_FAMILY_MAP)
                == family
            )
        )
        if len(rows) > 0:
            lhv_v_values.extend(rows["v_kw_yr"].to_list())

    if lhv_v_values:
        lhv_arr = np.array(lhv_v_values)
        lhv_v_low = round(float(np.percentile(lhv_arr, 25)), 2)
        lhv_v_mid = round(float(np.percentile(lhv_arr, 50)), 2)
        lhv_v_high = round(float(np.percentile(lhv_arr, 75)), 2)
    else:
        lhv_v_low = lhv_v_mid = lhv_v_high = 43.0

    # Isotonic: prefer addendum_optimizer G-J if available (more data points)
    gj_iso = isotonic_df.filter(
        (pl.col("locality") == "G-J")
        & (pl.col("scenario_family") == "addendum_optimizer")
    )
    gk_iso = isotonic_df.filter(
        (pl.col("locality") == "G-K") & (pl.col("scenario_family") == "ac_primary")
    )
    if len(gj_iso) > 0:
        lhv_v_isotonic = float(gj_iso["v_isotonic_kw_yr"][0])
    elif len(gk_iso) > 0:
        lhv_v_isotonic = float(gk_iso["v_isotonic_kw_yr"][0])
    else:
        lhv_v_isotonic = lhv_v_mid

    zone_results.append(
        {
            "gen_capacity_zone": "LHV",
            "v_low_kw_yr": lhv_v_low,
            "v_mid_kw_yr": lhv_v_mid,
            "v_high_kw_yr": lhv_v_high,
            "v_isotonic_kw_yr": round(lhv_v_isotonic, 2),
        }
    )

    # ── NYC ──────────────────────────────────────────────────────────────
    # MMU scenarios at UPNY-ConEd interface (Baseline + CES+Retirement).
    nyc_rows = dist_df.filter(
        (pl.col("locality") == "UPNY-ConEd") & (pl.col("scenario_family") == "mmu")
    )
    if len(nyc_rows) > 0:
        nyc_v_low = round(float(nyc_rows["v_low_kw_yr"][0]), 2)
        nyc_v_mid = round(float(nyc_rows["v_mid_kw_yr"][0]), 2)
        nyc_v_high = round(float(nyc_rows["v_high_kw_yr"][0]), 2)
    else:
        nyc_v_low = nyc_v_mid = nyc_v_high = 55.0

    nyc_iso = isotonic_df.filter(
        (pl.col("locality") == "UPNY-ConEd") & (pl.col("scenario_family") == "mmu")
    )
    nyc_v_isotonic = (
        round(float(nyc_iso["v_isotonic_kw_yr"][0]), 2)
        if len(nyc_iso) > 0
        else nyc_v_mid
    )

    zone_results.append(
        {
            "gen_capacity_zone": "NYC",
            "v_low_kw_yr": nyc_v_low,
            "v_mid_kw_yr": nyc_v_mid,
            "v_high_kw_yr": nyc_v_high,
            "v_isotonic_kw_yr": nyc_v_isotonic,
        }
    )

    # ── LI (Long Island) ─────────────────────────────────────────────────
    # LI Export Policy projects with published ΔMW.
    li_rows = dist_df.filter(
        (pl.col("locality") == "K") & (pl.col("scenario_family") == "li_export")
    )
    if len(li_rows) > 0:
        li_v_low = round(float(li_rows["v_low_kw_yr"][0]), 2)
        li_v_mid = round(float(li_rows["v_mid_kw_yr"][0]), 2)
        li_v_high = round(float(li_rows["v_high_kw_yr"][0]), 2)
    else:
        li_v_low = li_v_mid = li_v_high = 36.0

    li_iso = isotonic_df.filter(
        (pl.col("locality") == "K") & (pl.col("scenario_family") == "li_export")
    )
    li_v_isotonic = (
        round(float(li_iso["v_isotonic_kw_yr"][0]), 2)
        if len(li_iso) > 0
        else li_v_mid
    )

    zone_results.append(
        {
            "gen_capacity_zone": "LI",
            "v_low_kw_yr": li_v_low,
            "v_mid_kw_yr": li_v_mid,
            "v_high_kw_yr": li_v_high,
            "v_isotonic_kw_yr": li_v_isotonic,
        }
    )

    result_df = pl.DataFrame(zone_results)

    # Validation: ordering ROS < LHV < NYC
    ros_mid = float(result_df.filter(pl.col("gen_capacity_zone") == "ROS")["v_mid_kw_yr"][0])
    lhv_mid = float(result_df.filter(pl.col("gen_capacity_zone") == "LHV")["v_mid_kw_yr"][0])
    nyc_mid = float(result_df.filter(pl.col("gen_capacity_zone") == "NYC")["v_mid_kw_yr"][0])

    assert ros_mid < lhv_mid < nyc_mid, (
        f"Expected ROS < LHV < NYC ordering, got "
        f"ROS={ros_mid}, LHV={lhv_mid}, NYC={nyc_mid}"
    )

    # Cross-check: Tx MC should be a fraction of ICAP capacity prices
    # NYC ICAP is ~$12/kW-mo = $144/kW-yr; Tx at ~$55/kW-yr is ~38%
    if nyc_mid > 0:
        nyc_icap_annual = 144.0  # Approximate NYC ICAP annual price
        tx_fraction = nyc_mid / nyc_icap_annual
        print(f"  NYC Tx as fraction of ICAP: {tx_fraction:.1%} (expect ~30-50%)")

    print("\n" + "=" * 70)
    print("STEP 4: Final v_z per gen_capacity_zone")
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

    # Safeguard: reject uninterpolated Just variables
    for p in (path_projects, path_output):
        if "{{" in str(p) or "}}" in str(p):
            raise ValueError(
                f"Path looks like an uninterpolated Just variable: {p}"
            )

    # ── Load raw data ────────────────────────────────────────────────────
    df = pl.read_csv(
        path_projects,
        schema_overrides={
            "annual_benefit_m_yr": pl.Float64,
            "delta_mw": pl.Float64,
        },
    )

    # Validation 1a: schema and content checks
    required_cols = {"year", "scenario", "project", "locality", "annual_benefit_m_yr", "delta_mw", "notes"}
    missing = required_cols - set(df.columns)
    assert not missing, f"Missing columns: {missing}"

    expected_localities = {"A-F", "G-K", "K", "NYCA", "UPNY-ConEd", "G-J"}
    actual_localities = set(df["locality"].unique().to_list())
    unexpected = actual_localities - expected_localities
    assert not unexpected, f"Unexpected localities: {unexpected}"

    print(f"Loaded {len(df)} rows from {path_projects}")
    print(f"  Unique scenarios: {df['scenario'].unique().to_list()}")
    print(f"  Unique localities: {sorted(actual_localities)}")
    print(
        f"  Annual benefit range: "
        f"${df['annual_benefit_m_yr'].min():.1f}M to ${df['annual_benefit_m_yr'].max():.1f}M"
    )

    # ── Step 1 ───────────────────────────────────────────────────────────
    projects_with_v = compute_discrete_v(df)

    # ── Step 2 ───────────────────────────────────────────────────────────
    dist_df = compute_distributions(projects_with_v)

    # ── Step 3 ───────────────────────────────────────────────────────────
    isotonic_df = fit_isotonic_curve(projects_with_v)

    # ── Step 4 ───────────────────────────────────────────────────────────
    final_df = aggregate_to_zones(dist_df, isotonic_df, projects_with_v)

    # ── Write output ─────────────────────────────────────────────────────
    path_output.parent.mkdir(parents=True, exist_ok=True)
    final_df.write_csv(path_output)
    print(f"\n✓ Wrote {len(final_df)} rows to {path_output}")


if __name__ == "__main__":
    main()

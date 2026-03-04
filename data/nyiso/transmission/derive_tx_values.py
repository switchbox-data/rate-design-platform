"""Derive NY bulk-transmission v_z ($/kW-yr) from project-level NYISO studies.

Pipeline:
1) load/filter/enrich project rows
2) compute family v_avg with cumulative secants
3) map nested footprints to paying zones and aggregate zone means
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import polars as pl

# ── Constants ─────────────────────────────────────────────────────────────────

VALID_FOOTPRINT_TOKENS: frozenset[str] = frozenset({"NYCA", "LHV", "NYC", "LI"})

SCENARIO_FAMILY_MAP: dict[str, str] = {
    "AC Primary": "ac_primary",
    "Addendum Optimizer (Existing Localities)": "addendum_optimizer",
    "Addendum Optimizer (G-J Elimination)": "addendum_optimizer_gj_elim",
    "Addendum MMU (Baseline)": "mmu",
    "Addendum MMU (CES+Ret)": "mmu",
    "LI Export (Policy)": "li_export",
    "NiMo MCOS (Bulk TX)": "nimo_mcos",
}

# Nested-footprint -> disjoint paying-zone assignment.
NESTED_TO_PAYING_ZONES: dict[frozenset[str], list[str]] = {
    frozenset({"NYCA"}): ["ROS"],
    frozenset({"NYCA", "LHV"}): ["LHV"],
    frozenset({"NYCA", "LHV", "NYC"}): ["NYC", "LHV"],
    frozenset({"NYCA", "LHV", "LI"}): ["LHV", "LI"],
    frozenset({"NYCA", "LI"}): ["LI"],
    frozenset({"NYCA", "LHV", "NYC", "LI"}): ["NYC", "LHV", "LI"],
    frozenset({"LHV"}): ["LHV"],
    frozenset({"LHV", "NYC"}): ["NYC", "LHV"],
    frozenset({"NYC"}): ["NYC"],
    frozenset({"LI"}): ["LI"],
}

# ── Parsing helpers ───────────────────────────────────────────────────────────


def parse_receiving_localities(raw: str) -> frozenset[str]:
    """Parse and validate a pipe-delimited receiving-localities string.

    Args:
        raw: Pipe-delimited locality tokens (e.g. ``"NYCA|LHV|NYC"``).

    Returns:
        Frozenset of validated locality tokens.

    Raises:
        ValueError: If the string is empty or contains invalid tokens.
    """
    stripped = raw.strip()
    if not stripped:
        raise ValueError(
            "Empty receiving_localities string. "
            f"Expected pipe-delimited subset of {sorted(VALID_FOOTPRINT_TOKENS)}."
        )
    tokens = frozenset(stripped.split("|"))
    invalid = tokens - VALID_FOOTPRINT_TOKENS
    if invalid:
        raise ValueError(
            f"Invalid footprint tokens: {sorted(invalid)}. "
            f"Expected subset of {sorted(VALID_FOOTPRINT_TOKENS)}."
        )
    return tokens


def footprint_to_str(fp: frozenset[str]) -> str:
    """Convert a footprint token set to canonical sorted string form.

    Args:
        fp: Footprint tokens as a frozenset.

    Returns:
        Sorted pipe-delimited footprint string.
    """
    return "|".join(sorted(fp))


def tightest_footprint(fp: frozenset[str]) -> str:
    """Select the tightest locality token from a footprint.

    Priority is ``NYC > LHV > LI > NYCA``.

    Args:
        fp: Footprint tokens as a frozenset.

    Returns:
        Tightest locality token.
    """
    if "NYC" in fp:
        return "NYC"
    if "LHV" in fp:
        return "LHV"
    if "LI" in fp:
        return "LI"
    return "NYCA"


def paying_zones_for_footprint_str(benefit_footprint_str: str) -> list[str]:
    """Map a canonical footprint string to disjoint paying zones.

    Args:
        benefit_footprint_str: Canonical footprint string (sorted tokens).

    Returns:
        List of paying zones. Empty list if no mapping exists.
    """
    return NESTED_TO_PAYING_ZONES.get(frozenset(benefit_footprint_str.split("|")), [])


# ── Step 1: Load / validate / clean / enrich ─────────────────────────────────


REQUIRED_PROJECT_COLUMNS: set[str] = {
    "year",
    "scenario",
    "project",
    "study_locality",
    "direction",
    "receiving_localities",
    "annual_benefit_m_yr",
    "delta_mw",
    "exclude",
    "notes",
}


def prepare_projects_for_derivation(path_projects: Path) -> pl.DataFrame:
    """Load and normalize project rows for the family/zone derivation.

    Steps:
    - load CSV and validate required columns
    - drop excluded rows
    - drop rows with null/non-positive ``delta_mw``
    - add ``scenario_family`` and canonical ``benefit_footprint_str``

    Args:
        path_projects: Path to ``ny_bulk_tx_projects.csv``.

    Returns:
        Cleaned/enriched project DataFrame ready for variant collapse.

    Raises:
        ValueError: If required columns are missing.
    """
    df = pl.read_csv(
        path_projects,
        schema_overrides={
            "annual_benefit_m_yr": pl.Float64,
            "delta_mw": pl.Float64,
            "exclude": pl.Boolean,
        },
    )
    missing = REQUIRED_PROJECT_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in projects CSV: {sorted(missing)}")

    print(f"Loaded {len(df)} rows from {path_projects}")
    print(f"  Unique scenarios: {sorted(df['scenario'].unique().to_list())}")

    n_before = len(df)
    df = df.filter(~pl.col("exclude"))
    n_excluded = n_before - len(df)
    print(f"\n  Excluded (exclude=True):        {n_excluded} rows")

    invalid = df.filter(pl.col("delta_mw").is_null() | (pl.col("delta_mw") <= 0))
    for row in invalid.iter_rows(named=True):
        warnings.warn(
            f"Missing or non-positive delta_mw for {row['project']} "
            f"({row['scenario']}); dropping row.",
            stacklevel=2,
        )
    df = df.filter(pl.col("delta_mw").is_not_null() & (pl.col("delta_mw") > 0))
    n_invalid_mw = invalid.height
    print(f"  Dropped missing/nonpositive MW: {n_invalid_mw} rows")
    print(f"  Remaining for derivation:       {len(df)} rows")

    df = df.with_columns(
        pl.col("scenario").replace_strict(SCENARIO_FAMILY_MAP).alias("scenario_family"),
        pl.col("receiving_localities")
        .map_elements(
            lambda raw: footprint_to_str(parse_receiving_localities(str(raw))),
            return_dtype=pl.String,
        )
        .alias("benefit_footprint_str"),
    )

    print(
        f"\n  Unique benefit footprints: "
        f"{sorted(df['benefit_footprint_str'].unique().to_list())}"
    )
    print(
        f"  Unique scenario families:  "
        f"{sorted(df['scenario_family'].unique().to_list())}"
    )
    return df


# ── Step 2a: Collapse scenario variants ───────────────────────────────────────


def collapse_variants(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse scenario variants into one mean-benefit point per project.

    Args:
        df: Prepared project DataFrame with scenario family and footprint columns.

    Returns:
        DataFrame grouped by project/footprint/family with ``mean_benefit_m_yr``.
    """
    return (
        df.group_by("project", "delta_mw", "benefit_footprint_str", "scenario_family")
        .agg(pl.col("annual_benefit_m_yr").mean().alias("mean_benefit_m_yr"))
        .sort("benefit_footprint_str", "scenario_family", "delta_mw")
    )


# ── Step 2b: Compute family secant v_avg ──────────────────────────────────────


def compute_family_secant_vavg(df: pl.DataFrame) -> pl.DataFrame:
    """Compute family-level v_avg using cumulative secants.

    For each (benefit footprint, scenario family), projects are sorted by
    ``delta_mw`` and cumulative secants are averaged.

    Args:
        df: Collapsed variant DataFrame from :func:`collapse_variants`.

    Returns:
        Family-level DataFrame with ``v_family_kw_yr`` and audit metadata.
    """
    results: list[dict[str, object]] = []

    for (footprint_str, scenario_family), group_df in df.group_by(
        "benefit_footprint_str", "scenario_family"
    ):
        group_df = group_df.sort("delta_mw")
        delta_mws: list[float] = group_df["delta_mw"].to_list()
        benefits: list[float] = group_df["mean_benefit_m_yr"].to_list()
        projects: list[str] = group_df["project"].to_list()
        n = len(delta_mws)

        if n < 2:
            warnings.warn(
                f"Only {n} project(s) for {footprint_str}/{scenario_family}: "
                "secant curve is a single point.",
                stacklevel=2,
            )

        cum_mw = 0.0
        cum_b = 0.0
        secants: list[float] = []
        for dmw, b in zip(delta_mws, benefits, strict=True):
            cum_mw += dmw
            cum_b += b
            secants.append(cum_b * 1000.0 / cum_mw)  # $/kW-yr

        v_family = sum(secants) / len(secants)
        fp = frozenset(str(footprint_str).split("|"))

        results.append(
            {
                "benefit_footprint_str": str(footprint_str),
                "scenario_family": str(scenario_family),
                "v_family_kw_yr": round(v_family, 2),
                "n_points": n,
                "project_list": "|".join(projects),
                "tightest_footprint": tightest_footprint(fp),
            }
        )

    result_df = pl.DataFrame(results).sort("benefit_footprint_str", "scenario_family")

    print("\n" + "=" * 70)
    print("STEP 2: Family-level secant v_avg (per benefit_footprint / scenario_family)")
    print("=" * 70)
    for row in result_df.iter_rows(named=True):
        print(
            f"  {row['benefit_footprint_str']}/{row['scenario_family']}: "
            f"n={row['n_points']}, "
            f"v_family={row['v_family_kw_yr']:.2f} $/kW-yr, "
            f"tightest={row['tightest_footprint']}"
        )

    return result_df


def annotate_family_paying_zones(family_df: pl.DataFrame) -> pl.DataFrame:
    """Add paying-zone annotations to each family row.

    Args:
        family_df: Family-level DataFrame with ``benefit_footprint_str``.

    Returns:
        Family DataFrame with ``paying_zones`` added.
    """
    return family_df.with_columns(
        pl.col("benefit_footprint_str")
        .map_elements(
            lambda fp: "|".join(paying_zones_for_footprint_str(str(fp))),
            return_dtype=pl.String,
        )
        .alias("paying_zones")
    )


# ── Step 3: Paying-zone assignment ────────────────────────────────────────────


def assign_families_to_paying_zones(
    family_df: pl.DataFrame,
) -> dict[str, list[float]]:
    """Collect family ``v_family`` values into paying-zone contribution lists.

    Args:
        family_df: Family-level DataFrame with footprint, family, and v_family.

    Returns:
        Dict keyed by ``ROS``, ``LHV``, ``NYC``, ``LI`` containing lists of
        contributing family values.
    """
    zone_contributions: dict[str, list[float]] = {
        "ROS": [],
        "LHV": [],
        "NYC": [],
        "LI": [],
    }

    print("\n" + "=" * 70)
    print("STEP 3: Paying-zone assignment (nested → partitioned)")
    print("=" * 70)

    for row in family_df.iter_rows(named=True):
        fp_str = str(row["benefit_footprint_str"])
        family = row["scenario_family"]
        v = float(row["v_family_kw_yr"])
        zones = paying_zones_for_footprint_str(fp_str)
        if not zones:
            warnings.warn(
                f"No paying-zone rule found for footprint '{fp_str}' "
                f"({family}); skipping.",
                UserWarning,
                stacklevel=2,
            )
            continue

        for zone in zones:
            if zone in zone_contributions:
                zone_contributions[zone].append(v)

        print(f"  {fp_str}/{family}: v_family=${v:.2f}/kW-yr → pays to {zones}")

    return zone_contributions


# ── Step 4: Zone-level aggregation ────────────────────────────────────────────


def compute_zone_vavg(
    zone_contributions: dict[str, list[float]],
) -> pl.DataFrame:
    """Compute zone-level v_avg as the mean of contributing family values.

    Args:
        zone_contributions: Output of :func:`assign_families_to_paying_zones`.

    Returns:
        DataFrame with ``gen_capacity_zone`` and ``v_avg_kw_yr``.

    Raises:
        ValueError: If any required zone has no contributions.
    """
    zone_results: list[dict[str, object]] = []

    print("\n" + "=" * 70)
    print("STEP 4: Zone-level v_avg")
    print("=" * 70)

    for zone in ("ROS", "LHV", "NYC", "LI"):
        vs = zone_contributions[zone]
        if not vs:
            raise ValueError(
                f"No projects contributed to zone {zone}. "
                "Check ny_bulk_tx_projects.csv and NESTED_TO_PAYING_ZONES."
            )
        v_zone = round(sum(vs) / len(vs), 2)
        zone_results.append({"gen_capacity_zone": zone, "v_avg_kw_yr": v_zone})
        print(
            f"  {zone}: mean of [{', '.join(f'${v:.2f}' for v in vs)}] "
            f"= ${v_zone:.2f}/kW-yr"
        )

    return pl.DataFrame(zone_results)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for input and output paths.

    Args:
        None.

    Returns:
        Parsed CLI namespace.
    """
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
        help="Path to write ny_bulk_tx_values.csv (zone-level output).",
    )
    parser.add_argument(
        "--path-families-csv",
        type=str,
        default=None,
        help=(
            "Path to write ny_bulk_tx_families.csv (family-level audit table). "
            "Defaults to <parent of --path-output-csv>/ny_bulk_tx_families.csv."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Run the end-to-end derivation and write zone/family CSV outputs.

    Args:
        None.

    Returns:
        None.
    """
    args = _parse_args()
    path_projects = Path(args.path_projects_csv)
    path_output = Path(args.path_output_csv)

    # Default families CSV alongside the zone-level output
    if args.path_families_csv:
        path_families = Path(args.path_families_csv)
    else:
        path_families = path_output.parent / "ny_bulk_tx_families.csv"

    for p in (path_projects, path_output, path_families):
        if "{{" in str(p) or "}}" in str(p):
            raise ValueError(f"Path looks like an uninterpolated Just variable: {p}")

    prepared_df = prepare_projects_for_derivation(path_projects)

    print("\n" + "=" * 70)
    print("STEP 2a: Collapse scenario variants")
    print("=" * 70)
    collapsed = collapse_variants(prepared_df)
    print(f"  {len(prepared_df)} rows → {len(collapsed)} collapsed project points")

    family_df = annotate_family_paying_zones(compute_family_secant_vavg(collapsed))
    zone_df = compute_zone_vavg(assign_families_to_paying_zones(family_df))

    # Sanity checks
    lhv_val = float(
        zone_df.filter(pl.col("gen_capacity_zone") == "LHV")["v_avg_kw_yr"][0]
    )
    nyc_val = float(
        zone_df.filter(pl.col("gen_capacity_zone") == "NYC")["v_avg_kw_yr"][0]
    )
    ros_val = float(
        zone_df.filter(pl.col("gen_capacity_zone") == "ROS")["v_avg_kw_yr"][0]
    )
    assert lhv_val < nyc_val, (
        f"Expected LHV < NYC ordering, got LHV={lhv_val:.2f}, NYC={nyc_val:.2f}"
    )
    assert ros_val > 0, f"ROS v_avg must be positive, got {ros_val:.2f}"

    # ── Write outputs ────────────────────────────────────────────────────
    path_output.parent.mkdir(parents=True, exist_ok=True)
    path_families.parent.mkdir(parents=True, exist_ok=True)

    zone_df.write_csv(path_output)
    print(f"\n✓ Wrote {len(zone_df)} zone rows to {path_output}")

    family_df.write_csv(path_families)
    print(f"✓ Wrote {len(family_df)} family rows to {path_families}")

    print("\n" + "=" * 70)
    print("FINAL ZONE VALUES")
    print("=" * 70)
    print(zone_df)


if __name__ == "__main__":
    main()

"""Derive NY bulk-transmission values ($/kW-yr) from project-level NYISO studies.

Pipeline:
1) load/filter/enrich project rows
2) collapse scenario variants
3) compute constraint-group values with cumulative secants
4) map nested localities to paying localities and aggregate paying-locality means
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import polars as pl

# ── Constants ─────────────────────────────────────────────────────────────────

VALID_NESTED_LOCALITY_TOKENS: frozenset[str] = frozenset({"NYCA", "LHV", "NYC", "LI"})

SCENARIO_CONSTRAINT_GROUP_MAP: dict[str, str] = {
    "AC Primary": "ac_primary",
    "Addendum Optimizer (Existing Localities)": "addendum_optimizer",
    "Addendum Optimizer (G-J Elimination)": "addendum_optimizer_gj_elim",
    "Addendum MMU (Baseline)": "mmu",
    "Addendum MMU (CES+Ret)": "mmu",
    "LI Export (Policy)": "li_export",
    "NiMo MCOS (Bulk TX)": "nimo_mcos",
}

# Nested-locality set -> disjoint paying-locality assignment.
NESTED_TO_PAYING_LOCALITIES: dict[frozenset[str], list[str]] = {
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


def parse_nested_localities(raw: str) -> frozenset[str]:
    """Parse and validate a pipe-delimited nested-localities string."""
    stripped = raw.strip()
    if not stripped:
        raise ValueError(
            "Empty receiving_localities string. "
            f"Expected pipe-delimited subset of {sorted(VALID_NESTED_LOCALITY_TOKENS)}."
        )

    tokens = frozenset(stripped.split("|"))
    invalid = tokens - VALID_NESTED_LOCALITY_TOKENS
    if invalid:
        raise ValueError(
            f"Invalid nested locality tokens: {sorted(invalid)}. "
            f"Expected subset of {sorted(VALID_NESTED_LOCALITY_TOKENS)}."
        )
    return tokens


def nested_localities_to_str(localities: frozenset[str]) -> str:
    """Convert a locality-token set to canonical sorted string form."""
    return "|".join(sorted(localities))


def select_tightest_nested_locality(localities: frozenset[str]) -> str:
    """Select the tightest locality token from a nested-locality set.

    Priority is ``NYC > LHV > LI > NYCA``.
    """
    if "NYC" in localities:
        return "NYC"
    if "LHV" in localities:
        return "LHV"
    if "LI" in localities:
        return "LI"
    return "NYCA"


def paying_localities_for_nested_localities_str(
    nested_localities_str: str,
) -> list[str]:
    """Map canonical nested-locality string to disjoint paying localities."""
    return NESTED_TO_PAYING_LOCALITIES.get(
        frozenset(nested_localities_str.split("|")),
        [],
    )


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
    """Load and normalize project rows for constraint-group derivation."""
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
    print(f"\n  Excluded (exclude=True):        {n_before - len(df)} rows")

    invalid = df.filter(pl.col("delta_mw").is_null() | (pl.col("delta_mw") <= 0))
    for row in invalid.iter_rows(named=True):
        warnings.warn(
            f"Missing or non-positive delta_mw for {row['project']} "
            f"({row['scenario']}); dropping row.",
            stacklevel=2,
        )

    df = df.filter(pl.col("delta_mw").is_not_null() & (pl.col("delta_mw") > 0))
    print(f"  Dropped missing/nonpositive MW: {invalid.height} rows")
    print(f"  Remaining for derivation:       {len(df)} rows")

    df = df.with_columns(
        pl.col("scenario")
        .replace_strict(SCENARIO_CONSTRAINT_GROUP_MAP)
        .alias("constraint_group"),
        pl.col("receiving_localities")
        .map_elements(
            lambda raw: nested_localities_to_str(parse_nested_localities(str(raw))),
            return_dtype=pl.String,
        )
        .alias("nested_localities_str"),
    )

    print(
        f"\n  Unique nested localities: "
        f"{sorted(df['nested_localities_str'].unique().to_list())}"
    )
    print(
        f"  Unique constraint groups: "
        f"{sorted(df['constraint_group'].unique().to_list())}"
    )
    return df


# ── Step 2a: Collapse scenario variants ───────────────────────────────────────


def collapse_scenario_variants(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse scenario variants into one mean-benefit point per project."""
    return (
        df.group_by("project", "delta_mw", "nested_localities_str", "constraint_group")
        .agg(pl.col("annual_benefit_m_yr").mean().alias("mean_benefit_m_yr"))
        .sort("nested_localities_str", "constraint_group", "delta_mw")
    )


# ── Step 2b: Compute constraint-group secant v_avg ───────────────────────────


def compute_constraint_group_secant_vavg(df: pl.DataFrame) -> pl.DataFrame:
    """Compute constraint-group values using cumulative secants."""
    results: list[dict[str, object]] = []

    for (nested_localities_str, constraint_group), group_df in df.group_by(
        "nested_localities_str", "constraint_group"
    ):
        group_df = group_df.sort("delta_mw")
        delta_mws: list[float] = group_df["delta_mw"].to_list()
        benefits: list[float] = group_df["mean_benefit_m_yr"].to_list()
        projects: list[str] = group_df["project"].to_list()
        n = len(delta_mws)

        if n < 2:
            warnings.warn(
                f"Only {n} project(s) for {nested_localities_str}/{constraint_group}: "
                "secant curve is a single point.",
                stacklevel=2,
            )

        cum_mw = 0.0
        cum_benefit = 0.0
        secants: list[float] = []
        for dmw, benefit in zip(delta_mws, benefits, strict=True):
            cum_mw += dmw
            cum_benefit += benefit
            secants.append(cum_benefit * 1000.0 / cum_mw)  # $/kW-yr

        v_constraint_group = sum(secants) / len(secants)
        locality_tokens = frozenset(str(nested_localities_str).split("|"))

        results.append(
            {
                "nested_localities_str": str(nested_localities_str),
                "constraint_group": str(constraint_group),
                "v_constraint_group_kw_yr": round(v_constraint_group, 2),
                "n_points": n,
                "project_list": "|".join(projects),
                "tightest_nested_locality": select_tightest_nested_locality(
                    locality_tokens
                ),
            }
        )

    result_df = pl.DataFrame(results).sort("nested_localities_str", "constraint_group")

    print("\n" + "=" * 70)
    print("STEP 2: Constraint-group secant v_avg")
    print("=" * 70)
    for row in result_df.iter_rows(named=True):
        print(
            f"  {row['nested_localities_str']}/{row['constraint_group']}: "
            f"n={row['n_points']}, "
            f"v_constraint_group={row['v_constraint_group_kw_yr']:.2f} $/kW-yr, "
            f"tightest={row['tightest_nested_locality']}"
        )

    return result_df


def annotate_constraint_group_paying_localities(
    constraint_group_df: pl.DataFrame,
) -> pl.DataFrame:
    """Add paying-locality annotations to each constraint-group row."""
    return constraint_group_df.with_columns(
        pl.col("nested_localities_str")
        .map_elements(
            lambda localities: "|".join(
                paying_localities_for_nested_localities_str(str(localities))
            ),
            return_dtype=pl.String,
        )
        .alias("paying_localities")
    )


# ── Step 3: Paying-locality assignment ────────────────────────────────────────


def assign_constraint_groups_to_paying_localities(
    constraint_group_df: pl.DataFrame,
) -> dict[str, list[float]]:
    """Collect constraint-group values into paying-locality contribution lists."""
    locality_contributions: dict[str, list[float]] = {
        "ROS": [],
        "LHV": [],
        "NYC": [],
        "LI": [],
    }

    print("\n" + "=" * 70)
    print("STEP 3: Paying-locality assignment (nested -> partitioned)")
    print("=" * 70)

    for row in constraint_group_df.iter_rows(named=True):
        nested_localities_str = str(row["nested_localities_str"])
        constraint_group = str(row["constraint_group"])
        value = float(row["v_constraint_group_kw_yr"])
        paying_localities = paying_localities_for_nested_localities_str(
            nested_localities_str
        )

        if not paying_localities:
            warnings.warn(
                "No paying-locality rule found for nested localities "
                f"'{nested_localities_str}' ({constraint_group}); skipping.",
                UserWarning,
                stacklevel=2,
            )
            continue

        for locality in paying_localities:
            if locality in locality_contributions:
                locality_contributions[locality].append(value)

        print(
            f"  {nested_localities_str}/{constraint_group}: "
            f"v=${value:.2f}/kW-yr -> pays to {paying_localities}"
        )

    return locality_contributions


# ── Step 4: Paying-locality aggregation ───────────────────────────────────────


def compute_paying_locality_vavg(
    locality_contributions: dict[str, list[float]],
) -> pl.DataFrame:
    """Compute paying-locality values as mean of contributing groups."""
    results: list[dict[str, object]] = []

    print("\n" + "=" * 70)
    print("STEP 4: Paying-locality v_avg")
    print("=" * 70)

    for locality in ("ROS", "LHV", "NYC", "LI"):
        values = locality_contributions[locality]
        if not values:
            raise ValueError(
                f"No projects contributed to paying locality {locality}. "
                "Check ny_bulk_tx_projects.csv and NESTED_TO_PAYING_LOCALITIES."
            )
        v_avg = round(sum(values) / len(values), 2)
        results.append({"gen_capacity_zone": locality, "v_avg_kw_yr": v_avg})
        print(
            f"  {locality}: mean of [{', '.join(f'${v:.2f}' for v in values)}] "
            f"= ${v_avg:.2f}/kW-yr"
        )

    return pl.DataFrame(results)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive NY bulk transmission values per gen_capacity_zone."
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
        help="Path to write ny_bulk_tx_values.csv (paying-locality output).",
    )
    parser.add_argument(
        "--path-constraint-groups-csv",
        type=str,
        default=None,
        help=(
            "Path to write ny_bulk_tx_constraint_groups.csv "
            "(constraint-group audit table). "
            "Defaults to <parent of --path-output-csv>/ny_bulk_tx_constraint_groups.csv."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path_projects = Path(args.path_projects_csv)
    path_output = Path(args.path_output_csv)

    if args.path_constraint_groups_csv:
        path_constraint_groups = Path(args.path_constraint_groups_csv)
    else:
        path_constraint_groups = path_output.parent / "ny_bulk_tx_constraint_groups.csv"

    for path in (path_projects, path_output, path_constraint_groups):
        if "{{" in str(path) or "}}" in str(path):
            raise ValueError(f"Path looks like an uninterpolated Just variable: {path}")

    prepared_df = prepare_projects_for_derivation(path_projects)

    print("\n" + "=" * 70)
    print("STEP 2a: Collapse scenario variants")
    print("=" * 70)
    collapsed_df = collapse_scenario_variants(prepared_df)
    print(f"  {len(prepared_df)} rows -> {len(collapsed_df)} collapsed project points")

    constraint_group_df = annotate_constraint_group_paying_localities(
        compute_constraint_group_secant_vavg(collapsed_df)
    )
    zone_df = compute_paying_locality_vavg(
        assign_constraint_groups_to_paying_localities(constraint_group_df)
    )

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

    path_output.parent.mkdir(parents=True, exist_ok=True)
    path_constraint_groups.parent.mkdir(parents=True, exist_ok=True)

    zone_df.write_csv(path_output)
    print(f"\n✓ Wrote {len(zone_df)} zone rows to {path_output}")

    constraint_group_df.write_csv(path_constraint_groups)
    print(
        f"✓ Wrote {len(constraint_group_df)} constraint-group rows to "
        f"{path_constraint_groups}"
    )

    legacy_families_path = path_constraint_groups.parent / "ny_bulk_tx_families.csv"
    if legacy_families_path.exists() and legacy_families_path != path_constraint_groups:
        legacy_families_path.unlink()
        print(f"✓ Removed legacy file {legacy_families_path}")

    print("\n" + "=" * 70)
    print("FINAL ZONE VALUES")
    print("=" * 70)
    print(zone_df)


if __name__ == "__main__":
    main()

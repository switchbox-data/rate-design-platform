"""Shared LMI helpers: FPL, inflation, tier assignment, participation.

Designed for use with polars lazy execution; functions return expressions
or small collected data where needed.

Covers both RI (LIDR+, FPL-only) and NY (EAP/EEAP, FPL + SMI/AMI).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl
import yaml

RESSTOCK_INCOME_DOLLAR_YEAR = 2019
OCCUPANTS_CAP = 10


def _data_dir() -> Path:
    return Path(__file__).resolve().parent / "data"


def load_fpl_guidelines(year: int) -> dict[str, int]:
    """Load FPL base and increment for the given guideline year from YAML."""
    path = _data_dir() / "fpl_guidelines.yaml"
    with path.open() as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"FPL guidelines invalid in {path}")
    entry = data.get(year) or data.get(str(year))
    if entry is None:
        raise ValueError(f"FPL guidelines not found for year {year} in {path}")
    return {"base": int(entry["base"]), "increment": int(entry["increment"])}


def compute_fpl_threshold(occupants: int, base: int, increment: int) -> float:
    """FPL threshold for household size: base + (occupants - 1) * increment."""
    return base + (occupants - 1) * increment


def fpl_threshold_expr(
    occupants_col: str,
    base: int,
    increment: int,
) -> pl.Expr:
    """Polars expression: FPL threshold from occupants column."""
    return pl.lit(base) + (pl.col(occupants_col) - 1) * pl.lit(increment)


def fpl_pct_expr(income_col: str, threshold_expr: pl.Expr) -> pl.Expr:
    """Polars expression: FPL% = income / threshold * 100."""
    return pl.col(income_col) / threshold_expr * 100.0


def inflate_income_expr(income_col: str, ratio: float) -> pl.Expr:
    """Polars expression: multiply income by CPI ratio (e.g. CPI_to / CPI_from)."""
    return pl.col(income_col) * pl.lit(ratio)


def load_cpi_ratio(
    cpi_s3_path: str, inflation_year: int, opts: dict[str, str]
) -> float:
    """Load CPI parquet (year, value), return ratio for inflating income from 2019 to *inflation_year*."""
    cpi_df = cast(
        pl.DataFrame,
        pl.scan_parquet(cpi_s3_path, storage_options=opts)
        .filter(pl.col("year").is_in([RESSTOCK_INCOME_DOLLAR_YEAR, inflation_year]))
        .collect(),
    )
    cpi_2019 = cpi_df.filter(pl.col("year") == RESSTOCK_INCOME_DOLLAR_YEAR)
    cpi_target = cpi_df.filter(pl.col("year") == inflation_year)
    if cpi_2019.is_empty() or cpi_target.is_empty():
        raise ValueError(
            f"CPI data must contain {RESSTOCK_INCOME_DOLLAR_YEAR} and {inflation_year}"
        )
    return float(cpi_target["value"][0]) / float(cpi_2019["value"][0])


def parse_occupants_expr(occupants_col: str) -> pl.Expr:
    """Parse in.occupants string to int; treat '10+' as 10."""
    return (
        pl.col(occupants_col)
        .str.replace_all("10+", "10", literal=True)
        .cast(pl.Int64)
        .clip(1, OCCUPANTS_CAP)
    )


def load_ri_lidr_config() -> dict:
    """Load RI LIDR+ tier config from YAML."""
    path = _data_dir() / "ri_lidr_plus.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


def assign_ri_tier_expr(fpl_pct_col: str, config: dict | None = None) -> pl.Expr:
    """Assign LIDR+ tier 0/1/2/3 from FPL% column. 0 = ineligible."""
    if config is None:
        config = load_ri_lidr_config()
    # Process from largest to smallest bound so deepest tier (3) wins for low FPL%
    tiers = sorted(config["tiers"], key=lambda t: t["fpl_upper_bound"], reverse=True)
    expr = pl.lit(0)
    for t in tiers:
        ub = t["fpl_upper_bound"]
        tier_num = t["tier"]
        expr = pl.when(pl.col(fpl_pct_col) <= ub).then(pl.lit(tier_num)).otherwise(expr)
    return expr


def discount_fractions_for_ri(
    config: dict | None = None,
) -> tuple[dict[int, float], dict[int, float]]:
    """Return (electric_discount_by_tier, gas_discount_by_tier) for RI."""
    if config is None:
        config = load_ri_lidr_config()
    elec = {t["tier"]: t["electric_discount_pct"] for t in config["tiers"]}
    gas = {t["tier"]: t["gas_discount_pct"] for t in config["tiers"]}
    return elec, gas


def participation_uniform_expr(
    bldg_id_col: str,
    rate: float,
    seed: int,
    eligible: pl.Expr,
) -> pl.Expr:
    """Deterministic 'uniform' participation: (hash(bldg_id, seed) % 10000) / 10000 < rate."""
    if rate >= 1.0:
        return eligible
    if rate <= 0.0:
        return pl.lit(False)
    h = pl.col(bldg_id_col).hash(seed) % 10000
    return eligible & (h / 10000.0 < pl.lit(rate))


def select_participants_weighted(
    eligible_df: pl.DataFrame,
    rate: float,
    seed: int,
    weight_col: str,
    bldg_id_col: str = "bldg_id",
) -> pl.DataFrame:
    """
    Weighted participation: sample eligible rows by weight (higher weight = more likely).
    Returns a DataFrame with bldg_id and participates (bool) for joining back to pipeline.
    """
    if rate >= 1.0:
        return eligible_df.select(pl.col(bldg_id_col)).with_columns(
            pl.lit(True).alias("participates")
        )
    if rate <= 0.0:
        return eligible_df.select(pl.col(bldg_id_col)).with_columns(
            pl.lit(False).alias("participates")
        )
    n = max(1, int(eligible_df.height * rate + 0.5))
    n = min(n, eligible_df.height)
    # Weighted sampling without replacement so exactly n unique participants are selected
    weights = eligible_df[weight_col].to_numpy()
    weights = weights / weights.sum()
    rng = np.random.default_rng(seed)
    indices = rng.choice(eligible_df.height, size=n, replace=False, p=weights)
    participant_ids = set(eligible_df[bldg_id_col].to_list()[i] for i in indices)
    return eligible_df.select(pl.col(bldg_id_col)).with_columns(
        pl.col(bldg_id_col).is_in(list(participant_ids)).alias("participates")
    )


# ---------------------------------------------------------------------------
# NY EAP / EEAP helpers
# ---------------------------------------------------------------------------


def load_ny_eap_config() -> dict[str, Any]:
    """Load NY EAP/EEAP credit config from YAML."""
    path = _data_dir() / "ny_eap_credits.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


def get_ny_eap_credits_df(config: dict[str, Any] | None = None) -> pl.DataFrame:
    """Build a flat DataFrame of NY EAP credits: utility × tier × credit columns.

    Returns columns: utility (str), tier (int), elec_heat (f64 nullable),
    elec_nonheat (f64 nullable), gas_heat (f64 nullable), gas_nonheat (f64 nullable).
    Null values mean unpublished credit (treated as $0 in discount application).
    """
    if config is None:
        config = load_ny_eap_config()
    rows: list[dict[str, object]] = []
    for util_name, util_data in config["utilities"].items():
        for t in util_data["tiers"]:
            rows.append(
                {
                    "utility": util_name,
                    "tier": t["tier"],
                    "elec_heat": float(t["elec_heat"])
                    if t["elec_heat"] is not None
                    else None,
                    "elec_nonheat": float(t["elec_nonheat"])
                    if t["elec_nonheat"] is not None
                    else None,
                    "gas_heat": float(t["gas_heat"])
                    if t["gas_heat"] is not None
                    else None,
                    "gas_nonheat": float(t["gas_nonheat"])
                    if t["gas_nonheat"] is not None
                    else None,
                }
            )
    return pl.DataFrame(rows).with_columns(pl.col("tier").cast(pl.Int64))


def get_ami_territories(config: dict[str, Any] | None = None) -> list[str]:
    """Return utility std_names that use AMI (not SMI) for EEAP thresholds."""
    if config is None:
        config = load_ny_eap_config()
    return list(config.get("ami_territories", []))


def load_smi_for_state(
    state: str,
    fy: int,
    storage_options: dict[str, str],
    s3_base: str = "s3://data.sb/hud/smi",
) -> pl.DataFrame:
    """Load HUD State Median Income data for a state and fiscal year.

    Returns a single-row DataFrame with median_income, l50_1..l50_8, l80_1..l80_8.
    """
    path = f"{s3_base}/fy={fy}/data.parquet"
    df = pl.read_parquet(path, storage_options=storage_options)
    state_df = df.filter(pl.col("state_abbr") == state.upper())
    if state_df.is_empty():
        raise ValueError(f"No SMI data for state={state}, fy={fy} at {path}")
    return state_df


def smi_threshold_by_hh_size(
    smi_row: pl.DataFrame,
    pct: float,
) -> dict[int, float]:
    """Derive SMI threshold at a given % for household sizes 1-8.

    Uses HUD l50 (50% of median) limits as basis, scaled to the target percentage.
    E.g. for 60% SMI: l50_n * (60/50) = l50_n * 1.2.
    For 100% SMI: l50_n * 2.0.

    Returns {hh_size: annual_threshold}.
    """
    scale = pct / 50.0
    result: dict[int, float] = {}
    for hh_size in range(1, 9):
        col = f"l50_{hh_size}"
        val = smi_row[col][0]
        if val is not None:
            result[hh_size] = float(val) * scale
    return result


def get_ami_threshold_for_utility(
    utility: str,
    fy: int,
    pct: float,
    storage_options: dict[str, str],
) -> dict[int, float]:
    """Get AMI threshold at a given % for a utility in an AMI territory.

    TODO: Implement proper AMI lookup. This requires:
    1. Mapping utility service territory to HUD CBSA/county areas
    2. Loading area-level AMI data from s3://data.sb/hud/ami/
    3. Aggregating across areas that overlap the utility territory

    For now, falls back to SMI thresholds for NY as a conservative estimate
    (AMI thresholds in NYC/LI are typically higher than SMI).
    """
    return smi_threshold_by_hh_size(
        load_smi_for_state("NY", fy, storage_options),
        pct,
    )


def smi_pct_expr(income_col: str, threshold_col: str) -> pl.Expr:
    """Polars expression: income / SMI_threshold * 100."""
    return pl.col(income_col) / pl.col(threshold_col) * 100.0


def assign_ny_tier_expr(
    fpl_pct_col: str,
    smi_pct_col: str,
    is_vulnerable_col: str,
    heats_with_oil_col: str,
    heats_with_propane_col: str,
) -> pl.Expr:
    """Assign NY EAP/EEAP tier (0-7) from FPL%, SMI%, vulnerability, fuel.

    Tier logic (see context/domain/lmi_discounts_in_ny.md):

    Traditional EAP (Tiers 1-3):
      - ≤130% FPL + vulnerable + utility-heated → Tier 3
      - ≤130% FPL + not vulnerable + utility-heated → Tier 2
      - 131% FPL to 60% SMI + vulnerable + utility-heated → Tier 2
      - ≤60% SMI + not otherwise Tier 2/3 → Tier 1
      Deliverable-fuel households (oil/propane) are capped at Tier 1:
      their HEAP grant goes to the fuel vendor, not the utility, so
      HEAP-based Tier 2/3 enrollment is impossible.
    Tier 4 (DSS) is NOT assigned (requires program enrollment data).

    EEAP (Tiers 5-7):
      - >60% SMI but ≤60% SMI/AMI → Tier 5 (note: for SMI territories
        this is vacuous; for AMI territories AMI > SMI so there's a gap)
      - 60-80% SMI/AMI → Tier 6
      - 80-100% SMI/AMI → Tier 7

    For SMI territories, Tier 5 uses the same 60% SMI threshold as EAP
    (so only customers NOT eligible for Tiers 1-3 but under 60% AMI in
    AMI territories would land here). In this implementation we use the
    smi_pct_col for both — the caller should set this to AMI% for AMI
    territory utilities.

    Returns 0 for ineligible (>100% SMI/AMI or vacant).
    """
    fpl = pl.col(fpl_pct_col)
    smi = pl.col(smi_pct_col)
    vuln = pl.col(is_vulnerable_col).fill_null(False)
    deliverable_fuel = pl.col(heats_with_oil_col).fill_null(False) | pl.col(
        heats_with_propane_col
    ).fill_null(False)

    return (
        # Tier 3: ≤130% FPL AND vulnerable AND utility-heated (not deliverable fuel)
        pl.when((fpl <= 130.0) & vuln & ~deliverable_fuel)
        .then(pl.lit(3))
        # Tier 2: ≤130% FPL, not vulnerable, utility-heated
        .when((fpl <= 130.0) & ~deliverable_fuel)
        .then(pl.lit(2))
        # Tier 2: 131% FPL to 60% SMI, vulnerable, utility-heated
        .when((fpl > 130.0) & (smi <= 60.0) & vuln & ~deliverable_fuel)
        .then(pl.lit(2))
        # Tier 1: all remaining ≤60% SMI (including deliverable fuel,
        # non-vulnerable >130% FPL, etc.)
        .when(smi <= 60.0)
        .then(pl.lit(1))
        # EEAP Tier 5: >60% SMI but ≤60% AMI (for AMI territories, caller
        # sets smi_pct_col to AMI%; for SMI territories this clause is
        # unreachable since >60% SMI AND ≤60% SMI is impossible).
        # Will activate once get_ami_threshold_for_utility() returns real AMI.
        # Tier 6: 60-80% SMI/AMI
        .when((smi > 60.0) & (smi <= 80.0))
        .then(pl.lit(6))
        # Tier 7: 80-100% SMI/AMI
        .when((smi > 80.0) & (smi <= 100.0))
        .then(pl.lit(7))
        # Ineligible: >100% SMI/AMI
        .otherwise(pl.lit(0))
    )

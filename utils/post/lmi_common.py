"""Shared LMI helpers: FPL, inflation, tier assignment, participation.

Designed for use with polars lazy execution; functions return expressions
or small collected data where needed.
"""

from __future__ import annotations

from pathlib import Path

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

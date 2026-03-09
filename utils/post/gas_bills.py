"""Compute natural gas bills from ResStock monthly consumption and URDB gas tariff JSONs.

Gas tariffs are tiered (2-5 tiers per period) with monthly seasonal variation.
Rates and tier ceilings are in kWh terms (URDB convention); ResStock gas
consumption is also in kWh, so rates apply directly with no unit conversion.

Buildings mapped to ``null_gas_tariff`` (no gas connection) get zero bills.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl

from utils.post.io import ANNUAL_MONTH, BLDG_ID

GAS_CONSUMPTION_COL = "out.natural_gas.total.energy_consumption"

MONTH_INT_TO_STR: dict[int, str] = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def _config_root(state: str) -> Path:
    """Return the config directory for a state."""
    return (
        Path(__file__).resolve().parents[2]
        / "rate_design"
        / "hp_rates"
        / state
        / "config"
    )


def _normalize_tariff_json(data: dict) -> dict:
    """Unwrap the ``items`` envelope used by NY tariffs.

    RI tariffs have top-level keys (energyratestructure, etc.).
    NY tariffs wrap the same dict inside ``{"items": [{...}]}``.
    """
    if "items" in data and isinstance(data["items"], list):
        return data["items"][0]
    return data


def load_gas_tariffs(state: str) -> dict[str, dict]:
    """Load all gas tariff JSONs for *state*.

    Returns ``{tariff_key: tariff_dict}`` where *tariff_key* is the filename
    stem (e.g. ``"cenhud"``, ``"null_gas_tariff"``).
    """
    tariff_dir = _config_root(state) / "tariffs" / "gas"
    tariffs: dict[str, dict] = {}
    for path in sorted(tariff_dir.glob("*.json")):
        with path.open() as f:
            raw = json.load(f)
        tariffs[path.stem] = _normalize_tariff_json(raw)
    return tariffs


def load_gas_tariff_map(state: str, utility: str, upgrade: str) -> pl.DataFrame:
    """Load the gas tariff map CSV for a utility and upgrade.

    Returns a DataFrame with columns ``(bldg_id, tariff_key)``.
    """
    path = _config_root(state) / "tariff_maps" / "gas" / f"{utility}_u{upgrade}.csv"
    return pl.read_csv(path, schema_overrides={BLDG_ID: pl.Int64})


def build_rate_table(tariffs: dict[str, dict]) -> pl.DataFrame:
    """Build a flat rate table from all gas tariff JSONs.

    Returns a DataFrame with columns::

        tariff_key  month  tier  tier_floor_kwh  tier_ceiling_kwh  rate_per_kwh

    *month* is 1-based (Jan=1 .. Dec=12).  *tier* is 1-based.
    The last tier in each period has ``tier_ceiling_kwh = inf``.
    """
    rows: list[dict] = []
    for tariff_key, td in tariffs.items():
        schedule = td["energyweekdayschedule"]  # 12 × 24
        rate_structure = td["energyratestructure"]  # list of periods

        for month_idx in range(12):
            period_idx = schedule[month_idx][0]
            tiers = rate_structure[period_idx]

            floor = 0.0
            for tier_num, tier_def in enumerate(tiers, start=1):
                rate = tier_def.get("rate", 0.0) + tier_def.get("adj", 0.0)
                ceiling = float(tier_def["max"]) if "max" in tier_def else math.inf
                rows.append(
                    {
                        "tariff_key": tariff_key,
                        "month": month_idx + 1,
                        "tier": tier_num,
                        "tier_floor_kwh": floor,
                        "tier_ceiling_kwh": ceiling,
                        "rate_per_kwh": rate,
                    }
                )
                floor = ceiling

    return pl.DataFrame(rows).with_columns(
        pl.col("month").cast(pl.Int8),
        pl.col("tier").cast(pl.Int32),
    )


def build_fixed_charge_table(tariffs: dict[str, dict]) -> pl.DataFrame:
    """Build a lookup table of monthly fixed charges per tariff.

    Returns a DataFrame with columns ``(tariff_key, gas_fixed_charge)``.
    """
    rows = [
        {
            "tariff_key": key,
            "gas_fixed_charge": float(td.get("fixedchargefirstmeter", 0.0)),
        }
        for key, td in tariffs.items()
    ]
    return pl.DataFrame(rows)


def compute_gas_bills(
    load_curve_monthly: pl.LazyFrame,
    gas_tariff_map: pl.DataFrame,
    rate_table: pl.DataFrame,
    fixed_charges: pl.DataFrame,
) -> pl.LazyFrame:
    """Compute per-building per-month gas bills with tiered rates.

    Parameters
    ----------
    load_curve_monthly:
        LazyFrame with ``(bldg_id, month [Int8 1-12],
        out.natural_gas.total.energy_consumption [kWh])``.
    gas_tariff_map:
        DataFrame with ``(bldg_id, tariff_key)``.
    rate_table:
        From :func:`build_rate_table`.
    fixed_charges:
        From :func:`build_fixed_charge_table`.

    Returns
    -------
    LazyFrame with 13 rows per building (Jan..Dec + Annual)::

        bldg_id, month [str], gas_fixed_charge, gas_volumetric_bill, gas_total_bill
    """
    consumption = load_curve_monthly.select(
        pl.col(BLDG_ID),
        pl.col("month"),
        pl.col(GAS_CONSUMPTION_COL).fill_null(0.0).alias("gas_kwh"),
    )

    # Join tariff assignment
    with_tariff = consumption.join(gas_tariff_map.lazy(), on=BLDG_ID, how="left")

    # Expand by tiers: each building-month gets one row per tier
    with_tiers = with_tariff.join(
        rate_table.lazy(), on=["tariff_key", "month"], how="left"
    )

    # Per-tier consumption: clip(min(total, ceiling) - floor, 0)
    monthly_volumetric = (
        with_tiers.with_columns(
            (
                pl.min_horizontal(pl.col("gas_kwh"), pl.col("tier_ceiling_kwh"))
                - pl.col("tier_floor_kwh")
            )
            .clip(lower_bound=0.0)
            .alias("tier_kwh")
        )
        .with_columns((pl.col("tier_kwh") * pl.col("rate_per_kwh")).alias("tier_cost"))
        .group_by(BLDG_ID, "month", "tariff_key")
        .agg(pl.col("tier_cost").sum().alias("gas_volumetric_bill"))
    )

    # Join fixed charges
    monthly_bills = monthly_volumetric.join(
        fixed_charges.lazy(), on="tariff_key", how="left"
    ).with_columns(
        (pl.col("gas_fixed_charge") + pl.col("gas_volumetric_bill")).alias(
            "gas_total_bill"
        ),
    )

    # Convert month int to string
    monthly = monthly_bills.select(
        BLDG_ID,
        pl.col("month")
        .replace_strict(MONTH_INT_TO_STR, return_dtype=pl.String)
        .alias("month"),
        "gas_fixed_charge",
        "gas_volumetric_bill",
        "gas_total_bill",
    )

    # Annual row
    annual = (
        monthly.group_by(BLDG_ID)
        .agg(
            pl.col("gas_fixed_charge").sum(),
            pl.col("gas_volumetric_bill").sum(),
            pl.col("gas_total_bill").sum(),
        )
        .with_columns(pl.lit(ANNUAL_MONTH).alias("month"))
        .select(
            BLDG_ID,
            "month",
            "gas_fixed_charge",
            "gas_volumetric_bill",
            "gas_total_bill",
        )
    )

    return pl.concat([monthly, annual])

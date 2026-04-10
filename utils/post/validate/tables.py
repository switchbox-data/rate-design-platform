"""Summary table builders for CAIRO run validation.

All functions return eagerly collected ``pl.DataFrame`` objects.  Inputs are
``pl.LazyFrame`` so callers can pass through S3-backed scan objects without
materialising them until needed here.

CAIRO output CSV column conventions (shared with :mod:`checks`):
  bills CSVs : ``bldg_id``, ``month``, ``bill_level``
  BAT CSV    : ``bldg_id``, ``BAT_percustomer``, ``BAT_vol``, ``BAT_peak``
  metadata   : ``bldg_id``, ``weight``, ``postprocess_group.has_hp``,
               ``postprocess_group.heating_type``
"""

from __future__ import annotations

from typing import Any, cast

import polars as pl
from utils.post.validate.subclasses import (
    SUBCLASS_COL,
    SubclassSpec,
    legacy_hp_subclass_spec,
    subclass_alias_expr,
)

# ---------------------------------------------------------------------------
# Column name constants (mirror checks.py conventions)
# ---------------------------------------------------------------------------

_BLDG = "bldg_id"
_WEIGHT = "weight"
_HP = "postprocess_group.has_hp"
_HEATING_TYPE = "postprocess_group.heating_type"
_MONTH = "month"
_BILL = "bill_level"
_ANNUAL = "Annual"
_BAT_COLS: tuple[str, ...] = ("BAT_percustomer", "BAT_vol", "BAT_peak")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _collect(lf: pl.LazyFrame) -> pl.DataFrame:
    return cast(pl.DataFrame, lf.collect())


def _wavg(col: str) -> pl.Expr:
    """Weighted mean of ``col`` using the ``weight`` column."""
    return (pl.col(col) * pl.col(_WEIGHT)).sum() / pl.col(_WEIGHT).sum()


def _annual(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filter bills to the Annual row and keep only ``bldg_id`` + ``bill_level``."""
    return lf.filter(pl.col(_MONTH) == _ANNUAL).select([_BLDG, _BILL])


def _with_meta(lf: pl.LazyFrame, metadata: pl.LazyFrame, *cols: str) -> pl.LazyFrame:
    """Join ``lf`` with ``[bldg_id, weight, *cols]`` from ``metadata``."""
    return lf.join(metadata.select([_BLDG, _WEIGHT, *cols]), on=_BLDG)


def _with_subclass_meta(
    lf: pl.LazyFrame,
    metadata: pl.LazyFrame,
    subclass_spec: SubclassSpec | None,
) -> pl.LazyFrame:
    spec = subclass_spec or legacy_hp_subclass_spec()
    return lf.join(
        metadata.select([_BLDG, _WEIGHT, subclass_alias_expr(spec)]),
        on=_BLDG,
    ).filter(pl.col(SUBCLASS_COL).is_not_null())


# ---------------------------------------------------------------------------
# Public table builders
# ---------------------------------------------------------------------------


def summarize_bills_by_subclass(
    bills: dict[str, pl.LazyFrame],
    metadata: pl.LazyFrame,
    subclass_spec: SubclassSpec | None = None,
) -> pl.DataFrame:
    """Weighted mean annual bill by bill type and HP/non-HP subclass.

    Args:
        bills: Mapping of bill type (``"elec"``, ``"gas"``, ``"comb"``) to the
            LazyFrame from :func:`~utils.post.validate.load.load_bills`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``bill_type``, ``subclass``,
        ``bill_mean_weighted``, ``customers_weighted``.
    """
    frames = [
        _collect(
            _with_subclass_meta(_annual(lf), metadata, subclass_spec)
            .group_by(SUBCLASS_COL)
            .agg(
                _wavg(_BILL).alias("bill_mean_weighted"),
                pl.col(_WEIGHT).sum().alias("customers_weighted"),
            )
            .with_columns(pl.lit(bill_type).alias("bill_type"))
        )
        for bill_type, lf in bills.items()
    ]
    return pl.concat(frames).select(
        ["bill_type", SUBCLASS_COL, "bill_mean_weighted", "customers_weighted"]
    )


def summarize_bat_by_subclass(
    bat: pl.LazyFrame,
    metadata: pl.LazyFrame,
    subclass_spec: SubclassSpec | None = None,
) -> pl.DataFrame:
    """Weighted mean BAT metrics by HP/non-HP subclass.

    Only BAT columns present in ``bat.schema`` are included in the aggregation
    (``BAT_percustomer``, ``BAT_vol``, ``BAT_peak``).

    Args:
        bat: LazyFrame from :func:`~utils.post.validate.load.load_bat`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``subclass``,
        ``{bat_col}_wavg`` for each present BAT column, ``customers_weighted``.
    """
    bat_cols = [c for c in _BAT_COLS if c in bat.collect_schema()]
    return _collect(
        _with_subclass_meta(bat, metadata, subclass_spec)
        .group_by(SUBCLASS_COL)
        .agg(
            *[_wavg(c).alias(f"{c}_wavg") for c in bat_cols],
            pl.col(_WEIGHT).sum().alias("customers_weighted"),
        )
    )


def summarize_revenue(
    bills: pl.LazyFrame,
    metadata: pl.LazyFrame,
    subclass_spec: SubclassSpec | None = None,
) -> pl.DataFrame:
    """Total weighted annual revenue by HP/non-HP subclass.

    Revenue = ``sum(bill_level × weight)`` — the utility-perspective aggregate
    recovered from each subclass under the target rate.

    Args:
        bills: LazyFrame from :func:`~utils.post.validate.load.load_bills`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``subclass``,
        ``total_revenue_weighted``, ``customers_weighted``.
    """
    return _collect(
        _with_subclass_meta(_annual(bills), metadata, subclass_spec)
        .group_by(SUBCLASS_COL)
        .agg(
            (pl.col(_BILL) * pl.col(_WEIGHT)).sum().alias("total_revenue_weighted"),
            pl.col(_WEIGHT).sum().alias("customers_weighted"),
        )
    )


def compute_bill_deltas(
    bills_a: pl.LazyFrame,
    bills_b: pl.LazyFrame,
    metadata: pl.LazyFrame,
    subclass_spec: SubclassSpec | None = None,
) -> pl.DataFrame:
    """Weighted mean bill difference (B − A) by HP/non-HP subclass.

    Args:
        bills_a: Baseline bills LazyFrame (e.g. precalc run output).
        bills_b: Comparison bills LazyFrame (e.g. default run output).
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``subclass``, ``bill_mean_a``,
        ``bill_mean_b``, ``bill_delta`` (B − A), ``customers_weighted``.
    """

    def _mean_by_subclass(lf: pl.LazyFrame, alias: str) -> pl.LazyFrame:
        return (
            _with_subclass_meta(_annual(lf), metadata, subclass_spec)
            .group_by(SUBCLASS_COL)
            .agg(
                _wavg(_BILL).alias(alias),
                pl.col(_WEIGHT).sum().alias("customers_weighted"),
            )
        )

    return _collect(
        _mean_by_subclass(bills_a, "bill_mean_a")
        .join(
            _mean_by_subclass(bills_b, "bill_mean_b").drop("customers_weighted"),
            on=SUBCLASS_COL,
        )
        .with_columns(
            (pl.col("bill_mean_b") - pl.col("bill_mean_a")).alias("bill_delta")
        )
    )


def summarize_tariff_rates(tariff_config: dict[str, Any]) -> pl.DataFrame:
    """Extract per-(period, tier) volumetric rates and fixed charges from a CAIRO tariff config.

    Parses ``ur_ec_tou_mat`` rows — each row is
    ``[period, tier, max_usage, units, rate, adjustment, sell]`` — and
    ``ur_monthly_fixed_charge`` for each top-level tariff key.

    Args:
        tariff_config: CAIRO internal tariff config dict from
            :func:`~utils.post.validate.load.load_tariff_config`.

    Returns:
        DataFrame with columns: ``tariff_key``, ``period``, ``tier``,
        ``rate_per_kwh``, ``fixed_charge_per_month``.
    """
    rows = [
        {
            "tariff_key": key,
            "period": int(row[0]),
            "tier": int(row[1]),
            "rate_per_kwh": float(row[4]),
            "fixed_charge_per_month": float(entry.get("ur_monthly_fixed_charge", 0.0)),
        }
        for key, entry in tariff_config.items()
        for row in entry.get("ur_ec_tou_mat", [])
    ]
    return pl.DataFrame(rows)


def summarize_customer_counts(metadata: pl.LazyFrame) -> pl.DataFrame:
    """Weighted customer count by HP/non-HP subclass.

    Args:
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``subclass`` (``"HP"`` / ``"Non-HP"``),
        ``customers_weighted`` (sum of building weights).
    """
    return (
        _collect(
            metadata.group_by(_HP).agg(
                pl.col(_WEIGHT).sum().alias("customers_weighted")
            )
        )
        .with_columns(
            pl.when(pl.col(_HP))
            .then(pl.lit("HP"))
            .otherwise(pl.lit("Non-HP"))
            .alias("subclass")
        )
        .select(["subclass", "customers_weighted"])
        .sort("subclass")
    )


def summarize_nonhp_composition(metadata: pl.LazyFrame) -> pl.DataFrame:
    """Count non-HP customers by heating type, weighted and unweighted.

    Useful for understanding which non-HP fuel types remain after upgrade 02
    (e.g. gas, oil, electric resistance).

    Args:
        metadata: LazyFrame with ``postprocess_group.has_hp``,
            ``postprocess_group.heating_type``, and ``weight`` columns.

    Returns:
        DataFrame with columns: ``postprocess_group.heating_type``, ``count``,
        ``customers_weighted``, sorted by ``customers_weighted`` descending.
    """
    return _collect(
        metadata.filter(~pl.col(_HP))
        .group_by(_HEATING_TYPE)
        .agg(
            pl.len().alias("count"),
            pl.col(_WEIGHT).sum().alias("customers_weighted"),
        )
        .sort("customers_weighted", descending=True)
    )


def summarize_customer_weight_stats(metadata: pl.LazyFrame) -> pl.DataFrame:
    """Summarize per-customer weight statistics by HP/non-HP subclass.

    Uses re-weighted weights from ``customer_metadata.csv`` (CAIRO re-weighted,
    not raw ResStock).

    Args:
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``subclass`` (``"HP"`` / ``"Non-HP"`` / ``"Total"``),
        ``n_buildings`` (count), ``n_customers_weighted`` (sum of weight),
        ``weight_mean``, ``weight_min``, ``weight_max``.
    """
    meta_collected = _collect(metadata)
    by_subclass = (
        meta_collected.with_columns(
            pl.when(pl.col(_HP))
            .then(pl.lit("HP"))
            .otherwise(pl.lit("Non-HP"))
            .alias("subclass")
        )
        .group_by("subclass")
        .agg(
            pl.len().alias("n_buildings"),
            pl.col(_WEIGHT).sum().alias("n_customers_weighted"),
            pl.col(_WEIGHT).mean().alias("weight_mean"),
            pl.col(_WEIGHT).min().alias("weight_min"),
            pl.col(_WEIGHT).max().alias("weight_max"),
        )
        .sort("subclass")
    )
    # Add Total row
    total = (
        meta_collected.select(
            pl.len().alias("n_buildings"),
            pl.col(_WEIGHT).sum().alias("n_customers_weighted"),
            pl.col(_WEIGHT).mean().alias("weight_mean"),
            pl.col(_WEIGHT).min().alias("weight_min"),
            pl.col(_WEIGHT).max().alias("weight_max"),
        )
        .with_columns(pl.lit("Total").alias("subclass"))
        .select(
            [
                "subclass",
                "n_buildings",
                "n_customers_weighted",
                "weight_mean",
                "weight_min",
                "weight_max",
            ]
        )
    )
    return pl.concat([by_subclass, total])


def compute_hourly_cost_of_service(
    loads_by_subclass_df: pl.DataFrame,
    mc_kwh: pl.Series,
) -> pl.DataFrame:
    """Compute hourly cost of service (MC × weighted load) by subclass.

    Args:
        loads_by_subclass_df: DataFrame with columns ``hour`` (int, 0–8759),
            ``subclass`` (``"HP"`` / ``"Non-HP"``), and ``total_weighted_load_kwh``
            (sum of weighted loads per hour per subclass).
        mc_kwh: Series of 8760 floats ($/kWh) aligned to hour index (0–8759).

    Returns:
        DataFrame with columns: ``hour``, ``subclass``, ``cost_usd`` (MC × load).
    """
    # Create a DataFrame from the MC series with hour index
    mc_df = pl.DataFrame({"hour": range(8760), "mc_kwh": mc_kwh})
    # Join loads with MC on hour, compute cost
    return (
        loads_by_subclass_df.join(mc_df, on="hour", how="inner")
        .with_columns(
            (pl.col("mc_kwh") * pl.col("total_weighted_load_kwh")).alias("cost_usd")
        )
        .select(["hour", "subclass", "cost_usd"])
    )

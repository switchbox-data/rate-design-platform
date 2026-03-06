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


# ---------------------------------------------------------------------------
# Public table builders
# ---------------------------------------------------------------------------


def summarize_bills_by_subclass(
    bills: dict[str, pl.LazyFrame],
    metadata: pl.LazyFrame,
) -> pl.DataFrame:
    """Weighted mean annual bill by bill type and HP/non-HP subclass.

    Args:
        bills: Mapping of bill type (``"elec"``, ``"gas"``, ``"comb"``) to the
            LazyFrame from :func:`~utils.post.validate.load.load_bills`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``bill_type``, ``postprocess_group.has_hp``,
        ``bill_mean_weighted``, ``customers_weighted``.
    """
    frames = [
        _collect(
            _with_meta(_annual(lf), metadata, _HP)
            .group_by(_HP)
            .agg(
                _wavg(_BILL).alias("bill_mean_weighted"),
                pl.col(_WEIGHT).sum().alias("customers_weighted"),
            )
            .with_columns(pl.lit(bill_type).alias("bill_type"))
        )
        for bill_type, lf in bills.items()
    ]
    return pl.concat(frames).select(["bill_type", _HP, "bill_mean_weighted", "customers_weighted"])


def summarize_bat_by_subclass(
    bat: pl.LazyFrame,
    metadata: pl.LazyFrame,
) -> pl.DataFrame:
    """Weighted mean BAT metrics by HP/non-HP subclass.

    Only BAT columns present in ``bat.schema`` are included in the aggregation
    (``BAT_percustomer``, ``BAT_vol``, ``BAT_peak``).

    Args:
        bat: LazyFrame from :func:`~utils.post.validate.load.load_bat`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``postprocess_group.has_hp``,
        ``{bat_col}_wavg`` for each present BAT column, ``customers_weighted``.
    """
    bat_cols = [c for c in _BAT_COLS if c in bat.schema]
    return _collect(
        _with_meta(bat, metadata, _HP)
        .group_by(_HP)
        .agg(
            *[_wavg(c).alias(f"{c}_wavg") for c in bat_cols],
            pl.col(_WEIGHT).sum().alias("customers_weighted"),
        )
    )


def summarize_revenue(
    bills: pl.LazyFrame,
    metadata: pl.LazyFrame,
) -> pl.DataFrame:
    """Total weighted annual revenue by HP/non-HP subclass.

    Revenue = ``sum(bill_level × weight)`` — the utility-perspective aggregate
    recovered from each subclass under the target rate.

    Args:
        bills: LazyFrame from :func:`~utils.post.validate.load.load_bills`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``postprocess_group.has_hp``,
        ``total_revenue_weighted``, ``customers_weighted``.
    """
    return _collect(
        _with_meta(_annual(bills), metadata, _HP)
        .group_by(_HP)
        .agg(
            (pl.col(_BILL) * pl.col(_WEIGHT)).sum().alias("total_revenue_weighted"),
            pl.col(_WEIGHT).sum().alias("customers_weighted"),
        )
    )


def compute_bill_deltas(
    bills_a: pl.LazyFrame,
    bills_b: pl.LazyFrame,
    metadata: pl.LazyFrame,
) -> pl.DataFrame:
    """Weighted mean bill difference (B − A) by HP/non-HP subclass.

    Args:
        bills_a: Baseline bills LazyFrame (e.g. precalc run output).
        bills_b: Comparison bills LazyFrame (e.g. default run output).
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        DataFrame with columns: ``postprocess_group.has_hp``, ``bill_mean_a``,
        ``bill_mean_b``, ``bill_delta`` (B − A), ``customers_weighted``.
    """

    def _mean_by_subclass(lf: pl.LazyFrame, alias: str) -> pl.LazyFrame:
        return (
            _with_meta(_annual(lf), metadata, _HP)
            .group_by(_HP)
            .agg(
                _wavg(_BILL).alias(alias),
                pl.col(_WEIGHT).sum().alias("customers_weighted"),
            )
        )

    return _collect(
        _mean_by_subclass(bills_a, "bill_mean_a")
        .join(_mean_by_subclass(bills_b, "bill_mean_b").drop("customers_weighted"), on=_HP)
        .with_columns((pl.col("bill_mean_b") - pl.col("bill_mean_a")).alias("bill_delta"))
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

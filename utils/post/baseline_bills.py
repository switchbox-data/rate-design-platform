"""Load baseline electric bill components from a master comb-bills segment.

Bill changes are measured against one (scenario, stage) declared as
``bill_change_baseline`` in the pipeline YAML — typically today's rates on the
pre-upgrade population.  Every master table repeats that run's electric bill
components as ``baseline_elec_fixed_charge`` / ``baseline_elec_delivery_bill`` /
``baseline_elec_supply_bill``, so a row carries both what a customer would pay
under the modelled rate and what they pay today.

The three columns mirror the ``elec_*`` columns exactly, so on the baseline
segment itself they equal the row's own values.
"""

from __future__ import annotations

import polars as pl

from utils.post.io import ANNUAL_MONTH, BLDG_ID

BASELINE_COLS = [
    "baseline_elec_fixed_charge",
    "baseline_elec_delivery_bill",
    "baseline_elec_supply_bill",
]


def baseline_ref_root(*, state_lower: str, output_batch: str, segment: str) -> str:
    """Hive root of the master ``comb_bills_year_target`` holding the baseline."""
    return (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state_lower}/"
        f"all_utilities/{output_batch}/{segment}/comb_bills_year_target/"
    )


def baseline_columns_from_self() -> list[pl.Expr]:
    """Baseline columns for the baseline segment itself: copies of its own bills."""
    return [
        pl.col("elec_fixed_charge").alias("baseline_elec_fixed_charge"),
        pl.col("elec_delivery_bill").alias("baseline_elec_delivery_bill"),
        pl.col("elec_supply_bill").alias("baseline_elec_supply_bill"),
    ]


def load_baseline_reference_monthly(
    *,
    state_lower: str,
    output_batch: str,
    segment: str,
    utility: str,
    expected_upgrade: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """Per ``(bldg_id, month)``: baseline bill components for one electric utility.

    *expected_upgrade* is the ResStock upgrade the baseline stage represents.
    A segment holds exactly one upgrade, so a mismatch means the table was
    built from a different stage than ``bill_change_baseline`` declares.
    """
    root = baseline_ref_root(
        state_lower=state_lower, output_batch=output_batch, segment=segment
    )
    lf = pl.scan_parquet(
        root,
        hive_partitioning=True,
        storage_options=storage_options,
    )
    q = lf.filter(pl.col("sb.electric_utility") == pl.lit(utility)).select(
        pl.col(BLDG_ID),
        pl.col("month"),
        pl.col("upgrade").cast(pl.Int64),
        *baseline_columns_from_self(),
    )
    df = q.collect()

    if df.is_empty():
        raise FileNotFoundError(
            f"[{utility}] Baseline segment {segment!r} has no rows for this "
            f"utility at {root}. Build master comb bills for the baseline first."
        )

    upgrades = set(df["upgrade"].unique().to_list())
    if upgrades != {int(expected_upgrade)}:
        raise AssertionError(
            f"[{utility}] Baseline segment {segment!r} holds upgrade(s) "
            f"{sorted(upgrades)}, expected [{int(expected_upgrade)}]. The "
            f"baseline table was built from a different stage than "
            f"bill_change_baseline declares."
        )

    return df.drop("upgrade")


def load_baseline_reference_annual(
    *,
    state_lower: str,
    output_batch: str,
    segment: str,
    utility: str,
    expected_upgrade: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    """One row per ``bldg_id`` (``month == Annual``) of baseline bill components."""
    return (
        load_baseline_reference_monthly(
            state_lower=state_lower,
            output_batch=output_batch,
            segment=segment,
            utility=utility,
            expected_upgrade=expected_upgrade,
            storage_options=storage_options,
        )
        .filter(pl.col("month") == pl.lit(ANNUAL_MONTH))
        .drop("month")
    )

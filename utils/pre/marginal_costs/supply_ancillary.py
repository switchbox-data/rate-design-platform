"""Ancillary service marginal cost computation for ISO-NE and NYISO supply MCs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, assert_never

import polars as pl

from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ISONE_ANCILLARY_S3_BASE,
    DEFAULT_NYISO_ANCILLARY_S3_BASE,
    prepare_component_output,
    strip_tz_if_needed,
)

AncillaryIso = Literal["isone", "nyiso"]

# Hive parquet interval column (shared naming across ISO ancillary datasets).
_INTERVAL_START_COL = "interval_start_et"


@dataclass(frozen=True)
class AncillaryPipelineConfig:
    """Per-ISO settings: which columns to read and how errors/notes are labeled."""

    iso: AncillaryIso
    scan_columns: tuple[str, ...]
    data_label: str
    backfill_hint: str

    def duplicate_interval_note(self, n_dup: int, n_rows: int, n_unique: int) -> str:
        """Human-readable note when naive ``interval_start_et`` values collide."""
        if self.iso == "isone":
            return (
                f"  DST fallback: {n_dup} duplicate naive timestamp(s) detected "
                f"({n_rows} rows, {n_unique} unique). Both fallback-hour values will be "
                f"preserved and averaged during alignment to 8760."
            )
        return (
            f"  {self.data_label}: {n_dup} duplicate {_INTERVAL_START_COL} value(s) "
            f"({n_rows} rows, {n_unique} unique naive timestamps)."
        )


def _ancillary_config(iso: AncillaryIso) -> AncillaryPipelineConfig:
    if iso == "isone":
        return AncillaryPipelineConfig(
            iso="isone",
            scan_columns=(
                _INTERVAL_START_COL,
                "reg_service_price_usd_per_mwh",
                "reg_capacity_price_usd_per_mwh",
            ),
            data_label="ISO-NE ancillary",
            backfill_hint="s3://data.sb/isone/ancillary/",
        )
    if iso == "nyiso":
        return AncillaryPipelineConfig(
            iso="nyiso",
            scan_columns=(
                _INTERVAL_START_COL,
                "time_zone",
                "zone",
                "nyca_regulation_capacity_usd_per_mwhr",
                "nyca_regulation_movement_usd_per_mw",
            ),
            data_label="NYISO ancillary",
            backfill_hint="s3://data.sb/nyiso/ancillary/",
        )
    raise ValueError(f"Invalid ISO: {iso}")


def _default_ancillary_s3_base(iso: AncillaryIso) -> str:
    if iso == "isone":
        return DEFAULT_ISONE_ANCILLARY_S3_BASE
    if iso == "nyiso":
        return DEFAULT_NYISO_ANCILLARY_S3_BASE
    raise ValueError(f"Invalid ISO: {iso}")


def _validate_calendar_months(
    df: pl.DataFrame,
    *,
    timestamp_col: str,
    year: int,
    data_label: str,
    backfill_hint: str,
) -> None:
    months_present = (
        df.with_columns(pl.col(timestamp_col).dt.month().alias("month"))
        .select("month")
        .unique()
        .sort("month")
        .to_series()
        .to_list()
    )
    expected_months = list(range(1, 13))
    missing_months = sorted(set(expected_months) - set(months_present))
    if missing_months:
        raise ValueError(
            f"{data_label} data is incomplete for year {year}. "
            f"Missing months: {missing_months}. Present months: {months_present}. "
            f"Backfill missing months in source data ({backfill_hint}) "
            f"before generating marginal costs."
        )


def load_ancillary_source_for_year(
    year: int,
    storage_options: dict[str, str],
    ancillary_s3_base: str | None,
    *,
    iso: AncillaryIso,
) -> pl.DataFrame:
    """Load one ISO's ancillary parquet rows for *year* (wide schema, no end-use sum).

    Strips timezone from ``interval_start_et`` when present, validates calendar
    month coverage, and emits a note if duplicate naive interval timestamps exist
    (e.g. ISO-NE DST fall-back).

    Raises:
        FileNotFoundError: If no rows exist for *year*.
        ValueError: If any calendar month is missing.
    """
    cfg = _ancillary_config(iso)
    base = (ancillary_s3_base or _default_ancillary_s3_base(iso)).rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("year") == year)
        .select(cfg.scan_columns)
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError(
            f"Expected DataFrame from ancillary collect() ({cfg.data_label})"
        )
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ancillary data found for year={year} under {base} ({cfg.data_label})"
        )

    collected = strip_tz_if_needed(collected, _INTERVAL_START_COL)
    _validate_calendar_months(
        collected,
        timestamp_col=_INTERVAL_START_COL,
        year=year,
        data_label=cfg.data_label,
        backfill_hint=cfg.backfill_hint,
    )

    n_rows = collected.height
    n_unique_ts = collected.select(pl.col(_INTERVAL_START_COL).n_unique()).item()
    if n_rows != n_unique_ts:
        print(cfg.duplicate_interval_note(n_rows - n_unique_ts, n_rows, n_unique_ts))

    return collected


def ancillary_wide_to_enduse(
    df: pl.DataFrame,
    *,
    iso: AncillaryIso,
    year: int,
) -> pl.DataFrame:
    """Reduce wide ancillary *df* to ``timestamp`` + ``ancillary_cost_enduse`` ($/MWh).

    ISO-specific rules are isolated in ``iso`` branches: which price columns exist on
    *df* and how they combine into one end-use marginal cost series.

    Raises:
        NotImplementedError: For ``nyiso`` until movement ($/MW) and zonal/market
            aggregation methodology is implemented.
        ValueError: If output timestamps do not cover all calendar months for *year*.
    """
    cfg = _ancillary_config(iso)
    if iso == "isone":
        out = (
            df.rename({_INTERVAL_START_COL: "timestamp"})
            .with_columns(
                (
                    pl.col("reg_service_price_usd_per_mwh")
                    + pl.col("reg_capacity_price_usd_per_mwh")
                ).alias("ancillary_cost_enduse")
            )
            .select("timestamp", "ancillary_cost_enduse")
        )
        avg_ancillary = out["ancillary_cost_enduse"].mean()
        print(
            f"Loaded {cfg.data_label}: {len(out):,} hourly rows, year {year}, "
            f"avg ancillary cost = ${avg_ancillary:.2f}/MWh"
        )
        return out

    if iso == "nyiso":
        raise NotImplementedError(
            "NYISO ancillary end-use aggregation is not yet implemented. Expected "
            f"wide columns on df: {cfg.scan_columns} (year={year}). Implement the nyiso "
            "branch in ancillary_wide_to_enduse() (zonal rollup, DAM vs RT, movement "
            "$/MW → $/MWh)."
        )

    assert_never(iso)


def load_ancillary_for_year(
    year: int,
    storage_options: dict[str, str],
    ancillary_s3_base: str | None = None,
    *,
    iso: AncillaryIso = "isone",
) -> pl.DataFrame:
    """Load ancillary prices for *year* and reduce to ``timestamp`` + ``ancillary_cost_enduse``.

    Uses :func:`load_ancillary_source_for_year` then :func:`ancillary_wide_to_enduse`.
    When *ancillary_s3_base* is omitted, the default base matches *iso*.
    """
    base = ancillary_s3_base or _default_ancillary_s3_base(iso)
    wide = load_ancillary_source_for_year(year, storage_options, base, iso=iso)
    return ancillary_wide_to_enduse(wide, iso=iso, year=year)


def compute_supply_ancillary_mc(
    year: int,
    storage_options: dict[str, str],
    ancillary_s3_base: str | None = None,
    *,
    iso: AncillaryIso = "isone",
) -> pl.DataFrame:
    """Compute hourly supply ancillary MC from ISO regulation-related clearing prices.

    Loads ancillary data for *iso*, aggregates to ``ancillary_cost_enduse`` ($/MWh),
    and returns a Cairo-compatible 8760 hourly DataFrame with columns ``timestamp``
    and ``ancillary_cost_enduse``.

    Raises:
        ValueError: If source data is incomplete (missing months).
        NotImplementedError: If *iso* is ``nyiso`` and end-use aggregation is missing.
    """
    resolved_base = ancillary_s3_base or _default_ancillary_s3_base(iso)
    ancillary_df = load_ancillary_for_year(
        year,
        storage_options,
        resolved_base,
        iso=iso,
    )
    output = prepare_component_output(
        df=ancillary_df,
        year=year,
        input_col="ancillary_cost_enduse",
        output_col="ancillary_cost_enduse",
        scale=1.0,
    )

    iso_label = "ISO-NE" if iso == "isone" else "NYISO"
    avg_cost = output["ancillary_cost_enduse"].mean()
    print(
        f"  Ancillary MC ({iso_label}): {output.height} hours, "
        f"avg cost = ${avg_cost:.2f}/MWh"
    )
    return output

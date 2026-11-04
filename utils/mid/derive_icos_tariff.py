"""Derive ICOS (incremental cost of service) tariff JSONs from CAIRO outputs.

Given a parent scenario's four CAIRO output directories (precalc/calibrated x
delivery/supply), compute the ICOS delivery rate that zeroes the weighted-average
overpayment for the target population, then write delivery and supply URDB JSON
tariff pairs.

The ICOS rate satisfies, in expectation over the target population:

    overpayment = bill_after - (bill_before + delta_MC) = 0

where:
    bill_before = r_base * kwh_before
    bill_after  = r_new  * kwh_after
    delta_MC    = economic_burden_after - economic_burden_before

Flat:
    r_new = (r_base * avg_kwh_before + avg_delta_MC) / avg_kwh_after

Seasonal (summer pinned at r_base, winter adjusted):
    r_winter = (r_base * avg_kwh_before + avg_delta_MC
                - r_base * avg_kwh_after_summer) / avg_kwh_after_winter

Called from ``run_pipeline.py``'s dependent-scenario loop when
``scenario.derive_method == "icos"``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from utils.pre.create_tariff import (
    create_default_flat_tariff,
    create_seasonal_tariff,
    extract_base_rate_and_fixed_charge,
    write_tariff_json,
)
from utils.pre.season_config import load_winter_months_from_periods

if TYPE_CHECKING:
    from rate_design.hp_rates.pipeline_config import PipelineConfig, ScenarioConfig

log = logging.getLogger(__name__)

BLDG_ID = "bldg_id"
WEIGHT = "weight"
HEATING_TYPE_V2 = "postprocess_group.heating_type_v2"
ECONOMIC_BURDEN = "customer_level_economic_burden"
BAT_CSV = "cross_subsidization/cross_subsidization_BAT_values.csv"
KWH_ANNUAL = "billing_kwh_annual.parquet"
KWH_8760 = "billing_kwh_8760.parquet"
METADATA_CSV = "customer_metadata.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def extract_supply_component(base_delivery_path: Path, base_supply_path: Path) -> float:
    """Compute the average supply component from a delivery/supply tariff pair.

    Returns supply_combined_avg - delivery_avg, both computed by simple-
    averaging across all periods in the tariff's ``energyratestructure``.
    """
    d_rate, _ = extract_base_rate_and_fixed_charge(base_delivery_path)
    s_rate, _ = extract_base_rate_and_fixed_charge(base_supply_path)
    return s_rate - d_rate


def _load_population(metadata_dir: Path, heating_types: list[str]) -> pl.DataFrame:
    """Load CAIRO output metadata and filter to the target population.

    Returns a DataFrame with ``bldg_id`` and ``weight`` for buildings whose
    ``postprocess_group.heating_type_v2`` matches one of *heating_types*.
    """
    meta = pl.read_csv(
        metadata_dir / METADATA_CSV,
        columns=[BLDG_ID, WEIGHT, HEATING_TYPE_V2],
    )
    pop = meta.filter(pl.col(HEATING_TYPE_V2).is_in(heating_types))
    if pop.is_empty():
        raise ValueError(
            f"No buildings found with {HEATING_TYPE_V2} in {heating_types}. "
            f"Distinct values: {meta[HEATING_TYPE_V2].unique().sort().to_list()}"
        )
    log.info(
        "icos: population filter %s -> %d / %d buildings",
        heating_types,
        pop.height,
        meta.height,
    )
    return pop.select(BLDG_ID, WEIGHT)


def _load_economic_burden(run_dir: Path, bldg_ids: pl.Series) -> pl.DataFrame:
    """Load per-building economic burden from a CAIRO BAT CSV, filtered to *bldg_ids*."""
    bat = pl.read_csv(
        run_dir / BAT_CSV,
        columns=[BLDG_ID, ECONOMIC_BURDEN],
    )
    return bat.filter(pl.col(BLDG_ID).is_in(bldg_ids.to_list()))


def _load_annual_kwh(run_dir: Path, bldg_ids: pl.Series) -> pl.DataFrame:
    """Load per-building annual grid kWh from a CAIRO output, filtered to *bldg_ids*."""
    kwh = pl.read_parquet(
        run_dir / KWH_ANNUAL,
        columns=[BLDG_ID, "annual_kwh_grid"],
    )
    return kwh.filter(pl.col(BLDG_ID).is_in(bldg_ids.to_list()))


def _weighted_avg(values: pl.Series, weights: pl.Series) -> float:
    """Compute a weighted average, raising if weights sum to zero."""
    w_sum = float(weights.sum())
    if w_sum == 0:
        raise ValueError("Weights sum to zero; cannot compute weighted average.")
    return float((values * weights).sum()) / w_sum


# ---------------------------------------------------------------------------
# Core derivation
# ---------------------------------------------------------------------------


def _compute_icos_inputs(
    precalc_dir: Path,
    calibrated_dir: Path,
    heating_types: list[str],
) -> dict[str, Any]:
    """Compute the weighted-average ICOS inputs from two CAIRO delivery runs.

    Returns a dict with keys: ``avg_delta_mc``, ``avg_kwh_before``,
    ``avg_kwh_after``, and ``population`` (DataFrame with bldg_id + weight).
    """
    population = _load_population(precalc_dir, heating_types)
    bldg_ids = population[BLDG_ID]

    eb_before = _load_economic_burden(precalc_dir, bldg_ids)
    eb_after = _load_economic_burden(calibrated_dir, bldg_ids)
    kwh_before = _load_annual_kwh(precalc_dir, bldg_ids)
    kwh_after = _load_annual_kwh(calibrated_dir, bldg_ids)

    joined = (
        population.join(eb_before, on=BLDG_ID, how="inner", suffix="_eb_pre")
        .join(eb_after, on=BLDG_ID, how="inner", suffix="_eb_post")
        .join(kwh_before, on=BLDG_ID, how="inner", suffix="_kwh_pre")
        .join(kwh_after, on=BLDG_ID, how="inner", suffix="_kwh_post")
    )

    delta_mc = joined[f"{ECONOMIC_BURDEN}_eb_post"] - joined[ECONOMIC_BURDEN]
    w = joined[WEIGHT]

    return {
        "avg_delta_mc": _weighted_avg(delta_mc, w),
        "avg_kwh_before": _weighted_avg(joined["annual_kwh_grid"], w),
        "avg_kwh_after": _weighted_avg(joined["annual_kwh_grid_kwh_post"], w),
        "population": population,
    }


def _compute_seasonal_kwh(
    calibrated_dir: Path,
    bldg_ids: pl.Series,
    weights_df: pl.DataFrame,
    winter_months: list[int],
) -> tuple[float, float]:
    """Compute weighted-average post-retrofit winter and summer kWh.

    Reads hourly grid consumption from ``billing_kwh_8760.parquet`` in the
    calibrated (HP, u2) delivery run directory.

    Returns ``(avg_kwh_after_winter, avg_kwh_after_summer)``.
    """
    kwh_8760 = (
        pl.scan_parquet(calibrated_dir / KWH_8760)
        .filter(pl.col(BLDG_ID).is_in(bldg_ids.to_list()))
        .with_columns(pl.col("timestamp").dt.month().alias("month"))
        .with_columns(pl.col("month").is_in(winter_months).alias("is_winter"))
        .group_by(BLDG_ID)
        .agg(
            pl.when(pl.col("is_winter"))
            .then(pl.col("grid_cons_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("kwh_winter"),
            pl.when(~pl.col("is_winter"))
            .then(pl.col("grid_cons_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("kwh_summer"),
        )
        .collect()
    )
    joined = weights_df.join(kwh_8760, on=BLDG_ID, how="inner")
    w = joined[WEIGHT]
    avg_winter = _weighted_avg(joined["kwh_winter"], w)
    avg_summer = _weighted_avg(joined["kwh_summer"], w)
    log.info(
        "icos seasonal: avg_kwh_after_winter=%.1f, avg_kwh_after_summer=%.1f",
        avg_winter,
        avg_summer,
    )
    return avg_winter, avg_summer


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def derive_icos_tariffs(
    config: PipelineConfig,
    scenario: ScenarioConfig,
    dep_outputs: dict[str, Path],
) -> list[Path]:
    """Derive ICOS delivery + supply tariff JSONs and write them to disk.

    Parameters
    ----------
    config
        The loaded pipeline config.
    scenario
        The ICOS scenario config (must have ``derive_method``, ``structure``,
        ``icos`` fields set).
    dep_outputs
        Dict keyed by ``{stage}_{variant}`` pointing to CAIRO run output dirs,
        as returned by ``check_dependency()``.

    Returns
    -------
    list[Path]
        Paths to the written tariff JSON files (delivery, supply).
    """
    from rate_design.hp_rates.pipeline_config import tariff_json_path

    assert scenario.derive_method == "icos"
    assert scenario.icos is not None
    assert scenario.structure in ("flat", "seasonal")

    heating_types: list[str] = scenario.icos["heating_types"]

    # --- Resolve parent scenario's base tariff ---
    assert scenario.depends_on is not None
    parent_name = scenario.depends_on[0]
    parent = config.scenarios[parent_name]
    base_delivery_path = tariff_json_path(
        config, f"{config.utility}_{parent.tariff_base}"
    )
    base_supply_path = tariff_json_path(
        config, f"{config.utility}_{parent.tariff_base}_supply"
    )

    base_rate, fixed_charge = extract_base_rate_and_fixed_charge(base_delivery_path)
    supply_component = extract_supply_component(base_delivery_path, base_supply_path)
    log.info(
        "icos: base_rate=%.5f, fixed_charge=%.2f, supply_component=%.5f",
        base_rate,
        fixed_charge,
        supply_component,
    )

    # --- Compute ICOS inputs from CAIRO outputs ---
    precalc_dir = dep_outputs["precalc_delivery"]
    calibrated_dir = dep_outputs["calibrated_delivery"]

    inputs = _compute_icos_inputs(precalc_dir, calibrated_dir, heating_types)
    avg_delta_mc = inputs["avg_delta_mc"]
    avg_kwh_before = inputs["avg_kwh_before"]
    avg_kwh_after = inputs["avg_kwh_after"]
    population: pl.DataFrame = inputs["population"]

    log.info(
        "icos: avg_delta_mc=%.2f, avg_kwh_before=%.1f, avg_kwh_after=%.1f",
        avg_delta_mc,
        avg_kwh_before,
        avg_kwh_after,
    )

    # --- Derive tariff rates ---
    utility = config.utility
    label_d = f"{utility}_{scenario.tariff_base}"
    label_s = f"{label_d}_supply"

    if scenario.structure == "flat":
        if avg_kwh_after == 0.0:
            raise ValueError("avg_kwh_after is zero; cannot derive ICOS flat rate.")
        r_new = (base_rate * avg_kwh_before + avg_delta_mc) / avg_kwh_after
        log.info("icos flat: r_new=%.6f (base=%.6f)", r_new, base_rate)

        delivery_tariff = create_default_flat_tariff(
            label=label_d,
            volumetric_rate=round(r_new, 8),
            fixed_charge=round(fixed_charge, 2),
            utility=utility,
        )
        supply_tariff = create_default_flat_tariff(
            label=label_s,
            volumetric_rate=round(r_new + supply_component, 8),
            fixed_charge=round(fixed_charge, 2),
            utility=utility,
        )

    else:
        # seasonal: summer = base, winter adjusted
        periods_path = config.state_config_dir / config.run_defaults.periods_yaml
        winter_months = load_winter_months_from_periods(periods_path)

        bldg_ids = population[BLDG_ID]
        avg_kwh_winter, avg_kwh_summer = _compute_seasonal_kwh(
            calibrated_dir, bldg_ids, population, winter_months
        )

        if avg_kwh_winter == 0.0:
            raise ValueError(
                "avg_kwh_after_winter is zero; cannot concentrate ICOS adjustment "
                "in winter."
            )
        r_winter = (
            base_rate * avg_kwh_before + avg_delta_mc - base_rate * avg_kwh_summer
        ) / avg_kwh_winter
        r_summer = base_rate
        log.info(
            "icos seasonal: r_winter=%.6f, r_summer=%.6f (base=%.6f)",
            r_winter,
            r_summer,
            base_rate,
        )

        summer_months = sorted(set(range(1, 13)) - set(winter_months))
        delivery_tariff = create_seasonal_tariff(
            label=label_d,
            seasons=[
                (winter_months, round(r_winter, 8)),
                (summer_months, round(r_summer, 8)),
            ],
            fixed_charge=round(fixed_charge, 2),
            utility=utility,
        )
        supply_tariff = create_seasonal_tariff(
            label=label_s,
            seasons=[
                (winter_months, round(r_winter + supply_component, 8)),
                (summer_months, round(r_summer + supply_component, 8)),
            ],
            fixed_charge=round(fixed_charge, 2),
            utility=utility,
        )

    # --- Write tariff JSONs ---
    out_d = tariff_json_path(config, label_d)
    out_s = tariff_json_path(config, label_s)
    write_tariff_json(delivery_tariff, out_d)
    write_tariff_json(supply_tariff, out_s)
    log.info("icos: wrote %s", out_d.name)
    log.info("icos: wrote %s", out_s.name)

    return [out_d, out_s]

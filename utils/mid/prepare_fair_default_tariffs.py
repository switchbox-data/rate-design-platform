"""Prepare 12 uncalibrated fair-default tariff JSONs for one utility.

For each of 6 (subclass × strategy) combos and 2 (delivery, supply) variants:
1. Calls compute_fair_default_inputs to derive the fair-default parameters.
2. Calls create_fair_default_tariff to emit the uncalibrated URDB tariff JSON.

The tariff_key embedded in each JSON matches the filename stem so that
copy_calibrated_tariff_from_run produces correctly-named _calibrated outputs.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pandas as pd
import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.cairo import (
    add_bulk_tx_and_dist_and_sub_tx_marginal_cost,
    load_dist_and_sub_tx_marginal_costs,
)
from utils.mid.compute_fair_default_inputs import (
    compute_fair_default_inputs,
)
from utils.mid.compute_subclass_rr import (
    DEFAULT_BAT_METRIC,
    _resolve_path_or_s3,
    parse_group_value_to_subclass,
)
from utils.mid.create_fair_default_tariff import (
    FairDefaultStrategy,
    _read_json,
    _write_json,
    create_fair_default_tariff,
)
from utils.pre.compute_tou import (
    compute_mc_seasonal_ratio,
)
from utils.pre.season_config import load_winter_months_from_periods

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SubclassConfig:
    """Subclass identification for fair-default design."""

    shorthand: str
    group_col: str
    group_value: str
    group_value_to_subclass: str | None


@dataclass(frozen=True)
class FairDefaultCombo:
    """One (subclass, strategy) combination."""

    subclass: SubclassConfig
    strategy: FairDefaultStrategy
    strategy_key: str


SUBCLASSES: list[SubclassConfig] = [
    SubclassConfig(
        shorthand="hp",
        group_col="has_hp",
        group_value="true",
        group_value_to_subclass=None,
    ),
    SubclassConfig(
        shorthand="eheat",
        group_col="heating_type_v2",
        group_value="electric_heating",
        group_value_to_subclass=(
            "heat_pump=electric_heating,electrical_resistance=electric_heating"
        ),
    ),
]

STRATEGY_KEYS: list[tuple[FairDefaultStrategy, str]] = [
    ("fixed_charge_only", "default_fixed"),
    ("seasonal_rates_only", "default_seasonal"),
    ("fixed_plus_seasonal_mc", "default_fixed_seasonal_mc"),
]

ALL_COMBOS: list[FairDefaultCombo] = [
    FairDefaultCombo(subclass=sub, strategy=strat, strategy_key=key)
    for sub in SUBCLASSES
    for strat, key in STRATEGY_KEYS
]


def _compute_delivery_mc_seasonal_ratio(
    path_dist_and_sub_tx_mc: str,
    path_bulk_tx_mc: str,
    winter_months: list[int] | None,
) -> float:
    """Compute MC seasonal ratio from delivery-side marginal costs."""
    dist_series = load_dist_and_sub_tx_marginal_costs(path_dist_and_sub_tx_mc)
    combined = add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
        path_dist_and_sub_tx_mc=path_dist_and_sub_tx_mc,
        path_bulk_tx_mc=path_bulk_tx_mc,
        target_index=pd.DatetimeIndex(dist_series.index),
    )
    load_uniform = pd.Series(1.0, index=combined.index, name="load")
    return compute_mc_seasonal_ratio(combined, load_uniform, winter_months)


def _compute_supply_mc_seasonal_ratio(
    path_supply_energy_mc: str,
    path_supply_capacity_mc: str,
    storage_options: dict | None,
    winter_months: list[int] | None,
) -> float:
    """Compute MC seasonal ratio from supply-side marginal costs.

    Sums energy and capacity MC columns to get total supply MC per hour.
    """
    energy_df = cast(
        pl.DataFrame,
        pl.scan_parquet(
            path_supply_energy_mc, storage_options=storage_options or {}
        ).collect(),
    ).to_pandas()
    cap_df = cast(
        pl.DataFrame,
        pl.scan_parquet(
            path_supply_capacity_mc, storage_options=storage_options or {}
        ).collect(),
    ).to_pandas()

    for df in [energy_df, cap_df]:
        for col in ["timestamp", "datetime", "time"]:
            if col in df.columns:
                df.index = pd.DatetimeIndex(df[col])
                break

    numeric_energy = [
        c for c in energy_df.columns if energy_df[c].dtype in (float, "float64")
    ]
    numeric_cap = [c for c in cap_df.columns if cap_df[c].dtype in (float, "float64")]
    combined = energy_df[numeric_energy[0]] + cap_df[numeric_cap[0]]
    combined.name = "total_supply_mc"
    load_uniform = pd.Series(1.0, index=combined.index, name="load")
    return compute_mc_seasonal_ratio(combined, load_uniform, winter_months)


def _patch_tariff_key(tariff: dict, tariff_key: str) -> dict:
    """Set the tariff_key field (used by copy_calibrated_tariff_from_run)."""
    tariff["tariff_key"] = tariff_key
    return tariff


def prepare_fair_default_tariffs(
    *,
    utility: str,
    state: str,
    run_dir_delivery: str | Path | S3Path,
    run_dir_supply: str | Path | S3Path,
    output_dir: Path,
    resstock_base: str,
    upgrade: str,
    path_dist_and_sub_tx_mc: str,
    path_bulk_tx_mc: str,
    path_supply_energy_mc: str,
    path_supply_capacity_mc: str,
    path_base_tariff_delivery: str | Path,
    path_base_tariff_supply: str | Path,
    path_periods_yaml: Path | None = None,
    allow_infeasible: bool = False,
) -> list[str]:
    """Build 12 uncalibrated fair-default tariff JSONs.

    Returns:
        List of paths to written tariff JSON files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    storage_options = (
        get_aws_storage_options()
        if isinstance(run_dir_delivery, S3Path)
        or str(run_dir_delivery).startswith("s3://")
        or resstock_base.startswith("s3://")
        else None
    )

    run_dir_delivery_resolved = _resolve_path_or_s3(str(run_dir_delivery))
    run_dir_supply_resolved = _resolve_path_or_s3(str(run_dir_supply))

    winter_months: list[int] | None = None
    if path_periods_yaml is not None:
        from utils.pre.season_config import (
            DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
        )

        winter_months = load_winter_months_from_periods(
            path_periods_yaml,
            default_winter_months=DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
        )

    LOGGER.info("Computing delivery MC seasonal ratio for %s ...", utility)
    delivery_mc_ratio = _compute_delivery_mc_seasonal_ratio(
        path_dist_and_sub_tx_mc=path_dist_and_sub_tx_mc,
        path_bulk_tx_mc=path_bulk_tx_mc,
        winter_months=winter_months,
    )
    LOGGER.info("Delivery MC seasonal ratio: %.4f", delivery_mc_ratio)

    LOGGER.info("Computing supply MC seasonal ratio for %s ...", utility)
    supply_mc_ratio = _compute_supply_mc_seasonal_ratio(
        path_supply_energy_mc=path_supply_energy_mc,
        path_supply_capacity_mc=path_supply_capacity_mc,
        storage_options=storage_options,
        winter_months=winter_months,
    )
    LOGGER.info("Supply MC seasonal ratio: %.4f", supply_mc_ratio)

    base_tariff_delivery = _read_json(
        Path(path_base_tariff_delivery)
        if not isinstance(path_base_tariff_delivery, Path)
        else path_base_tariff_delivery
    )
    base_tariff_supply = _read_json(
        Path(path_base_tariff_supply)
        if not isinstance(path_base_tariff_supply, Path)
        else path_base_tariff_supply
    )

    written_paths: list[str] = []

    for combo in ALL_COMBOS:
        sub = combo.subclass
        strategy = combo.strategy
        key = combo.strategy_key

        mc_ratio_delivery = (
            delivery_mc_ratio if strategy == "fixed_plus_seasonal_mc" else None
        )
        mc_ratio_supply = (
            supply_mc_ratio if strategy == "fixed_plus_seasonal_mc" else None
        )

        for is_supply in (False, True):
            run_dir = (
                run_dir_supply_resolved if is_supply else run_dir_delivery_resolved
            )
            base_tariff = base_tariff_supply if is_supply else base_tariff_delivery
            mc_ratio = mc_ratio_supply if is_supply else mc_ratio_delivery

            if is_supply:
                tariff_stem = f"{utility}_{sub.shorthand}_{key}_supply"
            else:
                tariff_stem = f"{utility}_{sub.shorthand}_{key}"

            LOGGER.info("Processing %s ...", tariff_stem)

            inputs_output_dir = output_dir / ".fair_default_inputs"
            inputs_output_dir.mkdir(parents=True, exist_ok=True)

            inputs_df = compute_fair_default_inputs(
                run_dir=run_dir,
                resstock_base=resstock_base,
                state=state,
                upgrade=upgrade,
                group_col=sub.group_col,
                subclass_value=sub.group_value,
                cross_subsidy_col=DEFAULT_BAT_METRIC,
                storage_options=storage_options,
                group_value_to_subclass=(
                    parse_group_value_to_subclass(sub.group_value_to_subclass)
                    if sub.group_value_to_subclass
                    else None
                ),
                base_tariff_json_path=(
                    Path(path_base_tariff_supply)
                    if is_supply
                    else Path(path_base_tariff_delivery)
                ),
                periods_yaml_path=path_periods_yaml,
                mc_seasonal_ratio=mc_ratio,
                fixed_charge_floor=0.0,
            )

            inputs_csv_path = inputs_output_dir / f"{tariff_stem}_inputs.csv"
            csv_text = inputs_df.write_csv(None)
            if not isinstance(csv_text, str):
                raise ValueError("Failed to render fair default input CSV text.")
            inputs_csv_path.write_text(csv_text, encoding="utf-8")
            LOGGER.info("Wrote inputs CSV: %s", inputs_csv_path)

            row = inputs_df.row(0, named=True)

            tariff = create_fair_default_tariff(
                base_tariff=base_tariff,
                inputs_row=row,
                strategy=strategy,
                label=tariff_stem,
                periods_yaml_path=path_periods_yaml,
                allow_infeasible=allow_infeasible,
            )
            tariff = _patch_tariff_key(tariff, tariff_stem)

            output_path = output_dir / f"{tariff_stem}.json"
            written = _write_json(output_path, tariff)
            LOGGER.info("Wrote tariff JSON: %s", written)
            written_paths.append(written)

    return written_paths


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description=(
            "Build 12 uncalibrated fair-default tariff JSONs for one utility. "
            "Loops 6 (subclass × strategy) combos × 2 (delivery, supply) variants."
        )
    )
    parser.add_argument("--utility", required=True, help="Utility name (e.g. nyseg).")
    parser.add_argument("--state", required=True, help="State abbreviation (e.g. NY).")
    parser.add_argument(
        "--run-dir-delivery",
        required=True,
        help="CAIRO output directory for baseline delivery run (run-1).",
    )
    parser.add_argument(
        "--run-dir-supply",
        required=True,
        help="CAIRO output directory for baseline supply run (run-2).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory to write uncalibrated tariff JSONs (e.g. config/tariffs/electric).",
    )
    parser.add_argument(
        "--resstock-base",
        required=True,
        help="Base path to ResStock release (e.g. s3://.../res_2024_amy2018_2_sb).",
    )
    parser.add_argument(
        "--upgrade",
        default="00",
        help="ResStock upgrade partition to use (default: 00).",
    )
    parser.add_argument(
        "--path-dist-and-sub-tx-mc",
        required=True,
        help="Parquet path for dist+sub-tx marginal costs.",
    )
    parser.add_argument(
        "--path-bulk-tx-mc",
        required=True,
        help="Parquet path for bulk-tx marginal costs.",
    )
    parser.add_argument(
        "--path-supply-energy-mc",
        required=True,
        help="Parquet path for supply energy marginal costs.",
    )
    parser.add_argument(
        "--path-supply-capacity-mc",
        required=True,
        help="Parquet path for supply capacity marginal costs.",
    )
    parser.add_argument(
        "--base-tariff-delivery",
        required=True,
        help="Calibrated delivery base tariff JSON (e.g. nyseg_default_calibrated.json).",
    )
    parser.add_argument(
        "--base-tariff-supply",
        required=True,
        help=(
            "Calibrated supply base tariff JSON "
            "(e.g. nyseg_default_supply_calibrated.json)."
        ),
    )
    parser.add_argument(
        "--periods-yaml",
        type=Path,
        default=None,
        help="Optional periods YAML for winter months configuration.",
    )
    parser.add_argument(
        "--allow-infeasible",
        action="store_true",
        help="Clip negative charges/rates at zero instead of raising.",
    )
    args = parser.parse_args()

    written = prepare_fair_default_tariffs(
        utility=args.utility,
        state=args.state,
        run_dir_delivery=args.run_dir_delivery,
        run_dir_supply=args.run_dir_supply,
        output_dir=args.output_dir,
        resstock_base=args.resstock_base,
        upgrade=args.upgrade,
        path_dist_and_sub_tx_mc=args.path_dist_and_sub_tx_mc,
        path_bulk_tx_mc=args.path_bulk_tx_mc,
        path_supply_energy_mc=args.path_supply_energy_mc,
        path_supply_capacity_mc=args.path_supply_capacity_mc,
        path_base_tariff_delivery=args.base_tariff_delivery,
        path_base_tariff_supply=args.base_tariff_supply,
        path_periods_yaml=args.periods_yaml,
        allow_infeasible=args.allow_infeasible,
    )
    print(f"Wrote {len(written)} tariff JSON files:")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()

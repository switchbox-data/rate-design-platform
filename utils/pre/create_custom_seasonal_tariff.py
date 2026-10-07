"""Generate a seasonal URDB v7 tariff pair from a custom-rates YAML config.

Companion to ``create_custom_flat_tariff.py``.  Reads a seasonal custom-rates
YAML with per-season delivery component rates and writes two seasonal (N-period)
URDB v7 tariffs:

  - <utility>_<label>.json        (delivery only)
  - <utility>_<label>_supply.json (delivery + supply combined)

Config format::

    label: icos_fair_seasonal
    utility: ct_eversource
    fixed_charge: 12.36  # $/month

    seasons:
      winter:
        months: [12, 1, 2, 3, 11]   # 1-indexed month numbers
        delivery:
          distribution: 0.08
          riders: 0.04139
      summer:
        months: [4, 5, 6, 7, 8, 9, 10]
        delivery:
          distribution: 0.12196
          riders: 0.04139

    supply_rate: 0.10469  # $/kWh, added on top of delivery for the _supply variant

Usage::

    uv run python utils/pre/create_custom_seasonal_tariff.py \\
        --config rate_design/hp_rates/ct/config/tariffs/electric/custom_rates/ct_eversource_icos_fair_seasonal.yaml \\
        --output-dir rate_design/hp_rates/ct/config/tariffs/electric
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import yaml

from utils.pre.create_tariff import create_seasonal_tariff, write_tariff_json

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def build_tariffs(config: dict) -> tuple[dict, dict]:
    """Build the (delivery, delivery+supply) seasonal URDB tariff dicts from a parsed config.

    Iterates over ``seasons``, sums each season's ``delivery`` component rates
    into a single volumetric rate, then builds the delivery-only tariff and the
    delivery+supply tariff (which adds ``supply_rate`` on top of each season).
    Both share the same fixed charge.
    """
    label = config["label"]
    utility = config["utility"]
    fixed_charge = float(config["fixed_charge"])
    supply_rate = float(config["supply_rate"])

    seasons_delivery: list[tuple[list[int], float]] = []
    seasons_supply: list[tuple[list[int], float]] = []

    for season_cfg in config["seasons"].values():
        months: list[int] = season_cfg["months"]
        delivery_rate = sum(float(v) for v in season_cfg["delivery"].values())
        seasons_delivery.append((months, round(delivery_rate, 8)))
        seasons_supply.append((months, round(delivery_rate + supply_rate, 8)))

    delivery_tariff = create_seasonal_tariff(
        label=f"{utility}_{label}",
        seasons=seasons_delivery,
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )
    supply_tariff = create_seasonal_tariff(
        label=f"{utility}_{label}_supply",
        seasons=seasons_supply,
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )
    return delivery_tariff, supply_tariff


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a seasonal URDB v7 tariff pair from a custom-rates YAML config"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a seasonal custom-rates YAML config (label, utility, fixed_charge, seasons, supply_rate)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for the generated tariff JSONs",
    )
    args = parser.parse_args()

    with args.config.open(encoding="utf-8") as f:
        config = yaml.safe_load(f)

    label = config["label"]
    utility = config["utility"]
    delivery_tariff, supply_tariff = build_tariffs(config)

    for season_name, season_cfg in config["seasons"].items():
        delivery_rate = sum(float(v) for v in season_cfg["delivery"].values())
        supply_rate = float(config["supply_rate"])
        log.info(
            "%s_%s [%s]: delivery=$%.5f/kWh, delivery+supply=$%.5f/kWh",
            utility,
            label,
            season_name,
            delivery_rate,
            delivery_rate + supply_rate,
        )
    log.info("%s_%s: fixed=$%.2f/mo", utility, label, float(config["fixed_charge"]))

    args.output_dir.mkdir(parents=True, exist_ok=True)

    delivery_path = args.output_dir / f"{utility}_{label}.json"
    supply_path = args.output_dir / f"{utility}_{label}_supply.json"
    write_tariff_json(delivery_tariff, delivery_path)
    write_tariff_json(supply_tariff, supply_path)

    log.info("  wrote %s", delivery_path.name)
    log.info("  wrote %s", supply_path.name)


if __name__ == "__main__":
    main()

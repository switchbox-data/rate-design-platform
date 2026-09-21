"""Generate a flat URDB v7 tariff pair from a custom-rates YAML config.

For hand-specified proposed rates (e.g. a regulatory testimony exhibit) that
aren't derived from a utility's filed monthly rates, this reads a small YAML
config of component-level volumetric rates and writes two flat (single-period)
URDB v7 tariffs:

  - <utility>_<label>.json        (delivery only)
  - <utility>_<label>_supply.json (delivery + supply combined)

Config format (see config/tariffs/electric/custom_rates/ for examples)::

    label: rhp_flat
    utility: bge
    fixed_charge: 10.0  # $/month

    delivery:
      delivery_service: 0.031  # $/kWh, summed into the delivery volumetric rate
      transmission: 0.015
      energy_efficiency: 0.007

    supply_rate: 0.117  # $/kWh, added on top of delivery for the _supply variant

Usage::

    uv run python utils/pre/create_custom_flat_tariff.py \\
        --config rate_design/hp_rates/md/config/tariffs/electric/custom_rates/bge_rhp_flat.yaml \\
        --output-dir rate_design/hp_rates/md/config/tariffs/electric
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import yaml

from utils.pre.create_tariff import create_default_flat_tariff, write_tariff_json

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def build_tariffs(config: dict) -> tuple[dict, dict]:
    """Build the (delivery, delivery+supply) URDB tariff dicts from a parsed config.

    Sums ``delivery`` component rates into a single volumetric rate, then builds
    the delivery-only tariff and the delivery+supply tariff (which adds
    ``supply_rate`` on top).  Both share the same fixed charge.
    """
    label = config["label"]
    utility = config["utility"]
    fixed_charge = float(config["fixed_charge"])
    delivery_rate = sum(float(v) for v in config["delivery"].values())
    supply_rate = float(config["supply_rate"])

    delivery_tariff = create_default_flat_tariff(
        label=f"{utility}_{label}",
        volumetric_rate=round(delivery_rate, 8),
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )
    supply_tariff = create_default_flat_tariff(
        label=f"{utility}_{label}_supply",
        volumetric_rate=round(delivery_rate + supply_rate, 8),
        fixed_charge=round(fixed_charge, 2),
        utility=utility,
    )
    return delivery_tariff, supply_tariff


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a flat URDB v7 tariff pair from a custom-rates YAML config"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a custom-rates YAML config (label, utility, fixed_charge, delivery, supply_rate)",
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

    delivery_rate = sum(float(v) for v in config["delivery"].values())
    supply_rate = float(config["supply_rate"])
    log.info(
        "%s_%s: delivery=$%.5f/kWh, delivery+supply=$%.5f/kWh, fixed=$%.2f/mo",
        utility,
        label,
        delivery_rate,
        delivery_rate + supply_rate,
        float(config["fixed_charge"]),
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    delivery_path = args.output_dir / f"{utility}_{label}.json"
    supply_path = args.output_dir / f"{utility}_{label}_supply.json"
    write_tariff_json(delivery_tariff, delivery_path)
    write_tariff_json(supply_tariff, supply_path)

    log.info("  wrote %s", delivery_path.name)
    log.info("  wrote %s", supply_path.name)


if __name__ == "__main__":
    main()

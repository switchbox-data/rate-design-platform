"""Utilities for creating URDB v7 tariff JSON structures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

WINTER_MONTHS = {12, 1, 2}


def create_default_flat_tariff(
    label: str,
    volumetric_rate: float,
    fixed_charge: float,
    adjustment: float = 0.0,
    utility: str = "GenericUtility",
) -> dict[str, list[dict[str, Any]]]:
    """Generate a single-period flat tariff in URDB v7 format."""
    schedule = [[0] * 24 for _ in range(12)]
    return {
        "items": [
            {
                "label": label,
                "uri": "",
                "sector": "Residential",
                "energyweekdayschedule": schedule,
                "energyweekendschedule": schedule,
                "energyratestructure": [
                    [{"rate": volumetric_rate, "adj": adjustment, "unit": "kWh"}]
                ],
                "fixedchargefirstmeter": fixed_charge,
                "fixedchargeunits": "$/month",
                "mincharge": 0.0,
                "minchargeunits": "$/month",
                "utility": utility,
                "servicetype": "Bundled",
                "name": label,
                "is_default": False,
                "country": "USA",
                "demandunits": "kW",
                "demandrateunit": "kW",
            }
        ]
    }


def load_tariff_json(path: Path) -> dict[str, Any]:
    """Load a tariff JSON file from disk."""
    return json.loads(path.read_text(encoding="utf-8"))


def write_tariff_json(tariff: dict[str, Any], output_path: Path) -> Path:
    """Write a tariff JSON file to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(tariff, indent=2), encoding="utf-8")
    return output_path


def _seasonal_schedule() -> list[list[int]]:
    # URDB schedules are 12 months x 24 hours, period index per hour.
    # We reserve period=1 for winter (Dec-Feb) and period=0 for non-winter.
    schedule: list[list[int]] = []
    for month_num in range(1, 13):
        period = 1 if month_num in WINTER_MONTHS else 0
        schedule.append([period] * 24)
    return schedule


def create_seasonal_rate(
    base_tariff: dict[str, Any],
    *,
    label: str,
    winter_rate: float,
    summer_rate: float,
) -> dict[str, Any]:
    """Create a 2-period seasonal tariff from a base tariff template."""
    if "items" not in base_tariff or not base_tariff["items"]:
        raise ValueError("Base tariff must contain at least one item in `items`.")

    new_tariff = json.loads(json.dumps(base_tariff))
    item = new_tariff["items"][0]
    item["label"] = label
    item["name"] = label
    item["energyweekdayschedule"] = _seasonal_schedule()
    item["energyweekendschedule"] = _seasonal_schedule()
    item["energyratestructure"] = [
        [{"rate": float(summer_rate), "adj": 0.0, "unit": "kWh"}],
        [{"rate": float(winter_rate), "adj": 0.0, "unit": "kWh"}],
    ]
    return new_tariff

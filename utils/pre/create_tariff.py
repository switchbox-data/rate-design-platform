"""Utilities for creating URDB v7 tariff JSON structures."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

WINTER_MONTHS = {12, 1, 2}


@dataclass(slots=True)
class SeasonalTouTariffSpec:
    """Per-season input needed to build a seasonal TOU tariff."""

    months: list[int]  # 1-indexed month numbers
    base_rate: float
    peak_hours: list[int]  # hour-of-day values [0..23]
    peak_offpeak_ratio: float


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


def create_tou_tariff(
    label: str,
    peak_hours: list[int],
    peak_offpeak_ratio: float,
    base_rate: float,
    fixed_charge: float = 6.75,
    adjustment: float = 0.0,
    utility: str = "GenericUtility",
) -> dict[str, Any]:
    """Create a two-period (off-peak/peak) annual TOU tariff.

    This is the non-seasonal TOU constructor used by inline TOU derivation
    paths in scenario execution.
    """
    peak_set = set(peak_hours)
    schedule = [[1 if hour in peak_set else 0 for hour in range(24)] for _ in range(12)]
    rates = [
        (float(base_rate), float(adjustment)),
        (float(base_rate) * float(peak_offpeak_ratio), float(adjustment)),
    ]
    return create_urdb_tariff(
        label=label,
        schedule=schedule,
        rates=rates,
        fixed_charge=fixed_charge,
        utility=utility,
    )


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


def create_seasonal_tariff(
    label: str,
    seasons: list[tuple[list[int], float]],
    fixed_charge: float = 6.75,
    adjustment: float = 0.0,
    utility: str = "GenericUtility",
) -> dict[str, Any]:
    """Build an N-period seasonal flat tariff (one period per season)."""
    month_to_period: dict[int, int] = {}
    for period_idx, (months, _rate) in enumerate(seasons):
        for month in months:
            month_to_period[month] = period_idx

    schedule = [
        [month_to_period[month_0 + 1] for _hour in range(24)] for month_0 in range(12)
    ]
    rates = [(float(rate), float(adjustment)) for _months, rate in seasons]

    return create_urdb_tariff(
        label=label,
        schedule=schedule,
        rates=rates,
        fixed_charge=fixed_charge,
        utility=utility,
    )


def create_seasonal_tou_tariff(
    label: str,
    specs: list[SeasonalTouTariffSpec],
    fixed_charge: float = 6.75,
    adjustment: float = 0.0,
    utility: str = "GenericUtility",
) -> dict[str, Any]:
    """Build a 2N-period seasonal+TOU tariff.

    Each season maps to two periods: off-peak (2*i) and peak (2*i + 1).
    """
    month_info: dict[int, tuple[int, set[int]]] = {}
    for i, spec in enumerate(specs):
        offpeak_period = 2 * i
        peak_set = set(spec.peak_hours)
        for month in spec.months:
            month_info[month] = (offpeak_period, peak_set)

    schedule: list[list[int]] = []
    for month_0 in range(12):
        month_1 = month_0 + 1
        offpeak_period, peak_set = month_info[month_1]
        peak_period = offpeak_period + 1
        schedule.append(
            [peak_period if hour in peak_set else offpeak_period for hour in range(24)]
        )

    rates: list[tuple[float, float]] = []
    for spec in specs:
        rates.append((float(spec.base_rate), float(adjustment)))
        rates.append(
            (float(spec.base_rate) * float(spec.peak_offpeak_ratio), float(adjustment))
        )

    return create_urdb_tariff(
        label=label,
        schedule=schedule,
        rates=rates,
        fixed_charge=fixed_charge,
        utility=utility,
    )


def create_urdb_tariff(
    label: str,
    schedule: list[list[int]],
    rates: list[tuple[float, float]],
    fixed_charge: float,
    utility: str,
) -> dict[str, Any]:
    """Assemble the URDB v7 tariff envelope from schedule + rates."""
    rate_structure = [
        [{"rate": round(rate, 6), "adj": adj, "unit": "kWh"}] for rate, adj in rates
    ]
    return {
        "items": [
            {
                "label": label,
                "uri": "",
                "sector": "Residential",
                "energyweekdayschedule": schedule,
                "energyweekendschedule": schedule,
                "energyratestructure": rate_structure,
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

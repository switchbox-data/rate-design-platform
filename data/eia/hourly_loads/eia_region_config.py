"""Shared state and utility configuration for EIA load workflows.

This module centralizes:
- state-level fetch config used by EIA ingestion scripts
- utility service areas used by utility aggregation scripts
"""

import os
from dataclasses import dataclass
from typing import TypedDict


@dataclass(frozen=True)
class StateConfig:
    """Configuration for state-specific EIA fetch and storage behavior."""

    state: str
    eia_parent: str
    eia_subba_filters: list[str] | None
    zone_mapping: dict[str, str]
    zones: list[str]
    timezone: str
    iso_region: str
    label: str


class UtilityServiceArea(TypedDict):
    """Typed utility service area entry."""

    state: str
    zones: list[str]


STATE_CONFIGS: dict[str, StateConfig] = {
    "NY": StateConfig(
        state="NY",
        eia_parent="NYIS",
        eia_subba_filters=None,
        zone_mapping={
            "ZONA": "A",
            "ZONB": "B",
            "ZONC": "C",
            "ZOND": "D",
            "ZONE": "E",
            "ZONF": "F",
            "ZONG": "G",
            "ZONH": "H",
            "ZONI": "I",
            "ZONJ": "J",
            "ZONK": "K",
        },
        zones=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"],
        timezone="America/New_York",
        iso_region="nyiso",
        label="NYISO",
    ),
    "RI": StateConfig(
        state="RI",
        eia_parent="ISNE",
        eia_subba_filters=["4005"],
        zone_mapping={
            "4005": "RI",
        },
        zones=["RI"],
        timezone="America/New_York",
        iso_region="isone",
        label="ISONE",
    ),
    # MD does not use the EIA-930 load pipeline: its sub-TX/DX marginal-cost
    # workflow reads PJM-native utility loads from
    # s3://data.sb/pjm/hourly_demand/utilities/ (see data/pjm/hourly_demand/).
    # This entry exists so get_state_config("MD") resolves for that workflow;
    # the EIA-specific fields (eia_parent, eia_subba_filters, zone_mapping,
    # zones) are unused for MD. The ISO-native load layout has no region=
    # partition, so iso_region is not used as a load filter for MD.
    "MD": StateConfig(
        state="MD",
        eia_parent="PJM",
        eia_subba_filters=None,
        zone_mapping={},
        zones=[],
        timezone="America/New_York",
        iso_region="pjm",
        label="PJM",
    ),
}


# Utilities are modeled as service areas to support future multi-state utilities.
# A single utility can have multiple state entries.
UTILITY_SERVICE_AREAS: dict[str, list[UtilityServiceArea]] = {
    "nyseg": [
        {"state": "NY", "zones": ["A", "B", "C", "D", "E", "F"]},
    ],
    "rge": [
        {"state": "NY", "zones": ["B"]},
    ],
    "cenhud": [
        {"state": "NY", "zones": ["G"]},
    ],
    "nimo": [
        {"state": "NY", "zones": ["A", "B", "C", "D", "E", "F"]},
    ],
    "coned": [
        {"state": "NY", "zones": ["G", "H", "I", "J"]},
    ],
    "or": [
        {"state": "NY", "zones": ["G"]},
    ],
    "psegli": [
        {"state": "NY", "zones": ["K"]},
    ],
    "rie": [
        {"state": "RI", "zones": ["RI"]},
    ],
}


def get_state_config(state: str) -> StateConfig:
    """Return normalized state configuration."""
    state_upper = state.upper()
    if state_upper not in STATE_CONFIGS:
        raise ValueError(
            f"Unsupported state: {state}. Supported values: {', '.join(sorted(STATE_CONFIGS.keys()))}"
        )
    return STATE_CONFIGS[state_upper]


def get_utility_zone_mapping_for_state(state: str) -> dict[str, list[str]]:
    """Return utility->zones mapping for a specific state."""
    state_upper = state.upper()
    utility_zone_mapping: dict[str, list[str]] = {}

    for utility, service_areas in UTILITY_SERVICE_AREAS.items():
        for area in service_areas:
            area_state = str(area["state"]).upper()
            if area_state == state_upper:
                utility_zone_mapping[utility] = list(area["zones"])  # copy
                break

    return utility_zone_mapping


def get_aws_storage_options() -> dict[str, str]:
    """Return Polars-compatible AWS storage options for S3 access."""
    aws_region = (
        os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "us-west-2"
    )
    return {
        "region": aws_region,
        "default_region": aws_region,
    }

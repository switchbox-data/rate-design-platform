"""Shared constants for the ResStock data pipeline."""

from __future__ import annotations

# Columns added by identify_heating_type.
HEATING_TYPE_COLS: frozenset[str] = frozenset(
    {
        "postprocess_group.heating_type",
        "postprocess_group.heating_type_v2",
        "heats_with_electricity",
        "heats_with_natgas",
        "heats_with_oil",
        "heats_with_propane",
    }
)

"""Shared constants for the ResStock data pipeline."""

from __future__ import annotations

# Columns added by identify_hp_customers.
HP_CUSTOMERS_COLS: frozenset[str] = frozenset({"postprocess_group.has_hp"})

# Columns added by identify_heating_type (requires has_hp from identify_hp_customers).
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

# Columns added by identify_natgas_connection (requires heats_with_natgas from heating_type).
NATGAS_CONNECTION_COLS: frozenset[str] = frozenset({"has_natgas_connection"})

# Columns added by add_vulnerability_columns (NY only).
VULNERABILITY_COLS: frozenset[str] = frozenset(
    {
        "has_child_under_6",
        "has_person_over_60",
        "has_disabled_person",
        "is_vulnerable",
    }
)

# File types that belong only to the raw NREL release and must never be copied
# to the _sb release, uploaded under _sb, or validated against _sb.
# load_curve_annual has no post-approximation equivalent: the only valid
# aggregation of the modified _sb load curves is load_curve_monthly (derived
# from load_curve_hourly by add_monthly_loads after all modifications are done).
SB_EXCLUDED_FILE_TYPES: frozenset[str] = frozenset({"load_curve_annual"})

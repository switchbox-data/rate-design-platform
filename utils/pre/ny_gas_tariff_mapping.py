"""
Rate Acuity gas (utility, rate name) → tariff_key for NY.

Used by fetch_gas_tariffs_rateacuity.py and install_ny_gas_tariffs_from_staging.py.
"""

from __future__ import annotations

import re

# (utility substring, rate name regex) → tariff_key. Order matters.
GAS_URDB_TO_TARIFF_KEY_NY = [
    (
        "Consolidated Edison",
        r"ON HOLD-1-RESIDENTIAL AND RELIGIOUS FIRM SALES",
        "coned_sf",
    ),
    (
        "Consolidated Edison",
        r"1-RESIDENTIAL AND RELIGIOUS FIRM SALES SERVICE",
        "coned_sf",
    ),
    ("Consolidated Edison", r"Dwelling Units <=4", "coned_mf_lowrise"),
    ("Consolidated Edison", r"Dwelling Units > 4", "coned_mf_highrise"),
    ("Brooklyn Union", r"1A-RESIDENTIAL NON-HEATING", "kedny_sf_nonheating"),
    ("Brooklyn Union", r"1B-RESIDENTIAL HEATING", "kedny_sf_heating"),
    ("Brooklyn Union", r"3-HEATING AND/OR WATER HEATING.*MULTI-FAMILY", "kedny_mf"),
    (
        "Keyspan Gas East",
        r"1-RESIDENTIAL SERVICE-1A - Non-Heating",
        "kedli_sf_nonheating",
    ),
    ("Keyspan Gas East", r"1-RESIDENTIAL SERVICE-1B - Heating", "kedli_sf_heating"),
    (
        "Keyspan Gas East",
        r"3-MULTIPLE-DWELLING SERVICE-3A - Non-Heating",
        "kedli_mf_nonheating",
    ),
    (
        "Keyspan Gas East",
        r"3-MULTIPLE-DWELLING SERVICE-3B - Heating",
        "kedli_mf_heating",
    ),
    ("New York State Electric & Gas", r"PSC 87.*Non-Heating", "nyseg_nonheating"),
    ("New York State Electric & Gas", r"PSC 87.*Heating--", "nyseg_heating"),
    ("Niagara Mohawk", r"1-RESIDENTIAL DELIVERY", "nimo"),
    ("Rochester Gas", r"1-GENERAL SERVICE.*Residential", "rge"),
    ("Central Hudson", r"SC 1-RESIDENCE DELIVERY RATE", "cenhud"),
    ("Orange and Rockland", r"SERVICE CLASSIFICATION 1-Residential", "or"),
    ("National Fuel Gas", r"1-RESIDENTIAL.*Customers Purchasing", "nfg"),
]


def match_tariff_key(utility: str, name: str) -> str | None:
    """Return tariff_key for (utility, rate name) in NY, or None if no mapping."""
    for util_pattern, name_pattern, tariff_key in GAS_URDB_TO_TARIFF_KEY_NY:
        if util_pattern not in utility:
            continue
        if re.search(name_pattern, name, re.IGNORECASE | re.DOTALL):
            return tariff_key
    return None

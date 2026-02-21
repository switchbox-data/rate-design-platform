"""
Rate Acuity gas (utility, rate name) → tariff_key for RI.

Used by fetch_gas_tariffs_rateacuity.py and install_ny_gas_tariffs_from_staging.py (when --state RI).
"""

from __future__ import annotations

import re

# (utility substring, rate name regex) → tariff_key. Order matters (non-heating before heating).
GAS_URDB_TO_TARIFF_KEY_RI = [
    ("Narragansett", r"non.heating|Non-Heating|non heating", "rie_nonheating"),
    ("Narragansett", r"heating|Heating", "rie_heating"),
    ("RI Energy", r"non.heating|Non-Heating|non heating", "rie_nonheating"),
    ("RI Energy", r"heating|Heating", "rie_heating"),
]


def match_tariff_key(utility: str, name: str) -> str | None:
    """Return tariff_key for (utility, rate name) in RI, or None if no mapping."""
    for util_pattern, name_pattern, tariff_key in GAS_URDB_TO_TARIFF_KEY_RI:
        if util_pattern not in utility:
            continue
        if re.search(name_pattern, name, re.IGNORECASE | re.DOTALL):
            return tariff_key
    return None

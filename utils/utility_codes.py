"""Central crosswalk for utility identifiers across data sources.

Single source of truth: UTILITIES list. All lookup tables are derived.
Add one record per utility; no duplication across modules.

Adding a new state
-----------------

1. Add records to UTILITIES (below) for each investor-owned utility in the state.
   Each record needs:
   - std_name: short identifier (lowercase, e.g. "xyz")
   - state: 2-letter state code (e.g. "MA")
   - fuels: ["electric"], ["gas"], or ["electric", "gas"]
   - display_name: human-readable name

2. Optional fields by data source:

   - ny_open_data_state_names: names from geometry/polygon data used to assign
     utility to buildings. For NY this comes from NY Open Data service-territory
     polygons. For other states, use the names from your state's equivalent
     source (e.g. PUC filings, service territory shapefiles), or [] if none.
     Used by assign_utility_<state> and get_ny_open_data_to_std_name().

   - eia_utility_ids: EIA-861 utility IDs for that utility in this state.
     Look up in EIA-861 data (e.g. Sales table) or use
     data/eia/861/fetch_electric_utility_stat_parquets.py to discover IDs.
     Enables get_eia_utility_id_to_std_name() and the utility_code column.

   - gas_tariff_key / electric_tariff_key: keys used in tariff filenames and
    tariff_map CSVs. Must match keys in rate_design/<state>/hp_rates/config/
    tariffs (electricity/ and gas/) and tariff_maps (electric/ and gas/).

   - rate_acuity_utility_names: names as they appear in the Rate Acuity gas
    history dropdown (one or more candidates). Used by fetch_gas_tariffs_rateacuity
    to resolve std_name to the exact dropdown string. Only set for utilities
    whose gas tariffs are fetched from Rate Acuity.

3. Create or extend assign_utility_<state> in data/resstock/ to map building
   locations to std_name, using ny_open_data_state_names (or your state's
   equivalent) and get_ny_open_data_to_std_name() / a state-specific lookup.

4. Update utils/types.py: add new std_names to ElectricUtility / GasUtility
   Literals. Keep them in sync with get_electric_std_names() / get_gas_std_names().

5. Add Justfile recipes in rate_design/<state>/hp_rates/ for
   map-electric-tariff and map-gas-tariff using the new std_names.
"""

from __future__ import annotations

from typing import TypedDict


class UtilityRecord(TypedDict, total=False):
    """One record per utility. All fields except std_name/state/fuels optional."""

    std_name: str
    state: str
    fuels: list[str]
    display_name: str
    ny_open_data_state_names: list[str]
    eia_utility_ids: list[int]
    gas_tariff_key: str
    electric_tariff_key: str
    rate_acuity_utility_names: list[str]


# Single list - user adds one record per utility
UTILITIES: list[UtilityRecord] = [
    {
        "std_name": "bath",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "Bath",
        "ny_open_data_state_names": ["Bath Electric Gas and Water"],
    },
    {
        "std_name": "cenhud",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "Central Hudson",
        "ny_open_data_state_names": ["Central Hudson Gas and Electric"],
        "eia_utility_ids": [3249],
        "gas_tariff_key": "cenhud",
        "electric_tariff_key": "cenhud",
        "rate_acuity_utility_names": [
            "Central Hudson Gas & Electric",
            "Central Hudson",
        ],
    },
    {
        "std_name": "chautauqua",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "Chautauqua",
        "ny_open_data_state_names": ["Chautauqua Utilities, Inc."],
    },
    {
        "std_name": "coned",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "Coned",
        "ny_open_data_state_names": ["Consolidated Edison"],
        "eia_utility_ids": [4226],
        "gas_tariff_key": "coned",
        "electric_tariff_key": "coned",
        "rate_acuity_utility_names": [
            "Consolidated Edison",
            "Consolidated Edison Company of New York",
        ],
    },
    {
        "std_name": "corning",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "Corning",
        "ny_open_data_state_names": ["Corning Natural Gas"],
    },
    {
        "std_name": "fillmore",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "Fillmore",
        "ny_open_data_state_names": ["Fillmore Gas Company"],
    },
    {
        "std_name": "kedny",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "National Grid - NYC",
        "ny_open_data_state_names": ["National Grid - NYC"],
        "gas_tariff_key": "national_grid",
        "rate_acuity_utility_names": [
            "The Brooklyn Union Gas Company",
            "Brooklyn Union",
        ],
    },
    {
        "std_name": "kedli",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "National Grid - Long Island",
        "ny_open_data_state_names": ["National Grid - Long Island"],
        "gas_tariff_key": "national_grid",
        "rate_acuity_utility_names": ["Keyspan Gas East", "KeySpan Gas East"],
    },
    {
        "std_name": "nimo",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "National Grid",
        "ny_open_data_state_names": ["National Grid"],
        "eia_utility_ids": [13573],  # Niagara Mohawk Power Corp.
        "gas_tariff_key": "national_grid",
        "electric_tariff_key": "nimo",
        "rate_acuity_utility_names": [
            "Niagara Mohawk Power Corporation",
            "Niagara Mohawk",
            "National Grid",
        ],
    },
    {
        "std_name": "none",
        "state": "NY",
        "fuels": [],
        "display_name": "None",
        "ny_open_data_state_names": ["None"],
    },
    {
        "std_name": "nfg",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "National Fuel Gas",
        "ny_open_data_state_names": ["National Fuel Gas Distribution"],
        "gas_tariff_key": "nfg",
        "rate_acuity_utility_names": [
            "National Fuel Gas",
            "National Fuel Gas Distribution",
        ],
    },
    {
        "std_name": "nyseg",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "NYSEG",
        "ny_open_data_state_names": ["NYS Electric and Gas"],
        "eia_utility_ids": [13511],
        "gas_tariff_key": "nyseg",
        "electric_tariff_key": "nyseg",
        "rate_acuity_utility_names": ["New York State Electric & Gas", "NYSEG"],
    },
    {
        "std_name": "or",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "Orange & Rockland",
        "ny_open_data_state_names": ["Orange and Rockland Utilities"],
        "eia_utility_ids": [14154],
        "gas_tariff_key": "or",
        "electric_tariff_key": "or",
        "rate_acuity_utility_names": [
            "Orange and Rockland Utilities",
            "Orange and Rockland",
            "O&R",
        ],
    },
    {
        "std_name": "psegli",
        "state": "NY",
        "fuels": ["electric"],
        "display_name": "LIPA",
        "ny_open_data_state_names": ["Long Island Power Authority"],
        "eia_utility_ids": [
            11171
        ],  # Long Island Power Authority (State; PSEG LI is operator)
        "electric_tariff_key": "psegli",
    },
    {
        "std_name": "reserve",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "Reserve",
        "ny_open_data_state_names": ["Reserve Gas Company"],
    },
    {
        "std_name": "rge",
        "state": "NY",
        "fuels": ["electric", "gas"],
        "display_name": "RG&E",
        "ny_open_data_state_names": ["Rochester Gas and Electric"],
        "eia_utility_ids": [16183],
        "gas_tariff_key": "rge",
        "electric_tariff_key": "rge",
        "rate_acuity_utility_names": [
            "Rochester Gas and Electric",
            "Rochester Gas and Electric Corporation",
            "RG&E",
        ],
    },
    {
        "std_name": "stlaw",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "St. Lawrence Gas",
        "ny_open_data_state_names": ["St. Lawrence Gas"],
    },
    {
        "std_name": "valley",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "Valley Energy",
        "ny_open_data_state_names": ["Valley Energy"],
    },
    {
        "std_name": "woodhull",
        "state": "NY",
        "fuels": ["gas"],
        "display_name": "Woodhull",
        "ny_open_data_state_names": ["Woodhull Municipal Gas Company"],
    },
    {
        "std_name": "rie",
        "state": "RI",
        "fuels": ["electric", "gas"],
        "display_name": "RIE",
        "ny_open_data_state_names": [],
        "eia_utility_ids": [13214],  # The Narragansett Electric Co
        "gas_tariff_key": "rie",
        "electric_tariff_key": "rie",
        "rate_acuity_utility_names": [
            "The Narragansett Electric Company",
            "Narragansett Electric",
            "RI Energy",
            "RIE",
        ],
    },
]


def get_rate_acuity_utility_names(state: str, std_name: str) -> list[str]:
    """Return Rate Acuity dropdown candidate names for a utility. Used by fetch_gas_tariffs_rateacuity."""
    state_upper = state.upper()
    for u in UTILITIES:
        if u.get("state") != state_upper or u["std_name"] != std_name:
            continue
        names = u.get("rate_acuity_utility_names")
        if not names:
            raise ValueError(
                f"No rate_acuity_utility_names for {std_name!r} in {state}. "
                "Add rate_acuity_utility_names to that utility in utils/utility_codes.py."
            )
        return names
    raise ValueError(
        f"Utility {std_name!r} not found for state {state}. "
        f"Valid gas std_names: {get_utilities_for_state(state_upper, 'gas')}"
    )


def get_ny_open_data_to_std_name() -> dict[str, str]:
    """Map NY Open Data polygon state_name -> std_name."""
    result: dict[str, str] = {}
    for u in UTILITIES:
        for name in u.get("ny_open_data_state_names", []):
            result[name] = u["std_name"]
    return result


def get_eia_utility_id_to_std_name(state: str) -> dict[int, str]:
    """Map EIA utility_id_eia -> std_name for a state."""
    state_upper = state.upper()
    result: dict[int, str] = {}
    for u in UTILITIES:
        if u.get("state") != state_upper:
            continue
        for eia_id in u.get("eia_utility_ids", []):
            result[eia_id] = u["std_name"]
    return result


def get_std_name_to_gas_tariff_key() -> dict[str, str]:
    """Map std_name -> gas tariff filename key."""
    result: dict[str, str] = {}
    for u in UTILITIES:
        if "gas_tariff_key" in u:
            result[u["std_name"]] = u["gas_tariff_key"]
    return result


def get_std_name_to_electric_tariff_key() -> dict[str, str]:
    """Map std_name -> electric tariff filename key."""
    result: dict[str, str] = {}
    for u in UTILITIES:
        if "electric_tariff_key" in u:
            result[u["std_name"]] = u["electric_tariff_key"]
    return result


def get_utilities_for_state(state: str, fuel: str | None = None) -> list[str]:
    """Return std_names for a state, optionally filtered by fuel."""
    state_upper = state.upper()
    result: list[str] = []
    for u in UTILITIES:
        if u.get("state") != state_upper:
            continue
        if fuel is None or fuel in u.get("fuels", []):
            result.append(u["std_name"])
    return sorted(result)


def std_name_to_display_name(std_name: str) -> str:
    """Return display_name for a std_name."""
    for u in UTILITIES:
        if u["std_name"] == std_name:
            return u.get("display_name", std_name)
    return std_name


def get_all_std_names() -> tuple[str, ...]:
    """All std_names. Returns tuple for Literal type inference."""
    return tuple(u["std_name"] for u in UTILITIES)


def get_electric_std_names() -> tuple[str, ...]:
    """std_names that serve electric. Returns tuple for Literal type inference."""
    result: list[str] = []
    for u in UTILITIES:
        if "electric" in u.get("fuels", []):
            result.append(u["std_name"])
    return tuple(sorted(result))


def get_gas_std_names() -> tuple[str, ...]:
    """std_names that serve gas. Returns tuple for Literal type inference."""
    result: list[str] = []
    for u in UTILITIES:
        if "gas" in u.get("fuels", []):
            result.append(u["std_name"])
    return tuple(sorted(result))

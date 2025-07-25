"""
Building characteristics parser and data structures for building-dependent parameters.
"""

import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional


@dataclass
class BuildingCharacteristics:
    """Building characteristics extracted from HPXML files and external data sources."""

    # From XML files
    state_code: str
    zip_code: str
    year_built: int
    n_residents: float
    n_bedrooms: int
    conditioned_floor_area: float
    residential_facility_type: str
    climate_zone: Optional[str] = None

    # From external data sources (ResStock, etc.)
    ami: Optional[float] = None  # Area Median Income
    wh_type: str = "storage"  # Default to storage water heater


def _get_text_or_default(parent: ET.Element, xpath: str, namespace: dict, default: str = "") -> str:
    """Get text from XML element or return default."""
    elem = parent.find(xpath, namespace)
    return elem.text or default if elem is not None else default


def parse_building_xml(xml_file_path: str) -> BuildingCharacteristics:
    """
    Parse building characteristics from HPXML file.

    Args:
        xml_file_path: Path to the HPXML building file

    Returns:
        BuildingCharacteristics object with parsed data
    """
    tree = ET.parse(xml_file_path)  # noqa: S314
    root = tree.getroot()
    namespace = {"hpxml": "http://hpxmlonline.com/2023/09"}

    # Extract location information
    address = root.find(".//hpxml:Address", namespace)
    state_code = _get_text_or_default(address, "hpxml:StateCode", namespace) if address is not None else ""
    zip_code = _get_text_or_default(address, "hpxml:ZipCode", namespace) if address is not None else ""

    # Extract building construction details with defaults
    construction = root.find(".//hpxml:BuildingConstruction", namespace)
    if construction is not None:
        year_built = int(_get_text_or_default(construction, "hpxml:YearBuilt", namespace, "2000"))
        n_bedrooms = int(_get_text_or_default(construction, "hpxml:NumberofBedrooms", namespace, "3"))
        conditioned_floor_area = float(
            _get_text_or_default(construction, "hpxml:ConditionedFloorArea", namespace, "1500.0")
        )
        residential_facility_type = _get_text_or_default(
            construction, "hpxml:ResidentialFacilityType", namespace, "single-family detached"
        )
    else:
        year_built, n_bedrooms, conditioned_floor_area, residential_facility_type = (
            2000,
            3,
            1500.0,
            "single-family detached",
        )

    # Extract occupancy information
    occupancy = root.find(".//hpxml:BuildingOccupancy", namespace)
    n_residents = (
        float(_get_text_or_default(occupancy, "hpxml:NumberofResidents", namespace, "2.0"))
        if occupancy is not None
        else 2.0
    )

    # Extract climate zone
    climate_zone = _get_text_or_default(root, ".//hpxml:ClimateZoneIECC/hpxml:ClimateZone", namespace) or None

    return BuildingCharacteristics(
        state_code=state_code,
        zip_code=zip_code,
        year_built=year_built,
        n_residents=n_residents,
        n_bedrooms=n_bedrooms,
        conditioned_floor_area=conditioned_floor_area,
        residential_facility_type=residential_facility_type,
        climate_zone=climate_zone,
    )


def get_ami_for_location(state_code: str, zip_code: str) -> Optional[float]:
    """
    Get Area Median Income for a given location.

    This function should be implemented to access ResStock AMI data or
    external datasets like Census ACS data.

    Args:
        state_code: Two-letter state code (e.g., "NJ", "NY")
        zip_code: 5-digit ZIP code

    Returns:
        AMI as a fraction of 80% AMI (e.g., 1.0 = 80% AMI, 1.25 = 100% AMI)
        None if data not available
    """
    # Placeholder implementation - should be replaced with actual data access
    # For now, return a default value based on state
    ami_defaults = {
        "NJ": 1.2,  # Above 80% AMI
        "NY": 1.1,  # Slightly above 80% AMI
        "CA": 0.9,  # Below 80% AMI
        "TX": 1.0,  # At 80% AMI
    }
    return ami_defaults.get(state_code, 1.0)  # Default to 80% AMI


def map_climate_zone_to_numeric(climate_zone: str) -> int:
    """
    Map IECC climate zone string to numeric zone for calculations.

    Args:
        climate_zone: IECC climate zone (e.g., "4A", "5B")

    Returns:
        Numeric climate zone (1-8)
    """
    if not climate_zone:
        return 4  # Default to zone 4

    # Extract numeric part of climate zone
    numeric_part = "".join(filter(str.isdigit, climate_zone))
    if numeric_part:
        return min(8, max(1, int(numeric_part)))
    else:
        return 4  # Default


def calculate_switching_cost_to(building_chars: BuildingCharacteristics, base_cost: float = 35.0) -> float:
    """
    Calculate building-specific switching cost from default to TOU schedule.

    Based on section 2.1 formula:
    C^{switch,to} = C * f_{AMI} * f_{age} * f_{residents} * f_{WH}

    Args:
        building_chars: Building characteristics
        base_cost: Base switching cost for average household ($)

    Returns:
        Building-specific switching cost ($)
    """
    ami = building_chars.ami or 1.0  # Default to 80% AMI if not available

    # f_AMI = sqrt(AMI / 0.8) - income factor
    f_ami = math.sqrt(ami / 0.8) if ami > 0 else 1.0

    # f_age = 1.0 + 0.005 * max(0, 2000 - YearBuilt) - building age proxy
    f_age = 1.0 + 0.005 * max(0, 2000 - building_chars.year_built)

    # f_residents = 1.0 + 0.1 * ln(N_residents) - coordination complexity
    f_residents = 1.0 + 0.1 * math.log(max(1.0, building_chars.n_residents))

    # f_WH = {1.0 (storage), 1.5 (tankless), 0.7 (heat pump)} - water heater complexity
    wh_factors = {"storage": 1.0, "tankless": 1.5, "heat_pump": 0.7}
    f_wh = wh_factors.get(building_chars.wh_type, 1.0)

    return base_cost * f_ami * f_age * f_residents * f_wh


def calculate_switching_cost_back(switching_cost_to: float) -> float:
    """
    Calculate cost to switch back from TOU to default schedule.

    Based on section 2.1: C^{switch,back} = 0.4 * C^{switch,to}

    Args:
        switching_cost_to: Cost to switch to TOU schedule

    Returns:
        Cost to switch back to default schedule ($)
    """
    return 0.4 * switching_cost_to


def calculate_comfort_penalty_factor(building_chars: BuildingCharacteristics, base_alpha: float = 0.15) -> float:
    """
    Calculate building-specific comfort penalty monetization factor.

    Based on section 2.1 formula:
    alpha = alpha_base * g_{AMI} * g_{residents} * g_{climate}

    Args:
        building_chars: Building characteristics
        base_alpha: Base comfort penalty factor ($/kWh)

    Returns:
        Building-specific comfort penalty factor ($/kWh)
    """
    ami = building_chars.ami or 1.0  # Default to 80% AMI if not available

    # g_AMI = (AMI / 0.8)^0.6 - income factor with diminishing returns
    g_ami = (ami / 0.8) ** 0.6 if ami > 0 else 1.0

    # g_residents = 1.0 + 0.2 * (N_residents - 1) - household size effect
    g_residents = 1.0 + 0.2 * max(0, building_chars.n_residents - 1)

    # g_climate = {0.8 (zones 1-3), 1.0 (zones 4-5), 1.2 (zones 6-8)} - climate factor
    climate_zone = map_climate_zone_to_numeric(building_chars.climate_zone or "4A")
    if climate_zone <= 3:
        g_climate = 0.8
    elif climate_zone <= 5:
        g_climate = 1.0
    else:
        g_climate = 1.2

    return base_alpha * g_ami * g_residents * g_climate


def enrich_building_characteristics(building_chars: BuildingCharacteristics) -> BuildingCharacteristics:
    """
    Enrich building characteristics with external data sources.

    Args:
        building_chars: Basic building characteristics from XML

    Returns:
        Enhanced building characteristics with AMI and other data
    """
    # Get AMI data for the location
    ami = get_ami_for_location(building_chars.state_code, building_chars.zip_code)
    building_chars.ami = ami

    # Additional enrichment could include:
    # - Water heater type from OCHRE simulation data
    # - Additional demographic data from ResStock
    # - Local utility rate data

    return building_chars

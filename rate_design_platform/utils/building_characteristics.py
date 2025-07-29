"""
Building characteristics parser and data structures for building-dependent parameters.
"""

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

    # Define namespace for HPXML files
    namespace = {"hpxml": "http://hpxmlonline.com/2023/09"}

    # Extract location information
    address = root.find(".//hpxml:Address", namespace)
    if address is not None:
        state_code_elem = address.find("hpxml:StateCode", namespace)
        zip_code_elem = address.find("hpxml:ZipCode", namespace)
        state_code = state_code_elem.text if state_code_elem is not None else ""
        zip_code = zip_code_elem.text if zip_code_elem is not None else ""
    else:
        state_code = ""
        zip_code = ""

    # Extract building construction details
    building_construction = root.find(".//hpxml:BuildingConstruction", namespace)
    if building_construction is not None:
        year_built = int(building_construction.find("hpxml:YearBuilt", namespace).text)
        n_bedrooms = int(building_construction.find("hpxml:NumberofBedrooms", namespace).text)
        conditioned_floor_area = float(building_construction.find("hpxml:ConditionedFloorArea", namespace).text)
        residential_facility_type = building_construction.find("hpxml:ResidentialFacilityType", namespace).text
    else:
        # Fallback defaults
        year_built = 2000
        n_bedrooms = 3
        conditioned_floor_area = 1500.0
        residential_facility_type = "single-family detached"

    # Extract occupancy information
    building_occupancy = root.find(".//hpxml:BuildingOccupancy", namespace)
    if building_occupancy is not None:
        n_residents_elem = building_occupancy.find("hpxml:NumberofResidents", namespace)
        n_residents = float(n_residents_elem.text) if n_residents_elem is not None else 2.0
    else:
        n_residents = 2.0

    # Extract climate zone if available
    climate_zone_elem = root.find(".//hpxml:ClimateZoneIECC/hpxml:ClimateZone", namespace)
    climate_zone = climate_zone_elem.text if climate_zone_elem is not None else None

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

#!/usr/bin/env python3
"""
Test file for utils/building_characteristics.py module.
"""

from rate_design_platform.utils.building_characteristics import (
    BuildingCharacteristics,
    enrich_building_characteristics,
    get_ami_for_location,
    map_climate_zone_to_numeric,
    parse_building_xml,
)


def test_parse_building_xml():
    """Test parsing building characteristics from XML files."""
    xml_path = "rate_design_platform/inputs/bldg0000072-up00.xml"
    building = parse_building_xml(xml_path)

    assert building.state_code == "NJ"
    assert building.zip_code == "07960"
    assert building.year_built == 1940
    assert building.n_residents == 2.0
    assert building.n_bedrooms == 4
    assert building.conditioned_floor_area == 1228.0
    assert building.residential_facility_type == "single-family detached"


def test_get_ami_for_location():
    """Test AMI lookup by location."""
    # Test known states
    assert get_ami_for_location("NJ", "07960") == 1.2
    assert get_ami_for_location("NY", "11758") == 1.1
    assert get_ami_for_location("CA", "90210") == 0.9
    assert get_ami_for_location("TX", "78701") == 1.0

    # Test unknown state (should default to 1.0)
    assert get_ami_for_location("ZZ", "00000") == 1.0


def test_map_climate_zone_to_numeric():
    """Test climate zone string to numeric mapping."""
    assert map_climate_zone_to_numeric("4A") == 4
    assert map_climate_zone_to_numeric("5B") == 5
    assert map_climate_zone_to_numeric("1") == 1
    assert map_climate_zone_to_numeric("8C") == 8
    assert map_climate_zone_to_numeric("") == 4  # Default
    assert map_climate_zone_to_numeric(None) == 4  # Default
    assert map_climate_zone_to_numeric("ABC") == 4  # No numeric part


# Removed obsolete function tests - calculate_switching_cost_to, calculate_switching_cost_back,
# and calculate_comfort_penalty_factor no longer exist in the refactored code


def test_enrich_building_characteristics():
    """Test building characteristics enrichment with external data."""
    # Create basic building characteristics
    building = BuildingCharacteristics(
        state_code="NJ",
        zip_code="07960",
        year_built=1940,
        n_residents=2.0,
        n_bedrooms=4,
        conditioned_floor_area=1228.0,
        residential_facility_type="single-family detached",
        climate_zone="4A",
    )

    # Should start with no AMI data
    assert building.ami is None

    # Enrich the data
    enriched = enrich_building_characteristics(building)

    # Should now have AMI data
    assert enriched.ami is not None
    assert enriched.ami == 1.2  # Expected for NJ


def test_building_characteristics_dataclass():
    """Test BuildingCharacteristics dataclass creation."""
    building = BuildingCharacteristics(
        state_code="CA",
        zip_code="90210",
        year_built=2000,
        n_residents=4.0,
        n_bedrooms=5,
        conditioned_floor_area=2500.0,
        residential_facility_type="single-family detached",
        climate_zone="3B",
        ami=0.8,
        wh_type="heat_pump",
    )

    assert building.state_code == "CA"
    assert building.zip_code == "90210"
    assert building.year_built == 2000
    assert building.n_residents == 4.0
    assert building.n_bedrooms == 5
    assert building.conditioned_floor_area == 2500.0
    assert building.residential_facility_type == "single-family detached"
    assert building.climate_zone == "3B"
    assert building.ami == 0.8
    assert building.wh_type == "heat_pump"

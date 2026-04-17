# Approximating Non-HP Loads in `approximate_non_hp_load.py`

This document explains the high-level methodology implemented in `utils/pre/approximate_non_hp_load.py`.

The script creates synthetic heat-pump versions of selected buildings by borrowing HVAC behavior from similar buildings and then relabeling the selected buildings as heat-pump homes in metadata.

## What Problem This Script Solves

In ResStock 2024.2, heat-pump upgrades are not applied to every dwelling. The documentation for Measure Package 2, which corresponds to the cold-climate air-source heat pump upgrade used here, says the package only applies to some existing heating-fuel types and excludes some shared-HVAC cases. In particular, high-rise dwellings with shared HVAC are excluded, and ducted heat-pump portions do not apply to shared-HVAC dwellings.

As a result, some buildings in the upgrade 2 data do not receive a modeled heat-pump upgrade directly from ResStock, even though downstream Switchbox analysis may still want to include them in a heat-pump-oriented rate-design workflow.

This script fills that gap by:

1. Identifying buildings that should receive an approximated heat-pump profile.
2. Finding similar donor buildings.
3. Replacing the target building's HVAC behavior with an average of those donors.
4. Updating metadata so the target building is treated as a heat-pump building downstream.

In short, this is a pragmatic approximation layer used to extend heat-pump-style load shapes to buildings that were not directly upgraded in the ResStock release.

## Which Buildings Are Targeted

The script can approximate two groups.

### Non-HP Multifamily Buildings

These are buildings where:

- `postprocess_group.has_hp == False`
- `in.geometry_building_type_height` contains `"Multifamily"`

This is the main group motivated by the ResStock upgrade exclusions described above.

### Buildings in an "Other Fuel Type" Bucket

These are buildings where:

- `postprocess_group.has_hp == False`
- all of these are `False`:
  - `heats_with_electricity`
  - `heats_with_natgas`
  - `heats_with_oil`
  - `heats_with_propane`

The script can run on either group or both together.

## Core Methodology

The method is a nearest-neighbor HVAC substitution procedure.

For each target building, the script:

1. Restricts candidate donor buildings to the same weather station.
2. Measures similarity between the target building and each candidate donor.
3. Keeps the `k` nearest donors.
4. Replaces the target building's HVAC-related hourly values with the average of those donors.
5. Updates metadata so the building is treated as a heat-pump building.

This is not a full re-simulation of the building. It is a targeted substitution of HVAC behavior.

## How Similarity Is Defined

By default, similarity is based on the building's hourly heating load shape.

The script compares the target building to candidate donors using hourly load-shape RMSE and selects the `k` lowest-error matches. Cooling can also be included in the matching logic, but the main execution path currently uses heating-only matching.

The main modeling assumption is:

- buildings exposed to the same weather station are the right donor pool
- buildings with similar hourly heating shapes are reasonable HVAC donors for one another

## What Gets Replaced

The script does not overwrite the entire building record. It replaces only HVAC-related hourly behavior.

At a high level, it rewrites:

- heating and cooling delivered load
- HVAC-related electricity use
- HVAC-related natural gas, fuel oil, and propane use
- associated total fuel-use fields that need to stay internally consistent after the HVAC swap

Conceptually, the target building keeps its non-HVAC characteristics, while its HVAC profile is transplanted from the average of similar donor buildings.

## Metadata Updates

After the hourly HVAC behavior is rewritten, the script updates metadata so downstream analysis interprets the building as a heat-pump case.

At a high level, it:

- marks the building as having a heat pump
- updates heating-fuel flags toward an electric heat-pump interpretation
- updates HVAC descriptor fields to heat-pump-style labels
- adds an `approximated_hp_load` flag so the building can be identified later as synthetic rather than directly modeled by ResStock

The script also checks whether the rewritten hourly data still implies natural-gas use and uses that to update the natural-gas connection flag.

## End Result

The output is a modified version of the selected buildings in which:

- the building still represents the same dwelling sample
- the HVAC-related hourly behavior now looks like a heat-pump case derived from similar donor buildings
- the metadata is aligned with that interpretation

This lets downstream billing and rate-design workflows include buildings that ResStock did not directly upgrade under the chosen heat-pump package.

## Validation Logic in the Script

The file also includes validation utilities, although they are not run in the main execution path.

The basic validation idea is:

1. Take buildings that already do have heat pumps.
2. Pretend they need to be approximated.
3. Reconstruct them using the nearest-neighbor method.
4. Compare the reconstructed HVAC behavior to the real one.

This provides a way to evaluate whether the nearest-neighbor approximation is a reasonable proxy.

## Bottom Line

`approximate_non_hp_load.py` is a nearest-neighbor HVAC substitution pipeline. It uses similar same-weather buildings as donors, averages their HVAC profiles, writes that averaged HVAC behavior onto selected target buildings, and then updates metadata so those buildings can participate in downstream heat-pump analysis.

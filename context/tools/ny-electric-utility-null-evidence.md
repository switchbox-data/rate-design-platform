# Why are there no null electric utilities in NY utility assignment?

## Evidence

1. **Code does not drop rows.**\
   `assign_utility_ny` joins `building_utilities` to `input_metadata` with `how="left"` and asserts equal row counts. Every metadata building stays in the output.

2. **Code can assign null electric.**\
   For electric, we call `_calculate_utility_probabilities(..., include_municipal=False, filter_none=True)`, so muni-* and "none" are removed. In `_sample_utility_per_building`, if a building’s PUMA has no remaining utilities (all probs 0 or NaN), `sample_utility` returns `None`, so that building gets `sb.electric_utility = null`.

3. **NY utility assignment has no null electric.**\
   On NY upgrade 00: metadata and utility assignment both have 33,790 rows and the same `bldg_id`s. Distinct non-null `sb.electric_utility` are only the seven IOUs (`cenhud`, `coned`, `nimo`, `nyseg`, `or`, `psegli`, `rge`). Count of `sb.electric_utility` null is 0.

4. **So no building is in a “muni-only” situation in the data.**\
   For every building to get a non-null electric utility, every PUMA must have at least one IOU in the electric probability table after filtering. So either no NY PUMA is muni-only in the polygon overlap, or something else is going on.

## Question

**Why does every NY building have a non-null electric utility?**\
Is it because (a) in the current NY Open Data polygons, every PUMA really overlaps at least one IOU, or (b) the run that produced this utility assignment used different options (e.g. `include_municipal=True`), or (c) there is another code path or fallback that assigns an IOU when the probability table is empty for a PUMA?

Checking whether any PUMA has zero rows in the electric-probability table after filtering (and whether the script was ever run with `include_municipal=True`) would answer this.

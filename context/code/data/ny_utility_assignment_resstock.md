# ResStock utility assignment

How electric and gas utilities are assigned to ResStock buildings — generic architecture, NY-specific implementation, and instructions for adding new states.

**Use when:** Working on utility assignment for any state, adding a new state, excluded gas utility handling, PUMA–utility overlap, or ResStock metadata columns `sb.electric_utility` / `sb.gas_utility`.

---

## Overview

- **Entrypoint:** `assign_utility_ny()` in `data/resstock/utility/assign_utility_ny.py` (and CLI via `assign_utility_ny.py`). This is a thin wrapper that builds the NY utility-name crosswalk and passes NY-specific configuration (utility name map, excluded gas utilities, state CRS) to the generic `create_hh_utilities()` in `data/resstock/utility/utils.py`.
- **Inputs:** ResStock metadata (with `in.puma`, `in.heating_fuel`, `has_natgas_connection`), electric and gas utility service-territory polygons (CSV with WKT), Census PUMAs (pygris).
- **Outputs:** Same metadata with `sb.electric_utility` and `sb.gas_utility` added (or overwritten).
- **Logic:** PUMA–utility overlap → PUMA-level probability tables → per-building sampling (deterministic seed). Electric: every building gets an electric utility. Gas: only buildings with `has_natgas_connection` get a gas utility; others get null.
- **Generic functions:** `create_hh_utilities()`, `zero_excluded_gas_utilities_and_renormalize()`, `calculate_puma_utility_overlap()`, `calculate_utility_probabilities()`, `calculate_prior_distributions()`, `sample_utility_per_building()`, `print_comparison_summary()`, `puma_id_series_for_join()`, `read_csv_to_gdf_from_s3()` all live in `data/resstock/utility/utils.py` and are state-generic.

---

## Excluded gas utilities

A fixed set of **excluded gas utilities** are excluded from assignment: their prior probability is set to zero before sampling.

- **Constant:** `EXCLUDED_GAS_UTILITIES` in `assign_utility_ny.py` (loaded from `data/resstock/state_configs.yaml` → `NY.excluded_gas_utilities`):
  - `bath`, `chautauqua`, `corning`, `fillmore`, `reserve`, `stlaw`
- **Rationale:** These utilities have very few customers; we do not assign ResStock buildings to them for rate-design/BAT purposes.
- **Implementation:** `zero_excluded_gas_utilities_and_renormalize()` (in `data/resstock/utility/utils.py`), called from `create_hh_utilities()` when `excluded_gas_utilities` is non-empty:
  1. Set to 0 the probability columns whose name is in `excluded_utilities`.
  2. For each PUMA row, if the remaining (non-excluded) gas probabilities sum to zero, the PUMA is "bad" and must be handled (see below).
  3. Otherwise, renormalize each row so gas probabilities sum to 1.
  4. Final gas probability table is used by `sample_utility_per_building(..., only_when_fuel="Natural Gas")`.

---

## PUMAs with zero gas probability after zeroing

If, for a given PUMA, **all** gas probability was in excluded utilities, then after zeroing that PUMA has no gas utility left. Two behaviors:

1. **`pumas` not provided:** `zero_excluded_gas_utilities_and_renormalize(..., pumas=None)` raises `ValueError` with the affected `puma_id`(s).
2. **`pumas` provided (GeoDataFrame):** A **donor** PUMA is chosen and its gas probability row is used for the bad PUMA.
   - **Donor selection:** Prefer a **good** PUMA (non-zero gas probability after exclusion) that is **adjacent** to the bad PUMA (geometries touch). Among adjacent good PUMAs, choose the one whose centroid is closest to the bad PUMA's centroid. If no adjacent good PUMA exists, use the good PUMA with the nearest centroid (fallback).
   - **Result:** The bad PUMA's row is replaced by the donor's gas probability row; then all rows are renormalized so each sums to 1.
   - **Debug:** When a donor is used, the script prints the bad PUMA id, donor PUMA id, whether it was "adjacent (touching boundary)" or "no adjacent PUMA with gas; using nearest by centroid (fallback)", distance, and -- if `puma_and_heating_fuel` is provided -- how many gas buildings (`has_natgas_connection`) are in the bad PUMA. It also prints the excluded utilities that were zeroed and their prior probabilities, and the resulting distribution after the nearest-neighbor approximation.

---

## PUMA ID normalization

PUMA identifiers can appear as integers or strings (e.g. `100` vs `"00100"`). For consistent matching between the gas-probability table and the PUMAs GeoDataFrame:

- **`puma_id_series_for_join(pumas)`** (in `data/resstock/utility/utils.py`) returns a pandas Series of 5-character zero-padded PUMA ids derived from `pumas`:
  - If `PUMACE10` exists: `pumas["PUMACE10"].astype(str).str.zfill(5)`.
  - Else if `GEOID` exists: last 5 characters of `GEOID`.
  - Else `None`.
- Bad/donor PUMA matching in `zero_excluded_gas_utilities_and_renormalize` uses this normalization (e.g. `str(bad_puma_id).zfill(5)`) so that geometry lookups and probability row replacement are consistent.

---

## Renormalization

After zeroing excluded gas utilities (and optionally replacing bad-PUMA rows with donor rows), **every row** of the gas probability table is renormalized so that the sum of all utility columns for that row equals 1. So each PUMA's gas distribution is a valid probability distribution for sampling.

---

## Invocation and data flow

**Recommended (via main.py):** Utility assignment runs as step 2b inside `data/resstock/main.py` (`_assign_utility` function), immediately after metadata transforms (step 2a) and before load curve modifications. It operates directly on the `_sb` release -- reads `metadata-sb.parquet` from the `_sb` tree (after all metadata transforms have been applied), routes to `assign_utility()` in `data/resstock/utility/assign_utility.py` which loads state configuration from `state_configs.yaml` internally, and writes `metadata_utility/state=NY/utility_assignment.parquet` into the `_sb` tree on local EBS, then uploads immediately to S3 via `aws s3 cp`. No separate copy step is needed. See `context/code/data/resstock_sb_release_pipeline_main_py.md` for details.

**State support:** NY is included in `SUPPORTED_UTILITY_STATES` because its entry in `data/resstock/state_configs.yaml` contains both `electric_poly_filename` and `gas_poly_filename` keys with non-null values. The polygon filenames default from this config; they can be overridden at the CLI via `--electric-poly-filename` / `--gas-poly-filename`. Pre-flight validation (`validate_utility_assignment_args`) checks that all requested states are in `SUPPORTED_UTILITY_STATES` before any data processing begins.

**Legacy (individual Justfile recipe):** `assign-utility-ny` in `data/resstock/Justfile` downloads NY polygons, then calls `assign_utility_ny.py` directly with S3 paths. In the old workflow this ran on the **standard** release (step 3), and the output was brought into `_sb` by the copy step (step 4). These individual recipes are still available for debugging.

- **Run order (legacy):** After `identify-hp-and-heating-type-all-upgrades-and-natgas-connection` (metadata has `has_natgas_connection` and `in.puma`). See `context/code/data/resstock_data_preparation_run_order.md`.
- **Output column file:** `metadata_utility/state=NY/utility_assignment.parquet` -- contains only `bldg_id`, `sb.electric_utility`, `sb.gas_utility`.

---

## Tests

`tests/test_assign_utility_ny.py` covers:

- `EXCLUDED_GAS_UTILITIES` constant (loaded from `state_configs.yaml`).
- `puma_id_series_for_join` (PUMACE10, GEOID, missing columns).
- `zero_excluded_gas_utilities_and_renormalize`: no excluded cols (unchanged); zero + renormalize; bad PUMA with `pumas=None` (raises); bad PUMA with `pumas` and touching geometries (donor used, row sums to 1).
- Other helpers: `calculate_utility_probabilities`, `calculate_prior_distributions`, `sample_utility_per_building` (determinism, gas only when `has_natgas_connection`, etc.).

---

## Adding a new state

There are two patterns: **GIS-based** (like NY — PUMA overlap + probabilistic sampling) and **rule-based** (like RI — deterministic assignment, no GIS). Follow the checklist for the appropriate pattern.

### Step 1 — `data/resstock/state_configs.yaml`

Add an entry keyed by the 2-letter state code. All states need:

```yaml
XX:
  state_fips: "00"           # 2-digit FIPS string, zero-padded
  add_vulnerability_columns: true   # or false — whether to run PUMS vulnerability logic
  electric_poly_filename: xx_electric_utilities_YYYYMMDD.csv  # empty if rule-based
  gas_poly_filename: xx_gas_utilities_YYYYMMDD.csv            # empty if rule-based
```

GIS-based states also need:

```yaml
state_crs: 0000            # EPSG code of a projected CRS for area calculations (e.g. 2260 for NY)
puma_year: 2019            # Census PUMA vintage year (used by pygris)
excluded_gas_utilities:    # optional — list of gas utility names to zero before sampling
  - smallutil1
```

**How `SUPPORTED_UTILITY_STATES` is derived:** `assign_utility.py` reads `state_configs.yaml` at import time and includes every state whose entry contains both `electric_poly_filename` and `gas_poly_filename` keys (even if the values are null, as for RI). Adding the keys is enough; no manual set update is needed.

### Step 2 — `data/resstock/utility/assign_utility_{xx}.py`

Create a new state-specific module. Choose the pattern that matches:

**GIS-based (NY pattern):** Build a state-specific `utility_name_map` (a Polars LazyFrame mapping raw polygon names → standardised names), load `EXCLUDED_GAS_UTILITIES` from `state_configs.yaml`, then delegate to the generic `create_hh_utilities()`:

```python
from data.resstock.utils import load_state_configs, select_puma_and_heating_fuel_metadata
from data.resstock.utility.utils import create_hh_utilities

EXCLUDED_GAS_UTILITIES: frozenset[str] = frozenset(
    load_state_configs()["XX"].get("excluded_gas_utilities", [])
)

def assign_utility_xx(input_metadata, electric_polygons, gas_polygons, pumas, config):
    puma_and_heating_fuel = select_puma_and_heating_fuel_metadata(input_metadata)
    utility_name_map = ...  # state-specific name crosswalk as pl.LazyFrame
    building_utilities = create_hh_utilities(
        puma_and_heating_fuel=puma_and_heating_fuel,
        electric_polygons=electric_polygons,
        gas_polygons=gas_polygons,
        pumas=pumas,
        utility_name_map=utility_name_map,
        state_crs=config["state_crs"],
        excluded_gas_utilities=EXCLUDED_GAS_UTILITIES,
    )
    # join building_utilities back to input_metadata (see assign_utility_ny.py for the full pattern)
    ...
```

**Rule-based (RI pattern):** Directly return a LazyFrame with `sb.electric_utility` and `sb.gas_utility` derived from metadata columns without GIS data. See `assign_utility_ri.py` as a reference.

### Step 3 — `data/resstock/utility/assign_utility.py`

1. Import the new function at the top:

   ```python
   from data.resstock.utility.assign_utility_xx import assign_utility_xx
   ```

2. Add a branch inside `assign_utility()` before the final `raise ValueError`:

   **GIS-based:**

   ```python
   if state == "XX":
       config = _STATE_CONFIGS["XX"]
       elec_file = electric_poly_filename or config.get("electric_poly_filename")
       gas_file = gas_poly_filename or config.get("gas_poly_filename")
       if not path_s3_gis_dir:
           raise ValueError("--path-s3-gis-dir is required for XX utility assignment.")
       if not elec_file:
           raise ValueError("--electric-poly-filename is required for XX utility assignment.")
       if not gas_file:
           raise ValueError("--gas-poly-filename is required for XX utility assignment.")
       gis_base = S3Path(path_s3_gis_dir.rstrip("/"))
       electric_polygons = read_csv_to_gdf_from_s3(gis_base / elec_file, utility_type="electric", state_crs=config["state_crs"])
       gas_polygons = read_csv_to_gdf_from_s3(gis_base / gas_file, utility_type="gas", state_crs=config["state_crs"])
       print("    Loading Census PUMA shapefiles via pygris...", flush=True)
       pumas = cast(gpd.GeoDataFrame, get_pumas(state=state, year=config["puma_year"], cb=True))
       pumas = pumas.to_crs(epsg=config["state_crs"])
       return assign_utility_xx(input_metadata=metadata, electric_polygons=electric_polygons, gas_polygons=gas_polygons, pumas=pumas, config=config)
   ```

   **Rule-based:**

   ```python
   if state == "XX":
       return assign_utility_xx(metadata)
   ```

### Step 4 — Tests

Add `tests/test_assign_utility_{xx}.py`. At minimum cover:

- Any state-specific constants (e.g. `EXCLUDED_GAS_UTILITIES` loaded correctly from `state_configs.yaml`).
- The core assignment function with a small synthetic LazyFrame (no real S3 data).
- Edge cases relevant to the state (e.g. buildings without gas, missing puma values).

For GIS-based states, the generic helpers (`calculate_puma_utility_overlap`, `sample_utility_per_building`, etc.) are already tested in `tests/test_assign_utility_ny.py`; focus new tests on state-specific logic.

### Checklist summary

| Step | File                                                  | Action                                                                                                  |
| ---- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 1    | `data/resstock/state_configs.yaml`                    | Add state entry with required keys (polygon filenames enable `SUPPORTED_UTILITY_STATES` auto-inclusion) |
| 2    | `data/resstock/utility/assign_utility_{xx}.py`        | Create state module (GIS or rule-based pattern)                                                         |
| 3    | `data/resstock/utility/assign_utility.py`             | Import new function; add `if state == "XX":` branch                                                     |
| 4    | `tests/test_assign_utility_{xx}.py`                   | Add unit tests                                                                                          |
| 5    | `context/code/data/ny_utility_assignment_resstock.md` | Update the NY overview section if new generic helpers were added to `utils.py`                          |

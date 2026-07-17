# ResStock utility assignment

How electric and gas utilities are assigned to ResStock buildings — generic architecture, state-specific implementations (NY, MD, RI), and instructions for adding new states.

**Use when:** Working on utility assignment for any state, adding a new state, excluded gas utility handling, PUMA–utility overlap, nearest-neighbor PUMA fill, or ResStock metadata columns `sb.electric_utility` / `sb.gas_utility`.

---

## Overview

- **Dispatcher:** `data/resstock/utility/assign_utility.py` — reads `state_configs.yaml`, dynamically imports the state-specific module, and calls its `assign_utility(metadata, **kwargs)` entry point. No `if state == "XX":` branches; adding a config entry and a module is enough.
- **State modules:** `assign_utility_ny.py` (GIS-based, HIFLD electric + gas, name crosswalk, excluded utilities), `assign_utility_md.py` (GIS-based, EIA-861 county polygons for electric + HIFLD for gas, nearest-neighbor PUMA fill for gas), `assign_utility_ri.py` (rule-based, no GIS).
- **Inputs:** ResStock metadata (with `in.puma`, `in.heating_fuel`, `has_natgas_connection`), electric and gas utility service-territory polygons (CSV with WKT), Census PUMAs (pygris).
- **Outputs:** Same metadata with `sb.electric_utility` and `sb.gas_utility` added (or overwritten).
- **Logic:** PUMA–utility overlap → PUMA-level probability tables → per-building sampling (deterministic seed). Electric: every building gets an electric utility. Gas: only buildings with `has_natgas_connection` get a gas utility; others get null.
- **Generic functions:** `create_hh_utilities()`, `zero_excluded_gas_utilities_and_renormalize()`, `fill_missing_puma_probabilities()`, `calculate_puma_utility_overlap()`, `calculate_utility_probabilities()`, `calculate_prior_distributions()`, `sample_utility_per_building()`, `print_comparison_summary()`, `puma_id_series_for_join()`, `read_csv_to_gdf_from_s3()` all live in `data/resstock/utility/utils.py` and are state-generic.

---

## Excluded gas utilities

A fixed set of **excluded gas utilities** are excluded from assignment: their prior probability is set to zero before sampling.

- **Constant:** `EXCLUDED_GAS_UTILITIES` in `assign_utility_ny.py` (loaded from `data/resstock/state_configs.yaml` → `NY.utility_assignment.kwargs.excluded_gas_utilities`):
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

**Recommended (via main.py):** Utility assignment runs as step 2b inside `data/resstock/main.py` (`_assign_utility` function), immediately after metadata transforms (step 2a) and before load curve modifications. It operates directly on the `_sb` release — reads `metadata-sb.parquet` from the `_sb` tree (after all metadata transforms have been applied), routes to `assign_utility()` in `data/resstock/utility/assign_utility.py` which loads state configuration from `state_configs.yaml` internally, and writes `metadata_utility/state=<XX>/utility_assignment.parquet` into the `_sb` tree on local EBS, then uploads immediately to S3 via `aws s3 cp`. No separate copy step is needed. See `context/code/data/resstock_sb_release_pipeline_main_py.md` for details.

**State support:** A state is included in `SUPPORTED_UTILITY_STATES` when its entry in `data/resstock/state_configs.yaml` contains a `utility_assignment` key. The `utility_assignment` section specifies a `module` (dotted Python module path) and optional `kwargs` (passed to the module's `assign_utility()` function). GIS-based states store polygon filenames, CRS, and PUMA year under `kwargs`; CLI flags `--electric-poly-filename` / `--gas-poly-filename` / `--path-s3-gis-dir` override or supplement these at runtime. Pre-flight validation (`validate_utility_assignment_args`) checks that all requested states are in `SUPPORTED_UTILITY_STATES` before any data processing begins.

**Legacy (individual Justfile recipe):** `assign-utility-ny` in `data/resstock/Justfile` downloads NY polygons, then calls `assign_utility_ny.py` directly with S3 paths. In the old workflow this ran on the **standard** release (step 3), and the output was brought into `_sb` by the copy step (step 4). These individual recipes are still available for debugging.

- **Run order (legacy):** After `identify-hp-and-heating-type-all-upgrades-and-natgas-connection` (metadata has `has_natgas_connection` and `in.puma`). See `context/code/data/resstock_data_preparation_run_order.md`.
- **Output column file:** `metadata_utility/state=<XX>/utility_assignment.parquet` — contains only `bldg_id`, `sb.electric_utility`, `sb.gas_utility`.

---

## Tests

`tests/test_assign_utility_ny.py` covers:

- `EXCLUDED_GAS_UTILITIES` constant (loaded from `state_configs.yaml`).
- `puma_id_series_for_join` (PUMACE10, GEOID, missing columns).
- `zero_excluded_gas_utilities_and_renormalize`: no excluded cols (unchanged); zero + renormalize; bad PUMA with `pumas=None` (raises); bad PUMA with `pumas` and touching geometries (donor used, row sums to 1).
- Other helpers: `calculate_utility_probabilities`, `calculate_prior_distributions`, `sample_utility_per_building` (determinism, gas only when `has_natgas_connection`, etc.).

---

## Maryland (MD)

MD follows the same GIS-based pattern as NY — PUMA overlap → probability table → per-building sampling — with one structural difference in the electric side: instead of HIFLD utility polygons, MD uses **Census county polygons weighted by EIA Form 861 service territory data**. Gas assignment continues to use HIFLD polygon CSVs, identical to NY.

### Why not HIFLD for electric

HIFLD is missing three of the five major MD investor-owned utilities — Pepco, Potomac Edison, and Delmarva Power — because those utilities never submitted their boundary shapes to the HIFLD portal. The 2024 HIFLD snapshot for MD covers only BGE, SMECO, Choptank, and a few small municipals, representing roughly 40% of MD customers. The other 60% would fall back to the nearest HIFLD polygon (BGE), producing systematically wrong assignments for all of Montgomery County, Prince George's County, the Eastern Shore, and western MD.

PJM does not distribute service territory shapefiles (FERC critical infrastructure policy). The Maryland PSC publishes utility reports but no GIS data. No other federal or state source publishes complete sub-county boundaries for all MD utilities.

The most complete authoritative source with full utility coverage is **EIA Form 861 Schedule 8**, which requires every distribution utility to report the counties it serves. PUDL processes this into `core_eia861__yearly_service_territory`, available via HTTPS at the same S3 bucket as the EIA-861 sales data already used in this pipeline.

### Electric utility assignment: county-weighted PUMA overlap

**Data sources**

- **EIA-861 county service territory:** PUDL `core_eia861__yearly_service_territory.parquet` (PUDL stable release v2026.2.0). Maps each utility to the counties it serves. MD has 24 counties + Baltimore City; the 2023 data has 46 (county, utility) rows — many counties are served by more than one utility.
- **EIA-861 utility stats:** Our existing `s3://data.sb/eia/861/electric_utility_stats/` (year=2023/state=MD). Provides statewide residential customer counts used to weight split counties.
- **Census county polygons:** `pygris.counties(state="MD", year=2019)` — standard TIGER/Line county boundaries, 2019 vintage to match the PUMA year.

**Pre-processing: `data/eia/861/fetch_service_territory.py`**

Run once (or annually) to produce:

```
s3://data.sb/eia/861/service_territory/state=MD/data.parquet
```

Schema: `county_id_fips`, `county`, `utility_id_eia`, `utility_name_eia`, `residential_customers`, `weight`, `report_year`.

- Only distribution utilities with residential customers > 0 are included (retail marketers and power marketers are excluded).
- `weight` normalises `residential_customers` within each county so weights sum to 1.0. For single-utility counties, weight = 1.0. For split counties, each utility gets its share of statewide MD residential customers as a proxy.

Invoked via:

```
just -f data/eia/861/Justfile fetch-service-territory MD
```

**Runtime: `assign_utility_md.py`**

`assign_utility()` calls `calculate_puma_county_utility_overlap()` (in `utils.py`) instead of `calculate_puma_utility_overlap()`. The function:

1. Projects PUMAs and county polygons to `state_crs` (2248).
2. Computes the intersection area of every (PUMA, county) pair via `gpd.overlay(..., how="intersection")`.
3. For each (PUMA, county) pair, fans out to one row per utility serving that county, with `pct_overlap = overlap_area × weight / puma_area × 100`.
4. Groups by (PUMA, utility) and sums — so a PUMA spanning multiple counties accumulates weighted contributions from each.
5. Returns a LazyFrame with `puma_id`, `utility`, `pct_overlap` — identical format to `calculate_puma_utility_overlap`, plugging directly into the existing probability and sampling machinery.

**Granularity and accuracy**

PUMAs that straddle a county line get geographic signal from both sides. A PUMA in western Frederick County (split BGE/Potomac Edison) that also overlaps Washington County (Potomac Edison-only) accumulates extra weight toward Potomac Edison; one in eastern Frederick that overlaps Howard County (BGE-only) tilts toward BGE. For PUMAs entirely inside a split county, the statewide customer-share proxy applies.

Major utilities covered (2023 EIA-861, MD distribution utilities):

| Utility                           | EIA ID | Std name       | Residential customers |
| --------------------------------- | ------ | -------------- | --------------------- |
| Baltimore Gas & Electric Co       | 1167   | `bge`          | 1,208k                |
| Potomac Electric Power Co (Pepco) | 15270  | `pepco`        | 548k                  |
| The Potomac Edison Company        | 15263  | `poted`        | 253k                  |
| Delmarva Power                    | 5027   | `dpl`          | 185k                  |
| Southern Maryland Elec Coop       | 17637  | `smeco`        | 159k                  |
| Choptank Electric Cooperative     | 3503   | `choptank`     | ~30k                  |
| Somerset Rural Electric Coop      | 84     | `somerset_rec` | small                 |
| Town of Berlin (MD)               | 1615   | `berlin_muni`  | small                 |

**EIA utility ID → std name mapping** is defined in `_EIA_ID_TO_STD_NAME` in `assign_utility_md.py`. Utilities not in the map fall back to the EIA name string.

### Gas utility assignment

Unchanged from the original HIFLD-based approach:

- **Source:** HIFLD Open "Natural Gas Service Territories" feature layer (archived at DataLumos). Fetch via `load_utility_boundaries()`.
- **Cached** as dated WKT CSV in `s3://data.sb/gis/utility_boundaries/`; filename in `state_configs.yaml` under `MD.utility_assignment.kwargs.gas_poly_filename`.
- **MD LDCs present** (as of 2024 snapshot):
  - Baltimore Gas and Electric Co — 92.9% of natgas-connected buildings
  - Columbia Gas of Washington/Maryland — 3.4%
  - Sand-Piper Energy — 2.3%
  - Easton Utilities — 1.3%
  - Elkton Gas Company — 0.1%

### PUMA boundaries

- **Source:** `pygris.pumas(state="MD", year=2019, cb=True)`.
- **Vintage:** 2019 — 2010-definition PUMAs. MD has 44 PUMAs.
- **CRS:** `state_crs: 2248` — NAD83 / Maryland State Plane (feet).
- **Load/cache:** via `load_pumas()` in `utils.py` (local cache → S3 → pygris fallback).

### Nearest-neighbor PUMA fill

County polygons cover all of Maryland's land area by definition, so the electric assignment has no coverage gaps — every PUMA overlaps at least one county. The nearest-neighbor fill (`fill_missing_puma_probabilities`) is still called for electric as a safety net but should produce zero fills in practice.

Gas coverage gaps remain (HIFLD gas boundaries still have the same rural/suburban gaps as before), so the nearest-neighbor fill is genuinely needed for gas, with the same behavior as documented in the previous HIFLD-based approach.

### Excluded gas utilities

`excluded_gas_utilities` for MD (in `state_configs.yaml`):

- `easton_muni` — Easton Utilities municipal gas LDC; too small for ResStock
  rate-design sampling. Still present in `utility_codes.py` (tariffs, EIA IDs,
  HIFLD names). Assignment zeros its PUMA gas probability and renormalizes;
  donor-PUMA fill applies if a PUMA would otherwise have no gas utility left.

### State_configs.yaml for MD

```yaml
MD:
  state_fips: "24"
  add_vulnerability_columns: false
  utility_assignment:
    module: data.resstock.utility.assign_utility_md
    kwargs:
      state_crs: 2248
      puma_year: 2019
      excluded_gas_utilities:
        - easton_muni
```

### Full MD pipeline (start to finish)

```bash
# 1. Fetch EIA-861 utility stats to S3 (if not already current)
just -f data/eia/861/Justfile update

# 2. Fetch county service territory weights to S3 (electric assignment data)
just -f data/eia/861/Justfile fetch-service-territory MD

# 3. Download ResStock metadata for MD
just s MD fetch-resstock-metadata

# 4. Run utility assignment
just s MD assign-utility

# 5. Upload utility assignment to S3
just s MD upload-utility-assignment
```

### Output

`metadata_utility/state=MD/utility_assignment.parquet` — `bldg_id`, `sb.electric_utility`, `sb.gas_utility`. Written to local EBS and uploaded to `s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/metadata_utility/state=MD/utility_assignment.parquet`.

---

## Adding a new state

There are two patterns: **GIS-based** (like NY — PUMA overlap + probabilistic sampling) and **rule-based** (like RI — deterministic assignment, no GIS). Follow the checklist for the appropriate pattern.

Routing is fully dynamic: `assign_utility.py` reads the `module` from `state_configs.yaml`, imports it via `importlib`, and calls `mod.assign_utility(metadata, **kwargs)`. No `if state == "XX":` branches are needed — just a config entry and a module.

### Step 1 — `data/resstock/state_configs.yaml`

Add an entry keyed by the 2-letter state code. Every state needs `state_fips` and a `utility_assignment` section with at least a `module` pointing to the state's module. GIS-based states put their parameters in `kwargs`; rule-based states can omit `kwargs` entirely.

**Rule-based state (e.g. RI):**

```yaml
XX:
  state_fips: "00"
  add_vulnerability_columns: false
  utility_assignment:
    module: data.resstock.utility.assign_utility_xx
```

**GIS-based state (e.g. NY):**

```yaml
XX:
  state_fips: "00"
  add_vulnerability_columns: true
  utility_assignment:
    module: data.resstock.utility.assign_utility_xx
    kwargs:
      state_crs: 0000            # EPSG code for area calculations
      puma_year: 2019            # Census PUMA vintage (used by pygris)
      electric_poly_filename: xx_electric_utilities_YYYYMMDD.csv
      gas_poly_filename: xx_gas_utilities_YYYYMMDD.csv
      excluded_gas_utilities:    # optional — gas utility names to zero before sampling
        - smallutil1
```

**How `SUPPORTED_UTILITY_STATES` is derived:** `assign_utility.py` reads `state_configs.yaml` at import time and includes every state whose entry contains a `utility_assignment` key. Adding the key is enough; no manual set update is needed.

**How kwargs are dispatched:** `assign_utility.py` merges the YAML `kwargs` with any CLI overrides (`path_s3_gis_dir`, `electric_poly_filename`, `gas_poly_filename`) before calling the state module's `assign_utility()`. CLI values replace the YAML defaults when provided.

### Step 2 — `data/resstock/utility/assign_utility_{xx}.py`

Create a new state-specific module that exposes an `assign_utility(metadata, **kwargs)` function. This is the entry point called by the dynamic dispatch.

**GIS-based (NY pattern):** The `assign_utility()` function receives all kwargs from YAML + CLI overrides. It loads GIS data (polygon CSVs from S3, PUMAs via pygris), builds the state-specific utility name crosswalk, and delegates to `create_hh_utilities()`:

```python
from data.resstock.utils import load_state_configs, select_puma_and_heating_fuel_metadata
from data.resstock.utility.utils import create_hh_utilities, read_csv_to_gdf_from_s3

EXCLUDED_GAS_UTILITIES: frozenset[str] = frozenset(
    (load_state_configs()["XX"]["utility_assignment"].get("kwargs") or {}).get(
        "excluded_gas_utilities", []
    )
)

def assign_utility(
    metadata: pl.LazyFrame,
    *,
    path_s3_gis_dir: str,
    electric_poly_filename: str,
    gas_poly_filename: str,
    state_crs: int,
    puma_year: int,
    excluded_gas_utilities: list[str] | None = None,
) -> pl.LazyFrame:
    # Load polygon CSVs from S3, fetch PUMAs via pygris, build name map,
    # call create_hh_utilities(), join back to metadata.
    # See assign_utility_ny.py for the full pattern.
    ...
```

**Rule-based (RI pattern):** The `assign_utility()` function ignores extra kwargs and directly computes the assignment:

```python
def assign_utility(metadata: pl.LazyFrame, **_kwargs: Any) -> pl.LazyFrame:
    return assign_utility_xx(metadata)
```

### Step 3 — Tests

Add `tests/test_assign_utility_{xx}.py`. At minimum cover:

- Any state-specific constants (e.g. `EXCLUDED_GAS_UTILITIES` loaded correctly from `state_configs.yaml`).
- The core assignment function with a small synthetic LazyFrame (no real S3 data).
- Edge cases relevant to the state (e.g. buildings without gas, missing puma values).

For GIS-based states, the generic helpers (`calculate_puma_utility_overlap`, `sample_utility_per_building`, etc.) are already tested in `tests/test_assign_utility_ny.py`; focus new tests on state-specific logic.

### Checklist summary

| Step | File                                           | Action                                                                    |
| ---- | ---------------------------------------------- | ------------------------------------------------------------------------- |
| 1    | `data/resstock/state_configs.yaml`             | Add state entry with `utility_assignment.module` (and `kwargs` for GIS)   |
| 2    | `data/resstock/utility/assign_utility_{xx}.py` | Create state module with `assign_utility(metadata, **kwargs)` entry point |
| 3    | `tests/test_assign_utility_{xx}.py`            | Add unit tests                                                            |

No changes to `data/resstock/utility/assign_utility.py` are needed — it dynamically imports the module specified in `state_configs.yaml`.

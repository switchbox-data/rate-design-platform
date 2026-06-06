# ResStock utility assignment

How electric and gas utilities are assigned to ResStock buildings — generic architecture, state-specific implementations (NY, MD, RI), and instructions for adding new states.

**Use when:** Working on utility assignment for any state, adding a new state, excluded gas utility handling, PUMA–utility overlap, nearest-neighbor PUMA fill, or ResStock metadata columns `sb.electric_utility` / `sb.gas_utility`.

---

## Overview

- **Dispatcher:** `data/resstock/utility/assign_utility.py` — reads `state_configs.yaml`, dynamically imports the state-specific module, and calls its `assign_utility(metadata, **kwargs)` entry point. No `if state == "XX":` branches; adding a config entry and a module is enough.
- **State modules:** `assign_utility_ny.py` (GIS-based, name crosswalk, excluded utilities), `assign_utility_md.py` (GIS-based, HIFLD names directly, nearest-neighbor PUMA fill), `assign_utility_ri.py` (rule-based, no GIS).
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

MD follows the same GIS-based pattern as NY — PUMA overlap → probability table → per-building sampling — with two key differences: no utility name crosswalk is needed (HIFLD names are used directly), and a **nearest-neighbor PUMA fill** is required because HIFLD boundaries leave 10 of 44 MD PUMAs (~23%) with no electric coverage and a different set of 10 with no gas coverage.

### Data sources

**Electric service territory polygons**

- **Source:** HIFLD Open "Electric Retail Service Territories" feature layer. The DHS HIFLD Open portal was deactivated on **August 26, 2025**; the dataset is now archived at DataLumos: <https://www.datalumos.org/datalumos/project/239091>. Data vintage: September 2024 snapshot.
- **Fetched by:** `load_utility_boundaries()` in `data/resstock/utility/utils.py`, which tries a list of ArcGIS REST endpoints (`HIFLD_ELEC_URLS`) in order and falls back to DataLumos if all fail.
- **Cached locally** as a dated WKT CSV (e.g. `md_electric_utilities_20260605.csv`), uploaded to `s3://data.sb/gis/utility_boundaries/`, and the filename is stored in `state_configs.yaml` under `MD.utility_assignment.kwargs.electric_poly_filename`. Subsequent runs read directly from S3 without re-fetching.
- **MD utilities present** (as of 2024 snapshot, 7 features after filtering to state=MD):
  - Baltimore Gas & Electric Co (BGE) — 74.6% of buildings
  - Choptank Electric Cooperative, Inc — 9.6%
  - Southern Maryland Electric Coop Inc (SMECO) — 7.2%
  - Hagerstown Light Department — 4.6%
  - Thurmont Municipal Light Co — 3.9%
  - Town of Williamsport (MD) — 0.1%
  - Easton Utilities Comm — <0.1%

**Gas (LDC) service territory polygons**

- **Source:** HIFLD Open "Natural Gas Service Territories" feature layer. Same portal deactivation; archived at DataLumos: <https://portal.datarescueproject.org/datasets/hifld-open-natural-gas-service-territories/>. The fetch code (`HIFLD_GAS_URLS`) tries live ArcGIS endpoints first and falls back to a locally cached DataLumos ZIP (`HIFLD_GAS_DATALUMOS_ZIP`) as a last resort.
- **Fetched and cached** identically to electric: dated WKT CSV → S3 → `state_configs.yaml` (`gas_poly_filename`).
- **MD LDCs present** (as of 2024 snapshot, 5 features after filtering):
  - Baltimore Gas and Electric Co — 92.9% of natgas-connected buildings
  - Columbia Gas of Washington/Maryland — 3.4%
  - Sand-Piper Energy — 2.3%
  - Easton Utilities — 1.3%
  - Elkton Gas Company — 0.1%

**PUMA boundaries**

- **Source:** Census TIGER/Line PUMA shapefiles fetched on demand via **pygris** (`pygris.pumas(state="MD", year=2019, cb=True)`).
- **Vintage:** `puma_year: 2019` — the 2010-definition PUMAs used throughout ResStock `res_2024_amy2018_2`. MD has 44 PUMAs total.
- **No separate upload to S3:** pygris is called fresh each run; the result is projected to `state_crs` in-memory. (Contrast with NY, which caches PUMAs as a local shapefile.)
- **CRS:** `state_crs: 2248` — NAD83 / Maryland State Plane (feet). All area calculations for PUMA–utility overlap are done in this CRS.

**No PUMS microdata used**

`add_vulnerability_columns: false` in `state_configs.yaml` means the ACS PUMS-based vulnerability columns (LMI, has_child_under_6, etc.) are **not** added for MD. The Census PUMA _boundaries_ (from pygris) are used for GIS assignment, but ACS PUMS _person/housing microdata_ is not used.

### Utility name crosswalk

MD has **no name crosswalk** (`utility_name_map` is an empty DataFrame). HIFLD names are written to `sb.electric_utility` and `sb.gas_utility` verbatim. This differs from NY, which maps HIFLD names to standardised Switchbox names.

### No excluded gas utilities

`excluded_gas_utilities` is not set for MD (not in `state_configs.yaml`). All HIFLD LDCs in MD are assigned; none are zeroed before sampling.

### Nearest-neighbor PUMA fill

HIFLD boundaries do not cover the full land area of Maryland: 10 of 44 PUMAs have no electric coverage and 10 (a partly different set) have no gas coverage. Without a fill, ~25.8% of MD buildings (2,575 of 9,996) would be unassigned.

**Why the gaps exist** — HIFLD utility boundaries are compiled from data submitted voluntarily by utilities; there is no federal mandate for every utility to provide precise GIS polygons. Common gap sources:

1. **Municipal utilities and co-ops** that don't submit to national databases (state PUC filings are the authoritative source, but are not aggregated nationally).
2. **County-level EIA-861 reporting** — some HIFLD features are derived from the EIA-861 survey, which maps utilities to counties rather than to precise polygon boundaries, leaving sub-county gaps.
3. **Real physical gaps in gas distribution** — rural and exurban areas of MD genuinely have no gas LDC (residents use propane/oil), so no polygon is expected there.
4. **HIFLD portal deactivation** — the portal was shut down in August 2025; the 2024 snapshot is the final maintained version.

**Analysis of unassigned buildings (before fill):**

Of the 2,575 buildings that would have been unassigned for at least one utility:

- 100% use electricity (every ResStock building has electrical load).
- 59.2% have `has_natgas_connection = True` — i.e. the gap is not just rural/no-gas areas; many are urban/suburban Baltimore-area PUMAs where BGE or SMECO should cover them but HIFLD polygons are missing.

Per-utility breakdown:

| Utility type | Unassigned PUMAs | Unassigned buildings | % with natgas |
| ------------ | ---------------- | -------------------- | ------------- |
| Electric     | 10 of 44         | 2,142                | 61.6%         |
| Gas          | 10 of 44         | 2,162                | 61.8%         |

**Fix:** `fill_missing_puma_probabilities()` in `data/resstock/utility/utils.py` is called when `fill_missing_pumas=True` (set in `assign_utility_md.py`). For each uncovered PUMA:

1. Find all covered PUMAs whose geometry **touches** (shares a boundary with) the uncovered PUMA.
2. Among touching PUMAs, pick the one whose centroid is **nearest** to the uncovered PUMA's centroid.
3. If no touching covered PUMA exists, fall back to the globally nearest covered PUMA by centroid distance.
4. Copy the donor's full probability distribution to the uncovered PUMA.

After the fill, 0 buildings are unassigned for either utility. This function is state-generic and can be reused for any state with coverage gaps.

**Post-fill assignment verified** (`dev_no_commit.py`):

```
PASS — all 9,996 buildings have sb.electric_utility assigned.
PASS — all 5,231 natgas-connected buildings have sb.gas_utility assigned.
```

### Output

`metadata_utility/state=MD/utility_assignment.parquet` — slim file with only `bldg_id`, `sb.electric_utility`, `sb.gas_utility`. Written to `res_2024_amy2018_2_sb/metadata_utility/state=MD/` on local EBS and uploaded to `s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/metadata_utility/state=MD/utility_assignment.parquet`.

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

# NY utility assignment (ResStock)

How electric and gas utilities are assigned to ResStock buildings in NY: `data/resstock/assign_utility_ny.py` and related Justfile recipes.

**Use when:** Working on NY utility assignment, small gas utility handling, PUMA–utility overlap, or ResStock metadata columns `sb.electric_utility` / `sb.gas_utility`.

---

## Overview

- **Entrypoint:** `assign_utility_ny()` (and CLI via `assign_utility_ny.py`).
- **Inputs:** ResStock metadata (with `in.puma`, `in.heating_fuel`, `has_natgas_connection`), electric and gas utility service-territory polygons (CSV with WKT), Census PUMAs (pygris).
- **Outputs:** Same metadata with `sb.electric_utility` and `sb.gas_utility` added (or overwritten).
- **Logic:** PUMA–utility overlap → PUMA-level probability tables → per-building sampling (deterministic seed). Electric: every building gets an electric utility. Gas: only buildings with `has_natgas_connection` get a gas utility; others get null.

---

## Small gas utilities

A fixed set of **small gas utilities** are excluded from assignment: their prior probability is set to zero before sampling.

- **Constant:** `SMALL_GAS_UTILITIES` in `assign_utility_ny.py`:
  - `bath`, `chautauqua`, `corning`, `fillmore`, `reserve`, `stlaw`
- **Rationale:** These utilities have very few customers; we do not assign ResStock buildings to them for rate-design/BAT purposes.
- **Implementation:** `_zero_small_gas_utilities_and_renormalize()`:
  1. Set to 0 the probability columns whose name is in `SMALL_GAS_UTILITIES`.
  2. For each PUMA row, if the remaining (non-small) gas probabilities sum to zero, the PUMA is “bad” and must be handled (see below).
  3. Otherwise, renormalize each row so gas probabilities sum to 1.
  4. Final gas probability table is used by `_sample_utility_per_building(..., only_when_fuel="Natural Gas")`.

---

## PUMAs with zero gas probability after zeroing

If, for a given PUMA, **all** gas probability was in small utilities, then after zeroing that PUMA has no gas utility left. Two behaviors:

1. **`pumas` not provided:** `_zero_small_gas_utilities_and_renormalize(..., pumas=None)` raises `ValueError` with the affected `puma_id`(s).
2. **`pumas` provided (GeoDataFrame):** A **donor** PUMA is chosen and its gas probability row is used for the bad PUMA.
   - **Donor selection:** Prefer a **good** PUMA (non-zero gas probability after zeroing) that is **adjacent** to the bad PUMA (geometries touch). Among adjacent good PUMAs, choose the one whose centroid is closest to the bad PUMA’s centroid. If no adjacent good PUMA exists, use the good PUMA with the nearest centroid (fallback).
   - **Result:** The bad PUMA’s row is replaced by the donor’s gas probability row; then all rows are renormalized so each sums to 1.
   - **Debug:** When a donor is used, the script prints the bad PUMA id, donor PUMA id, whether it was “adjacent (touching boundary)” or “no adjacent PUMA with gas; using nearest by centroid (fallback)”, distance, and—if `puma_and_heating_fuel` is provided—how many gas buildings (`has_natgas_connection`) are in the bad PUMA. It also prints the small utilities that were zeroed and their prior probabilities, and the resulting distribution after the nearest-neighbor approximation.

---

## PUMA ID normalization

PUMA identifiers can appear as integers or strings (e.g. `100` vs `"00100"`). For consistent matching between the gas-probability table and the PUMAs GeoDataFrame:

- **`_puma_id_series_for_join(pumas)`** returns a pandas Series of 5-character zero-padded PUMA ids derived from `pumas`:
  - If `PUMACE10` exists: `pumas["PUMACE10"].astype(str).str.zfill(5)`.
  - Else if `GEOID` exists: last 5 characters of `GEOID`.
  - Else `None`.
- Bad/donor PUMA matching in `_zero_small_gas_utilities_and_renormalize` uses this normalization (e.g. `str(bad_puma_id).zfill(5)`) so that geometry lookups and probability row replacement are consistent.

---

## Renormalization

After zeroing small gas utilities (and optionally replacing bad-PUMA rows with donor rows), **every row** of the gas probability table is renormalized so that the sum of all utility columns for that row equals 1. So each PUMA’s gas distribution is a valid probability distribution for sampling.

---

## Justfile and data flow

- **Recipe:** `assign-utility-ny` in `data/resstock/Justfile` (download polygons, then call `assign_utility_ny.py` with S3 paths for metadata and polygon CSVs).
- **Run order:** After `identify-hp-and-heating-type-all-upgrades-and-natgas-connection` (metadata has `has_natgas_connection` and `in.puma`). See `context/code/data/resstock_data_preparation_run_order.md` step 3.
- **Output:** Written to the standard release’s `metadata_utility` path; the copy step then brings `metadata_utility` into the sb release. The script’s `sink_parquet` call may be commented out for local testing.

---

## Tests

`tests/test_assign_utility_ny.py` covers:

- `SMALL_GAS_UTILITIES` constant.
- `_puma_id_series_for_join` (PUMACE10, GEOID, missing columns).
- `_zero_small_gas_utilities_and_renormalize`: no small cols (unchanged); zero + renormalize; bad PUMA with `pumas=None` (raises); bad PUMA with `pumas` and touching geometries (donor used, row sums to 1).
- Other helpers: `_calculate_utility_probabilities`, `_calculate_prior_distributions`, `_sample_utility_per_building` (determinism, gas only when `has_natgas_connection`, etc.).

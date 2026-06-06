# ResStock data preparation: run order

This document describes how to prepare a Switchbox `_sb` ResStock release from the standard NREL release. The end state covers: HP identification, non-HP approximation, multifamily electricity adjustment, utility assignment, monthly load curves (aggregated from hourly for gas/oil/propane billing), and upload to S3.

**The recommended approach is `data/resstock/main.py`** (run via the Justfile recipes in the "Complete, all-in-one recipes" section). It consolidates the entire pipeline into a single invocation with provenance tracking. The individual Justfile recipes documented below are retained for debugging and partial re-runs.

All commands assume the project root as the current working directory unless noted. ResStock Justfile: `data/resstock/Justfile`. Default release names: standard `res_2024_amy2018_2`, sb release `res_2024_amy2018_2_sb`.

---

## Recommended approach: main.py

`main.py` runs the full pipeline end-to-end on the local EBS mount. All steps are enabled by default and controlled by flags. See `context/code/data/resstock_sb_release_pipeline_main_py.md` for full documentation.

### Justfile structure

The ResStock data pipeline uses a three-tier Justfile structure:

1. **`data/resstock/Justfile`** — State-generic recipes. Every recipe takes `state` as an argument. Config values (release, paths, etc.) are read via the `data.resstock.constants` CLI module. This is the primary interface for running the pipeline.

2. **`rate_design/hp_rates/ny/Justfile`** — NY-specific wrappers (prefixed `resstock-*`). Pin NY defaults (state code, upgrade IDs, polygon filenames from `state_configs.yaml`) and delegate to the generic recipes. Also contain NY-only operations like polygon downloads.

3. **`rate_design/hp_rates/ri/Justfile`** — RI-specific wrappers (prefixed `resstock-*`). Same pattern as NY but for RI. RI uses rule-based utility assignment (no polygon files).

### Invocation examples

**Via state-specific Justfile (recommended for state work):**

```bash
# NY — full pipeline
just -f rate_design/hp_rates/ny/Justfile resstock-run-pipeline

# RI — full pipeline
just -f rate_design/hp_rates/ri/Justfile resstock-run-pipeline
```

**Via generic Justfile (for any state, any flags):**

```bash
# Full pipeline
just -f data/resstock/Justfile run-pipeline NY --upgrade-ids 0 2

# Generic with overrides
just -f data/resstock/Justfile run-pipeline NY \
    --upgrade-ids 0 2 \
    --electric-poly-filename ny_electric_utilities_20260309.csv \
    --gas-poly-filename ny_gas_utilities_20260309.csv \
    --sample 10
```

**Key differences from the old Justfile workflow:**

| Old workflow                                                     | main.py                                                                                                                                 |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| Ran each step against S3 separately                              | Writes everything to local EBS, uploads at the end                                                                                      |
| Utility assignment on the standard release, then copied to `_sb` | Utility assignment runs directly on `_sb`                                                                                               |
| Required `sudo aws s3 sync` before monthly loads                 | No intermediate S3 sync needed                                                                                                          |
| 9 separate Justfile invocations                                  | 1 invocation with manifest-based provenance                                                                                             |
| No crash recording                                               | Crash recording: Justfile wrapper, manual CLI, and startup safety net (see `resstock_sb_release_pipeline_main_py.md § Crash recording`) |
| State-specific recipes in generic Justfile                       | Generic Justfile is fully state-agnostic; state recipes in `rate_design/hp_rates/{state}/`                                              |

---

## Individual step recipes (for debugging and partial re-runs)

---

## 1. Fetch standard NREL release data

Download metadata, hourly load curves, and annual load curves for the state(s) from the standard ResStock release (via bsf).

```bash
just -f data/resstock/Justfile fetch <STATE>
```

- **Example:** `just -f data/resstock/Justfile fetch NY` or `fetch RI`.
- **Result:** Data under `s3://data.sb/nrel/resstock/` (or local `path_local_parquet` if configured) for release `res_2024_amy2018_2`, all upgrade IDs (00–05), file types metadata, load_curve_hourly, load_curve_annual.

---

## 2. Prepare metadata in the standard release

Run the metadata preparation chain on the standard release before utility assignment and copy.

**Via state-specific Justfile:**

```bash
# NY (includes vulnerability columns by default)
just -f rate_design/hp_rates/ny/Justfile resstock-prepare-metadata

# NY without vulnerability columns
just -f rate_design/hp_rates/ny/Justfile resstock-prepare-metadata lmi="false"

# RI (no vulnerability columns — RI default)
just -f rate_design/hp_rates/ri/Justfile resstock-prepare-metadata
```

**Via generic Justfile (metadata chain only, without utility assignment):**

```bash
just -f data/resstock/Justfile identify-all-metadata <STATE>
```

- **What it does:** Runs, in order, across all upgrade IDs:
  - `identify-hp-customers` (`metadata.parquet` → `metadata-sb.parquet`)
  - `identify-heating-type` (`metadata-sb.parquet` in place)
  - `identify-natgas-connection` (`metadata-sb.parquet` in place)
- **Result:** Standard release metadata under `.../metadata/state=<STATE>/upgrade=<ID>/` now has `metadata-sb.parquet` with postprocess-group and related columns.

---

## 3. Add utility assignment (standard release)

Assign electric and gas utilities to buildings in the **standard** release so that downstream steps and the `sb` copy use utility-aware metadata. Run once for upgrade `00`; the assignments should remain constant across upgrades.
For state-specific details on utility assignment (excluded gas utilities, nearest-neighbor PUMA fill, HIFLD data sources), see `context/code/data/utility_assignment_resstock.md`.

**Via state-specific Justfile:**

```bash
# NY — downloads fresh polygons then runs GIS-based assignment
just -f rate_design/hp_rates/ny/Justfile resstock-assign-utility

# NY — override polygon filenames
just -f rate_design/hp_rates/ny/Justfile resstock-assign-utility \
    electric_poly="ny_electric_utilities_20260601.csv" \
    gas_poly="ny_gas_utilities_20260601.csv"

# RI — rule-based assignment (no polygons)
just -f rate_design/hp_rates/ri/Justfile resstock-assign-utility
```

- **NY:** `resstock-assign-utility` runs `download-ny-utility-polygons` first, then calls `assign_utility_ny.py`. Polygon filenames default from `state_configs.yaml`.
- **RI:** `resstock-assign-utility` calls `assign_utility_ri.py` directly (single-utility rule-based assignment).
- **Result:** `.../res_2024_amy2018_2/metadata_utility/state=<STATE>/` contains utility assignment for that release.

---

## 4. Copy standard release -> sb release

Copy the relevant release and upgrade(s) from the standard release to the new `sb` release, including `metadata_utility` so the `sb` release gets utility assignment by copy.

**Via state-specific Justfile (defaults: upgrades 00+02, metadata+metadata_utility+load_curve_hourly):**

```bash
just -f rate_design/hp_rates/ny/Justfile resstock-copy
just -f rate_design/hp_rates/ri/Justfile resstock-copy
```

**Via generic Justfile:**

```bash
just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb <STATE> "<UPGRADE_IDS>" "<FILE_TYPES>"
```

- **Result:** `s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/` has the same structure and files for the chosen state and upgrade(s), including metadata_utility. Use `metadata-sb.parquet` and other files as the source for step 5.

---

## 5. Approximate non-HP load in the sb release

Run the non-HP load approximation for the target upgrade in the `sb` release. This reads from the **input** release (standard) for neighbor search and writes updated load curves and metadata to the **output** (`sb`) release.

```bash
just -f data/resstock/Justfile approximate-non-hp-load <STATE> <UPGRADE_ID> res_2024_amy2018_2 res_2024_amy2018_2_sb <K> <UPDATE_MF> <UPDATE_OTHER_FUEL>
```

- **Example (NY, upgrade 02, k=15, update MF and other fuel types):**\
  `just -f data/resstock/Justfile approximate-non-hp-load NY 02 res_2024_amy2018_2 res_2024_amy2018_2_sb 15 True True`
- **Arguments:**
  - `K`: number of nearest neighbors (e.g. 15).
  - `UPDATE_MF`: `True` to update non-HP MF buildings.
  - `UPDATE_OTHER_FUEL`: `True` to update “other fuel type” non-HP buildings.
- **Result:** In `res_2024_amy2018_2_sb`, load curves and metadata for the selected non-HP buildings are replaced with approximated (HP-like) loads and metadata (postprocess_group.has_hp, heating_type, has_natgas_connection, etc.). In the current end-to-end shortcut recipes, this is run for upgrade `02`.

---

## 6. Adjust multifamily electricity in the sb release

After approximation, run the multifamily non-HVAC electricity adjustment for both upgrades `00` and `02` in the `sb` release.

**Via state-specific Justfile:**

```bash
just -f rate_design/hp_rates/ny/Justfile resstock-adjust-mf-electricity
just -f rate_design/hp_rates/ri/Justfile resstock-adjust-mf-electricity
```

**Via generic Justfile:**

```bash
just -f data/resstock/Justfile adjust-mf-electricity <STATE> res_2024_amy2018_2 res_2024_amy2018_2_sb "<UPGRADE_IDS>"
```

- **What it does:** Runs `utils/pre/adjust_mf_electricity.py` against the `sb` release.
- **Current end-to-end behavior:** Upgrade `00` and upgrade `02` are both adjusted.

---

## 7. Sync the finished sb release to EBS

After approximation and multifamily-electricity adjustment, sync the finished `sb` release from S3 to local EBS so it is available for rate-design runs and for building monthly load curves.

```bash
sudo aws s3 sync s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/ /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/
```

- **Result:** Local path `/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/` mirrors the sb release (including `load_curve_hourly`) for use by CAIRO and by step 8.

---

## 8. Add monthly load curves (EBS)

Aggregate hourly load curves to monthly on local EBS (fast). Uses the same column-level aggregation rules as buildstock-fetch (sum for energy/emissions, mean for load/temperature). Output is written under `load_curve_monthly/state=<STATE>/upgrade=<ID>/` with one parquet per building (`{bldg_id}-{upgrade}.parquet`), consumed by gas/oil/propane bill logic in post-processing.

**Via state-specific Justfile:**

```bash
just -f rate_design/hp_rates/ny/Justfile resstock-add-monthly-loads
just -f rate_design/hp_rates/ri/Justfile resstock-add-monthly-loads
```

**Via generic Justfile:**

```bash
just -f data/resstock/Justfile add-monthly-loads <STATE> "<UPGRADE_IDS>"
```

- **Input:** `path_local_parquet` (default `/ebs/data/nrel/resstock/`, read from `config.yaml`) — expects `load_curve_hourly/state=<STATE>/upgrade=<ID>/` from step 7.
- **Output:** `load_curve_monthly/state=<STATE>/upgrade=<ID>/` on EBS (12 rows per building, 140 columns).
- **Result:** Local EBS has monthly load curves; upload to S3 in step 9 so the sb release on S3 is complete.

---

## 9. Upload monthly load curves to S3

Sync the monthly load curves from EBS to S3 so the sb release on S3 includes `load_curve_monthly` for downstream (e.g. `build_master_bills`, gas/delivered-fuel bills).

**Via state-specific Justfile:**

```bash
just -f rate_design/hp_rates/ny/Justfile resstock-upload-monthly-loads
just -f rate_design/hp_rates/ri/Justfile resstock-upload-monthly-loads
```

**Via generic Justfile:**

```bash
just -f data/resstock/Justfile upload-monthly-loads <STATE> "<UPGRADE_IDS>"
```

- **Result:** `s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/load_curve_monthly/state=<STATE>/upgrade=<ID>/` is populated; the sb release on S3 is complete for rate-design and post-processing.

---

## End-to-end shortcuts

Both invoke `main.py` via the generic `run-pipeline` recipe. See the "Recommended approach" section at the top for full details.

**Via state-specific Justfile (simplest):**

```bash
just -f rate_design/hp_rates/ny/Justfile resstock-run-pipeline
just -f rate_design/hp_rates/ri/Justfile resstock-run-pipeline
```

**Via generic Justfile:**

```bash
just -f data/resstock/Justfile run-pipeline <STATE> --upgrade-ids 0 2 [flags]
```

---

## Summary checklist

**Recommended (single invocation via main.py):**

| Action                                                                              |
| ----------------------------------------------------------------------------------- |
| `just -f rate_design/hp_rates/ny/Justfile resstock-run-pipeline` (or `.../ri/...`)  |
| Or: `just -f data/resstock/Justfile run-pipeline <STATE> --upgrade-ids 0 2 [flags]` |

**Individual Justfile recipes (for debugging or partial re-runs):**

| Step | State-specific recipe                                        | Generic recipe                                                    |
| ---- | ------------------------------------------------------------ | ----------------------------------------------------------------- |
| 1    | `resstock-fetch`                                             | `fetch <STATE>`                                                   |
| 2    | `resstock-prepare-metadata`                                  | `identify-all-metadata <STATE>`                                   |
| 3    | `resstock-assign-utility`                                    | (use state-specific recipe — assignment logic is state-dependent) |
| 4    | `resstock-copy`                                              | `copy-resstock-data <FROM> <TO> <STATE> "<IDS>" "<TYPES>"`        |
| 5    | `resstock-approximate-non-hp-load`                           | `approximate-non-hp-load <STATE> 02 <FROM> <TO> 15 True True`     |
| 6    | `resstock-adjust-mf-electricity`                             | `adjust-mf-electricity <STATE> <FROM> <TO> "<IDS>"`               |
| 7    | _(N/A when using main.py — pipeline writes directly to EBS)_ | `sudo aws s3 sync ...`                                            |
| 8    | `resstock-add-monthly-loads`                                 | `add-monthly-loads <STATE> "<IDS>"`                               |
| 9    | `resstock-upload-monthly-loads`                              | `upload-monthly-loads <STATE> "<IDS>"`                            |

State-specific recipes are in `rate_design/hp_rates/{ny,ri}/Justfile`; generic recipes are in `data/resstock/Justfile`. Both read config values from `data/resstock/config.yaml` and `data/resstock/state_configs.yaml` via the `data.resstock.constants` CLI.

After the pipeline completes, the `_sb` release on S3 and EBS includes `metadata`, `metadata_utility`, `load_curve_hourly`, and `load_curve_monthly`, and is ready for rate-design runs (CAIRO) and post-processing (master bills, gas/oil/propane bills) using `res_2024_amy2018_2_sb` and the chosen state/upgrade.

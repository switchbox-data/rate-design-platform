# ResStock data preparation: run order

This document outlines the steps for downloading and preparing new ResStock data (standard NREL release → Switchbox “sb” release with HP identification, heating type, utility assignment, and approximated non-HP loads). Run the steps in order.

All commands assume the project root as the current working directory unless noted. ResStock Justfile: `data/resstock/Justfile`. Default release names: standard `res_2024_amy2018_2`, sb release `res_2024_amy2018_2_sb`.

---

## 1. Fetch standard NREL release data

Download metadata, hourly load curves, and annual load curves for the state(s) from the standard ResStock release (via bsf).

```bash
just -f data/resstock/Justfile fetch <STATE>
```

- **Example:** `just -f data/resstock/Justfile fetch NY` or `fetch RI`.
- **Result:** Data under `s3://data.sb/nrel/resstock/` (or local `path_local_parquet` if configured) for release `res_2024_amy2018_2`, all upgrade IDs (00–05), file types metadata, load_curve_hourly, load_curve_annual.

---

## 2. Add postprocess_group and categorization columns

Add HP identification, heating type, and natural-gas-connection columns to metadata in the **standard** release. Run once per state; it loops over all upgrades internally.

```bash
just -f data/resstock/Justfile identify-hp-and-heating-type-all-upgrades-and-natgas-connection <STATE>
```

- **Example:** `just -f data/resstock/Justfile identify-hp-and-heating-type-all-upgrades-and-natgas-connection NY`
- **What it does:** In order: `identify-hp-customers` (metadata.parquet → metadata-sb.parquet), `identify-heating-type` (metadata-sb.parquet in place), `identify-natgas-connection` (metadata-sb.parquet in place). All upgrades 00–05 for that state.
- **Result:** Standard release metadata under `.../metadata/state=<STATE>/upgrade=<ID>/` now has `metadata-sb.parquet` with postprocess_group and related columns.

---

## 3. Add utility assignment (standard release)

Assign electric and gas utilities to buildings in the **standard** release so that downstream steps (and the copy) use utility-aware metadata. Run once for upgrade 00, the utility assignment values should stay constant.

**NY:**

```bash
just -f data/resstock/Justfile download-ny-utility-polygons
just -f data/resstock/Justfile assign-utility-ny res_2024_amy2018_2 <UPGRADE> <ELECTRIC_POLY> <GAS_POLY>
```

- `<UPGRADE>`: e.g. `00`.
- `<ELECTRIC_POLY>` / `<GAS_POLY>`: filenames in `s3://data.sb/gis/utility_boundaries/`, e.g. `ny_electric_utilities_YYYYMMDD.csv`, `ny_gas_utilities_YYYYMMDD.csv` (from `download-ny-utility-polygons`; use the date that was written).

**RI:**

```bash
just -f data/resstock/Justfile assign-utility-ri res_2024_amy2018_2 <UPGRADE>
```

- **Result:** `.../res_2024_amy2018_2/metadata_utility/state=<STATE>/` contains utility assignment for that release.

---

## 4. Copy standard release → sb release

Copy the relevant release and upgrade(s) from the standard release to the new sb release, including **metadata_utility** so the sb release gets utility assignment by copy (no re-run of utility assignment later). Copy metadata, load_curve_hourly, and metadata_utility.

```bash
just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb <STATE> "<UPGRADE_IDS>" "<FILE_TYPES>"
```

- **Example (NY, upgrade 02, metadata + hourly loads + metadata_utility):**  
  `just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb NY "02" "metadata load_curve_hourly metadata_utility"`
- **Example (all upgrades):**  
  `just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb NY "00 01 02 03 04 05" "metadata load_curve_hourly metadata_utility"`
- **Result:** `s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/` has the same structure and files for the chosen state and upgrade(s), including metadata_utility. Use `metadata-sb.parquet` and other files as the source for step 5.

---

## 5. Approximate heating load for non-HP households, then sync to EBS

Run the non-HP load approximation for the state and upgrade(s) in the sb release. This reads from the **input** release (standard) for neighbor search and writes updated load curves and metadata to the **output** (sb) release. Then sync the sb release from S3 to the local EBS data folder so it is available for rate-design runs.

```bash
just -f data/resstock/Justfile approximate-non-hp-load <STATE> <UPGRADE_ID> res_2024_amy2018_2 res_2024_amy2018_2_sb <K> <UPDATE_MF> <UPDATE_OTHER_FUEL>
```

- **Example (NY, upgrade 02, k=15, update MF and other fuel types):**  
  `just -f data/resstock/Justfile approximate-non-hp-load NY 02 res_2024_amy2018_2 res_2024_amy2018_2_sb 15 True True`
- **Arguments:**  
  - `K`: number of nearest neighbors (e.g. 15).  
  - `UPDATE_MF`: `True` to update non-HP MF buildings.  
  - `UPDATE_OTHER_FUEL`: `True` to update “other fuel type” non-HP buildings.
- **Result:** In `res_2024_amy2018_2_sb`, load curves and metadata for the selected non-HP buildings are replaced with approximated (HP-like) loads and metadata (postprocess_group.has_hp, heating_type, has_natgas_connection, etc.). Run once per upgrade you care about (e.g. 02).

**Sync S3 sb release to EBS (after approximation):**

```bash
aws s3 sync s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/ /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/
```

- **Result:** Local path `/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/` mirrors the sb release for use by CAIRO and other pipelines.

---

## Summary checklist

| Step | Action |
|------|--------|
| 1 | `just -f data/resstock/Justfile fetch <STATE>` |
| 2 | `just -f data/resstock/Justfile identify-hp-and-heating-type-all-upgrades-and-natgas-connection <STATE>` |
| 3 | Utility assignment on **standard** release (assign-utility-ny / assign-utility-ri for `res_2024_amy2018_2`, upgrade "00") |
| 4 | `just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb <STATE> "<UPGRADE_IDS>" "metadata load_curve_hourly metadata_utility"` |
| 5 | `just -f data/resstock/Justfile approximate-non-hp-load <STATE> <UPGRADE_ID> res_2024_amy2018_2 res_2024_amy2018_2_sb 15 True True` (per upgrade); then `aws s3 sync s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/ /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/` |

After step 5, the sb release is ready for rate-design runs (e.g. CAIRO) using `res_2024_amy2018_2_sb` and the chosen state/upgrade.

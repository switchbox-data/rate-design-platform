# ResStock data preparation: run order

This document outlines the current `data/resstock/Justfile` workflow for preparing a Switchbox `sb` ResStock release from the standard NREL release. The current end state is not just HP identification and non-HP approximation: the canonical flow also copies `metadata_utility`, runs multifamily electricity adjustment on the `sb` release for upgrades `00` and `02`, and syncs the finished `sb` release to EBS.

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

## 2. Prepare metadata in the standard release

Run the state-specific metadata preparation recipe on the standard release before utility assignment and copy.

**NY:**

```bash
just -f data/resstock/Justfile prepare-metadata-ny lmi="true"
```

- **What it does:** Runs, in order, across upgrades `00`-`05`:
  - `identify-hp-customers` (`metadata.parquet` -> `metadata-sb.parquet`)
  - `identify-heating-type` (`metadata-sb.parquet` in place)
  - `identify-natgas-connection` (`metadata-sb.parquet` in place)
  - `add-vulnerability-columns` when `lmi="true"`
- **When to use `lmi="false"`:** If you explicitly want to skip vulnerability columns.

**RI:**

```bash
just -f data/resstock/Justfile prepare-metadata-ri
```

- **What it does:** Runs, in order, across upgrades `00`-`05`:
  - `identify-hp-customers`
  - `identify-heating-type`
  - `identify-natgas-connection`
- **Difference from NY:** No vulnerability-column step.

**Equivalent lower-level shortcut still available:**

```bash
just -f data/resstock/Justfile identify-hp-and-heating-type-all-upgrades-and-natgas-connection <STATE>
```

- **Result:** Standard release metadata under `.../metadata/state=<STATE>/upgrade=<ID>/` now has `metadata-sb.parquet` with postprocess-group and related columns.

---

## 3. Add utility assignment (standard release)

Assign electric and gas utilities to buildings in the **standard** release so that downstream steps and the `sb` copy use utility-aware metadata. Run once for upgrade `00`; the assignments should remain constant across upgrades.
For NY-specific details on small gas utilities and nearest-neighbor donor behavior, see `context/tools/data/ny_utility_assignment_resstock.md`.

**NY:**

```bash
just -f data/resstock/Justfile assign-utility-ny res_2024_amy2018_2 <UPGRADE> <ELECTRIC_POLY> <GAS_POLY>
```

- `<UPGRADE>`: e.g. `00`.
- `<ELECTRIC_POLY>` / `<GAS_POLY>`: filenames in `s3://data.sb/gis/utility_boundaries/`, e.g. `ny_electric_utilities_YYYYMMDD.csv`, `ny_gas_utilities_YYYYMMDD.csv`.
- **Note:** `assign-utility-ny` itself runs `download-ny-utility-polygons` first.

**RI:**

```bash
just -f data/resstock/Justfile assign-utility-ri res_2024_amy2018_2 <UPGRADE>
```

- **Result:** `.../res_2024_amy2018_2/metadata_utility/state=<STATE>/` contains utility assignment for that release.

---

## 4. Copy standard release -> sb release

Copy the relevant release and upgrade(s) from the standard release to the new `sb` release, including `metadata_utility` so the `sb` release gets utility assignment by copy. The current wrapper recipes used by the end-to-end flow copy upgrades `00` and `02` and file types `metadata metadata_utility load_curve_hourly`.

```bash
just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb <STATE> "<UPGRADE_IDS>" "<FILE_TYPES>"
```

- **Example (generic):**\
  `just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb NY "02" "metadata load_curve_hourly metadata_utility"`
- **Example (all upgrades):**\
  `just -f data/resstock/Justfile copy-resstock-data res_2024_amy2018_2 res_2024_amy2018_2_sb NY "00 01 02 03 04 05" "metadata load_curve_hourly metadata_utility"`
- **Wrapper used by current NY shortcut:**\
  `just -f data/resstock/Justfile copy-resstock-data-2024-amy2018-2-NY "00 02" "metadata metadata_utility load_curve_hourly"`
- **Wrapper used by current RI shortcut:**\
  `just -f data/resstock/Justfile copy-resstock-data-2024-amy2018-2-RI "00 02" "metadata metadata_utility load_curve_hourly"`
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

**NY:**

```bash
just -f data/resstock/Justfile adjust-mf-electricity-NY-upgrade-00
just -f data/resstock/Justfile adjust-mf-electricity-NY-upgrade-02
```

**RI:**

```bash
just -f data/resstock/Justfile adjust-mf-electricity-RI-upgrade-00
just -f data/resstock/Justfile adjust-mf-electricity-RI-upgrade-02
```

**Equivalent generic recipe:**

```bash
just -f data/resstock/Justfile adjust-mf-electricity <STATE> res_2024_amy2018_2 res_2024_amy2018_2_sb "<UPGRADE_IDS>"
```

- **What it does:** Runs `utils/pre/adjust_mf_electricity.py` against the `sb` release.
- **Current end-to-end behavior:** Upgrade `00` and upgrade `02` are both adjusted.

---

## 7. Sync the finished sb release to EBS

After approximation and multifamily-electricity adjustment, sync the finished `sb` release from S3 to local EBS so it is available for rate-design runs.

```bash
sudo aws s3 sync s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/ /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/
```

- **Result:** Local path `/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/` mirrors the sb release for use by CAIRO and other pipelines.

---

## End-to-end shortcuts

The current `Justfile` exposes two top-level recipes that run the state-specific end-to-end flow for upgrade-02 `sb` release prep:

**NY:**

```bash
just -f data/resstock/Justfile create-sb-release-for-upgrade-02-NY
```

- Runs:
  - `prepare-metadata-ny lmi="true"`
  - `copy-resstock-data-2024-amy2018-2-NY "00 02" "metadata metadata_utility load_curve_hourly"`
  - `approximate-non-hp-load NY 02 res_2024_amy2018_2 res_2024_amy2018_2_sb 15 True True`
  - `adjust-mf-electricity-NY-upgrade-00`
  - `adjust-mf-electricity-NY-upgrade-02`
  - `sudo aws s3 sync ...`

**RI:**

```bash
just -f data/resstock/Justfile create-sb-release-for-upgrade-02-RI
```

- Runs:
  - `prepare-metadata-ri`
  - `copy-resstock-data-2024-amy2018-2-RI "00 02" "metadata metadata_utility load_curve_hourly"`
  - `approximate-non-hp-load RI 02 res_2024_amy2018_2 res_2024_amy2018_2_sb 15 True True`
  - `adjust-mf-electricity-RI-upgrade-00`
  - `adjust-mf-electricity-RI-upgrade-02`
  - `sudo aws s3 sync ...`

---

## Summary checklist

| Step | Action                                                                                                                                                                                                                    |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | `just -f data/resstock/Justfile fetch <STATE>`                                                                                                                                                                            |
| 2    | Prepare standard-release metadata: `prepare-metadata-ny lmi="true"` or `prepare-metadata-ri`                                                                                                                              |
| 3    | Utility assignment on **standard** release: `assign-utility-ny res_2024_amy2018_2 00 <ELECTRIC_POLY> <GAS_POLY>` or `assign-utility-ri res_2024_amy2018_2 00`                                                             |
| 4    | Copy standard -> `sb`: `copy-resstock-data ...` or the `copy-resstock-data-2024-amy2018-2-{NY,RI}` wrappers; current shortcut flow uses upgrades `"00 02"` and file types `"metadata metadata_utility load_curve_hourly"` |
| 5    | `just -f data/resstock/Justfile approximate-non-hp-load <STATE> 02 res_2024_amy2018_2 res_2024_amy2018_2_sb 15 True True`                                                                                                 |
| 6    | Adjust MF electricity in `sb`: `adjust-mf-electricity-<STATE>-upgrade-00` and `adjust-mf-electricity-<STATE>-upgrade-02`                                                                                                  |
| 7    | `sudo aws s3 sync s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/ /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/`                                                                                                       |

After step 7, the `sb` release is ready for rate-design runs (e.g. CAIRO) using `res_2024_amy2018_2_sb` and the chosen state/upgrade.

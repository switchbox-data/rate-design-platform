# `_sb` annual load curves from modified hourly

How we build `load_curve_annual` for the Switchbox `_sb` ResStock release (`res_2024_amy2018_2_sb`), and why specific columns are kept, aggregated, or dropped.

**Implementation:** `data/resstock/load_curve/aggregate_loads.py` — one hourly read per building conditionally produces monthly parquet **and/or** an in-memory annual row, controlled by `add_monthly` / `add_annual` flags in `process_upgrade`. Rows are concatenated into one ResStock-style annual file after the thread pool finishes.

**Pipeline:** `data/resstock/main.py` step 2d (`_add_aggregate_loads`) runs when `--add-monthly-loads` and/or `--add-annual-loads` is True (both default True) and `load_curve_hourly` is in `--file-types`. Manifest step `add_aggregate_loads` records whichever of `load_curve_monthly` / `load_curve_annual` were produced.

**Related:** `resstock_sb_release_pipeline_main_py.md` (pipeline / `SB_CLONE_EXCLUDED_FILE_TYPES`), `approximate_non_hp_load.md` (hourly HVAC rewrite), `investigate_resstock_eia_load_discrepancy.md` / MF adj (hourly electricity rewrite).

**Downstream use:** Maryland gas tariff mapping reads this `_sb` annual dataset.
`utils/pre/gas_tariff_mapper.py` converts
`out.natural_gas.total.energy_consumption.kwh` to annual therms and assigns
Chesapeake RES-1 (≤150 therms) or RES-2 (>150 therms). It deliberately does not
use or fall back to raw NREL annual totals.

---

## Why this exists

The `_sb` release modifies **hourly** load curves in place:

1. Non-HP approximation (upgrade 02): rewrites heating/cooling energy consumption and `out.load.{heating,cooling}.energy_delivered.kbtu` for selected MF highrise buildings.
2. Multifamily electricity adjustment: scales selected electricity columns.

NREL’s shipped `load_curve_annual` is computed from **unmodified** 15-minute/hourly simulations. Copying raw annual into `_sb` would disagree with `_sb` hourly. Raw annual remains listed in `SB_CLONE_EXCLUDED_FILE_TYPES` so **clone** never copies it; `_sb` annual is generated from modified hourly instead.

**Prerequisite:** Hourly curves must have been downloaded/aggregated with **buildstock-fetch ≥ 1.6.6**. See [Energy delivered and the 4× bug](#energy-delivered-and-the-4-bug) below. Re-download raw hourly before building `_sb` if older hourly (mean-aggregated delivered) is still on disk or S3.

---

## High-level procedure (combined monthly + annual)

I/O dominates, so each worker does **one** hourly read:

For each `(state, upgrade)` building parquet under `_sb` `load_curve_hourly/`:

1. Read hourly parquet once into memory.
2. **Monthly** (when `add_monthly=True`): `group_by("month")` with the **full** bsf column-aggregation CSV (sum for energy / emissions / energy_delivered; mean for temperatures; first for `bldg_id`) → write one monthly parquet per building.
3. **Annual** (when `add_annual=True`): apply the **same** bsf CSV but only the [annual metric subset](#columns-aggregated-from-hourly--sb-annual) → one in-memory row (`bldg_id` + energy `.kwh` + delivered).

After all buildings for the upgrade:

4. Concatenate annual rows.
5. From raw NREL `load_curve_annual`, keep `bldg_id`, `upgrade`, `weight`, `out.params.*`, and `upgrade_name` when present.
6. Left-join aggregated metrics onto that slim slice on `bldg_id`.
7. Write one consolidated parquet under `_sb` `load_curve_annual/…`.
8. Pipeline syncs whichever of `load_curve_monthly/state=<s>/` and `load_curve_annual/state=<s>/` were produced to S3.

CLI (both monthly + annual):

```bash
uv run python data/resstock/load_curve/aggregate_loads.py \
  --path-input /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \
  --path-output /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \
  --path-annual-raw /ebs/data/nrel/resstock/res_2024_amy2018_2 \
  --state CT --upgrade-ids "00 02" \
  --bsf-release res_2024_amy2018_2 --workers 50 \
  --add-monthly --add-annual
```

Annual-only (same CLI, omit `--add-monthly`):

```bash
uv run python data/resstock/load_curve/aggregate_loads.py \
  --path-input /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \
  --path-output /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \
  --path-annual-raw /ebs/data/nrel/resstock/res_2024_amy2018_2 \
  --state CT --upgrade-ids "00 02" \
  --bsf-release res_2024_amy2018_2 --workers 50 \
  --add-annual
```

`load_curve_annual` stays in `SB_CLONE_EXCLUDED_FILE_TYPES` so clone does not overwrite generated `_sb` annual with raw NREL. The file type becomes expected on `_sb` once the manifest records an `add_aggregate_loads` step.

---

## Schemas (ResStock 2024.2, verified MD/CT)

| Source                      | Typical width | Notes                                                         |
| --------------------------- | ------------- | ------------------------------------------------------------- |
| Hourly (all upgrades 00–05) | ~142 cols     | Identical energy/delivered/temp column set across upgrades    |
| Annual upgrade 00           | ~112 cols     | Absolute metrics only                                         |
| Annual upgrades 01–05       | ~200–202 cols | Absolute metrics **plus** baseline-relative extras (~90 cols) |

Hourly energy and the three load-delivered columns map 1:1 onto annual absolute columns for upgrades **00–05** (50 energy columns; 0 missing / 0 extra after `+.kwh` rename).

---

## Columns aggregated from hourly → `_sb` annual

Annual uses the **same** bsf aggregation CSV as monthly (`res_2024_amy2018_2.csv`, etc.), filtered to:

- columns ending in `.energy_consumption` but **not** `.energy_consumption_intensity`
- any column whose name contains `energy_delivered`

Aggregation function comes from the CSV (`sum` for these under bsf ≥ 1.6.6). Energy columns are renamed with a `.kwh` suffix for the annual file.

### Energy consumption (sum + rename)

| Hourly name                            | Annual name                | Aggregation |
| -------------------------------------- | -------------------------- | ----------- |
| `*.energy_consumption` (not intensity) | `*.energy_consumption.kwh` | `sum`       |

Example: `out.electricity.heating.energy_consumption` → `out.electricity.heating.energy_consumption.kwh`.

There are **50** such end-use/fuel columns in 2024.2 (electricity, natural gas, fuel oil, propane, site_energy nets/totals, etc.). Mapping was checked for MD upgrades 00–05: every hourly energy column has an annual `.kwh` counterpart and vice versa.

**Why aggregate these:** They are the quantities `_sb` modifies (HVAC approximation, MF electricity adj). Annual must reflect those edits. Units are already kWh-equivalent in the hourly series; annual only adds the `.kwh` suffix in the column name.

**Why not intensities:** Annual NREL files have no intensity columns; intensities are rates, not annual totals. Monthly still aggregates them via the full bsf rule set.

### Load delivered (sum, no rename)

| Column                                     | Aggregation |
| ------------------------------------------ | ----------- |
| `out.load.heating.energy_delivered.kbtu`   | `sum`       |
| `out.load.cooling.energy_delivered.kbtu`   | `sum`       |
| `out.load.hot_water.energy_delivered.kbtu` | `sum`       |

Same names in hourly and annual (already `.kbtu`).

**Why include them:** Heating/cooling delivered are rewritten by non-HP approximation on upgrade 02; hot water is not rewritten there but is still a physical annual total that should stay consistent with the (corrected) hourly series. After bsf ≥ 1.6.6, Σ(hourly) matches NREL annual for unmodified buildings.

**No unit conversion:** Do not apply a kWh↔kBtu factor; both schemas label these columns as kBtu.

---

## Columns kept from raw NREL annual (not recomputed)

| Column(s)      | When present            | Why keep                                                                                                 |
| -------------- | ----------------------- | -------------------------------------------------------------------------------------------------------- |
| `bldg_id`      | Always                  | Join key                                                                                                 |
| `upgrade`      | Always                  | Upgrade id                                                                                               |
| `weight`       | Always                  | Sample weight; not in hourly files                                                                       |
| `out.params.*` | Always (~20)            | Building geometry/area parameters only in annual                                                         |
| `upgrade_name` | Upgrades **01–05** only | Human-readable package label (e.g. “High efficiency cold-climate heat pump…”); not derivable from hourly |

Upgrade **00** has no `upgrade_name`; the selector keeps it only if the column exists.

These fields are independent of the hourly load edits, so copying them from raw annual is correct.

---

## Columns deliberately not produced

### From hourly: not summed into `_sb` annual

| Hourly columns                                          | Why dropped                                                                                                                                         |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `*.energy_consumption_intensity` (~50)                  | Annual has no intensity columns; intensities are rates, not annual totals                                                                           |
| Zone / outdoor temperatures (`*.c`, ~13)                | Annual has no temps; hourly aggregation uses **mean** for these in bsf (correct for temperatures). Summing would be meaningless for our annual file |
| Time keys (`timestamp`, `year`, `month`, `day`, `hour`) | Not annual attributes                                                                                                                               |
| Hourly `out.total.lrmer_*` emissions (~20)              | We do not rebuild emissions for `_sb` annual (see below). Could be summed later if needed                                                           |

### From raw annual: not copied into `_sb` annual

Everything not listed in [Columns kept from raw NREL annual](#columns-kept-from-raw-nrel-annual-not-recomputed) is dropped from the raw file, including absolute energy/load/bills/peaks/emissions that would otherwise conflict with recomputed `_sb` totals.

#### Absolute metrics (present on upgrade 00 and upgrades 01–05)

| Category           | Examples                                               | Why drop                                                                                                                                                      |
| ------------------ | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Energy consumption | `*.energy_consumption.kwh`                             | Replaced by Σ(`_sb` hourly)                                                                                                                                   |
| Load delivered     | `out.load.*.energy_delivered.kbtu`                     | Replaced by Σ(`_sb` hourly)                                                                                                                                   |
| Emissions          | `out.emissions.*` / LRMER columns                      | Not recomputed from `_sb` hourly; raw values would not match modified loads                                                                                   |
| Bills              | `out.bills.*.usd`                                      | Tariff/assumption-dependent; not from `_sb` load edits                                                                                                        |
| Peaks              | `out.electricity.*.peak.kw`, `out.load.*.peak.kbtu_hr` | Need **max** (or NREL’s peak definition) over the year, not a sum; after HVAC/MF edits, peaks would need a separate max-over-hourly pass we are not doing yet |
| Hot water volumes  | `out.hot_water.*.gal`                                  | Not in hourly files                                                                                                                                           |
| Unmet hours        | `out.unmet_hours.*.hour`                               | Not in hourly files                                                                                                                                           |
| Energy burden      | `out.energy_burden.percentage`                         | Not from load curves                                                                                                                                          |

#### Baseline-relative extras (upgrades **01–05** only; ~90 columns)

NREL annual files for non-baseline upgrades add columns that compare the upgrade to upgrade 00. Empirically (MD), for energy:

`*.savings` ≈ `upgrade_00_value − upgrade_N_value` (positive = savings).

| Category                   | Count (typical) | Pattern                                          | Why drop                                                                    |
| -------------------------- | --------------- | ------------------------------------------------ | --------------------------------------------------------------------------- |
| Energy consumption savings | 50              | `*.energy_consumption.kwh.savings`               | Precomputed vs **NREL** baseline, not `_sb` u00; invalid after hourly edits |
| Load delivered savings     | 3               | `out.load.*.energy_delivered.kbtu.savings`       | Same                                                                        |
| Bill savings               | 5               | `out.bills.*.usd.savings`                        | Same + tariff-dependent                                                     |
| Peak savings               | 4               | `*.peak.*.savings`                               | Same + peaks not rebuilt                                                    |
| Hot water volume savings   | 4               | `out.hot_water.*.gal.savings`                    | Not in hourly; baseline-relative                                            |
| Unmet hours savings        | 2               | `out.unmet_hours.*.hour.savings`                 | Same                                                                        |
| Energy burden savings      | 1               | `out.energy_burden.percentage.savings`           | Same                                                                        |
| Emissions reductions       | 20              | `out.emissions_reduction.{fuel}.lrmer_*.co2e_kg` | Same idea as savings; named `emissions_reduction` rather than `*.savings`   |

**Kept among the upgrade-only fields:** only `upgrade_name` (see above).

Recomputing savings correctly would mean differencing `_sb` upgrade _N_ annual against `_sb` upgrade 00 annual after both are rebuilt — explicitly out of scope for this step.

#### Upgrade 03 quirk (ignored)

On MD, upgrade 03’s annual file is missing absolute `out.energy_burden.percentage` while still having `.savings`. We do not special-case this: same keep list as other upgrades (`bldg_id`, `upgrade`, `weight`, optional `upgrade_name`, `out.params.*`).

---

## Energy delivered and the 4× bug

### Symptom

For older hourly downloads, Σ(hourly `out.load.*.energy_delivered.kbtu`) was **exactly 1/4** of NREL annual (and of Σ(15-minute)). Electricity `*.energy_consumption` summed correctly (ratio ≈ 1).

### Cause

`buildstock-fetch` builds hourly from NREL’s native **15-minute** timeseries using per-column rules in `buildstock_fetch/data/load_curve_column_map/*.csv`. Energy columns use `sum`; temperatures use `mean` (correct).

A Jan 2026 change set the three `energy_delivered` columns to `mean` (treating kBtu as a “power-like” quantity). Averaging four 15-minute energy intervals into one hour understates hourly energy by 4×, so the annual sum of hourly is 4× low.

### Fix

| Artifact                                     | Status                                                                                                                   |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| GitHub `main` / **buildstock-fetch ≥ 1.6.6** | `energy_delivered` → `sum` again in 2022/2024 maps (and 2025 maps with NREL’s `energy_delivered..kbtu` double-dot names) |
| PyPI **1.6.5 and earlier**                   | Still had `mean`                                                                                                         |
| This repo                                    | Pins `buildstock-fetch>=1.6.6` in `pyproject.toml`                                                                       |

Verified with bsf 1.6.6: fresh hourly downloads and re-aggregation of stored 15-minute files give annual / Σ(hourly) ≈ **1.000** for heating/cooling/hot-water delivered.

**Operational implication:** Hourly already on S3/local from older bsf must be **re-downloaded** (or re-aggregated from 15-minute) before `_sb` annual is trusted for delivered columns. Energy consumption columns were never affected by this bug.

Temperature columns remaining as `mean` in the CSV maps is intentional and unrelated.

---

## What `_sb` annual contains (summary)

Per building, one row with approximately:

- Identity / sample: `bldg_id`, `upgrade`, `weight`, optional `upgrade_name`
- ~20 `out.params.*`
- 50 `*.energy_consumption.kwh` from Σ(`_sb` hourly)
- 3 `out.load.*.energy_delivered.kbtu` from Σ(`_sb` hourly)

No savings, emissions, bills, peaks, gallons, unmet hours, or energy burden.

---

## Validation notes used when designing this

- **Name mapping:** MD upgrades 00–05 — 50 hourly energy ↔ 50 annual `.kwh`; three delivered names match.
- **Unmodified buildings:** With bsf ≥ 1.6.6, Σ(hourly) matches NREL annual for energy and delivered (float noise ~1e−5 relative).
- **Modified buildings:** `_sb` annual will **diverge** from NREL annual wherever hourly was rewritten; that divergence is the point of the `_sb` annual file.

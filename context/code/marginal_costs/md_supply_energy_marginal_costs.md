# MD supply energy marginal cost pipeline

How PJM real-time LMP data is fetched, cleaned, and converted into CAIRO-ready
hourly supply energy marginal costs for Maryland utilities.

## What this represents

The supply energy marginal cost for an MD utility is the **real-time locational
marginal price (LMP)** at the PJM zone aggregate node that serves that utility's
territory. It represents the incremental cost of producing one more MWh of
electricity at a given hour — what the wholesale market charges for that energy
at that location. In the BAT framework, this is the hourly `energy_cost_enduse`
($/MWh) used to assess whether a customer's bill is aligned with the marginal
cost of serving them.

PJM is the ISO/RTO for all MD utilities. Unlike NY (NYISO) or RI (ISO-NE), where
supply energy is derived from zone-level LBMP with possible load-weighted averaging
across multiple zones, every MD utility maps to exactly one PJM zone aggregate.
No load-weighting is needed.

## Scripts

| Script                                                        | Purpose                                                                                                                                      |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `data/pjm/fetch_lmp.py`                                       | Fetch raw hourly zone-aggregate LMP from PJM Data Miner 2 API, or extract from a pre-downloaded S3 CSV; write Hive-partitioned parquet to S3 |
| `data/pjm/zone_mapping/generate_zone_mapping_csv.py`          | Generate/refresh the utility-to-zone crosswalk CSV                                                                                           |
| `data/pjm/zone_mapping/validate_pjm_zone_mapping.py`          | Validate the crosswalk CSV (schema, pnode_ids, zone coverage)                                                                                |
| `data/pjm/validate_pjm_lmp.py`                                | Validate fetched LMP parquet (row counts, timestamp completeness, DST, value ranges)                                                         |
| `utils/data_prep/marginal_costs/generate_supply_energy_mc.py` | CLI: reads raw LMP from S3, produces CAIRO-ready 8760 parquet                                                                                |

CLI (energy): `--iso pjm --utility bge --year 2025 [--upload]`

Output: `s3://data.sb/switchbox/marginal_costs/md/supply/energy/utility={utility}/year={YYYY}/data.parquet`

Schema: `timestamp` (datetime, naive Eastern wall-clock), `energy_cost_enduse` ($/MWh)

Each partition also contains a `zero.parquet` — an all-zeros placeholder used by
delivery-only CAIRO runs that don't include supply.

## Utility-to-zone mapping

All MD utilities, including cooperatives and municipals that share a zone with a
host IOU, map to one of four PJM zone aggregate pricing nodes. Defined in
`utils/data_prep/marginal_costs/supply_utils.py` (`PJM_UTILITY_ZONES`) and
`data/pjm/zone_mapping/generate_zone_mapping_csv.py` (`_MAPPING_ROWS`).

| Utility slug     | Type                         | PJM zone | pnode_id |
| ---------------- | ---------------------------- | -------- | -------- |
| `bge`            | IOU                          | BGE      | 51292    |
| `pepco`          | IOU                          | PEPCO    | 51298    |
| `dpl`            | IOU                          | DPL      | 51293    |
| `potomac-edison` | IOU                          | APS      | 8394954  |
| `smeco`          | Co-op (in PEPCO territory)   | PEPCO    | 51298    |
| `choptank`       | Co-op (in DPL territory)     | DPL      | 51293    |
| `an-electric`    | Co-op (in DPL territory)     | DPL      | 51293    |
| `somerset-rec`   | Co-op (in APS territory)     | APS      | 8394954  |
| `hagerstown`     | Municipal (in APS territory) | APS      | 8394954  |
| `thurmont`       | Municipal (in APS territory) | APS      | 8394954  |
| `williamsport`   | Municipal (in APS territory) | APS      | 8394954  |
| `easton`         | Municipal (in DPL territory) | DPL      | 51293    |
| `berlin`         | Municipal (in DPL territory) | DPL      | 51293    |

Utilities that share a zone get identical supply energy marginal cost profiles
(the zone LMP is the same for all of them).

## Raw LMP data

### S3 location

```
s3://data.sb/pjm/lmp/real_time/zones/zone={ZONE}/year={YYYY}/data.parquet
```

Canonical base URI defined once in `data/pjm/__init__.py` as `PJM_LMP_S3_BASE`
and re-exported from `utils/data_prep/marginal_costs/supply_utils.py` as
`DEFAULT_PJM_LMP_S3_BASE`. Both scripts import from the same source of truth.

### Schema

| Column                   | Type                                 | Description                                      |
| ------------------------ | ------------------------------------ | ------------------------------------------------ |
| `datetime_beginning_utc` | `Datetime("us", "UTC")`              | Hour start in UTC (unambiguous reference)        |
| `datetime_beginning_ept` | `Datetime("us", "America/New_York")` | Hour start in Eastern Prevailing Time (tz-aware) |
| `pnode_id`               | `Int64`                              | Numeric pricing node ID                          |
| `pnode_name`             | `Utf8`                               | Zone name (e.g. `"BGE"`, `"PEPCO"`)              |
| `type`                   | `Utf8`                               | Always `"ZONE"` for zone aggregate rows          |
| `total_lmp_rt`           | `Float64`                            | Real-time LMP in $/MWh                           |

Hive partition key `year` uses the **EPT year** (not UTC), so year-boundary hours
near midnight on Dec 31 / Jan 1 partition correctly without splitting.

### Fetching: archive vs. standard API behavior

The PJM Data Miner 2 API treats data older than 731 days differently from recent
data. `fetch_lmp.py` branches on this automatically:

| Data age              | API endpoint  | `pnode_id` filter                                                                                      | Client-side filter                                                      |
| --------------------- | ------------- | ------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| ≤ 731 days (standard) | Standard feed | Allowed — used to fetch only the target zone (one page, no pagination needed for year-by-year fetches) | Not needed                                                              |
| > 731 days (archive)  | Archive feed  | **Not allowed** (returns HTTP 400)                                                                     | Required — fetch all `type=ZONE` rows, filter by `pnode_name` in Python |

The `pnode_id` values are stored in the zone-mapping CSV (`data/pjm/zone_mapping/pjm_md_zone_mapping.csv`). Passing `pnode_id` on a standard request returns only the target zone's rows — roughly 8,760 rows/year vs ~175,000 for all zones. For archive data, all zone rows are fetched and filtered client-side.

### Timezone handling

PJM reports timestamps in Eastern Prevailing Time (EPT = `America/New_York`,
which is EDT in summer and EST in winter). The API also provides a UTC column.
The fetch pipeline:

1. Parses `datetime_beginning_utc` as tz-aware UTC (unambiguous, used as the
   reference to derive EPT).
2. Converts UTC → `America/New_York` to produce `datetime_beginning_ept`
   (tz-aware). Using the UTC column as the source correctly disambiguates the two
   1:00 AM EPT hours that occur during the fall-back DST transition — they have
   different UTC offsets (`-04:00` before, `-05:00` after) and remain distinct
   tz-aware rows.
3. Derives the Hive `year` partition from the EPT year, so no hours are split
   across year boundaries due to the UTC-to-Eastern offset.

## 8760 normalization and DST handling

`load_lmp_for_pjm_zone` in `supply_energy.py` reads the tz-aware EPT column and
calls `strip_tz_if_needed`, which uses `replace_time_zone(None)` to strip the
timezone annotation **without shifting the wall-clock value**. This produces
naive Eastern timestamps. DST edge cases are then handled by
`prepare_component_output` in `supply_utils.py`:

| DST event                     | What PJM provides                                  | After `strip_tz_if_needed`          | In `prepare_component_output`                                                                                       |
| ----------------------------- | -------------------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Spring-forward (Mar, 2:00 AM) | No row (that hour doesn't exist)                   | No row                              | Left-joined against 8760 reference → null; filled by linear interpolation between 1:00 AM and 3:00 AM               |
| Fall-back (Nov, 1:00 AM)      | Two tz-aware rows: `01:00-04:00` and `01:00-05:00` | Two identical naive rows `01:00:00` | Collapsed to one row by taking mean; duplicates detected by `n_before != n_unique` check in `load_lmp_for_pjm_zone` |

Leap years (2020, 2024 — 8,784 calendar hours) are normalized to 8,760 by the
reference join: `build_cairo_8760_timestamps` generates timestamps through Dec 30
23:00 for leap years (dropping Dec 31), so Dec 31 PJM data is silently excluded
by the left join. The output is always exactly 8,760 naive Eastern timestamps.

Observed row counts per year:

| Year type | Raw EPT rows from S3 | After fall-back collapse | After 8760 reference join + spring-forward fill |
| --------- | -------------------- | ------------------------ | ----------------------------------------------- |
| Non-leap  | 8,760 (8,759 unique) | 8,759                    | 8,760                                           |
| Leap      | 8,784 (8,783 unique) | 8,783                    | 8,760                                           |

## Running the pipeline

### Fetch raw LMP from PJM API

From the repo root:

```bash
uv run python data/pjm/fetch_lmp.py fetch-api \
  --zone BGE,PEPCO,DPL,APS \
  --start-date 2018-01-01 \
  --end-date 2025-12-31
```

This writes to `s3://data.sb/pjm/lmp/real_time/zones/` by default. Archive data
(before 731 days ago) is fetched without `pnode_id` filtering (client-side only);
recent data uses `pnode_id` for server-side filtering.

### Generate CAIRO-ready 8760

From `rate_design/hp_rates/`:

```bash
# One utility, one year (inspect only — no S3 write)
just -f md/Justfile create-supply-energy-mc bge 2023

# One utility, all years (2018–2025)
just -f md/Justfile create-supply-energy-mc-all-years bge --upload

# All utilities, one year
just -f md/Justfile create-supply-energy-mc-all 2023 --upload

# Full backfill: all 13 utilities × 2018–2025 = 104 combinations
just -f md/Justfile create-supply-energy-mc-full-backfill --upload
```

Available years are defined in `pjm_lmp_years` in `rate_design/hp_rates/md/Justfile`.
Omit `--upload` to inspect output without writing to S3.

## Sanity checks on observed outputs

Average annual LMP values by zone across years. These reflect known market events
and are a useful cross-check when re-running.

| Year | Avg LMP (approx, $/MWh) | Notable events                                                           |
| ---- | ----------------------- | ------------------------------------------------------------------------ |
| 2018 | ~39                     | Jan 2018 bomb cyclone: spikes to ~664 $/MWh                              |
| 2019 | ~25                     | Jan 2019 polar vortex: spikes to ~634 $/MWh                              |
| 2020 | ~21                     | COVID demand reduction; summer heat spike ~637 $/MWh                     |
| 2021 | ~38                     | Dec 2021 cold snap; Mar 2021 winter storm                                |
| 2022 | ~72                     | Global energy crisis (Russia/Ukraine); Dec 24 bomb cyclone: ~3,762 $/MWh |
| 2023 | ~27                     | July heat waves: spikes to ~920 $/MWh                                    |
| 2024 | ~31                     | July heat waves: spikes to ~825 $/MWh                                    |
| 2025 | ~47                     | June 2025 heat event: spikes to ~1,842 $/MWh                             |

# PJM hourly zonal loads (Data Miner 2 `hrl_load_metered`)

How the PJM-native hourly zonal load pipeline (`data/pjm/hourly_demand/`) works:
where the data comes from, the Data Miner 2 quirks it handles, how zones map to
utilities, and the timezone/completeness conventions for the output.

## Why PJM-native (not EIA-930)

For non-NY states the sub-TX/DX marginal-cost workflow defaults to EIA-930
utility loads (`s3://data.sb/eia/hourly_demand/utilities/`). EIA-930 only
re-publishes the demand PJM submits, and it has recurring ~24-hour gaps around
the DST transitions (confirmed for the BGE/`BC` zone across 2019–2025). Filling
a full missing day needs imputation we'd rather avoid for a real deliverable.

PJM Data Miner 2 is the authoritative upstream source: complete across those
days and it ships a native `datetime_beginning_utc` column. This mirrors the NY
precedent, where the shared EIA default is overridden to use NYISO-native loads.

## Source feed

- Feed: `hrl_load_metered`
  (definition: <https://dataminer2.pjm.com/feed/hrl_load_metered/definition>).
- Auth: `PJM_API_PRIMARY_KEY` from the project `.env`, sent as the
  `Ocp-Apim-Subscription-Key` header.
- Fields (confirmed against the live feed, lowercase):
  `datetime_beginning_utc`, `datetime_beginning_ept`, `nerc_region`,
  `mkt_region`, `zone`, `load_area`, `mw`, `is_verified`.

### Two facts that shape the pipeline

1. **Zone labels are Data Miner _legacy_ codes.** BGE is `zone="BC"` (there is
   no literal `BGE` in this feed), Pepco is `PEP`, Delmarva is `DPL`, APS is
   `AP`. This matches the `dataminer_zone` column of the PJM zone crosswalk
   (`data/pjm/zone_mapping/`) — NOT the `price_zone`/`fivecp_zone_label`
   columns, which use the modern labels (`BGE`, `PEPCO`, …). The LMP feed
   (`rt_hrl_lmps`) uses the modern labels instead, so do not assume the two PJM
   feeds share a vocabulary.
2. **The feed is `load_area`-grained, not `zone`-grained.** A single zone can
   span several load areas (e.g. `PEP -> {PEPCO, SMECO}`, `DPL -> {DPLCO,
   EASTON}`; `BC` and `AP` are single-area). A zone series is therefore the
   **sum of `mw` over its load areas, per hour**.

### Bad values and the raw-zone / clean-utility split

Occasionally a load area reports a bogus value for a single hour (observed at
DST transitions — e.g. DPL's `DPLCO` reports a hard `0.0` at `2025-03-09 01:00`
while flanked by ~2,000 MW; PJM even marks it `is_verified=true`). This is a
**present but bad value**, not a missing row, so a row-count check can't catch
it. We handle it with a deliberate two-layer split:

- **Zones are a faithful raw mirror of PJM.** `sum_load_areas_to_zone` keeps the
  raw sum and instead sets a boolean **`value_flag`** for any zone-hour where a
  constituent load area is an isolated single-hour spike vs _both_ its temporal
  neighbours — either down (< 25% of the lower neighbour, e.g. a hard `0.0`) or
  up (> 4× the higher neighbour, a garbage high reading). The genuinely tiny
  EASTON area at ~28 MW is never flagged. Nothing is altered, so the dataset
  reconciles exactly against Data Miner. (Known blind spots: runs of ≥2 bad
  hours, and moderate distortions that aren't distinguishable from real load
  variation without an external reference.)
- **Utilities are the curated product.** When building utility profiles,
  `interpolate_flagged_zone_hours` nulls the flagged hours and linearly
  interpolates per zone (uniform hourly grid → position-based interpolation is
  exact) before summing zones into utilities. The MC workflow consumes utilities,
  so it gets clean load shapes. The utility output carries an **`interpolated`**
  boolean (True for any utility-hour built from an interpolated zone-hour) so the
  cleaning stays auditable downstream.

The zone validator **warns** (does not fail) on `value_flag` hours so the raw
artifacts stay visible.

Note: `row_is_current` (a valid filter on the LMP feed) is **not** valid on
`hrl_load_metered`; use `is_verified` if you need verified-only rows.

## Data Miner 2 archive/standard boundary

PJM data older than **731 days** is "archive" and obeys different request rules
than recent "standard" data:

- A single request must not span the archive/standard boundary.
- Archive requests must stay within one calendar year.
- Standard requests are limited to ~365 days.

The shared client `data/pjm/dataminer.py` centralises this:

- `archive_cutoff()` — `today_utc - 731 days`.
- `split_date_range(start, end)` — chunks a range to satisfy all three rules.
- `fetch_date_range(feed, start, end, build_params, ...)` — pages through
  `rowCount` windows, retries timeouts/5xx, backs off on 429, and **bisects** a
  chunk if PJM still reports it straddles the boundary.

`build_params(chunk_start, chunk_end, is_archive)` returns the per-chunk query
params, so feed-specific filters stay in each feed's module. The load fetch has
no server-side `type`/`pnode_id` filter; it filters to the MD zones client-side
(works for both archive and standard chunks).

## Timezone and completeness conventions

- **Calendar year is derived from EPT** (`datetime_beginning_ept`), not UTC, to
  avoid a ~5-hour year-boundary misalignment.
- **The output `timestamp` is built from UTC** (`datetime_beginning_utc` →
  marked UTC → converted to tz-aware `America/New_York`). Building the instant
  from UTC means the fall-back DST hour is two distinct instants (no collision)
  and the spring-forward hour is simply absent — so a full Eastern calendar year
  is exactly **8760 hours (8784 in a leap year)** with no DST holes.
- The validator counts expected hours DST-aware off UTC
  (`expected_hours_in_year`) and hard-fails on wrong counts, nulls, negatives,
  duplicate timestamps, or internal gaps; peak sanity (BGE ≈ 6,100 MW) is a
  warning.

## Output layout

Local staging (gitignored), then `aws s3 sync` to S3:

- Zones: `zone={CODE}/year=YYYY/month=MM/data.parquet`
  → `s3://data.sb/pjm/hourly_demand/zones/`
- Utilities: `utility={slug}/year=YYYY/month=MM/data.parquet`
  → `s3://data.sb/pjm/hourly_demand/utilities/`

Year/month are Eastern wall-clock (`year`/`month` of the local `timestamp`),
matching the NYISO/ISO-NE/EIA partition convention so all ISO load layouts agree.

Output schema:

- Zones: `timestamp` (tz-aware `America/New_York`), `zone`, `load_mw` (`Float64`,
  raw), `value_flag` (`Boolean`, marks raw bad load-area values).
- Utilities: `timestamp`, `utility`, `load_mw` (`Float64`, flagged hours
  interpolated), `interpolated` (`Boolean`, marks hours built from an
  interpolated zone-hour).

Utility aggregation maps each utility to its zone(s) via
`data/pjm/zone_mapping/csv/pjm_utility_zone_mapping.csv` and sums zone loads by
timestamp. Each MD utility maps to exactly one zone (bge→BC, pepco→PEP, dpl→DPL,
potomac-edison→AP), so a utility series equals its zone series. The
`capacity_weight` column is for capacity-cost allocation and is **not** applied
to load.

## Running it

From the repo root (scripts run as modules because they import `data.pjm`):

```sh
just -f data/pjm/hourly_demand/Justfile prepare 2025          # fetch + aggregate-all + validate
just -f data/pjm/hourly_demand/Justfile fetch-zone-data 2025
just -f data/pjm/hourly_demand/Justfile aggregate-all-utility-loads 2025
just -f data/pjm/hourly_demand/Justfile validate
just -f data/pjm/hourly_demand/Justfile upload                # aws s3 sync to S3
```

## Scope

This pipeline produces the loads only. The MD sub-TX/DX workflow consumes these
PJM-native utility loads via the `md/Justfile` `path_s3_utility_loads` override
(`s3://data.sb/pjm/hourly_demand/utilities/`) and the ISO-native-path handling in
`generate_utility_tx_dx_mc.py` (the region filter applies only to EIA paths; all
ISO-native paths, including PJM, skip it).

# NY supply marginal costs pipeline

How energy (LBMP) and capacity (ICAP) supply marginal costs are generated for NY utilities.

## Script

`utils/pre/generate_utility_supply_mc.py`

CLI: `--utility cenhud --year 2025 [--load-year 2018] --zone-mapping-path s3://... [--upload]`

Output: `s3://data.sb/switchbox/marginal_costs/ny/supply/utility={utility}/year={YYYY}/data.parquet`
Schema: `timestamp` (datetime), `energy_cost_enduse` ($/MWh), `capacity_cost_enduse` ($/MWh)

## Data sources

| Dataset                 | S3 path                                                                                    | Schema key columns                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| LBMP (energy prices)    | `s3://data.sb/nyiso/lbmp/real_time/zones/zone={ZONE_NAME}/year={Y}/month={M}/data.parquet` | `interval_start_est`, `zone`, `lbmp_usd_per_mwh` (5-minute intervals)               |
| ICAP (capacity prices)  | `s3://data.sb/nyiso/icap/year={Y}/month={M}/data.parquet`                                  | `locality` (Categorical), `auction_type` (Categorical), `price_per_kw_month`        |
| NYISO zone loads        | `s3://data.sb/nyiso/hourly_demand/zones/zone={NAME}/year={Y}/month={M}/data.parquet`       | `timestamp`, `zone`, `load_mw`                                                      |
| Zone mapping            | `s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv`                              | `utility`, `load_zone_letter`, `lbmp_zone_name`, `icap_locality`, `capacity_weight` |

## Utility-to-zone mapping

Defined in two places that **must stay consistent** (enforced by `test_eia_zones_match_zone_mapping`):

- `data/eia/hourly_loads/eia_region_config.py` → `UTILITY_SERVICE_AREAS` (zones for EIA load aggregation)
- `data/nyiso/zone_mapping/generate_zone_mapping_csv.py` → `_MAPPING_ROWS` (zones + ICAP locality + capacity weight)

| Utility | Load Zones          | ICAP Locality         | Capacity Weight |
| ------- | ------------------- | --------------------- | --------------- |
| cenhud  | G                   | GHIJ                  | 1.0             |
| coned   | G, H, I, J          | NYC: 0.87, GHIJ: 0.13 | split           |
| nimo    | A, B, C, D, E, F    | NYCA                  | 1.0             |
| nyseg   | A, B, C, D, E, F    | NYCA                  | 1.0             |
| or      | G                   | GHIJ                  | 1.0             |
| rge     | B                   | NYCA                  | 1.0             |
| psegli  | K                   | LI                    | 1.0             |

## Energy MC (LBMP)

Real-time LBMP prices (5-minute intervals) are aggregated to hourly averages by taking the mean of all 5-minute intervals within each hour. These hourly prices are then used to compute energy marginal costs.

- **Single-zone utility** (rge, cenhud, or, psegli): uses that zone's hourly real-time LBMP directly.
- **Multi-zone utility** (nyseg, nimo, coned): load-weighted average across zones. For each hour: `Σ(LBMP_zone × load_zone) / Σ(load_zone)`, where zone loads come from NYISO zone-level hourly data.

## Capacity MC (ICAP MCOS)

Capacity MC now uses **two locality models**, each for a different purpose.

### 1) Nested localities for capacity peak loads (hour-shape)

These are overlapping nested localities used only to build the hourly load profile that determines peak-hour weights:

- `NYCA = WEST, GENESE, CENTRAL, NORTH, MHK_VL, CAPITL, HUD_VL, MILLWD, DUNWOD, N.Y.C., LONGIL`
- `LHV = HUD_VL, MILLWD, DUNWOD, N.Y.C.`
- `NYC = N.Y.C.`
- `LI = LONGIL`

`icap_locality` from mapping is normalized as:

- `NYCA -> NYCA`
- `GHIJ -> LHV`
- `NYC -> NYC`
- `LI -> LI`

For split utilities (like ConEd), locality load profiles are blended by `capacity_weight` before peak identification.

### 2) Partitioned localities for ICAP prices (monthly $/kW-mo)

Prices use non-overlapping localities after applying utility splits via `gen_capacity_zone`:

- `ROS -> NYCA` (WEST through CAPITL)
- `LHV -> LHV` (HUD_VL through N.Y.C.)
- `NYC -> NYC` (N.Y.C.)
- `LI -> LI` (LONGIL)

This is the "nested to partitioned" transform used for prices. ICAP source localities are read from NYISO as `NYCA/GHIJ/NYC/LI`, with `GHIJ` mapped to partitioned `LHV`.

### Allocation

Monthly ICAP Spot prices ($/kW-month) are allocated to individual hours using threshold-exceedance weighting on the nested-locality blended load profile:

1. For each month, sort hours by load (descending).
2. Threshold = load of the 9th-highest hour (i.e., the hour just below the top 8).
3. Exceedance_h = max(load_h − threshold, 0) for each hour.
4. Weight_h = exceedance_h / Σ(exceedance in month).
5. capacity_cost_h ($/kW) = weight_h × icap_price_month ($/kW-month).
6. Convert to $/MWh: multiply by 1000.

Non-peak hours get zero capacity cost. Only the top 8 load hours per month carry capacity costs.

**ConEd example**:

- Peak-load shape: `0.87 × NYC(J) + 0.13 × LHV(G-J)` (nested localities)
- Price blend: `0.87 × NYC + 0.13 × LHV` (partitioned localities; `GHIJ` source mapped to `LHV`)

**Validation**: a 1 kW constant load must recover the sum of 12 monthly ICAP prices exactly (within tolerance). This validates that the hourly allocation sums back to the correct annual total.

## NYISO load pipeline

Zone loads are fetched/managed via `data/nyiso/hourly_demand/`:

- `fetch-zone-data` — fetches from NYISO MIS portal to local parquet (canonical zone names)
- `aggregate-utility-loads` / `aggregate-all-utility-loads` — aggregate zones to utility profiles
- `download` / `upload` — sync local parquet to/from S3

Supply MC uses zone-level hourly loads for:

- utility-zone LBMP weighting (energy MC)
- nested locality capacity peak profiling (capacity MC)

## Load year / output year separation

The `--year` argument controls the output year (ICAP/LBMP prices, output partition, timestamps). The optional `--load-year` argument (defaults to `--year`) controls which year's load profile is used for LBMP weighting and ICAP peak identification.

When `load_year != year`, capacity timestamps are remapped from load_year to year after allocation using `dt.offset_by()`. This allows using e.g. 2018 AMY load shapes (matching ResStock AMY2018 weather) with 2025 prices.

## 8760 normalization

Leap years (8784 hours) are normalized to 8760 by dropping Dec 31 hours when Feb 29 exists. Duplicate timestamps from DST fall-back are deduplicated by keeping the first occurrence. This matches CAIRO's expected 8760-row input.

# NY supply marginal costs pipeline

How energy (LBMP) and capacity (ICAP) supply marginal costs are generated for NY utilities.

## Script

`utils/pre/generate_utility_supply_mc.py`

CLI: `--utility cenhud --year 2025 [--load-year 2018] --zone-mapping-path s3://... [--upload]`

Output: `s3://data.sb/switchbox/marginal_costs/ny/supply/utility={utility}/year={YYYY}/data.parquet`
Schema: `timestamp` (datetime), `energy_cost_enduse` ($/MWh), `capacity_cost_enduse` ($/MWh)

## Data sources

| Dataset                | S3 path                                                                                    | Schema key columns                                                                  |
| ---------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| LBMP (energy prices)   | `s3://data.sb/nyiso/lbmp/real_time/zones/zone={ZONE_NAME}/year={Y}/month={M}/data.parquet` | `interval_start_est`, `zone`, `lbmp_usd_per_mwh` (5-minute intervals)               |
| ICAP (capacity prices) | `s3://data.sb/nyiso/icap/year={Y}/month={M}/data.parquet`                                  | `locality` (Categorical), `auction_type` (Categorical), `price_per_kw_month`        |
| NYISO zone loads       | `s3://data.sb/nyiso/hourly_demand/zones/zone={NAME}/year={Y}/month={M}/data.parquet`       | `timestamp`, `zone`, `load_mw`                                                      |
| Zone mapping           | `s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv`                              | `utility`, `load_zone_letter`, `lbmp_zone_name`, `icap_locality`, `capacity_weight` |

## Utility-to-zone mapping

Defined in two places that **must stay consistent** (enforced by `test_eia_zones_match_zone_mapping`):

- `data/eia/hourly_loads/eia_region_config.py` → `UTILITY_SERVICE_AREAS` (zones for EIA load aggregation)
- `data/nyiso/zone_mapping/generate_zone_mapping_csv.py` → `_MAPPING_ROWS` (zones + ICAP locality + capacity weight)

| Utility | Load Zones       | ICAP Locality         | Capacity Weight |
| ------- | ---------------- | --------------------- | --------------- |
| cenhud  | G                | GHIJ                  | 1.0             |
| coned   | G, H, I, J       | NYC: 0.87, GHIJ: 0.13 | split           |
| nimo    | A, B, C, D, E, F | NYCA                  | 1.0             |
| nyseg   | A, B, C, D, E, F | NYCA                  | 1.0             |
| or      | G                | GHIJ                  | 1.0             |
| rge     | B                | NYCA                  | 1.0             |
| psegli  | K                | LI                    | 1.0             |

## Energy MC (LBMP)

Real-time LBMP prices (5-minute intervals) are aggregated to hourly averages by taking the mean of all 5-minute intervals within each hour. These hourly prices are then used to compute energy marginal costs.

- **Single-zone utility** (rge, cenhud, or, psegli): uses that zone's hourly real-time LBMP directly.
- **Multi-zone utility** (nyseg, nimo, coned): load-weighted average across zones. For each hour: `Σ(LBMP_zone × load_zone) / Σ(load_zone)`, where zone loads come from NYISO zone-level hourly data.

## Capacity MC (ICAP MCOS)

Capacity MC uses a **component-by-component** approach (analogous to `compute_utility_bulk_tx_signal` in bulk TX). Each NYISO ICAP locality identifies its own peak hours independently from its own raw load profile; `capacity_weight` scales only the ICAP *cost*, never the load used for peak identification. This avoids the distortion that arises from blending differently-sized zone footprints before picking peaks.

Each `(icap_locality, gen_capacity_zone, capacity_weight)` row from the zone mapping is one component. All components are summed to produce the utility-level hourly signal. Non-zero hours equal the union of all localities' peak hours (up to `8 × 12 × n_localities` distinct hours).

### Locality models

**Nested localities** (used for peak-hour identification via raw zone-sum load profiles):

- `NYCA = WEST, GENESE, CENTRAL, NORTH, MHK_VL, CAPITL, HUD_VL, MILLWD, DUNWOD, N.Y.C., LONGIL`
- `LHV = HUD_VL, MILLWD, DUNWOD, N.Y.C.`
- `NYC = N.Y.C.`
- `LI = LONGIL`

`icap_locality` from mapping is normalized to nested locality:

- `NYCA → NYCA`
- `GHIJ → LHV`
- `NYC → NYC`
- `LI → LI`

**Partitioned localities** (used for ICAP price lookup, non-overlapping):

- `ROS → NYCA` (zones A–F)
- `LHV → LHV` (zones G–I)
- `NYC → NYC` (zone J)
- `LI → LI` (zone K)

`gen_capacity_zone` from the zone mapping selects which partitioned locality's price applies to a component.

### Allocation (per-component)

For each `(icap_locality, gen_capacity_zone, capacity_weight)` component:

1. Build raw unweighted load profile: sum zone loads for the nested locality footprint.
2. Normalize to Cairo-compatible 8760 hours.
3. For each month, sort hours by load (descending); select top 8.
4. Threshold = highest load strictly below the 8th-highest hour (tie-safe).
5. Exceedance_h = load_h − threshold for each top-8 hour.
6. Weight_h = exceedance_h / Σ(exceedance in month).
7. capacity_cost_h ($/kW) = weight_h × (icap_price_month × capacity_weight).

Sum all component `capacity_cost_h` values to get the utility-level hourly capacity cost. Convert to $/MWh by multiplying by 1000.

**ConEd example** (NYC: weight=0.87, GHIJ→LHV: weight=0.13):

- Component 1: peaks identified from raw NYC (J) load; cost = NYC ICAP price × 0.87
- Component 2: peaks identified from raw LHV (G–J) load; cost = LHV ICAP price × 0.13
- Final signal: sum of both components; non-zero hours = union (up to 192 distinct hours/year)

**Validation**: the sum of all hourly `capacity_cost_per_kw` values must equal `Σ_locality(capacity_weight × Σ_month(icap_price_per_kw_month))` within 0.01%. Computed via `compute_weighted_icap_prices` + `validate_capacity_allocation`.

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

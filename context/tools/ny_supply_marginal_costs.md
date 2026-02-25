# NY supply marginal costs pipeline

How energy (LBMP) and capacity (ICAP) supply marginal costs are generated for NY utilities.

## Script

`utils/pre/generate_utility_supply_mc.py`

CLI: `--utility cenhud --year 2025 --zone-mapping-path s3://... [--upload]`

Output: `s3://data.sb/switchbox/marginal_costs/ny/supply/utility={utility}/year={YYYY}/data.parquet`
Schema: `timestamp` (datetime), `energy_cost_enduse` ($/MWh), `capacity_cost_enduse` ($/MWh)

## Data sources

| Dataset | S3 path | Schema key columns |
|---|---|---|
| LBMP (energy prices) | `s3://data.sb/nyiso/lbmp/day_ahead/zones/zone={ZONE_NAME}/year={Y}/month={M}/data.parquet` | `interval_start_est`, `zone`, `lbmp_usd_per_mwh` |
| ICAP (capacity prices) | `s3://data.sb/nyiso/icap/year={Y}/month={M}/data.parquet` | `locality` (Categorical), `auction_type` (Categorical), `price_per_kw_month` |
| EIA zone loads | `s3://data.sb/eia/hourly_demand/zones/region=nyiso/zone={LETTER}/year={Y}/month={M}/data.parquet` | `timestamp`, `zone`, `load_mw` |
| EIA utility loads | `s3://data.sb/eia/hourly_demand/utilities/region=nyiso/utility={NAME}/year={Y}/month={M}/data.parquet` | `timestamp`, `utility`, `load_mw` |
| Zone mapping | `s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv` | `utility`, `load_zone_letter`, `lbmp_zone_name`, `icap_locality`, `capacity_weight` |

## Utility-to-zone mapping

Defined in two places that **must stay consistent** (enforced by `test_eia_zones_match_zone_mapping`):

- `data/eia/hourly_loads/eia_region_config.py` → `UTILITY_SERVICE_AREAS` (zones for EIA load aggregation)
- `data/nyiso/zone_mapping/generate_zone_mapping_csv.py` → `_MAPPING_ROWS` (zones + ICAP locality + capacity weight)

| Utility | Load Zones | ICAP Locality | Capacity Weight |
|---|---|---|---|
| cenhud | G | GHIJ | 1.0 |
| coned | G, H, J | NYC: 0.87, GHIJ: 0.13 | split |
| nimo | A, B, C, D, E, F | NYCA | 1.0 |
| nyseg | A, B, C, D, E, F | NYCA | 1.0 |
| or | G | GHIJ | 1.0 |
| rge | B | NYCA | 1.0 |
| psegli | K | LI | 1.0 |

## Energy MC (LBMP)

- **Single-zone utility** (rge, cenhud, or, psegli): uses that zone's day-ahead LBMP directly.
- **Multi-zone utility** (nyseg, nimo, coned): load-weighted average across zones. For each hour: `Σ(LBMP_zone × load_zone) / Σ(load_zone)`, where zone loads come from EIA zone-level hourly data.

## Capacity MC (ICAP MCOS)

Monthly ICAP Spot prices ($/kW-month) are allocated to individual hours using threshold-exceedance weighting on the utility's aggregate load profile:

1. For each month, sort hours by load (descending).
2. Threshold = load of the 9th-highest hour (i.e., the hour just below the top 8).
3. Exceedance_h = max(load_h − threshold, 0) for each hour.
4. Weight_h = exceedance_h / Σ(exceedance in month).
5. capacity_cost_h ($/kW) = weight_h × icap_price_month ($/kW-month).
6. Convert to $/MWh: multiply by 1000.

Non-peak hours get zero capacity cost. Only the top 8 load hours per month carry capacity costs.

**ConEd special case**: ICAP price is blended `0.87 × NYC_spot + 0.13 × GHIJ_spot` per month before allocation.

**Validation**: a 1 kW constant load must recover the sum of 12 monthly ICAP prices exactly (within tolerance). This validates that the hourly allocation sums back to the correct annual total.

## EIA load pipeline

Utility loads are aggregated from NYISO zone loads via `data/eia/hourly_loads/`:

- `fetch-zone-data` — fetches from EIA API to local parquet
- `aggregate-utility-loads` / `aggregate-all-utility-loads` — sums zone loads per utility
- `download` — syncs zone/utility parquet from S3 to local
- `upload` — syncs local to S3

All 7 NY utilities have 2025 load profiles on S3.

## 8760 normalization

Leap years (8784 hours) are normalized to 8760 by dropping Dec 31 hours when Feb 29 exists. Duplicate timestamps from DST fall-back are deduplicated by keeping the first occurrence. This matches CAIRO's expected 8760-row input.

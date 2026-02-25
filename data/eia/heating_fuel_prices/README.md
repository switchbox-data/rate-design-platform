# EIA-877 Heating Fuel Prices

State-level residential and wholesale prices for heating oil and propane,
sourced from the EIA's Heating Oil and Propane Update (Form EIA-877).

## Why we need this

ResStock includes buildings that heat with propane and fuel oil. To calculate
heating bills for these customers, we need state-average retail fuel prices at
monthly granularity.

## About the data product

**Survey**: EIA-877, the Winter Heating Fuels Telephone Survey. EIA surveys
fuel dealers by phone to collect retail and wholesale prices.

**Collection cadence**:

- **Oct–Mar (heating season)**: surveyed every Monday → 4–5 weekly observations
  per month.
- **Apr–Sep (off-season)**: surveyed on the second Monday of each month → 1
  observation per month. This off-season collection only began in **2024**.
  Before that, Apr–Sep had no data at all.

**Products**: No. 2 Fuel Oil / Heating Oil (`EPD2F`) and Propane (`EPLLPA`).

**Price types**: Residential (`PRS` — "Price Delivered to Residential
Consumers") and Wholesale (`PWR` — "Wholesale/Resale Price"). Prices are in
$/gallon excluding taxes.

**Geographic coverage**: ~39 states, concentrated in the Northeast and
Midwest. Not all states have both products:

- **Heating oil**: ~22 states (where oil heating is common)
- **Propane**: ~38 states (much broader)
- NY and RI have both products.

**History**: 1990-10 through present.

### Raw API fields

The EIA API v2 endpoint `petroleum/pri/wfr` returns these fields per row:

| Field                | Example                                                             | Description                                                                     |
| -------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `period`             | `2026-02-16`                                                        | Week-ending date (Monday of survey)                                             |
| `duoarea`            | `SNY`                                                               | Geography code (`S` + 2-letter state abbrev for states, `R*`/`NUS` for regions) |
| `area-name`          | `NEW YORK`                                                          | Human-readable area name                                                        |
| `product`            | `EPD2F`                                                             | Product code                                                                    |
| `product-name`       | `No 2 Fuel Oil / Heating Oil`                                       | Human-readable product                                                          |
| `process`            | `PRS`                                                               | `PRS` = residential, `PWR` = wholesale                                          |
| `process-name`       | `Price Delivered to Residential Consumers`                          | Human-readable process                                                          |
| `series`             | `W_EPD2F_PRS_SNY_DPG`                                               | EIA series ID                                                                   |
| `series-description` | `New York No. 2 Heating Oil Residential Price (Dollars per Gallon)` | Full description                                                                |
| `value`              | `4.317`                                                             | Price in $/gallon excl. taxes (string)                                          |
| `units`              | `$/GAL`                                                             | Always $/gallon                                                                 |

### What we aggregate and store

At fetch time, we:

1. Filter to state-level `duoarea` only (S-prefix codes; drop PADD/national
   aggregates).
2. Filter to the two products (`EPD2F`, `EPLLPA`) and two processes (`PRS`,
   `PWR`).
3. Normalize codes: `EPD2F` → `heating_oil`, `EPLLPA` → `propane`, `PRS` →
   `residential`, `PWR` → `wholesale`, `SNY` → `NY`.
4. Group weekly observations by (state, product, price_type, year, month) and
   take the **mean** of `price_per_gallon`. During the heating season this
   averages 4–5 weekly readings; off-season it's a single observation passed
   through.
5. Drop rows where the price is null (some state/product combos have null
   values, e.g. DC propane).

## Schema

Hive-partitioned parquet with partition columns `product`, `year`, `month`.

Within each partition file (`data.parquet`):

| Column             | Type    | Description                       |
| ------------------ | ------- | --------------------------------- |
| `state`            | String  | 2-char state abbreviation         |
| `price_type`       | String  | `residential` or `wholesale`      |
| `price_per_gallon` | Float64 | Monthly avg, $/gallon excl. taxes |

Partition columns (Hive-style, in order):

| Column    | Type   | Description                |
| --------- | ------ | -------------------------- |
| `product` | String | `heating_oil` or `propane` |
| `year`    | Int64  | Calendar year              |
| `month`   | Int64  | Calendar month (1–12)      |

**Sort order** within each partition: `state`, `price_type`.

## S3 path

```
s3://data.sb/eia/heating_fuel_prices/product={heating_oil,propane}/year=YYYY/month=M/data.parquet
```

## Known data availability gaps

- **Pre-2024, Apr–Sep**: No data. The off-season monthly survey started in
  2024. For 1990–2023, only Oct–Mar (heating season) data exists.
- **1990 starts in October**: The survey began in Oct 1990, so Jan–Sep 1990
  have no data.
- **State coverage is not all 50 states**: ~22 states for heating oil, ~38 for
  propane. States not in the survey (mostly South/West for heating oil) have no
  data here. For those states, fall back to SEDS annual data or PADD regional
  averages.
- **Wholesale coverage is sparser than residential**: Not all states that have
  residential prices also have wholesale prices.

## Pipeline

| Recipe                | What it does                                                                                                |
| --------------------- | ----------------------------------------------------------------------------------------------------------- |
| `fetch [start] [end]` | Fetch weekly data from EIA API, aggregate to monthly, write local parquet. Optional YYYY-MM start/end args. |
| `validate`            | Run QA checks on local parquet (schema, nulls, dupes, price ranges, invariants).                            |
| `update`              | Discover latest partition on S3, fetch only new months since then.                                          |
| `prepare`             | `fetch` + `validate` (no upload).                                                                           |
| `upload`              | `aws s3 sync` local parquet to S3.                                                                          |
| `clean`               | Remove local `parquet/` staging directory.                                                                  |

Typical incremental update workflow:

```bash
just -f data/eia/heating_fuel_prices/Justfile update
just -f data/eia/heating_fuel_prices/Justfile validate
just -f data/eia/heating_fuel_prices/Justfile upload
```

## API access

Uses the EIA API v2 at `https://api.eia.gov/v2/petroleum/pri/wfr/data/`.
Requires `EIA_API_KEY` in the project `.env` file (same key used by
`data/eia/hourly_loads/`). Register at https://www.eia.gov/opendata/.

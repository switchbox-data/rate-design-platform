# NY Marginal Cost Allocation System

This directory contains scripts for allocating marginal transmission and distribution costs to hourly price signals for New York utilities using the Probability of Peak (PoP) methodology.

## Overview

The system implements a three-phase pipeline:

1. **Phase 1: Zone Load Collection** - Fetch NYISO zone load data (zones A-K)
2. **Phase 2: Utility Aggregation** - Map zones to utilities and aggregate loads
3. **Phase 3: Cost Allocation** - Allocate $/kW-yr costs to $/kWh hourly signals using PoP

## Data Flow

```
NYISO Zone Data (A-K)
    ↓
s3://data.sb/nyiso/loads/year=YYYY/zone/zone_X.parquet
    ↓
Utility Aggregation (zone-to-utility mapping)
    ↓
s3://data.sb/nyiso/loads/year=YYYY/utility/{utility}.parquet
    ↓
Cost Allocation (PoP methodology)
    ↓
s3://data.sb/switchbox/marginal_costs/ny/{utility}/year={year}/mc_8760.parquet
```

## S3 Data Schema

### Zone Load Data
**Path**: `s3://data.sb/nyiso/loads/year={YYYY}/zone/zone_{A-K}.parquet`

**Schema**:
- `timestamp` (datetime): Hour ending timestamp
- `zone` (string): NYISO zone identifier (A-K)
- `load_mw` (float): Actual load in MW
- `forecast_mw` (float): Forecasted load in MW

**Example**:
```
timestamp            zone  load_mw  forecast_mw
2024-01-01 00:00:00  A     1234.5   1250.0
2024-01-01 01:00:00  A     1150.0   1175.0
...
```

### Utility Load Data
**Path**: `s3://data.sb/nyiso/loads/year={YYYY}/utility/{utility_name}.parquet`

**Schema**:
- `timestamp` (datetime): Hour ending timestamp
- `utility` (string): Utility name
- `load_mw` (float): Aggregated load in MW

**Example**:
```
timestamp            utility  load_mw
2024-01-01 00:00:00  NYSEG    5678.9
2024-01-01 01:00:00  NYSEG    5234.1
...
```

### Allocated Marginal Costs
**Path**: `s3://data.sb/switchbox/marginal_costs/ny/{utility}/year={year}/mc_8760.parquet`

**Schema**:
- `timestamp` (datetime): Hour ending timestamp
- `utility` (string): Utility name
- `year` (int): Target year for marginal costs
- `load_mw` (float): Utility load in MW
- `mc_upstream_per_kwh` (float): Upstream (transmission) marginal cost in $/kWh
- `mc_dist_per_kwh` (float): Distribution marginal cost in $/kWh
- `mc_total_per_kwh` (float): Total marginal cost in $/kWh
- `is_upstream_peak` (bool): Whether hour is in top 100 load hours
- `is_dist_peak` (bool): Whether hour is in top 50 load hours
- `w_upstream` (float): Probability of peak weight for upstream (sums to 1.0)
- `w_dist` (float): Probability of peak weight for distribution (sums to 1.0)

**Example**:
```
timestamp            utility  year  load_mw  mc_upstream_per_kwh  mc_dist_per_kwh  mc_total_per_kwh  is_upstream_peak  is_dist_peak  w_upstream  w_dist
2024-07-15 15:00:00  NYSEG    2026  8234.5   0.01234              0.00876          0.02110           true              true          0.01145     0.01876
2024-07-15 16:00:00  NYSEG    2026  8456.7   0.01289              0.00912          0.02201           true              true          0.01178     0.01923
2024-01-01 03:00:00  NYSEG    2026  3456.1   0.00000              0.00000          0.00000           false             false         0.00000     0.00000
...
```

## Scripts

### 1. fetch_nyiso_zone_loads.py

Fetches NYISO zone load data from the EIA API v2 and uploads to S3 with year partitioning.

**Setup**:
1. Register for a free EIA API key at https://www.eia.gov/opendata/
2. Create a `.env` file in the project root:
   ```bash
   EIA_API_KEY=your_actual_api_key_here
   ```

**Usage**:
```bash
# Fetch full year 2024
python fetch_nyiso_zone_loads.py --start 2024-01-01 --end 2024-12-31

# Fetch specific date range
python fetch_nyiso_zone_loads.py --start 2024-06-01 --end 2024-08-31

# Override API key via CLI
python fetch_nyiso_zone_loads.py --start 2024-01-01 --end 2024-12-31 --eia-api-key YOUR_KEY
```

**Arguments**:
- `--start`: Start date in YYYY-MM-DD format (required)
- `--end`: End date in YYYY-MM-DD format (required)
- `--eia-api-key`: EIA API key (optional if set in `.env` file)
- `--s3-base`: Base S3 path for uploads (default: s3://data.sb/nyiso/loads)

**Output**: 11 parquet files (one per zone A-K) at `s3://data.sb/nyiso/loads/year={YYYY}/zone/`

**Features**:
- Uses `python-dotenv` for API key management
- Automatic pagination for EIA API (5000 row limit per request)
- Interactive API key prompting if not found in `.env`
- Data validation (completeness, null checks, range checks)
- Error handling for rate limits and API failures

### 2. aggregate_utility_loads.py

Maps NYISO zones to utilities and creates aggregated utility-level load profiles.

**Usage**:
```bash
# Process all utilities
python aggregate_utility_loads.py --year 2024

# Process single utility
python aggregate_utility_loads.py --year 2024 --utility NYSEG
```

**Arguments**:
- `--year`: Target year (default: 2024)
- `--s3-base`: Base S3 path (default: s3://data.sb/nyiso/loads)
- `--utility`: Specific utility to process, or 'all' (default: all)

**Zone-to-Utility Mapping**:
```python
UTILITY_ZONE_MAPPING = {
    "NYSEG": ["A", "C", "D", "E", "F"],
    "RG&E": ["A", "B"],
    "Central Hudson": ["G"],
    "National Grid": ["A", "B", "C", "D", "E", "F"],
}
```

**Output**: Parquet files at `s3://data.sb/nyiso/loads/year={YYYY}/utility/{utility}.parquet`

### 3. generate_utility_tx_dx_MC.py

Allocates marginal transmission and distribution costs to hourly price signals using the Probability of Peak (PoP) methodology.

**Usage**:
```bash
python generate_utility_tx_dx_MC.py \
    --utility NYSEG \
    --year 2026 \
    --mc-table-path ../data/marginal_costs/ny_marginal_costs_2026_2035.csv
```

**Arguments**:
- `--utility`: Utility name (required)
- `--year`: Target year for MC allocation (2026-2035) (required)
- `--year-load`: Year of load profile to use (optional, defaults to --year)
- `--mc-table-path`: Path to marginal cost CSV table (required)
- `--nyiso-s3-base`: Base S3 path for NYISO loads (default: s3://data.sb/nyiso/loads)
- `--output-s3-base`: Base S3 path for output (default: s3://data.sb/switchbox/marginal_costs/ny)
- `--upstream-hours`: Number of top hours for upstream allocation (default: 100)
- `--dist-hours`: Number of top hours for distribution allocation (default: 50)

**Output**: `s3://data.sb/switchbox/marginal_costs/ny/{utility}/year={year}/mc_8760.parquet`

**Example with different load year**:
```bash
# Apply 2030 marginal costs to 2024 load shape
python generate_utility_tx_dx_MC.py \
    --utility NYSEG \
    --year 2030 \
    --year-load 2024 \
    --mc-table-path ../data/marginal_costs/ny_marginal_costs_2026_2035.csv
```

**Note on Validation**: The 1 kW constant load validation test is built into `generate_utility_tx_dx_MC.py` and runs automatically. If validation fails (error > 0.01%), the script will raise an error and prevent writing to S3.

## Probability of Peak (PoP) Methodology

The PoP method allocates annual capacity costs ($/kW-yr) to hourly price signals ($/kWh) based on load-weighted probabilities during peak hours.

### Algorithm

1. **Identify Peak Hours**:
   - Upstream window: Top 100 load hours
   - Distribution window: Top 50 load hours

2. **Calculate Load-Weighted Weights**:
   ```
   For each hour h:
     if h in top_100_hours:
       w_h_upstream = load_h / sum(top 100 loads)
     else:
       w_h_upstream = 0
     
     if h in top_50_hours:
       w_h_dist = load_h / sum(top 50 loads)
     else:
       w_h_dist = 0
   
   Verify: sum(w_h_upstream) = 1.0, sum(w_h_dist) = 1.0
   ```

3. **Allocate Costs to Hours**:
   ```
   For each hour h:
     P_h_upstream = MC_upstream * w_h_upstream  ($/kWh)
     P_h_dist = MC_dist * w_h_dist  ($/kWh)
     P_h_total = P_h_upstream + P_h_dist  ($/kWh)
   ```

4. **Validation**:
   - A constant 1 kW load for the year should result in total cost = MC_upstream + MC_dist ($/kW-yr)
   - Formula: `sum(1 kW * P_h * 1 hour) = MC_upstream + MC_dist`

### Example

Given:
- MC_upstream = 10.14 $/kW-yr
- MC_dist = 14.00 $/kW-yr (12.34 substation + 1.66 feeder)
- Top 100 load hours sum = 654,321 MW
- Hour h has load = 7,500 MW

Calculation:
- w_h_upstream = 7,500 / 654,321 = 0.01146
- P_h_upstream = 10.14 * 0.01146 = 0.1162 $/kWh

For all 8760 hours, the sum equals 10.14 $/kW-yr.

## Input Data

### Marginal Cost Table

**Path**: `rate_design/ny/hp_rates/data/marginal_costs/ny_marginal_costs_2026_2035.csv`

**Format**:
```csv
Utility,Year,Upstream,Distribution Substation,Primary Feeder,Total MC
Central Hudson,2026,0.00,0.00,3.03,3.03
Central Hudson,2027,0.00,0.07,3.10,3.17
NYSEG,2026,0.00,1.37,0.00,1.45
NYSEG,2027,0.10,4.37,0.00,4.72
RG&E,2026,0.00,0.00,0.00,0.00
RG&E,2027,19.92,14.28,0.00,37.47
```

**Source**: 2025 MCOS (Marginal Cost of Service) studies, collected in docket R2-84.

**Notes**:
- All costs in nominal $/kW-yr
- Uses "diluted" method (territory-wide peak-load weighted average)
- Covers Central Hudson, NYSEG, RG&E (2026-2035)
- ConEd, O&R, NiMo excluded (use different methodology)

## Testing

Run unit tests:
```bash
# Test marginal cost allocation logic
python tests/test_marginal_cost_allocation.py

# Test utility aggregation logic
python tests/test_utility_aggregation.py

# Or use pytest
pytest tests/test_marginal_cost_allocation.py
pytest tests/test_utility_aggregation.py
```

## Integration with ResStock

To apply marginal costs to ResStock meter data:

```python
import polars as pl
from cloudpathlib import S3Path
import io

# Load allocated marginal costs
mc_path = S3Path("s3://data.sb/switchbox/marginal_costs/ny/NYSEG/year=2026/mc_8760.parquet")
mc_df = pl.read_parquet(io.BytesIO(mc_path.read_bytes()))

# Load ResStock customer load data
# Assume: customer_df has columns: timestamp, bldg_id, cust_kwh

# Join by timestamp
joined = customer_df.join(
    mc_df.select(["timestamp", "mc_upstream_per_kwh", "mc_dist_per_kwh", "mc_total_per_kwh"]),
    on="timestamp",
    how="left"
)

# Calculate hourly marginal cost contribution
result = joined.with_columns([
    (pl.col("cust_kwh") * pl.col("mc_upstream_per_kwh")).alias("mc_upstream_cost"),
    (pl.col("cust_kwh") * pl.col("mc_dist_per_kwh")).alias("mc_dist_cost"),
    (pl.col("cust_kwh") * pl.col("mc_total_per_kwh")).alias("mc_total_cost"),
])

# Aggregate to annual costs per building
annual_costs = result.group_by("bldg_id").agg([
    pl.col("mc_upstream_cost").sum(),
    pl.col("mc_dist_cost").sum(),
    pl.col("mc_total_cost").sum(),
])
```

## Pipeline Example

Complete end-to-end example:

```bash
# 1. Fetch NYISO zone loads
python scripts/fetch_nyiso_zone_loads.py --year 2024

# 2. Aggregate to utility level
python scripts/aggregate_utility_loads.py --year 2024

# 3. Allocate marginal costs for all utilities and years
# (validation runs automatically in each script execution)
for utility in NYSEG "RG&E" "Central Hudson"; do
    for year in {2026..2035}; do
        python scripts/generate_utility_tx_dx_MC.py \
            --utility "$utility" \
            --year $year \
            --mc-table-path ../data/marginal_costs/ny_marginal_costs_2026_2035.csv
    done
done
```

## Troubleshooting

### Validation fails with large error

**Problem**: The 1 kW constant load test shows error > 0.01%

**Solutions**:
- Check that weights sum to exactly 1.0 (numerical precision issues)
- Verify that the correct number of peak hours was identified
- Ensure no missing or duplicate timestamps in the load profile

### Missing zone data

**Problem**: `Zone file not found` error in aggregation

**Solutions**:
- Run fetch_nyiso_zone_loads.py first to collect zone data
- Check that S3 paths are correct and accessible
- Verify year parameter matches available data

### Allocation produces all zeros

**Problem**: All marginal costs are zero in output

**Solutions**:
- Check that MC table has non-zero values for that utility/year
- Verify load profile was loaded correctly
- Check for mismatched year parameters (--year vs --year-load)

## Future Enhancements

1. **NYISO Data Fetching**: Implement actual NYISO API/CSV download logic
2. **ConEd/O&R/NiMo Support**: Add non-diluted methodology
3. **Load Forecasting**: Apply load growth factors for future years
4. **Alternative Methods**: Support uniform or other weighting methods
5. **Automation**: Create orchestration script for full pipeline
6. **Monitoring**: Add data quality checks and alerting

## References

- NYISO Load Data Portal: https://www.nyiso.com/load-data
- 2025 MCOS Studies: Docket R2-84
- NYISO Zone Definitions: See NYISO System Map

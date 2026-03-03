# NYSEG MCOS Analysis

## Overview

Computes marginal costs from NYSEG's 2025 MCOS study workbook (`nyseg_study_workpaper.xlsx`), prepared by CRA International. The workbook contains project-level data in **W2 (Investment Location Detail)**, which we aggregate using **NERA-style project-level formulas** for cross-utility consistency with NiMo, ConEd, O&R, and CenHud.

All values are at **primary distribution voltage** (no secondary loss adjustment). NYSEG's MCOS explicitly excludes NYISO Transmission Service Charges — there is no bulk TX cost center. All three cost centers (upstream, distribution substation, primary feeder) are local sub-transmission and distribution, included in the BAT input.

### Why NERA-style instead of CRA native?

CRA's native methodology computes MC at each substation using location-specific growth factors (W5), demand-related loss factors (W4), and N-0/N-1 capacity analysis — then aggregates upward through division-level tables (T4–T7) to system-wide tables (T1A, T2). This produces MCs that embed location-specific adjustments our other utilities don't use.

By reading the **raw project data** from W2 (capital, capacity, in-service date) and applying the same `Capital / Peak × Composite Rate` formula used by NiMo/ConEd/O&R/CenHud, we get a consistent methodology across all NY utilities. The trade-off is that our output differs from CRA's published tables (T1A, T2) by a few percent — see `context/domain/ny_mcos_studies_comparison.md` §9 for the full rationale.

## Cost centers

| Cost center             | Voltage class    | Description                        |
| ----------------------- | ---------------- | ---------------------------------- |
| Upstream Substation     | 115 kV / 46 kV   | Local sub-transmission substations |
| Upstream Feeder         | 115 kV / 34.5 kV | Local sub-transmission feeders     |
| Distribution Substation | 12.5 kV          | Distribution substations           |
| Primary Feeder          | 12.5 kV / 4 kV   | Primary distribution feeders       |

Upstream Substation and Upstream Feeder are combined into a single **upstream** bucket in the output CSVs (capacity-weighted for undiluted variants, summed for diluted).

## Study parameters

| Parameter       | Value                                    | Source                               |
| --------------- | ---------------------------------------- | ------------------------------------ |
| Study period    | 2026–2035 (10 years)                     | All sheets                           |
| Inflation       | 2.0%/yr                                  | CRA MCOS report                      |
| WACC            | 6.975%                                   | T1A row 43, T4 row 19                |
| System peak     | 2,035.73 MW                              | T4 row 20 col P (2035 forecast)      |
| Divisions       | 13                                       | T4 row 6 cols C–O                    |
| Composite rate  | 0.10248 (substations), 0.09801 (feeders) | Derived from W2 (see below)          |
| Projects parsed | 107 (of 201 in W2)                       | 94 are prospective with zero capital |

### Composite rate derivation

The composite rate folds together the **Economic Carrying Charge (ECC)** and **O&M + A&G loading** into a single annualization multiplier. It is derived from W2 by comparing the per-project "Marginal per-kW Investment" (col 31–40) to the "Final Marginal Annualized per-kW Cost" (col 51–60) at the project's ISD year:

```
composite_rate = col_51(ISD) / col_31(ISD)
```

The rate is constant within each equipment type:

| Equipment type | ECC    | O&M+A&G loading | Composite rate |
| -------------- | ------ | --------------- | -------------- |
| Substation     | 7.788% | 1.3159×         | 0.10248        |
| Feeder         | 7.350% | 1.3335×         | 0.09801        |

### Loss factors

From **W4 (Demand Losses)**, used to compute the "Total at Primary" column:

| Plant from              | Loss factor to primary |
| ----------------------- | ---------------------- |
| Upstream (Bulk) Dist.   | 1.0497                 |
| Distribution Substation | 1.0292                 |
| Primary Feeder          | 1.0220                 |

`Total_at_primary(Y) = upstream(Y) × 1.0497 + dist_sub(Y) × 1.0292 + primary(Y) × 1.0220`

## Data source: W2 (Investment Location Detail)

W2 contains 201 rows of individual project data. Each row has:

| Column       | Content                                           |
| ------------ | ------------------------------------------------- |
| C (3)        | Division                                          |
| E (5)        | Segment: "Upstream" or "Distribution"             |
| F (6)        | Substation / Location name                        |
| G (7)        | Equipment: "Substation" or "Feeder"               |
| I (9)        | ISD (In-Service Date)                             |
| J–S (10–19)  | Annual investment ($000) by year, 2026–2035       |
| T (20)       | Total nominal capital ($000)                      |
| U–AD (21–30) | Added Peak Load-Carrying Capability (MVA) by year |
| BK (63)      | Final load-carrying capability (MVA)              |

**107 projects** have nonzero capital and are parsed by the script. The remaining 94 are prospective planning projects with allocated capacity but zero capital (mostly ISD 2034–2035); they contribute no MC and are excluded.

### Cost center classification

| Segment      | Equipment  | Cost center      |
| ------------ | ---------- | ---------------- |
| Upstream     | Substation | `ups_sub`        |
| Upstream     | Feeder     | `ups_feed`       |
| Distribution | Substation | `dist_sub`       |
| Distribution | Feeder     | `primary_feeder` |

## How the script computes each variant

For each project `p`: `annualized_per_kw(p) = [total_capital_k(p) / final_capacity_mva(p)] × composite_rate(equipment)` in ISD-year dollars.

### 1. Incremental diluted

```
MC(Y) = Σ_{p: ISD(p)=Y} [annualized_per_kw(p) × capacity_mva(p)] / system_peak_mw
```

Cost of projects entering service in year Y, spread across the system peak.

### 2. Cumulative diluted

```
cumulative(Y) = Σ_{t=2026}^{Y} incremental(t) × (1.02)^(Y−t)
```

### 3. Incremental undiluted

```
MC(Y) = Σ_{p: ISD(p)=Y} [annualized(p) × cap(p)] / Σ_{p: ISD(p)=Y} [cap(p)]
```

Capacity-weighted average cost of projects entering service in year Y. Undefined (zero) for years with no projects.

### 4. Cumulative undiluted

```
MC(Y) = Σ_{p: ISD(p)≤Y} [annualized(p) × cap(p) × (1.02)^(Y−ISD(p))]
         / Σ_{p: ISD(p)≤Y} [cap(p)]
```

## Worked examples

### Example 1: Incremental diluted, distribution substation, 2028

Two dist_sub projects enter service in 2028:

| Project       | Capital ($000) | Capacity (MVA) | $/kW    | Annualized ($/kW-yr) |
| ------------- | -------------- | -------------- | ------- | -------------------- |
| Center Point  | 41,509         | 20.16          | 2,059.0 | 211.01               |
| Fourth Street | 27,900         | 67.14          | 415.6   | 42.59                |

```
total_cost = 211.01 × 20.16 + 42.59 × 67.14 = 4,253.9 + 2,859.4 = 7,113.3
MC = 7,113.3 / 2,035.73 = 3.49 $/kW-yr
```

Script output for dist_sub incremental diluted 2028 = $3.49 $/kW-yr. ✓

### Example 2: Incremental diluted, total at primary, 2028

2028 has dist_sub = $3.49 and primary_feeder = $0.38 (from 4 feeder projects). Upstream = $0.00 (no upstream projects in 2028).

```
total = 0.00 × 1.0497 + 3.49 × 1.0292 + 0.38 × 1.0220
      = 0.00 + 3.59 + 0.39 = 3.98 $/kW-yr
```

Script output for total incremental diluted 2028 = $3.98 $/kW-yr. ✓

### Example 3: Cumulative diluted, total at primary, 2029

Incremental total: 2028 = $3.98, 2029 = $3.77.

```
cumulative(2029) = 3.98 × 1.02 + 3.77 = 4.06 + 3.77 = $7.83/kW-yr
```

### Example 4: Incremental undiluted, distribution substation, 2028

Same two dist_sub projects as Example 1:

```
MC = (211.01 × 20.16 + 42.59 × 67.14) / (20.16 + 67.14)
   = 7,113.3 / 87.30 = 81.48 $/kW-yr
```

This is the capacity-weighted average $/kW-yr of the dist_sub project cohort entering in 2028. Center Point has high $/kW ($2,059) but contributes only 23% of the capacity; Fourth Street has low $/kW ($416) and dominates at 77%.

### Example 5: Cumulative undiluted, distribution substation, 2029

By 2029, four dist_sub projects are in service (2028's Center Point + Fourth Street, plus 2029's Orchard Park + Ferndale):

```
num = (211.01 × 20.16 × 1.02¹) + (42.59 × 67.14 × 1.02¹)
    + (57.26 × 11.74 × 1.02⁰) + (70.46 × 40.50 × 1.02⁰)
    = 4,338.9 + 2,916.6 + 672.2 + 2,853.6 = 10,781.3
den = 20.16 + 67.14 + 11.74 + 40.50 = 139.54
MC = 10,781.3 / 139.54 = 77.27 $/kW-yr
```

## Projects by ISD year

| ISD  | ups_sub | ups_feed | dist_sub | primary_feeder | Total |
| ---- | ------- | -------- | -------- | -------------- | ----- |
| 2028 | —       | —        | 2        | 4              | 6     |
| 2029 | 1       | —        | 2        | 1              | 4     |
| 2030 | 4       | 1        | 6        | 2              | 13    |
| 2031 | 2       | 1        | 1        | —              | 4     |
| 2032 | 1       | 1        | 7        | 3              | 12    |
| 2033 | 6       | 5        | 7        | 2              | 20    |
| 2034 | 3       | 4        | 8        | 5              | 20    |
| 2035 | 13      | 8        | 4        | 3              | 28    |

No projects enter service in 2026 or 2027 — the incremental MC is zero for those years. This differs from the CRA native output (T1A), which shows nonzero values because CRA attributes CWIP during construction.

## Outputs

8 CSVs in this directory:

| File                                         | Content                                           |
| -------------------------------------------- | ------------------------------------------------- |
| `nyseg_incremental_diluted_annualized.csv`   | Year-by-year incremental diluted (nominal + real) |
| `nyseg_incremental_diluted_levelized.csv`    | Levelized incremental diluted per cost center     |
| `nyseg_cumulative_diluted_annualized.csv`    | Year-by-year cumulative diluted                   |
| `nyseg_cumulative_diluted_levelized.csv`     | Levelized cumulative diluted                      |
| `nyseg_incremental_undiluted_annualized.csv` | Year-by-year incremental undiluted                |
| `nyseg_incremental_undiluted_levelized.csv`  | Levelized incremental undiluted                   |
| `nyseg_cumulative_undiluted_annualized.csv`  | Year-by-year cumulative undiluted                 |
| `nyseg_cumulative_undiluted_levelized.csv`   | Levelized cumulative undiluted                    |

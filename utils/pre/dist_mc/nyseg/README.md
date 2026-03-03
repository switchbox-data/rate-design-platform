# NYSEG MCOS Analysis

## Overview

Computes marginal costs from NYSEG's 2025 MCOS study workbook (`nyseg_study_workpaper.xlsx`), prepared by CRA International using the NERA methodology. The workbook aggregates project-level data at the **division level** (13 divisions) before computing system-wide MC.

All values are at **primary distribution voltage** (no secondary loss adjustment). NYSEG's MCOS explicitly excludes NYISO Transmission Service Charges — there is no bulk TX cost center. All three cost centers (upstream, distribution substation, primary feeder) are local sub-transmission and distribution, included in the BAT input.

## Cost centers

| Cost center             | Voltage class    | Description                        |
| ----------------------- | ---------------- | ---------------------------------- |
| Upstream Substation     | 115 kV / 46 kV   | Local sub-transmission substations |
| Upstream Feeder         | 115 kV / 34.5 kV | Local sub-transmission feeders     |
| Distribution Substation | 12.5 kV          | Distribution substations           |
| Primary Feeder          | 12.5 kV / 4 kV   | Primary distribution feeders       |

The script combines "Upstream Substation" and "Upstream Feeder" into a single **upstream** cost center using capacity-weighted averaging from T7.

## Study parameters

| Parameter          | Value                | Source                         |
| ------------------ | -------------------- | ------------------------------ |
| Study period       | 2026–2035 (10 years) | All sheets                     |
| Inflation          | 2.0%/yr              | CRA MCOS report                |
| WACC               | 6.975%               | T1A row 43, T4 row 19          |
| System peak        | 2,035.73 MW          | T4 row 20 col P (system total) |
| Divisions          | 13                   | T4 row 6 cols C–O              |
| Utilization factor | 90%                  | CRA planning criteria          |

The system peak is the **2035 forecast** (fixed across all study years), unlike NiMo/CenHud which use actual 2024 peaks.

## Workbook layout

### Summary sheets

| Sheet | Content                                                                   | Script use                                   |
| ----- | ------------------------------------------------------------------------- | -------------------------------------------- |
| T1A   | System-wide MC: Table 2 (division-weighted) + Table 3 (diluted, adjusted) | **Primary source** for incremental diluted   |
| T1B   | Cumulative MC (undiluted, per sub-type)                                   | Reference only                               |
| T2    | Undiluted MC (capacity-weighted per sub-type)                             | **Primary source** for incremental undiluted |

### Division-level sheets

| Sheet | Content                                                     | Script use                      |
| ----- | ----------------------------------------------------------- | ------------------------------- |
| T4    | Upstream sub MC per division ($/kVA of load added)          | Verification (derive Table 2)   |
| T4B   | Upstream feeder MC per division                             | Verification (derive Table 2)   |
| T5    | Distribution substation MC per division                     | Verification (derive Table 2)   |
| T6    | Primary feeder MC per division                              | Verification (derive Table 2)   |
| T7    | Investment ($000s) and capacity (MVA) per division per year | Capacity for upstream combining |

### Loss factors

| Sheet | Content                                      |
| ----- | -------------------------------------------- |
| W4    | Demand-related loss factors by voltage level |

The "Total at Primary" column in T1A and T2 applies loss factors from W4 to express the combined MC at primary voltage. Individual cost center columns are at their own voltage level. This means `total ≠ upstream + dist_sub + primary_feeder`; the total is ~3% higher due to loss factors.

## Workbook cell references

### T1A (system-wide diluted MC)

T1A contains two year-by-year tables, identified by searching for `col B == 2026`:

**First year block (Table 2, row 31):** Division-peak-weighted annual MC.

| Row   | Col C          | Col D            | Col E          | Col F            | Col G              |
| ----- | -------------- | ---------------- | -------------- | ---------------- | ------------------ |
| 28    | Header: "Year" |                  |                |                  |                    |
| 29    | $/kW-yr        | $/kW-yr          | $/kW-yr        | $/kW-yr          | $/kW-yr            |
| 31–40 | Upstream       | Dist. Substation | Primary Feeder | Total at Primary | Total at Secondary |
| 42    | Levelized      | 30.22            | 24.68          | 11.46            | 68.83              |
| 43    | WACC (0.06975) |                  |                |                  |                    |

**Second year block (Table 3, row 49):** System-wide MC adjusted for areas with no investment.

| Row   | Col C                               | Col D            | Col E          | Col F            | Col G              |
| ----- | ----------------------------------- | ---------------- | -------------- | ---------------- | ------------------ |
| 47    | Upstream                            | Dist. Substation | Primary Feeder | Total at Primary | Total at Secondary |
| 49–58 | Year-by-year diluted MC (2026–2035) |                  |                |                  |                    |
| 60    | Levelized Charge                    | 8.46             | 9.79           | 4.07             | **23.11**          |

**Table 3 is our incremental diluted output.** It differs from Table 2 because it adjusts for the fraction of each division's load served by substations with no planned investment.

### T2 (undiluted MC)

| Row   | Col C                                         | Col D        | Col E           | Col F     | Col G          | Col H            |
| ----- | --------------------------------------------- | ------------ | --------------- | --------- | -------------- | ---------------- |
| 9     | Year                                          | Upstream Sub | Upstream Feeder | Dist. Sub | Primary Feeder | Total at Primary |
| 12–21 | 2026–2035 year-by-year undiluted MC ($/kW-yr) |              |                 |           |                |                  |
| 23    | Levelized                                     | 30.37        | 35.27           | 48.41     | 42.33          | **162.00**       |

Values are capacity-weighted averages: `MC(sub-type, Y) = total_cost(Y) / total_capacity(Y)` across all projects of that sub-type entering service in year Y.

### T4/T4B/T5/T6 (division-level MC)

All four sheets share the same layout:

| Row  | Content                                                            |
| ---- | ------------------------------------------------------------------ |
| 5    | "Marginal Annualized Cost per kVA of Load Added..."                |
| 6    | Division names (cols C–O for 13 divisions, col P = System Average) |
| 7–16 | Year-by-year MC (2026–2035), one row per year                      |
| 18   | Levelized MC                                                       |
| 19   | WACC (0.06975)                                                     |
| 20   | Peak Load 2035 (MW) per division; col P = system total 2,035.73 MW |

The MC values are in **$/kVA of division peak load** — they represent the annual cost of new infrastructure spread across the entire division's peak demand, not per kVA of project capacity.

### T7 (investment and capacity per division)

T7 has three side-by-side sections:

| Section    | Columns | Content                                                    |
| ---------- | ------- | ---------------------------------------------------------- |
| Investment | B–G     | Division (B), Year (C), $000s for 4 sub-types (D–G)        |
| Capacity   | I–N     | Division (I), Year (J), MVA for 4 sub-types (K–N)          |
| MC per kW  | P–U     | Division (P), Year (Q), $/kW for upstream sub/feeder (R–S) |

Data starts at row 17. Each division has 10 rows (2026–2035) followed by a blank separator.

NYSEG divisions in order: Auburn (row 17), Binghamton (28), Brewster (39), Elmira (50), Geneva (61), Hornell (72), Ithaca (83), Lancaster (94), Liberty (105), Lockport (116), Mechanicville (127), Oneonta (138), Plattsburgh (149).

## How the script computes each variant

### 1. Incremental diluted (BAT input)

Read directly from T1A Table 3 (rows 49–58), columns C–F.

These values represent the system-wide MC from **new** projects entering service in each year, adjusted for areas with no investment, at primary voltage.

### 2. Cumulative diluted

Accumulated from incremental diluted with 2%/yr inflation:

```
cumulative(Y) = Σ_{t=2026}^{Y} incremental(t) × (1.02)^(Y−t)
```

Each prior year's new-project cost is inflated forward to year-Y dollars before summing.

### 3. Incremental undiluted

Read from T2 (rows 12–21). For individual sub-types (upstream sub, upstream feeder, dist sub, primary feeder) directly from columns C–F. The "total at primary" from column G includes loss-factor adjustment.

The **upstream** combined value is capacity-weighted from T7:

```
upstream(Y) = [MC_sub(Y) × cap_sub(Y) + MC_feed(Y) × cap_feed(Y)] / [cap_sub(Y) + cap_feed(Y)]
```

where capacity comes from T7 columns K (upstream sub) and L (upstream feeder), summed across all 13 divisions.

### 4. Cumulative undiluted

Accumulated using capacity-weighted inflation:

```
cumulative(Y) = Σ_{t≤Y}[mc(t) × cap(t) × (1.02)^(Y−t)] / Σ_{t≤Y}[cap(t)]
```

This weights each year's cost by how much capacity was added that year, then divides by total cumulative capacity.

## Table 2 derivation (verification)

The script independently derives the T1A Table 2 values from division-level data:

```
Table2_MC(cc, Y) = Σ_divisions [division_MC(cc, Y) × division_peak / system_peak]
```

where `division_MC` comes from T4/T4B/T5/T6 (rows 7–16) and `division_peak` from T4 row 20.

This verification confirms that we correctly understand the division-to-system aggregation. The max delta between derived and workbook Table 2 values is <0.0001 for individual cost centers. The total has a larger delta because our derived total is a simple sum (no loss factors), while the workbook total applies W4 loss factors.

## Table 2 vs. Table 3 (the within-division adjustment)

Table 2 spreads project costs across the full division peak, then weights by peak share. Table 3 further adjusts for the fact that within each division, only a fraction of substations have planned investment. The comparison doc notes "~77% of upstream and ~65% of dist substations/feeders have no investment."

The adjustment reduces the MC because it accounts for areas that will not see capacity investment. We read Table 3 directly from the workbook because deriving the within-division coverage fraction requires substation-level peak load data not readily available in the summary sheets.

## Levelized computation

CRA uses NPV-based levelization at the WACC:

```
levelized = Σ[MC(Y) / (1+WACC)^(Y−2026)] / Σ[1 / (1+WACC)^(Y−2026)]
```

The CSVs also report simple-average levelized in real (base-year 2026) dollars for cross-utility comparison.

## Worked examples

### Example 1: Auburn dist sub contribution to Table 2 (2026)

From T5, Auburn 2026 dist sub MC = 30.97 $/kVA.
From T4 row 20, Auburn peak = 77.94 MW, system peak = 2,035.73 MW.

```
Auburn contribution = 30.97 × (77.94 / 2035.73) = 30.97 × 0.03829 = 1.186 $/kW-yr
```

Only Auburn and Ithaca have dist sub investment in 2026. Adding Ithaca's contribution (10.31 × 112.84/2035.73 = 0.571) gives Table 2 dist sub 2026 ≈ 1.757 $/kW-yr.

Workbook Table 2 row 31 col D = 1.7572 $/kW-yr. ✓

### Example 2: Upstream undiluted combining (2028)

From T2 row 14: upstream sub = 18.64 $/kW-yr, upstream feeder = 9.39 $/kW-yr.
From T7 (summed across all divisions): upstream sub capacity 2028 = ~171 MVA, upstream feeder capacity 2028 = ~82 MVA.

```
upstream_combined = (18.64 × 171 + 9.39 × 82) / (171 + 82) = (3,185 + 770) / 253 = 15.63 $/kW-yr
```

### Example 3: Cumulative diluted 2027

Incremental diluted total at primary: 2026 = $1.41, 2027 = $4.60.

```
cumulative(2027) = 1.41 × 1.02 + 4.60 = 1.44 + 4.60 = $6.04/kW-yr
```

### Example 4: NPV-based levelized

Incremental diluted total at primary annual values: 1.41, 4.60, 8.78, 9.10, 29.74, 25.04, 48.07, 48.01, 49.27, 39.83.

```
PV numerator = 1.41/1.0 + 4.60/1.070 + 8.78/1.144 + ... + 39.83/1.836 = 173.81
PV denominator = Σ[1/(1.06975)^i for i=0..9] = 7.525
levelized = 173.81 / 7.525 = $23.11/kW-yr
```

Workbook T1A row 60 col F = $23.1138. ✓

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

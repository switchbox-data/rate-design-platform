# RG&E MCOS Analysis

## Overview

Computes marginal costs from RG&E's 2025 MCOS study workbook (`rge_study_workpaper.xlsx`), prepared by CRA International using the same methodology as NYSEG. Like NYSEG, we read project-level data from **W2 (Investment Location Detail)** and apply **NERA-style formulas** for cross-utility consistency.

**For full methodology documentation, formulas, composite rate derivation, and loss factor details, see [`nyseg/README.md`](../nyseg/README.md).** This README only notes RG&E-specific differences.

The RG&E script (`analyze_rge_mcos.py`) imports all shared parsing and computation logic from the NYSEG script, overriding only the configuration (utility name, system peak).

## Key differences from NYSEG

| Parameter                   | NYSEG                     | RG&E                                      |
| --------------------------- | ------------------------- | ----------------------------------------- |
| Divisions                   | 13 (Auburn … Plattsburgh) | 4 (Canandaigua, Central, Fillmore, Sodus) |
| System peak (2035 forecast) | 2,035.73 MW               | 1,428.52 MW                               |
| Projects parsed             | 107                       | 96                                        |
| Composite rate (substation) | 0.10248                   | 0.10283                                   |
| Composite rate (feeder)     | 0.09801                   | 0.09836                                   |
| Loss: upstream → primary    | 1.0497                    | 1.0543                                    |
| Loss: dist_sub → primary    | 1.0292                    | 1.0320                                    |
| Loss: primary → primary     | 1.0220                    | 1.0228                                    |

The composite rates and loss factors differ slightly because CRA derives them separately for each utility from their respective system characteristics.

## RG&E-specific notes

### Smaller system, higher diluted MC

RG&E has higher diluted MC than NYSEG despite similar total investment, because its system peak (1,429 MW) is 30% smaller than NYSEG's (2,036 MW). The same dollar of investment is spread across fewer kW.

### Project composition

| ISD  | ups_sub | ups_feed | dist_sub | primary_feeder | Total |
| ---- | ------- | -------- | -------- | -------------- | ----- |
| 2027 | —       | 3        | —        | —              | 3     |
| 2028 | —       | 1        | —        | —              | 1     |
| 2029 | —       | —        | 3        | 2              | 5     |
| 2030 | —       | 3        | 2        | 4              | 9     |
| 2031 | —       | 2        | 2        | 1              | 5     |
| 2032 | —       | 2        | 5        | 3              | 10    |
| 2033 | —       | 7        | 7        | 7              | 21    |
| 2034 | 1       | 7        | 7        | 7              | 22    |
| 2035 | —       | 4        | 6        | 10             | 20    |

RG&E has only 1 upstream substation project (vs. NYSEG's 30) — most upstream investment is in feeders. The earliest projects enter in 2027 (3 upstream feeders), so 2026 has zero MC.

### Levelized targets

| Variant                                        | RG&E         | NYSEG        |
| ---------------------------------------------- | ------------ | ------------ |
| Incremental diluted (simple avg, real $/kW-yr) | $18.10/kW-yr | $14.78/kW-yr |
| Cumulative diluted (simple avg, real $/kW-yr)  | $42.05/kW-yr | $40.38/kW-yr |

## Outputs

8 CSVs in this directory, following the same naming convention as NYSEG:

`rge_{cumulative,incremental}_{diluted,undiluted}_{annualized,levelized}.csv`

# RG&E MCOS Analysis

## Overview

Computes marginal costs from RG&E's 2025 MCOS study workbook (`rge_study_workpaper.xlsx`), prepared by CRA International using the same methodology as NYSEG.

**For full methodology documentation, workbook layout, formulas, and worked examples, see [`nyseg/README.md`](../nyseg/README.md).** This README only notes RG&E-specific differences.

The RG&E script (`analyze_rge_mcos.py`) imports all shared parsing and computation logic from the NYSEG script, overriding only the configuration (divisions, system peak, sheet name patterns).

## Key differences from NYSEG

| Parameter                   | NYSEG                         | RG&E                                      |
| --------------------------- | ----------------------------- | ----------------------------------------- |
| Divisions                   | 13 (Auburn … Plattsburgh)     | 4 (Canandaigua, Central, Fillmore, Sodus) |
| System peak (2035 forecast) | 2,035.73 MW                   | 1,428.52 MW                               |
| T4 sheet name               | "T4 Summary Upstream Subs"    | "T4a Summary Ups Sub"                     |
| T4B sheet name              | "T4B Summary Upstream feeder" | "T4B Summary Upstream Feeder"             |
| T5/T6 per-division layout   | Yes (same as T4)              | No (different layout; weighted values)    |

### T5/T6 layout difference

NYSEG's T5 and T6 sheets show per-division MC in the same layout as T4/T4B (division names in row 6, year-by-year MC in rows 7–16, peaks at row 20). RG&E's T5 and T6 have a different structure — they show weighted/summary values rather than per-division columns.

Because of this, the Table 2 verification step for RG&E is limited to **upstream only** (from T4a/T4B), while NYSEG verifies all four cost centers. The primary outputs (diluted from T1A Table 7, undiluted from T2) are unaffected.

### Table numbering in T1A

The diluted year-by-year table in T1A is labeled "Table 7" in RG&E (vs. "Table 3" in NYSEG). The script finds it dynamically by locating the second block of year-2026 rows in T1A.

### Levelized targets

| Variant                                       | RG&E          | NYSEG         |
| --------------------------------------------- | ------------- | ------------- |
| Incremental diluted (NPV, total at primary)   | $37.14/kW-yr  | $23.11/kW-yr  |
| Incremental undiluted (NPV, total at primary) | $105.24/kW-yr | $161.99/kW-yr |

RG&E has higher diluted MC than NYSEG because its smaller system peak (1,429 MW vs. 2,036 MW) concentrates costs.

## Outputs

8 CSVs in this directory, following the same naming convention as NYSEG:

`rge_{cumulative,incremental}_{diluted,undiluted}_{annualized,levelized}.csv`

# RG&E MCOS marginal cost analysis

Computes marginal costs from RG&E's 2025 MCOS study workbook (`rge_study_workpaper.xlsx`), prepared by CRA International using the same methodology as NYSEG. Like NYSEG, we read project-level data from **W2 (Investment Location Detail)** and apply **NERA-style formulas** for cross-utility consistency.

**For full methodology documentation, formulas, composite rate derivation, and loss factor details, see [`nyseg/README.md`](../nyseg/README.md).** This README only notes RG&E-specific differences.

The RG&E script (`analyze_rge_mcos.py`) imports all shared parsing and computation logic from the NYSEG script, overriding only the configuration (utility name, system peak).

## Cost centers

Same four cost centers as NYSEG (upstream substation, upstream feeder, distribution substation, primary feeder). See [NYSEG README](../nyseg/README.md#cost-centers) for details.

## Bulk TX treatment

Same as NYSEG: CRA's MCOS explicitly excludes NYISO Transmission Service Charges. There is no bulk TX cost center — all four cost centers are local sub-transmission and distribution. The BAT input is the sum of all cost centers (reported as `sub_tx_and_dist`).

## MC formula and variants

Same base formula and four variants as NYSEG. See [NYSEG README](../nyseg/README.md#mc-formula-and-variants) for full details.

## Study parameters

| Parameter                   | RG&E                                      | NYSEG (for comparison)    |
| --------------------------- | ----------------------------------------- | ------------------------- |
| Study period                | 2026–2035 (10 years)                      | 2026–2035 (10 years)      |
| System peak (2035 forecast) | 1,428.52 MW                               | 2,035.73 MW               |
| Inflation                   | 2.0%/yr                                   | 2.0%/yr                   |
| Divisions                   | 4 (Canandaigua, Central, Fillmore, Sodus) | 13 (Auburn … Plattsburgh) |
| Projects parsed             | 96                                        | 107                       |

### Composite rates

Slightly different from NYSEG because CRA derives them separately for each utility from their respective system characteristics:

| Equipment type | RG&E composite rate | NYSEG composite rate |
| -------------- | ------------------- | -------------------- |
| Substation     | 0.10283             | 0.10248              |
| Feeder         | 0.09836             | 0.09801              |

### Loss factors

| Plant from                | RG&E loss factor | NYSEG loss factor |
| ------------------------- | ---------------- | ----------------- |
| Upstream → primary        | 1.0543           | 1.0497            |
| Dist substation → primary | 1.0320           | 1.0292            |
| Primary → primary         | 1.0228           | 1.0220            |

## Per-project data

### Projects by ISD year

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

### Smaller system, higher diluted MC

RG&E has higher diluted MC than NYSEG despite similar total investment, because its system peak (1,429 MW) is 30% smaller than NYSEG's (2,036 MW). The same dollar of investment is spread across fewer kW.

### Levelized targets

| Variant                                        | RG&E         | NYSEG        |
| ---------------------------------------------- | ------------ | ------------ |
| Incremental diluted (simple avg, real $/kW-yr) | $18.10/kW-yr | $14.78/kW-yr |
| Cumulative diluted (simple avg, real $/kW-yr)  | $42.05/kW-yr | $40.38/kW-yr |

## Inputs and outputs

| Input         | Source                                                           |
| ------------- | ---------------------------------------------------------------- |
| RG&E workbook | `s3://data.sb/ny_psc/mcos_studies_2025/rge_study_workpaper.xlsx` |
| System peak   | 1,428.52 MW — T4 row 20 col P (2035 forecast)                    |

| Output                                                                        | Description                                                                                           |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `rge_{cumulative,incremental}_{diluted,undiluted}_{annualized,levelized}.csv` | 8 CSVs, same naming convention and harmonized two-bucket schema as NYSEG (bulk_tx=0, sub_tx_and_dist) |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-rge
```

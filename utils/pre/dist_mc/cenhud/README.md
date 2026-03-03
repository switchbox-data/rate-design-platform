# CenHud MCOS marginal cost analysis

Central Hudson (CenHud) uses the NERA marginal cost methodology, prepared by Demand Side Analytics (DSA) for Docket 19-E-0283 (June 2025). The workbook provides per-project data at three cost center levels — Local Transmission, Substation, and Feeder Circuit — with 8 named capital projects plus 3 "Future Unidentified" placeholders (one per cost center, representing expected years 6–10 investment).

CenHud is the **simplest** of the four NY utilities we analyze:

- Only 11 project entries (8 named + 3 future unidentified placeholders)
- **No bulk TX** to exclude — all three cost centers are local (69/115 kV and below)
- **Flat nominal costs in workbook** — the only NY utility with no built-in escalation; we apply a 2.1%/yr GDP deflator for cross-utility consistency (see below)
- Per-project annual costs are pre-computed in the workbook (row 26), already including ECCR, reserve margin, general plant loading, working capital, and loss factors

## Cost centers

All three cost centers are **included** in the BAT input. No exclusion or reclassification is needed.

| Cost center        | Workbook sheet       | Grid level              | Voltages      | Projects |
| ------------------ | -------------------- | ----------------------- | ------------- | -------: |
| Local Transmission | `Local Transmission` | Sub-transmission        | 69 kV, 115 kV |        3 |
| Substation         | `Substation`         | Distribution substation |               |        6 |
| Feeder Circuit     | `Feeder`             | Primary distribution    |               |        2 |

## No bulk TX

CenHud has no FERC-jurisdictional bulk transmission in its MCOS study. The "Local Transmission" cost center covers 10 local TX areas at 69 kV and 115/69 kV. The MCOS report explicitly labels these as "local." Cross-referencing against NYISO Gold Book Table VII confirms: there are no CenHud entries. All MCOS costs are therefore sub-transmission and distribution — the BAT input is the sum of all three cost centers.

## MC formula and variants

The script produces **four variants** by combining two capital perspectives with two denominators:

| Variant               | Projects in scope               | Denominator            | Perspective                        |
| --------------------- | ------------------------------- | ---------------------- | ---------------------------------- |
| Cumulative diluted    | All with in\_service\_year ≤ Y  | System peak (MW)       | MCOS cost allocation               |
| Incremental diluted   | Only with in\_service\_year = Y | System peak (MW)       | BAT economic cost (cost causation) |
| Cumulative undiluted  | All with in\_service\_year ≤ Y  | Cumulative project MW  | Per-project cost recovery          |
| Incremental undiluted | Only with in\_service\_year = Y | New capacity in year Y | Per-project marginal cost          |

Levelized = simple arithmetic mean of real MC across all 10 study years, no discounting.

**Diluted formula:**

```
MC_diluted(Y) = sum[ annual_cost_per_kW(p) × capacity_kW(p) ] / system_peak_kW
```

This is `total annual cost / system peak` — the same approach used by ConEd, O&R, and NiMo.

CenHud's workbook uses a different formula: `sum[ cost_per_kW × peak_share ]`, where `peak_share` is the project area's fraction of the system coincident peak load. That formula weights by area load share rather than project capacity; it produces different values because `peak_share(p) ≠ capacity(p) / system_peak`. We use the capacity-based formula for cross-utility consistency. See `context/domain/ny_mcos_studies_comparison.md` §7A for the full comparison.

**Undiluted formula:**

```
MC_undiluted(Y) = sum[ annual_cost_per_kW(p) × capacity_kW(p) ] / sum[ capacity_kW(p) ]
```

This is the **capacity-weighted average** of annual costs for in-scope projects.

### Escalation (applied for cross-utility consistency)

CenHud's workbook provides **flat nominal costs** — the only NY utility with no built-in escalation. A project's annual cost ($/kW-yr) in the workbook is identical every year after in-service.

For consistency with ConEd, O&R, and NiMo — all of which use a GDP Implicit Price Deflator — we apply a **2.1%/yr** GDP deflator with **base year 2026** (the first study year). This matches NiMo's steady-state rate and ConEd/O&R's rate from year 2 onward (their first year is 2.4%, then 2.1%):

```
escalation(Y) = 1.021^(Y − 2026)
nominal_mc(Y) = real_mc(Y) × escalation(Y)
```

The `real_mc` column preserves the workbook's original flat values (base-year 2026 dollars). The `nominal_mc` column includes the applied escalation. The **levelized MC** (mean of `real_mc`) is unaffected by this change — it reflects the workbook's actual cost levels.

| Year | Escalation factor |
| ---: | ----------------: |
| 2026 |            1.0000 |
| 2027 |            1.0210 |
| 2028 |            1.0424 |
| 2029 |            1.0643 |
| 2030 |            1.0867 |
| 2031 |            1.1095 |
| 2032 |            1.1328 |
| 2033 |            1.1566 |
| 2034 |            1.1808 |
| 2035 |            1.2056 |

### Difference from workbook Table 1

The workbook's Table 1 ("Marginal Costs for Areas with Projects") is NOT the same as our undiluted variant. The workbook computes:

```
Table 1 MC(Y) = diluted MC(Y) / sum_of_ALL_peak_shares
```

where the sum is over ALL projects in the cost center (not just in-scope ones). This divides the system-wide cost by the fraction of the system that has projects. Our undiluted variant uses project **capacity** (kW) as the denominator instead. Values differ by design.

## Workbook cell references

### System peak

| Item        | Source           | Value    |
| ----------- | ---------------- | -------- |
| System peak | MCOS report p. 3 | 1,103 MW |

The 2024 actual coincident peak. Passed as a CLI argument (`--system-peak-mw 1103.0`), not read from the workbook.

### Financial assumptions (sheet `Financial Assumptions`)

These parameters are already baked into row (26) of each MCOS sheet. Listed here for traceability:

| Row | Parameter                           | Local TX | Substation | Feeder |
| --: | ----------------------------------- | -------: | ---------: | -----: |
|   2 | Reserve margin (%)                  |      30% |        30% |    30% |
|   4 | General plant loading (%)           |    16.1% |      16.1% |  16.1% |
|   5 | ECCR — economic carrying charge (%) |   13.72% |     13.33% | 17.83% |
|   6 | A&G loading — plant-related (%)     |       0% |      1.33% |     0% |
|   9 | Material and supplies (%)           |    0.96% |      0.96% |  0.96% |
|  10 | Prepayments (%)                     |    1.00% |      1.00% |  1.00% |
|  12 | Working capital return (%)          |    8.95% |      8.95% |  8.95% |
|  13 | Loss factor                         |    1.014 |      1.018 | 1.0494 |
|  14 | Discount rate                       |    6.33% |      6.33% |  6.33% |
|  15 | Salvage value (%)                   |     2.5% |       2.5% |   2.5% |

### Per-project data — Local Transmission (sheet `Local Transmission`)

Row labels in column A (negative integers) identify calculation rows.

| Project             | Col | In-svc year (row 4) | Capacity kW (row 9, label -3) | Annual cost $/kW-yr (row 35, label -26) | Peak share (row 6, workbook only) |
| ------------------- | --: | ------------------: | ----------------------------: | --------------------------------------: | --------------------------------: |
| Future Unidentified |   E |                2032 |                       296,500 |                                   55.40 |                            20.97% |
| Northwest 115/69    |   G |                2035 |                       166,500 |                                   87.17 |                            12.25% |
| RD-RJ Lines         |   H |                2035 |                       130,000 |                                   14.71 |                             8.39% |

### Per-project data — Substation (sheet `Substation`)

| Project             | Col | In-svc year (row 5) | Capacity kW (row 10, label -3) | Annual cost $/kW-yr (row 36, label -26) | Peak share (row 7, workbook only) |
| ------------------- | --: | ------------------: | -----------------------------: | --------------------------------------: | --------------------------------: |
| Future Unidentified |   E |                2030 |                         61,850 |                                  109.51 |                             7.35% |
| Maybrook            |   G |                2027 |                         24,000 |                                   70.17 |                             1.69% |
| Pulvers 13kV        |   H |                2027 |                          7,250 |                                  109.51 |                             0.49% |
| Woodstock           |   I |                2029 |                          6,800 |                                  274.54 |                             1.56% |
| New Baltimore       |   J |                2026 |                         13,400 |                                    4.98 |                             1.50% |
| Hurley Ave          |   K |                2032 |                         10,400 |                                  245.75 |                             1.62% |

Note: substation peak shares appear at both row 7 (header section) and row 43 (year-by-year section) with identical values. The script finds row 7 first via text search.

### Per-project data — Feeder Circuit (sheet `Feeder`)

| Project             | Col | In-svc year (row 6) | Capacity kW (row 11, label -3) | Annual cost $/kW-yr (row 37, label -26) | Peak share (row 8, workbook only) |
| ------------------- | --: | ------------------: | -----------------------------: | --------------------------------------: | --------------------------------: |
| Future Unidentified |   E |                2025 |                         16,459 |                                   12.57 |                            24.14% |
| WI_8031             |   G |                2026 |                         13,000 |                                   16.45 |                             0.38% |

### "Future Unidentified Projects"

Each cost center includes a "Future Unidentified Projects" entry representing hypothetical future investment at the average cost per kW. CenHud's 5-year capital forecast identifies specific projects for years 1–5; years 6–10 assume a similar proportion of territory needs investment. These entries are included because they are part of CenHud's official MCOS estimate and appear in the validation targets (Tables 1 and 2).

## Worked examples

### Example 1: Cumulative diluted MC for Substation, year 2028

In 2028, three substation projects are in scope (in\_service\_year ≤ 2028):

- **New Baltimore**: in-svc 2026, cost = $4.98/kW-yr, capacity = 13,400 kW
- **Maybrook**: in-svc 2027, cost = $70.17/kW-yr, capacity = 24,000 kW
- **Pulvers 13kV**: in-svc 2027, cost = $109.51/kW-yr, capacity = 7,250 kW

```
numerator = 4.976 × 13,400 + 70.168 × 24,000 + 109.513 × 7,250
          = 66,678 + 1,684,032 + 793,969
          = $2,544,680
MC_diluted = 2,544,680 / 1,103,000 = $2.307/kW-yr
```

Note: the workbook's Table 2 value for this cell is **$1.796** — that uses peak-share weighting, not capacity-based. Our value differs by design (see §7A in `context/domain/ny_mcos_studies_comparison.md`).

### Example 2: Cumulative undiluted MC for Feeder, year 2027

In 2027, two feeder projects are in scope (in\_service\_year ≤ 2027):

- **Future Unidentified**: in-svc 2025, cost = $12.569/kW-yr, cap = 16,459 kW
- **WI\_8031**: in-svc 2026, cost = $16.453/kW-yr, cap = 13,000 kW

```
numerator = 12.569 × 16,459 + 16.453 × 13,000
          = 206,878 + 213,889
          = $420,767
denominator = 16,459 + 13,000 = 29,459 kW
MC_undiluted = 420,767 / 29,459 = $14.28/kW-yr
```

This is the capacity-weighted average of the two projects' annual costs.

### Example 3: Incremental diluted MC for all cost centers, year 2030

Only projects with in\_service\_year = 2030:

- **Substation Future Unidentified**: in-svc 2030, cost = $109.51/kW-yr, capacity = 61,850 kW
- No Local TX or Feeder projects have in-svc = 2030

```
MC_incremental_diluted(local_tx) = $0.00
MC_incremental_diluted(substation) = 109.513 × 61,850 / 1,103,000 = $6.143/kW-yr
MC_incremental_diluted(feeder) = $0.00
MC_incremental_diluted(total) = $6.143/kW-yr
```

### Example 4: Incremental undiluted MC for Substation, year 2027

Only substation projects with in\_service\_year = 2027:

- **Maybrook**: in-svc 2027, cost = $70.17/kW-yr, capacity = 24,000 kW
- **Pulvers 13kV**: in-svc 2027, cost = $109.51/kW-yr, capacity = 7,250 kW

```
numerator = 70.168 × 24,000 + 109.513 × 7,250
          = 1,684,032 + 793,969
          = $2,478,001
denominator = 24,000 + 7,250 = 31,250 kW
MC_incremental_undiluted = 2,478,001 / 31,250 = $79.30/kW-yr
```

This is the capacity-weighted average of only the new projects entering in that year.

## Workbook reference values

### Table 2 — System-wide (diluted) — workbook peak-share formula (NOT our output)

These are the workbook's own diluted values, computed with the peak-share formula. Our capacity-based diluted output will differ. Listed here for traceability.

| Year | Local TX | Substation | Feeder |  Total |
| ---: | -------: | ---------: | -----: | -----: |
| 2026 |    $0.00 |      $0.00 |  $3.03 |  $3.03 |
| 2027 |    $0.00 |      $0.07 |  $3.10 |  $3.17 |
| 2028 |    $0.00 |      $1.80 |  $3.10 |  $4.89 |
| 2029 |    $0.00 |      $1.80 |  $3.10 |  $4.89 |
| 2030 |    $0.00 |      $6.08 |  $3.10 |  $9.18 |
| 2031 |    $0.00 |     $14.13 |  $3.10 | $17.23 |
| 2032 |    $0.00 |     $14.13 |  $3.10 | $17.23 |
| 2033 |   $11.62 |     $18.10 |  $3.10 | $32.81 |
| 2034 |   $11.62 |     $18.10 |  $3.10 | $32.81 |
| 2035 |   $11.62 |     $18.10 |  $3.10 | $32.81 |

Source: workbook sheet `Summary - System`, rows 6–15. Uses peak-share weighting.

### Table 1 — Areas with Projects (NOT our undiluted)

| Year | Local TX | Substation | Feeder |
| ---: | -------: | ---------: | -----: |
| 2026 |    $0.00 |      $0.00 | $12.37 |
| 2027 |    $0.00 |      $0.52 | $12.63 |
| 2028 |    $0.00 |     $12.65 | $12.63 |
| 2029 |    $0.00 |     $12.65 | $12.63 |
| 2030 |    $0.00 |     $42.84 | $12.63 |
| 2031 |    $0.00 |     $99.50 | $12.63 |
| 2032 |    $0.00 |     $99.50 | $12.63 |
| 2033 |   $27.92 |    $127.47 | $12.63 |
| 2034 |   $27.92 |    $127.47 | $12.63 |
| 2035 |   $27.92 |    $127.47 | $12.63 |

Source: workbook sheet `Summary - System`, rows 21–30. These values use peak-share-based aggregation (see "Difference from workbook Table 1" above) and will NOT match our capacity-based undiluted variant.

## Inputs and outputs

| Input           | Source                                                              |
| --------------- | ------------------------------------------------------------------- |
| CenHud workbook | `s3://data.sb/ny_psc/mcos_studies_2025/cenhud_study_workpaper.xlsx` |
| System peak     | 1,103 MW — 2024 actual coincident peak (MCOS report p. 3)           |

| Output                                        | Description                                                        |
| --------------------------------------------- | ------------------------------------------------------------------ |
| `cenhud_cumulative_diluted_levelized.csv`     | Two rows (bulk_tx=0, sub_tx_and_dist): levelized and final-year MC |
| `cenhud_cumulative_diluted_annualized.csv`    | One row per year: bulk_tx (0) and sub_tx_and_dist (nominal/real)   |
| `cenhud_incremental_diluted_levelized.csv`    | Same structure, incremental ÷ system peak                          |
| `cenhud_incremental_diluted_annualized.csv`   | Same structure, incremental ÷ system peak                          |
| `cenhud_cumulative_undiluted_levelized.csv`   | Same structure, cumulative ÷ project capacity                      |
| `cenhud_cumulative_undiluted_annualized.csv`  | Same structure, cumulative ÷ project capacity                      |
| `cenhud_incremental_undiluted_levelized.csv`  | Same structure, incremental ÷ incremental capacity                 |
| `cenhud_incremental_undiluted_annualized.csv` | Same structure, incremental ÷ incremental capacity                 |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-cenhud
```

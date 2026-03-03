# Con Edison MCOS marginal cost analysis

ConEd's workbook already separates costs into five NERA cost centers that cleanly map to voltage tiers. No project-level classification of projects to tiers—bulk tx, sub tx, and dist—is needed — the workbook's cost center structure IS the tier assignment.

## Cost centers

| Cost center               | Workbook sheet     | Type                     | BAT tier                |
| ------------------------- | ------------------ | ------------------------ | ----------------------- |
| Transmission (138/345 kV) | CapEx Transmission | Cumulative 10-yr capital | Bulk TX (exclude)       |
| Area Station & Sub-TX     | CapEx Substation   | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Primary                   | CapEx Distribution | Annual sample            | Sub-TX + dist (include) |
| Transformer               | CapEx Distribution | Annual sample            | Sub-TX + dist (include) |
| Secondary                 | CapEx Distribution | Annual sample            | Sub-TX + dist (include) |

## Bulk TX treatment

### Why no project-level classification is needed

ConEd's CapEx Substation bundles sub-transmission feeders and area station transformers in the same project descriptions (e.g., "Install new 138/27kV Transformer 4 + new 138kV Feeder 38Q05 from Vernon"). These cannot be separated into distinct sub-TX and distribution tiers. But for the BAT, what matters is excluding bulk TX — the remaining cost centers all represent local delivery investment, and the bundled "Substation" cost center correctly captures all sub-TX and distribution substation spending.

### Gold Book cross-reference

#### Projects in CapEx Transmission vs. Gold Book

Both ConEd CapEx Transmission projects appear in NYISO Gold Book Table VII (pp. 157–158):

| Workbook project          | Stations (rows)                               | Voltage | Est. cost | Gold Book entries                                                                                                                                                 |
| ------------------------- | --------------------------------------------- | ------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Eastern Queens            | Idlewild (8), Hillside (9)                    | 138 kV  | $296M     | Eastern Queens Substation (p. 158, 2028), Idlewild (138/27, 2028), Hillside (138/27, 2033), Queens Clean Energy Hub (p. 161)                                      |
| Brooklyn Clean Energy Hub | Nevins (10), Gateway Park (11), Atlantic (12) | 345 kV  | $285M     | BCEH Substation (345, 2028), Gateway Park (345/138/13, 2028), Nevins (138/27, 2032), East 13th St (345, 2033), Atlantic (345/138/27, 2034); C24-021 offshore wind |

Cumulative CapEx Transmission capital: $636M (2025) → $1,447M (2028+, flat).

#### Gold Book projects NOT in CapEx Transmission

18 other ConEd Gold Book Table VII entries do not appear in CapEx Transmission. These include spare transformers (Parkchester No. 1, Mott Haven/Parkview, Fox Hills, Cedar St.), PAR feeders (Gowanus→Greenwood, Goethals→Fox Hills), MTA/Amtrak connections, reconductoring (Hudson Ave East), transformer replacements (Millwood West, Rainey, Fresh Kills), and reconfigurations (Buchanan North). They range from 138 kV to 345 kV and have in-service dates from 2025 to 2031. These are either captured in CapEx Substation or CapEx Distribution, or are maintenance/replacement projects that don't add demand-driven capacity and are therefore excluded from the MCOS.

#### Boundary accounting

Under the assumption that bulk TX is handled by a separate analysis using all Gold Book entries and we exclude all of CapEx Transmission:

- **Double-counting:** None. Both CapEx TX projects (Eastern Queens, BCEH) are in the Gold Book, so excluding them from our calc avoids any overlap.
- **Dropped projects:** None for ConEd. Every CapEx TX project has a Gold Book match.
- **Reverse overlap risk:** The 18 Gold Book entries not in CapEx Transmission could theoretically appear in CapEx Substation (17 area station rows) or CapEx Distribution (~143 projects). The MCOS cost centers are designed to be mutually exclusive within the workbook, so a project should appear in only one sheet. But the Gold Book is a separate NYISO reporting exercise — cross-checking CapEx Substation station names against Gold Book entries would confirm no overlap.

## MC formula and variants

Base formula (all variants):

```
Annual RR(Y) = Capital(Y) × Composite Rate × Escalation(Y)
MC(Y)        = Annual RR(Y) / Denominator   [$/kW-yr]
```

Units: capital in $000s ÷ denominator in MW = $/kW.

The script produces **four variants** by combining two capital perspectives with two denominators:

| Variant               | Capital(Y)                          | Denominator                  |
| --------------------- | ----------------------------------- | ---------------------------- |
| Cumulative diluted    | Accumulated in-service capital to Y | System peak (MW)             |
| Incremental diluted   | New capital entering service in Y   | System peak (MW)             |
| Cumulative undiluted  | Accumulated in-service capital to Y | Cumulative project MW to Y   |
| Incremental undiluted | New capital entering service in Y   | New project MW entering in Y |

For **annual cost centers** (Primary, Transformer, Secondary), the workbook provides a representative annual sample rather than a 10-year capital plan. The cumulative variants accumulate this repeating sample: `Cum_Capital(Y) = annual_sample × N` where N is the number of years from 2025 through Y. Incremental variants use the flat annual sample directly. For undiluted variants, capacity accumulates proportionally (same ratio every year), so real MC is the same constant for both cumulative and incremental undiluted.

For **cumulative cost centers** (Transmission, Substation), the script reads each project's right-half cumulative cashflow columns (W–AF) and infers in-service year as the first year where cashflow stabilizes at its final value (CWIP ends). Capital and capacity are then built using **in-service-year scoping**: `Capital(Y) = sum(p.final_capital for projects where in_service_year ≤ Y)`, and likewise for capacity. This matches the NiMo/CenHud project-level methodology: each project's full capital and MW enter the MC calculation together when the project completes, excluding pre-completion CWIP. As a result, cumulative values are lower in early years (when many projects are under construction) but converge to the same total by the end of the study period.

See `context/domain/ny_mcos_studies_comparison.md` §6–§7 for the rationale and cross-utility comparison.

### Formulas for each variant

All variants share the base formula for annual revenue requirement:

```
Annual RR(Y) = Capital(Y) × Composite Rate × Escalation(Y)
```

They differ in Capital(Y) and the denominator:

**Cumulative diluted** (MCOS perspective — the infrastructure bill allocated across all customers):

```
MC(Y) = Cum_Capital(Y) × Rate × Esc(Y) / System_Peak
```

**Incremental diluted** (BAT perspective — cost of new investment per kW of system load):

```
Inc_Capital(Y) = Cum_Capital(Y) − Cum_Capital(Y−1)
MC(Y) = Inc_Capital(Y) × Rate × Esc(Y) / System_Peak
```

**Cumulative undiluted** (cost per kW of project capacity):

```
Capacity(Y)   = sum(p.MW for projects where p.in_service_year ≤ Y)
Cum_Capital(Y) = sum(p.final_capital for projects where p.in_service_year ≤ Y)
MC(Y) = Cum_Capital(Y) × Rate × Esc(Y) / Capacity(Y)
```

Unlike the diluted variants, the denominator grows in discrete steps as projects complete. Real MC varies by year because the $/kW of each project cohort differs.

**Incremental undiluted** (cost per kW of new capacity added in year Y):

```
Inc_Capital(Y) = sum(p.final_capital for projects where p.in_service_year = Y)
Inc_Capacity(Y) = sum(p.MW for projects where p.in_service_year = Y)
MC(Y) = Inc_Capital(Y) × Rate × Esc(Y) / Inc_Capacity(Y)
```

Non-zero only in years when projects complete. The value reflects the actual $/kW of the specific projects entering service that year — it varies significantly across years depending on which projects complete.

For **annual cost centers** (Primary, Transformer, Secondary), the annual sample is constant, but cumulative variants accumulate it: `Cum_Capital(Y) = sample × N` (where N counts years 2025 through Y). This makes cumulative diluted grow over the study window while incremental diluted stays flat. For undiluted variants, capacity accumulates proportionally (`Cum_Capacity(Y) = sample_MW × N`), so the capital/capacity ratio — and real MC — is the same constant for both cumulative and incremental.

**Levelized** = mean of real MC over the 7-year levelization window (2026–2032), chosen to capture near-term planned investment while excluding speculative back-loaded projects. See `context/domain/ny_mcos_studies_comparison.md` §10 for rationale.

## Study parameters

| Parameter    | Value        | Source                                     |
| ------------ | ------------ | ------------------------------------------ |
| Study period | 2025–2034    | All CapEx sheets                           |
| System peak  | 11,997.7 MW  | Coincident Load B26 (ASC total, 2025 fcst) |
| Escalation   | GDP deflator | Carrying Charge Loaders row 25, 2.4%→2.1%  |

**System Peak** — sheet **Coincident Load**, cell **B26** (Area Station Coincident Total, 2025 forecast = 11,997.7 MW). This is the sum of individual area station coincident peaks, NOT the true system peak (which is smaller — see coincidence factor below). Using the ASC total here is correct because the composite rate in column O has already been adjusted to compensate.

### Composite rates

Sheet **Carrying Charge Loaders** (note: sheet name has a trailing space), column **O** (Schedule 11 col 13, "Annual MC at System Peak"). The composite rate is a single multiplier per cost center that folds together the ECCR (~10%), general plant loading (7–24%), O&M (~1.1–1.2%), working capital return (~2.4% × 9.3%), loss factors, and a **coincidence factor** (0.95175). Other utilities apply these as separate steps; ConEd pre-multiplies them into one number for a cleaner formula: `Annual RR = Capital × Composite Rate × Escalation`.

**Why the coincidence factor is embedded.** Individual area stations peak at different hours. The sum of their individual peaks (the ASC total, 11,997.7 MW) is larger than the true system coincident peak (~11,418 MW) because not all stations peak simultaneously. The ratio is the coincidence factor: 11,418 / 11,997.7 ≈ 0.95175. ConEd's workbook presents capital at the area station level (summing to the ASC total), so the composite rate is scaled down by 0.95175 to produce the correct system-peak-level MC. Algebraically: `Capital × (Rate × 0.95175) / ASC_total = Capital × Rate / System_peak`. This is purely an accounting convenience — the final MC values are identical either way. No other utility in our analysis uses a coincidence factor because they work directly with a single system peak number.

| Cost center  | Cell | Value   |
| ------------ | ---- | ------- |
| Transmission | O12  | 0.13527 |
| Substation   | O13  | 0.12832 |
| Primary      | O14  | 0.12895 |
| Transformer  | O15  | 0.11271 |
| Secondary    | O16  | 0.12754 |

### Escalation

Sheet **Carrying Charge Loaders**, row **25** (GDP Implicit Price Deflator), columns C–L for years 2025–2034. Base year 2025 = 1.0; 2026 inflates at 2.4%, then 2.1%/yr thereafter.

| Year | Cell | Value  |
| ---- | ---- | ------ |
| 2025 | C25  | 1.0000 |
| 2026 | D25  | 1.0240 |
| 2027 | E25  | 1.0455 |
| 2028 | F25  | 1.0675 |
| 2029 | G25  | 1.0899 |
| 2030 | H25  | 1.1128 |
| 2031 | I25  | 1.1361 |
| 2032 | J25  | 1.1600 |
| 2033 | K25  | 1.1844 |
| 2034 | L25  | 1.2092 |

## Per-project data

Parsed from the right-half cashflow columns (W–AF) of each cumulative cost center sheet. In-service year is inferred as the first year where the cumulative cashflow equals its final value (CWIP construction spending ends). These tables are the primary audit artifact for the project-level methodology.

### CapEx Transmission — 5 projects

Rows 8–12. All share the same 358 MW capacity (ConEd's TX planning increment). All complete in 2028.

| Row | Name          |          MW | Capital ($000s) | In-service |
| --: | ------------- | ----------: | --------------: | ---------: |
|   8 | Idlewild      |       358.0 |         296,052 |       2028 |
|   9 | Hillside      |       358.0 |         296,052 |       2028 |
|  10 | Nevins Street |       358.0 |         285,000 |       2028 |
|  11 | Gateway Park  |       358.0 |         285,000 |       2028 |
|  12 | Atlantic      |       358.0 |         285,000 |       2028 |
|     | **Total**     | **1,790.0** |   **1,447,105** |            |

### CapEx Substation — 17 projects

Rows 9–25. Projects span 2026–2034, with the largest cohort completing in 2028 (4 projects, $2B) and 2034 (2 projects, $2.1B).

| Row | Name                |          MW | Capital ($000s) | In-service |
| --: | ------------------- | ----------: | --------------: | ---------: |
|   9 | Newtown             |       116.0 |         142,866 |       2026 |
|  10 | Parkchester #2      |        81.0 |          44,025 |       2028 |
|  11 | Idlewild (new)      |       358.0 |         379,229 |       2028 |
|  12 | Gateway (new)       |       358.0 |       1,344,121 |       2028 |
|  13 | Parkview            |        85.0 |         238,878 |       2028 |
|  14 | Mott Haven          |        84.0 |          47,340 |       2029 |
|  15 | Glendale            |       127.0 |         133,000 |       2030 |
|  16 | Fox Hills           |       118.0 |          54,000 |       2030 |
|  17 | Parkchester #1      |        76.0 |         124,900 |       2029 |
|  18 | Millwood West       |        79.0 |          84,000 |       2031 |
|  19 | Cedar Street        |        85.0 |         252,700 |       2031 |
|  20 | Nevins Street (new) |       358.0 |       1,465,000 |       2032 |
|  21 | Grasslands          |        82.0 |         128,000 |       2033 |
|  22 | Hillside (new)      |       358.0 |         871,710 |       2032 |
|  23 | Bruckner            |        32.0 |          19,675 |       2032 |
|  24 | Industry City (new) |       269.0 |       1,040,000 |       2034 |
|  25 | Atlantic (new)      |       358.0 |       1,050,000 |       2034 |
|     | **Total**           | **3,024.0** |   **7,419,444** |            |

### Annual capital (distribution)

Sheet **CapEx Distribution**, row **151** (total). Values are in **dollars** (not $000s; the code divides by 1,000 for consistency with the other CapEx sheets). The same capital is used for every year.

| Cost center | Cell | Value ($)   | As $000s |
| ----------- | ---- | ----------- | -------- |
| Primary     | L151 | ~13,100,000 | 13,100   |
| Transformer | M151 | ~5,900,000  | 5,900    |
| Secondary   | N151 | ~16,800,000 | 16,800   |

### Cumulative capital summary

Derived from the per-project tables above using in-service-year scoping. Each project's full capital enters the total in the year it completes (cashflow stabilizes). Values step up discretely as projects come in service, unlike the smooth CWIP-based totals in the left-half Section 2 rows (F–O). At 2034 the totals converge because all projects are in service.

| Year | TX Capital ($000s) | TX Capacity (MW) | Sub Capital ($000s) | Sub Capacity (MW) |
| ---- | ------------------ | ---------------- | ------------------- | ----------------- |
| 2025 | 0                  | 0                | 0                   | 0                 |
| 2026 | 0                  | 0                | 142,866             | 116               |
| 2027 | 0                  | 0                | 142,866             | 116               |
| 2028 | 1,447,105          | 1,790            | 2,149,119           | 998               |
| 2029 | 1,447,105          | 1,790            | 2,321,459           | 1,158             |
| 2030 | 1,447,105          | 1,790            | 2,508,459           | 1,403             |
| 2031 | 1,447,105          | 1,790            | 2,845,159           | 1,567             |
| 2032 | 1,447,105          | 1,790            | 5,201,544           | 2,315             |
| 2033 | 1,447,105          | 1,790            | 5,329,544           | 2,397             |
| 2034 | 1,447,105          | 1,790            | 7,419,544           | 3,024             |

## Worked examples

### Cumulative diluted — Substation, year 2028

Projects in-service by 2028: Newtown (2026, $142,866k), Parkchester #2, Idlewild, Gateway, Parkview (all 2028).

```
Capital(2028) = 142,866 + 44,025 + 379,229 + 1,344,121 + 238,878 = 2,149,119 ($000s)
Composite Rate = Carrying Charge O13 = 0.12832
Escalation     = Carrying Charge F25 = 1.0675
System Peak    = Coincident Load B26 = 11,997.7 MW

Nominal RR = 2,149,119 × 0.12832 × 1.0675 = 294,352 ($000s)
Diluted MC = 294,352 / 11,997.7            = $24.53/kW-yr (nominal)
Real MC    = 2,149,119 × 0.12832 / 11,997.7 = $22.99/kW-yr
```

### Cumulative diluted — Primary, year 2028

For annual cost centers, the workbook provides a representative annual sample rather than a 10-year plan. The cumulative variant accumulates this sample: by 2028 (year 4), four years of Primary capital are in service.

```
Annual Sample  = CapEx Distribution L151 ≈ $13,100,000 → 13,100 ($000s)
Cum Capital    = 13,100 × 4 years        = 52,400 ($000s)
Composite Rate = Carrying Charge O14      = 0.12895
Escalation     = Carrying Charge F25      = 1.0675
System Peak    = Coincident Load B26      = 11,997.7 MW

Nominal RR = 52,400 × 0.12895 × 1.0675 = 7,213 ($000s)
Diluted MC = 7,213 / 11,997.7           = $0.60/kW-yr (nominal)
Real MC    = 52,400 × 0.12895 / 11,997.7 = $0.56/kW-yr
```

Compare with incremental diluted in the same year: $0.15/kW-yr nominal. The difference ($0.45) is the carried-over capital from years 2025–2027.

### Incremental diluted — Substation, year 2028

Four projects come in-service in 2028 (Parkchester #2, Idlewild, Gateway, Parkview):

```
Inc Capital(2028)  = 44,025 + 379,229 + 1,344,121 + 238,878 = 2,006,253 ($000s)
Composite Rate     = Carrying Charge O13  = 0.12832
Escalation(2028)   = Carrying Charge F25  = 1.0675
System Peak        = 11,997.7 MW

Annual RR  = 2,006,253 × 0.12832 × 1.0675 = 274,768 ($000s)
Diluted MC = 274,768 / 11,997.7            = $22.90/kW-yr (nominal)
Real MC    = 2,006,253 × 0.12832 / 11,997.7 = $21.46/kW-yr
```

Compare with cumulative diluted in 2028: $24.53/kW-yr nominal. The difference ($1.63) is Newtown's contribution (in-service since 2026).

### Cumulative undiluted — Substation, year 2028

Using in-service-year scoping — same 5 projects as the diluted example above:

```
Cum Capital(2028) = 2,149,119 ($000s)  [same as diluted example]
Capacity(2028)    = 116 + 81 + 358 + 358 + 85 = 998 MW  [actual project MW]
Composite Rate    = 0.12832
Escalation(2028)  = 1.0675

Nominal RR   = 2,149,119 × 0.12832 × 1.0675 = 294,352 ($000s)
Undiluted MC = 294,352 / 998                  = $295/kW-yr (nominal)
Real MC      = 2,149,119 × 0.12832 / 998     = $276/kW-yr
```

Unlike the old proportional approach, real MC is NOT constant — it depends on the $/kW mix of in-service projects. Compare 2034 (all 17 projects, 3,024 MW): real MC = 7,419,544 × 0.12832 / 3,024 = $315/kW-yr.

### Incremental undiluted — Substation, year 2028

The 4 projects entering service in 2028:

```
Inc Capital(2028) = 2,006,253 ($000s)  [4 projects' final capitals]
Inc Capacity(2028) = 81 + 358 + 358 + 85 = 882 MW  [4 projects' MW]
Composite Rate     = 0.12832
Escalation(2028)   = 1.0675

Annual RR      = 2,006,253 × 0.12832 × 1.0675 = 274,768 ($000s)
Undiluted MC   = 274,768 / 882                  = $312/kW-yr (nominal)
Real MC        = 2,006,253 × 0.12832 / 882     = $292/kW-yr
```

Incremental undiluted is non-zero only in years when projects complete. It reflects the $/kW of each cohort — $292/kW for the 2028 class vs. $158/kW for Newtown in 2026 (lower because Newtown is a smaller, cheaper project per kW).

## Inputs and outputs

| Input          | Source                                                             |
| -------------- | ------------------------------------------------------------------ |
| ConEd workbook | `s3://data.sb/ny_psc/mcos_studies_2025/coned_study_workpaper.xlsx` |
| System peak    | 11,997.7 MW — Coincident Load sheet row 26, 2025 forecast          |

| Output                                               | Description                                                      |
| ---------------------------------------------------- | ---------------------------------------------------------------- |
| `outputs/coned_cumulative_diluted_levelized.csv`     | Two rows (bulk_tx, sub_tx_and_dist): levelized and final-year MC |
| `outputs/coned_cumulative_diluted_annualized.csv`    | One row per year: bulk_tx and sub_tx_and_dist (nominal/real)     |
| `outputs/coned_incremental_diluted_levelized.csv`    | Same structure, incremental capital ÷ system peak                |
| `outputs/coned_incremental_diluted_annualized.csv`   | Same structure, incremental capital ÷ system peak                |
| `outputs/coned_cumulative_undiluted_levelized.csv`   | Same structure, cumulative capital ÷ project capacity            |
| `outputs/coned_cumulative_undiluted_annualized.csv`  | Same structure, cumulative capital ÷ project capacity            |
| `outputs/coned_incremental_undiluted_levelized.csv`  | Same structure, incremental capital ÷ incremental capacity       |
| `outputs/coned_incremental_undiluted_annualized.csv` | Same structure, incremental capital ÷ incremental capacity       |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-coned
```

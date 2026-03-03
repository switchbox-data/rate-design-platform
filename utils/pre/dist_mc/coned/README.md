# Con Edison MCOS dilution analysis

ConEd's workbook already separates costs into five NERA cost centers that cleanly map to voltage tiers. No project-level classification of projects to tiers—bulk tx, sub tx, and dist—is needed — the workbook's cost center structure IS the tier assignment.

## Cost centers and tier mapping

| Cost center               | Workbook sheet     | Type                     | BAT tier                |
| ------------------------- | ------------------ | ------------------------ | ----------------------- |
| Transmission (138/345 kV) | CapEx Transmission | Cumulative 10-yr capital | Bulk TX (exclude)       |
| Area Station & Sub-TX     | CapEx Substation   | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Primary                   | CapEx Distribution | Annual sample            | Sub-TX + dist (include) |
| Transformer               | CapEx Distribution | Annual sample            | Sub-TX + dist (include) |
| Secondary                 | CapEx Distribution | Annual sample            | Sub-TX + dist (include) |

## Why no project-level classification is needed

ConEd's CapEx Substation bundles sub-transmission feeders and area station transformers in the same project descriptions (e.g., "Install new 138/27kV Transformer 4 + new 138kV Feeder 38Q05 from Vernon"). These cannot be separated into distinct sub-TX and distribution tiers. But for the BAT, what matters is excluding bulk TX — the remaining cost centers all represent local delivery investment, and the bundled "Substation" cost center correctly captures all sub-TX and distribution substation spending.

## Gold Book cross-reference

### Projects in CapEx Transmission vs. Gold Book

Both ConEd CapEx Transmission projects appear in NYISO Gold Book Table VII (pp. 157–158):

| Workbook project          | Stations (rows)                               | Voltage | Est. cost | Gold Book entries                                                                                                                                                 |
| ------------------------- | --------------------------------------------- | ------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Eastern Queens            | Idlewild (8), Hillside (9)                    | 138 kV  | $296M     | Eastern Queens Substation (p. 158, 2028), Idlewild (138/27, 2028), Hillside (138/27, 2033), Queens Clean Energy Hub (p. 161)                                      |
| Brooklyn Clean Energy Hub | Nevins (10), Gateway Park (11), Atlantic (12) | 345 kV  | $285M     | BCEH Substation (345, 2028), Gateway Park (345/138/13, 2028), Nevins (138/27, 2032), East 13th St (345, 2033), Atlantic (345/138/27, 2034); C24-021 offshore wind |

Cumulative CapEx Transmission capital: $636M (2025) → $1,447M (2028+, flat).

### Gold Book projects NOT in CapEx Transmission

18 other ConEd Gold Book Table VII entries do not appear in CapEx Transmission. These include spare transformers (Parkchester No. 1, Mott Haven/Parkview, Fox Hills, Cedar St.), PAR feeders (Gowanus→Greenwood, Goethals→Fox Hills), MTA/Amtrak connections, reconductoring (Hudson Ave East), transformer replacements (Millwood West, Rainey, Fresh Kills), and reconfigurations (Buchanan North). They range from 138 kV to 345 kV and have in-service dates from 2025 to 2031. These are either captured in CapEx Substation or CapEx Distribution, or are maintenance/replacement projects that don't add demand-driven capacity and are therefore excluded from the MCOS.

### Boundary accounting

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

For **annual cost centers** (Primary, Transformer, Secondary), capital is the same every year (representative sample), so cumulative = incremental and the distinction only affects cumulative variants (which accumulate the repeating annual sample). Undiluted uses the sample's total capacity (row 151 col O) as the denominator.

For **cumulative cost centers** (Transmission, Substation), incremental capital is the year-over-year delta of the cumulative. Capacity is derived **proportionally** from capital: each year's capacity is `total_project_MW × capital(Y) / capital(final_year)`. This ensures capital and capacity are aligned at every year. (Per-project in-service year detection was considered but rejected because the workbook's $/kW columns reflect construction-in-progress, not actual completion, creating a timing mismatch that produces wild spikes in the incremental undiluted variant.)

See `context/domain/ny_mcos_studies_comparison.md` §6 for the rationale.

### Workbook cell references for each input

**System Peak** — sheet **Coincident Load**, cell **B26** (Area Station Coincident Total, 2025 forecast = 11,997.7 MW). Using col 13 with the ASC total is equivalent to using col 11 with the true system peak (see composite rate note below).

**Composite Rate** — sheet **Carrying Charge Loaders** (note: sheet name has a trailing space), column **O** (Schedule 11 col 13, "Annual MC at System Peak"). This rate already adjusts for area-to-system diversity (coincidence factor = 0.95175).

| Cost center  | Cell | Value   |
| ------------ | ---- | ------- |
| Transmission | O12  | 0.13527 |
| Substation   | O13  | 0.12832 |
| Primary      | O14  | 0.12895 |
| Transformer  | O15  | 0.11271 |
| Secondary    | O16  | 0.12754 |

**Escalation** — sheet **Carrying Charge Loaders**, row **25** (GDP Implicit Price Deflator), columns C–L for years 2025–2034. Base year 2025 = 1.0; 2026 inflates at 2.4%, then 2.1%/yr thereafter.

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

**Cumulative Capital** ($000s) — columns F–O for years 2025–2034. Values grow each year as projects are built. Transmission is sheet **CapEx Transmission** row **22**; Substation is sheet **CapEx Substation** row **35** (both Section 2 grand-total rows). Values are approximate (back-calculated from output CSVs).

| Year | Col | TX: CapEx Transmission row 22 ($000s) | Sub: CapEx Substation row 35 ($000s) |
| ---- | --- | ------------------------------------- | ------------------------------------ |
| 2025 | F   | 635,900                               | 356,200                              |
| 2026 | G   | 970,300                               | 1,250,100                            |
| 2027 | H   | 1,220,400                             | 2,157,000                            |
| 2028 | I   | 1,447,500                             | 3,325,700                            |
| 2029 | J   | 1,447,500                             | 4,249,500                            |
| 2030 | K   | 1,447,500                             | 5,313,500                            |
| 2031 | L   | 1,447,500                             | 6,172,800                            |
| 2032 | M   | 1,447,500                             | 6,881,500                            |
| 2033 | N   | 1,447,500                             | 7,239,600                            |
| 2034 | O   | 1,447,500                             | 7,419,400                            |

**Annual Capital** — sheet **CapEx Distribution**, row **151** (total). Values are in **dollars** (not $000s; the code divides by 1,000 for consistency with the other CapEx sheets). The same capital is used for every year.

| Cost center | Cell | Value ($)   | As $000s |
| ----------- | ---- | ----------- | -------- |
| Primary     | L151 | ~13,100,000 | 13,100   |
| Transformer | M151 | ~5,900,000  | 5,900    |
| Secondary   | N151 | ~16,800,000 | 16,800   |

### Worked example: Substation, year 2025

```
Cumulative Capital = CapEx Substation F35     ≈ 356,230 ($000s)
Composite Rate     = Carrying Charge O13      = 0.12832
Escalation         = Carrying Charge C25      = 1.0
System Peak        = Coincident Load B26      = 11,997.7 MW

Annual RR  = 356,230 × 0.12832 × 1.0 = 45,719 ($000s)
Diluted MC = 45,719 / 11,997.7        = $3.81/kW-yr
```

### Worked example: Primary, year 2025

```
Annual Capital = CapEx Distribution L151 ≈ $13,100,000 → 13,100 ($000s)
Composite Rate = Carrying Charge O14     = 0.12895
Escalation     = Carrying Charge C25     = 1.0
System Peak    = Coincident Load B26     = 11,997.7 MW

Annual RR  = 13,100 × 0.12895 × 1.0 = 1,689 ($000s)
Diluted MC = 1,689 / 11,997.7        = $0.14/kW-yr
```

## Inputs and outputs

| Input          | Source                                                             |
| -------------- | ------------------------------------------------------------------ |
| ConEd workbook | `s3://data.sb/ny_psc/mcos_studies_2025/coned_study_workpaper.xlsx` |
| System peak    | 11,997.7 MW — Coincident Load sheet row 26, 2025 forecast          |

| Output                                       | Description                                                |
| -------------------------------------------- | ---------------------------------------------------------- |
| `coned_cumulative_diluted_levelized.csv`     | One row per cost center: levelized and final-year MC       |
| `coned_cumulative_diluted_annualized.csv`    | One row per (cost center, year): nominal and real MC       |
| `coned_incremental_diluted_levelized.csv`    | Same structure, incremental capital ÷ system peak          |
| `coned_incremental_diluted_annualized.csv`   | Same structure, incremental capital ÷ system peak          |
| `coned_cumulative_undiluted_levelized.csv`   | Same structure, cumulative capital ÷ project capacity      |
| `coned_cumulative_undiluted_annualized.csv`  | Same structure, cumulative capital ÷ project capacity      |
| `coned_incremental_undiluted_levelized.csv`  | Same structure, incremental capital ÷ incremental capacity |
| `coned_incremental_undiluted_annualized.csv` | Same structure, incremental capital ÷ incremental capacity |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-coned
```

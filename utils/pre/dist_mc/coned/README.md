# Con Edison MCOS dilution analysis

ConEd's workbook already separates costs into five NERA cost centers that cleanly map to voltage tiers. No project-level classification is needed — the workbook's cost center structure IS the tier assignment.

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

## Dilution formula

For cumulative cost centers (Transmission, Substation):

```
Annual RR(Y) = Cumulative Capital(Y) × Composite Rate × Escalation(Y)
Diluted MC(Y) = Annual RR(Y) / System Peak
```

For annual cost centers (Primary, Transformer, Secondary):

```
Annual RR(Y) = Annual Capital × Composite Rate × Escalation(Y)
Diluted MC(Y) = Annual RR(Y) / System Peak
```

Where:

- **Cumulative Capital** ($000s) comes from the CapEx sheet Section 2 total row
- **Annual Capital** ($) comes from the CapEx Distribution right-table total
- **Composite Rate** is Schedule 11 col 13 ("Annual MC at System Peak"), which adjusts for area-to-system diversity (coincidence factor = 0.95175)
- **Escalation** is the GDP Implicit Price Deflator (2.4% in 2026, then 2.1%/yr)
- **System Peak** is the 2025 area station coincident total (11,997.7 MW). Using col 13 with the ASC total is equivalent to using col 11 with the true system peak.

## Inputs and outputs

| Input          | Source                                                             |
| -------------- | ------------------------------------------------------------------ |
| ConEd workbook | `s3://data.sb/ny_psc/mcos_studies_2025/coned_study_workpaper.xlsx` |
| System peak    | 11,997.7 MW — Coincident Load sheet row 26, 2025 forecast          |

| Output                         | Description                                                     |
| ------------------------------ | --------------------------------------------------------------- |
| `coned_diluted_levelized.csv`  | One row per cost center: levelized and full-buildout diluted MC |
| `coned_diluted_annualized.csv` | One row per (cost center, year): nominal and real diluted MC    |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-coned
```

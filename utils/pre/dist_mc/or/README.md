# O&R MCOS dilution analysis

O&R (Orange & Rockland, a ConEd subsidiary) uses the same NERA methodology as ConEd. See the [ConEd README](../coned/README.md) for the general approach — this document covers only what differs.

## Differences from ConEd

### Five cost centers (with TX split) instead of ConEd's five

O&R combines Transformer and Secondary into a single "Secondary Distribution" cost center. Additionally, we split CapEx Transmission into bulk TX (excluded) and local TX (included):

| Cost center                      | Workbook sheet                | Type                     | BAT tier                |
| -------------------------------- | ----------------------------- | ------------------------ | ----------------------- |
| Bulk TX (Gold Book, 138 kV)      | CapEx Transmission, row 8     | Cumulative 10-yr capital | Bulk TX (exclude)       |
| Local TX (non-Gold-Book, 138 kV) | CapEx Transmission, rows 9–10 | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Area Station & Sub-TX            | CapEx Substation              | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Primary                          | CapEx Primary                 | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Secondary Distribution           | CapEx Secondary               | Flat $/kW (system-wide)  | Sub-TX + dist (include) |

Note: O&R's Primary is **cumulative** (unlike ConEd's annual sample), with 26 individual feeder projects over 10 years. O&R's Secondary Distribution is derived from just 2 sample projects, yielding a single system-wide $/kW (12.57) with no regional or temporal variation.

### CapEx Transmission split: bulk vs. local

O&R's CapEx Transmission contains three 138 kV projects. Only one (West Nyack) appears in the NYISO Gold Book; the other two (Oak Street, New Hempstead) do not. If we excluded all of CapEx Transmission and assumed the bulk TX analysis would pick up all Gold Book projects, the two non-Gold-Book projects ($36.5M in capital) would fall through the gap — neither analysis would account for them.

We reclassify Oak Street and New Hempstead as **local TX (sub-TX)** and include them in the sub-TX + dist total. Justification:

1. **Not in Gold Book** — NYISO doesn't report them as bulk transmission
2. **138 kV reconductoring** — upgrading existing local lines, not building new backbone capacity
3. **Small scale** — $29M + $7.5M = $36.5M, serving O&R's Eastern NY load area
4. **Avoids accounting gap** — without reclassification, they're in neither analysis

Local TX uses the same composite rate as Transmission (same plant type / carrying charge characteristics). The reclassification changes WHERE these costs land in our tier structure, not the economic parameters applied to them. Impact: ~$1.89/kW-yr levelized addition to the sub-TX + dist total.

### Gold Book cross-reference

#### Projects in CapEx Transmission vs. Gold Book

| Workbook project (row)                                | Voltage | MW    | Est. cost | Gold Book match?                                                                      |
| ----------------------------------------------------- | ------- | ----- | --------- | ------------------------------------------------------------------------------------- |
| West Nyack (8) — new 138 kV UG line, Burns→West Nyack | 138 kV  | 277.4 | $46.1M    | **Yes** — line 3962 (UG cable, 2026), 3963 (transformer, 2026), 3985 (reconfig, 2029) |
| Oak Street (9) — reconductor to standard 138 kV       | 138 kV  | 79.8  | $29.0M    | **No**                                                                                |
| New Hempstead (10) — reconductor to standard 138 kV   | 138 kV  | 89.3  | $7.5M     | **No**                                                                                |

Cumulative CapEx Transmission capital: $36M (2025) → $83M (2031+, flat).

#### Gold Book projects NOT in CapEx Transmission

25 other O&R Gold Book Table VII entries (pp. 160–162) do not appear in CapEx Transmission. These include:

- **Already in-service (2024):** Ramapo→Sterling Forest and Ramapo→NY/NJ State Line (138 kV reconductoring)
- **Sub-TX voltage:** 69 kV line rebuilds (Monroe→Blooming Grove, Shoemaker→Cuddebackville), 34.5→69 kV upgrades (Dean, Shoemaker→Pocatello, Port Jervis→Rio, Washington Heights→Bullville, Decker→Bullville, Pocatello→Decker)
- **Equipment:** Cap banks (West Milford, Forrest Avenue, West Warwick, West Point, Shoemaker, Little Tor, West Milford), breaker replacement (West Milford), station reconfigurations (Shoemaker, Wilson Gate)
- **Future:** Ramapo→Sugarloaf (138 kV, 2036), West Nyack→Harings Corner/RECO (69→138, 2029)

Most of these are at 69 kV or below and are likely captured in CapEx Substation (4 projects) or CapEx Primary (26 feeder projects).

#### Boundary accounting

Under the assumption that bulk TX is handled by a separate analysis using all Gold Book entries:

- **Double-counting:** None. West Nyack (bulk TX) is in both Gold Book and CapEx TX; we exclude it, so it's only counted once (by the bulk TX analysis). Oak St. and New Hempstead are NOT in the Gold Book and are included only in our local TX cost center.
- **Dropped projects:** None — the TX split resolves the gap that would otherwise exist for Oak St. and New Hempstead.
- **Reverse overlap risk:** Same caveat as ConEd — Gold Book entries not in CapEx TX could theoretically also appear in CapEx Substation or Primary, but the MCOS cost centers should be mutually exclusive within the workbook.

### MC variants

Like ConEd, the script produces four MC variants (cumulative/incremental × diluted/undiluted) — see the [ConEd README](../coned/README.md#mc-formula-and-variants) for the general formula.

O&R-specific notes:

- All cumulative cost centers (TX bulk/local, Substation, Primary) derive capacity proportionally from capital — see the [ConEd README](../coned/README.md#mc-formula-and-variants) for rationale.
- **Secondary Distribution** is flat $/kW of system peak. The undiluted variant equals the diluted variant because the $/kW is already normalized by system peak — there's no separate project capacity.

### Secondary Distribution dilution

Because the CapEx Secondary sheet provides capital as $/kW (not total $), the system peak cancels out:

```
Secondary Diluted MC(Y) = Capital($/kW) × Composite Rate × Escalation(Y)
```

Total capital = $/kW × peak, so Annual RR / peak = $/kW × rate × escalation.

### Carrying charge schedule

O&R uses Schedule 10 (not Schedule 11), and the sheet name has **no trailing space** (ConEd's does). Escalation factors are in row 26 (vs. 25 for ConEd).

### System peak

1,078.5 MW — Coincident Forecast sheet row 65, 2024 value (vs. ConEd's 2025 value).

## Workbook cell references

### System Peak

Sheet **Coincident Forecast**, cell **D65** (Grand Total, 2024 forecast) = 1,078.5 MW.

### Composite Rates

Sheet **Carrying Charge Loaders** (no trailing space — unlike ConEd), column **O** (Schedule 10 col 13, "Annual MC at System Peak").

| Cost center    | Cell | Value   |
| -------------- | ---- | ------- |
| Transmission   | O12  | 0.13035 |
| Local TX       | O12  | 0.13035 |
| Substation     | O13  | 0.11850 |
| Primary        | O14  | 0.15394 |
| Secondary Dist | O17  | 0.13725 |

Local TX uses the same Transmission rate (row 12) because it's the same plant type.

### Escalation

Sheet **Carrying Charge Loaders**, row **26** (vs. row 25 for ConEd), columns C–L for years 2025–2034. Same GDP deflator as ConEd: base year 2025 = 1.0; 2026 inflates at 2.4%, then 2.1%/yr.

| Year | Cell | Value  |
| ---- | ---- | ------ |
| 2025 | C26  | 1.0000 |
| 2026 | D26  | 1.0240 |
| 2027 | E26  | 1.0455 |
| 2028 | F26  | 1.0675 |
| 2029 | G26  | 1.0899 |
| 2030 | H26  | 1.1128 |
| 2031 | I26  | 1.1361 |
| 2032 | J26  | 1.1600 |
| 2033 | K26  | 1.1844 |
| 2034 | L26  | 1.2092 |

### Cumulative Capital — Transmission (bulk + local split)

Sheet **CapEx Transmission**, **right-half** cumulative cashflow columns W(2025)–AF(2034), read per-project row. Bulk TX = row 8 (West Nyack). Local TX = sum of rows 9 + 10 (Oak St. + New Hempstead). Values are in $000s. Approximate (back-calculated from output CSVs).

| Year | Col | Bulk TX row 8 ($000s) | Local TX rows 9+10 ($000s) |
| ---- | --- | --------------------- | -------------------------- |
| 2025 | W   | 32,100                | 0                          |
| 2026 | X   | 46,100                | 500                        |
| 2027 | Y   | 46,100                | 1,200                      |
| 2028 | Z   | 46,100                | 2,500                      |
| 2029 | AA  | 46,100                | 7,000                      |
| 2030 | AB  | 46,100                | 11,500                     |
| 2031 | AC  | 46,100                | 24,000                     |
| 2032 | AD  | 46,100                | 36,500                     |
| 2033 | AE  | 46,100                | 36,500                     |
| 2034 | AF  | 46,100                | 36,500                     |

### Cumulative Capital — Substation and Primary

Substation: sheet **CapEx Substation**, row **18** (grand total), columns G–P. Primary: sheet **CapEx Primary**, sum of region rows **57 + 58 + 59** (Central, Eastern, Western; no explicit total row), columns G–P. Both in $000s. Approximate (back-calculated from output CSVs).

| Year | Col | Substation row 18 ($000s) | Primary rows 57-59 ($000s) |
| ---- | --- | ------------------------- | -------------------------- |
| 2025 | G   | 66,300                    | 2,400                      |
| 2026 | H   | 77,700                    | 3,300                      |
| 2027 | I   | 91,800                    | 5,700                      |
| 2028 | J   | 123,900                   | 8,900                      |
| 2029 | K   | 139,900                   | 12,500                     |
| 2030 | L   | 149,900                   | 15,500                     |
| 2031 | M   | 169,800                   | 15,500                     |
| 2032 | N   | 189,900                   | 15,500                     |
| 2033 | O   | 199,900                   | 15,500                     |
| 2034 | P   | 199,900                   | 15,500                     |

### Secondary Distribution — flat $/kW

Sheet **CapEx Secondary**, cell **F18** = 12.5659 ($/kW capital cost, system-wide). This is NOT annual MC — the composite rate must still be applied. The system peak cancels out in the formula (see "Secondary Distribution dilution" above), so diluted MC = $/kW × composite rate × escalation.

### Worked example: Substation, year 2025

```
Cumulative Capital = CapEx Substation G18       ≈ 66,300 ($000s)
Composite Rate     = Carrying Charge Loaders O13 = 0.11850
Escalation         = Carrying Charge Loaders C26 = 1.0
System Peak        = Coincident Forecast D65     = 1,078.5 MW

Annual RR  = 66,300 × 0.11850 × 1.0 = 7,857 ($000s)
Diluted MC = 7,857 / 1,078.5         = $7.28/kW-yr
```

### Worked example: Secondary Distribution, year 2026

```
Capital ($/kW)  = CapEx Secondary F18            = 12.5659
Composite Rate  = Carrying Charge Loaders O17    = 0.13725
Escalation      = Carrying Charge Loaders D26    = 1.0240

Diluted MC = 12.5659 × 0.13725 × 1.0240 = $1.77/kW-yr
```

## Inputs and outputs

| Input        | Source                                                          |
| ------------ | --------------------------------------------------------------- |
| O&R workbook | `s3://data.sb/ny_psc/mcos_studies_2025/or_study_workpaper.xlsx` |
| System peak  | 1,078.5 MW — Coincident Forecast sheet row 65, 2024 value       |

| Output                                    | Description                                                |
| ----------------------------------------- | ---------------------------------------------------------- |
| `or_cumulative_diluted_levelized.csv`     | One row per cost center: levelized and final-year MC       |
| `or_cumulative_diluted_annualized.csv`    | One row per (cost center, year): nominal and real MC       |
| `or_incremental_diluted_levelized.csv`    | Same structure, incremental capital ÷ system peak          |
| `or_incremental_diluted_annualized.csv`   | Same structure, incremental capital ÷ system peak          |
| `or_cumulative_undiluted_levelized.csv`   | Same structure, cumulative capital ÷ project capacity      |
| `or_cumulative_undiluted_annualized.csv`  | Same structure, cumulative capital ÷ project capacity      |
| `or_incremental_undiluted_levelized.csv`  | Same structure, incremental capital ÷ incremental capacity |
| `or_incremental_undiluted_annualized.csv` | Same structure, incremental capital ÷ incremental capacity |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-or
```

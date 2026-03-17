# O&R MCOS marginal cost analysis

O&R (Orange & Rockland, a ConEd subsidiary) uses the same NERA methodology as ConEd. See the [ConEd README](../coned/README.md) for the general approach — this document covers only what differs.

## Cost centers

O&R combines Transformer and Secondary into a single "Secondary Distribution" cost center. Additionally, we split CapEx Transmission into bulk TX (excluded) and local TX (included):

| Cost center                      | Workbook sheet                | Type                     | BAT tier                |
| -------------------------------- | ----------------------------- | ------------------------ | ----------------------- |
| Bulk TX (Gold Book, 138 kV)      | CapEx Transmission, row 8     | Cumulative 10-yr capital | Bulk TX (exclude)       |
| Local TX (non-Gold-Book, 138 kV) | CapEx Transmission, rows 9–10 | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Area Station & Sub-TX            | CapEx Substation              | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Primary                          | CapEx Primary                 | Cumulative 10-yr capital | Sub-TX + dist (include) |
| Secondary Distribution           | CapEx Secondary               | Flat $/kW (system-wide)  | Sub-TX + dist (include) |

Note: O&R's Primary is **cumulative** (unlike ConEd's annual sample), with 26 individual feeder projects over 10 years. O&R's Secondary Distribution is derived from just 2 sample projects, yielding a single system-wide $/kW (12.57) with no regional or temporal variation.

## Bulk TX treatment

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

## MC formula and variants

Base formula (all variants):

```
Annual RR(Y) = Capital(Y) × Composite Rate × Escalation(Y)
MC(Y)        = Annual RR(Y) / Denominator   [$/kW-yr]
```

The script produces **four variants** by combining two capital perspectives with two denominators:

| Variant               | Capital(Y)                          | Denominator                  |
| --------------------- | ----------------------------------- | ---------------------------- |
| Cumulative diluted    | Accumulated in-service capital to Y | System peak (MW)             |
| Incremental diluted   | New capital entering service in Y   | System peak (MW)             |
| Cumulative undiluted  | Accumulated in-service capital to Y | Cumulative project MW to Y   |
| Incremental undiluted | New capital entering service in Y   | New project MW entering in Y |

See the [ConEd README](../coned/README.md#formulas-for-each-variant) for the full formula derivation.

**Levelized** = mean of real MC over the 7-year levelization window (2026–2032), chosen to capture near-term planned investment while excluding speculative back-loaded projects. See `context/methods/marginal_costs/ny_mcos_studies_comparison.md` §10 for rationale.

### O&R-specific notes

- All cumulative cost centers (TX bulk/local, Substation, Primary) now use **in-service-year scoping**: each project's capital and MW enter the calculation when the project completes (cashflow stabilizes for TX/Sub; first nonzero budget year for Primary). This matches the NiMo/CenHud project-level methodology. Undiluted MC varies by year, reflecting the actual $/kW of each project cohort.
- **Secondary Distribution** is flat $/kW of system peak. The undiluted variant equals the diluted variant because the $/kW is already normalized by system peak — there's no separate project capacity.

### Secondary Distribution dilution

Because the CapEx Secondary sheet provides capital as $/kW (not total $), the system peak cancels out:

```
Secondary Diluted MC(Y) = Capital($/kW) × Composite Rate × Escalation(Y)
```

Total capital = $/kW × peak, so Annual RR / peak = $/kW × rate × escalation.

## Study parameters

| Parameter    | Value        | Source                                      |
| ------------ | ------------ | ------------------------------------------- |
| Study period | 2025–2034    | All CapEx sheets                            |
| System peak  | 1,078.5 MW   | Coincident Forecast D65 (Grand Total, 2024) |
| Escalation   | GDP deflator | Carrying Charge Loaders row 26, 2.4%→2.1%   |

### Composite rates

Sheet **Carrying Charge Loaders** (no trailing space — unlike ConEd), column **O** (Schedule 10 col 13, "Annual MC at System Peak").

| Cost center    | Cell | Value   |
| -------------- | ---- | ------- |
| Transmission   | O12  | 0.13035 |
| Local TX       | O12  | 0.13035 |
| Substation     | O13  | 0.11850 |
| Primary        | O14  | 0.15394 |
| Secondary Dist | O17  | 0.13725 |

Local TX uses the same Transmission rate (row 12) because it's the same plant type. O&R uses Schedule 10 (not Schedule 11).

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

## Per-project data

Parsed from each cumulative cost center's right-half columns. In-service year is inferred from cashflow stabilization (TX, Substation) or first nonzero annual budget (Primary). These tables are the primary audit artifact for the project-level methodology.

### CapEx Transmission — 3 projects

Rows 8–10. Row 8 is bulk TX (West Nyack, Gold Book match); rows 9–10 are local TX (non-Gold-Book reconductoring).

| Row | Classification | Name                        |    MW | Capital ($000s) | In-service |
| --: | -------------- | --------------------------- | ----: | --------------: | ---------: |
|   8 | Bulk TX        | West Nyack (Burns→WN UG)    | 277.4 |          46,100 |       2026 |
|   9 | Local TX       | Oak Street (reconductor)    |  79.8 |          29,000 |       2032 |
|  10 | Local TX       | New Hempstead (reconductor) |  89.3 |           7,500 |       2030 |

### CapEx Substation — 4 projects

Rows 8–11. Row 12 is the total/summary row (excluded from project parsing).

| Row | Station     | Description                             |        MW | Capital ($000s) | In-service |
| --: | ----------- | --------------------------------------- | --------: | --------------: | ---------: |
|   8 | Little Tor  | New 2-bank station, 56 MVA transformers |     106.4 |          14,410 |       2029 |
|   9 | New Goshen  | New 3-bank station, 56 MVA transformers |     159.6 |         124,000 |       2033 |
|  10 | Wilson Gate | Upgrade 20+35 MVA → 2×56+35 MVA         |      87.4 |          43,450 |       2029 |
|  11 | Sloatsburg  | Upgrade 1-25 bank → 2×56 MVA            |      82.6 |          18,000 |       2028 |
|     |             | **Total**                               | **436.0** |     **199,860** |            |

### CapEx Primary — 26 projects

Unlike TX/Sub, Primary uses **annual budgets** (not cumulative cashflow): each project has a constant yearly budget starting in its in-service year. In-service year = first nonzero column in X–AG.

| Row | Region/Location     |       MW | Budget ($000s) | In-service |
| --: | ------------------- | -------: | -------------: | ---------: |
|   8 | Cen/Blooming Grove  |      2.1 |            259 |       2025 |
|   9 | Wes/Wurstboro       |      0.5 |            700 |       2025 |
|  10 | Wes/Wurstboro       |      0.7 |             99 |       2025 |
|  11 | Wes/Wurstboro       |      0.7 |            350 |       2025 |
|  12 | Wes/Wurstboro       |      0.5 |            850 |       2025 |
|  13 | Wes/Port Jervis     |      2.1 |            150 |       2025 |
|  14 | Cen/Dean            |      2.8 |            450 |       2026 |
|  15 | Eas/Stony Point     |      2.8 |            400 |       2026 |
|  16 | Cen/Sterling Forest |      0.7 |            158 |       2027 |
|  17 | Cen/Blooming Grove  |      3.7 |            800 |       2027 |
|  18 | Cen/Sterling Forest |      1.4 |            100 |       2027 |
|  19 | Eas/Tallman         |      3.5 |            650 |       2027 |
|  20 | Wes/Otisville       |      2.1 |            800 |       2027 |
|  21 | Cen/Blooming Grove  |      2.5 |            475 |       2028 |
|  22 | Cen/Sterling Forest |      3.7 |            850 |       2028 |
|  23 | Eas/Corporate Drive |      2.1 |            750 |       2028 |
|  24 | Wes/Summitville     |      2.1 |            550 |       2028 |
|  25 | Cen/Blooming Grove  |      3.7 |            475 |       2028 |
|  26 | Cen/Sterling Forest |      3.7 |            700 |       2029 |
|  27 | Wes/Bullville       |      1.1 |            800 |       2029 |
|  28 | Wes/Mongaup         |      1.1 |            880 |       2029 |
|  29 | Wes/Mongaup         |      1.1 |          1,300 |       2029 |
|  30 | Eas/Stony Point     |      2.8 |            400 |       2030 |
|  31 | Wes/Bullville       |      2.8 |          1,700 |       2030 |
|  32 | Wes/East Wallkill   |      2.8 |            400 |       2030 |
|  33 | Wes/Bullville       |      1.1 |            450 |       2030 |
|     |                     | **54.2** |     **15,496** |            |

### Cumulative capital summary — Transmission (bulk + local split)

Derived from the per-project tables above. Bulk TX = row 8 cashflow. Local TX = sum of rows 9 + 10 cashflows. In-service-year scoping: West Nyack completes 2026 (bulk), New Hempstead 2030 (local), Oak Street 2032 (local). Values in $000s.

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

### Cumulative capital summary — Substation and Primary (in-service-year scoping)

Each project's full capital and MW enter in its in-service year. Substation: cashflow stabilization. Primary: sum of annual budgets for in-service projects.

| Year | Sub Capital ($000s) | Sub Capacity (MW) | Primary Capital ($000s) | Primary Capacity (MW) |
| ---- | ------------------- | ----------------- | ----------------------- | --------------------- |
| 2025 | 0                   | 0.0               | 2,408                   | 6.7                   |
| 2026 | 0                   | 0.0               | 3,258                   | 12.3                  |
| 2027 | 0                   | 0.0               | 5,758                   | 23.7                  |
| 2028 | 18,000              | 82.6              | 8,858                   | 37.8                  |
| 2029 | 75,860              | 276.5             | 12,538                  | 44.7                  |
| 2030 | 75,860              | 276.5             | 15,488                  | 54.2                  |
| 2031 | 75,860              | 276.5             | 15,488                  | 54.2                  |
| 2032 | 75,860              | 276.5             | 15,488                  | 54.2                  |
| 2033 | 199,860             | 436.0             | 15,488                  | 54.2                  |
| 2034 | 199,860             | 436.0             | 15,488                  | 54.2                  |

### Secondary Distribution — flat $/kW

Sheet **CapEx Secondary**, cell **F18** = 12.5659 ($/kW capital cost, system-wide). This is NOT annual MC — the composite rate must still be applied. The system peak cancels out in the formula (see "Secondary Distribution dilution" above), so diluted MC = $/kW × composite rate × escalation.

## Worked examples

### Cumulative diluted — Substation, year 2029

In-service by 2029: Sloatsburg (2028, $18,000k), Little Tor (2029, $14,410k), New Goshen (2029, $43,450k).

```
Capital(2029) = 18,000 + 14,410 + 43,450 = 75,860 ($000s)
Composite Rate = Carrying Charge Loaders O13 = 0.11850
Escalation     = Carrying Charge Loaders G26 = 1.0899
System Peak    = Coincident Forecast D65     = 1,078.5 MW

Nominal RR = 75,860 × 0.11850 × 1.0899 = 9,797 ($000s)
Diluted MC = 9,797 / 1,078.5            = $9.08/kW-yr (nominal)
Real MC    = 75,860 × 0.11850 / 1,078.5 = $8.33/kW-yr
```

### Cumulative diluted — Secondary Distribution, year 2026

```
Capital ($/kW)  = CapEx Secondary F18            = 12.5659
Composite Rate  = Carrying Charge Loaders O17    = 0.13725
Escalation      = Carrying Charge Loaders D26    = 1.0240

Diluted MC = 12.5659 × 0.13725 × 1.0240 = $1.77/kW-yr
```

For Secondary Distribution, incremental diluted is identical (flat capital every year), and undiluted = diluted ($/kW is already per kW of system peak — no separate project capacity).

### Incremental diluted — Substation, year 2029

Two projects enter service in 2029: Little Tor ($14,410k) and New Goshen ($43,450k).

```
Inc Capital(2029)  = 14,410 + 43,450 = 57,860 ($000s)
Composite Rate     = 0.11850
Escalation(2029)   = 1.0899
System Peak        = 1,078.5 MW

Annual RR  = 57,860 × 0.11850 × 1.0899 = 7,474 ($000s)
Diluted MC = 7,474 / 1,078.5            = $6.93/kW-yr (nominal)
Real MC    = 57,860 × 0.11850 / 1,078.5 = $6.36/kW-yr
```

### Cumulative undiluted — Substation, year 2029

Same 3 in-service projects as the diluted example:

```
Capital(2029)  = 75,860 ($000s)
Capacity(2029) = 82.6 + 106.4 + 87.4 = 276.5 MW  [actual project MW]
Composite Rate = 0.11850
Escalation     = 1.0899

Nominal RR   = 75,860 × 0.11850 × 1.0899 = 9,797 ($000s)
Undiluted MC = 9,797 / 276.5              = $35.43/kW-yr (nominal)
Real MC      = 75,860 × 0.11850 / 276.5  = $32.51/kW-yr
```

Real MC varies by year because each project cohort has a different $/kW. Compare 2034 (all 4 projects, 436 MW): real MC = 199,860 × 0.11850 / 436 = $54.31/kW-yr.

### Incremental undiluted — Substation, year 2029

Two projects enter service in 2029:

```
Inc Capital(2029) = 14,410 + 43,450 = 57,860 ($000s)
Inc Capacity(2029) = 106.4 + 87.4   = 193.8 MW
Composite Rate     = 0.11850
Escalation(2029)   = 1.0899

Annual RR      = 57,860 × 0.11850 × 1.0899 = 7,474 ($000s)
Undiluted MC   = 7,474 / 193.8              = $38.57/kW-yr (nominal)
Real MC        = 57,860 × 0.11850 / 193.8  = $35.39/kW-yr
```

Non-zero only in years when projects complete. Compare with Sloatsburg alone in 2028: $18,000 × 0.11850 / 82.6 = $25.82/kW-yr — different $/kW per cohort.

## Inputs and outputs

| Input        | Source                                                          |
| ------------ | --------------------------------------------------------------- |
| O&R workbook | `s3://data.sb/ny_psc/mcos_studies_2025/or_study_workpaper.xlsx` |
| System peak  | 1,078.5 MW — Coincident Forecast sheet row 65, 2024 value       |

| Output                                            | Description                                                      |
| ------------------------------------------------- | ---------------------------------------------------------------- |
| `outputs/or_cumulative_diluted_levelized.csv`     | Two rows (bulk_tx, sub_tx_and_dist): levelized and final-year MC |
| `outputs/or_cumulative_diluted_annualized.csv`    | One row per year: bulk_tx and sub_tx_and_dist (nominal/real)     |
| `outputs/or_incremental_diluted_levelized.csv`    | Same structure, incremental capital ÷ system peak                |
| `outputs/or_incremental_diluted_annualized.csv`   | Same structure, incremental capital ÷ system peak                |
| `outputs/or_cumulative_undiluted_levelized.csv`   | Same structure, cumulative capital ÷ project capacity            |
| `outputs/or_cumulative_undiluted_annualized.csv`  | Same structure, cumulative capital ÷ project capacity            |
| `outputs/or_incremental_undiluted_levelized.csv`  | Same structure, incremental capital ÷ incremental capacity       |
| `outputs/or_incremental_undiluted_annualized.csv` | Same structure, incremental capital ÷ incremental capacity       |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-or
```

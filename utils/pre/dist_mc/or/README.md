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

## Inputs and outputs

| Input        | Source                                                          |
| ------------ | --------------------------------------------------------------- |
| O&R workbook | `s3://data.sb/ny_psc/mcos_studies_2025/or_study_workpaper.xlsx` |
| System peak  | 1,078.5 MW — Coincident Forecast sheet row 65, 2024 value       |

| Output                      | Description                                                     |
| --------------------------- | --------------------------------------------------------------- |
| `or_diluted_levelized.csv`  | One row per cost center: levelized and full-buildout diluted MC |
| `or_diluted_annualized.csv` | One row per (cost center, year): nominal and real diluted MC    |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-or
```

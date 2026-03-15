# NiMo MCOS marginal cost analysis

NiMo (National Grid Upstate) required the most work of any utility. The workbook lists ~237 planned capital projects but does not distinguish **bulk transmission** (≥230 kV) from **sub-transmission** (69–115 kV), so we had to classify every project by voltage tier before computing marginal costs.

## Cost centers

NiMo's workbook uses four cost centres (T\_Sta, T\_Line, D\_Sta, D\_Line) which we map to three voltage tiers:

- **bulk_tx** (≥230 kV) — NYISO bulk transmission; excluded from local marginal cost
- **sub_tx** (69–115 kV) — NiMo's sub-transmission network
- **distribution** (≤13.2 kV) — distribution substations and feeders

## Bulk TX treatment

The workbook does not label projects by voltage. Classification requires cross-referencing project names and sub-project detail against the NYISO Gold Book. See [How to reproduce the classification](#how-to-reproduce-the-classification) for the full process.

### Inputs for classification

| Input                     | Where it lives                                                    | What it contains                                                                                                                                                                  |
| ------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| NiMo MCOS workbook        | `s3://data.sb/ny_psc/mcos_studies_2025/nimo_study_workpaper.xlsx` | Sheet 1 (Exhibit 1): one row per project with station name, MW, and capital by cost centre. Sheet 11 (FinalData): sub-project–level detail with descriptive names and InvestType. |
| NYISO Gold Book Table VII | `context/sources/nyiso_gold_book_2025.md` (extracted from PDF)    | Every proposed transmission project in New York, with station names, voltages, and descriptions.                                                                                  |

## MC formula and variants

Base formula (all variants):

```
Real MC(Y)    = Σ[ F26(p) × capacity(p) ] / Denominator   [$/kW-yr]
Nominal MC(Y) = Σ[ F_Y(p) × capacity(p) ] / Denominator   [$/kW-yr]
```

NiMo's workbook differs from ConEd/O&R: instead of a composite rate × escalation formula, each project has pre-computed **ECCR** values with year-by-year F columns. **F26(p)** is the base-year (FY2026) annual cost/MW ($000s/MW, col AN), and **F\_Y(p)** is the nominal annual cost/MW in year Y ($000s/MW, cols AN–AX).

The script produces **four variants** by combining two capital perspectives with two denominators:

| Variant               | Capital(Y)                          | Denominator                  |
| --------------------- | ----------------------------------- | ---------------------------- |
| Cumulative diluted    | Accumulated in-service capital to Y | System peak (MW)             |
| Incremental diluted   | New capital entering service in Y   | System peak (MW)             |
| Cumulative undiluted  | Accumulated in-service capital to Y | Cumulative project MW to Y   |
| Incremental undiluted | New capital entering service in Y   | New project MW entering in Y |

Levelized = mean(real MC) over the 7-year levelization window (2026–2032), consistent across all utilities. See `context/domain/ny_mcos_studies_comparison.md` §10 for rationale.

Unlike ConEd/O&R, NiMo has per-project in-service years, so incremental values are computed directly (filter to projects entering in year Y) rather than differencing cumulative totals.

### Escalation

NiMo uses the Blue Chip GDP Implicit Price Deflator at **2.1%/yr**. This is already baked into the F columns — F27 = F26 × 1.021, F28 = F26 × 1.021², etc. The script does not apply escalation separately; it reads the pre-computed F\_Y values directly from the workbook.

Verification from the summary row (line 245): F27/F26 = 73.026/71.524 = 1.02100 ✓

### ECCR vs. F-value relationship

The E column (AI, idx 34) contains the ECCR annualized cost per MW at the project's **in-service year** prices. The F columns adjust this to each year's price level using the 2.1%/yr deflator:

```
F_Y(p) = E(p) × (1.021)^(Y − in_service_year)
F26(p) = E(p) × (1.021)^(2026 − in_service_year)
```

For example, Sawyer Ave (in-service 2029): E = $39.44, F26 = $39.44 × (1.021)^(−3) = $37.05.

The F columns always contain values regardless of in-service year. The script uses the in-service year filter to determine which projects contribute in a given year — F values for non-in-service projects are ignored.

## Study parameters

| Parameter           | Value         | Source                                                        |
| ------------------- | ------------- | ------------------------------------------------------------- |
| Study period        | FY2026–FY2036 | Exhibit 1 columns AN–AX                                       |
| System peak         | 6,616 MW      | 2024 actual peak, NiMo Peak Load Forecast (Mar 2025)          |
| Escalation          | 2.1%/yr       | Blue Chip GDP deflator, baked into F columns                  |
| Undiluted MC target | $71.524/kW-yr | Exhibit 1 line 245 col AN — capacity-weighted average all 237 |

The system peak is passed as a CLI argument (`--system-peak-mw 6616`), not read from the workbook. The undiluted MC target is passed via `--undiluted-mc-per-kw 71.524` for cross-checking.

### Key workbook columns (Exhibit 1, Sheet 1)

| Excel col | 0-idx | Code    | Header                        | Content                                       |
| --------- | ----: | ------- | ----------------------------- | --------------------------------------------- |
| A         |     0 | —       | Line                          | Row number (1–245)                            |
| B         |     1 | (A)     | Reference                     | FN reference (e.g. `FN010097`)                |
| C         |     2 | (A1)    | Station                       | Station name or `Transm Net`                  |
| D         |     3 | (B)     | Capacity Added MW             | Project capacity in MW                        |
| E–P       |  4–15 | (C26X)… | FY26X–FY36 Capex              | Yearly capital expenditures ($000s)           |
| Q         |    16 | (C-Sum) | Sum                           | Total capital spending ($000s)                |
| S–V       | 18–21 | (C-TS)… | T\_Sta/T\_Line/D\_Sta/D\_Line | Capital by cost centre ($000s)                |
| W         |    22 | (C)     | Sum                           | Capital cost centre total ($000s)             |
| Y–AB      | 24–27 | (D-TS)… | T\_Sta/T\_Line/D\_Sta/D\_Line | D columns (cost per MW $000s/MW)              |
| AC        |    28 | (D)     | Sum                           | D total (cost per MW $000s/MW)                |
| AD        |    29 | (F)     | In-Service Year               | Fiscal year project enters service            |
| AE–AH     | 30–33 | (E-TS)… | T\_Sta/T\_Line/D\_Sta/D\_Line | ECCR per MW by cost centre ($000s/MW)         |
| AI        |    34 | (E)     | Sum                           | ECCR total per MW ($000s/MW)                  |
| AK        |    36 | (F)     | In-Service Year               | (duplicate column)                            |
| AL        |    37 | (F-1)   | Rationale                     | Project rationale text                        |
| AN        |    39 | (F26)   | 2026                          | Annual MC per MW in FY2026 dollars ($000s/MW) |
| AO        |    40 | (F27)   | 2027                          | Annual MC per MW in FY2027 dollars ($000s/MW) |
| …         |     … | …       | …                             | …                                             |
| AX        |    49 | (F36)   | 2036                          | Annual MC per MW in FY2036 dollars ($000s/MW) |

The script reads four things per project:

1. **Capacity** (col D, idx 3): MW of new capacity added
2. **In-service year** (col AD, idx 29): fiscal year project enters service
3. **F26** (col AN, idx 39): base-year (FY2026) annual cost per MW ($000s/MW)
4. **F\_Y** (cols AN–AX, idx 39–49): nominal annual cost per MW in each fiscal year

The classification (bulk\_tx / sub\_tx / distribution) comes from the external CSV, not the workbook.

### Workbook layout (Exhibit 1, Sheet 1)

| Rows    | Content                                                |
| ------- | ------------------------------------------------------ |
| 1–4     | Title and header (row 5 in Excel = row 4 zero-indexed) |
| 5       | Column codes (`(A)`, `(B)`, `(C26)`, `(F26)`, etc.)    |
| 6       | Blank separator                                        |
| 7+      | One row per project (data starts at row 8 in Excel)    |
| 245     | Summary / total row (line 245, capacity = 11,532.5 MW) |
| 246–263 | Additional rows (empty or subtotals)                   |

The script reads 237 valid project rows (non-null line number and station name, excluding "xxx" placeholders).

## Per-project data

NiMo has 237 projects — too many to list individually. The classification CSV (`nimo_project_classifications.csv`) contains the full project-level detail. See [How to reproduce the classification](#how-to-reproduce-the-classification) for the methodology.

## Worked examples

### Cumulative diluted — sub\_tx\_plus\_dist, FY2026

In FY2026, projects with in\_service\_year ≤ 2026 contribute. Using Sawyer Ave as one example among many:

- **Sawyer Ave** (FN010097): distribution, 12 MW, in-service FY2029, F26 = $37.054/MW

Sawyer Ave does NOT contribute in FY2026 (in-service 2029 > 2026). Only projects with in\_service\_year ≤ 2026 are in scope. The FY2026 cumulative diluted sub\_tx\_plus\_dist total across all qualifying projects:

```
Real MC(2026) = sum[ F26(p) × cap(p) ] / system_peak
              = (all sub_tx + dist projects with in_service ≤ 2026) / 6,616 MW
              = $3.60/kW-yr
```

As more projects enter service each year, this grows — by FY2036 it reaches $137.37/kW-yr (real).

### Incremental diluted — Sawyer Ave contribution, FY2029

Sawyer Ave (line 3, FN010097) is a distribution substation project:

- Station: Sawyer Ave (distribution, ≤13.2 kV)
- Capacity: 12 MW (col D)
- In-service: FY2029 (col AD)
- F26: $37.054/MW ($000s/MW, col AN)
- F2029: F26 × (1.021)³ = $37.054 × 1.0642 = $39.42/MW (col AQ)

Only projects with in\_service\_year = 2029 contribute. Sawyer Ave's contribution:

```
Real contribution     = F26 × capacity / system_peak
                      = 37.054 × 12 / 6,616 = $0.0672/kW-yr

Nominal contribution  = F2029 × capacity / system_peak
                      = 39.42 × 12 / 6,616  = $0.0715/kW-yr
```

This is one of many projects entering in FY2029 — the total incremental diluted distribution MC for FY2029 is $1.26/kW-yr (real), reflecting all projects with in-service = 2029.

### Cumulative undiluted — headline validation, FY2036

The workbook's summary row (line 245) reports an undiluted MC of $71.524/kW-yr. This is the capacity-weighted average F26 across all 237 projects:

```
Undiluted MC = sum[ F26(p) × cap(p) ] / sum[ cap(p) ]
             = sum across 237 projects / 11,532.5 MW
             = $71.524/kW-yr
```

By FY2036, all projects are in-service (in\_service\_year ≤ 2036 for all), so cumulative undiluted total should equal this headline value. In earlier years, only a subset contributes — FY2026 cumulative undiluted total is $167.86/kW-yr (higher than the headline because only 142 MW of capacity is in scope, and those early projects have above-average cost/MW).

Unlike ConEd/O&R, the undiluted MC is NOT constant across years — it changes as the mix of in-service projects shifts. Each project has its own capacity and cost; the capacity-weighted average depends on which projects are in scope.

### Incremental undiluted — Sawyer Ave contribution, FY2029

Same project, but now the denominator is the new capacity added in FY2029 (not system peak):

```
Total new capacity in FY2029 = sum[ cap(p) ] for in_service_year = 2029
                              = 242 MW (across all buckets)

Real MC(2029)  = sum[ F26(p) × cap(p) ] / sum[ cap(p) ]
               = capacity-weighted average F26 for FY2029 projects only
```

Sawyer Ave's contribution to the numerator is 37.054 × 12 = $444.6 ($000s). Its 12 MW enters the denominator. But the total is dominated by larger projects entering that year — the incremental undiluted sub\_tx\_plus\_dist real MC for FY2029 is $201.02/kW-yr, reflecting the cost profile of all projects entering that year.

Incremental undiluted can be volatile year to year because a single expensive project with little capacity drives the weighted average up.

### FY2036 spike in incremental diluted

FY2036 shows a dramatic spike ($60.45/kW-yr real for sub\_tx\_plus\_dist) because it's the final study year and many large projects have in-service = 2036:

- FN008276 (Transm Net): 1,650 MW, F26 = $131.64/MW — Smart Path Connect (bulk TX)
- FN013571 (Transm Net): 1,100 MW, F26 = $8.85/MW — Niagara-Dysinger (bulk TX)
- FN013182 (Transm Net): 844 MW, F26 = $9.27/MW
- Plus 15+ additional projects totaling ~2,700 MW

This is an artifact of the study horizon — projects without firm schedules are assigned to the last year. The 7-year levelization window (2026–2032) excludes this spike entirely.

## How to reproduce the classification

If you need to redo this for NiMo or adapt it for another utility's MCOS study, here's the process:

### Step 1: Separate distribution from transmission

Open Exhibit 1 (Sheet 1). Every project has a "station" column. If it lists a specific substation name (e.g. "NEWTONVILLE", "Sawyer Ave"), that project is a **distribution** area study — the station name identifies a distribution substation. Classify it as distribution even if some capital is booked to T-side cost centres (this is common when distribution area studies require upgrades at the feeding sub-TX substation).

Projects listed as "Transm Net" are transmission — they need voltage determination. Everything below applies only to these.

### Step 2: Look up sub-project detail

For each Transm Net project, find its FN reference number in FinalData (Sheet 11). This gives you sub-project names and InvestTypes (T_Station, T_Line, D_Station, D_Line). The sub-project names are where the useful information is — they often contain station names, line numbers, or explicit voltages that Exhibit 1 doesn't have.

### Step 3: Extract explicit voltages from sub-project names

Some sub-project names state the voltage directly: "Meco 115kV Rebuild", "Eastover 230kV 70 MVAR cap bank", "MVT Rott 69kV Rebuild". These are the easiest cases — the voltage tells you the tier.

### Step 4: Cross-reference station names against the Gold Book

For sub-projects without explicit voltages, extract the station/corridor names from the sub-project name and look them up in Gold Book Table VII. The Gold Book lists every proposed NGRID transmission project with its voltage. For example, if a sub-project mentions "Gardenville" and "Dunkirk", Gold Book Table VII shows "NGRID Gardenville Dunkirk 115 kV" — so it's sub-TX.

Do this for **every** Transm Net sub-project, not just the ones that look ambiguous. The systematic pass is how you catch things the targeted approach misses.

**Watch out for:**

- **Abbreviated names.** The workbook uses short forms like "Gard-Dun" for "Gardenville-Dunkirk", "LHH" for "Lighthouse Hill", "BF" for "Browns Falls". You need to recognize these to match Gold Book entries.
- **Stations at multiple voltage levels.** Some stations have equipment at both 345 kV and 115 kV (Rotterdam, Oswego, Clay, Elm St). A sub-project mentioning "Rotterdam" could be 230 kV or 69 kV work. Don't assume bulk just because the station name appears in a high-voltage Gold Book entry — check whether the sub-project name has context pointing to a specific voltage level.
- **False substring matches.** "South Oswego" (115 kV) is not the same station as "Oswego" (345 kV). "Porter" appears in both 230 kV SPCP retirements and 115 kV Boonville-Porter rebuilds. Always check the full corridor, not just one station name.

### Step 5: Identify well-known projects

Some projects are identifiable by name regardless of Gold Book matching. Smart Path Connect (SPCP) and Niagara-Dysinger are major NYISO bulk transmission projects. If a sub-project name contains "Smart Path", "SPCP", or "Niagara-Dysinger", it's bulk.

### Step 6: Use NiMo-specific conventions for remaining gaps

For projects that don't have explicit voltages and don't match Gold Book entries, use domain knowledge:

- **Line numbering:** NiMo's 100-series line numbers (e.g. line 181, 130, 141) are 115 kV. 200-series are 69 kV. You can confirm this by checking known lines against the Gold Book.
- **EV Ready Sites:** Small T_Line projects named "EV RS - [location]" are taps off existing 115 kV lines.
- **Numbered stations (STA XX):** Projects with "STA 74", "STA 129", etc. are sub-TX tap + conversion projects.
- **Station type analogy:** If a station rebuild isn't in the Gold Book, compare it to similar stations that are. Every NiMo greenfield or rebuilt transmission station in the Gold Book is at 115 kV — there are no examples of NiMo building new ≥230 kV stations on its own (the only 345 kV work is NYISO-level projects like SPCP).

### Step 7: Handle mixed-voltage projects

Some FN parents contain sub-projects at different voltage levels. For example, FN011642 includes a new 345/115 kV station (Marshville/Ames Road, confirmed in Gold Book) alongside 115 kV line rebuilds and 69 kV substation work. For these, figure out how much capital is at each voltage level and classify by majority. Document the split.

### Step 8: Record everything

For each project, write down:

- The classification (bulk_tx / sub_tx / distribution)
- The voltage(s) identified
- How you identified it (explicit kV in name, Gold Book match, line numbering convention, analogy, etc.)
- The specific evidence (which sub-project, which Gold Book entry, what the match was)
- Your confidence level

This is what `nimo_project_classifications.csv` captures. The evidence column is the most important — it's what lets someone else audit your work.

## Inputs and outputs

| Input                     | Source                                                            |
| ------------------------- | ----------------------------------------------------------------- |
| NiMo MCOS workbook        | `s3://data.sb/ny_psc/mcos_studies_2025/nimo_study_workpaper.xlsx` |
| NYISO Gold Book Table VII | `context/sources/nyiso_gold_book_2025.md`                         |
| System peak               | 6,616 MW — 2024 actual, NiMo Peak Load Forecast (Mar 2025)        |

| Output                                                                     | Description                                                                                                         |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `nimo_project_classifications.csv`                                         | One row per project: classification, voltage, inference method, evidence text, sub-project names, confidence level. |
| `classify_nimo_projects.py`                                                | The script that produces the CSV. All manual decisions are encoded as commented data structures at the top.         |
| `outputs/nimo_{cumulative,incremental}_{diluted,undiluted}_levelized.csv`  | Two rows (bulk_tx, sub_tx_and_dist): levelized MC ($/kW-yr), final-year real and nominal MC.                        |
| `outputs/nimo_{cumulative,incremental}_{diluted,undiluted}_annualized.csv` | One row per year: bulk_tx and sub_tx_and_dist nominal and real MC ($/kW-yr).                                        |

## How to run

```bash
cd utils/pre/dist_mc
just analyze-nimo
```

To regenerate the classification CSV:

```bash
cd utils/pre/dist_mc/nimo
uv run python classify_nimo_projects.py
```

This reads the workbook from S3, applies all the classification logic, and overwrites `nimo_project_classifications.csv`. The Gold Book reference data is hardcoded in the script — if the Gold Book changes, update the `GOLD_BOOK_NGRID_HIGH_VOLTAGE` and `GOLD_BOOK_NGRID_SUB_TX` lists.

### Downstream consumers

The classifications feed into `analyze_nimo_mcos.py`, which uses them to compute marginal costs by tier (bulk TX, sub-TX, distribution, sub-TX + distribution) in all four variants. The **incremental diluted** values are the BAT rate design inputs.

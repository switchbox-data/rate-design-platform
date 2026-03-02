# NiMo MCOS dilution analysis

NiMo (National Grid Upstate) required the most work of any utility. The workbook lists ~237 planned capital projects but does not distinguish **bulk transmission** (≥230 kV) from **sub-transmission** (69–115 kV), so we had to classify every project by voltage tier before computing diluted marginal costs.

## Voltage tiers

- **bulk_tx** (≥230 kV) — NYISO bulk transmission; excluded from local marginal cost
- **sub_tx** (69–115 kV) — NiMo's sub-transmission network
- **distribution** (≤13.2 kV) — distribution substations and feeders

## Inputs

| Input                     | Where it lives                                                    | What it contains                                                                                                                                                                  |
| ------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| NiMo MCOS workbook        | `s3://data.sb/ny_psc/mcos_studies_2025/nimo_study_workpaper.xlsx` | Sheet 1 (Exhibit 1): one row per project with station name, MW, and capital by cost centre. Sheet 11 (FinalData): sub-project–level detail with descriptive names and InvestType. |
| NYISO Gold Book Table VII | `context/papers/nyiso_gold_book_2025.md` (extracted from PDF)     | Every proposed transmission project in New York, with station names, voltages, and descriptions.                                                                                  |

## Outputs

| Output                             | Description                                                                                                         |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `nimo_project_classifications.csv` | One row per project: classification, voltage, inference method, evidence text, sub-project names, confidence level. |
| `reclassify_nimo_projects.py`      | The script that produces the CSV. All manual decisions are encoded as commented data structures at the top.         |
| `nimo_diluted_levelized.csv`       | One row per tier: levelized and full-buildout diluted MC.                                                           |
| `nimo_diluted_annualized.csv`      | One row per (tier, year): nominal and real diluted MC.                                                              |

## How to reproduce the classification (manual process)

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

## How to run

```bash
cd utils/pre/dist_mc
just analyze-nimo
```

To regenerate the classification CSV:

```bash
cd utils/pre/dist_mc/nimo
uv run python reclassify_nimo_projects.py
```

This reads the workbook from S3, applies all the classification logic, and overwrites `nimo_project_classifications.csv`. The Gold Book reference data is hardcoded in the script — if the Gold Book changes, update the `GOLD_BOOK_NGRID_HIGH_VOLTAGE` and `GOLD_BOOK_NGRID_SUB_TX` lists.

## Downstream consumers

The classifications feed into `analyze_nimo_mcos.py`, which uses them to compute diluted marginal costs by tier (bulk TX, sub-TX, distribution, sub-TX + distribution). Those diluted MCs are the input to the BAT rate design.

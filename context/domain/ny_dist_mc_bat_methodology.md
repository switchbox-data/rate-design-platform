# Sub-TX and distribution marginal cost: shared methodology for BAT inputs

How we derive comparable sub-transmission and distribution marginal cost inputs for the Bill Alignment Test (BAT) from the seven heterogeneous NY MCOS study workbooks. For a comparison of the studies themselves, see `ny_mcos_studies_comparison.md`. For per-utility implementation details (cell references, script usage, output format), see the READMEs in `utils/pre/dist_mc/`.

---

## 1. Purpose

The BAT requires a sub-TX and distribution marginal cost ($/kW-yr) for each utility — a single number representing the forward-looking cost of distribution infrastructure per kW of system peak. The seven NY MCOS studies filed in 2025 are the best available data source, but they differ in methodology, data format, cost center labels, escalation treatment, and what they include. To produce comparable BAT inputs, we developed a shared methodology that:

1. Reads **project-level data** from each utility's workbook (or PDF).
2. Classifies projects into two buckets: `bulk_tx` (excluded from BAT) and `sub_tx_and_dist` (the BAT input).
3. Computes four MC variants (cumulative/incremental × diluted/undiluted) using in-service-year scoping.
4. Levelizes the **incremental diluted** real MC over a **7-year window (2026–2032)** in common **2026 dollars**.
5. Outputs eight CSVs per utility (annualized + levelized for each variant).

The result is seven levelized incremental diluted values — one per utility — that answer: "what is the average annual forward-looking sub-TX and distribution investment cost per kW of system peak, in constant 2026 dollars, over a 7-year near-term planning horizon?"

---

## 2. The shared methodology at a glance

```
For each utility:
  1. Parse project-level data (capital $, capacity MW, in-service year)
  2. Classify → bulk_tx or sub_tx_and_dist
  3. For each year Y in study period:
     - Incremental: scope to projects with in_service_year = Y
     - Cumulative: scope to projects with in_service_year ≤ Y
     - Compute annualized cost = capital × rate × escalation
     - Diluted MC = annualized cost / system_peak
     - Undiluted MC = annualized cost / project_capacity
  4. Levelized MC = mean(real MC for Y in 2026–2032)
```

Where "rate" is the utility-specific composite rate or ECCR (varies — see §4), "escalation" converts to nominal dollars, and "real MC" is in constant 2026 dollars.

The four MC variants serve different purposes:

| Variant               | Capital scope          | Denominator      | Use                                             |
| --------------------- | ---------------------- | ---------------- | ----------------------------------------------- |
| Cumulative diluted    | All projects through Y | System peak      | Cross-check against MCOS filing tables          |
| Incremental diluted   | Projects entering in Y | System peak      | **BAT input** (levelized over 2026–2032)        |
| Cumulative undiluted  | All projects through Y | Project capacity | Reference: cost per MW at constrained locations |
| Incremental undiluted | Projects entering in Y | Project capacity | Reference: project-level construction economics |

---

## 3. Key conceptual choices

### 3.1. Incremental, not cumulative

There are two coherent ways to interpret "T&D marginal cost" from an MCOS study:

**Perspective A — Accumulated ("what's the infrastructure bill?"):** In year Y, sum the annualized carrying charges on ALL projects in service through year Y, divide by system peak. This grows over time as more projects enter service. The levelized value is the time-average of the growing trajectory.

**Perspective B — Incremental ("what does one more kW cause?"):** In year Y, the carrying charges triggered by that year's new investment only, divided by system peak. This represents the cost one additional kW of peak demand imposes on the system.

The difference matters concretely: for ConEd, the accumulated Primary MC grows from $19.26/kW-yr (2025) to $50.17/kW-yr (2034) — 2.6× growth, far exceeding the 1.21× from escalation alone. The incremental Primary MC is roughly flat at ~$19/kW-yr (real), representing one year's sample of ~143 distribution projects.

**All six MCOS studies take Perspective A.** Every utility's year-by-year tables show the accumulated bill. Year 1 includes carrying charges on year 1's projects; year 5 includes carrying charges on years 1–5's projects; year 10 includes all ten years'. The tables grow because the portfolio grows, not just because of inflation.

**The BAT needs Perspective B.** The BAT (Simeone et al. 2023) splits each customer's costs into:

- **Economic cost** = marginal cost × consumption — the cost this customer's load _causes_.
- **Residual** = revenue requirement minus total economic cost — allocated by some non-causal principle.
- **Bill alignment** = bill − (economic cost + residual share).

For T&D infrastructure, the economic cost should capture the investment triggered by the customer's contribution to peak demand — the incremental perspective. Using accumulated MC would inflate the economic cost bucket by attributing to today's customer not just the cost their load triggers, but also the carrying charges on investments triggered by prior years' load growth. Those prior investments are embedded costs — part of the revenue requirement, but not marginal. They belong in the residual.

The BAT paper's own data source supports this interpretation. Appendix A describes the distribution CapEx input as "deferrable distribution capacity costs related to peak demand reductions" from the CPUC Avoided Cost Calculator — a prospective, deferral-based number. That's Perspective B.

### 3.2. Levelized, not year-by-year

The lumpiness in the year-by-year incremental series is an artifact of discrete project timing — a substation happens to come online in 2028 rather than 2029. But the underlying need for distribution investment is continuous. The fact that CenHud has zero incremental MC in 2028 and 2031 doesn't mean there's no marginal cost of distribution in those years — it means no project happens to complete then.

**Levelization resolves this.** The levelized incremental diluted value is the simple mean of incremental diluted real MC across a study window. Because adding across years just sums total capital:

```
levelized = mean(inc_real_mc across years)
          = Σ(all project annual costs in window) / (system_peak × N_years)
```

This depends only on **total capital** in the window, not on how it's distributed across years. Whether $60M enters in one year or is spread across three makes zero difference to the levelized value. The lumpiness that makes the year-by-year series noisy washes out completely in the average.

This is also why the non-project-based methods (ConEd's "annual sample" cost centers for Primary/Transformer/Secondary) exist — not because cumulative is conceptually better, but because those cost centers represent hundreds of small routine projects better characterized as a steady annual spend than as discrete lumpy capital.

### 3.3. The 7-year window (2026–2032)

The seven MCOS studies have different study periods (8 to 11 years). Using each utility's full study period produces levelized values that are not directly comparable — different time horizons capture different amounts of capital, and the later years of utility capital plans are increasingly speculative.

We evaluated three common windows:

| Utility | 5 yr (2026–2030) | 7 yr (2026–2032) | Full window | Full span |
| ------- | ---------------: | ---------------: | ----------: | --------- |
| ConEd   |             5.88 |             8.52 |        8.51 | 2025–2034 |
| O&R     |             4.04 |             3.90 |        4.69 | 2025–2034 |
| CenHud  |             2.07 |             3.93 |        4.24 | 2026–2035 |
| NiMo    |             3.87 |             4.49 |       10.13 | 2026–2036 |
| NYSEG   |             2.49 |             4.40 |       14.78 | 2026–2035 |
| RG&E    |             2.62 |             4.36 |       18.10 | 2026–2035 |
| PSEG-LI |             1.28 |             1.16 |        1.70 | 2025–2032 |

All values are sub_tx_and_dist incremental diluted real 2026 $/kW-yr.

**7 years (2026–2032) is the right window** because:

1. **At 5 years, you lose real investment.** ConEd's $2.3B in named substations (Nevins, Hillside) and PSEG-LI's Moriches/Deerfield phases (backed by regulatory filings) all fall in 2031–2032.
2. **At 7 years, speculative risk is minimal.** The only genuinely speculative entry is CenHud's "Future Unidentified" placeholder at 2032. Everything else is either named/identified or at least individually enumerated in the utility workbook.
3. **The real speculative risk lives in years 8–10 (2033+),** where NYSEG/RG&E show extreme back-loading (project counts roughly double, $/kW-yr roughly triples) and NiMo stacks $59/kW-yr at the FY2036 horizon.
4. **The window is the same for all utilities.** All seven have data covering 2026–2032. No utility is truncated or needs extrapolation.

See Appendix B for per-utility project-level evidence supporting this choice.

### 3.4. No discounting

The workbooks use NPV-based levelization with utility-specific discount rates:

| Utility   | Discount rate                                 |
| --------- | --------------------------------------------- |
| NiMo      | Not explicit in workbook                      |
| CenHud    | 6.76% (from PV multiplier column in workbook) |
| NYSEG/RGE | 6.975% WACC                                   |
| ConEd/O&R | Not shown separately (embedded in composite)  |

These are utility-specific WACCs that depend on each utility's capital structure. They range from ~6.8% (CenHud) to ~9.0% (O&R's ECCR-embedded rate), and two utilities (NiMo, ConEd) don't expose a standalone levelization discount rate at all. Using any single discount rate across all seven utilities would be arbitrary.

Our levelization uses a **simple arithmetic mean** — equivalent to a 0% discount rate. This is:

- **Transparent**: the only assumption-free choice.
- **Consistent**: the same formula for all seven utilities, with no utility-specific parameters.
- **Conservative**: on an increasing MC stream (typical — costs ramp as projects enter service), a simple average produces a higher levelized value than NPV-based levelization would.

The practical impact is modest. With a 7-year window, the difference between a simple average and a discounted average at ~7% is about 5% — smaller than the cross-utility methodology differences.

### 3.5. Common 2026 dollar year

The real columns are in different base years: 2025 for ConEd/O&R/PSEG-LI, 2026 for CenHud/NiMo/NYSEG/RG&E. We rebase all real values to a common **2026 dollar year** before levelizing.

The rebase multiplier is the one-year escalation factor from each utility's workbook:

- **ConEd / O&R**: GDP escalation index from Schedule 11/10 gives `escalation[2026]` ≈ 1.024. Applied as a constant multiplier to all real MC values.
- **PSEG-LI**: 2.1% GDP deflator; `REBASE_FACTOR = 1.021`. Applied to the filing's undiluted MC rate before computing year-by-year values.
- **CenHud / NiMo / NYSEG / RG&E**: Already in 2026 dollars; no adjustment needed.

The adjustment is small (~2%) but ensures all values are in the same units.

---

## 4. Per-utility deviations and harmonization

### Summary

All seven utilities now use the same core pipeline (§2), but each study's workbook required utility-specific handling to extract project-level data and normalize it. The table below summarizes the key dimensions along which the studies deviated from the shared methodology and what we did:

| Dimension           | ConEd / O&R                     | CenHud                          | NiMo                         | NYSEG / RG&E               | PSEG-LI                    |
| ------------------- | ------------------------------- | ------------------------------- | ---------------------------- | -------------------------- | -------------------------- |
| **Data source**     | Workbook per-project cashflows  | Workbook per-project rows       | Workbook per-project rows    | W2 per-project investment  | PDF → project CSV          |
| **ISD method**      | Cashflow stabilization          | Explicit column                 | Explicit column              | Explicit ISD column        | Explicit from project list |
| **Annualization**   | Composite rate × escalation     | Pre-computed annual costs       | Pre-computed ECCR + F-values | Derived composite rate     | ECCR+O&M rates             |
| **Escalation**      | GDP 2.4%/2.1%/yr (native)       | **Flat → applied 2.1%/yr**      | GDP 2.1%/yr (native)         | 2.0%/yr (native)           | Not stated (2.1% assumed)  |
| **Diluted formula** | Cost / system_peak              | **Peak-share → capacity-based** | Cost / system_peak           | Cost / system_peak         | Cost / system_peak         |
| **Bulk TX**         | Separate cost center (excluded) | No bulk TX                      | **Classified via Gold Book** | Explicitly excluded by CRA | Classified via voltage     |
| **Base year**       | **2025 → rebased to 2026**      | 2026 (native)                   | 2026 (native)                | 2026 (native)              | **2025 → rebased to 2026** |

Bold entries indicate deviations from the shared pipeline that required normalization. Per-utility details follow.

### 4.1. ConEd and O&R

ConEd and O&R use the NERA methodology with composite rates. Their workbooks present cumulative cost centers (Transmission, Substation for both; Primary for O&R) as **aggregate** capital-by-year totals in the left half of each sheet. These aggregate totals include Construction Work In Progress (CWIP) — capital spent on projects still under construction but not yet in service.

**The problem.** Reading aggregate totals directly would include CWIP before any capacity is added, inflating early-year cumulative capital and producing an inconsistency with NiMo/CenHud (which exclude CWIP because projects contribute only when complete).

**The solution.** Both workbooks contain per-project data in the **right half** of the cumulative cost center sheets: project name, capacity in MW, and year-by-year cumulative cashflow columns spanning the study period. We read this project-level data and infer in-service years using a **cashflow-stabilization heuristic**: the in-service year is the first year at which the cumulative cashflow reaches its final value (i.e., CWIP ends and the project is complete).

For O&R Primary, the data is structured differently: each project's annual budget (not cumulative cashflow) appears in columns X–AG, with zeros before the project starts and a constant value afterward. The in-service year is the first nonzero column.

**Bulk TX treatment.** ConEd's "Transmission System" cost center covers FERC-jurisdictional 138/345 kV facilities. O&R has the same structure. Both are excluded from the `sub_tx_and_dist` bucket (and available in the `bulk_tx` bucket for separate use).

**Base year rebase.** Both studies start in 2025 with real values in 2025 dollars. Rebased to 2026 using the GDP escalation index from their workbooks (×1.024).

See the ConEd and O&R READMEs for cell-level workbook references and worked examples.

### 4.2. CenHud

CenHud's study (DSA, June 2025) departs from the other utilities in two ways.

**Escalation.** CenHud's workbook provides flat nominal costs — a project's annual $/kW-yr is identical every year after in-service. No GDP deflator, no inflation. All other utilities escalate at approximately 2.1%/yr. We treat the workbook's flat costs as base-year (2026) real values and apply:

```
escalation(Y) = 1.021^(Y − 2026)
nominal_mc(Y) = real_mc(Y) × escalation(Y)
```

The 2.1% rate matches NiMo's steady-state rate and ConEd/O&R's rate from year 2 onward. The `real_mc` column preserves the workbook's original flat values. The levelized MC (mean of `real_mc`) is unaffected.

**Diluted formula.** CenHud's workbook computes diluted MC using **peak-share weighting**: each project's cost is weighted by how much of the system's load its service area represents (`peak_share`). The other utilities all weight by `capacity / system_peak`. These produce different values because `peak_share(p) ≠ capacity(p) / system_peak`. We normalize CenHud to capacity-based for cross-utility consistency. See Appendix C for the detailed comparison.

**Bulk TX.** CenHud has no FERC-jurisdictional bulk transmission. The "Local Transmission" cost center covers 69 kV and 115/69 kV areas — all explicitly labeled "local." All three cost centers are included in `sub_tx_and_dist`.

### 4.3. NiMo

NiMo's workbook labels cost components as T-Station / T-Line / D-Station / D-Line but does not distinguish bulk TX from sub-TX. The 238 projects span voltage levels from 13.2 kV distribution feeders to 345 kV NYISO-scale transmission.

**Classification.** All 238 projects were classified by cross-referencing with the NYISO Gold Book (Table VII) for voltage levels:

- **Bulk TX** (≥230kV): 2 projects, 2,100 MW, $1.07B (Smart Path Connect, Niagara-Dysinger). Excluded from `sub_tx_and_dist`.
- **Sub-TX** (69–115kV): 47 projects, 6,889 MW, $6.71B. Included in `sub_tx_and_dist`.
- **Distribution** (≤13.2kV): 189 projects, 2,543 MW, $2.93B. Included in `sub_tx_and_dist`.

Full classifications with evidence are in `utils/pre/dist_mc/nimo/nimo_project_classifications.csv` and are reproducible via `classify_nimo_projects.py`.

**Annualization.** NiMo uses the simplest approach: bare ECCR × capital/MW with no additional loaders. The F columns in the workbook provide pre-computed annualized costs per year, inflating at 2.1%/yr.

**FY2036 horizon spike.** NiMo backloads ~4,700 MW of sub-TX and distribution projects into the final fiscal year (FY2036), producing a $59/kW-yr spike. The 7-year levelization window (2026–2032) excludes this artifact.

### 4.4. NYSEG and RG&E

NYSEG and RG&E were analyzed by CRA International using a more complex methodology than NERA. CRA computes MC at each substation using location-specific growth factors, demand-related loss factors, within-division adjustments, and N-0/N-1 capacity analysis. The final system-wide tables embed these adjustments, making them not directly comparable to the NERA utilities.

**The solution.** The CRA workbooks contain **W2 (Investment Location Detail)** with per-project data: capital, capacity (MVA, derated to 90% utilization), in-service date, division, and cost center. We read W2 directly and apply NERA-style project-level aggregation.

**Composite rate** is derived from W2 by comparing col 51 (fully loaded annualized $/kW-yr) to col 31 (capital $/kW) at each project's in-service year:

| Equipment  | NYSEG   | RG&E    |
| ---------- | ------- | ------- |
| Substation | 0.10248 | 0.10283 |
| Feeder     | 0.09801 | 0.09836 |

**Loss factors** from W4 are applied when computing the "Total at Primary" column:

| Factor             | NYSEG  | RG&E   |
| ------------------ | ------ | ------ |
| Upstream → primary | 1.0497 | 1.0543 |
| Dist sub → primary | 1.0292 | 1.0320 |
| Primary → primary  | 1.0220 | 1.0228 |

**Bulk TX.** CRA explicitly excludes NYISO TSCs. All cost centers are included in `sub_tx_and_dist`.

**System peak.** NYSEG/RG&E use the 2035 forecast peak (2,036 MW and 1,429 MW respectively) — larger than the current actual. This systematically lowers diluted values relative to current-peak-based utilities.

See Appendix D for the full comparison of NERA-style vs. CRA native values.

### 4.5. PSEG-LI

PSEG-LI's study is filed as a PDF only (no workbook). The 30 projects were transcribed to a CSV and classified by voltage using LIPA's network architecture:

- **Sub-TX** (T-Substation, ≤69 kV): 15 projects. LIPA's 138/345 kV = BES (excluded); ≤69 kV = sub-TX (included).
- **Distribution** (D-Substation + D-Feeders): 15 projects.
- No T-Line projects in the screened portfolio.

The two-step pipeline (`classify_psegli_projects.py` → `analyze_psegli_mcos.py`) mirrors NiMo's classification approach.

**Annualization.** ECCR+O&M from Exhibit 1: Sub-TX 8.2%, Distribution 13.9%. The filing's undiluted MC per kW is multiplied by the ECCR+O&M rate and diluted by system peak (4,935 MW).

**Base year rebase.** Study starts in 2025 with real values in 2025 dollars. Rebased to 2026 using `REBASE_FACTOR = 1.021`.

---

## 5. Final values and sanity check

### Recommended BAT input

The BAT input for sub-TX and distribution marginal cost is the **levelized incremental diluted** value over the **2026–2032 window** (7 years), using **real** (constant-dollar) values **rebased to 2026 dollars**, with **no discounting**:

| Utility | Levelized inc. diluted (real 2026 $/kW-yr, 2026–2032) |
| ------- | ----------------------------------------------------: |
| PSEG-LI |                                                  1.16 |
| O&R     |                                                  3.90 |
| CenHud  |                                                  3.93 |
| RG&E    |                                                  4.36 |
| NYSEG   |                                                  4.40 |
| NiMo    |                                                  4.49 |
| ConEd   |                                                  8.52 |

This represents: "the average annual forward-looking sub-TX and distribution investment cost per kW of system peak, in constant 2026 dollars, over a 7-year near-term planning horizon."

### Sanity check

The 7× spread between ConEd ($8.52) and PSEG-LI ($1.16) is large, but it is fully explained by the underlying project data and system characteristics:

1. **ConEd is highest** — despite the largest system peak (12,000 MW denominator), ConEd has massive named substation projects entering in the window: Parkchester #2, Idlewild, Gateway, Parkview in 2028 (~$2B) and Nevins Street, Hillside, Bruckner in 2032 (~$2.4B). Plus annual primary/transformer/secondary distribution sample costs. NYC underground infrastructure is categorically more expensive per kW than anywhere else in the state.

2. **PSEG-LI is lowest** — the second-largest peak (4,935 MW) dilutes relatively modest investment. Year-by-year incremental diluted is very flat ($0.6–$1.9/kW-yr nominal). Most capacity enters early (2025–2027); 2030–2032 additions are small. Long Island's overhead distribution and lower infrastructure density explain the gap with ConEd.

3. **NYSEG ≈ RG&E** ($4.40 vs $4.36) — these sister Avangrid utilities, both analyzed with the same CRA methodology, should have similar near-term investment intensity. At the full study period they looked very different ($14.78 vs $18.10) due to differential back-loading artifacts. The 7-year window strips that noise, and their convergence is reassuring.

4. **The mid-tier cluster** (O&R $3.90, CenHud $3.93, RG&E $4.36, NYSEG $4.40, NiMo $4.49) — these are all suburban/rural upstate and suburban NY utilities with moderate investment profiles. The tight $3.90–$4.49 band is consistent with their similar infrastructure types and investment horizons.

5. **Ordering matches physical intuition** — dense urban underground (ConEd) >> large upstate territory with significant sub-TX (NiMo, NYSEG/RG&E) ≈ smaller suburban (CenHud, O&R) >> large-peak overhead (PSEG-LI).

---

## Appendix A. The four harmonized formulas

After harmonization, all seven utilities use the same core formulas, differing only in the utility-specific composite rate / ECCR mechanism and escalation schedule. The key invariant is that **capital and capacity enter together**, gated by in-service year.

**Cumulative diluted:**

```
Capital(Y) = sum(p.final_capital for p where p.in_service_year ≤ Y)
MC(Y) = Capital(Y) × composite_rate × escalation(Y) / system_peak
```

**Incremental diluted:**

```
Capital(Y) = sum(p.final_capital for p where p.in_service_year = Y)
MC(Y) = Capital(Y) × composite_rate × escalation(Y) / system_peak
```

**Cumulative undiluted:**

```
Capital(Y) = sum(p.final_capital for p where p.in_service_year ≤ Y)
Capacity(Y) = sum(p.MW for p where p.in_service_year ≤ Y)
MC(Y) = Capital(Y) × composite_rate × escalation(Y) / Capacity(Y)
```

**Incremental undiluted:**

```
Capital(Y) = sum(p.final_capital for p where p.in_service_year = Y)
Capacity(Y) = sum(p.MW for p where p.in_service_year = Y)
MC(Y) = Capital(Y) × composite_rate × escalation(Y) / Capacity(Y)
```

### Impact of harmonization on MC values

**Cumulative diluted**: lower in early years vs. pre-harmonization (CWIP excluded), converges to the same terminal value. The levelized value is moderately lower because the trajectory is pulled down in early years.

**Incremental diluted**: year-by-year distribution changes (capital is now assigned to the completion year, not spread across construction years), but the levelized value is similar because the total capital added over the study period is unchanged.

**Cumulative undiluted**: previously roughly constant (proportional capital / proportional capacity). Now varies meaningfully year by year, reflecting the actual $/kW of the project cohort in service at each point.

**Incremental undiluted**: now reflects the specific $/kW of the project(s) entering service in each year, and is undefined (no projects) in years without completions. The most volatile variant but also the most economically meaningful.

### Remaining cross-utility differences

The harmonization covers the **scoping** of capital and capacity. It does not eliminate:

- **Composite rate vs. bare ECCR**: ConEd/O&R fold loaders into a single multiplier; NiMo uses bare ECCR; CenHud uses ECCR + explicit loaders; NYSEG/RG&E use derived composite rates.
- **Escalation**: CenHud's workbook is flat nominal (we apply 2.1%/yr); others escalate natively at 2.0–2.4%/yr.
- **Diluted formula**: CenHud's workbook uses peak-share weighting (we normalize to capacity-based).
- **System peak denominator**: utilities use different fixed peaks — current actual (NiMo, CenHud), end-of-horizon forecast (NYSEG/RG&E), or unstated fixed (ConEd/O&R).

---

## Appendix B. Empirical evidence for the 7-year window

To choose between 5 and 7 years, we examined the actual projects that fall in 2031–2032 for each utility.

**ConEd 2031–2032 ($3.98 + $25.58 = $29.56/kW-yr across 2 years):**

- 2031: Millwood West (79 MW, $84M), Cedar Street (85 MW, $253M) — named substations with specific locations
- 2032: Nevins Street new (358 MW, $1.47B), Hillside new (358 MW, $872M), Bruckner (32 MW, $20M) — named, specific projects
- **Assessment: Very real.** Nevins and Hillside alone are $2.3B of identified investment. This is the biggest dollar impact and the primary driver of ConEd's 5yr→7yr jump ($5.88 → $8.52). Excluding them would materially understate ConEd's distribution MC.

**O&R 2031–2032 ($1.72 + $5.23 = $6.95):**

- 2031: nothing new
- 2032: Oak Street (local TX reconductoring, 79.8 MW, $29M) — named, specific
- **Assessment: Real.** Small, named project.

**CenHud 2031–2032 ($0.00 + $17.21 = $17.21):**

- 2031: nothing
- 2032: "Future Unidentified" Local TX (296.5 MW, $55.40/kW-yr) + Hurley Ave (substation, 10.4 MW, $245.75/kW-yr)
- **Assessment: Mixed.** Hurley Ave is a named project (real). But the "Future Unidentified" is explicitly a placeholder — CenHud's 5-year capital forecast identifies specific projects for years 1–5; years 6–10 assume a similar proportion of territory needs investment. The placeholder drives most of the $17.21 spike. This is the **only genuinely speculative entry** in the 7-year window.

**NiMo 2031–2032 ($8.40 + $3.68 = $12.08):**

- 237 projects with individual FN numbers in the workbook. 2031–2032 are years 6–7 of the study. The annualized values ($8.40, $3.68) are moderate — not spiky.
- **Assessment: Moderately real.** Specific project entries exist but outer-year firmness is inherently lower than years 1–5.

**NYSEG 2031–2032 ($5.40 + $12.98 = $18.38):**

- 2031: 4 projects. 2032: 12 projects. All from W2 with specific location and capital data.
- Compare with 2033–2035: 20+20+28 projects at $39+$44+$34/kW-yr — the extreme ramp clearly indicates back-loading. 2031–2032 are the last years before this ramp.
- **Assessment: Moderately real.** Individual entries exist; the extreme back-loading doesn't start until 2033.

**RG&E 2031–2032 ($5.30 + $12.15 = $17.45):**

- 2031: 5 projects. 2032: 10 projects. Same CRA methodology as NYSEG.
- 2033–2035 ramp: $30+$45+$76/kW-yr — same pattern as NYSEG.
- **Assessment: Same as NYSEG.** The real back-loading lives in 2033+.

**PSEG-LI 2031–2032 ($0.63 + $1.05 = $1.68):**

- 2031: Moriches phase 2 (67 MW) — named project, NYISO interconnection queue C24-061
- 2032: Deerfield phase 2 (112 MW) — named project, NY Article VII filing Case 24-T-0113
- **Assessment: Very real.** Both have public regulatory filings and specific engineering documentation.

---

## Appendix C. CenHud peak-share vs. capacity-based dilution

CenHud's workbook computes diluted MC using a peak-share weighting formula:

```
CenHud workbook:  MC_diluted(Y) = sum[ cost_per_kW(p) × peak_share(p) ]
Other utilities:  MC_diluted(Y) = sum[ cost(p) ] / system_peak
                                = sum[ cost_per_kW(p) × capacity(p) ] / system_peak
```

These produce different values because `peak_share(p) ≠ capacity(p) / system_peak`:

| CenHud project                 | capacity / system_peak          | peak_share | Ratio |
| ------------------------------ | ------------------------------- | ---------- | ----- |
| Feeder Future Unidentified     | 16,459 / 1,103,000 = **1.49%**  | **24.14%** | 16×   |
| WI_8031 (Feeder)               | 13,000 / 1,103,000 = **1.18%**  | **0.38%**  | 0.3×  |
| Substation Future Unidentified | 61,850 / 1,103,000 = **5.61%**  | **7.35%**  | 1.3×  |
| Northwest 115/69 (Local TX)    | 166,500 / 1,103,000 = **15.1%** | **12.25%** | 0.8×  |

The peak-share approach weights by how much of the system's load the project's service area represents. The capacity-based approach weights by how much new capacity is added relative to the system. These answer fundamentally different questions: "how much of the system bears this cost" (peak-share) vs. "how much capacity was added per kW of system load" (capacity-based).

What each utility does:

| Utility             | Diluted formula                      | Weighting                                               |
| ------------------- | ------------------------------------ | ------------------------------------------------------- |
| **ConEd**           | `Capital × Rate × Esc / system_peak` | Aggregate cost / system peak (no per-project weighting) |
| **O&R**             | Same as ConEd                        | Same                                                    |
| **NiMo**            | `sum(F_Y × capacity) / system_peak`  | Per-project total cost / system peak                    |
| **CenHud workbook** | `sum(cost_per_kW × peak_share)`      | Per-project cost weighted by area load share            |

ConEd, O&R, and NiMo all compute diluted MC as **total annual cost / system peak**. We normalize CenHud to the capacity-based approach for consistency.

**Trade-off.** The capacity-based formula does not match CenHud's workbook Table 2 validation targets. The workbook's Table 2 values are documented in the CenHud README as a reference, but our output CSVs use the capacity-based formula for cross-utility comparability.

### CenHud escalation: flat nominal vs. GDP deflator

CenHud's workbook provides flat nominal costs. What each utility does:

| Utility             | Escalation                  | Rate                    | Mechanism                                                   |
| ------------------- | --------------------------- | ----------------------- | ----------------------------------------------------------- |
| **ConEd**           | GDP Implicit Price Deflator | 2.4% yr 1, then 2.1%/yr | Compounding factor from Carrying Charge Loaders row 25      |
| **O&R**             | GDP Implicit Price Deflator | 2.4% yr 1, then 2.1%/yr | Same as ConEd (Carrying Charge Loaders row 26)              |
| **NiMo**            | Blue Chip GDP Deflator      | 2.1%/yr flat            | Baked into F columns: F_Y = E × 1.021^(Y − in_service_year) |
| **CenHud workbook** | **None**                    | —                       | Flat nominal. cost(2033) = cost(2034) = cost(2035)          |

We chose 2.1% flat (rather than ConEd/O&R's 2.4%/2.1% schedule) because: (a) CenHud's study period starts in 2026, not 2025, so there's no "first year" in the ConEd/O&R sense; and (b) 2.1% is the consensus long-run GDP deflator across all three other utilities.

### Effect summary

| Change                                  | Affects                                                 | Effect on levelized MC                            | Effect on annualized CSVs                   |
| --------------------------------------- | ------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------- |
| Diluted formula: peak-share → capacity  | Diluted variants only (both cumulative and incremental) | Changes diluted levelized values                  | Changes diluted nominal and real columns    |
| Escalation: flat → 2.1%/yr GDP deflator | All variants (both diluted and undiluted)               | None (levelized uses real_mc, which is unchanged) | Nominal columns now escalate year over year |

---

## Appendix D. NYSEG/RG&E: CRA native vs. NERA-style comparison

### How CRA methodology differs

The four NERA utilities compute MC from project-level data using straightforward formulas. CRA's methodology for NYSEG/RG&E is more complex:

1. **Growth factors (W5)**: per-division seasonal growth rates determining when capacity is needed at each location.
2. **Demand-related loss factors (W4)**: voltage-level-specific loss multipliers per substation.
3. **Within-division adjustment**: only a fraction of substations have planned investment (~23% of upstream, ~35% of dist substations/feeders). CRA adjusts for this.
4. **N-0/N-1 capacity analysis**: per-substation capacity adequacy assessment.

These produce substation-level MCs, aggregated upward: substation → division → system-wide.

### Why we departed from CRA's tables

Reading CRA's pre-computed tables embedded location-specific adjustments with no NERA analog:

1. CRA attributes investment costs during **construction** (CWIP-style), not just at completion.
2. CRA's within-division adjustment reduces MC by ~30–50% because it accounts for areas with no planned investment.
3. Growth factors cause different divisions' investments to contribute differently, creating a location-weighted result.

### W2 parsing details

| Data element                  | W2 column | Notes                                   |
| ----------------------------- | --------- | --------------------------------------- |
| Division                      | C (3)     |                                         |
| Segment (Upstream/Dist)       | E (5)     | Maps to cost center                     |
| Equipment (Substation/Feeder) | G (7)     | Determines composite rate               |
| In-Service Date               | I (9)     | Used for project scoping                |
| Total capital ($000)          | T (20)    | Sum of annual investment cols J–S       |
| Final capacity (MVA)          | BK (63)   | Derated (= nameplate × 0.9 utilization) |

### Project coverage

| Utility | Total W2 rows | Parsed (nonzero capital) | Excluded (zero/no capital) |
| ------- | ------------- | ------------------------ | -------------------------- |
| NYSEG   | 201           | 107                      | 94                         |
| RG&E    | 132           | 96                       | 36                         |

NYSEG has no projects entering service in 2026–2027; RG&E has 3 upstream feeder projects in 2027 and 1 in 2028.

### Impact on values

The NERA-style values differ from CRA native because of no CWIP, no within-division adjustment, and no growth factor weighting:

| Metric                   | NYSEG (NERA) | NYSEG (CRA native) | RG&E (NERA) | RG&E (CRA native) |
| ------------------------ | ------------ | ------------------ | ----------- | ----------------- |
| Inc. diluted lev. (real) | $14.78       | ~$23.11            | $18.10      | ~$37.14           |

The NERA values are **lower** than CRA native, primarily because CRA's location-specific adjustments inflate costs at constrained substations. The undiluted variants show larger differences because CRA capacity-weights by effective capacity at each substation (accounting for losses and growth), while we use the raw MVA from W2.

---

## Appendix E. ConEd/O&R: CWIP exclusion and in-service-year inference

### The original inconsistency

NiMo and CenHud have always used project-level data: each project has an explicit in-service year, capital cost, and capacity. The cumulative MC for year Y sums capital and capacity only for projects where `in_service_year ≤ Y`; the incremental MC uses projects where `in_service_year = Y`.

ConEd and O&R's workbooks present cumulative cost centers as **aggregate** capital-by-year totals. These include CWIP — capital spent on projects still under construction. Reading these directly produced:

1. **Inflated early-year cumulative capital** (CWIP counted before capacity is added).
2. **Proportional capacity derivation** (smooth trajectories that don't reflect actual project completion timing).
3. **Constant undiluted MC** (both capital and capacity grow proportionally, masking real variation in $/kW).

### In-service year inference methods

| Utility    | Cost centers                                            | Data format                | In-service year method                                                   |
| ---------- | ------------------------------------------------------- | -------------------------- | ------------------------------------------------------------------------ |
| **NiMo**   | All (T-Station, T-Line, D-Station, D-Line)              | Explicit per-project       | Stated in workbook (column D)                                            |
| **CenHud** | All (Local TX, Substation, Feeder)                      | Explicit per-project       | Stated in workbook (in-service year column)                              |
| **ConEd**  | TX (5 project-area rows), Substation (17 rows)          | Cumulative cashflow (W–AF) | First year where cashflow = final value (CWIP ends)                      |
| **O&R**    | Bulk TX (1 row), Local TX (2 rows), Substation (4 rows) | Cumulative cashflow (W–AF) | Same as ConEd: cashflow stabilization                                    |
| **O&R**    | Primary (26 rows)                                       | Annual budget (X–AG)       | First nonzero year (each project has constant annual budget after start) |

The cashflow-stabilization heuristic works because CWIP causes the cumulative cashflow to grow year over year during construction. Once a project is in service, its cumulative cashflow plateaus at the final capital cost. The in-service year is the first year at which this plateau is reached.

---

## Appendix F. System peak denominator choices across utilities

Every utility uses a **fixed** peak value across all study years, but they differ in which year's peak they use. This finding was verified against the actual workbook cells.

| Utility    | System peak (MW) | Basis                                 |
| ---------- | ---------------- | ------------------------------------- |
| **NiMo**   | 6,616            | 2024 actual system peak               |
| **CenHud** | 1,103            | 2024 actual coincident peak           |
| **NYSEG**  | 2,036            | 2035 forecast (end of study period)   |
| **RGE**    | 1,429            | 2035 forecast (end of study period)   |
| **ConEd**  | ~12,000 (fixed)  | Fixed peak, year not stated in report |
| **O&R**    | ~2,200 (fixed)   | Fixed peak, year not stated in report |

**Verification for ConEd.** The fixed-denominator finding was confirmed by examining MC growth rates after all transmission projects are built out (2029+). If the denominator were the forecasted (growing) peak, MC growth would be ~0.6%/yr (escalation minus load growth). Instead, MC growth exactly matches the 2.1% escalation rate, confirming a fixed denominator:

| Year transition | MC growth | Expected if denominator fixed | Expected if denominator = forecast |
| --------------- | --------- | ----------------------------- | ---------------------------------- |
| 2029→2030       | 2.10%     | 2.10% ✓                       | ~0.6% ✗                            |
| 2030→2031       | 2.11%     | 2.10% ✓                       | ~0.6% ✗                            |
| 2031→2032       | 2.05%     | 2.10% ≈✓                      | ~0.6% ✗                            |
| 2032→2033       | 2.10%     | 2.10% ✓                       | ~0.6% ✗                            |
| 2033→2034       | 2.10%     | 2.10% ✓                       | ~0.6% ✗                            |

The same pattern holds for O&R.

**Why this matters.** NYSEG/RG&E use the end-of-horizon forecast (2035), which is larger than the current peak. This systematically lowers their diluted values relative to current-peak-based utilities (~10% for NYSEG). The choice interacts with electrification forecasts: if load growth doesn't materialize, the diluted MC understates the per-kW cost. Using the current peak is more conservative.

**Cross-referencing with NYISO Gold Book.** Zone-level peaks in the Gold Book do not map 1:1 to utility service territories. NYSEG's 2,036 MW is below Zone C's 3,615 MW because NYSEG serves only part of the zone. CenHud's 1,103 MW is about half of Zone G's 2,081 MW (Zone G also includes O&R territory).

---

## Appendix G. The three approaches to annualization

The six utility filings use three distinct approaches to convert capital costs into annual revenue requirements. Understanding these is useful for interpreting cross-utility differences in raw MC values, even though the harmonized formulas (Appendix A) produce comparable outputs.

### 1. Composite rate (ConEd, O&R)

One multiplier per cost center folds ECCR, general plant loading, O&M, working capital, loss factors, and (for ConEd) a coincidence factor into a single number:

```
Annual RR = Capital × Composite Rate × Escalation
```

ConEd composite rates: 12–15% depending on cost center. O&R: similar range.

**Pro:** Cleanest formula — one multiplication. **Con:** Cannot decompose into ECCR vs. loaders without reverse-engineering from Schedule 11/10.

### 2. Separate loaders (CenHud, NYSEG/RG&E)

ECCR and each loader applied as visible, sequential steps:

- **CenHud**: Reserve margin (×1.30) and general plant loading (×1.161) multiply the $/kW _before_ ECCR. Working capital and loss factors add on _after_. Most explicit approach — you can see how much each component contributes.
- **NYSEG/RG&E**: ECCR result × ~1.32–1.34 (O&M + A&G loading factor) = final annualized $/kW. Loaders bundled into a single post-ECCR multiplier.

### 3. Bare ECCR (NiMo)

No loaders at all:

```
Annual RR = Capital/MW × ECCR
```

NiMo ECCR rates: T-Station 8.21%, T-Line 8.44%, D-Station 8.06%, D-Line 14.13%.

All overhead is implicitly absorbed into the ECCR rates or captured elsewhere in the revenue requirement. Simplest to replicate. NiMo's base ECCR rates look low (8%) compared to CenHud (13–18%) but the effective all-in rate is similar because CenHud stacks reserve margin and plant loading on top before applying its ECCR.

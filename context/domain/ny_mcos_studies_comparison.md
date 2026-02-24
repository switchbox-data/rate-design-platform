# NY 2025 MCOS studies: cross-utility comparison

Comparison of the six Marginal Cost of Service studies filed under NY PSC Docket 19-E-0283 in 2025. All studies respond to the August 19, 2024 Order Addressing Marginal Cost of Service Studies, which directed the Joint Utilities to file MCOS studies using the NERA methodology with a 10-year planning horizon.

Source extractions are in `context/papers/mcos/`.

---

## 1. Overview and comparison of the six studies

### Who did the studies

| Utility                        | Shortcode  | Consultant                     | Date filed       | Total pages |
| ------------------------------ | ---------- | ------------------------------ | ---------------- | ----------- |
| Consolidated Edison            | ConEd      | In-house (NERA methodology)    | June 2025        | 20          |
| Orange & Rockland              | O&R        | In-house (NERA methodology)    | June 2025        | 20          |
| Central Hudson                 | CenHud     | Demand Side Analytics (DSA)    | June 2025        | 23          |
| Niagara Mohawk / National Grid | NiMo       | In-house                       | June 30, 2025    | 49          |
| NYSEG + RG&E                   | NYSEG/RG&E | Charles River Associates (CRA) | July 15, 2025    | 80          |
| PSEG Long Island / LIPA        | PSEG-LI    | In-house                       | December 9, 2025 | 11          |

### Study periods

| Utility | Study period  | Years | Notes                                               |
| ------- | ------------- | ----- | --------------------------------------------------- |
| ConEd   | 2025–2034     | 10    | Calendar year                                       |
| O&R     | 2025–2034     | 10    | Calendar year                                       |
| CenHud  | 2026–2035     | 10    | Calendar year                                       |
| NiMo    | FY2026–FY2036 | 11    | Fiscal year (Apr–Mar); FY2026 = Apr 2025 – Mar 2026 |
| NYSEG   | 2026–2035     | 10    | Calendar year                                       |
| RG&E    | 2026–2035     | 10    | Calendar year                                       |
| PSEG-LI | 2025–2032     | 8     | Below the 10-year requirement                       |

### Methodology

All studies nominally follow the NERA methodology as required by the Order. In practice:

**Similarities across all six:**

- Project-based: identify load-growth and multi-value capital projects in the planning horizon.
- Convert project cost to $/kW of added capacity, then annualize using an Economic Carrying Charge (ECC/ECCR) that includes return on/of capital, O&M, taxes, and insurance.
- Inflate/deflate to each year using the Blue Chip GDP Implicit Price Deflator (2.1% annual, from June 2, 2025 Blue Chip Forecasts).
- Present costs at the substation serving area level, by year, for each cost segment.
- Include both load-growth and multi-value (growth + reliability) projects.

**Key differences:**

| Dimension                         | ConEd / O&R                                                                                                             | CenHud                                                                                                                 | NiMo                                                                                                                              | NYSEG / RG&E                                                                                                                                     | PSEG-LI                                                                               |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| **Annualization**                 | Composite factor (ECCR + O&M + working capital + general plant loading, all folded into one multiplier per cost center) | Similar composite; separate ECCR by asset level, with explicit 30% reserve margin applied to $/kW before annualization | ECCR per asset type (T-Station 8.21%, T-Line 8.44%, D-Station 8.06%, D-Line 14.13%); no composite factor — just ECCR × capital/MW | ECC formula produces first-year annual revenue requirement constant in real terms over asset life; adds O&M and A&G loading factors separately   | ECCR (TX 4.79%, Dist 5.02%) plus separate loaders (general plant 2.9%, O&M 3.4%/8.9%) |
| **Escalation**                    | GDP deflator 2.1%/yr applied to all years                                                                               | GDP deflator 2.1%/yr                                                                                                   | GDP deflator 2.1%/yr                                                                                                              | GDP deflator 2.1%/yr                                                                                                                             | Not stated in extractable text                                                        |
| **Reserve margin**                | Not explicitly stated as a multiplier; embedded in capacity calculations                                                | 30% reserve margin applied to $/kW before annualization                                                                | Not explicitly stated; capacity added is project-level                                                                            | Capacity added × 0.9 to reflect 90% utilization planning criteria                                                                                | Not stated                                                                            |
| **Capital plan horizon**          | 10-year plan for TX and area station; 1–1.5 years for lower-voltage                                                     | 5-year corporate capital forecast; years 6–10 assumed similar proportion of territory with needs as years 1–5          | 10-year FY2026–FY2036 capital plan; excluded projects < $1M                                                                       | Capital plan through 2031; CAS (Comprehensive Area Studies) extend to 2032–2035; independent N-0 screening adds potential projects for 2033–2035 | 8-year plan FY2025–FY2032                                                             |
| **Diluted vs undiluted**          | Undiluted only (excludes zero-MC areas from system average)                                                             | Both (Table 1 = undiluted, Table 2 = diluted)                                                                          | Undiluted only (projects with capital only)                                                                                       | Both (Tables 1–2 = undiluted by division and year; Table 3 = diluted with % of system needing no investment)                                     | Not extractable                                                                       |
| **Loss adjustment**               | Not explicitly stated for system average                                                                                | Loss factors applied (1.01 TX, 1.02 sub, 1.05 feeder)                                                                  | Not stated                                                                                                                        | Total MC shown at both primary and secondary voltage (loss-adjusted)                                                                             | Not stated                                                                            |
| **Time differentiation**          | Not addressed in MCOS filing                                                                                            | Not addressed                                                                                                          | Not addressed                                                                                                                     | Section 5: probability-of-peak (PoP) analysis by hour, day-type, month; shift from summer to winter peaking by ~2028                             | Not addressed                                                                         |
| **Local distribution facilities** | Not addressed as a separate cost                                                                                        | Not addressed                                                                                                          | Not addressed                                                                                                                     | Section 6: per-customer-class $/kW-yr for secondary transformers, secondary lines, local primary taps (residential ~$110/kW-yr)                  | Not addressed                                                                         |

### Notable similarities

1. All use the same Blue Chip GDP deflator (2.1%/yr, June 2025 edition).
2. All present year-by-year nominal $/kW at the substation level.
3. All include multi-value (growth + reliability) projects, per the Order.
4. None produce a single "levelized" system-wide number as the primary output — all emphasize that MC varies by location and year.

### Notable differences

1. **Diluted MC availability:** Only CenHud and NYSEG/RG&E provide a system-wide MC that includes zero-MC areas (the diluted number). ConEd, O&R, NiMo, and PSEG-LI only report the undiluted (projects-only) MC.
2. **Time differentiation:** Only NYSEG/RG&E (CRA) perform a probability-of-peak analysis. The others present annual $/kW-yr without hourly allocation.
3. **Local distribution facilities:** Only NYSEG/RG&E (CRA) estimate per-customer-class costs for secondary lines, transformers, and local primary taps. The NERA-method studies (ConEd, O&R) include secondary cable and transformers as system-level cost centers instead.
4. **Component granularity:** ConEd has the finest breakdown (5 cost centers); NiMo lumps everything into 4 broad asset types with no sub-breakdown of transmission vs sub-transmission; PSEG-LI has only 2 categories (TX, Dist).

---

## 2. System-wide marginal cost tables

### 2A. Undiluted tables (areas with projects only)

These numbers represent the average MC across locations that have planned capital projects. Zero-MC areas are excluded. For use as a system-wide rate, these **overstate** the average cost of serving one incremental kW anywhere on the system.

#### ConEd — Schedule 1 (PDF p. 9)

System-weighted T&D marginal costs per kW of system peak. Excludes zero-MC area substations.

| Year | Transmission | Area Station & Sub-TX | Primary Feeder | Transformer | Secondary Cable |  **Total** |
| ---- | -----------: | --------------------: | -------------: | ----------: | --------------: | ---------: |
| 2025 |        44.94 |                 21.22 |          19.26 |       10.21 |           28.83 | **124.47** |
| 2026 |        73.24 |                 71.31 |          19.71 |       10.50 |           29.49 | **204.26** |
| 2027 |        95.77 |                155.35 |          20.59 |       10.98 |           30.85 | **313.54** |
| 2028 |       117.25 |                203.33 |          21.89 |       11.72 |           32.86 | **387.05** |
| 2029 |       119.72 |                235.74 |          23.85 |       12.78 |           35.84 | **427.93** |
| 2030 |       122.23 |                269.37 |          26.58 |       14.22 |           39.85 | **472.25** |
| 2031 |       124.81 |                296.32 |          30.22 |       16.15 |           45.48 | **512.96** |
| 2032 |       127.36 |                319.29 |          35.01 |       18.75 |           52.94 | **553.35** |
| 2033 |       130.04 |                333.99 |          41.47 |       22.22 |           62.74 | **590.46** |
| 2034 |       132.78 |                344.74 |          50.17 |       26.90 |           75.89 | **630.48** |

#### O&R — Schedule 1 (PDF p. 10)

Same structure as ConEd. Excludes zero-MC area substations.

| Year | Transmission | Area Station & Sub-TX | Primary Feeder | Secondary Dist. |  **Total** |
| ---- | -----------: | --------------------: | -------------: | --------------: | ---------: |
| 2025 |        10.54 |                 14.98 |           8.47 |            1.72 |  **35.72** |
| 2026 |        13.93 |                 18.97 |           7.91 |            1.77 |  **42.58** |
| 2027 |        14.45 |                 24.79 |          16.23 |            1.85 |  **57.32** |
| 2028 |        15.14 |                 38.77 |          26.61 |            1.97 |  **82.49** |
| 2029 |        16.89 |                 45.59 |          33.86 |            2.15 |  **98.50** |
| 2030 |        18.71 |                 49.00 |          42.35 |            2.39 | **112.46** |
| 2031 |        23.25 |                 55.07 |          43.17 |            2.72 | **124.21** |
| 2032 |        27.97 |                 61.38 |          43.99 |            3.15 | **136.50** |
| 2033 |        28.56 |                 65.30 |          44.84 |            3.73 | **142.44** |
| 2034 |        29.16 |                 66.67 |          45.71 |            4.51 | **146.05** |

#### CenHud — Table 1 (PDF p. 14)

"Marginal Costs for Areas with Projects." Only locations with identified projects.

| Year                | Local Transmission | Substation | Feeder Circuit |
| ------------------- | -----------------: | ---------: | -------------: |
| 2026                |              $0.00 |      $0.00 |         $12.37 |
| 2027                |              $0.00 |      $0.52 |         $12.63 |
| 2028                |              $0.00 |     $12.65 |         $12.63 |
| 2029                |              $0.00 |     $12.65 |         $12.63 |
| 2030                |              $0.00 |     $42.84 |         $12.63 |
| 2031                |              $0.00 |     $99.50 |         $12.63 |
| 2032                |              $0.00 |     $99.50 |         $12.63 |
| 2033                |             $27.92 |    $127.47 |         $12.63 |
| 2034                |             $27.92 |    $127.47 |         $12.63 |
| 2035                |             $27.92 |    $127.47 |         $12.63 |
| **10-yr levelized** |          **$6.56** | **$55.51** |     **$12.60** |

#### NiMo — Exhibit 1, p. 28, line 251

Single system-wide average: **$71,524/MW-yr** ($71.52/kW-yr). Includes only substations with capital projects. Component breakdown (annual ECCR-weighted $/MW): T-Station $16k, T-Line $38k, D-Station $13k, D-Line $15k. Total $83k/MW at in-service year; $71.5k/MW weighted-average discounted to FY2026.

No year-by-year system-wide table is provided in the extractable narrative. The year-by-year values are in Exhibit 1, columns F26–F36, for each of 153 individual assets (PDF pp. 1–28 of Exhibit 1).

#### NYSEG — Tables 1–2 (PDF pp. 16–17)

Table 1: 10-year levelized by division (undiluted). Table 2: year-by-year system-wide averaged across divisions (undiluted — still only areas with projects, weighted by division peak load share).

**NYSEG Table 1 — Divisional 10-year levelized ($/kW-yr):**

| Division      |  Upstream | Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ------------- | --------: | --------: | -------------: | ---------------: | -----------------: |
| Auburn        |     14.83 |     69.34 |          29.57 |           117.15 |             120.15 |
| Binghamton    |     24.98 |     65.58 |          22.33 |           116.54 |             119.53 |
| Brewster      |     27.78 |     11.78 |          10.68 |            52.20 |              53.54 |
| Elmira        |     38.38 |      0.00 |           4.06 |            44.44 |              45.58 |
| Geneva        |     19.76 |     29.33 |          22.77 |            74.20 |              76.10 |
| Hornell       |     61.22 |     40.90 |           7.02 |           113.53 |             116.44 |
| Ithaca        |     53.69 |     24.28 |          10.72 |            92.30 |              94.66 |
| Lancaster     |     31.44 |     24.05 |          11.02 |            69.02 |              70.79 |
| Liberty       |     39.88 |     26.74 |           6.73 |            76.26 |              78.21 |
| Lockport      |      0.00 |      0.00 |          12.74 |            13.02 |              13.35 |
| Mechanicville |     38.02 |      8.93 |           3.36 |            52.53 |              53.88 |
| Oneonta       |     10.68 |      7.81 |           7.86 |            27.29 |              27.99 |
| Plattsburgh   |     41.84 |     22.76 |           5.29 |            72.76 |              74.63 |
| **System**    | **30.22** | **24.68** |      **11.46** |        **68.83** |          **70.60** |

**RG&E Table 5 — Divisional 10-year levelized ($/kW-yr):**

| Division    |  Upstream | Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ----------- | --------: | --------: | -------------: | ---------------: | -----------------: |
| Canandaigua |      7.01 |     13.14 |           2.74 |            23.74 |              24.89 |
| Central     |     47.02 |     33.25 |          20.06 |           104.40 |             109.44 |
| Fillmore    |      0.00 |      5.37 |           0.00 |             5.54 |               5.81 |
| Sodus       |      0.00 |      4.02 |           0.00 |             4.15 |               4.35 |
| **System**  | **41.78** | **30.27** |      **17.81** |        **93.51** |          **98.03** |

#### PSEG-LI

Exhibits not extractable from PDF. The narrative provides ECCRs (TX 4.79%, Dist 5.02%) and O&M loaders (TX 3.4%, Dist 8.9%) but no system-wide MC totals appear in the extractable text. See PDF Exhibit 1 (pp. 6–7).

### 2B. Diluted tables (system-wide including zero-MC areas)

These numbers reflect the true system-wide average: areas with no planned investment are weighted as zero. For use as a uniform system-wide rate, these are more appropriate but also lower.

Only CenHud and NYSEG/RG&E provide diluted tables.

#### CenHud — Table 2 (PDF p. 14)

"System-wide Marginal Costs" — includes areas with no projects (weighted zero).

| Year                | Local Transmission | Substation | Feeder Circuit |  **Total** |
| ------------------- | -----------------: | ---------: | -------------: | ---------: |
| 2026                |              $0.00 |      $0.00 |          $3.03 |  **$3.03** |
| 2027                |              $0.00 |      $0.07 |          $3.10 |  **$3.17** |
| 2028                |              $0.00 |      $1.80 |          $3.10 |  **$4.89** |
| 2029                |              $0.00 |      $1.80 |          $3.10 |  **$4.89** |
| 2030                |              $0.00 |      $6.08 |          $3.10 |  **$9.18** |
| 2031                |              $0.00 |     $14.13 |          $3.10 | **$17.23** |
| 2032                |              $0.00 |     $14.13 |          $3.10 | **$17.23** |
| 2033                |             $11.62 |     $18.10 |          $3.10 | **$32.81** |
| 2034                |             $11.62 |     $18.10 |          $3.10 | **$32.81** |
| 2035                |             $11.62 |     $18.10 |          $3.10 | **$32.81** |
| **10-yr levelized** |          **$2.73** |  **$7.88** |      **$3.09** | **$13.70** |

CenHud's dilution is significant: undiluted total levelized ~$75/kW-yr vs diluted **$13.70/kW-yr** (5.5× ratio). Many of CenHud's 66 substations and 10 TX areas have declining or stable loads with ample headroom.

#### NYSEG — Table 3 (PDF p. 18)

"System-Wide Marginal Costs Adjusted for Areas with No Anticipated Capacity Investment Needs." ~77% of upstream and ~65% of dist substations/feeders have no investment.

| Year          |  Upstream | Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ------------- | --------: | --------: | -------------: | ---------------: | -----------------: |
| 2026          |     $0.00 |     $1.37 |          $0.00 |            $1.41 |          **$1.45** |
| 2027          |     $0.10 |     $4.37 |          $0.00 |            $4.60 |          **$4.72** |
| 2028          |     $1.02 |     $5.47 |          $2.04 |            $8.78 |          **$9.01** |
| 2029          |     $4.30 |     $1.64 |          $2.83 |            $9.10 |          **$9.33** |
| 2030          |    $14.85 |     $8.51 |          $5.28 |           $29.74 |         **$30.50** |
| 2031          |    $10.14 |    $12.34 |          $1.66 |           $25.04 |         **$25.68** |
| 2032          |    $16.58 |    $25.66 |          $4.16 |           $48.07 |         **$49.30** |
| 2033          |    $11.66 |    $23.20 |         $11.64 |           $48.01 |         **$49.24** |
| 2034          |    $19.95 |    $18.34 |          $9.24 |           $49.27 |         **$50.53** |
| 2035          |    $19.43 |     $8.20 |         $10.76 |           $39.83 |         **$40.85** |
| **Levelized** | **$8.46** | **$9.79** |      **$4.07** |       **$23.11** |         **$23.71** |

Dilution ratio: undiluted $70.60 → diluted $23.71 (3.0× ratio).

#### RG&E — Table 7 (PDF p. 22)

Same approach. ~54% of dist substations and ~66% of feeders have no investment.

| Year          |   Upstream |  Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ------------- | ---------: | ---------: | -------------: | ---------------: | -----------------: |
| 2026          |      $0.00 |      $0.00 |          $0.00 |            $0.00 |          **$0.00** |
| 2027          |     $19.92 |     $14.28 |          $0.00 |           $35.75 |         **$37.47** |
| 2028          |     $16.35 |     $16.41 |          $0.00 |           $34.18 |         **$35.83** |
| 2029          |      $0.00 |     $13.93 |         $15.81 |           $30.55 |         **$32.02** |
| 2030          |     $17.33 |     $11.13 |          $7.23 |           $37.16 |         **$38.96** |
| 2031          |     $12.08 |     $15.60 |          $5.72 |           $34.68 |         **$36.35** |
| 2032          |     $19.71 |     $18.47 |         $10.61 |           $50.70 |         **$53.15** |
| 2033          |     $25.98 |     $27.13 |          $9.86 |           $65.48 |         **$68.64** |
| 2034          |     $23.61 |     $26.74 |         $14.06 |           $66.87 |         **$70.10** |
| 2035          |     $16.02 |     $13.88 |         $13.19 |           $44.70 |         **$46.86** |
| **Levelized** | **$14.17** | **$14.77** |      **$6.80** |       **$37.14** |         **$38.94** |

Dilution ratio: undiluted $98.03 → diluted $38.94 (2.5× ratio).

---

## 3. Transmission component: bulk vs local, per utility

| Utility        | Label                     | Voltages covered               | FERC-jurisdictional?                                                         | Includes NYISO bulk TX?                                                             | Evidence                                                                                                                                                       |
| -------------- | ------------------------- | ------------------------------ | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ConEd**      | "Transmission System"     | 138 kV, 345 kV stations        | Yes — ConEd is a NYISO TO                                                    | **Yes** — but project-based; only 5 substations (Brooklyn, Queens) have non-zero MC | Schedule 4: Atlantic, Gateway Park, Hillside, Idlewild, Nevins Street. Bronx, Westchester, Manhattan, SI all zero.                                             |
| **O&R**        | "Transmission System"     | 138 kV                         | Yes — O&R is a NYISO TO                                                      | **Yes** — but only 2 substations (West Nyack, Viola Road)                           | Schedule 4; all other substations "–".                                                                                                                         |
| **CenHud**     | "Local Transmission"      | 69 kV, 115/69 kV               | Ambiguous — 115 kV may be FERC-jurisdictional, 69 kV is state-jurisdictional | **No** — explicitly labeled "local"                                                 | 10 local TX areas named (Northwest 115/69, Northwest 69, RD-RJ Lines, etc.). No reference to FERC or bulk TX.                                                  |
| **NiMo**       | T-Station + T-Line        | ≥ 69 kV (explicitly defined)   | Yes — NiMo is a major NYISO TO                                               | **Yes** — bulk TX is dominant (~65% of total MC)                                    | 49 "Transm Net" entries, some massive (3,202 MW / $3.2B, 929 MW / $929M). These are NYISO-scale projects. No separation of bulk vs local TX.                   |
| **NYSEG/RG&E** | "Upstream" (sub + feeder) | 115 kV, 46 kV, 34.5 kV         | Mixed — 115 kV may be FERC-jurisdictional; lower is state                    | **No** — explicitly excluded                                                        | CRA footnote 4: "to differentiate them from the more regional transmission grid costs that are recoverable through NYISO Transmission Service Charges (TSCs)." |
| **PSEG-LI**    | T-Station + T-Line        | Not stated in extractable text | LIPA has unique FERC relationship                                            | **Unclear** — exhibits not extractable                                              | Study framed as "location-specific avoided distribution costs." Modest $600M portfolio.                                                                        |

---

## 4. Component taxonomy: first principles and cross-utility mapping

### 4A. The real-world physical components, bottom to top

The delivery system has a clear physical hierarchy. From the customer meter upward:

| Level                           | Physical components                                                                                               | Function                                                                                    | Typical voltages                                                                     |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **A. Secondary**                | Secondary lines (overhead/underground conductors from transformer to meter); service drops                        | Final delivery to customer premises                                                         | 120/240V (residential), 120/208V, 277/480V (commercial)                              |
| **B. Distribution transformer** | Pole-top or pad-mount transformers; network transformers in urban areas                                           | Step down from primary voltage to secondary/service voltage                                 | Primary side: 4–35 kV; secondary side: 120–480V                                      |
| **C. Primary feeder**           | Trunk-line primary conductors (overhead or underground cable) from substation to transformers and load tap points | Distribute power from substation to transformers across the service area                    | 4 kV, 12.47 kV, 13.2 kV, 27 kV, 33 kV (varies by utility)                            |
| **D. Distribution substation**  | Substation transformers (step down from sub-TX/TX voltage to primary); buses, breakers, switches                  | Convert sub-transmission voltage to primary distribution voltage                            | High side: 34.5–138 kV; low side: 4–35 kV                                            |
| **E. Sub-transmission**         | Sub-transmission lines and cables (overhead/underground); sub-transmission switches, regulators                   | Move power from bulk TX delivery points to distribution substations across the service area | 34.5 kV, 46 kV, 69 kV, 115 kV (varies by utility; can overlap with bulk TX voltages) |
| **F. Bulk system substation**   | High-voltage substations (step down from EHV to sub-TX/area voltage); switchyards; NYISO interconnection points   | Interface between bulk transmission grid and utility sub-transmission                       | High side: 115–345 kV; low side: 34.5–138 kV                                         |
| **G. Bulk transmission lines**  | High-voltage and extra-high-voltage lines (overhead towers, underground cables)                                   | Move power long distances between generation and load centers; NYISO-operated               | 115 kV, 230 kV, 345 kV, 500 kV+                                                      |

The boundary between E (sub-transmission) and F–G (bulk system) is fuzzy and utility-specific. In NYISO, the jurisdictional boundary between FERC-regulated bulk transmission and state-regulated local facilities varies by TO and is defined in each TO's OATT. Generally, facilities at 115 kV+ that are part of the NYISO grid are FERC-jurisdictional. But some 115 kV facilities are "local transmission" in the sense that they serve only a small area (as with CenHud's 115/69 kV areas).

### 4B. Cross-utility mapping table

The table below maps each utility's cost center labels to the physical component levels defined above. Where a cost center spans multiple physical levels, all are listed. "?" means the boundary is ambiguous from the filing.

| Physical level                           | ConEd                                                 | O&R                                                   | CenHud                                                             | NiMo                                         | NYSEG/RG&E                                                             | PSEG-LI                                    |
| ---------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------ | -------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------ |
| **A. Secondary lines**                   | "Secondary Cable"                                     | "Secondary Distribution" (combined with B)            | Not separated (included in "Feeder Circuit" via historical sample) | "D-Line" (combined with C)                   | "Local Distribution Facilities" per customer class — NOT in system MC  | Not separated                              |
| **B. Distribution transformer**          | "Transformer"                                         | "Secondary Distribution" (combined with A)            | Not separated (included in "Feeder Circuit" via historical sample) | "D-Line" (combined with A, C?)               | "Local Distribution Facilities" per customer class — NOT in system MC  | Not separated                              |
| **C. Primary feeder**                    | "Primary Feeder"                                      | "Primary Feeder"                                      | "Feeder Circuit" (includes B?, A?)                                 | "D-Line" (combined with A, B?)               | "Primary Feeder" (12.5 kV, 4.6 kV)                                     | "D-Line" (combined with D?)                |
| **D. Distribution substation**           | "Area Station and Sub-Transmission" (combined with E) | "Area Station and Sub-Transmission" (combined with E) | "Substation"                                                       | "D-Station"                                  | "Dist. Substation" (12.5 kV)                                           | "D-Station"                                |
| **E. Sub-transmission lines/components** | "Area Station and Sub-Transmission" (combined with D) | "Area Station and Sub-Transmission" (combined with D) | "Local Transmission" (69 kV, 115/69 kV)                            | "T-Station" or "T-Line" (combined with F, G) | "Upstream Substation" (115/46/34 kV) + "Upstream Feeder" (115/34.5 kV) | "T-Station" or "T-Line" (combined with F?) |
| **F. Bulk system substation**            | "Transmission System"                                 | "Transmission System"                                 | Not included                                                       | "T-Station" (combined with E)                | **Not included** (explicitly excluded: "NYISO TSCs")                   | "T-Station" (combined with E?)             |
| **G. Bulk transmission lines**           | "Transmission System" (if 345 kV line projects exist) | Not visible in schedules                              | Not included                                                       | "T-Line" (combined with E)                   | **Not included** (explicitly excluded)                                 | "T-Line" (combined with E?)                |

### 4C. Reading the mapping: what's actually comparable?

**Same label, different contents:**

- **"Transmission"** in ConEd/O&R = FERC-jurisdictional TX stations (levels F–G). **"Transmission"** in NiMo = everything ≥ 69 kV (levels E + F + G combined). **"Local Transmission"** in CenHud = local sub-TX (level E only). **"Upstream"** in NYSEG/RG&E = local sub-TX (level E only), explicitly not bulk.
- **"Substation"** in ConEd/O&R ("Area Station and Sub-Transmission") = distribution substations + sub-transmission lines bundled together (levels D + E). In CenHud = distribution substations only (level D). In NYSEG/RG&E = distribution substations only (level D). In NiMo = D-Station (level D only), but sub-TX components are in T-Station/T-Line.

**Same real-world thing, different buckets:**

- **Sub-transmission wires and components (level E):** In ConEd/O&R → "Area Station and Sub-Transmission" (bundled with dist substations). In CenHud → "Local Transmission." In NiMo → "T-Station" / "T-Line" (bundled with bulk TX). In NYSEG/RG&E → "Upstream Substation" + "Upstream Feeder." These all cover level E, but four different labels and three different bundling strategies.
- **Secondary lines and transformers (levels A + B):** ConEd separates them into two cost centers. O&R bundles them into "Secondary Distribution." CenHud apparently rolls them into the "Feeder Circuit" cost center (lower-voltage costs from historical samples). NiMo puts them in "D-Line." NYSEG/RG&E excludes them from the system MC entirely and reports them as per-customer-class "Local Distribution Facilities."

**Which utilities actually include NYISO bulk system costs (levels F–G)?**

| Utility    | Includes levels F–G? | Confidence | Notes                                                                                             |
| ---------- | -------------------- | ---------- | ------------------------------------------------------------------------------------------------- |
| ConEd      | **Yes**              | High       | Separate "Transmission System" cost center. Few projects, but genuine FERC-jurisdictional TX.     |
| O&R        | **Yes**              | High       | Same structure as ConEd. Only 2 substations with TX MC.                                           |
| CenHud     | **No**               | High       | "Local Transmission" is explicitly local (69/115 kV areas).                                       |
| NiMo       | **Yes**              | High       | Massive "Transm Net" projects are NYISO-scale bulk TX. But inseparable from local TX in the data. |
| NYSEG/RG&E | **No**               | Certain    | CRA explicitly states upstream excludes "NYISO Transmission Service Charges."                     |
| PSEG-LI    | **Unclear**          | Low        | Exhibits not extractable. Framed as "avoided distribution costs."                                 |

---

## 5. Reassessment of bulk TX marginal cost gap

The research note "NY bulk transmission marginal cost: how to construct it" was written under the assumption that "The MCOS studies filed under PSC Docket 19-E-0283 by each utility … cover local sub-transmission and distribution. … [They] explicitly exclude bulk NYISO transmission."

**That blanket statement is wrong.** The component scrub reveals a more nuanced picture:

### What actually changed

1. **ConEd's MCOS includes bulk TX MC.** ConEd's "Transmission System" cost center covers FERC-jurisdictional 138/345 kV facilities. Schedule 1 shows system-weighted transmission MC growing from $45/kW (2025) to $133/kW (2034). These are project-based (only 5 substations in Brooklyn and Queens have planned TX projects), but the system average is non-trivial — especially compared to the OATT proxy of ~$120/kW-yr. The Schedule 1 "Transmission Costs" column _is_ a bulk TX marginal cost for ConEd, albeit an undiluted one (areas-with-projects-only).

2. **O&R's MCOS includes bulk TX MC.** Same framework. Smaller: $11–29/kW over 2025–2034. Only two substations have TX projects.

3. **NiMo's MCOS includes bulk TX MC — and it dominates.** T-Station + T-Line accounts for ~65% of NiMo's total $71.5/kW-yr system MC. The "Transm Net" entries include projects at the multi-GW / multi-billion-dollar scale. This is unambiguously NYISO bulk transmission. NiMo's total MC is more "transmission MC with some distribution" than "distribution MC with some transmission."

4. **NYSEG/RG&E confirmed: no bulk TX.** The CRA study explicitly excludes NYISO TSCs. The "upstream" category is local sub-TX only.

5. **CenHud confirmed: no bulk TX.** "Local Transmission" = 69/115 kV area feeders, explicitly local.

6. **PSEG-LI: still unclear.** Exhibits not extractable.

### Implications for the gap analysis

The original note's framing — "Nobody in New York currently publishes a $/kW-year long-run marginal cost of bulk transmission" — is too strong. ConEd, O&R, and NiMo do publish project-based bulk TX MCs in their MCOS filings. But the gap is real and varies by utility:

| Utility | Bulk TX MC in MCOS?                                             | Gap status                                                            |
| ------- | --------------------------------------------------------------- | --------------------------------------------------------------------- |
| ConEd   | Yes — $45–133/kW-yr undiluted (Schedule 1 "Transmission")       | **No gap** for BAT; this is a usable (if undiluted) bulk TX MC signal |
| O&R     | Yes — $11–29/kW-yr undiluted (Schedule 1 "Transmission")        | **No gap**                                                            |
| NiMo    | Yes — embedded in $71.5/kW-yr total; ~$54/MW T-Station + T-Line | **No gap**, but bulk and local TX are inseparable in the data         |
| NYSEG   | No                                                              | **Gap remains** — need OATT proxy or CLCPA project costs              |
| RG&E    | No                                                              | **Gap remains**                                                       |
| CenHud  | No                                                              | **Gap remains**                                                       |
| PSEG-LI | Unclear                                                         | **Likely gap** — treat as gap until exhibits are extracted            |

### Revised recommendation

The original note recommended "Option 1: Use OATT ATRR as upper-bound proxy" uniformly for all utilities. This should be revised:

**For ConEd, O&R, NiMo:** Use the MCOS filing's own transmission component. These are genuine forward-looking, project-based marginal costs — superior to the embedded-cost OATT proxy. For ConEd and O&R, the Schedule 1 "Transmission" column is directly usable. For NiMo, the T-Station + T-Line share of the total MC can be extracted from the Exhibit 1 component breakdown.

The catch: ConEd and O&R's numbers are **undiluted** (only areas with projects). If a diluted system-wide bulk TX MC is needed, apply the same dilution approach as for sub-TX — weight by the fraction of the system requiring TX-level investment. ConEd's Schedule 2 shows TX MC by region, and the system average weights these by region size, but it still excludes zero-TX-MC regions from the averaging. To dilute, you'd need to weight the regional TX MC by the fraction of total system load in TX-project regions (Brooklyn + Queens for ConEd ≈ 35% of system load) and apply zero for the rest.

**For NYSEG, RG&E, CenHud:** The gap is real. Use Option 1 (OATT ATRR proxy) or Option 2 (CLCPA project costs) as originally recommended. The OATT proxy gives:

| TO     | Approx. $/kW-yr (embedded) |
| ------ | -------------------------- |
| NYSEG  | ~$53                       |
| RG&E   | ~$43                       |
| CenHud | ~$55                       |

These are embedded averages, not LRMC. They likely overstate MC for utilities with surplus TX capacity and understate it where the system is constrained.

**For PSEG-LI:** Treat as a gap until the MCOS Exhibit 1 data can be extracted from the PDF. OATT proxy ~$58/kW-yr.

### What didn't change

The hourly allocation guidance remains valid: bulk TX is driven by zonal/statewide coincident peaks, and PoP allocation should use NYISO zonal load shapes. The FERC Order 1920 compliance process (due April 2026) remains the best long-term source for ISO-endorsed LRMC data.

The sensitivity testing recommendation remains critical: run the BAT with and without bulk TX MC to quantify its impact, regardless of which source is used.

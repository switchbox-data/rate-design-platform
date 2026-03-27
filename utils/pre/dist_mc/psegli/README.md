# PSEG-LI MCOS marginal cost analysis

PSEG Long Island (on behalf of LIPA) filed a Marginal Cost of Service study in December 2025 for Docket 19-E-0283. Unlike other NY utilities, PSEG-LI did not publish a machine-readable workbook — only an 11-page PDF with summary exhibits. The project list (Exhibit 2) was manually transcribed from the PDF into a CSV (`psegli_study_project_list.csv` on S3), and the aggregate ECCR+O&M rates come from Exhibit 1.

The study covers 30 projects totaling $598M ($691M with risk/contingency) over FY2025–2032 (8 years).

Key differences from other utilities:

- **No workbook** — project list transcribed from PDF; aggregate MC rates from Exhibit 1
- **8-year study period** (2025–2032), shorter than the 10-year standard
- **2 cost centers** only: Sub-TX (T-Substation, ≤69kV) and Distribution (D-Substation + D-Feeders)
- **No T-Line projects** in the screened portfolio, despite the filing listing T-Line as one of the 4 asset classes
- **Actual project-level ISD timing** from Exhibit 2, not a uniform-entry assumption
- **Separate classification step** — a classification script cross-references each T-Substation project against public voltage data to confirm all are local sub-transmission (≤69kV), not bulk (≥138kV)

## Cost centers

| Cost center  | Asset classes            | BAT tier                | ECCR+O&M rate |
| ------------ | ------------------------ | ----------------------- | :-----------: |
| Sub-TX       | T-Substation (all ≤69kV) | Sub-TX + dist (include) |     8.2%      |
| Distribution | D-Substation, D-Feeders  | Sub-TX + dist (include) |     13.9%     |

**BAT input = Sub-TX + Distribution = Total** (all projects included).

This differs from the initial analysis which excluded all T-Substation projects from BAT. The reclassification is based on voltage evidence showing all T-Substation projects operate at sub-transmission voltages.

## Bulk TX treatment

### Two-step pipeline

Unlike utilities where the MCOS workbook directly labels projects by voltage, PSEG-LI's filing only provides an "asset class" (T-Substation vs D-Substation/D-Feeders) with no voltage information. We therefore split the analysis into two scripts:

1. **`classify_psegli_projects.py`** — reads the raw project list CSV from S3, classifies each project as `sub_tx` or `distribution` using per-project voltage evidence, and outputs `psegli_project_classifications.csv` (checked into the repo).

2. **`analyze_psegli_mcos.py`** — reads the classified CSV, applies Exhibit 1's ECCR+O&M rates, and computes MC variants. Both sub_tx and distribution projects are included in BAT.

This mirrors the NiMo approach (`classify_nimo_projects.py` → `nimo_project_classifications.csv` → `analyze_nimo_mcos.py`).

### LIPA's voltage architecture

LIPA's Transmission Planning Criteria (Rev 1, Dec 2022, Section 5) defines the system's voltage hierarchy:

> "The LIPA transmission system is divided into transmission and sub-transmission systems. The transmission system consists of **138 kV and 345 kV** voltage levels and the sub-transmission system consists of **23, 34.5 kV and 69 kV** voltage levels."

Section 4.1 further clarifies:

> "Application of the BES definition has resulted in the **entire LIPA 138 kV transmission system being classified as BES**."

So on LIPA's system: **138/345 kV = Bulk Electric System** (NYISO-jurisdictional, excluded from BAT), and **≤69 kV = sub-transmission** (local, included in BAT like distribution).

### Per-project voltage evidence

Each of the 15 T-Substation projects was classified using public sources. 12 have high-confidence direct voltage evidence; 3 are classified by systemic inference:

| SOS  | Location        |  MVA | Voltage               | Source                                                       | Confidence |
| ---- | --------------- | ---: | --------------------- | ------------------------------------------------------------ | ---------- |
| 1545 | Hither Hills    |   13 | **23→33 kV**          | LIPA environmental assessment (Apr 2024)                     | High       |
| 2143 | Belmont         |  157 | **33→69 kV**          | psegliny.com/reliability/belmont                             | High       |
| 1940 | Miller Place    |   33 | **≤69 kV**            | psegliny.com/reliability/millerplace (dist feeders/poles)    | High       |
| 1476 | Bridgehampton   |  125 | **69 kV**             | PSEG LI project page; 69/13kV transformer banks              | High       |
| 2588 | Lindbergh       | 46.6 | **69 kV**             | psegliny.com/reliability/lindbergh (69kV underground cables) | High       |
| 1986 | Southampton     |  145 | **69 kV**             | South Fork 69kV network (feeds Bridgehampton, Deerfield)     | High       |
| 1822 | Deerfield (ph1) |  112 | **69 kV (138-rated)** | southampton2deerfield.com, DPS Case 24-T-0113                | High       |
| 2370 | Deerfield (ph2) |  112 | **69 kV (138-rated)** | Same corridor as SOS 1822                                    | High       |
| 2583 | Moriches (ph1)  |   14 | **69 kV**             | NYISO Gold Book Table VII, queue C24-061                     | High       |
| 2358 | Moriches (ph2)  |   67 | **69 kV**             | Same substation as SOS 2583                                  | High       |
| 2512 | Quogue          |   41 | **69 kV**             | NYISO Gold Book Table VII, queue C24-158                     | High       |
| 1102 | Peconic         | 39.5 | **69 kV**             | NYISO Gold Book Table VII, queue C24-023                     | High       |
| 1540 | North Bellmore  |   33 | **≤69 kV**            | No Article VII filing (required for ≥100kV)                  | Medium     |
| 1041 | New South Road  |   33 | **≤69 kV**            | No Article VII filing                                        | Medium     |
| 2330 | Arverne         |   56 | **≤69 kV**            | No Article VII filing; Rockaway peninsula sub-TX             | Medium     |

**Result: 0 of 15 T-Substation projects are bulk transmission (138kV+).** All are sub-TX (≤69kV) and included in BAT.

### The Deerfield edge case

The Southampton-to-Deerfield cable (SOS 1822 and 2370, $81M combined) is physically **rated at 138 kV** and went through Article VII (Case 24-T-0113). However, it will **operate at 69 kV** during the entire MCOS study period (FY2025–2032). It serves South Fork load growth — a local, not bulk, function. We classify it as sub_tx because the operating voltage during the study period is 69 kV = sub-transmission per LIPA's own planning criteria.

### Medium-confidence classifications

Three projects (North Bellmore, New South Road, Arverne) have no direct voltage evidence. They are classified as sub_tx based on:

1. No NY PSL Article VII filing exists (required for ≥100 kV facilities)
2. All 12 confirmed T-Substation projects are 69 kV or below
3. LIPA has no intermediate voltages — projects are either 138/345 kV (BES, with extensive public record) or ≤69 kV (sub-TX)
4. 138 kV projects generate Article VII environmental reviews, NYISO interconnection queue entries, and media coverage — all absent for these projects

## MC formula and variants

Base formula (all variants):

```
Annual RR(Y) = Undiluted MC($/kW-yr) × Capacity(kW)   [per cost center]
MC(Y)        = Σ Annual RR(Y) / Denominator            [$/kW-yr]
```

Where `Undiluted MC = Capital($/kW) × ECCR+O&M rate` from Exhibit 1.

The script produces **four variants** by combining two capital perspectives with two denominators:

| Variant               | Capital(Y)                          | Denominator                  |
| --------------------- | ----------------------------------- | ---------------------------- |
| Cumulative diluted    | Accumulated in-service capital to Y | System peak (MW)             |
| Incremental diluted   | New capital entering service in Y   | System peak (MW)             |
| Cumulative undiluted  | Accumulated in-service capital to Y | Cumulative project MW to Y   |
| Incremental undiluted | New capital entering service in Y   | New project MW entering in Y |

Levelized = mean(real MC) over the 7-year levelization window (2026–2032), consistent across all utilities. See `context/methods/marginal_costs/ny_mcos_studies_comparison.md` §10 for rationale.

### Key output values (incremental diluted, levelized)

| Bucket                          | Levelized MC ($/kW-yr) |
| ------------------------------- | ---------------------: |
| bulk_tx                         |                  $0.00 |
| **sub_tx_and_dist (BAT input)** |              **$1.16** |

## Study parameters

| Parameter    | Value     | Source                                                  |
| ------------ | --------- | ------------------------------------------------------- |
| Study period | 2025–2032 | 8 years (shorter than 10-year standard)                 |
| System peak  | 4,935 MW  | LIPA 2024 actual NCP, NYISO Gold Book Zone K Table I-4a |
| Escalation   | 2.1%/yr   | Applied externally for cross-utility consistency        |

### ECCR+O&M rates (Exhibit 1)

| Parameter             | Sub-TX (T-Station) | Distribution | Source                 |
| --------------------- | -----------------: | -----------: | ---------------------- |
| Capital cost ($/kW)   |            $563.17 |      $721.12 | Exhibit 1, columns 4–5 |
| ECCR                  |              4.79% |        5.02% | Exhibit 3              |
| O&M (incl. A&G, WC)   |              3.40% |        8.90% | Exhibit 3-OM           |
| **Combined ECCR+O&M** |           **8.2%** |    **13.9%** | Exhibit 1, columns 6–7 |
| **Undiluted MC**      |         **$46.18** |  **$100.24** | = capital × ECCR+O&M   |

## Per-project data

### Capacity entry timing

| Year      | Sub-TX (MW) | Dist (MW) |  Total (MW) | Notes                                             |
| --------- | ----------: | --------: | ----------: | ------------------------------------------------- |
| 2025      |       328.0 |     114.0 |       442.0 | Includes 3 projects with 2024 ISDs (37.5 MW Dist) |
| 2026      |       178.0 |      10.0 |       188.0 |                                                   |
| 2027      |       126.0 |      10.0 |       136.0 |                                                   |
| 2028      |        79.6 |      20.0 |        99.6 |                                                   |
| 2029      |        80.5 |      29.0 |       109.5 |                                                   |
| 2030      |        56.0 |       0.0 |        56.0 | Sub-TX only                                       |
| 2031      |        67.0 |       0.0 |        67.0 | Sub-TX only                                       |
| 2032      |       112.0 |       0.0 |       112.0 | Sub-TX only                                       |
| **Total** | **1,027.1** | **183.0** | **1,210.1** |                                                   |

### Data sources

| Input               | S3 path / source                                                      |
| ------------------- | --------------------------------------------------------------------- |
| Raw project list    | `s3://data.sb/ny_psc/mcos_studies_2025/psegli_study_project_list.csv` |
| Classified projects | `psegli/psegli_project_classifications.csv` (checked into repo)       |
| MC parameters       | MCOS filing Exhibit 1 (PDF p. 6) — hardcoded in analyze script        |

The raw CSV contains all 30 projects from Exhibit 2. The classified CSV adds columns: `classification`, `bat_included`, `voltage_kv`, `confidence`, `inference_method`, `evidence`.

## Worked examples

### Incremental diluted — Distribution, year 2025

In 2025, 114.0 MW of Distribution capacity enters (includes 3 projects clamped from 2024):

```
cap(2025) = 114.0 MW = 114,000 kW
undiluted_mc = $721.12 × 0.139 = $100.236/kW-yr
annual_cost = $100.236 × 114,000 = $11,426,857

real_mc = $11,426,857 / 4,935,000 kW = $2.32/kW-yr
escalation(2025) = 1.0
nominal_mc = $2.32/kW-yr
```

### Incremental diluted — Sub-TX, year 2025

In 2025, 328.0 MW of Sub-TX capacity enters:

```
cap(2025) = 328.0 MW = 328,000 kW
undiluted_mc = $563.17 × 0.082 = $46.180/kW-yr
annual_cost = $46.180 × 328,000 = $15,146,980

real_mc = $15,146,980 / 4,935,000 kW = $3.07/kW-yr
escalation(2025) = 1.0
nominal_mc = $3.07/kW-yr
```

### Incremental diluted — total, year 2025

```
total_real_mc = Sub-TX + Dist = $3.07 + $2.32 = $5.38/kW-yr
```

### Incremental undiluted — Sub-TX, any year

Every year that Sub-TX projects enter, the per-kW cost is the same:

```
undiluted_mc = $563.17 × 0.082 = $46.18/kW-yr  [constant]
nominal_mc(2028) = $46.18 × 1.021^3 = $46.18 × 1.0643 = $49.15/kW-yr
```

## Inputs and outputs

| Input           | Source                                                                                         |
| --------------- | ---------------------------------------------------------------------------------------------- |
| Raw project CSV | `s3://data.sb/ny_psc/mcos_studies_2025/psegli_study_project_list.csv`                          |
| Classified CSV  | `psegli/psegli_project_classifications.csv` (checked into repo)                                |
| MC rates        | PSEG-LI 2025 MCOS filing Exhibit 1 (hardcoded: $563.17/kW Sub-TX, $721.12/kW Dist, 8.2%/13.9%) |
| System peak     | 4,935 MW — LIPA 2024 actual NCP, NYISO Gold Book Zone K Table I-4a                             |

| Output                                                | Description                                                        |
| ----------------------------------------------------- | ------------------------------------------------------------------ |
| `outputs/psegli_cumulative_diluted_levelized.csv`     | Two rows (bulk_tx=0, sub_tx_and_dist): levelized and final-year MC |
| `outputs/psegli_cumulative_diluted_annualized.csv`    | One row per year: bulk_tx (0) and sub_tx_and_dist (nominal/real)   |
| `outputs/psegli_incremental_diluted_levelized.csv`    | Same structure, incremental ÷ system peak                          |
| `outputs/psegli_incremental_diluted_annualized.csv`   | Same structure, incremental ÷ system peak                          |
| `outputs/psegli_cumulative_undiluted_levelized.csv`   | Same structure, cumulative ÷ cumulative capacity                   |
| `outputs/psegli_cumulative_undiluted_annualized.csv`  | Same structure, cumulative ÷ cumulative capacity                   |
| `outputs/psegli_incremental_undiluted_levelized.csv`  | Same structure, incremental ÷ annual capacity                      |
| `outputs/psegli_incremental_undiluted_annualized.csv` | Same structure, incremental ÷ annual capacity                      |

## How to run

```bash
cd utils/pre/dist_mc

# Step 1: classify projects (reads from S3, outputs local CSV)
just classify-psegli

# Step 2: compute MCs (reads local classified CSV)
just analyze-psegli
```

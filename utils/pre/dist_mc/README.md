# MCOS dilution analysis — NiMo, Con Edison, O&R, CenHud, NYSEG, RG&E, PSEG-LI

This directory computes **marginal costs** from each NY utility's MCOS study workbook (or filing), separating bulk transmission (excluded) from sub-transmission and distribution costs (used as BAT inputs).

Each utility has its own subdirectory with analysis scripts, output CSVs, and a README documenting the methodology.

| Utility | Directory            | Methodology                                                                                 |
| ------- | -------------------- | ------------------------------------------------------------------------------------------- |
| NiMo    | [`nimo/`](nimo/)     | Project-level Gold Book classification                                                      |
| ConEd   | [`coned/`](coned/)   | Accept cost center structure                                                                |
| O&R     | [`or/`](or/)         | Split CapEx TX: Gold Book → bulk, non-Gold-Book → local sub-TX                              |
| CenHud  | [`cenhud/`](cenhud/) | All local (no bulk TX); flat nominal costs                                                  |
| NYSEG   | [`nyseg/`](nyseg/)   | CRA W2 project data, NERA-style; no bulk TX; 2035 forecast peak                             |
| RG&E    | [`rge/`](rge/)       | CRA W2 project data, NERA-style; no bulk TX; shares logic with NYSEG                        |
| PSEG-LI | [`psegli/`](psegli/) | Filing data (Exhibit 2 CSV); voltage-classified (all ≤69kV sub-TX); real project ISD timing |

## Running

From this directory:

```bash
just analyze-all      # all seven utilities
just analyze-nimo     # NiMo only
just analyze-coned    # ConEd only
just analyze-or       # O&R only
just analyze-cenhud   # CenHud only
just analyze-nyseg    # NYSEG only
just analyze-rge      # RG&E only
just analyze-psegli   # PSEG-LI only
```

## Common methodology

All seven utilities use a variant of the same base formula:

```
Annual RR(Y) = Capital(Y) × Composite Rate × Escalation(Y)
MC(Y)        = Annual RR(Y) / Denominator   [$/kW-yr]
```

The composite rate comes from the utility's carrying charge schedule (ECCR for NiMo, NERA Schedule 11/10 for ConEd/O&R). Escalation uses the GDP Implicit Price Deflator.

### Four MC variants

All seven utilities produce four variants by combining two capital perspectives with two denominator choices:

| Variant               | Capital(Y)                     | Denominator            | Perspective                        |
| --------------------- | ------------------------------ | ---------------------- | ---------------------------------- |
| Cumulative diluted    | Accumulated capital to year Y  | System peak (MW)       | MCOS cost allocation               |
| Incremental diluted   | New capital entering in year Y | System peak (MW)       | BAT economic cost (cost causation) |
| Cumulative undiluted  | Accumulated capital to year Y  | Cumulative project MW  | Per-project cost recovery          |
| Incremental undiluted | New capital entering in year Y | New capacity in year Y | Per-project marginal cost          |

Each variant is exported in both annualized (year-by-year) and levelized form — 8 CSVs per utility. See `context/domain/ny_mcos_studies_comparison.md` §6 for the rationale behind cumulative vs. incremental and the BAT's preference for incremental.

ConEd/O&R use NERA's composite rate × escalation formula; NiMo uses pre-computed ECCR values per project (see `nimo/README.md` for details). CenHud uses NERA methodology with per-project ECCR rates; its workbook provides flat nominal costs (no built-in escalation), but we apply a 2.1%/yr GDP deflator for consistency with the other utilities (see `cenhud/README.md`). NYSEG and RG&E read project-level data from CRA's W2 sheet and apply NERA-style formulas with composite rates derived from the workbook (see `nyseg/README.md` and `rge/README.md`; `context/domain/ny_mcos_studies_comparison.md` §9). PSEG-LI has no workbook, but the Exhibit 2 project list was transcribed from the filing PDF into a CSV on S3; actual per-project in-service dates drive capacity timing, while Exhibit 1's aggregate ECCR+O&M rates provide the per-kW MC computation (see `psegli/README.md`).

### Harmonized project-level methodology

All seven MCOS utilities now use **in-service-year scoping** with project-level data: each project's capital and capacity enter the MC calculation when the project completes, excluding pre-completion CWIP. NiMo and CenHud have explicit in-service years in the workbook; ConEd and O&R infer in-service year from per-project cumulative cashflow stabilization (O&R Primary uses first-nonzero year); NYSEG and RG&E have explicit ISD in W2; PSEG-LI's ISDs come from the transcribed Exhibit 2 project list. See `context/domain/ny_mcos_studies_comparison.md` §8–§9 for details.

The key difference across utilities is **how we identify which costs are bulk TX vs. sub-TX + distribution** — NiMo requires project-level classification against the Gold Book, ConEd's cost center structure maps cleanly to tiers, O&R requires a partial split of CapEx Transmission (Gold Book projects → bulk, non-Gold-Book 138 kV reconductoring → local sub-TX), CenHud has no bulk TX at all, NYSEG/RG&E explicitly exclude NYISO TSCs (no bulk TX; all cost centers are local sub-TX and distribution), and PSEG-LI uses a separate classification step (`classify_psegli_projects.py`) that cross-references each T-Substation project against public voltage data (PSEG LI reliability pages, LIPA environmental assessments, NYISO interconnection queue) — all 15 T-Substation projects were confirmed as ≤69kV local sub-transmission, so both Sub-TX and Distribution are included in BAT. See each utility's README for details.

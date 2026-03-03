# MCOS dilution analysis — NiMo, Con Edison, O&R, CenHud

This directory computes **marginal costs** from each NY utility's MCOS study workbook, separating bulk transmission (excluded) from sub-transmission and distribution costs (used as BAT inputs).

Each utility has its own subdirectory with analysis scripts, output CSVs, and a README documenting the methodology.

| Utility | Directory            | Methodology                                                    |
| ------- | -------------------- | -------------------------------------------------------------- |
| NiMo    | [`nimo/`](nimo/)     | Project-level Gold Book classification                         |
| ConEd   | [`coned/`](coned/)   | Accept cost center structure                                   |
| O&R     | [`or/`](or/)         | Split CapEx TX: Gold Book → bulk, non-Gold-Book → local sub-TX |
| CenHud  | [`cenhud/`](cenhud/) | All local (no bulk TX); flat nominal costs                     |

## Running

From this directory:

```bash
just analyze-all      # all four utilities
just analyze-nimo     # NiMo only
just analyze-coned    # ConEd only
just analyze-or       # O&R only
just analyze-cenhud   # CenHud only
```

## Common methodology

All four utilities use a variant of the same base formula:

```
Annual RR(Y) = Capital(Y) × Composite Rate × Escalation(Y)
MC(Y)        = Annual RR(Y) / Denominator   [$/kW-yr]
```

The composite rate comes from the utility's carrying charge schedule (ECCR for NiMo, NERA Schedule 11/10 for ConEd/O&R). Escalation uses the GDP Implicit Price Deflator.

### Four MC variants

All four utilities produce four variants by combining two capital perspectives with two denominator choices:

| Variant               | Capital(Y)                     | Denominator            | Perspective                        |
| --------------------- | ------------------------------ | ---------------------- | ---------------------------------- |
| Cumulative diluted    | Accumulated capital to year Y  | System peak (MW)       | MCOS cost allocation               |
| Incremental diluted   | New capital entering in year Y | System peak (MW)       | BAT economic cost (cost causation) |
| Cumulative undiluted  | Accumulated capital to year Y  | Cumulative project MW  | Per-project cost recovery          |
| Incremental undiluted | New capital entering in year Y | New capacity in year Y | Per-project marginal cost          |

Each variant is exported in both annualized (year-by-year) and levelized form — 8 CSVs per utility. See `context/domain/ny_mcos_studies_comparison.md` §6 for the rationale behind cumulative vs. incremental and the BAT's preference for incremental.

ConEd/O&R use NERA's composite rate × escalation formula; NiMo uses pre-computed ECCR values per project (see `nimo/README.md` for details). CenHud uses NERA methodology with per-project ECCR rates; its workbook provides flat nominal costs (no built-in escalation), but we apply a 2.1%/yr GDP deflator for consistency with the other utilities (see `cenhud/README.md`). The key difference across utilities is **how we identify which costs are bulk TX vs. sub-TX + distribution** — NiMo requires project-level classification against the Gold Book, ConEd's cost center structure maps cleanly to tiers, O&R requires a partial split of CapEx Transmission (Gold Book projects → bulk, non-Gold-Book 138 kV reconductoring → local sub-TX), and CenHud has no bulk TX at all (all three cost centers are local). See each utility's README for details.

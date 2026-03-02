# MCOS dilution analysis — NiMo, Con Edison, O&R

This directory computes **diluted marginal costs** from each NY utility's MCOS study workbook, separating bulk transmission (excluded) from sub-transmission and distribution costs (used as BAT inputs). The diluted values feed into the Bill Alignment Test.

Each utility has its own subdirectory with analysis scripts, output CSVs, and a README documenting the methodology.

| Utility | Directory          | Levelized sub-TX + dist     | Methodology                                                    |
| ------- | ------------------ | --------------------------- | -------------------------------------------------------------- |
| NiMo    | [`nimo/`](nimo/)   | ~$111/kW-yr (sub-TX + dist) | Project-level Gold Book classification                         |
| ConEd   | [`coned/`](coned/) | ~$48/kW-yr (sub-TX + dist)  | Accept cost center structure                                   |
| O&R     | [`or/`](or/)       | ~$21/kW-yr (sub-TX + dist)  | Split CapEx TX: Gold Book → bulk, non-Gold-Book → local sub-TX |

## Running

From this directory:

```bash
just analyze-all    # all three utilities
just analyze-nimo   # NiMo only
just analyze-coned  # ConEd only
just analyze-or     # O&R only
```

## Common methodology

All three utilities use a variant of the same dilution formula:

```
Annual RR(Y) = Capital(Y) × Composite Rate × Escalation(Y)
Diluted MC(Y) = Annual RR(Y) / System Peak
```

The composite rate comes from the utility's carrying charge schedule (ECCR for NiMo, NERA Schedule 11/10 for ConEd/O&R). Escalation uses the GDP Implicit Price Deflator. The system peak is a fixed value (actual or near-term forecast) from the MCOS filing.

The key difference across utilities is **how we identify which costs are bulk TX vs. sub-TX + distribution** — NiMo requires project-level classification against the Gold Book, ConEd's cost center structure maps cleanly to tiers, and O&R requires a partial split of CapEx Transmission (Gold Book projects → bulk, non-Gold-Book 138 kV reconductoring → local sub-TX). See each utility's README for details.

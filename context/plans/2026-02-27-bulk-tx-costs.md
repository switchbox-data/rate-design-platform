# Bulk Transmission Marginal Costs for NY — Plan and Analysis

> Tracks work for GH issue #302. Branch: `feature/bulk-tx-costs`.

## Goal

Incorporate bulk transmission marginal costs into the NY rate design platform. Each `gen_capacity_zone` (ROS, LHV, NYC, LI) needs a v_z value ($/kW-yr) representing the forward-looking marginal cost of bulk transmission infrastructure.

## Status

| Task                          | Status     | Notes                                                              |
| ----------------------------- | ---------- | ------------------------------------------------------------------ |
| Branch + draft PR             | ✅ Done    | `feature/bulk-tx-costs`, PR closes #302                            |
| Raw project CSV               | ✅ Done    | `data/nyiso/transmission/csv/ny_bulk_tx_projects.csv`              |
| `derive_tx_values.py`         | ✅ Done    | Isotonic + quantile derivation for LHV, NYC, LI; NiMo MCOS for ROS |
| `tx_locality` in zone mapping | ✅ Done    | Added to `generate_zone_mapping_csv.py`                            |
| Context doc                   | ✅ Done    | `context/tools/ny_bulk_tx_marginal_costs.md`                       |
| Hourly allocation script      | ✅ Done    | `utils/pre/generate_bulk_tx_mc.py` (SCR top-40 seasonal peaks)     |
| CAIRO integration             | ✅ Done    | Load bulk Tx MC + merge with dist+sub-tx delivery MC in CAIRO run  |

## Derivation pipeline

```text
ny_bulk_tx_projects.csv (raw NYISO study data)
        │
        ▼
derive_tx_values.py (Steps 1–4)
        │
        ├── Step 1: Discrete v = B / (ΔMW × 1000) per project
        ├── Step 2: P25/P50/P75 per (locality, scenario_family)
        ├── Step 3: Isotonic regression B = g(ΔMW) per group
        └── Step 4: Aggregate to gen_capacity_zone
        │
        ▼
ny_bulk_tx_values.csv → s3://data.sb/nyiso/bulk_tx/
        │
        ▼
generate_bulk_tx_mc.py (future: hourly allocation)
        │
        ▼
s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/

## Refactor note (naming)

Completed naming refactor to use **`bulk_tx`** (instead of `transmission` / `sub_tx`) for the
separate hourly bulk transmission MC trace and S3 output paths. Delivery-side marginal costs
loaded from `mc_total_per_kwh` remain keyed as `path_distribution_marginal_costs` in scenario
YAMLs for compatibility, but represent **dist + sub-tx** (upstream + distribution).
```

## Key design decision: ROS value

### Problem

The NYISO AC Transmission study does not provide a usable upstate bulk Tx MC:

- A–F locality has **negative** benefit (procurement cost increase).
- Scaling NYCA system benefit by upstate load share gives ~$10/kW-yr.
- This $10 measures **incremental interface benefit**, not **infrastructure cost**.

### Analysis

We investigated what the ~$10/kW-yr value actually represents:

1. **NYISO studies measure interface congestion relief** — the benefit of adding MW of transfer capability at specific NYISO interfaces (e.g., UPNY-ConEd, Central-East).
2. **For downstate zones** (LHV, NYC, LI), interface benefit ≈ infrastructure MC because serving more load requires expanding the interfaces that are already binding constraints.
3. **For upstate** (ROS), the story is different:
   - Upstate is a net generator/exporter.
   - Additional upstate generation displaces downstate generation, so A–F benefit is negative.
   - Upstate bulk Tx investment is driven by internal load growth and reliability, not interface flows.
   - The NYCA system benefit (~$43/kW-yr) accrues to downstate load behind constrained interfaces.

### NiMo MCOS analysis

We analyzed NiMo's 2025 MCOS (`context/papers/mcos/nimo_2025_mcos.md`) to understand the T-Station + T-Line composition:

| Component   | System capital | ECCR   | MC ($/kW-yr) | What it covers                                        |
| ----------- | -------------- | ------ | ------------ | ----------------------------------------------------- |
| T-Station   | $2,316M        | 8.21%  | $16          | HV substation equipment (≥69 kV)                      |
| T-Line      | $5,259M        | 8.44%  | $38          | Inter-substation transmission lines, network upgrades |
| **T total** | **$7,576M**    | —      | **$54**      | **All bulk transmission**                             |
| D-Station   | $1,895M        | 8.06%  | $13          | MV substation equipment                               |
| D-Line      | $1,233M        | 14.13% | $15          | Distribution feeders                                  |

Key findings:

- **T-Station + T-Line = $54/kW-yr** is a forward-looking LRMC across 11,533 MW of capacity additions (FY2026–2036).
- **Transmission assets operate at ≥69 kV** — this is genuinely "bulk" transmission.
- The T-Line component ($38/kW-yr, 70% of T total) represents inter-substation lines — the core bulk Tx network.
- **ECCR includes** return on capital, depreciation, O&M, insurance, and property tax.
- **Inflation-adjusted** to FY2026 base year using 2.1% Blue Chip consensus forecast.

### OATT cross-validation

OATT (Open Access Transmission Tariff) ATRR provides an embedded-cost cross-check:

| Utility  | OATT proxy ($/kW-yr) | Type                 |
| -------- | -------------------- | -------------------- |
| NYSEG    | ~$53                 | Embedded cost        |
| RG&E     | ~$43                 | Embedded cost        |
| CenHud   | ~$55                 | Embedded cost        |
| NiMo T+T | $54                  | Forward-looking LRMC |

All upstate utilities converge on **$43–55/kW-yr**. NiMo's $54 falls squarely in the center of this range. The convergence between embedded cost (OATT) and forward-looking LRMC (MCOS) gives high confidence.

### Decision

**Use NiMo T+T = $54/kW-yr as the ROS bulk Tx MC.** Sensitivity range: $43–57/kW-yr.

This is implemented in `derive_tx_values.py` in the `aggregate_to_zones` function — ROS uses hardcoded NiMo MCOS values rather than the NYISO study-derived interface benefit.

## Final v_z values ($/kW-yr)

| Zone | v_low | v_mid | v_high | Source                                |
| ---- | ----- | ----- | ------ | ------------------------------------- |
| ROS  | 43.20 | 54.00 | 56.70  | NiMo MCOS T+T                         |
| LHV  | ~35   | ~43   | ~50    | NYISO AC Primary + Addendum Optimizer |
| NYC  | ~48   | ~55   | ~62    | NYISO MMU (UPNY-ConEd)                |
| LI   | ~30   | ~36   | ~42    | NYISO LI Export Policy                |

## Next steps

1. **Hourly allocation script** (`generate_bulk_tx_mc.py`): allocate v_z to 8760 hourly trace using SCR top-40-per-season peak hours.
2. **CAIRO integration**: include bulk Tx hourly trace in `bulk_marginal_costs` alongside energy and generation capacity.
3. **End-to-end validation**: run BAT with bulk Tx included and verify cross-subsidization results.

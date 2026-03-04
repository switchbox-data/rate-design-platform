# PSEG-LI revenue requirement estimation

## Why PSEG-LI needs a different approach

Most NY utilities have delivery revenue requirements set by PSC rate cases.
PSEG-LI is different: LIPA is a public authority that sets its own rates
through an annual budget process, not through PSC filings. There is no
rate-case delivery revenue requirement to look up.

Additionally, LIPA's residential tariff (Rate Code 180) uses Time-of-Use rates
with four bands (Summer Peak, Summer Off-Peak, Winter Peak, Winter Off-Peak)
for both delivery and supply. The standard `compute_rr.py` top-up logic
multiplies each band's rate by total residential kWh independently, which
overcounts by 4x since the bands are mutually exclusive time periods, not
additive charges.

## Bill-proportional method

We estimate PSEG-LI's revenue requirements using LIPA's published budget,
specifically the "Average Residential Monthly Bill Impact" table from the
annual budget fact sheet.

### Source

LIPA's "Fact Sheet: LIPA 2026 Budget as Compared to 2025"
(extracted to `context/papers/lipa_2025_2026_budget_one_pager.md`)

### Bill component breakdown (2025 Budget)

| Component                    | Monthly ($) | Share of bill |
| ---------------------------- | ----------: | ------------: |
| Delivery & System            |      $96.55 |        49.77% |
| Power Supply                 |      $85.20 |        43.92% |
| Distributed Energy Resources |       $4.04 |         2.08% |
| Taxes, PILOTs, Assessments   |       $8.39 |         4.33% |
| Merchant Function Charge     |       $1.36 |         0.70% |
| Revenue Credit               |     ($1.56) |       (0.80%) |
| **Total**                    | **$193.98** |      **100%** |

### Formula

```
delivery_rr = (delivery_system / total_bill) x EIA_residential_sales_revenue
            = (96.55 / 193.98) x EIA_residential_sales_revenue

supply_base = (power_supply / total_bill) x EIA_residential_sales_revenue
            = (85.20 / 193.98) x EIA_residential_sales_revenue
```

EIA-861 `residential_sales_revenue` is total revenue from residential
customers across all charge components. Multiplying by the delivery share
gives the delivery portion; same for supply.

### What each component maps to

| Bill component             | Pipeline treatment                                                   |
| -------------------------- | -------------------------------------------------------------------- |
| Delivery & System          | `delivery_rev_requirements_from_rate_cases.yaml` (budget-estimated)  |
| Power Supply               | `supply_base_overrides.yaml` (budget-estimated)                      |
| DER                        | `add_to_drr` top-up (flat $/kWh, works correctly in `compute_rr.py`) |
| Taxes, PILOTs, Assessments | `exclude` (pass-throughs: Shoreham, NYSA, PILOTs)                    |
| MFC                        | `add_to_srr` top-up (flat $/kWh, works correctly)                    |
| Revenue Credit             | `exclude` (reconciliation: RDM, DSA)                                 |

## Implementation

Script: `utils/pre/rev_requirement/estimate_psegli_rr.py`

The script:

1. Parses the budget one-pager markdown for the bill component split
2. Reads EIA-861 residential sales revenue for PSEG-LI
3. Writes the delivery RR to `delivery_rev_requirements_from_rate_cases.yaml`
4. Writes the supply base to `supply_base_overrides.yaml`

Justfile recipe: `estimate-psegli-rr` (standalone, not in `all-pre`; only
needs to run when LIPA budget data changes).

## Limitations

- The bill component breakdown is for a "typical" residential customer (722
  kWh/mo). If the actual residential load distribution differs significantly
  from typical, the proportional split may not perfectly match aggregate
  revenue. In practice, the LIPA budget's own revenue figures ($2.3B
  residential net) are consistent with this approach.

- The EIA-861 `residential_sales_revenue` includes all charges (delivery,
  supply, taxes, surcharges). The proportional method assumes the bill
  breakdown is representative of the aggregate, which holds when the rate
  structure is uniform across residential customers (as it is for LIPA).

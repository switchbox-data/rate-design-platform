# Revenue requirement pipeline

Scripts for estimating delivery and supply revenue requirements from tariff data,
EIA-861 utility statistics, and (for PSEG-LI) LIPA budget documents.

## Overview

The revenue requirement pipeline computes how much revenue each utility needs to
collect from residential customers for delivery and supply. The result feeds
CAIRO's rate calibration: the delivery revenue requirement (DRR) sets the target
that CAIRO's flat delivery rate must recover.

### Base + top-ups model

For most utilities, the DRR comes from two sources:

1. **Rate-case base** -- the delivery revenue requirement filed in PSC rate cases,
   stored in `delivery_rev_requirements_from_rate_cases.yaml`.
2. **Top-ups** -- surcharges not covered by the rate-case filing (e.g. SBC, CES
   Delivery, DLM, DER). Each surcharge's monthly $/kWh rate is fetched from
   Genability, averaged over the year, and multiplied by EIA-861 residential kWh
   to get an annual budget.

Supply revenue works the same way but has no rate-case base -- it's purely the
sum of supply-side top-ups (commodity charges, MFC, securitization, etc.).

### How `charge_unit` drives calculation

Each charge in `charge_decisions.json` carries a `charge_unit`:

| `charge_unit` | Revenue calculation                                                                                                |
| ------------- | ------------------------------------------------------------------------------------------------------------------ |
| `$/kWh`       | Day-weighted average rate x total residential kWh (from EIA-861)                                                   |
| `$/day`       | Rate x days-in-month x residential customer count (from EIA-861)                                                   |
| `$/month`     | Rate x residential customer count                                                                                  |
| `%`           | Skipped by `compute_rr.py` (percentage-of-bill charges; handled via CAIRO's `utility_tax_rate` or post-processing) |

## Scripts

### `fetch_electric_tariffs_genability.py`

Downloads full Genability/Arcadia tariff JSON snapshots for each utility. Output
goes to `top-ups/default_tariffs/` under the state config directory.

### `fetch_monthly_rates.py`

Reads `charge_decisions.json`, then hits the Genability API once per month to
resolve the effective rate for each classified charge. Output is a
`<utility>_monthly_rates_<year>.yaml` with per-charge monthly rates and the
`decision` field carried forward from charge decisions.

### `compute_rr.py`

Reads the monthly rates YAML, filters for `add_to_drr` and `add_to_srr`
charges, computes annual budgets per charge, and produces the final
`rev_requirement/<utility>.yaml` with:

- `delivery_revenue_requirement_from_rate_case` (from YAML input)
- `delivery_revenue_requirement_topups` (sum of `add_to_drr` charge budgets)
- `supply_revenue_requirement_topups` (sum of `add_to_srr` charge budgets + optional supply base override)
- `total_delivery_revenue_requirement` and `total_delivery_and_supply_revenue_requirement`
- Per-charge detail in `delivery_top_ups` and `supply_top_ups`

**Optional: `--path-supply-base`** -- a YAML mapping utility keys to a supply
base dollar amount. If the current utility has an entry, the amount is added as
a `supply_base_from_budget` entry inside `supply_top_ups`, keeping the output
shape consistent with other utilities. This is used for PSEG-LI, where TOU
supply rates can't be summed directly (see below).

### `estimate_psegli_rr.py`

Estimates PSEG-LI's delivery and supply revenue requirements from LIPA's
published budget data. See [PSEG-LI special case](#pseg-li-special-case) below.

## Pipeline flow

```
1. fetch-genability-tariffs    -> top-ups/default_tariffs/*.json
2. classify charges (manual)   -> top-ups/charge_decisions/*_charge_decisions.json
3. fetch-monthly-rates         -> top-ups/monthly_rates/*_monthly_rates_*.yaml
4. estimate-psegli-rr          -> delivery_rev_requirements_from_rate_cases.yaml (psegli entry)
   (PSEG-LI only)                supply_base_overrides.yaml (psegli entry)
5. compute-rr                  -> rev_requirement/<utility>.yaml
```

Steps 1-3 are standard for all utilities. Step 4 is PSEG-LI only. Step 5
runs for every utility.

## PSEG-LI special case

PSEG-LI (operated by PSEG Long Island on behalf of LIPA) does not participate
in traditional PSC rate cases. Additionally, LIPA's residential tariff uses
Time-of-Use (TOU) rates for both delivery and supply, with four rate bands
(Summer Peak, Summer Off-Peak, Winter Peak, Winter Off-Peak). The standard
top-up logic in `compute_rr.py` multiplies each band's rate by total
residential kWh independently, which overcounts by 4x.

### Bill-proportional method

Instead, `estimate_psegli_rr.py` uses LIPA's published budget data:

**Source**: `context/sources/lipa_2025_2026_budget_one_pager.md`
(extracted from LIPA's "Fact Sheet: LIPA 2026 Budget as Compared to 2025")

The "Average Residential Monthly Bill Impact" table breaks down a typical
residential bill by component. Each component's share of the total bill,
applied to EIA-861 residential sales revenue, estimates the revenue collected
by that component:

```
delivery_rr = (Delivery & System / Total Bill) x EIA residential sales revenue
supply_base = (Power Supply / Total Bill) x EIA residential sales revenue
```

For 2025:

- Delivery & System = $96.55/mo (49.77% of $193.98 total)
- Power Supply = $85.20/mo (43.92% of $193.98 total)

### Charge classification

With the budget-derived delivery RR, all base delivery charges (Customer Charge

- 4 TOU Energy Charges) are classified as `already_in_drr`. The only remaining
  delivery top-up is VDER/DER Cost Recovery, which LIPA bills separately as "DER"
  ($4.04/mo).

Similarly, the 8 TOU supply commodity entries (4 base + 4 rider) are classified
as `exclude`, replaced by the `supply_base_overrides.yaml` entry. MFC and
Securitization remain as `add_to_srr` top-ups (they're flat rates and work
correctly).

Tax/pass-through charges (Shoreham, NY State Assessment, PILOTs) are `exclude`
-- they appear in the "Taxes, PILOTs, Assessments" line of the LIPA bill, not
in "Delivery & System" or "Power Supply".

## Adding a new utility or state

1. Add the utility to `utils/utility_codes.py` with its EIA utility ID.
2. Add an entry to `tariffs_by_utility.yaml` in the state's `top-ups/` directory.
3. Fetch the base tariff: `fetch-genability-tariffs`.
4. Classify every charge (manual research). Document in
   `context/domain/<state>_residential_charges_in_bat.md`.
5. Fetch monthly rates: `fetch-monthly-rates`.
6. Add a rate-case DRR entry in `delivery_rev_requirements_from_rate_cases.yaml`.
7. Run `compute-rr` to produce `rev_requirement/<utility>.yaml`.

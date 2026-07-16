# Revenue requirement pipeline

Scripts for estimating delivery and supply revenue requirements from tariff data,
EIA-861 utility statistics, rate-case testimony, and ResStock load simulations.

## Overview

The revenue requirement pipeline computes how much revenue each utility needs to
collect from residential customers for delivery and supply. The result feeds
CAIRO's rate calibration: the delivery revenue requirement (DRR) sets the target
that CAIRO's flat delivery rate must recover.

### Base + top-ups model

For all utilities, the DRR comes from two sources:

1. **Rate-case base** -- the delivery revenue requirement from PSC/PUC rate case
   filings, either in `delivery_rev_requirements_from_rate_cases.yaml` (Track 1)
   or the hand-seeded partial `*_rate_case_test_year.yaml` (Track 2).
2. **Top-ups** -- surcharges not covered by the rate-case filing (e.g. SBC, CES
   Delivery, Empower Maryland, transmission adjustments). Each surcharge's
   monthly $/kWh rate is fetched from Genability, and its annual budget is
   computed from those rates multiplied by a kWh or customer-count determinant.

Supply revenue works the same way but has no rate-case base -- it's purely the
sum of supply-side top-ups (commodity charges, MFC, securitization, etc.).

### How `charge_unit` drives calculation

Each charge in the monthly rates YAML carries a `charge_unit`:

| `charge_unit` | Revenue calculation                                                                                                |
| ------------- | ------------------------------------------------------------------------------------------------------------------ |
| `$/kWh`       | Day-weighted average rate x total residential kWh                                                                  |
| `$/day`       | Rate x days-in-month x residential customer count                                                                  |
| `$/month`     | Rate x residential customer count                                                                                  |
| `%`           | Skipped by `compute_rr.py` (percentage-of-bill charges; handled via CAIRO's `utility_tax_rate` or post-processing) |

## Track 1 vs Track 2

The pipeline has two tracks that share the same upstream steps (fetch tariff,
classify charges, fetch monthly rates) but differ in where the kWh and customer
count **determinants** come from and how the final YAML is produced.

### Track 1: EIA-derived (`compute_rr.py`)

Used for **NY utilities** and any utility where EIA-861 is the best available
source for total residential kWh and customer count.

- **Determinants**: EIA-861 residential sales kWh and customer count (with
  year-fallback logic).
- **Pipeline**: `compute-rr` recipe reads the monthly rates YAML, pulls EIA
  determinants from S3, computes budgets, and writes `rev_requirement/<utility>.yaml`.
- **Output**: `<utility>.yaml` (e.g. `coned.yaml`, `nimo.yaml`).

The scenario YAML points to this file via `utility_revenue_requirement`.

### Track 2: Testimony-derived (`build_rate_case_test_year.py`)

Used for **RI** and **MD (BGE)** -- utilities where rate-case filings provide
test-year kWh and customer-count determinants that supersede EIA estimates.

- **Determinants**: Test-year residential kWh and customer counts from rate-case
  testimony, entered in a hand-seeded partial YAML.
- **Pipeline**: `build-rate-case-test-year` recipe reads the partial YAML plus
  the monthly rates YAML, aggregates ResStock loads to compute the scaling block,
  and writes the completed `*_rate_case_test_year.yaml`.
- **Output**: `<utility>_rate_case_test_year.yaml` (e.g. `bge_rate_case_test_year.yaml`).

Track 2 also produces a `resstock_kwh_scale_factor` that CAIRO uses to scale
ResStock building loads so the simulation's total residential kWh matches the
testimony's test-year kWh. See [ResStock kWh scaling](#resstock-kwh-scaling)
below.

### When to use which track

| Criterion               | Track 1                       | Track 2                            |
| ----------------------- | ----------------------------- | ---------------------------------- |
| Best kWh source         | EIA-861                       | Rate-case testimony                |
| Customer count source   | EIA-861                       | Testimony (annual bills / 12)      |
| ResStock scaling        | No `kwh_scale_factor`         | Yes, testimony-to-ResStock scaling |
| Customer count override | No `test_year_customer_count` | Yes                                |
| Script                  | `compute_rr.py`               | `build_rate_case_test_year.py`     |

In general, use Track 2 when the rate case provides granular test-year
determinants (kWh by schedule, annual bills by schedule) and you want CAIRO's
simulation to match those testimony totals exactly.

## How CAIRO consumes the output

Both tracks produce a YAML that `_parse_utility_revenue_requirement` in
`utils/scenario_config.py` reads. CAIRO extracts:

- `total_delivery_revenue_requirement` or
  `total_delivery_and_supply_revenue_requirement` (depending on
  `run_includes_supply`): the calibration target.
- `test_year_customer_count` → `customer_count_override`: replaces EIA-861
  customer count for building weight scaling.
- `resstock_kwh_scale_factor` → `kwh_scale_factor`: multiplied into
  `raw_load_elec` before CAIRO bills (see `run_scenario.py:695-700`).
- `subclass_revenue_requirements`: per-tariff-key RR (for differentiated runs).

### The `electricity_net` floor and `grid_cons`

ResStock's `electricity_net` column (`out.electricity.net.energy_consumption`)
can be **negative** during hours when rooftop PV exports exceed on-site
consumption. CAIRO, however, bills on non-negative grid consumption:

```
grid_cons = max(total_load - abs(pv), 0)
```

This formula is defined in `utils/loads.py` as `grid_consumption_expr` and uses
`out.electricity.total.energy_consumption` and
`out.electricity.pv.energy_consumption`.

**The floor patch in `run_scenario.py:702-713`** clips `electricity_net` to ≥ 0
before CAIRO sees the loads, so `electricity_net == grid_cons` throughout the
rest of the pipeline. The `kwh_scale_factor` is applied **before** the floor.

This has two consequences for revenue requirement computation:

1. `resstock_total_residential_kwh` in Track 2 must be computed using the same
   `grid_cons` formula so the `kwh_scale_factor` aligns with what CAIRO actually
   bills against. Using raw `electricity_net` (with negatives) or
   `out.electricity.total` (without PV subtraction) would produce a slightly
   different total and a mismatched scale factor.

2. `build_rate_case_test_year.py` uses `grid_consumption_expr` from
   `utils/loads.py` for this aggregation. `compute_rr.py` (Track 1) currently
   uses `out.electricity.total.energy_consumption` without PV subtraction --
   this is a known gap for utilities with significant residential PV.

### ResStock kWh scaling

Track 2 computes a two-stage scaling that maps ResStock's simulated kWh to the
testimony's test-year kWh:

```
resstock_customer_scale_factor = test_year_customer_count / resstock_customer_count
resstock_total_kwh_scaled_customer = resstock_total_kwh * customer_scale_factor
resstock_kwh_scale_factor = test_year_kwh / resstock_total_kwh_scaled_customer
```

The `resstock_kwh_scale_factor` is written to the output YAML and consumed by
`run_scenario.py` to scale all building loads before simulation.

The monthly scaled kWh (`resstock_monthly_kwh_scaled_customer_kwh`) is used to
compute the `supply_commodity_bundled` budget, which captures the covariance
between seasonal supply rates and seasonal load variation.

## Shared pipeline steps

Steps 1-3 are common to both tracks. They fetch and classify tariff data and
produce the monthly rates YAML that both `compute_rr.py` and
`build_rate_case_test_year.py` consume.

### 1. Fetch the base tariff snapshot

`utils/pre/rev_requirement/fetch_electric_tariffs_genability.py` downloads the
full Genability/Arcadia tariff JSON for each utility. Output goes to
`top-ups/default_tariffs/`.

```bash
UTILITY=bge just s md fetch-genability-tariffs
```

### 2. Classify each charge (manual research)

Each tariff line item is classified by whether it belongs in the delivery revenue
requirement, supply revenue requirement, or should be excluded. The result is
`<utility>_charge_decisions.json` in `top-ups/charge_decisions/`.

Decisions:

- **`add_to_drr`** -- delivery surcharge to be topped up
- **`add_to_srr`** -- supply-side surcharge
- **`already_in_drr`** -- base delivery rate already in the rate-case DRR
- **`exclude_*`** -- true-ups, taxes, eligibility-restricted, or redundant charges

### 3. Fetch actual monthly rates

`utils/pre/rev_requirement/fetch_monthly_rates.py` reads the charge decisions,
hits the Genability API once per month, and writes per-charge monthly rates.

```bash
UTILITY=bge just s md fetch-monthly-rates
```

Output: `top-ups/monthly_rates/<utility>_monthly_rates_<year>.yaml`.

### 4a. Track 1: Compute topped-up revenue requirement

```bash
UTILITY=coned just s ny compute-rr
```

Reads the monthly rates YAML plus EIA-861 data and writes
`rev_requirement/<utility>.yaml`.

### 4b. Track 2: Complete testimony-based revenue requirement

```bash
UTILITY=bge just s md build-rate-case-test-year
```

Reads the partial `*_rate_case_test_year.yaml`, the monthly rates YAML, and
ResStock monthly loads. Writes the completed
`rev_requirement/<utility>_rate_case_test_year.yaml` with top-ups, scaling
block, and summed totals.

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

**Track 1.** Reads the monthly rates YAML, filters for `add_to_drr` and
`add_to_srr` charges, computes annual budgets per charge, and produces the final
`rev_requirement/<utility>.yaml` with:

- `delivery_revenue_requirement_from_rate_case` (from YAML input)
- `delivery_revenue_requirement_topups` (sum of `add_to_drr` charge budgets)
- `supply_revenue_requirement_topups` (sum of `add_to_srr` charge budgets + optional supply base override)
- `total_delivery_revenue_requirement` and `total_delivery_and_supply_revenue_requirement`
- Per-charge detail in `delivery_top_ups` and `supply_top_ups`

Two modes for volumetric ($/kWh) charges:

- **EIA mode** (default): day-weighted avg rate × EIA-861 total residential kWh.
- **ResStock mode** (`--use-resstock-loads`): monthly rate × monthly utility-level
  kWh from ResStock, scaled to EIA-861 customer count.

**Optional: `--path-supply-base`** -- a YAML mapping utility keys to a supply
base dollar amount. Used for PSEG-LI (see below).

### `build_rate_case_test_year.py`

**Track 2.** Reads a hand-seeded partial YAML (base DRR, test-year kWh, customer
counts from testimony) and the monthly rates YAML. Aggregates ResStock monthly
loads using `grid_cons`, computes the scaling block, and writes the completed
YAML with all top-ups and summed totals.

See [Track 2](#track-2-testimony-derived-build_rate_case_test_yearpy) above.

### `estimate_psegli_rr.py`

Estimates PSEG-LI's delivery and supply revenue requirements from LIPA's
published budget data. See [PSEG-LI special case](#pseg-li-special-case) below.

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

### Shared steps (both tracks)

1. Add the utility to `utils/utility_codes.py` with its EIA utility ID.
2. Add an entry to `tariffs_by_utility.yaml` in the state's `top-ups/` directory.
3. Fetch the base tariff: `just s <state> fetch-genability-tariffs`.
4. Classify every charge (manual research). Document in
   `context/methods/bat_mc_residual/<state>_residential_charges_in_bat.md`.
5. Fetch monthly rates: `just s <state> fetch-monthly-rates`.

### Track 1 (EIA determinants)

6. Add a rate-case DRR entry in `delivery_rev_requirements_from_rate_cases.yaml`.
7. Run `just s <state> compute-rr` to produce `rev_requirement/<utility>.yaml`.

### Track 2 (testimony determinants)

6. Create a partial `<utility>_rate_case_test_year.yaml` with:
   - `utility`: utility short code
   - `delivery_revenue_requirement_from_rate_case`: base DRR from testimony
   - `test_year_residential_kwh`: total residential kWh from testimony
   - `test_year_customer_count`: residential customers (annual bills / 12)
   - Optional per-schedule detail for documentation
   - Optional `test_year_fixed_charge_from_rate_case` for a durable fixed-charge
     reference (see below)
7. Run `just s <state> build-rate-case-test-year` to complete the YAML with
   top-ups, ResStock scaling block, and summed totals.
8. Also create a `<utility>_large_number_rate_case_test_year.yaml` with
   `revenue_requirement: 1e12` and the same `test_year_customer_count` for
   the large-number calibration runs.

### Durable testimony references

`build_rate_case_test_year.py` always recomputes the `delivery_top_ups` and the
`delivery_revenue_requirement` breakdown from the Genability monthly rates — there
are no per-charge overrides. Because the script reads and writes the same
`<utility>_rate_case_test_year.yaml`, re-running always refreshes those budgets
from the current monthly-rates snapshot.

Sometimes the rate-case testimony pins a value that differs from the Genability
snapshot and needs to be referenced elsewhere in the pipeline — for example,
Karas testimony for BGE uses a flat $10/month customer charge rather than the
blended $9.65/$10.00 in the (regenerated) monthly-rates YAML. Editing the
monthly-rates file is not durable, since `fetch_monthly_rates.py` overwrites it.

Store such values as top-level scalar fields in the partial YAML instead. The
script preserves them verbatim (value and comment) across re-runs:

```yaml
# --- Karas Exhibit E-3, row 1 Customer Charge: flat $10/month per testimony ---
test_year_fixed_charge_from_rate_case: 10.0 # $/month
```

This keeps the Genability monthly-rates file a clean, regenerated data snapshot
while encoding durable testimony decisions in the hand-maintained partial YAML,
where the rest of the pipeline can reference them.

To preserve a new reference field, add it to the pass-through block in
`_build_output_yaml` (next to `test_year_fixed_charge_from_rate_case`).

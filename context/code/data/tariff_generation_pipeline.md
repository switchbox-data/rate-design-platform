# Tariff generation pipeline: flat vs default-structure tariffs

How pre-calibrated URDB v7 tariff JSONs are built from upstream data, and
how CAIRO calibrates them against the revenue requirement.

## Why two tariff types

Every utility gets two families of pre-calibrated tariffs:

| Family                | Files                                                     | Derivation                        | Rate structure                                                                 |
| --------------------- | --------------------------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------ |
| **Flat**              | `{utility}_flat.json`, `{utility}_flat_supply.json`       | Top-down from revenue requirement | Single volumetric rate (one $/kWh)                                             |
| **Default-structure** | `{utility}_default.json`, `{utility}_default_supply.json` | Bottom-up from filed rates        | Preserves the utility's actual rate structure (seasonal, tiered, TOU, or flat) |

Both are starting points for CAIRO's precalc, which calibrates the
volumetric rate(s) to exactly hit the revenue requirement. The calibrated
copies are written to `{utility}_{pattern}_calibrated.json`.

**Why the distinction matters:**

- **Flat** tariffs spread the revenue requirement uniformly across all kWh.
  The rate is `(total_delivery_rr - fixed_revenue) / total_residential_kwh`
  where `total_residential_kwh` comes from the ResStock sample (which may
  differ from EIA-861 totals). This gives CAIRO an easy starting point but
  discards rate-design signals (seasons, tiers, TOU).

- **Default-structure** tariffs preserve the utility's filed volumetric rate
  structure — seasonal tiers (ConEd, O&R), seasonal TOU (PSEGLI), or flat
  (NYSEG, NiMo, CenHud, RGE, RIE). The pre-calibration rates come from the
  Genability API and reflect the actual tariff book. CAIRO then scales them
  to hit the RR while preserving relative structure (e.g., summer/winter
  tier ratios stay the same).

**Which one is used in runs:**

`BASE_TARIFF_PATTERN` in `state.env` controls which family is used for runs
1–4 (the "base" runs). Currently set to `default` for both NY and RI, so
CAIRO sees the actual utility rate structure in the base-case BAT.

## Pipeline overview

```text
Genability API  ──→  fetch_monthly_rates.py  ──→  monthly_rates YAML
                                                       │
                                                       ├──→  create_default_structure_tariffs.py  ──→  {utility}_default.json
                                                       │                                              {utility}_default_supply.json
                                                       │
                                                       ├──→  create_flat_tariffs.py  ──→  {utility}_flat.json
                                                       │     (also needs rev_requirement YAML)         {utility}_flat_supply.json
                                                       │
                                                       └──→  compute_rr.py  ──→  rev_requirement/{utility}.yaml
                                                             (also needs rate-case RR, EIA-861)
```

## Step 1: Fetch monthly rates from Genability

**Script:** `utils/pre/rev_requirement/fetch_monthly_rates.py`

**Justfile recipe:** `fetch-monthly-rates` (from `rate_design/hp_rates/Justfile`)

Calls the Genability (Arcadia) API for each month in the target year with
`effectiveOn` + `lookupVariableRates=true`. For each tariffRateId
classified in the utility's `charge_decisions.json`, resolves the
day-weighted rate for that month.

### Charge classification

Each Genability rate is pre-classified in
`config/rev_requirement/top-ups/charge_decisions/{utility}_charge_decisions.json`
with a `decision` label:

| Decision         | Meaning                                                                                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `already_in_drr` | Core delivery rates already recovered through the rate case revenue requirement. These are the base rates (e.g., ConEd's tiered delivery charge).                  |
| `add_to_drr`     | Surcharges that add to delivery revenue (rider-sourced charges like SBC, DLM, EV Make Ready, CES delivery, VDER). Not part of the rate-case RR but still delivery. |
| `add_to_srr`     | Supply charges (commodity, merchant function charge, CES supply). Used for `_supply.json` tariffs.                                                                 |
| `exclude_*`      | Excluded charges (true-ups, taxes, zonal adjustments, percentage-of-bill). Not included in tariffs.                                                                |

### Output: monthly_rates YAML

Path: `config/rev_requirement/top-ups/monthly_rates/{utility}_monthly_rates_{year}.yaml`

Structure:

```yaml
utility: coned
master_tariff_id: 809
start_month: 2025-01
end_month: 2025-12

already_in_drr:
  rate_structure: seasonal_tiered   # or flat, seasonal_tou
  seasons:                          # present for seasonal_tiered and seasonal_tou
    summer: {from_month: 6, to_month: 9, ...}
    winter: {from_month: 10, to_month: 5, ...}
  tou_periods:                      # present for seasonal_tou only
    on_peak: {from_hour: 15, to_hour: 19, weekdays_only: true}
    off_peak: {from_hour: 19, to_hour: 15, weekdays_only: true}
  charges:
    customer_charge:                # fixed charge ($/month or $/day)
      charge_unit: $/month
      monthly_rates: {2025-01: 20.0, ...}
    core_delivery_rate:             # tiered volumetric ($/kWh)
      charge_unit: $/kWh
      tiers:
        - upper_limit_kwh: 250.0
          monthly_rates:
            summer: {2025-01: 0.16107, ...}
            winter: {2025-01: 0.16107, ...}
        - upper_limit_kwh: null     # unlimited
          monthly_rates:
            summer: {2025-01: 0.18518, ...}
            winter: {2025-01: 0.16107, ...}

add_to_drr:
  rate_structure: flat              # surcharges are always flat $/kWh
  charges:
    dlm_surcharge:
      charge_unit: $/kWh
      monthly_rates: {2025-01: 0.0016, ...}
    # ... more surcharges ...

add_to_srr:
  rate_structure: flat              # or seasonal_tou for PSEGLI
  charges:
    supply_commodity_bundled:
      charge_unit: $/kWh
      monthly_rates: {2025-01: 0.0967, ...}
    # ... more supply charges ...

excluded:
  tax_sur_credit: {decision: exclude_expired}
  # ...
```

The `rate_structure` discriminator on `already_in_drr` determines which
builder is invoked downstream. Current structures by utility:

| Utility                       | `already_in_drr.rate_structure`                                 |
| ----------------------------- | --------------------------------------------------------------- |
| ConEd                         | `seasonal_tiered` (summer/winter, 250 kWh tier boundary)        |
| O&R                           | `seasonal_tiered` (summer/winter, 250 kWh tier boundary)        |
| PSEGLI                        | `seasonal_tou` (summer/winter, on-peak/off-peak weekday 3–7 PM) |
| NYSEG, RGE, NiMo, CenHud, RIE | `flat`                                                          |

### Discovery mode

`fetch_monthly_rates.py --discover` enumerates all active rates for a
masterTariffId at a given effective date and writes a rump
`charge_decisions.json` with expanded schema but no decision labels. This
is the first step in building charge decisions for a new utility from
scratch — the output needs human review to fill in `decision`,
`master_charge`, and `master_type`.

## Step 2: Compute revenue requirements

**Script:** `utils/pre/rev_requirement/compute_rr.py`

**Justfile recipe:** `compute-rr`

Uses the monthly_rates YAML (the `add_to_drr` charges) and rate-case
delivery RR to compute a total delivery revenue requirement and a supply RR
for each utility. The output YAML at
`config/rev_requirement/{utility}.yaml` is consumed by `create_flat_tariffs.py`.

Key fields:

- `total_delivery_revenue_requirement` — rate-case RR + top-up surcharges
- `total_residential_kwh` — from ResStock sample or EIA-861
- `supply_revenue_requirement_topups` — sum of `add_to_srr` charges × load

## Step 3a: Generate flat tariffs

**Script:** `utils/pre/create_flat_tariffs.py`

**Justfile recipe:** `create-flat-tariffs`

For each utility, reads the rev_requirement YAML and computes:

```text
fixed_charge  = average monthly fixed from monthly_rates YAML ($/month + $/day)
fixed_revenue = fixed_charge × customer_count (EIA-861) × 12
delivery_vol  = (total_delivery_rr − fixed_revenue) / total_residential_kwh
supply_vol    = supply_rr / total_residential_kwh
```

Writes a single-period URDB v7 flat tariff with one `energyratestructure`
entry. The flat rate is a starting point; CAIRO calibration will adjust it.

## Step 3b: Generate default-structure tariffs

**Script:** `utils/pre/create_default_structure_tariffs.py`

**Justfile recipe:** `create-default-structure-tariffs`

For each utility, reads the monthly_rates YAML and dispatches on
`already_in_drr.rate_structure`:

### Flat utilities (NYSEG, RGE, NiMo, CenHud, RIE)

1. Sum all $/kWh charges from `already_in_drr` (base delivery) +
   `add_to_drr` (surcharges) per month.
2. Detect seasonal periods by grouping consecutive months with identical
   combined rates (e.g., months where the SBC changes create a period
   boundary).
3. Build a `create_seasonal_tiered_tariff` with one tier per period (no
   upper limit), preserving fixed charges.

### Seasonal-tiered utilities (ConEd, O&R)

1. Extract per-season, per-tier delivery rates from `already_in_drr.charges`
   (e.g., ConEd tier 1 = $0.16107, tier 2 summer = $0.18518, tier 2 winter
   = $0.16107 with 250 kWh boundary).
2. Extract flat surcharges from `add_to_drr` and compute per-season average.
3. Add surcharges to each tier rate for each season.
4. Build `create_seasonal_tiered_tariff` with season-month mapping.

For the `_supply.json` variant, supply rates (which vary monthly) are added
on top, producing more period boundaries when supply rates change month to
month.

### Seasonal-TOU utilities (PSEGLI)

1. Extract per-season, per-TOU-slot delivery rates from `already_in_drr`.
2. Build weekday/weekend schedule matrices (12 months × 24 hours) mapping
   each hour to a period index.
3. Add surcharges (average of `add_to_drr`).
4. Build `create_seasonal_tou_tariff_direct` with the schedule and rate
   structure.

For the `_supply.json` variant, if `add_to_srr.rate_structure` is
`seasonal_tou` (as for PSEGLI), TOU-structured supply rates are layered in
per slot; if flat, they're distributed across all slots.

### Fixed charges

Both `already_in_drr` and `add_to_drr` may contain $/month and $/day
charges (e.g., ConEd's $20/month customer charge + $1.28/month billing
charge). These are summed, converted to monthly equivalents, averaged
across months, and set as `fixedchargeunits` in the URDB JSON. Fixed
charges are recovered as-is by CAIRO (not absorbed into volumetric
calibration).

### Output format

Both scripts produce URDB v7 JSON via helpers in `utils/pre/create_tariff.py`:

- `create_default_flat_tariff()` — single-period flat
- `create_seasonal_tiered_tariff()` — seasonal periods with optional tiers
- `create_seasonal_tou_tariff_direct()` — TOU with weekday/weekend schedules

Each JSON has `energyratestructure` (list of period entries with rate/unit),
`energyweekdayschedule` + `energyweekendschedule` (12×24 matrices for TOU),
and `fixedchargeunits` / `fixedchargefirstperiod`.

## Step 4: Copy to nonhp variants

**Recipes:** `copy-default-flat-to-nonhp-flat-all`,
`copy-default-structure-to-nonhp-all`

Uses `utils/pre/copy_flat_to_nonhp_flat.py` to create
`{utility}_nonhp_default.json` and `{utility}_nonhp_default_supply.json`
(and flat equivalents) with updated labels. These are the "counterfactual"
tariffs for non-heat-pump customers in runs that differentiate HP vs non-HP
rates.

## Justfile wiring

The `all-pre` recipe in `rate_design/hp_rates/Justfile` runs the full
pre-processing pipeline:

```text
all-pre:
    create-scenario-yamls
    create-electric-tariff-maps-all
    create-gas-tariff-maps-all
    ensure-gas-tariff-envelope
    compute-rr                              ← Step 2
    create-flat-tariffs                     ← Step 3a
    create-default-structure-tariffs        ← Step 3b
    copy-default-flat-to-nonhp-flat-all     ← Step 4 (flat)
    copy-default-structure-to-nonhp-all     ← Step 4 (default)
    create-seasonal-tou-tariffs {base_tariff_pattern}
    validate-config
```

Key Justfile variables:

| Variable              | Source                                   | Purpose                                                                |
| --------------------- | ---------------------------------------- | ---------------------------------------------------------------------- |
| `base_tariff_pattern` | `BASE_TARIFF_PATTERN` env / `state.env`  | `flat` or `default`; controls which tariff family is used in base runs |
| `monthly_rates_year`  | `MONTHLY_RATES_YEAR` env, default `2025` | Selects which `*_monthly_rates_{year}.yaml` to use                     |
| `path_monthly_rates`  | derived                                  | Full path to the utility's monthly_rates YAML                          |
| `path_genability`     | derived                                  | Parent of `monthly_rates/` and `charge_decisions/` dirs                |

## CAIRO calibration

The tariff JSONs produced above are **pre-calibrated** starting points.
CAIRO's precalc step takes the tariff and adjusts volumetric rates to
exactly recover the revenue requirement:

1. Compute total revenue under the input tariff at the ResStock sample loads.
2. Scale all volumetric rates proportionally so total revenue = target RR.
3. Write `tariff_final_config.json` with the calibrated rates.
4. The `copy-calibrated-tariff-from-run` recipe extracts the calibrated
   tariff and saves it as `{utility}_{pattern}_calibrated.json` in
   `config/tariffs/electric/`.

For default-structure tariffs, calibration preserves relative rate ratios —
e.g., if ConEd's summer tier-2 rate is 1.15× the winter rate before
calibration, it stays 1.15× after. This means the BAT sees
structure-preserving rates at the correct revenue level.

## Rate difference: flat vs default

Flat and default tariffs will generally show different $/kWh rates even for
the same utility, for two reasons:

1. **kWh denominator:** Flat tariffs divide the RR by `total_residential_kwh`
   from the rev_requirement YAML (which uses ResStock sample loads). Default
   tariffs use the actual filed rates from the utility's tariff book (which
   are implicitly based on the utility's own load forecasts / EIA totals).

2. **Structure:** Default tariffs may have higher marginal rates for large
   consumers (tier 2) and lower rates for small consumers (tier 1), or
   peak/off-peak differentials. The weighted average may not equal the flat
   rate.

After CAIRO calibration, both families recover the same total revenue
requirement — the difference is in how that revenue is distributed across
customers with different load shapes.

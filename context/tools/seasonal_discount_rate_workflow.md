# Seasonal Discount Rate Workflow (RI)

## Purpose

Build a seasonal electric tariff for HP customers using:

- CAIRO run outputs (`run_dir`)
- ResStock hourly electric loads
- BAT cross-subsidy metric (default: `BAT_percustomer`)

The workflow computes a winter HP rate and then creates a tariff JSON that keeps
summer at the default rate.

## Winter Definition

Winter is defined explicitly as **December, January, February**.

## Core Formula (HP only)

`winter_rate_hp = default_rate - (total_cross_subsidy_hp / winter_kwh_hp)`

Where:

- `default_rate` comes from `<run_dir>/tariff_final_config.json` first period/tier
  effective rate (`rate + adj`).
- `total_cross_subsidy_hp` comes from
  `cross_subsidization/cross_subsidization_BAT_values.csv` for `has_hp=true`.
- `winter_kwh_hp` is the sum of `total_fuel_electricity` across HP customers and
  winter months from ResStock hourly loads.

## CLI / Justfile Flow

1. Compute subclass RR (existing output)

```bash
just compute-subclass-rr <run_dir> has_hp BAT_percustomer
```

2. Compute seasonal discount inputs (writes `seasonal_discount_rate_inputs.csv`)

```bash
just compute-seasonal-discount-inputs <run_dir> <resstock_loads_path> BAT_percustomer
```

3. Create seasonal tariff JSON (summer fixed at default rate)

```bash
just create-seasonal-discount-tariff \
  <base_tariff_json> \
  <seasonal_inputs_csv> \
  <label> \
  <output_json>
```

4. Generate tariff map using existing mapper support (`seasonal_discount`)

```bash
just map-electric-rie-seasonal-discount
```

## Expected Seasonal Inputs CSV

`seasonal_discount_rate_inputs.csv` includes:

- `subclass` (always `true` for this workflow)
- `cross_subsidy_col`
- `default_rate`
- `total_cross_subsidy_hp`
- `winter_kwh_hp`
- `winter_rate_hp`
- `winter_months` (always `12,1,2`)

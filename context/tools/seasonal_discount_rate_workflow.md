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
  buy-rate in CAIRO internal `ur_ec_tou_mat` (period 1, tier 1).
- `total_cross_subsidy_hp` comes from
  `cross_subsidization/cross_subsidization_BAT_values.csv` for `has_hp=true`.
- `winter_kwh_hp` is the weighted sum of
  `out.electricity.net.energy_consumption` across HP customers and winter months
  from ResStock hourly loads.

## CLI / Justfile Flow

1. Compute seasonal discount inputs (writes `seasonal_discount_rate_inputs.csv`)

```bash
just compute-seasonal-discount-inputs <run_dir> <resstock_loads_path> BAT_percustomer
```

This command now uses a dedicated utility:
`utils/post/compute_seasonal_discount_inputs.py`.
It does **not** run subclass RR aggregation and does **not** write RI revenue
requirement YAML outputs.

2. (Optional, separate flow) Compute subclass RR + RI revenue requirement YAMLs

```bash
just compute-subclass-rr <run_dir> 1 <scenario_config> has_hp BAT_percustomer
```

3. Create seasonal tariff JSON (summer fixed at default rate)

```bash
just create-rie-hp-seasonal-calibrated <seasonal_inputs_csv>
just create-rie-hp-seasonal-calibrated-supply <seasonal_inputs_csv_from_run2>
```

Output names are fixed to match RI config tariff naming:

- Run 1 (delivery): `rate_design/ri/hp_rates/config/tariffs/electric/rie_hp_seasonal_calibrated.json`
- Run 2 (delivery + supply): `rate_design/ri/hp_rates/config/tariffs/electric/rie_hp_seasonal_calibrated_supply.json`

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

Both `total_cross_subsidy_hp` and `winter_kwh_hp` are weighted by
`customer_metadata.weight`.

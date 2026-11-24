# hp_rates output files and columns

Open this when a filename or column is required. Paths and how to run are in [SKILL.md](SKILL.md).

## Files in one CAIRO run directory

`s3://data.sb/switchbox/cairo/outputs/hp_rates/<state>/<utility>/<batch>/<cairo_ts>_<run_name>/`

| Path                                                     | What it is                                                                                                                            |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `bill_assistance_metrics.csv`                            | Bill-assistance metrics, including LMI customer impacts                                                                               |
| `bills/comb_bills_year_run.csv`                          | Annual electric + gas bills, proposed rate                                                                                            |
| `bills/comb_bills_year_target.csv`                       | Annual electric + gas bills, baseline rate                                                                                            |
| `bills/elec_bills_year_run.csv`                          | Annual electric bills, proposed rate                                                                                                  |
| `bills/elec_bills_year_target.csv`                       | Annual electric bills, baseline rate                                                                                                  |
| `bills/gas_bills_year_run.csv`                           | Annual gas bills, proposed rate                                                                                                       |
| `bills/gas_bills_year_target.csv`                        | Annual gas bills, baseline rate                                                                                                       |
| `cross_subsidization/cross_subsidization_BAT_values.csv` | Customer-level bill alignment                                                                                                         |
| `customer_metadata.csv`                                  | ResStock metadata for the customers in the run                                                                                        |
| `tariff_final_config.json`                               | Calibrated tariff in CAIRO's shape, one key per tariff. The copy step writes `<key>_calibrated.json` under `config/tariffs/electric`. |

## Master bills

`comb_bills_year_target/`. One row per building per month, including an Annual row. Electric delivery and gas, propane, and oil come from the delivery run. Electric supply comes from the supply run.

| Column                                                                                   | What it is                                                                                    |
| ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `bldg_id`                                                                                | ResStock building id                                                                          |
| `sb.electric_utility`, `sb.gas_utility`                                                  | Utility assignment. `sb.electric_utility` is the Hive partition on the `all_utilities` table. |
| `upgrade`                                                                                | ResStock upgrade (0 baseline, 2 heat pump)                                                    |
| `postprocess_group.has_hp`, `postprocess_group.heating_type`                             | Heat-pump flag and heating classification, from upgrade 00 metadata                           |
| `heats_with_electricity`, `heats_with_natgas`, `heats_with_oil`, `heats_with_propane`    | Fuel flags, from upgrade 00 metadata                                                          |
| `month`                                                                                  | `Jan` through `Dec`, or `Annual`                                                              |
| `weight`                                                                                 | CAIRO sample weight                                                                           |
| `elec_fixed_charge`                                                                      | Electric fixed charge                                                                         |
| `elec_delivery_bill`                                                                     | Electric delivery volumetric bill                                                             |
| `elec_supply_bill`                                                                       | Electric supply bill                                                                          |
| `elec_total_bill`                                                                        | Fixed + delivery + supply                                                                     |
| `baseline_elec_fixed_charge`, `baseline_elec_delivery_bill`, `baseline_elec_supply_bill` | The same three components on the baseline segment. Prefect batches only.                      |
| `gas_total_bill`, `propane_total_bill`, `oil_total_bill`                                 | Fuel bills                                                                                    |
| `energy_total_bill`                                                                      | Sum of fuel bills                                                                             |
| `elec_grid_kwh`                                                                          | Monthly grid electricity, CAIRO's billing basis                                               |
| `gas_therms`                                                                             | Monthly natural gas                                                                           |

## Master BAT

`cross_subsidization_BAT_values/`. One row per building, annual. Delivery BAT is the delivery run. Total BAT is the supply run. Supply BAT is total minus delivery. The same `baseline_elec_*` columns as master bills are joined from the baseline segment's master bills.

| Column                                                                                                                  | What it is                                   |
| ----------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| `bldg_id`, utilities, `upgrade`, `postprocess_group.has_hp`, `postprocess_group.heating_type`, `heats_with_*`, `weight` | Same meanings as master bills                |
| `BAT_vol_delivery`, `BAT_vol_supply`, `BAT_vol_total`                                                                   | Volumetric bill alignment                    |
| `BAT_peak_delivery`, `BAT_peak_supply`, `BAT_peak_total`                                                                | Peak bill alignment                          |
| `BAT_percustomer_delivery`, `BAT_percustomer_supply`, `BAT_percustomer_total`                                           | Per-customer bill alignment                  |
| `annual_bill_*`, `economic_burden_*`, `residual_share_*`                                                                | Cost-allocation components BAT is built from |

Read BAT from precalc segments. Calibrated-segment BAT uses the large revenue requirement those runs set and is not a cross-subsidy. Bills on calibrated segments are still the billed amounts.

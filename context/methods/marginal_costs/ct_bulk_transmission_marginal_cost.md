# CT bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in Connecticut. CT uses the same AESC 2024 avoided PTF cost as Rhode Island. Three allocations are available through the ct Justfile's `bulk_tx_method` variable (the generator's `--method` flag). The default, `pop_kde`, spreads the CPI-adjusted cost by a month × day-type × hour probability of peak fit on 2022–2025 New England system load. `month_hour` uses OCC-863's published probability of peak instead of a load curve. `isone_aesc_ptf` is the top-100 exceedance method RI uses. For the two load-based methods, `--allocation-load` chooses whether the signal is fit on the NE system aggregate or on CT zone load alone.

---

## Summary

| Feature                   | Value                                                                                                                                                                                     |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Marginal cost source      | AESC 2024 avoided PTF cost                                                                                                                                                                |
| Value                     | $69/kW-year in 2024 dollars (same for all 6 NE states)                                                                                                                                    |
| Default allocation method | `pop_kde`: KDE probability of peak on 2022–2025 NE system load; $69 CPI-adjusted to 2026 (the ct recipe's target dollar year)                                                             |
| Alternative allocations   | `month_hour`: OCC-863 Attachment 2 probability of peak, $69 CPI-adjusted to 2026 (`ct_eversource` only). `isone_aesc_ptf`: top-100-hour exceedance on one load year, no CPI (RI's method) |
| Allocation load           | NE system aggregate (all 8 ISO-NE load zones) by default; `--allocation-load utility_zone` for CT zone load (`pop_kde` and `isone_aesc_ptf`)                                              |
| ISO                       | ISO-NE                                                                                                                                                                                    |
| Utilities covered         | `ct_eversource`, `ct_ui` (`month_hour` is `ct_eversource` only)                                                                                                                           |
| Output S3 path            | `s3://data.sb/switchbox/marginal_costs/ct/bulk_tx/`                                                                                                                                       |

---

## Cost source: AESC 2024 avoided PTF cost

CT uses the same cost source as RI: the **Avoided Energy Supply Components (AESC) 2024** study by Synapse Energy Economics [@synapse_AvoidedEnergySupply_2024], which publishes an avoided PTF (Pool Transmission Facility) cost in $/kW-year for benefit-cost screening of efficiency and DER programs in New England.

**Key value:** `$69/kW-year` (AESC 2024, Table: Avoided T&D — PTF) — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325).

This is the LRMC of bulk transmission in ISO-NE territory: the cost deferred or avoided when a CT customer reduces peak demand by 1 kW during peak hours. The same $69/kW-year applies across all six NE states because PTF is a regional (pooled) cost. The value is implemented as `AESC_2024_AVOIDED_PTF_KW_YEAR` in `utils/data_prep/marginal_costs/inputs/isone.py`.

For full background on the AESC study, the RNS rate, and the reasoning behind choosing AESC over the embedded RNS rate, see `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`.

---

## Hourly allocation

The $69/kW-year value must be converted to an 8760 hourly signal for CAIRO. The ct Justfile's `bulk_tx_method` variable picks one of three allocations and passes it as `--variant` to `run_allocation.py`. RI uses the same runner with the `isone_aesc_ptf` block and does not pass a variant.

### KDE probability of peak (`pop_kde`, default)

`bulk_tx_method=pop_kde` (the ct Justfile default) spreads the cost by a month × day-type × hour probability of peak ([probability_of_peak_kde.md](probability_of_peak_kde.md)). The CT inputs:

| Input         | CT value                                                                                                                                                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Load history  | 2022–2025 (`--pop-history-years`), stacked. Default is the sum of all 8 ISO-NE zones. `utility_zone` uses CT zone load alone. Four years, and these four, because that is the history Eversource used for its own PoP.                           |
| Annual cost   | AESC `$69/kW-yr` in 2024 dollars, CPI-inflated to `--target-dollar-year`. The ct recipe passes 2026, the same dollar year as distribution. For 2026 the factor is 1.0560, so `$69` becomes `$72.87/kW-yr`. The exceedance path does not inflate. |
| Year          | The output year, stamped on the timestamps.                                                                                                                                                                                                      |
| Calendar year | `--calendar-year`, default the output year.                                                                                                                                                                                                      |

Each history year is read with the same `load_isone_zone_loads` as the exceedance method, so the time-zone treatment is identical (hour-beginning Eastern prevailing time kept as naive wall-clock time; the fall-back 1 AM averaged; the spring-forward 2 AM interpolated; December 31 dropped in a leap year). The 2025 system peak lands at 2025-06-24 18:00, hour ending 19, which matches ISO-NE's reported peak hour. On 2022–2025 NE system load the delimiter is 1.552 × the year mean, about 0.83 of the annual peak.

The writer calls `pop_kde` and keeps the column name `bulk_tx_cost_enduse`. A flat 1 kW load pays the inflated `$/kW-yr`. This default covers `ct_eversource` and `ct_ui`. The inputs are recorded under `bulk_tx` in `rate_design/hp_rates/ct/config/marginal_costs/allocation.yaml`. `--allocation-load utility_zone` is the same method with `zones: [CT]`. `--pop-history-years 2021-2025` is the five-year sensitivity.

On 2022–2025 NE system load, 121 of 576 cells are nonzero. June, July, August, and September carry 13%, 56%, 26%, and 5% of the probability; weekend-and-holiday cells carry 25%; February holds a trace (0.05%). The result is about 1,900 nonzero hours, and the maximum falls on the July weekday 17:00 cell. The level of every hour scales with the inflated `$/kW-yr`.

Implementation: `isone_zone_load` in `inputs/isone.py` stacks the history years, and `run_allocation.py` calls `pop_kde` in `methods/pop_kde.py`.

The manifest key is `aesc_ptf69_pop_kde_ne_system_hist2022to2025_cal2025_2026usd`, which is the key in `pipeline_ct_eversource.yaml`. All three methods write the same path under different manifest keys, and the pipeline YAML's `key` says which one a run accepts.

### Month-hour allocation from OCC-863 (`month_hour`, ct_eversource only)

`bulk_tx_method=month_hour` (`run_allocation.py --variant month_hour`) is the same month-hour
calendar expansion as distribution ([month_hour_allocation.md](month_hour_allocation.md)), with
`table_kind` `probability`. It builds the 8760 from OCC-863 Attachment 2 page 3 instead of from a
load curve. Page 3 is the probability of distribution peak by month, hour, and day type. There is
no loss factor.

| Input         | CT value                                                                                                                                                                        |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Weekday table | `month_hour_probability_of_peak_weekday.parquet` (`month`, `hour`, `num_days`, `probability_of_peak`)                                                                           |
| Weekend table | `month_hour_probability_of_peak_weekend_holidays.parquet`, same shape                                                                                                           |
| Annual cost   | AESC `$69/kW-yr` in 2024 dollars, CPI-inflated to 2026 so it matches the dollar year of Eversource's distribution MC. The 8760 sums to `$69` only when the target year is 2024. |
| Year          | The recipe's `year_arg`, stamped on the timestamps                                                                                                                              |
| Calendar year | `month_hour_calendar_year`, default `year_arg`                                                                                                                                  |
| Holidays      | `include_holidays` defaults to true                                                                                                                                             |

`num_days` on the published table is the divisor inside each cell. The uniform scale absorbs a
mismatch with the target calendar's day counts. When they match, the factor is 1. The parquets
live under `s3://data.sb/eversource/occ863/parquet/`. These
inputs are the `month_hour` alternative under `bulk_tx` in
`rate_design/hp_rates/ct/config/marginal_costs/allocation.yaml`. `--aesc-ptf-kw-year` overrides
the `$69` scalar before CPI.

The manifest key is `month_hour_occ863att2_aesc_ptf69_holidays_cal2025_2026usd`. The ct
recipe wires CL&P's OCC-863 probability files, so it runs for `ct_eversource` only. The generator
itself takes whatever probability paths it is given.

```bash
just -f ct/Justfile bulk_tx_method=month_hour create-bulk-tx-mc-data ct_eversource 2025
just -f ct/Justfile bulk_tx_method=month_hour include_holidays=false create-bulk-tx-mc-data ct_eversource 2025
```

### Top-N exceedance (`isone_aesc_ptf`)

`bulk_tx_method=isone_aesc_ptf` is the allocation RI uses, and what CT used before `pop_kde`. The formula is top-N exceedance ([top_n_exceedance_allocation.md](top_n_exceedance_allocation.md)). The CT inputs:

| Input       | CT value                                                                                                                                      |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Load curve  | One year of the allocation load (`--load-year`). Default is the sum of all 8 ISO-NE zones. `utility_zone` uses CT zone load alone. See below. |
| Annual cost | AESC `$69/kW-yr`, used as filed. This path does not apply CPI. `--aesc-ptf-kw-year` overrides the scalar.                                     |
| N           | 100                                                                                                                                           |

The writer calls `top_n_exceedance` and keeps the column name `bulk_tx_cost_enduse`. A flat 1 kW load pays `$69/kW-yr`. This alternative covers `ct_eversource` and `ct_ui`. The inputs are recorded under `bulk_tx.alternatives.isone_aesc_ptf` in `rate_design/hp_rates/ct/config/marginal_costs/allocation.yaml`. `--allocation-load utility_zone` is the same alternative with `zones: [CT]`.

---

## Allocation load: system-wide vs. CT zone

This applies to `pop_kde` and `isone_aesc_ptf`. `month_hour` does not use a load curve. The allocation load is the `zones` list on `isone_zone_load` in `allocation.yaml`:

| Flag value     | Load used                           |
| -------------- | ----------------------------------- |
| `ne_system`    | All 8 ISO-NE zones summed (default) |
| `utility_zone` | CT zone load only                   |

**`ne_system` (default):** Peak hours are identified from the aggregate New England load curve. This is consistent with how RNS costs are allocated — they are triggered by NE-wide coincident peaks, not CT-specific peaks. A CT customer reducing load at a NE system peak avoids regional PTF cost regardless of whether CT individually peaks at that moment.

**`utility_zone`:** Peak hours are identified from CT zone load only. This would be appropriate if CT's transmission constraints are primarily local and CT's own peak is the relevant driver of PTF investment for CT. In practice, the NE system peak and the CT zone peak are highly correlated but not identical.

**Which to use:** `ne_system` is the default and the recommended primary value. The `utility_zone` option exists for sensitivity analysis and to explore how much the two signals diverge.

A comparison of the two loads under the exceedance method (2025 ISO-NE data) found that the signals share most peak hours, with a small set of hours where one is non-zero and the other is zero. The divergence is typically small relative to the total cost level.

---

## Load data source

CT zone load is pulled from the ISO-NE hourly demand dataset on S3:

```
s3://data.sb/isone/hourly_demand/zones/
```

This dataset contains 8760 rows per year per zone, with columns `timestamp` and `load_mw`. CT-specific load is zone label `"CT"`. NE system aggregate is the sum across all 8 ISO-NE load zones: `CT`, `ME`, `NH`, `VT`, `RI`, `SEMASS`, `WCMASS`, `NEMASSBOST`.

Loading is handled by `load_isone_zone_loads()` in `utils/data_prep/marginal_costs/supply/supply_capacity_isone.py`.

---

## Output

The pipeline produces an 8760-row Parquet file at:

```
s3://data.sb/switchbox/marginal_costs/ct/bulk_tx/utility={ct_eversource|ct_ui}/year={year}/data.parquet
```

| Column                | Type    | Description                   |
| --------------------- | ------- | ----------------------------- |
| `timestamp`           | String  | ISO 8601, hourly, target year |
| `bulk_tx_cost_enduse` | Float64 | $/kWh for that hour           |

The output is validated at runtime: the sum of `bulk_tx_cost_enduse` over all 8760 hours (for a hypothetical 1-kW constant load) must equal the allocated `$/kW-year` within 0.01%. For `pop_kde` and `month_hour` it is the value inflated to the target dollar year (`$72.87` for 2026, the ct recipe's year), which is `$69` only when that year is 2024; for exceedance there is no CPI and the sum must equal `$69.00`.

---

## Running the pipeline

From `rate_design/hp_rates/`:

```bash
# Default: pop_kde (dry run, then upload); both utilities
just s ct create-bulk-tx-mc-data ct_eversource 2025
just s ct create-bulk-tx-mc-data ct_eversource 2025 --upload
just s ct create-bulk-tx-mc-data ct_ui 2025 --upload

# Fit the PoP on CT zone load instead of the NE system
just s ct create-bulk-tx-mc-data ct_eversource 2025 --allocation-load utility_zone --upload

# Month-hour from OCC-863's published PoP (ct_eversource only)
just s ct bulk_tx_method=month_hour create-bulk-tx-mc-data ct_eversource 2025 --upload

# Top-100 exceedance, uninflated $69/kW-yr, NE system or CT zone load
just s ct bulk_tx_method=isone_aesc_ptf create-bulk-tx-mc-data ct_eversource 2025 --upload
just s ct bulk_tx_method=isone_aesc_ptf create-bulk-tx-mc-data ct_eversource 2025 --allocation-load utility_zone --upload
```

The manifest keys are `aesc_ptf69_pop_kde_ne_system_hist2022to2025_cal2025_2026usd` (default), `month_hour_occ863att2_aesc_ptf69_holidays_cal2025_2026usd`, and `aesc_ptf69_ne_system_100h_load2025` ([mc_provenance_manifest.md](../../code/marginal_costs/mc_provenance_manifest.md)).

`create-bulk-tx-mc-data-all` lists the utilities on the selected `allocation.yaml` block. `month_hour` lists only `ct_eversource`, because that alternative is CL&P's OCC-863 tables. Both utilities are in `supply_utils.py` as entries in `ISONE_UTILITY_ZONES`. External data sources still use historical identifiers (HIFLD: "CONNECTICUT LIGHT & POWER CO" / "UNITED ILLUMINATING CO"; EIA IDs 4176 / 19497).

---

## Implementation files

| File                                                             | Role                                                                                                                                                          |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `utils/data_prep/marginal_costs/inputs/isone.py`                 | ISO-NE zone load (`isone_zone_load`), including a multi-year history for `pop_kde`                                                                            |
| `utils/data_prep/marginal_costs/methods/month_hour.py`           | Month-hour calendar expansion. `month_hour` with `table_kind` `probability` is the `month_hour` alternative; `pop_kde` calls its expander and skips the scale |
| `utils/data_prep/marginal_costs/methods/pop_kde.py`              | KDE probability of peak. `pop_kde` places the annual cost on the 8760                                                                                         |
| `utils/data_prep/marginal_costs/run_allocation.py`               | Runner. `--variant` selects `pop_kde` (default), `month_hour`, or `isone_aesc_ptf`                                                                            |
| `utils/data_prep/marginal_costs/methods/top_n_exceedance.py`     | Top-N exceedance formula used by `isone_aesc_ptf`                                                                                                             |
| `utils/data_prep/marginal_costs/supply_utils.py`                 | `ISONE_UTILITY_ZONES` mapping                                                                                                                                 |
| `utils/data_prep/marginal_costs/supply/supply_capacity_isone.py` | FCA supply capacity. Imports `load_isone_zone_loads` from `inputs/isone.py`                                                                                   |
| `rate_design/hp_rates/ct/Justfile`                               | `create-bulk-tx-mc-data`, `create-bulk-tx-mc-data-all` recipes; `bulk_tx_method` variable                                                                     |
| `rate_design/hp_rates/ct/state.env`                              | `UTILITIES=ct_eversource,ct_ui`, `REGION=isone`, `YEAR=2025`                                                                                                  |
| `tests/test_top_n_exceedance.py`                                 | Tests for the exceedance formula and the RI and CT config entries                                                                                             |
| `tests/test_ri_bulk_tx_mc.py`                                    | Tests for the ISO-NE bulk-tx writer: system vs. zone load, 8760 fill, validation                                                                              |
| `tests/test_month_hour.py`                                       | Tests for the month-hour formula, its CLI, and the CT distribution and bulk-tx config entries                                                                 |
| `tests/test_bulk_tx_month_hour.py`                               | Tests for CPI on the `month_hour` annual cost, composed with the expander                                                                                     |
| `tests/test_pop_kde.py`                                          | Tests for the KDE formula, its CLI, and the CT config entry                                                                                                   |
| `tests/test_bulk_tx_pop_kde.py`                                  | Tests for the ISO-NE `pop_kde` writer and its manifest key                                                                                                    |

---

## Comparison with RI

| Feature                 | RI                        | CT                                                                                               |
| ----------------------- | ------------------------- | ------------------------------------------------------------------------------------------------ |
| Cost source             | AESC 2024 ($69/kW-yr)     | AESC 2024 ($69/kW-yr, 2024 dollars)                                                              |
| Default allocation      | Top-100 exceedance        | KDE probability of peak on 2022–2025 load, CPI to 2026                                           |
| Alternatives            | (the only method)         | `bulk_tx_method=month_hour` (OCC-863 PoP, CPI to 2026); `bulk_tx_method=isone_aesc_ptf` (no CPI) |
| Default allocation load | NE system                 | NE system (`pop_kde`, `isone_aesc_ptf`); none for `month_hour`                                   |
| Zone-load option        | Not used by the RI recipe | `--allocation-load utility_zone` on the two load-based methods                                   |
| Utilities               | `rie`                     | `ct_eversource`, `ct_ui` (`month_hour` is `ct_eversource` only)                                  |
| ISO-NE zone label       | `RI`                      | `CT`                                                                                             |

The shared CLI still defaults to top-100 exceedance, which is what RI runs. CT's Justfile passes `--method pop_kde` unless `bulk_tx_method` is set to `month_hour` or `isone_aesc_ptf`. `--allocation-load` is available for RI as well; the RI recipe does not pass it.

---

## Key references

- **AESC 2024 Report** [@synapse_AvoidedEnergySupply_2024]: https://www.synapse-energy.com/sites/default/files/AESC%202024.pdf
- **AESC avoided PTF ($69/kW-yr)** — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325)
- **AESC 2024 Materials** (User Interfaces, appendices): https://www.synapse-energy.com/aesc-2024-materials
- **RI methodology doc** (full AESC vs RNS discussion): `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`
- **RI cost recovery doc** (RNS/LNS mechanics, PTF allocation): `context/domain/marginal_costs/ri_bulk_transmission_cost_recovery.md`

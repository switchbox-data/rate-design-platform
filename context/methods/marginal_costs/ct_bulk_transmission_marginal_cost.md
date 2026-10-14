# CT bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in Connecticut. CT uses the same AESC 2024 avoided PTF cost as Rhode Island, spread by a month × day-type × hour probability of peak fit on 2022–2025 New England system load. The top-100 exceedance method RI uses is still available with `--bulk-tx-method exceedance_top_n`. `--allocation-load` chooses whether that signal is fit on the NE system aggregate or on CT zone load alone.

---

## Summary

| Feature                     | Value                                                                                                       |
| --------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Marginal cost source        | AESC 2024 avoided PTF cost                                                                                  |
| Value                       | $69/kW-year in 2024 dollars (same for all 6 NE states); CPI-adjusted to the analysis year before allocation |
| Hourly allocation method    | KDE probability of peak on 2022–2025 load (`pop_kde`)                                                       |
| Alternative allocation      | Top-100-hour exceedance (`--bulk-tx-method exceedance_top_n`), which does not inflate the $69               |
| Default allocation load     | NE system aggregate (all 8 ISO-NE load zones)                                                               |
| Alternative allocation load | CT zone load only                                                                                           |
| ISO                         | ISO-NE                                                                                                      |
| Utilities covered           | `ct_eversource`, `ct_ui`                                                                                    |
| Output S3 path              | `s3://data.sb/switchbox/marginal_costs/ct/bulk_tx/`                                                         |

---

## Cost source: AESC 2024 avoided PTF cost

CT uses the same cost source as RI: the **Avoided Energy Supply Components (AESC) 2024** study by Synapse Energy Economics [@synapse_AvoidedEnergySupply_2024], which publishes an avoided PTF (Pool Transmission Facility) cost in $/kW-year for benefit-cost screening of efficiency and DER programs in New England.

**Key value:** `$69/kW-year` (AESC 2024, Table: Avoided T&D — PTF) — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325).

This is the LRMC of bulk transmission in ISO-NE territory: the cost deferred or avoided when a CT customer reduces peak demand by 1 kW during peak hours. The same $69/kW-year applies across all six NE states because PTF is a regional (pooled) cost. The value is implemented as `AESC_2024_AVOIDED_PTF_KW_YEAR` in `utils/data_prep/marginal_costs/bulk_tx_isone.py`.

For full background on the AESC study, the RNS rate, and the reasoning behind choosing AESC over the embedded RNS rate, see `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`.

---

## Hourly allocation

The $69/kW-year value must be converted to an 8760 hourly signal for CAIRO. The CT default, `--bulk-tx-method pop_kde` (what `create-bulk-tx-mc-data` passes), spreads it by a month × day-type × hour **probability of peak**. It is the method Eversource described in OCC-733 for its OCC-863 distribution PoP ([probability_of_peak_kde.md](probability_of_peak_kde.md)), applied to the New England system load:

1. Load four complete years of allocation load, **2022–2025** by default (`--pop-history-years`). Four years, and these four, because that is the history Eversource used for its own PoP, so the bulk-tx and distribution signals rest on the same years. Each year is read with the same `load_isone_zone_loads` as the exceedance method, so the time-zone treatment is identical (hour-beginning Eastern prevailing time kept as naive wall-clock time; the fall-back 1 AM averaged; the spring-forward 2 AM interpolated; December 31 dropped in a leap year). The 2025 system peak lands at 2025-06-24 18:00, hour ending 19, which matches ISO-NE's reported peak hour.
2. Label each hour by month, hour, and weekday vs weekend-and-holiday; divide each hour by its year's average load; set the delimiter as the mean over years of the year's highest cell mean (1.552 × the year mean on 2022–2025 NE load, about 0.83 of the annual peak); fit a Scott-bandwidth Gaussian KDE per cell, each kernel truncated at ±3 bandwidths; take the share above the delimiter; normalize to 1.
3. Inflate the PTF value from its dollar year (`--aesc-dollar-year`, default 2024 for AESC 2024) to the analysis year (`--target-dollar-year`, default `--year`) with FRED CPI. For 2025 the factor is 1.0263, so `$69` becomes `$70.82/kW-yr`. The exceedance method does not do this; it uses the value as given.
4. Each cell's `$/kWh` is the inflated `$/kW-yr` × PoP ÷ the number of days of that month and day type on the `--calendar-year` calendar (default `--year`). The cells are laid onto the CAIRO 8760 with `expand_month_hour_mc_to_8760`, the same expansion the OCC-863 month-hour distribution tables use, and the sum is checked against the inflated `$/kW-yr`.

On 2022–2025 NE system load, 121 of 576 cells are nonzero. June, July, August, and September carry 13%, 56%, 26%, and 5% of the probability; weekend-and-holiday cells carry 25%; February holds a trace (0.05%). The result is about 1,900 nonzero hours with a maximum of `$0.19/kWh` (July weekday 17:00).

`--allocation-load utility_zone` fits the PoP on CT zone load instead; `--pop-history-years 2021-2025` is the five-year sensitivity.

Implementation: `compute_isone_bulk_tx_pop_kde` and `load_isone_zone_load_history` in `bulk_tx_isone.py`, `_run_isone_pop_kde` in `generate_bulk_tx_mc.py`. The CT Justfile passes `--bulk-tx-method pop_kde`; the shared CLI still defaults to exceedance so a bare `generate_bulk_tx_mc.py --iso isone` call (and the RI recipe) is unchanged.

### Alternative: top-100-hour exceedance (`--bulk-tx-method exceedance_top_n`)

This is the allocation RI uses, and what CT used before `pop_kde`. It loads one year of the allocation curve (`--load-year`), keeps the top 100 hours, and spreads the uninflated `$69/kW-year` across them proportional to each hour's load above the 100th-hour threshold. Every other hour is zero. A 1 kW flat load sums to `$69/kW-year`.

The two methods write the same path under different manifest keys, and the pipeline YAML's `key` says which one a run accepts. `pipeline_ct_eversource.yaml` expects the `pop_kde` key.

---

## Allocation load: system-wide vs. CT zone

This is the CT-specific addition relative to RI. The `generate_bulk_tx_mc.py` CLI accepts `--allocation-load`:

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

Loading is handled by `load_isone_zone_loads()` in `utils/data_prep/marginal_costs/supply_capacity_isone.py`.

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

The output is validated at runtime: the sum of `bulk_tx_cost_enduse` over all 8760 hours (for a hypothetical 1-kW constant load) must equal the allocated `$/kW-year` within 0.01%. For `pop_kde` that is the CPI-adjusted value (`$70.82` for a 2025 analysis year); for exceedance it is the uninflated `$69.00`.

---

## Running the pipeline

From `rate_design/hp_rates/`:

```bash
# Single utility, single year (dry run)
just s ct create-bulk-tx-mc-data ct_eversource 2025

# Single utility, upload to S3
just s ct create-bulk-tx-mc-data ct_eversource 2025 --upload
just s ct create-bulk-tx-mc-data ct_ui 2025 --upload

# All CT utilities, all years (UTILITIES from state.env)
just s ct create-bulk-tx-mc-data-all --upload

# Fit the PoP on CT zone load instead of the NE system
just s ct create-bulk-tx-mc-data ct_eversource 2025 --allocation-load utility_zone --upload

# The previous top-100 exceedance allocation, uninflated $69/kW-yr
just s ct create-bulk-tx-mc-data ct_eversource 2025 --bulk-tx-method exceedance_top_n --upload
```

The default manifest key is `aesc_ptf69_pop_kde_ne_system_hist2022to2025_cal2025_2025usd`. The exceedance override writes `aesc_ptf69_ne_system_100h_load2025` ([mc_provenance_manifest.md](../../code/marginal_costs/mc_provenance_manifest.md)). `extra_args` are appended after `--bulk-tx-method pop_kde`, so the override replaces it.

Both `ct_eversource` and `ct_ui` are defined in `state.env` as `UTILITIES=ct_eversource,ct_ui` and in `supply_utils.py` as entries in `ISONE_UTILITY_ZONES`. External data sources still use historical identifiers (HIFLD: "CONNECTICUT LIGHT & POWER CO" / "UNITED ILLUMINATING CO"; EIA IDs 4176 / 19497).

---

## Implementation files

| File                                                      | Role                                                                                                                                                 |
| --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `utils/data_prep/marginal_costs/bulk_tx_isone.py`         | Core logic: `compute_isone_bulk_tx_signal`, `compute_isone_bulk_tx_pop_kde`, `load_isone_zone_load_history`, `prepare_output`, `validate_allocation` |
| `utils/data_prep/marginal_costs/probability_of_peak.py`   | KDE probability of peak and `allocate_annual_cost_by_pop`, used by `pop_kde`                                                                         |
| `utils/data_prep/marginal_costs/generate_bulk_tx_mc.py`   | CLI entrypoint; handles `--allocation-load` and `--bulk-tx-method` flags                                                                             |
| `utils/data_prep/marginal_costs/supply_utils.py`          | `ISONE_UTILITY_ZONES` mapping, `allocate_annual_exceedance_to_hours`                                                                                 |
| `utils/data_prep/marginal_costs/supply_capacity_isone.py` | `load_isone_zone_loads`                                                                                                                              |
| `rate_design/hp_rates/ct/Justfile`                        | `create-bulk-tx-mc-data`, `create-bulk-tx-mc-data-all` recipes                                                                                       |
| `rate_design/hp_rates/ct/state.env`                       | `UTILITIES=ct_eversource,ct_ui`, `REGION=isone`, `YEAR=2025`                                                                                         |
| `tests/test_ri_bulk_tx_mc.py`                             | Tests for exceedance allocation, system vs. zone load modes                                                                                          |
| `tests/test_bulk_tx_pop_kde.py`                           | Tests for the `pop_kde` allocation and its manifest key                                                                                              |

---

## Comparison with RI

| Feature                 | RI                        | CT                                                       |
| ----------------------- | ------------------------- | -------------------------------------------------------- |
| Cost source             | AESC 2024 ($69/kW-yr)     | AESC 2024 ($69/kW-yr), CPI-adjusted to the analysis year |
| Allocation method       | Top-100 exceedance        | KDE probability of peak on 2022–2025 load                |
| Default allocation load | NE system                 | NE system                                                |
| Zone-load option        | Not used by the RI recipe | `--allocation-load utility_zone`                         |
| Utilities               | `rie`                     | `ct_eversource`, `ct_ui`                                 |
| ISO-NE zone label       | `RI`                      | `CT`                                                     |

The CT recipe passes `--bulk-tx-method pop_kde`. The shared CLI still defaults to exceedance, which is what the RI recipe runs. `--allocation-load` is on the same CLI and is available for RI; the RI recipe does not pass it.

---

## Key references

- **AESC 2024 Report** [@synapse_AvoidedEnergySupply_2024]: https://www.synapse-energy.com/sites/default/files/AESC%202024.pdf
- **AESC avoided PTF ($69/kW-yr)** — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325)
- **AESC 2024 Materials** (User Interfaces, appendices): https://www.synapse-energy.com/aesc-2024-materials
- **RI methodology doc** (full AESC vs RNS discussion): `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`
- **RI cost recovery doc** (RNS/LNS mechanics, PTF allocation): `context/domain/marginal_costs/ri_bulk_transmission_cost_recovery.md`

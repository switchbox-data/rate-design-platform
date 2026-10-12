# CT bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in Connecticut. The default 8760 is a month-hour lookup: the AESC 2024 avoided PTF cost, inflated from 2024 to the target dollar year, times OCC-863's probability of peak. The RI-style top-100 exceedance allocation remains available, including a `--allocation-load` flag that chooses NE system load or CT zone load.

---

## Summary

| Feature                    | Value                                                             |
| -------------------------- | ----------------------------------------------------------------- |
| Marginal cost source       | AESC 2024 avoided PTF cost                                        |
| Value                      | $69/kW-year in 2024 dollars (same for all 6 NE states)            |
| Hourly allocation method   | Month-hour probability of peak (default), or top-100 exceedance   |
| Default dollar year        | 2026 (CPI from 2024; the ct recipe passes `--target-dollar-year`) |
| Exceedance allocation load | NE system aggregate (default), or CT zone only                    |
| ISO                        | ISO-NE                                                            |
| Default utility            | `ct_eversource` (month-hour). Exceedance also covers `ct_ui`      |
| Output S3 path             | `s3://data.sb/switchbox/marginal_costs/ct/bulk_tx/`               |

---

## Cost source: AESC 2024 avoided PTF cost

CT uses the same cost source as RI: the **Avoided Energy Supply Components (AESC) 2024** study by Synapse Energy Economics [@synapse_AvoidedEnergySupply_2024], which publishes an avoided PTF (Pool Transmission Facility) cost in $/kW-year for benefit-cost screening of efficiency and DER programs in New England.

**Key value:** `$69/kW-year` (AESC 2024, Table: Avoided T&D — PTF) — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325).

This is the LRMC of bulk transmission in ISO-NE territory: the cost deferred or avoided when a CT customer reduces peak demand by 1 kW during peak hours. The same $69/kW-year applies across all six NE states because PTF is a regional (pooled) cost. The value is implemented as `AESC_2024_AVOIDED_PTF_KW_YEAR` in `utils/data_prep/marginal_costs/bulk_tx_isone.py`.

For full background on the AESC study, the RNS rate, and the reasoning behind choosing AESC over the embedded RNS rate, see `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`.

---

## Hourly allocation

The $69/kW-year value must be converted to an 8760 hourly signal for CAIRO. The default is the month-hour lookup. The top-100-hour exceedance method, identical to RI, is the alternative (`bulk_tx_method=isone_aesc_ptf`).

### Month-hour allocation (default, ct_eversource)

`bulk_tx_method=month_hour` is the ct Justfile default (`generate_bulk_tx_mc.py --method month_hour`). It builds the 8760 from
OCC-863 Attachment 2 page 3 instead of from a load curve. Page 3 is the probability of
distribution peak by month, hour, and day type. Pages 1 and 2 turn that into $/kWh as

$$
MC_{m,h} = \frac{S \cdot PoP_{m,h}}{D_{m}}
$$

where S is the annual cost in $/kW-yr and D is that day type's day count for the month. S starts
as the AESC 69 $/kW-yr in 2024 dollars (`--aesc-ptf-kw-year`, `--ptf-dollar-year`). There is no
loss factor. `--target-dollar-year` (default `--year`; the ct recipe passes 2026, the same dollar
year as `create-dist-mc-data`) inflates S by CPIAUCSL when the years differ, the same rule as
distribution. The 8760 is then scaled so it sums to that inflated S, which is $69/kW-yr only when
the target year is 2024.

Weekday and weekend-and-holiday probabilities are applied separately, then looked up onto the
calendar year with the same Saturday/Sunday and federal-holiday rule as distribution
`month_hour` (`context/methods/marginal_costs/ct_eversource_dist_mc_methodology.md` §7). The
uniform factor corrects for the calendar's day counts differing from `num_days`. When they match,
that factor is 1 and the sum is already S.

The manifest key is `month_hour_occ863att2_aesc_ptf69_holidays_cal2025_2026usd`. The ct
recipe wires CL&P's OCC-863 probability files, so it runs for `ct_eversource` only. The generator
itself takes whatever probability paths it is given.

```bash
just -f ct/Justfile create-bulk-tx-mc-data ct_eversource 2025
just -f ct/Justfile include_holidays=false create-bulk-tx-mc-data ct_eversource 2025
```

### Top-100 exceedance (alternative)

`bulk_tx_method=isone_aesc_ptf` allocates the uninflated $69/kW-year the same way RI does:

1. Load the 8760 aggregate load curve (either NE system or CT zone, see below).
2. Identify the top 100 hours by load level.
3. Allocate the total annual cost across those 100 hours proportional to each hour's load magnitude above the threshold (exceedance weighting).
4. All other hours receive $0.

A customer who runs a kW of load during the top-100 peak hours sees a positive bulk TX marginal cost on those hours; all other hours are zero. The sum over all 8760 hours for a 1 kW flat load equals $69/kW-year. This method covers `ct_eversource` and `ct_ui`.

---

## Allocation load: system-wide vs. CT zone

This applies to `bulk_tx_method=isone_aesc_ptf` only. Month-hour does not use a load curve. The `generate_bulk_tx_mc.py` CLI accepts `--allocation-load`:

| Flag value     | Load used to identify peak hours    |
| -------------- | ----------------------------------- |
| `ne_system`    | All 8 ISO-NE zones summed (default) |
| `utility_zone` | CT zone load only                   |

**`ne_system` (default):** Peak hours are identified from the aggregate New England load curve. This is consistent with how RNS costs are allocated — they are triggered by NE-wide coincident peaks, not CT-specific peaks. A CT customer reducing load at a NE system peak avoids regional PTF cost regardless of whether CT individually peaks at that moment.

**`utility_zone`:** Peak hours are identified from CT zone load only. This would be appropriate if CT's transmission constraints are primarily local and CT's own peak is the relevant driver of PTF investment for CT. In practice, the NE system peak and the CT zone peak are highly correlated but not identical.

**Which to use:** `ne_system` is the default and the recommended primary value. The `utility_zone` option exists for sensitivity analysis and to explore how much the two signals diverge.

A comparison run (2025 ISO-NE data) found that the two signals share most peak hours, with a small set of hours where one signal is non-zero and the other is zero. The divergence is typically small relative to the total cost level, making the choice low-stakes for the BAT.

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

| Column                | Type    | Description                             |
| --------------------- | ------- | --------------------------------------- |
| `timestamp`           | String  | ISO 8601, hourly, target year           |
| `bulk_tx_cost_enduse` | Float64 | $/kW per hour (zero for non-peak hours) |

The output is validated at runtime. For month-hour, the sum of `bulk_tx_cost_enduse` over all 8760 hours (a hypothetical 1 kW constant load) must equal the CPI-adjusted annual within 0.01%, which is $69/kW-year only when the target dollar year is 2024. For exceedance there is no CPI, and that sum must equal `$69.00/kW-year`.

---

## Running the pipeline

From `rate_design/hp_rates/`:

```bash
# Default: month-hour for ct_eversource (dry run, then upload)
just s ct create-bulk-tx-mc-data ct_eversource 2025
just s ct create-bulk-tx-mc-data ct_eversource 2025 --upload

# Top-100 exceedance instead, including ct_ui
just s ct bulk_tx_method=isone_aesc_ptf create-bulk-tx-mc-data ct_eversource 2025 --upload
just s ct bulk_tx_method=isone_aesc_ptf create-bulk-tx-mc-data ct_ui 2025 --upload

# Exceedance with CT zone load instead of NE system load
just s ct bulk_tx_method=isone_aesc_ptf create-bulk-tx-mc-data ct_eversource 2025 --allocation-load utility_zone --upload
```

`create-bulk-tx-mc-data-all` loops `UTILITIES` from `state.env`. With the month-hour default that loop only succeeds for `ct_eversource`, because the recipe wires CL&P's OCC-863 files. Both utilities are in `supply_utils.py` as entries in `ISONE_UTILITY_ZONES`. External data sources still use historical identifiers (HIFLD: "CONNECTICUT LIGHT & POWER CO" / "UNITED ILLUMINATING CO"; EIA IDs 4176 / 19497).

---

## Implementation files

| File                                                      | Role                                                                                |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `utils/data_prep/marginal_costs/bulk_tx_isone.py`         | Core logic: `compute_isone_bulk_tx_signal`, `prepare_output`, `validate_allocation` |
| `utils/data_prep/marginal_costs/generate_bulk_tx_mc.py`   | CLI entrypoint; `--allocation-load` and `--method month_hour`                       |
| `utils/data_prep/marginal_costs/supply_utils.py`          | `ISONE_UTILITY_ZONES` mapping, `allocate_annual_exceedance_to_hours`                |
| `utils/data_prep/marginal_costs/supply_capacity_isone.py` | `load_isone_zone_loads`                                                             |
| `rate_design/hp_rates/ct/Justfile`                        | `create-bulk-tx-mc-data`, `create-bulk-tx-mc-data-all` recipes                      |
| `rate_design/hp_rates/ct/state.env`                       | `UTILITIES`, `REGION=isone`, `YEAR=2025`                                            |
| `tests/test_ri_bulk_tx_mc.py`                             | Tests for exceedance allocation, system vs. zone load modes                         |

---

## Comparison with RI

| Feature                 | RI                    | CT                                                         |
| ----------------------- | --------------------- | ---------------------------------------------------------- |
| Cost source             | AESC 2024 ($69/kW-yr) | AESC 2024 ($69/kW-yr, 2024 dollars)                        |
| Default allocation      | Top-100 exceedance    | Month-hour probability of peak, CPI to 2026 dollars        |
| Exceedance alternative  | (the only method)     | `bulk_tx_method=isone_aesc_ptf`, no CPI                    |
| Default allocation load | NE system             | None for month-hour; NE system when using exceedance       |
| Zone-load option        | Not used              | `--allocation-load utility_zone` on the exceedance method  |
| Utilities               | `rie`                 | `ct_eversource` (month-hour); both utilities on exceedance |
| ISO-NE zone label       | `RI`                  | `CT`                                                       |

The shared CLI still defaults to top-100 exceedance, which is what RI runs. CT's Justfile passes `--method month_hour` unless `bulk_tx_method` is set to `isone_aesc_ptf`. `--allocation-load` is only read by that exceedance path. It is available for RI as well, but RI currently only runs with the NE system default.

---

## Key references

- **AESC 2024 Report** [@synapse_AvoidedEnergySupply_2024]: https://www.synapse-energy.com/sites/default/files/AESC%202024.pdf
- **AESC avoided PTF ($69/kW-yr)** — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325)
- **AESC 2024 Materials** (User Interfaces, appendices): https://www.synapse-energy.com/aesc-2024-materials
- **RI methodology doc** (full AESC vs RNS discussion): `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`
- **RI cost recovery doc** (RNS/LNS mechanics, PTF allocation): `context/domain/marginal_costs/ri_bulk_transmission_cost_recovery.md`

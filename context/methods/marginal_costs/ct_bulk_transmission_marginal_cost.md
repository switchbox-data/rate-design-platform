# CT bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in Connecticut. CT follows the same ISO-NE framework as Rhode Island — the AESC 2024 avoided PTF cost allocated to peak hours via exceedance weighting — with one addition: a `--allocation-load` flag that lets you choose whether peak hours are identified from the NE system aggregate load or from the CT zone load alone.

---

## Summary

| Feature                     | Value                                               |
| --------------------------- | --------------------------------------------------- |
| Marginal cost source        | AESC 2024 avoided PTF cost                          |
| Value                       | $69/kW-year (same for all 6 NE states)              |
| Hourly allocation method    | Top-100-hour exceedance on load curve               |
| Default allocation load     | NE system aggregate (all 8 ISO-NE load zones)       |
| Alternative allocation load | CT zone load only                                   |
| ISO                         | ISO-NE                                              |
| Utilities covered           | `ct_eversource`, `ct_ui`                            |
| Output S3 path              | `s3://data.sb/switchbox/marginal_costs/ct/bulk_tx/` |

---

## Cost source: AESC 2024 avoided PTF cost

CT uses the same cost source as RI: the **Avoided Energy Supply Components (AESC) 2024** study by Synapse Energy Economics [@synapse_AvoidedEnergySupply_2024], which publishes an avoided PTF (Pool Transmission Facility) cost in $/kW-year for benefit-cost screening of efficiency and DER programs in New England.

**Key value:** `$69/kW-year` (AESC 2024, Table: Avoided T&D — PTF) — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325).

This is the LRMC of bulk transmission in ISO-NE territory: the cost deferred or avoided when a CT customer reduces peak demand by 1 kW during peak hours. The same $69/kW-year applies across all six NE states because PTF is a regional (pooled) cost. The value is implemented as `AESC_2024_AVOIDED_PTF_KW_YEAR` in `utils/data_prep/marginal_costs/bulk_tx_isone.py`.

For full background on the AESC study, the RNS rate, and the reasoning behind choosing AESC over the embedded RNS rate, see `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`.

---

## Hourly allocation

The $69/kW-year value must be converted to an 8760 hourly signal for CAIRO. The approach is the **top-100-hour exceedance** method, identical to RI and to the sub-TX/dist PoP methodology used elsewhere in the platform.

**How it works:**

1. Load the 8760 aggregate load curve (either NE system or CT zone, see below).
2. Identify the top 100 hours by load level.
3. Allocate the total annual cost ($69/kW-year) across those 100 hours proportional to each hour's load magnitude above the threshold (exceedance weighting).
4. All other hours receive $0.

The result: a customer who runs a kW of load during the top-100 peak hours sees a positive bulk TX marginal cost on those hours; all other hours are zero. The sum over all 8760 hours for a 1-kW flat load equals $69/kW-year (validated automatically at runtime).

---

## Allocation load: system-wide vs. CT zone

This is the CT-specific addition relative to RI. The `generate_bulk_tx_mc.py` CLI accepts `--allocation-load`:

| Flag value     | Load used to identify peak hours    |
| -------------- | ----------------------------------- |
| `ne_system`    | All 8 ISO-NE zones summed (default) |
| `utility_zone` | CT zone load only                   |

**`ne_system` (default):** Peak hours are identified from the aggregate New England load curve. This is consistent with how RNS costs are allocated — they are triggered by NE-wide coincident peaks, not CT-specific peaks. A CT customer reducing load at a NE system peak avoids regional PTF cost regardless of whether CT individually peaks at that moment.

**`utility_zone`:** Peak hours are identified from CT zone load only. This would be appropriate if CT's transmission constraints are primarily local and CT's own peak is the relevant driver of PTF investment for CT. In practice, the NE system peak and the CT zone peak are highly correlated but not identical.

**Which to use:** `ne_system` is the default and the recommended primary value. The `utility_zone` option exists for sensitivity analysis and to explore how much the two signals diverge. A comparison script is available at `dev/compare_ct_bulk_tx_allocation_loads.py`.

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

The output is validated at runtime: the sum of `bulk_tx_cost_enduse` over all 8760 hours (for a hypothetical 1-kW constant load) must equal `$69.00/kW-year` within 0.01%.

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

# Use CT zone load for allocation instead of NE system
just s ct create-bulk-tx-mc-data ct_eversource 2025 --allocation-load utility_zone --upload
```

Both `ct_eversource` and `ct_ui` are defined in `state.env` as `UTILITIES=ct_eversource,ct_ui` and in `supply_utils.py` as entries in `ISONE_UTILITY_ZONES`. External data sources still use historical identifiers (HIFLD: "CONNECTICUT LIGHT & POWER CO" / "UNITED ILLUMINATING CO"; EIA IDs 4176 / 19497).

---

## Implementation files

| File                                                      | Role                                                                                |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `utils/data_prep/marginal_costs/bulk_tx_isone.py`         | Core logic: `compute_isone_bulk_tx_signal`, `prepare_output`, `validate_allocation` |
| `utils/data_prep/marginal_costs/generate_bulk_tx_mc.py`   | CLI entrypoint; handles `--allocation-load` flag                                    |
| `utils/data_prep/marginal_costs/supply_utils.py`          | `ISONE_UTILITY_ZONES` mapping, `allocate_annual_exceedance_to_hours`                |
| `utils/data_prep/marginal_costs/supply_capacity_isone.py` | `load_isone_zone_loads`                                                             |
| `rate_design/hp_rates/ct/Justfile`                        | `create-bulk-tx-mc-data`, `create-bulk-tx-mc-data-all` recipes                      |
| `rate_design/hp_rates/ct/state.env`                       | `UTILITIES=ct_eversource,ct_ui`, `REGION=isone`, `YEAR=2025`                        |
| `dev/compare_ct_bulk_tx_allocation_loads.py`              | Compare system vs. CT-zone allocation signals                                       |
| `tests/test_ri_bulk_tx_mc.py`                             | Tests for exceedance allocation, system vs. zone load modes                         |

---

## Comparison with RI

| Feature                 | RI                    | CT                               |
| ----------------------- | --------------------- | -------------------------------- |
| Cost source             | AESC 2024 ($69/kW-yr) | AESC 2024 ($69/kW-yr)            |
| Allocation method       | Top-100 exceedance    | Top-100 exceedance               |
| Default allocation load | NE system             | NE system                        |
| Zone-load option        | Not implemented       | `--allocation-load utility_zone` |
| Utilities               | `rie`                 | `ct_eversource`, `ct_ui`         |
| ISO-NE zone label       | `RI`                  | `CT`                             |

The only material difference is the `--allocation-load` flag, which was added when implementing CT to allow sensitivity analysis. It is available for RI as well (the same CLI flag exists), but RI currently only runs with the NE system default.

---

## Key references

- **AESC 2024 Report** [@synapse_AvoidedEnergySupply_2024]: https://www.synapse-energy.com/sites/default/files/AESC%202024.pdf
- **AESC avoided PTF ($69/kW-yr)** — [DocumentCloud p. 288](https://www.documentcloud.org/documents/28039756-synapse-2024-avoided-energy-supply-components-in-new-england-2024-repor/#document/p288/a2811325)
- **AESC 2024 Materials** (User Interfaces, appendices): https://www.synapse-energy.com/aesc-2024-materials
- **RI methodology doc** (full AESC vs RNS discussion): `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md`
- **RI cost recovery doc** (RNS/LNS mechanics, PTF allocation): `context/domain/marginal_costs/ri_bulk_transmission_cost_recovery.md`

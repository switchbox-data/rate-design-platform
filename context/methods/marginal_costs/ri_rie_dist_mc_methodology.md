# RIE sub-transmission + distribution marginal cost (Rhode Island)

How we set the `sub_tx_and_dist` BAT marginal-cost input for Rhode Island Energy (RIE, formerly National Grid's Narragansett Electric, ISO-NE). This is the implementation note. For why RI uses an avoided-cost value while NY uses FLIC, see [dist_mc_definition_choice.md](dist_mc_definition_choice.md). RI bulk transmission is a separate component; see [ri_bulk_transmission_marginal_cost.md](ri_bulk_transmission_marginal_cost.md).

---

## 1. Value and approach

RIE's `sub_tx_and_dist` marginal cost is **`$80.24/kW-yr`**, RIE's own avoided distribution cost for evaluating and screening demand-side programs. RI has no MCOS workbook, so there is no project-level build like NY's. The single published scalar goes in a config CSV, is CPI-adjusted to the run year, and is spread over RIE's top 100 load hours.

This is the avoided / AIC definition of LRMC, the same definition MD uses for BGE.

---

## 2. Source and dollar year

The value comes from **AESC 2024, Table 130** ("Summary of utility avoided T&D cost methodologies", report p. 288; [AESC 2024 report, May re-release](https://www.synapse-energy.com/sites/default/files/inline-images/AESC%202024%20May%202024.pdf)). Table 130 is Synapse's survey of the values each New England utility was already using. It is not an AESC-calculated avoided cost. The RI column lists:

| Field in Table 130                          | RI                  |
| ------------------------------------------- | ------------------- |
| Existing value of avoided distribution cost | `$80.24/kW` (2019$) |
| Year developed                              | 2019                |

RIE derived the number as distribution investment times a carrying charge, divided by peak load growth. RIE's response to Division 2-5 in RIPUC Docket No. 24-06-EE (April 12, 2024) gives the inputs for its 2020/2021 energy-efficiency plans: `$142,202,847 × 16.5% ÷ 293 MW = $80.24/kW-yr`.

**The dollar year is not settled.** The config uses 2019, following Table 130. RIE's own response to Division 1-1 in the same docket (March 21, 2024) labels `$80.24` as **2017 dollars**, and its Division 2-5 table says the dollar values are nominal. A 2017 base would raise the CPI-adjusted value by the 2017-to-2019 change in CPI. The CPI snapshot on S3 starts in 2019, so switching the config to 2017 also requires refreshing CPI data (`data/fred/cpi/`).

The same RIE responses show the value RIE used in later plan years: `$100.02` (2021$) in 2022, `$121.58`(2022$) in 2023, and` $174.41` (2023$) in 2024. The config still uses the 2020/2021 value.

---

## 3. Config and CPI handling

Stored at `rate_design/hp_rates/ri/config/marginal_costs/ri_marginal_costs_2025.csv`:

```csv
utility,sub_tx_and_dist_mc_kw_yr,dollar_year
rie,80.24,2019
```

[run_allocation.py](../../../utils/data_prep/marginal_costs/run_allocation.py) reads the `dollar_year` column (`inputs/generic.py`) and CPI-inflates (CPIAUCSL, latest vintage under `s3://data.sb/fred/cpi/`) from 2019 to the target dollar year, which defaults to `--year`. For the 2025 run the factor is about 1.2594, so the allocated value is about `$101.05/kW-yr`. No carrying-charge math is applied on our side.

`rate_design/hp_rates/ri/state.env` sets `MC_TABLE` to an absolute path under another user's home directory. Without it, the shared Justfile falls back to the repo CSV above.

---

## 4. Hourly allocation

The CPI-adjusted value is spread with the peak N load hours allocation ([top_n_load_share_allocation.md](top_n_load_share_allocation.md)). The RI inputs:

| Input       | RI value                                                                                                                                                                                                  |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Load curve  | RIE hourly load from `s3://data.sb/isone/hourly_demand/utilities/utility=rie/`, the ISO-NE RI load zone (location 4005) relabeled as `rie` by `data/isone/hourly_demand/aggregate_isone_utility_loads.py` |
| Load year   | `DIST_LOAD_YEAR`, unset in `ri/state.env`, so it defaults to `YEAR` (2025)                                                                                                                                |
| Annual cost | The CPI-adjusted `$/kW-yr` from §3                                                                                                                                                                        |
| N           | 100 (`UPSTREAM_HOURS` in `ri/state.env`)                                                                                                                                                                  |

The writer aligns the load to the CAIRO 8760 with `normalize_load_to_cairo_8760` before allocating. On the 2025 load, the top 100 hours are 32 in June, 60 in July, and 8 in August. The peak hour (2025-06-24 16:00) is about `1.17 $/kWh`, and the 100th hour is about `0.94 $/kWh`.

These inputs are also recorded in `rate_design/hp_rates/ri/config/marginal_costs/allocation.yaml`.

---

## 5. Output and rebuilding

Output: `s3://data.sb/switchbox/marginal_costs/ri/dist_and_sub_tx/utility=rie/year=2025/data.parquet`, with columns `timestamp`, `utility`, `year`, and `mc_total_per_kwh`. The RI scenario and pipeline YAMLs read this path. The partition has no `manifest.yaml`; the file predates the manifest system.

Rebuild from `rate_design/hp_rates/`:

```bash
just s ri create-dist-and-sub-tx-mc-data
```

The recipe always passes `--upload`, so it overwrites the S3 file and writes a manifest beside it. To inspect without writing, call `run_allocation.py` without `--upload`.

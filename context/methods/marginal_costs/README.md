# Marginal-cost methods

Read one row, then open that file. This page names the allocation and where its inputs are chosen. The linked writeup holds the formula.

## Allocation methods

Each method turns an annual `$/kW-yr` (or a month-hour table) into an hourly series. The state, utility, or ISO wrapper chooses the dollar amount and the load curve.

| Method                  | What it does                                                                                                                                                                                                                                                                                                | Code                                 | Used for                                                                                                                                                   | Writeup                                                                        |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Top-N load-share        | The top N hours, each weighted by its load. Every other hour is 0.                                                                                                                                                                                                                                          | `methods/top_n_load_share.py`        | Distribution, N = 100 (NY, RI, MD, and the CT `pop_occ863` / `pop_isone_zone` runs). MD bulk transmission, N = 150.                                        | [top_n_load_share_allocation.md](top_n_load_share_allocation.md)               |
| Top-N exceedance        | The top N hours, each weighted by how far its load sits above the threshold (the highest load strictly below the Nth hour). A tie at the cutoff can leave fewer than N hours with a positive cost.                                                                                                          | `methods/top_n_exceedance.py`        | RI bulk transmission and the CT `isone_aesc_ptf` run, N = 100, AESC `$69/kW-yr` as filed. ISO-NE supply capacity (FCA).                                    | [top_n_exceedance_allocation.md](top_n_exceedance_allocation.md)               |
| Seasonal SCR exceedance | The top N hours of summer and of winter. Each season's share of the annual cost comes from how far its peak sits above the lower of the two thresholds; within the season, hours are weighted by exceedance. A tie at a season's cutoff can leave that season with fewer than N hours with a positive cost. | `methods/seasonal_scr_exceedance.py` | NY bulk transmission, 40 hours per season, winter October–March.                                                                                           | [seasonal_scr_exceedance_allocation.md](seasonal_scr_exceedance_allocation.md) |
| KDE probability of peak | A month × day-type × hour probability from one or more years of load, placed on the 8760 as `$/kW-yr` × probability / days in the cell.                                                                                                                                                                     | `methods/pop_kde.py`                 | CT bulk transmission default (`pop_kde`).                                                                                                                  | [probability_of_peak_kde.md](probability_of_peak_kde.md)                       |
| Month-hour expansion    | A 288-row month-hour table looked up onto one calendar year, then scaled so the 8760 sums to a `$/kW-yr`. Weekends and, by default, federal holidays take the weekend table.                                                                                                                                | `methods/month_hour.py`              | CT distribution default (`month_hour`, published `$/kWh` tables). CT bulk transmission `month_hour` (published probability of peak, `ct_eversource` only). | [month_hour_allocation.md](month_hour_allocation.md)                           |

Code paths are under `utils/data_prep/marginal_costs/`.

## How to run

One runner reads the state's `allocation.yaml`, resolves each `from:` input, calls the method, checks the 8760, and writes on `--upload`.

```bash
uv run python utils/data_prep/marginal_costs/run_allocation.py \
  --config rate_design/hp_rates/<state>/config/marginal_costs/allocation.yaml \
  --component dist_and_sub_tx \
  --utility <utility> \
  --year <year> \
  --load-year <year> \
  --upload
```

`--variant` selects an `alternatives` entry. The state Justfiles call this through `create-mc-data`. The older recipe names (`create-dist-and-sub-tx-mc-data`, `create-bulk-tx-mc-data`, CT `create-dist-mc-data`) are aliases.

Readers live in `utils/data_prep/marginal_costs/inputs/`. `generic.py` covers a literal, an MC-table cell, CPI, one parquet, the `utility=/year=` hive, and a periods YAML. `isone.py` reads the ISO-NE zone hive (`isone_zone_load`). `pjm.py` blends a NITS rate and reads PJM load with the old null-fill (`pjm_hive_load`). `nyiso.py` returns one `{load, annual_cost_kw_year}` per utility ICAP row (`nyiso_scr_components`); the runner calls seasonal SCR once per row and sums the cost column.

## Adding a state

| Situation                                                                   | New code                                                                                  |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| A state in an existing ISO, existing method, inputs on S3 in a known layout | An `allocation.yaml`. No Python.                                                          |
| Same, but a filed table arrives as an Excel workbook                        | A `convert_*.py` script that writes parquet, plus the config.                             |
| A new ISO whose `$/kW-yr` or load cannot be read with the generic loaders   | `inputs/<iso>.py` with one function, registered in `inputs/__init__.py`, plus the config. |
| A new allocation formula                                                    | A new `methods/<name>.py` script, with tests, plus the config.                            |

## Other files

Open one of these when the task is about a state's inputs, a study comparison, or which marginal-cost definition to use. The allocation formula is in the table above.

| File                                      | Open when                                                                                  |
| ----------------------------------------- | ------------------------------------------------------------------------------------------ |
| ny_dist_mc_bat_methodology.md             | Deriving the NY sub-transmission and distribution `$/kW-yr` from the seven MCOS workbooks. |
| ny_mcos_studies_comparison.md             | Comparing the seven NY 2025 MCOS studies.                                                  |
| ny_bulk_transmission_marginal_cost.md     | Choosing a NY bulk-transmission `$/kW-yr`, and the implemented seasonal SCR inputs.        |
| ri_bulk_transmission_marginal_cost.md     | RI bulk transmission: AESC and RNS, and the implemented exceedance inputs.                 |
| ri_rie_dist_mc_methodology.md             | RIE distribution `$/kW-yr` and the top-100 load-share inputs.                              |
| ct_bulk_transmission_marginal_cost.md     | CT bulk transmission: `pop_kde` (default), `month_hour`, and `isone_aesc_ptf`.             |
| ct_eversource_dist_mc_methodology.md      | Eversource distribution: month-hour default and the two load-share alternatives.           |
| md_bulk_transmission.md                   | MD bulk transmission: the NITS blend and the top-150 load-share inputs.                    |
| md_bge_dist_mc_methodology.md             | BGE distribution `$/kW-yr` and the top-100 load-share inputs.                              |
| dist_mc_definition_choice.md              | Which long-run marginal-cost definition each state's distribution input uses.              |
| capacity_market_comparison_nyiso_isone.md | NYISO ICAP compared with the ISO-NE forward capacity market.                               |

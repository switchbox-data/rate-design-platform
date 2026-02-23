# ResStock vs EIA-861 load comparison

**Use when:** Validating ResStock total load per utility against EIA-861 residential sales, or checking per-utility coverage and scale.

## Purpose

`utils/post/compare_resstock_eia861_loads.py` reads ResStock **annual** load curves (`load_curve_annual`): a single parquet per state/upgrade with one row per building and annual kWh columns. It multiplies each building’s electricity kWh by its **sample weight** (each building represents ~252 dwellings in ResStock 2024.2), joins to utility assignment for `sb.electric_utility`, then sums **weighted** kWh by utility. It compares these totals to **EIA-861 residential sales** (MWh → kWh) and outputs ratio and percent difference.

- **ResStock source:** Single parquet per state/upgrade:\
  `s3://data.sb/nrel/resstock/<release>/load_curve_annual/state=<state>/upgrade=<upgrade>/<state>_upgrade<upgrade>_metadata_and_annual_results.parquet`\
  Columns used: `bldg_id`, **`out.electricity.total.energy_consumption.kwh`** (annual electricity kWh), **`weight`** (sample weight).
- **Utility assignment:**\
  `s3://data.sb/nrel/resstock/<release>/metadata_utility/state=<state>/utility_assignment.parquet`\
  Must include `bldg_id` and `sb.electric_utility`.
- **EIA-861 source:**\
  `s3://data.sb/eia/861/electric_utility_stats/year=<year>/state=<state>/data.parquet`\
  (columns `utility_code`, `residential_sales_mwh`). Default **year=2018** to align with ResStock AMY 2018 (use `--eia-year` to override).

Output columns: `utility_code`, `resstock_total_kwh`, `eia_residential_kwh`, `ratio` (ResStock/EIA), `pct_diff` (percent difference).

## How to run

From repo root:

```bash
# RI (single utility)
uv run python utils/post/compare_resstock_eia861_loads.py --state RI --output comparison_ri.csv

# NY (all NY utilities)
uv run python utils/post/compare_resstock_eia861_loads.py --state NY --output comparison_ny.csv
```

- **Required:** `--state` (e.g. `RI`, `NY`).
- **Optional:** `--eia-year` (default `2018`, to match ResStock AMY 2018), `--resstock-release` (default `res_2024_amy2018_2`), `--upgrade` (default `00`), `--path-annual` (override path to the annual parquet), `--path-utility-assignment`, `--path-eia861`, `--output <path>`, `--load-column` (annual parquet column for electricity kWh; default `out.electricity.total.energy_consumption.kwh`).

## Runtime

Runs use **one parquet per state** (load_curve_annual), so both RI and NY complete in a few seconds.

## Results (weighted)

Run date: 2026-02-22. Release: `res_2024_amy2018_2`, upgrade `00`. EIA-861 **year=2018** (matches ResStock AMY 2018). Totals are **weighted** (annual kWh × `weight` per building).

### RI

| utility_code | resstock_total_kwh | eia_residential_kwh | ratio | pct_diff |
| ------------ | ------------------ | ------------------- | ----- | -------- |
| rie          | 3,662,740,818      | 3,084,918,000       | 1.19  | +18.7%   |

### NY (all utilities)

| utility_code | resstock_total_kwh | eia_residential_kwh | ratio | pct_diff |
| ------------ | ------------------ | ------------------- | ----- | -------- |
| coned        | 24,450,457,927     | 14,263,892,000      | 1.71  | +71.4%   |
| nimo         | 14,979,149,197     | 12,068,863,000      | 1.24  | +24.1%   |
| psegli       | 10,158,240,936     | 9,538,864,000       | 1.06  | +6.5%    |
| nyseg        | 8,435,994,664      | 6,973,256,000       | 1.21  | +21.0%   |
| rge          | 3,373,455,500      | 2,821,548,000       | 1.20  | +19.6%   |
| cenhud       | 2,741,261,531      | 2,165,887,000       | 1.27  | +26.6%   |
| or           | 1,767,698,315      | 1,668,063,000       | 1.06  | +6.0%    |

ResStock weighted totals are generally above EIA residential; ConEd shows the largest ratio (1.71). Orange & Rockland (or) and PSEG LI (psegli) are closest to 1:1.

## Interpretation

- **Ratio ≈ 1:** Weighted ResStock total is close to EIA residential sales for that utility.
- **Ratio > 1:** ResStock weighted sum exceeds EIA (sample/weighting or coverage differences).
- **Ratio < 1:** ResStock weighted sum below EIA (e.g. missing buildings or weights).

For further scaling (e.g. to match utility customer counts), use the platform’s reweighting and customer-count logic elsewhere; this script uses ResStock’s built-in sample weight only.

## Hypotheses for discrepancies

Possible reasons ResStock weighted totals are often above EIA-861 residential (ratios 1.01–1.79), with rough plausibility:

| Hypothesis                                                  | Plausibility | Notes                                                                                                                                                                                                                                       |
| ----------------------------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ResStock weight is national, not state/utility-specific** | High         | Weight (~252) scales to U.S. housing stock / 550k buildings. If a state or utility has a different share of U.S. housing than the sample implies, weighted totals will be off. Could explain ConEd’s 1.79×.                                 |
| **Different “residential” definitions**                     | High         | EIA-861 “residential” is what utilities report under that class; ResStock is housing-unit–based (SFD, SFA, MF, mobile). Different universes (e.g. small commercial in EIA, or MF/group quarters treated differently) can shift totals.      |
| **Geographic / utility-boundary mismatch**                  | Medium       | Buildings are assigned to utilities via county/PUMA and territory maps. If boundaries don’t match how the utility reports (e.g. partial counties, LIPA vs PSEG LI), we sum the wrong buildings to each utility. Could over-assign to ConEd. |
| **Weather / vintage year mismatch**                         | Medium       | ResStock uses AMY 2018; EIA-861 is reported by year (e.g. 2023). Different years or weather can create gaps.                                                                                                                                |
| **Sample design: over-representation of high-use segments** | Medium       | If high-electric-use housing is over-sampled or gets higher weight, weighted kWh will be biased up vs EIA.                                                                                                                                  |
| **Orange & Rockland (or) ≈ 1.01 is partly luck**            | Medium       | One utility near 1:1 may reflect better alignment for that territory or smaller customer base; doesn’t imply the method is correct everywhere.                                                                                              |
| **EIA-861 under-reporting or misclassification**            | Low–Medium   | Residential could be under-reported or miscoded; would inflate ratio. Less likely to explain the pattern across utilities unless reporting quality varies.                                                                                  |
| **End-use scope (e.g. net metering, PV)**                   | Low–Medium   | ResStock is site total electricity; EIA is delivered sales. Net metering/PV can cause small differences; unlikely to explain 20–80% gaps alone.                                                                                             |

**Data sources to check:** EIA column used is **`residential_sales_mwh`** (× 1000 → kWh). ResStock weights are the **`weight`** column in the load_curve_annual parquet (per-building sample weight), not a per-utility multiplier.

## EIA-861 / PUDL source and report year

Our EIA-861 numbers come from the **year- and state-partitioned parquet** produced by `data/eia/861/fetch_electric_utility_stat_parquets.py`, which reads PUDL’s (Catalyst Cooperative) **stable** release of EIA-861 yearly sales.

### Layout and which year we use

- **S3 layout:** `s3://data.sb/eia/861/electric_utility_stats/year=<year>/state=<state>/data.parquet`. The fetch script writes **all report years** (e.g. 2001–2024) so each year is a separate partition.
- **This comparison:** The compare script defaults to **`--eia-year 2018`** so EIA-861 residential sales match the **ResStock AMY 2018** weather year. Use `--eia-year` to compare against another report year, or `--path-eia861` to point at a specific file.
- **Source URL (stable):**\
  `https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/v2026.2.0/core_eia861__yearly_sales.parquet`\
  (see `PUDL_STABLE_VERSION` in the fetch script). PUDL EIA-861 coverage is **2001–2024** ([PUDL docs](https://catalystcoop-pudl.readthedocs.io/en/latest/data_sources/eia861.html)).

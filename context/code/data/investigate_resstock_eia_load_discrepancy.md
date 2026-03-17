# Investigate ResStock vs EIA load discrepancy (by utility)

**Use when:** Understanding why ResStock totals diverge from EIA-861 residential sales by utility, or applying a multifamily (MF) non-HVAC adjustment to better align ResStock with EIA.

## Purpose

`utils/post/investigate_resstock_eia_load_discrepancy.py` loads ResStock **annual** load curves and metadata, joins to utility assignment and EIA-861 residential sales, and:

1. Compares ResStock vs EIA load **by utility**, normalized by customer count (so percent difference reflects consumption intensity, not customer-count mismatch).
2. Performs a **column-by-column** single-family (SF) vs multifamily (MF) comparison: electrical kWh per floor area (sqft) for each electricity end-use column, using only buildings with **non-zero** consumption for that column.
3. Computes **per-column MF/SF ratios** (mean kWh/sqft, non-zero only) for **non-HVAC** columns and applies them as an adjustment: for MF buildings, each non-HVAC column is scaled by `value / ratio` (HVAC columns are unchanged).
4. Re-aggregates by utility and plots **percent difference (ResStock − EIA) / EIA** vs **multifamily share** before and after the adjustment.

The script produces plots and printed statistics to assess whether the MF non-HVAC adjustment improves alignment with EIA at the utility level.

## Data sources and paths

- **ResStock annual:** `s3://data.sb/nrel/resstock/<release>/load_curve_annual/state=<state>/upgrade=<upgrade>/<state>_upgrade<upgrade>_metadata_and_annual_results.parquet`\
  Uses `bldg_id`, `weight`, `out.electricity.total.energy_consumption.kwh`, plus HVAC and non-HVAC component columns (see below).
- **Utility assignment:** `s3://data.sb/nrel/resstock/<release>/metadata_utility/state=<state>/utility_assignment.parquet`\
  Must include `bldg_id` and `sb.electric_utility`.
- **ResStock metadata:** `s3://data.sb/nrel/resstock/<release>/metadata/state=<state>/upgrade=<upgrade>/metadata-sb.parquet`\
  Used for `in.geometry_building_type_recs`, `in.geometry_floor_area` (SF vs MF, floor area for kWh/sqft).
- **EIA-861:** `s3://data.sb/eia/861/electric_utility_stats/year=<year>/state=<state>/data.parquet`\
  Default `year=2018` to align with ResStock AMY 2018.

Defaults: release `res_2024_amy2018_2`, upgrade `00`, state `NY` (hardcoded in `if __name__ == "__main__"`).

## Electricity column groups

- **HVAC-related:** cooling, cooling_fans_pumps, heating, heating_fans_pumps, heating_hp_bkup, heating_hp_bkup_fa, mech_vent.\
  Not adjusted; only non-HVAC columns are scaled for MF.
- **Non-HVAC-related:** ceiling_fan, clothes_dryer, clothes_washer, dishwasher, freezer, hot_water, lighting (exterior/garage/interior), permanent_spa (heat/pump), plug_loads, pool (heater/pump), pv, range_oven, refrigerator, well_pump.\
  Each has a **MF/SF ratio** (mean kWh/sqft, non-zero only). For MF buildings, `new_value = value / ratio`; ratio 1.0 or missing → no change.

Total non-HVAC and `annual_kwh` are recomputed from the (possibly adjusted) component columns after the adjustment.

## Workflow (main block)

1. **Load:** ResStock annual by utility, ResStock annual at building level (with HVAC/non-HVAC sums), metadata with utility, EIA by utility.
2. **Building-type shares:** For each utility, compute multifamily_pct and single_family_pct (and mobile_home_pct) from `in.geometry_building_type_recs`.
3. **Original comparison:** `compare_resstock_eia_by_utility` → join ResStock and EIA, normalize ResStock kWh by `eia_customers / resstock_customers`, compute `kwh_ratio` and `kwh_pct_diff`. Fit and plot `kwh_pct_diff` vs `multifamily_pct` (original).
4. **SF vs MF column-by-column:** `print_sf_mf_column_by_column_floor_area_comparison` — for each electricity column, kWh/sqft for SF vs MF (only non-zero consumers), print difference (MF−SF), ratio (MF/SF), and Welch t-test significance.
5. **Aggregate non-HVAC difference-of-means:** `calculate_total_non_hvac_related_electricity_kwh` for "multifamily" and "single_family"; Welch t-test on `total_non_hvac_kwh_by_floor_area`; print aggregate MF/SF ratio.
6. **Per-column ratios:** `get_non_hvac_mf_to_sf_ratios` — same non-zero, per-sqft logic restricted to non-HVAC columns; returns `dict[column_name, ratio]`. Ratio is 1.0 when either group has &lt;2 non-zero samples.
7. **Adjust MF:** `adjust_mf_electricity(resstock_annual, metadata_with_utility, non_hvac_column_ratios)` — for MF bldg_ids, each non-HVAC column is divided by its ratio; recompute totals and `annual_kwh`; recompute `weighted_kwh`.
8. **Adjusted comparison:** `group_resstock_annual_by_utility` on adjusted frame → `compare_resstock_eia_by_utility` → fit and plot `kwh_pct_diff` vs `multifamily_pct` (adjusted).

Outputs: `kwh_pct_diff_vs_multifamily_original.png`, `kwh_pct_diff_vs_multifamily_adjusted.png`, and printed fit stats and column-by-column comparison.

## Why the multifamily non-HVAC adjustment helps

- ResStock assigns end-use and appliance characteristics using RECS-based logic; RECS is dominated by single-family homes, so multifamily buildings tend to get “SF-like” appliance and usage assumptions (e.g. per-dwelling plug loads, laundry) that overstate non-HVAC consumption when normalized by floor area.
- Shared or building-level systems (e.g. shared laundry, common-area lighting) are not modeled the same way as in SF; per-dwelling or per-sqft non-HVAC in MF is often lower in reality than in the simulation.
- The **per-column MF/SF ratio** (mean kWh/sqft, restricting to buildings with non-zero consumption for that column) captures how much higher ResStock simulates MF vs SF for that end-use. Scaling MF values down by this ratio brings simulated MF non-HVAC in line with the observed SF-equivalent intensity, which improves utility-level alignment with EIA when utilities have different MF shares.
- **Non-zero filtering** avoids diluting the ratio with buildings that don’t use that end-use (e.g. no pool); the ratio then reflects “conditional on having this load” and is more stable. HVAC is left unchanged because the script does not apply an HVAC-specific adjustment (by design; see below).

## Design decisions (from implementation history)

- **No HVAC-specific adjustment:** Only non-HVAC columns are scaled for MF. HVAC-related columns are left as simulated (no separate heat-pump or electrical-resistance MF scaling).
- **Column-by-column non-HVAC only:** The MF adjustment uses one ratio per non-HVAC column (from `get_non_hvac_mf_to_sf_ratios`), not a single aggregate non-HVAC ratio. This allows end-uses with very different MF/SF patterns (e.g. plug_loads vs clothes_dryer) to be corrected separately.
- **Ratio 1.0 when insufficient data:** If either SF or MF has fewer than two buildings with non-zero consumption for a column, that column’s ratio is set to 1.0 (no adjustment) to avoid unstable or undefined ratios.
- **Customer-count normalization:** Comparison is done after normalizing ResStock total kWh by (EIA customers / ResStock customers), so `kwh_pct_diff` reflects per-customer consumption difference, not customer-count mismatch. See `compare_resstock_eia_by_utility`.

## Key functions

| Function                                             | Purpose                                                                                                                                   |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `load_data`                                          | One entrypoint: loads ResStock annual by utility, building-level annual, metadata with utility, metadata_by_utility dict, EIA by utility. |
| `compare_resstock_eia_by_utility`                    | Join ResStock and EIA by utility; normalize ResStock kWh by customer count; return `kwh_ratio`, `kwh_pct_diff`, customer stats.           |
| `building_type_share_by_utility`                     | For each utility, multifamily_pct, single_family_pct, mobile_home_pct from metadata.                                                      |
| `print_sf_mf_column_by_column_floor_area_comparison` | Print SF vs MF kWh/sqft per electricity column (non-zero only), difference, ratio, Welch p-value.                                         |
| `get_non_hvac_mf_to_sf_ratios`                       | Return dict of non-HVAC column → MF/SF mean kWh/sqft (non-zero only); 1.0 if insufficient samples.                                        |
| `adjust_mf_electricity`                              | For MF bldg_ids, scale each non-HVAC column by 1/ratio; recompute total_non_hvac, annual_kwh, weighted_kwh.                               |
| `fit_kwh_pct_diff_vs_multifamily_pct`                | Linear fit of `kwh_pct_diff` on `multifamily_pct`; returns slope, intercept, R², F-test, etc.                                             |
| `plot_kwh_pct_diff_vs_multifamily_pct`               | Scatter of `kwh_pct_diff` vs `multifamily_pct` with utility labels and regression line.                                                   |

Helpers: `_bldg_ids_for_building_type`, `_parse_floor_area_sqft` (e.g. `750-999` → midpoint; `4000+` → 6000), `two_sample_difference_of_means_test` (Welch t-test).

## Relationship to compare_resstock_eia861_loads

- **`compare_resstock_eia861_loads.py`** (see `context/code/data/compare_resstock_eia861_loads.md`): Simpler script; outputs a CSV of ResStock vs EIA totals and customer counts by utility (optionally with `--normalize-by-customer-count`). No MF adjustment, no SF/MF analysis.
- **`investigate_resstock_eia_load_discrepancy.py`**: Investigation and correction; adds building-type and floor-area logic, SF vs MF comparison, non-HVAC column-wise ratios, and MF adjustment, then plots percent difference vs multifamily share before/after.

For quick utility-level totals and ratios, use `compare_resstock_eia861_loads`. For understanding and reducing the MF-related bias, use `investigate_resstock_eia_load_discrepancy`.

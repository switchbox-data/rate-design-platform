# Demand flex: seasonal elasticity and TOU derivation path resolution

## Purpose

This note records how **seasonal ε** reaches **`apply_runtime_tou_demand_response`** via **TOU derivation JSON** paths (**`find_tou_derivation_path`**), why **YAML `winter` / `summer`** must align with **`load_season_specs`** season names, and how a **wrong or missing** derivation file yields **ε = 0** effective flex (run **15** ≈ run **11**).

**Incident write-up (NiMo \$2.1M outlier, PV / near-zero period division, `FLEX_SHIFT_MIN_PERIOD_ABS_KWH`, commits `185519c` / `fee4706`):** **`context/code/cairo/nimo_flex_demand_charge_regression.md`**.

When comparing NY batches (e.g. delivery run **15** with flex): huge **`bill_level`** on the delivery-only run while supply run **16** is unchanged, master **`elec_total_bill`** stable, and **`elec_delivery_bill` / `elec_supply_bill`** split pathological **can** come from **bad shifted hourly loads** after flex (see incident doc). **Demand charges** on **`nimo_hp_seasonalTOU_flex_calibrated`** are **not** enabled (**`ur_dc_enable = 0`** on resolved configs)—do not default to demand-charge theory for that tariff.

## Artifacts (reference)

| Artifact                             | Column / field                                              | What drifted                                                                                         |
| ------------------------------------ | ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `bills/elec_bills_year_target.csv`   | `bill_level`, `month`                                       | Run **15** (delivery, flex) monthly and Annual exploded; run **16** matched across batches           |
| `demand_flex_elasticity_tracker.csv` | `bldg_id`, pivoted `summer_period_*`, `winter_period_*`     | Tracker present on newer runs; null ε in some cells is often data, not billing                       |
| Master `comb_bills_year_target`      | `elec_delivery_bill`, `elec_supply_bill`, `elec_total_bill` | `elec_total_bill` = run **16** `bill_level`; delivery/supply split derived from run **15** vs **16** |

Master decomposition (**`utils/post/build_master_bills.py`**):

- **`bill_delivery`**: `elec_bills_year_target.bill_level` from the **delivery** run directory (**~456–459**).
- **`bill_supply`**: same from the **supply** run (**~460–461**).
- **`elec_total_bill`**: set to **`bill_supply`** only (**~461**), so identical run **16** ⇒ identical total.
- **`elec_delivery_bill`** = `bill_delivery − elec_fixed_charge` (**~457–458**).
- **`elec_supply_bill`** = `bill_supply − bill_delivery` (**~460**).

So a corrupt **run 15** `bill_level` forces a bogus delivery/supply split while preserving total.

## Commits (timeline)

| Commit        | Area                                                      | Relevance                                                                                                                                                                                                                          |
| ------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`185519c`** | `utils/cairo.py`, `utils/demand_flex.py`                  | Per-season ε in **`apply_runtime_tou_demand_response`** via **`_shift_season`** **`demand_elasticity.get(season_name, 0.0)`** (~**1052–1058**)                                                                                     |
| **`fee4706`** | `rate_design/hp_rates/run_scenario.py`                    | YAML **`elasticity`** mapping parsed (**~272–279**); **`apply_demand_flex(..., elasticity=...)`** (**~653–668**). Before this, **`_parse_float`** on a dict fails (**`utils/scenario_config.py`** **`_parse_float`** ~**481–490**) |
| **`ce75b14`** | NY scenario YAMLs                                         | Flex runs use **`elasticity: { summer: …, winter: … }`** (e.g. **`rate_design/hp_rates/ny/config/scenarios/scenarios_nimo.yaml`** runs **15** / **16**)                                                                            |
| **`f56475e`** | `create_scenario_yamls` / periods YAML                    | Which ε block is written into generated YAML                                                                                                                                                                                       |
| **`b62117e`** | `utils/mid/patches.py`, `run_scenario`, `scenario_config` | EPMC / residual / BAT-style hooks — **weak** direct explanation for **`elec_bills_year_target.bill_level`** vs identical run **16**                                                                                                |

## Root cause: YAML `all` vs runtime tariff key (stems)

**`utils/scenario_config._parse_path_tariffs`** (**~375–405**) builds **`path_tariffs_electric`** with **keys = JSON filename stems**, not the YAML dict keys (`all`, `hp`, …). So **`apply_demand_flex`** iterates **`tou_key`** in **`{"nimo_hp_seasonalTOU_flex_calibrated", …}`**, and **`tariff_map_df["tariff_key"] == tou_key`** matches the map CSV.

**Delivery** stem **`nimo_hp_seasonalTOU_flex_calibrated`**: **`find_tou_derivation_path`** strips **`_calibrated`** / **`_flex`** → base **`nimo_hp_seasonalTOU`** → **`nimo_hp_seasonalTOU_derivation.json`** exists → **`load_season_specs`** → season names **`winter` / `summer`** align with YAML **`elasticity`**.

**Supply** stem **`nimo_hp_seasonalTOU_flex_supply_calibrated`**: same strip → base **`nimo_hp_seasonalTOU_supply`**. There is usually **no** **`…_supply_derivation.json`**. Then **`season_specs`** is **`None`**, **`_infer_season_groups_from_tariff`** uses **`season_1`**, **`season_2`**, … → **`demand_elasticity.get("season_1", 0.0)`** → **ε = 0** for intended **`{ winter, summer }`**. So **supply** runs often apply **no** seasonal shift while **delivery** runs **do**, which can distort the delivery vs supply decomposition in master bills even when run **16** totals look sane.

**Erratum (reverted):** A filename-stem **fallback** that reused the **delivery** derivation JSON when resolving the **supply** stem was **unsafe**: seasonal hour slices from the delivery TOU derivation must not be applied while shifting load priced against the **supply** tariff JSON — that pairing can drive **`bill_level`** into the millions on supply runs.

NiMo seasonal TOU derivation on disk: **`rate_design/hp_rates/ny/config/tou_derivation/nimo_hp_seasonalTOU_derivation.json`** (season names **`winter`**, **`summer`**), aligned with **`utils.pre.compute_tou.load_season_specs`**.

**Proper follow-up (not in repo):** add a **`nimo_hp_seasonalTOU_supply_derivation.json`** (or equivalent) derived from the **supply** TOU structure, **or** document that supply flex uses inferred seasons with ε = 0 unless that file exists.

## Demand charges (when enabled on a tariff)

**`utils/mid/patches.py`** ~**565–578**: if any tariff in the map has **`ur_dc_enable == 1`**, **`has_demand`** forces **`PATCH_FALLBACK`** for **`_vectorized_run_system_revenues`**. That is **not** the NiMo flex **calibrated** case that drove the 2026 Q2 incident—see **`nimo_flex_demand_charge_regression.md`** (**`ur_dc_enable = 0`**).

## Hourly shift allocation and tiny period totals (code)

**`utils/cairo.process_residential_hourly_demand_response_shift`** spreads period-level **`load_shift`** across hours using **`hour_share`**. When **`|q_orig|`** (period sum of **`electricity_net`**) is tiny or **≤ 0**, proportional **`electricity_net / q_orig`** is unsafe. The fix uses **`FLEX_SHIFT_MIN_PERIOD_ABS_KWH`** (default **1.0**), **`_flex_shift_hour_share_from_groups`**, and **`_zero_unsafe_period_shifts_and_rebalance`** — see **`nimo_flex_demand_charge_regression.md`**, section “Fix (near-zero / negative period net)”.

## Tracker null ε (not the same as billing bug)

**`utils/cairo.process_residential_hourly_demand_response_shift`** ~**929–937**: **`epsilon`** is only filled when **`valid`** — `(Q_new > 0) & (Q_orig > 0) & (rate != flat_tariff)`. Pivoted columns **`{season}_period_{energy_period}`** (**`apply_runtime_tou_demand_response`** ~**1107–1112**) can be **null** when there is no valid triple for that building/period (e.g. zero kWh in that TOU period).

## `find_tou_derivation_path` (current behavior)

**`utils/demand_flex.find_tou_derivation_path(tariff_key, tou_derivation_dir)`**: strip trailing **`_calibrated`** and **`_flex`** from **`tariff_key`** (the tariff JSON **stem** at runtime), then return **`{tou_derivation_dir}/{base}_derivation.json`** if it exists, else **`None`**. No fallback to another stem.

Tests: **`tests/test_demand_flex.py`** — **`test_find_tou_derivation_path_*`**.

## Related tooling

Empirical NiMo regression write-up (batch Mar 27 vs Apr 2026, **bldg_id 187980**): **`context/code/cairo/nimo_flex_demand_charge_regression.md`**.

| Path                                                           | Role                                                                                                   |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| **`utils/post/compare_cairo_runs.py`**                         | Diff **`bills/elec_bills_year_target.csv`**, tracker, BAT, tariff between two CAIRO run directory URIs |
| **`utils/post/diagnose_ny_epmc_batch_diff.py`**                | Master BAT + **`comb_bills_year_target`** per utility; use **`--run-pairs run_15+16`**                 |
| **`context/code/cairo/nimo_flex_demand_charge_regression.md`** | Incident: seasonal ε wiring, near-zero period kWh guard, **`FLEX_SHIFT_MIN_PERIOD_ABS_KWH`**           |
| **`context/code/cairo/cairo_demand_flexibility_workflow.md`**  | End-to-end demand-flex data flow                                                                       |

For **`bill_level`** joins without this module: list **`s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/<utility>/<batch>/`**, pick the newest directory whose name contains **`_run15_`** (or **`_run16_`**), append **`bills/elec_bills_year_target.csv`**, filter **`month == "Annual"`**, and compare **`bill_level`** on **`bldg_id`**.

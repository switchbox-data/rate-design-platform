# NiMo demand-flex run 15: seasonal ε wiring, PV / near-zero period bug, and fix (2026 Q2)

## Plain-language summary (for handoff)

**What we saw:** Comparing NY EPMC batches, **NiMo** delivery run **15** (demand flex, `elec_bills_year_target.csv`) could show a **normal** annual bill (~~\$1k) on an **older** CAIRO batch and a **catastrophic** bill (e.g. **~~\$2M** for one `bldg_id`) on a **newer** batch—while paired supply run **16** and master **`elec_total_bill`** looked fine. That pointed at **flex changing hourly load** on the delivery track, not at the tariff JSON alone.

**Why the older batch looked like “no flex”:** Effective flex needs **seasonal elasticity** from the scenario YAML (keys like **`winter`** / **`summer`**) to reach **`apply_runtime_tou_demand_response`** in **`utils/cairo.py`**. That path only uses a non-zero ε when the **season name** passed into **`_shift_season`** matches a key in that dict (`demand_elasticity.get(season_name, 0.0)`). Season names come from **`load_season_specs`** in **`utils/demand_flex.py`**, which reads a **TOU derivation JSON** discovered by **`find_tou_derivation_path(tariff_key, tou_derivation_dir)`**. If the derivation file is missing, wrong, or the inferred season labels do not match the YAML (e.g. code has **`season_1`** but YAML only has **`winter`** / **`summer`**), **ε is 0** for those slices—so hourly load barely moves and **run 15 ≈ run 11** (no flex), even though flex is “on” in the scenario. **March 27, 2026** batch runs often coincide with **empty or non-standard `demand_flex_elasticity_tracker.csv`** (no `bldg_id` header), which makes elasticity comparisons harder but matches “little effective shift.”

**Why the newer batch blew up (root cause in platform code):** Once ε was wired correctly, flex computed a **period-level** **`load_shift`** (total kWh to add/remove in each TOU period for a season). To write **hourly** values, the code spread that shift across hours **in proportion to each hour’s share of that period’s original kWh**:

- **`hour_share ≈ hourly_kWh / q_orig`**, where **`q_orig`** is the **sum of `electricity_net` for that building in that TOU period** in the season slice.

That is intentional: “cut 100 kWh from the evening block” is implemented by taking the cut back from each hour in proportion to its original share. **The bug:** the old guard was only **`|q_orig| > 0`**. For **PV** or strong netting, **`q_orig`** can be **positive but tiny** (e.g. imports and exports almost cancel). Then **`hour_share`** becomes **enormous**, **`hourly_shift = load_shift × hour_share`** explodes, and CAIRO prices nonsense hourly kWh → **absurd `bill_level`**.

**Not the driver:** For **`nimo_hp_seasonalTOU_flex_calibrated`**, **`tariff_final_config.json`** on S3 has **`ur_dc_enable = 0`** and the calibrated JSON has **no** `demandratestructure`. CAIRO’s **`_demand_charge_aggregation`** branch for monthly peak kW is **off** for that tariff. A **~\$2M** residential bill is **not** explained by demand charges on that flex tariff.

## Verified commits (elasticity path)

| Commit                                         | Message (exact)                                        |
| ---------------------------------------------- | ------------------------------------------------------ |
| **`185519cc81451e8c179fbb9429ef893012b930a8`** | Add seasonal elasticity support to demand-flex runtime |
| **`fee4706faa59f0c9a8366a9c603cb93374d48da7`** | Wire seasonal elasticity dict through run_scenario     |

**Author / date (from `git show`):** Alex Smith; **2026-03-26** (both commits).

**What they do:**

- **`185519c`** — **`utils/cairo.py`**, **`utils/demand_flex.py`**: `apply_runtime_tou_demand_response` / demand-flex call chain accepts a **dict** for elasticity; inside **`_shift_season`**, **`season_eps = demand_elasticity.get(season_name, 0.0)`** when `demand_elasticity` is a **`dict`**.
- **`fee4706`** — **`rate_design/hp_rates/run_scenario.py`**: passes scenario YAML **`elasticity`** through to **`apply_demand_flex(..., elasticity=...)`** so **`{ summer: …, winter: … }`** from generated YAML actually reaches the runtime (instead of failing earlier normalization paths).

**Related (derivation filename / stem, not a single “typo”):** **`utils/demand_flex.find_tou_derivation_path`**: strips **`_calibrated`** and **`_flex`** from the **tariff JSON stem** (from **`utils.scenario_config._parse_path_tariffs`** keys, **not** YAML `all` keys), then returns **`{tou_derivation_dir}/{base}_derivation.json`** if present. Example delivery stem → **`nimo_hp_seasonalTOU_derivation.json`**. Full diagram: **`context/code/cairo/demand_flex_seasonal_elasticity_derivation_path.md`**.

## Fix (near-zero / negative period net): exact symbols in `utils/cairo.py`

**Module constant:** **`FLEX_SHIFT_MIN_PERIOD_ABS_KWH: float = 1.0`**

**Helpers:**

- **`_flex_shift_hour_share_from_groups(electricity_net, q_orig, n_in_group, *, min_abs_kwh)`** — If **`q > 0`** and **`|q| >= min_abs_kwh`**, use **`hourly_kWh / q`** (same proportional idea as before). Otherwise use **`1 / n_hours`** in that group so weights still sum to **1** and **`load_shift`** is fully distributed without dividing by a tiny **`q`**.
- **`_zero_unsafe_period_shifts_and_rebalance`**: for rows where **`Q_orig <= 0`** or **`|Q_orig| < min_abs_kwh`**, zero **`load_shift`**, then set the **receiver** period (lowest energy rate) shift to **minus the sum of donor shifts** per **`bldg_id`** so period-level shifts stay **zero-sum**.

**Call path:**

1. **`process_residential_hourly_demand_response_shift`** — after **`_build_period_shift_targets`**, calls **`_zero_unsafe_period_shifts_and_rebalance`**; builds **`hour_share`** via **`_flex_shift_hour_share_from_groups`**; **`hourly_shift = load_shift × hour_share`**, **`shifted_net = electricity_net + hourly_shift`**.
2. **`apply_runtime_tou_demand_response`** — each season calls **`process_residential_hourly_demand_response_shift`** from **`_shift_season`** (around the merge with **`period_lookup`**).

**Optional argument:** **`min_period_abs_kwh`** on **`process_residential_hourly_demand_response_shift`** overrides **`FLEX_SHIFT_MIN_PERIOD_ABS_KWH`** (tests use this).

**Tests:** **`tests/test_cairo.py`** — **`test_flex_shift_near_zero_period_net_does_not_explode_hourly_values`**, **`test_flex_shift_negative_period_net_stays_finite`**.

**Note on git:** If your checkout shows these edits only in the working tree, merge them to **`main`** and then replace this sentence with the **merge commit hash**.

## Previous code (before guard)

**`process_residential_hourly_demand_response_shift`** used:

```python
hour_share = np.where(
    q_orig.abs().values > 0,
    hourly_load_df["electricity_net"].values / q_orig.values,
    0.0,
)
```

Same pattern in **`_shift_building_hourly_demand`**: **`abs(Q_orig) > 0`** → **`electricity_net / Q_orig`**. **No** minimum **`|q_orig|`** — only exact zero skipped.

## Empirical batch comparison (reference)

| Check                                                                                 | Result (historical bad NEW run)                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **OLD** batch `ny_20260327_r1-16_epmc` run **15** Annual **`bill_level`**, **187980** | ≈ **\$1020** (≈ run **11**)                                                                                                                                                                                                                                                                                          |
| **NEW** batch `ny_20260401_all_epmc_r1-24` run **15** (pre–cairo guard, bad folder)   | ≈ **\$2.1e6** Annual for **187980**                                                                                                                                                                                                                                                                                  |
| **NiMo** population                                                                   | One building **>** \$100 **\|Δ\|**, thousands with small flex noise                                                                                                                                                                                                                                                  |
| After **cairo** guard + rerun                                                         | Per-utility checks on **`bills/elec_bills_year_target.csv`** (Annual inner join OLD vs NEW run **15** / **16**) showed **no** buildings with **\|Δ bill_level\| > \$100k** across **cenhud, coned, nimo, nyseg, or, psegli, rge** (same EPMC batch pair as **`utils/post/diagnose_ny_epmc_batch_diff.py`** defaults) |

## `compare_cairo_runs` (example)

```bash
uv run python -m utils.post.compare_cairo_runs \
  --baseline "s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/nimo/ny_20260327_r1-16_epmc/<OLD_run15_dir>/" \
  --challenger "s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/nimo/ny_20260401_all_epmc_r1-24/<NEW_run15_dir>/" \
  --artifacts bills_elec_target,elasticity,tariff_config
```

Resolve **`<…_dir>`** by listing
**`s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/<utility>/<batch>/`** and choosing the **newest** common prefix whose name contains **`_run15_`** (or **`_run16_`**), then append that folder to the URI before **`bills/elec_bills_year_target.csv`**.

## Tariff diff (rules out calibration as \$2M driver)

Leaf diff of **`tariff_final_config.json`** for **`nimo_hp_seasonalTOU_flex_calibrated`** (OLD vs NEW bad run): only a few **`ur_ec_tou_mat`** energy **\$/kWh** tweaks (~1–2%). **`ur_dc_*`** / demand tables unchanged. **`ur_dc_enable = 0`** on that key.

Compare the two JSON objects with any deterministic diff tool (e.g. leaf-wise compare of **`tariff_final_config.json`** at
**`<run_dir>/tariff_final_config.json`** for each run).

## Master bills split

**`utils/post/build_master_bills.py`**: **`elec_delivery_bill`** comes from delivery run **`bill_level`**; **`elec_total_bill`** from supply; bad run **15** **`bill_level`** poisons **delivery/supply decomposition** while total can stay stable. Join OLD vs NEW master **`comb_bills_year_target`** (Hive-partitioned under **`…/all_utilities/<batch>/run_15+16/`**) on **`(bldg_id, sb.electric_utility, month)`** and inspect **\|Δ elec_delivery_bill\|**, **\|Δ elec_supply_bill\|**, **\|Δ elec_total_bill\|** — same aggregates as **`utils/post/diagnose_ny_epmc_batch_diff.py`** for **`--run-pairs run_15+16`**.

## Related tooling (tracked in repo)

| Path                                            | Role                                                                                                                                                          |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`utils/post/compare_cairo_runs.py`**          | Module / CLI: diff **`bills/elec_bills_year_target.csv`**, tracker, BAT, etc., between two CAIRO run directory URIs                                           |
| **`utils/post/diagnose_ny_epmc_batch_diff.py`** | Master **`cross_subsidization_BAT_values`** + **`comb_bills_year_target`**: per-utility max **\|old−new\|**, worst buildings; use **`--run-pairs run_15+16`** |
| **`utils/post/verify_ny_epmc_master_batch.py`** | Guideline checks on NEW batch master schemas (shared helpers with diagnose)                                                                                   |

## Related context

- **`context/code/cairo/demand_flex_seasonal_elasticity_derivation_path.md`** — **`find_tou_derivation_path`**, YAML vs runtime keys, supply vs delivery derivation gap, reverted unsafe fallback.
- **`context/code/cairo/cairo_demand_flexibility_workflow.md`** — End-to-end flex pipeline.

## Obsolete lines of investigation

- **Demand-charge amplification** via **`utils/mid/patches.py`** **`PATCH_FALLBACK`** when **`ur_dc_enable == 1`**: **not applicable** to **`nimo_hp_seasonalTOU_flex_calibrated`** as deployed (**`ur_dc_enable = 0`**).
- **Deep CAIRO **`_demand_charge_aggregation`** debugging** for this incident: **not** the primary explanation once **`ur_dc_enable`** was confirmed.

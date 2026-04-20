# Revenue requirement YAMLs and the Runs sheet (`utility_revenue_requirement`)

## Purpose

Document how each CAIRO run’s **`utility_revenue_requirement`** field is produced from the **Runs & Charts** Google Sheet, how it resolves to files under `rate_design/hp_rates/<state>/config/rev_requirement/`, and what the common filename patterns mean.

## Sheet → scenario YAML

- **Script:** `utils/pre/create_scenario_yamls.py` (module docstring describes the sheet and column conventions).
- **Sheet column:** `utility_revenue_requirement` — a **string** that must end in `.yaml` or `.yml`. It is copied verbatim into each run block in `rate_design/hp_rates/<state>/config/scenarios_<utility>.yaml` (see `run["utility_revenue_requirement"] = get("utility_revenue_requirement")`).
- **Typical values:** Paths relative to the state **`config/`** directory, e.g. `rev_requirement/coned.yaml`, `rev_requirement/coned_hp_vs_nonhp.yaml`. Resolution at runtime uses that directory as the base (see `run_scenario.py` + `_parse_utility_revenue_requirement` in `utils/scenario_config.py`).

If the column is blank for a run, scenario generation still writes an empty string; **`run_scenario` will fail** when parsing the run, so the sheet should always carry an explicit path for every row you intend to execute.

## Runtime use

For each `--run-num`, `rate_design/hp_rates/run_scenario.py` loads the run dict and calls `utils/scenario_config._parse_utility_revenue_requirement` with:

- The path string from `utility_revenue_requirement`
- `run_includes_supply` (delivery-only vs delivery+supply totals: `total_delivery_revenue_requirement` vs `total_delivery_and_supply_revenue_requirement`)
- When `run_includes_subclasses` is true: `residual_allocation_delivery` and `residual_allocation_supply`, plus a **`subclass_revenue_requirements`** block in the YAML (per-tariff-key subclass dollars for the active allocation methods)

That produces a `RevenueRequirementConfig` (scalar total RR, optional per-subclass map) used for CAIRO precalc / calibration.

**Subclass RR generation** (after run 1 / run 2 BAT outputs exist) does **not** read the sheet again. The Justfile recipes `compute-subclass-rev-requirements` and (NY) `compute-electric-heating-subclass-rev-requirements` call `utils/mid/resolve_rr_paths.py` to pick **which** YAML paths to pass into `utils/mid/compute_subclass_rr.py`:

- **Base file:** always run **1**’s `utility_revenue_requirement` (usually `{utility}.yaml`).
- **Differentiated file:** by default, the **first** run in the scenario file with `run_includes_subclasses: true` (typically run 5 for the HP vs non-HP track). With **`--subclass-run-num 29`**, NY’s electric-heating track uses run **29**’s path instead (e.g. `*_elec_heat_vs_non_elec_heat.yaml`).

## Filename patterns (what each YAML “type” means)

These are **team conventions**, not enforced by the parser. The code cares about **schema** (`total_delivery_*`, optional `subclass_revenue_requirements`, etc.), not the stem.

| Pattern                                         | Role                                                                                                                                                                                                                                                                                                                                      |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`{utility}.yaml`**                            | **Default / rate-case RR.** Full utility delivery (and optionally supply) totals from regulatory or internal RR pipelines (`utils/pre/rev_requirement/compute_rr.py`, top-ups, etc.). Used for single-tariff runs (e.g. precalc run 1) and as the **base** RR when regenerating subclass splits.                                          |
| **`{utility}_large_number.yaml`**               | **Placeholder for calibrated default runs.** Minimal YAML with a huge scalar `revenue_requirement` (e.g. `1e12`) so CAIRO’s calibration path does not bind on the real RR; the authoritative totals stay in the precalc run’s `{utility}.yaml`.                                                                                           |
| **`{utility}_hp_vs_nonhp.yaml`**                | **Subclass RR for HP vs non-HP** (`group_col: has_hp`, tariff keys like `hp` / `non-hp`). Written by `compute_subclass_rr.py` from run 1 (and supply companion) BAT + bills. Used when `run_includes_subclasses` is true for the **standard** multi-tariff track (e.g. runs 5–8, 9–12, 13–16).                                            |
| **`{utility}_elec_heat_vs_non_elec_heat.yaml`** | **Subclass RR for electric heating vs not** (`group_col: heating_type_v2`, keys like `electric_heating` / `non_electric_heating`). Same computation pattern as HP vs non-HP but a different metadata split; used for NY runs that point `utility_revenue_requirement` at this file (often the **29–32** electric-heating parallel track). |

Other stems (e.g. `_supply`-only variants) may exist for specific utilities; treat them the same way: read the `utility` line, `group_col`, and `subclass_revenue_requirements` to see the split.

## `heating_type_breakdown` (appendix on differentiated RR YAMLs)

Differentiated files such as `nyseg_hp_vs_nonhp.yaml` often include a second top-level block, **`heating_type_breakdown`**, alongside `subclass_revenue_requirements`. Example (abbreviated): under each allocation key (`percustomer`, `epmc`, `volumetric`), one entry per **`heating_type_v2`** category (`heat_pump`, `electrical_resistance`, `natgas`, `delivered_fuels`, `other`) with nested **`delivery`**, **`supply`**, and **`total`** dollar amounts.

### How the values are created

Implementation: `utils/mid/compute_subclass_rr.py`, in `main()`, **after** the primary subclass pass that uses the scenario’s real tariff split (`--group-col`, e.g. `has_hp` or `heating_type_v2` for electric-heating runs).

1. The script runs **`compute_subclass_rr` again** with **`group_col="heating_type_v2"`** on the **same** run 1 directory (`--run-dir`), using the same comma-separated **`--cross-subsidy-col`** BAT columns as the main pass (default: `BAT_percustomer`, `BAT_epmc`, `BAT_vol` — see `SUBCLASS_RR_ALLOCATION_METHODS` in that module). That yields **delivery** subclass RRs per heating category and BAT method (same definition as everywhere else in the file: weighted sum of annual target bills minus weighted sum of the chosen BAT column, grouped by subclass).

2. If **`--run-dir-supply`** is set (normal `compute-subclass-rev-requirements` flow), it repeats that grouping on the **run 2** directory to get **total** (delivery+supply) RRs per category and BAT column.

3. For each heating-type subclass and each BAT column, it writes **`supply = total − delivery`** (mirroring how supply subclasses are derived for `subclass_revenue_requirements` in `_write_revenue_requirement_yamls`).

4. BAT column names are mapped to YAML keys via **`BAT_COL_TO_ALLOCATION_KEY`** (e.g. `BAT_percustomer` → `percustomer`). The default CLI does **not** include `BAT_peak`, so a **`peak`** subsection usually **does not** appear unless you pass a custom `--cross-subsidy-col` list that includes it.

5. If **`heating_type_v2` is missing** from `customer_metadata.csv` (or another error occurs), the block is **skipped** and a log line records that the breakdown was omitted.

The structured dict is passed into **`_write_revenue_requirement_yamls`** and serialized as **`heating_type_breakdown`** on the differentiated YAML only when that dict is non-empty.

### Why it exists

The **tariff-driving** split in `subclass_revenue_requirements` follows **`group_col`** on the differentiated file (`has_hp` → `hp` / `non-hp`, or `heating_type_v2` → `electric_heating` / `non_electric_heating`, etc.). That is what CAIRO needs to calibrate **two** retail subclasses.

**`heating_type_breakdown`** answers a different question: “Within those tariff subclasses, how do **fine-grained heating categories** (ResStock `heating_type_v2`) contribute to bill-weighted RR and BAT-adjusted RR under each residual method?” It is a **frozen diagnostic** tied to the same `source_run_dir` as the parent YAML, useful for reports, sanity checks, and reconciling HP vs non-HP dollars to underlying heating mix **without** changing simulation inputs.

### How it is not used as input

- **`utils/scenario_config._parse_utility_revenue_requirement`** (called from `run_scenario.py`) loads **`total_delivery_revenue_requirement`** / **`total_delivery_and_supply_revenue_requirement`** (when present) and, for subclass runs, **`subclass_revenue_requirements`** only. There is **no** reference to `heating_type_breakdown` anywhere in `utils/scenario_config.py`.

- CAIRO therefore **never** sees this block; altering or deleting it would not change precalc or calibration **as long as** `subclass_revenue_requirements` and the scalar totals are unchanged.

For the high-level distinction between sheet-driven RR paths and this appendix, see the opening sections above; for the core BAT/bill aggregation on a single `group_col`, see `context/code/orchestration/subclass_revenue_requirement_utility.md`.

## Related docs and code

- Run ordering and when `compute-subclass-rev-requirements` runs: `context/code/orchestration/run_orchestration.md`
- BAT-based subclass dollar math and CLI: `context/code/orchestration/subclass_revenue_requirement_utility.md` (`utils/mid/compute_subclass_rr.py`)
- Flat-tariff pipeline’s initial `{utility}.yaml` creation: `context/code/data/tariff_generation_pipeline.md`
- Sheet columns for residual allocation: `context/methods/bat_mc_residual/epmc_and_supply_allocation.md`

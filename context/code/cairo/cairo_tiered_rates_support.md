# CAIRO Tiered Rate Support: Precalc and Bill Calc

**Summary:** CAIRO supports **tiered rate precalcs** and **tiered rate bill calcs**. Tariffs use (period, tier) structure end-to-end; load is aggregated by period and tier; precalc preserves that structure and calibrates a rate per (period, tier).

## 1. Tiered rate bill calculation

- **`customer_bill_calculation.py`** — `calculate_energy_charges()` (lines 249–309):
  - Reads tariff from `tariff_config["ur_ec_tou_mat"]` with columns **period**, **tier**, max_usage, rate, adjustments (and optionally sell_rate).
  - Builds a `charges` DataFrame and merges with load on **`["period", "tier"]`** (line 304), then computes `costs = grid_cons * rate` per (period, tier).
  - Bill calc is explicitly **per (period, tier)**.

- **`calculate_demand_charges()`** (lines 311–317): Uses `ur_dc_tou_mat` and merges on **period** and **tier** the same way.

- **`loads.py`**: Load is aggregated by period and tier for energy charges (`_energy_charge_aggregation`, `_tou_or_tier_energy_charge_aggregation`), producing rows with `period`, `tier`, and `charge_type` so the merge in bill calc is well-defined.

**Conclusion:** Bill calculation is tiered — multiple tiers per period are supported for both energy and demand.

## 2. Tiered rate precalc

- **`system_revenues.py`** — `_precalc_customer_rates()` (lines 202–290):
  - Docstring (lines 216–222) states that precalc assumes “demand charges, fixed charges, minimum bills, **energy charge period timing, and tiers** are all set by regulatory guidance” and only solves for the energy rate level.
  - Precalc uses the same bill path: it calls `run_aggregator_precalculation()` in `customer_bill_calculation.py`, which uses `_return_bill_elements()` → `calculate_energy_charges()`. So the same **(period, tier)** structure is used.

- **`_energy_charge_calculate()`** (lines 292–513):
  - Energy revenue is grouped by **`["charge_type", "period", "tier", "rate", "tariff"]`** (lines 401–412).
  - It merges with **`precalc_rel_energy_charge_mapping`** on **`["period", "tier", "tariff"]`** (lines 429–431). The mapping supplies a **`rel_value`** per (period, tier) so relative rates across period/tiers are preserved.
  - Final rate per (period, tier) is `rate_unity * rel_value` (lines 472–474).
  - The updated rates are written back into **`ur_ec_tou_mat`** by iterating over each `energy_period_structure` and selecting the pre-calculated rate where **`(rate_customer["period"] == energy_period_structure[0]) & (rate_customer["tier"] == energy_period_structure[1])`** (lines 498–508). So **each (period, tier)** in the tariff gets its own calibrated rate.

**Conclusion:** Precalc is tiered — it preserves the tier (and period) structure and calibrates a rate for every (period, tier) using the provided period/tier mapping (e.g. from the platform’s `generate_default_precalc_mapping()`, which outputs period, tier, rel_value, tariff).

## 3. Tariff ingestion (URDB → PySAM)

- **`tariffs.py`** — `try_get_rate_structure()` (lines 362–389): Fills **`ur_ec_tou_mat`** from URDB `energyratestructure` with 1-based **(period, tier)** indices and supports multiple tiers per period.
- **`URDBv7_to_ElectricityRates()`** and **`_check_combined_tiered_tou_energy_charge()`** handle combined TOU + tiered schedules (with `tou_tier_comb_type` when both multiple periods and multiple tiers exist).

## Reference

- CAIRO package location in this repo: `.venv/lib/python3.13/site-packages/cairo/` (git dependency: `switchbox-data/CAIRO`).
- Platform precalc mapping: `utils/pre/generate_precalc_mapping.py` — `generate_default_precalc_mapping()` builds (period, tier, rel_value, tariff) from tariff JSON.

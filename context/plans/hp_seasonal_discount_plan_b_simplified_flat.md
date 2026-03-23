# Plan B: Simplified flat seasonal discount (chosen)

**Status:** Active. This is the plan being implemented.

## Goal

Fix the seasonal discount computation so it works correctly when runs 1-4 use default-structure tariffs (`BASE_TARIFF_PATTERN=default`). The HP seasonal tariff remains a flat 2-period rate (summer + winter) -- no structure preservation. The fix is localized to how `summer_rate` and `winter_rate` are derived inside `compute_hp_seasonal_discount_inputs`.

## Context and motivation

Runs 1-4 use the actual utility rate structure (tiered, seasonal, TOU) so that cross-subsidy quantification under the default rates is realistic -- this is the core finding of the analysis. The HP seasonal discount (runs 5-8) then proposes a simple seasonal rate that eliminates the cross-subsidy.

Plan A (structure-preserving) would maintain the default tariff's period/tier/TOU structure in the HP seasonal rate. It was rejected because:

- Mixed-period splitting is the common case (6/7 NY utilities + RIE), adding substantial implementation complexity.
- The analytical narrative already describes the HP seasonal rate as a "simple seasonal delivery rate" -- the reports and notebooks treat it as flat.
- The compromise of using default structure for cross-subsidy measurement (runs 1-4) while flattening for the proposed rate (runs 5-8) is consistent with the analysis.

## The problem

The current `_extract_default_rate_from_tariff_config` in `utils/mid/compute_subclass_rr.py` (lines 211-247) picks a single rate from `ur_ec_tou_mat[period=1, tier=1]` -- one arbitrary entry from a multi-rate tariff. When `BASE_TARIFF_PATTERN=default` and the tariff has multiple periods/tiers (e.g. ConEd's 2-tier seasonal, PSEG-LI's 4-period TOU), this extracts an incorrect rate.

The fix is to **flatten** the structured rates into effective average seasonal rates using the HP-load-weighted energy revenue from run-1's actual bill outputs. For flat tariffs this is a no-op (the single rate IS the effective average), so existing RI behavior is preserved.

## The fix

Replace `_extract_default_rate_from_tariff_config` and the downstream single-rate formula with a **revenue-based approach** that computes effective average rates from run-1's actual bill outputs.

### Math

From run-1's monthly bills for HP buildings (weighted):

\[
Rev^{summer}_{energy,HP} = \sum_{i \in HP} \sum_{m \in summer} weight_i \times (bill\_level_{i,m} - fixed\_charge)
\]

\[
Rev^{winter}_{energy,HP} = \sum_{i \in HP} \sum_{m \in winter} weight_i \times (bill\_level_{i,m} - fixed\_charge)
\]

Then:

\[
summer\_rate = \frac{Rev^{summer}_{energy,HP}}{summer\_kWh_{HP}}
\]

\[
winter\_rate = \frac{Rev^{winter}_{energy,HP} - CS^{HP}}{winter\_kWh_{HP}}
\]

For a flat tariff, \(Rev^{summer} / summer\_kWh = Rev^{winter} / winter\_kWh = flat\_rate\), so this reduces to the existing formula. For a structured tariff, it gives the correct load-weighted effective rates.

### Revenue neutrality proof

Under the flat seasonal tariff, CAIRO computes each HP building's bill as:

\[
bill_i = 12 \times fixed\_charge + summer\_rate \times summer\_kWh_i + winter\_rate \times winter\_kWh_i
\]

Aggregate weighted HP revenue:

\[
\begin{align}
Rev_{HP,flat} &= 12 \times fixed\_charge \times N_{weighted} + summer\_rate \times summer\_kWh_{HP} + winter\_rate \times winter\_kWh_{HP} \\
&= 12 \times fixed\_charge \times N_{weighted} + Rev^{summer}_{energy,HP} + (Rev^{winter}_{energy,HP} - CS^{HP}) \\
&= 12 \times fixed\_charge \times N_{weighted} + (Total\_Bills_{HP} - 12 \times fixed\_charge \times N_{weighted}) - CS^{HP} \\
&= Total\_Bills_{HP} - CS^{HP} \\
&= RR_{HP}
\end{align}
\]

Revenue neutrality holds exactly by construction. The `rate_unity` for the HP key in run-5 should be essentially 1.0. The flattening redistributes revenue across individual buildings (some HP homes pay slightly more in summer, some less, compared to the structured default) but preserves the aggregate.

**Non-HP side:** CAIRO calibrates the non-HP tariff to \(RR_{non-HP} = Total\_RR - RR_{HP}\) in run-5. Plan B doesn't touch the non-HP key or the RR split. Revenue neutral on the non-HP side too.

**Fixed charge consistency:** Both the revenue decomposition (subtracting `fixedchargefirstmeter` from bills) and the output tariff (via `create_seasonal_rate`) use the same fixed charge value from the URDB base tariff JSON. CAIRO's precalc only scales volumetric rates via `rate_unity`, not fixed charges, so no mismatch is introduced.

### Data sources (all available at the time `compute-seasonal-discount-inputs` runs)

| Input                        | Source                                        | Already read?                             |
| ---------------------------- | --------------------------------------------- | ----------------------------------------- |
| HP building IDs + weights    | `customer_metadata.csv` in run-1              | Yes                                       |
| \(CS^{HP}\)                  | `cross_subsidization_BAT_values.csv` in run-1 | Yes                                       |
| winter_kwh_hp, summer_kwh_hp | ResStock loads scan                           | Partially (winter only today; add summer) |
| Monthly bill_level for HP    | `bills/elec_bills_year_target.csv` in run-1   | **New**                                   |
| Fixed charge ($/month)       | Base tariff JSON (`fixedchargefirstmeter`)    | **New**                                   |
| Winter months                | `config/periods/<utility>.yaml`               | Yes                                       |

## Changes

### 1. `compute_hp_seasonal_discount_inputs` in `utils/mid/compute_subclass_rr.py`

**Delete** `_extract_default_rate_from_tariff_config` (lines 211-247).

**Replace** the single-rate formula (lines 348-354, currently `default_rate = ...; winter_rate_raw = default_rate - CS / winter_kwh`) with:

- Accept a new parameter `base_tariff_json_path` (path to the URDB-format base tariff JSON)
- Read `fixedchargefirstmeter` from the base tariff JSON
- Read `bills/elec_bills_year_target.csv` from `run_dir` for HP buildings (join on `bldg_id`), filter to monthly rows (exclude "Annual")
- Classify each month row as winter or summer using `winter_months`
- Compute weighted `energy_revenue = (bill_level - fixed_charge) * weight` per season
- Also compute `summer_kwh_hp` (currently only `winter_kwh_hp` is computed -- extend the loads scan to also aggregate summer kWh, or derive from annual - winter)
- Compute `summer_rate` and `winter_rate` per the formulas above

**Output CSV columns:** Replace `default_rate` with `summer_rate`. Keep `winter_rate_hp`, `winter_kwh_hp`. Add `summer_kwh_hp`, `rev_summer_energy_hp`, `rev_winter_energy_hp` for auditability.

### 2. `create_seasonal_discount_tariff.py` in `utils/mid/create_seasonal_discount_tariff.py`

Line 101: change `summer_rate = float(row["default_rate"])` to `summer_rate = float(row["summer_rate"])`.

### 3. `compute_seasonal_discount_inputs.py` in `utils/mid/compute_seasonal_discount_inputs.py`

Add `--base-tariff-json` CLI arg and pass it through to `compute_hp_seasonal_discount_inputs` (for fixed charge extraction).

### 4. Justfile `rate_design/hp_rates/Justfile`

In `run-5` (line 476) and `run-6` (line 489), pass the base tariff JSON path to `compute-seasonal-discount-inputs`:

```
just compute-seasonal-discount-inputs "${run1_dir}" \
    "{{ path_resstock_release }}" "{{ state_upper }}" "{{ upgrade }}" \
    --base-tariff-json "{{ path_tariffs_electric }}/{{ utility }}_{{ base_tariff_pattern }}_calibrated.json"
```

Note: `_calibrated.json` is in URDB format (output of `copy-calibrated-tariff`), so `fixedchargefirstmeter` is directly available. CAIRO's calibration doesn't modify fixed charges, so pre-calibrated and calibrated values are identical.

### 5. No other changes

- `create_seasonal_rate` in `utils/pre/create_tariff.py` -- unchanged (already produces flat 2-period)
- Revenue YAML -- unchanged
- Scenario YAML -- unchanged
- Analysis notebooks -- unchanged (already describe HP seasonal rate as "simple seasonal delivery rate")
- Justfile run-5/run-6 structure -- same two-step flow, just added an arg

## What this does NOT change

The HP seasonal tariff is still a flat 2-period rate (summer + winter). No structure preservation, no period splitting. The default-structure tariff is used only for runs 1-4 (cross-subsidy quantification under realistic rates). The seasonal discount flattens into a simple rate that eliminates the cross-subsidy.

## Relationship to other scenarios

The seasonal discount (runs 5-8), TOU (runs 9-12), and TOU flex (runs 13-16) scenarios are **parallel branches** sharing only the `hp_vs_nonhp.yaml` revenue requirement derived from runs 1-2:

- **TOU tariffs (runs 9-12):** Generated by `create-seasonal-tou-tariffs` during `all-pre` from marginal cost data. Never reads `hp_seasonal.json` or `seasonal_discount_rate_inputs.csv`. Uses the same `hp_vs_nonhp.yaml` RR but a completely independent tariff structure. Unaffected by Plan B.
- **Flex tariffs (runs 13-16):** Copied from TOU tariff (`cp hp_seasonalTOU.json hp_seasonalTOU_flex.json`) and run with `elasticity: -0.1`. Same independence as TOU. Unaffected by Plan B.
- **Revenue requirement YAML:** Derived from runs 1-2 via `compute-rev-requirements`. Plan B doesn't change runs 1-2 or the RR computation.
- **Non-HP tariff key:** Hardcoded in scenario YAMLs (NY: `nonhp_flat.json`, RI: `nonhp_default.json`). Plan B only touches the HP key.

## Adversarial review

### 1. Fixed charge assumed constant across months

True for all current tariffs (`fixedchargefirstmeter` is a single scalar). If a utility had seasonal fixed charges, we'd need per-month values. Not a current concern.

### 2. CS^HP includes fixed-charge cross-subsidy

Same issue as Plan A. `BAT_percustomer` measures total-bill cross-subsidy, but we only discount energy rates. The fixed-charge residual causes `rate_unity` to drift slightly from 1.0. Expected to be small (< 1-2%). Self-correcting via CAIRO precalc.

### 3. summer_kwh_hp computation

The current code computes only `winter_kwh_hp`. We need `summer_kwh_hp` too. Two options: (a) extend the loads scan to also sum summer months (one more filter+agg), or (b) compute `annual_kwh_hp` and derive `summer_kwh_hp = annual - winter`. Option (b) avoids a second scan.

### 4. For flat tariffs, this is equivalent to the current formula

\(Rev^{summer} / summer\_kWh = Rev^{winter} / winter\_kWh = flat\_rate\), and \(winter\_rate = flat\_rate - CS^{HP} / winter\_kWh\). So existing RI runs (flat pattern) are unaffected.

### 5. Minimum bill floors

Same as Plan A. If `mincharge > 0` and some HP buildings hit the minimum in winter, revenue for those buildings stays at the floor. Expected severity: low (HP customers have high winter consumption).

### 6. RI policy presentation

RI run-5 pairs a flat `hp_seasonal.json` with a structured `nonhp_default.json`. This isn't a model inconsistency (CAIRO calibrates each key independently), but the HP rate proposal looks less like a real tariff filing. This is a known tradeoff accepted in exchange for implementation simplicity.

## Implementation plan (todos)

1. **fix-rate-derivation**: Replace `_extract_default_rate_from_tariff_config` with revenue-based summer/winter rate computation in `compute_hp_seasonal_discount_inputs` (read run-1 bills, extract fixed charge, compute seasonal effective rates).
2. **update-csv-consumer**: Update `create_seasonal_discount_tariff.py` to read `summer_rate` instead of `default_rate` from CSV.
3. **add-cli-arg**: Add `--base-tariff-json` arg to `compute_seasonal_discount_inputs.py` CLI and wire in Justfile run-5/run-6.
4. **verify-flat-equivalence**: Verify that the new formula produces identical results to the old formula when `BASE_TARIFF_PATTERN=flat` (unit test or manual check on RI).
5. **verify-rate-unity**: Run one utility with `BASE_TARIFF_PATTERN=default` through run-5 and check that `rate_unity` for HP key is close to 1.0.

## Comparison with Plan A

| Dimension              | Plan A (structure-preserving)                                                         | Plan B (simplified flat)                |
| ---------------------- | ------------------------------------------------------------------------------------- | --------------------------------------- |
| HP tariff output       | Preserves default structure (periods, tiers, TOU)                                     | Flat 2-period (summer, winter)          |
| Code changes           | New script with mixed-period splitting, winter revenue mapping, schedule manipulation | ~30 lines changed in existing functions |
| Mixed-period handling  | Must split 6/7 NY utilities + RIE                                                     | Not applicable                          |
| Analytical consistency | HP rate uses same structure as default                                                | HP rate is a deliberate simplification  |
| Revenue neutrality     | By construction (scale factor)                                                        | By construction (revenue-based rates)   |
| Adversarial issues     | Same fixed-charge residual                                                            | Same fixed-charge residual              |
| RI policy alignment    | HP rate structure matches filing expectations                                         | HP rate is simpler than filed structure |
| Implementation risk    | High (schedule manipulation, multi-tier, TOU)                                         | Low (reading existing bill outputs)     |

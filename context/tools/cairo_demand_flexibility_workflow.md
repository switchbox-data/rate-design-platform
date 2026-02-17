# CAIRO Demand Flexibility Workflow

## Scope

This document summarizes the demand-flexibility (price-response) workflow in CAIRO.

Primary code references:

- `cairo/rates_tool/loads.py:563` (`process_residential_hourly_demand_response_shift`, commented)
- `cairo/rates_tool/loads.py:2091` (`_shift_building_hourly_demand`, commented)
- `cairo/rates_tool/postprocessing.py:1018`

Related upstream load-period assignment logic:

- `cairo/rates_tool/loads.py:1376` (`_apply_time_indicators_and_periods`)
- `cairo/rates_tool/loads.py:1462` (`_energy_charge_aggregation`)
- `cairo/rates_tool/loads.py:1707` (`_tou_or_tier_energy_charge_aggregation`)

---

## High-Level Objective

Model short-run customer load shifting under time-varying rates by:

1. Calculating target demand response at tariff period level (by building),
2. Converting those period shifts to hourly shifts proportionally within each period,
3. Preserving total energy (zero-sum shifting),
4. Tracking achieved elasticity for diagnostics.

---

## Function-Level Workflow

## 1) Parent: Period-level shift targets

Function: `process_residential_hourly_demand_response_shift(...)`\
Reference: `cairo/rates_tool/loads.py:563`

Inputs:

- `hourly_load_df`: hourly building load with `bldg_id`, `energy_period`, `tier`, `out.electricity.total.energy_consumption`
- `rate_structure`: period/tier rates with `energy_period`, `tier`, `rate`
- `equivalent_flat_tariff`: baseline flat price
- `demand_elasticity`: price elasticity coefficient (typically negative)

Steps:

1. Aggregate hourly consumption to building-period consumption:
   - Group by `bldg_id`, `energy_period`
2. Join period rates from `rate_structure`.
3. Set baseline price:
   - `rate_orig = equivalent_flat_tariff`
4. Compute target period consumption:
   - `Q_target = Q_orig * (P_period / P_flat)^epsilon`
5. Compute period shift:
   - `load_shift = Q_target - Q_orig`
6. Pivot to matrix indexed by `bldg_id`, columns `(energy_period, tier)`.
7. Identify receiver period:
   - Filter `rate_structure["rate"] < equivalent_flat_tariff`
   - Assert exactly one such `(period, tier)` exists
8. Enforce zero-sum:
   - Receiver shift = negative sum of all other period shifts
9. Dispatch building-level hourly allocation in parallel via Dask:
   - Calls `_shift_building_hourly_demand(...)` per building
10. Concatenate outputs:

- `shifted_load` (hourly adjusted load)
- `demand_elasticity_tracker` (achieved elasticity diagnostics)

Why this design:

- Period-level elasticity model is simpler and more stable than hour-by-hour elasticity.
- Single receiver avoids ambiguous allocation among multiple low-price periods.
- Zero-sum guarantees energy is shifted, not created/destroyed.
- Parallelization scales across large building sets.

---

## 2) Worker: Hourly proportional allocation

Function: `_shift_building_hourly_demand(...)`\
Reference: `cairo/rates_tool/loads.py:2091`

Inputs:

- `load_shift`: target kWh shift per `(energy_period, tier)` for one building
- `hourly_df`: one building's hourly load with `energy_period`, `tier`, original consumption
- `rate_new`: new period/tier rates
- `rate_orig`: equivalent flat baseline price
- `demand_elasticity_target`: passed through, not used directly in computation

Steps:

1. Compute each hour's share of period consumption:
   - `share_hour = Q_hour / Q_period`
2. Merge period shift targets onto hourly rows.
3. Allocate shift proportionally:
   - `shift_hour = shift_period * share_hour`
4. Compute shifted hourly load:
   - `Q_hour_shifted = Q_hour_orig + shift_hour`
5. Validation checks (debug prints):
   - Warn if shifted load falls below 10% of original in any hour
   - Warn if period-level energy conservation appears violated
6. Compute achieved elasticity by `(period, tier)`:
   - `epsilon_achieved = log(Q_new/Q_orig) / log(P_new/P_orig)`
7. Drop intermediate helper columns and return:
   - hourly shifted DataFrame
   - one-row elasticity tracker for the building

Why proportional distribution:

- Preserves intra-period temporal shape.
- Avoids arbitrary hour-picking.
- Keeps shifts physically plausible relative to observed load shape.

---

## Input Derivation and Parameter Setting

The table below focuses on how each input should be derived/set for this module.

| Input                               | How it is derived / set                                                                                                                                                                                                                                                                       | Constraints                                                                                                                                      |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `hourly_load_df`                    | Start from raw hourly building load (8760). Map each hour to tariff `energy_period` via tariff schedules (`_apply_time_indicators_and_periods`, `cairo/rates_tool/loads.py:1376`). Ensure `tier` assignment exists for hourly rows (typically from tier/TOU aggregation logic in `loads.py`). | Must include `bldg_id`, `energy_period`, `tier`, `out.electricity.total.energy_consumption`. One building-year of complete hourly data expected. |
| `rate_structure`                    | Construct period/tier price table corresponding to the tariff under analysis. For TOU/tier tariffs this should reflect effective prices used for behavior response.                                                                                                                           | Must include unique `energy_period`, `tier`, `rate` combinations.                                                                                |
| `equivalent_flat_tariff` (`P_flat`) | Caller-supplied scalar baseline price. Intended as "equivalent flat rate" comparator. Practical choice: load-weighted average of period prices under baseline consumption.                                                                                                                    | Must be strictly positive. Current logic requires exactly one period with `rate < P_flat`.                                                       |
| `demand_elasticity` (`epsilon`)     | Exogenous behavioral parameter. Typical short-run residential ranges noted in comments: about `-0.1` to `-0.3`.                                                                                                                                                                               | Usually negative; magnitude controls shift intensity. Constant elasticity assumption.                                                            |
| receiver period                     | Derived internally as periods where `rate < equivalent_flat_tariff`; asserted to be exactly one row.                                                                                                                                                                                          | Assertion fails if zero or multiple receiving periods.                                                                                           |
| period-level `load_shift`           | Derived internally as `Q_target - Q_orig`.                                                                                                                                                                                                                                                    | Enforced zero-sum by assigning receiver period to negative sum of all others.                                                                    |
| hourly shift scalar                 | Derived internally as `Q_hour / Q_period` within each period/tier.                                                                                                                                                                                                                            | Requires non-zero period totals for stable division.                                                                                             |
| achieved elasticity tracker         | Derived internally from post-shift totals and price ratios.                                                                                                                                                                                                                                   | Diagnostic output; should be compared against input `demand_elasticity`.                                                                         |

---

## Mathematical Framework

### A) Period-level demand response

For building `b`, period `p`:

- Baseline period load: `Q_{b,p}`
- New period price: `P_p`
- Baseline equivalent flat price: `P_flat`
- Elasticity: `epsilon`

Target:

`Q^*_{b,p} = Q_{b,p} * (P_p / P_flat)^{epsilon}`

Shift:

`Delta_{b,p} = Q^*_{b,p} - Q_{b,p}`

Zero-sum enforcement (single receiver period `r`):

`Delta_{b,r} = - sum_{p != r} Delta_{b,p}`

So:

`sum_p Delta_{b,p} = 0`

### B) Hourly allocation within period

For hour `h` in period `p`:

- Hour share:
  `w_{b,h} = Q_{b,h} / sum_{k in p} Q_{b,k}`
- Hourly shift:
  `delta_{b,h} = Delta_{b,p} * w_{b,h}`
- Shifted hourly load:
  `Q'_{b,h} = Q_{b,h} + delta_{b,h}`

### C) Realized elasticity diagnostic

For period/tier:

`epsilon_realized = log(Q_new / Q_orig) / log(P_new / P_orig)`

(Implementation uses `log10`, which is equivalent for ratio-of-logs.)

---

## Assumptions and Justifications

1. Short-run temporal substitution, not long-run conservation/efficiency.\
   Justification: zero-sum shift design.

2. Constant elasticity across load levels and times.\
   Justification: simple, tractable behavioral model.

3. Single receiving period.\
   Justification: avoids ambiguous allocation but can under-represent multi-period shifting behavior.

4. Proportional hourly redistribution.\
   Justification: preserves observed shape and avoids arbitrary redistribution.

5. Marginal-cost postprocessing caveat.\
   `postprocessing.py` warns that fixed marginal prices may be inconsistent with rate-responsive loads (`cairo/rates_tool/postprocessing.py:1018`).

---

## Practical Parameter Guidance

1. Set `equivalent_flat_tariff` as a baseline comparator price:
   - recommended: load-weighted average effective energy rate under baseline usage.
2. Start `demand_elasticity` in conservative short-run range:
   - residential pilot values around `-0.1` to `-0.2`.
3. Ensure tariff design yields one clear low-price sink period if using current logic.
4. Validate post-run:
   - energy conservation by building,
   - reasonable min shifted load,
   - achieved elasticity near target.

---

## Known Gaps If Re-enabled

1. No active call path currently found from simulator to these functions.
2. Receiver-period assertion is restrictive for realistic TOU tariffs.
3. Debug checks use print statements (`"weait"`, `"wait"`) rather than structured logging/errors.
4. Interaction with dynamic marginal costs is not integrated.

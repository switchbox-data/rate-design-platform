# Plan A: Structure-preserving HP seasonal discount

**Status:** Superseded by Plan B (simplified flat seasonal). Retained for reference.

## Goal

Build the HP seasonal tariff by cloning the calibrated default structure, fixing all summer rates at their calibrated values, and scaling all winter rates down by a factor that eliminates the cross-subsidy. The resulting HP tariff preserves the default tariff's period/tier/TOU structure.

## Algorithm

Three steps, all run **before** `bs.simulate` (replaces today's `compute-seasonal-discount-inputs` + `create-seasonal-discount-tariff`). Repeated independently for **delivery** (run-5, from run-1 reference) and **delivery+supply** (run-6, from run-2 reference).

### Step 1: Clone default tariff and split mixed periods

Start from the **calibrated default** tariff JSON (from run-1/run-2 `tariff_final_config`). Read `energyweekdayschedule`, `energyweekendschedule`, and `energyratestructure`.

For each period, collect which months reference it (from both weekday and weekend schedules). Classify each period as **all-winter**, **all-summer**, or **mixed** against the policy `winter_months` from `config/periods/<utility>.yaml`.

**Split every mixed period** into two: one inheriting the winter months, one the summer months. Both get identical rates, tiers, and tier breakpoints. Update both schedule matrices so winter months point to the new winter copy and summer months remain on the original.

This is the **common case**, not a corner case. After generating default-structure tariffs from `monthly_rates` YAMLs, the period-to-month mapping reflects utility-defined seasonal boundaries, which rarely align with the Oct-Mar policy winter:

| Utility    | Structure                 | Default periods | Mixed periods | Why mixed                                              |
| ---------- | ------------------------- | --------------- | ------------- | ------------------------------------------------------ |
| **rie**    | flat (3 seasonal)         | 3               | 1             | Period 2 = Jul-Dec straddles Oct                       |
| **cenhud** | flat (monthly)            | 12              | 0             | Each month = own period                                |
| **coned**  | seasonal_tiered (2 tiers) | 2               | 1             | "Non-summer" = Jan-May + Oct-Dec                       |
| **or**     | seasonal_tiered (2 tiers) | 2               | 1             | Same as ConEd                                          |
| **psegli** | seasonal_tou              | 4               | 2             | Off-peak and on-peak non-summer span Apr-May + Oct-Dec |
| **nimo**   | flat (5 periods)          | 5               | 2             | Period 1 = Feb-Jun, Period 4 = Sep-Dec                 |
| **nyseg**  | flat (6 periods)          | 6               | 1             | Period 5 = Aug-Dec                                     |
| **rge**    | flat (3 periods)          | 3               | 2             | Period 1 = Feb-Apr, Period 2 = May-Dec                 |

**Concrete example -- ConEd (seasonal_tiered, 2 periods, 2 tiers each):**

| Before split      | Months            | Tiers        |     | After split       | Months            | Tiers        |
| ----------------- | ----------------- | ------------ | --- | ----------------- | ----------------- | ------------ |
| period 0 (summer) | Jun-Sep           | $0.171/0.196 | ->  | period 0 (summer) | Jun-Sep           | $0.171/0.196 |
| period 1 (mixed)  | Jan-May + Oct-Dec | $0.172/0.172 | ->  | period 1 (summer) | Apr-May           | $0.172/0.172 |
|                   |                   |              | ->  | period 2 (winter) | Jan-Mar + Oct-Dec | $0.172/0.172 |

**Concrete example -- PSEG-LI (seasonal_tou, 4 periods):**

| Before split             | Months            | Rate   |     | After split              | Months            | Rate   |
| ------------------------ | ----------------- | ------ | --- | ------------------------ | ----------------- | ------ |
| period 0 (summer off-pk) | Jun-Sep           | $0.110 | ->  | period 0 (summer off-pk) | Jun-Sep           | $0.110 |
| period 1 (summer on-pk)  | Jun-Sep           | $0.218 | ->  | period 1 (summer on-pk)  | Jun-Sep           | $0.218 |
| period 2 (mixed off-pk)  | Jan-May + Oct-Dec | $0.095 | ->  | period 2 (summer off-pk) | Apr-May           | $0.095 |
|                          |                   |        | ->  | period 4 (winter off-pk) | Jan-Mar + Oct-Dec | $0.095 |
| period 3 (mixed on-pk)   | Jan-May + Oct-Dec | $0.186 | ->  | period 3 (summer on-pk)  | Apr-May           | $0.186 |
|                          |                   |        | ->  | period 5 (winter on-pk)  | Jan-Mar + Oct-Dec | $0.186 |

After splitting, every period is cleanly winter or summer. The tariff is semantically identical -- same hour, same rate -- just with more period indices.

### Step 2: Compute the winter scale factor

\[
f = 1 - \frac{CS^{HP}}{Rev^{winter}_{default,HP}}
\]

- \(CS^{HP}\): aggregate weighted HP cross-subsidy from run-1 (delivery) or run-2 (supply) BAT. Computed as \(\sum_i BAT\_percustomer_i \times weight_i\) over HP buildings.
- \(Rev^{winter}_{default,HP}\): weighted HP energy revenue in winter hours under default calibrated rates. For each HP building, map each hour to its (period, tier) via the tariff schedule, look up the rate, multiply by hourly kWh, then sum over winter hours and weight.

**Linearity:** For volumetric energy charges with fixed tier breakpoints (kWh thresholds), scaling all winter rates by \(f\) scales winter revenue by exactly \(f\). Tier allocation (which kWh falls in which tier) is determined by breakpoints, not rates. So \(f\) is exact in one shot -- no iteration.

**Guards:**
- \(f < 0\): infeasible -- \(CS^{HP}\) exceeds total winter revenue. Raise error.
- \(f\) close to 0 (e.g. < 0.1): warn -- winter rates near-zero, no marginal cost signal.

### Step 3: Apply f to winter rates and write HP tariff

Multiply the rate field of every winter-active (period, tier) row in `energyratestructure` by \(f\). Summer periods untouched. Tier breakpoints (`max` / `max_usage`) unchanged. Fixed charges unchanged. Write as HP tariff JSON.

Pass to `bs.simulate` via the scenario YAML as today (hp key -> HP JSON, non-hp key -> default). CAIRO precalc finds `rate_unity ~ 1.0` for the HP key because HP revenue already equals the subclass RR target by construction.

**Revenue YAML unchanged.** No new seasonal line items.

## Why precalc should be a near-no-op for HP

Under default rates (run-1), HP customers pay \(Bills_{HP} = RR_{HP} + CS^{HP}\). The subclass RR YAML records \(RR_{HP} = Bills_{HP} - CS^{HP}\).

Our new HP tariff reduces HP winter revenue by \(CS^{HP}\). So HP revenue under the new tariff = \(Bills_{HP} - CS^{HP} = RR_{HP}\) -- exactly the YAML target. CAIRO precalc for the HP key should converge to `rate_unity ~ 1.0`.

**Caveats:**
- Fixed charges contribute to \(Bills_{HP}\) and \(CS^{HP}\) but are not scaled -- creates a small residual.
- Precalc drift should be small (< 1-2%) and empirically verifiable.

## Implementation

### New script: `utils/mid/build_hp_seasonal_tariff.py`

Single CLI that does all three steps. Called twice per utility: once for delivery (from run-1), once for delivery+supply (from run-2).

**Inputs:**
- `--path-default-tariff`: calibrated default JSON from reference run
- `--path-bat-values`: reference run's `cross_subsidization_BAT_values.csv`
- `--path-customer-metadata`: for sample weights + HP classification
- `--path-resstock-loads`: hourly load path (or base + state + upgrade for ResStock scan)
- `--path-periods`: utility period config (`config/periods/<utility>.yaml`) for `winter_months`
- `--cross-subsidy-col`: BAT metric (default `BAT_percustomer`)
- `--output`: path for the HP tariff JSON

**Core functions:**
1. `split_mixed_periods(tariff, winter_months)` -- returns tariff with all periods cleanly winter or summer
2. `compute_winter_revenue(tariff, loads, weights, winter_months)` -- returns \(Rev^{winter}_{default,HP}\)
3. `compute_scale_factor(cs_hp, rev_winter)` -- returns \(f\) with guards
4. `apply_winter_discount(tariff, f, winter_months)` -- returns tariff with winter rates scaled

### Replaces

| Today | New |
| ----- | --- |
| `utils/mid/compute_seasonal_discount_inputs.py` | Absorbed (the \(f\) computation) |
| `utils/mid/create_seasonal_discount_tariff.py` + `create_seasonal_rate` | Absorbed (structure-preserving, no 2-period collapse) |

### Justfile changes

In `rate_design/hp_rates/Justfile`, run-5/6 pre-steps currently call `compute-seasonal-discount-inputs` then `create-seasonal-discount-tariff`. Replace with a single `build-hp-seasonal-tariff` recipe that calls the new script (once for delivery, once for supply).

### Files unchanged

- `run_scenario.py` -- reads whatever HP JSON the scenario points to
- `generate_precalc_mapping.py` -- generates rel_values from HP JSON's actual rates
- Scenario YAMLs -- still point hp key to the HP JSON, non-hp to default
- Tariff maps -- still assign buildings to hp vs non-hp keys
- Revenue requirement YAMLs -- unchanged, no seasonal additions

## Relationship to other scenarios

The seasonal discount (runs 5-8), TOU (runs 9-12), and TOU flex (runs 13-16) scenarios are **parallel branches** sharing only the `hp_vs_nonhp.yaml` revenue requirement derived from runs 1-2. Plan A only modifies the seasonal branch (runs 5-8). TOU tariffs are derived from marginal costs during `all-pre` and never read the seasonal discount output.

## Adversarial review

### 1. CS^HP includes non-energy charges but we only discount energy rates

`BAT_percustomer` measures the gap between total bill (energy + fixed + demand) and cost-causation-based fair share. We reduce only energy rates. The fixed-charge and demand-charge portions of \(CS^{HP}\) are left unaddressed, so HP revenue after discounting won't exactly equal \(RR_{HP}\).

**Quantifying the gap:** Fixed charges are typically $6-$24/month ($72-$288/year). For a sample of ~5,000 HP buildings at weight ~1, that's a fixed-charge contribution to \(Bills_{HP}\) of a few hundred thousand dollars. If HP's share of fixed-charge cross-subsidy is small relative to energy cross-subsidy, the mismatch is negligible. For utilities with high fixed charges and low volumetric rates, it could matter more.

**Consequence:** `rate_unity` for HP drifts slightly from 1.0. CAIRO precalc adjusts all (period, tier) rates by the same scalar -- including summer rates we pinned. The summer drift should be proportional to the fixed-charge residual divided by total HP revenue (< 1%).

**Mitigation:** Accept and document.

### 2. Computing Rev_winter requires proper (period, tier) -> rate mapping

For single-tier periods, mapping hour -> period -> rate is straightforward. For multi-tier periods (ConEd/O&R default with 2 tiers at 250 kWh breakpoint), the tier depends on cumulative monthly consumption. To compute \(Rev^{winter}\) correctly, we'd need to simulate monthly bill calculation per building.

However, scaling all tier rates by \(f\) is exact regardless of tier allocation -- revenue scales linearly. So \(f\) being slightly off due to tier approximation means the winter discount is slightly over/under \(CS^{HP}\), but the error is second-order.

**Alternative:** Extract \(Rev^{winter}\) from run-1's monthly bill outputs (`bills/elec_bills_year_target.csv`), which gives CAIRO-computed bills by month. Subtract fixed charge to isolate energy revenue.

### 3. Supply tariffs need parallel treatment

Run-6 uses delivery+supply tariffs. The supply tariff may have a **different period-to-month mapping** than the delivery tariff (e.g. PSEG-LI supply has 48 periods vs 4 delivery). The splitting logic handles this -- same algorithm, different input.

### 4. Period splitting must handle both schedule matrices and multiple concurrent splits

The 12x24 `energyweekdayschedule` and `energyweekendschedule` may reference the same or different period indices. Splitting must scan **both** matrices, assign new period indices sequentially, and update both simultaneously. For utilities with multiple mixed periods (NiMo, PSEG-LI, RG&E each have 2), the splitting creates multiple new periods in one pass.

### 5. f close to zero (policy concern)

If \(CS^{HP}\) is large relative to \(Rev^{winter}\), winter rates approach zero -- no marginal cost signal in winter for HP customers. Guard: log a warning if \(f < 0.1\).

### 6. Minimum bill floors

If `mincharge > 0` and some HP buildings hit the minimum in winter, reducing winter rates doesn't reduce their bill. So true winter revenue under the discounted tariff > \(f \times Rev^{winter}\). For RIE, `mincharge = 0`. For other utilities, need to verify. Expected severity: low (HP customers have high winter consumption).

### Summary

**Sound:** Core algorithm, linearity of volumetric scaling, structure preservation, no YAML changes.

**Small known errors (< 1% expected):** Fixed-charge portion of \(CS^{HP}\) not addressed by energy discount; minimum bill floors; precalc drift.

**Major complexity:** Mixed-period splitting is the common case (6/7 NY utilities + RIE); must handle tiered periods, TOU periods, both schedule matrices, and multiple concurrent splits. This is the primary reason Plan B was chosen instead.

# Demand flex: frozen vs. adjusted residual

When demand-response load shifting changes the marginal cost (MC) decomposition of the revenue requirement (RR), there are two ways to handle the residual. They produce different economic outcomes and embed different assumptions about what the model is trying to say.

## Setup

Before any load shifting:

- RR target = $632M (pre-topped-up, from `compute_rr`)
- Total MC (from Cambium + distribution, weighted by original system load) = $400M
- Residual = RR − MC = $232M

After TOU customers shift load from peak to off-peak, the hourly load shape changes. Off-peak hours are cheaper, so total MC drops:

- MC_shifted = $380M

The question: what happens to the residual and the total RR?

## Approach 1: Freeze the residual (current implementation)

Freeze the residual at $232M (computed from original loads). Recompute MC from shifted loads. The new RR floats:

- Residual = $232M (frozen)
- MC_shifted = $380M
- New RR = $380M + $232M = **$612M**

The total RR **decreases by the MC savings** ($20M). Algebraically: `new_RR = original_RR + (MC_shifted − MC_original)`.

### Interpretation

The MC savings from load shifting flow through as lower total system costs. Customers who shift load to cheaper hours reduce the system's marginal cost burden, and that reduction is passed on — the utility collects less total revenue.

This frames the residual as **invariant to short-run demand response**: it represents embedded infrastructure costs (debt service, depreciation, return on equity, O&M on existing assets) that don't change because some customers rearranged when they consume. Only the marginal component — which reflects the forward-looking cost of serving load at each hour — adjusts.

The economic claim is: demand response has real value, and the model should reflect that. If the total RR stayed fixed, load shifting would have zero effect on total costs, which would mean there's no economic value to responding to TOU price signals. That undermines the entire point of TOU rate design.

### Temporal assumption

This interpretation assumes the MC savings are realized **contemporaneously** with the modeled year. But Cambium marginal costs are long-run avoided costs: avoided generation capacity, avoided bulk transmission, etc. These savings don't reduce the utility's revenue requirement in the current rate case period. They represent future infrastructure investments that won't need to be built if the load shape persists. Whether those avoided costs fall within the horizon of the current rate case settlement is an open question — and for near-term capacity that's already contracted or under construction, the answer is often no.

So the frozen-residual approach embeds a **long-run equilibrium assumption**: if this load shape were sustained, the system would eventually need less capacity, and the RR would eventually decrease. The model treats that future savings as present.

## Approach 2: Fix the RR, let the residual adjust

Keep the RR at its regulatory target. Recompute MC from shifted loads. The residual absorbs the difference:

- RR = $632M (fixed)
- MC_shifted = $380M
- Residual = $632M − $380M = **$252M**

The total RR is unchanged. The residual **increases** by the MC savings ($20M).

### Interpretation

The utility still needs to collect $632M — that's what the rate case settlement requires, and it doesn't change because some customers shifted load within a year. What changes is the **allocation**: customers who shifted to off-peak have lower marginal cost responsibility, so more of what they pay is residual. But the total cost pie is the same size.

This frames the RR as **regulatory reality**: it's set by commission proceedings based on embedded costs and near-term forecasts, and it doesn't shrink because of demand response in the modeled year. The MC decomposition still matters — it determines each customer's cost-causation share via the BAT — but the total is fixed.

The economic claim is: in any given rate case period, the utility's costs are what they are. Load shifting changes which customers are responsible for marginal costs, but it doesn't make embedded costs disappear. The value of demand response is that it changes **who pays what share of a fixed total**, not that it shrinks the total.

### Temporal alignment

This interpretation is more temporally honest for a single-year analysis. The RR reflects the utility's actual near-term cost of service. The MC prices from Cambium are still forward-looking, so there's still a hybrid temporal frame (forward-looking MC allocation against a backward-looking RR target), but at least the total stays grounded in regulatory reality rather than assuming future capacity savings are already realized.

## Which is correct?

Neither is objectively correct — they answer different questions.

| Question                                                                                      | Better approach                                                                              |
| --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| "What is the value of demand flexibility to the system over the long run?"                    | Frozen residual: let the RR decrease to reflect avoided future capacity                      |
| "How does demand flexibility change cost allocation within a fixed rate case period?"         | Fixed RR: the utility's costs are what they are; MC allocation changes but the total doesn't |
| "Should TOU customers see lower total bills because they shifted load?"                       | Frozen residual: the MC savings accrue to them as lower total costs                          |
| "Should TOU customers see a different cost-causation share even if total bills are the same?" | Fixed RR: the BAT allocation changes (less MC, more residual) but the total is regulatory    |

The frozen-residual approach is more favorable to the case for TOU rates: it shows demand response creating real dollar savings. The fixed-RR approach is more conservative: it shows demand response changing the fairness of allocation without changing the total cost.

For regulatory filings where the RR is set by a commission order and won't change based on modeled demand response, the fixed-RR approach may be more defensible. For long-run planning or rate design advocacy where you want to show the system-level benefits of flexible load, the frozen-residual approach makes the case.

## Where this lives in the code

The current implementation uses the frozen-residual approach:

- `utils/demand_flex.py`: Phase 1a computes `frozen_residual = RR − MC_original`, Phase 2 recomputes `new_RR = MC_shifted + frozen_residual`
- `rate_design/hp_rates/run_scenario.py`: the no-flex path (elasticity == 0) calls `_return_revenue_requirement_target` with a fixed RR target, which is equivalent to the fixed-RR approach (no shifting → no MC change → residual is just `RR − MC`)

Switching to the fixed-RR approach for demand flex would mean replacing the two-pass workflow with a single call: shift loads, then call `_return_revenue_requirement_target(revenue_requirement_target=rr_total)` with the shifted loads. The residual would adjust, and the total RR would stay at the regulatory target.

# Theory and practice of cost-reflective TOU rate design

## The problem

TOU tariffs charge one flat rate per period — one price for all peak kWh, one for all off-peak kWh. But marginal costs vary hour by hour, and load varies hour by hour within each period. The question is: what should that one flat rate be per period, so that the rate reflects what it actually costs the system to serve the load in that period?

## Why not a simple average of hourly MC prices?

A simple average treats every hour in the period equally. But load isn't uniform across the period. If most peak consumption happens at hour 17 (MC = $0.25/kWh) and almost none at hour 19 (MC = $0.10/kWh), the simple average says "peak costs $0.175/kWh." But the kWh that will actually be charged the peak rate are overwhelmingly at $0.25 — the $0.10 hour barely matters because almost no one is consuming then.

A simple average implicitly assumes load is flat within the period. When it isn't, the simple average under- or over-represents hours depending on how much load they carry.

## The demand-weighted average

Weight each hour's MC price by its share of total period load:

$$\text{avg\_mc\_peak} = \sum_{h \in \text{peak}} \text{MC}_h \times \frac{\text{load}_h}{\sum_{k \in \text{peak}} \text{load}_k}$$

Equivalently:

$$\text{avg\_mc\_peak} = \frac{\sum_{h \in \text{peak}} \text{MC}_h \times \text{load}_h}{\sum_{h \in \text{peak}} \text{load}_h}$$

This gives you the one flat $/kWh rate that, when applied to every kWh consumed during the period, recovers the same total MC dollars as if each kWh had been charged its true hourly MC:

$$\text{avg\_mc\_peak} \times \sum_{h \in \text{peak}} \text{load}_h = \sum_{h \in \text{peak}} \text{MC}_h \times \text{load}_h$$

That's the property that makes it the right choice: it's the rate that recovers the true total marginal cost for the period.

## Why the ratio of demand-weighted averages is the right TOU differential

With demand-weighted averages in hand, the cost-causation ratio is:

$$\text{ratio} = \frac{\text{avg\_mc\_peak}}{\text{avg\_mc\_offpeak}}$$

What does this actually say? Each side of the ratio is already the best single $/kWh rate for its period — the one that, applied to all kWh in the period, recovers the true MC dollars. So the ratio compares two quantities that are directly commensurable: the cost per kWh served during peak vs. the cost per kWh served during off-peak.

A ratio of 4.8 means: if a customer moves 1 kWh from off-peak to peak, the system's marginal cost increases by a factor of 4.8. That's the price signal a cost-reflective TOU tariff should send. In practice, this ratio sets the tariff _structure_ (peak charges 4.8× off-peak); a separate calibration step then finds the absolute $/kWh level that recovers the total revenue requirement.

> **Implementation note:** In our platform, the cost-causation ratio is stored as the `rel_value` in CAIRO's precalc period mapping. CAIRO uses it to set the tariff structure, then calibrates the absolute rate level to recover the revenue requirement.

This works because the demand-weighted average has already solved the hard problem (compressing a variable hourly cost curve into one number per period without losing accuracy). The ratio inherits that accuracy: it's comparing apples to apples, both denominators in the same units ($/kWh served), both numerators reflecting where load actually falls.

### Why not the ratio of total MC dollars?

You might think: just compute total MC in peak hours and total MC in off-peak hours, take the ratio. But this conflates the per-kWh cost signal with the volume of load in each period, and you can arrive at the same ratio with very different underlying cost structures.

Consider two scenarios with the same total MC ratio but very different economics:

**Scenario A:**

- Peak: 4 hours, 100 GWh, avg MC $0.20/kWh → $20M total MC
- Off-peak: 20 hours, 400 GWh, avg MC $0.05/kWh → $20M total MC
- Total MC ratio: $20M / $20M = **1:1**
- Demand-weighted ratio: $0.20 / $0.05 = **4:1**

**Scenario B:**

- Peak: 4 hours, 200 GWh, avg MC $0.10/kWh → $20M total MC
- Off-peak: 20 hours, 200 GWh, avg MC $0.10/kWh → $20M total MC
- Total MC ratio: $20M / $20M = **1:1**
- Demand-weighted ratio: $0.10 / $0.10 = **1:1**

The total MC ratio is 1:1 in both cases, but the economics are completely different. In Scenario A, peak kWh cost the system 4× off-peak kWh — there should be a strong TOU differential to signal that. In Scenario B, costs genuinely are the same per kWh — a flat rate is appropriate. The total MC ratio can't tell these apart because it mixes volume into the signal. The demand-weighted ratio can, because it isolates the per-kWh cost.

The fundamental issue is that the total MC ratio answers "what fraction of system costs falls in each period?" — which depends on both price _and_ how much load happens to be there. But TOU rate design needs to answer "how much does it cost per kWh in each period?" — which is purely a price question (once you account for when load falls within the period). These are different questions, and the total MC ratio answers the wrong one.

## The cost-causation ratio assumes cost-reflective periods

Everything above takes the peak/off-peak period definitions as given and asks: "given these boundaries, what's the right rate ratio?" But the ratio is only as cost-reflective as the period definitions themselves.

If the peak window is 4pm–8pm but the actual cost peak is 2pm–6pm, you've lumped some expensive hours into off-peak and some cheap hours into peak. The demand-weighted averages will faithfully reflect what each period _actually_ costs — but the tariff can't send the right signal to a customer deciding whether to consume at 3pm (cheap according to the tariff, expensive according to the system) or 7pm (expensive according to the tariff, actually past the worst of the cost peak).

A cost-reflective TOU rate requires both:

1. **Period definitions that align with the underlying cost pattern.** The peak window should capture the hours when marginal costs are genuinely highest. If it doesn't, the ratio is computed correctly but applied to the wrong hours — the signal is internally consistent but misleading.

2. **A rate ratio derived from demand-weighted MCs within those periods.** Given well-chosen boundaries, this ensures the per-kWh charge in each period reflects the actual cost of serving load there.

Neither alone is sufficient. Good boundaries with a bad ratio (say, a 2:1 differential when costs are 5:1) under-signals cost differences. A good ratio with bad boundaries sends accurate per-kWh prices for the wrong hours. The two are complementary: boundaries define _when_ the price signal changes, the ratio defines _how much_ it changes.

In practice, peak window selection involves its own analysis — typically picking the N hours with highest system MC or highest system load, subject to regulatory constraints (contiguous blocks, not too narrow, stable across days). That analysis is upstream of the ratio computation and equally important for cost-reflectiveness. The ratio math just takes the result as input.

## Why this matters for TOU design

TOU periods are coarse buckets drawn ahead of time. They don't perfectly track when costs are actually highest — a 4-hour peak window is a rough approximation of the underlying hourly cost pattern. By demand-weighting, the rate for each period reflects not just the MC prices in that window but how much load actually shows up there. This compensates for the coarseness: the resulting flat-per-period rate is the best single number that, multiplied by actual load, recovers the true hourly MC for that period, assuming the load shape repeats.

But "compensates for the coarseness" has limits. If the period boundaries are badly misaligned with costs, no amount of ratio math can fix the fact that customers face the wrong price at the wrong time. The demand-weighted ratio is the best you can do _given_ the period definitions — it doesn't validate that the definitions are right.

## Choosing cost-reflective periods

The standard approach: find the contiguous block of N hours with the highest demand-weighted marginal cost. This is the same demand-weighting idea applied to period selection rather than ratio computation.

### The method

1. Compute demand-weighted MC for each hour of the day: `dw_mc_h = MC_h × load_h`. This gives the total MC dollar contribution at each hour. Average across all days in the season (or year) to get a 24-hour profile of how much each hour costs the system.

2. Slide a contiguous N-hour window across the 24-hour profile (wrapping around midnight). For each window position, sum the demand-weighted MC values. The window with the highest sum is the peak window.

3. Everything outside the peak window is off-peak.

The key insight is the same as before: you're not looking for the hours with the highest MC _prices_ — you're looking for the hours with the highest MC _dollars_, which is price × load. An hour at 3am might have a high MC price (say, because of a maintenance outage), but if load is negligible, it doesn't matter — almost no kWh are being served at that price. An hour at 5pm might have a moderate MC price, but if load is enormous, the total cost to the system is high and it belongs in the peak window.

### Why contiguity?

The contiguity constraint is a practical concession. The mathematically optimal set of N hours (the N hours with highest individual demand-weighted MC, regardless of adjacency) would capture more MC dollars. But non-contiguous peak periods are hard for customers to understand and respond to. A 4pm–8pm peak is actionable; a peak that's hours 7, 11, 17, and 18 is not. Regulators generally require contiguous blocks for this reason.

The contiguity constraint means the window might include some lower-cost hours and exclude some higher-cost hours. This is another source of coarseness, on top of the fixed window width. The demand-weighted ratio downstream compensates for the included low-cost hours (they pull down the peak average), but it can't help with the excluded high-cost hours that end up in the off-peak bucket.

### Seasonal and utility variation

In most jurisdictions the cost pattern differs by season. Summer peaks are driven by cooling load and generation capacity scarcity in the afternoon. Winter peaks (in heating-heavy regions) may be driven by evening heating load as people come home and temperatures drop after sunset. Running the peak-window search per-season produces season-specific period definitions that track these different drivers — even with the same N, summer might land on hours 16–19 while winter lands on 17–20.

Different utilities can also end up with different peak schedules for the same season, because each utility has its own distribution marginal cost profile, service territory load shape, and generation mix on the margin. The same algorithm with the same N, applied to different utilities' data, can produce different peak hours.

> **Implementation note:** In our current tariffs, RI's summer peak is 16–19 while NY utilities land on 17–20 for both seasons. Each utility's MC data comes from its Cambium balancing area (different generation mixes) and its own distribution MC profile — same algorithm, same N = 4, different data in, different hours out.

### Choosing the window width (N)

N is a design trade-off between signal accuracy and customer actionability:

- **Smaller N** (e.g. 2 hours) concentrates the peak rate on the truly expensive hours. The peak/off-peak ratio is higher because you've isolated the cost spike. The price signal is sharper. But customers have a very narrow window to avoid — harder to respond to, especially for loads like heating and cooling that aren't perfectly shiftable.

- **Larger N** (e.g. 6 hours) gives customers more hours to shift away from, making the signal easier to act on. But it dilutes the peak average by including cheaper hours alongside the real cost peak. The ratio compresses toward 1, weakening the incentive.

At the extremes: N = 1 is essentially real-time pricing (one peak hour, maximum ratio), and N = 12 splits the day in half with a modest differential. Most TOU tariffs in practice land on 3–5 hours.

There's no closed-form formula for the optimal N. In theory you could look at the hourly demand-weighted MC profile and find a natural "elbow" — the point where expanding the window starts significantly diluting the peak average MC. If the profile has a sharp spike concentrated in 3–4 hours and then flattens, a 4-hour window captures most of the cost variation. If costs are elevated across a broader plateau, a wider window may be warranted.

The choice also depends on the customer population and the elasticity model. A wider window may capture more shiftable load (more kWh that could move), even if the per-kWh incentive is weaker. A narrow window with a high ratio sends a strong signal to fewer kWh. Which delivers more total MC savings depends on the shape of the load flexibility distribution.

In practice, N is usually a regulatory/practical judgment rather than a derived quantity: regulators set it based on precedent, customer communication considerations, and stakeholder input. The cost-causation analysis informs where to place the window and what ratio to use, but the width is typically an exogenous design choice.

> **Implementation note:** We use N = 4 for all utilities, set in per-utility periods YAMLs (`rate_design/hp_rates/<state>/config/periods/<utility>.yaml`). The value is configurable (the `tou_window_hours` field), but currently every utility uses the same 4-hour default. There's no automated elbow-detection or optimization over N — it's a fixed input.

### How period selection and ratio computation fit together

Both halves of cost-reflective TOU design — period selection and ratio computation — use demand-weighted MC from the same marginal cost and load data. The period boundaries and the rate differential are derived jointly from the same underlying hourly cost and load patterns: first find the peak window by maximizing demand-weighted MC dollars, then compute the cost-causation ratio within the resulting periods.

> **Implementation note:** `utils/pre/compute_tou.py:find_tou_peak_window` implements the sliding-window peak search. `utils/pre/derive_seasonal_tou.py` calls it per-season (splitting the year into winter and summer), then calls `compute_tou_cost_causation_ratio` on each season's slice. The result is a `SeasonTouSpec` per season (peak hours, peak/off-peak ratio, seasonal base rate), which feeds into `utils/pre/create_tariff.py` to build a URDB v7 JSON tariff.

## Assumptions: what has to hold for a cost-reflective TOU to stay cost-reflective

A cost-reflective TOU rate is designed from one year of observed (or modeled) MC prices and load shapes. It's then applied to customers in future years. For the rate to remain cost-reflective, two things need to hold:

1. **MC prices stay roughly the same.** The hourly $/kWh cost profile next year looks like the one the rate was designed from.
2. **Load shapes stay roughly the same.** The hourly pattern of when customers consume looks like the one used to compute demand-weighted averages and select peak windows.

Both assumptions are fragile for different reasons, and breaking either one makes the TOU design stale in a different way.

### What happens when MC prices change

If the hourly MC price profile shifts — prices spike at different hours, or the overall peak/off-peak price spread compresses or widens — then:

- **The ratio is wrong.** A 4.8:1 ratio was derived from last year's prices. If peak prices drop (say, new solar capacity reduces afternoon scarcity), the true ratio might be 3:1. The tariff now over-signals the cost of peak consumption — customers face a stronger incentive than costs justify.

- **The period boundaries may be wrong.** If the hours with the highest MC prices shift (say, from late afternoon to early evening as solar duck-curve dynamics change), the peak window is in the wrong place. Customers are paying peak rates during hours that aren't actually expensive anymore.

### What happens when load shapes change

Even if MC prices are identical year-over-year, a change in load shapes breaks the TOU design through two mechanisms:

**Mechanism 1: The demand-weighted averages change.** The ratio is not just a function of MC prices — it's demand-weighted. If load within the peak window redistributes (say, more load at the cheaper hours of the window, less at the expensive hours), the demand-weighted peak average drops even though the hourly prices haven't changed. The ratio was 4.8:1 last year; this year it should be 4.3:1 at the same prices because the load weights shifted.

**Mechanism 2: The period boundaries may be wrong.** The peak window was selected by finding the N hours with the highest demand-weighted MC, which is price × load. If load moves to different hours (say, EV charging pushes evening load later), the hours with the highest MC _dollars_ shift even at the same prices. The old 4pm–8pm window might capture less total MC than a 5pm–9pm window under the new load shape.

### What drives MC prices to change

MC prices at any hour reflect the intersection of demand and supply at that hour. Both sides can shift:

**Demand-side factors** (reflected in the load level that determines which generators are dispatched):

- Weather — a hotter summer or colder winter pushes load up, dispatching more expensive generators
- End-use adoption — EVs increase evening load, heat pumps increase winter load, rooftop solar reduces midday net load
- Economic activity — industrial/commercial load patterns change with business cycles
- Demand response — the very TOU response we're modeling changes system load, which changes which generators are marginal

**Supply-side factors** (the cost of generators available to meet that load):

- Fleet composition — retirements (coal plant closes), additions (new solar/wind/storage), and the resulting capacity mix
- Fuel prices — natural gas sets the marginal price in most US hours; a gas price swing directly shifts MC across the board
- Day-to-day availability — outages, maintenance schedules, wind/solar intermittency (a windless week changes the MC profile)
- Transmission constraints — congestion can make the local marginal cost differ from the system-wide cost

Note that load appears on _both_ sides. Changed load shapes affect TOU accuracy directly (via the demand-weighting mechanisms above) and indirectly (by changing which generators are marginal, which changes MC prices). This means load is a double channel: it operates through the demand-weighted math _and_ through the MC price profile. A large-scale shift in load patterns — like widespread heat pump adoption changing winter peaks — can make a TOU design stale through both channels simultaneously.

## Demand flexibility: when loads respond to TOU prices

Everything up to this point has assumed that loads don't change as a result of the TOU prices themselves. Under that assumption, the main benefit of a cost-reflective TOU rate is **fairness**: customer-level prices better reflect customer-level costs, so customers who are expensive to serve (heavy peak consumers) pay more, and customers who are cheap to serve (off-peak-heavy) pay less. No one's behavior changes; the same kWh are consumed at the same hours. What changes is who pays what.

But besides fairness, TOU rates are supposed to promote **economic efficiency**: if peak kWh are priced higher, customers have an incentive to shift load away from expensive hours and toward cheap ones. This reduces the system's total marginal cost — less peak load means fewer expensive peakers dispatched. That's a real resource savings, not just a reallocation.

Given some demand-response methodology that models how loads react to TOU prices (under some elasticity assumption), the question becomes: what happens to the TOU design itself when loads respond to it?

### If you don't adjust the TOU after load shifting

Suppose you derive a TOU rate from original loads (4.8:1 ratio, 4pm–8pm peak), customers respond by shifting load off-peak, and you don't touch the tariff. What happens?

- **The ratio is now too high.** Peak load dropped, so the demand-weighted average MC during peak hours decreased. The true cost-causation ratio might be 4.2:1, but you're still charging 4.8:1. You're over-signaling the cost of peak consumption.

- **The period boundaries might be wrong.** If the load shift was big enough to change which hours have the highest MC dollars, the peak window should move. But it doesn't — it's baked into the tariff.

- **The tariff is internally inconsistent.** It was designed for one load shape and is being applied to a different one. It's no longer cost-reflective in the same way a flat rate applied to TOU-shaped costs isn't cost-reflective.

In practice, for small elasticities (-0.1 to -0.2) and modest load shifts, these effects are small and the tariff is approximately fine. But the inconsistency is real in principle.

### Why you might want to adjust the TOU

If you care about the tariff being cost-reflective _with respect to the loads it actually induces_, then after load shifting you should:

1. Recompute the demand-weighted average MC for each period using the shifted loads
2. Take the new ratio
3. Use that as the tariff structure going forward

What does this mean for the total revenue requirement? The lower total MC from shifted loads means a lower marginal cost component. Whether total system costs decrease depends on how the non-marginal component (embedded infrastructure costs, fixed overheads — the "residual") is treated. If the residual is held fixed and only the marginal component adjusts, the MC savings flow through as reduced total costs. See the discussion of residual treatment approaches below.

> **Implementation note:** This is what Phase 1.75 in our demand-flex pipeline does: it recomputes the precalc `rel_values` from the shifted load shape so that the tariff structure matches post-flex cost responsibility. For the residual treatment, see `context/tools/demand_flex_residual_treatment.md`.

### The circularity problem

But here's the catch: the TOU rate's 4.8:1 ratio is what caused customers to shift load in the first place. After shifting, we recompute and get 4.2:1. But if customers had faced 4.2:1 from the start, they would have shifted _less_ — the incentive is weaker. With less shifting, the ratio wouldn't have dropped as far. Maybe it would have been 4.5:1.

And if they'd faced 4.5:1, they'd have shifted an intermediate amount, producing a ratio of... somewhere between 4.2 and 4.5. You can keep going:

- Rate → customers respond → new load shape → new ratio → customers respond to new ratio → newer load shape → newer ratio → ...

The true self-consistent answer is the **fixed point**: the ratio where, given _this_ ratio, customers shift to the point where the demand-weighted MC profile produces _exactly this ratio_. Neither the pre-shift 4.8 nor the post-shift 4.2 is that fixed point — they bracket it.

A common practical approach is to do **one iteration** (original ratio → shift → recompute ratio) and stop. For small elasticities, one step is probably close to the fixed point — the ratio moves by a modest amount, and the second-order correction would be smaller still. But it's a first-order approximation, not the equilibrium.

> **Implementation note:** Our demand-flex pipeline does exactly one iteration: derive the initial TOU, shift loads, recompute the ratio. It does not iterate to convergence. See `context/tools/cairo_demand_flexibility_workflow.md` for the full pipeline.

## From partial equilibrium to general equilibrium

The preceding sections identified two distinct problems where TOU rate design falls short of full self-consistency:

**Problem 1: Exogenous changes to MC prices and load shapes.** The assumptions section showed that the TOU design is fit to a specific (MC price profile, load shape) pair, and either changing for reasons outside the model — weather, fleet turnover, fuel prices, new end-use adoption — makes the TOU stale. A typical TOU analysis takes MC prices and baseline load shapes as fixed inputs and doesn't model these changes.

**Problem 2: The TOU-load-TOU circularity.** The demand flexibility section showed that TOU prices change loads, changed loads change the cost-causation ratio, and the changed ratio would change loads differently. A one-iteration approach (shift loads, recompute ratio) doesn't iterate to the fixed point, and critically, it doesn't update the MC _prices_ — only the MC _dollars_ (via changed load weights on the same prices).

Both problems arise because the standard analysis is **partial equilibrium**: it takes some things as fixed (MC prices, baseline loads) and analyzes what happens when one thing changes at a time. A **general equilibrium** analysis would let everything adjust simultaneously until the whole system is mutually consistent.

### What partial equilibrium looks like

A typical partial equilibrium TOU analysis proceeds sequentially:

1. Take MC prices as given (from a dispatch or capacity expansion model that ran with its own load assumptions)
2. Take baseline load shapes as given (from building simulation, historical data, or load forecasts)
3. Derive a TOU rate from (1) and (2): select peak window, compute cost-causation ratio, set absolute level to recover the revenue requirement
4. Optionally model demand response: shift loads based on the TOU rate and an elasticity parameter
5. Recompute MC _dollars_ and TOU ratio from shifted loads, but at the same MC _prices_
6. Stop

The MC prices never update. The load shapes update once (in step 4) but don't re-respond to the adjusted rate (from step 5). Exogenous changes (weather, fleet, fuel) aren't modeled at all.

> **Implementation note:** In our platform, MC prices come from NREL's Cambium model (a national capacity expansion and dispatch model) and baseline loads come from NREL's ResStock (building energy simulation). Steps 4–5 correspond to our demand-flex pipeline's Phase 1.5 (load shifting) and Phase 1.75 (ratio recomputation).

### What general equilibrium would look like

A fully self-consistent analysis would close both loops — the TOU-load circularity _and_ the load-MC price feedback:

1. Start with an initial MC price profile and load shape
2. Derive a TOU rate from them (periods, ratio, level)
3. Model how customers respond to that rate (shift loads based on elasticity)
4. Feed the shifted _aggregate_ load back into a dispatch model to get **new MC prices**. Different load levels at each hour mean different generators on the margin, so the hourly $/kWh prices change.
5. With the new MC prices and shifted loads, re-derive the TOU rate (new period boundaries if the cost pattern shifted, new ratio from demand-weighted averages at new prices and loads)
6. Model how customers respond to the _new_ rate (different ratio → different incentive → different shift)
7. Repeat steps 4–6 until convergence

At convergence, everything is mutually consistent: the MC prices reflect the load shapes that result from customers responding to a TOU rate that was derived from those same MC prices and load shapes. The TOU rate is a fixed point of the system, not a snapshot from one moment in an evolving process.

This would resolve both problems simultaneously:

- **Problem 1 (exogenous MC/load changes):** By re-running the dispatch model in the loop, changes to the fleet, fuel prices, and non-residential load would be reflected in the MC prices at each iteration. The converged TOU rate would be consistent with the full set of system conditions, not just one year's snapshot.
- **Problem 2 (TOU-load circularity):** By iterating until the rate, loads, and prices stabilize, you'd find the true fixed-point ratio rather than a one-step approximation. The rate would be the one that induces exactly the load shift that justifies exactly that rate.

### Why this isn't standard practice

- **Dispatch models are expensive to run.** National-scale capacity expansion and dispatch models (the source of MC prices) are major modeling efforts. Re-running one in a loop with modified load shapes is not something that can be done casually. Step 4 above would require either re-running the dispatch model or building a simpler surrogate, neither of which is trivial.
- **The feedback may be small.** Residential TOU customers are typically a small share of total system load. Their demand response probably doesn't move the needle on which generators are marginal at the interconnection level. The MC price feedback is likely negligible as long as the modeled population is small relative to total system demand. The TOU-load circularity is also small for modest elasticities — one iteration gets close to the fixed point.
- **Regulatory practice doesn't require it.** Rate cases are built on partial equilibrium analysis — observed costs, projected loads, exogenous MC inputs. General equilibrium modeling would be a significant departure from how tariffs are actually designed, litigated, and evaluated.

The partial equilibrium approach is the standard in rate design practice and in the academic literature. It's a known limitation, not a flaw — as long as the modeled population is small enough that their behavior doesn't meaningfully change system-level MC prices, and the elasticity is small enough that one iteration approximates the fixed point. If electrified end-use adoption (heat pumps, EVs) scales to the point where residential load fundamentally reshapes the dispatch stack, or if demand elasticity grows as smart-home technology improves, these assumptions would need revisiting.

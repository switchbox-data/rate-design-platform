# Fairness in cost allocation: from beneficiary-pays to practical rate design

A step-by-step guide to the philosophy of who should pay for electricity system costs, why cost-causation is the right principle for some costs but not others, and how the same foundational principle — beneficiaries should pay — leads to different allocation mechanisms for different kinds of costs.

---

## 1. Fairness and equity: two conceptions of who should pay

Start with a simple question: when a cost is incurred to provide a service, who should pay for it?

There are two distinct and widely held answers, and they rest on different conceptions of justice.

**Fairness** means **proportionality between benefit and payment**: people who benefit from a cost should bear that cost, in proportion to the benefit they receive. This is the **beneficiary-pays principle**. It is a _relational_ concept — it asks about the correspondence between what you get from the system and what you are asked to contribute. A customer who draws 5 kW during the substation's peak hour benefits more from that substation than a customer drawing 1 kW, and fairness says the first customer should pay more — regardless of their income, household size, or any other personal characteristic. Fairness is about the relationship between a person and a cost, not about the person's circumstances.

**Equity** means **consideration of people's capacity to bear costs**: the allocation should account for differences in economic position, and the burden should not fall disproportionately on those least able to bear it. This is the **ability-to-pay principle**, rooted in the Rawlsian tradition of distributive justice. It is a _consequentialist_ concept — it asks about the impact of the payment on people's lives. A low-income household spending 10% of income on energy is bearing an unjust burden, and equity says their share should be reduced — regardless of how much infrastructure benefit they receive. Equity is about the relationship between a person and their circumstances, not between the person and the cost.

These principles can point in opposite directions. Consider a low-income family in a large, poorly insulated house drawing 8 kW during summer peak. Fairness says they should pay more than a wealthy retiree in a small efficient apartment drawing 1 kW — because they benefit more from the infrastructure. Equity says the opposite — because they can afford less. A rate that satisfies both principles simultaneously does not exist for this pair of customers.

The resolution, developed over the rest of this document, is that fairness and equity need not govern the same costs. The electricity bill can be decomposed into components, and different principles can govern different components: fairness for the costs that track current behavior (marginal costs), equity for the costs that are sunk and must be recovered regardless (the residual). This decomposition—between marginal and residual costs—is the foundation of the modern economic literature on rate design, of the Bill Alignment Test and of the tariff design philosophy described here.
https://github.com/switchbox-data/rate-design-platform/blob/main/context/methods/bat_mc_residual/bat_lrmc_residual_allocation_methodology.md#9-interpretation-what-the-mcresidual-split-means-and-the-steady-state-assumptioni
---

## 2. Why apply fairness to electricity cost allocation?

We have defined fairness as beneficiary-pays. Why should we adopt it as the governing principle for electricity cost allocation? There are two complementary reasons — one normative, one instrumental — and they reinforce each other.

### The normative reason: fairness as justice

If you accept that people who benefit from a service should bear its cost, then cost allocation should track benefit. This is a moral commitment, not a derived result — it is a choice about what kind of society we want. Under this view, a customer who imposes large costs on the system and shifts those costs to others is committing an injustice, even if the outcome happens to be economically efficient. Cost allocation that tracks benefit is just; cost allocation that doesn't is unjust.

### The instrumental reason: fairness produces efficiency

Fairness-as-beneficiary-pays also has a powerful instrumental justification: it produces **economic efficiency**. When beneficiaries pay, prices reflect the actual cost of providing the service. Customers who value the service more than it costs will consume it; customers who don't, won't. Resources flow to their highest-valued use. No one overconsumes (because the price is not subsidized) and no one underconsumes (because the price is not inflated).

**Efficiency** — in the economic sense — means that no reallocation of resources can make anyone better off without making someone else worse off. An efficient tariff leaves no unexploited gains from trade: every kWh consumed is valued by the consumer at least as much as it costs the system to produce and deliver.

Why should anyone who is not an economist care about efficiency? Because its absence has concrete consequences:

- **Overbuilding the grid.** If capacity costs are not charged to the customers who trigger them, customers face no price signal for peak demand. The result is more peak demand than there would otherwise be, more infrastructure investment to meet it, and higher costs for everyone. Efficient pricing reduces peak demand and avoids unnecessary capital expenditure.
- **Handicapping electrification.** If the retail price of electricity bundles sunk infrastructure costs into a volumetric $/kWh charge, the price per kWh is far above the actual cost of serving the marginal customer. A heat pump that costs the system 5¢/kWh to serve but faces a retail rate of 25¢/kWh (inflated by sunk cost recovery) looks five times more expensive than it actually is. The customer sticks with gas — an inefficient outcome that slows electrification.
- **Slowing the transition to clean electricity.** In the current technological regime, energy prices are correlated with emissions — the marginal generator is typically a fossil plant, and the highest-emission hours are the highest-price hours. Efficient pricing that encourages consumption when prices are al and discourages unnecessary peak consumption when prices are high helps renewables and reuces the hours when the dirtiest peaker plants run. And efficient pricing that does not artificially inflate the cost of electricity therefore accelerates the transition from fossil fuels to clean electric alternatives.

The instrumental case for fairness-as-beneficiary-pays is, in short: it produces prices that lead to better decisions — less overbuilding, lower bills, faster electrification, less climate and air. These are not abstract welfare-theoretic concerns. They are concrete outcomes that affect everyone's bills and the planet.

### Cost-causation: a special case, not the whole story

**Cost-causation** — the principle that customers should pay for the costs they _cause_ — is perhaps the most frequently invoked principle in rate design proceedings. But cost-causation is not a freestanding principle. It is a _special case_ of beneficiary-pays: it applies when the person who causes a cost is the same person who benefits from it, at the same time.

As we will see, this alignment holds for marginal costs — the energy consumed this hour, the capacity investment triggered this year. For these costs, cost-causation and beneficiary-pays are identical, and cost-reflective pricing is both fair (in the sense described above) and efficient (in the economic sense). But marginal costs are a small fraction of the bill — typically 2–15% of the revenue requirement for NY utilities. The remaining 85–98% is sunk infrastructure cost, where the original causers are long gone and the current beneficiaries are everyone connected to the grid. For these costs, cost-causation fails — on pracitcal, efficiency, _and_ fairness grounds — and beneficiary-pays, the principle that _motivated_ cost-causation in the first place, points in a different direction entirely.

---

## 3. Two kinds of electricity costs

Electricity systems incur two fundamentally different kinds of costs, and the difference matters for everything that follows.

### Energy costs (short-run, consumed immediately)

When a customer draws a kWh from the grid at 3 PM on a Tuesday, a generator somewhere burns fuel to produce it. The cost is immediate: fuel, variable O&M, emissions. The kWh is consumed in the moment it is produced. There is no lasting asset — the fuel is gone, the electricity is gone, the cost is incurred and done.

**Example:** A natural gas peaker plant produces electricity at 8¢/kWh. That 8¢ is the short-run marginal cost (SRMC) of energy. The customer who consumes the kWh benefits from it (they get electricity) and causes the cost (the generator burns fuel to serve them). Cause and benefit are perfectly aligned, in the same hour, for the same customer.

### Capacity costs (long-run, paid off over decades)

When peak demand approaches the system's capacity limit, the utility builds new infrastructure: a substation, a feeder, a transmission line. This investment costs millions of dollars and enters the rate base, where it is paid off over 30–50 years through the revenue requirement. The asset provides service for decades. The customers who use it in year 20 may be entirely different from the customers whose load growth triggered the investment in year 1.

**Example:** A $10 million substation is built in 2010 because load growth in a developing neighborhood exceeded the existing substation's capacity. The substation enters service in 2012 and will be paid off by 2042. In 2012, the customers who triggered the investment are the same customers the substation serves. By 2026, half of the original residents have moved away; new families have moved in; the load profile has changed. The substation still serves the neighborhood — but the connection between "who caused it" and "who uses it" has weakened.

This difference — between costs that are incurred and consumed in the same instant, and costs that are incurred once and provide service for decades — is the source of nearly every difficulty in electricity cost allocation.

---

## 4. Energy costs: the easy case

For energy costs, beneficiary-pays produces a clear and uncontroversial answer.

**Who benefits?** The customer consuming electricity in that hour. They get light, heat, cooling, or motive power.

**Who caused the cost?** The same customer. Their demand in that hour caused the marginal generator to run, burning fuel at a cost of $MC^{\text{energy}}_h$.

**Are cause and benefit aligned?** Yes. The causer and the beneficiary are the same person, at the same time, for the same quantity.

**What follows?** The customer should pay the marginal cost of energy in each hour: $p_h = MC^{\text{energy}}_h$. This is a **cost-reflective charge**: a volumetric rate ($/kWh) that varies by hour and tracks the short-run marginal cost of generation. It sends the right price signal (customers who consume during expensive hours pay more) and satisfies both the fairness and efficiency justifications of beneficiary-pays.

This is the standard result in electricity pricing theory. Borenstein (2016) calls it "social marginal cost" pricing for the energy component; the BAT paper (Simeone et al. 2023) uses it as the energy component of the economic burden; Pérez-Arriaga et al. (2017) classify it as a "cost-reflective charge." There is no controversy here.

---

## 5. New capacity costs: cost-causation in year 1

Now consider the moment a new capacity investment is triggered.

**The trigger:** Load growth in a specific area exceeds the existing infrastructure's capacity. The utility's planners determine that a new feeder, substation, or transmission line is needed.

**Who caused the cost?** The customers whose load growth triggered the investment. Their peak-hour consumption — specifically, their contribution to the system peak during the hours when existing capacity was exhausted — is what made the investment necessary.

**Who benefits?** The same customers. The new infrastructure serves the load that triggered it. In year 1, the customers whose peak demand caused the substation to be built are the customers whose peak demand the substation serves.

**Are cause and benefit aligned?** Yes — in year 1. The causer and the beneficiary are the same entity, at the same time, for the same capacity.

**What follows?** The customer should pay the forward-looking incremental cost (FLIC) of capacity, allocated to the hours when their load contributes to the peak that triggers investment:

$$p^{\text{capacity}}_h = MC^{\text{capacity}}_h$$

where $MC^{\text{capacity}}_h$ is nonzero only in peak hours and reflects the annualized cost of the next increment of capacity per kW of peak demand. This is a **cost-reflective charge** for capacity: a peak-coincident charge that tracks the long-run marginal cost of infrastructure. It is cost-causation, and it is beneficiary-pays, and they are the same thing.

**Example:** A new feeder costs $500,000 and adds 5 MW of capacity. The annualized FLIC is $50,000/year, or $10/kW-yr. Customers whose load contributes to the feeder's peak hours pay $10/kW-yr proportional to their peak contribution. A customer contributing 2 kW to the peak pays $20/year. This is both the cost they caused and the benefit they receive (2 kW of capacity available during peak hours).

This is the basis for the marginal cost of capacity in our methodology (see `context/methods/bat_mc_residual/bat_lrmc_residual_allocation_methodology.md`, §§5–7). ICAP prices, NYISO incremental benefit studies, and MCOS incremental diluted capital are all FLIC measures — they capture the cost of the next capacity increment and allocate it to peak hours. The literature uniformly classifies these as cost-reflective charges that should be priced at their marginal cost level.

---

## 6. The passage of time: what happens to capacity costs

Here is where the story gets complicated. Energy costs are born and die in the same hour. Capacity costs are born in one year and live for decades. Follow a single investment through time:

**Year 0 (planning):** Load growth projections show that Substation X will be overloaded by 2012. The utility initiates a $10M upgrade project.

**Year 1 (in-service, 2012):** The upgraded substation enters service. It serves the customers whose load growth triggered the project. Cost-causation and benefit are aligned. The annualized FLIC ($600K/year at a 6% carrying charge rate) should be recovered through a cost-reflective peak-coincident charge from those customers.

**Year 5 (2017):** Some of the original customers have moved away. New customers have moved in. A small business has opened. The substation still serves the neighborhood, but the customer base has partially turned over. The customers _causing_ load in 2017 are somewhat different from the customers who _caused_ the investment in 2010. But the load profile — summer-peaking, evening-heavy — is broadly similar.

**Year 10 (2022):** The neighborhood has changed significantly. Several homes have installed heat pumps, shifting load toward winter. A few have rooftop solar, reducing summer net demand. The load profile that the substation serves is now measurably different from the one that triggered its construction. The original "cause" of the investment is a historical fact about 2010 load growth; the current "benefit" of the investment is the 2022 load it serves.

**Year 20 (2032):** Half the homes have turned over. The substation's remaining useful life is 10 years. It serves a load profile that looks nothing like the one that triggered it. Some of the capacity may be underutilized (if the area's load has declined) or over-utilized (if load has grown beyond the upgrade). The causal connection between 2010 load growth and 2032 customers is almost entirely severed.

**Year 30 (2042):** The substation is fully depreciated. Its cost has been recovered through the revenue requirement over 30 years. The customers who paid for it in years 1–30 include a shifting population that overlaps only partially with the customers who triggered it.

The pattern is clear: **in year 1, cause and benefit are aligned. Over time, they diverge.** The cost is fixed (sunk); the benefit is ongoing and shifts to whoever is currently connected. Cost-causation — "charge the people who caused the cost" — becomes progressively less meaningful as the causal connection fades and the beneficiary population changes.

---

## 7. Three reasons cost-causation fails for sunk costs

By year $n$, cost-causation is no longer the right allocation principle for the substation's cost. There are three independent reasons, and they reinforce each other.

### 7a. The measurement problem (you can't do it)

Even if you wanted to charge the original causers, you can't:

- **Customer turnover:** The people who lived in the neighborhood in 2010 may have moved to another state. You cannot charge them.
- **Record limitations:** Utility planning records from a decade ago may not specify which customers' load growth triggered which specific project. Planning decisions are driven by area-level load forecasts, not individual customer load profiles.
- **Load evolution:** Even for customers who haven't moved, their load profile has changed. A customer who ran a high-demand home business in 2010 may have retired and now uses half the electricity. Is their 2010 load or their 2026 load the relevant "cause"?

This is a _practical_ impossibility, not a _conceptual_ one. If perfect records existed and no one ever moved, you could in principle trace the causal chain. But they don't, and they do.

### 7b. The efficiency problem (you shouldn't do it)

Even if you _could_ trace the causal chain, charging current customers based on historical cost-causation would be economically inefficient:

- **Sunk costs are sunk.** The substation exists whether anyone pays for it or not. No current decision — consuming more, consuming less, installing solar, buying a heat pump — can undo the investment. Pricing as though current behavior caused the sunk cost creates a _fictitious_ marginal cost signal: the customer sees a price above true marginal cost, and curtails consumption (or avoids electrification) even though their curtailment saves the system nothing.
- **Distortion of behavior.** If the substation's sunk cost is recovered through a volumetric $/kWh charge (the dominant U.S. practice), every kWh of consumption appears to cost more than it actually does. A heat pump that saves the system 3¢/kWh of marginal cost but faces a retail rate of 25¢/kWh (inflated by sunk cost recovery) looks uneconomical to the customer, even though it is efficient from the system's perspective. The sunk-cost-laden price distorts the electrification decision.
- **The efficient recovery mechanism** is one that does not affect marginal decisions: a **fixed charge** — a payment that is the same regardless of how much the customer consumes. This recovers the sunk cost without distorting any consumption, investment, or technology-adoption decision.

### 7c. The fairness problem (beneficiary-pays says you shouldn't)

This is the most subtle argument, and the most important. The beneficiary-pays principle — the same principle that _justified_ cost-causation in year 1 — now argues _against_ it in year $n$.

Recall the logic in year 1: "The customers whose peak load triggered the substation should pay for it, _because they are the ones who benefit from it_." Cost-causation was fair because cause and benefit were aligned. The causer was the beneficiary; charging the causer was charging the beneficiary.

In year $n$, this alignment is gone:

- **The original causers** may no longer be present, may have changed their load profile, or may no longer benefit from the substation at all (they moved to a different feeder).
- **The current beneficiaries** — everyone whose load the substation currently serves — had nothing to do with the original investment decision. They didn't cause the substation to be built. But they benefit from it every day.

If fairness means "beneficiaries should pay," then in year $n$ the current beneficiaries should pay — not the historical causers. Cost-causation, applied to sunk costs, would charge the wrong people (or their statistical proxies) while letting the actual beneficiaries off the hook. This is _less fair_ than benefit-proportional allocation, judged by the very same principle that motivated cost-causation in the first place.

```
┌──────────────────────────────────────────────────────────────────┐
│                  YEAR 1                    YEAR n                │
│                                                                  │
│  Causer ←──── same person ────→ Beneficiary   (aligned)         │
│    │                                                             │
│    ▼                                                             │
│  Cost-causation = Beneficiary-pays  ✓                            │
│                                                                  │
│  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
│                                                                  │
│  Causer (2010) ──── moved away / changed / unknown               │
│                                                                  │
│  Beneficiary (2026) ──── everyone currently using the asset      │
│                                                                  │
│  Cost-causation ≠ Beneficiary-pays  ✗                            │
│  Beneficiary-pays → proportional to current benefit              │
└──────────────────────────────────────────────────────────────────┘
```

This is the critical insight: **cost-causation is not a freestanding principle. It is a derivative of beneficiary-pays that works when cause and benefit are aligned. When they diverge, beneficiary-pays remains the foundational principle, and it points away from cost-causation.**

---

## 8. What "benefit" means for sunk capacity

If sunk costs should be allocated by benefit rather than by historical causation, we need to define what "benefit" means for an asset that already exists.

The benefit of a sunk capacity asset is: **having capacity available when your load needs it.** Not "having triggered the investment" (that's cost-causation, and it's backward-looking). Not "consuming energy" (that's the energy benefit, already priced at SRMC). The capacity benefit is the ability to draw power from the grid at any moment, up to the capacity the infrastructure provides.

**Who benefits?** Everyone whose load is served by the asset during the hours when the asset's capacity is being used. This means:

- A customer with 5 kW of demand during the substation's peak hour benefits from 5 kW of the substation's capacity.
- A customer with 1 kW of demand during the same hour benefits from 1 kW.
- A customer with zero demand during that hour (e.g., they are away, or their solar panels are covering their load) benefits from zero kW in that hour.

The benefit is proportional to the customer's load during the hours when the capacity is needed — which is, conceptually, a **coincident peak** measure. Not coincident with the system peak in the year the asset was built, but coincident with the asset's current utilization peak.

**Example:** A substation serves 200 homes and a small commercial strip. During the substation's peak hour (a hot July afternoon), the 200 homes collectively draw 800 kW and the commercial strip draws 200 kW. The substation's capacity benefit in that hour is allocated: 80% residential, 20% commercial. Among the residential customers, the allocation is proportional to each home's demand during that hour — a home running central AC at 4 kW gets 4x the allocation of a home drawing 1 kW.

---

## 9. The theoretical framework for benefit-proportional sunk cost allocation

Given the definition of benefit in §8, we can sketch — in principle — a fully rigorous benefit-proportional allocation of sunk costs.

### Step 1: Identify every sunk asset still being paid off

The utility's rate base contains hundreds or thousands of individual assets: substations, feeders, transformers, transmission lines, each with a remaining book value and an annual revenue requirement. This is the pool of sunk costs to allocate.

### Step 2: For each asset, identify who it serves

- A substation serves all customers on its secondary feeders.
- A feeder serves all customers connected to it.
- A transmission line serves all customers in the load zones it connects.
- The bulk grid serves everyone.

The service population is asset-specific and known from the utility's network topology.

### Step 3: For each asset, identify when its capacity is being used

An asset's capacity is "needed" (its benefit is realized) during the hours when load approaches or exceeds the asset's rating. For a substation rated at 1,000 kW, the benefit is concentrated in the hours when load is near 1,000 kW. In hours when load is 300 kW, there is surplus capacity — the asset still provides service, but the marginal benefit of having the full 1,000 kW available is lower.

For a simple first pass: allocate the asset's annual revenue requirement to its top $K$ load hours using an exceedance method (the same approach used for marginal cost allocation in our methodology). Customers active during those hours receive benefit in proportion to their load.

### Step 4: Compute each customer's benefit share

For each asset $a$ and each peak hour $h$ of that asset:

$$\text{Benefit}*{i,a,h} = \frac{L*{i,h}}{\sum_j L_{j,h}}$$

where the sum is over all customers served by asset $a$. The customer's annual benefit from asset $a$ is:

$$\text{Benefit}*{i,a} = \sum_h w*{a,h} \times \text{Benefit}_{i,a,h}$$

where $w_{a,h}$ weights each peak hour (e.g., by exceedance above a threshold). The customer's total sunk cost allocation is:

$$\text{Sunk Cost}*i = \sum_a RR_a \times \text{Benefit}*{i,a}$$

This gives a **differentiated fixed charge per customer**: a dollar amount per year that varies by customer based on their load profile, location, and the specific assets that serve them.

### The easy case: one asset, homogeneous customers

Consider a single feeder serving 100 homes with identical load profiles. The feeder's annual revenue requirement is $50,000. Each home's benefit share is 1/100 = 1%. Each home pays $500/year.

Now introduce heterogeneity: 50 homes have heat pumps (high winter load, lower summer peak), 50 do not (higher summer peak). The feeder's peak hours are summer afternoons. The non-HP homes contribute more to the feeder's peak and therefore receive a larger benefit share — say 60% vs. 40%. Non-HP homes pay $600/year; HP homes pay $400/year. This is benefit-proportional and satisfies the fairness principle.

---

## 10. Three complications that make the theoretical framework hard

The framework in §9 works cleanly for a single asset with a clearly defined peak. Real networks introduce three complications that make the fully rigorous allocation much harder.

### 10a. Joint costs: when one asset serves multiple purposes

A substation provides not just peak capacity but also voltage regulation, fault isolation, and switching flexibility. A transmission line provides both capacity and loss reduction. Different customers may benefit differently from each function — a customer near the end of a long feeder benefits more from the substation's voltage support than a customer near the substation, even if their peak loads are identical.

When an asset serves multiple purposes, its cost is a **joint cost**: it cannot be cleanly attributed to any single function. Allocating the full cost based on peak capacity alone ignores the voltage, reliability, and switching benefits that other customers receive. The problem is familiar from everyday life: if three roommates share an apartment, you can't say "the kitchen costs $500/month and Alice uses it 40% of the time, so she owes $200." The kitchen's cost is joint — it exists for all of them simultaneously, and the benefit to each person depends on who else is there.

The intuitive solution is: instead of trying to divide the asset's cost by function, ask a different question — "how much would total costs change if this customer weren't on the system at all?" Then average that answer over every possible way of assembling the customer base, so no one benefits from being "first" or "last" in line. This averaging is the core idea: each customer's share reflects their average marginal contribution, accounting for the fact that benefits overlap.

**In economic terms:** Cooperative game theory formalizes this as the **Shapley value** — a method that allocates joint costs by averaging each participant's marginal contribution over all possible orderings of participants. If there are $n$ customers, the Shapley value considers all $n!$ orderings and computes each customer's average marginal contribution to the cost. This is mathematically well-defined and satisfies several desirable fairness axioms (symmetry, efficiency, additivity). But computing it requires evaluating the cost function for every possible coalition — $2^n$ evaluations for $n$ customers — which is computationally intractable for any real network.

### 10b. Insurance value: when capacity isn't always "used"

Grid infrastructure is built with redundancy. The N-1 reliability criterion requires that the system can withstand the loss of any single element without interruption. This means a significant fraction of capacity exists not for normal-operation peak load but for contingency: it provides "insurance" against equipment failure.

A backup transformer that sits idle 99% of the time still provides real benefit: the assurance that if the primary transformer fails, customers won't lose power. But who benefits from capacity that is almost never "used"? If you only allocate costs based on normal-operation peaks (as §9 does), you assign zero benefit to the backup transformer — even though everyone on the feeder sleeps better knowing it's there. The problem is that the benefit of insurance is invisible in normal operations and only materializes in rare failure events.

The intuitive solution is: don't just look at what happens on a normal day — look at what _could_ happen. For each possible failure (a transformer blows, a line goes down), ask who would benefit from the backup capacity in that scenario, and how much. Then weight each scenario by how likely it is to occur. The result is a benefit measure that accounts for the insurance value of redundancy, not just the peak-serving value.

**In economic terms:** This is an **expected value** computation over contingency states: for each possible failure scenario (with its probability), determine which customers benefit from the redundant capacity and how much, then take the probability-weighted average. More precisely, the insurance value depends on the probability of the contingency, the customer's load during the contingency, and the cost of interruption — a customer who would lose $10,000/hour from a power outage benefits more from redundancy than a customer who would lose $100/hour. This is conceptually clean but requires modeling every contingency scenario — thousands of possible equipment failures — and each customer's value of lost load.

### 10c. Network interdependence: when benefit depends on everything else

In a network, the value of any single asset depends on every other asset. Adding a new transmission line changes the flow patterns on every existing line. Removing a substation changes the loading on neighboring substations. The benefit that Customer A receives from Substation X depends on whether Substation Y exists — because if Y were removed, X would serve Y's customers too, changing the peak loading and benefit allocation entirely.

This means the benefit of any individual asset cannot be determined in isolation. It is a function of the entire network configuration — like trying to determine the value of one road in a highway system without knowing which other roads exist. This is the classic network joint-cost problem, and it is much harder than the single-asset joint-cost problem in §10a, because now the "roommates" are not just customers sharing an asset but assets sharing a network.

The intuitive solution is the same averaging idea from §10a, but applied to assets as well as customers: ask "how much would total network value change if this asset weren't here?" and average over every possible ordering of assets. Each asset's contribution to the network — and therefore each customer's benefit from that asset — is computed as a marginal contribution averaged over all configurations.

**In economic terms:** This extends the Shapley value framework to assets as well as customers, computing the marginal contribution of each asset to the network's total value. Or one can use the **Aumann-Shapley value** (the continuous-player generalization) for divisible capacity. Both are well-defined mathematically. Neither is computationally feasible for a real utility network with thousands of assets and millions of customers.

---

## 11. The theoretical solution exists — and it's a differentiated fixed charge

Despite the complications in §10, the theoretical framework for fully rigorous benefit-proportional sunk cost allocation _does_ exist. In principle, you could:

1. **Handle joint costs** via Shapley values (averaging marginal contributions over all orderings).
2. **Handle insurance value** via expected value over contingency states (probability-weighting each failure scenario).
3. **Handle network interdependence** via multi-dimensional decomposition (Shapley or Aumann-Shapley over both customers and assets).

If you could compute all of this, the result would be: **a differentiated fixed charge per customer per year.** Each customer would pay a specific dollar amount reflecting the probability-weighted, jointly-allocated, multi-function benefit they receive from every sunk asset in the network. The charge would vary by customer — a large home near a heavily loaded substation in an area with little redundancy would pay more than a small apartment in a well-reinforced part of the grid.

Critically, this charge would be **fixed** — it would not depend on the customer's current consumption or future behavior. It would be computed from the customer's location, load profile, and the network topology — all observable characteristics that are independent of marginal consumption decisions. It would therefore satisfy the efficiency requirement: no distortion of consumption or investment decisions.

This is a remarkable convergence. The beneficiary-pays principle, rigorously applied to sunk costs using the best available theoretical tools, produces the same recommendation as the efficiency-only argument: **fixed charges.** The efficiency argument says "fixed charges don't distort behavior." The fairness argument says "fixed charges reflect who benefits from sunk infrastructure." They arrive at the same destination from different starting points.

---

## 12. Why we can't compute the theoretical solution in practice

The theoretical framework in §11 is computationally intractable:

- **Shapley values** for $n$ customers require $2^n$ evaluations of the cost function. For a utility with 1 million residential customers, this is absurd.
- **Contingency enumeration** for a network with 10,000 elements requires modeling $\binom{10000}{1} \approx 10^4$ N-1 scenarios and $\binom{10000}{2} \approx 5 \times 10^7$ N-2 scenarios. Each requires a power flow solution.
- **Multi-dimensional Shapley** over both customers and assets multiplies these combinatorial explosions together.

Beyond computation, the framework requires **perfect information**: the exact network topology, the exact load profile of every customer in every hour, the value of lost load for every customer, and the joint cost function of every asset. Utilities have approximate versions of some of this data, but nothing close to what the theoretical framework demands.

So the theoretical answer exists but cannot be computed. This leaves practitioners with two options: approximate the theory with feasible proxies, or adopt a different principle entirely.

---

## 13. What we can do in practice

### 13a. Start with uniform fixed charges

The simplest approximation of benefit-proportional allocation is: **equal per-customer fixed charges.** Each customer pays $R / N$, where $R$ is the total residual (sunk cost revenue requirement) and $N$ is the number of customers.

This says: "everyone connected to the grid benefits approximately equally from the existence of the network, so everyone pays the same share." It is a coarse approximation — it ignores differences in load profile, location, and the specific assets each customer uses — but it has two important virtues:

- **Zero distortion.** The charge is independent of consumption. It sends no false price signal. It does not discourage electrification, penalize high usage, or reward low usage. All marginal decisions (peak-shifting, heat pump adoption, EV charging) are driven by the cost-reflective charges, which are set at marginal cost.
- **Administrative simplicity.** No data beyond a customer count is needed. No network topology, no load profiles, no contingency modeling.

This is the recommendation of Borenstein (2016), Pérez-Arriaga et al. (2017), and Schittekatte & Meeus (2020) for the efficiency-optimal tariff. It is the default residual allocator in CAIRO (`BAT_percustomer`).

The acknowledged weakness is equity: a studio apartment pays the same as a 5,000 sq ft house. A low-income household pays the same as a wealthy household. The fixed charge is regressive — it takes a larger share of income from poorer customers. This tension between fairness (beneficiary-pays → uniform fixed charge) and equity (ability-to-pay → differentiated charge) is inherent and cannot be resolved within the beneficiary-pays framework alone.

### 13b. Refine with crude proxies for benefit-proportionality

If uniform fixed charges are too blunt, practitioners can introduce differentiation using observable proxies for the unobservable "true benefit share." Each proxy captures some dimension of benefit but introduces some distortion:

| Proxy                                                | What it captures                                                                                                                | Distortion risk                                                                                   | Example                                                                                                                 |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Connection capacity** (contracted kW tier)         | Larger connections draw more peak capacity → more benefit                                                                       | Customers may choose a low tier and exceed it, or a high tier and waste it                        | France: 3 kW = EUR 75/yr, 9 kW = EUR 160/yr, 15 kW = EUR 285/yr                                                         |
| **Historical consumption** (past 3-year average kWh) | Higher historical usage correlates with larger homes, more appliances, more infrastructure utilization                          | Frozen — cannot be gamed by changing current behavior; but penalizes conservation retroactively   | Batlle, Mastropietro & Rodilla (2020): "efficient, equitable, non-distortive"                                           |
| **Measured peak demand** (max kW in billing period)  | Peak demand is the direct driver of infrastructure capacity                                                                     | Gameable with batteries — customers can shave measured peaks without reducing infrastructure need | Burger, Knittel, Perez-Arriaga et al. (2020): peak demand correlates with income (attractive distributional properties) |
| **Property value or square footage**                 | Larger/more valuable homes correlate with higher benefit (more appliances, higher demand, more infrastructure serving the area) | Not directly related to electricity usage; politically contentious                                | Abdelmotteleb, Perez-Arriaga & Gomez (2018): included as a permissible allocation weight                                |

Each proxy is an imperfect approximation of the theoretical benefit share from §11. None captures joint costs, insurance value, or network interdependence. But each moves the allocation closer to benefit-proportionality than a uniform fixed charge does, at the cost of some complexity and some distortion.

The key constraint, emphasized by Pérez-Arriaga et al. (2017) and Abdelmotteleb et al. (2018), is that **the proxy must be non-reactive to current behavior.** If the allocation basis can be gamed — if a customer can reduce their allocation by changing current consumption — then the "fixed" charge becomes a de facto volumetric charge, reintroducing the distortion that fixed charges were designed to avoid. Connection capacity (if contractually fixed), historical consumption (if frozen at a reference period), and property value satisfy this constraint. Measured peak demand does not, unless the measurement window is sufficiently long and backward-looking.

---

## 14. Equity: the complementary principle for sunk costs

Everything in §§1–13 uses fairness — beneficiary-pays — as the governing principle. Fairness governs both the marginal cost component (where it produces cost-causation) and the sunk cost component (where it produces benefit-proportional fixed charges). But for the sunk cost component, there is a second option: applying **equity** instead of (or alongside) fairness.

Recall from §1: equity means consideration of ability to bear costs. Under equity, the residual should be allocated by income, not by benefit:

- **Low-income customers pay less; high-income customers pay more.** The allocation is explicitly redistributive.
- **The justification is not proportionality but justice.** The claim is not "this tracks who benefits from the grid" but "it is wrong for essential services to impose proportionally larger burdens on those least able to bear them."
- **Energy burden** — the fraction of household income spent on energy — is the metric. A high energy burden (>6% of income, by common definition) indicates that the tariff structure is placing an unjust burden on low-income households.

Equity and fairness are not fully incompatible. A tariff can apply fairness to the marginal cost component (cost-reflective charges for energy and capacity, which produce efficient price signals) while applying equity to the residual component (income-differentiated fixed charges for sunk costs, which produce equitable distributional outcomes). This is the approach implied by the Pérez-Arriaga framework's distinction between cost-reflective charges (efficiency-driven) and residual charges (normatively chosen):

```
┌─────────────────────────────────────────────────────────┐
│                  TARIFF STRUCTURE                        │
│                                                         │
│  ┌───────────────────────────────────┐                  │
│  │     Cost-reflective charges       │  Principle:      │
│  │     (energy SRMC + capacity FLIC) │  FAIRNESS        │
│  │     = Economic Burden             │  (beneficiary-   │
│  │     → Volumetric, TOU, peak       │  pays = cost-    │
│  └───────────────────────────────────┘  causation)      │
│                    +                                    │
│  ┌───────────────────────────────────┐                  │
│  │     Residual charges              │  Principle:      │
│  │     (sunk infrastructure costs)   │  FAIRNESS        │
│  │     = Fixed charge                │  (benefit-       │
│  │     → Uniform, or differentiated  │  proportional)   │
│  │       by proxy, or by income      │  and/or EQUITY   │
│  └───────────────────────────────────┘  (ability-to-pay)│
│                    =                                    │
│             Total customer bill                         │
└─────────────────────────────────────────────────────────┘
```

Under this hybrid approach, the cost-reflective component sends the right price signal (efficiency), and the residual component addresses distributional concerns (equity). Neither component does double duty — which is exactly the problem with current U.S. rate design, where volumetric rates bundle both efficiency and equity objectives into a single price that serves neither well (see `context/methods/bat_mc_residual/bat_lrmc_residual_allocation_methodology.md`, §10 on EPMC).

---

## 15. Summary: principles, allocation, and rate design

The argument of this document moves through three distinct layers that are often conflated in rate design proceedings but should be kept separate:

1. **Allocation principles** — the normative and instrumental reasoning that determines _who should pay_
2. **Cost allocation** — the methodology that computes _how much each customer owes_
3. **Rate design** — the tariff structure that _collects revenue_ from customers

These are different questions. Who should pay is a question of efficiency (instrumental) and justice (normative)—viewed as fairness or as equity. How much each customer owes is a computational question given the principle. How we collect is a tariff design question that must also satisfy regulatory, administrative, and political constraints. Bill alignment — the central concept in the BAT framework — measures the gap between layers 2 and 3: between what a customer _should_ pay (per the allocation) and what they _do_ pay (per the tariff).

The following table maps these three layers across each cost type:

```
                  │  Energy             │  New capacity          │  Sunk capacity
                  │  (SRMC)             │  (LRMC / FLIC)         │  (residual)
──────────────────┼─────────────────────┼────────────────────────┼────────────────────────
                  │                     │                        │
 ALLOCATION       │  Fairness:          │  Fairness:             │  Fairness:
 PRINCIPLE        │  beneficiary-pays   │  beneficiary-pays      │  beneficiary-pays
                  │  = cost-causation   │  = cost-causation      │  ≠ cost-causation
                  │  (cause = benefit   │  (cause = benefit      │  = benefit-
                  │   in same hour)     │   in year 1)           │    proportionality
                  │                     │                        │  (cause ≠ benefit
                  │                     │                        │   after year 1)
                  │                     │                        │  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
                  │                     │                        │  and/or Equity:
                  │                     │                        │  ability-to-pay
                  │                     │                        │
──────────────────┼─────────────────────┼────────────────────────┼────────────────────────
                  │                     │                        │
 COST             │  SRMC allocation:   │  LRMC allocation:      │  Residual allocation:
 ALLOCATION       │  customer load ×    │  customer peak         │  • uniform: R/N
                  │  MC_h in each hour  │  contribution ×        │  • proxy-based
                  │                     │  MC_h in peak hours    │    (historical kWh,
                  │  Each customer's    │                        │    contracted kW,
                  │  energy economic    │  Each customer's       │    property value)
                  │  burden             │  capacity economic     │  • benefit-proportional
                  │                     │  burden                │    (§§9–11; intractable)
                  │                     │                        │  • income-based
                  │                     │                        │
──────────────────┼─────────────────────┼────────────────────────┼────────────────────────
                  │                     │                        │
 RATE             │  Cost-reflective    │  Cost-reflective       │  Fixed charge:
 DESIGN           │  volumetric charge  │  capacity charge       │  • uniform $/month
                  │  (TOU $/kWh at     │  ($/kW in peak hours,  │  • tiered by
                  │   marginal cost     │   or TOU $/kWh with    │    connection kW
                  │   level)            │   peak-period          │  • income-tiered
                  │                     │   premium, at MC       │  • demand-based
                  │                     │   level)               │    $/month
                  │                     │                        │
──────────────────┼─────────────────────┼────────────────────────┼────────────────────────
                  │                     │                        │
 BAT              │     ← Economic burden →                     │  Residual share
 DECOMPOSITION    │     ∑_h L_{i,h} × MC_h                     │  f(i, R)
                  │                     │                        │
```

### The path from principle to practice

1. **Start with fairness (beneficiary-pays).** Both normative justice and instrumental efficiency support it.
2. **For energy costs:** beneficiary-pays = cost-causation. _Allocate_ by SRMC × hourly load. _Charge_ via a cost-reflective volumetric rate (TOU $/kWh at marginal cost level). Done — the easy case.
3. **For new capacity costs:** beneficiary-pays = cost-causation. _Allocate_ by LRMC × peak contribution. _Charge_ via a cost-reflective capacity charge ($/kW in peak hours at FLIC level). Done — cause and benefit are aligned.
4. **For sunk capacity costs:** beneficiary-pays ≠ cost-causation. The original causers may be gone; the current beneficiaries are everyone using the asset. Cost-causation fails on measurement (§7a), efficiency (§7b), and fairness (§7c) grounds.
5. **The principled fairness answer is benefit-proportional fixed charges** — individuated by location, load profile, and network topology. This is theoretically possible (§§9–11) but computationally intractable (§12).
6. **In practice, approximate with feasible mechanisms.** _Allocate_ via uniform $R/N$, or via crude proxies for benefit-proportionality (historical consumption, contracted capacity, measured peak demand). _Charge_ via a fixed $/month that does not distort consumption decisions.
7. **Optionally, apply equity for the residual.** _Allocate_ by ability-to-pay (income-differentiated). _Charge_ via an income-tiered fixed charge. This addresses distributional concerns that fairness-as-beneficiary-pays cannot resolve — the regressive impact of uniform fixed charges on low-income households.

The BAT framework (Simeone et al. 2023) measures how far current tariffs depart from this ideal. By computing each customer's economic burden (the cost allocation from steps 2–3) and a residual share (the cost allocation from steps 6–7), it produces a total allocated cost per customer. Bill alignment compares this allocation to the customer's actual tariff charges — making visible the cross-subsidies that arise when the three layers (principle, allocation, rate design) are conflated, as they are whenever sunk costs are bundled into volumetric rates via EPMC or its functional equivalents.

---

## References

- Abdelmotteleb, I., Pérez-Arriaga, I. J., & Gomez, T. (2018). Design of efficient distribution network charges in the context of active demand. _Applied Energy_, 210, 815–826.
- Batlle, C., Mastropietro, P., & Rodilla, P. (2020). Redesigning residual cost allocation in electricity tariffs: A proposal to balance efficiency and equity. _Renewable Energy_, 155, 257–266.
- Borenstein, S. (2016). The economics of fixed cost recovery by utilities. _The Electricity Journal_, 29(7), 5–12.
- Borenstein, S., Fowlie, M., & Sallee, J. (2024). Designing electricity rates for an equitable energy transition. Working paper.
- Burger, S. P., Knittel, C. R., Perez-Arriaga, I. J., Schneider, I., & vom Scheidt, F. (2020). The efficiency and distributional effects of alternative residential electricity rate designs. _The Energy Journal_, 41(1).
- Pérez-Arriaga, I. J., Jenkins, J. D., & Batlle, C. (2017). A regulatory framework for an evolving electricity sector: Highlights of the MIT Utility of the Future study. _IEEE Power and Energy Magazine_, 15(3), 21–33.
- Schittekatte, T., & Meeus, L. (2020). Least-cost distribution network tariff design in theory and practice. _The Energy Journal_, 41(5).
- Simeone, C., et al. (2023). The bill alignment test: A measure of utility tariff performance. _Utilities Policy_, 85, 101676.

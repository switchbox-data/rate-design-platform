# BAT reasoning: reconstruction and stress-test

A formal reconstruction of the Bill Alignment Test reasoning as it applies to heat pump rate design, followed by an econ-seminar-style critique that probes for logical holes.

## The BAT framework

The BAT answers "is this rate structure fair?" in two parts:

### Part A: Marginal costs

Each customer's bill should be aligned with their marginal cost allocation: their hourly load profile × hourly marginal prices (energy, generation capacity, transmission, distribution). This is justified if we accept future-looking cost causation as the basis for rate design — the principle that customers who drive the need for new investment should pay for it.

In CAIRO, marginal costs come from Cambium (energy, generation capacity, bulk transmission) and utility-specific data (sub-transmission and distribution). The BAT compares what each customer pays under the tariff to what they would pay if charged at their true marginal cost allocation.

### Part B: Residual costs

Marginal costs don't recover the full revenue requirement. The gap — "residual costs" — is (revenue requirement) − (sum of all customers' marginal cost allocations). These residual costs represent the embedded infrastructure: assets already built, costs already incurred. They must be paid for, but there is no cost-causation basis for allocating them, because the costs are sunk.

This creates an inherently normative choice. CAIRO offers three residual allocation methods:

**Peak allocation.** Each customer's residual share is proportional to their contribution to system peak. This is a backward-looking cost-causation view: the customers whose load shaped the peaks that justified past investment should bear the residual cost of that investment. Economists don't usually apply cost-causation reasoning to sunk costs (the efficient thing is to treat them as sunk), but regulators and rate designers have a long tradition of embedded cost allocation based on peak responsibility, and it does have an internal logic — even if the costs are sunk, the allocation reflects who caused them.

**Per-customer (flat).** Each customer pays an equal share of residual costs, regardless of their load. This is the efficiency-oriented view: residual costs are sunk, and recovering them through any consumption-based mechanism distorts the retail price of electricity above marginal cost, leading to inefficient consumption decisions. A fixed residual charge avoids distortion — it's a lump-sum transfer that doesn't affect the marginal price signal. This is the approach most microeconomists would favor (Ramsey pricing aside).

**Volumetric allocation.** Each customer's residual share is proportional to their total kWh consumption. This is what utilities effectively do today with flat volumetric rates: high-kWh customers pay a larger share of all costs, including embedded/residual ones. The problem is that this doesn't correspond to any principled view of why someone ought to pay for historical costs:

- It's not backward-looking cost causation (that's peak allocation).
- It's not the sunk-cost/efficiency view (that's per-customer).
- It's not "amplify the marginal cost signal" (that would mean layering additional price components onto the marginal-cost-reflective part of the tariff).
- At best it's a lazy proxy: "if you consume more now, you probably caused more costs in the past." But that's a weak correlation, especially for HP customers who have high kWh but whose load pattern (winter-heavy) may have had nothing to do with the summer-peak-driven investments that dominate the embedded cost base.

The BAT evaluates cross-subsidization under each allocator. The cross-subsidy identified depends on which allocator you choose — and the choice itself is normative.

## Stress-test

### Is the marginal cost logic sound?

**Yes, with a nuance.** The case for aligning bills with marginal costs rests on standard welfare economics: if retail prices reflect marginal costs, customers face the true cost of their consumption decisions, leading to efficient outcomes (appropriate conservation, demand flexibility, investment in DERs, etc.). The Simenone et al. paper formalizes this.

The nuance is temporal: Cambium marginal costs are forward-looking (the cost of the next unit of generation capacity, transmission, distribution), but the revenue requirement is backward-looking (the cost of assets already in service). CAIRO handles this by separating the two — marginal costs determine the "efficient" allocation, and residual costs are the gap. This separation is standard in the literature and not logically problematic, but it means the BAT is a hybrid: it uses forward-looking costs to evaluate a backward-looking revenue target. The BAT doesn't claim to find the globally optimal tariff; it identifies cross-subsidies relative to a chosen allocator.

### Are the three allocators exhaustive?

**No.** There are other principled bases for residual allocation:

- **Income-based**: ability-to-pay, as in progressive taxation. Regulators increasingly care about affordability and equity.
- **Ramsey pricing**: inverse elasticity — charge more to customers who are less price-sensitive, maximizing social welfare subject to a revenue constraint. Theoretically efficient but requires demand elasticity estimates and raises equity concerns.
- **Property-value or dwelling-size**: a proxy for historical infrastructure cost (larger homes on bigger lots may have required more distribution investment).

CAIRO's three allocators don't cover these. But the three it offers — peak, flat, volumetric — span the most common regulatory and economic perspectives on embedded cost recovery. The BAT results under each give a useful "envelope" of cross-subsidy estimates.

### Is the critique of volumetric allocation fair?

**Mostly, yes.** Volumetric allocation has no strong theoretical justification for residual costs. It conflates the marginal price signal (which should reflect forward-looking costs) with residual recovery (which shouldn't distort consumption). When applied to a flat rate, it creates cross-subsidies between customers with different load shapes — high-kWh winter-heating customers overpay relative to their marginal cost allocation, while low-kWh customers underpay.

The one defense of volumetric allocation: it's simple, it's the status quo, and changing it redistributes costs — so there's a transition/fairness argument for keeping it (even if it's not efficient in steady state). The BAT quantifies what the cross-subsidy IS under volumetric allocation, which is useful precisely because it's the status quo benchmark.

### Cross-subsidy definitions: BAT vs. strict economics

The BAT defines "cross-subsidy" as: a customer pays more (or less) than their allocated cost of service, where the allocation is determined by the chosen residual allocator. This is a normative, regulator-specified definition — the cross-subsidy depends on which allocator you pick.

Strict microeconomics defines cross-subsidy differently:

- A customer group is **cross-subsidized** if they pay less than their **incremental cost** (the additional cost of serving them, given everyone else is already being served).
- A customer group **cross-subsidizes others** if they pay more than their **standalone cost** (the total cost to serve them if they were the only customers).
- Prices between incremental cost and standalone cost are **subsidy-free**.

For a regulated utility with a fixed network, standalone cost is much higher than marginal cost — you'd need to rebuild the entire distribution network to serve just one customer class. This means the subsidy-free range is wide, and many allocations that look like "cross-subsidies" under BAT are actually subsidy-free under the strict definition.

**Standalone cost ≠ marginal cost.** Standalone cost includes the full infrastructure cost allocated to that customer group if they were isolated. Incremental cost is closer to (but not identical to) marginal cost — it's the system cost increase from adding that group to an already-built system. In a network industry with large fixed costs, standalone >> incremental ≈ marginal.

**Practical implication.** The strict economic definition is permissive — almost any reasonable allocation is subsidy-free. The BAT definition is more demanding and more useful for rate design: it takes a normative stance (via the allocator choice) and measures deviation from that stance. For heat pump rate design, where we want to evaluate whether the current tariff structure over- or under-charges HP customers relative to their cost of service, the BAT definition is the right tool.

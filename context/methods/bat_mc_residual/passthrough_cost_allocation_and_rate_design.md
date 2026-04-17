# Passthrough is not a cost allocation — it's a rate design

This document works through a puzzle that arises when comparing passthrough to the other residual allocation methods (volumetric, EPMC, per-customer). The punchline: passthrough isn't a cost allocation method at all. It's the output of a rate design, and the underlying cost allocation that produced it is unknown.

## The puzzle

We have four "allocation methods" for splitting the revenue requirement between HP and non-HP:

- **Per-customer**: HP_EB + (N_HP / N_total) × Residual
- **EPMC**: HP_EB + (HP_EB / total_EB) × Residual
- **Volumetric**: HP_EB + (HP_kWh / total_kWh) × Residual
- **Passthrough**: HP's actual bills under the default tariff

The first three follow the same pattern: allocate EB by cost causality (actual MCs), allocate residual by some share metric. The fourth is different — it's just "whatever HP pays today."

A natural expectation: volumetric should sit between passthrough and per-customer, because it allocates EB by MC (correcting the cross-subsidy on the MC portion) but leaves residual allocated by kWh (same as a flat rate). Since passthrough doesn't correct the MC cross-subsidy at all, and per-customer corrects both MC and residual, volumetric should be in the middle.

For RIE delivery, the actual numbers are:

| Method       | HP subclass RR |
| ------------ | -------------- |
| Per-customer | $11.5M         |
| EPMC         | $14.5M         |
| Passthrough  | $23.8M         |
| Volumetric   | $24.2M         |

Volumetric is _above_ passthrough, not below. What's going on?

## A numerical example

**Setup:** 10 customers, 1 is HP, 9 are non-HP.

|                | HP (1 customer) | Non-HP (9 customers) | Total  |
| -------------- | --------------- | -------------------- | ------ |
| Customers      | 1               | 9                    | 10     |
| kWh/year       | 2,000           | 1,000 each = 9,000   | 11,000 |
| kWh share      | 18.2%           | 81.8%                | 100%   |
| Customer share | 10%             | 90%                  | 100%   |

The default tariff has:

- Fixed charge: $100/year per customer
- Flat volumetric rate: $0.818/kWh (calibrated so total revenue = RR)

This gives:

- Total delivery RR: $10,000
- Fixed charge revenue: $1,000 (10 customers × $100)
- Volumetric revenue: $9,000 (11,000 kWh × $0.818)

CAIRO's BAT also tells us:

- Total EB (from MCs): $2,000
- HP EB: $300 (HP avg MC/kWh = $0.15; system avg = $0.182 — HP is below average)
- Residual: $8,000

### Passthrough HP

HP's actual bills: $100 (fixed) + 2,000 × $0.818 (vol) = $100 + $1,636 = **$1,736**

### Volumetric HP

HP_EB + (HP_kWh / total_kWh) × Residual = $300 + 18.2% × $8,000 = $300 + $1,455 = **$1,755**

### Why is volumetric higher?

Volumetric ($1,755) > passthrough ($1,736) by $19, even though HP's MC per kWh is below average. Two effects are at work:

**Effect 1 — MC correction (helps HP, −$64).** Under passthrough, the flat volumetric rate charges every kWh the same, so HP's share of volumetric revenue equals their kWh share (18.2%). Under volumetric allocation, the EB portion uses HP's actual MCs. HP's actual EB ($300) is less than their kWh-proportional share of total EB (18.2% × $2,000 = $364), a saving of $64. This is because HP's load is winter-heavy and delivery MCs peak in summer.

**Effect 2 — Fixed charge reallocation (hurts HP, +$82).** Under passthrough, HP pays $100 in fixed charges (1 customer × $100/year). Under volumetric allocation, the fixed charge revenue ($1,000) is embedded in the residual — because the residual is RR minus EB, and EB is purely from MCs, not from tariff structure. That $1,000 of fixed charge revenue sits inside the $8,000 residual and gets split by kWh. HP's kWh share of the fixed charge portion: 18.2% × $1,000 = $182. That's $82 more than the $100 they actually pay. HP is 18.2% of kWh but only 10% of customers, so splitting fixed charges by kWh assigns them more.

**Net:** −$64 + $82 = **+$18** (≈ $19 with exact fractions). The fixed charge effect outweighs the MC correction.

If the tariff had no fixed charges (pure volumetric), the MC correction would be the only effect, and volumetric would be $64 lower than passthrough. Fixed charges are what flip the result.

### The supply side is different

On the supply side, there are no fixed charges (verified across all 8 utilities — every supply charge is $/kWh). So for supply, passthrough is purely kWh-proportional, and the only difference between volumetric and passthrough is the MC correction. Whether volumetric is higher or lower than passthrough on supply depends entirely on whether HP's supply MC per kWh is above or below the system average.

## Passthrough is a rate design, not a cost allocation

The three BAT-based methods (per-customer, EPMC, volumetric) follow a two-step process:

1. **Cost allocation**: split total EB and total residual between subclasses using some rule
2. **Rate design**: given the subclass RR from step 1, calibrate a tariff (fixed charge + vol rate) to recover it

Passthrough skips step 1. It goes straight to the observed tariff and says: HP's subclass RR = their actual bills. This is the output of the utility's rate design, not of any explicit cost allocation.

### Can we reverse-engineer the cost allocation?

Given the observed tariff, we can uniquely determine HP's subclass RR ($1,736 in the example). But we cannot determine _how_ the utility arrived at that number. Any pair (α, β) satisfying:

α × $2,000 (EB) + β × $8,000 (Residual) = $1,736

would produce the same RR. That's one equation with two unknowns — infinitely many solutions. For example:

- α = β = 17.36% (uniform share of everything)
- α = 15% (MC-proportional), β = 17.95%
- α = 20%, β = 16.7%

All produce the same $1,736 RR, the same fixed charges, and the same volumetric rate. From the observed tariff, these are indistinguishable.

### Is it "kWh-proportional for both EB and residual"?

No. If we allocated both EB and residual by kWh share, we'd get:

18.2% × $2,000 + 18.2% × $8,000 = 18.2% × $10,000 = **$1,818**

That's $82 higher than the actual $1,736. The difference is exactly the fixed charge effect: the tariff's fixed charges allocate $1,000 of the RR by customer count (HP gets 10%), while pure kWh-proportional would allocate it by kWh (HP gets 18.2%). The fixed charge gives HP a $82 benefit.

### What the tariff implicitly does

The default tariff is a hybrid:

- Fixed charges: allocated by **customer count** (each customer pays the same $/month)
- Volumetric revenue: allocated by **kWh** (each kWh pays the same rate)
- MC and residual are lumped together — the tariff doesn't distinguish them

This hybrid allocation doesn't correspond to any of our clean theoretical methods. It's an artifact of the tariff having both fixed and volumetric components.

## Implications

1. **Passthrough and volumetric are not directly comparable in the way the other methods are.** Per-customer, EPMC, and volumetric all use the same framework (separate MC and residual, allocate each by some rule). Passthrough uses a fundamentally different framework (actual bills under an observed tariff). Comparing them requires accounting for the fixed charge effect.

2. **On delivery (where fixed charges exist), volumetric can be above or below passthrough.** It depends on whether the MC correction (which can go either way) or the fixed charge reallocation (which always hurts HP when HP has more kWh/customer than average) dominates.

3. **On supply (where there are no fixed charges), the comparison is clean.** Volumetric vs passthrough is purely the MC correction. If HP's supply MC/kWh is above average, volumetric > passthrough; if below, volumetric < passthrough.

4. **The "cross-subsidy" that passthrough preserves is not well-defined.** We can say passthrough preserves the status quo bills. But we can't say it preserves a specific MC cross-subsidy plus a specific residual cross-subsidy, because the decomposition into MC and residual is not observable from the tariff alone.

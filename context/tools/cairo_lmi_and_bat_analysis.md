# How CAIRO's LMI Features Work and What They Mean for the Bill Alignment Test

## Questions This Document Answers

CAIRO exposes a handful of LMI-related parameters (`low_income_bill_assistance_program`, `low_income_participation_target`, `low_income_strategy`) that you can pass into a simulation run. But what do they actually do? Specifically:

1. **What discount mechanisms does CAIRO support natively?** Does it apply a percentage off the total bill, a fixed monthly credit, or something else? Can it represent tiered programs (multiple discount levels by income band)?

2. **How does the LMI discount flow into the Bill Alignment Test?** The BAT compares a customer's actual bill to their cost-of-service allocation (marginal cost burden + residual share). If LMI discounts change the bill but not the cost allocation, does that distort the BAT? Does it break the revenue-balance check?

3. **Can CAIRO's native LMI features represent real-world programs like RI's LIDR+ (tiered percentage discounts by FPL) or NY's EAP (fixed credits varying by utility, rate code, and tier)?**

4. **If I'm using the BAT to measure cross-subsidization between heat pump and non-heat pump customers, how should I handle LMI?** LMI discounts are intentional income-based cross-subsidies — the whole point. But I don't want them to contaminate the rate-design cross-subsidization signal. What's the right analytical setup?

---

## 1. CAIRO's Native LMI Mechanisms

### The three parameters

CAIRO's `MeetRevenueSufficiencySystemWide.simulate()` accepts three LMI-related arguments:

| Parameter                            | Values                                   | Purpose                                                                |
| ------------------------------------ | ---------------------------------------- | ---------------------------------------------------------------------- |
| `low_income_bill_assistance_program` | `"generic"` or `None`                    | Main switch: enables bill discounts + cost recovery rider              |
| `low_income_participation_target`    | `{"generic": <float>}` or `None`         | Fraction of eligible customers who actually participate                |
| `low_income_strategy`                | `"income_based_fixed_charges"` or `None` | Separate mechanism — **dead code** (`assert False, "not set up yet!"`) |

Only the first two do anything. `low_income_strategy="income_based_fixed_charges"` hits an unimplemented assertion. Ignore it.

### What `"generic"` bill assistance does

When you set `low_income_bill_assistance_program="generic"`, CAIRO executes three steps:

**Step A — Identify participants.** The `LowIncomeDiscountApplicator` class (in `low_income_assistance.py`) uses ResStock building metadata — `federal_poverty_level`, `income`, `occupants`, `tenure` — to flag eligible customers against hardcoded income limits, then samples down to the participation target. It also separately identifies a "renters" sub-program.

**Step B — Apply discounts.** Two discount types are supported, but each applies a single, flat amount to all participants — no tiers:

- **Generic (percentage discount):** Multiplies all monthly bills by `(1 - discount_generic)`:

  ```python
  # low_income_assistance.py, line ~700
  bills_df.loc[bills_df["generic"], lookups.months] = bills_df.loc[
      bills_df["generic"], lookups.months
  ] * (1 - discount_generic)
  ```

- **Renter (fixed monthly credit):** Subtracts a flat $/month from each month:

  ```python
  # low_income_assistance.py, line ~739
  bills_df.loc[bills_df["renter"], lookups.months] = (
      bills_df.loc[bills_df["renter"], lookups.months] - discount_renter
  )
  ```

**Step C — Cost recovery rider.** Total program cost (sum of all discounts given) is divided by total non-participant kWh consumption to produce a volumetric rider ($/kWh). This rider is added to every non-participating customer's monthly bill, ensuring the utility still collects the same total revenue:

```python
# low_income_assistance.py, line ~585
low_income_rider = program_cost.sum() / total_non_participant_load
# applied to non-participants:
bill_assistance_costs = grid_cons * low_income_rider
```

### Pipeline sequencing

The ordering in `simulate()` matters:

1. Calculate base bills from tariff structures
2. Adjust energy charges to meet revenue requirement (`precalc` mode)
3. Apply `low_income_strategy` (dead code, no-op)
4. Apply utility taxes/surcharges
5. **Apply LMI bill assistance** — discount participants, add rider to non-participants
6. Calculate gas bills
7. **Run BAT** using the now-LMI-adjusted `self.customer_bills_monthly`

---

## 2. How LMI Affects the BAT

### The BAT formula

The Bill Alignment Test (per [Simeone et al., 2023](https://doi.org/10.1016/j.jup.2023.101539)) is:

```
BAT = Annual Bill − (Economic Burden + Residual Share)
```

where:

- **Economic Burden** = Σ(hourly_load × hourly_marginal_price) — the customer's marginal cost allocation. Purely load-driven.
- **Residual Share** = customer's share of (Revenue Requirement − Total Marginal Costs), allocated volumetrically, by peak contribution, or per-customer. Also purely load-driven.
- **Annual Bill** = what the customer actually pays under the tariff, including any LMI discount or rider surcharge.

### The key insight

**Economic Burden and Residual Share are computed entirely from load profiles and marginal prices. They do not change when LMI is applied.** The same customer has the same marginal cost allocation and the same residual share regardless of whether they receive an LMI discount.

Only the Annual Bill term changes:

| Customer type   | Bill change with LMI          | BAT change                                                          |
| --------------- | ----------------------------- | ------------------------------------------------------------------- |
| LMI participant | Bill goes **down** (discount) | BAT goes **more negative** — appears more cross-subsidized          |
| Non-participant | Bill goes **up** (rider)      | BAT goes **more positive** — appears to cross-subsidize others more |

### Revenue balance is preserved

The rider is designed to recover exactly the total discount amount from non-participants. So `Σ(bill_i × weight_i)` still equals the revenue requirement, `Σ((EB_i + RS_i) × weight_i)` still equals the revenue requirement, and the weighted BAT sum still equals zero. The aggregate check on CAIRO's line 708 still passes.

### What this means for HP vs. non-HP analysis

If you include LMI in the BAT, you conflate two distinct cross-subsidization phenomena:

1. **Rate-design cross-subsidization** — flat volumetric rates over/under-charging HP customers relative to their load shape and cost-causation.
2. **Income-based cross-subsidization** — LMI customers paying less by policy design, recovered via rider from everyone else.

If HP adoption correlates with income (e.g., higher-income households are more likely to have heat pumps), then LMI discounts will systematically shift BAT in a way that looks like "non-HP customers are being subsidized more" — but that's the income transfer, not the rate design.

---

## 3. Can CAIRO Natively Represent RI and NY Programs?

**No.** The gap is significant:

| Feature needed                    | RI LIDR+                              | NY EAP                                                               | CAIRO native                                             |
| --------------------------------- | ------------------------------------- | -------------------------------------------------------------------- | -------------------------------------------------------- |
| Multiple discount tiers           | 3 tiers (60%, 30%, 10%) by FPL        | 4–7 tiers by SMI/HEAP                                                | **1 tier only**                                          |
| Discount mechanism                | % of total bill                       | Fixed monthly credit varying by utility × rate code × tier           | % discount OR fixed credit, not both; one amount for all |
| Tier assignment basis             | % of FPL (3 bands)                    | HEAP grant amount / SMI / vulnerability flags                        | Income + occupants (generic eligibility check)           |
| Per-utility credit amounts        | N/A (single utility)                  | ~20 utility × service type combinations with distinct credit amounts | Single amount for all participants                       |
| Heating / non-heating distinction | Different base rates, same discount % | Different credit amounts for heating vs. non-heating                 | Not modeled in LMI logic                                 |

CAIRO's native LMI was built around a simpler California-style bill assistance model (single percentage discount + renter credit). It cannot express RI's tiered percentage structure or NY's utility-specific fixed credit tables without modification.

---

## 4. Recommended Analytical Approach

### For the core BAT (HP vs. non-HP cross-subsidization)

**Run CAIRO with all LMI parameters set to `None`** — exactly as in the current `run_scenario.py`. This gives you the cleanest BAT that isolates rate-design-driven cross-subsidization. The bills reflect what customers would pay under the tariff with no LMI overlay, and the BAT tells you purely how the rate structure allocates costs relative to cost-causation.

### For reporting realistic bills

**Apply LMI discounts in postprocessing**, after CAIRO finishes. This is the right approach for both RI and NY because:

- You need multi-tier logic that CAIRO doesn't support natively.
- You need utility-specific credit amounts (NY).
- You want to keep the BAT clean while still reporting what customers actually pay.
- You're already planning per-utility scenario runs.

The postprocessing module would:

1. Read CAIRO's per-customer monthly bill output.
2. Flag each customer's LMI tier using ResStock metadata (`federal_poverty_level` for RI; income-based SMI mapping for NY).
3. Apply the appropriate discount — percentage for RI, fixed credit for NY — to each flagged customer's bills.
4. Optionally compute a cost-recovery rider and add it to non-participant bills.

### For an LMI-inclusive BAT comparison (optional but valuable)

You likely want both a "without LMI" and "with LMI" BAT, because they answer different questions:

- **Without-LMI BAT:** _"Does the rate structure itself create cross-subsidies between HP and non-HP customers?"_ This is the pure rate design question — if you're optimizing tariff structures, this is what you evaluate against.

- **With-LMI BAT:** _"What is the total cross-subsidization a real customer experiences?"_ This matters for policy analysis — e.g., "a low-income non-HP customer gets a 60% LIDR+ discount that more than offsets any rate-design cross-subsidy. But a moderate-income HP customer who doesn't qualify for LMI pays the rider _on top of_ any rate-design over-charge."

To produce the with-LMI BAT, you don't need to re-run CAIRO. After applying LMI discounts in postprocessing, simply recompute:

```
BAT_with_lmi = adjusted_annual_bill − (economic_burden + residual_share)
```

using the same economic burden and residual share that CAIRO already calculated and saved. Only the bill term changes.

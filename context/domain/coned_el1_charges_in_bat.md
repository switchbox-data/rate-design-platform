# ConEd EL1: charge-by-charge cross-subsidy analysis

Every charge on a ConEd residential (SC1/EL1) bill, evaluated for whether it belongs in CAIRO's BAT and bill calculations, and whether it creates a cross-subsidy between heat pump and non-HP customers.

For the BAT framework itself — marginal costs, residual allocation, and the reasoning behind the three allocators — see `bat_reasoning_stress_test.md`.

## Summary table

The MSC (Market Supply Charge) bundles several NYISO cost components into one $/kWh line item. We decompose it here because the sub-components have different cross-subsidy properties.

| Charge                           | $/kWh?          | In rev req?           | Fixed budget?        | HP cross-subsidy?   | Why                                                      | Notes                                      |
| -------------------------------- | --------------- | --------------------- | -------------------- | ------------------- | -------------------------------------------------------- | ------------------------------------------ |
| **Customer Charge**              | No ($/mo)       | Yes                   | —                    | Analyzed in BAT     | Base rate; part of tariff structure                      | $20/mo                                     |
| **Billing & Payment Processing** | No ($/mo)       | Yes                   | —                    | Analyzed in BAT     | Base rate; affects fixed/vol split                       | $1.28/mo                                   |
| **Core Delivery Rate**           | Yes             | Yes                   | —                    | Analyzed in BAT     | The rate CAIRO calibrates in precalc                     | Seasonal, tiered                           |
| **MAC**                          | Yes             | Yes (forecast)        | —                    | Already captured    | True-up noise; costs in base rate                        | Exclude from tariff                        |
| **Uncollectible Bill Expense**   | Yes             | Yes (forecast)        | —                    | Already captured    | MAC subcomponent; same logic                             | Exclude from tariff                        |
| **Transition Adjustment**        | Yes             | Yes (forecast)        | —                    | Already captured    | MAC subcomponent; same logic                             | Exclude from tariff                        |
| **Reconciliation Rate**          | Yes             | Yes (forecast)        | —                    | Already captured    | MAC subcomponent; same logic                             | Exclude from tariff                        |
| **RDM**                          | Yes             | N/A (revenue true-up) | —                    | Already captured    | Load-forecast noise; CAIRO precalc handles               | Exclude from tariff                        |
| **DRS**                          | Yes             | N/A (temporary)       | —                    | Already captured    | Rate-delay artifact; temporary                           | Exclude from tariff                        |
| **CES Delivery**                 | Yes             | **No**                | Yes                  | **Yes**             | Fixed program budget ÷ kWh                               | Separate PSC order                         |
| **System Benefits Charge**       | Yes             | **No**                | Yes                  | **Yes**             | Fixed NYSERDA budget ÷ kWh                               | Separate PSC order                         |
| **NY State Surcharge**           | Yes             | **No**                | Yes                  | **Yes**             | Fixed PSL §18-a assessment ÷ kWh                         | Statutory pass-through                     |
| **DLM Surcharge**                | Yes             | **No**                | Yes                  | **Yes**             | Fixed DR program costs ÷ kWh                             | Case 14-E-0423                             |
| **EV Make Ready**                | Yes             | **No**                | Yes                  | **Yes**             | Fixed infra program costs ÷ kWh                          | Case 18-E-0138; $0.0008/kWh                |
| **Arrears Management**           | Yes             | **No**                | Yes (finite)         | **Yes** (temporary) | COVID debt forgiveness ÷ kWh                             | Case 14-M-0565; $0.0012/kWh                |
| **VDER Cost Recovery**           | Yes             | **No**                | Yes                  | **Yes**             | Fixed solar credit payments ÷ kWh                        | REV proceedings; $0.0011/kWh               |
| **MSC: Energy (LBMP)**           | Yes             | **No**                | **No**               | **No**              | True marginal cost; scales 1:1 with kWh                  | Bulk of MSC (~$35-45/MWh)                  |
| **MSC: Capacity (ICAP/UCAP)**    | Yes (embedded)  | **No**                | Yes (current period) | **Yes**             | Peak-determined cost, recovered volumetrically           | ~$3-6/MWh equivalent                       |
| **MSC: Ancillary Services**      | Yes (embedded)  | **No**                | Mostly               | **Yes** (small)     | Hourly pool ÷ hourly load                                | ~$1-2/MWh                                  |
| **MSC: Uplift (BPCG)**           | Yes (embedded)  | **No**                | Yes                  | **Yes** (tiny)      | Reliability dispatch costs ÷ load                        | ~$0.5-1/MWh                                |
| **MSC: NYISO Schedule 1**        | Yes (embedded)  | **No**                | Yes                  | **Yes** (tiny)      | Fixed NYISO admin budget ÷ MWh                           | ~$0.9/MWh                                  |
| **MSC: Working capital**         | Yes (embedded)  | **No**                | **No**               | **No**              | Scales with procurement volume                           | Small                                      |
| **MSC I Adjustment**             | Yes             | **No**                | —                    | Already captured    | Energy true-up; same logic as MAC                        | Exclude                                    |
| **MSC II Adjustment**            | Yes             | **No**                | —                    | Already captured    | Capacity/ancillary true-up; same logic                   | Exclude                                    |
| **Merchant Function Charge**     | Yes             | **No**                | Mixed                | **Weak**            | Fixed admin + load-proportional working capital/bad debt | ~0.15-0.30¢/kWh                            |
| **CES Supply Surcharge**         | Yes             | **No**                | **No**               | **No**              | Per-MWh LSE obligation to NYSERDA; cost scales with load | Separate CES order                         |
| **GRT (Distribution)**           | % of charges    | **No**                | **No**               | **No**              | Percentage of own bill; no fixed pool                    | Varies by zone                             |
| **GRT (Supply)**                 | % of charges    | **No**                | **No**               | **No**              | Percentage of own bill; no fixed pool                    | Varies by zone                             |
| **Tax Sur-Credit**               | Yes             | N/A                   | N/A                  | N/A                 | **Expired**                                              | Joint proposal removes obsolete references |
| **CBC**                          | $/kW of solar   | **No**                | N/A                  | N/A                 | Solar-only; not applicable to HP analysis                | $1.84/kW system size                       |
| **Minimum Charge**               | No ($/mo floor) | Yes                   | —                    | N/A                 | Bill floor = Customer Charge; rarely binds               | $20/mo                                     |

---

## How this document is organized

**Section 1** covers charges in the delivery revenue requirement — the ones CAIRO should model directly. This includes both the base rates (include in tariff) and the delivery adjustment mechanisms (exclude, because the costs are already in base rates and the adjustments are uniform $/kWh noise).

**Section 2** covers charges outside the revenue requirement that create a cross-subsidy when HP customers electrify. This is the novel extension: the BAT traditionally operates on the revenue requirement, but many charges outside the revenue requirement have the same "fixed budget recovered volumetrically" structure, and HP adoption mechanically lowers the per-kWh rate for everyone else. The section opens with a conceptual introduction using the System Benefits Charge as a worked example.

**Section 3** covers charges with no cross-subsidy — either because they're genuine marginal costs, because they scale with consumption, or because they're self-contained (percentage of own charges).

---

## 1. Charges in the delivery revenue requirement

### Base rates: include in CAIRO tariff

These three charges define the tariff structure that collects the delivery revenue requirement. CAIRO should model all of them.

**Customer Charge ($20.00/month).** Set in the joint proposal (22-E-0064, Section G.3) by service class and rate year. In precalc mode, its revenue is subtracted before deriving the volumetric rate. It affects the BAT because fixed charges are load-shape-insensitive — every customer pays the same amount regardless of when or how much they consume.

**Billing and Payment Processing Charge ($1.28/month).** Also a fixed $/month charge set in the rate case. The joint proposal classifies it as a "competitive service charge" with a reconciliation mechanism for variances (Section D.5), but the charge level is part of base rates and contributes to delivery revenue. At $1.28/month the monetary impact is small relative to the ~$20 customer charge and ~$80+/month volumetric delivery, but including it ensures CAIRO gets the fixed/volumetric split right in precalc.

**Core Delivery Rate ($0.16107–$0.18518/kWh).** The main volumetric delivery charge (seasonal, tiered at 250 kWh). Set in the rate case (joint proposal Section G.2, Appendix 16). CAIRO calibrates this rate in precalc mode — it takes the tariff structure (tiers, seasons) and scales the rate so total bills = revenue requirement. This is the primary charge analyzed in the BAT.

### Delivery adjustment mechanisms: exclude from CAIRO tariff

The delivery bill includes several variable $/kWh adjustments that true up actual costs or revenue against the rate-case forecast. For structural rate design, these should be excluded.

**The core argument.** The cross-subsidy between HP and non-HP customers comes from the shape mismatch between how the tariff collects revenue (e.g., flat volumetric) and how costs are actually caused (e.g., load shape × marginal costs). A uniform $/kWh adder shifts both sides of the BAT equation by the same amount per kWh — the bill alignment ranking between customers does not change. So if an adjustment is (a) uniform $/kWh, and (b) either already embedded in the revenue requirement forecast or averages to ~zero over time, it's noise, not signal. Including it would overfit the BAT to whichever year happened to have a big storm or warm winter.

**MAC (Monthly Adjustment Clause).** True-ups actual delivery costs (property taxes, storm costs, pension, environmental) against what was billed via base rates. Monthly, uniform $/kWh. The base rate already embeds the forecasted level of all MAC-covered costs. MAC is the mechanism that corrects when reality diverges — level/timing noise, not rate-structure signal.

**Uncollectible Bill Expense, Transition Adjustment, Reconciliation Rate.** All three are subcomponents of the MAC statement. Each is a uniform $/kWh true-up for a specific cost basket (bad debt, stranded-cost recovery, reconciliation). Same logic as MAC — costs are forecasted in base rates; adjustments are noise.

**RDM (Revenue Decoupling Mechanism).** True-ups revenue (load forecast error), not costs. Uniform $/kWh. CAIRO's precalc already calibrates rates so total bills = revenue requirement over the ResStock population — that IS the structural question. RDM noise would overfit to a particular year's load deviation.

**Delivery Revenue Surcharge (DRS).** Recovers a revenue shortfall caused by new rates being delayed — the PSC approved higher rates but they didn't take effect for several months. Temporary, uniform $/kWh, and irrelevant to structural rate design. You'd use the approved tariff structure, not old rates plus a DRS kicker.

---

## 2. Charges outside the revenue requirement that create a cross-subsidy

### Conceptual introduction: the System Benefits Charge and the generalized cross-subsidy

Readers familiar with the BAT understand how cross-subsidies arise in the delivery revenue requirement: a fixed cost pool (the revenue requirement) is recovered through a tariff structure that may not match how those costs were caused. The BAT gives us a principled way to measure this — marginal cost allocation + residual allocation under a normative choice of allocator.

But the delivery revenue requirement isn't the only fixed cost pool recovered volumetrically on a ConEd bill. Consider the **System Benefits Charge (SBC)**.

**What the SBC is.** Established by the PSC in 1996, the SBC funds NYSERDA programs: energy efficiency rebates, low-income energy assistance, renewable energy research, the Clean Energy Fund, and related public-benefit initiatives. It is not a utility cost in the traditional sense — it's a state-mandated program funded by ratepayer contributions. The SBC is governed by its own PSC proceedings (Cases 14-M-0094, 18-M-0084), independent of ConEd's rate case. ConEd files a separate "Statement of System Benefits Charge" with its own effective dates. The rate is a uniform $/kWh applied to all customers.

**The question.** Even though SBC is not in the delivery revenue requirement, isn't it structurally similar? The SBC has a fixed budget for each program period (the PSC approves a total funding level for NYSERDA). ConEd collects that budget by charging all customers a flat $/kWh rate, which is set based on a sales forecast. If actual sales exceed the forecast, ConEd over-collects; the surplus is trued up at the next rate reset, lowering the per-kWh SBC rate going forward.

**The HP cross-subsidy mechanic.** Now suppose a wave of residential customers adopt heat pumps. Their annual kWh consumption roughly doubles. All else equal:

1. The SBC budget for the current period is fixed (NYSERDA's approved funding level doesn't change because some customers got heat pumps).
2. Total kWh billed increases (more HP consumption).
3. ConEd collects more SBC revenue than the budget requires (same $/kWh rate × more kWh).
4. At the next true-up, the surplus is credited back — the $/kWh SBC rate drops.
5. Non-HP customers, who didn't change their consumption, now pay less per kWh for SBC.
6. HP customers paid more in total (higher kWh × the original rate), but the budget was fixed, so their extra payments mechanically subsidized non-HP customers' SBC costs.

This is the same structural dynamic as the delivery revenue requirement: a fixed cost pool, recovered volumetrically, where one group's increased consumption mechanically reduces the per-unit cost for everyone else.

**How this differs from the BAT's usual analysis.** With the delivery revenue requirement, the BAT has a cost-of-service basis for evaluating the cross-subsidy. Marginal costs tell us what each customer's load actually costs the system; the residual allocator provides a normative standard for the rest. Together they define what each customer _should_ pay, and the difference between that and what they _do_ pay is the cross-subsidy.

For SBC, there is no analogous cost-of-service basis. NYSERDA's programs don't have customer-level marginal costs — energy efficiency rebates and renewable research don't vary by load shape. So we can't say "HP customer X should pay $Y for SBC based on their cost causation." We can only say: if the SBC budget is fixed and recovered volumetrically, HP adoption mechanically lowers SBC bills for non-HP customers. Whether that constitutes a "cross-subsidy" depends on how you define the term — it's not a deviation from cost-of-service, but it IS a wealth transfer caused by the rate structure.

**Integration with BAT: topping up the revenue requirement.** One practical approach is to add SBC costs to the revenue requirement that CAIRO optimizes against. If we do, the BAT treats SBC like any other fixed cost pool and applies the chosen residual allocator:

- **Peak allocation**: HP customers' residual share depends on their contribution to system peak. If HP load doesn't drive the peak (currently true — NYISO is summer-peaking), HP customers get a small residual share and thus a small SBC allocation. The cross-subsidy from volumetric recovery would show up as HP customers overpaying.
- **Per-customer (flat) allocation**: Each customer gets an equal residual share. SBC costs are spread evenly regardless of kWh. The cross-subsidy from volumetric recovery would again show HP customers overpaying (since flat rates charge high-kWh customers more than their equal share).
- **Volumetric allocation**: Each customer's residual share is proportional to kWh. This replicates what SBC already does (uniform $/kWh), so topping up the revenue requirement with SBC costs and then using volumetric allocation changes nothing — no additional cross-subsidy is identified, because the allocator's normative standard matches the existing recovery mechanism.

The upshot: topping up the revenue requirement is most informative under peak or per-customer residual allocation, where it reveals the cross-subsidy that volumetric recovery of SBC creates. Under volumetric allocation, it's a no-op.

**SBC isn't the only charge like this.** Every charge in this section shares the same structure: a cost pool outside the rate-case revenue requirement, recovered via a uniform $/kWh surcharge, where the budget is fixed (or approximately fixed) for a given period. HP adoption increases total kWh, the budget stays the same, and the per-kWh rate drops — mechanically lowering non-HP customers' costs. The analysis above applies to all of them. What follows is the charge-by-charge detail.

### Delivery-side surcharges

**CES Delivery Surcharge.** Recovers ConEd's costs for grid infrastructure needed to comply with NY's Clean Energy Standard (distribution upgrades, DER integration). Established by Case 15-E-0302, governed by its own PSC order. Not in the rate-case revenue requirement. Uniform $/kWh (~$0.001–0.003/kWh). Fixed program budget recovered volumetrically. Cross-subsidy: **yes**.

**NY State Surcharge.** The PSL §18-a regulatory assessment: a state fee on regulated utilities to fund the Department of Public Service. ConEd collects it per kWh and remits it to the state; variances are reconciled. Despite its name, this is a permanent, ongoing charge. The assessment level is set by statute/PSC order and does not change when individual customers consume more. Fixed budget, recovered volumetrically. Cross-subsidy: **yes**.

**DLM (Dynamic Load Management) Surcharge.** Recovers ConEd's payments to demand response participants (customers and batteries that reduce load during peak periods). Established under Case 14-E-0423 and REV proceedings. The DR program budget is set separately from the rate case. Uniform $/kWh. Fixed program budget recovered volumetrically. Cross-subsidy: **yes**.

**EV Make Ready Surcharge ($0.0008/kWh).** Recovers ConEd's costs for installing EV charging infrastructure (conduit, wiring) under the Make-Ready Program Order (Case 18-E-0138, July 2020). A defined infrastructure investment program with a fixed cost pool. Uniform $/kWh. Cross-subsidy: **yes**.

**Arrears Management Program Recovery Surcharge ($0.0012/kWh).** Recovers costs from ConEd's COVID-era arrears forgiveness program (Case 14-M-0565, Phase 2 approved January 2023). The total forgiven debt is a defined, finite cost pool being amortized over a recovery period. Uniform $/kWh. Cross-subsidy: **yes** — but temporary. Once the forgiven debt is fully recovered, the surcharge goes away.

**VDER Cost Recovery ($0.0011/kWh).** Recovers ConEd's payments to solar generators under NY's VDER/Value Stack tariff (REV/VDER proceedings: Cases 14-M-0224, 15-E-0082, 19-M-0463). When solar systems export power, ConEd compensates them at time-varying rates; this surcharge recovers those costs from all ratepayers. At any given point, the total VDER credits paid out form the cost pool to be recovered. Uniform $/kWh. Cross-subsidy: **yes**. (The VDER budget could grow with solar adoption, but that's a separate causal channel from HP adoption. Ceteris paribus — holding the VDER budget fixed — more HP kWh mechanically lowers the per-kWh surcharge.)

### Supply-side: Market Supply Charge sub-components

The MSC is the main supply charge on a ConEd bill. For SC1 residential customers, it's a single $/kWh line item. But it bundles several distinct NYISO wholesale cost components that have different cross-subsidy properties. Understanding the MSC requires decomposing it.

**How the MSC works.** Each month, ConEd forecasts the total cost of procuring electricity for its default-service customers and converts that into a $/kWh rate by zone (H, I, J). The rate is load-weighted: ConEd takes NYISO day-ahead LBMPs for each hour, weights them by the residential rate class's average hourly load profile, adjusts for losses, and produces a flat $/kWh rate (or TOU-differentiated for optional TOU customers). This rate is set prospectively; actual cost deviations are reconciled through MSC I and MSC II adjustments (discussed in Section 3).

The costs bundled into the MSC:

#### MSC: Capacity (ICAP/UCAP) — cross-subsidy: **yes**

**~$3–6/MWh equivalent for residential (embedded in the flat $/kWh MSC rate).**

This is the supply-side analog of the delivery revenue requirement problem — and the most significant supply-side cross-subsidy.

**What it is.** NYISO operates an Installed Capacity (ICAP) market. Every Load Serving Entity (ConEd, for default service) must procure enough Unforced Capacity (UCAP) to cover its share of the system's resource adequacy requirement. ConEd's share is determined by its **coincident peak load ratio** — its fraction of the NYCA (New York Control Area) peak load forecast. The cost per MW of capacity is set by NYISO's capacity market auction, governed by administratively-determined demand curves. Zone J (NYC) has its own locational capacity requirement with higher clearing prices (~$19.84/kW-month reference point in 2024-2025).

**Why the cost pool is fixed.** For a given capability year, ConEd's capacity obligation (in MW) and the auction clearing prices are locked in. The total capacity cost for the period doesn't change when individual customers consume more or fewer kWh.

**Why it's recovered volumetrically.** SC1 residential customers don't have demand meters. ConEd can't bill them per-kW based on their contribution to peak. Instead, the total residential capacity cost is divided by total residential kWh to produce a flat per-kWh component embedded in the MSC.

**The HP cross-subsidy mechanic.** HP customers consume more kWh (roughly double). ConEd collects more capacity revenue (more kWh × same embedded capacity rate) but its capacity cost for the current period is unchanged. At MSC II reconciliation, the overcollection is credited back, lowering future rates. Non-HP customers pay less.

**The critical nuance: when does HP load affect the capacity obligation itself?** ConEd's capacity obligation depends on its contribution to the NYCA coincident peak. Right now, NYISO is **summer-peaking**. HP load is primarily a **winter** phenomenon. So currently, HP winter heating load does not increase the summer peak, does not increase ConEd's capacity obligation, and the capacity cost pool remains truly fixed relative to HP adoption. This makes the cross-subsidy pure today.

However, NYISO is actively studying the transition to winter peaking as electrification grows (EPRI heat pump assessment for NYISO's Long-Term Forecasting Task Force). At extreme winter design conditions (-3°F in the Hudson Valley), air-source heat pump COP drops to ~1.12 and supplemental resistance heat kicks in, creating very high winter peak demand. If NYISO becomes winter-peaking, HP customers' extreme-cold demand would start driving the coincident peak, increasing ConEd's capacity obligation. The cost pool would grow with HP adoption, and the cross-subsidy would shrink. This is a feature of the capacity market: it sends a price signal when a new load pattern starts driving the peak.

**Bottom line:** For the foreseeable future (summer-peaking system), capacity is a fixed cost pool recovered volumetrically from residential with no demand metering. Clear cross-subsidy. As winter peaks bind, the dynamic partially self-corrects.

#### MSC: Ancillary Services — cross-subsidy: **yes** (small)

**~$1–2/MWh.**

NYISO ancillary services include regulation, operating reserves (spinning + non-spinning), and voltage support. Each is allocated to LSEs based on **hourly load ratio share** — proportional to each LSE's MWh withdrawn in each hour. The total ancillary service cost in each hour is determined by grid conditions and generator bids, not by any individual customer's consumption. If HP customers add load, total state ancillary service costs don't increase meaningfully (the marginal impact of one customer's additional MWh on total regulation/reserve needs is negligible). The per-MWh rate falls as the denominator grows. Cross-subsidy mechanic: yes, but small dollar amounts.

#### MSC: Uplift (BPCG) — cross-subsidy: **yes** (tiny)

**~$0.5–1/MWh.**

When NYISO dispatches generators out of merit order for reliability, generators receive Bid Production Cost Guarantee (BPCG) payments. These are socialized to LSEs by load ratio share. Total uplift is determined by reliability dispatch decisions (transmission constraints, local reliability), not by individual customer consumption. Fixed pool ÷ MWh. Cross-subsidy: yes, de minimis.

#### MSC: NYISO Schedule 1 (admin) — cross-subsidy: **yes** (tiny)

**~$0.9/MWh.**

NYISO's annual operating budget (~$202M in 2025) for market administration, dispatch, and reliability planning. Recovered via a fixed monthly rate split 72/28 between load (withdrawals) and generation (injections). ~$0.92/MWh for load. Classic fixed budget ÷ total MWh. Cross-subsidy: yes, negligible amounts.

### Partial cross-subsidy: Merchant Function Charge

**~0.15–0.30¢/kWh. Cross-subsidy: weak.**

The MFC recovers ConEd's costs to administer the supply function for default-service customers: procurement staff, working capital (the float on money tied up in power purchases and hedges), credit & collection on the commodity portion of bills, and IT/billing systems. Only default-service customers pay it; ESCO customers don't.

The MFC is outside the delivery revenue requirement — it's classified as a "competitive service charge" (joint proposal Section D.5). Rates are set in the rate case and reconciled annually.

**Why it's partial.** Unlike the purely fixed-budget charges above, a significant portion of MFC costs genuinely scale with kWh:

- **Working capital** (~largest component): proportional to the dollar volume of power purchases. More kWh purchased → more dollars tied up → more working capital cost. If HP customers consume more kWh, ConEd's working capital costs increase roughly proportionally.
- **Credit & collection**: proportional to commodity revenue billed. More billed → more potential bad debt. Also scales with load.
- **Fixed admin overhead**: procurement staff salaries, IT systems, billing infrastructure. These don't change when an individual customer consumes more kWh. This portion does create the cross-subsidy dynamic.

The net effect: only the fixed-admin portion of MFC creates a cross-subsidy. The working-capital and bad-debt portions scale with load, so the $/kWh rate doesn't drop as much as it would if the entire cost were fixed. The total MFC is small (~0.15-0.30¢/kWh), so in practice this is a rounding error for the cross-subsidy analysis.

---

## 3. Charges with no cross-subsidy

### True marginal costs

#### MSC: Energy (LBMP) — no cross-subsidy

**~$35–45/MWh. The bulk of the MSC and the entire supply bill.**

Energy is a genuine marginal cost. ConEd buys energy from the NYISO day-ahead market at Locational Based Marginal Prices (LBMP), which reflect the cost of the marginal generator dispatched in each hour. Each additional MWh an HP customer consumes costs ConEd the LBMP in that hour. The cost scales 1:1 with consumption.

If HP customers add 1,000 MWh, ConEd pays ~$40,000 more in energy purchases and collects ~$40,000 more in MSC revenue. No overcollection, no redistribution, no cross-subsidy.

There IS a second-order load-shape averaging effect: the residential MSC rate is calculated using the class-average hourly load profile. If HP customers shift the class average toward hours with different LBMPs, the flat rate changes slightly. But this is a load-shape averaging problem (which hours get more weight in the average), not a fixed-budget cross-subsidy. It can go in either direction — HP winter morning load may be cheaper or more expensive than the class-average load shape, depending on gas prices and dispatch conditions that year.

#### MSC: Working capital — no cross-subsidy

ConEd's cost of capital tied up in purchasing power before billing customers. Scales roughly with the dollar volume of procurement, which scales with kWh. If HP customers consume more, ConEd's working capital costs genuinely increase. No fixed pool to redistribute.

### Costs that scale with consumption

#### CES Supply Surcharge — no cross-subsidy

The CES Supply Surcharge recovers the cost of RECs (Tier 1), ZECs (Tier 3), and other CES compliance obligations that ConEd must purchase from NYSERDA as a Load Serving Entity.

The mechanism has a distinctive two-level structure:

1. **NYSERDA sets a statewide $/MWh rate** = (forecasted net REC procurement costs) ÷ (forecasted statewide electric load). Updated quarterly.
2. **ConEd pays NYSERDA** = (statewide $/MWh rate) × (ConEd's actual MWh served).

Each additional MWh an HP customer consumes literally costs ConEd money in REC purchases. ConEd's obligation to NYSERDA scales linearly with actual MWh served. If HP customers add 1,000 MWh, ConEd's NYSERDA bill goes up by (statewide rate × 1,000). ConEd also collects (surcharge rate × 1,000) from those customers. Costs and revenues move in lockstep. No overcollection, no redistribution.

This is fundamentally different from SBC. SBC has a fixed budget that doesn't change with consumption. CES Supply has a per-unit obligation — each MWh consumed creates a real REC purchase cost.

(There IS a tiny statewide effect: NYSERDA's REC contract costs are fixed in the near term, so if statewide load increases, the statewide $/MWh rate drops at the next quarterly reset. But this effect is diluted across all LSEs in the entire state — ConEd's HP adoption is a fraction of statewide load, so the impact on any individual customer is negligible.)

### Percentage-based pass-throughs

#### GRT (Gross Receipts Tax) — no cross-subsidy

NY municipalities levy a Gross Receipts Tax on utilities. Applied as a **percentage of each customer's charges** (not a flat $/kWh; not a fixed budget):

| Zone                  | GRT Distribution | GRT Supply |
| --------------------- | ---------------- | ---------- |
| H (Upper Westchester) | 3.33%            | 1.01%      |
| I (Lower Westchester) | 5.51%            | 3.09%      |
| J (NYC)               | 4.79%            | 2.41%      |

GRT is self-contained: each customer's GRT = (their charges) × (tax rate). If an HP customer's charges go up, their GRT goes up proportionally, but it doesn't lower anyone else's GRT. There is no fixed pool being spread. No cross-subsidy.

### Reconciliation adjustments

These are supply-side true-ups, analogous to MAC/RDM on the delivery side. They reconcile the forecasted MSC rate against actual wholesale costs. They are uniform $/kWh and don't change cross-subsidy rankings.

**MSC I Adjustment.** Reconciles actual energy costs vs. the forecasted MSC energy rate. Applied with a 2–3 month lag after NYISO settlement finalizes. Zone-specific (because LBMP differs by location). This is the supply analog of MAC: the base MSC rate already embeds the energy cost forecast; MSC I corrects when actuals diverge. Exclude for the same reason as MAC — it's level/timing noise, not rate-structure signal.

**MSC II Adjustment.** Reconciles actual capacity costs, ancillary services, and working capital vs. what was embedded in the MSC rate. Not zone-specific. Same logic as MSC I: the base MSC rate already embeds forecasts for these cost components; MSC II is the true-up mechanism. Exclude.

(Note: MSC I is zone-specific while MSC II is not. If we were modeling zone-level bill differences, the zone-specific MSC I adjustments would theoretically matter — but they're still uniform $/kWh _within_ a zone, so they don't affect within-zone cross-subsidy rankings. And across zones, the base MSC rate itself is already zone-specific, capturing the structural price differences.)

### Not applicable

**Tax Sur-Credit.** Expired. The joint proposal (22-E-0064) explicitly states: "Eliminate obsolete references to the Tax Sur-Credit under General Information Section IX.17 because it expired." Whatever appears in the Arcadia tariff JSON is a stale entry with a zero or near-zero lookup value.

**Customer Benefit Contribution (CBC, $1.84/kW of solar system size).** Only applies to customers with solar panels. Charged based on nameplate capacity, not consumption. Not relevant to the HP cross-subsidy analysis. For non-solar customers (the vast majority), this is $0.

**Minimum Charge ($20/month).** A bill floor equal to the Customer Charge. Ensures ConEd recovers basic service costs from very-low-usage customers. Rarely binds for typical residential consumption. Not a separate cost for cross-subsidy purposes.

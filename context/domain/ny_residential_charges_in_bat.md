# NY Residential Electric Charges: Cross-Subsidy Analysis for BAT

Every charge on a New York default residential electric bill — across all seven major utilities (ConEd, PSEG-LI, RG&E, NYSEG, National Grid, O&R, Central Hudson) — evaluated for whether it belongs in CAIRO's BAT and bill calculations, and whether it creates a cross-subsidy between heat pump (HP) and non-HP customers.

**Utility abbreviations used throughout:** CE = ConEd, LI = PSEG-LI, RG = RG&E, NY = NYSEG, NG = National Grid, OR = O&R, CH = Central Hudson.

---

## Charge type taxonomy

Charges are classified into families based on their economic structure, not their tariff name. Many charges that appear under different names on different utility bills are structurally identical.

| Type                       | What it is                                                                            | Cross-subsidy?                            | Decision pattern               |
| -------------------------- | ------------------------------------------------------------------------------------- | ----------------------------------------- | ------------------------------ |
| **Base delivery**          | Rates set in the rate case that collect the delivery revenue requirement              | Yes (the BAT's core subject)              | Already in d.r.r.              |
| **Cost reconciliation**    | Uniform $/kWh true-ups of costs already embedded in base rates (delivery or supply)   | No — shifts all bills equally             | `exclude_trueup`               |
| **Revenue true-up**        | Revenue decoupling and temporary over/under-collection corrections                    | No — shifts all bills equally             | `exclude_trueup`               |
| **Program surcharge**      | Fixed PSC-mandated program budgets recovered via uniform $/kWh                        | Yes — fixed pool ÷ kWh                    | `add_to_drr`                   |
| **Sunk-cost recovery**     | Fixed debt, bond, or settlement pools recovered via $/kWh or % of charges             | Yes — fixed pool ÷ kWh                    | `add_to_drr` (or `add_to_srr`) |
| **DER credit recovery**    | Fixed DER/VDER credit payments recovered via uniform $/kWh                            | Yes — fixed pool ÷ kWh                    | `add_to_drr`                   |
| **Performance incentive**  | REV Earnings Adjustment Mechanism — utility bonus for hitting PSC targets             | Yes in structure, negligible in magnitude | `exclude_negligible`           |
| **Supply commodity**       | Main supply charge (MSC or equivalent) bundling NYISO wholesale costs                 | Mixed — see sub-components                | `add_to_srr`                   |
| **Merchant function**      | Supply administration costs (procurement, working capital, bad debt)                  | Weak                                      | `add_to_srr`                   |
| **CES supply**             | Per-MWh LSE obligation to NYSERDA for RECs/ZECs — cost scales with load               | No                                        | `add_to_srr` + MC 8760         |
| **Tax pass-through**       | GRT (% of bill) or PILOTs (fixed per-customer)                                        | No                                        | `exclude_trueup` (tax)         |
| **Eligibility / optional** | Solar CBC, agricultural discounts, LMI credits, GreenUp — $0 for default customer     | N/A                                       | `exclude_eligibility`          |
| **Zonal duplicate**        | Same charge repeated per geographic zone/territory; only one representative is needed | No — already counted via representative   | `exclude_zonal`                |
| **Percentage of bill**     | QUANTITY/% charges the pipeline can't process (only $/kWh, $/month, $/day supported)  | Varies                                    | `exclude_percentage`           |
| **Redundant**              | Bill floor (minimum charge) that rarely binds; redundant with customer charge         | N/A                                       | `exclude_redundant`            |
| **Expired**                | Stale/expired charge; zero or near-zero value                                         | N/A                                       | `exclude_expired`              |

---

## Summary table

The MSC (Market Supply Charge) bundles several NYISO cost components into one $/kWh line item. We decompose it here because the sub-components have different cross-subsidy properties. The decomposition was done on ConEd's MSC but generalizes to all NYISO utilities — see [Supply charge decomposition generalizes](#supply-charge-decomposition-generalizes-to-all-nyiso-utilities).

| Charge                                                      | Type                  | Utilities                 | Unit                             | In rev req?     | Fixed budget?        | HP cross-subsidy?    | Why                                                          | Decision                | MC 8760?               |
| ----------------------------------------------------------- | --------------------- | ------------------------- | -------------------------------- | --------------- | -------------------- | -------------------- | ------------------------------------------------------------ | ----------------------- | ---------------------- |
| **Customer Charge**                                         | Base delivery         | All 7                     | $/mo or $/day                    | Yes             | —                    | Yes                  | Base rate; part of tariff                                    | Already in d.r.r.       | No—residual            |
| **Billing & Payment Processing**                            | Base delivery         | CE, NY, RG, OR            | $/mo                             | Yes             | —                    | Yes                  | Affects fixed/vol split                                      | Already in d.r.r.       | No—residual            |
| **Core Delivery Rate**                                      | Base delivery         | All 7                     | $/kWh                            | Yes             | —                    | Yes                  | Rate CAIRO calibrates                                        | Already in d.r.r.       | Yes—sub-tx + dx MCs    |
| **Make-Whole Energy**                                       | Base delivery         | NY, RG                    | $/kWh                            | Yes             | —                    | Yes                  | Supplemental delivery rate, fixed in rate case               | Already in d.r.r.       | Yes—sub-tx + dx MCs    |
| **Minimum Charge**                                          | Redundant             | All 7                     | $/mo (floor)                     | Yes             | —                    | N/A                  | Bill floor; rarely binds; redundant with customer charge     | `exclude_redundant`     | —                      |
| **Delivery cost true-up (MAC / RAM / DSA)**                 | Cost recon            | All 7                     | $/kWh                            | N/A (true-up)   | —                    | No                   | Uniform $/kWh; true-up noise                                 | `exclude_trueup`        | —                      |
| **MAC sub-components (Uncollectible, Reconciliation Rate)** | Cost recon            | CE                        | $/kWh                            | N/A (true-up)   | —                    | No                   | Sub-components of MAC statement                              | `exclude_trueup`        | —                      |
| **Transition / Restructuring**                              | Cost recon            | All 7                     | $/kWh                            | N/A (true-up)   | —                    | No                   | Legacy stranded costs; balance paid down                     | `exclude_trueup`        | —                      |
| **Transmission Revenue Adjustment**                         | Cost recon            | NG                        | $/kWh                            | N/A (true-up)   | —                    | No                   | FERC formula rate tx cost reconciliation                     | `exclude_trueup`        | —                      |
| **Net Utility Plant / Depreciation Recon**                  | Cost recon            | NG                        | $/kWh                            | N/A (true-up)   | —                    | No                   | Capital spending vs rate-case forecast                       | `exclude_trueup`        | —                      |
| **Purchased Power Adjustment**                              | Cost recon            | CH                        | $/kWh                            | N/A (true-up)   | —                    | No                   | Retained gen + mandatory IPP true-up                         | `exclude_trueup`        | —                      |
| **Supply cost true-up (MSC I/II, ECA, MPA, ESR)**           | Cost recon            | CE, NG, OR, CH            | $/kWh                            | N/A (true-up)   | —                    | No                   | Wholesale cost forecast error                                | `exclude_trueup`        | —                      |
| **Revenue Decoupling (RDM)**                                | Revenue true-up       | All 7                     | $/kWh                            | N/A (revenue)   | —                    | No                   | Load forecast error; true-up noise                           | `exclude_trueup`        | —                      |
| **Delivery Revenue Surcharge / Electric Bill Credit**       | Revenue true-up       | CE, OR, CH                | $/kWh                            | N/A (temporary) | —                    | No                   | Rate-delay or over-collection correction                     | `exclude_trueup`        | —                      |
| **System Benefits Charge**                                  | Program surcharge     | All except LI             | $/kWh                            | **No**          | Yes                  | **Yes**              | Fixed NYSERDA budget ÷ kWh                                   | Add to d.r.r.           | No—residual            |
| **CES Delivery**                                            | Program surcharge     | CE, NY, NG                | $/kWh                            | **No**          | Yes                  | **Yes**              | Fixed CES program budget ÷ kWh                               | Add to d.r.r.           | No—residual            |
| **NY State Surcharge (§18-a)**                              | Program surcharge     | CE, LI                    | % of total bill (LI); $/kWh (CE) | **No**          | Yes                  | **Yes**              | Fixed PSL §18-a assessment; LI: % of bill (tariff Leaf 182H) | Add to d.r.r.           | No—residual            |
| **DLM Surcharge**                                           | Program surcharge     | CE, RG, NY, NG, OR        | $/kWh                            | **No**          | Yes                  | **Yes**              | Fixed DR program costs ÷ kWh                                 | Add to d.r.r.           | No—residual            |
| **EV Make Ready**                                           | Program surcharge     | CE, NY, NG, OR            | $/kWh                            | **No**          | Yes                  | **Yes**              | Fixed EV infra program ÷ kWh                                 | Add to d.r.r.           | No—residual            |
| **Energy Storage Surcharge**                                | Program surcharge     | OR (+ CH bundled)         | $/kWh                            | **No**          | Yes                  | **Yes** (negligible) | Fixed storage program ÷ kWh; $0.00002/kWh                    | Add to d.r.r.           | No—residual            |
| **Central Hudson Misc Charges**                             | Program surcharge     | CH                        | $/kWh                            | **No**          | Mostly yes           | **Yes** (mixed)      | Umbrella: arrears, EV, storage, VDER, DR, make-whole         | Add to d.r.r.           | No—residual            |
| **Arrears / COVID forgiveness**                             | Sunk-cost recovery    | CE, NY, NG (+ CH bundled) | $/kWh                            | **No**          | Yes (finite)         | **Yes** (temporary)  | COVID debt forgiveness ÷ kWh                                 | Add to d.r.r.           | No—residual            |
| **Late Payment / Waived Fees**                              | Sunk-cost recovery    | NY                        | $/kWh                            | **No**          | Yes (finite)         | **Yes** (temporary)  | Waived COVID-era fees ÷ kWh                                  | Add to d.r.r.           | No—residual            |
| **Recovery Charge (storm bonds)**                           | Sunk-cost recovery    | NY                        | $/kWh                            | **No**          | Yes (bond schedule)  | **Yes** (temporary)  | $710.6M securitized storm bonds ÷ kWh                        | Add to d.r.r.           | No—residual            |
| **Shoreham Property Tax Settlement**                        | Sunk-cost recovery    | LI (Suffolk only)         | % of total bill                  | **No**          | Yes (settlement)     | **Yes** (temporary)  | $620M settlement; % of total bill (tariff Leaf 172)          | Add to d.r.r.           | No—residual            |
| **Securitization Charge / Offset (UDSA)**                   | Sunk-cost recovery    | LI                        | $/kWh                            | **No**          | Yes (bond schedule)  | **Yes**              | Fixed UDSA bond debt service ÷ kWh                           | Add net to s.r.r.       | No—residual            |
| **VDER / DER Cost Recovery**                                | DER credit recovery   | CE, LI                    | $/kWh                            | **No**          | Yes                  | **Yes**              | Fixed DER credit payments ÷ kWh                              | Add to d.r.r.           | No—residual            |
| **Earnings Adjustment Mechanism**                           | Performance incentive | NY, RG, NG, CH            | $/kWh                            | **No**          | Yes (once earned)    | **Negligible**       | Performance bonus; ~$0.02–0.16/mo                            | `exclude_negligible`    | —                      |
| **Supply commodity (bundled)**                              | Supply commodity      | All 7                     | $/kWh                            | **No**          | Mixed                | Mixed                | Energy + capacity + ancillary bundled                        | Add to s.r.r.           | See sub-components     |
| **↳ MSC: Energy (LBMP)**                                    | Supply sub-component  | All 7                     | $/kWh                            | **No**          | **No**               | **No**               | True marginal cost; scales 1:1                               | (in s.r.r.)             | No—residual            |
| **↳ MSC: Capacity (ICAP/UCAP)**                             | Supply sub-component  | All 7                     | $/kWh (embedded)                 | **No**          | Yes (current period) | **Yes**              | Peak-determined cost, volumetric recovery                    | (in s.r.r.)             | Yes—zonal LBMPs        |
| **↳ MSC: Ancillary Services**                               | Supply sub-component  | All 7                     | $/kWh (embedded)                 | **No**          | Mostly               | **Yes** (small)      | Hourly pool ÷ hourly load                                    | (in s.r.r.)             | Yes—???                |
| **↳ MSC: Uplift (BPCG)**                                    | Supply sub-component  | All 7                     | $/kWh (embedded)                 | **No**          | Yes                  | **Yes** (tiny)       | Reliability dispatch costs ÷ load                            | (in s.r.r.)             | No—residual            |
| **↳ MSC: NYISO Schedule 1**                                 | Supply sub-component  | All 7                     | $/kWh (embedded)                 | **No**          | Yes                  | **Yes** (tiny)       | Fixed NYISO admin budget ÷ MWh                               | (in s.r.r.)             | No—residual            |
| **↳ MSC: Working Capital**                                  | Supply sub-component  | All 7                     | $/kWh (embedded)                 | **No**          | **No**               | **No**               | Scales with procurement volume                               | (in s.r.r.)             | No—residual            |
| **Merchant Function Charge**                                | Merchant function     | All 7                     | $/kWh                            | **No**          | Mixed                | **Weak**             | Fixed admin + load-proportional costs                        | Add to s.r.r.           | No—residual            |
| **CES Supply Surcharge**                                    | CES supply            | CE, RG, NY, NG            | $/kWh                            | **No**          | **No**               | **No**               | Per-MWh LSE obligation; cost scales with load                | Add to s.r.r.           | Yes—flat $/kWh all hrs |
| **GRT**                                                     | Tax pass-through      | CE                        | % of charges                     | **No**          | **No**               | **No**               | % of own bill; no fixed pool                                 | `exclude_trueup`        | —                      |
| **PILOTs (Cities/Villages)**                                | Tax pass-through      | LI                        | $/mo (fixed)                     | **No**          | Yes (PILOT)          | **No**               | Fixed per-customer; not volumetric                           | `exclude_trueup`        | —                      |
| _**CBC (solar only)**_                                      | _Eligibility_         | _All 7_                   | _$/kW_                           | _No_            | _N/A_                | _N/A_                | _Solar-only; $0 for non-solar_                               | _`exclude_eligibility`_ | _—_                    |
| _**RAD (agricultural)**_                                    | _Eligibility_         | _NY, RG_                  | _$/kWh credit_                   | _N/A_           | _—_                  | _N/A_                | _$0 for non-ag customers_                                    | _`exclude_eligibility`_ | _—_                    |
| _**Low-income discounts**_                                  | _Eligibility_         | _RG, OR, CH, NG_          | _$/mo or $/kWh credit_           | _N/A_           | _—_                  | _N/A_                | _$0 for non-LMI; use in LMI analysis_                        | _`exclude_eligibility`_ | _—_                    |
| _**GreenUp**_                                               | _Eligibility_         | _NG_                      | _$/kWh_                          | _N/A_           | _—_                  | _N/A_                | _Voluntary opt-in; $0 for default_                           | _`exclude_eligibility`_ | _—_                    |
| _**Tax Sur-Credit**_                                        | _Expired_             | _CE_                      | _$/kWh_                          | _N/A_           | _N/A_                | _N/A_                | _Expired_                                                    | _`exclude_expired`_     | _—_                    |

---

## The generalized cross-subsidy: fixed cost pools recovered volumetrically

The BAT traditionally operates on the delivery revenue requirement: a fixed cost pool recovered through a tariff structure that may not match how those costs were caused. Marginal cost allocation + residual allocation under a normative choice of allocator defines what each customer _should_ pay; the difference between that and what they _do_ pay is the cross-subsidy.

But the delivery revenue requirement isn't the only fixed cost pool recovered volumetrically on a NY residential electric bill. Many charges outside the revenue requirement share the same structure. Consider the **System Benefits Charge (SBC)** as a worked example.

**What the SBC is.** Established by the PSC in 1996, the SBC funds NYSERDA programs: energy efficiency rebates, low-income energy assistance, renewable energy research, the Clean Energy Fund, and related initiatives. It is governed by its own PSC proceedings (Cases 14-M-0094, 18-M-0084), independent of any utility's rate case. The SBC has a fixed budget for each program period (the PSC approves a total NYSERDA funding level). The utility collects that budget by charging all customers a flat $/kWh rate set based on a sales forecast.

**The HP cross-subsidy mechanic.** Suppose a wave of residential customers adopt heat pumps, roughly doubling their kWh consumption. All else equal:

1. The SBC budget for the current period is fixed (NYSERDA's approved funding level doesn't change because some customers got heat pumps).
2. Total kWh billed increases (more HP consumption).
3. The utility collects more SBC revenue than the budget requires (same $/kWh rate × more kWh).
4. At the next true-up, the surplus is credited back — the $/kWh SBC rate drops.
5. Non-HP customers, who didn't change their consumption, now pay less per kWh for SBC.
6. HP customers paid more in total (higher kWh × the original rate), but the budget was fixed, so their extra payments mechanically subsidized non-HP customers' SBC costs.

This is the same structural dynamic as the delivery revenue requirement: a fixed cost pool, recovered volumetrically, where one group's increased consumption mechanically reduces the per-unit cost for everyone else.

**How this differs from the BAT's usual analysis.** For the delivery revenue requirement, the BAT has a cost-of-service basis — marginal costs tell us what each customer's load actually costs the system; the residual allocator provides a normative standard for the rest. For SBC, there is no analogous cost-of-service basis. NYSERDA's programs don't have customer-level marginal costs. We can only say: if the SBC budget is fixed and recovered volumetrically, HP adoption mechanically lowers SBC bills for non-HP customers. Whether that constitutes a "cross-subsidy" depends on definition — it's not a deviation from cost-of-service, but it IS a wealth transfer caused by the rate structure.

**Integration with BAT: topping up the revenue requirement.** One practical approach is to add SBC costs to the revenue requirement CAIRO optimizes against. The BAT then applies the chosen residual allocator:

- **Peak allocation**: HP customers' residual share depends on their contribution to system peak. If HP load doesn't drive the peak (currently true — NYISO is summer-peaking), HP customers get a small residual share and thus a small SBC allocation. The cross-subsidy from volumetric recovery would show up as HP customers overpaying.
- **Per-customer (flat) allocation**: Each customer gets an equal residual share. SBC costs are spread evenly regardless of kWh. The volumetric recovery cross-subsidy would again show HP customers overpaying.
- **Volumetric allocation**: Each customer's residual share is proportional to kWh. This replicates what SBC already does (uniform $/kWh), so topping up changes nothing — no additional cross-subsidy is identified.

The upshot: topping up the revenue requirement is most informative under peak or per-customer residual allocation. Under volumetric allocation, it's a no-op.

**This applies to every "fixed pool ÷ kWh" charge.** Program surcharges, sunk-cost recovery charges, and DER credit recovery charges all share this structure. The analysis above applies to all of them.

---

## Charge-by-charge analysis

### Base delivery rates

These charges define the tariff structure that collects the delivery revenue requirement. CAIRO should model all of them.

**Customer Charge.** All seven NY utilities have a monthly (or daily, for PSEG-LI) fixed customer charge set in rate cases. CE: $20/mo; LI: $0.56/day (~$16.80/mo); RG: $23/mo; NY: $19/mo; NG: $19/mo; OR: $22.50/mo; CH: $22.50/mo. In precalc mode, this revenue is subtracted before deriving the volumetric rate. Fixed charges are load-shape-insensitive — every customer pays the same regardless of when or how much they consume.

**Billing and Payment Processing.** CE: $1.28/mo; NY: $0.89/mo; RG: $0.99/mo; OR: $2.10/mo. Unbundled from the customer charge so billing cost recovery is transparent. Not all utilities break this out (LI, NG, CH fold it into the customer charge or delivery rate). Including it ensures CAIRO gets the fixed/volumetric split right.

**Core Delivery Rate.** The main volumetric delivery charge on every NY utility bill. Structures vary: CE is seasonal, tiered (250 kWh), zoned (H/I/J); LI is TOU + seasonal (3–7 PM weekday peak); OR is seasonal + tiered; others are flat $/kWh. CAIRO calibrates this rate in precalc mode.

**Make-Whole Energy (NYSEG $0.00276/kWh, RG&E $0.00221/kWh).** A small, fixed per-kWh delivery charge on NYSEG and RG&E bills only (both Avangrid subsidiaries). Despite the name, this is not a variable true-up — the rate is set in the rate case and fixed for the rate-case period. For RG&E, it is in the same "Energy Delivery Charge" rate group as the base delivery rate and classified as DISTRIBUTION. Poorly documented; appears to recover a specific cost shortfall the PSC ordered shown separately rather than rolled into the base rate. Functionally part of the base delivery rate structure. Same marginal costs apply.

**Minimum Charge.** All utilities have a bill floor (equal to or slightly above the customer charge). Rarely binds for typical residential consumption. Not a separate cost for cross-subsidy purposes.

**Cross-subsidy analysis.** The base delivery rates are the BAT's core subject. The tariff structure — fixed vs. volumetric split, tier design, TOU/seasonal differentiation — determines how the revenue requirement is collected and whether that collection aligns with cost causation. CAIRO calibrates against these rates.

**Decision.** Already in d.r.r. — these define the tariff.

---

### Cost reconciliation

The delivery and supply bills include several variable $/kWh adjustments that true up actual costs against the rate-case or supply-charge forecast. Despite different names across utilities, they all share the same structure.

**The core argument for excluding all cost reconciliation.** The cross-subsidy between HP and non-HP customers comes from the _shape mismatch_ between how the tariff collects revenue and how costs are actually caused. A uniform $/kWh adder shifts both sides of the BAT equation by the same amount per kWh — the bill alignment ranking between customers does not change. If an adjustment is (a) uniform $/kWh and (b) either already embedded in the revenue requirement forecast or averages to ~zero over time, it's noise, not signal. Including it would overfit the BAT to whichever year happened to have a big storm or warm winter.

#### Delivery-side cost true-ups

These are all structurally identical — different utilities call them different things:

**MAC (Monthly Adjustment Clause) — ConEd.** True-ups actual delivery costs (property taxes, storm costs, pension, environmental) against base rates. Monthly, uniform $/kWh. The base rate already embeds the forecasted level of all MAC-covered costs; MAC corrects when reality diverges.

**MAC sub-components — ConEd.** Uncollectible Bill Expense, Transition Adjustment, and Reconciliation Rate are sub-components of the MAC statement. Each is a uniform $/kWh true-up for a specific cost basket (bad debt, stranded-cost recovery, reconciliation).

**RAM (Rate Adjustment Mechanism) — NYSEG, RG&E, National Grid, Central Hudson.** The same mechanism as MAC but under a different name. For NYSEG and RG&E (Avangrid), the RAM is defined in PSC No. 120, Section 25 under the Transition Charge umbrella. It reconciles generation and purchased power costs: hydro output, NUG/NYPA contract costs, demand response expenses, transmission wheeling, and PSC-approved adjustments. Monthly true-up; symmetric (overcollection credited, undercollection surcharged). Magnitudes are small — RG&E's RAM is $0.00029/kWh (~$0.15/month at 500 kWh).

**Delivery Service Adjustment (DSA) — PSEG-LI.** LIPA's equivalent. Reconciles three financing cost categories: (1) debt service (actual vs. projected), (2) coverage adjustments to hit LIPA's 1.45x debt coverage ratio target, (3) other financing costs (interest rate swaps, LOC fees, remarketing fees). From LIPA's 2020 budget, the DSA ranged $23M–$31M — a small fraction of ~$3.6B total delivery revenue.

**Transmission Revenue Adjustment (TRA) — National Grid.** Reconciles actual transmission costs against what's embedded in retail delivery rates. Unlike distribution costs (set in PSC rate cases), National Grid's transmission costs are governed by a FERC-approved formula rate under the NYISO Open Access Transmission Tariff (2009 Settlement Agreement). The formula computes a Transmission Revenue Requirement from investment return and income taxes on transmission plant (~~$137M), depreciation (~$43M), and property tax (~~ $40M) — totaling ~$220M. It updates annually; the TRA adjusts residential bills to match. Same exclude logic — backwards-looking regulatory cost recovery, not forward-looking marginal cost (Cambium captures that).

**Net Utility Plant and Depreciation Expense Reconciliation — National Grid.** Trues up actual capital spending against the rate-case-approved capital budget. If National Grid spends less than approved, base rates over-collected → credit to customers. If it spends more (PSC-approved) → surcharge. Currently ~$0 (well-aligned). The two biggest pieces of any revenue requirement (return on rate base and depreciation) are already in base rates; this corrects forecast error.

**Purchased Power Adjustment (PPA) — Central Hudson.** Reconciles non-avoidable costs from Central Hudson's remaining generating facilities and mandatory IPP purchases under legacy PURPA/restructuring-era contracts. Unlike ConEd (which divested all generation), Central Hudson still owns plants and is obligated to buy from IPPs at predetermined rates. These costs are classified as delivery (DISTRIBUTION) because customers can't escape them by switching to an ESCO. Costs genuinely vary (hydro depends on water availability, IPP contracts have fuel-index components); the PPA reconciles actuals against the base-rate forecast monthly.

**Transition / Restructuring — All 7 utilities.** Recovers legacy costs from NY's 1990s electricity restructuring (stranded costs, deferred balances). Appears under various names: Transition Adjustment (CE, CH), Transition Charge (RG, NY), Legacy Transition Charge (NG), Transition Adj. for Competitive Services (OR). The rate changes as balances are paid down. Uniform $/kWh true-up.

#### Supply-side cost true-ups

Same logic as the delivery side — these reconcile the forecasted supply rate against actual wholesale costs.

**MSC I Adjustment — ConEd.** Reconciles actual energy costs vs. the forecasted MSC energy rate. Applied with a 2–3 month lag after NYISO settlement. Zone-specific (LBMPs differ by location). MSC I is still uniform $/kWh _within_ a zone.

**MSC II Adjustment — ConEd.** Reconciles actual capacity costs, ancillary services, and working capital vs. what was embedded in the MSC rate. Not zone-specific.

**Equivalent mechanisms at other utilities:** O&R's Energy Cost Adjustment (ECA), Central Hudson's Market Price Adjustment (MPA), National Grid's Electricity Supply Reconciliation (ESR). All are supply-side wholesale cost true-ups under different names.

**Decision for all cost reconciliation.** `exclude_trueup`. Uniform $/kWh true-ups of costs already in base rates (or the supply charge). They don't change cross-subsidy rankings. Including them would overfit the BAT to whichever period's cost variance happened to show up.

---

### Revenue true-ups

**RDM (Revenue Decoupling Mechanism) — All 7.** True-ups revenue (load forecast error), not costs. Part of NY's statewide decoupling policy. CAIRO's precalc already calibrates rates so total bills = revenue requirement over the ResStock population — that IS the structural question. RDM noise would overfit to a particular year's load deviation. Reconciled annually (not monthly like MAC).

**Delivery Revenue Surcharge (DRS) — ConEd, O&R.** Recovers a revenue shortfall caused by new rates being delayed — the PSC approved higher rates but they didn't take effect for several months. Temporary, uniform $/kWh.

**Electric Bill Credit — Central Hudson.** The inverse of DRS: returned $16.351 million in accumulated regulatory liabilities (over-collection). Incorporated into Case 24-E-0461, reduced the revenue increase request by ~32% for the rate year ending June 30, 2025. Expired June 30, 2025.

**Decision.** `exclude_trueup` for all. Revenue true-ups are noise, not structural rate design signal.

---

### Program surcharges

All charges in this section share the "fixed cost pool ÷ kWh" structure described in [The generalized cross-subsidy](#the-generalized-cross-subsidy-fixed-cost-pools-recovered-volumetrically). Many are statewide, mandated by the same PSC orders, with the same structure at every utility.

**System Benefits Charge — All except PSEG-LI.** Established by the PSC in 1996. Funds NYSERDA programs: energy efficiency rebates, low-income assistance, renewable energy research, Clean Energy Fund. Governed by Cases 14-M-0094, 18-M-0084. RG&E breaks it into two sub-components (Energy Efficiency + Clean Energy Fund). PSEG-LI does not have a separate SBC in Genability — may be bundled into other charges under LIPA's governance.

**CES Delivery Surcharge — ConEd, NYSEG, National Grid.** Recovers utility costs for grid infrastructure needed to comply with NY's Clean Energy Standard (distribution upgrades, DER integration). Established by Case 15-E-0302. Not present in Genability for LI, RG, OR, CH — may be bundled differently.

**NY State Surcharge (PSL §18-a) — ConEd, PSEG-LI.** The state regulatory assessment that funds the Department of Public Service and Long Island office of DPS. ConEd shows it as "NY State Surcharge"; PSEG-LI calls it "New York State Assessment." All utilities must collect it, but only CE and LI show it as a separate Genability line item — for others it is likely bundled into delivery rates or another adjustment. Per LIPA's tariff book (Leaves 182H–182I), the PSEG-LI version is a **percentage of the total bill** — the assessment amount divided by projected revenues, applied to base rates, Power Supply Charge, VBA, DER Cost Recovery, Shoreham SPT factor, RDM, DSA, Securitization charges, UGC, CBC, and miscellaneous charges (everything except PILOTs). The rate adjusts annually to hit the fixed assessment target, so it has the same fixed-pool cross-subsidy mechanic as other surcharges. ConEd's version may be structured differently ($/kWh).

**DLM (Dynamic Load Management) Surcharge — ConEd, RG&E, NYSEG, National Grid, O&R.** Recovers payments to demand response participants (customers and batteries that reduce load during peaks). Established under Case 14-E-0423 and REV proceedings. RG&E: $0.000084/kWh. Not present in Genability for LI or CH (CH bundles DR costs into Misc Charges).

**EV Make Ready Surcharge — ConEd, NYSEG, National Grid, O&R.** Recovers utility costs for installing EV charging infrastructure (conduit, wiring) under Case 18-E-0138 (July 2020). CE: $0.0008/kWh; NY: $0.000127/kWh; NG: $0.00147/kWh; OR: $0.00236/kWh. Not present in Genability for LI, RG, or CH (CH bundles it into Misc Charges).

**Energy Storage Surcharge — O&R (+ Central Hudson bundled).** Recovers costs for energy storage deployment programs — battery incentives, grid integration, pilot programs (e.g., O&R's 300-customer residential solar+battery pilot with Sunrun). Part of NY's statewide energy storage mandate. OR: $0.00002/kWh ($0.01/month at 500 kWh — one penny). Central Hudson's energy storage component ($0.00001/kWh) is inside Misc Charges.

**Central Hudson Miscellaneous Charges — Central Hudson only.** An umbrella bundling multiple sub-charges into a single Genability line item ($0.00522/kWh as of Feb 2025):

| Sub-component                   | $/kWh    | Equivalent                  | Cross-subsidy?       |
| ------------------------------- | -------- | --------------------------- | -------------------- |
| Make Whole Factor               | $0.00223 | DRS (regulatory make-whole) | No (reconciliation)  |
| Arrears Reduction Surcharge     | $0.00185 | Arrears Management          | **Yes** (temporary)  |
| EV Make Ready Program           | $0.00068 | EV Make Ready               | **Yes**              |
| Miscellaneous Charges II Factor | $0.00045 | MAC (reconciliation)        | No (true-up)         |
| Energy Storage Program Recovery | $0.00001 | Energy Storage (O&R)        | **Yes** (negligible) |
| Clean Heat Deployment Program   | $0.00000 | None (HP incentives)        | **Yes** when active  |

Also covers: Targeted Demand Response (≈ DLM), DSIP costs, Value Stack Compensation (≈ VDER), Commercial System Relief. Genability presents them as one rate; no way to pull sub-components via the API. The program-cost riders dominate the cross-subsidy-relevant portion; the reconciliation pieces are small and absorbed into the residual.

**Note on Clean Heat Deployment Program.** Currently $0, but this is Central Hudson's heat pump incentive cost recovery. When it activates, HP adoption will literally increase the surcharge's cost pool (more HP incentives → higher costs to recover), creating a feedback loop.

**Decision for all program surcharges.** Add to d.r.r. No MC 8760 — all are residual costs. For Central Hudson Misc Charges, add the full amount; the reconciliation components don't distort the BAT.

---

### Sunk-cost recovery

These are fixed debt, bond, or settlement pools recovered volumetrically. They share the "fixed pool ÷ kWh" cross-subsidy mechanic but differ from program surcharges in that the costs are entirely sunk — the events already happened, the money is already owed. There is no marginal cost component and no ongoing program budget that could change.

**Arrears / COVID forgiveness.** Recovers costs from COVID-era arrears forgiveness programs (Case 14-M-0565). CE: $0.0012/kWh (Arrears Management Program Recovery Surcharge). NY: Phase 1 + Phase 2 (separate line items). NG: $0.00087/kWh. CH: bundled in Misc Charges ($0.00185/kWh Arrears Reduction Surcharge). The total forgiven debt is a defined, finite pool being amortized. Temporary — once recovered, the surcharge goes away.

**Late Payment / Waived Fees Surcharge — NYSEG.** Recovers revenue from late payment charges and fees NYSEG waived during the COVID moratorium and winter 2022–2023 (when supply prices spiked ~42% and the PSC ordered fee suspensions). Filed semi-annually as the pool is paid down. Closer to Arrears Management than to Uncollectible Bill Expense: it recovers specific, finite amounts from defined waiver periods, not an ongoing bad-debt reconciliation. Magnitude likely $0.001–0.003/kWh range.

**Recovery Charge — NYSEG ($0.009922/kWh).** Securitized storm-cost recovery authorized under NY's Utility Corporation Securitization Act (signed August 2024) and PSC Financing Order Case 24-E-0493 (December 2024). NYSEG issued $710.6 million in Recovery Bonds (Series 2025-A, rated Aaa) through NYSEG Storm Funding, LLC. Three tranches maturing 2031, 2034, 2037. Effective February 14, 2025, non-bypassable. At $0.009922/kWh this is one of the larger riders on the bill (~$5/month at 500 kWh; 8× ConEd's Arrears surcharge). Rate adjusted semi-annually so collections match the bond debt-service schedule. Material cross-subsidy: a 10,000 kWh/yr HP customer pays ~$99/yr vs. ~$50/yr for a 5,000 kWh non-HP customer — a $49/yr difference that is entirely a function of volumetric recovery of a fixed pool.

**Shoreham Property Tax Settlement — PSEG-LI (Suffolk County only).** Recovers LIPA's payment of a $620 million property-tax refund related to the Shoreham nuclear power plant. Shoreham was built by LILCO (1973–1984) at $6 billion, operated briefly in 1986, decommissioned 1989 after Suffolk County determined safe evacuation was impossible. LIPA acquired it for $1 in 1992. The settlement authorized a surcharge on Suffolk County customers for up to 30 years (~2003 to ~2033). Per LIPA's tariff book (Leaf No. 172), this is a **percentage-of-revenue factor applied to monthly bills** — the annual bond repayment cost divided by expected retail revenues for the year. Applied as a percentage of the total bill (Genability: chargeType QUANTITY, rateUnit PERCENTAGE), not as a flat $/kWh or a percentage of delivery charges alone. Since it recovers a fixed pool (bond repayment) and the percentage rate adjusts annually to hit the target, HP customers with higher total bills pay a larger share of the fixed pool — the same cross-subsidy mechanic as any fixed-pool ÷ volumetric charge, but computed on total bill rather than kWh. Zero for Outside Suffolk (Nassau, Rockaways).

**Securitization Charge / Offset — PSEG-LI.** Two paired line items (both SUPPLY,CONTRACTED) arising from the Utility Debt Securitization Authority (UDSA), created to refinance LIPA's massive legacy debt (LILCO acquisition, Shoreham obligations) through lower-cost restructuring bonds. UDSA has issued five series, most recently ~$1.09B in Series 2025, generating cumulative NPV savings of ~$699M. The **Securitization Charge** collects bond debt service; the **Securitization Offset** removes the old, higher debt-service costs from base rates. The net (Charge − Offset) is the actual burden. Aggregate restructuring charge represents ~11% of total residential bills; the offset partially reduces it. Annual true-up adjusts the per-kWh rate. No ConEd equivalent — ConEd is investor-owned and doesn't have LIPA's public-authority debt history.

**Decision for all sunk-cost recovery.** Add to d.r.r. (delivery-classified) or s.r.r. (supply-classified, i.e., PSEG-LI Securitization net). No MC 8760 — purely residual. These are fixed pools with no marginal cost component.

---

### DER credit recovery

**VDER Cost Recovery — ConEd ($0.0011/kWh).** Recovers payments to solar generators under NY's VDER/Value Stack tariff (Cases 14-M-0224, 15-E-0082, 19-M-0463). When solar systems export power, the utility compensates them at time-varying Value Stack rates; this surcharge recovers those costs from all ratepayers.

**DER Cost Recovery — PSEG-LI.** LIPA's equivalent. Recovers costs from compensating DER (rooftop solar, CDG, behind-the-meter storage) under the VDER / Value Stack framework. CONSUMPTION_BASED, variable via lookup. ~$1.50/month at 500 kWh.

**Cross-subsidy analysis.** The total DER credits paid out in a given period form a fixed cost pool (determined by installed DER capacity and generation, not by non-DER customers' consumption). Recovered via uniform $/kWh. The DER budget could grow with solar adoption, but that's a separate causal channel from HP adoption. Ceteris paribus — holding DER credits fixed — more HP kWh mechanically lowers the per-kWh surcharge.

**Decision.** Add to d.r.r. No MC 8760 — residual cost.

---

### Performance incentive (Earnings Adjustment Mechanism)

The EAM is a performance-based incentive created by the NY PSC under REV (Reforming the Energy Vision). It is **not a cost recovery mechanism** — it is a bonus paid to the utility for achieving policy-outcome targets. Present at NYSEG, RG&E, National Grid, and Central Hudson (not ConEd, not PSEG-LI).

**What it is.** Metrics include: electric peak reduction (MW), DER utilization, residential energy intensity, LMI energy efficiency savings, and beneficial electrification (HP adoption, EV carbon reduction). The PSC sets tiered targets (minimum/midpoint/maximum). If the utility hits targets, it earns a bonus collected from ratepayers as a uniform $/kWh surcharge over the following 12 months. If it doesn't, the EAM is $0. Floor is zero — no penalty below baseline. Example magnitudes: O&R earned $1.95M (electric) in 2024; National Grid earned $12.3M in 2019. Per-kWh: NYSEG $0.00032/kWh ($0.16/mo at 500 kWh), RG&E $0.000047/kWh ($0.02/mo). Among the smallest line items on the bill.

**Cross-subsidy analysis.** Technically, once the EAM amount is determined, it is a fixed dollar pool recovered volumetrically — same structure as SBC. However: (1) amounts are negligible — doubling consumption shifts ~$1.60/year at NYSEG, ~$0.24/year at RG&E; (2) it's conceptually different — a reward for utility behavior, not a customer-caused cost; (3) one of the EAM metrics is HP adoption itself, so more HP customers could _increase_ the EAM (utility hits its electrification target), partially offsetting the volumetric effect.

**Decision.** `exclude_negligible`. Cross-subsidy is real in structure but negligible in magnitude. No cost-of-service story to tell. Complexity for no meaningful BAT change.

---

### Supply commodity

All seven NY utilities buy power from the NYISO wholesale market. The main supply charge on each bill bundles several distinct NYISO cost components into a single $/kWh line item:

| Utility        | Supply charge name         | Structure                            |
| -------------- | -------------------------- | ------------------------------------ |
| ConEd          | MSC (Market Supply Charge) | By zone (H/I/J); Lookup              |
| PSEG-LI        | Power Supply Charge        | TOU + seasonal; fixed rates          |
| RG&E           | Supply Charge              | Lookup                               |
| NYSEG          | Supply Service Charge      | By territory (East/LHV/West); Lookup |
| National Grid  | Electricity Supply Charge  | By zone (6 zones); Lookup            |
| O&R            | Market Supply Charge       | Lookup                               |
| Central Hudson | Market Price Charge        | Lookup                               |

**How the supply charge works (ConEd MSC as example).** Each month, the utility forecasts the total cost of procuring electricity for default-service customers and converts it into a $/kWh rate. The rate is load-weighted: NYISO day-ahead LBMPs for each hour, weighted by the residential class's average hourly load profile, adjusted for losses, producing a flat $/kWh rate. Set prospectively; actual cost deviations are reconciled through the supply cost true-ups described above.

#### MSC sub-component decomposition

The costs bundled into every NY utility's supply charge:

**Energy (LBMP) — ~$35–45/MWh. No cross-subsidy.** Genuine marginal cost. Each additional MWh costs the utility the LBMP in that hour. Costs scale 1:1 with consumption. If HP customers add 1,000 MWh, the utility pays ~$40,000 more and collects ~$40,000 more. No overcollection, no redistribution. (There is a second-order load-shape averaging effect if HP customers shift the class-average profile toward different-priced hours, but this can go in either direction and is not a fixed-budget cross-subsidy.)

**Capacity (ICAP/UCAP) — ~$3–6/MWh equivalent. Cross-subsidy: yes.** This is the most significant supply-side cross-subsidy. NYISO's Installed Capacity market requires every LSE to procure enough UCAP to cover its share of the resource adequacy requirement, determined by coincident peak load ratio. Zone J (NYC) has its own locational requirement with higher clearing prices (~$19.84/kW-month reference 2024–2025). For a given capability year, the utility's capacity obligation (MW) and auction clearing prices are locked in — total capacity cost doesn't change with individual consumption. But SC1 residential customers don't have demand meters; capacity costs are divided by total kWh to produce a flat per-kWh component. HP customers consuming more kWh → pay a larger share of the fixed capacity cost pool.

**The winter-peaking nuance.** Currently, NYISO is summer-peaking. HP load is primarily winter. So HP winter heating load does not increase the summer peak, does not increase the utility's capacity obligation, and the capacity cost pool is truly fixed relative to HP adoption — making the cross-subsidy pure. However, NYISO is studying the transition to winter peaking as electrification grows (EPRI heat pump assessment for NYISO's Long-Term Forecasting Task Force). At extreme winter design conditions (-3°F in the Hudson Valley), ASHP COP drops to ~1.12 and supplemental resistance heat kicks in, creating very high winter peak demand. If NYISO becomes winter-peaking, HP demand would drive the coincident peak, increasing the capacity obligation. The cost pool would grow with HP adoption and the cross-subsidy would shrink. This is a feature of the capacity market: it sends a price signal when a new load pattern starts driving the peak.

**Ancillary Services — ~$1–2/MWh. Cross-subsidy: yes (small).** Regulation, operating reserves (spinning + non-spinning), voltage support. Allocated to LSEs by hourly load ratio share. Total ancillary cost per hour is determined by grid conditions and generator bids, not individual consumption. Per-MWh rate falls as the denominator grows.

**Uplift (BPCG) — ~$0.5–1/MWh. Cross-subsidy: yes (tiny).** When NYISO dispatches generators out of merit order for reliability, generators receive Bid Production Cost Guarantee payments socialized by load ratio share. Determined by reliability dispatch decisions, not individual consumption.

**NYISO Schedule 1 (admin) — ~$0.9/MWh. Cross-subsidy: yes (tiny).** NYISO's annual operating budget (~$202M in 2025) for market administration, dispatch, reliability planning. ~$0.92/MWh for load. Classic fixed budget ÷ MWh.

**Working Capital — small. No cross-subsidy.** Cost of capital tied up in purchasing power before billing customers. Scales with the dollar volume of procurement, which scales with kWh.

**Decision.** Add total supply charge to s.r.r. The sub-component decomposition informs the MC 8760 treatment (energy gets zonal LBMPs; capacity needs peak-based allocation) but the entire bundled supply charge enters the supply revenue requirement.

---

### Merchant Function Charge

All seven utilities have an MFC (~0.15–0.30¢/kWh). It recovers costs to administer default supply: procurement staff, working capital, credit & collection, IT/billing. Only default-supply customers pay it; ESCO customers don't. Central Hudson breaks it into three sub-components (Allocation of MFC Lost Revenue, Base MFC Supply, MFC Administration). National Grid has multiple zone-specific components (Working Capital on Purchased Power, Uncollectible Expense, Procurement, Credit & Collection).

**Cross-subsidy analysis: weak.** Unlike purely fixed-budget charges, a significant portion of MFC costs genuinely scales with kWh:

- **Working capital** (~largest component): proportional to procurement dollar volume. More kWh → more dollars tied up → more cost. Scales with load.
- **Credit & collection**: proportional to commodity revenue billed. Also scales with load.
- **Fixed admin overhead**: procurement staff, IT, billing infrastructure. Does NOT scale with kWh. This portion creates a cross-subsidy.

Only the fixed-admin portion creates a cross-subsidy. Total MFC is ~0.15–0.30¢/kWh, so in practice this is a rounding error.

**Decision.** Add to s.r.r. No MC 8760 — treat as residual.

---

### CES Supply Surcharge

Present at ConEd, RG&E, NYSEG, and National Grid. Recovers the cost of RECs (Tier 1), ZECs (Tier 3), and other CES compliance obligations the utility must purchase from NYSERDA as an LSE. RG&E shows it as two fixed rates: REC $0.00103/kWh + ZEC $0.00299/kWh.

**Cross-subsidy analysis: no.** The mechanism has a distinctive two-level structure: (1) NYSERDA sets a statewide $/MWh rate = forecasted net REC costs ÷ forecasted statewide load, updated quarterly; (2) the utility pays NYSERDA = statewide rate × actual MWh served. Each additional MWh consumed literally costs the utility money in REC purchases. Costs and revenues move in lockstep. No overcollection, no redistribution. This is fundamentally different from SBC: SBC has a fixed budget; CES Supply has a per-unit obligation. (There is a tiny statewide effect — NYSERDA's REC contract costs are fixed near-term, so if statewide load increases, the statewide $/MWh rate drops. But this is diluted across all LSEs statewide and negligible for any individual customer.)

**Decision.** Add to s.r.r. MC 8760: yes — flat $/kWh all hours (the obligation is per-MWh regardless of when the MWh occurs).

---

### Tax pass-throughs

**GRT (Gross Receipts Tax) — ConEd.** NY municipalities levy a GRT on utilities, applied as a percentage of each customer's charges (not a flat $/kWh; not a fixed budget). Zone H (Upper Westchester): 3.33% delivery / 1.01% supply; Zone I (Lower Westchester): 5.51% / 3.09%; Zone J (NYC): 4.79% / 2.41%. Self-contained: each customer's GRT = their charges × tax rate. If an HP customer's charges go up, their GRT goes up proportionally, but it doesn't lower anyone else's GRT. No fixed pool, no cross-subsidy.

**PILOTs (Rates to Recover Costs for Cities/Villages) — PSEG-LI.** LIPA makes Payments in Lieu of Taxes (~$346–351M/year, ~8% of operating budget) to municipalities. The bulk is in base delivery rates. This separate line item recovers incremental PILOT obligations for customers in cities and incorporated villages on Long Island. Two fixed per-customer charges: $1.1404/mo + $3.5921/mo (combined ~$4.73/mo). Because these are fixed per-customer (not $/kWh), there is no HP cross-subsidy — doubling consumption doesn't change the PILOT charge. Structurally the safest type of charge from a cross-subsidy perspective.

**Decision.** `exclude_trueup` (tax pass-throughs are not utility cost-of-service and don't belong in the BAT; $/kWh variants get `exclude_trueup`, QUANTITY/% variants get `exclude_percentage` in Phase 0 of the classifier).

---

### Eligibility-gated and optional charges

These charges are $0 for the default residential customer and irrelevant to the general HP cross-subsidy analysis.

**CBC (Customer Benefit Contribution) — All 7.** Solar-only. Monthly charge based on nameplate capacity ($/kW). Part of VDER reforms so solar customers contribute to grid costs. CE: $1.84/kW; LI: $0.0372/kW; RG: $1.3056/kW; NY: $1.1917/kW; NG: $0.97/kW; OR: $1.00/kW; CH: $1.67/kW. If no solar, $0.

**Residential Agricultural Discount (RAD) — NYSEG, RG&E.** Per-kWh credit for qualifying agricultural customers. Gated by `isRadProgramParticipant` / `isAgriculturalCustomer` (default false). $0 for non-agricultural customers. ResStock does not distinguish agricultural customers.

**Low-income discounts — RG&E, O&R, Central Hudson, National Grid.** Bill credits tiered by HEAP eligibility (RG&E), SMI percentage (O&R), electric heat status + income tier (CH), or income-eligible tier (NG). ConEd has EAP discounts but they're not in Genability. All eligibility-gated — $0 for non-qualifying customers. **Relevant to post-BAT LMI analysis** (CAIRO's `utils/post/` LMI discount logic) but not to the charge-by-charge BAT evaluation.

**GreenUp — National Grid.** Voluntary renewable energy rider. Customers who opt in pay a premium per kWh for renewable supply. $0 for non-participants (the default).

**Tax Sur-Credit — ConEd.** Expired. The joint proposal (22-E-0064) eliminates obsolete references. Stale Genability entry with zero/near-zero value.

**Decision.** `exclude_eligibility` for all — $0 for default residential customer; irrelevant to BAT.

---

## Structural notes

### Statewide vs. utility-specific charges

Many charges are mandated statewide by the NY PSC or statute and have the same cross-subsidy properties everywhere:

- **SBC**: Cases 14-M-0094, 18-M-0084. Same fixed-budget/volumetric structure.
- **CES** (delivery + supply): Case 15-E-0302.
- **NY State Surcharge**: PSL §18-a. All utilities must collect; only CE and LI show separate Genability line items.
- **DLM**: Statewide REV proceeding.
- **EV Make Ready**: Case 18-E-0138.
- **Arrears recovery**: Statewide COVID-era program (Case 14-M-0565).
- **CBC**: Statewide VDER reform. Solar-only.
- **RDM**: Statewide policy.

Charges requiring utility-specific research: RAM, EAM, Make-Whole Energy, Recovery Charge, Shoreham, Securitization, PPA, Miscellaneous Charges, and the various delivery/supply adjustment mechanisms that differ from ConEd's MAC.

### PSEG-LI is structurally different

PSEG-LI (Long Island Power Authority) operates under a different regulatory structure. LIPA is a public authority; PSEG-LI is the service provider under contract. Several charges are LI-specific (Shoreham, Securitization, PILOTs). PSEG-LI does not appear to have SBC, CES, or DLM surcharges in Genability — these may be bundled into other charges or handled differently under LIPA's governance.

### Supply charge decomposition generalizes to all NYISO utilities

All seven utilities buy power from the NYISO wholesale market. The same cost components (energy LBMP, capacity ICAP/UCAP, ancillary services, uplift, Schedule 1 admin) flow through every utility's supply charge. The ConEd MSC decomposition and its cross-subsidy conclusions (energy = no cross-subsidy; capacity = yes; ancillary/uplift/admin = yes but small) apply to all NY utilities. The difference is presentation: ConEd breaks out MSC I (energy true-up) and MSC II (capacity/ancillary true-up) separately; other utilities may bundle differently.

### National Grid's Delivery Charge Adjustment is a Genability umbrella

In Genability, National Grid's "Delivery Charge Adjustment" (riderId 801) bundles two distinct things per zone (12 sub-rates across 6 zones): a zone-specific delivery rate adjustment (DISTRIBUTION) and the zone-specific default supply charge (SUPPLY,CONTRACTED). This mirrors National Grid's tariff book, which presents both in one rider statement. No separate BAT action needed — the delivery portion is already in d.r.r. (it IS the zone-specific delivery rate) and the supply portion is the main supply charge (add to s.r.r.).

---

## Genability data quirks and pitfalls

Lessons learned from the 2025-01-01 effective date period. These apply to the Genability API generally but were discovered in the NY context.

### tariffRateId is version-specific, not persistent

A `tariffRateId` uniquely identifies a rate **within a specific tariff version**. When a tariff is updated (new effective date, rate case update, rider refresh), the tariffRateIds change — old IDs become invalid. This means `charge_decisions.json` files keyed by `tariffRateId` will break silently whenever the underlying Genability tariff is refreshed: lookup misses, entries disappear from monthly rates, revenue requirements shrink.

**Stable identifiers to prefer:**

- `masterTariffRateId` — persistent across tariff versions for the same conceptual rate. Requires `fields=ext` in the API call (not returned by default).
- `variableRateKey` — stable string identifier for variable/lookup rates. Present only when the rate uses Genability's variable rate system (supply charges, some surcharges).
- `rateName` — human-readable; generally stable across versions but can change with tariff redesigns or Genability re-labeling.

**Recommendation:** The discovery → classify workflow (`fetch_monthly_rates.py --discover` then `classify_charges.py`) uses `tariffRateId` as the JSON key (because the API returns it), but classification rules should match on `rate_name`, `variable_rate_key`, and `charge_class` — not on the tariffRateId itself. This makes classifications portable across tariff versions.

### SC1 placeholder stubs (riderId entries)

When the Genability API resolves rider tariffs (e.g., EV Make Ready, CBC), it returns two entries per rider rate: a **resolved entry** (with actual rate data, tagged with `riderTariffId`) and an **unresolved placeholder stub** (tagged with `riderId`, no rate data, and a rate name suffixed "- SC1"). The placeholder is an API artifact — a reference to the parent tariff's slot where the rider is plugged in.

Example: EV Make Ready Surcharge appears as:

- `tariffRateId=20041688`: "Electric Vehicle Make Ready Surcharge" (resolved, `riderTariffId=3463699`) — **keep this**
- A separate entry: "Electric Vehicle Make Ready Surcharge - SC1" (unresolved, `riderId=3463699`) — **discard this**

`fetch_monthly_rates.py --discover` filters these out by checking `r.get("riderId")` and skipping entries with it. If placeholders slip through, they create shadow entries that duplicate resolved riders.

### Zonal duplicates and overcounting risk (2025 period)

Three NY utilities have zone- or territory-specific rates that appear as separate entries in Genability. If all zone variants are included in `add_to_srr` or `add_to_drr`, the revenue requirement is inflated by 2–6× for those charges. The `exclude_zonal` decision marks non-representative zone entries so they are excluded from revenue calculations but remain distinguishable from entries excluded for substantive reasons.

**ConEd — 3 NYISO zones (H, I, J):**

| Charge           | Zone H (representative) | Zone I                 | Zone J                 |
| ---------------- | ----------------------- | ---------------------- | ---------------------- |
| MSC Rate         | `add_to_srr`            | `exclude_zonal`        | `exclude_zonal`        |
| MSC I Adjustment | `exclude_trueup`        | `exclude_trueup`       | `exclude_trueup`       |
| GRT Distribution | `exclude_trueup` (tax)  | `exclude_trueup` (tax) | `exclude_trueup` (tax) |
| GRT Supply       | `exclude_trueup` (tax)  | `exclude_trueup` (tax) | `exclude_trueup` (tax) |

Zone H is the representative zone for ConEd supply. The MSC I Adjustments and GRT entries are excluded for substantive reasons regardless of zone, so they don't need `exclude_zonal`. Zonal entries are identifiable by `rate_name` containing "Zone H/I/J".

**National Grid (NiMo) — 6 zones (Adirondack, Capital, Central, Frontier, Genesee, Utica):**

| Charge family                           | Central (representative) | Other 5 zones    |
| --------------------------------------- | ------------------------ | ---------------- |
| Electric Supply Charge (rider 801)      | `add_to_srr`             | `exclude_zonal`  |
| Delivery Charge Adjustment (rider 801)  | `already_in_drr`         | `exclude_zonal`  |
| Electricity Supply Reconciliation (ESR) | `exclude_trueup`         | `exclude_trueup` |

Central is the representative zone because it is NiMo's largest service territory in the NY residential population. Rider 801 entries are identifiable by zone name in `rate_name` (e.g., "Electric Supply Charge - Central Zone"). ESR entries are excluded for substantive reasons (supply cost true-up) regardless of zone.

The VRK pattern for NiMo zonal entries is: `electricSupplyChargeSC1{Zone}` (supply), `electricSupplyChargeSc1DeliveryAdj{Zone}` (delivery adj), `esrMechanismSC1{Zone}` (ESR).

**NYSEG — 3 territories (Regular, West, Lower Hudson Valley):**

| Charge                | Regular (representative) | West            | LHV             |
| --------------------- | ------------------------ | --------------- | --------------- |
| Supply Service Charge | `add_to_srr`             | `exclude_zonal` | `exclude_zonal` |

Regular is the representative territory for NYSEG. Unlike ConEd and NiMo, NYSEG's zonal entries are **not** distinguishable by `rate_name` — all three are named "Supply Service Charge". They can only be distinguished by `variable_rate_key`: `supplyChargeResidentialRegularSchedule1` (Regular), `supplyChargeSC1West` (West), `supplyChargeSC1LHV` (LHV).

**CenHud, O&R, PSEG-LI, RG&E** do not have zone-specific supply entries in Genability.

### National Grid MFC overlap duplicates (2025 period)

National Grid's Merchant Function Charge rider (tariffId 3445398) contains 27 sub-entries in the 2025 period. Many of these share `variableRateKey` values with rates that already appear elsewhere in the tariff (supply commodity or ESR), creating a double-counting risk.

**Category 1: MFC entries with supply commodity VRKs.** Six "Working Capital on Purchased Power Costs Factor - Supply Charge" entries and six "Electricity Supply Uncollectible Expense Factor - Supply Charge" entries. Each has a VRK like `electricSupplyChargeSC1{Zone}`, identical to the main supply commodity entries from rider 801. Sample rates are also identical — Genability is echoing the underlying supply rate into the MFC sub-entry. **Decision: `exclude_zonal` for all 12** (VRK overlap with supply commodity rates).

**Category 2: MFC entries with ESR VRKs.** Six "Working Capital on ... ESRM" and six "Uncollectible Expense ... ESRM" entries, with VRKs like `esrMechanismSC1{Zone}`. These overlap with the base ESR entries. **Decision:** Keep Working Capital ESRM for Central zone (`add_to_srr`); `exclude_zonal` for other 5 zones; `exclude_zonal` Uncollectible Expense ESRM Central (duplicate VRK of Working Capital); `exclude_zonal` for other 5 zones.

**Category 3: MFC entry with CES Supply VRK.** One "Electricity Supply Uncollectible Expense Factor - CESS" entry with VRK `cleanEnergyStandardSupply579`, matching the CES Supply base rate. **Decision: `exclude_zonal`** (VRK overlap with CES Supply).

**Category 4: Non-overlapping MFC entries.** "Electricity Supply Procurement Charge" (VRK `merchantFunction...Procurement`) and "Electricity Supply Credit and Collection Charge" (VRK `merchantFunction...CreditCollection`) are genuine, unique MFC sub-components with their own VRKs. **Decision: `add_to_srr`.**

The pattern: any NiMo MFC entry whose `variable_rate_key` starts with `electricSupplyChargeSC1`, `esrMechanismSC1`, or `cleanEnergyStandardSupply` is an overlap duplicate and should be `exclude_zonal` (both VRK-overlap entries and non-representative zones of ESR entries use this decision).

---

## Genability charge name variants by utility (2025 period)

Future agents classifying charges need to know the various names Genability uses for the same conceptual charge across utilities. These were observed for the `effectiveOn=2025-01-01` period.

### Supply commodity

| Utility | rate_name                                           | variable_rate_key pattern                                  |
| ------- | --------------------------------------------------- | ---------------------------------------------------------- |
| CE      | "MSC Rate - Zone H/I/J"                             | `marketSupplyChargeResidentialZone{H,I,J}`                 |
| LI      | "Power Supply Charge - Summer/Winter Peak/Off-Peak" | (none — fixed rates)                                       |
| NY      | "Supply Service Charge"                             | `supplyCharge{ResidentialRegularSchedule1,SC1West,SC1LHV}` |
| NG      | "Electri(city/c) Supply Charge - {Zone} Zone"       | `electricSupplyChargeSC1{Zone}`                            |
| OR      | "Market Supply Charge"                              | `marketSupplyChargeResidential1`                           |
| CH      | "Market Price Charge"                               | `marketPriceCharge`                                        |
| RG      | "Supply Charge"                                     | `SupplySc1Residential`                                     |

### CES delivery

| Utility | rate_name                                                                                   |
| ------- | ------------------------------------------------------------------------------------------- |
| CE      | "Clean Energy Standard Delivery Surcharge"                                                  |
| NY      | "Clean Energy Standard Surcharge" (no "Delivery" — charge_class=DISTRIBUTION disambiguates) |
| NG      | "Clean Energy Standard Delivery Charge"                                                     |

### CES supply

| Utility | rate_name(s)                                                              |
| ------- | ------------------------------------------------------------------------- |
| CE      | "Clean Energy Standard Supply Surcharge"                                  |
| NG      | "Clean Energy Standard Supply Charge"                                     |
| RG      | "Renewable Energy Credit" + "Zero Emission Credit" (two separate entries) |

### Low-income discounts

| Utility | rate_name                              |
| ------- | -------------------------------------- |
| CE      | (not in Genability default tariff)     |
| CH      | "Low Income Discount"                  |
| NG      | "Income Eligible Basic Service Credit" |
| OR      | "Low Income Bill Credit"               |
| RG      | "Low Income Program Discount"          |

### Arrears / COVID forgiveness

| Utility | rate_name(s)                                                               |
| ------- | -------------------------------------------------------------------------- |
| CE      | "Arrears Management Program Recovery Surcharge"                            |
| NG      | "Arrears Management Program Recovery Surcharge - Phase 1", "... - Phase 2" |
| NY      | "Arrears Relief Program - Phase 1", "... - Phase 2"                        |

### Late payment / waived fees

| Utility | rate_name                                             |
| ------- | ----------------------------------------------------- |
| NY      | "Late Payment Charge and Other Waived Fees Surcharge" |
| NG      | "Late Payment Charge & Other"                         |

### Transition / restructuring

| Utility | rate_name                                                                                      |
| ------- | ---------------------------------------------------------------------------------------------- |
| CE      | "Transition Adjustment"                                                                        |
| CH      | "Transition Adjustment"                                                                        |
| NG      | "Legacy Transition Charge"                                                                     |
| NY      | "Transition Charge Statement"                                                                  |
| OR      | "Transition Adjustment for Competitive Services" (may appear 2×: full-service + retail access) |
| RG      | "Transition Charge"                                                                            |

### Miscellaneous and utility-specific

- **CH**: "Miscellaneous Charges" (note: trailing non-breaking space `\u00a0` in Genability data)
- **CH**: "Purchased Power Adjustment" (also has trailing `\u00a0`), "Electric Bill Credit" (trailing `\u00a0`)
- **NG**: "Net Utility Plant and Depreciation Expense Reconciliation Mechanism"
- **NG**: "GreenUp Charge" (optional rider)
- **NY**: "Make-Whole Energy Charge", "Reliability Support Service Charge" (rate=$0, inactive/placeholder)
- **RG**: "Make-Whole Energy" (no "Charge" suffix)
- **LI**: "Costumer Benefit Contribution" (typo — "Costumer" not "Customer")
- **LI**: "Shoreham Property Tax (SPT) Settlement Factors" (two entries: Suffolk County + Outside Suffolk at $0)
- **LI**: "Securitization Charge", "Securitization Offset Charge"
- **LI**: "Distributed Energy Resources Cost Recovery Rate"
- **LI**: "Rate for Cities and Incorporated Villages - Transportation/Commodity" (PILOTs)
- **LI**: "New York State Assessment" (CE calls it "New York State Surcharge")
- **NY**: "Residential Agricultural Discount" (VRK `residentialAgriculturalDiscount562`)
- **RG**: "Residential Agricultural Discount" (VRK `residentialAgriculturalDiscount1007`)

---

## Rate structures across utilities

NY utilities have three delivery rate structures, which the `monthly_rates` YAML
represents using a `rate_structure` discriminator on each decision section.

### `flat` (NiMo, NYSEG, RG&E, CenHud)

Single-band, non-seasonal delivery rate. All charges have a simple
`monthly_rates: {month: rate}` shape.

### `seasonal_tiered` (ConEd, O&R)

Two-tier, seasonal delivery rate. The first 250 kWh per month are billed at
one rate; usage above 250 kWh at a higher rate. The summer second-tier
premium is the seasonal component:

| Utility | Season | Tier 1 (≤250 kWh) | Tier 2 (>250 kWh) |
| ------- | ------ | ----------------- | ----------------- |
| ConEd   | Summer | \$0.16107/kWh     | \$0.18518/kWh     |
| ConEd   | Winter | \$0.16107/kWh     | \$0.16107/kWh     |
| O&R     | Summer | \$0.10409/kWh     | \$0.13018/kWh     |
| O&R     | Winter | \$0.10409/kWh     | \$0.10409/kWh     |

(Rates as of Jan 2025 snapshot.)

In the YAML, these appear as:

```yaml
already_in_drr:
  rate_structure: seasonal_tiered
  seasons:
    summer: {from_month: 6, from_day: 1, to_month: 9, to_day: 30}
    winter: {from_month: 10, from_day: 1, to_month: 5, to_day: 31}
  charges:
    core_delivery_rate:
      charge_unit: $/kWh
      tiers:
        - upper_limit_kwh: 250.0
          monthly_rates:
            summer: {2025-06: 0.16107, ...}
            winter: {2025-01: 0.16107, ...}
        - upper_limit_kwh: null
          monthly_rates:
            summer: {2025-06: 0.18518, ...}
            winter: {2025-01: 0.16107, ...}
```

### `seasonal_tou` (PSEG-LI)

Seasonal time-of-use rates for both delivery and supply. Four rate periods:
summer on-peak, summer off-peak, winter on-peak, winter off-peak. On-peak is
weekdays 3–7 PM.

PSEG-LI is the only utility where supply charges also have a TOU structure.
Its `add_to_srr` section uses `rate_structure: seasonal_tou`, with the supply
commodity charge having 4 period-specific monthly rate series. `compute_rr.py`
skips non-flat `add_to_srr` sections and relies on the
`supply_base_overrides.yaml` mechanism (estimated from LIPA budget data via
`estimate_psegli_rr.py`).

---

## Updating charge_decisions for a new period

When tariffs are refreshed (new effective date), charge_decisions must be regenerated because `tariffRateId`s change. The workflow:

1. **Discover** — Run `just s ny discover-charges effective_date=YYYY-MM-DD` for each utility. This calls `fetch_monthly_rates.py --discover`, which queries the Genability API for all rates active on that date and writes a `*_discovered.json` with null decisions but rich metadata (rate_name, charge_class, variable_rate_key, master_tariff_rate_id, sample_rate, etc.).

2. **Classify** — Run `just s ny classify-charges` for each utility. This calls `classify_charges.py`, which applies ~40 regex rules on rate_name, plus utility-specific zonal dedup and NiMo MFC overlap logic, to produce hydrated `*_charge_decisions.json` files.

3. **Verify** — Spot-check the output. The script warns about any unclassified entries. Pay attention to:
   - New charges that don't match any regex (the PSC adds new surcharges periodically)
   - Changes to zonal structure (new zones, renamed zones, merged zones)
   - Changes to NiMo MFC sub-entries (new overlap patterns, removed entries)
   - Changes to charge names (Genability may relabel charges when tariffs update)

4. **Fetch monthly rates** — Run the standard `fetch-monthly-rates` recipe to populate the monthly YAML files using the new charge_decisions.

The discovered JSON files are kept as intermediate artifacts alongside the charge_decisions files. The old charge_decisions (renamed `*_charge_decisions_old.json`) can be deleted once the new ones are verified.

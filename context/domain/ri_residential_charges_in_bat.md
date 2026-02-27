# RI Residential Electric Charges: Cross-Subsidy Analysis for BAT

Every charge on a Rhode Island default residential electric bill — Rhode Island Energy (RIE), tariff A-16 — evaluated for whether it belongs in CAIRO's BAT and bill calculations, and whether it creates a cross-subsidy between heat pump (HP) and non-HP customers. Rhode Island has a single major electric distribution utility (RIE, formerly National Grid – Rhode Island); A-16 is the default residential rate. Regulation is by the Rhode Island Public Utilities Commission (RI PUC). Default supply is Last Resort Service (LRS), procured via competitive solicitations; wholesale costs flow from ISO-NE.

**Data sources:** Genability tariff JSON at `rate_design/hp_rates/ri/config/tariffs/electric/genability/rie_default_2025-01-01.json`; RI PUC compliance filings (e.g. Dockets 25-03-EL, 25-04-EL, 25-05-EL); [tariff-fetch RIE residential A-16 wiki](https://github.com/switchbox-data/tariff_fetch/blob/main/docs/wiki/utilities/rie/residential-a16/index.md).

---

## Charge type taxonomy

Charges are classified into families based on their economic structure, not their tariff name.

| Type                       | What it is                                                                          | Cross-subsidy?                             | Decision pattern            |
| -------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------ | --------------------------- |
| **Base delivery**          | Rates set in the rate case that collect the delivery revenue requirement            | Yes (the BAT's core subject)               | Already in d.r.r.           |
| **Cost reconciliation**    | Uniform $/kWh true-ups of costs already embedded in base rates (delivery or supply) | No — shifts all bills equally              | Exclude                     |
| **Revenue true-up**        | Revenue decoupling and temporary over/under-collection corrections                  | No — shifts all bills equally              | Exclude                     |
| **Program surcharge**      | Fixed state-mandated program budgets recovered via uniform $/kWh or $/mo            | Yes — fixed pool ÷ kWh (or ÷ customers)    | Add to d.r.r.               |
| **Sunk-cost recovery**     | Fixed debt, storm fund, or settlement pools recovered via $/kWh                     | Yes — fixed pool ÷ kWh                     | Add to d.r.r.               |
| **DER credit recovery**    | Fixed net metering / RE Growth program costs recovered via uniform $/kWh            | Yes — fixed pool ÷ kWh                     | Add to d.r.r.               |
| **LMI cost recovery**      | Recovery from non-LMI customers of low-income discount costs                        | Yes — but income transfer, not rate design | Exclude (LMI; post-process) |
| **Performance incentive**  | PUC-approved performance incentive (e.g. EE program performance)                    | Yes in structure, negligible in magnitude  | Exclude (negligible)        |
| **Supply commodity**       | LRS (Standard Offer) bundling ISO-NE wholesale energy, capacity, ancillary, admin   | Mixed — see sub-components                 | Add to s.r.r.               |
| **Merchant function**      | LRS administrative cost adjustment (procurement, working capital)                   | Weak                                       | Add to s.r.r.               |
| **RES supply**             | Per-MWh REC obligation (Renewable Energy Standard); cost scales with load           | No                                         | Add to s.r.r. + MC 8760     |
| **Tax pass-through**       | Gross Earnings Tax (% of bill)                                                      | No                                         | Exclude (tax)               |
| **Eligibility / optional** | A-60 low-income discount; $0 for default A-16 customer                              | N/A                                        | Skip for BAT (use in LMI)   |

---

## Summary table

The Standard Offer Service (LRS) charge bundles several ISO-NE cost components. We decompose it here because the sub-components have different cross-subsidy properties. Rates below reflect the Genability tariff and April 2025 RI PUC compliance filing where noted; many factors use lookups (variable rates).

| Charge                                        | Type                  | Unit             | In rev req?   | Fixed budget?        | HP cross-subsidy?   | Why                                                                            | Decision                    | MC 8760?               |
| --------------------------------------------- | --------------------- | ---------------- | ------------- | -------------------- | ------------------- | ------------------------------------------------------------------------------ | --------------------------- | ---------------------- |
| **Customer Charge**                           | Base delivery         | $/mo             | Yes           | —                    | Yes                 | Base rate; part of tariff                                                      | Already in d.r.r.           | No—residual            |
| **Distribution Charge**                       | Base delivery         | $/kWh            | Yes           | —                    | Yes                 | Rate CAIRO calibrates                                                          | Already in d.r.r.           | Yes—sub-tx + dx MCs    |
| **Operating & Maintenance Exp Charge**        | Base delivery         | $/kWh            | Yes           | —                    | Yes                 | O&M in rate case                                                               | Already in d.r.r.           | Yes—sub-tx + dx MCs    |
| **CapEx Factor Charge**                       | Base delivery         | $/kWh            | Yes           | —                    | Yes                 | ISR plan capital recovery                                                      | Already in d.r.r.           | Yes—sub-tx + dx MCs    |
| **Pension Adjustment Factor**                 | Base delivery         | $/kWh (credit)   | Yes           | —                    | Yes                 | Pension/benefit cost recovery                                                  | Already in d.r.r.           | No—residual            |
| **Transmission Charge**                       | Base delivery         | $/kWh            | Yes           | —                    | Yes                 | FERC/ISO-NE OATT pass-through                                                  | Already in d.r.r.           | Yes—transmission MCs   |
| **Minimum Charge**                            | Base delivery         | $/mo (floor)     | Yes           | —                    | N/A                 | Bill floor; equals customer charge                                             | N/A                         | —                      |
| **O&M Reconciliation Factor**                 | Cost recon            | $/kWh            | N/A (true-up) | —                    | No                  | Uniform $/kWh; true-up noise                                                   | Exclude                     | —                      |
| **CapEx Reconciliation Factor**               | Cost recon            | $/kWh            | N/A (true-up) | —                    | No                  | Capital spending vs ISR forecast                                               | Exclude                     | —                      |
| **RDM Adjustment Factor**                     | Revenue true-up       | $/kWh            | N/A (revenue) | —                    | No                  | Load forecast error; decoupling true-up                                        | Exclude                     | —                      |
| **Energy Efficiency Programs Charge**         | Program surcharge     | $/kWh            | **No**        | Yes                  | **Yes**             | Fixed EE budget (LCP) ÷ kWh                                                    | Add to d.r.r.               | No—residual            |
| **Net Metering Charge**                       | DER credit recovery   | $/kWh            | **No**        | Yes                  | **Yes**             | Fixed net metering cost recovery ÷ kWh                                         | Add to d.r.r.               | No—residual            |
| **Long Term Contracting Charge**              | Program surcharge     | $/kWh            | **No**        | Yes (contracts)      | **Yes**             | Long-term renewable PPAs (e.g. offshore wind) ÷ kWh                            | Add to d.r.r.               | No—residual            |
| **RE Growth Charge**                          | Program surcharge     | $/mo (or lookup) | **No**        | Yes                  | **Yes**             | RE Growth Program (distributed gen) ÷ customers                                | Add to d.r.r.               | No—residual            |
| **LIHEAP Enhancement Charge**                 | Program surcharge     | $/mo             | **No**        | Yes (capped)         | **No**              | Fixed per-customer; cap $10/yr                                                 | Add to d.r.r.               | No—residual            |
| **Storm Fund Replenishment Factor**           | Sunk-cost recovery    | $/kWh            | **No**        | Yes (replenishment)  | **Yes**             | Storm contingency fund ÷ kWh                                                   | Add to d.r.r.               | No—residual            |
| **Arrearage Management Adjustment Factor**    | Sunk-cost recovery    | $/kWh            | **No**        | Yes (finite)         | **Yes** (temporary) | Arrearage forgiveness / AMP cost recovery ÷ kWh                                | Add to d.r.r.               | No—residual            |
| **Transition Charge**                         | Cost recon            | $/kWh            | N/A (true-up) | —                    | No                  | Legacy restructuring; balance paid down                                        | Exclude                     | —                      |
| **Low-Income Discount Recovery Factor**       | LMI cost recovery     | $/kWh            | **No**        | Yes                  | **Yes**             | A-60 discount cost from all; exclude so BAT isolates rate-design cross-subsidy | Exclude (LMI; post-process) | —                      |
| **Performance Incentive Factor**              | Performance incentive | $/kWh            | **No**        | Yes (once earned)    | **Negligible**      | EE performance bonus; small magnitude                                          | Exclude (negligible)        | —                      |
| **Last Resort Adjustment Factor**             | Cost recon / supply   | $/kWh            | N/A (true-up) | —                    | No                  | LRS cost reconciliation                                                        | Exclude                     | —                      |
| **Supply commodity (LRS bundled)**            | Supply commodity      | $/kWh            | **No**        | Mixed                | Mixed               | Energy + capacity + ancillary bundled                                          | Add to s.r.r.               | See sub-components     |
| **↳ LRS: Energy (LMP)**                       | Supply sub-component  | $/kWh            | **No**        | **No**               | **No**              | True marginal cost; scales 1:1                                                 | (in s.r.r.)                 | No—residual            |
| **↳ LRS: Capacity (FCM)**                     | Supply sub-component  | $/kWh (embedded) | **No**        | Yes (FCM obligation) | **Yes**             | Peak-determined CLO; volumetric recovery                                       | (in s.r.r.)                 | Yes—zonal/peak         |
| **↳ LRS: Ancillary / ISO-NE**                 | Supply sub-component  | $/kWh (embedded) | **No**        | Mostly               | **Yes** (small)     | Reserves, regulation, admin ÷ load                                             | (in s.r.r.)                 | Yes / No—residual      |
| **↳ LRS: Working capital / admin**            | Supply sub-component  | $/kWh (embedded) | **No**        | **No**               | **No**              | Scales with procurement volume                                                 | (in s.r.r.)                 | No—residual            |
| **LRS Administrative Cost Adjustment Factor** | Merchant function     | $/kWh            | **No**        | Mixed                | **Weak**            | Procurement, admin; part scales with load                                      | Add to s.r.r.               | No—residual            |
| **Renewable Standard Energy Charge**          | RES supply            | $/kWh            | **No**        | **No**               | **No**              | Per-MWh REC obligation; cost scales with load                                  | Add to s.r.r.               | Yes—flat $/kWh all hrs |
| **Gross Earnings Tax**                        | Tax pass-through      | % of charges     | **No**        | **No**               | **No**              | % of own bill; no fixed pool                                                   | Exclude (tax)               | —                      |

---

## The generalized cross-subsidy: fixed cost pools recovered volumetrically

The BAT operates on the delivery revenue requirement: a fixed cost pool recovered through a tariff structure that may not match cost causation. Marginal cost allocation plus residual allocation under a chosen allocator defines what each customer _should_ pay; the difference from what they _do_ pay is the cross-subsidy.

As in New York, many charges outside the delivery revenue requirement on the RI bill share the same structure: a **fixed cost pool recovered volumetrically** ($/kWh). Examples: Energy Efficiency Programs Charge (fixed EE budget ÷ kWh), Net Metering Charge (fixed DER credit recovery ÷ kWh), RE Growth Charge (fixed program ÷ customers or kWh), Storm Fund Replenishment, Arrearage Management. (We exclude **Low-Income Discount Recovery** from the BAT so it doesn't distort the rate-design cross-subsidy signal; LMI is applied in post-processing of bills — see [cairo_lmi_and_bat_analysis.md](../tools/cairo_lmi_and_bat_analysis.md).) For each included charge, if the pool is fixed and recovery is $/kWh, then HP customers' higher kWh mechanically reduce the per-kWh burden for others — the same wealth-transfer dynamic as the core delivery revenue requirement.

The same integration approach applies: **top up the revenue requirement** (delivery or supply as appropriate) with these charges so the BAT can allocate them under the chosen residual allocator (peak, per-customer, or volumetric). See the NY analysis document for the full worked example (System Benefits Charge). Under peak or per-customer allocation, topping up reveals HP overpayment for these riders; under volumetric allocation it is a no-op.

---

## Charge-by-charge analysis

### Base delivery rates

These charges define the tariff structure that collects the delivery revenue requirement. CAIRO should model all of them.

**Customer Charge.** Rhode Island Energy's A-16 customer charge is **$6.00/month** (set in RI PUC rate cases). It covers account-level costs: metering, billing systems, customer service. One of the lower customer charges in the region. Fixed charges are load-shape-insensitive.

**Distribution Charge.** The main volumetric delivery rate: **$0.0458/kWh** (effective April 2025 per compliance filing). Recovers the cost of local distribution (wires, poles, transformers). Flat rate; applies to every kWh. This is the primary rate CAIRO calibrates in precalc mode.

**Operating & Maintenance Exp Charge.** **$0.00223/kWh.** Recovers day-to-day O&M for the distribution system. Set in the rate case; part of the delivery revenue requirement. Functionally part of base delivery; same marginal cost logic applies as for the distribution charge.

**CapEx Factor Charge.** **$0.00832/kWh** (April 2025). Recovers capital expenditure for the distribution system under Rhode Island's **Infrastructure, Safety, and Reliability (ISR)** plan. RIE files annual Electric ISR Plans with the RI PUC; the plan covers capital investment, vegetation management O&M, and other reliability spending. The CapEx factor is the revenue requirement component for approved ISR capital. Functionally part of base delivery; same marginal costs apply.

**Pension Adjustment Factor.** **$0.00339/kWh** as a **credit** (negative charge) in the Genability tariff; compliance filing showed ($0.00274)/kWh. Recovers pension and benefit costs for distribution employees; the credit reflects reconciliation of actual vs. forecast pension costs (e.g. Docket 25-10-EL). Part of labor-related cost recovery in the rate case. Treat as part of base delivery.

**Transmission Charge.** **$0.04773/kWh** (April 2025; tariff shows Base Transmission + Transmission Adjustment Factor + Transmission Uncollectible Factor). Recovers cost of high-voltage transmission (FERC-regulated, ISO-NE Open Access Transmission Tariff). Pass-through of Regional Network Service (RNS) and related OATT charges to distribution customers. Set by FERC/ISO-NE; RIE passes through. Same transmission marginal cost logic as in the BAT (transmission MC 8760).

**Minimum Charge.** **$6.00/month** — equals the customer charge. Bill floor for very low usage or net-generation months. Rarely binds for typical residential consumption. Not a separate cost for cross-subsidy purposes.

**Decision.** All base delivery rates are already in d.r.r. They define the tariff. Sub-transmission and distribution marginal costs (MC 8760) apply to the volumetric delivery components; transmission MCs apply to the transmission charge.

---

### Cost reconciliation

**O&M Reconciliation Factor.** Variable (lookup). True-up of actual O&M costs vs. the amount collected under the fixed O&M charge. Small surcharge or credit; uniform $/kWh. Same exclude logic as NY: uniform true-up of costs already in base rates; does not change cross-subsidy rankings.

**CapEx Reconciliation Factor.** Variable (lookup). True-up of actual capital spending vs. ISR/rate-case forecast. Ensures allowed CapEx recovery matches actual spending. Uniform $/kWh. Exclude.

**Transition Charge.** **$0.00001/kWh** (minimal; Genability and filing). Recovers legacy restructuring costs under Rhode Island's Utility Restructuring Act of 1996 (stranded costs, regulatory assets, above-market power contracts). Non-bypassable; applied to all distribution users. As the remaining balance is paid down, the rate is effectively a small true-up. Exclude for the same reason as other cost reconciliation: uniform $/kWh, backwards-looking balance recovery.

**Last Resort Adjustment Factor.** Variable (often $0 or small credit, e.g. ($0.00355)/kWh in filing). Reconciles LRS (Standard Offer) actual costs vs. the base LRS rate. Supply-side cost true-up. Exclude.

**Decision.** Exclude all cost reconciliation and the transition charge. Uniform $/kWh true-ups; no change to BAT cross-subsidy rankings.

---

### Revenue true-up

**RDM Adjustment Factor.** Variable (lookup; e.g. $0.00123/kWh in compliance filing). Rhode Island's **Revenue Decoupling Mechanism (RDM)** reconciles the difference between the utility's target delivery revenue and actual billed distribution revenue. The adjustment is allocated by rate class and forecasted kWh to produce a per-kWh factor. Over-recovery → credit; under-recovery → surcharge. Same as NY: revenue true-up is noise, not structural rate design. Exclude.

---

### Program surcharges and DER / LMI recovery

**Energy Efficiency Programs Charge.** Variable (lookup; e.g. $0.01098/kWh in April 2025 filing). Recovers the cost of Rhode Island's **Least Cost Procurement (LCP)** energy efficiency programs (2006 Act). EE is the "first fuel"; the utility must invest in cost-effective efficiency before additional supply. Budget is set in annual EE plans (e.g. Docket 24-39-EE); recovery is via uniform $/kWh. Fixed program budget ÷ kWh → HP cross-subsidy. **Add to d.r.r.** No MC 8760 — residual.

**Net Metering Charge.** **$0.01457/kWh** (April 2025). Recovers the cost of net metering programs (solar and other DER). Spread across all customers. Fixed cost pool (DER credits paid) ÷ kWh → same cross-subsidy structure as NY's VDER/DER cost recovery. **Add to d.r.r.** No MC 8760 — residual.

**Long Term Contracting Charge.** Variable (lookup; e.g. $0.00656/kWh April 2025). Recovers costs of long-term renewable PPAs under the **Long-Term Contracting Standard** (R.I. Gen. Laws Ch. 39-26.1) and **Affordable Clean Energy Security Act** (Ch. 39-31) — e.g. offshore wind (Revolution Wind). Fixed contract payments ÷ kWh. **Add to d.r.r.** No MC 8760 — residual.

**RE Growth Charge.** Variable (lookup); often shown as a **fixed $/mo** on bills (e.g. $5.75 in April 2025 for A-16). Supports the **Renewable Energy Growth Program** — distributed generation (solar, wind, etc.) under long-term tariffs with performance-based incentives. Cost recovery is a fixed program amount; on bills it can appear as $/mo or embedded. Fixed pool → if recovered volumetrically elsewhere, HP cross-subsidy; if strictly $/mo per customer, no volumetric HP cross-subsidy. Genability shows FIXED_PRICE (monthly). **Add to d.r.r.** No MC 8760 — residual. (If in practice some recovery is $/kWh, the fixed-pool logic applies.)

**LIHEAP Enhancement Charge.** **$0.79/month** (fixed per customer in filing). Supplements federal LIHEAP funding for low-income households. Capped by law at $10/year per customer (R.I. Gen. Laws § 39-1-27.12). Fixed per-customer → no HP cross-subsidy from volumetric recovery. **Add to d.r.r.** for completeness; structurally no volumetric cross-subsidy. No MC 8760.

**Low-Income Discount Recovery Factor.** **$0.00251/kWh** (A-16; A-60 customers do not pay this factor). Recovers the cost of the A-60 low-income discount from **all other customers**. Fixed discount cost pool ÷ kWh of non-A-60 customers. We **exclude** this from the BAT so the BAT isolates rate-design cross-subsidization (HP vs. non-HP) and is not distorted by the intentional income-based transfer. LMI discounts and this recovery are applied in post-processing of bills; see [cairo_lmi_and_bat_analysis.md](../tools/cairo_lmi_and_bat_analysis.md).

**Decision.** Add all program surcharges and DER recovery to d.r.r. Exclude LMI cost recovery from BAT; include in bill post-processing. No MC 8760 for the added riders.

---

### Sunk-cost recovery

**Storm Fund Replenishment Factor.** **$0.00788/kWh** (April 2025; Genability may show $0 as placeholder). Replenishes the **Storm Contingency Fund** (PUC Order No. 15360). After qualifying storms, RIE files reports (e.g. Docket 2509); incremental restoration costs are recovered through this factor. Fixed replenishment amount ÷ kWh → HP cross-subsidy. **Add to d.r.r.** No MC 8760 — residual.

**Arrearage Management Adjustment Factor.** **$0.00006/kWh** (Genability); filing showed $0.00009. Recovers costs of arrearage management and forgiveness programs (e.g. COVID-related forgiveness of ~$43.5M for low-income/protected customers, Docket 22-08-GE; ongoing Arrearage Management Program (AMP)). Finite pool ÷ kWh → temporary cross-subsidy. **Add to d.r.r.** No MC 8760 — residual.

**Decision.** Add both to d.r.r. No MC 8760.

---

### Performance incentive

**Performance Incentive Factor.** Variable (often $0). Ties delivery rates to performance metrics (e.g. reliability, customer service, EE program performance). When non-zero, it is a small utility bonus recovered via $/kWh. Same as NY EAM: structurally a fixed pool ÷ kWh but negligible magnitude. **Exclude (negligible).**

---

### Supply commodity (LRS) and ISO-NE decomposition

Rhode Island Energy procures default supply through **Last Resort Service (LRS)** — competitive solicitations (quarterly auctions); mix is largely Fixed Price Full Requirements (FPFR) load-following plus a portion of spot. **RIE retains capacity obligation**; winning suppliers typically cover energy, ancillary, and ISO-NE charges for the load. The **Standard Offer Service Charge** (LRS base + LRS Adjustment + LRS Administrative Cost Adjustment) and the **Renewable Standard Energy Charge** together form the supply side on the bill.

**LRS sub-component decomposition (ISO-NE):**

- **Energy (LMP).** Wholesale energy cost from ISO-NE real-time and day-ahead markets. True marginal cost; scales 1:1 with consumption. **No cross-subsidy.**
- **Capacity (FCM).** ISO-NE Forward Capacity Market. RIE has a capacity load obligation (CLO); FCM costs are allocated by peak load contribution (PLC). For unmetered residential class, capacity is recovered volumetrically ($/kWh). **Cross-subsidy: yes** — same logic as NY: peak-determined cost, volumetric recovery. ISO-NE is summer-peaking; HP winter load does not drive the peak, so HP customers overpay capacity share. **MC 8760:** yes — need capacity/peak-based allocation (e.g. zonal FCM or peak-share).
- **Ancillary services / ISO-NE admin.** Reserves, regulation, ISO-NE Schedule 2/3 and other admin. Largely fixed or load-ratio allocated; recovered in the bundled LRS rate. **Cross-subsidy: yes (small).** MC 8760: partially (e.g. reserves by load share); admin typically residual.
- **Working capital / procurement admin.** LRS Administrative Cost Adjustment Factor (~$0.00256/kWh in filing). Procurement and administration; part scales with volume. **Weak** cross-subsidy. **Add to s.r.r.** No MC 8760 — treat as residual.

**Decision.** Add total LRS (Standard Offer) to s.r.r. Decomposition drives MC 8760: energy already marginal; capacity needs peak/zonal treatment; ancillary/admin largely residual or small.

---

### RES supply (Renewable Standard Energy Charge)

**Renewable Standard Energy Charge.** Variable (lookup; e.g. $0.01461/kWh April 2025). Rhode Island's **Renewable Energy Standard** requires an increasing share of retail sales from renewables (e.g. 38.5% by 2035). Compliance is via REC purchases; the charge passes through REC (and reconciliation) costs. **Per-MWh obligation** — each additional MWh consumes RECs and increases cost. Cost scales with load; no fixed pool cross-subsidy. **Add to s.r.r. MC 8760:** yes — flat $/kWh all hours (obligation is per MWh regardless of when it occurs), analogous to NY CES Supply.

---

### Tax pass-through

**Gross Earnings Tax (GET).** **4.166667%** of charges (Genability: QUANTITY, PERCENTAGE). Rhode Island's public service corporation gross earnings tax (R.I. Gen. Laws Ch. 44-13) on electric (and gas) utilities, passed through to customers. Percentage of each customer's bill; no fixed pool. **Exclude (tax).**

---

### Eligibility-gated and optional

**A-60 rate (low-income discount).** Customers on the A-60 discount rate receive a percentage reduction (e.g. 25% or 30%) on delivery and supply. The **Low-Income Discount Recovery Factor** on A-16 (and other non-A-60) bills recovers that cost. We **exclude both** the discount and the recovery factor from the BAT so the BAT isolates rate-design cross-subsidy; **include in LMI post-processing** of bills (e.g. `utils/post/` LMI logic). See [cairo_lmi_and_bat_analysis.md](../tools/cairo_lmi_and_bat_analysis.md).

---

## Structural notes

### Single utility, single default residential tariff

Rhode Island has one major electric distribution utility (RIE) and one default residential tariff (A-16). The A-60 rate is the same structure with an income-based discount; cost recovery is via the Low-Income Discount Recovery Factor on other classes. No cross-utility comparison is needed.

### ISO-NE vs NYISO

- **Markets:** ISO-NE (LMP, FCM, ancillary) vs NYISO (LBMP, ICAP/UCAP, ancillary). Both are FERC-regulated RTOs.
- **Capacity:** ISO-NE uses a Forward Capacity Market (FCM) with annual auctions and reconfiguration auctions; capacity obligation is by peak load contribution. NYISO uses installed capacity (ICAP/UCAP) with locational requirements. In both cases, residential default service recovers capacity cost volumetrically → HP cross-subsidy when system is not winter-peaking.
- **Supply procurement:** RI uses competitive LRS procurement (FPFR + spot); the utility retains capacity responsibility. NY utilities typically procure from NYISO and pass through via an MSC-like charge. Decomposition (energy vs capacity vs ancillary vs admin) is analogous for BAT and MC 8760.

### ISR plan and reconciliation

RI's **Infrastructure, Safety, and Reliability (ISR)** plan is an annual capital (and related O&M) plan filed with the PUC. The CapEx Factor Charge recovers approved ISR capital; the CapEx Reconciliation Factor true-ups actual vs. forecast. This is distinct from a single multi-year rate case but functionally part of base delivery for BAT purposes.

### Delivery vs supply on the bill

Genability and the compliance filing separate **delivery** (customer charge, distribution, O&M, CapEx, pension, transmission, transition, EE, net metering, long-term contracting, RE Growth, LIHEAP, storm fund, arrearage, LMI recovery, performance, LRS adjustment) from **supply** (LRS base, LRS adjustment, LRS admin, RES). For BAT: delivery components either sit in d.r.r. (base + add-ons we top up) or are excluded (reconciliation, revenue true-up, LMI recovery, tax); supply sits in s.r.r. with RES and LRS admin added and LRS true-up excluded.gst

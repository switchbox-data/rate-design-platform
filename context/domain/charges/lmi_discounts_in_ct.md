# Connecticut low-income / energy affordability programs (DRAFT skeleton)

**Status:** Skeleton only — sections and research pointers below; program facts, rates, and participation numbers are intentionally blank pending research.

**Utilities in scope:** `ct_eversource` (electric + Yankee Gas), `ct_ui` (electric; Avangrid gas affiliates CNG/SCG as relevant).

**Sibling docs:** [lmi_discounts_in_ny.md](lmi_discounts_in_ny.md), [lmi_discounts_in_ri.md](lmi_discounts_in_ri.md). Cost-recovery of these programs on the retail bill is discussed under SBC / Energy Assistance in [ct_electric_bill_components.md](ct_electric_bill_components.md) and [ct_residential_charges_in_bat.md](../methods/bat_mc_residual/ct_residential_charges_in_bat.md).

---

## 1. Program landscape overview

_What distinct LMI / affordability programs exist in CT, which ones matter for bill-impact modeling vs. arrearage/shutoff protection, and how they relate to each other (e.g. hardship designation as a gateway)._

- Programs to characterize (confirm / rename as research settles):
  - [ ] Low-Income Discount Rate (LIDR) — electric bill discount
  - [ ] Financial hardship designation
  - [ ] Matching Payment Program (MPP)
  - [ ] Connecticut Energy Assistance Program (CEAP)
  - [ ] Winter protection / medical protection
  - [ ] Legacy arrearage programs (New Start / Balance Forgiveness) if still relevant to historical baselines
  - [ ] Gas-side assistance (if any beyond MPP / CEAP)
- [ ] One-paragraph summary of the current affordability stack
- [ ] What we will / will not model in CAIRO post-processing

---

## 2. Eligibility criteria

### 2.1 Financial hardship designation

_Income thresholds, categorical / benefits-based pathways, documentation, annual renewal._

- [ ] Income screen (e.g. % of State Median Income) by household size
- [ ] Categorical eligibility (benefit programs list)
- [ ] How hardship is established (utility vs. CAA / DSS data share)
- [ ] Relationship of hardship status to LIDR / MPP / winter protection

### 2.2 Low-Income Discount Rate (LIDR)

_Who qualifies for which discount tier; differences between Eversource and UI._

- [ ] Tier definitions (income bands and/or categorical rules)
- [ ] Eversource tier schedule
- [ ] UI tier schedule
- [ ] Auto-enrollment vs. customer-initiated enrollment
- [ ] Re-verification / enrollment duration
- [ ] Programmer-readable mapping table: household characteristics → tier (ResStock-implementable)

### 2.3 Matching Payment Program (MPP) and related arrearage programs

_Eligibility beyond hardship (past-due balance rules, heating-fuel scope, program-year calendar)._

- [ ] Past-due / aging requirements
- [ ] Electric vs. gas vs. delivered-fuel scope
- [ ] Interaction with CEAP awards

### 2.4 CEAP and other non-utility assistance

_State heating assistance that feeds utility matches or bill credits._

- [ ] CEAP eligibility and typical benefit ranges
- [ ] Whether CEAP alone creates LIDR / hardship status

---

## 3. How discounts / benefits are applied

### 3.1 LIDR bill mechanics

_What is discounted (total bill vs. delivery vs. specific charge groups), when it appears on the bill, and how tiers map to % or $_._

- [ ] Discount base (which charges)
- [ ] Discount form (% of bill, fixed credit, rate class, etc.)
- [ ] Tier → discount amount table (Eversource)
- [ ] Tier → discount amount table (UI)
- [ ] Effective dates of current structure (and prior 2-tier structure if needed for historical runs)

### 3.2 MPP / arrearage mechanics

_Match rates, payment plan structure, forgiveness timing — mainly for context; may be out of scope for master-bills LMI columns._

- [ ] Match formula (customer payment + CEAP)
- [ ] Program year / grace rules
- [ ] Whether this should be modeled in bill post-processing (likely no for steady-state BAT)

### 3.3 Cost recovery on non-participant bills

_How program costs show up for other ratepayers (SBC, Energy Assistance, RAM filings)._

- [ ] Eversource cost-recovery vehicle(s)
- [ ] UI cost-recovery vehicle(s)
- [ ] Link to BAT charge-classification decisions (`exclude_eligibility` vs. other)

---

## 4. Participation in the real world today

_Enrolled counts, eligible population estimates, and an implied participation rate we can use for sampling (analogous to NY ~40% / RI ~32% scenarios)._

- [ ] Eversource: enrolled LIDR customers (by tier if available)
- [ ] UI: enrolled LIDR customers (by tier if available)
- [ ] Statewide or per-utility eligible population estimate
- [ ] Implied participation rate(s) and date stamp
- [ ] Hardship-designated customer counts (if distinct from LIDR)
- [ ] MPP enrollment (optional; for context)
- [ ] Known enrollment shocks (e.g. DSS data-sharing auto-enrollment) and whether current numbers are post-redesign

---

## 5. Implications for rate-design modeling

_How we intend to operationalize CT LMI in this repo once research is done._

- [ ] Proposed tier-assignment logic from ResStock metadata (`in.representative_income`, occupants, FPL/SMI)
- [ ] Electric vs. gas treatment
- [ ] Participation sampling defaults (p100 vs. observed rate; weighted vs. uniform)
- [ ] Planned script / wiring (e.g. `apply_ct_lmi_*` → `build_master_bills.py`)
- [ ] Open questions / approximations (vulnerability, categorical eligibility, UI vs. Eversource asymmetry)

---

## 6. Key open questions

_Park unresolved research questions here as they arise._

1. …
2. …
3. …

---

## Appendix A. Research sources — where to look

Pointers only; do not treat linked pages as already transcribed into this doc.

### A.1 Official program overviews (start here)

| Source                                                                                                                                  | Why look here                                                                                                |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| [PURA — Payment Assistance Programs](https://portal.ct.gov/pura/consumer-services/payment-assistance-programs)                          | Canonical list: hardship, LIDR, MPP, CEAP, winter protection, legacy New Start / BFP                         |
| [CT Heating Help — utility payment plans](https://portal.ct.gov/heatinghelp/utility-assistance-information)                             | Plain-language LIDR tier summary (Eversource multi-tier vs. UI two-tier as of that page), MPP + CEAP linkage |
| [CT Heating Help — CEAP](https://portal.ct.gov/heatinghelp/connecticut-energy-assistance-program-ceap)                                  | Heating-assistance eligibility and application path via CAAs                                                 |
| [Eversource bill-help fact sheet (PDF)](https://www.eversource.com/docs/default-source/my-account/bill-help-fact-sheet.pdf)             | Hardship income table (% SMI by HH size), LIDR discount tiers, MPP match example                             |
| [Eversource LIDR / discount rate page](https://www.eversource.com/content/residential/account-billing/payment-assistance/discount-rate) | Utility-facing LIDR enrollment and current tier messaging                                                    |
| [UI — Help with your bill](https://www.uinet.com/web/uinet/account/waystopay/help-with-bill)                                            | UI LIDR tiers, hardship, MPP as described to customers                                                       |

### A.2 Regulatory dockets and decisions (authoritative structure + enrollment)

| Source                                                                                                                                                                                 | Why look here                                                                                                                   |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **PURA Docket 17-12-03RE11** (New Rate Designs / LIDR)                                                                                                                                 | Origin and redesign of the Low-Income Discount Rate; search PURA eFiling for final / modified decisions and compliance filings  |
| **PURA Docket 25-05-01** (Annual Review of Affordability Programs)                                                                                                                     | Ongoing affordability annual review — likely home for updated program rules and stats                                           |
| Eversource / UI **Revenue Adjustment Mechanism (RAM)** / public-benefits annual filings                                                                                                | PURA has directed EDCs to report LIDR costs and statistics in RAM-type proceedings — best bet for enrollment by tier and $ cost |
| [PURA Q1 2025 Newsletter (PDF)](https://portal.ct.gov/-/media/pura/1---website-media/q1-2025-newsletter.pdf)                                                                           | Notes five-tier LIDR modification (5 / 15 / 20 / 40 / 50%) and RAM reporting requirement                                        |
| [PURA 2024 Annual Report](https://portal.ct.gov/-/media/pura/2024-annual-report/pura-2024-annual-report---section-7-grid-modernization.pdf)                                            | MPP statutory background and “New MPP” changes (PA 23-102)                                                                      |
| Secondary write-up citing docket numbers/enrollment: [Inside Investigator — LIDR cost article](https://insideinvestigator.org/new-pura-discount-program-costs-ratepayers-137-million/) | Useful for finding docket cites and enrollment shock numbers; verify everything against PURA PDFs                               |

### A.3 In-repo context already relevant

| File                                                                                            | What it already covers                                                                                                 |
| ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| [ct_electric_bill_components.md](ct_electric_bill_components.md)                                | OCC (Jan 2022) description of SBC funding hardship + matching-payment programs; Eversource/UI SBC magnitudes           |
| [ct_residential_charges_in_bat.md](../methods/bat_mc_residual/ct_residential_charges_in_bat.md) | Classifies SBC / UI “Energy Assistance Costs” as LMI cost recovery (`exclude_eligibility`); open UI confirmation items |
| [lmi_discounts_in_ri.md](lmi_discounts_in_ri.md)                                                | Template for % bill discount + participation-rate section; CT LIDR is conceptually closer to RI than to NY EAP credits |
| [lmi_discounts_in_ny.md](lmi_discounts_in_ny.md)                                                | Template for multi-tier eligibility tables and “programmer-readable” logic                                             |
| [resstock_lmi_metadata_guide.md](../code/data/resstock_lmi_metadata_guide.md)                   | How to map ResStock income/occupants → FPL%/SMI% for tier assignment                                                   |
| [lmi_master_bills_workflow.md](../code/orchestration/lmi_master_bills_workflow.md)              | How NY LMI is wired into master bills (target pattern for a future CT script)                                          |
| [lmi_common.py](../../../utils/post/lmi_common.py)                                              | Shared FPL / CPI / participation helpers to reuse once CT rules are known                                              |

### A.4 Suggested research order

1. Skim PURA payment-assistance page + Eversource fact sheet + UI bill-help page → fill **§1** and draft **§2–3** for LIDR.
2. Pull the latest **17-12-03RE11** (and any RE reopeners) decision(s) that set the five-tier structure → lock tier tables and effective dates.
3. Pull the latest Eversource and UI **RAM / affordability compliance** filings → fill **§4** enrollment and cost figures; compute participation rate vs. an eligible-population estimate (ACS / ResStock / utility residential accounts).
4. Decide modeling scope in **§5** (LIDR only vs. also MPP); only then design the apply script.

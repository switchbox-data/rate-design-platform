# Maryland low-income / energy affordability programs

**Status:** Partially filled from MD PSC Order No. 92190 (PC 59 LIM, Feb 12, 2026). Current OHEP grant details and LIM tier→% tables remain **Still needed** from other sources.

**Utilities in scope:** Statewide LIM applies to MD utilities generally; this repo currently emphasizes `bge` (`UTILITIES=bge` in `rate_design/hp_rates/md/state.env`). Also relevant: Pepco, Delmarva (DPL), SMECO, Potomac Edison, Columbia Gas, UGI, WGL.

**Sibling docs:** [lmi_discounts_in_ny.md](lmi_discounts_in_ny.md), [lmi_discounts_in_ri.md](lmi_discounts_in_ri.md). Cost recovery of assistance on the retail bill (e.g. Universal Service / EUSP) is touched in [md_residential_charges_in_bat.md](../methods/bat_mc_residual/md_residential_charges_in_bat.md).

### Citation key

| Short cite           | Document                                                                                                                    | Local path                                                                                                                                  |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **PC 59 Order**      | MD PSC Order No. 92190, _Order on Limited Income Mechanism_, Administrative Docket PC 59 (Feb 12, 2026; Maillog No. 327093) | PDF: `dev/md_lmi_sources/Order_LIM-PC59.pdf`. Extract: [Order_LIM-PC59.md](../../sources/Order_LIM-PC59.md)                                 |
| **FY26 MEAP matrix** | OHEP FY26 MEAP heating grant schedule                                                                                       | PDF: `dev/md_lmi_sources/FY26-MEAP-Benefit-Matrix.pdf`. Extract: [FY26-MEAP-Benefit-Matrix.md](../../sources/FY26-MEAP-Benefit-Matrix.md)   |
| **FY26 EUSP matrix** | OHEP FY26 EUSP electric grant schedule                                                                                      | DOCX: `dev/md_lmi_sources/FY26-EUSP-Benefit-Matrix.docx`. Extract: [FY26-EUSP-Benefit-Matrix.md](../../sources/FY26-EUSP-Benefit-Matrix.md) |

**Scope note:** Filled LIM facts below come from **PC 59 Order** only. Current OHEP (MEAP/EUSP) program rules are **not** specified in that order — see Appendix A.1.

**Framing:** Maryland has (a) **today’s OHEP grant stack** (MEAP / EUSP / arrearage / USPP) and (b) a forthcoming **Limited Income Mechanism (LIM)** approved in design under PC 59, targeted for implementation **before Jan 1, 2027**. Decide modeling scope: grants, LIM, or both.

**LIM sits on top of OHEP (not a replacement):** Yes — that is the correct reading. OHEP remains the eligibility gateway and continues to pay MEAP/EUSP grants; LIM is an **additional** utility on-bill discount/credit sized so that, **after netting existing OHEP assistance**, the customer’s remaining energy burden approaches the target (≈6%). The formula’s middle term is literally “Applicable Bill **Net of OHEP Assistance**” (see §3.2). Private charity funds and arrearage grants are **excluded** from that netting ([PC 59 Order](../../sources/Order_LIM-PC59.md) §III.3).

---

## 1. Program landscape overview

| Program                                                                   | Role                                                                                               | Modeling relevance                                                                                        |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| **OHEP**                                                                  | DHS Office of Home Energy Programs — enrollment / certification gateway                            | Eligibility backbone for LIM                                                                              |
| **MEAP**                                                                  | Heating assistance **grant** (paid to heating vendor / utility)                                    | Current stack; **subtracted** in LIM “net of OHEP” term                                                   |
| **EUSP**                                                                  | Electric assistance **grant**                                                                      | Current stack; **subtracted** in LIM “net of OHEP” term; Universal Service Charge recovers costs on bills |
| **Arrearage assistance** (ARA / GARA etc.)                                | Past-due balance grants                                                                            | Context / arrearage; consensus: **not** in LIM benefit calc                                               |
| **USPP**                                                                  | Winter shutoff protection + budget billing for MEAP-eligible                                       | Protections, not a rate discount                                                                          |
| **LIM (PC 59)**                                                           | Forthcoming utility **on-bill discount/credit** toward ~6% energy burden **on top of** OHEP grants | **Primary** forward-looking bill-impact lever                                                             |
| **Utility-specific** (e.g. Columbia AMP / HeatShare; UGI Operation Share) | Company hardship / arrearage offerings                                                             | Columbia/UGI sought LIM alternatives; not exempted yet                                                    |

**Primary LIM goal (Work Group → Order):**

> “The report emphasized that the primary goal of the mechanism is to help ensure eligible customers achieve an energy burden of approximately 6 percent.”
>
> — [PC 59 Order](../../sources/Order_LIM-PC59.md) §III (Work Group Report summary), citing Work Group Report (Docket Item No. 66)

**Current stack (one paragraph):** Today, limited-income MD households primarily get help through **OHEP grants** (MEAP for heat, EUSP for electric), optional arrearage grants, and USPP shutoff protection — not through a statewide percent-of-bill utility discount. (**Still needed:** full OHEP brochure/ops-manual writeup for §2.1 / §3.1.)

**Forthcoming LIM (one paragraph):** Under PUA § 4-309 and **PC 59 Order**, utilities must adopt a **Limited Income Mechanism**. The Commission **approves the general design** of a tiered, OHEP-based mechanism with group-average credits aiming toward ~**6%** energy burden, implementable **prior to January 1, 2027**, with Phase II for tariffs/marketing. LIM **supplements** OHEP; it does not replace MEAP/EUSP. ([PC 59 Order](../../sources/Order_LIM-PC59.md) §I, §III, §VI.1)

**Working modeling stance (tentative):** For HP rate-design bill impacts, prioritize modeling **LIM as an incremental on-bill credit** (like RI % discount) with MEAP/EUSP as eligibility + “net of OHEP” inputs — not as a substitute for OHEP.

---

## 2. Eligibility criteria

### 2.1 OHEP / MEAP / EUSP (current)

**Still needed** from OHEP sources (Appendix A.1) — **not** in PC 59 Order beyond LIM’s reliance on OHEP certification:

- [ ] Income screen by HH size (FY26 brochure: ~200% FPL monthly limits)
- [ ] Categorical eligibility (SNAP, SSI, TCA/TANF, veterans benefits)
- [ ] Program year (Jul 1 – Jun 30)
- [ ] Responsibility for energy costs / landlord rules
- [ ] How OHEP Poverty Levels 1–7 map to FPL%

### 2.2 Limited Income Mechanism (LIM / PC 59)

**Eligibility (quoted):**

> “The mechanism proposed by the Work Group will be available to all residential utility customers who are certified by the OHEP as eligible, and is in keeping with the Commission’s directive for the model mechanism’s standard for eligibility. This proposed eligibility includes residential utility customers categorized by OHEP as Poverty Level 6 or lower, which is effectively 200 percent of the federal Poverty Limit (“FPL”) or lower.”
>
> — [PC 59 Order](../../sources/Order_LIM-PC59.md) §III.1 (Mechanism Design and Eligibility); PDF pp. ~8–9 of `dev/md_lmi_sources/Order_LIM-PC59.pdf`

**Coverage gaps when tying LIM to OHEP (quoted):**

> “However the report notes that tying eligibility to OHEP’s classification means some customers could be left out, as approximately 0.5 percent of customers receiving benefits through the Maryland Energy Assistance Program (“MEAP”) do not receive Electric Universal Service Program (“EUSP”) benefits, so they may not show up in the utility’s system. This also means that, at this time, the proposed LIM does not extend to master meter customers who lack a unique utility account. Because of the aforementioned issues, the Report recommends the Work Group be extended into a Phase II to determine possible solutions.”
>
> — [PC 59 Order](../../sources/Order_LIM-PC59.md) §III.1; PDF same section (extract previously garbled “0.5” as a footnote — corrected here to **0.5 percent**)

| Fact                    | Value                                                                                                                        | Cite                                     |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| Who                     | All **residential** utility customers **certified by OHEP as eligible**                                                      | PC 59 Order §III.1 (quote above)         |
| Income / Poverty Level  | OHEP **Poverty Level 6 or lower** ≈ **≤200% FPL**; Level 7 excluded                                                          | PC 59 Order §III.1 (quote above)         |
| Enrollment              | Driven by OHEP certification (utilities already know OHEP-eligible accounts); no separate LIM application described in Order | PC 59 Order §III.1; PSC press FAQ        |
| Tiers                   | Grouped by **OHEP Poverty Level** **and heating source**                                                                     | PC 59 Order §III.2, §VI.1                |
| MEAP-without-EUSP gap   | ~**0.5%** of MEAP recipients lack EUSP → may be invisible to utilities                                                       | PC 59 Order §III.1 (quote above)         |
| Master-metered          | **Not** covered initially (no unique utility account); Phase II                                                              | PC 59 Order §III.1 (quote above), §VI.10 |
| Non-OHEP limited-income | **Out of initial scope**; Commission separately concerned about reach                                                        | PC 59 Order §VI.1                        |

**Still needed (Work Group report / tariffs):**

- [ ] Exact Poverty Level → FPL band table
- [ ] Per-tier / per-heating-source credit or % schedules
- [ ] Programmer-readable ResStock mapping (FPL% → Poverty Level)

### 2.3 Utility-specific pathways

| Fact               | Value                                                                                                                                   | Cite                             |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| SMECO vs others    | **Percent-of-rate** for all utilities **except SMECO** → **flat bill credit**                                                           | PC 59 Order §VI.2                |
| Columbia Gas / UGI | Sought exemption / alternative programs; Commission **declines exemption for now**; must file detailed alternative plans within 60 days | PC 59 Order §VI.9                |
| Retail choice      | Shopping for supply **does not** change LIM credit/discount size (Work Group consensus; accepted unless otherwise stated)               | PC 59 Order §III.3, Ordering (1) |

---

## 3. How discounts / benefits are applied

### 3.1 Current OHEP grants (MEAP / EUSP / arrearage)

**Still needed** from OHEP brochure / Operations Manual / EUSP Plan.

From LIM context only:

| Fact                 | Value                                                                                                                                                                       | Cite               |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| Interaction with LIM | Final LIM benefits determined **after considering OHEP grants** (EUSP, MEAP, supplemental); **exclude** private funds (e.g. Fuel Fund) and **arrearages** from the LIM calc | PC 59 Order §III.3 |

### 3.2 Limited Income Mechanism (LIM) bill mechanics

**Structure and formula (quoted):**

> “The proposed mechanism employs a tiered discount structure that groups customers by OHEP Poverty Level group identification and heating source. The proposed mechanism also employs an averaging approach, under which the average discount or credit a customer within a Poverty Level group would need to receive annually to achieve the target energy burden, is calculated by averaging customer bills, OHEP benefits received, and income for each customer group. The discount or credit is calculated by subtracting the Target Energy Burden Threshold (Average Income multiplied by the Energy Burden Percentage) from the Applicable Bill Net of OHEP Assistance, reflected in the equations:”
>
> — [PC 59 Order](../../sources/Order_LIM-PC59.md) §III.2 (General Mechanism Structure); PDF pp. ~9–10 of `dev/md_lmi_sources/Order_LIM-PC59.pdf`

$$
[\text{Average Income}] \times [\text{Energy Burden Percentage}] = [\text{Target Energy Burden Threshold}]
$$

$$
[\text{Average Applicable Utility Charges}] - [\text{Average Existing OHEP Assistance}] = [\text{Applicable Bill Net of OHEP Assistance}]
$$

$$
[\text{Applicable Bill Net of OHEP Assistance}] - [\text{Target Energy Burden Threshold}] = [\text{Discount Needed}]
$$

Same equations restated in Commission decision text at [PC 59 Order](../../sources/Order_LIM-PC59.md) §VI.1.

**Why this confirms “LIM on top of OHEP”:** The second equation subtracts **existing OHEP assistance** from utility charges before computing how much **additional** discount is needed to hit the burden target. LIM fills the residual gap; it does not replace MEAP/EUSP.

| Fact                         | Value                                                                                                                                    | Cite                                  |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| Form (most utilities)        | **Percent-of-rate** discount — negative rider/credit based on usage (`$/kWh` or `$/therm`)                                               | PC 59 Order §VI.2                     |
| Form (SMECO)                 | **Flat bill credit** (same credit within a benefit group)                                                                                | PC 59 Order §VI.2                     |
| Bill base                    | **Supply + distribution** costs                                                                                                          | PC 59 Order §VI.7                     |
| Appearance                   | Separate **line item** (Work Group consensus)                                                                                            | PC 59 Order §III.3                    |
| Core formula (group average) | Equations above                                                                                                                          | PC 59 Order §III.2, §VI.1             |
| Heating split                | Gas+electric: energy-burden goal **split evenly** across fuels; electric-only: full goal on electric bills                               | PC 59 Order §VI.1                     |
| Burden goal                  | Primary goal ~**6%** (Work Group); Commission goal approximately 6%, **not locked** as a concrete number yet — revisit at tariff filings | PC 59 Order §III (quote in §1); §VI.6 |
| Timeline                     | Marketing Summer 2026; tariffs Fall 2026; implement **before Jan 1, 2027**                                                               | PC 59 Order §VI.10                    |

**Implication of averaging:** At a 6% goal, roughly half of OHEP customers may need credits; others may already be at/below target — see Work Group report for magnitudes (~`$79M` figures discussed in Order §III.6).

**Still needed (Work Group report + utility tariffs):**

- [ ] Exact conversion of “Discount Needed” into a `$/kWh` (or flat `$/mo`) by tier
- [ ] Seasonal adjustment rules (Staff floated seasonal flat credits)
- [ ] Soft cap on non-participant surcharge (`$2`–`$4`/mo discussed; **deferred**)

### 3.3 Cost recovery on non-participant bills

| Fact                                  | Value                                                                                                                                     | Cite                 |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| Recovery base                         | Work Group consensus: recover from **all ratepayers**; soft cap on final rider                                                            | PC 59 Order §III.8–9 |
| Residential vs C&I split              | **Deferred** — utilities propose allocation in tariff filings (options discussed: 25/75, 75/25, utility discretion)                       | PC 59 Order §VI.5    |
| Soft cap amount                       | Discussed ~`$2`–`$4`/mo residential; Commission **defers** until final bill-impact estimates                                              | PC 59 Order §VI.8    |
| EUSP Universal Service Charge (today) | Existing BGE bill line funding EUSP — see [md_residential_charges_in_bat.md](../methods/bat_mc_residual/md_residential_charges_in_bat.md) | In-repo BAT notes    |

**Still needed:** BAT classification for LIM surcharge; SEIF / Utility RELIEF Act offsets (Jul 2026 PC 59 data order).

---

## 4. Participation in the real world today

**Not covered** with enrollment counts in PC 59 Order.

| Fact from Order   | Value                                                                                                         | Cite                      |
| ----------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------- |
| LIM starting pool | OHEP-certified customers (Poverty Level ≤6)                                                                   | PC 59 Order §III.1, §VI.1 |
| Data path         | OHEP holds eligibility, benefits, usage; will provide heating-source data; utilities QC usage outliers        | PC 59 Order §VI.3         |
| Reporting         | LIM participation in PC 53 monthly reports; quarterly avg bills before/after discount; annual surcharge stats | PC 59 Order §VI.4         |

**Still needed:** Statewide / per-utility OHEP enrollment and implied participation rates (Appendix A.1 / Jul 2026 data order filings).

---

## 5. Implications for rate-design modeling

| Topic               | Implication from PC 59 Order                                                         | Confidence                        |
| ------------------- | ------------------------------------------------------------------------------------ | --------------------------------- |
| What to model first | **LIM** (percent-of-rate; SMECO flat credit)                                         | High                              |
| Eligibility proxy   | ResStock FPL% ≤ **200%** as OHEP Poverty Level ≤6 stand-in                           | Medium — need Poverty Level table |
| Discount shape      | Closer to **RI % of bill** than NY fixed `$`/mo EAP                                  | High                              |
| Net of grants       | Must subtract modeled MEAP/EUSP (or assume zero grants in sensitivity)               | High                              |
| Fuels               | Split burden goal for dual-fuel; electric-only gets full goal on electric            | High                              |
| Supply+delivery     | Apply to total energy charges, not delivery-only                                     | High                              |
| Burden %            | Use **6%** as default scenario; sensitivity at 3%/9% (analyzed in Work Group report) | Medium — not locked by Commission |
| Script              | Future `apply_md_lmi_*` → `build_master_bills.py` via `lmi_common.py`                | Planned                           |

---

## 6. Key open questions

1. Exact **Poverty Level → FPL%** and **tier → $/kWh or flat `$`** schedules — need **Work Group report** + utility tariffs.
2. How to model **MEAP/EUSP grant amounts** for the “net of OHEP” term with ResStock.
3. Observed **OHEP participation rate** for sampling (p100 vs. ~pXX).
4. Soft-cap / cost-allocation outcomes once tariffs are filed.
5. Whether BGE-first modeling should ignore SMECO flat-credit exception initially.

---

## Appendix A. Research sources — where to look

### A.1 Current OHEP (today’s grants) — start here for §2.1 / §3.1 / §4

| Source                                                                                                                                   | Why look here                                                    |
| ---------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [DHS OHEP hub](https://dhs.maryland.gov/office-of-home-energy-programs/)                                                                 | How to apply, program year, links to docs                        |
| [About energy assistance](https://dhs.maryland.gov/office-of-home-energy-programs/about-energy-assistance/)                              | MEAP / EUSP / arrearage / USPP plain-language overview           |
| [OHEP Program Documents](https://dhs.maryland.gov/office-of-home-energy-programs/ohep-documents/)                                        | Brochures, applications, forms index                             |
| [OHEP Brochure FY26 (PDF)](https://dhs.maryland.gov/documents/OHEP/OHEP_Englishbrochure_2026.pdf)                                        | Jul 2025–Jun 2026 income limits by HH size; program descriptions |
| [Income Guidelines FY2026 (PDF)](https://dhs.maryland.gov/documents/OHEP/Advisory%20Board/Income-Guidelines-FY2026-Updated-7.9.2025.pdf) | Weekly / monthly / annual 200% FPG tables                        |
| [OHEP Operations Manual (PDF)](https://dhs.maryland.gov/documents/OHEP/OHEP-Operations-Manual.pdf)                                       | Authoritative eligibility, categorical rules, benefit admin      |
| Annual **EUSP Plan** to MD PSC                                                                                                           | EUSP benefit methodology filed with regulators                   |
| OHEP public data / monthly reports (linked from DHS OHEP site)                                                                           | Enrollment / participation for §4                                |

### A.2 LIM design — order + Work Group report (detailed math)

| Source                                                                                                                                 | Why look here                                                                                                                                                                                                                                                                                                        |
| -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **[PC 59 Order extract](../../sources/Order_LIM-PC59.md)** / `dev/md_lmi_sources/Order_LIM-PC59.pdf`                                   | Commission decisions (§VI) — **already used** above                                                                                                                                                                                                                                                                  |
| **[PC 59 Work Group Report — Oct 1, 2025 (PDF)](https://www.nclc.org/wp-content/uploads/2026/02/PC59-WG-Report-Oct-1-2025-Final.pdf)** | **Detailed math** for discounts: averaging method, Appendices (e.g. Target Energy Burden), cost scenarios at 3%/6%/9%, consensus vs non-consensus. Cited in Order as Docket Item No. 66. Also listed on [NCLC resource page](https://www.nclc.org/resources/limited-income-mechanism-for-utility-customer-maryland/) |
| [PSC press release](https://psc.maryland.gov/news/2026/psc-advances-discounted-rate-mechanism-for-limited-income-utility-customers/)   | Short FAQ                                                                                                                                                                                                                                                                                                            |
| [Jul 8, 2026 PC 59 data/funds order](https://psc.maryland.gov/wp-content/uploads/2026/07/Order_DataPropRelatingDistFunds-PC-59.pdf)    | Utility reporting of EUSP/MEAP counts — participation inputs                                                                                                                                                                                                                                                         |
| Future utility LIM tariffs (Fall 2026)                                                                                                 | Company-specific `$/kWh` or flat credits                                                                                                                                                                                                                                                                             |

### A.3 In-repo context

| File                                                                                            | Role                                                      |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| [Order_LIM-PC59.md](../../sources/Order_LIM-PC59.md)                                            | Primary fill source for LIM sections                      |
| [FY26-MEAP-Benefit-Matrix.md](../../sources/FY26-MEAP-Benefit-Matrix.md)                        | MEAP annual `$` by Poverty Level × fuel                   |
| [FY26-EUSP-Benefit-Matrix.md](../../sources/FY26-EUSP-Benefit-Matrix.md)                        | EUSP annual `$` by Poverty Level × heat source × kWh band |
| [md_residential_charges_in_bat.md](../methods/bat_mc_residual/md_residential_charges_in_bat.md) | Universal Service / EUSP cost recovery on BGE bills       |
| [resstock_lmi_metadata_guide.md](../code/data/resstock_lmi_metadata_guide.md)                   | ResStock → FPL% for eligibility proxy                     |
| [lmi_master_bills_workflow.md](../code/orchestration/lmi_master_bills_workflow.md)              | NY wiring pattern for a future MD script                  |

### A.4 Remaining research order

1. Download **Work Group Report** → lock tier math for §3.2 (**Discount Needed** → `$/kWh`).
2. Skim **OHEP brochure + Income Guidelines FY2026 + Ops Manual** → fill §2.1 / §3.1.
3. Pull enrollment stats → §4 participation rate.
4. Decide LIM-only vs. grants+LIM → implement `apply_md_lmi_*`.

# Connecticut low-income / energy affordability programs

**Status:** Partially filled from the PURA 2026 payment-assistance one-pager. Gaps that need other sources are marked **Still needed**.

**Utilities in scope:** `ct_eversource` (electric; Yankee Gas for gas), `ct_ui` (electric; Avangrid gas affiliates CNG/SCG as relevant).

**Sibling docs:** [lmi_discounts_in_ny.md](lmi_discounts_in_ny.md), [lmi_discounts_in_ri.md](lmi_discounts_in_ri.md). Cost-recovery of these programs on the retail bill is discussed under SBC / Energy Assistance in [ct_electric_bill_components.md](ct_electric_bill_components.md) and [ct_residential_charges_in_bat.md](../methods/bat_mc_residual/ct_residential_charges_in_bat.md).

### Citation key (for DocumentCloud annotations)

| Short cite       | Document                                                                                                                                                                                                         | Local path                                                                                                                                                                           | DocumentCloud                                                                                                                                                                                          |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **PURA PA 2026** | _Residential Energy Payment Assistance Information for UI and Eversource Customers_, Connecticut Public Utilities Regulatory Authority (PDF titled `utility-bill-payment-assistance---2026`; created 2026-07-31) | Source PDF: `dev/utility-bill-payment-assistance---2026.pdf`. Markdown extract: [utility-bill-payment-assistance---2026.md](../../sources/utility-bill-payment-assistance---2026.md) | [DocumentCloud](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/) — **p. 1** program comparison table; **p. 2** contacts / CAAs; **p. 3** CEAP application notes |

**Cite style:** Prefer annotation links in the **Cite** column as `([DocumentCloud p. N](…/#document/pN/aNNNN))`, matching [`ct_residential_charges_in_bat.md`](../methods/bat_mc_residual/ct_residential_charges_in_bat.md). Where no annotation exists yet, cite the page: `([DocumentCloud p. N](…/#document/pN))`.

**Annotated values on DocumentCloud (PURA PA 2026):**

| Annotation title (on DocumentCloud)         | Annotation ID | Used for                 |
| ------------------------------------------- | ------------- | ------------------------ |
| LIDR discount amount                        | `a2826849`    | 5%–50%                   |
| LIDR Coverage max electric usage            | `a2826850`    | 800 / 1200 kWh/mo caps   |
| CEAP max benefit amount                     | `a2826851`    | `$530`/season            |
| CEAP eligibility                            | `a2826852`    | ≤60% SMI                 |
| CEAP enrollment period                      | `a2826853`    | Aug 1 CAA scheduling     |
| CEAP application requirement                | `a2826854`    | Must still apply (p. 3)  |
| Generation Power CT benefit and eligibility | `a2826855`    | `$500`/year and ≤75% SMI |

**Scope note:** Almost all filled facts below come from **PURA PA 2026** only. Anything from another in-repo source is labeled separately.

---

## 1. Program landscape overview

PURA’s 2026 one-pager lists five assistance categories for UI and Eversource residential customers ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)):

| Program                                          | Role in the affordability stack                                                             | Primary modeling relevance                                      |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| **Low Income Discount Rate (LIDR)**              | Ongoing **on-bill % discount** on electric service                                          | **Yes — primary** for master-bills LMI columns                  |
| **Matching Payment Plan (MPP)**                  | Past-due balance payment arrangement + match / forgiveness (electric and gas)               | Context / arrearage only; not a steady monthly rate discount    |
| **Connecticut Energy Assistance Program (CEAP)** | Seasonal **heating** assistance paid to the heating utility (or delivered-fuel vendor path) | Context; can feed MPP matches; not itself an on-bill % discount |
| **Generation Power CT (Operation Fuel)**         | One-time heating assistance (up to `$500`/year)                                             | Out of scope for rate-design LMI post-processing                |
| **Other**                                        | Misc. heating aid via 211 / local orgs                                                      | Out of scope                                                    |

**Financial hardship** is not a separate row in the table, but it is the **eligibility gateway** named for CEAP and MPP, and one of three LIDR eligibility paths ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1) — CEAP / MPP / LIDR rows).

**Winter protection / medical protection / legacy New Start / BFP** are **not** described in this one-pager (**Still needed** from the broader [PURA payment-assistance web page](https://portal.ct.gov/pura/consumer-services/payment-assistance-programs) or other filings).

**Working modeling stance (tentative):** post-process **LIDR only** for CT bill impacts; treat MPP/CEAP/Operation Fuel as eligibility/context, not as additional monthly discount columns — subject to confirmation once tier tables and participation are known.

---

## 2. Eligibility criteria

### 2.1 Financial hardship designation

From **PURA PA 2026** (tied to CEAP / MPP / LIDR rows):

| Fact                                   | Value                                                                  | Cite                                                                                                                                |
| -------------------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Income screen used for CEAP hardship   | At or below **60% State Median Income (SMI)**                          | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826852)) |
| Hardship as MPP gate                   | MPP requires financial hardship qualification                          | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))          |
| Hardship as one LIDR path              | LIDR if customer has “a financial hardship status” (among other paths) | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))          |
| Operation Fuel alternate income screen | Hardship **or below 75% SMI**                                          | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826855)) |

**Still needed (not in PURA PA 2026):**

- [ ] SMI income limits **by household size** (dollar table)
- [ ] Full categorical / benefits list that confers hardship
- [ ] How hardship is established (utility intake vs. CAA / DSS data share)
- [ ] Explicit relationship to winter / medical protection

### 2.2 Low-Income Discount Rate (LIDR)

From **PURA PA 2026**, LIDR row ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)):

| Fact                 | Value                                                                                                                             | Cite                                                                                                                       |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Fuel / service       | **Electric only**                                                                                                                 | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| Eligibility (any of) | (1) Meet household income requirements, **or** (2) receive a public assistance benefit, **or** (3) have financial hardship status | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| Enrollment           | Year-round; contact utility **or auto-enrollment**                                                                                | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| Re-verification      | **Annual verification**                                                                                                           | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| How to apply         | Contact utility                                                                                                                   | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |

**Still needed (not in PURA PA 2026):**

- [ ] Exact **tier definitions** (income bands → 5% / 15% / 20% / 40% / 50%, or UI’s schedule)
- [ ] Eversource vs. UI asymmetry (if any remains after five-tier redesign)
- [ ] Which “public assistance benefits” count
- [ ] Programmer-readable ResStock mapping table

### 2.3 Matching Payment Program (MPP)

From **PURA PA 2026**, MPP row ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)):

| Fact                            | Value                                                          | Cite                                                                                                                       |
| ------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Who                             | **Any electric or gas** customers                              | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| Eligibility                     | Financial hardship qualified                                   | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| Explicitly removed requirements | **CEAP and primary heating source are no longer requirements** | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| Enrollment window               | Year-round; winter heating season remains Nov 1–May 1          | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| How to apply                    | Contact utility                                                | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |

**Still needed:**

- [ ] Past-due balance / aging rules (e.g. `$100` / 60 days — appears on other PURA/utility pages, **not** in this one-pager)
- [ ] Detailed interaction rules with CEAP awards beyond “energy assistance award match”

### 2.4 CEAP and other non-utility assistance

#### CEAP (**PURA PA 2026**, CEAP row + p. 3 notes)

| Fact                                        | Value                                                                                                                                                            | Cite                                                                                                                                                                                                                                                   |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Type                                        | Heating assistance — direct payment to heating utility                                                                                                           | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))                                                                                                                             |
| Max benefit (primary heat, electric or gas) | Up to **`$530`/season**                                                                                                                                          | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826851))                                                                                                                    |
| Additional aid                              | Crisis and safety-net assistance for eligible **delivered-fuels** customers                                                                                      | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))                                                                                                                             |
| Customer scope                              | Primary heat source electric or gas; also covers delivered fuels                                                                                                 | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))                                                                                                                             |
| Eligibility                                 | Financial hardship (≤ **60% SMI**)                                                                                                                               | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826852))                                                                                                                    |
| Season / application                        | Nov 1–May 1; may schedule CAA visit from **Aug 1**; **annual application**                                                                                       | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826853))                                                                                                                    |
| Application path                            | CAA / 211, or DSS online winter-heating application                                                                                                              | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)); ([DocumentCloud p. 3](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p3)) |
| Separate application required               | Receipt of other government benefits can **qualify** a client for CEAP, but the customer **must still apply** — CEAP is **not** auto-awarded from other benefits | ([DocumentCloud p. 3](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p3/a2826854))                                                                                                                    |

#### Operation Fuel (**PURA PA 2026**, p. 1)

| Fact        | Value                                                               | Cite                                                                                                                                |
| ----------- | ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Max benefit | Up to **`$500`**, one time per year                                 | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826855)) |
| Eligibility | Hardship **or below 75% SMI**; additional proof of payment required | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826855)) |
| Timing      | Typically spring and fall (September, April); annual application    | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))          |

**Does CEAP alone create LIDR / hardship?** Not stated in **PURA PA 2026**. Hardship is defined for CEAP via ≤60% SMI; LIDR lists hardship as one of three paths. Treat as **Still needed** from utility / PURA detailed rules.

---

## 3. How discounts / benefits are applied

### 3.1 LIDR bill mechanics

From **PURA PA 2026**, LIDR row:

| Fact                             | Value                                      | Cite                                                                                                                                |
| -------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| Form                             | **On-bill discount**                       | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))          |
| Magnitude                        | **5% up to 50%** depending on eligibility  | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826849)) |
| Usage cap — non-electric heating | Discount limited to **800 kWh per month**  | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826850)) |
| Usage cap — electric heating     | Discount limited to **1200 kWh per month** | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826850)) |
| Service                          | Electric only                              | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1))          |

**Still needed (critical for coding):**

- [ ] Discount base — % of **total bill** vs. delivery-only vs. specific charge groups (**not** stated in PURA PA 2026)
- [ ] Exact tier → % table (Eversource and UI)
- [ ] Effective dates of current multi-tier structure vs. prior 2-tier (10%/50%) design
- [ ] Whether the kWh cap means “discount applies only to the first N kWh” (most natural reading) — confirm in tariff / decision text

### 3.2 MPP / arrearage mechanics

From **PURA PA 2026**, MPP row ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)):

| Fact  | Value                                                                                              | Cite                                                                                                                       |
| ----- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Type  | Past-due balance payment arrangement **and forgiveness**                                           | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |
| Match | Monthly payment matches made, **as well as** energy-assistance award match, **up to zero balance** | ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)) |

**Modeling note:** MPP is arrearage forgiveness, not a recurring volumetric discount → **likely out of scope** for steady-state master-bills LMI columns (same stance as NY EAP vs. one-time HEAP). Confirm in §5 when implementing.

**Still needed:** program-year / grace detail beyond Nov 1–May 1 winter season note.

### 3.3 Cost recovery on non-participant bills

**Not covered** in **PURA PA 2026**.

**Still needed** from rate-case / RAM / OCC materials (already sketched elsewhere in-repo):

- [ ] Eversource: SBC / related riders (see [ct_electric_bill_components.md](ct_electric_bill_components.md); OCC DocumentCloud cites in [ct_residential_charges_in_bat.md](../methods/bat_mc_residual/ct_residential_charges_in_bat.md))
- [ ] UI: Energy Assistance / public-benefits breakout
- [ ] BAT classification (`exclude_eligibility` for LMI cost recovery)

---

## 4. Participation in the real world today

**Not covered** in **PURA PA 2026** (no enrollment counts, eligible population, or participation rates).

**Still needed** (see Appendix A.2):

- [ ] Eversource / UI LIDR enrollment by tier
- [ ] Eligible population estimate → implied participation rate
- [ ] Hardship and MPP counts
- [ ] Post–DSS data-share / five-tier redesign status of current numbers

---

## 5. Implications for rate-design modeling

Inferences from **PURA PA 2026** only (implementation still blocked on open items):

| Topic                    | Implication from this source                                                                                                                                                                                                                                                         | Confidence                                                             |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| What to model first      | **LIDR** (electric % discount with kWh caps)                                                                                                                                                                                                                                         | High                                                                   |
| Gas LIDR?                | No gas on-bill LIDR in this table; gas appears under **MPP** (and CEAP if gas heat)                                                                                                                                                                                                  | High for “no gas LIDR row”; confirm no separate gas discount elsewhere |
| Discount shape           | Percentage (**5–50%**), **not** NY-style fixed `$`/month credit ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826849))                                                                                  | High                                                                   |
| Cap                      | Need heating-type flag (electric vs. non-electric heat) to choose **800** vs. **1200** kWh/mo ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826850))                                                    | High                                                                   |
| Eligibility for ResStock | At least ≤60% SMI hardship path exists (via CEAP definition); LIDR also allows unspecified “household income requirements” and public assistance ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826852)) | Medium — exact LIDR income bands **Still needed**                      |
| Participation sampling   | Cannot set a default observed rate from this doc                                                                                                                                                                                                                                     | —                                                                      |
| Script / wiring          | Future `apply_ct_lmi_*` → `build_master_bills.py`, reusing `lmi_common.py`                                                                                                                                                                                                           | Planned, not in source                                                 |

---

## 6. Key open questions

1. Exact LIDR **tier → %** schedule and income cutoffs (Eversource vs. UI) — **not** in PURA PA 2026.
2. Is the discount % applied to the **total electric bill** or a subset of charges?
3. Confirm kWh-cap mechanics (first N kWh only?).
4. Observed **participation rate** for sampling (p100 vs. ~pXX).
5. Should MPP/CEAP ever appear as modeled bill columns, or LIDR-only?

---

## Appendix A. Research sources — where to look next

### A.1 Official program overviews

| Source                                                                                                                                                                                                                                                       | Why look here                                                      |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| **[PURA PA 2026 on DocumentCloud](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/)** / [markdown extract](../../sources/utility-bill-payment-assistance---2026.md) / `dev/utility-bill-payment-assistance---2026.pdf` | **Already used** for §§1–3 above                                   |
| [PURA — Payment Assistance Programs](https://portal.ct.gov/pura/consumer-services/payment-assistance-programs)                                                                                                                                               | Hardship detail, winter/medical protection, legacy New Start / BFP |
| [CT Heating Help — utility payment plans](https://portal.ct.gov/heatinghelp/utility-assistance-information)                                                                                                                                                  | Plain-language LIDR tier summary                                   |
| [Eversource bill-help fact sheet (PDF)](https://www.eversource.com/docs/default-source/my-account/bill-help-fact-sheet.pdf)                                                                                                                                  | SMI-by-HH-size table; LIDR tier messaging; MPP examples            |
| [Eversource LIDR page](https://www.eversource.com/content/residential/account-billing/payment-assistance/discount-rate)                                                                                                                                      | Current utility-facing tier/enrollment text                        |
| [UI — Help with your bill](https://www.uinet.com/web/uinet/account/waystopay/help-with-bill)                                                                                                                                                                 | UI LIDR / hardship / MPP                                           |

### A.2 Regulatory dockets (tiers + participation)

| Source                                                                                                 | Why look here                          |
| ------------------------------------------------------------------------------------------------------ | -------------------------------------- |
| **PURA Docket 17-12-03RE11**                                                                           | LIDR design / five-tier redesign       |
| **PURA Docket 25-05-01**                                                                               | Affordability annual review            |
| Eversource / UI **RAM** / public-benefits filings                                                      | Enrollment and cost statistics         |
| [PURA Q1 2025 Newsletter](https://portal.ct.gov/-/media/pura/1---website-media/q1-2025-newsletter.pdf) | Notes 5/15/20/40/50% LIDR modification |

### A.3 In-repo context

| File                                                                                                 | Role                                                           |
| ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| [utility-bill-payment-assistance---2026.md](../../sources/utility-bill-payment-assistance---2026.md) | Extract of **PURA PA 2026** (primary fill source for this doc) |
| [ct_electric_bill_components.md](ct_electric_bill_components.md)                                     | SBC / hardship cost recovery (OCC) — for §3.3                  |
| [ct_residential_charges_in_bat.md](../methods/bat_mc_residual/ct_residential_charges_in_bat.md)      | BAT charge classification for LMI recovery                     |
| [resstock_lmi_metadata_guide.md](../code/data/resstock_lmi_metadata_guide.md)                        | ResStock → FPL%/SMI% for tier assignment                       |
| [lmi_master_bills_workflow.md](../code/orchestration/lmi_master_bills_workflow.md)                   | NY master-bills LMI wiring pattern                             |

### A.4 Remaining research order

1. Pull **17-12-03RE11** (and reopeners) for tier tables → finish §§2.2 and 3.1.
2. Pull RAM / affordability filings → finish §4.
3. Decide LIDR-only modeling → implement `apply_ct_lmi_*`.

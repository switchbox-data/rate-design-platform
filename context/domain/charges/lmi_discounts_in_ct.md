# Connecticut low-income / energy affordability programs

**Status:** LIDR's five-tier structure, discount percentages, fuel scope (electric only), and 800/1200 kWh usage caps are confirmed against PURA's primary decision record and PURA's own program comparison table. FPL% uses HHS poverty guidelines for `--fpl-year`. Tier 1 SMI% uses the DSS/CEAP **60% HHS LIHEAP SMI** dollar table in `ct_lidr.yaml` (not HUD SMI) — that table is an annually updated guideline (see §2.2). **Implemented** in `utils/post/apply_ct_lidr_to_master_bills.py` (config: `utils/post/data/ct_lidr.yaml`; tests: `tests/test_ct_lidr_discounts.py`), wired into `build_master_bills_prefect.py`. Production participation defaults are **p100 and p53** (`--lmi-participation-rates 1.0 0.53`), matching RI/NY/MD's two-rate pattern; 53% is observed Eversource take-up (§4.2). The usage-cap proration mechanic (§3.1): the discount applies to the whole bill (fixed + volumetric), but the dollar discount is capped at what a customer using exactly 800/1200 kWh would receive on their total bill (including fixed charge). This still uses a locally-linear average $/kWh approximation per building-month (no per-tariff block-rate detail in master bills). CEAP and Operation Fuel are documented but **not implemented** — they could theoretically be added later as independent toggleable components, but their program structures (annual individual applications, one-time crisis assistance) are incompatible with steady-state population-wide modeling (see §2.4). Still needed: UI enrollment by tier.

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

**Scope note:** General program facts come from **PURA PA 2026**. Current LIDR tiers and enrollment mechanics also use PURA's 2024 Annual Report (Table 14 is the primary source for the shared tier/discount/FPL-band framework — see §2.2), PURA's Q1 2025 newsletter, Eversource's current bill-help fact sheet, Eversource's dated rollout fact sheet (republished by the Town of Woodbury, CT), and UI's current LIDR page; links appear where used.

---

## 1. Program landscape overview

PURA’s 2026 one-pager lists five assistance categories for UI and Eversource residential customers ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1)):

| Program                                          | Role in the affordability stack                                                             | Modeling default                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Low Income Discount Rate (LIDR)**              | Ongoing **on-bill % discount** on electric service                                          | **On** — primary CT LMI component for master bills                                                                                                                                                                                                                                                                                                              |
| **Connecticut Energy Assistance Program (CEAP)** | Seasonal **heating** assistance paid to the heating utility (or delivered-fuel vendor path) | **Not implemented.** Requires annual individual application through a CAA (not auto-enrolled); each application is reviewed individually; benefit amounts vary by household. Incompatible with population-wide steady-state modeling. Could theoretically be added as a toggleable component if benefit-allocation and participation assumptions are developed. |
| **Generation Power CT (Operation Fuel)**         | One-time heating assistance (up to `$500`/year)                                             | **Not implemented.** One-time crisis assistance for households experiencing financial hardship (e.g. job loss); not a recurring annual benefit. Incompatible with steady-state bill modeling. Could theoretically be added as a toggleable component if defensible participation assumptions are developed.                                                     |
| **Matching Payment Plan (MPP)**                  | Past-due balance payment arrangement + match / forgiveness (electric and gas)               | **Off** — arrearage treatment, not a current-year recurring bill discount                                                                                                                                                                                                                                                                                       |
| **Other**                                        | Misc. heating aid via 211 / local orgs                                                      | Off / out of initial scope                                                                                                                                                                                                                                                                                                                                      |

**Financial hardship** is not a separate row in the table, but it is the **eligibility gateway** named for CEAP and MPP, and one of three LIDR eligibility paths ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1) — CEAP / MPP / LIDR rows).

**Winter protection / medical protection / legacy New Start / BFP** are **not** described in this one-pager (**Still needed** from the broader [PURA payment-assistance web page](https://portal.ct.gov/pura/consumer-services/payment-assistance-programs) or other filings).

**Modeling component defaults:** CT post-processing applies **LIDR only** because it is the recurring utility on-bill discount that can be modeled as a population-wide percentage reduction on the electric bill. **CEAP is not implemented** because it requires an annual individual application through a local Community Action Agency (CAA), each application is processed and reviewed individually, benefit amounts vary by household circumstances (`$355`–`$705` for the 2026–2027 season), and it is explicitly not auto-enrolled — a customer must apply each year even if they already receive qualifying government benefits ([CT CEAP page](https://portal.ct.gov/heatinghelp/connecticut-energy-assistance-program-ceap); [New Opportunities Inc. CEAP page](https://newoppinc.org/Service/CT-Energy-Assistance-Program)). This individual-application, variable-benefit structure is fundamentally incompatible with steady-state population-wide modeling where we apply a uniform discount to all income-eligible households. **Operation Fuel is not implemented** because it is one-time crisis assistance (up to `$500`/year) intended for households experiencing acute financial hardship such as job loss — not a recurring annual benefit that can be modeled as a steady-state bill reduction ([New Opportunities Inc.](https://newoppinc.org/Service/CT-Energy-Assistance-Program)). **MPP is not implemented** because it addresses past-due balances through matching and forgiveness rather than reducing the current tariff-based bill. CEAP and Operation Fuel could theoretically be added later as independently toggleable components if defensible benefit-allocation and participation assumptions are developed — but their program structures make population-wide steady-state application inappropriate without such assumptions.

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

LIDR applies to residential **electric service only** for Eversource and UI customers. Both utilities implement the **same five-tier structure** — same discount percentages, same FPL/SMI-defined tier bands — because it was created by a single PURA decision, not by each utility independently. Eversource rolled out the five-tier design on **June 23, 2025**; UI, granted extra time for IT changes, moved from its interim 10% / 50% design to the same five tiers on **August 3, 2026**.

**The tier framework itself (discount %, FPL/SMI bands, qualifying programs) comes from PURA's _2024 Annual Report_, Table 14** (pp. 81–82) — the actual regulatory decision record, binding on both EDCs — not from either utility's website:

> "the discount levels established in the new five-tiered structure (i.e. 5%, 15%, 20%, 40%, and 50%) were selected based on an analysis of expected enrollments and costs for each tier that would result in an estimated annual cost of **1.6% of Eversource's annual revenue, and 1.8% of UI's**."
> — [PURA 2024 Annual Report, Section 7](https://portal.ct.gov/-/media/pura/2024-annual-report/pura-2024-annual-report---section-7-grid-modernization.pdf), p. 81

That revenue-share line matters for a common misreading: **the benefit programs named per tier are not funding sources for LIDR.** LIDR is funded like any other on-bill discount — through the electric rate paid by all ratepayers (the 1.6%/1.8%-of-revenue "budgetary target" above). The named programs (SNAP, HUSKY, CEAP levels, etc.) are **categorical-eligibility shortcuts**: proof of enrollment in one of them is accepted in lieu of separate income documentation, because enrollment in that program already implies the household is under a known income threshold.

| Tier | Discount | Income ceiling                                                | Qualifying benefit programs (one household member)                                                                                                     |
| ---- | -------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1    | **5%**   | **60% SMI** (roughly 212–275% FPL, varying by household size) | ALMB / SLMB; CEAP Level 3; HUSKY A pregnant / postpartum; HUSKY B Band 1 or prenatal; HUSKY C LTSS; Section 8 / RAP; Special / Limited Medical Benefit |
| 2    | **15%**  | **211% FPL**                                                  | CEAP Level 2; free or reduced-price school lunch; HUSKY A children; MSP / QMB; SNAP; WIC                                                               |
| 3    | **20%**  | **160% FPL**                                                  | HUSKY A parents; HUSKY D; Medicaid / Access Health                                                                                                     |
| 4    | **40%**  | **125% FPL**                                                  | CEAP Level 1                                                                                                                                           |
| 5    | **50%**  | **100% FPL**                                                  | Head Start; HUSKY C non-LTSS; Refugee Assistance; SSDI; SAGA; State Cash Assistance / State Supplement; Temporary Family Assistance                    |

Source: [PURA 2024 Annual Report, Table 14](https://portal.ct.gov/-/media/pura/2024-annual-report/pura-2024-annual-report---section-7-grid-modernization.pdf) (p. 82). Table 14 lists a per-program FPL % for each qualifying benefit (e.g. Tier 1 programs individually range 212–275% FPL); the "Income ceiling" column above collapses each tier to its outer bound for a simple lookup — see the source table directly if per-program precision matters.

**Tier 1's "212–275% FPL | 60% SMI" is one threshold in two units, not an OR condition.** The report is explicit that 60% SMI is the actual policy rule: "PURA directed the EDCs to implement a LIDR for electric customers with an **overall eligibility cap at 60% State Median Income (i.e., Tier 1)**" (p. 80). Table 14's "212–275% FPL" range is descriptive, not a second qualifying test — it's the spread of FPL-equivalent limits across Tier 1's seven individual categorical programs (ALMB/SLMB at 212%, CEAP Level 3 at the top). The CEAP Level 3 row makes the equivalence explicit by printing both figures for the same limit: `275% (60% SMI)`. Tiers 2–5, by contrast, are defined in FPL terms only — no SMI figure appears for them anywhere in Table 14. **Model Tier 1 as `≤ 60% SMI` only; do not add an independent `≤ 276% FPL` (or similar) fallback** — FPL% and SMI% don't move in lockstep across household sizes, so an FPL-based OR clause would admit households PURA never intended to cover.

**The dollar-value income table is a moving target — do not hard-code it.** Federal Poverty Guidelines update annually (~January) and Connecticut's State Median Income figure updates on a similar annual cycle, so the utilities' published $-by-household-size tables drift year to year even though the %FPL/%SMI tier definitions above stay fixed. I pulled three snapshots within a 15-month span and got three different tables, confirming this:

| Source                                                                                                                                                                                                    | As of     | HH size 1, Tier 5 (50%) ceiling | HH size 4, Tier 5 (50%) ceiling |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ------------------------------- | ------------------------------- |
| [Eversource rollout fact sheet](https://woodburyct.org/vertical/Sites/%7B59751637-3DF2-41D3-B20A-866E470B1D1D%7D/uploads/Discount_Rate_Fact_Sheet_Handout_May2025(1).pdf) ("MN0525", i.e. May 2025 print) | May 2025  | `$15,060`                       | `$31,200`                       |
| [UI LIDR page](https://www.uinet.com/low-income-discount-rate), [Wayback Machine snapshot](https://web.archive.org/web/20260613085954/https://www.uinet.com/low-income-discount-rate)                     | June 2026 | `$15,650`                       | `$32,150`                       |
| [UI LIDR page](https://www.uinet.com/low-income-discount-rate), live                                                                                                                                      | Sept 2026 | `$15,960`                       | `$33,000`                       |

The Eversource-authored fact sheet above is important on its own: Eversource's [bill-help fact sheet PDF](https://www.eversource.com/docs/default-source/my-account/bill-help-fact-sheet.pdf) — cited previously as confirming "the same five discount percentages and 60% SMI ceiling" — actually only confirms the **percentages** (_"You may be eligible for a 5%, 15%, 20%, 40% or 50% discount"_) and a **single blanket 60%-SMI income line** used generically for hardship/discount/other-program qualification; it does **not** break that down into a five-tier dollar table. The rollout handout above (an Eversource-produced, dated customer-facing PDF distributed to CT municipalities, republished by the Town of Woodbury, CT) is the actual source that shows Eversource independently published its own full five-tier $-by-household-size table, matching the same structure as UI's.

Below is the **most recent snapshot** (UI's live page, as of Sept 2026), used as the working table for now — replace it whenever this doc is revisited, and see the "Still needed" item below on parameterizing this instead of hard-coding a vintage:

| Household size | Tier 1: 5% | Tier 2: 15% | Tier 3: 20% | Tier 4: 40% | Tier 5: 50% |
| -------------- | ---------- | ----------- | ----------- | ----------- | ----------- |
| 1              | `$48,714`  | `$33,676`   | `$25,536`   | `$19,950`   | `$15,960`   |
| 2              | `$63,703`  | `$45,660`   | `$34,624`   | `$27,050`   | `$21,640`   |
| 3              | `$78,692`  | `$57,645`   | `$43,712`   | `$34,150`   | `$27,320`   |
| 4              | `$93,681`  | `$69,630`   | `$52,800`   | `$41,250`   | `$33,000`   |
| 5              | `$108,669` | `$81,615`   | `$61,888`   | `$48,350`   | `$38,680`   |
| 6              | `$123,658` | `$93,600`   | `$70,976`   | `$55,450`   | `$44,360`   |
| 7              | `$126,469` | `$105,584`  | `$80,064`   | `$62,550`   | `$50,040`   |
| 8              | `$129,279` | `$117,569`  | `$89,152`   | `$69,650`   | `$55,720`   |

Income thresholds are nested: assign the **highest discount for which the household qualifies** by income or categorical program.

Enrollment is available year-round through the utility, a local CAA, DSS, Operation Fuel, or direct income documentation. PURA's [2024 Annual Report](https://portal.ct.gov/-/media/pura/2024-annual-report/pura-2024-annual-report---section-7-grid-modernization.pdf) describes a monthly, opt-out EDC–DSS data-sharing process: the utilities send active residential accounts to DSS, DSS identifies benefit-program eligibility, and qualifying customers are auto-enrolled into the appropriate tier. Eligibility lasts 12 months and must be verified annually; a customer may submit proof at any point to receive a higher tier.

**Decisions made for implementation** (`lmi_common.assign_ct_lidr_tier_expr` / `utils/post/data/ct_lidr.yaml`):

- Tier assignment is **income-only** (FPL%/SMI% thresholds from `ct_lidr.yaml`), not by simulating categorical program enrollment — since Table 14's per-program FPL ranges mostly nest inside the income-based tier bands anyway, this is a defensible simplification, consistent with how other states' LMI modules in this codebase assign tiers from income directly.
- The config encodes **%FPL / %SMI boundaries, discount fractions, and the DSS/HHS LIHEAP 60% SMI dollar table** (not HUD SMI). HUD statewide median family income is a different series (different 4-person median and different household-size factors) and would understate a 4-person Tier 1 cap by about `$19,000`. FPL% still uses HHS poverty guidelines for `--fpl-year`. Pair `--fpl-year 2026` with this vintage so inflated ResStock income, 2026 FPL, and the FFY 2027 60% SMI table are in the same year's dollars. When DSS/HHS publish a new 60% SMI table, replace `smi.by_household_size` in `ct_lidr.yaml` and bump `dollar_year` / `fpl_guideline_year`.

**Still needed for coding:**

- [x] Confirm the guideline vintage / annual update source (Federal Register poverty guidelines + CT SMI publication) matches the FPL/SMI-year semantics already used by `--fpl-year` for other states — resolved: Tier 1 uses the DSS/CEAP 60% SMI table (HHS LIHEAP IM-2026-01 / FFY 2027), not HUD SMI; FPL uses HHS FPG for `--fpl-year` (2026 added to `fpl_guidelines.yaml`).

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

**Why CEAP and Operation Fuel are not modeled:**

- **CEAP** requires an annual individual application through a local Community Action Agency. Each application takes 30–45 minutes and is reviewed individually; benefit amounts vary by household size, income, and heating source (`$355`–`$705` for the 2026–2027 season). Critically, CEAP is **not** auto-enrolled: "CEAP is an annual benefit, so you must apply each year to receive assistance" ([CT CEAP page](https://portal.ct.gov/heatinghelp/connecticut-energy-assistance-program-ceap)). Even households already receiving qualifying government benefits (SNAP, TANF, SSI) must separately apply for CEAP through a CAA ([PURA PA 2026, p. 3](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p3/a2826854); [New Opportunities Inc.](https://newoppinc.org/Service/CT-Energy-Assistance-Program)). This individual-application, variable-benefit, seasonal structure is incompatible with our modeling approach of applying a uniform discount percentage to all income-eligible households in a population-wide steady-state simulation.
- **Operation Fuel** (Generation Power CT) is one-time crisis assistance (up to `$500`/year) for households experiencing acute financial hardship such as job loss, not a recurring annual benefit. It "provides one-time help to families and individuals in financial crisis" ([New Opportunities Inc.](https://newoppinc.org/Service/CT-Energy-Assistance-Program)). Its crisis-driven, non-recurring nature makes it inappropriate for steady-state bill modeling.
- Both programs could theoretically be added later as independently toggleable components if defensible benefit-allocation assumptions (flat vs. variable benefit) and participation rates (application-based enrollment, not auto-enrollment) are developed. No `--ceap` or `--operation-fuel` CLI flags exist yet.

---

## 3. How discounts / benefits are applied

### 3.1 LIDR bill mechanics

The current five-tier structure is:

| Fact                             | Value                                                                                                                                                                                                                         | Source                                                                                                                                                                                                                                                                       |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Form                             | Percentage **on-bill discount**                                                                                                                                                                                               | PURA PA 2026                                                                                                                                                                                                                                                                 |
| Magnitude                        | **5%, 15%, 20%, 40%, or 50%** by tier — same for both utilities, set by [PURA 2024 Annual Report, Table 14](https://portal.ct.gov/-/media/pura/2024-annual-report/pura-2024-annual-report---section-7-grid-modernization.pdf) | PURA 2024 Annual Report; corroborated by UI's LIDR page and Eversource's own rollout fact sheet (§2.2)                                                                                                                                                                       |
| Bill / fuel                      | Residential **electric bill only**; no natural-gas LIDR                                                                                                                                                                       | PURA PA 2026; utility pages                                                                                                                                                                                                                                                  |
| Bill base                        | PURA's establishing decision describes the discount as applying to the **total monthly electric bill**; current utility materials call it the monthly electric bill / current monthly charges                                 | PURA Docket 17-12-03RE11 decision and FAQ; utility pages                                                                                                                                                                                                                     |
| Usage cap — non-electric heating | Discount limited to **800 kWh per month**                                                                                                                                                                                     | [PURA PA 2026](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826850)                                                                                                                                                  |
| Usage cap — electric heating     | Discount limited to **1200 kWh per month**                                                                                                                                                                                    | [PURA PA 2026](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826850)                                                                                                                                                  |
| Eversource effective date        | Five-tier design effective **June 23, 2025**                                                                                                                                                                                  | Eversource rollout fact sheet ("in June... expanding to five discount tiers"); date confirmed as June 23 by Eversource's customer notice, republished by [CT House GOP](https://www.cthousegop.com/stewart/blog/eversource-energy-low-income-discount-rate-lidr-information) |
| UI effective date                | Five-tier design effective **August 3, 2026**                                                                                                                                                                                 | UI current LIDR page                                                                                                                                                                                                                                                         |

The 800/1200 kWh cap is double-checked directly against PURA's own program-comparison table (not just the DocumentCloud annotation), which states the mechanic in one sentence:

> "Discount limited to **800 kWh** for non-electric heating customers and **1200 kWh** for electric heating customers **per month**."
> — [PURA PA 2026, LIDR row](../../sources/utility-bill-payment-assistance---2026.md), p. 1

This confirms the cap depends on a **heating-fuel flag** (electric heat vs. not), which is a standard ResStock attribute, so building the flag itself isn't a gap. What's still unresolved is the _arithmetic_ the cap implies on a bill with tiered/seasonal volumetric rates and a fixed charge — see below.

**How the cap is applied:** The discount applies to the **whole bill** (fixed charge + volumetric charges), but the dollar discount is capped at what a customer using exactly 800 or 1200 kWh would receive on their total bill (including the fixed charge). In other words, the cap prevents high-usage households from receiving unbounded discounts, but every household — including those under the cap — has their fixed charge discounted alongside their volumetric charges.

The formula in `apply_ct_lidr_to_master_bills.py` is:

```
bill_at_cap = fixed_charge + avg_volumetric_rate * cap_kwh
discount = discount_pct * min(total_bill, bill_at_cap)
```

- **At or below the cap** (usage ≤ 800/1200 kWh): `total_bill ≤ bill_at_cap`, so the discount is simply `discount_pct × total_bill` — the full bill is discounted.
- **Above the cap** (usage > 800/1200 kWh): `total_bill > bill_at_cap`, so the discount is `discount_pct × bill_at_cap` — the discount stops growing beyond what an 800/1200 kWh customer would receive.

`avg_volumetric_rate` is the building-month's actual average $/kWh (`(elec_total_bill - elec_fixed_charge) / elec_grid_kwh`), since master bills carry only aggregate bill totals, not the underlying tariff's rate schedule. This average-rate approximation is exact for a flat volumetric rate but an approximation for any tiered/seasonal block rate (e.g. Eversource's optional HP-heating Rate 6 has a 700 kWh winter block break — see `exhibit_clp_rates_3_residential_tariffs.md`).

PURA's establishing decision describes the discount as applying to the "total monthly electric bill" / "current monthly charges," which naturally includes the fixed charge. The usage cap limits the _extent_ of the discount, not its _scope_ — it prevents high-usage households from receiving unbounded discounts, rather than carving the fixed charge out of the discount base entirely.

**Alternative interpretation (not implemented):** One could read PURA's "discount limited to 800/1200 kWh" as applying _only_ to the volumetric charge for the first 800/1200 kWh, with the fixed charge never discounted. Under that reading the formula would be `discount = discount_pct × avg_rate × min(usage, cap)` — no fixed charge in the discount base. The two approaches differ by exactly `discount_pct × fixed_charge` per household per month. Since the fixed charge is a small share of the total bill (~`$10`–`$20`/month vs. `$100`–`$300`+ in volumetric charges), the practical difference is modest (on the order of `$1`–`$10`/month per household). A utility customer-facing FAQ uses language that could support either reading:

> "How the Discount Rate Is Applied to Your Bill — Heating customers: If you qualify, your next electric bill will have a discount applied to the first 1,200 kWh of your monthly electric usage. On the monthly electric bill example below, if a customer uses 1,450 kWh of electricity in August, the first 1,200 kWh will be discounted, but the remaining 250 kWh will not."
> — <!-- TODO: source URL/page title pending; utility (Eversource or UI) customer FAQ, "How the Discount Rate Is Applied to Your Bill" section, heating-customer 1,200 kWh example -->

**Still needed:**

- [ ] Pin down the exact source document/URL for the FAQ quote above (utility, page title, fetch date) to complete the citation.
- [ ] Confirm whether the tiered/seasonal block-rate approximation materially changes results for HP-heating rate classes.
- [ ] Confirm treatment of third-party supplier charges under consolidated billing

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

PURA's 2024 Annual Report confirms three enrollment pathways:

1. **Automatic DSS data match:** each utility sends active residential accounts to DSS monthly; DSS identifies qualifying benefit participation and tier.
2. **Assisted enrollment:** utility, local CAA, DSS, or Operation Fuel.
3. **Income documentation:** customers not identified through benefits may submit household income directly.

Enrollment lasts **12 months** and is reverified annually. Customers can provide proof during that period to move to a higher discount tier.

### 4.1 Actual Eversource enrollment by tier (calendar year 2025)

Eversource reports LIDR enrollment annually to PURA under Order 10 of Docket 17-12-03RE11, filed into the RAM docket. The calendar-year 2025 report is [`ES Order Compliance 10 (March 2026).pdf`](https://www.dpuc.state.ct.us/dockcurr.nsf/8e6fc37a54110e3e852576190052b64d/c2c4fb73fe0ddda785258daf005d1910/$FILE/ES%20Order%20Compliance%2010%20(March%202026).pdf), filed 03/03/2026 in **Docket 26-01-03** (Annual Review of the Rate Adjustment Mechanisms of CL&P). Its response to sub-part (c), "The number of customers enrolled in each LIDR Tier":

| Tier | Discount | Eversource customers enrolled (2025) |
| ---- | -------- | ------------------------------------ |
| 1    | 5%       | 20,211                               |
| 2    | 15%      | 57,524                               |
| 3    | 20%      | 66,438                               |
| 4    | 40%      | 2,233                                |
| 5    | 50%      | 52,984                               |
| All  | —        | **199,390**                          |

The same filing reports total LIDR implementation cost of **$107,883,542** (of which $102,564,630 is the discount itself), against total billed revenues of **$4.9 B** — **2.18%** of revenue, above the 1.6%-of-revenue budgetary target PURA used when sizing the five tiers (§2.2).

No equivalent UI count has been located; UI's March 2026 RAM filing in Docket 26-01-04 does not carry the Order 10 exhibit, and UI only moved to the five-tier design in August 2026.

### 4.2 Implied participation rate

There is no published "eligible customers" denominator, so we estimate it from Census PUMS against the same income rules the model applies (`utils/post/data/ct_lidr.yaml`). Using ACS 1-year 2023 CT housing microdata (`s3://data.sb/census/pums/acs1/2023/housing/state=CT/`), restricted to occupied housing units, with household income inflated to 2026 dollars by CPI-U (`s3://data.sb/fred/cpi/`, 330.5/304.7 = 1.085) and compared to the 60% HHS LIHEAP SMI table by household size:

- CT occupied households: **1,442,969**
- Households at or below 60% SMI (LIDR's outer eligibility cap): **487,511** — **33.8%** of all households

Allocating that statewide eligible count to CL&P by its share of CT residential electric accounts (1,174,420 of ~1.51 M, EIA-861 2024, `s3://data.sb/eia/861/electric_utility_stats/`) gives roughly **379,000 eligible Eversource households**. Against 199,390 enrolled:

> **Estimated Eversource LIDR participation rate ≈ 53% (range 45–60%).**

**Modeling defaults:** CT follows the same two-rate pattern as RI (p100 / p40), NY (p100 / p40), and MD (p100 / p48):

| Scenario | Rate | Role                                                   |
| -------- | ---- | ------------------------------------------------------ |
| p100     | 100% | Policy / full take-up among income-eligible households |
| p53      | 53%  | Observed-behavior case — the second production default |

Wired as `DEFAULT_PARTICIPATION_RATES = [1.0, 0.53]` in `apply_ct_lidr_to_master_bills.py` and as `--lmi-participation-rates 1.0 0.53` in `rate_design/hp_rates/ct/Justfile` (`run-with-lmi`). Use a **single overall rate**, not tier-specific take-up: utilities assign tiers by categorical benefit match, so enrolled-by-tier ÷ PUMS-eligible-by-tier is not identifiable (Tier 3 would exceed 100%, Tier 4 would be 7%).

**45–60% is a future sensitivity range, not a default column set.** The range is the uncertainty on the denominator (see caveats below). When a report needs a robustness check around the observed-behavior case, rerun with `--lmi-participation-rates 1.0 0.45 0.53 0.60` — do not bake 45/60 into production defaults.

Caveats that set the range:

- **Territory allocation is uniform.** UI's service territory (New Haven, Bridgeport) is poorer than CL&P's, so CL&P's true low-income share is likely below 33.8%, which would push participation _above_ 53%.
- **Master-metered units inflate the denominator.** Eligible households without their own electric account cannot enroll, which also biases the estimate low.
- **PUMS is a 2023 sample inflated to 2026 dollars**, not a 2026 measurement.

**Still needed:**

- [ ] UI LIDR enrollment by five-tier level (its Order 10 equivalent, or Docket 26-05-01)
- [ ] A territory-resolved eligible denominator (PUMA-to-utility crosswalk) to tighten the 45–60% range
- [ ] Hardship and MPP counts

---

## 5. Implications for rate-design modeling

Implementation implications from the sources reviewed above:

| Topic                    | Implication from this source                                                                                                                                                                                                                                                             | Confidence                                                             |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Default component        | **LIDR on** (electric % discount on whole bill with kWh-based cap)                                                                                                                                                                                                                       | High                                                                   |
| Excluded: CEAP           | **Not implemented.** Requires annual individual CAA application; variable benefit amounts; not auto-enrolled — incompatible with population-wide steady-state modeling. Could be added as a toggleable component later if benefit-allocation and participation assumptions are developed | Design decision (§2.4)                                                 |
| Excluded: Operation Fuel | **Not implemented.** One-time crisis assistance, not a recurring benefit — incompatible with steady-state bill modeling. Could be added as a toggleable component later if participation assumptions are developed                                                                       | Design decision (§2.4)                                                 |
| Excluded: MPP            | **Not implemented.** Arrearage matching / forgiveness rather than a recurring current-bill discount                                                                                                                                                                                      | High                                                                   |
| Gas LIDR?                | No gas on-bill LIDR in this table; gas appears under **MPP** (and CEAP if gas heat)                                                                                                                                                                                                      | High for “no gas LIDR row”; confirm no separate gas discount elsewhere |
| Discount shape           | Percentage (**5–50%**), **not** NY-style fixed `$`/month credit ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826849))                                                                                      | High                                                                   |
| Cap                      | Need heating-type flag (electric vs. non-electric heat) to choose **800** vs. **1200** kWh/mo ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/#document/p1/a2826850))                                                        | High                                                                   |
| Eligibility for ResStock | Income supports direct placement into the highest qualifying tier; categorical benefit pathways cannot all be observed in ResStock, so tiers are assigned income-only (documented simplification, §2.2)                                                                                  | High for income placement; design choice for categorical eligibility   |
| Participation sampling   | **p100 and p53** are the production defaults (`[1.0, 0.53]`), matching RI/NY/MD. 53% is observed Eversource take-up (§4.2). Keep **45–60% as a later sensitivity range**, not extra default columns                                                                                      | High for the two-rate pattern; 53% is an estimate (§4.2)               |
| Script / wiring          | **Implemented**: `apply_ct_lidr_to_master_bills.py` (+ `ct_lidr.yaml` config), reusing `lmi_common.py`, dispatched from `build_master_bills_prefect.py` for `state_upper == "CT"`                                                                                                        | Implemented                                                            |

---

## 6. Key open questions

1. **(Resolved)** How do the current tariffs prorate fixed and volumetric discounts when usage exceeds the **800 / 1200 kWh cap**? The discount applies to the whole bill (fixed + volumetric), capped at what a cap-kWh customer's total bill would yield (§3.1). Uses a locally-linear-rate approximation since master bills carry only aggregate totals. Still need the exact FAQ source URL to complete the citation (§3.1).
2. Are third-party supply charges discounted under consolidated billing?
3. **(Resolved for Eversource)** What is the current enrollment by utility and five-tier level? Eversource's CY2025 counts by tier (199,390 total) are in its Order 10 compliance filing in Docket 26-01-03 — see §4.1. UI's equivalent is still outstanding.
4. **(Resolved)** What observed **participation rate** should be used for sampling? Production defaults are **p100 and p53** (`[1.0, 0.53]`), matching RI/NY/MD. 53% is Eversource enrolled ÷ estimated eligible (§4.2). Keep **45–60% as a future sensitivity range** around that estimate; do not emit those as default columns.
5. **(Resolved — excluded from scope)** What allocation and participation assumptions are defensible for CEAP and Operation Fuel? CEAP's annual individual-application structure and Operation Fuel's one-time crisis nature are incompatible with population-wide steady-state modeling (see §2.4). These programs are not implemented and have no CLI toggles. They could theoretically be added later if defensible benefit-allocation and participation assumptions are developed.
6. **(Resolved)** How should the income-ceiling dollar table be parameterized so it doesn't silently go stale? FPL% still uses `--fpl-year` + `fpl_guidelines.yaml`. Tier 1 SMI% uses the DSS/CEAP **60% HHS LIHEAP SMI** table in `ct_lidr.yaml` (not HUD SMI) — replace `smi.by_household_size` and bump `dollar_year` when HHS/DSS publish a new vintage. Pair `--fpl-year` with `smi.dollar_year` (currently 2026).

---

## Appendix A. Research sources — where to look next

### A.1 Official program overviews

| Source                                                                                                                                                                                                                                                       | Why look here                                                                                                                                      |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **[PURA PA 2026 on DocumentCloud](https://www.documentcloud.org/documents/28561820-utility-bill-payment-assistance-2026/)** / [markdown extract](../../sources/utility-bill-payment-assistance---2026.md) / `dev/utility-bill-payment-assistance---2026.pdf` | **Already used** for §§1–3 above                                                                                                                   |
| [PURA — Payment Assistance Programs](https://portal.ct.gov/pura/consumer-services/payment-assistance-programs)                                                                                                                                               | Hardship detail, winter/medical protection, legacy New Start / BFP                                                                                 |
| [CT Heating Help — utility payment plans](https://portal.ct.gov/heatinghelp/utility-assistance-information)                                                                                                                                                  | Plain-language LIDR tier summary                                                                                                                   |
| [Eversource bill-help fact sheet (PDF)](https://www.eversource.com/docs/default-source/my-account/bill-help-fact-sheet.pdf)                                                                                                                                  | Discount %, single blanket 60%-SMI hardship line, MPP examples — **no** per-tier dollar table                                                      |
| [Eversource rollout fact sheet, May 2025 (republished by Town of Woodbury, CT)](https://woodburyct.org/vertical/Sites/%7B59751637-3DF2-41D3-B20A-866E470B1D1D%7D/uploads/Discount_Rate_Fact_Sheet_Handout_May2025(1).pdf)                                    | Eversource's own full five-tier $-by-household-size table at rollout; June 2025 timing confirmation                                                |
| [UI — Low-Income Discount Rate](https://www.uinet.com/low-income-discount-rate)                                                                                                                                                                              | Current five-tier income table, categorical programs, verification (updates annually — check fetch date)                                           |
| [PURA 2024 Annual Report — Section 7](https://portal.ct.gov/-/media/pura/2024-annual-report/pura-2024-annual-report---section-7-grid-modernization.pdf)                                                                                                      | **Primary source for the shared tier framework** — Table 14 (discount %, FPL/SMI bands, qualifying programs); DSS data sharing; enrollment history |

### A.2 Regulatory dockets (tiers + participation)

| Source                                                                                                 | Why look here                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **PURA Docket 17-12-03RE11**                                                                           | LIDR design / five-tier redesign                                                                                                                                             |
| **PURA Docket 25-05-01**                                                                               | Affordability annual review                                                                                                                                                  |
| Eversource / UI **RAM** / public-benefits filings                                                      | Enrollment and cost statistics — **Eversource's Order 10 compliance exhibit is the one that carries per-tier enrollment**; see §4.1 for the CY2025 filing in Docket 26-01-03 |
| [PURA Q1 2025 Newsletter](https://portal.ct.gov/-/media/pura/1---website-media/q1-2025-newsletter.pdf) | Notes 5/15/20/40/50% LIDR modification                                                                                                                                       |

### A.3 In-repo context

| File                                                                                                 | Role                                                           |
| ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| [utility-bill-payment-assistance---2026.md](../../sources/utility-bill-payment-assistance---2026.md) | Extract of **PURA PA 2026** (primary fill source for this doc) |
| [ct_electric_bill_components.md](ct_electric_bill_components.md)                                     | SBC / hardship cost recovery (OCC) — for §3.3                  |
| [ct_residential_charges_in_bat.md](../methods/bat_mc_residual/ct_residential_charges_in_bat.md)      | BAT charge classification for LMI recovery                     |
| [resstock_lmi_metadata_guide.md](../code/data/resstock_lmi_metadata_guide.md)                        | ResStock → FPL%/SMI% for tier assignment                       |
| [lmi_master_bills_workflow.md](../code/orchestration/lmi_master_bills_workflow.md)                   | NY master-bills LMI wiring pattern                             |

### A.4 Remaining research order

1. Pull current LIDR tariff / billing specifications for 800 / 1200 kWh cap mechanics.
2. ~~Pull 2026 RAM / affordability filings for current enrollment by utility and tier.~~ Done for Eversource (§4.1); UI still outstanding.
3. ~~Implement `apply_ct_lmi_*` with LIDR on by default.~~ Done — `apply_ct_lidr_to_master_bills.py`. CEAP and Operation Fuel are excluded from scope (§2.4); MPP is excluded as arrearage treatment.

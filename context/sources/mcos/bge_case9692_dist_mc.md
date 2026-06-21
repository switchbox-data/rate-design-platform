# BGE distribution marginal cost: source citations (MD PSC Case No. 9692)

**Source proceeding**: Maryland PSC Case No. 9692 — BGE Multi-Year Plan (MYP 2), forecast period 2024–2026
**Utility**: Baltimore Gas & Electric (BGE), Maryland / PJM
**Compiled**: 2026-06 (curated citation digest, not a full PDF extraction — the underlying BGE filings are not stored in this repo)

> This file collects the verbatim, citation-bearing passages from BGE's Case 9692 filings that
> establish (a) the avoided distribution cost figure we adopt as BGE's `sub_tx_and_dist` marginal
> cost, and (b) the supporting evidence that BGE itself conceives of distribution cost as
> peak-driven (the AIC / avoided-cost view). See
> `context/methods/marginal_costs/md_bge_dist_mc_methodology.md` for how these are used and
> `context/methods/marginal_costs/dist_mc_definition_choice.md` for the cross-platform definition
> choice and tension.

---

## 1. Brattle avoided distribution cost: `$32/kW-yr` (2022$) — the figure we adopt

**Primary citation**: `001_2_BGE MYP 2_Case Direct Testimony_FinaL_w Att.pdf`, Appendix B.2.b / Brattle Group report (p. 53 of the Brattle report; **p. 138 of the testimony PDF**). @bge_Docket9692Direct_2023

**Direct source link**: <https://www.documentcloud.org/documents/28269392-001-2-bge-myp-2-case-direct-testimony-final-w-att/#document/p138/a2821808>

> "These estimates are obtained by dividing the replacement value of the distribution system by
> peak demand, leading to an overnight cost in $/kW that represents average system conditions as of
> 2021. To levelize this cost, we apply an economic carrying charge of 12%, arriving at $32/kW-year
> as the avoided distribution cost."

Key facts for our use:

- **Value**: $32/kW-yr, electric **distribution** (Maryland-jurisdictional; includes 34 kV sub-transmission — see §5).
- **Dollar year**: 2022$ (the Brattle benefit-cost analysis is expressed in 2022 dollars; the underlying replacement value reflects 2021 system conditions).
- **Already levelized**: the 12% economic carrying charge has been applied by Brattle. We do **not** re-annualize; we only CPI-inflate 2022 → target year.
- **Denominator**: total system peak demand (replacement value ÷ peak) — i.e. an avoided / AIC-style figure, not capital-over-system-peak FLIC.

**Independent corroboration** — Office of People's Counsel (OPC) witness Kenji Takahashi quotes the
same Brattle figure as a range:

> "$25.1 to $34.09 per kW-year"

Citation: `047_Takahashi Testimony - BGE Electrification FINAL PUBLIC.pdf`, Direct Testimony of Kenji Takahashi, p. 14.

---

## 2. E3 combined T&D capacity cost: `$203–258/kW-yr` (2022$) — upper-bound sensitivity only

**Citation**: `Petrenko - BGE Integrated Decarbonization Strategy.pdf`, Table 26 (value) and p. 79 (method).

> "E3 assessed incremental transmission and distribution capacity costs based on changes in BGE's
> system peak, after load flexibility, using a 1-in-10 year planning standard… transmission and
> distribution capacity costs were provided by BGE as an overnight cost and levelized by E3 using
> expected asset lifetimes and BGE's weighted average cost of capital."

Caveats:

- This is **combined transmission + distribution** and includes FERC-jurisdictional bulk transmission. Using it for the `sub_tx_and_dist` bucket would **double-count** a separately-derived PJM bulk-TX bucket. Hence it is a sensitivity ceiling, not the primary figure.
- Allocated to "Customer group's contribution to coincident 1-in-10 peak" (peak-driven, like Brattle).

---

## 3. BGE classifies distribution plant as demand-related, allocated on NCP peak (ECOSS)

**Citation**: `001_11_BGE MYP_ONeill Direct_FINAL-F-wATT.pdf` (Witness O'Neill) — <https://www.documentcloud.org/documents/28269436-001-11-bge-myp-oneill-direct-final-f-watt/>.

Classification (p. 9):

> "Distribution costs are primarily classified between demand and customer-related components."

Allocation of 34 kV / 13 kV / secondary plant (p. 15):

> "In BGE's ECOSS, distribution 34kV, 13kV, and secondary plant and associated O&M components are
> classified as demand-related and allocated to the customer classes based on each class's
> contribution to the total NCP kW, at their respective voltage levels."

Cost-causation framing (p. 16):

> "As a primary allocator of demand components, NCP is used in the ECOSS to reflect how substations
> and distribution feeders are planned and sized. Distribution feeders are planned and sized based
> primarily on substation load center peak demands — as opposed to total system peak demand…"

(p. 18):

> "The Company plans and builds distribution substations and distribution feeders to ensure that
> sufficient capacity is available to meet peak customer loads whenever they may occur."

The ECOSS is an **embedded** (historical/average) study (p. 6, 10): it "identifies electric
distribution system embedded costs for the 2021 calendar year." BGE's *marginal* numbers (Brattle,
E3) are computed outside the ECOSS.

---

## 4. Rate-design recovery is volumetric — the intervention hook

**Citation**: `001_12_BGE MYP 2 Fiery Direct Testimony Rate Design_F w Att.pdf` (Witness Fiery) — <https://www.documentcloud.org/documents/28269435-001-12-bge-myp-2-fiery-direct-testimony-rate-design-f-w-att/>.

Peak cost-causation (p. 14):

> "In fact, the Company's cost to serve a customer is related more to that customer's peak demand
> than the total amount of energy used by that customer over a period of time. This is primarily due
> to the fact that substations and other distribution equipment must be sized to meet the demand of
> the customer peak load in order to maintain reliability."

Over-recovery through volumetric charges (pp. 15–16):

> "The rate schedules for all customer classes… include a volumetric component which currently
> recovers a significant amount of the distribution portion of the customer bill (approximately 82%
> and 69% for electric and gas residential customers, respectively) — a greater percentage than what
> the ECOSS and GCOSS support being recovered through volumetric rates."

> "a large portion of fixed costs are recovered through the variable charges on a customer's bill."

This is the cross-subsidy story in BGE's own words: peak-driven (largely fixed/residual)
distribution cost recovered ~82% volumetrically for residential electric customers.

---

## 5. Sub-transmission treatment vs. FERC transmission

**Citation**: `001_11_BGE MYP_ONeill Direct_FINAL-F-wATT.pdf` (<https://www.documentcloud.org/documents/28269436-001-11-bge-myp-oneill-direct-final-f-watt/>); `001_12_BGE MYP 2 Fiery Direct Testimony Rate Design_F w Att.pdf` (<https://www.documentcloud.org/documents/28269435-001-12-bge-myp-2-fiery-direct-testimony-rate-design-f-w-att/>).

BGE's ECOSS segments the distribution voltage levels as:

1. sub-transmission voltages at 34 kV;
2. primary voltages at 13 kV;
3. secondary voltage.

Electric **transmission** costs (FERC-jurisdictional) are excluded from the distribution ECOSS.
BGE's Transmission Service tariffs apply to customers served at **115 kV and above**. Therefore
34 kV sub-transmission is part of Maryland-jurisdictional **distribution**, and Brattle's
"distribution system" replacement value (§1) covers it — so the `$32/kW-yr` figure is the correct
match for our combined `sub_tx_and_dist` bucket without overlapping bulk transmission.

---

## 6. System peak (for hourly allocation reference)

**Citation**: `001_11_BGE MYP_ONeill Direct_FINAL-F-wATT.pdf`, p. 12 — <https://www.documentcloud.org/documents/28269436-001-11-bge-myp-oneill-direct-final-f-watt/>.

- Historical coincident peak: **6,102 MW**, on **August 12, 2021, hour ending 18:00**.
- A system-wide *forecasted* peak by year for 2024–2026 was **not found** in these filings.

---

## Common metadata

- **Forecast period**: calendar years 2024–2026 (fully-forecasted Multi-Year Plan).
- **Historical test year** (ECOSS allocators): 12 months ending December 31, 2021.
- **Dollar year of avoided-cost figures**: 2022$.

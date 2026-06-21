# Distribution + sub-transmission marginal cost: which LRMC definition, and why it differs by state

This is a cross-cutting reference for the single most consequential choice in the `sub_tx_and_dist`
marginal-cost input to the Bill Alignment Test (BAT): **which definition of long-run marginal cost
(LRMC) we use**. The choice swings the input — and therefore every BAT cross-subsidy result — by an
order of magnitude. The platform currently makes _different_ choices in different states, so this
doc states plainly what is implemented where, preserves the source numbers for every alternative,
and explains the tension and the rationale.

For the underlying LRMC framework see
[bat_lrmc_residual_allocation_methodology.md](../bat_mc_residual/bat_lrmc_residual_allocation_methodology.md)
(§3 taxonomy, §7 distribution, §9 steady-state assumption). For the BGE-specific implementation see
[md_bge_dist_mc_methodology.md](md_bge_dist_mc_methodology.md). For the NY derivation see
[ny_dist_mc_bat_methodology.md](ny_dist_mc_bat_methodology.md).

---

## 1. The three definitions, ordered by magnitude

Per the methodology doc §3, "LRMC is lower than average cost" is universal, but the _magnitude_ of
the gap depends entirely on which definition is used. From largest to smallest:

1. **Perturbation / Turvey LRMC** (largest). The change in total discounted system cost from
   optimally accommodating a permanent 1 MW demand increase, including cascading reinforcements at
   all voltage levels. Recovers 49–111% of total cost in UK/Australian studies (Brown & Faruqui
   2014). Requires a full network expansion model; rarely available.

2. **Average Incremental Cost (AIC) / avoided cost** (intermediate–large). The capital cost of a
   defined increment divided by the capacity it serves:

   $$\text{AIC} = \frac{\Delta C}{\Delta \text{MW}}$$

   This is "cost per kW of peak" — what one more kW of peak _causes_, or equivalently what one kW
   of peak reduction _avoids_. The UK CDCM (500 MW increment), the CPUC Avoided Cost Calculator,
   the New England AESC avoided-T&D values, and utility "avoided distribution cost" studies all sit
   here.

3. **Forward-Looking Incremental Cost (FLIC)** (smallest). The capital of _specific planned
   projects_ entering service in a near-term window, annualized and divided by **total system
   peak**:

   $$\text{FLIC} = \frac{\Delta C_{\text{planned}} \times r}{\text{system peak}}$$

   The narrowest definition: only the marginal slice of the pipeline. Recovers ~2–15% of the
   revenue requirement.

### Why AIC and FLIC differ ~10–50×

Both annualize capital and divide by a peak, but the **denominator differs**:

$$\frac{\text{FLIC}}{\text{AIC}} \;=\; \frac{\Delta C_{\text{planned}}/\text{system peak}}{\Delta C/\Delta\text{MW}} \;\approx\; \frac{\Delta \text{MW}}{\text{system peak}} \;=\; \text{annual peak-growth rate}.$$

If peak grows ~1–3%/yr, FLIC is ~1–3% of AIC. That is the entire reason NY's `$1–8/kW-yr` and
RI's/BGE's `$30–80/kW-yr` look incompatible: **they are different quantities, not different
estimates of the same quantity.** FLIC asks "what's this year's incremental investment spread over
everyone?"; AIC/avoided asks "what does a marginal kW of peak cost?".

---

## 2. Source numbers (all retained, for every alternative)

| Source                 | Figure ($/kW-yr) | Dollar yr | Definition                           | Scope                          | Citation                                                                                                                                                                                                                        |
| ---------------------- | ---------------- | --------- | ------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| NY MCOS (7 utilities)  | 1.16 – 8.52      | 2026      | FLIC (incremental diluted)           | sub-TX + dist                  | `ny_sub_tx_and_dist_mc_levelized.csv`; [ny_dist_mc_bat_methodology.md](ny_dist_mc_bat_methodology.md)                                                                                                                           |
| RI AESC 2024 (RIE)     | 80.24            | 2019      | AIC / avoided                        | dist (non-PTF)                 | `ri_marginal_costs_2025.csv`                                                                                                                                                                                                    |
| **BGE Brattle (MD)**   | **32**           | **2022**  | **AIC / avoided**                    | **dist incl. 34 kV sub-TX**    | [BGE MYP testimony p.138](https://www.documentcloud.org/documents/28269392-001-2-bge-myp-2-case-direct-testimony-final-w-att/#document/p138/a2821808); [bge_case9692_dist_mc.md](../../sources/mcos/bge_case9692_dist_mc.md) §1 |
| BGE OPC/Takahashi (MD) | 25.1 – 34.09     | 2022      | AIC / avoided (corroborates Brattle) | dist                           | [bge_case9692_dist_mc.md](../../sources/mcos/bge_case9692_dist_mc.md) §1                                                                                                                                                        |
| BGE E3 (MD)            | 203 – 258        | 2022      | AIC / avoided                        | **combined T&D incl. FERC TX** | [bge_case9692_dist_mc.md](../../sources/mcos/bge_case9692_dist_mc.md) §2                                                                                                                                                        |
| BAT paper (PG&E)       | 54.46            | —         | AIC / avoided (CPUC ACC)             | dist CapEx                     | Simeone et al. 2023, App. A                                                                                                                                                                                                     |

Note the pattern: **every figure except NY's is an avoided/AIC number**, and they cluster at
`$25–80/kW-yr` for distribution alone (E3 is higher because it bundles FERC transmission). NY's FLIC
values are the outlier in magnitude — by construction (§1).

---

## 3. What we implement, by state (current production)

| State / utility  | Definition implemented                               | Value         | Why                                                                                                             |
| ---------------- | ---------------------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------- |
| NY (7 utilities) | **FLIC** (incremental diluted from MCOS)             | 1.16 – 8.52   | MCOS workbooks give project-level capital; PSC-blessed NERA method                                              |
| RI (RIE)         | **AIC / avoided** (AESC 2024 avoided non-PTF T&D)    | 80.24 (2019$) | AESC is the New England institutional standard; no MCOS workbook                                                |
| **MD (BGE)**     | **AIC / avoided** (Brattle replacement-value ÷ peak) | 32 (2022$)    | BGE's own COS classifies the plant 100% peak-driven; BGE's own marginal numbers use a peak denominator (see §5) |

All three feed the _same_ PoP allocator
([generate_utility_tx_dx_mc.py](../../../utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py))
in the identical `sub_tx_and_dist_mc_kw_yr` slot, with CPI inflation via the optional `dollar_year`
column.

---

## 4. The tension (stated plainly)

There are two real inconsistencies a reader should know about:

1. **NY vs RI/MD.** NY uses FLIC; RI and MD use avoided cost. These differ by ~10–50× (§1) and are
   not reconciled in code or docs. A customer with identical load would be assigned a far larger
   economic burden — and a far smaller residual — under the RI/MD convention than under NY's. This
   is an **open item for the team**: the platform should eventually either (a) standardize on one
   definition, or (b) document a principled reason the convention varies by jurisdiction (e.g. data
   availability: MCOS workbooks in NY vs published avoided-cost scalars elsewhere).

2. **Within the methodology doc itself.** [§3](../bat_mc_residual/bat_lrmc_residual_allocation_methodology.md)
   commits to FLIC and notes it recovers only 2–15% of RR; but [§7](../bat_mc_residual/bat_lrmc_residual_allocation_methodology.md)
   claims our FLIC distribution MC is "methodologically parallel" to the BAT paper's `$54.46/kW-yr`
   CPUC ACC figure. The `$54.46` is an avoided-cost (AIC) number an order of magnitude _above_ NY's
   FLIC values. The parallelism is conceptual (both are forward-looking, not embedded), but the two
   are not the same magnitude of LRMC. RI's `$80` and BGE's `$32` are actually closer to the BAT
   paper's own input than NY's `$1–8` are.

---

## 5. Rationale: why BGE (and RI) land on avoided cost

For **BGE specifically**, the avoided-cost definition is not just defensible — it is the one
consistent with BGE's _own_ cost-of-service conception, which matters because we intend to intervene
in BGE's rate case. From BGE's Case 9692 filings (see [bge_case9692_dist_mc.md](../../sources/mcos/bge_case9692_dist_mc.md)):

- BGE classifies sub-transmission (34 kV), primary (13 kV), and secondary distribution plant as
  **100% demand-related, allocated on non-coincident peak (NCP)** — because feeders and substations
  are "planned and sized based primarily on substation load center peak demands" (O'Neill, p. 15–16).
- Every marginal/avoided number BGE itself publishes uses a **peak denominator**: Brattle divides
  replacement value by peak (`$32`); E3 divides by peak _change_ (`$203–258`). Neither dilutes one
  year's capital over total system peak (FLIC). Using FLIC for BGE would adopt a definition BGE's
  own witnesses do not use.
- BGE's distribution load growth is **lumpy** (e.g. a 400 MW data-center substation, Port Covington).
  The methodology doc §9 itself warns that FLIC's steady-state assumption "is weakest during
  structural transitions… where the load shape driving future investment may differ substantially."
  Lumpy, capacity-driving growth is exactly where the per-kW-of-peak (AIC) view is the economically
  meaningful marginal cost and the diluted FLIC understates it.

For **RI**, the rationale is institutional: AESC is the New England standard avoided-cost input,
used by the RI PUC for program screening, and "already a marginal (avoided) cost, not an embedded
average" (see [ri_bulk_transmission_marginal_cost.md](ri_bulk_transmission_marginal_cost.md) §2).

### Strategic note for the rate-case intervention

The definition choice has a direction worth being explicit about. A **small** MC (FLIC) implies most
distribution cost is residual/sunk → strengthens "don't recover it volumetrically." A **large**
avoided cost implies much more is marginal. We adopt BGE's own avoided cost (`$32`) precisely so the
intervention rests on BGE's own framing; the cross-subsidy argument then comes not from a small MC
but from BGE's admission (Fiery, p. 15–16) that ~82% of residential distribution is recovered
volumetrically — "a greater percentage than what the ECOSS… support[s]."

---

## 6. Sensitivities to carry in BGE BAT runs

- **Primary**: Brattle `$32/kW-yr` (2022$ → CPI-inflated to run year).
- **Lower bound**: FLIC ~`$1.63/kW-yr` (incremental capacity-expansion capital ÷ 6,102 MW peak),
  the NY-comparable figure — useful to show how the choice moves results.
- **Upper bound**: E3 `$203–258/kW-yr` — but **only with the caveat** that it bundles FERC
  transmission and would double-count a separate PJM bulk-TX bucket; not for combined use.

# CT Residential Electric Charges: Cross-Subsidy Analysis for BAT (DRAFT)

**Status:** Draft — Genability discovery done. Eversource Rate 1: **all 16 charges are decided and settled** in `ct_eversource_charge_decisions.json` (Customer Charge, Energy Charge, Minimum Charge, ESI, RAM, Transmission, Generation Service Charge + FMCC Generation Charge ×2 seasons each, SBC, CTA, Conservation Charge + CAM, FMCC Delivery/NBFMCC, Renewable Energy Charge). Eversource Rates 5 and 7 (residential alternative tariffs, see [§ below](#eversource-rate-5--rate-7-residential-alternative-tariffs)) inherit the same decisions from Rate 1 — also fully decided. UI decisions are still hypotheses pending a UI REVREQ-equivalent filing (see [Documents still needed](#documents-still-needed)).

Connecticut has two investor-owned electric distribution utilities:

| Platform `std_name` | Brand / legal name                                        | Genability `masterTariffId` | Charge decisions file                 |
| ------------------- | --------------------------------------------------------- | --------------------------- | ------------------------------------- |
| `ct_eversource`     | Eversource Energy CT (formerly Connecticut Light & Power) | 614                         | `ct_eversource_charge_decisions.json` |
| `ct_ui`             | United Illuminating (Avangrid)                            | 3153052                     | `ct_ui_charge_decisions.json`         |

Eversource CT also offers two **other residential rate schedules** under the same Genability LSE (`lse_id` 2250) as Rate 1 — see [Eversource Rate 5 / Rate 7](#eversource-rate-5--rate-7-residential-alternative-tariffs) below:

| Platform `std_name`      | Tariff (per Genability `tariff_code`)                                             | `masterTariffId` | Charge decisions file                          |
| ------------------------ | --------------------------------------------------------------------------------- | ---------------- | ---------------------------------------------- |
| `ct_eversource_elecheat` | Rate 5, Residential Electric Heating Service (closed to new customers since 2006) | 616              | `ct_eversource_elecheat_charge_decisions.json` |
| `ct_eversource_tou`      | Rate 7, Residential Time-of-Day Electric Service                                  | 615              | `ct_eversource_tou_charge_decisions.json`      |

Default residential supply is **Standard Service** (ISO-NE wholesale costs + bypassable FMCC). Regulation is by **PURA**. Delivery riders and Public Benefits flow largely through the annual **Rate Adjustment Mechanism (RAM)** cycle (May 1 effective), separate from base distribution rates.

**Data sources (so far):**

- Charge decisions: `rate_design/hp_rates/ct/config/rev_requirement/top-ups/charge_decisions/`
- Discovered snapshots: `*_discovered.json` (effective 2025-01-01)
- Rate-case delivery RR testimony (Eversource / CL&P): [`context/sources/exhibit_clp_revreq_1.md`](../../sources/exhibit_clp_revreq_1.md) — PURA Docket **26-05-10**, Exhibit CLP-REVREQ-1 (O'Brien & Murray, July 14, 2026). Test Year CY2025; Rate Year Jul 1, 2027 – Jun 30, 2028; ~$451M deficiency excl. storms / ~$727M incl. storms (Schedule A-1.0). **Still need residential-class allocation** (SFR class / F schedules) for `delivery_rev_requirements_from_rate_cases.yaml`.
- UI rate-case RR: **TODO** (see documents list).
- Monthly rates YAML: **TODO** — run `fetch-monthly-rates` after decisions are finalized.

**How to use this doc while classifying:**

1. Work utility-by-utility through the summary tables.
2. For each row with `decision: null` (or a suspicious auto-label), fill in Decision / Why / MC 8760? after reading the tariff book or PURA filing.
3. Encode the final decision in the corresponding `*_charge_decisions.json` (`decision`, `master_charge`, `master_type`).
4. Expand the charge-by-charge analysis sections as you go (same style as `md_residential_charges_in_bat.md`).

---

## Charge type taxonomy

Charges are classified by **economic structure**, not tariff label. CT (like RI) sits in ISO-NE; the taxonomy below matches the RI/MD pattern.

| Type                          | What it is                                                                  | Cross-subsidy?                             | Decision               |
| ----------------------------- | --------------------------------------------------------------------------- | ------------------------------------------ | ---------------------- |
| **Base delivery**             | Rates set in the rate case that collect the delivery revenue requirement    | Yes (BAT core)                             | `already_in_drr`       |
| **Rate adjustment provision** | Annual charges set by rate-adjustment mechanisms outside the base rate case | Yes — fixed cost ÷ kWh                     | `add_to_drr`           |
| **Cost reconciliation**       | Uniform $/kWh true-ups of costs already embedded in base rates or supply    | No — shifts all bills equally              | `exclude_trueup`       |
| **Revenue true-up**           | Revenue decoupling / over–under collection corrections                      | No — shifts all bills equally              | `exclude_trueup`       |
| **Program surcharge**         | Fixed state-mandated program budgets recovered via uniform $/kWh or $/mo    | Yes — fixed pool ÷ kWh (or ÷ customers)    | `add_to_drr`           |
| **Sunk-cost recovery**        | Fixed debt, CTA / stranded-cost, storm, or settlement pools via $/kWh       | Yes — fixed pool ÷ kWh                     | `add_to_drr`           |
| **Transmission service**      | ISO-NE / FERC OATT pass-through                                             | Yes                                        | `add_to_drr`           |
| **Supply commodity**          | Standard Service generation (energy + capacity + ancillary bundled)         | Mixed — see sub-components                 | `add_to_srr`           |
| **Merchant / FMCC supply**    | Bypassable Federally Mandated Congestion Charges on the generation side     | Mixed / often true-up-like                 | research               |
| **RES / renewable supply**    | Per-MWh REC-style obligation that scales with load                          | No                                         | `add_to_srr` + MC 8760 |
| **LMI cost recovery**         | Recovery of low-income / energy-assistance discounts from all customers     | Yes — but income transfer, not rate design | `exclude_eligibility`  |
| **Redundant**                 | Minimum charge / bill floor equal to customer charge                        | N/A                                        | `exclude_redundant`    |

---

## The generalized cross-subsidy: fixed pools recovered volumetrically

As in NY, RI, and MD, many CT charges outside the rate-case delivery revenue requirement are **fixed annual budgets recovered via uniform $/kWh**. HP customers' higher kWh increase their dollar contribution to the same pool and reduce the per-kWh burden on non-HP customers.

Confirmed / strong CT examples from REVREQ + OCC bill-component materials:

- **Electric System Improvements (ESI)** — CapEx tracker outside base DRR (Docket 17-10-46 settlement); Docket 26-05-10 proposes rolling it into base rates effective Jul 1, 2027 (Pub. Act 23-102 §5 bars reauthorization of the on-bill CapEx tracker).
- **Systems Benefits Charge (SBC)** — affordability / hardship / LIDR / matching payment programs (OCC); some IT CapEx historically recovered through SBC RAM (REVREQ §VIII).
- **Conservation Adjustment Mechanism (CAM) / C&LM** — EE budget (OCC); C&LM GET deferred from base rates (REVREQ).
- **NBFMCC (non-bypassable FMCC)** — PURA clean-energy / congestion / program CapEx and O&M (DER Map, solar tariffs, SCEF, LIDR IT, etc. per REVREQ §VIII).
- **UI Public Benefits cluster** — Energy Efficiency, State Mandated Energy Purchases, Customer Produced Energy, Misc Mandates (Genability).

**Integration:** top up delivery (or supply) RR with `add_to_drr` / `add_to_srr` charges so BAT can allocate them under the chosen residual allocator. See `ri_residential_charges_in_bat.md` and `ny_residential_charges_in_bat.md` for the worked SBC/EE analogy.

**Timing caveat for BAT runs:** Genability discovery rates are **2025-01-01**. ESI / NBFMCC CapEx / SBC CapEx are still riders in 2025. After Docket 26-05-10 rates take effect (proposed Jul 1, 2027), those CapEx pieces move into base DRR — charge decisions for a 2027+ BAT must be revisited so we do not double-count.

---

## Eversource CT (`ct_eversource`) — summary table

16 Genability rates. Sample rates are from discovery effective **2025-01-01** (illustrative; replace with monthly_rates YAML after fetch). **All 16 are decided and encoded** in `ct_eversource_charge_decisions.json`.

| Charge (Genability)                  | tariffRateId | Unit  | Sample rate | Decision status                   | My recommendation                                                                                  |
| ------------------------------------ | ------------ | ----- | ----------- | --------------------------------- | -------------------------------------------------------------------------------------------------- |
| Customer Charge                      | 20448095     | $/mo  | 9.62        | ✅ decided: `already_in_drr`      | (no change)                                                                                        |
| Energy Charge                        | 20448096     | $/kWh | 0.05844     | ✅ decided: `already_in_drr`      | (no change)                                                                                        |
| Electric System Improvements         | 20448097     | $/kWh | 0.01967     | ✅ decided: `add_to_drr`          | (no change)                                                                                        |
| Revenue Adjustment Mechanism         | 20448098     | $/kWh | 0.00195     | ✅ decided: `exclude_trueup`      | (no change)                                                                                        |
| Transmission Service Charge          | 20448099     | $/kWh | 0.03401     | ✅ decided: `add_to_drr`          | (no change)                                                                                        |
| Systems Benefits Charge              | 20448100     | $/kWh | 0.03326     | ✅ decided: `exclude_eligibility` | (no change)                                                                                        |
| Competitive Transition Assessment    | 20448101     | $/kWh | 0.00038     | ✅ decided: `add_to_drr`          | (no change)                                                                                        |
| Conservation Charge                  | 20448102     | $/kWh | 0.00        | ✅ decided: `add_to_drr`          | lumped with CAM below — see [§ below](#conservation-charge--conservation-adjustment-mechanism-cam) |
| Conservation Adjustment Mechanism    | 20448103     | $/kWh | 0.006       | ✅ decided: `add_to_drr`          | (no change)                                                                                        |
| Renewable Energy Charge              | 20448104     | $/kWh | 0.001       | ✅ decided: `add_to_drr`          | (no change)                                                                                        |
| FMCC Delivery Charge                 | 20448105     | $/kWh | 0.04791     | ✅ decided: `add_to_drr`          | (no change)                                                                                        |
| Generation Service Charge (Jan–June) | 20448107     | $/kWh | 0.1129      | ✅ decided: `add_to_srr`          | (no change)                                                                                        |
| FMCC Generation Charge (Jan–June)    | 20448108     | $/kWh | −0.001      | ✅ decided: `add_to_srr`          | (no change)                                                                                        |
| Minimum Charge                       | 20448111     | $/mo  | 9.62        | ✅ decided: `exclude_redundant`   | (no change)                                                                                        |
| Generation Service Charge (July–Dec) | 20800838     | $/kWh | 0.09115     | ✅ decided: `add_to_srr`          | (no change)                                                                                        |
| FMCC Generation Charge (July–Dec)    | 20800839     | $/kWh | −0.0012     | ✅ decided: `add_to_srr`          | (no change)                                                                                        |

### Eversource — charge-by-charge notes

#### Base delivery (`already_in_drr`) — ✅ decided

- **Customer Charge** (`$9.62/mo`): the standard Rate 1 fixed monthly customer charge. OCC confirms: "The monthly fixed charge covers costs related to customer billing, meter reading, customer service and maintaining the service line... Currently Eversource charges $9.62 per month for Rate 1" ([DocumentCloud p. 2](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p2/a2826341)). Exactly the rate-case base distribution customer charge — no ambiguity.
- **Energy Charge** (`$0.05844/kWh`): the core volumetric distribution rate. OCC: "The price for delivery of electricity using the local wires, transformers, substations, and other equipment... Currently: Eversource Distribution rate is 5.844¢ per kWh for Rate 1." ([DocumentCloud p. 2](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p2/a2826342)) The `5.844¢` figure matches exactly, confirming Genability's "Energy Charge" line **is** the base distribution volumetric rate, not a bundle that includes any of the riders below.

#### CapEx tracker → top-up (`add_to_drr`) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

- **Electric System Improvements:** PURA approved ESI in the **17-10-46** settlement to recover resiliency CapEx and core capital above thresholds between rate cases. REVREQ §VII: Pub. Act 23-102 §5 prohibits reauthorizing the on-bill CapEx tracker; Company proposes reflecting ESI capital in **base distribution rates** effective Jul 1, 2027, with a wind-down through ~Apr 2029 for pre-effective true-ups. For **2025 BAT inputs**, ESI is outside base DRR → `add_to_drr` (no MC 8760 — residual), analogous to RI CapEx Factor. `master_charge`: "Electric System Improvements (ESI)", `master_type`: "Base delivery" (RI CapEx Factor convention).

#### Revenue true-up (`exclude_trueup`) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

- **Revenue Adjustment Mechanism:** REVREQ §II: continue revenue decoupling under Conn. Gen. Stat. §16-19-tt(b); reconciles actual annual base distribution revenues to the approved DRR. OCC independently confirms both the naming and mechanics: "Is a revenue decoupling mechanism that reconciles annual distribution revenues to the level allowed in the company's last rate case. Customers are charged if total annual revenues are below that set in a rate case, but customers are credited when total revenues exceed not allowed levels." ([DocumentCloud p. 2](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p2/a2826344)) This resolves the earlier naming ambiguity — OCC calls it "RAM," matching the exhibits' "RDM"/"Revenue Decoupling Mechanism" as the same Conn. Gen. Stat. §16-19-tt(b) mechanism under different labels. A pure two-sided reconciliation (can be a charge _or_ a credit) around the approved rate-case revenue level → `exclude_trueup`, not a net cost recovery. `master_charge`: "Revenue Decoupling (RDM)", `master_type`: "Revenue true-up".

#### Transmission (`add_to_drr`) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

- **Transmission Service Charge:** OCC: "The price for delivery of electricity over high voltage power lines from the generation company to the distribution company. These charges are regulated by the Federal Energy Regulatory Commission... Transmission charges are made up of local (CT only) charges as well as regional charges, which are charges from all of New England." ([DocumentCloud p. 2](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p2/a2826343)) FERC-regulated, ISO-NE pass-through → `add_to_drr`, MC = bulk TX ([`ct_bulk_transmission_marginal_cost.md`](../marginal_costs/ct_bulk_transmission_marginal_cost.md)). `master_charge`: "Transmission Charge", `master_type`: "Base delivery" (matches RI's Transmission Charge convention). Still want the annual Transmission Adjustment / TAC filing for exact RNS vs LNS labeling, but the DRR/SRR classification itself doesn't depend on that detail.

#### Systems Benefits Charge (SBC) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

OCC: "The cost of public education, hardship programs and other societal costs. The SBC varies by electric company over time. The SBC will produce approximately $95.6 million for Eversource and $24.2 million for UI. The primary uses of the SBC are paying electric company costs associated with hardship customers and providing a program that matches payments made by customers with arrearages that further reduces the amount they owe. Because most of these charges are related to residential consumers (as opposed to businesses), the residential SBC charges is higher than to commercial and industrial consumers." ([DocumentCloud p. 2](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p2/a2826338))

OCC names exactly two "primary uses" for the `$95.6M` Eversource SBC pool, and both are hardship/arrearage/low-income assistance programs — not a general public-benefits catch-all. That framing is consistent with earlier CGA OLR reporting on the Combined Public Benefits Charge ([2015-R-0047](https://cga.ct.gov/2015/rpt/2015-R-0047.htm)), which likewise treats hardship protection, matching-payment / arrearage programs, and related low-income assistance as the SBC's main cost drivers. That supports **`exclude_eligibility`** rather than `add_to_drr` — this reads as an income-transfer program (RI LIDRF precedent), not a rate-design cost that should shape the BAT's measured cross-subsidy. Decision: **`exclude_eligibility`**, `master_charge`: "Systems Benefits Charge (SBC)", `master_type`: "LMI cost recovery". Caveat carried forward: OCC's opening phrase "public education... and other societal costs" implies a residual non-LMI component that isn't quantified in the materials used here; if that residual is material in a later RAM year, only part of the `$0.03326/kWh` should really be `exclude_eligibility` with the rest `add_to_drr`, but the whole charge is encoded as `exclude_eligibility` for now given that hardship/LMI assistance is the stated primary use.

#### Competitive Transition Assessment (CTA) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

OCC: "Originally, the CTA covered the electric distribution company's stranded generation costs that were still on the company's books at the time of restructuring... The majority of these costs were recovered by 2011 for CL&P... Currently, the remaining charges and credits vary year to year and are associated with long-term purchased power contracts that remain from the 1980's-90's from cogeneration facilities... For Eversource Rate 1 the CTA is in a credit position of -0.116¢ per kWh" (2022 value — a **credit**, i.e. negative charge) ([DocumentCloud p. 3](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p3/a2826345)).

This isn't a program budget (no ongoing "pool" being funded), and it isn't a symmetric revenue-decoupling true-up either — it's recovery (or refund) of a specific, fixed set of legacy sunk-cost contracts whose balance moves year to year but isn't tied to actual-vs-approved _revenue_. The taxonomy's own "Sunk-cost recovery" category (`add_to_drr`) is defined as "Fixed debt, CTA / stranded-cost, storm, or settlement pools via $/kWh" — CTA is the taxonomy's own worked example. Decision: **`add_to_drr`**, `master_charge`: "Competitive Transition Assessment (CTA)", `master_type`: "Sunk-cost recovery," treating it as a fixed pool ÷ kWh even though the pool can be negative. Worth checking against the current CTA/RAM compliance filing since `exhibit_clp_revreq_1.md` notes CTA collections are now also used as a storm-cost offset vehicle in the 2026 case — but even under that reading it still lands on "Sunk-cost recovery" rather than changing category.

#### Conservation Charge + Conservation Adjustment Mechanism (CAM) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

OCC: "This charge is to support energy efficiency programs. The CAM Charge includes the state mandated 0.3 cent per kWh Conservation & Load Management Charge and up to an additional 0.3 cent per kWh through the Conservation Adjustment Mechanism (CAM). The two C&LM Charges collected through the CAM line item brings in up to approximately $160 million annually to fund conservation and energy efficiency programs." ([DocumentCloud p. 3](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p3/a2826339))

**Why these two Genability lines are lumped together.** OCC describes only **one** bill line here — "the CAM Charge" — and explains that it is itself made up of two statutory sub-components: a fixed, state-mandated `0.3¢/kWh` "Conservation & Load Management Charge," plus a variable top-up of up to another `0.3¢/kWh` through the "Conservation Adjustment Mechanism." Genability's discovery split that single OCC-described bill line into two separate tariff rows — `Conservation Charge` (`master_tariff_rate_id: 16959929`, `$0.00/kWh` at the 2025-01-01 snapshot) and `Conservation Adjustment Mechanism` (`master_tariff_rate_id: 17114276`, `$0.006/kWh` at the same snapshot) — but OCC's own accounting treats them as one combined charge funding one program budget ("the two C&LM Charges collected through the **CAM line item**"). There's no basis in the documents for classifying the two pieces differently: both recover slices of the same fixed, roughly `$160M`/year statewide EE program budget ÷ kWh — a textbook program-surcharge cross-subsidy (RI EE Programs Charge analogue) — so both get the same decision. The `$0.00` sample rate on the base "Conservation Charge" line at this one snapshot doesn't change that: OCC's own numbers show the combined charge fluctuating with program funding levels (reduced from `0.6¢` to `0.515¢` for Eversource during COVID-era underspending [DocumentCloud p. 3](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p3/a2826339)), so a `$0` snapshot on one of the two sub-components just reflects where that fixed budget happened to sit at that point in the cycle, not a different cost-recovery mechanism.

Decision for both: **`add_to_drr`**, `master_charge`: "Conservation Adjustment Mechanism (CAM)", `master_type`: "Program surcharge" — same `master_charge` label for both so they aggregate together in downstream processing, mirroring how OCC itself reports them as a single "CAM Charge" line.

#### Renewable Energy Charge — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

OCC: "The payments to the Renewable Energy Investment Fund, which promotes the growth, development and sale of renewable energy sources. The renewables charge is a 0.1 cent per kWh charge to support renewable energy programs. It is the primary funding source for the Connecticut Clean Energy Fund, administered by the Connecticut Green Bank." ([DocumentCloud p. 3](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p3/a2826340))

**Why we flipped this from the auto-label.** This charge was originally auto-classified `add_to_srr` / "RES supply" by `classify_charges.py`'s generic name-based rule `Renewable (Standard )?Energy` → `add_to_srr`. That rule was written for **RI's** Renewable Standard Energy Charge, a genuine per-MWh REC-compliance obligation — the utility must buy a certain number of RECs per MWh sold, so it scales 1:1 with a customer's own consumption and gets a flat supply-side marginal-cost signal like other RES/supply commodity charges. Because CT's charge happens to share the name "Renewable Energy," the same regex matched it and carried over RI's `add_to_srr` label — a name coincidence, not a CT-specific decision.

OCC's description of what CT's charge actually is doesn't match a REC-compliance obligation at all: it's a fixed `$0.001/kWh` contribution to a state incentive-**program** fund (the Clean Energy Fund, administered by the Connecticut Green Bank) that finances renewable-energy programs — not a REC purchased against the customer's own load. That's the same economic shape as CAM above (a fixed program budget ÷ kWh), not a supply commodity. Decision: **`add_to_drr`**, `master_charge`: "Renewable Energy Investment (Clean Energy Fund)", `master_type`: "Program surcharge". Applied identically across all three Eversource tariffs — Rate 1's "Renewable Energy Charge," Rate 5's "Renewable Energy Charge," and Rate 7's "Renewable Charge" (same underlying charge, Rate 7 just uses a shorter Genability display label).

#### FMCC Delivery Charge (NBFMCC) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

OCC: "By law, NBFMCCs are collected on electricity bills to cover certain costs approved by the Federal Energy Regulatory Commission (FERC) and related costs approved by the Public Utility Regulatory Authority (PURA) to reduce federally mandated congestion charges and reliability 'must run' contracts (CGS § 16-1(35))... NBFMCCs capture costs that cannot be avoided if a customer chooses a retail electric supplier... Non-bypassable NBFMCCs include costs associated with ISO-NE, costs to avoid congestion on the transmission system, renewable energy incentives, **the Millstone contract**, and other initiatives required by state law." ([DocumentCloud p. 3](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p3/a2826346))

This is the messiest one. It bundles several distinct things — ISO-NE congestion-avoidance costs, renewable-energy program incentives (possibly overlapping with the Renewable Energy Charge above), and specifically **the Millstone nuclear power purchase contract**, which is an economically distinct legacy above/below-market PPA, not a "program" in the same sense as SBC/CAM. `exhibit_clp_revreq_1.md` §VIII's CapEx list (DER Map, CT Solar Tariff, SCEF, Bill Redesign) only covers the _capital_ sliver of NBFMCC, not the whole ongoing charge, so that passage alone doesn't settle it. Decision: **`add_to_drr`** (fixed-cost pool ÷ kWh, like the other program surcharges), `master_charge`: "Non-Bypassable FMCC (NBFMCC)", `master_type`: "Program surcharge". Caveat carried forward: the Millstone piece is non-bypassable and large enough (it's a full nuclear PPA settlement, not a small incentive line) that if its recovery mechanics turn out not to fit "fixed pool ÷ kWh," it might warrant splitting out of NBFMCC into its own sub-classification later — no document found so far separates the Millstone dollars from the rest of NBFMCC, so the whole charge is encoded as one `add_to_drr` line for now.

#### Generation Service Charge (GSC) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

OCC, in the "Supplier Services" section (not Delivery Services): "This is the charge for the actual electricity or kilowatt hours that you use... Standard Service is the default service for electricity provided to you by your electric distribution company... (Effective January 1, 2022, Eversource's GSC is 11.574¢ per kWh for Residential Rate 1 customers...)" ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p1/a2826347))

The utility's own tariff filing (Exhibit CLP-RATES-3, Schedule E-1.1, Rate 1) independently confirms this — it lists Rate 1's charges under four headings (Local Delivery, Transmission, Public Benefits, **Supply**), with Generation Service grouped under **Supply**, alongside Third-Party Service as the customer's other supplier option:

> "**Supply:** Supplier Service Options — Generation Service per kWh (as per Generation Services tariff) `$0.12791` / Supplier Service Options — Third-Party Service `as per contract`" ([`exhibit_clp_rates_3_residential_tariffs.md`](../../sources/exhibit_clp_rates_3_residential_tariffs.md))

Two independent, authoritative sources — OCC's consumer guide and the utility's own filed tariff — both place Generation Service Charge under the generation/supply side of the bill, not delivery: it's literally the price Eversource charges for procuring the electricity itself under Standard Service, as opposed to a customer buying that electricity from a competitive retail supplier instead. That maps directly onto this doc's taxonomy row "Supply commodity — Standard Service generation... → `add_to_srr`" — no delivery-side category (`already_in_drr`, `add_to_drr`, `exclude_trueup`) fits a charge that a customer can entirely opt out of by choosing a different generation supplier. Since the platform models Eversource's own Standard Service tariff (not retail-supplier contract pricing), this charge is universal across the modeled residential population. Decision for both seasonal instances (Jan–June, July–Dec): **`add_to_srr`**, `master_charge`: "Supply commodity (bundled)", `master_type`: "Supply commodity" (matches the NY/MD convention for Standard Service commodity charges).

#### FMCC Generation Charge (BFMCC) — ✅ decided, encoded in `ct_eversource_charge_decisions.json`

OCC: "'Bypassable' FMCCs are charges that customers may avoid by selecting a retail energy supplier rather than receiving service through the electric companies' Standard Service rates... include charges from... ISO-NE, costs related to congestion on the transmission system, and certain financial instruments meant to offset those costs. (Effective January 1, 2022, Eversource's FMCC-Generation is -0.09¢ per kWh for Residential Rate 1 customers. **When the BFMCC is netted with the Generation Service Charge, Eversource's GSC Charge is 11.484¢ per kWh.**)" ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28540602-01012022-electric-bill-components-final-122721/#document/p1/a2826348))

The tariff (same Rate 1 "Supply" section as Generation Service, above) lists it as a distinct rate line, applied to the same customer population as GSC:

> "FMCC Generation Charge — per kWh (as per FMCC tariff; **not applicable to customers taking Third-Party Service**) `-$0.00150`" ([`exhibit_clp_rates_3_residential_tariffs.md`](../../sources/exhibit_clp_rates_3_residential_tariffs.md))

Same value (`-$0.0015/kWh`) recurs identically for Rate 1, 5, 6, and 7 in both `exhibit_clp_rates_2_residential.md`'s rate-build worksheets and `exhibit_clp_rates_3_residential_tariffs.md`'s tariff schedules — always as a **credit** (`is_credit: true`, negative in every observed vintage from OCC's 2022 snapshot through the 2025 Genability discovery).

Reasoning for `add_to_srr`:

1. **Same bypassability as GSC, same population.** The tariff's "not applicable to customers taking Third-Party Service" parenthetical is identical in structure to the one on Generation Service — it's charged to (credited to) exactly the customers who pay GSC, and to no one else. It isn't a separate opt-in program with its own eligibility criteria; it's a component of what Standard Service customers pay/receive for their generation.
2. **Both authoritative sources place it in Supply, not Delivery.** OCC discusses it exclusively under "Supplier Services" (not "Delivery Services"), and the tariff lists it under the Rate 1 "Supply" heading, distinct from Local Delivery / Transmission / Public Benefits.
3. **It's netted with GSC for billing purposes.** OCC states outright that Eversource combines BFMCC with the Generation Service Charge into a single "GSC Charge" bill line — the regulator's own accounting treats them as one number, not two independently-recovered mechanisms.
4. **It doesn't fit `exclude_trueup`.** Unlike RAM/RDM (which reconcile actual vs. approved _revenue_ against a rate-case-set target), FMCC-Generation is a cost-recovery/settlement mechanism tied to ISO-NE congestion and "financial instruments" on the generation side — there's no revenue-requirement target it's reconciling against.

`master_charge`: "Supply commodity (bundled)", `master_type`: "Supply commodity" — same as Generation Service Charge, so the two aggregate together in downstream processing (matching the netted "GSC Charge" treatment OCC describes, and the naming convention already used for bundled Standard Service commodity charges in NY/MD).

---

## Eversource Rate 5 / Rate 7 (residential alternative tariffs)

**Current modeling decision: all Eversource CT ResStock buildings are assigned to Rate 1.** Rate 5 (electric heating, closed since 2006) and Rate 7 (TOU) are small rate classes (135k and 731 customers respectively, vs. ~1.05M on Rate 1). For the initial CT BAT runs, all buildings use Rate 1's tariff and revenue requirement. Rate 5/7 seed YAMLs exist (`ct_eversource_elecheat_rate_case_test_year.yaml`, `ct_eversource_tou_rate_case_test_year.yaml`) with testimony values for future use, but the build script's ResStock lookup uses `ct_eversource` — which is the only utility assignment in the CT ResStock data. Splitting buildings by heating fuel to model Rate 5 separately, or offering Rate 7 as an alternative tariff, is future work.

**Known limitation if Rate 7 seed YAML is ever built:** The TOU monthly_rates YAML (`ct_eversource_tou_monthly_rates_2025.yaml`) uses `all_on_peak`/`all_off_peak` sub-keys for transmission and NBFMCC charges, but `build_rate_case_test_year.py`'s `_day_weighted_avg_rate` expects flat `2025-01`, `2025-02`, … month keys. Flat charges (ESI, CAM, RE, CTA) will compute correctly; the TOU-differentiated ones (transmission, NBFMCC) will resolve to `$0` budgets. Running the build script for Rate 7 will require either manual top-up computation or a script enhancement to handle on/off-peak rate structures.

Rate 1 (`ct_eversource`, `masterTariffId` 614) is the residential **default** tariff. Eversource CT also has two other active residential tariffs, discovered under the same Genability LSE (`lse_id` 2250, "Eversource Energy (Formerly Connecticut Light & Power Co)"):

- **Rate 5 — Residential Electric Heating Service** (`ct_eversource_elecheat`, `masterTariffId` 616, Genability `tariff_code` "5"): closed to new applicants since December 21, 2006, but still carries active accounts; requires electric space heat as the primary heating source. See [`exhibit_clp_rates_3_residential_tariffs.md` § Rate 5](../../sources/exhibit_clp_rates_3_residential_tariffs.md).
- **Rate 7 — Residential Time-of-Day Electric Service** (`ct_eversource_tou`, `masterTariffId` 615, Genability `tariff_code` "7"): the platform's TOU alternative; on-peak = weekdays 12 p.m.–8 p.m. Eastern Prevailing Time, off-peak = all other hours. See [`exhibit_clp_rates_3_residential_tariffs.md` § Rate 7](../../sources/exhibit_clp_rates_3_residential_tariffs.md).

**Independent public confirmation that 616 = Rate 5 and 615 = Rate 7** (Genability/Arcadia's own API is closed/authenticated, so its `tariff_name`/`tariff_code` fields aren't independently checkable — these are third-party sources that corroborate them):

- **Eversource's own tariff-book page** lists exactly these three residential rate names side by side: "Rate 1 Residential Electric Service (Non-Heating)," "Rate 5 Residential Electric Heating Service," "Rate 7 Residential Time-of-Day Service" — <https://www.eversource.com/residential/about/doing-business-with-us/interconnections/connecticut/connecticut-tariffs-rules>, linking to the current filed tariff PDFs directly: [Rate 5 PDF](https://www.eversource.com/docs/default-source/rates-tariffs/ct-electric/rate-5-ct.pdf), [Rate 7 PDF](https://www.eversource.com/docs/default-source/rates-tariffs/ct-electric/rate-7-ct.pdf).
- **NREL/OpenEI's U.S. Utility Rate Database (URDB)** has independently-catalogued entries for the same two rates, sourced from those same Eversource PDFs: [URDB Rate 5 — Residential Electric Heating Service](https://apps.openei.org/USURDB/rate/view/696938431bf1b1f57709a6dd), [URDB Rate 7 — Residential Time-Of-Day Electric Service](https://apps.openei.org/USURDB/rate/view/6969389492370159da017a0d). URDB's Rate 5 fixed charge ("Fixed Charge (First Meter)") is `$23.75/month` — an exact match to the `$23.75` Customer Charge our Genability discovery returned for `masterTariffId` 616, which is strong independent confirmation (the two data sources don't reference each other).
- **Eversource's consumer-facing Rate 7 explainer page** confirms the on-peak window as "noon to 8 p.m. on weekdays," matching the `from_hour: 12, to_hour: 20, weekdays_only: true` TOU definition Genability returned for `masterTariffId` 615 — <https://www.eversource.com/residential/account-billing/manage-bill/about-your-bill/rates-tariffs/connecticut-time-of-day-rate-7>.

Both pseudo-utilities were added to `UTILITY_MASTER_TARIFF_IDS` in `fetch_monthly_rates.py` and discovered the same way as any other utility:

```bash
cd rate_design/hp_rates
UTILITY=ct_eversource_elecheat just s ct discover-charges 2025-01-01
UTILITY=ct_eversource_tou just s ct discover-charges 2025-01-01
```

**Charge classification: carried over from Rate 1, not re-derived.** Discovery shows every charge in Rate 5 (14 rate entries) and Rate 7 (17 rate entries) has a Genability `rate_group_name` that matches one of Rate 1's already-decided charges — these are the same statutory/regulatory charges applied on top of a different base distribution rate design, not economically distinct charges requiring their own classification. Two wrinkles surfaced while matching:

1. **Rate 7 splits three charges into On-Peak/Off-Peak pairs**: Transmission Service Charge, FMCC Delivery Charge, and Generation Service Charge each appear twice (once per TOU period) instead of once. All other charges (ESI, RAM, SBC, CTA, Conservation Charge, CAM, Renewable Charge, FMCC Generation Charge, Minimum Charge, Customer Charge, base Distribution/Energy Charge) are **not** TOU-differentiated in Rate 7 — consistent with `exhibit_clp_rates_3_residential_tariffs.md`'s Rate 7 tariff table, which shows the same non-TOU treatment for those line items.
2. **Rate 7's Generation Service Charge is mislabeled by `rate_name`.** Genability's `rate_name` for the two Rate 7 supply entries is "Energy Charge - On-Peak" / "Energy Charge - Off-Peak" (easily confused with the base distribution Energy Charge), but their `rate_group_name` ("Generation Service Charge") and `charge_class` (`SUPPLY,CONTRACTED`, vs. `DISTRIBUTION` for the real Energy Charge) correctly identify them as the Standard Service generation commodity charge, TOU-differentiated. Matching was done on `rate_group_name` (plus `charge_unit` to split "Distribution Service Charge" into its Customer Charge vs. base Energy/Distribution Charge components), not `rate_name`, specifically because of this mislabeling — `rate_name` is Genability's display label, `rate_group_name` is the more reliable identifier of what the charge economically is.

Both new decision files (`ct_eversource_elecheat_charge_decisions.json`, `ct_eversource_tou_charge_decisions.json`) now have **all charges decided**, inheriting the exact same `decision` / `master_charge` / `master_type` as the matching Rate 1 charge (see the Eversource summary table and charge-by-charge notes above for the reasoning behind each) — including the Renewable Energy Charge flip to `add_to_drr` (see [§ above](#renewable-energy-charge--decided-encoded-in-ct_eversource_charge_decisionsjson)), which applies identically to Rate 5's "Renewable Energy Charge" and Rate 7's "Renewable Charge" (same charge, different Genability display name for the same `rate_group_name`).

---

## United Illuminating (`ct_ui`) — summary table

22 Genability rates. Sample rates from discovery effective **2025-01-01**. **UI lacks a REVREQ extract in-repo**; decisions below are hypotheses pending UI rate case + RAM filings.

| Charge (Genability)                   | tariffRateId        | Unit  | Sample rate | Proposed decision      | Evidence / remaining gap                                                       |
| ------------------------------------- | ------------------- | ----- | ----------- | ---------------------- | ------------------------------------------------------------------------------ |
| Basic Service Charge                  | 20451713            | $/mo  | 11.34       | `already_in_drr`       | Customer charge (auto OK).                                                     |
| Energy Charge                         | 20451714            | $/kWh | 0.100714    | `already_in_drr`       | Core distribution energy rate (auto OK).                                       |
| Summer Rate (Transmission Charge)     | 20451711            | $/kWh | 0.046888    | `add_to_drr`           | Genability `charge_class=TRANSMISSION` — auto `already_in_drr` was wrong.      |
| Winter Rate (Transmission Charge)     | 20451712            | $/kWh | 0.046888    | `add_to_drr`           | Same seasonal TX pair.                                                         |
| Transmission Adjustment               | 20451716            | $/kWh | 0.00        | `exclude_trueup`?      | Variable-rate TX true-up — confirm in UI TAC filing.                           |
| Decoupling Adjustment                 | 20451715            | $/kWh | 0.000518    | `exclude_trueup`       | Revenue decoupling (same statute family as Eversource).                        |
| Energy Assistance Costs               | 20451700            | $/kWh | 0.027419    | `exclude_eligibility`? | LMI / hardship recovery — RI LIDRF pattern; confirm vs UI SBC/hardship.        |
| Energy Efficiency Programs            | 20451701            | $/kWh | 0.006       | `add_to_drr`           | Fixed EE budget ÷ kWh (auto OK).                                               |
| Renewable Energy Investment           | 20451702            | $/kWh | 0.001       | `add_to_drr`?          | Likely Clean Energy Fund program (same as Eversource Renewable Energy Charge). |
| New England Grid Operator Cost (S/W)  | 20451703 / 20451704 | $/kWh | 0.002039    | research               | ISO-NE Schedule 1 / admin pool? Need UI Public Benefits exhibit.               |
| State Mandated Energy Purchases (S/W) | 20451705 / 20451706 | $/kWh | 0.017584    | `add_to_drr`?          | Mandated PPA / nuclear / renewables recovery — RI LTC analogue.                |
| Customer Produced Energy (S/W)        | 20451707 / 20451708 | $/kWh | 0.00434     | `add_to_drr`?          | Net metering / DER credit recovery.                                            |
| Misc. & Other Mandates (S/W)          | 20451709 / 20451710 | $/kWh | 0.008948    | research               | Catch-all public benefits — need RAM / tariff description.                     |
| Generation Charge (Jan–June)          | 20797083            | $/kWh | 0.135683    | `add_to_srr`           | Standard Service commodity.                                                    |
| Generation Charge (July–Dec)          | 20451698            | $/kWh | 0.119101    | `add_to_srr`           | Standard Service commodity.                                                    |
| Bypassable FMCC (Jan–June / July–Dec) | 20797084 / 20451699 | $/kWh | 0.00        | research               | Supply FMCC; `$0` at discovery.                                                |
| Minimum Charge                        | 20451717            | $/mo  | 11.34       | `exclude_redundant`    | Equals Basic Service Charge.                                                   |

### UI — charge-by-charge notes

#### Base delivery (`already_in_drr`)

- **Basic Service Charge** (`$11.34/mo`): auto OK.
- **Energy Charge** (`$0.100714/kWh`): auto OK for distribution volumetric.

#### Likely mis-auto-classified → fix

- **Summer Rate / Winter Rate (Transmission Charge):** Genability `charge_class=TRANSMISSION` and `rate_group_name=Transmission Charge`, but auto-rules labeled Core Delivery / `already_in_drr`. Treat as TX top-up (`add_to_drr`, bulk TX MC) unless a UI rate-case exhibit shows these dollars inside base distribution RR (unlikely).

#### Public Benefits / still to pin down

- Energy Assistance → prefer `exclude_eligibility` after confirming LMI recovery. OCC's SBC description (see Eversource SBC notes above) covers both utilities in one paragraph — "The SBC will produce approximately $95.6 million for Eversource and $24.2 million for UI" — but UI's Genability breakout doesn't have a single "SBC" line; UI's public-benefits charges are split into several separate lines (Energy Assistance, EE Programs, Renewable Energy Investment, Grid Operator, State Mandated Purchases, Customer Produced Energy, Misc Mandates) that don't map 1:1 onto the OCC paragraph, so this is a weaker inference than the Eversource case — treat "Energy Assistance Costs" as the LMI-recovery candidate, not the others.
- EE Programs → `add_to_drr` (done).
- Renewable Energy Investment → likely `add_to_drr` (program), not RES. OCC's Renewable Energy Investment description explicitly covers both utilities together, same reasoning as the Eversource Renewable Energy Charge above.
- State Mandated Purchases / Customer Produced Energy → `add_to_drr` pending UI RAM descriptions.
- ISO-NE Grid Operator / Misc Mandates → need filing text.
- Decoupling + Transmission Adjustment → true-ups.
- Generation + Bypassable FMCC → supply side. OCC's GSC/BFMCC section gives a UI sample rate ("UI does not currently have a separate BFMCC so their GSC Charge is 10.6731¢ per kWh for Rate R") implying UI's Bypassable FMCC line was `$0` even in 2022 — consistent with the `$0.00` sample rate at 2025 discovery.

---

## Documents still needed

To **finalize every top-up decision** and populate `delivery_rev_requirements_from_rate_cases.yaml`, pull / extract the following (Zotero + markdown under `context/sources/` or a short citation digest, same workflow as CLP-REVREQ-1). CLP MCOS 1/2 are on a **separate ticket** (sub-TX/dist MC) and are not required for charge top-up classification.

### A. Charge classification (top-ups)

| # | Document                                                                                                                                        | Why                                                                                                                                                                                                                                                                    | Priority |
| - | ----------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| 1 | ✅ **OCC Electric Bill/Rate Components** — extracted to [`ct_electric_bill_components.md`](../../domain/charges/ct_electric_bill_components.md) | Authoritative definitions of SBC, CAM/C&LM, Renewable Investment, CTA, FMCC, Combined Public Benefits — maps Genability labels → economic type. Used above to turn most `research`/`null` charges into recommendations (not yet encoded — see charge-by-charge notes). | Done     |
| 2 | **Latest Eversource RAM decision / compliance** (e.g. Docket **25-01-03** / **26-01-03**) — rates effective May 1                               | Line-item budgets and over/under-recoveries for ESI, SBC, NBFMCC, CAM, CTA, RDM; separates provision vs true-up.                                                                                                                                                       | High     |
| 3 | **Latest UI RAM / Public Benefits filing** (parallel annual docket)                                                                             | Same for UI’s Energy Assistance, State Mandated Purchases, Customer Produced Energy, Misc Mandates, Grid Operator.                                                                                                                                                     | High     |
| 4 | **Eversource Rate 1 and UI Rate R tariff books** (current sheets)                                                                               | Confirm Genability line names vs tariff nomenclature; seasonal Standard Service; minimum charge.                                                                                                                                                                       | High     |
| 5 | **Bypassable vs non-bypassable FMCC** exhibit / Standard Service reconciliation                                                                 | Distinguish FMCC Delivery (NBFMCC → delivery top-up) from FMCC Generation / Bypassable FMCC (supply).                                                                                                                                                                  | High     |
| 6 | **Transmission / TAC annual filing** (Eversource + UI)                                                                                          | Confirm Transmission Service Charge / UI Summer–Winter TX are OATT pass-throughs vs anything inside base DRR.                                                                                                                                                          | Medium   |
| 7 | **Exhibit CLP-RATES-1** (Rates Panel, Docket 26-05-10)                                                                                          | Decoupling / RDM mechanics (already strong from REVREQ; this seals RAM carrying-charge tweak).                                                                                                                                                                         | Medium   |
| 8 | **Renewable Energy Investment / CEF statute or CEFIA budget**                                                                                   | Confirm UI Renewable Energy Investment should flip from auto-`add_to_srr` to program `add_to_drr`, same as the already-decided Eversource Renewable Energy Charge (see [§ above](#renewable-energy-charge--decided-encoded-in-ct_eversource_charge_decisionsjson)).    | Medium   |
| 9 | **CTA remaining-balance / RAM schedule**                                                                                                        | Decide `exclude_trueup` vs residual stranded-cost `add_to_drr`.                                                                                                                                                                                                        | Low      |

### B. Delivery revenue requirement dollars (for `compute-rr`)

| #  | Document                                                                                                                                                                         | Why                                                                                                                  | Priority |
| -- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | -------- |
| 10 | **CL&P SFR class revenue / cost-of-service schedules** from Docket **26-05-10** (residential Rate 1 allocated DRR — often “F” or class COS schedules, not just REVREQ narrative) | REVREQ gives **total-company** deficiency (~$451M / `$6.8B` rate base); BAT needs **residential-class** delivery RR. | High     |
| 11 | **UI rate-case revenue requirement + class allocation** — Docket **22-08-08** and/or **24-10-04** (REVREQ-equivalent exhibit)                                                    | Symmetric to CLP-REVREQ-1 for `ct_ui`.                                                                               | High     |
| 12 | Optional: prior CL&P settlement **17-10-46** (for historical ESI / ESM context only)                                                                                             | Background; not required if 26-05-10 + RAM suffice.                                                                  | Low      |

### C. Not required for top-up decisions (other tickets)

- **CLP MCOS 1 / CLP MCOS 2** — sub-transmission / distribution marginal costs (separate methodology ticket).
- **AESC PTF bulk TX** — already covered in [`ct_bulk_transmission_marginal_cost.md`](../marginal_costs/ct_bulk_transmission_marginal_cost.md).

---

## Open questions

Eversource: all 16 charges are now decided and encoded (see charge-by-charge notes above), across Rate 1, Rate 5, and Rate 7. Remaining open items are all on the UI side:

1. **UI Energy Assistance Costs:** Confirm `exclude_eligibility` (UI has no in-repo REVREQ or OCC-level SBC breakout to confirm against).
2. **UI Summer/Winter Transmission:** Confirm not inside base DRR (fix auto-label).
3. **Residential-class DRR:** Which SFR/class schedules in 26-05-10 (and UI dockets) give Rate 1 / Rate R allocated delivery RR?

---

## Discover → classify → fetch workflow

```bash
cd rate_design/hp_rates

# Already done
UTILITY=ct_eversource    just s ct discover-charges 2025-01-01
UTILITY=ct_eversource_elecheat just s ct discover-charges 2025-01-01
UTILITY=ct_eversource_tou just s ct discover-charges 2025-01-01
UTILITY=ct_ui            just s ct discover-charges 2025-01-01
UTILITY=ct_eversource    just s ct classify-charges
UTILITY=ct_ui            just s ct classify-charges
# ct_eversource_elecheat / ct_eversource_tou were NOT run through classify-charges
# (the generic name-based classifier would mis-fire on Rate 7's TOU splits and
# the "Energy Charge" mislabeling -- see the Rate 5/7 section above). Instead,
# decisions were carried over from ct_eversource_charge_decisions.json by
# matching rate_group_name (dev/carry_over_ct_elecheat_tou_decisions.py, gitignored
# one-off script).

# After editing *_charge_decisions.json
UTILITY=ct_eversource    just s ct fetch-monthly-rates 2025-01 2025-12
UTILITY=ct_eversource_elecheat just s ct fetch-monthly-rates 2025-01 2025-12
UTILITY=ct_eversource_tou just s ct fetch-monthly-rates 2025-01 2025-12
UTILITY=ct_ui            just s ct fetch-monthly-rates 2025-01 2025-12

# After delivery_rev_requirements_from_rate_cases.yaml exists
UTILITY=ct_eversource just s ct compute-rr
UTILITY=ct_ui        just s ct compute-rr
```

Encoded output path: `charge_decisions/{utility}_charge_decisions.json` → `monthly_rates/{utility}_monthly_rates_2025.yaml` → `rev_requirement/{utility}.yaml`.

---

## References

- CL&P revenue requirements testimony: [`context/sources/exhibit_clp_revreq_1.md`](../../sources/exhibit_clp_revreq_1.md) (PURA 26-05-10)
- CL&P rates/tariff testimony and exhibits: [`exhibit_clp_rates_1.md`](../../sources/exhibit_clp_rates_1.md), [`exhibit_clp_rates_2_residential.md`](../../sources/exhibit_clp_rates_2_residential.md), [`exhibit_clp_rates_3_residential_tariffs.md`](../../sources/exhibit_clp_rates_3_residential_tariffs.md) (PURA 26-05-10)
- OCC bill components (extracted): [`context/domain/charges/ct_electric_bill_components.md`](../../domain/charges/ct_electric_bill_components.md) — plain-English description of every Delivery/Supplier Services charge, used throughout the charge-by-charge notes above
- RI charge taxonomy / ISO-NE supply decomposition: `context/methods/bat_mc_residual/ri_residential_charges_in_bat.md`
- MD worked example (BGE): `context/methods/bat_mc_residual/md_residential_charges_in_bat.md`
- CT bulk TX MC (AESC PTF): `context/methods/marginal_costs/ct_bulk_transmission_marginal_cost.md`
- PURA filings: [PURVIEW](https://www.dpuc.state.ct.us/PURA.nsf/cv/Home)
- Supply vs delivery allocation: `context/domain/bat_mc_residual/supply_vs_delivery_cost_allocation.md`

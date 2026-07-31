# CT Residential Electric Charges: Cross-Subsidy Analysis for BAT (DRAFT)

**Status:** Draft — Genability discovery done; Eversource decisions partially grounded in Docket **26-05-10** Exhibit CLP-REVREQ-1. UI and several FMCC / Public Benefits splits still need filings listed under [Documents still needed](#documents-still-needed).

Connecticut has two investor-owned electric distribution utilities:

| Platform `std_name` | Brand / legal name                                        | Genability `masterTariffId` | Charge decisions file                 |
| ------------------- | --------------------------------------------------------- | --------------------------- | ------------------------------------- |
| `ct_eversource`     | Eversource Energy CT (formerly Connecticut Light & Power) | 614                         | `ct_eversource_charge_decisions.json` |
| `ct_ui`             | United Illuminating (Avangrid)                            | 3153052                     | `ct_ui_charge_decisions.json`         |

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

16 Genability rates. Sample rates are from discovery effective **2025-01-01** (illustrative; replace with monthly_rates YAML after fetch).

| Charge (Genability)                  | tariffRateId | Unit  | Sample rate | Proposed decision   | Evidence / remaining gap                                                                                           |
| ------------------------------------ | ------------ | ----- | ----------- | ------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Customer Charge                      | 20448095     | $/mo  | 9.62        | `already_in_drr`    | Base customer charge (auto OK).                                                                                    |
| Energy Charge                        | 20448096     | $/kWh | 0.05844     | `already_in_drr`    | Core distribution energy rate. Confirm vs Rate 1 tariff book that riders are separate Genability lines.            |
| Electric System Improvements         | 20448097     | $/kWh | 0.01967     | `add_to_drr`        | CapEx tracker outside base DRR (17-10-46); REVREQ §VII sunsets / rolls into base Jul 2027. Like RI CapEx Factor.   |
| Revenue Adjustment Mechanism         | 20448098     | $/kWh | 0.00195     | `exclude_trueup`    | Revenue decoupling (Conn. Gen. Stat. §16-19-tt(b)); REVREQ: reconciles actual base revenues vs approved DRR.       |
| Transmission Service Charge          | 20448099     | $/kWh | 0.03401     | `add_to_drr`        | ISO-NE / FERC OATT pass-through (RI TX pattern). Confirm TAC / RNS-LNS split in annual TX filing.                  |
| Systems Benefits Charge              | 20448100     | $/kWh | 0.03326     | `add_to_drr`\*      | Fixed public-benefits pool ÷ kWh. \*OCC: mostly hardship/LIDR — may reclassify LMI slice as `exclude_eligibility`. |
| Competitive Transition Assessment    | 20448101     | $/kWh | 0.00038     | `exclude_trueup`?   | Legacy stranded-cost; near-zero; REVREQ uses CTA collections as storm-cost offset. Confirm remaining balance.      |
| Conservation Charge                  | 20448102     | $/kWh | 0.00        | `add_to_drr`        | Statutory C&LM component; `$0` at discovery — keep if it can become non-zero; pair with CAM.                       |
| Conservation Adjustment Mechanism    | 20448103     | $/kWh | 0.006       | `add_to_drr`        | EE / C&LM recovery (OCC); fixed budget ÷ kWh — RI EE Programs analogue.                                            |
| Renewable Energy Charge              | 20448104     | $/kWh | 0.001       | verify              | OCC: funds CT Clean Energy Fund / Renewable Energy Investment — likely **program** (`add_to_drr`), not REC.        |
| FMCC Delivery Charge                 | 20448105     | $/kWh | 0.04791     | `add_to_drr`        | Non-bypassable FMCC / NBFMCC: clean-energy + congestion programs (OCC + REVREQ §VIII CapEx list).                  |
| Generation Service Charge (Jan–June) | 20448107     | $/kWh | 0.1129      | `add_to_srr`        | Standard Service commodity (winter).                                                                               |
| FMCC Generation Charge (Jan–June)    | 20448108     | $/kWh | −0.001      | research            | Bypassable / supply-side FMCC — need Standard Service / FMCC reconciliation filing.                                |
| Minimum Charge                       | 20448111     | $/mo  | 9.62        | `exclude_redundant` | Equals customer charge.                                                                                            |
| Generation Service Charge (July–Dec) | 20800838     | $/kWh | 0.09115     | `add_to_srr`        | Standard Service commodity (summer).                                                                               |
| FMCC Generation Charge (July–Dec)    | 20800839     | $/kWh | −0.0012     | research            | Bypassable / supply-side FMCC (summer).                                                                            |

### Eversource — charge-by-charge notes

#### Base delivery (`already_in_drr`)

- **Customer Charge** (`$9.62/mo`): auto OK.
- **Energy Charge** (`$0.05844/kWh`): auto OK for the rate-case distribution volumetric. Confirm Rate 1 tariff that Genability is not bundling riders into this line.

#### CapEx tracker → top-up (`add_to_drr`)

- **Electric System Improvements:** PURA approved ESI in the **17-10-46** settlement to recover resiliency CapEx and core capital above thresholds between rate cases. REVREQ §VII: Pub. Act 23-102 §5 prohibits reauthorizing the on-bill CapEx tracker; Company proposes reflecting ESI capital in **base distribution rates** effective Jul 1, 2027, with a wind-down through ~Apr 2029 for pre-effective true-ups. For **2025 BAT inputs**, ESI is outside base DRR → `add_to_drr` (no MC 8760 — residual), analogous to RI CapEx Factor.

#### Revenue true-up (`exclude_trueup`)

- **Revenue Adjustment Mechanism:** REVREQ §II: continue revenue decoupling under Conn. Gen. Stat. §16-19-tt(b); reconciles actual annual base distribution revenues to the approved DRR. Mechanics rely on but do not change the RR → `exclude_trueup`. (Rates Panel detail in Exhibit CLP-RATES-1 — still useful to pull.)

#### Transmission (`add_to_drr`)

- **Transmission Service Charge:** Treat as OATT / ISO-NE pass-through → `add_to_drr`, MC = bulk TX ([`ct_bulk_transmission_marginal_cost.md`](../marginal_costs/ct_bulk_transmission_marginal_cost.md)). Still want the annual Transmission Adjustment / TAC filing for exact RNS vs LNS labeling.

#### Public Benefits / RAM program pools (`add_to_drr`, with LMI caveat)

- **Systems Benefits Charge:** OCC bill-components: hardship programs, matching payments, LIDR-related costs; REVREQ: EE program FTEs continue through SBC even after CapEx rolls into base. Fixed pool ÷ kWh → structural HP cross-subsidy. Default `add_to_drr`; if OCC/RAM exhibits split LIDR dollars cleanly, move that slice to `exclude_eligibility` (RI LIDRF pattern).
- **Conservation Charge + CAM:** OCC: statutory C&LM + CAM fund EE. Fixed EE budget ÷ kWh → `add_to_drr` (RI EE Programs Charge).
- **FMCC Delivery (NBFMCC):** OCC / CEEJAC: non-bypassable FMCC funds PURA clean-energy programs (RRES, NRES, SCEF, ESS, EVs, IES) and related congestion costs. REVREQ §VIII lists CapEx currently recovered through NBFMCC that will roll into base rates. For 2025 → `add_to_drr`.

#### Legacy / small

- **Competitive Transition Assessment:** Near-zero at discovery; restructuring remnant. Prefer `exclude_trueup` unless a remaining stranded-cost balance is material — check CTA / RAM schedule.

#### Renewables line (verify)

- **Renewable Energy Charge (`$0.001/kWh`):** Auto-mapped to RES supply, but OCC describes this as the **Renewable Energy Investment / Clean Energy Fund** charge (fixed program), not a load-scaling REC obligation. **Likely flip to `add_to_drr`** once confirmed against the CEF statute / RAM workpapers. Do not use flat MC 8760 until confirmed as REC-style.

#### Supply (`add_to_srr` / research)

- **Generation Service Charge (seasonal pair):** Standard Service → `add_to_srr`.
- **FMCC Generation (bypassable):** Still research — may be supply congestion true-up (`exclude_trueup` or `add_to_srr`). Need Standard Service / bypassable FMCC reconciliation.

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

- Energy Assistance → prefer `exclude_eligibility` after confirming LMI recovery.
- EE Programs → `add_to_drr` (done).
- Renewable Energy Investment → likely `add_to_drr` (program), not RES.
- State Mandated Purchases / Customer Produced Energy → `add_to_drr` pending UI RAM descriptions.
- ISO-NE Grid Operator / Misc Mandates → need filing text.
- Decoupling + Transmission Adjustment → true-ups.
- Generation + Bypassable FMCC → supply side.

---

## Documents still needed

To **finalize every top-up decision** and populate `delivery_rev_requirements_from_rate_cases.yaml`, pull / extract the following (Zotero + markdown under `context/sources/` or a short citation digest, same workflow as CLP-REVREQ-1). CLP MCOS 1/2 are on a **separate ticket** (sub-TX/dist MC) and are not required for charge top-up classification.

### A. Charge classification (top-ups)

| # | Document                                                                                                                                                                        | Why                                                                                                                                             | Priority |
| - | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| 1 | **OCC Electric Bill/Rate Components** ([PDF](https://portal.ct.gov/-/media/OCC/01012022-Electric-Bill-Components-Final-122721.pdf); refresh if a newer OCC/PURA edition exists) | Authoritative definitions of SBC, CAM/C&LM, Renewable Investment, CTA, FMCC, Combined Public Benefits — maps Genability labels → economic type. | High     |
| 2 | **Latest Eversource RAM decision / compliance** (e.g. Docket **25-01-03** / **26-01-03**) — rates effective May 1                                                               | Line-item budgets and over/under-recoveries for ESI, SBC, NBFMCC, CAM, CTA, RDM; separates provision vs true-up.                                | High     |
| 3 | **Latest UI RAM / Public Benefits filing** (parallel annual docket)                                                                                                             | Same for UI’s Energy Assistance, State Mandated Purchases, Customer Produced Energy, Misc Mandates, Grid Operator.                              | High     |
| 4 | **Eversource Rate 1 and UI Rate R tariff books** (current sheets)                                                                                                               | Confirm Genability line names vs tariff nomenclature; seasonal Standard Service; minimum charge.                                                | High     |
| 5 | **Bypassable vs non-bypassable FMCC** exhibit / Standard Service reconciliation                                                                                                 | Distinguish FMCC Delivery (NBFMCC → delivery top-up) from FMCC Generation / Bypassable FMCC (supply).                                           | High     |
| 6 | **Transmission / TAC annual filing** (Eversource + UI)                                                                                                                          | Confirm Transmission Service Charge / UI Summer–Winter TX are OATT pass-throughs vs anything inside base DRR.                                   | Medium   |
| 7 | **Exhibit CLP-RATES-1** (Rates Panel, Docket 26-05-10)                                                                                                                          | Decoupling / RDM mechanics (already strong from REVREQ; this seals RAM carrying-charge tweak).                                                  | Medium   |
| 8 | **Renewable Energy Investment / CEF statute or CEFIA budget**                                                                                                                   | Flip Eversource Renewable Energy Charge and UI Renewable Energy Investment from auto-`add_to_srr` to program `add_to_drr` if confirmed.         | Medium   |
| 9 | **CTA remaining-balance / RAM schedule**                                                                                                                                        | Decide `exclude_trueup` vs residual stranded-cost `add_to_drr`.                                                                                 | Low      |

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

1. **SBC LMI split (Eversource):** Treat whole SBC as `add_to_drr`, or carve hardship/LIDR into `exclude_eligibility` once RAM exhibits split dollars?
2. **Renewable Energy Charge vs RES:** Program (CEF → `add_to_drr`) vs REC obligation (`add_to_srr` + flat MC)? OCC points to CEF.
3. **Bypassable FMCC (generation):** True-up vs fixed supply pool; `exclude_trueup` vs `add_to_srr`?
4. **UI Energy Assistance Costs:** Confirm `exclude_eligibility`.
5. **UI Summer/Winter Transmission:** Confirm not inside base DRR (fix auto-label).
6. **Residential-class DRR:** Which SFR/class schedules in 26-05-10 (and UI dockets) give Rate 1 / Rate R allocated delivery RR?

---

## Discover → classify → fetch workflow

```bash
cd rate_design/hp_rates

# Already done
UTILITY=ct_eversource just s ct discover-charges 2025-01-01
UTILITY=ct_ui        just s ct discover-charges 2025-01-01
UTILITY=ct_eversource just s ct classify-charges
UTILITY=ct_ui        just s ct classify-charges

# After editing *_charge_decisions.json
UTILITY=ct_eversource just s ct fetch-monthly-rates 2025-01 2025-12
UTILITY=ct_ui        just s ct fetch-monthly-rates 2025-01 2025-12

# After delivery_rev_requirements_from_rate_cases.yaml exists
UTILITY=ct_eversource just s ct compute-rr
UTILITY=ct_ui        just s ct compute-rr
```

Encoded output path: `charge_decisions/{utility}_charge_decisions.json` → `monthly_rates/{utility}_monthly_rates_2025.yaml` → `rev_requirement/{utility}.yaml`.

---

## References

- CL&P revenue requirements testimony: [`context/sources/exhibit_clp_revreq_1.md`](../../sources/exhibit_clp_revreq_1.md) (PURA 26-05-10)
- RI charge taxonomy / ISO-NE supply decomposition: `context/methods/bat_mc_residual/ri_residential_charges_in_bat.md`
- MD worked example (BGE): `context/methods/bat_mc_residual/md_residential_charges_in_bat.md`
- CT bulk TX MC (AESC PTF): `context/methods/marginal_costs/ct_bulk_transmission_marginal_cost.md`
- OCC bill components (external): [Electric Bill/Rate Components (PDF)](https://portal.ct.gov/-/media/OCC/01012022-Electric-Bill-Components-Final-122721.pdf)
- PURA filings: [PURVIEW](https://www.dpuc.state.ct.us/PURA.nsf/cv/Home)
- Supply vs delivery allocation: `context/domain/bat_mc_residual/supply_vs_delivery_cost_allocation.md`

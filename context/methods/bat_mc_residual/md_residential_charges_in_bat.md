# MD Residential Electric Charges: Cross-Subsidy Analysis for BAT (DRAFT)

**Status:** Draft for internal review (BGE / Docket 331766). Charge decisions are encoded in `bge_charge_decisions.json`; this document is the research audit trail. Other Maryland utilities (DPL, Pepco, SMECO, etc.) are out of scope until BGE is validated.

Every charge on a Baltimore Gas and Electric (BGE) default residential electric bill — Schedules **R** (flat) and **RL** (time-of-use) — evaluated for whether it belongs in CAIRO's Bill Alignment Test (BAT) and bill calculations, and whether it creates a cross-subsidy between heat pump (HP) and non-HP customers.

**Data sources:**

- Genability tariff JSON: `rate_design/hp_rates/md/config/rev_requirement/top-ups/default_tariffs/bge_default_2025-01-01.json` (`masterTariffId=674`)
- Charge decisions: `rate_design/hp_rates/md/config/rev_requirement/top-ups/charge_decisions/bge_charge_decisions.json` (22 active rates, discovered effective 2026-03-31)
- Monthly rates (test year Apr 2025–Mar 2026): `rate_design/hp_rates/md/config/rev_requirement/top-ups/monthly_rates/bge_monthly_rates_2025.yaml`
- Rate-case testimony: Docket **331766**, Karas Direct Testimony w/ Exhibits (7/2/2026); Frain Direct Testimony w/ Exhibits (7/2/2026) — extracted under `reports2/context/sources/md_hp_rates/`

**Regulatory context:** BGE is an Exelon subsidiary regulated by the Maryland PSC. Default supply is **Standard Offer Service (SOS)** under **Rider 1**. Delivery rates are set in base rate cases and multi-year rate plans (MYRP). State-mandated **EmPOWER Maryland** programs are recovered via **Rider 2** and are **not** in the rate-case delivery revenue requirement per Company testimony.

---

## Charge type taxonomy

Charges are classified by **economic structure**, not tariff label. BGE's bill groups charges into Delivery, Supply, and Taxes & Fees (Frain, Part I).

| Type | What it is | Cross-subsidy? | Decision |
| ---- | ---------- | -------------- | -------- |
| **Base delivery** | Rates set in the rate case / MYRP that collect the delivery revenue requirement | Yes (BAT core) | `already_in_drr` |
| **Program surcharge** | Fixed state- or PSC-mandated program budgets recovered via uniform $/kWh or $/mo | Yes — fixed pool ÷ kWh or ÷ customers | `add_to_drr` or `add_to_srr` |
| **Transmission service** | FERC/PJM transmission pass-through (often on SOS bill) | Yes if topped up volumetrically | `add_to_drr` *(open question)* |
| **Supply commodity** | SOS generation energy rate (Rider 1), month-varying | Mixed — energy scales with load; embedded capacity may not | `add_to_srr` |
| **Cost reconciliation** | Uniform $/kWh true-ups of costs already in base rates or SOS | No — shifts all bills equally | `exclude_trueup` |
| **Revenue true-up** | Decoupling, MYRP reconciliation, base-revenue offsets | No — shifts all bills equally | `exclude_trueup` |
| **Tax pass-through** | Franchise tax, local tax, environmental surcharge | No — small uniform noise or % effects | `exclude_trueup` |
| **Eligibility / optional** | Competitive supplier billing credit, smart-meter opt-out | N/A — not default customer | `exclude_eligibility` |
| **Redundant** | Minimum charge equals customer charge | N/A | `exclude_redundant` |

---

## Summary table (BGE Schedule R, 22 Genability rates)

Rates in the **Current** column are illustrative for the rate-case test year (Jan 2026 MYRP Rate Year 3 values where applicable). See `bge_monthly_rates_2025.yaml` for full month-by-month history.

| Charge (master) | Genability name | Rider | Unit | Current rate | In rev req? | Fixed budget? | HP cross-subsidy? | Decision | MC 8760? |
| --------------- | --------------- | ----- | ---- | ------------ | ----------- | ------------- | ----------------- | -------- | -------- |
| **Customer Charge** | Delivery Service Customer Charge | Base | $/mo | $10.00 | Yes (base DRR) | — | Yes | `already_in_drr` | No—residual |
| **Core Delivery Rate** | Delivery Service Charge | Base | $/kWh | $0.04859 | Yes (base DRR) | — | Yes | `already_in_drr` | Yes—sub-tx + dx |
| **EmPOWER Maryland Charge** | Electric Efficiency & Demand Response Charge | Rider 2 | $/kWh | $0.01310 | **No** (outside DRR) | Yes | **Yes** | `add_to_drr` | No—residual |
| **Transmission Rate Adjustment** | Transmission Rate Adjustments | Rider 1 (SOS) | $/kWh | $0.02322 | **No** | Mixed | **Yes** | `add_to_drr` | Yes—bulk TX *(if in BAT)* |
| **Universal Service Charge** | Universal Service Charge | Rider 3 | $/mo | $0.32 | **No** | Yes | **Yes** (per-customer) | `add_to_drr` | No—residual |
| **Demand Response** | Dmd Res Chg/Cr | Rider 15 | $/kWh | $0.00 | **No** | Yes (when active) | **Yes** | `add_to_drr` | No—residual |
| **Electric Reliability Initiative** | Electric Reliability Initiative Surcharge | — | $/kWh | $0.00 | **No** | Yes (when active) | **Yes** | `add_to_drr` | No—residual |
| **Supply commodity (bundled)** | Generation Charge | Rider 1 (SOS) | $/kWh | varies monthly | **No** | Mixed | Mixed | `add_to_srr` | See sub-components |
| **Energy Cost Adjustment** | Energy Cost Adjustment (R) | Rider 8 | $/kWh | ~(credit) | N/A | — | No | `exclude_trueup` | — |
| **Monthly Rate Adjustment** | Monthly Rate Adjustment | Rider 25 | $/kWh | varies | N/A | — | No | `exclude_trueup` | — |
| **Multi-Year Plan Adjustment** | Multi-Year Plan Adjustment | Rider 16 | $/kWh | $0.00086 | N/A | — | No | `exclude_trueup` | — |
| **Administrative Cost Adjustment** | Administrative Cost Adjustment | Rider 10 | $/kWh | ~(credit) | N/A | — | No | `exclude_trueup` | — |
| **Base Distribution Revenue Offset** | Customer / Delivery Service Charge Offset | Rider 34 | $/mo, $/kWh | $0 | N/A | — | No | `exclude_trueup` | — |
| **RGGI Rate Credit** | RGGI Rate Credit (Residential) | — | $/mo | $0 | N/A | — | No | `exclude_trueup` | — |
| **RSP Charge/Misc** | RSP Charge/Misc | — | $/kWh | $0 | N/A | — | No | `exclude_trueup` | — |
| **Franchise Tax** | Franchise Tax | Rider 3 | $/kWh | $0.00062 | N/A | — | No | `exclude_trueup` | — |
| **Local Tax** | Local Tax | Rider 3 | $/kWh | $0.00347 | N/A | — | No | `exclude_trueup` | — |
| **Environmental Surcharge** | Envir Srchg | — | $/kWh | $0.00015 | N/A | — | No | `exclude_trueup` | — |
| **Competitive Billing** | Competitive Billing | Base | $/mo | ($0.62) credit | N/A | — | N/A | `exclude_eligibility` | — |
| **Smart Meter Opt-Out** | Smart Meter Opt-Out Charge | Rider 23 | $/mo | $5.50 | N/A | — | N/A | `exclude_eligibility` | — |
| **Minimum Charge** | Minimum Charge | Base | $/mo | $10.00 | Yes | — | N/A | `exclude_redundant` | — |

**Rate-case DRR base (Schedules R + RL):** **$780,357,582** (Karas Exhibit E-2). Test-year residential kWh: **13,010,476,768**; test-year customer count: **1,222,270.17** (Karas Exhibit E-3).

---

## The generalized cross-subsidy: fixed pools recovered volumetrically

As in NY and RI, many BGE charges outside the rate-case delivery revenue requirement share the same structure: a **fixed annual budget recovered via uniform $/kWh** (or $/month per customer). The canonical MD example is **EmPOWER Maryland** (Rider 2).

**What EmPOWER is.** Maryland's utility-funded energy efficiency and demand-response programs (building codes, rebates, low-income EE). The PSC sets program budgets; BGE recovers approved costs through Rider 2's **Electric Efficiency & Demand Response Charge**. Frain testimony states EmPOWER is a **State-imposed** charge, not part of the distribution revenue requirement sought in the rate case.

**HP cross-subsidy mechanic.** The EmPOWER budget for a program year is largely fixed. Recovery is $/kWh. If HP customers increase class kWh, they pay more dollars into the same fixed pool, reducing the per-kWh burden on non-HP customers — the same wealth-transfer dynamic as the core delivery revenue requirement.

**Integration.** Top up the delivery revenue requirement with EmPOWER (and other `add_to_drr` surcharges) so the BAT can allocate them under the chosen residual allocator. See `ri_residential_charges_in_bat.md` and `ny_residential_charges_in_bat.md` for the full worked SBC/EE analogy.

---

## Charge-by-charge analysis

### Base delivery rates (`already_in_drr`)

These collect the **delivery revenue requirement** filed in Docket 331766 (Karas E-2). CAIRO calibrates the volumetric delivery rate in precalc; the customer charge is a fixed component.

**Customer Charge.** **$9.65/month** (MYRP Rate Year 2, Apr–Dec 2025); **$10.00/month** (Rate Year 3, Jan 2026+). Per Karas E-3, Schedule R annual customer bills = 13,973,317; Schedule RL = 693,925. Fixed; load-shape-insensitive.

**Core Delivery Rate (Delivery Service Charge).** **$0.04751/kWh** (Rate Year 2); **$0.04859/kWh** (Rate Year 3). Recovers local distribution infrastructure and O&M allowed in the rate case. Frain notes the billed delivery volumetric rate also includes Riders 10, 16, and 25 on the customer bill, but those riders are **excluded** from our BAT inputs as true-ups (see below). Test-year kWh determinants: 12,191,675,329 (Schedule R) + 818,801,439 (Schedule RL) = 13,010,476,768.

**Minimum Charge.** **$10.00/month** — equals the customer charge in Rate Year 3. Bill floor; rarely binds. `exclude_redundant`.

**Decision.** Customer Charge and Core Delivery Rate → `already_in_drr`. Minimum Charge → `exclude_redundant`.

---

### Delivery top-ups (`add_to_drr`)

#### EmPOWER Maryland Charge (Rider 2) — **primary delivery top-up**

- **Rate:** $0.01028/kWh (Apr–Dec 2025); **$0.01310/kWh** (Jan 2026+)
- **Not in rate-case DRR** per Frain; State-mandated program cost recovery
- Fixed program budget ÷ kWh → HP cross-subsidy
- **Decision:** `add_to_drr`

#### Transmission Rate Adjustment (Rider 1 / SOS) — **open question**

- **Rate:** $0.01682/kWh (Apr–May 2025); **$0.02322/kWh** (Jun 2025–Mar 2026)
- FERC-regulated PJM transmission pass-through; appears on the **supply (SOS)** portion of the customer bill per Frain
- Volumetric recovery of transmission costs → structural HP cross-subsidy if treated as a fixed pool
- **Decision:** `add_to_drr` *(draft — team chose to top up into delivery RR for BAT; confirm whether supply-side treatment is preferred)*
- **MC note:** Bulk TX marginal cost input is separate (`context/methods/marginal_costs/md_bulk_transmission.md`); topping TX into DRR is about revenue collection, not MC double-counting, but alignment with Frain's bill presentation should be reviewed

#### Universal Service Charge (Rider 3)

- **Rate:** **$0.32/month** per customer (Genability; stable across test year)
- Funds Maryland's Electric Universal Service Program (bill assistance)
- Fixed per-customer pool → cross-subsidy via volumetric rate design only if modeled as embedded in delivery; we treat as **$0.32 × customer count** top-up
- OPC consumer materials sometimes cite **$0.36/month** — Genability shows $0.32; verify against live bills before finalizing
- **Decision:** `add_to_drr`

#### Demand Response (Rider 15) and Electric Reliability Initiative

- **Rate:** **$0.00/kWh** throughout Apr 2025–Mar 2026 test year
- Structural placeholders for program surcharges; included in `add_to_drr` so future non-zero rates flow through automatically
- **Decision:** `add_to_drr` (zero budget in test year)

---

### Supply (`add_to_srr`)

#### Supply commodity — Generation Charge (Rider 1, SOS)

- **Rate:** Month-varying (e.g. $0.10397/kWh Apr 2025 → $0.14365/kWh Mar 2026)
- Bundles SOS energy procurement; Frain describes supply as the largest bill section for average residential customers
- Energy component scales with load (weak cross-subsidy); embedded capacity/admin components may not
- **Decision:** `add_to_srr`
- **Budget method (Track 2):** ResStock-scaled monthly kWh × Genability monthly generation rates (RI pattern), not flat EIA average

**Sub-components (not separately classified in Genability):** SOS total generation rate on the bill also embeds SOS administrative charge and applicable taxes (Rider 1 tariff book). We use the single **Generation Charge** `tariffRateId` as the supply commodity proxy; admin and tax true-ups are excluded separately.

---

### Excluded charges

#### Cost reconciliation and revenue true-ups (`exclude_trueup`)

| Charge | Rider | Rationale |
| ------ | ----- | --------- |
| Energy Cost Adjustment | 8 | SOS wholesale cost true-up |
| Administrative Cost Adjustment | 10 | SOS admin true-up; Frain: excluded from delivery volumetric "Delivery Service Charge" definition |
| Monthly Rate Adjustment | 25 | Revenue decoupling / load forecast error (RDM-like) |
| Multi-Year Plan Adjustment | 16 | MYRP reconciliation; OPC "distribution rate" summaries often bundle this with core delivery — we keep it out of DRR to avoid double-counting with rate-case base |
| Base Distribution Revenue Offset | 34 | Revenue offset rider; $0 in test year |
| RGGI Rate Credit | — | Cost recon / credit; $0 residential |
| RSP Charge/Misc | — | Miscellaneous supply/delivery reconciliation; $0 |
| Franchise Tax, Local Tax, Envir Srchg | 3 / misc | Tax and fee pass-throughs; uniform noise |

#### Eligibility / optional (`exclude_eligibility`)

- **Competitive Billing:** $0.62/month **credit** for customers who choose a competitive supplier — not the default SOS customer
- **Smart Meter Opt-Out:** $5.50/month for customers who opt out of AMI — not the default metered customer

---

## Open questions (for meeting)

1. **Transmission in DRR vs supply RR:** Frain places FERC transmission on the SOS bill. We currently `add_to_drr`. Should BAT top-ups mirror **bill presentation** (supply) or **cost-causation treatment** (delivery MC pipeline)?

2. **Universal Service amount:** Genability $0.32/mo vs OPC $0.36/mo — which is authoritative for the test year?

3. **Schedule RL:** This analysis uses Schedule R Genability tariff (674). RL customers have TOU delivery; BAT may eventually need a separate tariff map / charge pass for RL determinants (Karas E-3 reports RL separately).

4. **Other MD utilities:** DPL, Pepco, SMECO, Potomac Edison, and municipal/co-op utilities have separate Genability snapshots under `md/config/rev_requirement/top-ups/` but no charge decisions yet.

---

## Discover → classify workflow

```bash
cd rate_design/hp_rates
UTILITY=bge just s md discover-charges 2026-03-31
UTILITY=bge just s md classify-charges          # auto-rules; then manual review
UTILITY=bge just s md fetch-monthly-rates 2025-04 2026-03
UTILITY=bge just s md compute-rr              # Track 1: pending delivery_rev_requirements_from_rate_cases.yaml
```

Encoded output: `charge_decisions/bge_charge_decisions.json` → `monthly_rates/bge_monthly_rates_2025.yaml` → (Track 2) `rev_requirement/bge_rate_case_test_year.yaml`.

---

## References

- Karas Direct Testimony, Docket 331766: [DocumentCloud](https://www.documentcloud.org/documents/28475935-331766-7-karas-direct-testimony-wexhibits-7022026-f/)
- Frain Direct Testimony, Docket 331766: `reports2/context/sources/md_hp_rates/331766_2_Frain_Direct Testimony_wExhibits_7022026_F.md`
- OPC Maryland BGE rate summary: [opc.maryland.gov/Consumer-Learning/Utility-Rates-and-Basics/BGE](https://opc.maryland.gov/Consumer-Learning/Utility-Rates-and-Basics/BGE)
- Supply vs delivery allocation: `context/domain/bat_mc_residual/supply_vs_delivery_cost_allocation.md`
- CAIRO LMI / BAT: `context/code/cairo/cairo_lmi_and_bat_analysis.md`

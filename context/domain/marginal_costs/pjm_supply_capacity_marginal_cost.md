# PJM supply capacity marginal cost methodology

How to construct hourly supply **capacity** marginal costs for BAT (Bill Alignment Test) analysis in the PJM footprint. This document separates **PJM-wide rules** (any utility) from **BGE-specific** retail and zone details.

Related docs:

- `context/methods/marginal_costs/capacity_market_comparison_nyiso_isone.md` — NYISO ICAP vs ISO-NE FCM
- `context/code/marginal_costs/ny_supply_marginal_costs.md` — NY implementation (monthly ICAP)
- `utils/pre/marginal_costs/supply_capacity_isone.py` — closest platform template (annual FCA + exceedance)

---

## Scope and purpose

**Goal:** Build an 8760-row `capacity_cost_enduse` (`$/MWh`) parquet for CAIRO BAT runs, analogous to NY (`supply_capacity_nyiso.py`) and RI (`supply_capacity_isone.py`).

**What this is NOT:** PJM does not publish an official hourly marginal capacity cost schedule. RPM defines **obligation attribution** (PLC / 5CP) and **daily charges** (Locational Reliability Charge). The hourly MC profile is a **modeling choice** that translates annual adequacy cost into peak-hour price signals for BAT.

**Critical distinction:**

| PJM market reality                                                 | Our BAT MC construct                                                 |
| ------------------------------------------------------------------ | -------------------------------------------------------------------- |
| Locational Reliability Charge assessed **daily** all delivery year | We spread **annual** `$/kW-year` across **K peak hours**             |
| 5CP defines **who owes how much capacity** (PLC)                   | K=5 is an **analog** to obligation causation, not a PJM billing rule |
| Capacity PLC based on **5 summer hours**                           | Nonzero MC hours = our choice of K and season filter                 |

---

## Glossary

| Term     | Full name                          | Meaning                                                                                                                 |
| -------- | ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **BAT**  | Bill Alignment Test                | CAIRO framework comparing customer marginal cost to bills                                                               |
| **BRA**  | Base Residual Auction              | Primary PJM RPM auction; procures most capacity for a delivery year                                                     |
| **5CP**  | Five Coincident Peaks              | Five highest non-holiday weekday RTO unrestricted daily peak hours (Jun 1–Sep 30) used for capacity PLC                 |
| **CCP**  | Capacity Commitment Period         | ISO-NE term; PJM equivalent is **delivery year** (included for cross-ISO comparison)                                    |
| **CTR**  | Capacity Transfer Rights           | Rights that can offset part of an LSE's Locational Reliability Charge                                                   |
| **DY**   | Delivery Year                      | PJM capacity year: **June 1 – May 31**                                                                                  |
| **EDC**  | Electric Distribution Company      | Local utility (e.g. BGE) that allocates PLCs and uploads obligation data to PJM                                         |
| **FCM**  | Forward Capacity Market            | ISO-NE capacity market (analog to PJM RPM, not PJM)                                                                     |
| **FPR**  | Forecast Pool Requirement          | PJM reserve margin multiplier applied in UCAP obligation                                                                |
| **FLIC** | Forward-Looking Incremental Cost   | Our MC class for capacity auction prices                                                                                |
| **IA**   | Incremental Auction                | PJM RPM auction after BRA; adjusts obligations/prices within a delivery year                                            |
| **ICAP** | Installed Capacity                 | NYISO capacity market (not PJM)                                                                                         |
| **LDA**  | Locational Deliverability Area     | PJM subregion that may clear at a different capacity price than RTO                                                     |
| **LMP**  | Locational Marginal Price          | Real-time wholesale energy price (separate from capacity MC)                                                            |
| **LRC**  | Locational Reliability Charge      | Daily charge = UCAP obligation × Final Zonal Capacity Price                                                             |
| **LSE**  | Load Serving Entity                | Entity responsible for serving load and paying RPM charges (supplier or utility)                                        |
| **LRMC** | Long-Run Marginal Cost             | Economic cost of permanent output increase; capacity is LRMC-like                                                       |
| **NSPL** | Network Service Peak Load          | Transmission peak contribution; **different hours/window** than capacity PLC                                            |
| **OATT** | Open Access Transmission Tariff    | FERC-jurisdictional transmission tariff; governs NSPL                                                                   |
| **PLC**  | Peak Load Contribution             | Customer's average reconciled load during the five PJM-designated capacity peak hours; drives capacity obligation share |
| **PoP**  | Probability of Peak                | Exceedance-based peak allocation (used in platform for dist/bulk TX)                                                    |
| **RAA**  | Reliability Assurance Agreement    | PJM agreement governing capacity compliance                                                                             |
| **RMR**  | Reliability Must-Run               | Out-of-market payments to keep specific units online (separate from RPM MC, but affects bills)                          |
| **RPM**  | Reliability Pricing Model          | PJM's capacity market                                                                                                   |
| **RTO**  | Regional Transmission Organization | PJM-wide unconstrained region; default LDA for many zones                                                               |
| **SOS**  | Standard Offer Service             | Default retail supply service (Maryland term; analogous concept elsewhere)                                              |
| **UCAP** | Unforced Capacity                  | De-rated capacity obligation (MW)                                                                                       |
| **VRR**  | Variable Resource Requirement      | Administered demand curve in RPM auctions                                                                               |

---

# Part I — PJM-wide (any utility)

## 1. RPM market structure

PJM procures generation adequacy through the **Reliability Pricing Model (RPM)**:

1. **Base Residual Auction (BRA)** — primary procurement, historically three years forward (schedule evolving).
2. **Incremental Auctions (IA)** — adjustments within the delivery year (obligation or resource changes).
3. **Daily settlement** — **Locational Reliability Charge (LRC)** on each LSE's **daily UCAP obligation**.

**Sources:**

- [PJM Manual 18: PJM Capacity Market (PDF)](https://www.pjm.com/-/media/DotCom/documents/manuals/m18.pdf) — LRC = Daily UCAP Obligation × Final Zonal Capacity Price
- [PJM Capacity Exchange User Guide (PDF)](https://www.pjm.com/-/media/DotCom/etools/capacity-exchange/capacity-exchange-user-guide.pdf) — BRA, IA, Final Zonal Capacity Price definition
- [PJM RPM cost allocation education (PDF, Jul 2025)](https://www.pjm.com/-/media/DotCom/committees-groups/task-forces/202cstf/2025/20250722/20250722-item-03---202cstf-rpm-cost-allocation-education---presentation.pdf)

### Comparison to NY and ISO-NE (platform context)

| Feature                 | NYISO ICAP             | ISO-NE FCM       | PJM RPM                    |
| ----------------------- | ---------------------- | ---------------- | -------------------------- |
| Primary forward auction | Strip (6 mo)           | FCA (annual)     | BRA (delivery year)        |
| In-period adjustment    | Spot (monthly)         | MRA (monthly)    | IA                         |
| Delivery period         | Monthly capability     | Jun–May CCP      | **Jun–May DY**             |
| Obligation driver       | Coincident peak / UCAP | PLC              | **5CP → PLC**              |
| Platform template       | Monthly 8 peaks        | Annual 100 peaks | **Annual DY + 5CP analog** |

---

## 2. Capacity obligation: 5CP and PLC

### 2.1 How PJM selects the five hours (5CP)

Each summer, PJM:

1. Gathers hourly load for **June 1 – September 30**.
2. Builds RTO unrestricted loads (metered + load-drop add-backs).
3. Identifies the **five highest non-holiday weekday** RTO unrestricted **daily peaks**.
4. Publishes these **five hour-ending timestamps** (~mid-October).

**Sources:**

- [PJM Manual 19 (PDF) §4.3 Peak Load Allocation (5CP)](https://www.pjm.com/-/media/DotCom/documents/manuals/m19.pdf)
- [PJM RPM cost allocation education (PDF)](https://www.pjm.com/-/media/DotCom/committees-groups/task-forces/202cstf/2025/20250722/20250722-item-03---202cstf-rpm-cost-allocation-education---presentation.pdf)

### 2.2 How EDCs compute customer PLC

Each **Electric Distribution Company (EDC)**:

1. Maps **zonal loads** to the five PJM-designated hours.
2. Reconciles customer loads to zonal totals (loss adjustment, scaling).
3. Sets **capacity PLC** = **average** of customer reconciled load during those **five hours**.

PLCs are summed by **Load Serving Entity (LSE)** and reported to PJM. PJM applies scaling factors and **Forecast Pool Requirement (FPR)** to get **daily UCAP obligation**.

**Sources:**

- [PJM Manual 19 §4.3](https://www.pjm.com/-/media/DotCom/documents/manuals/m19.pdf)
- [PJM OATT EDC procedures (e.g. PECO PLC example)](https://agreements.pjm.com/eTariff/transformedTariffs/oatt/Sections/24315.html)
- [PJM Capacity Exchange User Guide](https://www.pjm.com/-/media/DotCom/etools/capacity-exchange/capacity-exchange-user-guide.pdf) — Obligation Peak Load, Daily UCAP

### 2.3 Transmission PLC is different (do not conflate)

**Network Service Peak Load (NSPL)** for transmission uses a **different** peak identification window (e.g. twelve months ending October 31 for BGE). Capacity MC for supply BAT should follow **capacity 5CP**, not NSPL.

---

## 3. Capacity price: what LSEs pay

**Locational Reliability Charge (LRC)** (per zone, per day):

$$\text{LRC} = \text{Daily UCAP Obligation (MW)} \times \text{Final Zonal Capacity Price (\$/MW-day)}$$

- **Final Zonal Capacity Price** = weighted combination of cleared auction prices (BRA + relevant IAs), including locational adders for constrained **Locational Deliverability Areas (LDAs)**.
- Charges are calculated **daily** and billed weekly across the full delivery year.

**Sources:**

- [PJM Manual 18 §1.2.1](https://www.pjm.com/-/media/DotCom/documents/manuals/m18.pdf)
- [PJM Capacity Exchange User Guide — Final Zonal Capacity Price](https://www.pjm.com/-/media/DotCom/etools/capacity-exchange/capacity-exchange-user-guide.pdf)
- [PJM RPM cost allocation education (PDF)](https://www.pjm.com/-/media/DotCom/committees-groups/task-forces/202cstf/2025/20250722/20250722-item-03---202cstf-rpm-cost-allocation-education---presentation.pdf)

### Annualizing for BAT

For a **calendar year** analysis (e.g. 2025), blend two overlapping delivery years (same logic as ISO-NE CCP):

- **DY1** (Jun 2024–May 2025): covers Jan–May of calendar year → **5 months**
- **DY2** (Jun 2025–May 2026): covers Jun–Dec of calendar year → **7 months**

$$\text{capacity\_cost\_kw\_year} = P_{\text{DY1}} \times 5 + P_{\text{DY2}} \times 7$$

where $P$ is the annualized `$/kW-year` from Final Zonal Capacity Price (convert from `$/MW-day`).

---

## 4. Design decisions for hourly BAT MC (any PJM utility)

Each decision below is **independent**. Recommended defaults are marked ★.

### Decision A — Price signal

| Option                                    | Description                                                                             | Pros                                                                                                        | Cons                                                                                     |
| ----------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| **A1 ★ BRA / Final Zonal Capacity Price** | Use cleared RPM price for the utility's LDA for the delivery year (BRA + finalized IAs) | Matches **LRC** mechanics; stable; LRMC/committed-cost interpretation; aligns with DSM avoided-cost studies | Not contemporaneous spot signal; 3-year-forward BRA can diverge from in-year reality     |
| **A2 BRA only (exclude IA adjustments)**  | Lock price at BRA clearing, ignore later IAs                                            | Simple; stable within DY                                                                                    | Diverges from actual in-year LRC when IAs fire; understates/overstates vs realized costs |
| **A3 Latest IA-inclusive (trued-up)**     | Use Final Zonal Capacity Price as it stands when utility files retail rates             | Matches **realized** wholesale cost for that year; good bill reconciliation                                 | More volatile; mixes forward and adjusted prices                                         |
| **A4 IA-only / near-term auction**        | Use incremental auction clearing prices only                                            | Most "current" marginal signal                                                                              | Poor match to committed revenue requirement; not what long-run adequacy costs represent  |

**Platform analogy:** A1 ≈ RI FCA; A4 ≈ NY ICAP Spot / ISO-NE MRA.

---

### Decision B — Allocation time window (monthly vs annual)

| Option                             | Description                                            | Pros                                                                            | Cons                                                                                                  |
| ---------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| **B1 ★ Annual delivery-year cost** | One `$/kW-year` per calendar year (5/7 month DY blend) | Matches RPM's **annual adequacy framing**; parallels `supply_capacity_isone.py` | Ignores within-year IA price changes unless using A3                                                  |
| **B2 Monthly**                     | Allocate a monthly `$/kW-month` to each month's peaks  | Parallels NY ICAP Spot                                                          | **Poor fit for RPM** — LRC is daily, not monthly spot; no PJM monthly capacity spot market equivalent |

**Recommendation:** B1 for all PJM utilities. Do not use NY's monthly pattern unless explicitly modeling a different price series.

---

### Decision C — Peak hours K (how many nonzero MC hours)

| Option                              | Description                                                       | Pros                                                                                    | Cons                                                                   |
| ----------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| **C1 ★ K = 5 (PJM 5CP timestamps)** | Allocate only to PJM-published five summer coincident peak hours  | **Strongest alignment with PLC obligation logic**; clear citation (Manual 19, EDC docs) | Very peaky; only 5 nonzero hours; differs from NY (96) and RI (100)    |
| **C2 K = 5, summer pool**           | Pick top 5 hours from Jun–Sep zone load (replicate PJM algorithm) | Reproducible without waiting for published 5CP; same season logic                       | May not exactly match published timestamps; EDC reconciliation differs |
| **C3 K = 100, annual exceedance**   | Platform default from RI dist/capacity/bulk TX                    | Cross-component consistency within a state run                                          | **No PJM citation** for 100; weak obligation alignment                 |
| **C4 K = 8/month × 12**             | NY ICAP pattern                                                   | Familiar to NY team                                                                     | Wrong price granularity for RPM; no PJM basis                          |
| **C5 K = 500**                      | BAT paper Appendix A reference                                    | Literature anchor                                                                       | Extremely peaky in `$/MWh`; unused elsewhere in platform               |

**Important:** K determines **nonzero hours in the MC parquet**, not PJM's billing calendar. PJM charges LRC **every day**.

---

### Decision D — Season filter for peak identification

| Option                                    | Description                                                                | Pros                                                      | Cons                                            |
| ----------------------------------------- | -------------------------------------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------- |
| **D1 ★ Jun 1 – Sep 30 only**              | Search for peaks only in PJM 5CP window                                    | **Definitionally correct** for capacity PLC per Manual 19 | Excludes winter peaks entirely from capacity MC |
| **D2 Full calendar year**                 | Rank peaks across all 8760 hours                                           | Captures winter load if system winter-peaks               | **Contradicts** PJM capacity 5CP methodology    |
| **D3 Summer + May (dispatch compliance)** | Manual 19 references broader "summer period" for some compliance equations | Slightly broader causation                                | Harder to defend for BAT; blurs 5CP definition  |

**Recommendation:** D1 for capacity MC. Winter peak matters for other components (e.g. winter peak load in some compliance contexts) but **not** for capacity PLC.

---

### Decision E — Load profile for peak identification

| Option                                                          | Description                                                                          | Pros                                            | Cons                                                             |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------- | ---------------------------------------------------------------- |
| **E1 ★ PJM RTO for hour selection, utility zone for weighting** | Use RTO aggregate to find 5CP hours; weight by utility-zone load at those timestamps | Matches PJM system-peaks + zonal reconciliation | Requires both RTO and zonal load data                            |
| **E2 Utility zone top-K**                                       | Rank hours by utility zone load only                                                 | Simple; single-zone utilities                   | Hour selection may differ from PJM 5CP when zone peak ≠ RTO peak |
| **E3 LDA aggregate**                                            | Sum loads across LDA for constrained areas                                           | Needed for constrained LDAs (EMAAC, etc.)       | BGE is RTO; overkill for unconstrained zones                     |

---

### Decision F — Weighting among the K peak hours

| Option                      | Description                                                        | Pros                                                                                    | Cons                                                 |
| --------------------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| **F1 ★ Equal 1/K weights**  | Each of 5 hours gets `annual_cost / 5`                             | Matches EDC definition: PLC = **average** of five hourly loads                          | Differs from platform exceedance convention          |
| **F2 Exceedance weighting** | Threshold-exceedance among the 5 hours (or top-K from summer pool) | Consistent with `allocate_annual_exceedance_to_hours` in `supply_utils.py`; used for RI | **Not** how PLC is defined (average, not exceedance) |
| **F3 Load-proportional**    | Weight by zone load in each hour                                   | Simple physical interpretation                                                          | Still not identical to PLC reconciliation            |

**Recommendation:** F1 if prioritizing PJM fidelity; F2 if prioritizing platform consistency. Document the choice explicitly.

---

### Decision G — Hourly MC vs retail bill recovery

| Option                                  | Description                                             | Pros                                                       | Cons                                                               |
| --------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------ |
| **G1 ★ Peak-hour MC + volumetric bill** | MC on 5CP hours; residential supply charge flat `¢/kWh` | Correct BAT cross-subsidy story (peak cost, flat recovery) | MC timing ≠ bill timing                                            |
| **G2 Spread MC across all hours**       | Uniform `$/MWh`                                         | Matches flat bill mathematically                           | **Wrong** cost-causation signal; understates peak HP cross-subsidy |
| **G3 TOU-block MC**                     | Put MC in defined on-peak periods matching utility TOU  | Aligns with shaped SOS for TOU customers                   | Requires tariff-specific period definitions; more work             |

BAT residential analysis typically uses **G1** for the cross-subsidy diagnosis.

---

## 5. ★ Recommended PJM-wide default (v1)

For any new PJM utility MC pipeline:

1. **Price (A1):** Final Zonal Capacity Price for the utility's LDA, annualized to `$/kW-year`, calendar-year 5/7 DY blend.
2. **Allocation window (B1):** Annual.
3. **Peak hours (C1 + D1):** K = 5, Jun–Sep, use PJM-published 5CP timestamps (or replicate per Manual 19).
4. **Load (E1):** RTO hour selection; utility-zone load for weights.
5. **Weights (F1):** Equal 1/5 (PLC-average analog).
6. **Output:** `capacity_cost_per_kw` on 5 hours → `prepare_component_output(scale=1000)` → `capacity_cost_enduse` `$/MWh`; all other hours zero.

**Validation:** Sum of hourly `capacity_cost_per_kw` = annual `$/kW-year` (same check as RI FCA validation).

**Sensitivity runs:** C3 (K=100), F2 (exceedance), A3 (IA-trued price).

---

## 6. Relationship to platform code

| PJM step              | Platform analog                                                       |
| --------------------- | --------------------------------------------------------------------- |
| DY 5/7 blend          | `resolve_fca_price_for_calendar_year()` in `supply_capacity_isone.py` |
| Exceedance allocation | `allocate_annual_exceedance_to_hours()` in `supply_utils.py`          |
| 8760 output           | `prepare_component_output()` in `supply_utils.py`                     |
| CLI entrypoint        | Extend `generate_supply_capacity_mc.py` with `--iso pjm`              |

**Do not clone NY** (`supply_capacity_nyiso.py`) unless implementing a monthly price series (not recommended for RPM).

---

# Part II — BGE-specific

## 7. Zone and LDA

- BGE serves the **BGE zone** in PJM (Maryland territory).
- BGE is **not** a separately constrained LDA; it typically clears at the **RTO** Final Zonal Capacity Price.
- Pepco/DPL serve other Maryland areas under different zones/LDAs — do not reuse BGE prices for them.

**Sources:**

- [Brattle — Alternative Resource Adequacy Structures for Maryland (PDF)](https://www.brattle.com/wp-content/uploads/2021/06/21870_alternative_resource_adequacy_structures_for_maryland_-_review_of_the_pjm_capacity_market_and_options_for_enhancing_alignment_with_marylands_clean_electricity_future.pdf) — LDA structure; BGE in SWMAAC/RTO context
- [MD OPC — PJM transmission cost impacts (PDF)](https://opc.maryland.gov/Portals/0/Files/Publications/Rising%20Transmission%20Costs%202026-03-25%20CORRECTED%20FINAL.pdf) — BGE entirely within Maryland

---

## 8. BGE PLC implementation

BGE's supplier-facing documentation matches PJM Manual 19:

- PJM identifies **five** system coincident peak hours (**June 1 – September 30**).
- BGE maps **BGE zonal loads** to those hours and reconciles to customers.
- **Capacity PLC** = **average** of reconciled customer load during those five hours.
- Supplier PLCs reported daily to PJM via eRPM.

**Source:**

- [BGE Peak Load Contribution (PLC) Overview](https://supplier.bge.com/electric/load/plcs.asp)

**BGE-specific detail:** BGE applies loss factors by voltage class and reconciles non-interval (monthly-metered) customers via load profiles before averaging.

---

## 9. BGE retail recovery (Standard Offer Service)

### 9.1 Schedule R (flat residential)

- SOS **energy rate** (`¢/kWh`) bundles **energy + capacity + ancillary** (full requirements).
- Capacity is **not** a separate demand charge for residential; it is **volumetric**.
- Cross-subsidy implication: peak-driven wholesale capacity cost, flat kWh recovery → HP winter load can overpay capacity share on a summer-peaking system.

**Source:**

- [BGE Rider 1 — Standard Offer Service (PDF, P.S.C. Md. E-6)](https://azure-na-assets.contentstack.com/v3/assets/blt71bfe6e8a1c2d265/blt18ec3ab504a9abd9/665a1445b41a32000a317de2/Rdr_1.pdf)

### 9.2 Schedule RL / GS (TOU residential)

- TOU SOS energy rates are **"shaped by rating period using historical summer settlement data and capacity costs."**
- Capacity tilted toward summer on-peak periods — closer to cost-causation than Schedule R.
- Separate BAT path if analyzing TOU customers (Decision G3).

**Source:**

- [BGE Supplement 728, Dec 2024 (PDF)](http://www.energychoicematters.com/stories/bgesummer2025.pdf)

### 9.3 IA true-up in rate filings

- BGE filed summer 2025 SOS rates with a **capacity proxy**, then revised **+16%** for Schedule R when **PJM IA #3** finalized actual capacity costs.
- For bill reconciliation of a specific year, **A3 (IA-trued price)** may match BGE filings better than BRA-only.

**Source:**

- [EnergyChoiceMatters — BGE summer 2025 rate revision (Mar 2025)](https://www.energychoicematters.com/stories/20250314b.html)

### 9.4 PSC shoulder-month smoothing (not MC)

- MD PSC ordered BGE to recover some 2025/26 capacity costs over **shoulder months** (Sep–Nov 2025, Mar–May 2026) rather than summer.
- This is **ratemaking cash-flow smoothing**, not PJM obligation logic. **Do not** encode in marginal cost.

**Sources:**

- [EnergyChoiceMatters — PSC deferral order (May 2025)](http://www.energychoicematters.com/stories/20250529a.html)
- [MD OPC bulletin on supply rates](https://content.govdelivery.com/accounts/MDOPC/bulletins/40304ab)

### 9.5 RMR (separate from RPM MC)

- **Reliability Must-Run** payments (e.g. Brandon Shores) are out-of-market charges that affect BGE bills but are **not** part of RPM Locational Reliability Charge.
- Treat as **residual / separate line item** in BAT charge classification, not in capacity MC.

**Source:**

- [MD OPC / Synapse — RMR and capacity bill impacts (PDF)](https://opc.maryland.gov/Portals/0/Files/Publications/RMR%20Bill%20and%20Rates%20Impact%20Report_2024-08-14%20Final.pdf)

---

## 10. BGE DSM / avoided capacity (regulatory context)

BGE's avoided-capacity test methodology in PSC cases uses:

- **BRA** price mitigation concepts
- **Zonal peak forecast** effects on obligation
- Phased MW reductions over several delivery years

Useful for validating order-of-magnitude but **not** a direct hourly MC formula.

**Sources:**

- [MD PSC Order 87591, Case 9406 (PDF)](https://psc.maryland.gov/wp-content/uploads/Order-No.-87591-Case-No.-9406-BGE-Rate-Case.pdf)
- [Chernick testimony excerpt (PDF)](https://resourceinsight.com/wp-content/uploads/2017/02/PLC-309_MD_CN9406_Direct_2-2016-2.pdf)

---

## 11. ★ Recommended BGE v1 package

| Parameter              | BGE choice                                                                                   |
| ---------------------- | -------------------------------------------------------------------------------------------- |
| LDA / price zone       | RTO (BGE zone unconstrained)                                                                 |
| Price                  | Final Zonal Capacity Price, 5/7 DY calendar-year blend; sensitivity with IA-trued price      |
| K                      | 5 (PJM 5CP timestamps)                                                                       |
| Season                 | Jun 1 – Sep 30                                                                               |
| Load                   | BGE zone load at 5CP timestamps (E1)                                                         |
| Weights                | Equal 1/5 (F1, PLC-average)                                                                  |
| Bill side              | Schedule R flat SOS; note PSC shoulder smoothing as non-MC                                   |
| Output path (proposed) | `s3://data.sb/switchbox/marginal_costs/md/supply/capacity/utility=bge/year={Y}/data.parquet` |

---

## 12. Data and implementation checklist

### PJM data needed (any utility)

| Dataset                         | Source                                                          | Use                        |
| ------------------------------- | --------------------------------------------------------------- | -------------------------- |
| BRA / IA clearing prices by LDA | [PJM Data Miner](https://dataminer2.pjm.com/) / auction results | Final Zonal Capacity Price |
| 5CP hour timestamps             | PJM posting (~October)                                          | Peak hour selection        |
| RTO + zonal hourly load         | PJM Data Miner / EIA                                            | PLC-weighting              |
| Delivery year calendar          | PJM Manual 18                                                   | 5/7 month blend            |

### BGE-specific

| Dataset              | Source                      | Use                    |
| -------------------- | --------------------------- | ---------------------- |
| BGE zone hourly load | PJM `BGE` zone              | Zonal weighting at 5CP |
| SOS energy rate      | BGE Rider 1 filings         | Bill reconciliation    |
| Utility zone mapping | To be created (`data/pjm/`) | `utility → zone, LDA`  |

### Platform work

Implemented (curated data pipelines, reproducible from committed source intermediates):

- `data/pjm/capacity/rpm/` — RPM BRA + Final Zonal prices, DY 2018/19–2026/27. Per-DY markdown intermediates under `sources/` (`rpm_YYYY_YY.md`, both source URLs in the header) → `just convert` → CSV. Rows carry `source_url` (final zonal) and `bra_source_url` citations.
- `data/pjm/capacity/5cp/` — summer 5CP peaks, **summers 2021–2025** (only those feeding 2025+ runs are retained; earlier summers dropped). Per-summer markdown intermediates under `sources/` (`5cp_YYYY.md`) → `just convert` → CSV.
- `data/pjm/zone_mapping/` — utility → zone/LDA crosswalk.

Not yet implemented:

- `data/pjm/hourly_demand/zones/` — zone load pipeline (deferred; see Future work below)
- `utils/pre/marginal_costs/supply_capacity_pjm.py` — computation
- `rate_design/hp_rates/md/` — state config (future)

---

## 13. References

1. [PJM Manual 18 — PJM Capacity Market (PDF)](https://www.pjm.com/-/media/DotCom/documents/manuals/m18.pdf)
2. [PJM Manual 19 — Load Forecasting (PDF) §4.3 5CP](https://www.pjm.com/-/media/DotCom/documents/manuals/m19.pdf)
3. [PJM Capacity Exchange User Guide (PDF)](https://www.pjm.com/-/media/DotCom/etools/capacity-exchange/capacity-exchange-user-guide.pdf)
4. [PJM RPM cost allocation education (PDF, Jul 2025)](https://www.pjm.com/-/media/DotCom/committees-groups/task-forces/202cstf/2025/20250722/20250722-item-03---202cstf-rpm-cost-allocation-education---presentation.pdf)
5. [PJM OATT — EDC PLC procedures (PECO example)](https://agreements.pjm.com/eTariff/transformedTariffs/oatt/Sections/24315.html)
6. [BGE — Peak Load Contribution Overview](https://supplier.bge.com/electric/load/plcs.asp)
7. [BGE Rider 1 — Standard Offer Service (PDF)](https://azure-na-assets.contentstack.com/v3/assets/blt71bfe6e8a1c2d265/blt18ec3ab504a9abd9/665a1445b41a32000a317de2/Rdr_1.pdf)
8. [BGE Supplement 728 — TOU SOS shaping (PDF)](http://www.energychoicematters.com/stories/bgesummer2025.pdf)
9. [EnergyChoiceMatters — BGE IA3 capacity true-up (Mar 2025)](https://www.energychoicematters.com/stories/20250314b.html)
10. [EnergyChoiceMatters — MD PSC shoulder-month recovery (May 2025)](http://www.energychoicematters.com/stories/20250529a.html)
11. [MD OPC / Synapse — 2025/26 capacity and RMR bill impacts (PDF)](https://opc.maryland.gov/Portals/0/Files/Publications/RMR%20Bill%20and%20Rates%20Impact%20Report_2024-08-14%20Final.pdf)
12. [Brattle — PJM capacity market review for Maryland (PDF)](https://www.brattle.com/wp-content/uploads/2021/06/21870_alternative_resource_adequacy_structures_for_maryland_-_review_of_the_pjm_capacity_market_and_options_for_enhancing_alignment_with_marylands_clean_electricity_future.pdf)
13. [MD PSC Order 87591 — BGE rate case (PDF)](https://psc.maryland.gov/wp-content/uploads/Order-No.-87591-Case-No.-9406-BGE-Rate-Case.pdf)
14. Platform: `context/methods/marginal_costs/capacity_market_comparison_nyiso_isone.md`
15. Platform: `context/code/marginal_costs/ny_supply_marginal_costs.md`
16. Platform: `utils/pre/marginal_costs/supply_capacity_isone.py`

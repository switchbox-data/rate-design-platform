# MD bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in Maryland. All MD utilities
are within PJM territory, so the relevant framework is PJM's OATT Attachment H (NITS rates and
NSPL peaks), not ISO-NE's RNS/AESC. This document covers the available data, the recommended
methodology, the NITS/ATRR values sourced from PJM's published documents, and the seasonal
allocation logic derived from PJM's NSPL peak-date data.

Related docs:

- `context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md` — PJM capacity (RPM/5-CP); **do not conflate with bulk TX**
- `context/methods/marginal_costs/ny_bulk_transmission_marginal_cost.md` — NY approach (OATT ATRR proxy, same method)
- `context/methods/marginal_costs/ri_bulk_transmission_marginal_cost.md` — RI approach (AESC avoided PTF, cleaner LRMC)
- `context/domain/marginal_costs/ny_bulk_transmission_cost_recovery.md` — how TX costs reach NY customers
- `context/domain/marginal_costs/ri_bulk_transmission_cost_recovery.md` — how TX costs reach RI customers

---

## Clarification: what the RTO pushback on NITS rates means

The E3 2025 Illinois ICC-VDER report used the PJM/MISO **NTS (Network Transmission Service) rate
as an upper-bound proxy** for avoided transmission capacity cost. In response, E3 noted:

> _"each RTO independently noted that the NTS rate is not an appropriate indicator of their
> capacity-driven marginal costs… [they] were not able to provide a more accurate, specific
> marginal cost for transmission capacity at this time."_

This does **not** mean the RTOs endorse the NITS rate as the right answer. It means:

- The NITS rate (= OATT Attachment H ATRR ÷ NSPL billing units) is an **embedded average
  cost** — a blend of depreciated decades-old plant and new investment — not a true LRMC.
- PJM and MISO themselves said they cannot produce a better LRMC estimate today.
- E3's conclusion: use it as an **acknowledged upper-bound proxy**, document it, and update
  when better data is available (e.g., PJM's FERC Order 1920 long-term transmission plan).

This is the accepted practice for PJM-territory avoided-cost studies. The same framing should be
used for MD.

**Source:** [E3 ICC-VDER Report, Illinois, Jan 2025](https://www.ethree.com/wp-content/uploads/2025/01/ICC-VDER-Report-FINAL-2025-1-17.pdf),
Section 3.2, Table 8.

---

## The MD marginal cost gap

| Cost component                  | Source                         | Status         |
| ------------------------------- | ------------------------------ | -------------- |
| Energy                          | PJM LMP / Cambium LRMER        | Have it        |
| Generation capacity             | PJM RPM / Cambium              | Have it        |
| Sub-transmission & distribution | Utility MCOS studies ($/kW-yr) | Not yet for MD |
| **Bulk transmission**           | **PJM NITS rate (OATT ATRR)**  | **This doc**   |

---

## Key distinction: capacity 5-CP vs. transmission NSPL

**Do not conflate these two PJM peak concepts.** They drive two completely different cost components:

| Concept                              | What it drives                      | Peak window                               | Season                               |
| ------------------------------------ | ----------------------------------- | ----------------------------------------- | ------------------------------------ |
| **5-CP (Five Coincident Peaks)**     | Capacity obligation (PLC / RPM LRC) | Top-5 RTO system peaks, Jun–Sep           | Summer only                          |
| **NSPL (Network Service Peak Load)** | Transmission cost allocation (NITS) | Top-5 zonal peaks, Nov–Oct rolling window | **Summer or winter, zone-dependent** |

The **5-CP** is the right methodology for **supply capacity MC** (see
`context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md`).

The **NSPL** is the right methodology for **bulk TX MC**. The key difference:

- NSPL uses a **12-month rolling window ending October 31** (varies by EDC; see each
  utility's OATT Attachment M-2 filing at https://www.pjm.com/markets-and-operations/billing-settlements-and-credit/theo-plc-and-nspl).
- NSPL is based on the **5 highest zonal load hours** during the relevant season
  (summer Jun–Sep if the zone is summer-peaking; winter Dec–Mar if winter-peaking).
- The season depends on which peak drives the zone's NSPL — determined by the actual
  zonal peak date published in PJM's annual NSPL document.

**Source:** [FirstEnergy/Potomac Edison NSPL Manual (PEMD)](https://www.firstenergycorp.com/content/dam/supplierservices/files/supplier-registration/PJMCapacityManualPEMD.pdf);
[Foster EC: PJM NSPL explainer](https://www.fosterec.com/pjm-transmission-capacity-costs-nspl-nits/).

---

## MD utility zone structure

| MD IOU            | PJM Zone | Transmission Owner (OATT Filer)        | Jurisdiction                    |
| ----------------- | -------- | -------------------------------------- | ------------------------------- |
| BGE               | BGE      | Baltimore Gas & Electric (Exelon)      | Entirely MD                     |
| Pepco             | PEPCO    | Potomac Electric Power Co. (Exelon)    | MD (Montgomery, PG) + DC        |
| Delmarva Power    | DPL      | Delmarva Power & Light (Exelon)        | MD Eastern Shore + DE           |
| Potomac Edison    | APS      | South FirstEnergy Operating Companies  | Western MD + PA, WV, VA, OH     |
| SMECO             | PEPCO    | Southern Maryland Electric Cooperative | Inside PEPCO zone (Southern MD) |
| Choptank Electric | DPL      | Co-op inside DPL zone (Eastern Shore)  | DPL zone rate applies           |

Co-ops and municipals (SMECO, Choptank, AN Electric) do **not** own transmission. They pay
through their host zone's NITS rate. For BAT purposes, assign them the NITS rate of their host zone.

Note: The APS zone spans multiple states. The MD OPC (March 2026 report) scales APS zone costs to
MD using the ratio of MD's 2024 peak load to total APS zonal peak load.

---

## PJM NITS rates and NSPL data

### 2025 NITS rates (from PJM official publications)

PJM publishes NITS rates at:
https://www.pjm.com/markets-and-operations/billing-settlements-and-credit

NITS rates update up to twice per year (some TOs update January 1, some June 1).

**As of January 1, 2025:**

| Zone  | Transmission Owner(s)                 | ATRR ($M)  | NITS Rate ($/MW-yr) | Equiv. ($/kW-yr) |
| ----- | ------------------------------------- | ---------- | ------------------- | ---------------- |
| BGE   | Baltimore Gas and Electric Company    | $357.8     | $55,851             | $55.85           |
| DPL   | Delmarva Power & Light Company        | $246.8     | $61,897             | $61.90           |
| PEPCO | Potomac Electric Power Co. + SMECO    | (see note) | $54,684             | $54.68           |
| APS   | South FirstEnergy Operating Companies | (see note) | ~$17–18             | ~$17–18          |

**As of June 1, 2025:**

| Zone  | Transmission Owner(s)                 | ATRR ($M)     | NITS Rate ($/MW-yr) | Equiv. ($/kW-yr) |
| ----- | ------------------------------------- | ------------- | ------------------- | ---------------- |
| BGE   | Baltimore Gas and Electric Company    | $399.7        | $59,070             | $59.07           |
| DPL   | Delmarva Power & Light Company + ODEC | $269.9 + $5.8 | $65,833             | $65.83           |
| PEPCO | Potomac Electric Power Co. + SMECO    | SMECO: $17.1  | $60,517             | $60.52           |
| APS   | South FirstEnergy Operating Companies | $156.1        | $25,052             | $25.05           |

**Source (primary):**

- Jan 2025: https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-integration-trans-service-jan-2025.pdf
- Jun 2025: https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-integration-trans-service-june-2025.pdf

**Note on APS:** The Jan 2025 APS NITS rate was not captured in the initial search; the Jun 2025
rate of $25,052/MW-yr is confirmed. To get Jan 2025, retrieve the Jan 2025 PDF directly. The
full-year 2025 blended rate should use 5/7 month weighting (Jan–May / Jun–Dec).

**Note on DPL zone:** DPL zone includes Old Dominion Electric Cooperative (ODEC) as a second
transmission owner. The total zonal ATRR includes both Delmarva P&L and ODEC contributions; the
published NITS rate is the combined zonal rate applied to all LSEs in the DPL zone.

**Note on PEPCO zone:** PEPCO zone includes SMECO as a second transmission owner ($17.1M ATRR
at Jun 2025). The NITS rate is the combined zonal rate.

### Calendar-year 2025 blended rate (5/7 method)

Formula: `(5 × Jan_rate + 7 × Jun_rate) / 12`

| Zone  | Jan Rate ($/kW-yr) | Jun Rate ($/kW-yr) | 2025 Blended ($/kW-yr) |
| ----- | ------------------ | ------------------ | ---------------------- |
| BGE   | $55.85             | $59.07             | ~$57.72                |
| DPL   | $61.90             | $65.83             | ~$64.25                |
| PEPCO | $54.68             | $60.52             | ~$58.12                |
| APS   | TBD (need Jan PDF) | $25.05             | ~$25 (est.)            |

### NSPL zonal peak data (for seasonal allocation)

PJM publishes the zonal peak hour used for each zone's NSPL annually at:
https://www.pjm.com/markets-and-operations/billing-settlements-and-credit/theo-plc-and-nspl

**2025 NSPL peaks (period 11/1/2023–10/31/2024):**

| Zone  | Zonal Peak (MW) | Date      | HE (EPT) | Season     | Implication                          |
| ----- | --------------- | --------- | -------- | ---------- | ------------------------------------ |
| BGE   | 6,765.9         | 7/16/2024 | 18       | Summer     | 100% summer TX allocation            |
| DPL   | 4,188.5         | 7/16/2024 | 18       | Summer     | 100% summer TX allocation            |
| PEPCO | 6,161.7         | 7/16/2024 | 18       | Summer     | 100% summer TX allocation            |
| APS   | 8,937.6         | 1/22/2024 | 8        | **Winter** | **Winter TX allocation** ← important |

**Source:** https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-service-peak-loads-2025.pdf

**Critical finding for heat pump rate design:**

- **BGE, PEPCO, DPL**: Bulk TX peak hours are in **summer**. HP winter load does **not** fall
  in NSPL peak hours → bulk TX costs do not penalize HP customers in winter for these utilities.
  This supports a winter TOU discount for HP customers.

- **APS (Potomac Edison, western MD)**: Zone peaked in **January** (winter). HP winter heating
  load **does** coincide with bulk TX cost-causation hours. This weakens the case for a winter
  HP discount on bulk TX for Potomac Edison customers and may require a separate treatment.

Note: the 2025 NSPL is set by the 2023–2024 measurement period. Check future NSPL publications
(released each fall by PJM) for updated seasonal peak dates, as warming summers could shift APS
back to summer-peaking.

---

## Recommended methodology

### Step 1 — Annual $/kW-yr value

Use the published PJM **NITS rate** from OATT Attachment H as the bulk TX marginal cost value.
This is the embedded-cost upper-bound proxy, following the E3 Illinois approach (same method as
NY Option 1 in `ny_bulk_transmission_marginal_cost.md`). Document it as an upper bound.

For a calendar-year BAT run, compute the **5/7 delivery-year blend** using the Jan and Jun NITS
rates (same method as the supply capacity blend in `pjm_supply_capacity_marginal_cost.md`).

### Step 2 — Seasonal allocation

Use the zonal NSPL peak date (from PJM's annual NSPL publication) to determine whether the
zone's bulk TX costs are summer- or winter-driven:

- **Summer-peaking zones (BGE, DPL, PEPCO):** Allocate 100% of the annual $/kW-yr to summer
  peak hours (June–September). Winter hours receive zero.
- **Winter-peaking zones (APS):** Allocate 100% to winter peak hours (December–March).

Verify each year: PJM publishes the updated NSPL document in late fall for the following January.
If APS shifts back to summer-peaking, update accordingly.

**Source for seasonal logic:** [PA PUC Act 129 Avoided T&D Cost Study (2025)](https://www.puc.pa.gov/pcdocs/1855615.pdf),
Table 7 — directly shows how Pennsylvania PJM utilities (structurally identical to MD utilities)
split their transmission avoided costs between summer and winter based on historical NSPL frequency.

### Step 3 — Hourly allocation within the relevant season

Apply the **PoP (Probability of Peak) exceedance** method to the appropriate season's zone load,
consistent with the platform convention used for RI ISO-NE:

1. Load the EIA/PJM zone hourly load for the relevant zone and year.
2. Filter to the relevant season (June–September for BGE/DPL/PEPCO; December–March for APS).
3. Identify the top-K hours by load (K = 5 for strict NSPL fidelity per the FirstEnergy PEMD
   manual, or K = 100 for platform consistency; see Decision C in
   `context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md`).
4. Allocate the annual $/kW-yr to those hours using exceedance weighting
   (`allocate_annual_exceedance_to_hours()` in `supply_utils.py`) or equal 1/K weights
   (strict PLC-average analog, Decision F1 from the capacity doc).
5. Fill all other hours with zero.
6. Validate: sum of hourly allocations = annual $/kW-yr.

**K choice recommendation:** Start with K = 100 (platform consistency), then run sensitivity at
K = 5 (NSPL strict). The difference is unlikely to be material for the BAT but is worth documenting.

---

## Comparison with NY and RI approaches

| Feature                    | NY (NYISO)                                  | RI (ISO-NE)                        | **MD (PJM)**                                                            |
| -------------------------- | ------------------------------------------- | ---------------------------------- | ----------------------------------------------------------------------- |
| Best available MC source   | OATT ATRR proxy (each TO)                   | AESC avoided PTF cost (LRMC)       | **OATT NITS rate proxy (each zone)**                                    |
| Institutional backing      | None for bulk TX MC                         | AESC used by RI PUC                | E3 Illinois report (state-commissioned)                                 |
| Data quality               | Embedded average only                       | Year-by-year LRMC projections      | Embedded average; updated annually                                      |
| Peak allocation            | NYISO zonal load, SCR hours (summer+winter) | NE system load, 12-CP informed     | **Zonal load, NSPL season-dependent**                                   |
| Season                     | Summer + winter (SCR)                       | Primarily summer (12-CP)           | **Summer (BGE/DPL/PEPCO); winter (APS)**                                |
| RTO pushback on rate       | FERC/NYISO have no better estimate          | AESC is preferred; RNS is fallback | **PJM/MISO said NTS is not ideal LRMC; no better alternative provided** |
| Approximate $/kW-yr (2025) | ~$36–120 (by TO)                            | ~$69 (AESC PTF)                    | **~$25–65 (by zone)**                                                   |

---

## Data sources and where to get them

| Dataset                      | URL / Location                                                                                                           | Use                                                |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------- |
| PJM NITS rates (current)     | https://www.pjm.com/markets-and-operations/billing-settlements-and-credit                                                | Annual $/kW-yr value per zone                      |
| PJM NITS archive PDFs        | Jan/Jun each year: `network-integration-trans-service-{jan,june}-{year}.pdf` at above URL                                | Historical year-by-year blended rates              |
| PJM NSPL (zonal peak dates)  | https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-service-peak-loads-{year}.pdf                         | Seasonal allocation (which season each zone peaks) |
| PJM OATT Formula Rates       | https://www.pjm.com/markets-and-operations/billing-settlements-and-credit/formula-rates                                  | ATRR inputs, true-up filings                       |
| NSPL methodology (per EDC)   | https://www.pjm.com/markets-and-operations/billing-settlements-and-credit/theo-plc-and-nspl                              | How each EDC computes NSPL (OATT Attachment M-2)   |
| EIA/PJM zone hourly loads    | Already on S3: `data/eia/hourly_loads/pjm/` or `data/pjm/` (see existing pipelines)                                      | Hourly peak identification for PoP allocation      |
| MD OPC rising TX cost report | https://opc.maryland.gov/Portals/0/Files/Publications/Rising%20Transmission%20Costs%202026-03-25%20CORRECTED%20FINAL.pdf | MD-specific policy context and scaling rationale   |
| E3 Illinois ICC-VDER report  | https://www.ethree.com/wp-content/uploads/2025/01/ICC-VDER-Report-FINAL-2025-1-17.pdf                                    | Precedent for NITS-as-upper-bound approach         |
| PA PUC avoided TX study      | https://www.puc.pa.gov/pcdocs/1855615.pdf                                                                                | Seasonal allocation precedent for PJM utilities    |

---

## Plan of next actions

### 1 — Retrieve missing NITS data points

- [ ] Download the Jan 2025 PJM NITS PDF and extract the APS (South FirstEnergy) Jan 2025 rate.
      URL: `https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-integration-trans-service-jan-2025.pdf`
- [ ] Retrieve historical NITS rates for 2019–2024 from PJM archive PDFs (same URL pattern).
      This enables multi-year BAT runs using the correct in-year blended rate rather than 2025 values
      for all years.
- [ ] Check the 2025 NSPL PDF for the APS zone: confirm 1/22/2024 peak was winter (already confirmed),
      and check whether the 5 peak hours for APS are all in December–March.
      URL: `https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-service-peak-loads-2025.pdf`

### 2 — Verify zone-level EIA/PJM hourly load data availability

- [ ] Confirm that PJM zone-level hourly loads (BGE, DPL, PEPCO, APS) are already on S3 or
      available via the existing `data/eia/hourly_loads/pjm/` or `data/pjm/` pipeline.
      These are needed for Step 3 (hourly PoP allocation).
- [ ] If not available, extend the existing EIA zone load fetch to include PJM zones for MD.

### 3 — Implement `bulk_tx_pjm.py`

Create `utils/data_prep/marginal_costs/bulk_tx_pjm.py` following the same structure as
`bulk_tx_isone.py` and `bulk_tx_nyiso.py`:

- Input: zone name (`bge`, `dpl`, `pepco`, `aps`), year, NITS rate ($/kW-yr)
- Seasonal filter: summer (Jun–Sep) for BGE/DPL/PEPCO; winter (Dec–Mar) for APS
- Hourly allocation: `allocate_annual_exceedance_to_hours()` on zone load, top-K hours
- Output: 8760-row DataFrame with `timestamp`, `bulk_tx_cost_enduse`
- Validation: sum of allocations = annual $/kW-yr

Extend `generate_bulk_tx_mc.py` with `--iso pjm` path.

### 4 — Create utility-to-zone mapping for MD

Create `data/pjm/zone_mapping/md_utility_zone_mapping.csv` (or extend the existing zone mapping
if one exists for capacity) with columns:

- `utility` (std_name from `utils/utility_codes.py`)
- `pjm_zone` (bgd, dpl, pepco, aps)
- `nits_rate_jan_kw_yr` (Jan value for each year)
- `nits_rate_jun_kw_yr` (Jun value for each year)
- `nspl_season` (summer / winter, from annual NSPL publication)

For co-ops: SMECO → PEPCO zone; Choptank → DPL zone.

### 5 — Create Justfile recipes for MD bulk TX MC

Add recipes to `rate_design/hp_rates/md/Justfile` analogous to the existing supply energy MC
recipes. Each recipe should take `utility` and `year` as arguments and invoke
`generate_bulk_tx_mc.py --iso pjm`.

### 6 — Sensitivity analysis

After implementation, run the BAT with:

- Primary: 2025 NITS blended rate, K = 100 summer peak hours
- Sensitivity A: K = 5 (strict NSPL)
- Sensitivity B: Use the MD OPC report's forward-looking cost estimates (if available) as an
  alternative to the embedded NITS rate

### 7 — Document in context/README.md

After creating the implementation files, update `context/README.md` to add this doc to the
methods/marginal_costs index.

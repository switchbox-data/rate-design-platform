# MD bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in Maryland. All MD utilities
are within PJM territory, so the relevant framework is PJM's OATT Attachment H (NITS rates and
NSPL peaks), not ISO-NE's RNS/AESC. This document covers the available data, the recommended
methodology (following E3's ICC-VDER full-year PCAF approach with K=150), and the NITS/ATRR
values sourced from PJM's published documents.

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

The **NSPL** is how PJM bills transmission costs. Our allocation methodology (K=150 PCAF,
full year) deliberately departs from NSPL billing — see "Why K = 150" section below. The key
NSPL difference vs. 5-CP:

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
| DPL   | Delmarva Power & Light Company + ODEC | $246.8     | $61,897             | $61.90           |
| PEPCO | Potomac Electric Power Co. + SMECO    | (see note) | $54,684             | $54.68           |
| APS   | South FirstEnergy Operating Companies | $156.1     | $25,052             | $25.05           |

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

**Note on APS:** The APS NITS rate did not change between Jan and Jun 2025 ($25,051.72/MW-yr).
The ATRR ($156.1M) and NSPL (8,937.6 MW) remained constant across both periods.

**Note on DPL zone:** DPL zone includes Old Dominion Electric Cooperative (ODEC) as a second
transmission owner ($5.8M ATRR at Jun 2025). The total zonal ATRR includes both Delmarva P&L
and ODEC contributions; the published NITS rate is the combined zonal rate applied to all LSEs
in the DPL zone.

**Note on PEPCO zone:** PEPCO zone includes SMECO as a second transmission owner ($17.1M ATRR
at Jun 2025). The NITS rate is the combined zonal rate.

### Calendar-year 2025 blended rate (day-weighted)

PJM bills NITS daily (Manual 27 §5.2.2). For non-leap 2025: 151 days (Jan 1–May 31) at Jan
rate + 214 days (Jun 1–Dec 31) at Jun rate.

Formula: `blended = (151 × jan + 214 × jun) / 365`

| Zone  | Jan Rate ($/kW-yr) | Jun Rate ($/kW-yr) | 2025 Blended ($/kW-yr) |
| ----- | ------------------ | ------------------ | ---------------------- |
| BGE   | $55.85             | $59.07             | $57.74                 |
| DPL   | $61.90             | $65.83             | $64.20                 |
| PEPCO | $54.68             | $60.52             | $58.10                 |
| APS   | $25.05             | $25.05             | $25.05                 |

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

## Recommended methodology (FINALIZED)

### Step 1 — Annual $/kW-yr value (day-weighted blended NITS rate)

Use the published PJM **NITS rate** from OATT Attachment H as the bulk TX marginal cost value.
This is the embedded-cost upper-bound proxy, following E3's 2025 Illinois ICC-VDER methodology
(Section 3.2, Table 8). Document it as an upper bound, acknowledging the RTO pushback but noting
no better alternative exists (see "Clarification" section at top of this doc).

#### What value does E3 use?

E3's ICC-VDER report (Table 8) uses a single **"2024 Network Transmission Service Rate"** taken
directly from MISO and PJM:

| Utility | NTS Rate (E3 Table 8) | RTO  |
| ------- | --------------------- | ---- |
| ComEd   | $39.80/kW-yr          | PJM  |
| Ameren  | $80.00/kW-yr          | MISO |

ComEd is in PJM (same RTO as MD utilities), so this is a direct precedent. E3 uses **a single
snapshot rate** labeled "2024" — they do not discuss PJM's mid-year rate update schedule or blend
the Jan and Jun rates. Their study was published January 2025, so the "2024" rate is almost
certainly the January 2024 NITS rate that was in effect for the 2024 measurement year.

E3 explicitly notes (pp. 28–29):

> _"It is important to note that the network transmission service rates are driven by gross annual
> transmission expenses incurred by the RTOs, plus a rate of return... each RTO independently noted
> that the NTS rate is not an appropriate indicator of their capacity-driven marginal costs."_

And:

> _"After extensive discussions with both utilities and RTOs, these organizations indicated to E3
> that they were not able to provide a more accurate, specific marginal cost for transmission
> capacity at this time."_

This is the authoritative confirmation that NITS/NTS is the best available estimate despite its
embedded-cost limitations.

#### Our adaptation: day-weighted blended rate

E3 uses a single rate snapshot because they are analyzing a given study year with one published
rate. For an ongoing BAT platform that runs against specific calendar years, PJM's mid-year rate
structure must be handled explicitly.

PJM publishes NITS rates **twice per year** (Jan 1 and Jun 1). Per PJM Manual 27 §5.2.2, NITS is
billed **daily** using whichever rate is in effect that day:

    Daily charge for customer i = PLCᵢ × (Annual Zonal NITS Rate / days_in_year)

A customer with constant 1 kW load pays over the full calendar year:

    Annual cost = (151 × jan_rate + 214 × jun_rate) / 365   # non-leap
    Annual cost = (152 × jan_rate + 214 × jun_rate) / 366   # leap

This is the **blended rate** — not an approximation, but the exact arithmetic of PJM's daily
billing integrated over the year. It is the correct annual scalar to use for the BAT.

**The blended rate and full-year PCAF answer separate, composable questions:**

- The **blended rate** (Step 1) answers: _How much annual bulk TX cost does a constant 1 kW
  load incur?_ Answer: B = `(151 × jan_rate + 214 × jun_rate) / 365`.
- The **full-year PCAF** (Steps 2–3) answers: _Which specific hours drive that cost?_
  Answer: the top-150 load hours across the full year.

They compose cleanly:

    cost_h = (load_h / Σ_{top-150} load) × B    for the 150 peak hours
    cost_h = 0                                    for all other hours
    Σ cost_h across all 8760 hours = B            ← full annual cost recovered ✓

**Rationale for blending rather than two separate rate-period passes:**

A two-pass approach (allocate Jan-rate costs to top hours in Jan–May, Jun-rate costs to top
hours in Jun–Dec) was considered but rejected. For summer-peaking utilities like BGE/DPL/PEPCO,
the top-150 full-year hours are almost entirely in Jun–Sep, so the Jan-rate portion would have
very few hours to land on — most of the annual cost would be allocated via the Jun rate anyway.
For APS, the Jan and Jun rates are identical (2021–2025), so splitting is a no-op. The blended
rate is simpler and directly represents PJM's actual annual billing arithmetic.

### Step 2 — No seasonal filter (full-year, following E3)

Use the **full year** of hourly load data — no seasonal restriction. This directly follows E3's
ICC-VDER methodology, which selects the top-150 hours from the entire year.

**Why no seasonal filter:**

An earlier draft applied a seasonal filter (summer for BGE/DPL/PEPCO, winter for APS) to align
with PJM's NSPL mechanism. This was removed because:

1. **Consistency with our K choice:** We already deliberately depart from PJM's strict NSPL
   billing mechanism by using K = 150 instead of K = 1 (see rationale below). Applying a
   seasonal filter was an attempt to partially follow PJM's billing logic, but if we're not
   following PJM's K = 1 approach, there is no coherent reason to carry the seasonal restriction
   either. We either follow PJM's billing (K = 1, single NSPL hour) or follow E3's rate design
   methodology (K = 150, full year). Mixing the two is internally inconsistent.
2. **E3 uses full year:** E3's Appendix C explicitly states that allocation factors are computed
   from the top-150 load hours of the full year. They note that in early years these hours are
   concentrated in summer, and that as electrification progresses they shift toward winter — but
   they make no seasonal restriction. This is the methodology we are adopting.
3. **APS winter consideration:** Without a seasonal filter, APS (a winter-peaking zone) will
   naturally have some of its top-150 hours fall in winter, since its peak is in January. The
   full-year approach lets the load data speak for itself rather than imposing a prior.

**Note on NSPL zonal peaks (informational — retained for reference, not used for filtering):**

PJM's 2025 NSPL zonal peak data shows:

| Zone  | 2025 NSPL peak date | Season |
| ----- | ------------------- | ------ |
| BGE   | 7/16/2024           | Summer |
| DPL   | 7/16/2024           | Summer |
| PEPCO | 7/16/2024           | Summer |
| APS   | 1/22/2024           | Winter |

This is informative context for understanding cost-causation under PJM billing, but we do not
use it to restrict which hours are eligible for PCAF allocation.

### Step 3 — Hourly allocation: PCAF (Peak Capacity Allocation Factor) method, K = 150

Apply the **PCAF load-share** method (following E3's ICC-VDER Appendix C) to allocate the
blended annual $/kW-yr across the top-K hours of the full year:

1. Load the PJM zone hourly demand for the relevant utility and year.
   Source: `s3://data.sb/pjm/hourly_demand/utilities/utility={name}/year={year}/data.parquet`
2. Rank all 8760 hours by `load_mw` descending.
3. Select the **top K = 150 hours** by load.
4. Compute each hour's **load-share allocation factor**:

       AF_h = load_h / Σ(load in top-K hours)

   where `load_h` is the zonal demand in hour `h`, and the sum is over all K = 150 peak hours.
   The sum of all AFs equals exactly 1.0.

5. Compute each hour's allocated cost:

       cost_h = AF_h × annual_cost_kw_yr

   This distributes the full annual $/kW-yr across exactly 150 hours.
   All other 8610 hours (8760 − 150) receive **zero** marginal cost.

6. **Validate:** sum of `cost_h` across all 8760 hours = blended annual $/kW-yr (tolerance < 0.01).

**Key property: exactly 150 non-zero hours.** All other hours have zero bulk TX marginal cost.
This is by construction — only the top-150 full-year hours receive allocation.

### Why K = 150? E3's approach vs. PJM's billing mechanism

This is a deliberate design choice. There are two defensible approaches:

**PJM's actual billing mechanism — K = 1 (the NSPL mechanism):**

PJM determines each customer's annual NITS bill using exactly **one peak hour** — the single
highest zonal load hour in the measurement period (Nov 1–Oct 31). That hour sets the customer's
Peak Load Contribution (PLC), which is then applied to daily billing for the entire year.

Under strict K = 1 logic, the hourly MC signal would be:

- cost at the single NSPL peak hour = blended\_rate (all annual cost concentrated there)
- cost at all other 8,759 hours = 0

This is mechanically accurate to PJM's billing. However, for rate design and BAT analysis it has
serious problems:

- The NSPL peak hour is **not known in advance** — it is only revealed after the measurement
  period ends. You cannot design a rate structure around one unknown hour per year.
- With K = 1, the BAT assigns all bulk TX cost responsibility based on what a customer happened
  to be doing at a single extreme hour. This is over-sensitive and not useful for differentiating
  HP vs. non-HP customers in a rate design context.
- A K = 1 signal cannot inform TOU rate design — no customer can reliably manage their load at
  one anonymous peak hour per year.

**E3's DER avoided-cost approach — K = 150 (our choice):**

E3's ICC-VDER report (Appendix C) explicitly chose K = 150 for transmission and distribution
capacity allocation. Verbatim from Appendix C:

> _"Allocation factors for transmission and distribution capacity were then assigned to the top
> 150 load hours based on the share of load in each of these hours divided by the total load
> across these 150 top load hours."_

E3 uses Cambium PJM West forecast load (not actual metered zone load) and applies this to the
**full year** with **no seasonal filter**. The resulting signal shows which 150 hours out of 8760
are highest-value for transmission capacity deferral — which is the relevant concept for DER
compensation and rate design.

| K       | Source                          | Rationale                                                          | Problem                                                           |
| ------- | ------------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------- |
| 1       | PJM NSPL billing (strict)       | Matches PJM's actual billing mechanism exactly                     | Unknown in advance; not actionable for rate design; oversensitive |
| 100     | NY/RI platform convention       | Consistent with sub-TX and capacity                                | No state-commissioned precedent for PJM territory                 |
| **150** | **E3 Illinois ICC-VDER (2025)** | **State-commissioned study for ComEd (PJM), transmission + dist.** | Slight departure from PJM billing; deliberate design choice       |

**We use K = 150 (E3's approach, not PJM's K = 1) because:**

1. **Rate design purpose:** The goal is a BAT signal that can inform TOU or demand-charge rate
   design. K = 150 creates an actionable signal covering a peak season window rather than a
   single unknowable hour. K = 1 is a billing rule, not a rate design tool.
2. **E3 precedent:** E3's ICC-VDER is the most recent official state-commissioned avoided cost
   study for a PJM-territory utility (ComEd). It explicitly chose K = 150 for transmission and
   distribution costs. This provides regulatory defensibility for the same methodology in MD.
3. **Robustness:** K = 1 is over-sensitive to a single extreme weather event. K = 150 averages
   cost-causation over a seasonally consistent set of peak hours, producing a stable signal
   across years.
4. **Consistency with E3's generation capacity methodology:** E3 uses K = 100 for generation
   capacity and K = 150 for T&D. Using K = 150 for bulk TX is internally consistent with the
   framework.

This is a **deliberate departure from PJM's billing mechanism**, clearly documented as such. If a
reviewer prefers strict K = 1 fidelity to PJM billing, the implementation can be changed; the
choice is explicit, not inadvertent.

### Why PCAF load-share (not exceedance weighting)?

Our existing platform uses PoP exceedance weighting (`allocate_annual_exceedance_to_hours()` in
`supply_utils.py`), which allocates proportionally to each hour's exceedance above a threshold.
For MD bulk TX, we instead use **PCAF load-share** weighting because:

1. **E3 precedent:** E3's Illinois methodology (the official source) uses raw load-share
   weighting, not exceedance. Quote from Appendix C: _"based on the share of load in each
   of these hours divided by the total load across these 150 top load hours."_
2. **Simplicity and transparency:** PCAF is easier to explain — each peak hour's share is
   simply its proportion of total peak load. No threshold parameter to calibrate.
3. **Practical difference is small:** For K = 150, exceedance vs. load-share produces nearly
   identical results (peak hours are close in magnitude, so the threshold is near the floor
   of the top-150 set). The choice is about defensibility, not material outcome.
4. **Matching official methodology:** When an official state-commissioned study exists for the
   same RTO (PJM) and cost component (transmission capacity), we follow it.

**PCAF formula (from E3 Appendix C, Figure 42):**

    PCAF_h = L_h / Σ_{k ∈ top-K} L_k     for h in top-K hours
    PCAF_h = 0                              for all other hours

    cost_h = PCAF_h × annual_$/kW-yr

Where `L_h` is the hourly load (MW) in hour `h`. The resulting hourly costs sum to the annual rate.

**Source:** [E3 ICC-VDER Report, Illinois, Jan 2025](https://www.ethree.com/wp-content/uploads/2025/01/ICC-VDER-Report-FINAL-2025-1-17.pdf),
Appendix C, pp. 98-99, Figure 42.

---

## Comparison with NY and RI approaches

| Feature                    | NY (NYISO)                                  | RI (ISO-NE)                        | **MD (PJM)**                                                            |
| -------------------------- | ------------------------------------------- | ---------------------------------- | ----------------------------------------------------------------------- |
| Best available MC source   | OATT ATRR proxy (each TO)                   | AESC avoided PTF cost (LRMC)       | **OATT NITS rate proxy (each zone)**                                    |
| Institutional backing      | None for bulk TX MC                         | AESC used by RI PUC                | E3 Illinois report (state-commissioned)                                 |
| Data quality               | Embedded average only                       | Year-by-year LRMC projections      | Embedded average; updated annually                                      |
| Peak allocation            | NYISO zonal load, SCR hours (summer+winter) | NE system load, 12-CP informed     | **Zonal load, top-150 full year (PCAF)**                                |
| Season                     | Summer + winter (SCR)                       | Primarily summer (12-CP)           | **Full year (following E3); no seasonal filter**                        |
| RTO pushback on rate       | FERC/NYISO have no better estimate          | AESC is preferred; RNS is fallback | **PJM/MISO said NTS is not ideal LRMC; no better alternative provided** |
| Approximate $/kW-yr (2025) | ~$36–120 (by TO)                            | ~$69 (AESC PTF)                    | **~$25–65 (by zone)**                                                   |

---

## Data sources and where to get them

| Dataset                      | URL / Location                                                                                                           | Use                                                                          |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| PJM NITS rates (current)     | https://www.pjm.com/markets-and-operations/billing-settlements-and-credit                                                | Annual $/kW-yr value per zone                                                |
| PJM NITS archive PDFs        | Jan/Jun each year: `network-integration-trans-service-{jan,june}-{year}.pdf` at above URL                                | Historical year-by-year blended rates                                        |
| PJM NSPL (zonal peak dates)  | https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-service-peak-loads-{year}.pdf                         | Informational context (which season each zone peaks; not used for filtering) |
| PJM OATT Formula Rates       | https://www.pjm.com/markets-and-operations/billing-settlements-and-credit/formula-rates                                  | ATRR inputs, true-up filings                                                 |
| NSPL methodology (per EDC)   | https://www.pjm.com/markets-and-operations/billing-settlements-and-credit/theo-plc-and-nspl                              | How each EDC computes NSPL (OATT Attachment M-2)                             |
| EIA/PJM zone hourly loads    | Already on S3: `data/eia/hourly_loads/pjm/` or `data/pjm/` (see existing pipelines)                                      | Hourly peak identification for PoP allocation                                |
| MD OPC rising TX cost report | https://opc.maryland.gov/Portals/0/Files/Publications/Rising%20Transmission%20Costs%202026-03-25%20CORRECTED%20FINAL.pdf | MD-specific policy context and scaling rationale                             |
| E3 Illinois ICC-VDER report  | https://www.ethree.com/wp-content/uploads/2025/01/ICC-VDER-Report-FINAL-2025-1-17.pdf                                    | Precedent for NITS-as-upper-bound approach                                   |
| PA PUC avoided TX study      | https://www.puc.pa.gov/pcdocs/1855615.pdf                                                                                | Reference for PJM utility transmission cost studies                          |

---

## Transmission zone coverage for all MD utilities

Four PJM transmission zones cover every electric utility in Maryland (IOUs,
cooperatives, and municipals). Co-ops and municipals do not own transmission;
they pay through their host zone's NITS rate. The utility-to-zone mapping is
already defined in `utils/data_prep/marginal_costs/supply_utils.py`
(`PJM_UTILITY_ZONES`) and `data/pjm/zone_mapping/generate_zone_mapping_csv.py`.
For bulk TX, the same four zones suffice:

| Zone  | IOUs           | Co-ops/Municipals in zone                        |
| ----- | -------------- | ------------------------------------------------ |
| BGE   | BGE            | (none)                                           |
| PEPCO | Pepco          | SMECO                                            |
| DPL   | Delmarva Power | Choptank, A&N Electric, Easton, Berlin           |
| APS   | Potomac Edison | Somerset REC, Hagerstown, Thurmont, Williamsport |

All 13 MD electric utilities are covered by these four zones. No additional
zones are needed.

---

## Calendar-year blending methodology

PJM publishes NITS rates effective January 1 and June 1 each year. PJM bills
NITS daily (Manual 27 §5.2.2):

    Daily NITS Charge = Daily PLC × (Annual Zonal NITS Rate / days_in_year)

For a calendar-year BAT run, the effective annual rate is the **day-weighted
average** of the two rates in effect:

    blended_rate = (days_jan × jan_rate + days_jun × jun_rate) / days_in_year

| Year type | Jan 1 – May 31 | Jun 1 – Dec 31 | Total |
| --------- | -------------- | -------------- | ----- |
| Non-leap  | 151 days       | 214 days       | 365   |
| Leap      | 152 days       | 214 days       | 366   |

The simpler 5/7 month approximation (`(5 × jan + 7 × jun) / 12`) differs by
< 0.5% and is acceptable when exact day counts are impractical. The reference
CSV stores per-period rates; blending is done at consumption time.

---

## Reference data storage

NITS rates are stored as an in-repo CSV following the same pattern as the PJM
RPM capacity prices (`data/pjm/capacity/rpm/`):

| Path                                           | Purpose                                                 |
| ---------------------------------------------- | ------------------------------------------------------- |
| `data/pjm/bulk_tx/nits/nits_rates.csv`         | Generated CSV: one row per (year, effective_date, zone) |
| `data/pjm/bulk_tx/nits/sources/nits_{year}.md` | Per-year markdown intermediates (citation + tables)     |
| `data/pjm/bulk_tx/nits/validate_nits_rates.py` | Schema and value validation script                      |
| `data/pjm/bulk_tx/nits/Justfile`               | `validate` recipe                                       |

CSV schema:

| Column            | Type   | Description                                       |
| ----------------- | ------ | ------------------------------------------------- |
| `year`            | Int    | Calendar year                                     |
| `effective_date`  | Date   | Rate effective date (YYYY-MM-DD)                  |
| `zone`            | String | PJM transmission zone (APS, BGE, DPL, PEPCO)      |
| `nits_rate_mw_yr` | Float  | NITS rate in $/MW-year (as published by PJM)      |
| `nits_rate_kw_yr` | Float  | Same rate in $/kW-year (= nits_rate_mw_yr / 1000) |
| `source_url`      | String | URL of the PJM PDF this row was transcribed from  |

This approach (RI-style constant, but stored in a versionable CSV instead of
hardcoded) was chosen because:

- RI uses a single hardcoded AESC constant (`$69/kW-yr`) — too simple for 4
  zones × 2 periods × multiple years
- NY uses a derived constraint-group CSV on S3 — too complex; MD's source is
  a single published scalar per zone per period
- PJM RPM capacity prices CSV (`data/pjm/capacity/rpm/rpm_capacity_prices.csv`)
  is the closest analogue: same data provider, same in-repo + validate pattern

---

## Plan of next actions

### 1 — NITS reference data (DONE for 2021-2025)

- [x] 2021–2025 rates extracted for all 4 zones (Jan + Jun) from PJM PDFs and CAPS Handbook
- [x] CSV, source markdown (5 years), and validator created at `data/pjm/bulk_tx/nits/`
- [x] Validation passing for 40 rows (4 zones × 2 periods × 5 years)
- [ ] Historical rates (2018–2020): retrieve from alternative sources when needed for pre-2021 BAT runs.
      PJM PDFs for these years are no longer accessible (404); ETCC historical table + inference
      can be used if needed. Add one `sources/nits_{year}.md` per year; re-run convert.

### 2 — Verify zone-level EIA/PJM hourly load data availability

- [ ] Confirm that PJM zone-level hourly loads (BGE, DPL, PEPCO, APS) are already on S3 or
      available via the existing `data/eia/hourly_loads/pjm/` or `data/pjm/` pipeline.
      These are needed for Step 3 (hourly PoP allocation).
- [ ] If not available, extend the existing EIA zone load fetch to include PJM zones for MD.

### 3 — Implement `bulk_tx_pjm.py`

Create `utils/data_prep/marginal_costs/bulk_tx_pjm.py` following the same structure as
`bulk_tx_isone.py` and `bulk_tx_nyiso.py`:

- Input: zone name (`bge`, `dpl`, `pepco`, `potomac-edison`), year, NITS rate ($/kW-yr)
- No seasonal filter (full-year top-K, following E3)
- Hourly allocation: PCAF load-share on zone load, top-K hours
- Output: 8760-row DataFrame with `timestamp`, `bulk_tx_cost_enduse`
- Validation: sum of allocations = annual $/kW-yr

Extend `generate_bulk_tx_mc.py` with `--iso pjm` path.

### 4 — Create Justfile recipes for MD bulk TX MC

Add recipes to `rate_design/hp_rates/md/Justfile` analogous to the existing supply energy MC
recipes. Each recipe should take `utility` and `year` as arguments and invoke
`generate_bulk_tx_mc.py --iso pjm`.

### 5 — Sensitivity analysis

After implementation, run the BAT with:

- Primary: 2025 NITS blended rate, K = 150 full-year peak hours
- Sensitivity A: K = 5 (strict NSPL) or K = 100 (NY/RI convention)
- Sensitivity B: Use the MD OPC report's forward-looking cost estimates (if available) as an
  alternative to the embedded NITS rate

### 6 — Document in context/README.md

After creating the implementation files, update `context/README.md` to add this doc to the
methods/marginal_costs index.

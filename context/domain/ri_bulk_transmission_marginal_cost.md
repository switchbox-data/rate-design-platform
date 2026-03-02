# RI bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in Rhode Island. Unlike NY, where no established bulk TX marginal cost methodology exists, New England has an institutional framework for avoided transmission costs: the Avoided Energy Supply Components (AESC) study by Synapse Energy Economics, and the peak-driven RNS rate allocation. This document covers the available data, methodology options, and a recommended approach.

---

## The gap (narrower than NY)

Our current marginal cost stack for RI:

| Cost component                  | Source                                   | Status                                  |
| ------------------------------- | ---------------------------------------- | --------------------------------------- |
| Energy                          | ISO-NE LMP / Cambium LRMER               | Have it                                 |
| Generation capacity             | FCM / Cambium                            | Have it                                 |
| Sub-transmission & distribution | RIE/NEP MCOS or rate case data ($/kW-yr) | Have it                                 |
| **Bulk transmission**           | **RNS rate + AESC values**               | **Have data; need methodology for BAT** |

The situation in RI is better than NY. The RNS rate is an explicit, visible charge on the retail bill, allocated via 12-CP (coincident peak). The AESC study provides avoided transmission cost values used for benefit-cost screening of efficiency programs. The question is not "does data exist?" but rather "which data source best represents the long-run marginal cost of bulk transmission for the BAT?"

---

## What data exists

### 1. The RNS rate (embedded cost, peak-allocated)

The Regional Network Service rate recovers the full embedded cost of Pool Transmission Facilities across all of New England. It is allocated to LSEs based on their 12-CP (twelve coincident peak) contribution. See `ri_bulk_transmission_cost_recovery.md` for the full framework.

Current and forecast RNS rates (from PTO-AC 2026 filing):

| Year | RNS rate ($/kW-yr) | $/kWh (at 54.5% LF) | Incremental additions ($M) | Forecasted RR ($M) |
| ---- | ------------------ | ------------------- | -------------------------- | ------------------ |
| 2026 | $184               | $0.029              | $1,125                     | $166               |
| 2027 | $186               | $0.029              | $1,101                     | $168               |
| 2028 | $197               | $0.031              | $1,337                     | $206               |
| 2029 | $210               | $0.033              | $1,491                     | $233               |
| 2030 | $220               | $0.034              | $1,151                     | $175               |

The 12-CP average monthly regional network load is 18,133 MW (2024 actuals). The rate is rising because PTOs are investing ~$1.6B/year in regional projects (asset condition upgrades, RSP projects, and other regional investments).

**Why RNS is useful but imperfect as a marginal cost:** The RNS rate is an embedded average cost — it blends depreciated old assets with new investment. But because it is allocated on 12-CP, it is inherently peak-responsive: a customer who reduces demand at regional coincident peaks pays less RNS. This makes the allocation mechanism marginal-cost-like, even though the rate level reflects average embedded cost. The rate is also rising because incremental investment is happening, so the embedded rate is converging toward the marginal cost of new PTF.

### 2. The AESC study (Synapse Energy Economics)

The **Avoided Energy Supply Components (AESC)** study is the New England standard for avoided cost inputs used in benefit-cost screening of energy efficiency and DER programs. Prepared by Synapse Energy Economics and a consortium of subcontractors (Sustainable Energy Advantage, North Side Energy, Resource Insight, Les Deman Consulting), it is updated roughly every three years. The current version is **AESC 2024** (released February 2024, amended May 2024).

**What AESC provides:**

- Year-by-year avoided cost projections from 2024 through 2050, extrapolated to 2060
- Components: avoided energy, avoided capacity, avoided T&D (PTF only), avoided natural gas, avoided fuel oil, DRIPE (price effects), non-embedded environmental costs, reliability value
- State-specific values and region-wide values
- User Interface workbooks with detailed data tables

**How AESC treats transmission:**

- The AESC summary tables include **avoided T&D costs related to pooled transmission facilities (PTF) only**. Non-PTF (local) T&D avoided costs are excluded from summary tables and treated separately.
- The avoided PTF cost represents the value of reducing peak demand on the regional transmission system. It captures the extent to which efficiency or DERs can defer or avoid new PTF investment.
- The AESC avoided transmission value is conceptually the LRMC of bulk transmission: the cost that would be incurred (or avoided) by a marginal change in peak load on the regional PTF system.

**Who uses AESC:**

- All New England states use AESC values for energy efficiency program benefit-cost screening
- Rhode Island: RI Test (PUC Docket 4600 framework) uses AESC as the primary avoided cost input. The "Electric Transmission Capacity Benefits" component of the RI Test draws from AESC.
- Massachusetts: Used in DPU energy efficiency proceedings and the MA TRC/PAC tests.
- The AESC 2024 Study Group includes all major electric and gas utilities in New England, efficiency program administrators, energy offices, regulators, and advocates.

**AESC 2024 report and materials:**

- Full report: https://www.synapse-energy.com/sites/default/files/AESC%202024.pdf
- May 2024 re-release: https://www.synapse-energy.com/sites/default/files/inline-images/AESC%202024%20May%202024.pdf
- User Interfaces, appendices, slide deck: https://www.synapse-energy.com/aesc-2024-materials
- Appendix C, D, and K (Excel workbooks with detailed data)

### 3. ISO-NE 2050 Transmission Study (forward-looking system needs)

Released February 2024 by ISO-NE. This is the closest thing in the Northeast to a forward-looking assessment of bulk transmission investment needs. Key findings relevant to marginal cost:

- **Peak load is the primary driver of transmission cost.** The study found that costs to support a 57 GW winter peak were **~$10 billion more** than costs for a 10%-reduced 51 GW peak. That $10B cost difference over 6 GW of peak reduction implies a marginal cost on the order of ~$1,667/kW (one-time capital, not annualized).
- **Four high-likelihood constraints**: North-South interface, Boston import, Northwestern Vermont import, Southwest Connecticut import.
- **Rebuilding existing lines** with larger conductors (rather than new ROWs) is a cost-effective approach in densely populated southern New England.

The study does not produce a single $/kW-year LRMC, but provides the building blocks: scenario-level capital cost estimates that can be annualized and divided by MW of capability.

### 4. LNS rate (NEP Schedule 21 — local, not bulk)

The Local Network Service rate under Schedule 21-NEP covers NEP's non-PTF facilities serving RI. This is charged separately from RNS. The LNS rate is local transmission and should be treated alongside sub-transmission/distribution MCs, not as "bulk" transmission. We likely already capture this in the delivery-side MC stack or need to add it there.

---

## Options for constructing bulk TX marginal cost for RI

### Option A: Use the AESC avoided PTF cost directly

**Approach:** Extract the avoided PTF transmission cost from the AESC 2024 User Interface workbook for Rhode Island. Use that value ($/kW-year, by year) as the bulk transmission marginal cost input to the BAT.

**Pros:**

- This is the authoritative New England avoided cost value. All six states use it for efficiency and DER program screening.
- Produced by Synapse, peer-reviewed by the Study Group (utilities, regulators, advocates).
- Already a marginal (avoided) cost, not an embedded average. The AESC methodology is designed to capture the incremental cost of new PTF investment that peak demand drives.
- Year-by-year projections through 2050 provide a time series for the BAT.
- State-specific and region-specific variants available.
- Institutionally accepted: using a value that RI PUC already uses for program screening is defensible in a regulatory context.

**Cons:**

- AESC is updated every 3 years; values may lag real-world changes.
- The methodology details are in a lengthy PDF; extracting the specific numbers requires the User Interface workbooks.
- AESC values are optimized for efficiency program screening, not rate design. The avoided cost perspective (what a kW of peak reduction saves) is closely related to but not identical to the marginal cost perspective (what the next kW of peak costs).

### Option B: Use the RNS rate as an upper-bound proxy

**Approach:** Use the PTO-AC's published RNS rate ($/kW-year) as the bulk TX marginal cost. Currently $184/kW-year, rising to ~$220/kW-year by 2030.

**Pros:**

- Publicly available, updated annually, simple.
- The RNS rate is rising because incremental PTF investment is increasing. The embedded rate is converging toward the marginal cost of new PTF.
- Parallel to Option 1 for NY (using the OATT rate as a proxy).

**Cons:**

- Embedded average cost, not marginal. Includes fully depreciated old assets alongside new investment.
- Likely overstates LRMC for now (old assets drag down the average marginal cost); may understate it later if new projects are very expensive.
- Less refined than AESC, which attempts to isolate the incremental component.

### Option C: Derive marginal cost from ISO-NE 2050 study capital cost scenarios

**Approach:** Take the total capital cost difference between the high-peak and low-peak scenarios in the ISO-NE 2050 study (~$10B for 6 GW of peak reduction). Annualize using a carrying charge factor (e.g., 12-15% for return, depreciation, taxes, O&M). Divide by GW to get $/kW-year.

**Rough calculation:**

- $10B ÷ 6,000 MW = ~$1,667/kW one-time capital
- At 12% carrying charge: ~$200/kW-year
- At 15% carrying charge: ~$250/kW-year

**Pros:**

- Forward-looking, scenario-based, ISO-endorsed study.
- Captures the physical reality: what new transmission actually costs.

**Cons:**

- Very rough: one aggregate capital figure for the entire region, not utility- or state-specific.
- The $10B includes all six states' needs; RI's share would be smaller.
- Sensitive to carrying charge assumptions.
- Intended as a planning study, not a marginal cost calculator.

### Option D: Blend AESC + RNS with sensitivity analysis

**Approach:** Use the AESC avoided PTF cost as the primary value, with the RNS rate as an upper-bound sensitivity. This gives a range rather than a point estimate.

**Pros:**

- Captures the best available marginal cost estimate (AESC) while acknowledging uncertainty.
- The range is informative: if BAT results are sensitive to the choice, that's important to know.

**Cons:**

- More complex to implement and report.

---

## Recommended approach

**Use Option A (AESC avoided PTF cost) as the primary value, with Option B (RNS rate) as an upper-bound sensitivity.**

The AESC is the institutional standard in New England for avoided transmission costs. It is already used by RI PUC for efficiency program screening. Using it for the BAT is consistent and defensible. The RNS rate provides a sanity check and upper bound.

**Implementation:**

1. **Obtain AESC 2024 avoided PTF cost for RI.** Download the User Interface workbook from the AESC 2024 Materials page (https://www.synapse-energy.com/aesc-2024-materials). Extract the "Avoided T&D - PTF" component, in $/kW-year, by year (2024-2050).
2. **Also obtain the RNS rate forecast.** From the PTO-AC annual filing (presented at NEPOOL RC/TC meetings, published on ISO-NE website). Current: $184/kW-year (2026), rising to $220/kW-year (2030).
3. **Add a `bulk_transmission` column to `ri_marginal_costs_2026_2035.csv`** with the AESC avoided PTF cost, utility by utility (for RI there is only one utility, RIE, but the framework should be generalizable).
4. **Allocate the annual $/kW-year to hourly $/kWh** using the PoP method on ISO-NE zonal load (top N peak hours). Because the RNS rate is allocated on 12-CP (monthly coincident peaks), the PoP allocation for bulk TX should use ISO-NE regional or RI-zone load, with N reflecting the monthly peak structure.
   - **Peak allocation consideration**: The RNS rate uses 12-CP (one peak per month, 12 peaks per year). For PoP allocation, this suggests using a wider peak window than for generation capacity (which uses a single annual peak or top ~100 hours). A reasonable approach: allocate to the top ~250-300 peak hours (roughly the top 20-25 hours per month across the 12 months), weighted by how close each hour is to the monthly peak.
   - Alternatively, use the same top-100-hours approach as for upstream sub-TX, for simplicity and consistency. The difference is unlikely to be material for the BAT.
5. **Run the BAT with AESC values (primary) and RNS rate (sensitivity)** to quantify the impact.

---

## Hourly allocation: peak definition for bulk TX in ISO-NE

ISO-NE is currently **summer-peaking** for bulk transmission purposes. The 12-CP peaks that drive RNS allocation are dominated by summer months (July-August). However, ISO-NE is transitioning toward winter peaking due to heating electrification, and the 2050 Transmission Study explicitly models winter peak constraints.

For the BAT:

- **Load shape**: ISO-NE RI zone load (or aggregate New England load, depending on whether we want state-specific or regional allocation).
- **Peak hours**: Top N hours of the load curve. N = 100-300 depending on how broadly we define "peak-driven."
- **Seasonality**: Currently summer-weighted. As electrification proceeds, winter peaks will increasingly drive bulk TX investment, and the allocation should reflect this. For a multi-year BAT run, using the actual zonal load shape will automatically capture the evolving seasonal pattern.

---

## Comparison with NY approach

| Feature                  | NY                                  | RI                                          |
| ------------------------ | ----------------------------------- | ------------------------------------------- |
| Best available MC source | OATT ATRR (embedded cost proxy)     | AESC avoided PTF cost (marginal cost)       |
| Institutional backing    | None for bulk TX MC                 | AESC is the regional standard, used by PUC  |
| Data quality             | OATT embedded rate only             | AESC provides year-by-year LRMC projections |
| Upper bound              | OATT ATRR / peak demand             | RNS rate                                    |
| Forward-looking study    | FERC Order 1920 (not yet available) | ISO-NE 2050 Transmission Study (2024)       |
| Allocation to peaks      | NYISO zonal load, top N hours       | ISO-NE zonal load, 12-CP-informed           |

RI is in a better position than NY for this component: the institutional framework (AESC + explicit RNS allocation) provides a cleaner marginal cost signal than anything available in NYISO territory.

---

## Key references

- **AESC 2024 Report**: https://www.synapse-energy.com/sites/default/files/AESC%202024.pdf
- **AESC 2024 Materials** (User Interfaces, appendices): https://www.synapse-energy.com/aesc-2024-materials
- **2026 RNS Rate Forecast** (PTO-AC presentation, July 2025): https://www.iso-ne.com/static-assets/documents/100025/pac_2026_rns_rate_forecast_rev1_clean.pdf
- **ISO-NE 2050 Transmission Study** (February 2024): https://www.iso-ne.com/static-assets/documents/100008/2024_02_14_pac_2050_transmission_study_final.pdf
- **RI PUC Docket 4600** (benefit-cost framework): https://ripuc.ri.gov/eventsactions/docket/4600page.html
- **RIE 2025 Annual EE Plan** (Docket 24-39-EE, showing AESC use): https://ripuc.ri.gov/Docket-24-39-EE

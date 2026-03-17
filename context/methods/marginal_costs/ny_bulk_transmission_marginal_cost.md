# NY bulk transmission marginal cost: how to construct it

How to create a bulk transmission marginal cost signal for the BAT in New York. The MCOS studies used for sub-transmission and distribution marginal costs explicitly exclude NYISO-level bulk transmission. This document covers the gap, the available data sources, the options for constructing an LRMC estimate, and a recommended approach.

---

## The gap

Our current marginal cost stack for NY:

| Cost component                  | Source                         | Status  |
| ------------------------------- | ------------------------------ | ------- |
| Energy                          | NYISO LBMP / Cambium LRMER     | Have it |
| Generation capacity             | CONE / Cambium                 | Have it |
| Sub-transmission & distribution | Utility MCOS studies ($/kW-yr) | Have it |
| **Bulk transmission**           | **???**                        | **Gap** |

The MCOS studies filed under PSC Docket 19-E-0283 by each utility (ConEd/Brattle, NYSEG-RG&E/CRA, National Grid, Central Hudson/NERA, O&R/NERA, PSEG-LI) cover local sub-transmission and distribution. The Charles River Associates study for NYSEG/RG&E explicitly clarifies that "upstream" means local sub-transmission (area substations, sub-transmission feeders), not bulk NYISO transmission. The VDER Value Stack's DRV (Demand Reduction Value) is also derived from these MCOS studies and similarly excludes bulk transmission.

Cambium provides energy and generation capacity marginal costs but does not cover transmission. The Cambium 2024 documentation confirms this.

Nobody in New York currently publishes a $/kW-year long-run marginal cost of bulk transmission.

---

## What data exists

### NYISO OATT Attachment H (embedded cost, not LRMC)

Each TO publishes an Annual Transmission Revenue Requirement (ATRR) under OATT Attachment H. Dividing by billing units gives a $/MWh rate. See `ny_bulk_transmission_cost_recovery.md` for the full table. The problem: this recovers **embedded average cost**, not marginal cost. It includes return on decades-old depreciated plant and brand-new investment, blended together. The embedded rate understates LRMC when the system needs expensive new investment (the current situation under CLCPA) and overstates it when the system has surplus capacity.

### CLCPA transmission project costs (actual new-build costs)

CLCPA Phase 1 and Phase 2 projects represent the actual cost of the next increment of bulk transmission. These are the closest thing to a revealed LRMC:

- **Phase 1** projects (local upgrades, PSC-authorized): Costs are in PSC filings. Utility-specific; relatively smaller scale.
- **Phase 2** projects (major new lines, FERC-jurisdictional): NYPA's Propel NY, approved by FERC July 2024. Costs flow through NYISO OATT Attachment H. Other Phase 2 projects are in planning.

If we could obtain the $/MW cost of CLCPA Phase 2 projects (total project cost ÷ MW of transfer capability added), that would be a genuine forward-looking LRMC of bulk transmission in NY. These costs are likely higher than the current embedded average.

### NYISO Reliability Needs Assessment and Comprehensive Reliability Plan

The 2024 RNA identified a transmission security deficiency in NYC starting 2033. The 2025-2034 CRP is a 10-year reliability outlook. These identify where the system is stressed but do not produce $/kW-year marginal cost estimates.

### FERC Order 1920 (future — compliance filing due April 2026)

FERC Order 1920 requires NYISO to develop a Long-Term Regional Transmission Plan using at least a 20-year horizon with scenario-based cost-benefit analysis. The compliance filing is due April 30, 2026. When complete, this could eventually yield proper LRMC estimates. But the tariff drafting is still in progress and usable data is 1-2+ years away.

### VDER DRV — does NOT include bulk transmission

The VDER Value Stack's DRV and LSRV are based on MCOS studies covering local sub-transmission and distribution only. ConEd's LSRV locations are area stations and sub-transmission (W. 42nd St, Parkchester, etc.) — all below the NYISO bulk system. Bulk transmission is not represented in VDER.

---

## Options for constructing bulk TX marginal cost

### Option 1: Use OATT ATRR as an upper-bound proxy

**Approach:** Take each TO's Annual Transmission Revenue Requirement from Attachment H Table 1, divide by the TO's coincident peak demand (not energy), to get a $/kW-year figure. Use this as the bulk transmission marginal cost input to the PoP allocation.

**Pros:**

- Data is publicly available and updates annually.
- This is exactly what E3 did for Illinois in the 2025 ICC-VDER report, using MISO/PJM Network Transmission Service rates as an upper bound ($80/kW-year for Ameren, $39.80/kW-year for ComEd).
- Simple, reproducible, defensible as a conservative upper bound.

**Cons:**

- Embedded average cost, not LRMC. Includes depreciated old plant alongside new investment.
- Both MISO and PJM told E3 that the NTS rate is "not an appropriate indicator of capacity-driven marginal costs."
- Likely overstates the marginal cost if there is surplus transmission capacity; likely understates it if the system is capacity-constrained (the CLCPA situation).

**Values (approximate, from Attachment H):**

| TO             | ATRR ($M) | Peak demand (MW) | Approx. $/kW-yr |
| -------------- | --------- | ---------------- | --------------- |
| ConEd          | ~1,200    | ~10,000          | ~120            |
| National Grid  | ~220      | ~5,500           | ~40             |
| NYSEG          | ~170      | ~3,200           | ~53             |
| RG&E           | ~65       | ~1,500           | ~43             |
| O&R            | ~40       | ~1,100           | ~36             |
| Central Hudson | ~55       | ~1,000           | ~55             |
| LIPA           | ~300      | ~5,200           | ~58             |

(Peak demand estimates are rough; actual values should come from NYISO Gold Book or Attachment H billing units converted.)

### Option 2: Use CLCPA Phase 2 project costs as the LRMC

**Approach:** Obtain the total cost and MW capability of approved CLCPA Phase 2 transmission projects (e.g., NYPA Propel NY). Convert to $/kW-year using a carrying charge rate (return, depreciation, taxes, O&M). This represents what the next increment of bulk transmission actually costs.

**Pros:**

- Genuine forward-looking marginal cost.
- These are real projects with approved costs, not theoretical.
- Most defensible from an economic theory standpoint.

**Cons:**

- Data may be hard to obtain in the right format (project cost ÷ MW of transfer capability).
- CLCPA projects serve policy goals (renewable interconnection, CLCPA compliance), not just load growth. The "marginal cost of serving one more MW of load" and "marginal cost of enabling CLCPA compliance" are different questions.
- Project-specific; may not represent a generalizable $/kW-year for all TOs.

### Option 3: Use a fraction of the OATT ATRR

**Approach:** Apply a discount factor (e.g., 30-50%) to the OATT ATRR to approximate the marginal share. The logic: much of the TRR is depreciated old plant with low remaining book value; the marginal cost is driven by new investment, which is a fraction of the total.

**Pros:**

- Simple adjustment to Option 1.
- Acknowledges the embedded/marginal distinction.

**Cons:**

- The discount factor is arbitrary. No principled way to choose 30% vs. 50% vs. 70%.
- Sensitive to assumptions about how much of the system is "marginal."

### Option 4: Wait for FERC Order 1920 outputs

**Approach:** Defer bulk TX marginal cost inclusion until NYISO's Order 1920 compliance process produces scenario-based long-term transmission plans with cost-benefit analysis.

**Pros:**

- Would produce authoritative, ISO-endorsed LRMC data.
- Avoids using a proxy that may be wrong.

**Cons:**

- Compliance filing not due until April 2026; usable LRMC data likely 2027+.
- Leaves a known gap in the marginal cost stack.

### Option 5: Status quo — acknowledge the gap, omit bulk TX MC

**Approach:** Document that bulk TX LRMC is not available, note the OATT embedded cost as context, and leave bulk TX out of the hourly marginal cost allocation. The delivery revenue requirement still includes bulk TX costs (confirmed), so they are part of the residual — just not allocated to hours via a marginal cost signal.

**Pros:**

- Honest about data limitations.
- No risk of using a wrong proxy.

**Cons:**

- Bulk TX costs are real and peak-driven; omitting them from the MC signal means the BAT underweights the cost causation of peak-driving customers.
- Every other MC component (energy, capacity, sub-TX, distribution) is allocated to hours; bulk TX is the only one left entirely in the residual.

---

## Recommended approach

**Start with Option 1 (OATT ATRR as upper-bound proxy) for each TO.** This is what the leading avoided-cost practitioners (E3, Synapse) do when no better marginal cost data exists. It is conservative (likely overstates MC for most TOs), transparent, and reproducible.

**Implementation:**

1. Obtain each TO's ATRR from Attachment H Table 1 (annual update, publicly filed at FERC).
2. Obtain each TO's coincident peak demand from the NYISO Gold Book or from Attachment H billing units.
3. Compute $/kW-year = ATRR / peak demand.
4. Add a `bulk_transmission` column to `ny_marginal_costs_2026_2035.csv` with these values, utility by utility.
5. In `generate_utility_tx_dx_mc.py`, allocate the annual $/kW-year to hourly $/kWh using the PoP method on NYISO zonal load (top N peak hours). The number of peak hours (N) should reflect that bulk transmission is driven by broader regional/zonal peaks, not local substation peaks — use the same N as for upstream sub-transmission (currently 100 hours) or a wider window.
6. The allocated hourly MC adds to `mc_total_per_kwh` alongside energy, capacity, sub-TX, and distribution MCs.

**Sensitivity:** Run the BAT with and without the bulk TX MC to quantify its impact on cross-subsidy results. If the OATT proxy produces unreasonable results (e.g., bulk TX MC dominates the total and distorts HP/non-HP comparisons), consider applying a discount factor (Option 3) or switching to CLCPA project costs (Option 2) if available.

**Long-term:** When FERC Order 1920 outputs become available (2027+), replace the OATT proxy with proper LRMC data from NYISO's long-term transmission plan.

---

## Hourly allocation: which peak to use?

Bulk NYISO transmission is driven by **zonal or statewide coincident peaks**, not local substation peaks. The PoP allocation should use:

- **Load shape**: NYISO zonal load (matching the TO's service territory), not individual customer or substation load.
- **Peak hours**: The top N hours of the NYISO zonal load curve. N = 100 is a reasonable starting point (consistent with sub-TX upstream allocation). Bulk TX may justify a narrower window (e.g., N = 50) since major transmission constraints bind only at extreme peaks, but this is a judgment call.
- **Seasonality**: NYISO is currently summer-peaking. Bulk TX peaks align with summer system peaks. As electrification grows and the system potentially shifts toward winter peaking, the allocation will need to reflect that.

The PoP weights for bulk TX should be computed from the same NYISO zonal load data used for energy and capacity, ensuring consistency across the MC stack.

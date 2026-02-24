# NY bulk transmission: how it works and how costs reach residential customers

How bulk high-voltage transmission is owned, regulated, priced, and recovered on residential electric bills in New York. Covers the NYISO OATT framework, the distinction between FERC and PSC jurisdiction, the mechanics of the Transmission Service Charge, per-utility evidence that transmission costs are embedded in the PSC-approved delivery revenue requirement, and how all of this differs from ISO-NE territory (Rhode Island, Massachusetts, etc.).

---

## The players: who owns bulk transmission in NY?

New York's bulk transmission system (roughly 115 kV and above) is operated by the New York Independent System Operator (NYISO) but owned by eight Transmission Owners (TOs):

| TO                                        | Abbreviated   | Owner type                            | Service territory (NYISO subzones)                                    |
| ----------------------------------------- | ------------- | ------------------------------------- | --------------------------------------------------------------------- |
| Consolidated Edison Company of New York   | ConEd         | Investor-owned (ConEd Inc.)           | NYC, Westchester (Dunwoodie, Hudson Valley, Millwood, NYC)            |
| Orange and Rockland Utilities             | O&R           | Investor-owned (ConEd Inc.)           | Lower Hudson Valley                                                   |
| Niagara Mohawk Power Corporation          | National Grid | Investor-owned (National Grid plc)    | Capital, Central, Genesee, Mohawk Valley, North, West                 |
| New York State Electric & Gas Corporation | NYSEG         | Investor-owned (Avangrid / Iberdrola) | Capital, Central, Hudson Valley, Millwood, Mohawk Valley, North, West |
| Rochester Gas and Electric Corporation    | RG&E          | Investor-owned (Avangrid / Iberdrola) | Genesee                                                               |
| Central Hudson Gas & Electric Corporation | CH            | Investor-owned (Fortis Inc.)          | Hudson Valley, Mohawk Valley                                          |
| Long Island Power Authority               | LIPA          | Governmental (public authority)       | Long Island                                                           |
| Power Authority of the State of New York  | NYPA          | Governmental (public authority)       | North (plus statewide 765/345 kV backbone)                            |

NYPA and LIPA are governmental entities. NYPA owns major backbone facilities (Moses–Adirondack 765 kV, Marcy–South 345 kV, etc.) but does not directly serve residential customers — its power and transmission services flow through other utilities or municipal/public customers. LIPA owns transmission on Long Island; PSEG Long Island operates the system under contract. The six investor-owned utilities (IOUs) each own both transmission and distribution plant in their service territories.

---

## What is the OATT and the Transmission Service Charge?

### The Open Access Transmission Tariff

FERC Order 888 (1996) required all transmission-owning utilities to offer non-discriminatory transmission service under an Open Access Transmission Tariff (OATT). NYISO administers a single OATT for all of New York, filed at FERC. The OATT governs wholesale transmission service: how generators, load-serving entities (LSEs), and marketers use the bulk transmission system, and how they pay for it.

The principal transmission service for native load is **Network Integration Transmission Service (NITS)**, analogous to what ISO-NE calls Regional Network Service (RNS). In the NYISO OATT, the charge for NITS is the **Transmission Service Charge (TSC)**.

### How the TSC is calculated

Each TO's TSC is computed from a formula in **OATT Attachment H**:

```text
Monthly Wholesale TSC = {(RR / 12) + (CCC / 12) - SR - ECR - CRR - WR - Reserved} / (BU / 12)
```

Where:

- **RR** = Annual Transmission Revenue Requirement. The total allowed revenue for the TO's transmission plant, covering: return on investment and associated income taxes, depreciation, property taxes, amortization of investment tax credits, O&M, administrative and general expenses, payroll taxes, billing adjustments — less revenue credits and transmission rents. Computed from FERC Uniform System of Accounts data (primarily FERC Form 1 plant accounts 350–359 for transmission plant in service).
- **CCC** = Annual Scheduling, System Control and Dispatch Costs. The TO's share of NYISO operating costs.
- **SR, ECR, CRR, WR** = Various revenue credits (scheduling revenues, excess congestion rent, congestion rent refunds, wheeling revenues).
- **BU** = Billing Units. The denominator: total MWh of network load served by the TO over the year.

The result is a **$/MWh wholesale rate** specific to each TO. This rate is what LSEs pay NYISO for using that TO's transmission facilities.

### Formula rates vs. stated rates

Originally, most TOs had "stated rates" — fixed $/MWh charges set in FERC rate cases and changed only through new filings. Starting in the late 2000s, TOs migrated to **formula rates** that update annually from auditable cost inputs:

- **National Grid (Niagara Mohawk)**: Filed to replace stated rates with formula rates in 2008. FERC accepted the Settlement TSC Formula Rate effective January 1, 2009, under a comprehensive Settlement Agreement (approved June 22, 2009). Annual updates compute new RR, CCC, and BU values from prior-year data.
- **NYPA**: Operates under formula rate protocols in OATT Section 14.2.3.2. Annual informational filings post by July 1 with open stakeholder meetings.
- **LIPA**: Non-FERC-jurisdictional (governmental utility), but voluntarily adopted a formula rate for its NYISO OATT TSC components. LIPA's Board of Trustees adopted an updated formula rate in December 2024 with methodology changes to weighted-average cost of capital and asset exclusions.
- **ConEd, O&R, NYSEG, RG&E, Central Hudson**: Each has its own rate mechanism under Attachment H. The specifics (formula rate vs. settlement-based stated rate) vary, but all produce an annual $/MWh TSC that feeds into the wholesale settlement.

### What the TSC recovers: embedded costs, not marginal costs

The TSC recovers the **full embedded cost** of a TO's transmission plant. "Embedded cost" means the total annual revenue requirement for all transmission assets currently in service — both decades-old lines that are largely depreciated and brand-new facilities still carrying full return on investment. The formula includes:

- **Return on rate base** (equity return + debt cost on net transmission plant) — the largest component
- **Depreciation** — recovery of the original investment over the asset's useful life
- **Property taxes and income taxes** — costs of owning plant
- **O&M** — operations and maintenance of transmission facilities
- **Administrative and general** — shared overhead allocated to transmission

This is not a marginal cost signal. It does not reflect the cost of the next MW of transmission capacity or the incremental cost of accommodating one more customer's load. It reflects the historical average cost of all transmission assets. The distinction matters: the long-run marginal cost (LRMC) of bulk transmission could be higher or lower than the embedded average, depending on whether the system is capacity-constrained and what new projects cost.

### Attachment H Table 1

OATT Attachment H, Table 1 publishes the key inputs and resulting TSC rate for each TO. Representative values (these update annually):

| TO             | Annual Revenue Requirement ($M) | CCC ($M) | Billing Units (GWh) | Rate ($/MWh) |
| -------------- | ------------------------------- | -------- | ------------------- | ------------ |
| Con Edison     | ~1,200                          | —        | ~50,000             | ~24          |
| Central Hudson | ~55                             | —        | ~4,500              | ~12          |
| NYSEG          | ~170                            | —        | ~14,000             | ~12          |
| RG&E           | ~65                             | —        | ~7,000              | ~9           |
| O&R            | ~40                             | —        | ~5,000              | ~8           |
| National Grid  | ~220                            | ~40      | ~26,000             | ~10          |
| LIPA           | ~300                            | —        | ~20,000             | ~15          |
| NYPA           | ~200                            | —        | —                   | varies       |

(Approximate; exact values change with each annual update. ConEd's high absolute RR reflects its enormous urban network: $4.7B in gross transmission plant as of 2024.)

---

## How TSC costs reach residential customers in New York

### The dual-jurisdiction structure

Each NY IOU is simultaneously:

1. A **FERC-jurisdictional transmission owner**, subject to FERC-approved rates for wholesale transmission service under the NYISO OATT.
2. A **PSC-regulated distribution utility**, subject to NY PSC-approved rates for retail delivery service.

The same company owns both the 345 kV transmission lines and the 13.8 kV distribution feeders. When the PSC sets retail "delivery rates" in a rate case, it sets a total **delivery revenue requirement** that covers the cost of both transmission and distribution. There is no separate "transmission charge" line item on a New York residential electric bill.

### The mechanism: embedding wholesale TX costs in retail delivery rates

1. **FERC sets the wholesale rate.** The TO's OATT Attachment H formula rate (or settlement-based rate) determines the $/MWh TSC that LSEs pay for using the TO's transmission facilities.

2. **The TO pays itself (in accounting terms).** For the TO's own native retail load, the transmission cost is internal — the TO is both the transmission owner charging the TSC and the distribution utility paying it. In the PSC rate case, this cost is captured as the utility's transmission revenue requirement: the same return, depreciation, taxes, and O&M that make up the FERC formula rate also appear in the utility's cost of service filing at the PSC.

3. **The PSC approves a delivery revenue requirement that includes transmission.** When ConEd (for example) files a rate case, it presents its total delivery cost of service: distribution plant costs, transmission plant costs, customer operations, administrative costs, etc. The PSC reviews these costs, applies its allowed rate of return, and approves a total delivery revenue requirement. The tariff rates (customer charge + volumetric delivery rate) are set to collect that total.

4. **The retail customer sees one bundled delivery charge.** The bill shows "delivery charges" (or equivalently, a customer charge + per-kWh delivery rate). There is no line item for "transmission" — it's embedded. The customer is paying for transmission, but it's invisible in the rate structure.

5. **Reconciliation mechanisms handle year-to-year variance.** Because FERC formula rates update annually while PSC rate cases are set for multi-year periods, the embedded transmission cost forecast can diverge from actuals. Each utility has a reconciliation mechanism:
   - **National Grid**: Transmission Revenue Adjustment (TRA) — explicitly reconciles FERC formula rate TX costs against what's embedded in retail delivery rates.
   - **ConEd**: Monthly Adjustment Clause (MAC) — covers property taxes, storm costs, pension, and other cost deviations including transmission.
   - **NYSEG / RG&E**: Rate Adjustment Mechanism (RAM).
   - **Central Hudson**: Various delivery-side true-up factors.

### Why NY bundles transmission into delivery

This is a consequence of history and corporate structure:

- **Vertically integrated origins.** Before restructuring (1990s), NY utilities were vertically integrated: they owned generation, transmission, and distribution. The PSC set rates for the whole bundle. When NY restructured and created NYISO, generation was unbundled (IOUs divested most plants and customers can now choose an ESCO for supply). But transmission and distribution stayed with the same wires company. The PSC continued setting retail delivery rates that cover T&D combined.

- **Single corporate entity.** ConEd the transmission owner and ConEd the distribution utility are the same legal entity. The TO's FERC-jurisdictional transmission revenue requirement is also a cost that appears in the utility's PSC-jurisdictional cost-of-service filing. The PSC reviews it and embeds it in delivery rates.

- **FERC vs. PSC jurisdiction.** FERC regulates wholesale transmission rates (the TSC that LSEs pay). The PSC regulates retail delivery rates (what residential customers pay). But for a self-serving TO, these are two views of the same cost. The PSC doesn't need a separate retail "transmission charge" — it simply includes transmission costs in the delivery revenue requirement.

- **No requirement for unbundled retail presentation.** FERC Order 888 required functional unbundling of transmission for wholesale open access, not retail rate presentation. The PSC has never required utilities to show transmission as a separate line item on residential bills.

---

## Per-utility evidence: transmission costs are in the delivery revenue requirement

### ConEd (Consolidated Edison)

**Source:** ConEd 2024 10-K SEC Filing (Consolidated Edison, Inc.).

ConEd's 10-K explicitly states that **transmission costs are included in PSC-approved delivery service rates**. The filing reports:

- Electric transmission plant: **$4.7 billion** (gross)
- Electric distribution plant: **$23.8 billion** (gross)

ConEd is simultaneously a FERC-jurisdictional TO under the NYISO OATT and a PSC-regulated distribution utility. Its delivery revenue requirement covers both. In Genability, all core ConEd delivery charges are classified as `chargeClass: "DISTRIBUTION"` — there is no separate TRANSMISSION charge on the residential bill.

### O&R (Orange and Rockland)

**Source:** Same ConEd 2024 10-K (O&R is a ConEd Inc. subsidiary).

O&R follows the identical structure as ConEd:

- Electric transmission plant: **$369 million** (gross)
- Electric distribution plant: **$1.4 billion** (gross)

Transmission costs are embedded in O&R's PSC-approved delivery rates.

### National Grid (Niagara Mohawk)

**Source:** National Grid Joint Proposal (PSC rate case).

The Joint Proposal explicitly includes an **embedded wholesale transmission rate of $0.01734/kWh** in PSC-approved delivery rates. This is the FERC formula rate TSC, converted to a per-kWh amount and baked into the retail delivery charge. National Grid's Transmission Revenue Adjustment (TRA) reconciles the actual FERC formula rate cost against this embedded amount annually.

National Grid's FERC Settlement Agreement (2009) established the formula rate mechanism. The Annual Transmission Revenue Requirement is approximately **$220 million**, comprising: return and income taxes on ~$137M of transmission investment, depreciation ~$43M, property taxes ~$40M, plus O&M and administrative costs.

In Genability, National Grid has a `chargeClass: "TRANSMISSION"` entry for the **Transmission Revenue Adjustment** — this is the reconciliation true-up, not a base rate. The base transmission cost is already in the delivery rate.

### NYSEG (New York State Electric & Gas)

**Source:** NYSEG 2024 DPS Annual Financial Report.

The filing states that NYSEG conducts "**regulated electricity transmission and distribution operations.**" Total electric plant in service: **$6.65 billion** (gross). The report explicitly notes:

> "Unlike other transmission owned by NYSEG" [referring to CLCPA Phase 1/2 projects with separate FERC formula rate recovery], existing transmission costs are recovered through **PSC-jurisdictional delivery rates**.

This is direct evidence: NYSEG's pre-CLCPA transmission plant is in the PSC delivery revenue requirement. Only new CLCPA transmission projects use separate FERC formula rate recovery (Phase 2) or PSC-authorized surcharges (Phase 1).

In Genability, all NYSEG core delivery charges are classified as `chargeClass: "DISTRIBUTION"`.

### RG&E (Rochester Gas and Electric)

**Source:** RG&E 2024 DPS Annual Financial Report.

The filing states that RG&E conducts "**regulated electricity transmission, distribution, and generation operations.**" Total electric plant: **$3.74 billion** (gross). The same explicit language appears:

> "Unlike other transmission owned by RG&E" [CLCPA projects], existing transmission costs are recovered through **PSC-jurisdictional delivery rates**.

RG&E's transmission plant is in the delivery revenue requirement, same as NYSEG.

### Central Hudson (Central Hudson Gas & Electric)

**Source:** Central Hudson 2024 Q3 Financial Report.

The filing provides an explicit transmission / distribution plant breakdown:

- Electric transmission plant: **$557 million** (gross)
- Electric distribution plant: **$1.367 billion** (gross)

Central Hudson's rate case filings request "**delivery revenue**" increases — the revenue requirement covers both T and D. The financial report does not separate transmission from distribution revenue; both are recovered through the same delivery tariff.

### PSEG Long Island (LIPA)

**Source:** LIPA 2024 Year-End Financial Statements; LIPA Tariff (January 2025); LIPA Board of Trustees December 2024 resolution.

LIPA is structurally different from the IOUs:

- **Not FERC-jurisdictional** for retail rates. LIPA is a state governmental authority; its retail rates are set by its Board of Trustees, not the PSC. (The PSC has limited oversight via the LIPA Reform Act but does not conduct rate cases.)
- **GASB accounting**, not FERC Uniform System of Accounts. Financial statements do not break out transmission and distribution plant in the same way as IOU FERC Form 1 filings.
- **No separate transmission line item on residential bills.** The tariff shows bundled delivery charges (customer charge + per-kWh delivery rate). Transmission costs are inside.

Evidence that transmission costs are in LIPA's delivery revenue:

- **Total utility plant and T&D capital expenditures**: ~$625M/year. **Total delivery revenue**: ~$2.15 billion. The capital budget covers both transmission and distribution.
- **NYISO OATT participation**: LIPA adopted a formula rate for its wholesale TSC (Board resolution December 2024), updating the methodology for weighted-average cost of capital.
- **No separate charge**: The January 2025 residential tariff has no transmission line item.

### NYPA (Power Authority of the State of New York)

NYPA is a TO but does not directly serve residential customers. It owns major backbone transmission facilities (765 kV and 345 kV) and sells power wholesale to municipal utilities, LIPA, and large industrial customers. Its transmission revenue requirement (~$200M/year) is recovered through the NYISO OATT TSC from LSEs whose load is served over NYPA's facilities. Those LSEs (which include the IOUs) embed this cost in their own retail delivery rates.

NYPA's recent **Propel NY** transmission projects (approved by FERC July 2024) will add to NYPA's ATRR and flow through the same mechanism.

---

## The magnitudes: how big is transmission relative to distribution?

| Utility        | TX plant ($B)            | DX plant ($B) | TX as % of T&D plant | Approx. TX in delivery ($/kWh) |
| -------------- | ------------------------ | ------------- | -------------------- | ------------------------------ |
| ConEd          | 4.7                      | 23.8          | 16%                  | ~0.02                          |
| O&R            | 0.37                     | 1.4           | 21%                  | ~0.01–0.02                     |
| National Grid  | ~1.4                     | ~5.5          | ~20%                 | 0.01734 (explicit)             |
| NYSEG          | included in $6.65B total | —             | —                    | —                              |
| RG&E           | included in $3.74B total | —             | —                    | —                              |
| Central Hudson | 0.56                     | 1.37          | 29%                  | ~0.01–0.02                     |
| LIPA           | not broken out (GASB)    | —             | —                    | —                              |

National Grid is the only utility where the embedded per-kWh transmission amount is explicitly stated in a public document ($0.01734/kWh from the Joint Proposal). For the others, the TSC contribution can be estimated from the OATT Attachment H data (annual RR ÷ annual billing units), but the retail delivery rate embeds it without separate disclosure.

**Order of magnitude**: Bulk transmission is roughly **15–30% of the total delivery (T&D) revenue requirement** depending on the utility. For a typical 600 kWh/month residential customer paying ~$0.08–0.12/kWh in delivery charges, roughly $0.01–0.03/kWh is transmission and the rest is distribution and customer operations.

---

## What about new transmission? CLCPA and the evolving picture

New York's Climate Leadership and Community Protection Act (CLCPA) is driving significant new transmission investment. These projects have different cost recovery mechanisms than legacy transmission:

### CLCPA Phase 1: PSC-authorized surcharges

Phase 1 projects (e.g., local upgrades enabling renewable interconnection) are authorized by the PSC. Cost recovery is through PSC-approved surcharges on retail delivery bills — still embedded in delivery, but potentially in a separate rider rather than base rates.

### CLCPA Phase 2: FERC formula rates via NYISO OATT

Phase 2 projects (major new transmission lines, e.g., NYPA's Propel NY) are FERC-jurisdictional. They file formula rates under NYISO OATT Attachment H and recover costs through the wholesale TSC. This cost flows to retail customers the same way as legacy transmission — through the delivery revenue requirement — but the NYSEG and RG&E DPS filings explicitly distinguish these newer projects: "Unlike other transmission owned by [NYSEG/RG&E]," CLCPA Phase 2 projects use separate FERC formula rate recovery rather than being rolled into the PSC base delivery rate.

As CLCPA transmission investment grows (tens of billions of dollars over the next decade), the transmission component of delivery rates will increase meaningfully.

---

## How this differs from ISO-NE (Rhode Island, Massachusetts, etc.)

In ISO-NE territory, residential customers see an **explicit "Transmission Charge" line item** on their electric bills. See the companion document `ri_bulk_transmission_cost_recovery.md` for full detail on the ISO-NE framework. The key structural differences:

| Feature                     | New York (NYISO)                                         | New England (ISO-NE)                                                  |
| --------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------- |
| TX on retail bill           | Embedded in bundled "delivery charge"                    | Explicit separate line item                                           |
| Retail rate regulator       | NY PSC sets bundled delivery rates                       | State PUCs (RI PUC, MA DPU, etc.) require unbundled presentation      |
| Utility structure           | Same entity owns T + D                                   | Distribution and transmission often separate entities (RIE ≠ NEP)     |
| Regional cost socialization | No — each TO has its own TSC rate                        | Yes — RNS socializes all PTF costs across New England                 |
| Customer visibility         | Low — TX invisible within delivery                       | High — customers can see TX costs rise                                |
| Restructuring history       | NY unbundled generation but kept T+D bundled in delivery | NE states unbundled all three: generation, transmission, distribution |
| Rate design implication     | No lever to discount TX for HP customers                 | Eversource offers 68% winter TX discount for heat pumps               |

The economic substance is identical: in both regions, the distribution utility pays a FERC-regulated wholesale transmission charge and recovers it from retail customers. The difference is whether the state PUC requires that recovery to be shown separately or allows it to be bundled into delivery.

---

## Summary

1. **Eight TOs** own bulk transmission in NY. Six are IOUs that also own distribution; two are governmental (NYPA, LIPA).
2. **FERC regulates the wholesale rate** (TSC) through OATT Attachment H formula rates. Each TO's TSC recovers its full embedded transmission revenue requirement.
3. **The PSC sets retail delivery rates** that include transmission costs. NY residential customers see a single bundled "delivery charge" — no separate transmission line item.
4. **Every utility's delivery revenue requirement includes transmission costs.** Confirmed through 10-K filings (ConEd, O&R), DPS Annual Financial Reports (NYSEG, RG&E), quarterly financials (Central Hudson), Joint Proposals (National Grid, with explicit $0.01734/kWh), and financial statements/tariff documents (LIPA).
5. **Reconciliation mechanisms** (TRA, MAC, RAM, etc.) handle year-to-year variance between the FERC formula rate and the PSC rate case forecast.
6. **ISO-NE differs in presentation, not substance.** RI and MA show transmission as a separate retail line item ($0.04–0.05/kWh). NY bundles it into delivery. The economics are the same.
7. **The TSC/RNS recovers embedded costs, not marginal costs.** The full revenue requirement covers both the "residual" (depreciated historical plant) and new investment. This is not a marginal cost signal — it's a cost recovery mechanism for the entire transmission portfolio.
8. **CLCPA will change the picture.** New transmission investment (Phase 1 and Phase 2) will increase the transmission component of delivery rates significantly.

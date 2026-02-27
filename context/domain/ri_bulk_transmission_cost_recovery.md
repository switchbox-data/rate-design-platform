# RI / ISO-NE bulk transmission: how it works and how costs reach residential customers

How bulk high-voltage transmission is owned, regulated, priced, and recovered on residential electric bills in Rhode Island and New England. Covers the ISO-NE OATT framework, Pool Transmission Facilities, the Regional Network Service charge, how RNS costs get passed through to the RIE retail bill as an explicit line item, why that presentation differs from New York, and how the two-tier (RNS + LNS) structure works.

---

## The players: who owns bulk transmission in New England?

New England's bulk transmission system is operated by ISO New England (ISO-NE) but owned by **Participating Transmission Owners (PTOs)** — utilities that are signatories to the Transmission Operating Agreement (TOA) with ISO-NE. The major PTOs include:

| PTO                                            | Parent / affiliate           | States served  |
| ---------------------------------------------- | ---------------------------- | -------------- |
| Eversource Energy (NSTAR Electric, CL&P, PSNH) | Eversource Energy            | MA, CT, NH     |
| New England Power Company (NEP)                | National Grid plc / PPL Corp | MA, RI, VT, NH |
| United Illuminating (UI)                       | Avangrid / Iberdrola         | CT             |
| Green Mountain Power (GMP)                     | Energir                      | VT             |
| Central Maine Power (CMP)                      | Avangrid / Iberdrola         | ME             |
| Versant Power                                  | ENMAX                        | ME             |
| Fitchburg Gas & Electric                       | Unitil                       | MA             |
| Vermont Electric Power Company (VELCO)         | VT utilities consortium      | VT             |

**Key for RI:** Rhode Island Energy (RIE, formerly Narragansett Electric) is a **distribution** utility — it does not own transmission. The transmission facilities serving RI are owned by **New England Power Company (NEP)**, a National Grid affiliate and PTO. NEP owns the high-voltage lines; RIE distributes power to retail customers. They are separate legal entities under the same corporate umbrella (PPL Corporation acquired Narragansett Electric from National Grid in 2022). RIE pays NEP for transmission service; RIE passes the cost through to retail customers.

---

## The ISO-NE OATT: structure and comparison with NYISO

### What the OATT is

Like NYISO, ISO-NE administers a single FERC-filed **Open Access Transmission Tariff (OATT)** for the region. The OATT governs how wholesale transmission service is priced and provided. Both ISOs implement FERC Order 888's requirement for non-discriminatory open access, but the internal structures differ in important ways.

### Pool Transmission Facilities (PTF) vs. non-PTF

ISO-NE draws a formal line between two tiers of transmission:

- **Pool Transmission Facilities (PTF)**: Facilities rated **69 kV and above** that are required to allow energy from significant power sources to move freely across the regional network. PTF costs are **socialized across all of New England** — every load-serving entity (LSE) in the region pays a share, regardless of which PTO owns the lines. This is the "bulk" transmission tier.

- **Non-PTF (local) transmission**: Lower-voltage or localized facilities that serve a specific utility's distribution territory. These costs are **not socialized regionally** — they are charged only to customers in the local service territory. Governed by **Schedule 21** of the OATT, with separate sub-schedules for each local transmission owner (e.g., Schedule 21-NEP for New England Power).

NYISO does not have this formal PTF / non-PTF split. In NYISO, each Transmission Owner (TO) has its own Transmission Service Charge (TSC) rate under Attachment H, and LSEs pay the TSC for the specific TO(s) whose facilities serve their load. There is no single regional pool rate that socializes all transmission costs across the state — each TO keeps its own revenue requirement.

| Feature                      | ISO-NE                                                               | NYISO                                                                                   |
| ---------------------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Regional pool rate           | Yes — RNS socializes all PTF costs across New England                | No — each TO has its own TSC rate                                                       |
| Cost socialization           | All LSEs share PTF costs regardless of which PTO owns the facilities | Each LSE pays the TSC for the TOs whose facilities it uses                              |
| Local/non-PTF tier           | Schedule 21 — utility-specific, charged only to local load           | No formal equivalent; all TX is in the TO-specific TSC                                  |
| Voltage threshold for "bulk" | 69 kV and above (PTF definition)                                     | No formal threshold; each TO's Attachment H covers its FERC-jurisdictional transmission |

### Why the socialization matters

Because PTF costs are pooled, RIE's residential customers pay a share of transmission investment across all of New England — not just NEP's facilities in Rhode Island. When Eversource builds new 345 kV transmission in Connecticut or CMP upgrades lines in Maine, RI customers pay a share. Conversely, when NEP builds in RI, the cost is spread across all six states. This regional cost-sharing model was a deliberate design choice: it prevents individual states from bearing the full cost of facilities that benefit the regional grid.

In NYISO, cost allocation is TO-specific. ConEd customers pay for ConEd's transmission; National Grid customers pay for National Grid's. There is no comparable regional pool. (NYISO does have some socialized costs — e.g., NYISO's Schedule 1 operating budget — but the bulk of transmission revenue requirements stay with each TO.)

---

## The Regional Network Service (RNS) charge

### What RNS is

RNS is the charge that LSEs pay ISO-NE for using Pool Transmission Facilities. It is defined in **OATT Schedule 9**. RNS is the ISO-NE equivalent of what NYISO calls Network Integration Transmission Service (NITS), charged through the Transmission Service Charge (TSC). The economic function is the same: recover the full embedded cost of the bulk transmission network from load.

### How the RNS rate is set

The RNS rate is set annually by the **PTO Administrative Committee (PTO-AC) Rates Working Group** — a body of the PTOs, independent of ISO-NE. ISO-NE plays no role in developing the rate formula, setting rates, or approving rates; it acts only as the settlement intermediary (collecting money from LSEs and distributing it to PTOs).

The process:

1. Each PTO files its annual **transmission revenue requirement** — the total cost of maintaining its PTF (return on investment, depreciation, taxes, O&M), computed under FERC Uniform System of Accounts.
2. The PTO-AC sums all PTOs' revenue requirements to get the **total regional revenue requirement**.
3. The total is divided by **regional 12-CP (twelve coincident peak) average monthly load** — the average of the region's 12 monthly coincident peak demands over the past year. The 2024 12-CP average was 18,133 MW.
4. The result is the **RNS rate in $/kW-month** (or equivalently $/kW-year).
5. The PTO-AC files the rate with FERC. Rates take effect January 1 each year.

### Recent RNS rates

| Year            | RNS rate ($/kW-year) | Approx. $/kWh (at 54.5% load factor) |
| --------------- | -------------------- | ------------------------------------ |
| 2025            | ~$184                | ~$0.029                              |
| 2026            | ~$184                | ~$0.029                              |
| 2027 (forecast) | ~$186                | ~$0.029                              |
| 2028 (forecast) | ~$197                | ~$0.031                              |
| 2029 (forecast) | ~$210                | ~$0.033                              |
| 2030 (forecast) | ~$220                | ~$0.034                              |

The rate is rising because PTOs are investing heavily in new PTF: ~$1.6 billion in regional project additions for 2025–2026, with incremental revenue requirements of ~$118 million.

### What the RNS rate recovers

Like the NYISO TSC, the RNS rate recovers the **full embedded cost** of all Pool Transmission Facilities — return on rate base, depreciation, taxes, O&M, administrative costs. This includes both:

- **Residual costs**: Old, partially or fully depreciated lines with low remaining book value. These represent historical investment that is largely sunk.
- **New investment**: Recently built lines carrying full return on investment.

The RNS rate is not a marginal cost signal. It reflects the average embedded cost of the entire regional PTF portfolio. The long-run marginal cost (LRMC) of bulk transmission could be higher or lower, depending on system conditions and what new projects cost.

### How RNS revenue is distributed

RNS payments collected by ISO-NE are allocated to PTOs **pro-rata based on each PTO's share of the total approved revenue requirement**. A PTO that owns 30% of the regional revenue requirement gets 30% of RNS collections.

---

## Local Network Service (LNS): the second tier

Below the regional PTF layer, each PTO has local transmission facilities — non-PTF lines, typically lower voltage, that serve a specific distribution territory. These are charged through **Schedule 21** of the OATT, with a separate sub-schedule for each local transmission owner:

- **Schedule 21-NEP**: New England Power Company's local facilities serving RI (and parts of MA)
- **Schedule 21-Eversource**: Eversource's local facilities in MA, CT, NH
- **Schedule 21-GMP**: Green Mountain Power's local facilities in VT

LNS costs are **not socialized regionally**. RIE's customers only pay for NEP's local facilities; they don't pay for Eversource's or CMP's local transmission. The LNS rate is a FERC-regulated formula rate specific to each PTO, updated annually.

The retail "Transmission Charge" on a RI bill reflects **both RNS (regional) and LNS (local) costs combined**.

---

## How transmission costs reach RIE residential customers

### The pass-through mechanism

Here is how it works, step by step:

1. **FERC sets the wholesale rates.** The PTO-AC sets the RNS rate; NEP's formula rate sets the LNS rate. Both are FERC-regulated.

2. **RIE pays ISO-NE for RNS.** As an LSE serving load in New England, RIE is a transmission customer under the OATT. Each month, RIE pays ISO-NE its share of RNS charges, based on RIE's monthly regional network load (peak demand contribution).

3. **RIE pays NEP for LNS.** RIE pays NEP for local network service over NEP's non-PTF facilities, under Schedule 21-NEP formula rates.

4. **RIE passes both costs through to retail customers.** The RI PUC requires RIE to show the combined RNS + LNS cost as a separate **"Transmission Charge"** line item on the retail bill. This is a direct pass-through — RIE does not mark up transmission costs or embed them in distribution rates. The charge is fully reconciling: a **Transmission Adjustment Factor (TAF)** annually trues up the billed amount to actual FERC-regulated costs.

5. **The retail customer sees a separate transmission charge.** On the A-16 residential bill (April 2025), the transmission charge breaks down as:

| Component                         | $/kWh        |
| --------------------------------- | ------------ |
| Base Transmission Charge          | $0.04411     |
| Transmission Adjustment Factor    | $0.00300     |
| Transmission Uncollectible Factor | $0.00062     |
| **Total Transmission Charge**     | **$0.04773** |

For a 500 kWh/month customer, this is **$23.87/month** — one of the largest single line items on the bill, comparable in magnitude to the entire distribution charge (~$0.0458/kWh × 500 = $22.90).

### Why the charge is not bundled (as in New York)

In New York, transmission costs are invisible — embedded in a bundled "delivery charge" that covers both T and D. In Rhode Island (and all of New England), transmission is a separate, visible line item. This is a direct consequence of how each region restructured in the 1990s.

**Rhode Island (and New England generally):**

- Rhode Island passed the **Utility Restructuring Act of 1996** — the first state in the country to restructure its electric industry for retail competition.
- The Act required utilities to **unbundle** generation, transmission, and distribution into separate functions and separate rate components.
- Narragansett Electric (now RIE) divested its generation assets. Transmission was already owned by a separate entity (NEP). Distribution remained with Narragansett.
- The RI PUC implemented unbundled rates: separate charges for supply (generation), transmission, and distribution on every retail bill.
- Massachusetts, Connecticut, New Hampshire, and Maine followed with similar restructuring between 1997 and 2000. All adopted unbundled retail rate presentation.
- This was modeled on FERC Order 888's wholesale unbundling, extended to the retail level by state legislatures.

**New York:**

- New York also restructured in the late 1990s, creating NYISO and allowing retail competition (ESCOs).
- But NY utilities are **vertically integrated wires companies** — the same legal entity owns both transmission and distribution (ConEd owns 345 kV lines and 13.8 kV feeders; National Grid owns both tiers in upstate NY).
- The NY PSC never required utilities to show transmission separately on residential bills. It sets a single "delivery revenue requirement" covering T + D combined, and the tariff rates (customer charge + volumetric delivery) collect that total.
- The Supreme Court case _New York v. FERC_ (2002) upheld FERC's authority over wholesale transmission but left bundled retail delivery under state jurisdiction. The PSC exercised that jurisdiction by keeping rates bundled.

**The bottom line:** New England states chose to unbundle retail rates in their restructuring statutes, making transmission visible to customers. New York did not. The economics are the same — in both regions, the distribution utility pays a FERC-regulated wholesale transmission charge and recovers it from retail customers. The difference is whether the state PUC requires that recovery to be shown as a separate line item.

| Feature             | Rhode Island / New England                                                 | New York                                            |
| ------------------- | -------------------------------------------------------------------------- | --------------------------------------------------- |
| TX on retail bill   | Explicit "Transmission Charge" line item                                   | Embedded in bundled "delivery charge"               |
| Regulatory driver   | State Restructuring Acts (RI 1996, MA 1997, etc.) required unbundling      | NY PSC never required unbundled presentation        |
| Utility structure   | Distribution and transmission often separate entities (RIE ≠ NEP)          | Same entity owns T + D (ConEd, National Grid, etc.) |
| Customer visibility | High — customers see TX costs rise ($0.031 → $0.048/kWh from 2024 to 2025) | Low — TX cost changes invisible within delivery     |
| Reconciliation      | Explicit Transmission Adjustment Factor on bill                            | TRA, MAC, RAM — delivery-side true-ups              |

---

## The magnitudes

### How big is the RI transmission charge relative to distribution?

| Component                            | $/kWh (April 2025) | Monthly (500 kWh) | Share of delivery |
| ------------------------------------ | ------------------ | ----------------- | ----------------- |
| Transmission                         | $0.04773           | $23.87            | ~43%              |
| Distribution + O&M + CapEx + Pension | ~$0.0553           | ~$27.65           | ~50%              |
| Customer charge (fixed)              | $6.00/mo           | $6.00             | ~7% (at 500 kWh)  |
| **Total delivery**                   | ~$0.115/kWh equiv. | ~$57.52           | 100%              |

Transmission is an enormous share of delivery costs in RI — roughly **43%** of the total delivery bill. This is much higher than in New York, where transmission is roughly 15–30% of delivery. The reason: ISO-NE's RNS rate socializes the cost of regional PTF investment across all New England, and that investment has been growing rapidly (new 345 kV lines for renewable interconnection, reliability upgrades). The RNS rate roughly doubled from ~$100/kW-year in 2020 to ~$184/kW-year in 2025.

### Rate design innovation: Eversource's heat pump transmission discount

Because transmission is a separate, visible charge in New England, individual utilities can apply differential rate design to it. **Eversource in Massachusetts** offers a discounted winter transmission rate for heat pump customers:

- Standard transmission: **$0.04673/kWh**
- Heat pump (winter, Nov–Apr): **$0.01492/kWh** — a **68% discount**

The rationale: ISO-NE is summer-peaking. The RNS rate is allocated based on 12-CP (monthly coincident peaks). Summer peaks drive the bulk of the allocation. Heat pump winter load does not contribute to the summer peaks that drive transmission cost allocation. Eversource's seasonal transmission rate reflects this: HP customers pay less in winter because their winter usage does not drive transmission investment.

This innovation is only possible because transmission is a separate line item. In New York, where transmission is buried inside a bundled delivery charge, there is no lever to offer a reduced transmission rate for winter HP load without restructuring the entire delivery tariff.

---

## What the RNS recovers: embedded costs, not marginal costs

Like NYISO's TSC, the RNS rate recovers the **full embedded cost** of the bulk transmission network — not the marginal cost. The revenue requirement includes return on decades-old, largely depreciated facilities alongside brand-new projects still carrying full investment value.

This means the RNS rate reflects both:

- **Residual (sunk) costs**: The cost of paying for infrastructure that already exists, whether or not it is needed for the next increment of load.
- **New investment costs**: The cost of recently completed projects that reflect current construction costs.

The weighted average produces the embedded $/kW-year rate. The long-run marginal cost (LRMC) of bulk transmission — what the next MW of transfer capability actually costs — is a different number. Given the scale of new investment in New England (offshore wind interconnection, North-South interface upgrades), the LRMC is likely higher than the current embedded average.

ISO-NE's **2050 Transmission Study** (February 2024) is the closest thing in the Northeast to a forward-looking assessment of bulk transmission investment needs. It identified four major high-likelihood constraints: North-South interface, Boston import, Northwestern Vermont import, and Southwest Connecticut import. A key finding: reducing peak load significantly reduces transmission costs. But the study produces scenario-level project portfolios, not a clean $/kW-year LRMC number.

---

## Summary

1. **ISO-NE operates a two-tier transmission system**: regional PTF (69 kV+, socialized across all New England via RNS) and local non-PTF (utility-specific, charged via Schedule 21 LNS).
2. **RNS is the regional pool charge** — set annually by the PTO Administrative Committee, approved by FERC, effective January 1. Currently ~$184/kW-year (~$0.029/kWh), rising to ~$220/kW-year by 2030.
3. **RIE doesn't own transmission.** NEP (a National Grid / PPL affiliate) is the PTO. RIE pays ISO-NE for RNS and NEP for LNS, then passes both through to retail customers.
4. **The Transmission Charge on RIE's A-16 tariff is a direct FERC pass-through**: Base Transmission + Transmission Adjustment Factor + Uncollectible Factor = $0.04773/kWh (April 2025). This recovers the cost of high-voltage transmission (FERC-regulated, ISO-NE OATT). It is a pass-through of Regional Network Service (RNS) and related OATT charges to distribution customers. Set by FERC/ISO-NE; RIE passes through.
5. **The charge is separate (not bundled) because of RI's 1996 Utility Restructuring Act**, which required unbundled retail rates. New York never required this, so NY utilities embed TX in a bundled delivery charge.
6. **Transmission is ~43% of RI's delivery bill** — far larger than in NY (~15–30%). The RNS rate has roughly doubled since 2020 due to regional PTF investment.
7. **The RNS rate recovers embedded costs, not marginal costs** — same as NYISO's TSC. It covers both sunk (residual) and new investment.
8. **ISO-NE's 2050 Transmission Study** is the closest thing to a bulk TX marginal cost assessment in the region, but it produces scenario-level project portfolios, not a single LRMC number.

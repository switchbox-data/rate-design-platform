# The Value of, and Compensation for, Distributed Energy Resources in Illinois

**Source**: ICC-VDER-Report-FINAL-2025-1-17.pdf
**URL**: [E3 ICC-VDER Report, Illinois (Jan 2025)](https://www.ethree.com/wp-content/uploads/2025/01/ICC-VDER-Report-FINAL-2025-1-17.pdf)
**Pages**: ~110 total (plus appendices)
**Date**: January 2025
**Prepared for**: Illinois Commerce Commission (ICC)
**Author(s)**: Andrew DeBenedictis, Fangxing Liu, Andrew Solfest, Stephanie Kinser, Jun Zhang,
Melissa Rodas, Hugh Somerset, Genevieve McQuillan, Eric Cutter (all E3); Mabell Garcia Paine,
Kristine Jiao (Viridis)
**Author affiliations**: Energy and Environmental Economics, Inc. (E3), 44 Montgomery Street,
Suite 1500, San Francisco, CA 94104 | www.ethree.com

**Note**: This file is a **focused extraction** of the sections directly referenced by the
Switchbox rate-design-platform — specifically Section 3.2 (Avoided Transmission, Table 8),
Section 6.1.2 (Transmission update process), and Appendix C (Hourly Allocation / PCAF). For
the full report, download the PDF at the URL above. Minor typographical errors have been
silently corrected for readability.

---

## Why this report matters for Switchbox

This is the primary regulatory precedent for our PJM bulk transmission marginal cost
methodology for Maryland. Key uses:

1. **Section 3.2 / Table 8**: Establishes that PJM NTS (Network Transmission Service) rates are
   the accepted upper-bound proxy for transmission avoided costs in a PJM-territory utility
   (ComEd, $39.80/kW-yr in 2024). Direct precedent for using NITS rates in MD.
2. **Section 6.1.2**: Documents that MISO and PJM explicitly confirmed to E3 they cannot provide
   marginal transmission cost estimates — the RTO pushback that justifies using the embedded-cost
   proxy.
3. **Appendix C**: Defines the PCAF (Peak Capacity Allocation Factor) methodology with K=150
   peak hours for transmission and distribution, and K=100 for generation capacity. Verbatim
   source for our allocation method.

---

## Table of Contents (abridged)

- Executive Summary
- 1 Introduction and Context
- 2 A Framework for Compensation Design
- 3 DER Benefits
  - 3.1 Distribution Avoided Costs
  - **3.2 Avoided Transmission** ← _key section_
  - 3.3 Other Avoided Costs
  - 3.4 Total Avoided Costs
  - 3.5 Bill savings
  - 3.6 State and federal incentives
  - 3.7 Non-monetized benefits
- 4 DER use case analysis
- 5 Proposed Compensation Formula
- 6 Update process and future improvements
  - 6.1 Data updates on DER values
    - 6.1.1 Distribution system value
    - **6.1.2 Transmission** ← _key section_
    - 6.1.3 Generation
    - 6.1.4 Non-monetized benefits
  - 6.2 External triggers for update
- 7 Stakeholder Engagement
- 8 Summary of Recommendations
- Appendix A. Stakeholder Feedback
- Appendix B. Avoided Cost Methodology
- **Appendix C. Hourly Allocation** ← _key section_
- Appendix D. Weather Re-mapping

---

## Executive Summary (excerpt, p. 1)

On September 15, 2021, the Climate and Equitable Jobs Act (CEJA) was signed into law with the
goal of guiding the transition of Illinois into a more sustainable and equitable energy future. The Act
recognizes that, due to their physical location close to load, Distributed Energy Resources (DERs)
can play a unique role in providing value to the electric distribution grid, but the state lacks both a
framework to quantify this value and a compensation mechanism to promote DER adoption and
dispatch that helps realize this value. To address this gap, the law mandates that the Illinois
Commerce Commission (ICC) initiate an investigation into the value of, and compensation for, DERs.
This report is the outcome of that investigation.

DERs may provide value to the grid in several ways. They can provide energy and capacity, relieve
stress on transmission and distribution systems during constrained hours, provide Greenhouse Gas
(GHG) benefits, and avoid system losses given their proximity to load. They also can provide
non-monetized benefits, which do not impact utility costs. DER customers may receive compensation
for these values through a reduction in their electricity bills based on the retail rate — a process known
as net metering — and through DER-specific incentive programs.

---

## Section 3 — DER Benefits (pp. 25–39)

### 3.1 Distribution Avoided Costs (pp. 26–28, excerpted)

**Table 7. Distribution Capacity Values by Utility (2024 $/kW-yr)**

| Source | Near-Term Distribution Value    | Long-Term Distribution Value       |
| ------ | ------------------------------- | ---------------------------------- |
|        | Utility Cost of Service Studies | FERC Form 1 and Refiled Grid Plans |
| Ameren | $9.43                           | $34.30                             |
| ComEd  | $5.54                           | $27.98                             |

---

### 3.2 Avoided Transmission (pp. 28–29)

DERs may also shift or reduce load further upstream from the distribution system, resulting in avoided
or deferred costs for transmission capacity. The three guiding principles discussed for evaluating
avoided costs as marginal, long-term, and technology-agnostic apply equally to the transmission
system, though the nature of transmission planning in Illinois poses some challenge to isolating for
marginal long-term costs.

Ameren Illinois and ComEd each receive transmission service from a regional transmission
organization or operator (RTO). Ameren is served by Midcontinent Independent System Operator
(MISO), while ComEd is served by PJM. The structures of each RTO and their relationships with the
respective utilities vary slightly, but in each instance the RTO is largely responsible for long term
transmission planning and coordinating between transmission assets that are frequently developed,
owned, or operated by a collection of smaller entities: Ameren and ComEd. As Ameren and ComEd
rely on this shared transmission system, they pay the RTO for their usage based on a dollar-per-
kilowatt-year network transmission service (NTS) rate. For the purposes of this analysis, these rates,
presented for each utility in Table 8, are used as an **upper bound estimate of transmission capacity
avoided costs**. These values are allocated to specific hours of transmission need using the PCAF
approach described in Appendix C.

**Table 8. Transmission Capacity Values by Utility (2024 $/kW-yr)**

|        | Source | 2024 Network Transmission Service Rate |
| ------ | ------ | -------------------------------------- |
| Ameren | MISO   | $80.00/kW-yr                           |
| ComEd  | PJM    | $39.80/kW-yr                           |

It is important to note that the network transmission service rates are driven by gross annual
transmission expenses incurred by the RTOs, plus a rate of return. While these rates are expressed
and charged to the utilities in terms of dollar-per-kilowatt-year, these expenses include several
categories which are embedded or not explicitly capacity-driven. Therefore, these rates are more
attuned to **average cost for the system rather than marginal costs**. In the very short term, if Ameren
or ComEd's coincident peak load were to increase or decrease by a certain amount, then the
difference in their required payment to the RTO could be calculated by multiplying that amount by
the listed NTS rate. However, the actual expenses incurred by the RTOs would be expected to
increase or decrease to some lesser degree, because the embedded costs have not changed with
the capacity. The next time the NTS rate is evaluated, that rate itself would be updated to more
appropriately reflect the actual impact on transmission costs. Because of this, each RTO independently
noted that the NTS rate is not an appropriate indicator of their capacity-driven marginal costs.

After extensive discussions with both utilities and RTOs, these organizations indicated to E3 that they
were not able to provide a more accurate, specific marginal cost for transmission capacity at this
time. Both MISO and PJM have stated that they were interested in further exploring this value through
collaborative future transmission planning with the Illinois utilities.

---

## Section 6 — Update Process and Future Improvements (pp. 69–88)

### 6.1 Data updates on DER values (p. 69)

### 6.1.2 Transmission (p. 71)

Like the current long-term distribution costs, the Network Transmission Service rates used as a proxy
for transmission avoided costs in this analysis are more indicative of an average cost of capacity
rather than a marginal cost. MISO and PJM explicitly noted this in our discussions, though did not yet
have a means for providing marginal cost estimates in terms of capacity or an avenue for sharing the
data necessary for us to calculate these values ourselves. However, both RTOs expressed an interest
in exploring these values further to determine a more appropriate avoided transmission cost value
for future use. Pursuing the key data will require closer collaboration between the RTOs and Ameren
and ComEd in their respective transmission planning processes. Deeper collaboration in long-term
transmission planning is expected to be beneficial to all parties involved even beyond the purposes
of future avoided cost analyses. We strongly recommend encouraging such an outcome and
continuing the discussions begun during this initial analysis to home in a marginal transmission cost
value.

Additionally, there may be an overlap between transmission and generation capacity avoided costs,
because Cambium capacity shadow prices can be driven by the cost of building additional
transmission capacity. In such cases, generation capacity avoided costs become equivalent to
transmission capacity avoided costs.

---

## Appendix C — Hourly Allocation (pp. 98–99)

To recognize the importance of DERs providing value during the specific hours when system capacity
is constrained, E3 allocated the total distribution, transmission, and generation capacity value across
the hours of the year with the greatest anticipated load. The approach used may be generally
categorized under a **Peak Capacity Allocation Factor (PCAF) methodology**.

Hourly system load forecasts for Ameren and ComEd were obtained from the Cambium MISO Central
and PJM West datasets, respectively, for the years 2025–2050.[^39] Allocation factors for transmission
and distribution capacity were then assigned to the **top 150 load hours** based on the share of load
in each of these hours divided by the total load across these 150 top load hours.[^40] The determination
of generation capacity allocation factors follows the same process but using the **top 100 hours of
net load**.[^41]

**Figure 42 displays the PCAF equation (p. 98):**

[DIAGRAM DESCRIPTION: PCAF Equation — Figure 42]

A mathematical formula graphic showing the Peak Capacity Allocation Factor calculation. The
formula defines the allocation factor $PCAF_h$ for each hour $h$ in the top-K peak hours as the
load in that hour divided by the sum of load across all K top peak hours:

$$PCAF_h = \frac{L_h}{\sum_{k \in \text{top-}K} L_k} \quad \text{for } h \in \text{top-}K \text{ hours}$$

All other hours receive an allocation factor of zero.

[→ See original PDF page 98 for visual rendering]

The sum of the resulting allocation factors for all top hours is equal to 1, while all other hours of the
year were assigned allocation factors of zero. The total avoided cost of capacity for each component
was multiplied by the allocation for each hour of the year to produce a $/kWh value, such that the
sum of all hourly values is equal again to the original $/kW-yr capacity cost.

Figure 43 illustrates how these allocation factors are distributed across the days and hours of the
year for a single historical year. In early years, the system peak and resulting allocation factors are
concentrated in summer months, so that is when additional capacity is most valuable. As load
patterns change over time, including due to heating and vehicle electrification, the allocation factors
shift in kind. In order to provide a clearer picture of the intraday hourly patterns, Figure 44 displays
the sum of all allocation factors assigned to each hour of the day when combined across all days of
the year. In early years the capacity value is concentrated in the mid to late afternoon, though this
also shifts with future load patterns.

Within the BCA tool, the expected load or generation profiles of individual DERs are multiplied by the
hourly $/kWh avoided costs to estimate the value provided by the given DER. As a result, any
resource able to provide capacity to the grid during all peak load hours of a year would then be
determined to provide the full capacity benefit for that year. A resource able to supply capacity
during a portion of the peak load hours would be estimated to provide some allocation-weighted
portion of the maximum potential benefit to the electric grid.

[^39]: Transmission and distribution allocation factors are based on the total end-use load minus distributed solar data,
    while generation capacity allocation factors are based on the total end-use load minus all renewable generation.

[^40]: For years when multiple hours are 'tied' at the threshold for the 150th peak hour, all of these threshold hours are
    included within the set of peak hours.

[^41]: The use of only 100 hours for generation capacity is intended to align with Cambium's allocation of these costs.

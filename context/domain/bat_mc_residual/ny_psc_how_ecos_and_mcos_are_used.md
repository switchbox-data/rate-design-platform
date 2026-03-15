# How New York Allocates Electric Utility Costs Between Customer Classes

**New York uses embedded cost of service (ECOS) studies — not marginal cost studies — as the primary tool for allocating revenue responsibility among customer classes in electric rate cases.** This finding may surprise those familiar with NY's pioneering role in marginal cost pricing during the 1970s. While NY utilities still file both ECOS and marginal cost of service (MCOS) studies in every rate case, the two serve distinct purposes: the ECOS drives inter-class revenue allocation, while the MCOS informs rate design, distributed energy resource (DER) compensation, and contract floor rates.[^synapse][^lbl] Because the ECOS directly allocates the full embedded revenue requirement, the "residual" reconciliation problem central to EPMC-based jurisdictions like California simply does not arise in NY rate proceedings.[^cpuc] This dual-study framework — rooted in a pragmatic hybrid of efficiency and fairness principles — has persisted across every major NY utility for decades and remains firmly in place through 2026.

## The dual-study framework: ECOS for allocation, MCOS for pricing signals

Every major NY electric utility files both an embedded and a marginal cost of service study in rate cases, but they perform fundamentally different jobs. The **ECOS** follows the traditional NARUC three-step methodology — functionalization, classification, and allocation — to assign the entire embedded revenue requirement to customer classes. [Central Hudson's Cost of Service Panel](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B85559E72-0940-406F-B562-0EF0D2DCDCE1%7D) explicitly cites the **1992 NARUC Electric Utility Cost Allocation Manual** as its "basic reference on cost of service methodology," and the other utilities follow essentially the same approach.[^cenhud_cos]

The **MCOS**, by contrast, calculates forward-looking long-run marginal costs that inform efficient price signals. Central Hudson files its MCOS studies as exhibits alongside the ECOS (labeled COSP-4 and COSP-5, with COSP-6 providing a marginal-vs.-embedded comparison).[^cenhud_cos] The MCOS methodology across NY utilities generally follows the **NERA approach** developed in the late 1970s for EPRI's Electric Utility Rate Design Study, using economic carrying charges to annualize marginal capital costs.[^rap] [Con Edison's MCOS studies](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B64F27D69-52AE-4302-B9F6-037C28B602AA%7D) have historically been prepared by NERA (National Economic Research Associates) and, more recently, by [The Brattle Group](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B21D11BD3-4538-45BA-B6DE-0B144B9AA5B4%7D).[^coned_brattle_mcos] [NYSEG and RG&E's 2025 MCOS studies](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30C5C197-0000-CB2E-BFF1-4EFC836B79EB%7D) were prepared by Charles River Associates using probability-of-peak analysis.

The critical distinction is that **no NY utility uses MCOS results to determine how much each customer class pays in total**. [Con Edison's response to DPS Staff interrogatories](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7BA2535CDD-CC9A-4CB9-9245-C9DCCAF06B85%7D) in Case 16-E-0060 stated explicitly that the company does "not consider price elasticity in allocating common costs in its marginal cost study" — ruling out both EPMC and inverse elasticity approaches for inter-class allocation.[^coned_cos] Instead, MCOS results are deployed for:

- **VDER Value Stack compensation** (the Demand Reduction Value and Locational System Relief Value components)[^aeu]
- **Marginal cost floor rates** for negotiated economic development contracts[^nucor_order]
- **Guidance on time-differentiated rate design** within classes[^standby]
- **Standby rate calibration** for DER customers[^standby_review]

## How ECOS methodology works across the major utilities

While each utility's ECOS has distinctive features, they share the same three-step architecture. **Functionalization** assigns each cost element to transmission, distribution, customer, or general categories using FERC Uniform System of Accounts. **Classification** determines whether functionalized costs are demand-related, energy-related, or customer-related. **Allocation** distributes classified costs to service classes using load-based, customer-count-based, or direct-assignment allocators.[^synapse]

### Con Edison

[Con Edison](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7BA2535CDD-CC9A-4CB9-9245-C9DCCAF06B85%7D) uses a five-day, four-hour coincident peak (CP) demand allocator for transmission costs and high-tension distribution. For low-tension distribution, it employs a weighted blend of individual customer maximum demands (ICMD) and non-coincident peak (NCP) demands — specifically, a **25% ICMD / 75% NCP weighting** (the "D08 allocator") for residential and certain small commercial classes, reflecting load diversity in multi-family buildings.[^coned_cos] Customer-related costs are allocated by customer counts using a minimum-system methodology. Each class's revenue responsibility is measured against a **±10% tolerance band** around the system rate of return — classes outside the band receive gradual adjustment toward cost of service, while those within the band are left unchanged.[^coned_dac] This framework has been in place since at least the 1990s.[^coned_rebuttal]

### National Grid (Niagara Mohawk)

[National Grid](https://www.nationalgridus.com/Rate-Case-2025-UNY) follows a textbook NARUC ECOSS approach: NCP demand for distribution, CP demand for transmission, customer counts for customer-related costs, and special circuit studies for secondary distribution allocation.[^natgrid_order] Revenue increases are allocated proportionally across service classes to avoid sharp interclass cost shifts.[^natgrid_news]

### NYSEG and RG&E

[NYSEG and RG&E](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B6BAA243C-F0B5-48EB-AB77-A912ECA911CF%7D) present a more contested picture. The companies file multiple ECOS studies simultaneously — one classifying distribution line costs (FERC Accounts 364–368) on a **100% demand basis** and another using a **50% customer / 50% demand split**.[^nyseg_jp] The Utility Intervention Unit (UIU) has consistently opposed the 50/50 split, noting that the companies themselves treat analogous gas distribution costs as 100% demand-related. The Joint Proposals in NYSEG/RG&E cases explicitly state that **"no single ECOS study forms the basis for revenue allocation"** — both studies are considered together, and classes that show contradictory results under the two methodologies are treated as falling within the tolerance band.[^nyseg_jp]

### Central Hudson

[Central Hudson](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B85559E72-0940-406F-B562-0EF0D2DCDCE1%7D) uses summer/winter coincident peak demand for production and transmission allocation, class non-coincident peak kW for distribution substations and primary lines, and the sum of individual customer peak demands (Sigma NCPi) for secondary distribution.[^cenhud_cos] Its 12 electric service subclasses and 9 gas subclasses reflect the granularity of its cost analysis.[^cenhud_news]

## Six recent rate cases confirm ECOS dominance

Analysis of recent NY electric rate cases across six utilities demonstrates the consistency of this framework.[^dps_cases]

### Con Edison, Case 25-E-0072 (filed January 2025, PSC order January 22, 2026)

Con Edison filed ECOS studies through its Demand Allocation and Cost of Service Panel.[^coned_dac] The three-year Joint Proposal allocated the transmission and delivery revenue increase as a **uniform percentage increase** to all customer classes. Approved electric delivery increases totaled roughly $234M (RY1), $410M (RY2), and $421M (RY3), with a **9.4% return on equity**.[^coned_summary][^coned_news]

### Con Edison, Case 22-E-0064 (filed January 2022, [PSC order July 20, 2023](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B0043B495-0000-C413-BF18-0E262AD26F75%7D))

The PSC order explicitly states that Con Edison filed Embedded Cost of Service (ECOS) studies with its initial electric filing designed to ascribe utility cost responsibility to each service class. DPS Staff found the ECOS study "reasonable."[^coned_order_22] Revenue allocation followed the same uniform-percentage methodology through a negotiated Joint Proposal, with residential customer charges stepping up from $17 to $20 over three rate years.

### Orange & Rockland, Case 24-E-0060 (filed January 2024, [PSC order March 20, 2025](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B154D555D-D3E1-4048-82AB-C14E82D86BD2%7D))

The PSC order stated explicitly: "The revenue allocation proposed in the Joint Proposal does not use or reflect any single embedded cost of service (ECOS) study sponsored by any party. Rather, the Signatory Parties agreed to an allocation of revenue increases for individual customer classes."[^or_order] O&R filed ECOS studies, UIU opposed them, but the negotiated compromise was accepted. Electric delivery charges were flat in Rate Year 1, with increases phased in during RY2 and RY3.

### NYSEG/RG&E, Case 22-E-0317/22-E-0319 (filed May 2022, [PSC order October 12, 2023](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B6BAA243C-F0B5-48EB-AB77-A912ECA911CF%7D))

Multiple ECOS studies were filed reflecting the ongoing 50/50 vs. 100% demand classification dispute.[^nyseg_jp] The three-year Joint Proposal allocated revenue increases guided by the ECOS results and the tolerance band methodology. NYSEG residential electric delivery rates rose roughly **62% cumulatively** over three years — among the steepest increases in recent NY history.[^nyseg_news]

### National Grid, Case 24-E-0322 (filed May 2024, [PSC order August 14, 2025](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B90E66D96-0000-CA12-A897-0A5E5C2B9F2B%7D))

National Grid filed its standard ECOSS using NARUC methodology.[^natgrid_order] The 15-party Joint Proposal allocated electric delivery increases of $167M (RY1), $297M (RY2), and $243M (RY3), with a **9.5% ROE**.[^natgrid_news]

### Central Hudson, Case 24-E-0461 (filed August 2024, [PSC order August 14, 2025](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7BB8B80A70-DAE4-4601-9B5C-6C41A934CA19%7D))

Central Hudson filed ECOS studies (exhibits COSP-1 through COSP-3) and MCOS studies (COSP-4 through COSP-5) along with a marginal-vs.-embedded comparison (COSP-6).[^cenhud_cos] The ECOS drove revenue allocation, while the MCOS informed rate design. Residential electric bill impacts were **3.47%** in each of the first two rate years and **3.23%** in the third.[^cenhud_news]

## Why the EPMC "residual" problem doesn't arise in New York

The question of how to allocate the residual — the gap between total marginal cost revenue and total embedded cost revenue — is central in jurisdictions like California that use MCOS as the primary inter-class allocation tool. [California's CPUC](https://docs.cpuc.ca.gov/published/Final_decision/72471-04.htm) applies Equal Percentage of Marginal Cost (EPMC) scaling, where each class's marginal cost revenue is multiplied by a uniform scalar to recover the full embedded revenue requirement.[^cpuc] Oregon uses a functionalized variant of EPMC, reconciling within generation, transmission, and distribution categories separately.[^rap]

**New York sidesteps this problem entirely.** Because the ECOS directly allocates the full embedded revenue requirement through its functionalization-classification-allocation steps, there is no residual to reconcile. The ECOS produces a rate-of-return index for each class relative to the system average, and revenue allocation adjustments are made to move classes toward cost-based ROR targets within gradualism constraints.[^coned_cos] In practice, Joint Proposals — the negotiated settlements that resolve the vast majority of NY rate cases — allocate revenue increases as uniform percentage adjustments across classes or through agreed-upon deviations from strict ECOS results, always informed by but never mechanically determined by any single cost study.[^or_order][^nyseg_jp]

For the limited purposes where MCOS results are applied — primarily VDER compensation — the residual question is also moot because the marginal cost values are used directly as compensation rates (e.g., DRV and LSRV in the Value Stack), not to recover the total revenue requirement.[^aeu][^standby]

## The policy framework: from Opinion 76-15 to Case 19-E-0283

New York's cost allocation framework rests on several foundational decisions. **Opinion 76-15** (Case 26806, August 10, 1976), issued under Chairman Alfred E. Kahn, established that marginal costs are the most relevant costs for rate-setting and should be utilized to the greatest extent practicable. This landmark decision — issued during the national wave of marginal cost reform following the 1973 oil crisis — established NY as a pioneer alongside Wisconsin (1974) and California (1976).[^lbl] However, despite its strong language favoring marginal cost principles, the opinion's practical impact was primarily on rate _design_ (time-differentiated pricing) rather than on inter-class revenue _allocation_, which continued to rely on ECOS.

The most significant active generic proceeding is **[Case 19-E-0283](https://dps.ny.gov/event/cost-service-psc-seeks-comment-whitepaper-filed-department-public-service-staff-makes-various)**, initiated in 2019 as a companion to the VDER proceeding (Case 15-E-0751). DPS Staff filed a whitepaper in 2023 recommending standardized MCOS methodology across utilities.[^mcos_whitepaper] A [technical conference](https://dps.ny.gov/event/cost-service-tech-conference-regarding-marginal-cost-service-studies) was held to discuss the proposed methodology.[^mcos_techconf] The Commission's **[August 19, 2024 Order Addressing Marginal Cost of Service Studies](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B80570F9B-0000-CF28-A6B7-644C6FA478D9%7D)** established methodology requirements, directed all Joint Utilities to file updated MCOS studies by June 30, 2025 (which they did), and specified that system-wide results should reflect long-run, non-zero marginal costs.[^mcos_order] Critically, this proceeding focuses on modernizing MCOS for DER valuation — not on replacing ECOS for inter-class allocation.

Other important policy decisions include **Case 08-G-0888** (prescribing a 65%/35% demand/customer classification for Central Hudson's gas mains), **Case 14-M-0565** (allocating low-income program costs across all service classes proportional to embedded T&D delivery revenue), and the **[March 2022 standby/buyback rate order](https://www.utilitydive.com/news/new-york-adjusts-standby-buyback-rate-methodologies-sweetening-value-prop/620919/)** establishing a "decision tree" methodology for classifying costs as customer, shared, or local.[^standby]

## How marginal costs shape rate design within customer classes

Given that Opinion 76-15 declared marginal costs the most relevant for rate-setting, and that MCOS studies continue to be filed in every rate case, the natural question is: what exactly do they _do_ at the rate design step? The answer is that MCOS data functions as a **directional guide and reasonableness check** — not as the binding mathematical starting point for setting charge levels.[^synapse][^naruc]

### The two-step rate design process

New York's rate design operates in two conceptually distinct stages. **Step one** uses the ECOS to determine each class's share of the total revenue requirement, producing indexed rates of return that reveal which classes over- or under-earn relative to the system average. **Step two** translates the ECOS-determined class revenue target into specific tariff charges (demand charges, energy charges, customer charges, TOU differentials). This second step is where MCOS data enters — but its role is advisory rather than determinative.[^synapse][^e3_fvt]

The [Synapse Energy Economics fact sheet](https://www.synapse-energy.com/sites/default/files/Ratemaking-Fundamentals-FactSheet.pdf) captures NY's approach precisely: some regulators rely on embedded cost studies to allocate costs between classes, and then use marginal cost information to inform rate design elements (such as inclining block rates or time-varying rates) within classes.[^synapse] The MCOS provides the marginal cost per kWh (energy-related) and per kW (demand-related) for each class, indicating the efficient price signal for each rate component. The rate designer then uses these marginal unit costs to set the _relative_ levels of demand and energy charges, adjusting charges to hit the class revenue target set by the ECOS.

The **residual** at the intra-class level — the gap between what pure marginal-cost pricing would collect and the ECOS-determined class revenue requirement — is handled implicitly. No formal EPMC scalar or Ramsey-pricing calculation is performed. Instead, rate components are adjusted within the class to meet the embedded-cost revenue target. For residential classes, this typically means the volumetric energy charge absorbs the residual. For commercial and industrial classes with demand charges, the residual is distributed across both demand and energy components. Rate continuity, bill-impact mitigation, and gradualism considerations often dominate the final charge levels more than pure marginal cost arithmetic.[^e3_fvt][^synapse_fix]

### What utility testimony reveals about actual practice

The most candid statement about MCOS's limited role in general rate design comes from [Central Hudson's Cost of Service Panel testimony](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B85559E72-0940-406F-B562-0EF0D2DCDCE1%7D). The panel explicitly stated that its marginal COS studies were utilized by the Forecasting and Rates Panel to revise the **Excelsior Jobs Program (EJP) rates** and to revise the **contract demand charge** in the buyback service classification — not to inform rate design for standard service classes.[^cenhud_cos] The panel further noted that it compared the results of the marginal COS studies to the embedded COS studies but not to the delivery rates proposed by the Forecast and Rates Panel.

This separation — where the COS Panel prepares both studies but the Rates Panel designs tariffs using primarily the embedded results — illustrates NY's practical reality. The MCOS comparison serves as an analytical exhibit, revealing whether embedded rates diverge significantly from marginal costs, but does not drive the actual charge-setting arithmetic for SC1 (residential), SC2 (commercial), or other standard classes.

The NYSEG/RG&E proceedings tell a slightly more integrated story. [DPS Staff's Electric Rates Panel (SERP)](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B90C1109C-0000-CA56-8028-C926039990E0%7D) in Cases 25-E-0375 et al. covers ECOS studies, MCOS studies, revenue allocation, and standard service rate design within a single panel.[^nyseg_serp] NYSEG and RG&E filed corrected MCOS studies (July 15, 2025) alongside their rate case filings, consistent with the August 2024 MCOS Order. But even here, the ECOS determines class revenue allocation while the MCOS provides informational context for within-class design choices.

## How marginal costs influence specific rate components

### Demand charges

Demand charges for commercial and industrial classes (Con Edison's SC 9, NYSEG's SC 7, National Grid's SC 3/3A) are the rate component most closely informed by marginal capacity costs. Distribution marginal costs are driven overwhelmingly by coincident peak demand at the circuit and substation level. In practice, demand charge levels in C&I classes reflect demand-classified embedded costs from the ECOS, but MCOS data provides directional guidance on whether demand charges should increase relative to energy charges over time.[^coned_cos][^mcos_order]

### Time-of-use rate differentials

TOU rate differentials represent the area where marginal cost analysis has the most direct structural influence. In Con Edison's current rate case (25-E-0072), EDF testimony collected hourly NYISO LBMPs for 2022–2024, indexed them to monthly peaks, and identified a **six-hour peak window (2 PM–8 PM)** where average LMPs were significantly higher than off-peak prices.[^coned_brattle_mcos] Cross-referencing with ten years of NYISO coincident peaks confirmed that system peaks cluster in the 4 PM–6 PM band. This marginal-cost-driven analysis contrasts with Con Edison's existing Rate III, which uses a **16-hour peak period (8 AM to midnight)** — a window so broad it dilutes the marginal cost signal. DPS Staff has similarly recommended that utilities consider whether existing seasonal differentials and block rate differences reflect cost differences, and perform marginal cost analysis for tail-block summer rates.[^nyseg_serp]

### Seasonal differentials

Seasonal differentials are informed by the ratio of seasonal peak loads to annual averages. For proposed heat-pump rates, primary distribution costs are seasonalized according to the ratio of each season's average hourly load to the annual average hourly load, while transmission costs are differentiated according to the ratio of each season's peak hourly load to the average system hourly load.[^e3_fvt] Summer rates are higher in downstate NY (Con Edison, O&R) reflecting summer-peaking load patterns. NYSEG, which experiences both summer and winter peaks, uses an average of the two for demand allocation. NYSEG/RG&E's 2025 MCOS studies include a [Probability of Peak analysis](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30C5C197-0000-CB2E-BFF1-4EFC836B79EB%7D) producing allocation factors for each hour by day-type and month, described as useful in evaluating the time differentiation of near-term marginal costs.

### Customer charges

Customer charges are set primarily by embedded customer costs (metering, billing, service drop) from the ECOS, not by marginal cost analysis. In Con Edison's 2019 Joint Proposal, the optional demand-based Rate IV included a **$27 customer charge** described as reflecting the full customer cost from the 2017 ECOS study.[^coned_cos] Consumer advocates consistently push for lower customer charges — PULP proposes a $10/month cap — while the REV Track 2 framework stated that the fixed customer charge should reflect only distribution costs that do not vary with customer demand or energy.[^synapse_fix] Marginal cost analysis generally supports minimizing customer charges because marginal costs are driven by demand, not customer count.[^atrium_rates]

## The marginal cost floor for economic development rates

The most explicit, dollar-for-dollar use of marginal cost in NY rate design outside VDER is as the **price floor for negotiated flex-rate contracts**. Opinion No. 94-15 (Case 93-M-0229, July 11, 1994) established that flex-rate contract prices must exceed the utility's marginal cost of providing service and include a contribution to system common costs (originally 1¢/kWh, waivable in specific circumstances). PSL §66(12-b) authorizes these "special rates or tariffs" for customer retention and economic development.

The **[Nucor Steel case](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B90062397-0000-CB14-9FBC-12776BAE1F11%7D)** (Case 01-E-0680, March 2002) provides the most detailed exposition.[^nucor_order] The PSC found that Nucor's base rate price was reasonable because it exceeded the marginal cost NYSEG would incur in supplying electricity to Nucor's existing demand over a seven-year forecast horizon. The difference between the flex-rate price and marginal cost constitutes the customer's "contribution to margin" — ensuring non-participating ratepayers are not subsidizing the discounted service. The PSC rejected NYSEG's request for a floor-price escalator, finding the seven-year marginal cost forecast sufficiently reliable.

This framework treats marginal cost as the absolute minimum viable rate: any price below marginal cost means incremental service costs more than the revenue it produces, creating a cross-subsidy from other customers. The [Bonbright framework](https://atriumecon.com/our-approach/sound-rate-design/) formalizes this as the rate range bounded by **marginal cost floor** and **value-of-service ceiling**, with embedded cost as the default benchmark.[^atrium_rates]

## Standby rates: where marginal cost meets the decision tree

The [March 16, 2022 PSC order](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B725EE95B-B8B1-48EE-A4EB-653998A62F0E%7D) in Case 15-E-0751 reformed standby rate design through a **"decision tree" methodology** — eight sequential questions determining whether each cost component should be recovered through customer charges (customer-specific), daily as-used demand charges (shared infrastructure), or contract demand charges (local/dedicated infrastructure).[^standby][^standby_review] The reform shifted costs away from contract demand charges (which standby customers found unmanageable and which reflected sunk embedded costs) toward as-used demand charges that more closely approximate marginal cost signals.[^aeu_standby]

RG&E explicitly employs the **marginal cost-of-service method** for its standby rates, while other utilities use embedded cost approaches modified by the decision tree. Con Edison's [Rider Q standby rate](https://www.nyserda.ny.gov/-/media/Project/Nyserda/Files/Programs/Energy-Storage/Rider-Q.pdf) uses **location-based as-used daily demand delivery charges** informed by granular MCOS data from the Brattle Group study, with different rates depending on the customer's specific network area.[^rider_q] The Rider Q peak demand window was narrowed to **four hours** (from 10–14 hours in conventional standby tariffs) to better reflect the temporal pattern of marginal distribution costs. [Intervenors in the proceeding](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B48C91555-E862-467F-A34F-42B9F7136F0D%7D) argued that it is appropriate to base standby rates much more significantly on marginal, rather than sunk costs — analogizing to wholesale NYISO prices that are based entirely on marginal costs.[^standby_intervenors]

As-used demand charges are the standby component most closely aligned with marginal cost. They reflect the actual peak demand a customer places on shared infrastructure during specific time periods. Contract demand charges, by contrast, recover local customer-dedicated infrastructure costs aligned more with embedded cost allocation. The 2022 reform increased the as-used share and reduced the contract-demand share, improving alignment with marginal cost principles and giving DER customers the ability to reduce charges through behavioral responses and distributed generation.[^aeu_standby][^nyserda_storage_guide]

## The August 2024 MCOS Order and where NY is heading

The Commission's [August 2024 Order Addressing Marginal Cost of Service Studies](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B80570F9B-0000-CF28-A6B7-644C6FA478D9%7D) (Cases 15-E-0751 & 19-E-0283) standardized MCOS methodology across all six Joint Utilities, establishing that system-wide MCOS should reflect **long-run, non-zero marginal costs** regardless of whether specific distribution segments have no planned capital projects.[^mcos_order] The order endorsed the pre-2018 NERA methodology and rejected the Brattle Group's modification that included zero marginal costs for areas without near-term investment needs. All Joint Utilities filed [compliant MCOS studies](https://dps.ny.gov/event/marginal-cost-service-comments-due-regarding-utilities-marginal-cost-service-studies) by June 30, 2025.[^mcos_comments]

The [December 2025 DPS Staff Proposal on DRV/LSRV](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B01C6CA38-5FE7-45DF-9946-E31843B8C93B%7D), building directly on the MCOS Order, articulated the clearest economic framework for marginal cost in NY regulation: efficient rate design practices should be used to set prices which result in quantities consistent with the level of demand at which demand intersects long-run marginal costs.[^drv_staff_proposal] Staff proposed DRV values ranging from **$21.64/kW-year** (Central Hudson) to **$252.54/kW-year** (Con Edison), with LSRV supplements for the roughly 10% of substation areas with costs exceeding 1.645 standard deviations above the system mean.

The [E3 Full Value Tariff study](https://www.ethree.com/wp-content/uploads/2016/12/Full-Value-Tariff-Design-and-Retail-Rate-Choices.pdf), prepared for NYSERDA during REV, represents the theoretical endpoint of NY's trajectory.[^e3_fvt] It defines a "fundamental economic rate" with three components: dynamic marginal-cost prices (hourly LBMPs + losses + capacity), a network subscription charge ($/kW-month, location-varying) to recover residual embedded costs, and a customer charge. E3 proposes recovering the residual through the size-based subscription charge rather than volumetric adders, preserving marginal cost price signals. This framework has not been adopted for general retail rates, but its architecture informs the ongoing expansion of optional demand-based and TOU rates for all customer classes, approved in the [October 2023 order](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B202DF28E-0000-CF13-8DDF-8A223502E3B8%7D) (Case 15-E-0751).[^oct_2023_order]

## Conclusion

New York's approach to cost allocation and rate design is best understood as a **pragmatic hybrid** that preserves the Kahn-era commitment to marginal cost principles while grounding both revenue allocation and charge-setting in the practical advantages of embedded cost analysis. The ECOS provides the fairness benchmark for how much each class pays in total; the MCOS provides the efficiency signals for how rates are structured within classes and how DERs are compensated. This dual framework is uniform across all six major NY investor-owned utilities, consistently applied in rate cases from 2020 through 2026, and firmly reinforced by the Commission's ongoing [Case 19-E-0283](https://dps.ny.gov/event/cost-service-psc-seeks-comment-whitepaper-filed-department-public-service-staff-makes-various) proceeding.

At the rate design level, marginal costs play an advisory rather than determinative role for standard service classifications. The highest-impact dollar-for-dollar applications of MCOS today are VDER compensation (DRV/LSRV), economic development rate floors, standby rate calibration, and energy-efficiency cost-effectiveness screening. For standard residential and commercial classes, MCOS data influences the _direction_ of rate design changes — whether demand charges should rise relative to energy charges, whether TOU windows should narrow, whether seasonal differentials should steepen — but does not mathematically determine charge levels through a formal reconciliation process.

The gap between Opinion 76-15's aspiration ("utilized to the greatest extent practicable") and current practice is substantial but narrowing. The standardization of MCOS methodology, the expansion of optional TOU and demand-based rates, the granular Brattle/Con Edison MCOS studies at the network level, the December 2025 Staff Proposal's economic framework, and the E3 Full Value Tariff architecture all point toward a future where marginal costs play a larger structural role in NY rate design. The outstanding question is whether NY will develop a formal mechanism — analogous to California's EPMC reconciliation or the E3 subscription-charge model — for translating MCOS results into binding rate-structure requirements within classes, or whether the advisory/directional model will persist.

For practitioners accustomed to California's EPMC approach, the key insight is that **NY never fully implemented marginal cost-based inter-class allocation despite pioneering the theory**, and that even at the intra-class rate design level, marginal costs remain a guidepost rather than a formula. The practical effectiveness of the ECOS tolerance-band approach, combined with NY's reliance on negotiated Joint Proposals, means that gradualism and political feasibility continue to shape rates at least as much as economic efficiency.

---

## References

[^synapse]: Synapse Energy Economics, [_Embedded versus Marginal Cost of Service_](https://www.synapse-energy.com/sites/default/files/Ratemaking-Fundamentals-FactSheet.pdf) (Ratemaking Fundamentals Fact Sheet).

[^lbl]: Lawrence Berkeley National Laboratory, [_Emerging Trends in Utility Cost Allocation_](https://eta-publications.lbl.gov/sites/default/files/boff_utility_cost_allocation_20220519.pdf) (May 17, 2022).

[^coned_cos]: Consolidated Edison Company of New York, [Direct Testimony of the Cost of Service Panel](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7BA2535CDD-CC9A-4CB9-9245-C9DCCAF06B85%7D), Case 22-E-0064.

[^cenhud_cos]: Central Hudson Gas & Electric Corporation, [Cost of Service Panel Direct Testimony](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B85559E72-0940-406F-B562-0EF0D2DCDCE1%7D), Case 24-E-0461.

[^coned_dac]: Consolidated Edison, [DAC Panel Update and Rebuttal Exhibits](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7BAF21138E-EA67-4890-9F6A-E6C225FD6268%7D), Case 25-E-0072.

[^coned_rebuttal]: Consolidated Edison, [Electric Rate Panel Rebuttal Testimony](https://investor.conedison.com/static-files/1a1ed488-d2af-4e3b-987f-031111a733b2), Case 07-E-0523.

[^coned_brattle_mcos]: The Brattle Group (Philip Q. Hanser), [Marginal Cost of Service Study for Consolidated Edison](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B21D11BD3-4538-45BA-B6DE-0B144B9AA5B4%7D), Case 22-E-0064.

[^or_order]: NY PSC, [Order Adopting Joint Proposal](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B154D555D-D3E1-4048-82AB-C14E82D86BD2%7D), Case 24-E-0060 (Orange & Rockland), March 20, 2025.

[^nyseg_jp]: NY PSC, [Order Adopting Joint Proposal](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B6BAA243C-F0B5-48EB-AB77-A912ECA911CF%7D), Cases 22-E-0317/22-E-0319 (NYSEG/RG&E), October 12, 2023.

[^nyseg_serp]: DPS Staff Electric Rates Panel, [Direct Testimony](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B90C1109C-0000-CA56-8028-C926039990E0%7D), Cases 25-E-0375 et al. (NYSEG/RG&E), 2025.

[^natgrid_order]: NY PSC, [Order Adopting Joint Proposal](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B90E66D96-0000-CA12-A897-0A5E5C2B9F2B%7D), Case 24-E-0322 (National Grid), August 14, 2025.

[^natgrid_news]: NY DPS, ["PSC Dramatically Reduces National Grid's Rate Request"](https://dps.ny.gov/news/psc-dramatically-reduces-national-grids-rate-request).

[^cenhud_news]: NY DPS, ["PSC Cuts Central Hudson's Rate Request"](https://dps.ny.gov/news/psc-cuts-central-hudsons-rate-request).

[^coned_order_22]: NY PSC, [Order Adopting Joint Proposal](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B0043B495-0000-C413-BF18-0E262AD26F75%7D), Case 22-E-0064 (Con Edison), July 20, 2023.

[^coned_summary]: NY DPS, [Con Edison Electric Rate Case Summary](https://dps.ny.gov/system/files/documents/2026/01/con-edison-electric-rate-case-summary.pdf) (January 2026).

[^coned_news]: NY DPS, ["PSC Dramatically Reduces Con Edison's Rate Request by Nearly 90 Percent"](https://dps.ny.gov/news/psc-dramatically-reduces-con-edisons-rate-request-nearly-90-percent).

[^nyseg_news]: See WSKG, ["Public Service Commission approves 3-year rate increase for NYSEG, RG&E customers"](https://www.wskg.org/news/2023-10-13/public-service-commission-approves-3-year-rate-increase-for-nyseg-rg-e-customers) (Oct. 13, 2023); The Ithaca Voice, ["PSC approves double-digit rate increases for NYSEG"](https://ithacavoice.org/2023/10/psc-approves-double-digit-rate-increases-for-nyseg/).

[^dps_cases]: NY DPS, [Pending and Recent Electric Rate Cases](https://dps.ny.gov/pending-and-recent-electric-rate-cases).

[^mcos_order]: NY DPS, [Order Addressing Marginal Cost of Service Studies](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B80570F9B-0000-CF28-A6B7-644C6FA478D9%7D), Case 19-E-0283, August 19, 2024.

[^mcos_whitepaper]: NY DPS, ["Cost of Service: PSC seeks comment on whitepaper filed by DPS staff..."](https://dps.ny.gov/event/cost-service-psc-seeks-comment-whitepaper-filed-department-public-service-staff-makes-various), Case 19-E-0283.

[^mcos_techconf]: NY DPS, ["Cost of Service: Tech conference regarding the marginal cost of service studies"](https://dps.ny.gov/event/cost-service-tech-conference-regarding-marginal-cost-service-studies), Case 19-E-0283.

[^mcos_comments]: NY DPS, ["Marginal Cost of Service: Comments due regarding utilities' marginal cost of service studies"](https://dps.ny.gov/event/marginal-cost-service-comments-due-regarding-utilities-marginal-cost-service-studies), Case 19-E-0283.

[^cpuc]: California PUC, [D.07-09-004: Interim Opinion Adopting Settlements on Marginal Cost, Revenue Allocation, and Rate Design](https://docs.cpuc.ca.gov/published/Final_decision/72471-04.htm).

[^rap]: Regulatory Assistance Project, ["Modern Marginal Cost of Service Studies"](https://www.raponline.org/blog/modern-marginal-cost-of-service-studies/).

[^standby]: Utility Dive, ["New York adjusts standby, buyback rate methodologies"](https://www.utilitydive.com/news/new-york-adjusts-standby-buyback-rate-methodologies-sweetening-value-prop/620919/) (March 2022).

[^standby_review]: NY PSC, [Standby Rates Review](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B92896AC8-6E77-4901-8913-661C9F3BD5F4%7D), Case 15-E-0751.

[^standby_intervenors]: Columbia University Medical Center / Mount Sinai Health System, [Comments on Standby Rate Reform](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B48C91555-E862-467F-A34F-42B9F7136F0D%7D), Case 15-E-0751.

[^aeu]: Advanced Energy United, ["A Cost-Allocation Win in New York for Distributed Energy Resources"](https://blog.advancedenergyunited.org/a-cost-allocation-win-in-new-york-for-distributed-energy-resources).

[^aeu_standby]: Advanced Energy United, ["New York PSC order could reduce costs, increase value of distributed energy resources"](https://blog.advancedenergyunited.org/articles/new-york-psc-order-could-reduce-costs-increase-value-of-distributed-energy-resources).

[^nucor_order]: NY PSC, [Order Establishing Rates for Nucor Steel](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B90062397-0000-CB14-9FBC-12776BAE1F11%7D), Case 01-E-0680, March 2002.

[^e3_fvt]: E3 (Energy+Environmental Economics), [_Full Value Tariff Design and Retail Rate Choices_](https://www.ethree.com/wp-content/uploads/2016/12/Full-Value-Tariff-Design-and-Retail-Rate-Choices.pdf), prepared for NYSERDA.

[^oct_2023_order]: NY PSC, [Order on Rate Design and TOU Expansion](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B202DF28E-0000-CF13-8DDF-8A223502E3B8%7D), Case 15-E-0751, October 2023.

[^drv_staff_proposal]: DPS Staff, [Proposal on DRV/LSRV Methodology](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B01C6CA38-5FE7-45DF-9946-E31843B8C93B%7D), Cases 15-E-0751 & 19-E-0283, December 2025.

[^rider_q]: NYSERDA / NY-BEST, [Standby Rate + Con Ed Rider Q Fact Sheet](https://www.nyserda.ny.gov/-/media/Project/Nyserda/Files/Programs/Energy-Storage/Rider-Q.pdf).

[^nyserda_storage_guide]: NYSERDA, [Energy Storage Customer Electric Rates Reference Guide](https://www.nyserda.ny.gov/-/media/Project/Nyserda/Files/Programs/Energy-Storage/energy-storage-customer-electric-rates-reference-guide.pdf).

[^synapse_fix]: Synapse Energy Economics, [_Caught in a Fix_](https://www.synapse-energy.com/sites/default/files/Caught-in-a-Fix.pdf) (analysis of fixed charges and rate design).

[^naruc]: NARUC, [Ratemaking Fundamentals and Principles](https://www.naruc.org/commissioners-desk-reference-manual/3-ratemaking-fundamentals-and-principles/) (Commissioner's Desk Reference).

[^atrium]: Atrium Economics, ["Cost of Service Studies – Part I"](https://atriumecon.com/our-approach/cost-of-service-studies-part-i/).

[^atrium_rates]: Atrium Economics, ["Principles of Sound Rate Design"](https://atriumecon.com/our-approach/sound-rate-design/).

[^value_stack_order]: NY PSC, [Updated Value Stack Order](https://www.nyserda.ny.gov/-/media/Project/Nyserda/Files/Programs/NY-Sun/Updated-Value-Stack-Order-2019-04-18.pdf), April 2019.

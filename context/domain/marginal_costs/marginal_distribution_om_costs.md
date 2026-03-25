# Distribution O&M cost causation: does incremental load create marginal distribution O&M costs?

Whether incremental electricity consumption — specifically winter heat pump load on a summer-peaking distribution grid — creates marginal distribution O&M costs. The short answer: almost certainly not. Distribution O&M is driven by time, weather, geography, and regulatory schedules, not by kWh or kW. The one theoretical mechanism (transformer thermal degradation from loading) is negligible when transformers operate well below their thermal limits with cool ambient temperatures.

This matters for the BAT framework because Lance Schaefer (Concentric, advising RIE) challenged whether winter usage truly imposes zero marginal O&M and depreciation costs. The question deserves a rigorous answer.

---

## What distribution O&M consists of

Distribution O&M encompasses the following major cost categories. FERC Uniform System of Accounts tracks these under accounts 580–598 (distribution expenses).

| Activity                                                      | What drives the cost                                                                                                 |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Vegetation management**                                     | Tree growth rates, species, proximity to lines, geography, weather. Largest single O&M line item for many utilities. |
| **Inspection & patrol**                                       | Regulatory mandates, time-based or condition-based schedules (typically 8–12 year cycles for poles).                 |
| **Equipment maintenance** (transformers, switches, reclosers) | Time/age-based degradation, condition monitoring. Industry moving from time-based to condition-based maintenance.    |
| **Storm restoration / emergency repair**                      | Weather events.                                                                                                      |
| **Underground cable maintenance**                             | Age, soil conditions, moisture ingress.                                                                              |

**Sources:**

- Hydro Ottawa, Exhibit 4: Operating Expenses (2020 and 2025 filings to Ontario Energy Board) — detailed breakdown of distribution O&M activities by category. Available at `hydroottawa.com/sites/default/files/2020-02/Exhibit%204%20-%20OPERATING%20EXPENSES.pdf` and `static.hydroottawa.com/documents/corporate/regulatory-affairs/2025/HOL_Ex%204_Operating%20Expenses_20250415.pdf`
- FERC 18 CFR Part 367, Subpart K — Operation and Maintenance Expense Chart of Accounts. `ecfr.gov/current/title-18/chapter-I/subchapter-U/part-367/subpart-K`
- NARUC Electric Utility Cost Allocation Manual (1992), classification of costs as customer-related, demand-related, and energy-related. Available via `pubs.naruc.org/pub/538103FD-2354-D714-5105-6C8824D830B9`

---

## The RAP manual on distribution maintenance costs

The Regulatory Assistance Project's _Electric Cost Allocation for a New Era_ (Lazar, Chernick, Marcus & LeBel, January 2020) is the most authoritative modern treatment of electric cost allocation. Its Table 4 (p. 60) classifies cost drivers for each distribution component:

| Component               | Investment-related costs | Maintenance costs |
| ----------------------- | ------------------------ | ----------------- |
| Substations             | High                     | **Low**           |
| Primary circuits        | High                     | **Low**           |
| Line transformers       | Medium                   | **Low**           |
| Secondary service lines | Medium                   | **Low**           |
| Meters (traditional)    | Low                      | **Low**           |
| Meters (advanced)       | Medium                   | **Low**           |

Maintenance costs are rated **Low** for every distribution component, in contrast to investment-related costs. Distribution O&M is a secondary cost driver overall, and what there is of it is not primarily driven by load.

The manual also notes (p. 59): "Substations and line transformers must be larger — or will wear out more rapidly — if they experience many high-load hours in the year and if daily load factors are high. Underground and overhead feeders are also subject to the effects of heat buildup from long hours of relatively high use." This is the capacity/investment cost causation — it's about sizing equipment for peaks, not about ongoing O&M.

Separately, the manual challenges the traditional NARUC classification of all distribution costs as either "demand-related," "energy-related," or "customer-related," noting (p. 38) that these "were always simplifications." Modern cost allocation should examine what actually causes each cost component, not rely on conventional buckets.

**Source:** Lazar, Chernick, Marcus & LeBel, _Electric Cost Allocation for a New Era: A Manual_, Regulatory Assistance Project, January 2020. Full PDF: `raponline.org/wp-content/uploads/2023/09/rap-lazar-chernick-marcus-lebel-electric-cost-allocation-new-era-2020-january.pdf`. Table 4 on p. 60; transformer wear discussion on pp. 59–60; classification critique on p. 38.

---

## Why distribution O&M is almost entirely not load-caused

### Vegetation management

Vegetation management is at or near the top of distribution O&M spending for most North American utilities. California's three major IOUs alone spend over $250 million annually on vegetation management for distribution lines, with PG&E estimating expenditures approaching $1 billion. Vegetation-related impacts cause more than 20% of U.S. power outages.

The cost is driven by tree growth rates, species composition, right-of-way proximity, geography, and storm exposure. It has **zero causal relationship to load** — trees do not grow faster because more electricity flows through nearby lines. Maintenance scheduling is based on vegetation growth cycles, compliance with Minimum Vegetation Clearance Distance (MVCD) standards, and storm hardening priorities.

**Sources:**

- U.S. Department of Energy, "Vegetation Management" (November 2024). `energy.gov/sites/default/files/2024-11/111524_Vegetation_Management.pdf`
- UVM/TD World survey: "More Than 20% of Outages Attributed to Vegetation" (2019). `tdworld.com/vegetation-management/article/21169246`
- ATK Energy Group, "Vegetation Management: A Strategic Priority for Utility Reliability" — cites FPL's $77M/year vegetation management budget for 16,400 miles of distribution lines. `atkenergygroup.com/casestudy/strategic-utility-vegetation-management-roi/`
- Scielo, "Optimal management of vegetation maintenance and the associated costs of its implementation in overhead power distribution systems" (2019). `scielo.org.co/scielo.php?pid=S0123-77992019000200093&script=sci_arttext`

### Inspection and patrol

Utilities inspect distribution equipment on time-based or condition-based schedules. The industry is moving from periodic (fixed-cycle) to condition-based maintenance, where intervention is triggered by detected degradation rather than elapsed time. Neither approach is load-based.

IEEE research notes that many routine inspections on properly functioning equipment simply confirm excellent condition. With equipment lifespans of 40–60 years, the maintenance trigger is time elapsed or condition changes, not kWh delivered or kW demanded.

Condition-based maintenance can reduce O&M expenditures by up to 50% compared to time-based approaches, while improving reliability — further evidence that the cost driver is scheduling policy, not load.

**Sources:**

- MDPI Energies, "Dynamic Inspection Interval Determination for Efficient Distribution Grid Asset-Management" (2020). `mdpi.com/1996-1073/13/15/3875`
- INMR, "Asset Replacement Strategies in Ageing Grids: Periodic vs. Condition-Based Maintenance." `inmr.com/asset-replacement-strategies-in-ageing-grids-periodic-vs-condition-based-maintenance/`
- Systems with Intelligence, "How Utilities can Transition from Scheduled Maintenance to a Condition-Based Maintenance Strategy." `systemswithintelligence.com/blog/how-utilities-can-transition-from-scheduled-maintenance-to-a-condition-based-maintenance-strategy`
- IEEE Power & Energy Magazine (March/April 2023), open article on inspection intervals and asset management.

### Storm restoration and emergency repair

Storm damage restoration is driven by weather events — wind, ice, flooding, lightning — not by customer load levels. This is self-evident but worth stating: a nor'easter doesn't cause more pole failures because heat pumps were running during the storm.

### Underground cable maintenance

Cable maintenance is driven by age, soil conditions, moisture ingress, and thermal cycling from ambient temperature changes. While extreme sustained overloading can accelerate cable insulation degradation, this requires loading well above rated capacity — not the below-rating winter operation relevant to heat pump loads on a summer-peaking system.

### Equipment failure statistics

A study of oil-based distribution transformers in Tanzania's electrical distribution networks found the leading failure causes were cellulose deterioration (33.2% of faults), arcing faults (26.2%), and tank/oil contamination — driven by overload, aging, and moisture content. IEEE literature on historical transformer failure and maintenance data confirms that age, maintenance era, and maintenance quality are the primary determinants of failure rates, with load being one factor among many.

Critically, the _age_ and _condition_ of the equipment dominate the failure probability. Even when overloading is cited, it refers to sustained loading above nameplate rating — not the incremental load from heat pumps on a grid with massive winter headroom.

**Sources:**

- Springer, "Transformer faults in tanzanian electrical distribution networks: indicators, types, and causes," Journal of Electrical Systems and Information Technology (2023). `link.springer.com/article/10.1186/s43067-023-00103-3`
- IEEE Xplore, "Analysis of Historical Transformer Failure and Maintenance Data: Effects of Era, Age, and Maintenance on Component Failure Rates" (2019). `ieeexplore.ieee.org/document/8809732`
- MDPI Energies, "Failure Diagnosis and Root-Cause Analysis of In-Service and Defective Distribution Transformers" (2021). `mdpi.com/1996-1073/14/16/4997`

---

## The one load-related mechanism: transformer thermal degradation

The one theoretical mechanism connecting load to distribution O&M is transformer thermal degradation. Transformer insulation life is governed by winding hottest-spot temperature (HST) per IEEE Standard C57.91. Higher loading raises winding temperature, which accelerates insulation aging and shortens transformer life — eventually requiring replacement (an O&M/capital cost).

Recent academic work has formalized this as a "transformer degradation marginal cost" component within Distribution Locational Marginal Costs (DLMCs). The framework captures how incremental loading in a given period affects loss-of-life not only during that period but also in subsequent periods, creating a forward-looking marginal aging cost.

### Why this mechanism is negligible for winter HP load in Rhode Island

Three factors make this mechanism essentially irrelevant:

**1. It's driven by peak loading and overload duration, not total energy.** A transformer loaded to 120% for 4 hours in summer degrades far more than one loaded to 60% for 24 hours in winter, even though the winter day delivers more total energy. The relationship between load and insulation aging is exponential — below the thermal knee, marginal degradation per additional kW is negligible. Transformers can handle moderate overloads for short periods but deteriorate quickly under extended overload conditions (RAP manual, pp. 59–60).

**2. Winter ambient temperatures provide additional thermal headroom.** Winding temperature = f(loading, ambient temperature). Cooler winter temperatures mean the same load level produces a lower winding temperature. Manitoba Hydro's operating policy illustrates: normal permissible loading is 100% of nameplate for both seasons, but contingency loading permits **125% in winter** (assuming ≤0°C) versus only **100% in summer** (assuming ≤30°C). The thermal margin is substantial.

**3. RI's distribution transformers have massive winter headroom.** RIE's own data shows: zero feeders are currently winter-constrained. 98% of feeders have >30% winter headroom. Median winter headroom is 56%. Even by 2039 with 19–22% HP penetration, zero feeders are expected to become winter-constrained. Winter HP loads operate deep in the flat part of the degradation curve, where marginal thermal stress per additional kW is essentially zero.

**Sources:**

- Andrianesis, Wang & Caramanis, "Distribution Network Marginal Costs: Enhanced AC OPF Including Transformer Degradation" (2018), arXiv:1811.09001. `arxiv.org/pdf/1906.01570` — develops the DLMC framework with transformer degradation as a marginal cost component.
- Duarte, Gorenstein Dedecca & Lumbreras, "Impact of transformer and cable aging on location marginal cost in active distribution networks" (2021), CIRED conference. `hal.science/hal-03407221`
- IEEE Standard C57.91-2011, "Guide for Loading Mineral-Oil-Immersed Transformers and Step-Voltage Regulators" — the foundational standard for transformer loading limits and loss-of-life calculations.
- Manitoba Hydro, "Power Transformer Life Cycle Cost Reduction," 97 MIPSYCON proceedings — documents 125% winter vs 100% summer contingency loading policy. `idc-online.com/technical_references/pdfs/electrical_engineering/POWER_TRANSFORMER_LIFE-CYCLE_COST_REDUCTION.pdf`
- IEEE Xplore, "Specifying transformer winter and summer peak-load limits" (1959). `ieeexplore.ieee.org/document/1375093`
- RIE feeder data and peak growth forecasts — see `context/sources/rie_2024_peak_forecast_2025-2039.md` in this repo.

---

## The PG&E pipeline replacement analogy

In a CPUC gas rate proceeding, PG&E argued that its pipeline replacement program "is related to facility deterioration, not throughput." The commission agreed, finding that "once a utility makes an investment in new facilities to serve increasing customer demand, the utility will repair or replace those facilities without regard for incremental increases in demand." The replacement cost adder was eliminated from the marginal cost calculation on this basis.

The same logic applies to most distribution O&M: once equipment is installed, maintenance is driven by its physical condition (age, weather exposure, environmental factors), not by how much electricity flows through it. Incremental load does not cause incremental vegetation management, pole inspection, or cable maintenance.

**Source:** CPUC Decision D.05-06-029, Section 8 (Calculation of Marginal Costs), PG&E gas rate proceeding. `docs.cpuc.ca.gov/published/Final_decision/47182-08.htm`

---

## Traditional CCOS classification vs. reality

In traditional embedded cost-of-service studies (ECOSS), the NARUC three-step process (functionalize → classify → allocate) puts some distribution O&M into the "demand-related" bucket. This is a conventional accounting classification, not a causal analysis.

The Idaho PUC's Class Cost of Service Process Guide describes the framework: demand-related costs are "investments in generation, transmission, and a portion of distribution plant, along with associated operation and maintenance expenses necessary to accommodate maximum demand imposed on the utility's system." The key phrase is "associated operation and maintenance expenses" — O&M that is classified as demand-related because it is associated with demand-related plant, not because load incrementally causes the O&M.

RAP and Lazar argue this conflation deserves correction. Their framework recommends analyzing each distribution segment individually based on its actual cost drivers, rather than inheriting the demand-related classification from the capital it is associated with.

**Sources:**

- Idaho PUC, "Class COS Process Guide," Appendix 7.3 to Idaho Power Company ELEC/IPC/IPCE2222 (2022). `puc.idaho.gov/Fileroom/PublicFiles/ELEC/IPC/IPCE2222/CaseFiles/20221026Appendix%207.3%20Class%20COS%20Process%20Guide.pdf`
- NARUC Electric Utility Cost Allocation Manual (January 1992). `pubs.naruc.org/pub/538103FD-2354-D714-5105-6C8824D830B9`
- Lazar et al., _Electric Cost Allocation for a New Era_ (2020), pp. 38, 60 (cited above).

---

## Summary

| O&M category                    | Load-caused?                                                    | What actually drives it                                                                                         |
| ------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Vegetation management           | No                                                              | Tree growth, species, geography, weather, regulatory clearance standards                                        |
| Inspection & patrol             | No                                                              | Time-based or condition-based schedules, regulatory mandates                                                    |
| Equipment maintenance           | Negligible in practice                                          | Age, condition monitoring; thermal degradation theory exists but requires sustained overloading above rating    |
| Storm restoration               | No                                                              | Weather events                                                                                                  |
| Underground cable               | No                                                              | Age, soil, moisture                                                                                             |
| Transformer thermal degradation | Theoretically yes, practically no for below-rating winter loads | Peak loading magnitude and duration, ambient temperature; negligible when headroom is large and ambient is cold |

**The bottom line:** Capacity is the only distribution cost with meaningful load-causation, and in RI it is near zero in winter. Distribution O&M is driven by time, weather, and geography — not by kWh or kW. Conceding that winter consumption causes marginal distribution O&M is an overconcession inherited from CCOS convention, not supported by cost-causation analysis.

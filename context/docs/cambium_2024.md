# Cambium 2024 Scenario Descriptions and Documentation

**Source**: cambium_2024.pdf\
**Pages**: 77 total pages\
**Date**: April 2025\
**Author(s)**: Pieter Gagnon, Pedro Andres Sanchez Perez, Julian Florez, James Morris, Marck Llerena Velasquez, and Jordan Eisenman\
**Author affiliations**: National Renewable Energy Laboratory, 15013 Denver West Parkway, Golden, CO 80401. Suggested citation: Gagnon, Pieter, Pedro Andres Sanchez Perez, Julian Florez, James Morris, Marck Llerena Velasquez, and Jordan Eisenman. 2025. _Cambium 2024 Scenario Descriptions and Documentation_. Golden, CO: National Renewable Energy Laboratory. NREL/TP-6A40-93005. https://www.nrel.gov/docs/fy25osti/93005.pdf.

---

## List of Abbreviations and Acronyms

| Abbreviation | Meaning                                    |
| ------------ | ------------------------------------------ |
| AEO          | Annual Energy Outlook                      |
| ATB          | Annual Technology Baseline                 |
| BA           | balancing area                             |
| CAGR         | compound annual growth rate                |
| CARB         | California Air Resources Board             |
| CCS          | carbon capture and storage                 |
| CES          | clean energy standard(s)                   |
| CO2e         | carbon dioxide equivalent                  |
| CONE         | cost of new entry                          |
| CSP          | concentrating solar power                  |
| dGen         | Distributed Generation Market Demand Model |
| EER          | Evolved Energy Research                    |
| EIA          | U.S. Energy Information Administration     |
| EPA          | U.S. Environmental Protection Agency       |
| g            | gram                                       |
| GEA          | generation and emission assessment         |
| GW           | gigawatt                                   |
| IPCC         | Intergovernmental Panel on Climate Change  |
| IRA          | Inflation Reduction Act                    |
| kg           | kilogram                                   |
| LRMER        | long-run marginal emission rate            |
| MW           | megawatt                                   |
| MWh          | megawatt-hour                              |
| NGCT         | natural gas combustion turbine             |
| NREL         | National Renewable Energy Laboratory       |
| PRAS         | Probabilistic Resource Assessment Suite    |
| PRM          | planning reserve margin                    |
| PV           | photovoltaics                              |
| ReEDS        | Regional Energy Deployment System          |
| RPS          | renewable portfolio standard(s)            |
| SRMC         | short-run marginal costs                   |
| SRMER        | short-run marginal emission rate           |
| TWh          | terawatt-hour                              |
| USLCI        | U.S. Life Cycle Inventory Database         |

---

## Table of Contents

1. Cambium Overview
   - 1.1 ReEDS
   - 1.2 PLEXOS
2. Changes to Scenarios, Metrics, and Methods Relative to Cambium 2023
3. 2024 Cambium Scenario Definitions
   - 3.1 Cambium Input Assumptions
4. User Guidance, Caveats, and Limitations of Cambium Databases
   - 4.1 Limitations and Caveats
   - 4.2 Comparing Cambium Projections to Historical Emissions Data
5. Cambium Metric Definitions
   - 5.1 Busbar and End-Use Values
   - 5.2 Time and Geographic Identifiers
   - 5.3 Generation and Capacity Metrics
   - 5.4 Emission Metrics
   - 5.5 Cost Metrics
   - 5.6 Interregional Transmission Metrics
   - 5.7 Load Metrics
   - 5.8 Operational Metrics
   - 5.9 Policy Metrics
6. Cambium Methods
   - 6.1 Technologies Represented in Cambium
   - 6.2 Emissions Factors by Fuel
   - 6.3 Coloring Power Flows
   - 6.4 Calculating Long-Run Marginal Emission Rates
   - 6.5 Identifying a Region's Short-Run Marginal Generator
   - 6.6 Identifying the Energy Source When an Energy-Constrained Generator Is on the Short-Run Margin
   - 6.7 Calculating Time-Varying Distribution Loss Rates
   - 6.8 Calculating Hourly Marginal Capacity Costs
   - 6.9 Calculating Marginal Portfolio Costs
7. Acknowledgments
8. References

---

## 1 Cambium Overview

The National Renewable Energy Laboratory's (NREL's) Cambium datasets are annually released sets of simulated hourly emission, cost, and operational data for a range of modeled futures of the U.S. electric sector with metrics designed to be useful for long-term forward-looking decision-making. The 2024 Cambium dataset is the fifth annual release. The datasets are a companion product to NREL's Standard Scenarios, which are likewise released annually and are a set of projections of how the U.S. electric sector could evolve across a suite of different potential futures, but covering more scenarios with less temporal granularity (Gagnon et al. 2024). Information about Cambium and related publications can be found at https://www.nrel.gov/analysis/cambium.html, and the Cambium datasets can be viewed and downloaded at https://scenarioviewer.nrel.gov/.

In this documentation, we describe Cambium 2024's scenarios (section 3), define the metrics (section 5), and document the Cambium-specific methods for calculating those metrics (section 6).

The Cambium datasets draw primarily from the outputs of two models:

- **The Regional Energy Deployment System (ReEDS™) model**, which uses a least-cost framework to project structural changes in the U.S. electric sector under different possible futures (Ho et al. 2021)
- **PLEXOS**, which is a commercial production cost model that we use to simulate the hourly operation of the future electric systems projected by ReEDS (Energy Exemplar 2019).

### 1.1 ReEDS

The first of two models underlying the Cambium datasets is ReEDS (Ho et al. 2021). ReEDS is an NREL-developed, publicly available mathematical programming model of the electric power sector. Given a set of input assumptions such as fuel costs, technology costs, and policies, ReEDS models the evolution of generation and transmission assets, solving a linear program to make investment and operational decisions to minimize the overall cost of the electric system. The model has been used to explore how the evolution of the electric sector is impacted by a range of technology and policy scenarios.

The conterminous United States (i.e., the lower 48 states and the District of Columbia) is represented in ReEDS as 133 model balancing areas (BAs), which are connected by a representation of the transmission network. The network starts with existing transmission capacity and can be expanded as part of ReEDS' decision space. Likewise, the model starts with representations of all existing generation capacity and announced future builds for each BA, and it can choose to build new capacity or retire old capacity to meet demand at the lowest cost. Historical patterns are used as a starting point for assumptions about end-use electricity demands, and assumptions about the evolution of that demand vary by scenario.

The linear program for balancing supply and demand within ReEDS includes a representative set of 328 time periods that are meant to capture seasonal and diurnal trends. The Probabilistic Resource Assessment Suite (PRAS) model is called between ReEDS solve steps to assess resource adequacy with hourly resolution across 7 years of historical weather data (with iterations of a given ReEDS solution occurring if PRAS identifies a resource adequacy shortfall). The linear program that forms the core of ReEDS makes investment and retirement decisions for bulk power system assets. For behind-the-meter solar photovoltaics (PV), the model imports projections from NREL's Distributed Generation Market Demand (dGen™) model (Sigrin et al. 2016).

### 1.2 PLEXOS

The ReEDS reduced-form dispatch, aided by Augur's parameterization, aims to capture enough operational detail for realistic bulk power system investment and retirement decisions, but it does not have the temporal resolution desired for Cambium databases. To obtain more-detailed simulations of the electric systems projected by ReEDS, NREL developed utilities to represent a ReEDS capacity expansion solution in the second of the two models that Cambium draws from: PLEXOS (Energy Exemplar 2019).

PLEXOS is a commercial production cost model that can simulate least-cost hourly dispatch of a set of generators with a network of nodes and transmission lines. It incorporates representations of unit-commitment decisions, detailed operating constraints (e.g., maximum ramp rates and minimum generation levels), and operating reserves; and it can be run with nested receding horizon planning periods (e.g., day-ahead and real-time) to simulate realistic electric system operations.

For representing a ReEDS solution as a PLEXOS model, the spatial resolution from ReEDS is retained: the 133 BAs in ReEDS are represented as transmission nodes in PLEXOS, and the connections between them are modeled using the line capacities and loss rates in the ReEDS aggregated transmission representation. Generation capacity at each node is, however, converted from aggregate ReEDS capacity to individual generators using a characteristic unit size for each technology. For consistency, ReEDS cost and performance parameters are used when possible and reasonable, but values derived from previous NREL studies are used when parameters are unavailable from ReEDS or are available but unreasonable because of structural differences between the models.

Once the ReEDS solution is converted to a PLEXOS database, the hourly dispatch of the grid can be simulated for a full year. For Cambium databases, we run PLEXOS as a mixed integer program, with day-ahead unit commitment and dispatch (without any real-time adjustments for subhourly dispatch or forecast error). For each modeled year, generators have constant heat rates, short-run marginal costs (SRMC), and maximum generator outputs. Supply and demand are balanced at the busbar level, and distribution losses are captured in data pre- and post-processing, as described in section 6.7. Inter-BA transmission is represented as pipe flow with constant loss rates, with no intra-BA transmission losses. Generator outages are represented by derating capacity to an effective capacity based on annual average outage rates that vary by technology. Three operating reserves are represented—regulation, flexibility, and spinning reserves.

We draw from these simulated results—from both ReEDS and PLEXOS—to calculate the metrics reported within Cambium databases, with varying degrees of post-processing, as described in the remainder of this document.

For more detail on the underlying models, see the public repositories and documentation: [ReEDS-2.0](https://github.com/NREL/ReEDS-2.0), [ReEDS model documentation wiki](https://nrel.github.io/ReEDS-2.0/model_documentation.html), [NREL ReEDS analysis](https://www.nrel.gov/analysis/reeds/), and [NREL dGen](https://www.nrel.gov/analysis/dgen/). Model changes that apply to both the 2024 Standard Scenarios and 2024 Cambium are summarized in appendix section A.2 of the Standard Scenarios 2024 report (Gagnon et al. 2024).

---

## 2 Changes to Scenarios, Metrics, and Methods Relative to Cambium 2023

This section highlights the major differences between the 2023 and 2024 Cambium data releases, in terms of the scenarios, metrics, and methods. For a more thorough list of model development since the 2023 release, see section A.2 in the Standard Scenarios 2024 report:

- **General updating of assumptions**: The 2024 Cambium scenarios include a general update of major inputs, such as state and federal policies, technology and fuel costs, and technology performance assumptions. The new inputs are documented in section 3.1 of this report.
- **Regions p119 and p122 combined**: Prior editions of Cambium had 134 BAs (see Figure 6). As with the 2024 Standard Scenarios, this edition of Cambium has combined p119 and p122 into one region labeled z122. This aggregation was motivated by deficiencies in the manner in which electricity demand and the transmission between the two regions were being resolved.
- **Updated scenario suite**: Relative to the prior edition, two scenarios that applied a national emissions constraint have been removed from the scenario suite. Two new scenarios have been added (a Low Renewable Energy and Battery Cost With High Natural Gas Prices and a High Renewable Energy and Battery Cost With Low Natural Gas Prices). Scenarios are described in more detail in section 3.
- **Scenario suite no longer includes scenarios with nascent technologies**: Cambium only represented nascent technologies within the scenarios with national emissions constraints, and this Cambium release does not contain such scenarios. Therefore, the outputs corresponding to nascent technologies have been removed. In this report, _nascent technologies_ means generators with carbon capture and storage (CCS), hydrogen combustion turbines, enhanced geothermal technologies, floating offshore wind, and nuclear small modular reactors. Note that the designation of a technology as nascent is not intended to pass judgment on the difficulty or the likelihood of the technology ultimately achieving commercial adoption. Indeed, many of the technologies have high technology readiness levels, and some have operational demonstration plants. Nonetheless, even if a technology is technically viable, there still can be great uncertainty about its future cost and performance as well as a lack of understanding of other considerations relevant to projecting deployment, such as siting preferences and restrictions. Consequently, they are not represented within these scenarios. For readers interested in understanding the potential role of nascent technologies in future grid conditions, we refer to the 2024 Standard Scenarios (Gagnon et al. 2024).
- **Near-term deployment restrictions**: The 2022 Cambium release projected a rapid scale-up in the deployment of renewable generators driven by the tax credits in the Inflation Reduction Act (IRA). In practice, significant frictions have been observed (e.g., interconnection queue backlogs, transmission constraints, community opposition, and so forth [Wiser, Nilson, et al. 2024]) that have not historically been explicitly represented within the ReEDS model. Since prior editions, development has occurred to represent or approximate key frictions. Four key developments are: (1) the restriction of generators deployed through 2029 based on what is currently present in interconnection queues (Rand et al. 2024), (2) restrictions of generators deployed through 2026 based on U.S. Energy Information Administration (EIA) 860M planned builds and historical maximum installation rates, (3) cost penalties through 2035 that increase depending on the rate that annual deployments increase by technology, and (4) a time-varying cost adder for wind technologies to approximate the growing evidence of near-term barriers to that technology's deployment not natively represented within ReEDS. These four developments are explained in more detail in section 3.
- **Interregional friction development**: Historically, Cambium analyses have used the ReEDS model to identify system-wide least-cost solutions with perfect coordination between regions. This omits known frictions, in terms of both grid planning and operation, that exist in practice between regions. Two new developments seek to approximate such frictions. First, there is a net firm capacity import limit, as a percentage of peak load within the model's approximations of North American Electric Reliability Corporation (NERC) regions, initialized based on historical data. Second, there are transmission hurdle rates applied to energy trading between regions if they are not currently part of a common market area. Both frictions are modeled as improving over time, aligned with a current trend toward improving interregional coordination.

---

## 3 2024 Cambium Scenario Definitions

The 2024 Cambium dataset contains eight scenarios that project the possible evolution of the contiguous United States' electricity sector through 2050. The scenarios draw heavily from NREL's companion analysis product, the Standard Scenarios (Gagnon et al. 2024), which contains a broader suite of future projections (but fewer metrics and only reports annual results). Note that, while the Cambium and Standard Scenario scenarios are largely similar, there are several differences in assumptions (described below), as well as modeling differences (such as different solve years), which can make the values of specific metrics (such as the capacity deployed of a particular technology) differ between similar scenarios within the two datasets.

Scenario assumptions have been updated since the prior release to reflect the technology, market, and policy changes that have occurred in the electricity sector, and many modeling enhancements have been made. Additionally, the ReEDS and dGen models and inputs used to generate these scenarios are publicly available.

The scenarios are built around a base set of assumptions that contain central or median values for inputs such as technology costs and fuel prices, demand growth averaging 1.8% per year, and both state and federal electricity sector policies as they existed in August 2024. The eight scenarios are then created by varying renewable energy costs and performance, battery cost and performance, natural gas prices, and the rate of electricity demand growth.

**Summary of the Eight Scenarios in Cambium 2024**

1. **Mid-case**: Central estimates for inputs such as technology costs, fuel prices, and demand growth
2. **Low Renewable Energy and Battery Costs**: The same set of base assumptions as the first scenario but where renewable energy and battery costs are assumed to be lower and performance improvements greater
3. **High Renewable Energy and Battery Costs**: The same set of base assumptions as the first scenario but where renewable energy and battery costs are assumed to be higher and performance improvements lesser
4. **High Demand Growth**: The same set of base assumptions as the first scenario but where demand growth is assumed to average 2.8% from 2024 through 2050
5. **Low Natural Gas Prices**: The same set of base assumptions as the first scenario but where natural gas prices are assumed to be lower
6. **High Natural Gas Prices**: The same set of base assumptions as the first scenario but where natural gas prices are assumed to be higher
7. **Low Renewable Energy and Battery Costs With High Natural Gas Prices**: The same set of base assumptions as the first scenario but with higher natural gas prices and where renewable energy and battery costs are assumed to be lower and performance improvements greater
8. **High Renewable Energy and Battery Costs With Low Natural Gas Prices**: The same set of base assumptions as the first scenario but with lower natural gas prices and where renewable energy and battery costs are assumed to be higher and performance improvements lesser.

Although the Cambium scenario set covers a wide range of futures, it is not exhaustive. Other NREL analyses have studied particular aspects of power sector evolution in more depth than is covered in this suite of scenarios. See https://www.nrel.gov/analysis/future-system-scenarios.html for a more complete list of NREL's other future power systems analyses.

### 3.1 Cambium Input Assumptions

This section contains a high-level summary of the input assumptions that vary within the Cambium scenarios (Table 1), followed by a more-detailed discussion of the inputs.

**Table 1. Summary of Inputs That Vary Within the 2024 Cambium Scenarios**

The scenario settings listed in blue italics correspond to those used in the base set of assumptions.

| Group                                                   | Scenario Setting                                       | Notes                                                                                                                                                        |
| ------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Electricity Demand Growth                               | Reference Demand Growth                                | End-use electricity trajectory reaching 6,509 TWh/year of demand (1.8% CAGR) with conservative assumptions about the impact of demand-side provisions in IRA |
|                                                         | High Demand Growth                                     | End-use electricity trajectory reaching 8,354 TWh/year (2.8% CAGR)                                                                                           |
| Fuel Prices                                             | Reference Natural Gas Prices                           | Annual Energy Outlook 2023 (AEO2023) reference                                                                                                               |
|                                                         | Low Natural Gas Prices                                 | AEO2023 high oil and gas resource and technology                                                                                                             |
|                                                         | High Natural Gas Prices                                | AEO2023 low oil and gas resource and technology                                                                                                              |
| Electricity Generation Technology Costs and Performance | Mid Technology Cost                                    | 2024 Annual Technology Baseline (ATB) moderate projections                                                                                                   |
|                                                         | Low RE and Battery Costs and Advanced Performance      | 2024 ATB renewable energy and battery advanced projections                                                                                                   |
|                                                         | High RE and Battery Costs and Conservative Performance | 2024 ATB renewable energy and battery conservative projections                                                                                               |

_Note: Natural gas prices are based on AEO electricity sector natural gas prices but are not identical because of the application of natural gas price elasticities in the modeling. See the Fuel Prices section below for details._

For details about the structure and assumptions in the models not mentioned here, see the companion Standard Scenarios 2024 report; the documentation for ReEDS (Ho et al. 2021) and dGen (Sigrin et al. 2016); and the in-progress ReEDS documentation wiki (https://nrel.github.io/ReEDS-2.0/model_documentation.html). Both models are publicly available, and inputs are viewable within the model repositories.

#### Demand Growth and Flexibility

This year's Cambium scenario suite includes the same two end-use electricity demand trajectories that were used in Cambium 2023, both produced through modeling by Evolved Energy Research (EER). The Reference Demand Growth trajectory reaches 6,509 TWh/year of electric load by 2050 (a CAGR of 1.8% from 2024 through 2050; see Figure 1). It reflects relatively conservative assumptions about the impact of demand-side provisions in the IRA (relative, compared to other scenarios developed by EER). More information about the stock changes underlying these load profiles can be found in the 2024 Standard Scenarios report appendix. The Reference Demand Growth trajectory is used for all scenarios except the High Demand Growth scenario.

The High Demand Growth trajectory reaches 8,354 TWh/year of electric load by 2050 (a CAGR of 2.8% from 2024 to 2050). It is largely similar, but not identical, to EER's Central scenario from Haley et al. (2022). The High Demand Growth trajectory is used only in the High Demand Growth scenario.

Electricity demand is assumed to be inelastic and inflexible in all scenarios. This is a poor assumption—grid-responsive flexible loads currently exist in practice, and the increasing value of energy arbitrage in many of the futures modeled would likely induce more loads to become grid-responsive. The omission of elastic and flexible loads from this modeling would tend to create systems that are more expensive and more difficult to integrate variable generators into, relative to situations where load is elastic and flexible.

[DIAGRAM DESCRIPTION: End-use electric demand trajectories]

A line chart showing two national end-use electricity demand trajectories from a baseline year through 2050. The x-axis is time (years); the y-axis is annual electricity demand in terawatt-hours (TWh). One curve represents Reference Demand Growth, reaching 6,509 TWh/year by 2050 (1.8% CAGR). The other represents High Demand Growth, reaching 8,354 TWh/year by 2050 (2.8% CAGR). Both curves generally trend upward with slight curvature. The figure illustrates the two demand assumptions used across the eight Cambium 2024 scenarios.

[→ See original PDF page 8 for visual rendering]

#### Fuel Prices

Natural gas input price points are based on the trajectories from AEO2023 (EIA 2023). The input price points are drawn from the AEO2023 Reference scenario, the AEO2023 Low Oil and Gas Supply scenario, and the AEO2023 High Oil and Gas Supply scenario. Actual natural gas prices in ReEDS are based on the AEO scenarios, but they are not exactly the same; instead, they are price responsive to ReEDS natural gas demand in the electric sector. Each census region includes a natural gas supply curve that adjusts the natural gas input price based on both regional and national demand (Cole, Medlock III, and Jani 2016). Figure 2 shows the output natural gas prices from the suite of scenarios.

Note that the implementation of all three natural gas price trajectories shared the AEO2023 Reference scenario inputs for the 2025 model solve because the datasets are being released within 2025, and to create a more consistent starting point for the eight scenarios.

[DIAGRAM DESCRIPTION: National average natural gas price outputs from the suite of scenarios]

A line chart of national average natural gas price (e.g., $/MMBtu) over time (years through 2050). Multiple curves represent the different scenario assumptions: Reference, Low Natural Gas Prices, and High Natural Gas Prices. The curves show how natural gas prices evolve under each scenario and may diverge over the projection period.

[→ See original PDF page 9 for visual rendering]

The coal and uranium price trajectories are from the AEO2023 Reference scenario and are shown in Figure 3. Both coal and uranium prices are assumed to be fully inelastic. Coal prices vary by census region (using the AEO2023 census region projections). Uranium prices are assumed to be the same across the United States.

[DIAGRAM DESCRIPTION: Input coal and uranium fuel prices used in Cambium 2022]

A chart showing input coal and uranium fuel price trajectories over time. Uranium prices are assumed to be the same across the United States. Coal prices vary by census region and are listed in descending order of average price in the legend. The figure provides the fuel cost inputs used in the scenarios.

[→ See original PDF page 10 for visual rendering]

#### Technology Cost and Performance

Technology cost and performance assumptions for newly built generators are taken from the 2024 ATB, and performance assumptions for existing generators are drawn from EIA-NEMS data (available through the open access ReEDS repository). The ATB includes advanced, moderate, and conservative cost and performance projections through 2050 for the generating and storage technologies used in the ReEDS and dGen models (for these scenarios, RE technologies include all solar, geothermal, hydropower, and wind generators).

In the Cambium 2024 scenarios, capital cost adders are applied to wind technologies, on top of the ATB trajectories referenced above. Note that the cost adders should not be interpreted as an estimate of the actual overnight capital cost of wind generators but instead are an approximation of the magnitude of the real-world barriers and frictions that are present for wind but otherwise omitted from the ReEDS model. For scenarios with moderate renewable energy cost assumptions, wind technologies have a $200/kW (in 2023 dollars) adder to overnight capital costs through 2030, which linearly declines to $100/kW by 2040 and is constant thereafter. For scenarios with conservative renewable energy cost assumptions, the adder is $200/kW throughout the modeled horizon. For scenarios with advanced renewable energy cost assumptions, the adder is $200/kW through 2030 but linearly declines to zero by 2050.

Nuclear cost trajectories draw from the conservative case in the 2024 ATB. Unlike Cambium 2023, Cambium 2024 only represents age-based retirements of existing nuclear generators, not economic retirements.

Generator lifetimes are shown in Table 2 and Table 3. These lifetimes represent the maximum lifetimes generators are allowed to remain online in ReEDS. ReEDS will retire generators before these lifetimes if their value to the system is less than 50% of their ongoing fixed maintenance and operational costs. If a retirement date has been announced for a generator, ReEDS will retire the capacity at that date or earlier.

**Table 2. Lifetimes of Wind, Solar, Geothermal, and Hydropower Energy Generators and Batteries**

| Technology      | Lifetime (Years) | Source                                                         |
| --------------- | ---------------- | -------------------------------------------------------------- |
| Land-based wind | 30               | Wind Vision (DOE 2015)                                         |
| Offshore wind   | 30               | Wind Vision (DOE 2015)                                         |
| Solar PV        | 30               | SunShot Vision (DOE 2012)                                      |
| CSP             | 30               | SunShot Vision (DOE 2012)                                      |
| Geothermal      | 30               | GeoVision (DOE 2019)                                           |
| Hydropower      | 100              | Hydropower Vision (DOE 2016)                                   |
| Biopower        | 50               | 2021 National Energy Modeling System plant database (EIA 2021) |
| Battery         | 15               | Cole, Frazier, and Augustine (2021)                            |

**Table 3. Lifetimes of Nonrenewable Energy Generators**

| Technology                            | Lifetime for Units Less Than 100 MW (Years) | Lifetime for Units Greater Than or Equal to 100 MW (Years) |
| ------------------------------------- | ------------------------------------------- | ---------------------------------------------------------- |
| Natural gas combustion turbine (NGCT) | 50                                          | 50                                                         |
| Coal                                  | 65                                          | 75                                                         |
| Oil-gas-steam (OGS)                   | 50                                          | 75                                                         |
| Nuclear                               | 80                                          | 80                                                         |

#### Policy/Regulatory Environment

All scenarios include representations of state, regional, and federal policies as of August 2024. These include representations of IRA's main electric sector provisions, updated CAA section 111 regulations based on the rules finalized in May 2024, state portfolio standards, and regional programs such as the Regional Greenhouse Gas Initiative. Local policies (e.g., city-level) are not represented.

As with the Cambium 2023 release, in all scenarios IRA's tax credits for qualifying generation are represented as not phasing out. This year's scenario suite includes a representation of the Clean Air Act's Section 111 implementing regulations for both existing coal plants and new gas plants as finalized in May 2024. For existing coal plants, ReEDS models an emissions rate-based compliance mechanism, enforced at the state level. In 2032 and for every year thereafter, the emissions rate (tonnes CO2 per megawatt-hour) of a state's coal fleet must be less than or equal to the emissions rate of a coal-CCS plant with a 90% capture rate. Because coal-CCS is not available as an investment option in these scenarios, coal is therefore induced to retire after 2032 in these scenarios. Also starting in 2032, the investment in new gas plants (built after May 23, 2023) is evaluated based on anticipated operation below a 40% capacity factor. Note, however, that the operation of new gas plants is not restricted in the dispatch solution, due to current limitations in the representation of this policy in Cambium's production cost modeling step. Existing gas plants are not regulated per the regulations.

#### Near-Term Generator Investment

ReEDS was built primarily to explore potential futures of the U.S. electricity system. It does not contain endogenous representations of many phenomena that can influence the near-term rate of generator investment. For this analysis, ReEDS was implemented with three features: state-level growth penalties, inclusion of interconnection queue data to guide model siting decisions, and a limit on the maximum national deployment of certain technologies in the 2025 model solve.

- **Generator growth penalties** are applied through 2034, based on the annual installation rate at the state level by technology. When the annual installation rate is equal to or less than 130% of the prior maximum, no penalty is applied. When the annual installation rate is between 130% and 175% of the prior maximum, a 10% penalty is applied to the generator's capital costs. When the annual installation rate is between 175% and 200% of the prior maximum, a 50% penalty is applied. Growth rates are not allowed to exceed 200% of the prior maximum.
- **Generator interconnection queue data** are used to guide near-term model siting decisions (Rand et al. 2024). In the 2025 solve, ReEDS is restricted to generator investment that is present in the interconnection queue and has a signed interconnection agreement. In the 2028 solve, ReEDS is restricted to generator investment that is present within the queue currently, regardless of the status of the agreement.
- **National maximum investment** in wind, solar, and batteries is constrained in the 2025 solve period. The upper limit is derived by the annualized sum of capacity within the EIA 860M data that was deployed within 2024 or has a planned online year of 2024 or 2025, plus the greater of either that value or the historical maximum annual installation for the technology. That equates to 12.0 GW/year for batteries, 28.6 GW/year for PV, and 8.9 GW/year for wind.

All scenarios represent the anticipated restart of the Palisades nuclear reactor in 2025.

---

## 4 User Guidance, Caveats, and Limitations of Cambium Databases

### 4.1 Limitations and Caveats

Cambium datasets are intended primarily to support long-term, forward-looking analyses and decisions in situations where it is defensible to assume that the evolution of the electric grid will approximate the investments and operations aligned with a least-cost reliable grid that respects policy and operational constraints. Potential users of these data are encouraged to review this section closely. Key limitations and caveats include:

- **Cambium data should not be the sole basis for decisions**: Modeled projections are unavoidably imperfect, and the future is highly uncertain.
- **Cambium is designed primarily for long-term, forward-looking analysis**: It is not recommended to use Cambium data for real-time decision-making or historical accounting.
- **Relevant phenomena may not be reflected in Cambium**: Expert judgment is strongly encouraged in interpreting whether non-modeled phenomena may be present for a particular intervention.
- **Cambium's metrics are derived from system-wide, cost-minimizing optimization models**: They do not necessarily reflect the decision-making of individual actors.
- **The spatial and temporal resolution of the underlying models is coarse**: The United States is represented as 133 "copperplate" BAs. This lack of transmission losses and constraints within BAs tends to produce lower and less variable marginal costs than what is observed in practice.
- **Cambium reports marginal costs, which can differ from market prices**: Market prices in practice can deviate from marginal costs due to market design, contract structures, cost recovery for nonvariable costs, and bidding strategies.
- **Cambium's marginal costs are not estimates of retail rates**: Retail rates typically include cost recovery for administrative, distribution infrastructure, and other expenses not represented in Cambium databases.
- **The full range of uncertainty is not captured**: Each scenario does not fully reflect the uncertainties in the underlying assumptions and data.
- **ReEDS' capacity expansion decisions have limited foresight**: Unless otherwise specified, scenarios in Cambium databases are not run with intertemporal optimization.
- **Cambium's production cost modeling does not have forecast error**: Load and variable generator forecast error are not deployed in the runs from which Cambium draws.
- **Cambium databases do not contain elasticity data**: There are no estimates of how much a metric's value would change if load or generation changed.
- **Flexible loads are not currently represented in Cambium**: Grid-responsive buildings and intelligent charging of electrical vehicles are not represented.
- **The project pipeline and retirements data are likely incomplete**.
- **A single year of weather data is used for most Cambium metrics**: The PLEXOS runs use weather data from 2012.
- **The p19 region** is erroneously represented as having no transmission connection to its neighbors. The long-run and short-run marginal emissions rates for that region are reported as the averages for the GEA region it is within. Analysts studying the region are encouraged to use the larger GEA regions instead of the p19 data.

We point interested users to a review of Cambium's 2020 marginal cost patterns performed by Lawrence Berkeley National Laboratory (Seel and Mills 2021).

### 4.2 Comparing Cambium Projections to Historical Emissions Data

To place Cambium's emissions intensity projections in context, this section compares the 2024 Cambium Mid-case in-region CO2 emissions intensity projections (the aer_gen_co2_c metric) against historical emissions data (Figure 5). The historical data are derived from eGRID, where the plant-level emissions and generation values have been aggregated to the GEA regions used for reporting Cambium data (shown in Figure 4).

[DIAGRAM DESCRIPTION: Cambium's generation and emission assessment (GEA) regions, 2024 version]

A map of the conterminous United States divided into 18 GEA regions. Each region is a contiguous geographic area with a distinct boundary. The regions approximate significant operational and planning boundaries but do not exactly correspond to many relevant administrative boundaries. The map is used for aggregating Cambium metrics and for comparing with historical eGRID data.

[→ See original PDF page 18 for visual rendering]

Note that there is a difference between how the eGRID and Cambium emissions intensities are calculated: Cambium's emissions intensity calculation includes generation from behind-the-meter PV, storage, and electricity imported from Canada, whereas eGRID does not. The native Cambium calculation is shown in dark blue in Figure 5, whereas a replication of the eGRID calculation with Cambium projections is shown in light blue.

Examining Figure 5, most of the GEA regions have seen a historical decline in emissions intensity, with the Cambium data often projecting a continuation (and in some cases, acceleration in the rate) of decline in the near term. A file with the historical emissions intensities for the Cambium GEA regions is available for download with the rest of the Cambium data in the NREL Scenario Viewer.

[DIAGRAM DESCRIPTION: Comparison of Cambium CO2 emission intensity projections against historical data]

A multi-panel or multi-series chart comparing historical eGRID-based CO2 emissions intensity (e.g., kg/MWh) by GEA region over time with Cambium Mid-case projections. Dark blue series represent the native Cambium calculation; light blue represents the eGRID-style calculation with Cambium projections. The figure shows alignment and gaps between historical trends and modeled projections across regions.

[→ See original PDF page 20 for visual rendering]

---

## 5 Cambium Metric Definitions

In this section, we briefly define all the metrics in Cambium databases. The outputs from ReEDS and PLEXOS are the starting point for Cambium's processing; some of the metrics are direct reports from those models, but others involve extensive post-processing. We describe the Cambium-specific post-processing methods in section 6.

### 5.1 Busbar and End-Use Values

Metrics in Cambium databases are reported at either the busbar or end-use level, depending on their most common usage. **Busbar** refers to the point where bulk generating stations connect to the grid, whereas **end use** refers to the point of consumption. In Cambium databases, busbar and end-use values differ by the distribution loss rates between the two points.

There are two distribution loss rates: an average and a marginal. Short-run marginal metrics (cost metrics and SRMER) use marginal loss metrics, whereas average or long-run marginal metrics use average loss metrics.

For a generic metric $X$, the end-use and busbar values are related by:

$$X_{end-use} = X_{busbar} (1 - \alpha)$$

where the relevant distribution loss rate is $\alpha$. Hourly $\alpha$ are given in Cambium databases as **distloss_rate_avg** and **distloss_rate_marg**. See section 6.7 for the approach and assumptions for calculating these metrics.

### 5.2 Time and Geographic Identifiers

**Metric Family: timestamp**\
**Metric Names:** timestamp, timestamp_local

The **timestamp** metric is the time in Eastern Standard Time. The **timestamp_local** variable is the time in the local Standard Time. If no timestamp_local variable is in a file, the data are in Eastern Standard Time. Both timestamp variables are hour-beginning (e.g., 1:00 indicates data for 1:00–2:00). Neither timestamp variable includes the effects of Daylight Savings Time. Every year in a Cambium dataset has 8,760 hours and preserves the 7-day weekday/weekend pattern throughout the full time period. Leap days are omitted in the timestamps during leap years. Every time series starts on a Sunday, regardless of the actual day of the week for January 1 of that year.

**Metric Family: time zone**\
**Metric Name:** tz

The **tz** variable in the metadata indicates the time zone used for the timestamp_local variable. For regions that contain multiple time zones, the data are reported using the time zone where the majority of the load is located.

**Metric Family: ReEDS model balancing area (BA)**\
**Metric Name:** r

The balancing area (**r**) is the finest geographic unit for which Cambium data are reported. There are 133 BAs. In Cambium 2024, p119 and p122 were combined (labeled as z122).

[DIAGRAM DESCRIPTION: Balancing areas]

A map of the conterminous United States showing 133 balancing areas (BAs) used as nodes in ReEDS and PLEXOS. Each BA is a contiguous region; boundaries may follow state lines, utility service territories, or other planning boundaries.

[→ See original PDF page 23 for visual rendering]

**Metric Family: Cambium generation and emission assessment (GEA) region**\
**Metric Name:** gea

Cambium's GEA regions are 18 regions covering the contiguous United States (Figure 7). They were selected to approximate significant operational and planning boundaries but do not exactly correspond to many relevant administrative boundaries. Shapefiles, mappings of GEA regions to ZIP codes, and mappings of GEA regions to counties can be obtained in the NREL Scenario Viewer.

[DIAGRAM DESCRIPTION: Cambium's generation and emission assessment (GEA) regions, 2024 version]

Same as Figure 4: a map of the conterminous United States divided into 18 GEA regions used for reporting and aggregating Cambium metrics.

[→ See original PDF page 24 for visual rendering]

### 5.3 Generation and Capacity Metrics

- **generation** (MWh busbar): Total generation from all generators within a region, including storage; does not include curtailed energy. Behind-the-meter PV is included as equivalent busbar generation.
- **variable_generation** (MWh busbar): Total generation from all variable generators (PV, CSP without storage, wind). Behind-the-meter PV included as equivalent busbar generation.
- **technology_MWh** (MWh busbar): Total generation within a region from each technology listed in Table 4.
- **technology_MW** (MW): Total net summer generating capacity within a region from each technology in Table 4 (except Canadian imports). Behind-the-meter PV is reported as AC inverter capacity.
- **technology_energy_cap_MWh** (MWh): Total nameplate energy storage capacity for batteries, pumped hydropower storage, and CSP.

### 5.4 Emission Metrics

Emissions are reported for CO2, CH4, and N2O. CO2 equivalent (CO2e) values combine the three using 100-year GWP values from the IPCC Sixth Assessment Report (1 for CO2, 29.8 for CH4, 273 for N2O). Emissions are reported for direct combustion ("_c") and precombustion processes ("_p"). Precombustion includes fuel extraction, processing, and transport (including fugitive emissions). For fuel-specific emissions factors, see section 6.2.

- **aer_gen_*** (average emission rates of in-region generation): Average emission rate of all generation within a region for the specified duration, in kg or g per MWh of busbar generation. No adjustment for imports/exports. Distribution loss metric: average.
- **aer_load_*** (average emission rates of generation induced by a region's load): Average emission rate of the generation allocated to a region's end-use load, including effects of imported and exported power. Distribution loss metric: average. Reflects credit trading for state portfolio standards.
- **srmer_*** (short-run marginal emission rates for a region's load): Rate of emissions that would be induced by a marginal increase in a region's load at a specific point in time. Value is the emission rate of whichever generator would have served the marginal increase in load, modified by transmission, distribution, and efficiency losses. Distribution loss metric: marginal. Reported as kg or g per MWh of end-use load.
- **lrmer_*** (long-run marginal emission rates for a region's load): Emission rate of the mixture of generation that would be either induced or avoided by an electric sector intervention, taking into account how the intervention may influence the structure of the grid. Distribution loss metric: average. Units: kg or g per MWh of end-use load.
- **total_gen_*** (total emissions by region): Total emissions from all generation within a region, in metric tons. No adjustment for imported or exported electricity.

### 5.5 Cost Metrics

All dollar values are in real terms for a constant dollar year. For the annual Cambium datasets, the dollar year is the year preceding the release (e.g., 2024 Cambium dollar values are in 2023 dollars).

- **energy_cost_busbar**, **energy_cost_enduse** ($/MWh): Short-run marginal costs of providing the energy for a marginal increase in load. Derived from the shadow price off of an energy constraint in PLEXOS. Include fuel and variable costs; do not reflect startup costs or fixed O&M. Marginal energy costs in Cambium have a floor of zero. Distribution loss metric: marginal.
- **capacity_cost_busbar**, **capacity_cost_enduse** ($/MWh): Long-run cost of additional capital investment necessary to maintain a target planning reserve margin when demand is increased. Derived from the shadow price off of the capacity constraint in ReEDS, allocated to the highest net-load hours. Distribution loss metric: marginal. Cambium 2024 duplicates the 2030 shadow prices in 2025 for this family of metrics.
- **portfolio_cost_busbar**, **portfolio_cost_enduse** ($/MWh): Marginal cost of staying in compliance with state RPS and CES when end-use demand is increased. Can be negative when a compliant technology is on the margin (e.g., curtailing solar). Distribution loss metric: marginal. Cambium 2024 duplicates the 2030 shadow prices in 2025 for this family.
- **total_cost_busbar**, **total_cost_enduse** ($/MWh): Sum of energy, capacity, and portfolio costs. Does not include distribution capacity, transmission capacity, administrative and general expenses, and other electric sector expenses. Has a floor of zero.

### 5.6 Interregional Transmission Metrics

These transmission metrics include only transmission between BAs, not within BAs. They do not include Canadian imports and exports.

- **imports**, **exports** (MWh busbar): Total imports and exports into and out of a region through interregional transmission lines. Value is the energy sent along the lines, not netted by transmission losses. Transmission losses are reported in **trans_losses** and are allocated equally between the sending and receiving regions.

### 5.7 Load Metrics

- **busbar_load** (MWh busbar): Total electric load in a region, including end-use load (busbar equivalent of load served by behind-the-meter PV), load from transmission losses, and load from storage charging.
- **enduse_load** (MWh end-use): Electricity consumed at the point of end use within a region, including end-use load served by behind-the-meter PV. **busbar_load_for_enduse**: quantity of load at the busbar level to meet that end-use load (larger than enduse_load because it is prior to distribution losses). Neither includes transmission losses or storage load.
- **trans_losses** (MWh busbar): Energy lost due to inter-BA transmission losses, represented as additional load at the busbar level, split equally between the sending and receiving BA.
- **battery_charging**, **phs_charging** (MWh busbar): Busbar load caused by the charging of electric battery storage and pumped hydropower storage, respectively.
- **net_load_busbar** (MWh busbar): busbar_load minus variable_generation.

### 5.8 Operational Metrics

- **curt_wind_MWh**, **curt_solar_MWh**, **curtailment_MWh** (MWh busbar): Curtailed generation from wind, solar, geothermal, non-dispatchable hydropower, and nuclear. curtailment_MWh is the total from all of these technology classes.
- **distloss_rate_avg**, **distloss_rate_marg** (MWh losses/MWh busbar load): Average and marginal distribution loss rates. The average loss rate $\alpha$ is defined as losses $L$ per busbar load consumed for end use $D$: $\alpha = L/D$. The marginal loss rate $\mu$ is the increase in losses per marginal increase in busbar load consumed for end use. See section 6.7.
- **prm** (MW firm/MW peak): Planning reserve margin used within ReEDS. $PRM = (Firm\ Capacity - Peak\ Demand) / Peak\ Demand$.
- **planning_capacity_MW** (MW firm): Amount of firm capacity called for in each region to meet the planning reserve margin.
- **capacity_shadow_price** ($/MW firm): Annual shadow price from ReEDS on the capacity constraint; marginal cost of procuring another MW of firm generation capacity.
- **marg_gen_tech**, **marg_es_tech**: Technology of the short-run marginal generator and short-run marginal energy source for a given location and time. Only reported at the hourly BA resolution.

### 5.9 Policy Metrics

- **rps_shadow_price**, **ces_shadow_price** ($/MWh credit): Shadow prices on portfolio standard constraints from ReEDS. Annual values; represent the marginal cost of procuring another MWh of generation (or credit) eligible to satisfy the policy.
- **rps_f**, **ces_f** (MWh credit/MWh end-use): Requirements of state-level portfolio standard constraints—the average fraction of end-use load within the region covered by the policy that must be covered by eligible generation.

---

## 6 Cambium Methods

### 6.1 Technologies Represented in Cambium

Data are reported for technology groups in Cambium (Table 4). The actual number of discrete technologies in ReEDS and PLEXOS is greater, but the data are grouped to reduce the size of the database.

**Table 4. Cambium Technologies**

| Technology Name in Cambium Database | Technologies in ReEDS and PLEXOS                                         |
| ----------------------------------- | ------------------------------------------------------------------------ |
| battery                             | Electric batteries                                                       |
| biomass                             | Biopower and landfill gas                                                |
| canada                              | Canadian imports                                                         |
| coal                                | Coal (scrubbed and unscrubbed, integrated gasification combined cycle)   |
| csp                                 | Concentrating solar power (with and without thermal energy storage)      |
| distpv                              | Behind-the-meter PV                                                      |
| gas-cc                              | Natural gas combined cycle                                               |
| gas-ct                              | Natural gas combustion turbine                                           |
| geothermal                          | Geothermal                                                               |
| hydro                               | Hydropower (existing and undiscovered, dispatchable and nondispatchable) |
| nuclear                             | Nuclear (conventional)                                                   |
| o-g-s                               | Oil-gas-steam                                                            |
| phs                                 | Pumped hydropower storage                                                |
| upv                                 | Utility-scale and distributed-utility-scale PV                           |
| wind-ofs                            | Offshore wind (fixed-bottom)                                             |
| wind-ons                            | Onshore wind                                                             |

### 6.2 Emissions Factors by Fuel

Cambium emission metrics are calculated using the fuel-specific emissions factors given in Table 5. The resulting emissions per MWh of electric generation depend on the generator's heat rate. Precombustion emission factors include fuel extraction, processing, and transport, including fugitive emissions. Precombustion emissions for natural gas are drawn from Alvarez et al. (2018); power plants are assumed to avoid local distribution losses, with fugitive methane emissions rate starting at 2.2% in 2022 and decreasing linearly by 30% by 2030. CO2e values use 100-year GWP from the IPCC Sixth Assessment Report (1 for CO2, 29.8 for CH4, 273 for N2O).

**Table 5. Emission Factors by Fuel**

| Fuel              | Type          | Emission | Emission Factor | Units    | Source                                                                                                                 |
| ----------------- | ------------- | -------- | --------------- | -------- | ---------------------------------------------------------------------------------------------------------------------- |
| Coal              | Precombustion | CO2      | 2.94            | kg/MMBtu | USLCI, Bituminous Coal at power plant                                                                                  |
| Coal              | Precombustion | CH4      | 208.26          | g/MMBtu  | USLCI                                                                                                                  |
| Coal              | Precombustion | N2O      | 0.05            | g/MMBtu  | USLCI                                                                                                                  |
| Coal              | Combustion    | CO2      | 95.52           | kg/MMBtu | EPA 2016, Table A-3, Coal and Coke, Mixed (Electric Power Sector)                                                      |
| Coal              | Combustion    | CH4      | 11.00           | g/MMBtu  | EPA 2016                                                                                                               |
| Coal              | Combustion    | N2O      | 1.60            | g/MMBtu  | EPA 2016                                                                                                               |
| Natural Gas       | Precombustion | CO2      | 6.27            | kg/MMBtu | USLCI, Natural Gas at power plant                                                                                      |
| Natural Gas       | Precombustion | CH4      | 571.6–400.2     | g/MMBtu  | Alvarez et al. (2018); Mason and Alper (2021); 571.6 in 2022 decreasing linearly to 400.2 in 2030, constant thereafter |
| Natural Gas       | Precombustion | N2O      | 0.02            | g/MMBtu  | USLCI                                                                                                                  |
| Natural Gas       | Combustion    | CO2      | 53.06           | kg/MMBtu | EPA 2016, Table A-3, Natural Gas                                                                                       |
| Natural Gas       | Combustion    | CH4      | 1.00            | g/MMBtu  | EPA 2016                                                                                                               |
| Natural Gas       | Combustion    | N2O      | 0.10            | g/MMBtu  | EPA 2016                                                                                                               |
| Residual Fuel Oil | Precombustion | CO2      | 9.91            | kg/MMBtu | USLCI at power plant                                                                                                   |
| Residual Fuel Oil | Precombustion | CH4      | 153.45          | g/MMBtu  | USLCI                                                                                                                  |
| Residual Fuel Oil | Precombustion | N2O      | 0.17            | g/MMBtu  | USLCI                                                                                                                  |
| Residual Fuel Oil | Combustion    | CO2      | 75.10           | kg/MMBtu | EPA 2016, Table A-3, Residual Fuel Oil No. 6                                                                           |
| Residual Fuel Oil | Combustion    | CH4      | 3.00            | g/MMBtu  | EPA 2016                                                                                                               |
| Residual Fuel Oil | Combustion    | N2O      | 0.60            | g/MMBtu  | EPA 2016                                                                                                               |
| Uranium           | Precombustion | CO2      | 0.84            | kg/MMBtu | USLCI, Uranium at power plant                                                                                          |
| Uranium           | Precombustion | CH4      | 2.10            | g/MMBtu  | USLCI                                                                                                                  |
| Uranium           | Precombustion | N2O      | 0.02            | g/MMBtu  | USLCI                                                                                                                  |
| Uranium           | Combustion    | CO2      | 0.00            | kg/MMBtu | ATB 2024                                                                                                               |
| Uranium           | Combustion    | CH4      | 0.00            | g/MMBtu  | -                                                                                                                      |
| Uranium           | Combustion    | N2O      | 0.00            | g/MMBtu  | -                                                                                                                      |
| Biomass           | Precombustion | CO2      | 2.46            | kg/MMBtu | CARB 11-307, Table 15                                                                                                  |
| Biomass           | Precombustion | CH4      | 2.94            | g/MMBtu  | CARB 11-307                                                                                                            |
| Biomass           | Precombustion | N2O      | 0.01            | g/MMBtu  | CARB 11-307                                                                                                            |
| Biomass           | Combustion    | CO2      | 0.00            | kg/MMBtu | ATB 2024                                                                                                               |
| Biomass           | Combustion    | CH4      | 0.00            | g/MMBtu  | -                                                                                                                      |
| Biomass           | Combustion    | N2O      | 0.00            | g/MMBtu  | -                                                                                                                      |

Sources: USLCI (NREL 2021), EPA 2016, ATB 2024 (NREL 2024), CARB 11-307 (Carreras-Sospedra et al. 2015), Alvarez et al. (2018).

### 6.3 Coloring Power Flows

When calculating the characteristics of the generation allocated to load at a certain point (e.g., average emission rate of the generators serving end-use consumption at a specific node), the composition of the source generation must be determined. Cambium assumes each node is a "perfect mixer": any electricity consumed or exported from a node is a perfect mixture of the electricity being supplied to the node.

[DIAGRAM DESCRIPTION: Simple network for illustrating power flow coloring]

A simple schematic network with five nodes (N1–N5) connected by four transmission lines. Only nodes N1, N2, and N3 have generation, with in-region generation emission rates (e.g., 400, 1,000, and 0 kg/MWh). The diagram illustrates how power flow coloring allocates generation from each generator to loads at each node under the perfect-mixing assumption. Flows and nodal loads are labeled; the method yields emission rates ascribed to each node's load (e.g., N3 receives 30% from N1 and 70% from N2, giving a weighted average 820 kg/MWh; N4 and N5 receive power from N3 and possibly local generation).

[→ See original PDF page 42 for visual rendering]

For each BA and each time-step, the documentation uses: generation in the node $g_i$, total imports $f_{in,i}$, total exports $f_{out,i}$, and derived load $l_i$:

$$l_i = g_i + f_{in,i} - f_{out,i}$$

Nodal through-flow $P_i$ is $P_i = l_i + f_{out,i}$. The downstream distribution matrix $\mathbf{A}_d$ is defined so that the $(i,j)$ element is 1 if $i=j$, $-|f_{i-j}|/P_i$ if $j$ is in the set of nodes directly supplied by $i$, and 0 otherwise. Using the inverse $\mathbf{A}_d^{-1}$, the amount of generation from source BA $i$ allocated to load in destination BA $j$ is:

$$g_{i-j} = g_i \cdot l_j \cdot [\mathbf{A}_d^{-1}]_{i,j} / P_i$$

Allocation factor $f_{i-j} = l_j \cdot [\mathbf{A}_d^{-1}]_{i,j} / P_i$ can be used to allocate any quantity from node $i$ to node $j$ if it is assumed to flow proportionally with generation. The weighted average of attribute $X$ for load at node $j$ is $X_j = \sum_{i=1}^{n} (g_{i-j} \cdot X_i) / l_j$. This follows the downstream-looking algorithm from Bialek (1996).

**Caveats**: Perfect mixing may not be appropriate when load has contracted for specific generation (e.g., PPAs) or when state restrictions on imports (e.g., California and coal) apply. Transmission losses and storage charging are treated as loads, so the approach yields a lower emissions rate than if the basis were solely end-use demand.

### 6.4 Calculating Long-Run Marginal Emission Rates

The long-run marginal emission rate (LRMER) is the emission rate of the generation that would be either induced or avoided by a marginal change in electric load, including both operational and structural consequences. It is distinct from the short-run marginal emission rate (SRMER), which treats grid assets as fixed.

Cambium estimates LRMER by solving each modeled year twice: once with projected conditions (Base) and again with a scalar increase in end-use electricity demand (Perturb). In Cambium 2024, the 2025 solve has a 1% scalar load perturbation; all other solve years have a 5% scalar load perturbation. The approach has five steps:

1. **Run each solve year twice** (Base and Perturb) in both ReEDS and PLEXOS so that structural responses (new capacity, retirement, transmission) are included.
2. **Allocate changes in generation to regions** using a modification of the Bialek (1996) power flow method for allocating differences (by GEA region and technology). Transmission flow differences between Base and Perturb are used; allocation factors $\omega_{g-l}$ give the fraction of increased generation from region $g$ that can be allocated to consuming GEA region $l$. Net consumed generation mixture by technology $\Delta g_{l,t,cons}$ is computed; negative values (net decreases) are removed from the mixture for each hour.
3. **Assign originating mixtures for storage and other energy-constrained generators**: Energy storage (batteries, PHS) does not create energy; their contribution is assigned the monthly mixture of nonstorage, non-energy-constrained generation in the receiving GEA region, with fuel consumption inflated by round-trip efficiency. Energy-constrained generators (dispatchable hydro, Canadian imports) are split into (a) increases from expanded capacity (assigned that technology’s characteristics) and (b) re-dispatched energy from the same budget (assigned like storage, using the monthly mixture without efficiency loss).
4. **Calculate LRMER and adjust to respect state policies**: State-level mixtures are computed from GEA mixtures. For states with a shortfall relative to portfolio standards, emissions intensity is decreased by the fraction that would have to be offset; the subtracted emissions are reallocated to states with excess qualifying generation within the interconnect (by hour, weighted by non-emitting share).
5. **Adjust for distribution losses**: Hourly values from Step 4 are inflated by the average distribution loss rate to report LRMER per MWh of end-use load.

**Levelization**: For multiyear interventions, LRMER can be levelized over the analysis horizon. For unit of time $h$, with $n$ years and social discount rate $d$:

$$LRMER_{h,levelized} = \frac{\sum_{y=0}^{n-1} \frac{LRMER_{h,y}}{(1+d)^y}}{\sum_{y=0}^{n-1} \frac{1}{(1+d)^y}}$$

**Caveats**: Geographic disaggregation from nationwide runs may differ from state-specific perturbations; each hour is treated as independent although dispatch and build-out are interdependent; power flow allocation assumes perfect mixing; transmission losses are not fully captured (losses represented as load dilute the emission rate).

### 6.5 Identifying a Region's Short-Run Marginal Generator

The short-run marginal generator for a location and time is the generator whose output would increase if there were a marginal increase in demand at that location and time. Cambium differentiates between the **marginal generator** (the unit that would provide the power at that moment) and the **marginal energy source** (the generator that would ultimately increase its generation at another time if the marginal generator is energy-constrained, e.g., a battery). The method has four steps:

1. **Identify BAs that share a marginal generator (T-regions)**: BAs connected by partially utilized transmission lines are assumed to share a marginal generator. Groupings of such BAs are "T-regions."
2. **Identify T-regions with dropped load**: These have no marginal generator and are labeled as such.
3. **Evaluate non-energy-constrained generators**: Among generators committed in the T-region at that time, filter out those at maximum output and energy-constrained generators. The generator with the lowest SRMC in the T-region is designated the marginal generator.
4. **Evaluate energy-constrained generators**: If no generator is identified in Step 3, the marginal generator is an energy-constrained generator that is charging or discharging. The SRMC of each such generator is estimated by finding a non-energy-constrained generator that could have increased output in another time-step to allow the energy-constrained generator to have more energy; SRMC is set by that generator’s SRMC modified by transmission and efficiency losses (see section 6.6). The energy-constrained generator with SRMC closest to the average marginal energy cost in the T-region is designated the marginal generator.

Region-hours where no generator was identified are marked "unknown" and assigned the mean SRMER for that GEA region at that timestamp.

**Caveats**: The marginal generator is not a native PLEXOS output; the method interprets model outputs and can be wrong when energy-constrained generators are involved. Real-world unit commitment and dispatch differ from PLEXOS. Marginal generator patterns are sensitive to demand; large load shifts (e.g., EV charging in response to margin) could change which technology is on the margin.

### 6.6 Identifying the Energy Source When an Energy-Constrained Generator Is on the Short-Run Margin

When the marginal generator is energy-constrained (e.g., battery, PHS, dispatchable hydro, CSP with storage), it cannot create new energy; another generator must increase generation at another time. Cambium identifies that source-energy generator as follows:

1. Identify the **opportunity window**: the span of time in which the energy-constrained generator could have obtained more energy (e.g., for a battery discharging in the anchor hour: from the most recent time the battery was full to the next time it is empty).
2. Restrict the window to ±24 hours from the anchor time-step (scheduling/forecasting limits).
3. Remove time-steps where the energy-constrained generator is already charging at maximum.
4. Remove time-steps where no marginal generator could have increased generation to charge the storage or cover reduced discharge.
5. For remaining time-steps, apply efficiency and transmission adjustments to get the energy-constrained generator’s effective SRMC if it drew from that time-step (Table 6).
6. Select the time-step (and generator) with the lowest resulting SRMC.
7. Calculate derivative values (e.g., SRMER). If that generator is another energy-constrained generator, iterate to find the ultimate source-energy generator.

[DIAGRAM DESCRIPTION: Charge and discharge patterns of an electrical battery]

A time-series plot over a 20-hour period showing a toy battery with 1 MW max charge/discharge and 2 MWh max storage, 80% round-trip efficiency. The plot shows charging and discharging power and state-of-charge over time. The 11th hour is highlighted as the "anchor" hour when the battery is assumed to be the marginal generator.

[→ See original PDF page 57 for visual rendering]

[DIAGRAM DESCRIPTION: Example of an "opportunity window" for an electric battery]

Two panels. Top: the 11th hour shows marginal discharge (battery on margin). Shaded area indicates the span (from start of hour 2 to end of hour 13) in which the battery could have increased stored energy to enable the increased discharge in the 11th hour. Bottom: illustration of the rule that the opportunity window extends back to the most recent time the battery was full and forward to the next time it is empty. For charging margin (left-hand marginal), bounds are inverted.

[→ See original PDF page 58 for visual rendering]

**Table 6. Efficiency Adjustments**

| Anchor Time-Step Behavior | Point Time-Step Behavior | Description                                                                                           | Efficiency Adjustment |
| ------------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------- | --------------------- |
| Charging                  | Charging                 | Energy-constrained generator reduces charging in anchor and increases charging in point time-step.    | 1.0                   |
| Charging                  | Discharging              | Energy-constrained generator reduces charging in anchor and reduces discharging in point time-step.   | RTE                   |
| Discharging               | Charging                 | Energy-constrained generator increases discharge in anchor and increases charging in point time-step. | 1/RTE                 |
| Discharging               | Discharging              | Energy-constrained generator increases discharge in anchor and reduces discharge in point time-step.  | 1.0                   |

RTE = round-trip efficiency of the energy-constrained generator.

### 6.7 Calculating Time-Varying Distribution Loss Rates

Cambium calculates both average and marginal hourly distribution loss rates following Borenstein and Bushnell (2019). Assumptions: 25% of annual distribution losses are fixed (no-load); 75% are resistive and scale with the square of flow. Annual average distribution loss rate is 3.6% for each BA (from eGRID Grid Gross Loss 2018, minus nondistribution losses from a ReEDS 2018 run).

For each BA: total annual fixed losses $L_{f,b}$ and variable losses $L_{v,b}$ as a function of annual busbar load consumed for end uses $Q_{b,b}$, no-load loss fraction $\pi$, and annual loss rate $F_b$:

$$L_{f,b} = Q_{b,b} \cdot F_b \cdot \pi$$

$$L_{v,b} = Q_{b,b} \cdot F_b \cdot (1 - \pi)$$

Annual variable loss factor $f_{v,b} = L_{v,b} / Q_{b,b}^2$. Hourly variable losses $L_{v,h} = f_{v,b} \cdot Q_{b,h}^2$. Total hourly losses $L_{t,h} = L_{v,h} + L_{f,b}/8760$. Then:

$$\alpha_h = L_{t,h} / Q_{b,h}$$

$$\mu_h = Q_{b,h} \cdot 2 \cdot f_{v,b}$$

### 6.8 Calculating Hourly Marginal Capacity Costs

The annual marginal cost of firm capacity is given by the shadow price on the ReEDS capacity constraint (which requires firm capacity to exceed peak demand by the PRM). ReEDS finds the least-cost option among: new generation (e.g., net CONE), new transmission, or delayed retirement. This annual value is then allocated to hours:

1. Obtain each BA’s shadow price $\rho_{BA}$ from the capacity constraint.
2. Multiply by $(1 + PRM)$ to get marginal cost of procuring the firm capacity for an increase in peak busbar load: $\delta_{BA} = \rho_{BA} \cdot (1 + PRM_{BA})$.
3. Hourly net load $\eta_{geb,h}$ for each GEA region (from **net_load_busbar**).
4. Threshold $\eta_{geb,threshold} = \min(\eta_{geb,101}, \eta_{geb,1} \cdot 0.95)$ (101st-highest net-load hour or 95% of peak).
5. Total net load above threshold in the year: $N_{geb} = \sum_{h=1}^{8760} \max(\eta_{geb,h} - \eta_{geb,threshold}, 0)$.
6. Weight for each hour where net load exceeds threshold: $w_{geb,h} = \max(\eta_{geb,h} - \eta_{geb,threshold}, 0) / N_{geb}$ (weights sum to 1).
7. Hourly marginal capacity cost for each BA: $C_{ba,h} = w_{geb,h} \cdot \delta_{BA}$ (using the GEA region that contains that BA).

**Caveats**: PRM and net-load threshold are heuristics; capacity shadow prices can be lower than Net CONE (e.g., NGCT) because the model can use retiring capacity, batteries, and variable resources; no elasticities are provided.

### 6.9 Calculating Marginal Portfolio Costs

Marginal portfolio costs are the costs of staying in compliance with state RPS and CES when end-use demand is increased. RPS/CES are represented in ReEDS but not in PLEXOS, so they are added in post-processing. Each policy is a constraint in ReEDS; the shadow price is the cost per additional credit.

If the marginal generator **cannot** contribute to the policy (ineligible or outside the policy region):

$$C_p = f_p \cdot \lambda_p$$

where $f_p$ is the fraction of end-use demand that must be covered by eligible generation (average over the policy region), and $\lambda_p$ is the annual shadow price for policy $p$.

If the marginal generator **can** contribute to the policy (eligible and in a region that can trade credits):

$$C_p = -\left( \frac{1}{1 - \mu_h} - f_p \right) \cdot \lambda_p$$

where $\mu_h$ is the marginal distribution loss rate. The negative value reflects the benefit of creating excess credits when additional consumption is served by eligible generation (e.g., curtailing solar). For busbar marginal costs, end-use marginal costs are modified by the marginal distribution loss rate.

**Caveats**: Policy representations in ReEDS are incomplete (e.g., no inter-year banking, technology multipliers); shadow prices are long-run values and should not be used directly as forecasts of credit market prices.

---

## Acknowledgments

This report was funded by the DOE Office of Energy Efficiency and Renewable Energy under contract number DE-AC36-08GO28308. Any errors or omissions are the sole responsibility of the authors.

---

## References

Alvarez, Ramón A., Daniel Zavala-Araiza, David R. Lyon, David T. Allen, Zachary R. Barkley, Adam R. Brandt, Kenneth J. Davis, et al. 2018. "Assessment of Methane Emissions from the U.S. Oil and Gas Supply Chain." _Science_ 361:186–88. https://doi.org/10.1126/science.aar7204.

Bloom, Aaron, Aaron Townsend, David Palchak, Joshua Novacheck, Jack King, Clayton Barrows, Eduardo Ibanez, et al. 2016. "Eastern Renewable Generation Integration Study." NREL/TP-6A20-64472. Golden, CO: National Renewable Energy Laboratory. https://doi.org/10.2172/1318192.

Cole, Wesley, A. Will Frazier, and Chad Augustine. 2021. "Cost Projections for Utility-Scale Battery Storage: 2021 Update." NREL/TP-6A20-75385. Golden, CO: National Renewable Energy Laboratory. https://doi.org/10.2172/1665769.

Cole, Wesley, Bethany Frew, Trieu Mai, Yinong Sun, John Bistline, Geoffrey Blanford, David Young, et al. 2017. "Variable Renewable Energy in Long-Term Planning Models: A Multi-Model Perspective." NREL/TP-6A20-70528. Golden, CO: National Renewable Energy Laboratory. https://www.nrel.gov/docs/fy18osti/70528.pdf.

Cole, Wesley, Nathaniel Gates, Trieu Mai, Daniel Greer, and Paritosh Das. 2019. "2019 Standard Scenarios Report: A U.S. Electricity Sector Outlook." NREL/TP-6A20-74110. Golden, CO: National Renewable Energy Laboratory. https://doi.org/10.2172/1481848.

Cole, Wesley, Daniel Greer, Jonathan Ho, and Robert Margolis. 2020. "Considerations for Maintaining Resource Adequacy of Electricity Systems with High Penetrations of PV and Storage." _Applied Energy_ 279 (December).

Cole, Wesley, Kenneth B. Medlock III, and Aditya Jani. 2016. "A View to the Future of Natural Gas and Electricity: An Integrated Modeling Approach." _Energy Economics_ 60 (November):486–96. https://doi.org/10.1016/j.eneco.2016.03.005.

DOE. 2012. "SunShot Vision Study." DOE/GO-102012-3037. Washington, D.C.: U.S. Department of Energy. http://www.nrel.gov/docs/fy12osti/47927.pdf.

DOE. 2015. "Wind Vision: A New Era for Wind Power in the United States." DOE/GO-102015-4557. Washington, D.C.: U.S. Department of Energy. http://www.energy.gov/sites/prod/files/WindVision_Report_final.pdf.

DOE. 2016. "Hydropower Vision: A New Chapter for America's 1st Renewable Electricity Source." DOE/GO-102016-4869. Washington, D.C.: U.S. Department of Energy. http://energy.gov/eere/water/articles/hydropower-vision-new-chapter-america-s-1st-renewable-electricity-source.

DOE. 2019. "GeoVision: Harnessing the Heat Beneath Our Feet." Washington, D.C.: U.S. Department of Energy. https://www.energy.gov/sites/prod/files/2019/06/f63/GeoVision-full-report-opt.pdf.

EIA. 2021. "Annual Energy Outlook 2021." Washington, D.C.: U.S. DOE Energy Information Administration. https://www.eia.gov/outlooks/aeo/pdf/AEO_Narrative_2021.pdf.

EIA. 2023. "Annual Energy Outlook 2023." Washington, D.C.: U.S. Energy Information Administration. https://www.eia.gov/outlooks/aeo/pdf/AEO2023_Narrative.pdf.

Energy+Environmental Economics. 2016. "Avoided Cost Calculator User Manual." Energy+Environmental Economics.

Energy Exemplar. 2019. "PLEXOS Integrated Energy Model." Energy Exemplar. https://energyexemplar.com/solutions/plexos/.

EPA. 2020. "EGRID 2018 Gross Grid Loss Estimates." Washington, D.C.: EPA. https://www.epa.gov/egrid/download-data.

Frew, Bethany, Wesley Cole, Paul Denholm, Will Frazier, Nina Vincent, and Robert Margolis. 2019. "Sunny with a Chance of Curtailment: Operating the US Grid with Very High Levels of Solar Photovoltaics." _IScience_ 21 (November):436447. https://doi.org/10.1016/j.isci.2019.10.017.

Gagnon, Pieter, and Wesley Cole. 2022. "Planning for the Evolution of the Electric Grid with a Long-Run Marginal Emission Rate." _IScience_ 25 (3). https://doi.org/10.1016/j.isci.2022.103915.

Gagnon, Pieter, and Eric O'Shaughnessy. 2024. "Consequential Analysis of the Greenhouse Gas Emissions Impacts of Actions That Influence the Electric Grid: The Theory and Practice of Using Marginal Emissions Rates." NREL/TP-6A40-91580. https://doi.org/10.2172/2481678.

Gagnon, Pieter, An Pham, Wesley Cole, Anne Hamilton, Sarah Awara, Anne Barlas, Maxwell Brown, et al. 2024. "2024 Standard Scenarios Report: A U.S. Electricity Sector Outlook." NREL/TP-6A40-92256. National Renewable Energy Laboratory. https://www.nrel.gov/docs/fy25osti/92256.pdf.

Hale, Elaine, Brady Stoll, and Trieu Mai. 2016. "Capturing the Impact of Storage and Other Flexible Technologies on Electric System Planning." _Renewable Energy_ 91.

Haley, Ben, Ryan Jones, Jim Williams, Gabe Kwok, Jamil Farbes, Jeremy Hargreaves, Katie Pickrell, Darcie Bentz, Andrew Waddell, and Emily Leslie. 2022. "Annual Decarbonization Perspective: Carbon Neutral Pathways for the United States 2022." Evolved Energy Research.

Ho, Jonathan, Jonathon Becker, Maxwell Brown, Patrick Brown, Ilya Chernyakhovskiy, Stuart Cohen, Wesley Cole, et al. 2021. "Regional Energy Deployment System (ReEDS) Model Documentation: Version 2020." Golden, CO: National Renewable Energy Laboratory. https://www.nrel.gov/docs/fy21osti/78195.pdf.

Bialek, Janusz. 1996. "Tracing the Flow of Electricity." _IEE Proceedings - Generation, Transmission, and Distribution_ 143 (4): 313–20.

Mason, Jeff, and Alexandra Alper. 2021. "Biden Asks World Leaders to Cut Methane in Climate Fight." Reuters, September 17, 2021. https://www.reuters.com/business/environment/biden-convenes-world-leaders-discuss-climate-change-ahead-glasgow-summit-2021-09-17/.

Lew, D., G. Brinkman, E. Ibanez, B. M. Hodge, M. Hummon, A. Florita, and M. Heaney. 2013. "The Western Wind and Solar Integration Study Phase 2." NREL/TP-5500-55588. National Renewable Energy Lab. https://doi.org/10.2172/1095399.

Carreras-Sospedra, Marc, Michael MacKinnon, Donald Dabdub, and Robert Williams. 2015. "Assessment of the Emissions and Energy Impacts of Biomass and Biogas Use in California." Agreement #11-307. California Air Resources Board.

National Renewable Energy Laboratory. 2021. "U.S. Life Cycle Inventory Database." NREL. https://www.lcacommons.gov/nrel/search.

NERC. 2020. "Glossary of Terms Used in NERC Reliability Standards." North American Electric Reliability Corporation. https://www.nerc.com/files/glossary_of_terms.pdf.

NREL. 2023. "2023 Annual Technology Baseline." Golden, CO: National Renewable Energy Laboratory. https://atb.nrel.gov/.

NREL. 2024. "2024 Annual Technology Baseline." Golden, CO: NREL. https://atb.nrel.gov/.

Rand, Joseph, Nick Manderlink, Will Gorman, Ryan Wiser, Julie Mulvaney Kemp, Seongeun Jeong, and Fritz Kahrl. 2024. "Queued Up: 2024 Edition." Lawrence Berkeley National Laboratory.

Seel, Joachim, and Andrew Mills. 2021. "Integrating Cambium Marginal Costs into Electric Sector Decisions: Opportunities to Integrate Cambium Marginal Cost Data into Berkeley Lab Analysis and Technical Assistance." https://doi.org/10.2172/1828856.

Borenstein, Severin, and James Bushnell. 2019. "Do Two Electricity Pricing Wrongs Make a Right? Cost Recovery, Externalities, and Efficiency." Working Paper Energy Institute WP 294R. Energy Institute at HAAS.

Sigrin, Benjamin, Michael Gleason, Robert Preus, Ian Baring-Gould, and Robert Margolis. 2016. "The Distributed Generation Market Demand Model (DGen): Documentation." NREL/TP-6A20-65231. Golden, CO: National Renewable Energy Laboratory. http://www.nrel.gov/docs/fy16osti/65231.pdf.

Siler-Evans, Kyle, Inês Lima Azevedo, and M. Granger Morgan. 2012. "Marginal Emissions Factors for the U.S. Electricity System." _Environmental Science & Technology_ 46 (9): 4742–48. https://doi.org/10.1021/es300145v.

United States Environmental Protection Agency. 2016. "Greenhouse Gas Inventory Guidance: Direct Emissions from Stationary Combustion Sources." United States Environmental Protection Agency. https://www.epa.gov/sites/default/files/2016-03/documents/stationaryemissions_3_2016.pdf.

Wiser, Ryan, Dev Millstein, Ben Hoen, Mark Bolinger, Will Gorman, Joe Rand, Galen Barbose, et al. 2024. "Land-Based Wind Market Report: 2024 Edition." Lawrence Berkeley National Laboratory. https://emp.lbl.gov/sites/default/files/2024-08/Land-Based%20Wind%20Market%20Report_2024%20Edition.pdf.

Wiser, Ryan, Robi Nilson, Joe Rand, and Ben Paulos. 2024. "Forecasts for Land-Based Wind Deployment in the United States: Wind Industry Survey Results." Lawrence Berkeley National Laboratory. https://eta-publications.lbl.gov/sites/default/files/2024-12/wind_industry_survey.pdf.

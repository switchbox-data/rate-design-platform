# ResStock 2024.2 Release: Technical Documentation

**Source**: resstock_2024.2.pdf  
**Pages**: 32 total pages  
**Date**: March 2024 (Updated January 2025)  
**Author(s)**: Philip R. White, Elaina Present, Rajendra Adhikari, Jes Brossman, Chioke Harris, Anthony Fontanini, Lixi Liu, Jeff Maguire, and Joseph Robertson  

---

## Table of Contents

1. [ResStock and Its Updates Since the 2022.1 Release](#1-resstock-and-its-updates-since-the-20221-release)
   - 1.1. [ResStock Tool Description](#11-resstock-tool-description)
   - 1.2. [Model Updates Since the 2022.1 Dataset Release](#12-model-updates-since-the-20221-dataset-release)
2. [Measure Package Composition and Logic](#2-measure-package-composition-and-logic)
   - 2.1. [Measure Package Selection Process](#21-measure-package-selection-process)
   - 2.2. [Measure Package Descriptions](#22-measure-package-descriptions)
3. [Outputs](#3-outputs)
   - 3.1. [Carbon Emissions](#31-carbon-emissions)
   - 3.2. [Utility Bill Calculations](#32-utility-bill-calculations)
   - 3.3. [Energy Burden](#33-energy-burden)
4. [Important Notes](#4-important-notes)
   - 4.1. [Heat Pump Modeling](#41-heat-pump-modeling)
   - 4.2. [Other Modeling Assumptions](#42-other-modeling-assumptions)
   - 4.3. [Data Sources and Availability](#43-data-sources-and-availability)
   - 4.4. [Quality Assurance and Quality Control](#44-quality-assurance-and-quality-control)
5. [Questions This Dataset Can and Cannot Answer](#5-questions-this-dataset-can-and-cannot-answer)
6. [Other ResStock Resources](#6-other-resstock-resources)

---

## 1. ResStock and Its Updates Since the 2022.1 Release

### 1.1. ResStock Tool Description

**ResStock™** is a tool that models the energy consumption of the U.S. housing stock. It is developed and maintained by the National Renewable Energy Laboratory (NREL). Its two main functions are (1) the creation of statistically representative building models that are informed by available data and (2) the physics-based simulation of these models using **EnergyPlus™** and **OpenStudio™**. The resulting ResStock dataset includes the energy consumption of each modeled dwelling unit and its respective dwelling unit characteristics (e.g., insulation levels, foundation type, wall construction) and household characteristics (e.g., setpoint properties, occupant information, household income). A **dwelling unit** is a single housing residence, such as one townhome, a single apartment within an apartment building, or a single-family detached home.

ResStock utilizes both actual meteorological year (AMY) historical weather files as well as typical meteorological year (TMY3) weather files when modeling building energy use. A key feature of ResStock is its ability to model the existing U.S. building stock with the addition of "what-if" scenarios including energy efficiency measures—for example, quantifying the energy savings if the entire housing stock was upgraded to R-60 attic insulation.

In the End-Use Savings Shapes (EUSS) datasets, this feature is used with various envelope, equipment, and electrification measures that are bundled together into measure packages. EUSS dataset users can obtain timeseries and annual energy use data from the existing, or "baseline", housing stock and each measure package on the ResStock datasets website. The 2022.1 dataset includes 10 measure packages across three weather years. This dataset release—**2024.2**—includes **16 measure packages** across two weather years and incorporates the ResStock improvements described in Section 1.2. A summary of ResStock timeseries data releases can be found in Table 1.

**Table 1. Summary of ResStock Datasets, Features, and Key Outputs**

| Feature / Output | 2021.1 | 2022.1 | 2024.2 |
| ----------------- | ------ | ------ | ------ |
| **Weather Year(s)** | TMY3, AMY2018 | TMY3, AMY2018, AMY2012 | TMY3, AMY2018 |
| **Approximate Housing Stock Year Represented** | 2018 | 2018 | 2018 |
| **Timeseries Results** | X | X | X |
| **Annual Results** | X | X | X |
| **Number of Measure Packages** | 0 | 10 | 15 |
| **Number of Dwelling Models in Baseline** | 550,000 | 550,000 | 550,000 |
| **Output Metrics (see Section 3)** | | | |
| Energy | X | X | X |
| Carbon Emissions | X | X | X |
| Utility Bills | | | X |
| Indoor Air Temperature | X | X | X |

*Note:* The document text in Section 2.1 states the 2024.2 dataset comprises 16 measure packages; the table above reflects the value as published in the source PDF (15). Table 1 describes all ResStock timeseries datasets released as of this report. Features and key output metrics that are included in each dataset are marked with an X or the relevant value.

#### Dwelling Unit and Household Characteristics Information

Dwelling unit and household characteristics are determined via ResStock's quota-based sampling of each characteristic's respective probability distributions. These distributions are created from datasets such as the U.S. Energy Information Administration's (EIA) Residential Energy Consumption Survey (RECS) and U.S. census data. In most recent datasets, the household characteristics include metrics useful for equity-centered analysis—income, area median income, federal poverty level, and tenure (renter/owner) status. Specific source information is contained in the housing characteristic files on this dataset's ResStock GitHub repository.

Calibration and validation of ResStock was completed in 2021 through the End-Use Load Profiles (EULP) project using EIA Form 861 monthly energy reporting and load research data as well as smart meter data from a handful of utilities across the United States.[^1] Still, discrepancies are possible between ResStock data and place-based housing stock knowledge or databases (such as tax assessors), particularly at smaller geographies, such as census tract and census block.

#### Sample Size Constraints

The smallest geography available in ResStock is the intersection of county and Public Use Microdata Area.[^2] ResStock captures what the existing U.S. housing stock looks like both in diversity and geospatial distribution, but the individual building samples do not correspond to any buildings that can be found on maps; they are only a statistical representation. However, it can be valuable to use ResStock data to estimate energy impacts of real buildings based on similar dwelling unit characteristics. Analyses using this dataset should estimate standard error for metrics of interest using the standard deviation divided by the square root of the number of samples (i.e., profiles or models). As discussed in Section 5.1.3 of Wilson et al.,[^3] for residential units, a good rule of thumb is to use **at least 1000 samples** to maintain 15% or less sampling discrepancy for common quantities of interest.

#### Representative Number of Dwellings for Each Modeled Dwelling Unit

Each modeled dwelling unit in this dataset is representative of a specific number of real-world dwellings. The specific number for this dataset is **252.3 dwellings**. When using data from individual buildings, this number can be used to scale up a model's timeseries or annual outputs to what a representative total would be. This scaling is not necessary if viewing data in the data viewer or if using pre-aggregated timeseries data files from the OEDI data repository.

---

### 1.2. Model Updates Since the 2022.1 Dataset Release

#### RECS 2020 Update

This 2024.2 dataset includes updated information from the recently released RECS 2020. Information included in RECS includes energy use patterns, housing unit characteristics, and household demographics. Household and dwelling characteristics in ResStock that were updated to include information from RECS 2020 include the following parameters:

- **Geometry**  
  Geometry Floor Area Bin; Geometry Attic Type; Geometry Garage; Geometry Space Combination  
- **Appliances**  
  Refrigerator; Misc Extra Refrigerator; Clothes Dryer; Clothes Washer; Clothes Washer Presence; Cooking Range; Dishwasher; Misc Freezer; Misc Hot Tub Spa; Misc Pool; Misc Pool Heater  
- **HVAC and Water Heating**  
  Water Heater Efficiency; Water Heater Fuel; Water Heater In Unit; Water Heater Location; HVAC Cooling Type; HVAC Heating Type; HVAC Heating Efficiency; HVAC Cooling Efficiency; HVAC Has Ducts; HVAC Has Shared System; HVAC Shared Efficiencies  
- **Envelope**  
  Windows; Roof Material  
- **Household Characteristics**  
  Cooling Setpoint; Cooling Setpoint Has Offset; Cooling Setpoint Offset Magnitude; Cooling Setpoint Offset Period; Heating Setpoint; Heating Setpoint Has Offset; Heating Setpoint Offset Magnitude; Heating Setpoint Offset Period  

More information can be found in the source report of this dataset's ResStock GitHub repository.

#### Geothermal Heat Pump Modeling

Improved geothermal heat pump modeling capabilities have been added to ResStock since the 2022.1 dataset release. ResStock had the capability to model geothermal heat pumps previously, but the model methodology was outdated and lacked documentation of the methodology, data sources, and assumptions. Recent funding from the U.S. Department of Energy Geothermal Technologies Office to revive geothermal heat pump modeling in OpenStudio-HPXML allowed the ResStock team to incorporate the modeling of geothermal heat pumps. During this funded effort, many aspects of the model and the default values were changed. Some of the major improvements include:

- Expanding the HPXML input schema with industry guidance  
- Alignment of fan and pump energy with ISO 13256-1  
- Single-speed performance curves updated to a new product line (ClimateMaster Tranquility TS)  
- Assignment of the undistributed ground temperature by interpolating from over 1,000 locations in North America  
- Allowing the ground thermal conductivity to vary by climate zone  
- Ground heat exchanger g-function integration with GHEDesigner (a software package to obtain g-functions of various vertical borehole/borefield configurations)  
- Expanding the available ground heat exchanger configurations to include rectangle, L, lopsided U, U, C, zoned rectangle, and open rectangle options  

In this dataset's geothermal heat pump measure packages, most geothermal heat pump properties are sized automatically (number of boreholes, borehole length, and loop flow) or have default values (grout type and conductivity, pipe properties, borehole spacing, borefield configuration, and soil diffusivity). Efficiency levels for geothermal heat pumps are tied to coefficient of performance (COP) and energy efficiency ratio (EER).

#### Variable Speed HVAC Updates

Assumptions for variable speed air conditioners, central heat pumps, and minisplit heat pumps have been updated since the release of the 2022.1 dataset. Data from the Northeast Energy Efficiency Partnerships (NEEP) Cold Climate Air Source Heat Pump List were used to update conversions from SEER/HSPF efficiency values to nominal COPs. Additionally, data from NEEP were used to update the variable speed performance curves used by EnergyPlus when modeling variable speed HVAC systems for ResStock models. These updates provided detailed heating and cooling performance data as inputs for variable speed HVAC models, including capacity retention fractions at different outdoor temperatures. For example, measure package 2 and measure package 3 have capacity retentions of **90% at 5°F**. These capacity retention settings paired with updated performance curves from the NEEP Cold Climate Air Source Heat Pump List allow for variable speed heat pump modeling that is more reflective of actual performance and operating conditions of cold climate heat pumps. For more information on how these changes were implemented within the OpenStudio-HPXML workflow, see the pull request description on the OpenStudio-HPXML GitHub repository.

---

## 2. Measure Package Composition and Logic

### 2.1. Measure Package Selection Process

The measure packages from this dataset were chosen based on feedback and comments from a diverse set of stakeholders and users of ResStock datasets. If there are measure packages or other technologies you would like to see in a future ResStock dataset, please e-mail the team at **ResStock@nrel.gov**.

The 2024.2 dataset comprises **16 measure packages**, described in Section 2.2.

### 2.2. Measure Package Descriptions

#### Measure Package 1: ENERGY STAR Air-to-Air Heat Pump With Electric Backup

**Summary**

- ENERGY STAR® air-to-air heat pump with electric resistance backup applied to all dwellings with and without ducts  
- Do not apply to high-rise dwelling units with shared HVAC. Additionally, ducted heat pump portions of measure package will not apply to any shared HVAC dwelling unit.  
- Only applies to dwelling units with an existing primary heating fuel of electricity, natural gas, propane, or fuel oil. Excludes homes with other heating fuels (e.g., wood) or no heating system in the baseline.  

**Technical Description**

- **Centrally ducted air-source heat pump (ASHP) SEER1 16, 9.2 HSPF1**  
  - *Applicability:* Dwellings with: ducts; primary heating fuel of electricity, natural gas, propane, or fuel oil; no shared HVAC  
  - *Performance:* Single-stage; sized to ACCA Manual S/J; backup heat provided by electric resistance, active when the heat pump cannot meet the load; capacity retention of 50% @ 5°F; partial space conditioning set to 100%  
- **Non-ducted minisplit heat pump (MSHP) SEER1 16, 9.2 HSPF1**  
  - *Applicability:* Dwellings with primary heating fuel of electricity, natural gas, propane, or fuel oil; and either no ducts, or all of: ducts, shared HVAC, in a building other than a multi-family high-rise (i.e. &lt;8 stories)  
  - *Performance:* Variable speed; sized to max load; backup heat provided by electric resistance; capacity retention of 50% @ 5°F; remove setpoint offsets; partial space conditioning set to 100%  

SEER2 and HSPF2 conversions are provided in Table 3 (Section 4.1).

---

#### Measure Package 2: High Efficiency Cold-Climate Air-to-Air Heat Pump With Electric Backup

**Summary**

- High efficiency cold-climate air-to-air heat pump with electric resistance backup applied to all dwellings with and without ducts  
- Do not apply to high-rise dwelling units with shared HVAC. Ducted heat pump portions do not apply to any shared HVAC dwelling unit.  
- Only applies if primary heating fuel is electricity, natural gas, propane, or fuel oil.  

**Technical Description**

- **Centrally ducted ASHP SEER1 20, 11 HSPF1**  
  - *Applicability:* Dwellings with ducts; primary heating fuel of electricity, natural gas, propane, or fuel oil; no shared HVAC  
  - *Performance:* Variable speed; sized to max load; backup heat by electric resistance; capacity retention of **90% @ 5°F**; remove setpoint offsets; partial space conditioning 100%  
- **Non-ducted MSHP SEER1 20, 11 HSPF1**  
  - Same applicability and performance as ducted (except duct / shared-HVAC logic as in Package 1).  

---

#### Measure Package 3: Ultra High Efficiency Air-to-Air Heat Pump With Electric Backup

**Summary**

- Ultra high efficiency air-to-air heat pump with electric resistance backup applied to all dwellings with and without ducts  
- Same applicability and exclusions as Package 2.  

**Technical Description**

- **Centrally ducted ASHP SEER1 24, 13 HSPF1** and **Non-ducted MSHP SEER1 24, 13 HSPF1**  
  - *Performance:* Variable speed; sized to max load; backup by electric resistance; capacity retention of 90% @ 5°F; remove setpoint offsets; partial space conditioning 100%  

---

#### Measure Package 4: ENERGY STAR Air-to-Air Heat Pump With Existing System as Backup

**Summary**

- ENERGY STAR air-to-air heat pump with the existing fossil fuel system providing backup heat applied to all dwellings with and without ducts and with existing primary heating fuel of natural gas, propane, or fuel oil.  
- Do not apply to units with shared HVAC.  

**Technical Description**

- **Centrally ducted ASHP SEER1 16, 9.2 HSPF1** with the existing heating system as an independent backup  
  - *Applicability:* Dwellings with fossil fuel heating (natural gas, propane, or fuel oil), ducts, and non-shared HVAC  
  - *Performance:* Single-stage; sized to ACCA Manual S/J; backup heat provided by existing heating when heat pump cannot meet load. For backup furnaces: compressor lockout 5°F, backup heating lockout 40°F; capacity retention 50% @ 5°F; partial space conditioning 100%  
- **Non-ducted MSHP SEER1 16, 9.2 HSPF1** with existing heating as backup  
  - *Applicability:* Dwellings with fossil fuel heating, without ducts, non-shared HVAC  
  - *Performance:* Variable speed; sized to max load; existing heating retained as backup; capacity retention 50% @ 5°F; remove setpoint offsets; partial space conditioning 100%  

---

#### Measure Package 5: ENERGY STAR Geothermal Heat Pump

**Summary**

- Geothermal heat pump applied to all dwellings with ducts  
- Do not apply to units with shared HVAC.  

**Technical Description**

- **Centrally ducted geothermal heat pump EER 20.5, COP 4.0**  
  - *Applicability:* Dwelling with ducts and non-shared HVAC  
  - *Performance:* Sized to max load; no backup system; partial space conditioning 100%  
  - *Borefield:* Loop configuration vertical borehole; borefield configuration rectangle; loop length calculated per ACCA Manual J (range 79 ft–500 ft); number of boreholes calculated during sizing; loop flow rates autosized (3 × max heating/cooling capacity in tons, min 3 gal/min); borehole diameter 5 in; borehole spacing 16.4 ft; grout type standard conductivity (0.75 Btu/hr-ft-°F); pipe type standard conductivity (0.23 Btu/hr-ft-°F); pipe loop diameter 1.25 in  

---

#### Measure Package 6: ENERGY STAR Air-to-Air Heat Pump With Electric Backup + Light Touch Envelope

**Summary**

- Measure Package 1 (ENERGY STAR ASHP with electric backup) plus:  
- Attic floor insulation increased to IECC-Residential 2021 levels for dwelling units with vented attics and lower-performing insulation  
- General air sealing: 30% total reduction in ACH50 for dwelling units with greater than 10 ACH50  
- Same exclusions as Package 1 (no high-rise shared HVAC for ducted portions; primary heating fuel electricity, natural gas, propane, or fuel oil).  

**Technical Description**

- Apply Measure Package 1; then if it applies, also apply:  
  - **Attic floor insulation** (vented attics only): R-30 in IECC 2004 climate zone 1A (if current &lt; R-30); R-49 in zones 2A, 2B, 3A, 3B, 3C (if current &lt; R-49); R-60 in zones 4A–7B (if current &lt; R-60)  
  - **Air leakage reduction:** 30% whole-home reduction in ACH50 for units with &gt; 10 ACH50  

---

#### Measure Package 7: High Efficiency Cold-Climate Air-to-Air Heat Pump With Electric Backup + Light Touch Envelope

**Summary**

- Measure Package 2 plus same light touch envelope (attic insulation to IECC 2021 levels, 30% ACH50 reduction where &gt; 10 ACH50). Same applicability and exclusions.  

**Technical Description**

- Measure Package 2 + same attic insulation and air sealing logic as Package 6.  

---

#### Measure Package 8: Ultra High Efficiency Air-to-Air Heat Pump With Electric Backup + Light Touch Envelope

**Summary**

- Measure Package 3 plus same light touch envelope. Same applicability and exclusions.  

**Technical Description**

- Measure Package 3 + same attic insulation and air sealing logic as Package 6.  

---

#### Measure Package 9: ENERGY STAR Air-to-Air Heat Pump With Existing System as Backup + Light Touch Envelope

**Summary**

- Measure Package 4 plus same light touch envelope. Only applies if primary heating fuel is natural gas, propane, or fuel oil. Do not apply to shared HVAC.  

**Technical Description**

- Measure Package 4 + same attic insulation and air sealing logic as Package 6.  

---

#### Measure Package 10: ENERGY STAR Geothermal Heat Pump + Light Touch Envelope

**Summary**

- Measure Package 5 (geothermal) plus same light touch envelope. Only applies if primary heating fuel is electricity, natural gas, propane, or fuel oil. Do not apply to shared HVAC.  

**Technical Description**

- Measure Package 5 + same attic insulation and air sealing logic as Package 6.  

---

#### Measure Package 11: ENERGY STAR Air-to-Air Heat Pump With Electric Backup + Light Touch Envelope + Full Electrification

**Summary**

- Measure Package 6 plus full appliance electrification with efficiency:  
  - Heat pump water heaters (HPWHs) for less efficient electric and all gas/propane/fuel oil water heaters  
  - Electric ENERGY STAR dryer for gas or propane dryers  
  - Electric induction cooktop and electric oven for gas or propane ranges  
  - Electric pool heater for natural gas pool heaters; electric spa heater for natural gas spa heaters  
- Same exclusions as Package 6.  

**Technical Description**

- Measure Package 6; then if heat pump from Package 6 applies, also apply:  
  - HPWH 50 gal, 3.45 UEF for ≤3 bedrooms (electric UEF &lt; 3.45 or gas/propane/fuel oil water heater)  
  - HPWH 66 gal, 3.35 UEF for 4 bedrooms (electric UEF &lt; 3.35 or gas/propane/fuel oil)  
  - HPWH 80 gal, 3.45 UEF for 5 bedrooms (electric UEF &lt; 3.45 or gas/propane/fuel oil)  
  - ENERGY STAR electric dryer CEF 3.93 if existing dryer is natural gas or propane  
  - Electric induction cooktop and electric oven if existing range is natural gas or propane  
  - Electric pool heater if existing pool heater is natural gas  
  - Electric spa heater if existing spa heater is natural gas  

---

#### Measure Package 12: High Efficiency Cold-Climate Air-to-Air Heat Pump With Electric Backup + Light Touch Envelope + Full Electrification

**Summary**

- Measure Package 7 + same full appliance electrification as Package 11.  

**Technical Description**

- Measure Package 7 + same HPWH, dryer, range, pool heater, spa heater electrification logic as Package 11.  

---

#### Measure Package 13: Ultra High Efficiency Air-to-Air Heat Pump With Electric Backup + Light Touch Envelope + Full Electrification

**Summary**

- Measure Package 8 + same full appliance electrification as Package 11.  

**Technical Description**

- Measure Package 8 + same electrification logic as Package 11.  

---

#### Measure Package 14: ENERGY STAR Air-to-Air Heat Pump With Existing System as Backup + Light Touch Envelope + Full Electrification

**Summary**

- Measure Package 9 + same full appliance electrification. Only applies if primary heating fuel is natural gas, propane, or fuel oil. Do not apply to shared HVAC.  

**Technical Description**

- Measure Package 9 + same electrification logic as Package 11.  

---

#### Measure Package 15: ENERGY STAR Geothermal Heat Pump + Light Touch Envelope + Full Electrification

**Summary**

- Measure Package 10 + same full appliance electrification. Only applies if primary heating fuel is electricity, natural gas, propane, or fuel oil. Do not apply to shared HVAC.  

**Technical Description**

- Measure Package 10 + same electrification logic as Package 11.  

---

#### Measure Package 16: Envelope Only — Light Touch Envelope

**Summary**

- Attic floor insulation increased to IECC-Residential 2021 levels for dwelling units with vented attics and lower-performing insulation  
- General air sealing: 30% total reduction in ACH50 for dwelling units with greater than 10 ACH50  

**Technical Description**

- **Attic floor insulation** (vented attics only): R-30 in zone 1A (if &lt; R-30); R-49 in 2A, 2B, 3A, 3B, 3C (if &lt; R-49); R-60 in 4A–7B (if &lt; R-60)  
- **Air leakage reduction:** 30% whole-home reduction in ACH50 where ACH50 &gt; 10  

---

## 3. Outputs

### 3.1. Carbon Emissions

Four sets of carbon emission impact results are included in this dataset release. These carbon emission results are not available in the data viewer's graphical interface or customizable timeseries data downloads, but are present in the metadata annual results, individual dwelling model timeseries files, and pre-aggregated timeseries files (e.g., timeseries for a geography available on the OEDI data repository [AMY2018, TMY3]).

#### Carbon Emissions Associated With On-Site Fossil Fuel Combustion

For calculating carbon emissions related to the on-site consumption of natural gas, propane, and fuel oil, this dataset uses emission factor values from Table 7.1.2(1) of draft PDS-01 of BSR/RESNET/ICCC 301 Addendum B, CO2 Index. These values include both combustion and pre-combustion (e.g., methane leakage from natural gas) CO2e (carbon dioxide equivalent) emissions:

- **147.3 lb/MMBtu** (228.5 kg/MWh) for natural gas  
- **177.8 lb/MMBtu** (275.8 kg/MWh) for propane  
- **195.9 lb/MMBtu** (303.9 kg/MWh) for fuel oil  

#### Carbon Emissions Associated With Electricity Consumption

For CO2e emissions from changes in electricity consumption in buildings, this dataset incorporates long-run marginal emission factors from **NREL's Cambium 2022** database. It is recommended to compare multiple standard scenarios to understand the range of potential emission outcomes from changes in electricity consumption. Table 2 briefly describes the standard scenarios used in this dataset.

**Table 2. Description of Carbon Emission Scenarios**

| NREL Standard Scenario | Start Year | Levelization Period (3% Discount Rate) |
| ----------------------- | ---------- | -------------------------------------- |
| MidCase                 | 2025       | 15 years                               |
| LowRECost               | 2025       | 15 years                               |
| HighRECost              | 2025       | 15 years                               |
| MidCase                 | 2025       | 25 years                               |

The NREL Standard Scenarios represent potential futures of the electric grid; the dataset includes three of those scenarios—Low Renewable Energy Cost (LowRECost), Mid-Case, and High Renewable Energy Cost (HighRECost). The emissions values represent a single year of emissions—weighted-average year over the levelization period, weighted toward years closer to the start year through the discount rate. Each emissions factor is applied at the Generation Emission Assessment Region geographic region (Cambium documentation Section 5.11).[^4] Information on the Standard Scenarios used in this dataset can be found in the Cambium 2022 Standard Scenarios Report.[^5] Cambium data can be viewed using [NREL's Cambium Scenario Viewer](https://scenarioviewer.nrel.gov/).

---

### 3.2. Utility Bill Calculations

Utility bill calculations for this dataset use the same methodology used in the 2024.1 dataset.[^6] A summary is below.

Utility costs are calculated using ResStock energy consumption results, fixed customer costs, and state-average volumetric costs from EIA. When possible, specific energy rates should be used to calculate utility bills instead of the pre-calculated bills based on state-averaged rates.

#### Residential Electricity Bills

Data from NREL's Utility Rate Database (obtained November 2021) were used to calculate the customer-weighted average fixed monthly electricity charge across all utilities in the database:

**Equation 1. Customer-weighted average fixed monthly electric charge**

$$
\text{Fixed cost} = \frac{\sum (\text{Fixed electric charge} \times \text{Number of customers})}{\sum \text{Number of customers}}
$$

This came out to approximately **$10/customer/month**, or **$120/customer/year**.

2022 EIA state average residential electricity data were used (total revenue in thousands of dollars, total sales in MWh, total customers). The average variable electricity rate for each state is:

**Equation 2. Average variable electricity rate**

$$
\text{Variable electric rate} = \frac{\text{Total revenue} - (\text{Fixed cost} \times \text{Number of customers})}{\text{Total sales}}
$$

This resulted in a per-unit residential utility customer rate for each state ranging from about $0.10/kWh (Washington) to $0.23/kWh (Maine).

The full-year electricity bill for each modeled dwelling unit is:

**Equation 3. Full year electricity bill**

$$
\text{Full year electricity bill} = \$120 + (\text{Electricity consumption} \times \text{Variable electric rate})
$$

#### Residential Natural Gas Bills

For natural gas, the American Gas Association value of **$11.25/customer/month**[^7] was used for the fixed portion (customer charge). 2022 EIA data by state (price, consumption, number of customers) were used to calculate the volumetric rate:

**Equation 4. Volumetric natural gas rate**

$$
\text{Volumetric natural gas rate} = \frac{(\text{Consumption} \times \text{Price}) - (\text{Fixed cost} \times \text{Number of customers})}{\text{Total sales}}
$$

The results ranged from about $0.49/therm (Idaho) to $1.64/therm (Florida).

**Equation 5. Full year natural gas bill**

$$
\text{Full year natural gas bill} = \$135 + (\text{Natural gas consumption} \times \text{Volumetric natural gas rate})
$$

Dwelling unit models without natural gas consumption have no natural gas bill in the results (including no customer cost), whether in the baseline or because a measure package eliminated all natural gas consumption.

#### Residential Propane and Residential Fuel Oil Bills

Weekly volumetric rate data for the 2021–2022 winter from EIA were used for residential fuel oil and residential propane, averaged over available weeks. When state-level data were not available, data from the state's Petroleum Administration for Defense Districts (PADD) region were used; when PADD data were not available, U.S. national average values were used. Rates ranged from about $1.79/gallon (Idaho) to $4.50 (Florida) for propane and from $2.54 (Nebraska) to $3.31 (Delaware) for fuel oil. Propane and fuel oil bills are calculated by multiplying each dwelling unit model's fuel consumption by the volumetric rate for that fuel and state.

---

### 3.3. Energy Burden

Energy burden is calculated as the ratio of full-year energy bills to full-year income. Full-year energy bills are the outputs from Section 3.2. Full-year income is derived from the ResStock income field (a range or bin) by converting it to a single numeric value per dwelling unit in a new field called **representative_income**.

Representative income is determined from lookup tables derived from **2019 5-year American Community Survey** data.[^8] The lookup tables provide the weighted median income for a given set of characteristics: income bin, occupants, FPL, tenure, building type, and geography (starting with the intersection of PUMA and county). Each dwelling sample is assigned a representative_income by matching these characteristics; the process starts at the highest geographic resolution and steps down until all dwelling samples have a numeric income.

Vacant dwelling units are not assigned an income bin in ResStock; their representative_income is set to zero, which would make energy burden undefined. To avoid undefined values, the energy burden field for vacant units is filled with **locational averages** (by county if available, otherwise by state).

---

## 4. Important Notes

### 4.1. Heat Pump Modeling

#### Backup Heating and Capacity Retention Fractions

Four of the five heat pump model types in this dataset's measure packages include supplemental backup heating; only the geothermal heat pump has no backup. For measure packages 1, 2, 3, 6, 7, 8, 11, 12, and 13 the backup is electric resistance. For measure packages 4, 9, and 14 the backup is the existing heating system. For electric resistance backup and non-furnace existing backup, the backup turns on when the heat pump cannot meet the full heating load. For heat pumps with furnaces as backup: compressor lockout below 5°F, furnace lockout above 40°F; the backup furnace runs when the heat pump cannot meet the load and outdoor temperature is between 5°F and 40°F.

Air-to-air heat pumps in this dataset have **capacity retention fraction** values at 5°F outdoor temperature. The capacity fraction is the percentage of the heat pump's rated capacity available at that temperature. A higher capacity fraction allows a smaller rated unit (e.g. "cold-climate" in Package 2). The tradeoff is that for a given HSPF1, a higher capacity retention fraction implies a lower COP at low temperatures.

#### Heat Pump Sizing

Two sizing methodologies are used. **Ducted ENERGY STAR air-to-air heat pumps** are sized to **ACCA Manuals S and J**.[^9] All other heat pumps are sized to the **maximum load** (heating or cooling) of the dwelling. ACCA Manuals S and J size to cooling load with oversizing allowances: for single-stage ENERGY STAR ASHPs, 15% oversizing and in cold/dry climates an extra ton; for variable speed ASHPs, 30% oversizing.

#### Duct Sizing Limitations for Heat Pump Retrofits

Ducted heat pump measure packages represent retrofits with an existing duct system. In a typical retrofit the duct size would not change; in ResStock there is a limitation such that duct system size and maximum airflow rates change with the heat pump installation requirements.

#### HVAC Equipment Rating Systems

ResStock uses legacy ratings—**HSPF1** and **SEER1**—as shown in Section 2.2. DOE has replaced these with **HSPF2** and **SEER2** (in effect for most equipment from 2023). The performance values in Section 2.2 are converted to the current rating system in Table 3.

**Table 3. Measure Package Converted Heat Pump Rated Performance**

| Measure Package     | System Type | SEER2 | HSPF2 |
| ------------------- | ----------- | ----- | ----- |
| 1, 4, 6, 9, 11, 14 | ASHP        | 15.2  | 7.8   |
|                     | MSHP        | 16    | 8.3   |
| 2, 7, 12            | ASHP        | 19    | 9.4   |
|                     | MSHP        | 20    | 9.9   |
| 3, 8, 13            | ASHP        | 22.8  | 11.1  |
|                     | MSHP        | 24    | 11.7  |
| 5, 10, 15           | GHP^a^      | —     | —     |

^a^ Geothermal heat pump (rated by EER). Calculated using conversion factors in Table 4.4.4.1(1) in MINHERS Addendum 71f: SEER2 and HSPF2 Conversions.

---

### 4.2. Other Modeling Assumptions

#### IECC Climate Zone Definitions and Attic Insulation

This dataset uses **2004 IECC** climate zone definitions rather than 2021. As a result, some dwelling units receive higher attic insulation in the Light Touch envelope than under 2021 definitions: e.g. units in counties redefined from 2A to 1A receive R-49 instead of IECC 2021 R-30; units in counties redefined from zone 4 to 3 receive R-60 instead of IECC 2021 R-49.

#### Exclusion of Some Shared HVAC System Dwellings and High-Rise Dwellings

All **high-rise** dwelling units with **shared HVAC** have **no** measure packages from this dataset applied. High-rise shared-HVAC dwellings typically have unique HVAC designs. In addition, no dwelling units with shared HVAC receive geothermal heat pump packages, ducted air-to-air variable speed heat pump packages, or ENERGY STAR air-to-air heat pump with existing system as backup packages.

---

### 4.3. Data Sources and Availability

ResStock only uses national or regionally representative, vetted sources. If a topic or residential housing characteristic has not been covered by such a survey, ResStock cannot include it. Key data sources are given in Table 2 of Wilson et al.[^10] Examples of topics not included or poorly represented due to data limitations include, but are not limited to:

- Housing in Alaska, Hawaii, U.S. territories, Tribal lands, HUD/Section 8 housing, manufactured housing/mobile homes, and rural areas  
- Structural or energy performance-related maintenance required before efficiency or electrification upgrades (e.g. health and safety, electrical panel upgrades, rehabilitation)  

### 4.4. Quality Assurance and Quality Control

Baseline and measure package annual and timeseries results were reviewed by NREL subject matter experts in four stages:

1. **Core upgrade definition:** The 2024.2 dataset upgrade definition file was reviewed to ensure correct applicability of all measure packages (Section 2).  
2. **Small-scale (500-dwelling) run:** Applicability, energy savings, emissions savings, and bill savings per measure package were reviewed.  
3. **Medium-scale (30,000-dwelling) run:** Deeper review: total annual savings (energy, electricity, natural gas, propane, fuel oil), heating/cooling/water heating savings, heat pump backup and key metrics (sizes, loads, unmet hours), nationwide and by cross-sections (e.g. climate zone, heating/cooling type). Timeseries and load shapes for summer and winter peak days at national and ISO/RTO and climate zone level. Heat pump COP curves vs. outdoor temperature were checked (ASHPs vs. NEEP Cold Climate list; geothermal constant deep ground temperature) using IECC climate zone aggregates in cold climates (zones 6 and 7).  
4. **Production-scale (550,000-dwelling) run:** Results were compared to the medium-scale run (annual and timeseries) and to other recent datasets (e.g. 2024.1).  

Not every data point or aggregation was independently reviewed. Errors may exist; users are encouraged to contact **ResStock@nrel.gov** if they encounter problems.

---

## 5. Questions This Dataset Can and Cannot Answer

### Analysis Questions That Can Be Answered

These can be addressed by processing, distilling, and visualizing this dataset.

1. **Housing makeup and current energy consumption**  
   How many homes are heated by electricity vs. gas? How leaky or well insulated are they? How prevalent are different housing types or vintages and how do their energy consumptions differ? How do low-income and renter-occupied units differ? What fuel types and end uses offer the largest efficiency and electrification opportunities?

2. **Technology potentials and what-if scenarios**  
   What is the range of savings per dwelling if measure packages X, Y, or Z are implemented? What are total expected savings for a program or community? What are the energy, carbon, and utility bill impacts of envelope improvements vs. heat pumps (electric or existing backup) vs. combinations? What are long-term carbon implications of electrification under different grid futures (Cambium emission factors)?

3. **Addressable with timeseries and customization**  
   Does residential electrification shift a region from summer-peaking to winter-peaking? What is the change in peak demand from electrification or efficiency? How do higher efficiency equipment or envelope + equipment upgrades affect peak demand? What are bill impacts under state average utility rates?

### Out-of-Scope Questions That Cannot Be Answered With ResStock Alone

These require additional research or data beyond ResStock. ResStock does not reference specific policies, rebates, or incentives; users must map programs to ResStock data.

- What is the potential for energy demand response or for rooftop/community solar to offset consumption and bills?  
- Given a program budget, what are tradeoffs between targeting low-income households vs. no targeting?  
- How many homes qualify for WAP by federal poverty level, and what savings are available from WAP-qualified measures?  
- How many homes qualify for IRA Section 50122 rebates by area median income, what rebates can they get, and what savings from measure packages X, Y, or Z?  
- Can appliance replacements alone qualify for IRA Section 50121 whole-home performance-based rebates?  
- How might different income groups use financing (e.g. pay-as-you-save) or federal/state/local rebates to make retrofits more cost-effective?  
- Do utility bill savings change under time-of-use electricity rates?  

---

## 6. Other ResStock Resources

- **ResStock Website:** <https://resstock.nrel.gov/>  
- **ResStock Documentation:** <https://resstock.readthedocs.io/en/latest/index.html>  
- **ResStock and ComStock YouTube Training Series:** <https://www.youtube.com/playlist?list=PLmIn8Hncs7bEY...> [→ See original PDF for full playlist URL if needed]  
- **Highlighted Publications:** <https://resstock.nrel.gov/page/publications>  
- **ComStock website:** <https://comstock.nrel.gov/>  

For specific questions, contact **ResStock@nrel.gov**. ResStock focuses on residential buildings; **ComStock™** focuses on commercial buildings.

---

## Footnotes

[^1]: Wilson, Eric J.H., Andrew Parker, Anthony Fontanini, Elaina Present, Janet L. Reyna, Rajendra Adhikari, Carlo Bianchi, et al. 2022. *End-Use Load Profiles for the U.S. Building Stock*. Washington, DC: U.S. Department of Energy. <https://www.nrel.gov/docs/fy22osti/80889.pdf>

[^2]: Public Use Microdata Areas are defined by the U.S. census to contain at least 100,000 people.

[^3]: Wilson et al. 2022, op. cit.

[^4]: Gagnon, Pieter, Will Frazier, Wesley Cole, and Elaine Hale. 2021. *Cambium Documentation: Version 2021*. Golden, CO: National Renewable Energy Laboratory. NREL/TP-6A40-81611. <https://www.nrel.gov/docs/fy22osti/81611.pdf>

[^5]: Gagnon, Pieter, Maxwell Brown, Dan Steinberg, and Patrick Brown. 2023. *2022 Standard Scenarios Report: A U.S. Electricity Sector Outlook*. Golden, CO: National Renewable Energy Laboratory. NREL/TP-6A40-84327. <https://www.nrel.gov/docs/fy23osti/84327.pdf>

[^6]: Present, Elaina, Philip R. White, Chioke Harris, Rajendra Adhikari, Yingli Lou, Lixi Liu, Anthony Fontanini, Christopher Moreno, Joseph Robertson, and Jeff Maguire. 2024. *ResStock Dataset 2024.1 Documentation*. Golden, CO: National Renewable Energy Laboratory. NREL/TP-5500-88109. <https://www.nrel.gov/docs/fy24osti/88109.pdf>

[^7]: American Gas Association. 2015. *Natural Gas Utility Rate Structure: The Customer Charge Component – 2015 Update*. Washington, DC. EA 2015-03. <https://www.ourenergypolicy.org/wp-content/uploads/2016/01/ea_2015-03_customercharge2015.pdf>

[^8]: Representative_income and income bins are in 2019 USD; utility bills use nominal rates from 2021–2022. Converting one to the other for real-dollar alignment is optional; the default energy burden change pre- and post-upgrade still gives a relative measure of improvement.

[^9]: 2016 Manual J (8th edition) and 2014 Manual S (2nd edition).

[^10]: Wilson et al. 2022, op. cit.

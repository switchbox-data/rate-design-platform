# Finding residential electricity delivery data for New York utilities

**EIA Form 861 does capture delivery-level volumes**, contrary to initial appearances — and several complementary state-level datasets make New York one of the better-documented restructured markets for this specific question. The key is Schedule 4C of Form 861, which records delivery-only MWh separately from bundled retail sales. Summing Schedule 4A (bundled) and 4C (delivery-only) produces **total residential kWh delivered** through each utility's distribution system, regardless of commodity supplier. Below is a source-by-source breakdown of exactly where this data lives and how to use it.

## EIA Form 861 captures delivery volumes through Schedule 4C

The form's Schedule 4 is split into four parts that together account for restructured markets. **Schedule 4A** covers "Full Service — Energy and Delivery Service (Bundled)" and reports MWh, revenue, and customer counts by state, balancing authority, and customer class. **Schedule 4C** covers "Delivery Only Service" — MWh delivered by the distribution utility to customers whose commodity comes from an ESCO or power marketer. The instructions explicitly use the term "megawatthours delivered." Schedule 4B captures the mirror image: energy-only MWh reported by the ESCO itself. Schedule 4D handles Texas-style full-deregulation bundled marketers.

The critical formula is: **Total Delivery = Schedule 4A MWh + Schedule 4C MWh.** This gives total kWh physically flowing through the utility's wires, by customer class (Residential, Commercial, Industrial, Transportation), for each utility and state.

The annual data files at [eia.gov/electricity/data/eia861](https://www.eia.gov/electricity/data/eia861/) include a "Sales to Ultimate Customers" file within the ZIP download (e.g., `f8612023.zip`). This file contains a **service_type field** distinguishing "Bundled," "Energy Only," and "Delivery Only" records. Any utility with delivery-only customers must file the full EIA-861 long form — the short form (861S) cannot be used. Annual data is available from **1990 through 2023** (most recent final release), with a typical lag of 12–18 months. The EIA also describes this file as containing "revenue, sales (in megawatthours), and customer count of electricity delivered to end-use customers."

One important caveat: the monthly EIA-861M data files only publish **bundled service** data publicly each month. Non-bundled (delivery-only and energy-only) data is considered protected at the monthly level and is released only in a separate annual file after final data publication. So monthly delivery-level tracking from EIA is not feasible.

## NYSERDA's Patterns & Trends report is the most convenient pre-compiled source

The **NYSERDA "Patterns and Trends" report** — specifically **Appendix F** — is arguably the single most accessible source for this question. Published roughly annually, the most recent edition covers 2009–2023 (released December 2025). Appendix F contains three critical table sets derived from EIA-861 data:

- **Table F-2a–c**: Electricity Customers by Sector by Utility, with columns for **Bundled**, **Delivery**, and **Total** customers
- **Table F-3a–c**: Electricity Sales by Sector by Utility (GWh), following the same Bundled/Delivery/Total breakdown
- **Tables F-3d–f**: Electricity Sales by Power Marketers (the ESCO/4B side)

For example, the 2021 residential data shows Con Edison with **2,482,139 bundled** and **458,590 delivery-only** customers for a total of **2,940,729 residential delivery customers**. The corresponding GWh figures follow the same format. This is available for all six major NY IOUs (Con Edison, National Grid, NYSEG, RG&E, Central Hudson, Orange & Rockland) plus LIPA and municipal utilities, annually from **1997 through 2023** across successive editions.

The full report and appendices are downloadable as PDFs from [nyserda.ny.gov/About/Publications/Energy-Analysis-Reports-and-Studies/Patterns-and-Trends](https://www.nyserda.ny.gov/About/Publications/Energy-Analysis-Reports-and-Studies/Patterns-and-Trends). Note that Appendix F-1 (prices) is bundled-only, but F-2 and F-3 explicitly include delivery totals. A dashboard version also exists on NYSERDA's website.

## NY PSC migration reports and the Utility Energy Registry fill remaining gaps

The **Electric Retail Access Migration Year End Summary Reports**, filed under NY DPS Matter **19-00157**, are the most targeted state source. Published annually by DPS Staff since 1999, these reports show for each IOU and each service class (Residential, Small Non-Residential, Large Non-Residential):

- Total eligible customers and total eligible load (kWh)
- Customers and load on ESCO supply vs. utility full service
- Migration percentages by customer count and by load volume

The "total eligible" figures represent total delivery customers and delivery kWh. Access these through the DPS Document and Matter Management system at [documents.dps.ny.gov](https://documents.dps.ny.gov/public/MatterManagement/CaseMaster.aspx?MatterCaseNo=19-00157). Monthly filings exist under Matter **94-E-0952** but are redacted for ESCO-specific confidential data; the year-end summaries are public.

The **Utility Energy Registry (UER)** on data.ny.gov offers a different angle: **monthly** electricity usage data by utility, by sector (Residential, Small Commercial, Other), at the **county, community, and ZIP code** level. Established by PSC Order in Case 17-M-0315 (April 2018), the UER requires distribution utilities to report community-level energy usage. Because the distribution utility meters all customers regardless of commodity supplier, and because the stated purpose is community GHG inventories and energy planning, the UER almost certainly captures total delivery volumes — not just utility commodity sales. Data runs from **2016 to present** across two protocol periods:

- 2016–2021 protocol: datasets [47km-hhvs](https://data.ny.gov/Energy-Environment/Utility-Energy-Registry-Monthly-County-Energy-Use-/47km-hhvs) (county), [m3xm-q3dw](https://data.ny.gov/Energy-Environment/Utility-Energy-Registry-Monthly-Community-Energy-U/m3xm-q3dw) (community), [tzb9-c2c6](https://data.ny.gov/Energy-Environment/Utility-Energy-Registry-Monthly-ZIP-Code-Energy-Us/tzb9-c2c6) (ZIP)
- 2021+ modified protocol: datasets [46pe-aat9](https://data.ny.gov/Energy-Environment/Utility-Energy-Registry-Monthly-County-Energy-Use-/46pe-aat9) (county), [4txm-py4p](https://data.ny.gov/Energy-Environment/Utility-Energy-Registry-Monthly-Community-Energy-U/4txm-py4p) (community), [g2x3-izm4](https://data.ny.gov/Energy-Environment/Utility-Energy-Registry-Monthly-ZIP-Code-Energy-Us/g2x3-izm4) (ZIP)

Privacy screening suppresses sectors with fewer than 15 accounts or where one account exceeds 15% of total usage, but aggregation to the utility level eliminates most suppression.

## FERC Form 1 does not report delivery volumes by customer class

This is a notable gap. FERC Form 1 Pages 300–301 report electric operating revenues and MWh by customer class, but only for **Accounts 440–448**, which cover bundled (full-service) sales. Per **FERC Order No. 715** (2008, Docket RM08-5-000), delivery-only revenues are recorded under **Account 456 (Other Electric Revenues)** as a single dollar figure with **no MWh breakdown and no customer class detail**. The order explicitly states: "Delivery-only revenues shall be recorded as Other Electric Revenues (Account 456), while sales of electricity shall be recorded on a full-service basis."

This means Page 304 ("Sales of Electricity by Rate Schedule") for a utility like Con Edison shows only bundled sales MWh by rate schedule — significantly understating total residential delivery for any utility with substantial ESCO migration. The NY PSC requires utilities to file an "NYPSC Modified" version of FERC Form 1 as their annual report (accessible via DPS document system under matter numbers 10-01660 through 10-01665 for the six major IOUs), which may contain additional NY-specific schedules with delivery data, but the standard FERC Form 1 template does not.

## NYISO zonal load data works as a territory-level proxy but lacks class granularity

NYISO publishes **hourly and 5-minute actual load data by zone** back to approximately 2001, available at [nyiso.com/load-data](https://www.nyiso.com/load-data) and the MIS public archive at [mis.nyiso.com/public/](http://mis.nyiso.com/public/). The 11 NYISO load zones map roughly to utility service territories (Zone J ≈ Con Edison NYC, Zone K ≈ PSEG Long Island, Zone A ≈ National Grid Buffalo, Zone F ≈ National Grid Albany). The annual **Gold Book** ([nyiso.com/gold-book-resources](https://www.nyiso.com/gold-book-resources)) compiles historical annual energy and peak demand by zone going back 20+ years.

The limitation is fundamental: **no customer class breakdown exists**. NYISO measures load at the transmission delivery point, capturing total system consumption in each zone without distinguishing residential from commercial or industrial. This data is useful for cross-validating utility-level totals but cannot answer the residential delivery question directly.

## Recommended approach for building a historical time series

For the specific goal of **total residential kWh delivered by each NY utility, annually**, the optimal strategy combines sources by strength:

| Source                                  | What it provides                                                     | Coverage              | Access                    |
| --------------------------------------- | -------------------------------------------------------------------- | --------------------- | ------------------------- |
| **EIA Form 861 (4A + 4C)**              | Definitive delivery MWh by utility, by class, Bundled/Delivery/Total | 1990–2023, annual     | Download ZIP from eia.gov |
| **NYSERDA Appendix F-3**                | Pre-compiled Bundled/Delivery/Total GWh by utility, by class         | 1997–2023, annual     | PDF from nyserda.ny.gov   |
| **NY DPS Migration Reports (19-00157)** | Total eligible load and ESCO split by utility, by class              | 1999–present, annual  | DPS document system       |
| **Utility Energy Registry**             | Monthly delivery MWh by utility, by sector, by geography             | 2016–present, monthly | data.ny.gov (Socrata API) |
| **NYISO Gold Book / Zonal Load**        | Total zonal energy (no class breakdown)                              | 2001–present          | nyiso.com                 |

The **EIA 861 raw files** are the most authoritative and longest-running source. Filter for New York utilities, sum the Bundled and Delivery Only service types for each utility's Residential sector, and the result is total residential delivery MWh. **NYSERDA Appendix F** has already done this work and presents it in clean tables — start there for quick analysis. The **DPS Migration Reports** add the ESCO penetration dimension. The **UER** uniquely offers monthly and sub-utility geographic granularity from 2016 onward. FERC Form 1 should be avoided for this purpose due to its structural inability to capture delivery volumes by class.

## This platform's EIA-861 stats (4A + 4C)

The parquet we build in **`data/eia/861/`** (script `fetch_electric_utility_stat_parquets.py`) and publish to **`s3://data.sb/eia/861/electric_utility_stats/`** uses PUDL's **`core_eia861__yearly_sales`** table. That table keeps EIA's **service_type** (bundled, delivery, energy) as part of its primary key. Our script **does not filter on service_type**; it aggregates over all rows per (utility, state, year). So the resulting **residential (and other class) sales MWh and customer counts are total delivery** — Schedule 4A + Schedule 4C — i.e. all customers and energy delivered through the utility's distribution system. This was verified by comparing PUDL's ConEd 2021 residential rows by service_type (bundled 2,482,139 + delivery 458,590 = 2,940,729) to the NYSERDA Appendix F totals cited above.

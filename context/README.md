# Context index

Reference docs and research notes for agents. **When you add or remove a file under `context/`, update this index.**

See **AGENTS.md → Reference context** for conventions (what goes in `papers/`, `docs/`, `domain/`, `tools/`) and when agents should read from here.

## domain/

Research notes on the domain: rate design, LMI programs, policy by state.

| File                   | Purpose                                                    |
| ---------------------- | ---------------------------------------------------------- |
| lmi_discounts_in_ny.md | NY utility low-income discount programs (e.g. EAP, EEAP)   |
| lmi_discounts_in_ri.md | RI utility low-income programs (RIE rates, LIDR+ proposal) |

## tools/

Research notes on tools, data, or implementation: CAIRO, ResStock metadata, BAT behavior.

- `cairo_lmi_and_bat_analysis.md`: CAIRO LMI parameters, discount mechanisms, and how the Bill Alignment Test (BAT) works
- `cairo_demand_flexibility_workflow.md`: CAIRO demand-flexibility workflow, CLI steps, and run/output expectations
- `resstock_lmi_metadata_guide.md`: ResStock 2024.2 parquet metadata columns for LMI tier assignment, FPL/SMI, and income
- `subclass_revenue_requirement_utility.md`: `compute_subclass_rr.py` behavior, BAT metric options, required inputs, and CLI/Just usage
- `seasonal_discount_rate_workflow.md`: RI seasonal discount workflow from subclass BAT outputs + winter kWh to tariff/map generation

## docs/

Technical documentation extracted from PDFs (e.g. Cambium, ResStock dataset docs). Add via the **extract-pdf-to-markdown** slash command.

| File                                | Use when working on …                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------------- |
| cambium_2024.md                     | Cambium 2024 scenarios, marginal costs, metrics, GEA/BA, LRMER/SRMER, methods               |
| resstock_2024.2.md                  | ResStock 2024.2 metadata, measure packages, load conventions, or building/upgrade schema    |
| census_pums_acs1_2022_data_dict.txt | ACS 1-year PUMS variable definitions (2022)                                                 |
| census_pums_acs1_2023_data_dict.txt | ACS 1-year PUMS variable definitions (2023)                                                 |
| census_pums_acs1_2023_user_guide.md | ACS 2023 1-year PUMS user guide: file structure, weights, geographies, data dictionary      |
| census_pums_acs1_2024_data_dict.txt | ACS 1-year PUMS variable definitions (2024)                                                 |
| census_pums_acs1_2024_user_guide.md | ACS 2024 1-year PUMS user guide: file structure, weights, geographies, data dictionary      |
| census_pums_acs5_2021_data_dict.txt | ACS 5-year PUMS variable definitions (2017-2021)                                            |
| census_pums_acs5_2022_data_dict.txt | ACS 5-year PUMS variable definitions (2018-2022)                                            |
| census_pums_acs5_2023_user_guide.md | ACS 2019–2023 5-year PUMS user guide: file structure, weights, geographies, data dictionary |
| census_pums_acs5_2023_data_dict.txt | ACS 5-year PUMS variable definitions (2019-2023)                                            |

## papers/

Academic papers extracted from PDFs (e.g. Bill Alignment Test). Add via the **extract-pdf-to-markdown** slash command.

| File                   | Use when working on …                                           |
| ---------------------- | --------------------------------------------------------------- |
| bill_alignment_test.md | Bill Alignment Test methodology, cross-subsidization, CAIRO BAT |

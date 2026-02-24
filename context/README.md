# Context index

Reference docs and research notes for agents. **When you add or remove a file under `context/`, update this index.**

See **AGENTS.md → Reference context** for conventions (what goes in `papers/`, `docs/`, `domain/`, `tools/`) and when agents should read from here.

## domain/

Research notes on the domain: rate design, LMI programs, policy by state.

| File                                 | Purpose                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| gas_heating_rates_in_ny.md           | NY gas heating rate structures and tariff landscape                                                                                                                                                                                                                                                                        |
| lmi_discounts_in_ny.md               | NY utility low-income discount programs (e.g. EAP, EEAP)                                                                                                                                                                                                                                                                   |
| lmi_discounts_in_ri.md               | RI utility low-income programs (RIE rates, LIDR+ proposal)                                                                                                                                                                                                                                                                 |
| ny_residential_charges_in_bat.md     | All NY residential electric charges (all 7 utilities) evaluated for BAT/bill calc: charge type taxonomy, master table, generalized cross-subsidy framework, charge-by-charge analysis by family (base delivery, cost recon, program surcharges, sunk-cost recovery, DER credits, supply decomposition, taxes, eligibility) |
| ri_residential_charges_in_bat.md     | All RI residential electric charges (RIE A-16) evaluated for BAT/bill calc: charge type taxonomy, summary table, generalized cross-subsidy, charge-by-charge analysis (base delivery, cost recon, program surcharges, sunk-cost, LMI recovery, LRS/ISO-NE supply decomposition, RES, GET), structural notes                |
| ny_genability_charge_fetch_map.md    | Exhaustive charge-level table for NY Genability tariffs: tariffRateId, fetch_type (fixed/lookup/rider_placeholder/superseded), variableRateKey, master_charge, decision; used to fetch 2025 monthly rates from Arcadia API for top-up implementation                                                                       |
| bat_reasoning_stress_test.md         | Formal reconstruction and econ-seminar-style stress-test of the BAT framework: marginal costs, residual allocation, the three allocators, cross-subsidy definitions (BAT vs strict economic), standalone vs incremental cost                                                                                               |
| coned_el1_charges_in_bat.md          | **Superseded by `ny_residential_charges_in_bat.md`** — original ConEd-only analysis, kept for reference                                                                                                                                                                                                                    |
| coned_el1_lookups_effective_dates.md | Arcadia Lookups API: one record per effective period (from/to), not per month; ConEd applies these charges monthly; we expand lookups to one row per month                                                                                                                                                                 |
| ny_mcos_studies_comparison.md        | Cross-utility comparison of all six NY 2025 MCOS studies: methodology, system-wide diluted/undiluted tables, component taxonomy mapping (physical hierarchy → cost-center labels), bulk vs local TX scrub, reassessment of bulk TX MC gap                                                                                  |

## tools/

Research notes on tools, data, or implementation: CAIRO, ResStock metadata, BAT behavior.

| File                                     | Purpose                                                                                              |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| cairo_lmi_and_bat_analysis.md            | CAIRO LMI parameters, discount mechanisms, and how the Bill Alignment Test (BAT) works               |
| cairo_demand_flexibility_workflow.md     | CAIRO demand-flexibility workflow, CLI steps, and run/output expectations                            |
| compare_resstock_eia861_loads.md         | ResStock hourly load vs EIA-861 residential sales comparison: script usage, defaults, interpretation |
| compute_tou_from_marginal_costs.md       | MC-driven TOU tariff derivation: peak-window finder, cost-causation ratio, URDB JSON                 |
| resstock_lmi_metadata_guide.md           | ResStock 2024.2 parquet metadata: columns for LMI tier assignment, FPL/SMI, income                   |
| subclass_revenue_requirement_utility.md  | `compute_subclass_rr.py` behavior, BAT metric options, required inputs, and CLI/Just usage           |
| seasonal_discount_rate_workflow.md       | RI seasonal discount workflow from subclass BAT outputs + winter kWh to tariff/map generation        |
| cairo_performance_analysis.md            | CAIRO execution profile, compute bottlenecks, parallelism status, and speedup opportunities          |
| cairo_tiered_rates_support.md            | CAIRO tiered rate support: precalc and bill calc use (period, tier); evidence from codebase          |
| cairo_parallelize_two_undasked_stages.md | Handoff: parallelize process_residential_hourly_demand and BAT in CAIRO via chunk + dask.delayed     |
| cairo_parallelism_and_workers.md         | How to think about parallelism: infra instance, worker count, series vs parallel tracks              |
| cairo_elastic_cluster.md                 | Elastic Dask cluster: why (many runs), options (dask-cloudprovider etc.), CAIRO + platform changes   |
| run_orchestration.md                     | RI runs 1–12 orchestration: Justfile dependency chain, `latest_run_output.sh`, design decisions      |
| nyiso_lbmp_zonal_data_sources.md         | NYISO Day-Ahead/Real-Time zonal LBMP: MIS ZIP vs gridstatus vs NYISOToolkit; data samples            |

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

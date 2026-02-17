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

| File                           | Purpose                                                                                |
| ------------------------------ | -------------------------------------------------------------------------------------- |
| cairo_lmi_and_bat_analysis.md  | CAIRO LMI parameters, discount mechanisms, and how the Bill Alignment Test (BAT) works |
| resstock_lmi_metadata_guide.md | ResStock 2024.2 parquet metadata: columns for LMI tier assignment, FPL/SMI, income     |

## docs/

Technical documentation extracted from PDFs (e.g. Cambium, ResStock dataset docs). Add via the **extract-pdf-to-markdown** slash command.

| File               | Use when working on …                                                                    |
| ------------------ | ---------------------------------------------------------------------------------------- |
| cambium_2024.md    | Cambium 2024 scenarios, marginal costs, metrics, GEA/BA, LRMER/SRMER, methods            |
| resstock_2024.2.md | ResStock 2024.2 metadata, measure packages, load conventions, or building/upgrade schema |

## papers/

Academic papers extracted from PDFs (e.g. Bill Alignment Test). Add via the **extract-pdf-to-markdown** slash command.

| File                   | Use when working on …                                           |
| ---------------------- | --------------------------------------------------------------- |
| bill_alignment_test.md | Bill Alignment Test methodology, cross-subsidization, CAIRO BAT |

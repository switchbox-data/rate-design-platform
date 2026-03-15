# Data Dictionary: `2025_proposed_transmission_facilities.csv`

**S3**: `s3://data.sb/nyiso/gold_book/2025_proposed_transmission_facilities.csv`
**Local pipeline**: `data/nyiso/gold_book/` (Justfile: `just fetch`, `just classify`, `just upload`)

Extracted from **Table VII** ("Proposed Transmission Facilities") of the
[NYISO 2025 Gold Book](https://www.nyiso.com/gold-book). The table spans
pages 156–163 of the original PDF and covers all proposed bulk transmission
projects in the New York Control Area (NYCA).

398 rows. 23 columns. Every row is a single project.

## Source and processing

The Gold Book PDF was converted to markdown
(`context/sources/nyiso_gold_book_2025.md`) via `extract-pdf-to-markdown`.
Table VII's data rows were extracted from that markdown with a parsing script,
then cleaned in several passes:

1. **Numeric normalization** — `line_length_miles`, `num_circuits`, and
   `thermal_rating_summer`/`thermal_rating_winter` were cleaned to proper
   numeric types. Non-numeric values (N/A, TBD, `-`, `---`) were moved to
   companion `_notes` columns and the main column set to null.
2. **Thermal rating unit extraction** — Units (A, MVA, MW, MVAR) were
   extracted from inline text (e.g. `"637 MVA"` → value `637`, unit `MVA`).
   Per Gold Book footnote 4, bare numbers without an explicit unit are in
   **Amperes**.
3. **Project classification** — Each row was classified as `line` or
   `equipment` and assigned a subtype. Rule-based classifiers handle the
   majority; 74 rows required manual overrides (documented in
   `data/nyiso/gold_book/classify_proposed_transmission_facilities.py`).
4. **Two parsing-error fixes** — Row 124 (ConEd Astoria East) had thermal
   ratings misplaced into `num_circuits`; row 231 (NYPA/NGRID STAMP) had
   the same issue. Both were corrected with notes in
   `line_length_miles_notes`.

## Columns

### Original columns (from Table VII)

| Column                            | Type           | Description                                                                                                                                                                                                                                                       |
| --------------------------------- | -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `category`                        | string         | Gold Book project category. One of: `Class Year Transmission Projects`, `TIP Projects`, `Firm Plans`, `Non-Firm Plans`. See Gold Book Section VII footnotes 5, 18, 19 for definitions.                                                                            |
| `project_queue_position`          | string         | NYISO interconnection queue position(s) and/or footnote references from the source table. May contain bracket notation (e.g. `[631]`), comma-separated values, or be empty. Not a clean integer — treat as an identifier.                                         |
| `transmission_owner`              | string         | Transmission owner(s). Values include `CHGE`, `ConEd`, `LIPA`, `NGRID`, `NYPA`, `NYSEG`, `O & R`, `RGE`, `New York Transco`, `CHPE LLC`, `Clean Path New York LLC`, and joint owners like `NYPA/NGRID`, `NYSEG/ConEd`, etc.                                       |
| `terminal_from`                   | string         | Originating terminal / station name. Parsed heuristically from PDF — may contain parsing artifacts for multi-word station names.                                                                                                                                  |
| `terminal_to`                     | string         | Destination terminal / station name. For equipment projects, often contains equipment type (e.g. `"Kerhonkson Transformer"`, `"Barrett Substation"`). Same parsing caveats as `terminal_from`.                                                                    |
| `line_length_miles`               | float \| null  | Line length in miles. Negative values indicate removal of an existing circuit segment (retirement projects). Null for equipment projects and some line projects where length was not specified in the source.                                                     |
| `in_service_season`               | string         | Proposed in-service season or status. Values: `S` (summer), `W` (winter — refers to the winter _beginning_ that year, e.g. W 2025 = winter 2025–26), or `In-Service` (already in service).                                                                        |
| `in_service_year`                 | string         | Proposed in-service year (4-digit). Combined with `in_service_season`, gives the target date.                                                                                                                                                                     |
| `voltage_operating_kv`            | string         | Nominal operating voltage in kV. May contain compound values for transformers (e.g. `345/115`). Some LIPA entries include `kV` suffix (e.g. `138kV`). A few entries are `-` for projects without a voltage (e.g. relay upgrades).                                 |
| `voltage_design_kv`               | string         | Nominal design voltage in kV. Same format as `voltage_operating_kv`.                                                                                                                                                                                              |
| `num_circuits`                    | int \| null    | Number of circuits (for lines) or number of units (for equipment like transformers). Null when not applicable or not specified. See `num_circuits_notes` for context on null values.                                                                              |
| `thermal_rating_summer`           | float \| null  | Summer thermal rating. Units are in `thermal_rating_units`. Null when N/A, TBD, or not specified.                                                                                                                                                                 |
| `thermal_rating_winter`           | float \| null  | Winter thermal rating. Same unit and null conventions as summer.                                                                                                                                                                                                  |
| `project_description`             | string         | Free-text project description from the source. Includes conductor specs, scope details, and sometimes footnote references. Some TBD thermal ratings were originally embedded here and have been extracted.                                                        |
| `class_year_or_construction_type` | string \| null | Either a class year (e.g. `2021`, `2023`), a construction type (`OH` = overhead, `UG` = underground), `CLCPA` (Climate Leadership and Community Protection Act project), or null. Line projects are more likely to have this populated (57%) vs. equipment (14%). |

### Added columns

| Column                            | Type           | Description                                                                                                                                                                                                                                 |
| --------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `project_type`                    | string         | `line` or `equipment`. Every project is exactly one. Line projects build, modify, or retire a transmission line. Equipment projects install, replace, or upgrade equipment at a station. Classification logic is in `classify_projects.py`. |
| `project_subtype`                 | string         | Finer classification within project type. See the subtype taxonomy below.                                                                                                                                                                   |
| `line_length_miles_notes`         | string \| null | Documents why `line_length_miles` is null or explains reclassifications. Examples: `"equipment project referencing a line"`, `"line length not specified in source"`, `"reclassified to equipment: PAR installation on existing line"`.     |
| `project_includes_circuits`       | bool           | `true` when `num_circuits` contains a valid integer. `false` when the source value was N/A, `-`, `---`, or empty.                                                                                                                           |
| `num_circuits_notes`              | string \| null | Documents the original non-numeric value and whether it implies unknown count vs. not applicable. Example: `'num_circuits originally said "-" (equipment project)'`.                                                                        |
| `thermal_rating_units`            | string \| null | Unit for `thermal_rating_summer` and `thermal_rating_winter`. One of: `A` (Amperes), `MVA`, `MW`, `MVAR`, or null (when ratings are N/A or TBD).                                                                                            |
| `project_includes_thermal_rating` | bool           | `true` when thermal ratings are specified or TBD (i.e. the project will have a rating). `false` when ratings are N/A or absent (the project doesn't have a meaningful thermal rating).                                                      |
| `thermal_rating_notes`            | string \| null | Documents thermal rating status. Values include `"thermal ratings are TBD"`, `"thermal ratings are N/A"`, `"thermal ratings not specified in source"`.                                                                                      |

## Project subtype taxonomy

### Line subtypes

| Subtype              | Count | Description                                                                                           | Examples                                                                                    |
| -------------------- | ----- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `line_rebuild`       | 62    | Rebuild or refurbish an existing line — new structures and/or conductor on the same right-of-way.     | "Rebuild 20 miles of Lockport-Batavia 112", "Refurbish 23.4 miles double circuit"           |
| `new_line`           | 43    | New transmission line, circuit, or cable, including HVDC. Creates a path that didn't exist before.    | "New 8.6 mile 115kV circuit with 795ACSR", "-/+ 320kV Bipolar HVDC cable"                   |
| `reconductor`        | 18    | Replace conductor on an existing line without rebuilding structures. Increases thermal capacity.      | "Reconductor existing line with ACSR 795 26/7 Drake", "Reconductor L949-1 with 1192.5 ACSR" |
| `retirement`         | 13    | Remove or retire an existing line. Always has a negative `line_length_miles`.                         | "Retire Existing Moses-Adirondack MA1 and MA2 230 kV Lines"                                 |
| `line_upgrade`       | 13    | Generic upgrade to an existing line — ratings improvement, minor modifications, or unspecified scope. | "Line Upgrade", "Terminal Equipment Upgrades to existing line"                              |
| `reconfiguration`    | 6     | Restructure line topology: loop-ins, taps, rerouting between stations.                                | "Tapping 345 kV Line between Pleasant Valley and Millwood West at Wood Street"              |
| `voltage_conversion` | 5     | Upgrade the operating voltage of an existing line (e.g. 69 kV → 115 kV).                              | "1-795 ACSR: Convert to 115 kV Operation"                                                   |
| `restoration`        | 1     | Return a decommissioned line to service.                                                              | "MR3 line back to service to supply loads"                                                  |

### Equipment subtypes

| Subtype                 | Count | Description                                                                                                                                                       | Examples                                                                                          |
| ----------------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `transformer`           | 55    | New, replacement, or upgraded power transformer, including auto-transformers and LTC-equipped units.                                                              | "Replacement of Niagara AutoTransformer #3", "Two (2) new 115/34.5 kV 50 MVA transformers"        |
| `reactive_compensation` | 42    | Capacitor banks, shunt reactors, SVCs, STATCOMs, series reactors, series compensators, and overvoltage mitigation. Any device managing reactive power or voltage. | "Install statcom and cap bank", "New 100 MVAR Shunt Reactor", "Coffeen Overvoltage"               |
| `new_substation`        | 37    | Construction of an entirely new substation, station, or clean energy hub.                                                                                         | "New 345 kV Substation", "Queens Clean Energy Hub"                                                |
| `substation_rebuild`    | 23    | Rebuild, expand, or major modification of an existing substation.                                                                                                 | "Full rebuild of substation as 115 kV three bay BAAH AIS", "Expand 345 kV bus to 3 bay BAAH"      |
| `terminal_upgrade`      | 21    | Terminal equipment upgrades: bus work, station connections, conductor drops, and similar station-side modifications that don't constitute a full rebuild.         | "Terminal Upgrades at Edic 345 kV Substation", "Replace Station connections. Line #4"             |
| `breaker_switch`        | 19    | Circuit breaker or disconnect switch installation or replacement.                                                                                                 | "Replace three 345kV switches", "Second 115kV Bus Tie Breaker at Mortimer Station"                |
| `equipment_replacement` | 11    | Generic equipment replacement that doesn't fit a more specific category.                                                                                          | "Replace TR2 as failure", "Replacement of Massena Breaker 765 kV Replacements"                    |
| `reconfiguration`       | 9     | Bus reconfiguration, bay additions, or station topology changes that don't involve new construction or a rebuild.                                                 | "Ring bus 345kV GIS installation", "Loop in-and-out reconfiguration at station"                   |
| `phase_angle_regulator` | 8     | Phase angle regulators (PARs) and phase shifters. Devices controlling power flow direction/magnitude on transmission lines.                                       | "New PAR for Y54 Line", "Phase Shifting Transformer between Hillside"                             |
| `feeder`                | 4     | PAR-regulated feeders or similar feeder-level equipment within a substation.                                                                                      | "New PAR regulated feeder (third connection)"                                                     |
| `retirement`            | 3     | Retire or remove equipment or a substation.                                                                                                                       | "Retire Clinton Ave tap", "Retire SD/SJ Lines"                                                    |
| `protection_controls`   | 3     | Relay protection upgrades, sectionalizing schemes, and control system changes.                                                                                    | "Install automatic line sectionalizing scheme at Whitaker", "Substation Relay Protection Upgrade" |
| `interconnection`       | 2     | Connection to an external entity outside the standard transmission network.                                                                                       | "Connection to MTA/Amtrak"                                                                        |

## Patterns: what to expect by project type

### Line projects (161 rows)

- `line_length_miles` is populated for 91% of line projects. The 14 without
  a length are mostly `reconductor` and `line_upgrade` projects where the
  source didn't specify mileage.
- `num_circuits` is populated for 98% of line projects.
- Thermal ratings are present for 87%. The 13% without are mostly
  `retirement` projects (retiring lines have N/A ratings) and a few TBD
  entries.
- `thermal_rating_units` is predominantly `A` (Amperes, 59%) or `MVA`
  (39%). The 3 `MW` entries are HVDC cables (Class Year Transmission
  Projects).
- `class_year_or_construction_type` is populated 57% of the time. `OH`
  (overhead) and `UG` (underground) only appear for line projects. `CLCPA`
  appears for both line and equipment.
- `retirement` projects always have negative `line_length_miles`.

### Equipment projects (237 rows)

- `line_length_miles` is null for 98% of equipment projects. The 5 with a
  value are parsing artifacts from the original PDF extraction (the value
  leaked from an adjacent column).
- `num_circuits` is populated for only 36%. When present on equipment
  projects, it means "number of units" (e.g. 2 transformers), not "number
  of parallel circuits."
- Thermal ratings are present for only 30%. Subtypes that almost always
  have them: `transformer` (76%), `phase_angle_regulator` (88%). Subtypes
  that almost never have them: `new_substation` (3%), `breaker_switch`
  (5%), `feeder` (0%), `retirement` (0%), `protection_controls` (0%),
  `interconnection` (0%).
- `thermal_rating_units` when present is mostly `MVA` (56%) or `A` (39%).
  The `A` values are all transformers where the source explicitly notes
  "Given Amp Ratings are for High Voltage side of xfmr." The 3 `MVAR`
  entries are reactive compensation devices.
- `class_year_or_construction_type` is populated for only 14%.
  `CLCPA`-tagged equipment projects are mostly NYSEG and NGRID station
  work associated with the Climate Leadership and Community Protection Act.

### TBD thermal ratings (25 rows)

These have `project_includes_thermal_rating = true` but null
`thermal_rating_summer`/`thermal_rating_winter`. The original source listed
`TBD` for the ratings. These are predominantly LIPA TIP Projects (many are
part of the LI PPTN program) and O&R Non-Firm Plans. The `thermal_rating_notes`
column says `"thermal ratings are TBD"` for all of these.

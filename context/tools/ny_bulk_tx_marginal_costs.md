# NY bulk transmission marginal costs

How bulk transmission marginal costs (v_z, $/kW-yr) are derived per `gen_capacity_zone` for NY utilities.

## Script

`data/nyiso/transmission/derive_tx_values.py`

CLI: `--path-projects-csv <input> --path-output-csv <output> [--path-families-csv <families>]`

Outputs:

- `s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_values.csv` — zone-level, schema: `gen_capacity_zone`, `v_avg_kw_yr`
- `s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_families.csv` — family-level audit table, see schema below

Justfile: `data/nyiso/transmission/Justfile` (recipes: `derive`, `upload`, `clean`)

## Current implementation shape (refactor notes)

`derive_tx_values.py` is organized as a small staged pipeline:

1. `prepare_projects_for_derivation(path_projects)`:
   load CSV, validate required columns, drop `exclude=True`, drop invalid `delta_mw`,
   map `scenario -> scenario_family`, and canonicalize `receiving_localities`
   into `benefit_footprint_str`.
2. `collapse_variants(df)`:
   average scenario variants per physical project point.
3. `compute_family_secant_vavg(collapsed)`:
   compute family `v_family_kw_yr` from cumulative secants.
4. `annotate_family_paying_zones(family_df)`:
   add `paying_zones` column from footprint rules.
5. `assign_families_to_paying_zones(family_df)` + `compute_zone_vavg(contribs)`:
   aggregate zone-level `v_avg_kw_yr`.

Important implementation detail:
- The nested-footprint lookup is centralized via
  `paying_zones_for_footprint_str()`, and reused by both family annotation
  and zone contribution assignment. This avoids duplicate footprint parsing and
  keeps paying-zone behavior consistent across stages.

Behavioral invariants preserved by tests:
- `LHV < NYC` ordering on final zone values.
- All four paying zones (`ROS`, `LHV`, `NYC`, `LI`) must receive at least one
  family contribution.
- Unknown footprint mappings emit `UserWarning` and are skipped.

## Data sources

| Dataset                            | Source                                         | What it provides                                                                                                           |
| ---------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| NYISO AC Transmission study (2019) | GH issue #302                                  | Project-level ΔMW and annual benefit ($M/yr) for AC Primary, Addendum Optimizer, and MMU scenarios across NYISO localities |
| NYISO LI Export study (2020)       | GH issue #302                                  | LI Export (Policy) projects with ΔMW and benefit                                                                           |
| NiMo 2025 MCOS (project workbook)  | `context/papers/mcos/nimo_2025_mcos.md`        | Per-project undiluted $/kW-yr for three bulk TX projects (≥230 kV), used for ROS zone                                      |
| OATT proxies (NYSEG, RG&E, CenHud) | `context/domain/ny_mcos_studies_comparison.md` | Cross-reference for total transmission costs (bulk + sub); not used directly for bulk TX v_z                               |

## Projects CSV schema

`data/nyiso/transmission/csv/ny_bulk_tx_projects.csv`

| Column                 | Type  | Description                                                                                        |
| ---------------------- | ----- | -------------------------------------------------------------------------------------------------- |
| `year`                 | int   | Study reference year                                                                               |
| `scenario`             | str   | Study scenario string (e.g. `Addendum MMU (Baseline)`)                                             |
| `project`              | str   | Project identifier (e.g. `T027+T019`)                                                              |
| `study_locality`       | str   | Where the infrastructure is sited (NYISO zone letters; informational)                              |
| `direction`            | str   | Power flow direction                                                                               |
| `receiving_localities` | str   | Pipe-delimited set of benefit-footprint tokens: `NYCA`, `LHV`, `NYC`, `LI` (e.g. `NYCA\|LHV\|NYC`) |
| `annual_benefit_m_yr`  | float | Annual net benefit ($M/yr) from the study                                                          |
| `delta_mw`             | float | Capacity increment (MW). Rows with missing or ≤0 MW are dropped (secant requires MW).              |
| `exclude`              | bool  | If `true`, row is dropped before derivation (used for sensitivity scenarios)                       |
| `notes`                | str   | Optional notes                                                                                     |

`receiving_localities` encodes the **benefit footprint** as a pipe-delimited set of nested locality tokens:

- `NYCA` — system-wide (zones A–K)
- `LHV` — Lower Hudson Valley (zones G–J), nested within NYCA
- `NYC` — New York City (zone J), nested within LHV
- `LI` — Long Island (zone K), separate from LHV but still within NYCA

## Locality semantics: nested footprints vs partitioned paying zones

This pipeline uses the same locality model as the NY generation capacity MC:

**Nested footprints** (for peak-load shape, overlapping by design):

```
NYCA = A-K   (superset)
LHV  = G-J   (LHV ⊂ NYCA)
NYC  = J     (NYC ⊂ LHV ⊂ NYCA)
LI   = K     (separate; LI ⊂ NYCA)
```

**Partitioned paying zones** (disjoint customer groups — "who should pay"):

```
ROS = A-F   (NYCA minus LHV and LI)
LHV = G-I   (LHV minus NYC)
NYC = J
LI  = K
```

The `receiving_localities` column on each project row is a set of nested footprint tokens that encodes "where the benefit accrues." The derivation pipeline converts benefit footprints to disjoint paying zones using the `NESTED_TO_PAYING_ZONES` lookup table.

### NESTED_TO_PAYING_ZONES rules

| Benefit footprint      | Disjoint paying zones | Rationale                                                                |
| ---------------------- | --------------------- | ------------------------------------------------------------------------ |
| `{NYCA}`               | ROS                   | System-wide benefit → only upstate (non-LHV/LI) load pays                |
| `{NYCA, LHV}`          | LHV                   | LHV-wide benefit; NYCA is the nested superset, not a separate payer      |
| `{NYCA, LHV, NYC}`     | NYC + LHV             | NYC sub-zone benefits; both J (NYC) and G-I (LHV-non-NYC) partitions pay |
| `{NYCA, LHV, LI}`      | LHV + LI              | Spans G-K corridor; G-I customers and K customers each pay               |
| `{NYCA, LI}`           | LI                    | LI-specific benefit only                                                 |
| `{NYCA, LHV, NYC, LI}` | NYC + LHV + LI        | Full downstate benefit                                                   |
| `{LHV}`                | LHV                   | LHV-only (no system-wide component)                                      |
| `{LHV, NYC}`           | NYC + LHV             | Same as above but without explicit NYCA token                            |
| `{NYC}`                | NYC                   | NYC-only                                                                 |
| `{LI}`                 | LI                    | LI-only                                                                  |

Key: NYCA is the nested superset and is **ignored as a paying zone** whenever any sub-locality is present. It maps to ROS only when it appears alone.

### Tightest footprint (for SCR peak-load shape)

Each family has a `tightest_footprint` — the most specific nested locality in its benefit footprint. Priority: NYC > LHV > LI > NYCA. The tightest footprint drives which zone-level load profile is used for SCR peak identification when allocating v_family to hours.

## Major NY Transmission Projects

Reference table of major bulk transmission projects in New York, their approximate costs, flow directions, and primary receiving localities:

| Project                                                    | Approx. Cost                 | Flow Direction           | Primary Receiving Locality     | Notes                                  |
| ---------------------------------------------------------- | ---------------------------- | ------------------------ | ------------------------------ | -------------------------------------- |
| **Niagara–Dysinger 345 kV Rebuild**                        | ~$100–120M                   | Ontario/Niagara → A/F    | A–F (ROS)                      | Western NY reliability reinforcement   |
| **Western NY Public Policy Portfolio (Segment A/B)**       | ~$350–400M                   | Niagara/Western NY → A–F | A–F (ROS)                      | Multiple 230/345 kV rebuilds           |
| **Smart Path Connect (Marcy–New Scotland + rebuilds)**     | ~$700–900M                   | Upstate → LHV/G–K        | LHV (primary), G–K (secondary) | Central East relief                    |
| **Edic–Pleasant Valley (Segment B)**                       | ~$1.2–1.5B                   | Upstate → Downstate      | LHV / G–K                      | Major CE bottleneck relief             |
| **Eastover Capacity Bank / CE Upgrades**                   | ~$100–200M                   | Upstate → LHV            | LHV                            | Improves CE transfer                   |
| **UPNY–ConEd AC Transmission (Segment A)**                 | ~$1.0–1.2B                   | Upstate → NYC            | Zone J (NYC)                   | Direct NYC deliverability increase     |
| **NY Energy Solution (NYES)**                              | ~$500–600M                   | Upstate → LHV            | LHV                            | Public policy carbon reduction project |
| **Empire State Line**                                      | ~$850–1,000M                 | Upstate → NYC            | Zone J                         | Competes with UPNY–ConEd               |
| **NYC Public Policy Transmission (AC Solution Portfolio)** | ~$1.5–2.0B (portfolio total) | Upstate → NYC            | Zone J                         | Selected AC solution for 70x30 goal    |
| **NYCLI (Long Island Export Cable)**                       | ~$700–800M                   | LI (K) → NYC (J)         | Zone J                         | Improves LI export to NYC              |

## Derivation method

### Step 1 — Parse and clean input

- Drop `exclude=True` rows (sensitivity scenarios, e.g. `Addendum Optimizer (G-J Elimination)`).
- Drop rows with missing or non-positive `delta_mw` (warn with project name; secant requires MW).
- Parse `receiving_localities` (pipe-delimited) into a frozenset per row.
- Map `scenario` → `scenario_family` via `SCENARIO_FAMILY_MAP`.

### Step 2a — Collapse scenario variants

Within each (project, delta_mw, benefit_footprint, scenario_family) group, average `annual_benefit_m_yr` so each physical project contributes one (B, MW) data point to the secant curve. E.g. `Addendum MMU (Baseline)` and `Addendum MMU (CES+Ret)` are both mapped to `scenario_family=mmu` and their benefits averaged per project.

### Step 2b — Average cumulative secant per (benefit_footprint, scenario_family)

1. **Sort** collapsed projects by ΔMW ascending — traces the supply curve from smallest to largest capacity increment.

2. **Compute cumulative secants.** At each step _i_:
   \[ \text{secant}_i = \frac{\text{cum\_B}_i \times 10^6}{\text{cum\_ΔMW}_i \times 10^3} \quad [\$/\text{kW-yr}] \]

3. **v_family = mean(secant_i)** across all steps.

A large-ΔMW low-v project only affects the last cumulative secant, not every term — so it has less downward influence than a simple mean or MW-weighted average.

### Step 3 — Nested → partitioned paying-zone assignment

Each family's `benefit_footprint` (frozenset) is looked up in `NESTED_TO_PAYING_ZONES` to get its disjoint paying zones. A family may contribute to more than one zone (e.g. `{NYCA, LHV, NYC}` → NYC and LHV) without double-counting, because NYC (J) and LHV-non-NYC (G-I) are disjoint customer partitions.

If a family's footprint is not in `NESTED_TO_PAYING_ZONES`, a `UserWarning` is emitted and the family is skipped.

### Step 4 — Zone-level aggregation

v_avg_kw_yr per zone = simple mean of contributing family v_family values.

Raises `ValueError` if any of {ROS, LHV, NYC, LI} receives zero contributing families.

## Family table schema (`ny_bulk_tx_families.csv`)

| Column                  | Type  | Description                                                    |
| ----------------------- | ----- | -------------------------------------------------------------- |
| `benefit_footprint_str` | str   | Sorted pipe-delimited footprint string (e.g. `LHV\|NYC\|NYCA`) |
| `scenario_family`       | str   | Collapsed scenario family name (e.g. `mmu`, `li_export`)       |
| `v_family_kw_yr`        | float | Average cumulative secant value $/kW-yr                        |
| `n_points`              | int   | Number of collapsed project points on the curve                |
| `project_list`          | str   | Pipe-delimited list of project IDs contributing to this family |
| `tightest_footprint`    | str   | Most specific nested locality (NYC > LHV > LI > NYCA)          |
| `paying_zones`          | str   | Pipe-delimited list of disjoint paying zones (e.g. `NYC\|LHV`) |

## Derivation by zone

### ROS (Rest of State / Upstate)

**Source:** NiMo 2025 MCOS — Niagara-Dysinger only (receiving_localities=`NYCA`).

Single project → v_avg = **$10.89/kW-yr** (345 kV, 1,100 MW, FY2036).

Smart Path Connect and Eastover have receiving_localities=`LHV|LI|NYCA` and contribute to LHV and LI, not ROS.

#### NiMo 2025 MCOS bulk TX project table (≥230 kV)

| Project                 | FN Ref   | Voltage    | In-Service | ΔMW   | Capital ($M) | v ($/kW-yr) | receiving_localities | Paying zones |
| ----------------------- | -------- | ---------- | ---------- | ----- | ------------ | ----------- | -------------------- | ------------ |
| Smart Path Connect      | FN008374 | 230/345 kV | FY2027     | 1,000 | $928.9       | $78.42      | `LHV\|LI\|NYCA`      | LHV + LI     |
| Eastover 230kV Cap Bank | FN013189 | 230 kV     | FY2033     | 20    | $9.8         | $40.21      | `LHV\|LI\|NYCA`      | LHV + LI     |
| Niagara-Dysinger        | FN013571 | 345 kV     | FY2036     | 1,100 | $142.2       | $10.89      | `NYCA`               | ROS          |

### LHV (Lower Hudson Valley, zones G–J)

**Five contributing families** (all with paying zone LHV):

| Family             | Footprint        | Projects                                    | v_family |
| ------------------ | ---------------- | ------------------------------------------- | -------- |
| nimo_mcos          | `LHV\|LI\|NYCA`  | Smart Path (1,000 MW) + Eastover (20 MW)    | $58.94   |
| ac_primary         | `LHV\|LI\|NYCA`  | Tier1+Tier2 (1,850 MW)                      | $45.41   |
| addendum_optimizer | `LHV\|NYCA`      | T027+T029 (1,300 MW) + T027+T019 (1,850 MW) | $26.10   |
| li_export          | `LHV\|NYCA`      | 7 LI Export projects (1,514–2,829 MW)       | $54.34   |
| mmu                | `LHV\|NYC\|NYCA` | T027+T029 (350 MW) + T027+T019 (375 MW)     | $53.53   |

Zone v_avg = mean($58.94, $45.41, $26.10, $54.34, $53.53) = **$47.66/kW-yr**

Note: The MMU family (`{NYCA, LHV, NYC}`) contributes to both NYC and LHV. NYC customers (J) and LHV-non-NYC customers (G-I) both benefit from improved UPNY-ConEd deliverability, and are disjoint partitions — no double-counting.

### NYC (New York City, zone J)

**Source:** MMU family only — Baseline + CES+Ret at UPNY-ConEd interface (receiving_localities=`LHV|NYC|NYCA`).

Baseline and CES+Ret averaged per project → two collapsed projects: T027+T029 (350 MW, avg B=$18.53M) and T027+T019 (375 MW, avg B=$20.73M). Cumulative secants: [$52.93, $54.14].

Zone v_avg = **$53.53/kW-yr**.

### LI (Long Island, zone K)

**Source:** G-K-footprint projects only (`LHV|LI|NYCA`).

| Family                              | v_family |
| ----------------------------------- | -------- |
| nimo_mcos (Smart Path + Eastover)   | $58.94   |
| ac_primary (AC Primary Tier1+Tier2) | $45.41   |

Zone v_avg = mean($58.94, $45.41) = **$52.17/kW-yr**

LI Export projects (direction LI→mainland) have receiving_localities=`LHV|NYCA` — no `LI` token — because the benefit of expanded export capability accrues to mainland load (G-I), not LI load. They pay to LHV only.

## Final v_avg values (output of `just derive`)

| gen_capacity_zone | v_avg_kw_yr | Contributing families                                                                                  |
| ----------------- | ----------- | ------------------------------------------------------------------------------------------------------ |
| ROS               | $10.89      | NYCA/nimo_mcos (Niagara-Dysinger)                                                                      |
| LHV               | $47.66      | nimo_mcos ($58.94), ac_primary ($45.41), addendum_optimizer ($26.10), li_export ($54.34), mmu ($53.53) |
| NYC               | $53.53      | mmu (MMU Baseline+CES+Ret, averaged per project)                                                       |
| LI                | $52.17      | nimo_mcos ($58.94), ac_primary ($45.41)                                                                |

## Zone mapping

The `gen_capacity_zone` column in `ny_utility_zone_mapping.csv` is the single four-zone grouping used for both generation capacity MC and bulk TX MC lookups. Bulk TX uses the same locality mapping as generation capacity — no separate `tx_locality` column is needed.

| Utility | Zones   | gen_capacity_zone     |
| ------- | ------- | --------------------- |
| cenhud  | G       | LHV                   |
| coned   | G, H, J | NYC (87%) / LHV (13%) |
| nimo    | A–F     | ROS                   |
| nyseg   | A–F     | ROS                   |
| or      | G       | LHV                   |
| rge     | B       | ROS                   |
| psegli  | K       | LI                    |

## Integration with CAIRO

Bulk Tx MC values from this pipeline are consumed by `utils/pre/generate_bulk_tx_mc.py`
and allocated to hourly $/kWh price signals using a two-level seasonal PoP method.

### Hourly allocation method

The script supports two modes. In both, the two-level SCR allocation is used:

**Level 1 — between seasons (peak surplus above SCR floor):**

The fraction of v_z attributed to each season is derived from how far each season's
coincident peak exceeds the capacity-triggering floor τ_min:

```
τ_min = min(τ_summer, τ_winter)        [lower of the two SCR thresholds]
φ_s   = (peak_s − τ_min) / ((peak_s − τ_min) + (peak_w − τ_min))
φ_w   = 1 − φ_s
```

τ_min is the load of the (N+1)th highest hour in the lower-threshold season (almost always
winter). Only load _above_ this floor is "capacity-driving".

**Level 2 — within each season (exceedance above the SCR threshold):**

Each of the top-40 SCR hours in a season is weighted by how far it exceeds that season's
own threshold (the 41st highest hour):

```
exc_h = max(load_h − τ_season, 0)   for h in top-40 SCR hours
w_h   = exc_h / Σ exc_h             [sums to 1 within season]
```

**Hourly price signal:**

```
pi_h = v_z × φ_season × w_h         [$/kW-yr]
bulk_tx_cost_enduse_h = pi_h        [$/kWh]
```

Global weights sum to 1 by construction: φ_s·Σw_s + φ_w·Σw_w = φ_s + φ_w = 1.

### Legacy mode (default — utility-level)

Enabled when `--family-table-path` is **not** provided.

Loads a single `v_avg_kw_yr` per zone from `ny_bulk_tx_values.csv`, resolves the utility's
capacity-weighted v_z (e.g. ConEd: 0.87 × NYC + 0.13 × LHV), then allocates using the
utility's own load profile (EIA utility-level hourly loads).

### Footprint-aware mode (`--family-table-path`)

Enabled by passing `--family-table-path ny_bulk_tx_families.csv`.

Implements "match the peak-load shape to the tightest benefit footprint, then blend per
utility's zone weights" — the same nested/partitioned semantics as the derivation step:

1. Load zone-level EIA loads for all NYISO zones (A–K).
2. Build nested footprint load profiles by summing zone loads:
   - NYCA = A-K, LHV = G-J, NYC = J, LI = K
3. For each footprint, compute SCR weights (φ_season × w_h) using that footprint's load.
4. For each family, use the tightest-footprint SCR weights to allocate v_family to hours:
   `pi_h^f = v_family × weight_h^{tightest_footprint}`
5. Per zone: average the hourly signals from all contributing families:
   `zone_signal_h = mean(pi_h^f  for f in contributing_families)`
   This integrates to `v_avg_zone` for a constant 1-kW load.
6. Per utility: blend zone signals by capacity weights:
   e.g. ConEd = 0.87 × NYC_signal + 0.13 × LHV_signal

This ensures MMU (tightest=NYC) uses J-zone SCR peaks, while NiMo projects
(tightest=LHV) use G-J corridor peaks, and ROS-footprint projects use NYCA peaks.

**Resulting φ values (2025 runs, legacy mode):**

| Utility | τ_min (MW) | φ_s   | φ_w   |
| ------- | ---------- | ----- | ----- |
| cenhud  | 1,537      | 0.884 | 0.117 |
| coned   | 9,119      | 0.890 | 0.110 |
| nimo    | 9,641      | 0.799 | 0.201 |
| nyseg   | 9,641      | 0.799 | 0.201 |
| or      | 1,537      | 0.884 | 0.117 |
| rge     | 1,439      | 0.883 | 0.117 |
| psegli  | 2,969      | 0.926 | 0.074 |

NIMO/NYSEG show lower φ_s because their summer and winter peaks are closest together;
PSEGLI shows the highest φ_s because its summer peak (5,543 MW) is nearly double τ_min.

### MC Loading Pipeline

The hourly bulk Tx MC trace (8760 rows, $/kWh) is loaded and combined with the delivery-side
dist+sub-tx MC in `utils/cairo.py`:

- **`load_bulk_tx_marginal_costs(path)`**: Loads bulk Tx MC parquet (stored as $/kWh), returns Series with EST timezone
- **`_align_mc_to_index(mc_series, target_index, mc_type)`**: Shared utility for aligning any MC Series to a target DatetimeIndex. Handles same-length position alignment (when MC file year differs from run year) and reindexing for different lengths.
- **`add_bulk_tx_and_distribution_marginal_cost(path_distribution_mc, path_bulk_tx_mc, target_index)`**: High-level function that:
  1. Loads dist+sub-tx MC (required; `mc_total_per_kwh` from `generate_utility_tx_dx_mc.py`)
  2. Loads bulk Tx MC (optional)
  3. Aligns both to target index (typically from bulk MC)
  4. Sums them into a single delivery MC Series
  5. Validates and logs statistics

This design keeps all MC loading/alignment logic in `utils/cairo.py`, making `run_scenario.py` clean and maintainable. The combined delivery MC is then passed to CAIRO's `add_delivery_mc` alongside supply MC (energy + capacity).

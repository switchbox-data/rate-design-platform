# NY bulk transmission marginal costs

How bulk transmission marginal costs (v_z, $/kW-yr) are derived per `gen_capacity_zone` for NY utilities.

## Script

`data/nyiso/transmission/derive_tx_values.py`

CLI: `--path-projects-csv <input> --path-output-csv <output>`

Output: `s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_values.csv`
Schema: `gen_capacity_zone`, `v_avg_kw_yr`

Justfile: `data/nyiso/transmission/Justfile` (recipes: `derive`, `upload`, `clean`)

## Data sources

| Dataset                            | Source                                         | What it provides                                                                                                           |
| ---------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| NYISO AC Transmission study (2019) | GH issue #302                                  | Project-level ΔMW and annual benefit ($M/yr) for AC Primary, Addendum Optimizer, and MMU scenarios across NYISO localities |
| NYISO LI Export study (2020)       | GH issue #302                                  | LI Export (Policy) projects with ΔMW and benefit                                                                           |
| NiMo 2025 MCOS (project workbook)  | `context/papers/mcos/nimo_2025_mcos.md`        | Per-project undiluted $/kW-yr for three bulk TX projects (≥230 kV), used for ROS zone                                      |
| OATT proxies (NYSEG, RG&E, CenHud) | `context/domain/ny_mcos_studies_comparison.md` | Cross-reference for total transmission costs (bulk + sub); not used directly for bulk TX v_z                               |

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

### Step 2 — Average secant of the diminishing-returns curve

For each (locality, scenario_family) group:

1. **Collapse scenario variants.** If multiple rows share the same (project, delta_mw) — e.g. Baseline vs CES+Ret for the same project — average their `annual_benefit_m_yr` first so each physical project contributes one data point.

2. **Sort by ΔMW ascending** — traces the supply curve from smallest to largest capacity increment.

3. **Compute cumulative secants.** At each step *i*:
   \[ \text{secant}_i = \frac{\text{cum\_B}_i \times 10^6}{\text{cum\_ΔMW}_i \times 10^3} \quad [\$/\text{kW-yr}] \]

4. **v\_avg = mean(secant\_i)** across all steps.

A large-ΔMW low-v project only affects the last cumulative secant, not every term — so it has less downward influence than in a simple mean or MW-weighted average.

### Step 3 — Zone assignment via `receiving_locality`

Zone membership is determined by the `receiving_locality` column (where the benefit accrues), not by the study `locality` (where the infrastructure sits):

| receiving_locality | gen_capacity_zone(s) | Rationale |
| ------------------ | -------------------- | --------- |
| `G-K` | **LHV** and **LI** | G-K spans zones G–K; K = Long Island, so G-K benefits both |
| `G-J` | **LHV** | G–J corridor; J is within LHV, not LI |
| `J` | **NYC** | UPNY-ConEd interface studies, explicitly Zone J only |
| `ROS` | **ROS** | Upstate only |

Key decisions:
- **G-J → LHV only** (not NYC). The MMU J-only studies are the clean NYC data source; mixing in G-J projects would dilute NYC with a broader corridor value.
- **LI Export projects (locality=K, receiving=G-J) → LHV**, not LI. These export cables deliver benefit to the mainland (G-J), not to Long Island load.
- **LI zone uses only G-K projects** (Smart Path Connect, Eastover, AC Primary G-K), since G-K spans to zone K.
- **`addendum_optimizer_gj_elim`** is excluded as a sensitivity scenario.

## Derivation by zone

### ROS (Rest of State / Upstate)

**Source:** NiMo 2025 MCOS — Niagara-Dysinger only (receiving_locality=ROS).

Single project → v_avg = **$10.89/kW-yr** (345 kV, 1,100 MW, FY2036).

Smart Path Connect and Eastover have receiving_locality=G-K and contribute to LHV and LI, not ROS.

#### NiMo 2025 MCOS bulk TX project table (≥230 kV)

| Project                 | FN Ref   | Voltage    | In-Service | ΔMW   | Capital ($M) | v ($/kW-yr) | receiving_locality | Zones  |
| ----------------------- | -------- | ---------- | ---------- | ----- | ------------ | ----------- | ------------------ | ------ |
| Smart Path Connect      | FN008374 | 230/345 kV | FY2027     | 1,000 | $928.9       | $78.42      | G-K                | LHV+LI |
| Eastover 230kV Cap Bank | FN013189 | 230 kV     | FY2033     | 20    | $9.8         | $40.21      | G-K                | LHV+LI |
| Niagara-Dysinger        | FN013571 | 345 kV     | FY2036     | 1,100 | $142.2       | $10.89      | ROS                | ROS    |

### LHV (Lower Hudson Valley, zones G–J)

**Four contributing families** (all with receiving_locality=G-K or G-J):

| Family | Projects | Secants | v_avg |
| ------ | -------- | ------- | ----- |
| G-K/nimo_mcos | Smart Path (1,000 MW) + Eastover (20 MW) | [$40.21, $77.67] | $58.94 |
| G-K/ac_primary | Tier1+Tier2 (1,850 MW) | [$45.41] | $45.41 |
| G-J/addendum_optimizer | T027+T029 (1,300 MW) + T027+T019 (1,850 MW) | [$26.92, $25.27] | $26.10 |
| K/li_export | 7 LI Export projects (1,514–2,829 MW) | [$74.54, $74.36, $57.80, $44.21, $45.86, $43.79, $39.84] | $54.34 |

Zone v_avg = mean($58.94, $45.41, $26.10, $54.34) = **$46.20/kW-yr**

### NYC (New York City, zone J)

**Source:** MMU Baseline + CES+Ret at UPNY-ConEd interface (receiving_locality=J).

Baseline and CES+Ret averaged per project → two collapsed projects: T027+T029 (350 MW, avg B=$18.52M) and T027+T019 (375 MW, avg B=$20.73M). Cumulative secants: [$52.93, $54.14].

Zone v_avg = **$53.53/kW-yr**.

### LI (Long Island, zone K)

**Source:** G-K projects only (receiving_locality=G-K includes zone K).

| Family | v_avg |
| ------ | ----- |
| G-K/nimo_mcos (Smart Path + Eastover) | $58.94 |
| G-K/ac_primary (AC Primary Tier1+Tier2) | $45.41 |

Zone v_avg = mean($58.94, $45.41) = **$52.17/kW-yr**

LI Export projects (locality=K, direction LI→mainland) have receiving_locality=G-J and go to LHV, not LI — the benefit of expanded export capability accrues to mainland load, not LI load.

## Final v_avg values (output of `just derive`)

| gen_capacity_zone | v_avg_kw_yr | Contributing families |
| ----------------- | ----------- | --------------------- |
| ROS | $10.89 | ROS/nimo_mcos (Niagara-Dysinger) |
| LHV | $46.20 | G-K/nimo_mcos ($58.94), G-K/ac_primary ($45.41), G-J/addendum_optimizer ($26.10), K/li_export ($54.34) |
| NYC | $53.53 | UPNY-ConEd/mmu (MMU Baseline+CES+Ret, averaged per project) |
| LI  | $52.17 | G-K/nimo_mcos ($58.94), G-K/ac_primary ($45.41) |

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
and allocated to hourly $/MWh price signals using a two-level seasonal PoP method.

### Hourly allocation method

**Level 1 — between seasons (peak surplus above SCR floor):**

The fraction of v_z attributed to each season is derived from how far each season's
coincident peak exceeds the capacity-triggering floor τ_min:

```
τ_min = min(τ_summer, τ_winter)        [lower of the two SCR thresholds]
φ_s   = (peak_s − τ_min) / ((peak_s − τ_min) + (peak_w − τ_min))
φ_w   = 1 − φ_s
```

τ_min is the load of the (N+1)th highest hour in the lower-threshold season (almost always
winter). Only load *above* this floor is "capacity-driving". Summer peaks far exceed τ_min;
winter peaks barely clear it — yielding φ_s ≈ 0.80–0.93 across NY utilities.

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
bulk_tx_cost_enduse_h = pi_h × 1000 [$/MWh]
```

Global weights sum to 1 by construction: φ_s·Σw_s + φ_w·Σw_w = φ_s + φ_w = 1.

**Resulting φ values (2025 runs):**

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

The hourly bulk Tx MC trace (8760 rows, $/MWh) is loaded and combined with the delivery-side
dist+sub-tx MC in `utils/cairo.py`:

- **`load_bulk_tx_marginal_costs(path)`**: Loads bulk Tx MC parquet, converts $/MWh → $/kWh, returns Series with EST timezone
- **`_align_mc_to_index(mc_series, target_index, mc_type)`**: Shared utility for aligning any MC Series to a target DatetimeIndex. Handles same-length position alignment (when MC file year differs from run year) and reindexing for different lengths.
- **`add_bulk_tx_and_distribution_marginal_cost(path_distribution_mc, path_bulk_tx_mc, target_index)`**: High-level function that:
  1. Loads dist+sub-tx MC (required; `mc_total_per_kwh` from `generate_utility_tx_dx_mc.py`)
  2. Loads bulk Tx MC (optional)
  3. Aligns both to target index (typically from bulk MC)
  4. Sums them into a single delivery MC Series
  5. Validates and logs statistics

This design keeps all MC loading/alignment logic in `utils/cairo.py`, making `run_scenario.py` clean and maintainable. The combined delivery MC is then passed to CAIRO's `add_delivery_mc` alongside supply MC (energy + capacity).

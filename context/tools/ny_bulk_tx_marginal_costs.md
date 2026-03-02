# NY bulk transmission marginal costs

How bulk transmission marginal costs (v_z, $/kW-yr) are derived per `gen_capacity_zone` for NY utilities.

## Script

`data/nyiso/transmission/derive_tx_values.py`

CLI: `--path-projects-csv <input> --path-output-csv <output>`

Output: `s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_values.csv`
Schema: `gen_capacity_zone`, `v_low_kw_yr`, `v_mid_kw_yr`, `v_high_kw_yr`, `v_isotonic_kw_yr`

Justfile: `data/nyiso/transmission/Justfile` (recipes: `derive`, `upload`, `clean`)

## Data sources

| Dataset                            | Source                                         | What it provides                                                                                                           |
| ---------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| NYISO AC Transmission study (2019) | GH issue #302                                  | Project-level ΔMW and annual benefit ($M/yr) for AC Primary, Addendum Optimizer, and MMU scenarios across NYISO localities |
| NYISO LI Export study (2020)       | GH issue #302                                  | LI Export (Policy) projects with ΔMW and benefit                                                                           |
| NiMo 2025 MCOS (project workbook)  | `context/papers/mcos/nimo_2025_mcos.md`        | Per-project undiluted $/kW-yr for three bulk TX projects (≥230 kV), used for ROS zone                                      |
| OATT proxies (NYSEG, RG&E, CenHud) | `context/domain/ny_mcos_studies_comparison.md` | Cross-reference for total transmission costs (bulk + sub); not used directly for bulk TX v_z                               |

## Derivation by zone

### LHV (Lower Hudson Valley, zones G–K)

**Source:** NYISO AC Primary G–K projects + Addendum Optimizer G–J projects.

Multiple comparable projects with published ΔMW enable both quantile distributions (P25/P50/P75) and isotonic regression fitting. The isotonic curve B = g(ΔMW) fits a non-decreasing piecewise linear function subject to g(0) = 0, yielding a marginal cost slope in $/kW-yr.

**Result:** ~$43/kW-yr (mid).

### NYC (New York City, zones G–J via UPNY-ConEd interface)

**Source:** MMU scenarios (Baseline + CES+Retirement) at the UPNY-ConEd interface.

These represent the marginal benefit of expanding the transmission corridor into downstate load, driven by generation retirement scenarios. Projects have published ΔMW for quantile and isotonic derivation.

**Result:** ~$55/kW-yr (mid).

### LI (Long Island, zone K)

**Source:** LI Export Policy projects (T035–T053) with published ΔMW.

Represents the marginal benefit of expanding LI export capability. Quantile and isotonic derivation from the set of comparable projects.

**Result:** ~$36/kW-yr (mid).

### ROS (Rest of State / Upstate, zones A–F) — NiMo MCOS project-level approach

**Source:** NiMo 2025 MCOS, three bulk TX projects (all ≥230 kV).

**Method:** Undiluted $/kW-yr per project — the same approach used for generation capacity MC. Each project's v = ECCR annual cost ÷ its own added capacity (kW). We do **not** divide by NiMo's system peak (6,616 MW); that would yield a diluted $13.19/kW-yr levelized figure, which is not what we want. The undiluted per-project values feed directly into the Steps 1–3 isotonic/quantile pipeline.

The NYISO AC Transmission study is not used for ROS:

- The A–F locality has **negative** annual benefit in the AC Primary scenario (procurement cost increase from upstate generation displacement).
- The NYCA system-wide benefit scaled by upstate load share (~23%) gives ~$10/kW-yr — an interface-congestion-relief measure, not an infrastructure LRMC.

#### NiMo 2025 MCOS bulk TX project table (≥230 kV)

| Project                 | FN Ref   | Voltage    | In-Service | Capacity (MW) | Capital ($M) | Undiluted ($/kW-yr) |
| ----------------------- | -------- | ---------- | ---------- | ------------- | ------------ | ------------------- |
| Smart Path Connect      | FN008374 | 230/345 kV | FY2027     | 1,000         | $928.9       | $78.42              |
| Eastover 230kV Cap Bank | FN013189 | 230 kV     | FY2033     | 20            | $9.8         | $40.21              |
| Niagara-Dysinger        | FN013571 | 345 kV     | FY2036     | 1,100         | $142.2       | $10.89              |
| **Total**               |          |            |            | **2,120**     | **$1,080.9** |                     |

- **Undiluted $/kW-yr** = E_total column = ECCR × capital/MW at in-service-year nominal prices (F-column value for the in-service year).
- These are the values entered in `ny_bulk_tx_projects.csv` as `annual_benefit_m_yr` = undiluted $/kW-yr × delta_mw / 1000 ($M/yr), so Step 1 recovers v = B / (ΔMW × 1000) = undiluted $/kW-yr exactly.
- **Diluted system-wide levelized** (÷ 6,616 MW NiMo peak) = $13.19/kW-yr. This is **not** used — bulk TX is treated as undiluted, consistent with generation capacity MC.

#### Notes on individual projects

- **Smart Path Connect** dominates in capital (86% of bulk TX spend) and enters service FY2027, so it is active for nearly the entire study horizon and drives the levelized value.
- **Niagara-Dysinger** is large in MW (1,100) but cheap per kW ($10.89/kW-yr) — it is a 345 kV line with modest capital for its scale. It enters service FY2036 (the last year of the study), so its effect on any levelized or time-averaged figure is small.
- **Eastover** is a minor capacitor bank (20 MW, $9.8M); it enters service FY2033.

#### Resulting v_z distribution for ROS

Three undiluted values: Niagara-Dysinger $10.89, Eastover $40.21, Smart Path $78.42 /kW-yr.
Output of `just derive` in `data/nyiso/transmission/`:

| Column             | Value        | Basis                                                       |
| ------------------ | ------------ | ----------------------------------------------------------- |
| `v_low_kw_yr`      | $40.21/kW-yr | P25 = Eastover (Polars nearest-quantile on 3 pts; see note) |
| `v_mid_kw_yr`      | $40.21/kW-yr | P50 = Eastover (median of three values)                     |
| `v_high_kw_yr`     | $78.42/kW-yr | P75 = Smart Path Connect                                    |
| `v_isotonic_kw_yr` | $40.21/kW-yr | Median slope from isotonic B = g(ΔMW) fit (see below)       |

**Note on P25 = P50:** With only 3 data points, Polars "nearest" quantile maps both P25 and P50 to the middle value (Eastover, $40.21). Niagara-Dysinger ($10.89) is the minimum and sits below P25 — it does not appear in any quantile column. If you want it to influence `v_low`, run the script with a different quantile interpolation or add more ROS project data points.

**Isotonic fit:** sorted by ΔMW, the B = g(ΔMW) curve passes through (0, 0), (20, $0.804M), (1000, $78.42M), (1100, $91.20M). The slope drops sharply from Smart Path ($78.42/kW-yr) to Niagara-Dysinger ($10.89/kW-yr), so PAV pools those two segments. The three resulting piecewise slopes are [$40.21, ~$45.3, ~$0] /kW-yr; the median is **$40.21/kW-yr**.

#### Why not use the OATT proxies for ROS v_mid?

OATT (Open Access Transmission Tariff) revenue requirements for upstate utilities span $43–55/kW-yr. However, those values reflect **all** transmission assets (including sub-transmission at 69–230 kV), not just bulk TX (≥230 kV). The NiMo MCOS project-level breakdown is the right source because it isolates the three ≥230 kV projects explicitly. The OATT figures remain useful for cross-checking that the overall (bulk + sub) transmission cost is in the right ballpark.

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

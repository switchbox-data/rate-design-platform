# NY bulk transmission marginal costs

How bulk transmission marginal costs (v_z, $/kW-yr) are derived per `gen_capacity_zone` for NY utilities.

## Script

`data/nyiso/transmission/derive_tx_values.py`

CLI: `--path-projects-csv <input> --path-output-csv <output>`

Output: `s3://data.sb/nyiso/transmission/ny_bulk_tx_values.csv`
Schema: `gen_capacity_zone`, `v_low_kw_yr`, `v_mid_kw_yr`, `v_high_kw_yr`, `v_isotonic_kw_yr`

Justfile: `data/nyiso/transmission/Justfile` (recipes: `derive`, `upload`, `clean`)

## Data sources

| Dataset | Source | What it provides |
| --- | --- | --- |
| NYISO AC Transmission study (2019) | GH issue #302 | Project-level ΔMW and annual benefit ($M/yr) for AC Primary, Addendum Optimizer, and MMU scenarios across NYISO localities |
| NYISO LI Export study (2020) | GH issue #302 | LI Export (Policy) projects with ΔMW and benefit |
| NiMo 2025 MCOS (Exhibit 1) | `context/papers/mcos/nimo_2025_mcos.md` | Forward-looking LRMC for T-Station and T-Line infrastructure, used for ROS zone |
| OATT proxies (NYSEG, RG&E, CenHud) | PR #286 MCOS studies comparison | Cross-validation of upstate bulk Tx costs |

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

### ROS (Rest of State / Upstate, zones A–F) — NiMo MCOS approach

**Source:** NiMo 2025 MCOS, T-Station + T-Line marginal cost components.

The NYISO AC Transmission study does **not** directly provide a usable upstate bulk Tx MC:

- The A–F locality has **negative** annual benefit in the AC Primary scenario (procurement cost increase from added upstate generation displacement).
- Scaling the NYCA system-wide benefit (~$43/kW-yr) by the upstate load share (~23%) yields only ~$10/kW-yr.

This ~$10/kW-yr figure measures **incremental interface benefit** — the value of relieving congestion at NYISO transmission interfaces — not the **infrastructure cost** of building or expanding bulk transmission assets that serve upstate load. These are different economic quantities:

| Concept | What it measures | Typical value |
| --- | --- | --- |
| Interface benefit (NYISO study) | Congestion relief value at interfaces | ~$10/kW-yr (upstate share) |
| Infrastructure LRMC (MCOS) | Forward-looking cost of new T assets | ~$43–55/kW-yr |
| Embedded cost (OATT ATRR) | Current revenue requirement recovery | ~$43–55/kW-yr |

For the BAT, we need infrastructure cost — the marginal cost of serving 1 additional kW of upstate load through bulk transmission infrastructure. NiMo's MCOS provides exactly this.

#### NiMo MCOS T-Station + T-Line breakdown

From the NiMo 2025 MCOS (Exhibit 1, system-wide summary, line 245):

| Component | Capital ($M) | ECCR | MC ($/kW-yr) |
| --- | --- | --- | --- |
| T-Station | $2,316M | 8.21% | $16 |
| T-Line | $5,259M | 8.44% | $38 |
| **T total** | **$7,576M** | — | **$54** |
| D-Station | $1,895M | 8.06% | $13 |
| D-Line | $1,233M | 14.13% | $15 |

- **Total system capacity:** 11,533 MW of additions over FY2026–2036.
- **System-wide total MC:** $71.5/kW-yr (all four components).
- **T-Station + T-Line = $54/kW-yr** is the bulk transmission portion.
- Transmission assets operate at ≥69 kV.
- This is a forward-looking **LRMC** (Long-Run Marginal Cost), inflation-adjusted to FY2026 base year using 2.1% Blue Chip consensus forecast.
- The T-Line component ($38/kW-yr) dominates and includes inter-substation transmission lines and network upgrades — these are the bulk transmission assets.
- The T-Station component ($16/kW-yr) includes high-voltage substation equipment (transformers, breakers, switchgear at ≥69 kV).

#### Cross-validation with OATT proxies

OATT (Open Access Transmission Tariff) Annual Transmission Revenue Requirements provide an embedded-cost proxy for bulk Tx (from PR #286):

| Utility | OATT proxy ($/kW-yr) | Basis |
| --- | --- | --- |
| NYSEG | ~$53 | Embedded Tx revenue requirement |
| RG&E | ~$43 | Embedded Tx revenue requirement |
| CenHud | ~$55 | Embedded Tx revenue requirement |
| NiMo T+T | $54 | Forward-looking LRMC |

All upstate utilities converge on **$43–55/kW-yr** range. NiMo's $54 is well within this range. The OATT values measure embedded cost (current RR recovery), while NiMo's MCOS measures forward-looking LRMC — their convergence gives high confidence.

**Result for ROS:** $54/kW-yr (mid), with sensitivity range $43–57/kW-yr based on OATT cross-checks.

## Why the NYISO study v_z ≈ $10/kW-yr is wrong for upstate

The NYISO AC Transmission and LI Export studies were designed to evaluate **specific transmission expansion projects** at NYISO interfaces. They quantify the **benefit** (reduced congestion, production cost savings) of adding MW of transfer capability at specific interfaces.

For downstate zones (LHV, NYC, LI), these interface benefits are a reasonable proxy for bulk Tx MC because the binding constraint for serving incremental load is the interface itself — adding load in these areas drives the need for the transmission projects studied.

For upstate (ROS / zones A–F), the situation is fundamentally different:

1. **Upstate is a net exporter** — additional upstate generation reduces downstate congestion but doesn't create upstate transmission investment need.
2. **The A–F benefit is negative** in the AC Primary scenario — upstate doesn't benefit from more AC transmission; it actually faces higher costs from displaced generation.
3. **Upstate bulk Tx infrastructure needs** are driven by internal load growth and reliability, not interface transfers. These needs are captured by the MCOS/OATT values.
4. **The NYCA system benefit (~$43/kW-yr)** accrues primarily to downstate load (the load behind constrained interfaces), so scaling it by upstate load share is not economically meaningful.

## Zone mapping

The `tx_locality` column in `ny_utility_zone_mapping.csv` maps each utility to its transmission locality zone. Currently `tx_locality` equals `gen_capacity_zone` for all utilities:

| Utility | Zones | tx_locality | gen_capacity_zone |
| --- | --- | --- | --- |
| cenhud | G | LHV | LHV |
| coned | G, H, J | NYC / LHV | NYC / LHV |
| nimo | A–F | ROS | ROS |
| nyseg | A–F | ROS | ROS |
| or | G | LHV | LHV |
| rge | B | ROS | ROS |
| psegli | K | LI | LI |

## Integration with CAIRO

Bulk Tx MC values from this pipeline are consumed by `utils/pre/generate_utility_supply_mc.py` (or a dedicated bulk Tx MC generator) and allocated to hourly price signals using SCR top-40-per-season peak hours, similar to generation capacity MC allocation but using seasonal rather than monthly peaks. The hourly $/kWh trace is then included in CAIRO's `bulk_marginal_costs` input alongside energy and generation capacity.

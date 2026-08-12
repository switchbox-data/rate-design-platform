# Supply capacity 8760 pipelines: NYISO, ISO-NE, and PJM

How each ISO's annual generation-adequacy cost is translated into an 8760-row
`capacity_cost_enduse` (`$/MWh`) parquet for CAIRO BAT runs. All three pipelines
share the same final unit chain but differ in price source, blending mechanics,
peak-hour selection, and zone-to-utility mapping.

Related docs:

- `context/methods/marginal_costs/capacity_market_comparison_nyiso_isone.md` — price-signal trade-offs (FCA vs MRA vs Spot)
- `context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md` — full PJM decision record (A–G choices)
- `context/code/data/isone_fca_yaml_data_guide.md` — ISO-NE FCA YAML data dictionary and sources
- `context/code/marginal_costs/ny_supply_marginal_costs.md` — NY supply pipeline (energy + capacity)

---

## Final unit chain (identical for all three ISOs)

```
annual $/kW-year
  → exceedance or equal-weight allocation across K peak hours
  → $/kW per peak hour    (non-peak hours = 0.0)
  → × 1000 via prepare_component_output
  → $/MWh = capacity_cost_enduse
```

Output schema for all three: `timestamp` (naive datetime), `capacity_cost_enduse`
(`$/MWh`), 8760 rows, non-peak hours zeroed.

---

## NYISO — New York (NY)

**Script:** `utils/data_prep/marginal_costs/supply_capacity_nyiso.py`
**CLI:** `generate_supply_capacity_mc.py --iso nyiso --utility <name> --year <Y>`
**Output:** `s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility={utility}/year={Y}/`

### Price source

NYISO **ICAP Spot** auction (`s3://data.sb/nyiso/icap/year={Y}/month={M}/data.parquet`).
Spot prices are set each month and reflect contemporaneous supply/demand conditions
— they vary month-to-month and are the most granular signal NYISO offers.

This is a deliberate choice to answer: _"what does capacity cost right now?"_
rather than what was contracted. See trade-offs in
`capacity_market_comparison_nyiso_isone.md`.

### Price blending

No blending required. Spot produces one `$/kW-month` price per ICAP locality per
month; twelve prices directly cover the calendar year.

Annual total: `Σ(price_per_kw_month)` over 12 months per locality.

### Zone-to-utility mapping

**Most complex of the three ISOs.** Each utility maps to one or more
`(icap_locality, gen_capacity_zone, capacity_weight)` components via
`data/nyiso/zone_mapping/ny_utility_zone_mapping.csv`.

| Utility | Load zones | ICAP locality | Capacity weight |
| ------- | ---------- | ------------- | --------------- |
| cenhud  | G          | GHIJ          | 1.0             |
| coned   | G, H, I, J | NYC           | 0.87            |
| coned   | G, H, I, J | GHIJ          | 0.13            |
| nimo    | A–F        | NYCA          | 1.0             |
| nyseg   | A–F        | NYCA          | 1.0             |
| or      | G          | GHIJ          | 1.0             |
| rge     | B          | NYCA          | 1.0             |
| psegli  | K          | LI            | 1.0             |

**Component-by-component logic:** Each row is computed independently. Peak hours
are identified from the **nested locality** load profile (NYCA, LHV, NYC, or LI
zone aggregates). The ICAP price applied is from the **partitioned locality**
(ROS/LHV/NYC/LI) — the non-overlapping pricing regions. `capacity_weight` scales
only the cost, never the load used for peak identification. Component results are
summed to the utility-level hourly signal.

Locality hierarchy:

- NYCA (nested) = all 11 zones; priced via ROS (zones A–F)
- LHV (nested) = zones G–J (GHIJ mapping); priced via LHV
- NYC (nested) = zone J; priced via NYC
- LI (nested) = zone K; priced via LI

### Peak allocation: monthly exceedance

For each month and each locality component:

1. Rank hours in the month by zone-aggregate load (descending).
2. Select top 8 hours.
3. Threshold = highest load strictly below the 8th-highest (tie-safe).
4. `exceedance_h = load_h − threshold` for each top-8 hour.
5. `weight_h = exceedance_h / Σ(exceedance in month)`.
6. `capacity_cost_h = weight_h × (icap_spot_price_month × capacity_weight)`.

### Nonzero hours in final output

- **Single-locality utility** (cenhud, nimo, nyseg, or, rge, psegli): `8 × 12 = 96` nonzero hours.
- **Multi-locality utility** (ConEd): union of peaks from both components = **up to 192** distinct nonzero hours (NYC peaks + LHV peaks; overlap possible since LHV ⊃ NYC).

### Validation

Sum of all hourly `capacity_cost_per_kw` = `Σ_locality(capacity_weight × Σ_month(icap_spot_price))` within 0.01%.

---

## ISO-NE — Rhode Island (RI)

**Script:** `utils/data_prep/marginal_costs/supply_capacity_isone.py`
**CLI:** `generate_supply_capacity_mc.py --iso isone --utility rie --year <Y>`
**Output:** `s3://data.sb/switchbox/marginal_costs/ri/supply/capacity/utility=rie/year={Y}/`

### Price source

ISO-NE **Forward Capacity Auction (FCA)** clearing price for the **SENE zone**
(Southeast New England: RI + SEMA), from
`s3://data.sb/isone/capacity/fca/data.parquet`.

FCA prices are locked in three years ahead of the Capacity Commitment Period
(CCP) and never change — they reflect committed procurement cost, not the
current marginal cost of capacity. This answers: _"what did we contract to pay?"_

The YAML config (`rate_design/hp_rates/ri/config/marginal_costs/isone_fca_assumptions.yaml`)
stores the `payment_rate_per_kw_month` (raw FCA clearing price) and RI winter CSO
per CCP segment. **The CSO is not used in the per-kW computation** — it is
recorded for documentation only; the pipeline computes a per-kW marginal cost,
not total supplier revenue.

### Price blending: two-CCP blend

A calendar year always spans two ISO-NE CCPs (each runs June 1 – May 31):

| Segment | CCP                       | Calendar-year months covered |
| ------- | ------------------------- | ---------------------------- |
| CCP1    | Jun (year−1) – May (year) | Jan–May = **5 months**       |
| CCP2    | Jun (year) – May (year+1) | Jun–Dec = **7 months**       |

```
capacity_cost_kw_year = price_CCP1 × 5 + price_CCP2 × 7
```

Example (2025): FCA 15 SENE `$3.980/kW-mo × 5 = $19.900`, FCA 16 SENE
`$2.639/kW-mo × 7 = $18.473` → total `$38.373/kW-year`.

If the primary SENE zone (ID 8506) has no entry for a CCP (e.g. FCA 17–18 where
all zones cleared at the same price), the pipeline falls back to the System/Rest-of-Pool
zone (ID 8500).

### Zone-to-utility mapping

**Simplest of the three ISOs.** All RI utilities map to the same SENE capacity zone.
Zone loads are summed across RI + SEMA (`ISONE_CAPACITY_ZONE_LOAD_ZONES` in
`supply_utils.py`). No per-utility zone split or weighting is required.

### Peak allocation: annual exceedance

Unlike NYISO, FCA commits a single annual price — there is no monthly price
granularity to match. Allocation uses the top **100** highest-load hours across
the full calendar year:

1. Sum RI + SEMA hourly loads to form the SENE aggregate (8760 rows).
2. Rank all 8760 hours by load (descending); select top 100.
3. Threshold = highest load strictly below the 100th-highest.
4. `exceedance_h = load_h − threshold`.
5. `weight_h = exceedance_h / Σ(exceedance over 100 hours)`.
6. `capacity_cost_h = weight_h × capacity_cost_kw_year`.

The 100-hour annual window was chosen for consistency with the RI dist/bulk-TX
component pipelines (which also use annual exceedance). It has no direct ISO-NE
citation analog to PJM's 5CP, but it is calibrated to capture the highest-demand
hours across all seasons, which is appropriate for a winter-peaking ISO.

### Nonzero hours in final output

**100** nonzero hours (all in the highest-load segment of the year, predominantly winter for RI given ISO-NE's winter peak profile).

### Validation

Sum of all hourly `capacity_cost_per_kw` = `capacity_cost_kw_year` within 0.01%.

---

## PJM — Maryland / BGE (MD)

**Script:** `utils/data_prep/marginal_costs/supply_capacity_pjm.py`
**CLI:** `generate_supply_capacity_mc.py --iso pjm --utility bge --year <Y>`
**Output:** `s3://data.sb/switchbox/marginal_costs/md/supply/capacity/utility=bge/year={Y}/`

### Price source

PJM **RPM Final Zonal Capacity Price** (`$/MW-day`) from the curated dataset at
`s3://data.sb/pjm/capacity/rpm/data.parquet` (DY 2018/19–2026/27). This is the
per-zone weighted combination of BRA + finalized Incremental Auction prices,
inclusive of the locational adder for constrained LDAs.

BGE is a **constrained LDA** in most delivery years (6 of 9 in the dataset): its
Final Zonal Capacity Price exceeds the RTO system price. The pipeline selects
`zone == "BGE"` from the RPM dataset — never the RTO row.

### RPM dataset schema and key concepts

The curated RPM parquet is **one row per (delivery_year, zone)** — not a
timeseries. There are ~20 zones × 9 delivery years ≈ 180 rows. It is a small
lookup table, not hourly data. Schema:

| Column                                  | Type   | Example              | Meaning                                           |
| --------------------------------------- | ------ | -------------------- | ------------------------------------------------- |
| `delivery_year`                         | String | `"2025/26"`          | PJM delivery year label                           |
| `dy_start`                              | Date   | `2025-06-01`         | Start of the DY                                   |
| `dy_end`                                | Date   | `2026-05-31`         | End of the DY                                     |
| `zone`                                  | String | `"BGE"`              | PJM zone (= EDC territory)                        |
| `lda`                                   | String | `"BGE"` or `"RTO"`   | Most-specific LDA that cleared separately that DY |
| `bra_price_per_mw_day`                  | Float  | `466.35`             | BRA clearing price for the zone's LDA             |
| `final_zonal_capacity_price_per_mw_day` | Float  | `471.33`             | Final price after IAs settle (what LSEs pay)      |
| `source_url`                            | String | (PJM XLS link)       | Final Zonal price provenance                      |
| `bra_source_url`                        | String | (PJM XLS link)       | BRA price provenance                              |
| `final_price_as_of`                     | Date   | `2025-03-11`         | When the final zonal price was last updated       |
| `notes`                                 | String | `"BGE constrained…"` | Editorial notes                                   |

**BRA (Base Residual Auction)** — PJM's primary capacity procurement auction.
Runs once per delivery year, ~3 years in advance. Generators bid to supply
capacity; the auction clears at a price per LDA. `bra_price_per_mw_day` is the
initial price set at this auction.

**Final Zonal Capacity Price** — the blended result of the BRA plus all
subsequent Incremental Auctions (IAs). This is what Load Serving Entities
_actually_ pay per MW-day during the delivery year.
`final_zonal_capacity_price_per_mw_day` is the number the pipeline uses.

Both columns exist to show how much the price moved after the BRA. For BGE DY
2025/26: BRA cleared at `$466.35`, IAs adjusted it to `$471.33` (+`$4.98`).

**RTO (Regional Transmission Organization)** — means PJM-wide, the entire
13-state footprint treated as a single unconstrained region. When a zone's `lda`
column says `"RTO"`, that zone cleared at the system-wide price (no local
transmission constraint binding). When it says `"BGE"`, BGE was a constrained
LDA that delivery year — local generation scarcity drove its price above the
system level.

BGE's LDA status flips between constrained and unconstrained across years:

| DY      | lda | Final Zonal price | RTO price | Spread               |
| ------- | --- | ----------------- | --------- | -------------------- |
| 2023/24 | BGE | `$72.15`          | `$34.18`  | +`$37.97`            |
| 2024/25 | BGE | `$76.76`          | `$29.50`  | +`$47.26`            |
| 2025/26 | BGE | `$471.33`         | `$270.43` | +`$200.90`           |
| 2026/27 | RTO | `$329.08`         | `$329.08` | `$0` (unconstrained) |

### How the pipeline consumes this data

The pipeline does one thing: filter to `zone == "BGE"`, pull
`final_zonal_capacity_price_per_mw_day` for the two DYs that overlap the
calendar year, and day-count-blend them into a single `$/kW-year`. That scalar
is the total annual capacity cost. The separate 5CP dataset then provides the
five specific timestamps on which to place it.

### Price blending: exact day-count blend

PJM delivery years also run June 1 – May 31 (same boundary as ISO-NE CCPs).
Because the native price unit is `$/MW-day`, blending uses **exact day counts**
rather than whole months:

| Segment | DY                        | Calendar-year days covered                       |
| ------- | ------------------------- | ------------------------------------------------ |
| DY1     | Jun (year−1) – May (year) | Jan 1 – May 31 = **151 days** (152 in leap year) |
| DY2     | Jun (year) – May (year+1) | Jun 1 – Dec 31 = **214 days**                    |

```
capacity_cost_kw_year = (P_DY1 × 151 + P_DY2 × 214) / 1000
```

The `/1000` converts MW → kW. This is more precise than the ISO-NE 5/7 month
approximation and is dimensionally exact for a `$/MW-day` native unit.

### Zone-to-utility mapping

**One row per utility.** `data/pjm/zone_mapping/pjm_utility_zone_mapping.csv`
maps each utility to its PJM zone label (e.g. `bge → BGE`, `pepco → PEPCO`).
The pipeline selects that zone's row from the RPM dataset. Pepco and DPL serve
separate Maryland areas under different zones and LDAs; BGE prices must not be
reused for them.

### Peak allocation: equal weight across 5CP hours

PJM publishes **five coincident-peak hours** (5CP) per summer
(June 1 – September 30): the five highest non-holiday weekday RTO unrestricted
daily peaks, designated ~October each year. These are the hours that determine
each customer's Peak Load Contribution (PLC) and thus their share of capacity
obligation.

The curated 5CP dataset (`s3://data.sb/pjm/capacity/5cp/data.parquet`) stores
these timestamps. The pipeline:

1. Loads the five RTO 5CP timestamps for the calendar year's summer.
2. Assigns **equal weight `1/5`** to each hour.
3. `capacity_cost_h = capacity_cost_kw_year / 5` on each of the five hours.

Equal weighting (F1) is the **definitionally correct** PJM analog: PLC is defined
as the _average_ of a customer's reconciled load over the five hours (PJM Manual
19 §4.3), which is mathematically equivalent to giving each hour weight 1/5.
Exceedance weighting (used by NYISO and ISO-NE) is not appropriate here because
it re-introduces a within-peak load signal that the PLC average deliberately
removes.

**Important distinction:** PJM actually charges the LRC (Locational Reliability
Charge) every day across the full delivery year. The 5-hour concentration is a
BAT modeling choice that mirrors the obligation-causation signal — it is not a
PJM billing rule.

### Nonzero hours in final output

**5** nonzero hours — the lowest concentration of any ISO in the platform. All
five fall within June–September.

### Validation

Sum of all hourly `capacity_cost_per_kw` = `capacity_cost_kw_year` within 0.01%.

---

## Cross-ISO comparison

| Dimension               | NYISO (NY)                                           | ISO-NE (RI)                                 | PJM (MD/BGE)                             |
| ----------------------- | ---------------------------------------------------- | ------------------------------------------- | ---------------------------------------- |
| **Market**              | ICAP Spot (monthly)                                  | FCM FCA (annual, 3-yr forward)              | RPM BRA (annual delivery year)           |
| **Price philosophy**    | Contemporaneous marginal cost                        | Committed procurement cost                  | Committed procurement cost               |
| **Native price unit**   | `$/kW-month`                                         | `$/kW-month`                                | `$/MW-day`                               |
| **Period boundary**     | Monthly capability periods                           | Jun–May CCP                                 | Jun–May DY                               |
| **Blending**            | None (prices already monthly)                        | 5 mo × CCP1 + 7 mo × CCP2                   | 151 d × DY1 + 214 d × DY2 (÷ 1000)       |
| **Peak window**         | Monthly (all months)                                 | Annual (full calendar year)                 | Summer only (Jun–Sep)                    |
| **Peak selection**      | Top 8 hours per month per locality                   | Top 100 hours across year                   | PJM-published 5CP timestamps             |
| **Peak weighting**      | Exceedance                                           | Exceedance                                  | Equal 1/5 (PLC-average analog)           |
| **Nonzero hours**       | **96** (single-locality); **≤ 192** (ConEd)          | **100**                                     | **5**                                    |
| **Zone-to-utility**     | Component-by-component, up to 2 localities + weights | Single SENE zone, all RI utilities share it | One zone row per utility from RPM        |
| **LDA pricing**         | Partitioned localities (ROS/LHV/NYC/LI)              | SENE zone with RoP fallback                 | BGE zone row (includes LDA adder)        |
| **Price data source**   | `s3://data.sb/nyiso/icap/`                           | `s3://data.sb/isone/capacity/fca/`          | `s3://data.sb/pjm/capacity/rpm/`         |
| **Load data source**    | NYISO zone hourly demand                             | ISO-NE zone hourly demand (RI + SEMA)       | Not used (equal weighting needs no load) |
| **Zone mapping source** | `data/nyiso/zone_mapping/`                           | `supply_utils.py` constants                 | `data/pjm/zone_mapping/`                 |

### Why the methodologies differ

**Price signal (Spot vs FCA vs BRA):** The NY pipeline uses ICAP Spot to reflect
the current cost of capacity; the RI pipeline uses FCA because ISO-NE's 3-year
forward lock means there is no liquid spot equivalent analogous to NYISO Spot (the
MRA is surplus-driven and typically lower). PJM's RPM BRA is the analogue to FCA —
a committed annual adequacy cost — so MD also uses the committed price rather than
a near-term adjustment auction.

**Monthly vs annual allocation:** NYISO's monthly Spot prices vary enough that
month-by-month exceedance allocation is meaningful — a high-price summer month
gets its cost concentrated on that month's peak hours. ISO-NE FCA is a single
annual rate; monthly exceedance would create artificial price steps with no
market basis. PJM RPM similarly sets one price per delivery year; there is no
monthly granularity to exploit.

**K = 5 vs 8/month vs 100:** PJM uses K = 5 because the 5CP obligation mechanism
is a regulatory fact: five specific hours determine every customer's capacity share.
NYISO uses K = 8/month (96/year) as a practical balance between peak concentration
and cross-subsidy signal strength; the ICAP market has no equivalent "5CP" rule.
ISO-NE uses K = 100 for consistency with the RI dist/bulk-TX components (all three
use annual exceedance), and because ISO-NE's obligation is driven by seasonal peaks
without a specific handful of statutory hours.

**Equal weighting vs exceedance:** PJM's PLC definition averages load over five
hours, so equal weighting is the correct analog. NYISO and ISO-NE have no
equivalent averaging rule; exceedance weighting better represents the continuous
load relationship to peak risk in those markets.

**Zone-to-utility complexity:** NY has seven utilities spanning multiple NYISO
capacity localities with split pricing regions; the component approach preserves
each locality's independent price signal. RI has one utility (RIE) that maps
entirely to SENE; no splitting is needed. MD BGE maps one-to-one to a PJM price
zone.

---

## Shared infrastructure

All three pipelines use shared helpers from `utils/data_prep/marginal_costs/supply_utils.py`:

| Helper                                                            | Used by                                                   |
| ----------------------------------------------------------------- | --------------------------------------------------------- |
| `allocate_annual_exceedance_to_hours`                             | ISO-NE; available as sensitivity for PJM (C3)             |
| `build_cairo_8760_timestamps`                                     | All three (reference grid, leap-year / DST normalization) |
| `prepare_component_output(scale=1000)`                            | All three (`$/kW` → `$/MWh`)                              |
| `PJM_UTILITY_ZONES`                                               | PJM zone lookup                                           |
| `ISONE_UTILITY_CAPACITY_ZONES` / `ISONE_CAPACITY_ZONE_LOAD_ZONES` | ISO-NE zone lookup                                        |

All pipelines validate that the sum of `capacity_cost_per_kw` over all nonzero
hours equals the annualized `$/kW-year` within 0.01%.

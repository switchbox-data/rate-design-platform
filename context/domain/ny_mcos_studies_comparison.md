# NY 2025 MCOS studies: cross-utility comparison

Comparison of the seven Marginal Cost of Service studies filed under NY PSC Docket 19-E-0283 in 2025 (six filings covering seven utilities). All studies respond to the August 19, 2024 Order Addressing Marginal Cost of Service Studies, which directed the Joint Utilities to file MCOS studies using the NERA methodology with a 10-year planning horizon.

Source extractions are in `context/papers/mcos/`. For how we derive BAT inputs from these studies, see `context/domain/ny_dist_mc_bat_methodology.md`.

---

## 1. Overview

### Who did the studies

| Utility                        | Shortcode  | Consultant                     | Date filed       | Total pages |
| ------------------------------ | ---------- | ------------------------------ | ---------------- | ----------- |
| Consolidated Edison            | ConEd      | In-house (NERA methodology)    | June 2025        | 20          |
| Orange & Rockland              | O&R        | In-house (NERA methodology)    | June 2025        | 20          |
| Central Hudson                 | CenHud     | Demand Side Analytics (DSA)    | June 2025        | 23          |
| Niagara Mohawk / National Grid | NiMo       | In-house                       | June 30, 2025    | 49          |
| NYSEG + RG&E                   | NYSEG/RG&E | Charles River Associates (CRA) | July 15, 2025    | 80          |
| PSEG Long Island / LIPA        | PSEG-LI    | In-house                       | December 9, 2025 | 11          |

### Study periods

| Utility | Study period  | Years | Notes                                               |
| ------- | ------------- | ----- | --------------------------------------------------- |
| ConEd   | 2025–2034     | 10    | Calendar year                                       |
| O&R     | 2025–2034     | 10    | Calendar year                                       |
| CenHud  | 2026–2035     | 10    | Calendar year                                       |
| NiMo    | FY2026–FY2036 | 11    | Fiscal year (Apr–Mar); FY2026 = Apr 2025 – Mar 2026 |
| NYSEG   | 2026–2035     | 10    | Calendar year                                       |
| RG&E    | 2026–2035     | 10    | Calendar year                                       |
| PSEG-LI | 2025–2032     | 8     | Below the 10-year requirement                       |

---

## 2. What MCOS numbers mean: undiluted, diluted, and why we dilute

### What a marginal cost of service study produces

An MCOS study starts with a list of planned capital projects — substation upgrades, feeder rebuilds, transmission line reconductoring, etc. Each project has a capital cost and a capacity (MW) it adds or relieves. The study converts each project's capital cost into an **annualized revenue requirement** using an Economic Carrying Charge Rate (ECCR), which folds in return on/of capital, O&M, taxes, and insurance. The result, for each project, is an annual cost in $/yr.

Projects enter service at different times over the 10-year horizon. In any given year, the "bill" is the sum of annualized costs for all projects in service by that year. Early years have a small bill; later years accumulate more. The year-by-year tables in these studies show this growing annual bill.

### Undiluted: cost per MW of new capacity

The **undiluted** marginal cost divides the total annual bill by the total capacity added by the projects:

> undiluted MC = total annual revenue requirement ($) ÷ total capacity added (MW)

This answers: **"Given the projects we have planned, what is the average cost of adding one MW of capacity where we're constrained?"** It's a measure of construction economics — the price tag per MW of relief at locations that need it.

Only substations or areas with planned projects enter the calculation. Areas with ample headroom contribute nothing to the numerator and nothing to the denominator.

### Diluted: the annual infrastructure bill, allocated per MW of existing load

The **diluted** marginal cost divides the same total annual bill by the **system peak demand**:

> diluted MC = total annual revenue requirement ($) ÷ system peak (MW)

This answers a different question: **"How should the cost of the capital plan be allocated across all customers?"**

The utility needs to recover, say, $825M/yr from ratepayers to fund its infrastructure program. The standard regulatory approach for demand-related costs is to allocate them in proportion to each customer's contribution to system peak. If the system peak is 6,600 MW, then each MW of peak demand is responsible for $825M / 6,600 = $125k/yr.

The diluted number is a **rate-making construct**, not a physical one. It doesn't mean each MW of load "causes" $125k of infrastructure cost. It means each MW's **share of the bill** is $125k. It's like splitting a dinner check by headcount: if 10 people ran up a $500 tab, the diluted cost is $50/person regardless of who ordered what.

Substations with no planned investment contribute MW to the denominator but zero to the numerator. Their customers still pay the diluted rate — their load is part of the peak that "dilutes" the average. This is why the diluted number is always lower than the undiluted (except in unusual cases like NiMo, where bulk transmission projects inflate project capacity beyond system peak — see the NiMo section in §5).

### Why allocating by peak contribution is the convention

Allocating infrastructure costs by peak demand is an imperfect proxy for cost causation, but it's the standard approach because:

1. **You can't retroactively bill the customers who caused each investment.** A substation upgrade triggered by load growth 5 years ago must be paid for by today's customers.
2. **Peak contribution is observable and measurable.** Each customer's demand during system peak is recorded.
3. **The grid is interconnected.** Transmission upgrades benefit the whole system, not just the constrained area. Even distribution upgrades improve reliability for adjacent areas.
4. **Over time, it averages out.** Today's unconstrained substations were yesterday's constrained ones. The customer base turns over and investment needs rotate across the territory.
5. **The alternative is impossibly complex.** Tracing each project's cost to the specific customers whose load growth caused it would require perfect historical attribution across decades.

### What the diluted number is used for

For rate design, the diluted number is the right system-wide average to apply to all customers. Using the undiluted number system-wide would overcharge customers by pretending everyone is behind a constrained substation.

The diluted number also connects to time-of-use rates and demand charges: once you have an annual $/kW-yr, you can convert it to hourly price signals using probability-of-peak (PoP) analysis or top-hour allocation, so customers who shift load off-peak pay less. This is how the annual capital bill becomes an operational incentive.

For DER compensation (VDER), the diluted number is the floor (DRV — system-wide value of demand reduction). The undiluted number, or the PSC Staff's hybrid variant, is closer to the actual avoided cost at constrained locations, which is why LSRV (locational adder) exists on top of DRV.

### Grid topology and the three-bucket classification

The physical grid has a hierarchy: **bulk transmission** (230kV+) → **sub-transmission** (69–115kV) → **distribution substations** (stepping to 13kV) → **primary feeders** → **transformers** → **secondary/service**.

MCOS projects span multiple levels. Properly classifying them matters because:

- **Bulk TX** costs are FERC-jurisdictional and recovered through NYISO charges, not local distribution rates. They should not be included in distribution MC or DRV.
- **Sub-TX** costs (69–115kV lines and the transmission-side of distribution substations) are state-jurisdictional and DRV-relevant — DERs can defer these investments.
- **Distribution** costs (substations, feeders, transformers) are clearly local and DRV-relevant.

The conventional boundary: a substation belongs to the voltage level it **delivers to**. A 115kV→13kV substation is a distribution asset. A 345kV→115kV substation is sub-transmission. Lines belong to the voltage tier they operate at.

Not all MCOS studies make this classification explicit. NiMo lumps everything into "T-Station / T-Line / D-Station / D-Line" components without separating bulk from sub-TX. Cross-referencing with the NYISO Gold Book (which lists all planned bulk transmission projects with voltages) is necessary to separate the buckets — see the NiMo analysis in §5.

---

## 3. Methodology comparison

All studies nominally follow the NERA methodology as required by the Order. In practice:

### Similarities across all six filings

- Project-based: identify load-growth and multi-value capital projects in the planning horizon.
- Convert project cost to $/kW of added capacity, then annualize using an Economic Carrying Charge (ECC/ECCR) that includes return on/of capital, O&M, taxes, and insurance.
- Most utilities inflate year-by-year costs using the Blue Chip GDP Implicit Price Deflator (~2%/yr). CenHud's workbook is the exception (flat nominal). See §4 for verified rates.
- Present costs at the substation serving area level, by year, for each cost segment.
- Include both load-growth and multi-value (growth + reliability) projects.

### Key differences

| Dimension                         | ConEd / O&R                                                                                                             | CenHud                                                                                                                 | NiMo                                                                                                                              | NYSEG / RG&E                                                                                                                                     | PSEG-LI                                                                               |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| **Annualization**                 | Composite factor (ECCR + O&M + working capital + general plant loading, all folded into one multiplier per cost center) | Similar composite; separate ECCR by asset level, with explicit 30% reserve margin applied to $/kW before annualization | ECCR per asset type (T-Station 8.21%, T-Line 8.44%, D-Station 8.06%, D-Line 14.13%); no composite factor — just ECCR × capital/MW | ECC formula produces first-year annual revenue requirement constant in real terms over asset life; adds O&M and A&G loading factors separately   | ECCR (TX 4.79%, Dist 5.02%) plus separate loaders (general plant 2.9%, O&M 3.4%/8.9%) |
| **Escalation**                    | GDP deflator: 2.4% (2026), then 2.1%/yr (2027–2034); compounding escalation applied to all years                        | **None — flat nominal.** The only utility that does not escalate.                                                      | GDP deflator 2.1%/yr applied to each project's ECCR cost; F columns = E × 1.021^(year − in_service_year)                          | Annual inflation 2.0%/yr applied cumulatively to prior-year costs                                                                                | Not stated in extractable text                                                        |
| **Reserve margin**                | Not explicitly stated as a multiplier; embedded in capacity calculations                                                | 30% reserve margin applied to $/kW before annualization                                                                | Not explicitly stated; capacity added is project-level                                                                            | Capacity added × 0.9 to reflect 90% utilization planning criteria                                                                                | Not stated                                                                            |
| **Capital plan horizon**          | 10-year plan for TX and area station; 1–1.5 years for lower-voltage                                                     | 5-year corporate capital forecast; years 6–10 assumed similar proportion of territory with needs as years 1–5          | 10-year FY2026–FY2036 capital plan; excluded projects < $1M                                                                       | Capital plan through 2031; CAS (Comprehensive Area Studies) extend to 2032–2035; independent N-0 screening adds potential projects for 2033–2035 | 8-year plan FY2025–FY2032                                                             |
| **Diluted vs undiluted**          | Undiluted only (excludes zero-MC areas from system average)                                                             | Both (Table 1 = undiluted, Table 2 = diluted)                                                                          | Undiluted only (projects with capital only)                                                                                       | Both (Tables 1–2 = undiluted by division and year; Table 3 = diluted with % of system needing no investment)                                     | Not extractable                                                                       |
| **Loss adjustment**               | Not explicitly stated for system average                                                                                | Loss factors applied (1.01 TX, 1.02 sub, 1.05 feeder)                                                                  | Not stated                                                                                                                        | Total MC shown at both primary and secondary voltage (loss-adjusted)                                                                             | Not stated                                                                            |
| **Time differentiation**          | Not addressed in MCOS filing                                                                                            | Not addressed                                                                                                          | Not addressed                                                                                                                     | Section 5: probability-of-peak (PoP) analysis by hour, day-type, month; shift from summer to winter peaking by ~2028                             | Not addressed                                                                         |
| **Local distribution facilities** | Not addressed as a separate cost                                                                                        | Not addressed                                                                                                          | Not addressed                                                                                                                     | Section 6: per-customer-class $/kW-yr for secondary transformers, secondary lines, local primary taps (residential ~$110/kW-yr)                  | Not addressed                                                                         |

### Notable similarities

1. All use an inflation escalation rate near 2%/yr. ConEd and O&R use the Blue Chip GDP deflator (2.4% for 2026, then 2.1%); NiMo uses 2.1%; NYSEG/RG&E use 2.0%. CenHud's workbook is the lone exception (flat nominal).
2. All present year-by-year nominal $/kW at the substation level.
3. All include multi-value (growth + reliability) projects, per the Order.
4. None produce a single "levelized" system-wide number as the primary output — all emphasize that MC varies by location and year.

### Notable differences

1. **Diluted MC availability:** Only CenHud and NYSEG/RG&E provide a system-wide MC that includes zero-MC areas (the diluted number). ConEd and O&R only report the undiluted (projects-only) MC. PSEG-LI and NiMo's diluted values can be estimated from project data and system peak (see §5).
2. **Time differentiation:** Only NYSEG/RG&E (CRA) perform a probability-of-peak analysis. The others present annual $/kW-yr without hourly allocation.
3. **Local distribution facilities:** Only NYSEG/RG&E (CRA) estimate per-customer-class costs for secondary lines, transformers, and local primary taps. The NERA-method studies (ConEd, O&R) include secondary cable and transformers as system-level cost centers instead.
4. **Component granularity:** ConEd has the finest breakdown (5 cost centers); NiMo lumps everything into 4 broad asset types with no sub-breakdown of transmission vs sub-transmission; PSEG-LI has only 2 categories (TX, Dist).

---

## 4. Annualization mechanics: workbook-level detail

The following is based on direct inspection of the Excel workbooks at `s3://data.sb/ny_psc/mcos_studies_2025/`. Every number below was verified against the actual cells, not the PDF narratives.

### The common pipeline (all six utilities)

Despite label differences, every utility follows the same conceptual pipeline:

1. **Capital cost** ($) for each project or location
2. **Divide by capacity** (kW or MW) to get capital per unit of capacity added
3. **Apply loaders** (reserve margin, general plant, O&M, working capital, loss factors — varies by utility)
4. **Apply ECCR** to convert one-time capital to annual revenue requirement
5. **Escalate by inflation** (or not — see below) to get that-year-dollar costs
6. **Year-by-year presentation**: only include projects in service that year
7. **System-wide aggregation**: weight by load share (or sum and divide by system peak)
8. **Levelization**: collapse the year-by-year stream to a single number (workbooks use NPV with utility-specific discount rates)

The utilities differ in how they implement each step and in what order loaders and ECCR are applied.

### Step-by-step comparison

**Step 1–2: Capital to $/capacity**

| Utility   | Unit            | Method                                                                                                 |
| --------- | --------------- | ------------------------------------------------------------------------------------------------------ |
| NiMo      | $000s/MW        | Per-project capital ($000s) ÷ capacity (MW), split by component (T-Station, T-Line, D-Station, D-Line) |
| CenHud    | $/kW            | Per-project capital net of 2.5% salvage ÷ incremental capacity (kW)                                    |
| NYSEG/RGE | $/kW (from kVA) | Sum of investment across same-type facilities in a (division, year) ÷ sum of capacity added            |
| ConEd/O&R | $/kW            | Capital budget already expressed as cumulative $/kW per area substation per year                       |

CenHud and NYSEG/RGE work in kW; NiMo works in MW ($000s/MW = $/kW). ConEd/O&R track cumulative investment per kW at each area substation — new projects add to the running total each year rather than appearing as separate line items.

**Step 3: Loaders (reserve margin, plant loading, O&M, working capital, losses)**

This is where the approaches diverge most:

| Utility       | Reserve margin    | General plant loading | O&M                                      | Working capital                                    | Loss factors | Net effect                                                                                                              |
| ------------- | ----------------- | --------------------- | ---------------------------------------- | -------------------------------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------- |
| **NiMo**      | None              | None                  | None                                     | None                                               | None         | ECCR × capital/MW is the final cost. Simplest approach — all overhead is absorbed into the ECCR rates themselves.       |
| **CenHud**    | ×1.30 (30%)       | ×1.161 (16.1%)        | $0                                       | Materials (0.96%) + prepay (1.0%) → × 8.95% return | ×1.01–1.05   | Most explicit. Reserve margin and plant loading multiply the $/kW before ECCR. Working capital and losses add on after. |
| **NYSEG/RGE** | None in $/kW step | None in $/kW step     | Bundled into post-ECCR loader            | Bundled                                            | ×1.02–1.08   | ECCR result × ~1.32 (upstream/dist sub) or ~1.34 (feeders) = final annualized $/kW.                                     |
| **ConEd**     | Embedded          | Embedded (7–24%)      | O&M as % of reproduction cost (1.1–1.2%) | WC ~2.4% × 9.3% return                             | Embedded     | Everything folded into a single "composite rate" per cost center.                                                       |
| **O&R**       | Embedded          | Embedded (8–25%)      | O&M (1.9–4.4%)                           | WC ~2.5% × 9.0% return                             | Embedded     | Same structure as ConEd; single composite rate.                                                                         |

**Step 4: ECCR rates**

| Utility       | ECCR rates                                                    | How derived                                                                                            |
| ------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| **NiMo**      | T-Station 8.21%, T-Line 8.44%, D-Station 8.06%, D-Line 14.13% | 4 rates by component, stated in workbook row 3                                                         |
| **CenHud**    | Transmission 13.72%, Substation 13.33%, Distribution 17.83%   | 3 rates from "EP2023-012 Levelized Fixed Charge Rates"                                                 |
| **NYSEG/RGE** | Upstream sub/dist sub ~7.79%, Dist feeder ~7.35%              | Not explicitly labeled; derived from investment → annualized columns in Sheet 12                       |
| **ConEd**     | 10.07% (uniform base, all asset classes)                      | Derived from cost of capital, discount rate 8.42%, inflation 2.13%, 50-yr service life, 20-yr tax life |
| **O&R**       | 8.72% (uniform base, all asset classes)                       | Same formula as ConEd; discount rate 8.96%, 54-yr service life                                         |

NiMo's and NYSEG/RGE's base ECCR rates look low (7–8%) but they apply fewer loaders, so the effective all-in rate is similar. CenHud's rates look high (13–18%) because they stack a 30% reserve margin and 16.1% general plant loading on top of the $/kW _before_ applying ECCR. ConEd/O&R absorb ECCR into a composite rate (12–15%) that also includes O&M, plant loading, insurance, and working capital.

**The three approaches to annualization.** Steps 3 and 4 show a spectrum of how utilities convert capital to annual revenue requirement:

1. **Composite rate** (ConEd, O&R): One multiplier per cost center folds ECCR, general plant loading, O&M, working capital, loss factors, and (for ConEd) a coincidence factor into a single number. The formula is simply `Capital × Composite Rate × Escalation`. This is the cleanest to use but the hardest to decompose — you cannot back out the ECCR in isolation without reverse-engineering the loaders from Schedule 11/10.

2. **Separate loaders** (CenHud, NYSEG/RGE): The ECCR and each loader are applied as visible, sequential steps. CenHud is the most explicit: reserve margin (×1.30) and general plant loading (×1.161) multiply the $/kW _before_ ECCR, then working capital and loss factors add on _after_. NYSEG/RGE bundle loaders into a post-ECCR multiplier (~1.32–1.34×). Both approaches let you see how much each component contributes.

3. **Bare ECCR** (NiMo): No loaders at all. `ECCR × capital/MW` is the final annualized cost. All overhead is implicitly absorbed into the ECCR rates or is assumed to be captured elsewhere in the revenue requirement. Simplest to replicate, but the ECCR must carry all the weight.

**Step 5: Inflation escalation — the key divergence**

| Utility       | Escalates?       | Rate                                         | Mechanism                                                                                                                             |
| ------------- | ---------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **NiMo**      | Yes              | 2.1%/yr                                      | E column = ECCR cost at in-service year prices. F columns = E × 1.021^(year − in_service_year). Each project's cost grows every year. |
| **CenHud**    | **Workbook: no** | —                                            | Flat nominal. cost(2033) = cost(2034) = cost(2035).                                                                                   |
| **NYSEG/RGE** | Yes              | 2.0%/yr                                      | Cumulative ECC values grow at ×1.02 annually. In years with no new investment, costs still rise by 2%.                                |
| **ConEd**     | Yes              | GDP deflator: 2.4% (2026), then 2.1% (2027+) | Annual MC = cumulative $/kW × composite rate × compounding escalation factor (1.000, 1.024, 1.0455, 1.0675, ...)                      |
| **O&R**       | Yes              | Same GDP deflator as ConEd                   | Same mechanism as ConEd.                                                                                                              |

Verification from workbooks:

- **NiMo**: HAGUE (FN000507), in-service 2027: F₂₀₂₆ = $279.41, F₂₀₂₇ = $285.28 (= E), F₂₀₂₈ = $291.27. Ratio F₂₀₂₈/F₂₀₂₇ = 1.02100. ✓
- **CenHud**: Northwest 115/69, in-service 2035: annual cost = $87.17/kW-yr in 2036, 2037, 2038, … 2045 — identical every year. ✓
- **NYSEG**: Auburn, Center Point dist sub: cumulative 2028 = $221.05, 2029 = $225.47. Ratio = 1.0200. ✓
- **ConEd**: Brooklyn transmission: raw $/kW = $583.81, composite = 0.135274, escalation₂₀₂₆ = 1.024. Product = $80.87 = actual Schedule 4 value. ✓

**Step 6–7: Year-by-year and system-wide aggregation**

All utilities use the same logic: in a given year, only projects in service contribute. The system-wide value is a load-weighted average across locations.

| Utility   | Aggregation unit               | Weighting                                                              |
| --------- | ------------------------------ | ---------------------------------------------------------------------- |
| NiMo      | Per-project                    | Sum(F_year × capacity_MW) for active projects ÷ system peak (6,616 MW) |
| CenHud    | Per-project                    | cost_per_kW × peak_share → sum                                         |
| NYSEG/RGE | Per-division (13 NYSEG, 4 RGE) | Division MC × (division peak load share of system) → sum               |
| ConEd/O&R | Per-area-substation            | Area station MC × (station coincident load / system load) → sum        |

For NiMo and ConEd/O&R, these are mathematically equivalent: total annual cost ÷ system peak = capacity-weighted average. CenHud's formula uses peak-share weighting instead, which produces different values because `peak_share(p) ≠ capacity(p) / system_peak` — see `ny_dist_mc_bat_methodology.md` §4.2 for the full comparison.

**System peak denominator: what each utility divides by**

A critical and often-overlooked detail: the system peak MW used as the denominator in dilution is **not** the same across utilities, and **none** of them use a year-varying peak that changes each year of the study. Every utility uses a **fixed** peak value across all study years — but they differ in which year's peak they use.

| Utility    | System peak (MW) | Basis                                   | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------- | ---------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **NiMo**   | 6,616            | 2024 actual system peak                 | NiMo Peak Load Forecast (March 2025). Workbook does not compute diluted values.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| **CenHud** | 1,103            | **2024 actual** coincident peak         | MCOS report (p. 3) explicitly states "Central Hudson's actual system peak in 2024 was 1,103 MW." Workbook Sheet 11 uses fixed "Share of Central Hudson Coincident Peak Load" per project; shares do not vary by year. Report says system-wide values are "weighed according to their share of Central Hudson's load." Note: CenHud's 2030 forecast is 1,087 MW (declining), so using a future peak would make diluted values slightly _higher_, opposite of the NYSEG/RGE effect.                                                                                                                                                         |
| **NYSEG**  | 2,036            | **2035 forecast** (end of study period) | Workbook Sheet 8 row 16 explicitly labeled "Peak Load 2035 (total division)." Division shares computed from these 2035 forecasts. Fixed across all years. CRA report footnote [^3]: "The Companies relied on NYISO's 'Gold Book' peak load projections for each respective Company's load zone, adjusting as needed."                                                                                                                                                                                                                                                                                                                     |
| **RGE**    | 1,429            | **2035 forecast** (end of study period) | Same methodology as NYSEG. Sheet 8 row 16: "Peak Load 2035 (total division)" = 1,428.5 MW. Same CRA footnote applies.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| **ConEd**  | ~12,000 (fixed)  | Fixed peak, year not stated in report   | MCOS report says "system average marginal costs per kW of system peak" (Schedule 1 header) and describes the Company's peak demand forecasting methodology in detail, but does not state which year's peak is the denominator. Workbook has year-varying coincident load forecasts (Sheet 13: 11,998 MW in 2025 → 13,848 MW in 2035), but these are used for **cross-region weighting**, not as the dilution denominator. Proof: after all transmission projects are fully built (2029+), system MC grows at exactly the 2.1% escalation rate — if the denominator were growing with load (~1.5%/yr), MC growth would be ~0.6%, not 2.1%. |
| **O&R**    | ~2,200 (fixed)   | Fixed peak, year not stated in report   | Same structure and language as ConEd ("System-Weighted T&D Marginal Costs per kW of System Peak"). Year-varying coincident loads (2,157 MW in 2024 → 2,921 MW in 2034) exist in Sheet 13 but are used for cross-region weighting. Same proof: post-build-out MC growth exactly matches the 2.1% escalation rate, confirming a fixed denominator.                                                                                                                                                                                                                                                                                          |

**Cross-referencing with NYISO Gold Book (2025).** NYISO Gold Book Table I-3a and I-4a provide zone-level peak demand forecasts, but zones do not map 1:1 to utility service territories (each TO's distribution peak is a subset of its zone's peak). For example, NYSEG's workbook peak of 2,036 MW is well below Zone C's Gold Book 2035 forecast of 3,615 MW because NYSEG's distribution system serves only part of the zone. Similarly, CenHud's 1,103 MW is about half of Zone G's 2,081 MW (Zone G also includes O&R territory). The CRA report for NYSEG/RGE explicitly cites the Gold Book as the source for their load zone projections, which they then adjust to their distribution-level service territory. ConEd and O&R produce their own independent peak demand forecasts (described in their MCOS reports) rather than directly citing the Gold Book.

The key divergence is NYSEG/RGE vs. everyone else. NYSEG/RGE use the **end-of-horizon forecast** (2035), which is larger than the current peak. This makes their diluted $/kW-yr values systematically smaller than they would be under a current-year peak. For example, if NYSEG's 2024 actual peak were ~1,850 MW, using the 2035 forecast of 2,036 MW deflates every diluted value by ~10%.

Verification of the fixed-denominator finding for ConEd (transmission MC, all projects built out by 2028):

| Year transition | MC growth | Expected if denominator fixed (= escalation) | Expected if denominator = forecast (~1.5%/yr load growth) |
| --------------- | --------- | -------------------------------------------- | --------------------------------------------------------- |
| 2029→2030       | 2.10%     | 2.10% ✓                                      | ~0.6% ✗                                                   |
| 2030→2031       | 2.11%     | 2.10% ✓                                      | ~0.6% ✗                                                   |
| 2031→2032       | 2.05%     | 2.10% ≈✓                                     | ~0.6% ✗                                                   |
| 2032→2033       | 2.10%     | 2.10% ✓                                      | ~0.6% ✗                                                   |
| 2033→2034       | 2.10%     | 2.10% ✓                                      | ~0.6% ✗                                                   |

The same pattern holds for O&R: once projects are built out, MC growth = escalation rate exactly.

**Why this matters.** The choice of peak denominator interacts with electrification forecasts. If a utility expects significant load growth from heat pumps and EVs, using a future (larger) peak as the denominator produces lower $/kW-yr values today. This assumes future load growth will materialize to "fill" the infrastructure investment. If that growth doesn't materialize, the diluted MC understates the per-kW cost that actual customers will bear. Conversely, using the current peak is conservative — it produces higher $/kW-yr values that would decrease if load growth occurs as forecasted.

For cross-utility consistency, the denominator choice should either be normalized to the same peak year or documented alongside each diluted value so consumers of the numbers know what they're comparing.

**Step 8: Levelization**

| Utility   | Discount rate                                 | Method                                                                       |
| --------- | --------------------------------------------- | ---------------------------------------------------------------------------- |
| NiMo      | Not explicit in workbook                      | Implicit in how F₂₆ column values are discounted relative to in-service year |
| CenHud    | 6.76% (from PV multiplier column in workbook) | NPV of flat nominal annual costs over 10 years (or 20 years)                 |
| NYSEG/RGE | 6.975% WACC                                   | NPV of inflation-escalated cumulative costs over 10 years                    |
| ConEd/O&R | Not shown separately                          | Cost of capital embedded in composite rate; no separate PV column            |

### Practical implications

1. **CenHud's flat-nominal approach understates later-year costs relative to other utilities.** A project entering service in 2033 contributes the same $/kW-yr in 2033, 2034, and 2035 in CenHud's framework, but would contribute escalating values in every other utility. Over a 10-year horizon, the ~2% annual difference in escalation is modest (~20% cumulative), but it means CenHud's levelized values are systematically lower than they would be under the other utilities' methodology.

2. **ConEd/O&R's composite-rate approach makes decomposition hard.** You cannot extract the ECCR step in isolation because O&M, plant loading, insurance, and working capital are pre-folded. If you need to compare "pure" annualization across utilities, you must back-calculate from ConEd's composite rate components (Schedule 11 in the workbook).

3. **NiMo's bare-ECCR approach makes it the simplest to replicate.** Capital/MW × ECCR = E column. Then inflate by 2.1%/yr. No reserve margin, no plant loading, no working capital. But this also means NiMo's ECCR rates implicitly assume these overheads are zero or are captured elsewhere in the revenue requirement — they aren't explicitly modeled.

4. **NYSEG/RGE aggregate by division before weighting to system.** This means the system-wide MC is sensitive to which divisions have investment. A single large project in a small division (e.g., Hornell, 5.4% of system peak) gets the full division weight. Other utilities weight at the project or area-station level, which can produce different results from the same underlying project costs.

5. **NYSEG/RGE's use of the 2035 forecasted peak as the dilution denominator systematically lowers their $/kW-yr values** relative to what other utilities would produce from the same project costs. The 2035 forecast is the largest peak in the study horizon. If load growth doesn't materialize (e.g., if energy efficiency offsets electrification), the diluted MC will have understated the per-kW cost. ConEd/O&R and NiMo use a fixed peak closer to the current actual, which is more conservative. See "System peak denominator" in Step 6–7 above for the full comparison and verification.

---

## 5. System-wide marginal cost tables

### 5A. Undiluted tables (areas with projects only)

These numbers represent the average MC across locations that have planned capital projects. Zero-MC areas are excluded. For use as a system-wide rate, these **overstate** the average cost of serving one incremental kW anywhere on the system.

#### ConEd — Schedule 1 (PDF p. 9)

System-weighted T&D marginal costs per kW of system peak. Excludes zero-MC area substations.

| Year | Transmission | Area Station & Sub-TX | Primary Feeder | Transformer | Secondary Cable |  **Total** |
| ---- | -----------: | --------------------: | -------------: | ----------: | --------------: | ---------: |
| 2025 |        44.94 |                 21.22 |          19.26 |       10.21 |           28.83 | **124.47** |
| 2026 |        73.24 |                 71.31 |          19.71 |       10.50 |           29.49 | **204.26** |
| 2027 |        95.77 |                155.35 |          20.59 |       10.98 |           30.85 | **313.54** |
| 2028 |       117.25 |                203.33 |          21.89 |       11.72 |           32.86 | **387.05** |
| 2029 |       119.72 |                235.74 |          23.85 |       12.78 |           35.84 | **427.93** |
| 2030 |       122.23 |                269.37 |          26.58 |       14.22 |           39.85 | **472.25** |
| 2031 |       124.81 |                296.32 |          30.22 |       16.15 |           45.48 | **512.96** |
| 2032 |       127.36 |                319.29 |          35.01 |       18.75 |           52.94 | **553.35** |
| 2033 |       130.04 |                333.99 |          41.47 |       22.22 |           62.74 | **590.46** |
| 2034 |       132.78 |                344.74 |          50.17 |       26.90 |           75.89 | **630.48** |

#### O&R — Schedule 1 (PDF p. 10)

Same structure as ConEd. Excludes zero-MC area substations.

| Year | Transmission | Area Station & Sub-TX | Primary Feeder | Secondary Dist. |  **Total** |
| ---- | -----------: | --------------------: | -------------: | --------------: | ---------: |
| 2025 |        10.54 |                 14.98 |           8.47 |            1.72 |  **35.72** |
| 2026 |        13.93 |                 18.97 |           7.91 |            1.77 |  **42.58** |
| 2027 |        14.45 |                 24.79 |          16.23 |            1.85 |  **57.32** |
| 2028 |        15.14 |                 38.77 |          26.61 |            1.97 |  **82.49** |
| 2029 |        16.89 |                 45.59 |          33.86 |            2.15 |  **98.50** |
| 2030 |        18.71 |                 49.00 |          42.35 |            2.39 | **112.46** |
| 2031 |        23.25 |                 55.07 |          43.17 |            2.72 | **124.21** |
| 2032 |        27.97 |                 61.38 |          43.99 |            3.15 | **136.50** |
| 2033 |        28.56 |                 65.30 |          44.84 |            3.73 | **142.44** |
| 2034 |        29.16 |                 66.67 |          45.71 |            4.51 | **146.05** |

#### CenHud — Table 1 (PDF p. 14)

"Marginal Costs for Areas with Projects." Only locations with identified projects.

| Year                | Local Transmission | Substation | Feeder Circuit |
| ------------------- | -----------------: | ---------: | -------------: |
| 2026                |              $0.00 |      $0.00 |         $12.37 |
| 2027                |              $0.00 |      $0.52 |         $12.63 |
| 2028                |              $0.00 |     $12.65 |         $12.63 |
| 2029                |              $0.00 |     $12.65 |         $12.63 |
| 2030                |              $0.00 |     $42.84 |         $12.63 |
| 2031                |              $0.00 |     $99.50 |         $12.63 |
| 2032                |              $0.00 |     $99.50 |         $12.63 |
| 2033                |             $27.92 |    $127.47 |         $12.63 |
| 2034                |             $27.92 |    $127.47 |         $12.63 |
| 2035                |             $27.92 |    $127.47 |         $12.63 |
| **10-yr levelized** |          **$6.56** | **$55.51** |     **$12.60** |

#### NiMo — Exhibit 1, p. 28, line 251

Single system-wide average: **$71,524/MW-yr** ($71.52/kW-yr). Includes only substations with capital projects. Component breakdown (annual ECCR-weighted $/MW): T-Station $16k, T-Line $38k, D-Station $13k, D-Line $15k. Total $83k/MW at in-service year; $71.5k/MW weighted-average discounted to FY2026.

No year-by-year system-wide table is provided in the extractable narrative. The year-by-year values are in Exhibit 1, columns F26–F36, for each of 153 individual assets (PDF pp. 1–28 of Exhibit 1).

#### NYSEG — Tables 1–2 (PDF pp. 16–17)

Table 1: 10-year levelized by division (undiluted). Table 2: year-by-year system-wide averaged across divisions (undiluted — still only areas with projects, weighted by division peak load share).

**NYSEG Table 1 — Divisional 10-year levelized ($/kW-yr):**

| Division      |  Upstream | Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ------------- | --------: | --------: | -------------: | ---------------: | -----------------: |
| Auburn        |     14.83 |     69.34 |          29.57 |           117.15 |             120.15 |
| Binghamton    |     24.98 |     65.58 |          22.33 |           116.54 |             119.53 |
| Brewster      |     27.78 |     11.78 |          10.68 |            52.20 |              53.54 |
| Elmira        |     38.38 |      0.00 |           4.06 |            44.44 |              45.58 |
| Geneva        |     19.76 |     29.33 |          22.77 |            74.20 |              76.10 |
| Hornell       |     61.22 |     40.90 |           7.02 |           113.53 |             116.44 |
| Ithaca        |     53.69 |     24.28 |          10.72 |            92.30 |              94.66 |
| Lancaster     |     31.44 |     24.05 |          11.02 |            69.02 |              70.79 |
| Liberty       |     39.88 |     26.74 |           6.73 |            76.26 |              78.21 |
| Lockport      |      0.00 |      0.00 |          12.74 |            13.02 |              13.35 |
| Mechanicville |     38.02 |      8.93 |           3.36 |            52.53 |              53.88 |
| Oneonta       |     10.68 |      7.81 |           7.86 |            27.29 |              27.99 |
| Plattsburgh   |     41.84 |     22.76 |           5.29 |            72.76 |              74.63 |
| **System**    | **30.22** | **24.68** |      **11.46** |        **68.83** |          **70.60** |

**RG&E Table 5 — Divisional 10-year levelized ($/kW-yr):**

| Division    |  Upstream | Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ----------- | --------: | --------: | -------------: | ---------------: | -----------------: |
| Canandaigua |      7.01 |     13.14 |           2.74 |            23.74 |              24.89 |
| Central     |     47.02 |     33.25 |          20.06 |           104.40 |             109.44 |
| Fillmore    |      0.00 |      5.37 |           0.00 |             5.54 |               5.81 |
| Sodus       |      0.00 |      4.02 |           0.00 |             4.15 |               4.35 |
| **System**  | **41.78** | **30.27** |      **17.81** |        **93.51** |          **98.03** |

#### PSEG-LI — Exhibit 1 (PDF p. 6)

Single system-wide number: **$146.90/kW-yr** (undiluted). Uniform across all rate classes. Derived from capital cost per kW × combined ECCR+O&M rate: TX/Sub-TX $563.17/kW × 8.2% = $46.18/kW-yr; Primary/Secondary Distribution $721.12/kW × 13.9% = $100.24/kW-yr. Costs escalated to 2025 using Handy-Whitman Index. Based on 30 discrete substation and line projects totaling $600M ($690M with risk & contingency) over FY2025–FY2032. No year-by-year breakdown; single system-wide result only.

The $146.90 is undiluted: the denominator is the 1,210.1 MVA of capacity added by the 30 projects, not LIPA's total system capacity. **Estimated diluted value:** LIPA's 2024 actual non-coincident peak is 4,935 MW (2025 NYISO Gold Book, Table I-4a, Zone K). Total annual marginal cost = $146.90/kW × 1,210.1 MW = ~$177.8M/yr. Diluted system-wide: $177.8M / 4,935 MW ≈ **$36/kW-yr**. Equivalently: $146.90 × (1,210 / 4,935) = $36/kW-yr. This implies ~75% of LIPA's system has no planned investment in the 8-year window — a dilution ratio of ~4.1×, comparable to NYSEG (3.0×) and between CenHud (5.5×) and RG&E (2.5×).

### 5B. Diluted tables (system-wide including zero-MC areas)

These numbers reflect the true system-wide average: areas with no planned investment are weighted as zero. For use as a uniform system-wide rate, these are more appropriate but also lower.

CenHud and NYSEG/RG&E provide diluted tables directly. PSEG-LI and NiMo diluted values can be estimated from project data and system peak (see below).

#### CenHud — Table 2 (PDF p. 14)

"System-wide Marginal Costs" — includes areas with no projects (weighted zero).

| Year                | Local Transmission | Substation | Feeder Circuit |  **Total** |
| ------------------- | -----------------: | ---------: | -------------: | ---------: |
| 2026                |              $0.00 |      $0.00 |          $3.03 |  **$3.03** |
| 2027                |              $0.00 |      $0.07 |          $3.10 |  **$3.17** |
| 2028                |              $0.00 |      $1.80 |          $3.10 |  **$4.89** |
| 2029                |              $0.00 |      $1.80 |          $3.10 |  **$4.89** |
| 2030                |              $0.00 |      $6.08 |          $3.10 |  **$9.18** |
| 2031                |              $0.00 |     $14.13 |          $3.10 | **$17.23** |
| 2032                |              $0.00 |     $14.13 |          $3.10 | **$17.23** |
| 2033                |             $11.62 |     $18.10 |          $3.10 | **$32.81** |
| 2034                |             $11.62 |     $18.10 |          $3.10 | **$32.81** |
| 2035                |             $11.62 |     $18.10 |          $3.10 | **$32.81** |
| **10-yr levelized** |          **$2.73** |  **$7.88** |      **$3.09** | **$13.70** |

CenHud's dilution is significant: undiluted total levelized ~$75/kW-yr vs diluted **$13.70/kW-yr** (5.5× ratio). Many of CenHud's 66 substations and 10 TX areas have declining or stable loads with ample headroom.

#### NYSEG — Table 3 (PDF p. 18)

"System-Wide Marginal Costs Adjusted for Areas with No Anticipated Capacity Investment Needs." ~77% of upstream and ~65% of dist substations/feeders have no investment.

| Year          |  Upstream | Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ------------- | --------: | --------: | -------------: | ---------------: | -----------------: |
| 2026          |     $0.00 |     $1.37 |          $0.00 |            $1.41 |          **$1.45** |
| 2027          |     $0.10 |     $4.37 |          $0.00 |            $4.60 |          **$4.72** |
| 2028          |     $1.02 |     $5.47 |          $2.04 |            $8.78 |          **$9.01** |
| 2029          |     $4.30 |     $1.64 |          $2.83 |            $9.10 |          **$9.33** |
| 2030          |    $14.85 |     $8.51 |          $5.28 |           $29.74 |         **$30.50** |
| 2031          |    $10.14 |    $12.34 |          $1.66 |           $25.04 |         **$25.68** |
| 2032          |    $16.58 |    $25.66 |          $4.16 |           $48.07 |         **$49.30** |
| 2033          |    $11.66 |    $23.20 |         $11.64 |           $48.01 |         **$49.24** |
| 2034          |    $19.95 |    $18.34 |          $9.24 |           $49.27 |         **$50.53** |
| 2035          |    $19.43 |     $8.20 |         $10.76 |           $39.83 |         **$40.85** |
| **Levelized** | **$8.46** | **$9.79** |      **$4.07** |       **$23.11** |         **$23.71** |

Dilution ratio: undiluted $70.60 → diluted $23.71 (3.0× ratio).

#### RG&E — Table 7 (PDF p. 22)

Same approach. ~54% of dist substations and ~66% of feeders have no investment.

| Year          |   Upstream |  Dist. Sub | Primary Feeder | Total at Primary | Total at Secondary |
| ------------- | ---------: | ---------: | -------------: | ---------------: | -----------------: |
| 2026          |      $0.00 |      $0.00 |          $0.00 |            $0.00 |          **$0.00** |
| 2027          |     $19.92 |     $14.28 |          $0.00 |           $35.75 |         **$37.47** |
| 2028          |     $16.35 |     $16.41 |          $0.00 |           $34.18 |         **$35.83** |
| 2029          |      $0.00 |     $13.93 |         $15.81 |           $30.55 |         **$32.02** |
| 2030          |     $17.33 |     $11.13 |          $7.23 |           $37.16 |         **$38.96** |
| 2031          |     $12.08 |     $15.60 |          $5.72 |           $34.68 |         **$36.35** |
| 2032          |     $19.71 |     $18.47 |         $10.61 |           $50.70 |         **$53.15** |
| 2033          |     $25.98 |     $27.13 |          $9.86 |           $65.48 |         **$68.64** |
| 2034          |     $23.61 |     $26.74 |         $14.06 |           $66.87 |         **$70.10** |
| 2035          |     $16.02 |     $13.88 |         $13.19 |           $44.70 |         **$46.86** |
| **Levelized** | **$14.17** | **$14.77** |      **$6.80** |       **$37.14** |         **$38.94** |

Dilution ratio: undiluted $98.03 → diluted $38.94 (2.5× ratio).

#### PSEG-LI — Estimated diluted (computed from Exhibit 2 project data)

PSEG-LI does not publish a diluted table. However, Exhibit 2 provides the total capacity added by the 30 projects (1,210.1 MVA) and the undiluted MC ($146.90/kW-yr). LIPA's 2024 actual non-coincident peak is 4,935 MW (2025 NYISO Gold Book, Table I-4a, Zone K). Diluted system-wide estimate:

> $146.90 × (1,210 / 4,935) = **~$36/kW-yr**

| Component    | Undiluted ($/kW-yr) | Project MVA | System MW | Diluted ($/kW-yr) |
| ------------ | ------------------: | ----------: | --------: | ----------------: |
| Transmission |              $46.18 |     1,027.1 |     4,935 |            ~$9.62 |
| Distribution |             $100.24 |       183.0 |     4,935 |            ~$3.72 |
| **Total**    |         **$146.90** | **1,210.1** | **4,935** |          **~$36** |

Dilution ratio: undiluted $146.90 → diluted ~$36 (4.1× ratio). ~75% of LIPA's system has no planned investment in the 8-year window, comparable to NYSEG's ~77% upstream figure.

**Note:** The per-component diluted split above uses the simplistic method of scaling each component by the ratio of its project capacity to system peak. A more precise dilution would weight by substation-level MW, which requires the workpapers.

#### NiMo — Computed diluted (from Exhibit 1 workbook with project-level classification)

NiMo does not publish a diluted table. One can be computed from the MCOS Exhibit 1 workbook, which has year-by-year annualized costs (FY2026–FY2036) for each of 238 projects. System peak: 6,616 MW (2024 actual, NiMo Peak Load Forecast March 2025).

**Why NiMo's undiluted number is misleading.** The headline undiluted MC is $71.52/kW-yr. But the "kW" in the denominator is kW of _capacity added_ by projects (11,533 MW total), not kW of system load. If you naively dilute — divide the total annual bill ($825M) by system peak (6,616 MW) — you get **$125/kW-yr**, _higher_ than the undiluted value. That's because the project portfolio adds 1.7× the system peak in capacity, driven by massive bulk transmission projects that add NYISO-scale transfer capability, not local load-serving capacity.

**Three-bucket classification.** NiMo's workbook labels cost components as T-Station / T-Line / D-Station / D-Line but doesn't distinguish bulk TX from sub-TX. All 238 projects can be classified by cross-referencing with the NYISO Gold Book (Table VII) for voltage levels:

- **Bulk TX** (≥230kV): 2 projects, 2,100 MW, $1.07B — Smart Path Connect (230/345kV) and Niagara-Dysinger (345kV). FERC-jurisdictional; not relevant for distribution MC or DRV.
- **Sub-TX** (69–115kV): 47 projects, 6,889 MW, $6.71B — all other "Transm Net" entries. These are sub-transmission lines and stations stepping bulk voltage down toward distribution, at 69kV or 115kV. State-jurisdictional and DRV-relevant.
- **Distribution** (≤13.2kV): 189 projects across 152 named substations, 2,543 MW, $2.93B — substation transformers, feeders, and related equipment. Clearly DRV-relevant.

Full classifications with evidence are in `utils/pre/dist_mc/nimo/nimo_project_classifications.csv`.

**Diluted MC by bucket (levelized across full study period):**

| Bucket                                   | Projects | Capacity (MW) | Capital ($B) | Annual cost ($M, FY26) | Diluted ($/kW-yr) |
| ---------------------------------------- | -------: | ------------: | -----------: | ---------------------: | ----------------: |
| All projects                             |      238 |        11,533 |       $10.70 |                   $825 |           $124.68 |
| Bulk TX (≥230kV)                         |        2 |         2,100 |        $1.07 |                    $87 |            $13.08 |
| Sub-TX (69–115kV)                        |       47 |         6,889 |        $6.71 |                   $481 |            $72.72 |
| Distribution (≤13.2kV)                   |      189 |         2,543 |        $2.93 |                   $257 |            $38.88 |
| **Sub-TX + Distribution (DRV-relevant)** |  **236** |     **9,433** |    **$9.63** |               **$738** |       **$111.59** |

The DRV-relevant diluted MC is **$111.59/kW-yr** — much higher than other utilities because NiMo's sub-transmission investment program is enormous (47 projects, $6.7B). The distribution-only component ($38.88/kW-yr) is in line with other utilities.

**Year-by-year diluted MC.** Because projects enter service at different times, the annual infrastructure bill grows over the study period. The workbook's F-columns provide each project's annualized cost in each fiscal year's dollars (inflating at 2.1%/yr GDP deflator). For each year, costs are summed only from projects in service by that year and divided by system peak:

|                                      FY | New MW | Cumulative MW | Annual bill ($M) | Diluted ($/kW-yr) |
| --------------------------------------: | -----: | ------------: | ---------------: | ----------------: |
| _Sub-TX + Distribution (DRV-relevant):_ |        |               |                  |                   |
|                                    2026 |    142 |           142 |              $24 |             $3.60 |
|                                    2027 |    178 |           320 |              $61 |             $9.19 |
|                                    2028 |    370 |           689 |              $72 |            $10.94 |
|                                    2029 |    242 |           931 |              $91 |            $13.75 |
|                                    2030 |  1,258 |         2,189 |             $139 |            $21.03 |
|                                    2031 |    891 |         3,079 |             $204 |            $30.79 |
|                                    2032 |    391 |         3,470 |             $236 |            $35.60 |
|                                    2033 |    195 |         3,665 |             $268 |            $40.56 |
|                                    2034 |    265 |         3,930 |             $312 |            $47.09 |
|                                    2035 |    816 |         4,746 |             $420 |            $63.44 |
|                                    2036 |  4,687 |         9,433 |             $909 |           $137.37 |

The diluted MC starts at $3.60/kW-yr in FY2026 (only 142 MW in service) and grows to $137/kW-yr by FY2036. FY2036 is a spike because NiMo backloads ~4,700 MW of sub-TX and distribution projects into the final year. The present-value-equivalent $111.59/kW-yr sits between the early and late years, reflecting the time-value-weighted average.

**Substation coverage:** 152 named substations have MCOS projects. NiMo's System Data Portal lists 569 distribution substations → ~73% have no planned investment in the 11-year window. DPS Staff identified 151 non-zero-cost substations in their hybrid DRV calculation and arrived at **$146.59/kW-yr** (Staff Proposal, Dec 2025) — higher than NiMo's own $71.52 because Staff re-levelized costs using WACC and allocated transmission capacity to substations differently.

**Sources:** NiMo 2025 MCOS workbook; project classifications (`utils/pre/dist_mc/nimo/nimo_project_classifications.csv`); NiMo Peak Load Forecast (March 2025, `context/papers/mcos/nimo_peak_load_forecast_2025.pdf`); NYISO Gold Book 2025 (`context/papers/nyiso_gold_book_2025.md`, Table VII); DPS Staff Proposal on DRV/LSRV (`context/papers/mcos/dps_staff_proposal_drv_lsrv_20251211.pdf`).

---

## 6. Component taxonomy: first principles and cross-utility mapping

### 6A. The real-world physical components, bottom to top

The delivery system has a clear physical hierarchy. From the customer meter upward:

| Level                           | Physical components                                                                                               | Function                                                                                    | Typical voltages                                                                     |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **A. Secondary**                | Secondary lines (overhead/underground conductors from transformer to meter); service drops                        | Final delivery to customer premises                                                         | 120/240V (residential), 120/208V, 277/480V (commercial)                              |
| **B. Distribution transformer** | Pole-top or pad-mount transformers; network transformers in urban areas                                           | Step down from primary voltage to secondary/service voltage                                 | Primary side: 4–35 kV; secondary side: 120–480V                                      |
| **C. Primary feeder**           | Trunk-line primary conductors (overhead or underground cable) from substation to transformers and load tap points | Distribute power from substation to transformers across the service area                    | 4 kV, 12.47 kV, 13.2 kV, 27 kV, 33 kV (varies by utility)                            |
| **D. Distribution substation**  | Substation transformers (step down from sub-TX/TX voltage to primary); buses, breakers, switches                  | Convert sub-transmission voltage to primary distribution voltage                            | High side: 34.5–138 kV; low side: 4–35 kV                                            |
| **E. Sub-transmission**         | Sub-transmission lines and cables (overhead/underground); sub-transmission switches, regulators                   | Move power from bulk TX delivery points to distribution substations across the service area | 34.5 kV, 46 kV, 69 kV, 115 kV (varies by utility; can overlap with bulk TX voltages) |
| **F. Bulk system substation**   | High-voltage substations (step down from EHV to sub-TX/area voltage); switchyards; NYISO interconnection points   | Interface between bulk transmission grid and utility sub-transmission                       | High side: 115–345 kV; low side: 34.5–138 kV                                         |
| **G. Bulk transmission lines**  | High-voltage and extra-high-voltage lines (overhead towers, underground cables)                                   | Move power long distances between generation and load centers; NYISO-operated               | 115 kV, 230 kV, 345 kV, 500 kV+                                                      |

The boundary between E (sub-transmission) and F–G (bulk system) is fuzzy and utility-specific. In NYISO, the jurisdictional boundary between FERC-regulated bulk transmission and state-regulated local facilities varies by TO and is defined in each TO's OATT. Generally, facilities at 115 kV+ that are part of the NYISO grid are FERC-jurisdictional. But some 115 kV facilities are "local transmission" in the sense that they serve only a small area (as with CenHud's 115/69 kV areas).

### 6B. Cross-utility mapping table

The table below maps each utility's cost center labels to the physical component levels defined above. Where a cost center spans multiple physical levels, all are listed. "?" means the boundary is ambiguous from the filing.

| Physical level                           | ConEd                                                 | O&R                                                   | CenHud                                                             | NiMo                                         | NYSEG/RG&E                                                             | PSEG-LI                                    |
| ---------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------ | -------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------ |
| **A. Secondary lines**                   | "Secondary Cable"                                     | "Secondary Distribution" (combined with B)            | Not separated (included in "Feeder Circuit" via historical sample) | "D-Line" (combined with C)                   | "Local Distribution Facilities" per customer class — NOT in system MC  | Not separated                              |
| **B. Distribution transformer**          | "Transformer"                                         | "Secondary Distribution" (combined with A)            | Not separated (included in "Feeder Circuit" via historical sample) | "D-Line" (combined with A, C?)               | "Local Distribution Facilities" per customer class — NOT in system MC  | Not separated                              |
| **C. Primary feeder**                    | "Primary Feeder"                                      | "Primary Feeder"                                      | "Feeder Circuit" (includes B?, A?)                                 | "D-Line" (combined with A, B?)               | "Primary Feeder" (12.5 kV, 4.6 kV)                                     | "D-Line" (combined with D?)                |
| **D. Distribution substation**           | "Area Station and Sub-Transmission" (combined with E) | "Area Station and Sub-Transmission" (combined with E) | "Substation"                                                       | "D-Station"                                  | "Dist. Substation" (12.5 kV)                                           | "D-Station"                                |
| **E. Sub-transmission lines/components** | "Area Station and Sub-Transmission" (combined with D) | "Area Station and Sub-Transmission" (combined with D) | "Local Transmission" (69 kV, 115/69 kV)                            | "T-Station" or "T-Line" (combined with F, G) | "Upstream Substation" (115/46/34 kV) + "Upstream Feeder" (115/34.5 kV) | "T-Station" or "T-Line" (combined with F?) |
| **F. Bulk system substation**            | "Transmission System"                                 | "Transmission System"                                 | Not included                                                       | "T-Station" (combined with E)                | **Not included** (explicitly excluded: "NYISO TSCs")                   | "T-Station" (combined with E?)             |
| **G. Bulk transmission lines**           | "Transmission System" (if 345 kV line projects exist) | Not visible in schedules                              | Not included                                                       | "T-Line" (combined with E)                   | **Not included** (explicitly excluded)                                 | "T-Line" (combined with E?)                |

### 6C. Reading the mapping: what's actually comparable?

**Same label, different contents:**

- **"Transmission"** in ConEd/O&R = FERC-jurisdictional TX stations (levels F–G). **"Transmission"** in NiMo = everything ≥ 69 kV (levels E + F + G combined). **"Local Transmission"** in CenHud = local sub-TX (level E only). **"Upstream"** in NYSEG/RG&E = local sub-TX (level E only), explicitly not bulk.
- **"Substation"** in ConEd/O&R ("Area Station and Sub-Transmission") = distribution substations + sub-transmission lines bundled together (levels D + E). In CenHud = distribution substations only (level D). In NYSEG/RG&E = distribution substations only (level D). In NiMo = D-Station (level D only), but sub-TX components are in T-Station/T-Line.

**Same real-world thing, different buckets:**

- **Sub-transmission wires and components (level E):** In ConEd/O&R → "Area Station and Sub-Transmission" (bundled with dist substations). In CenHud → "Local Transmission." In NiMo → "T-Station" / "T-Line" (bundled with bulk TX). In NYSEG/RG&E → "Upstream Substation" + "Upstream Feeder." These all cover level E, but four different labels and three different bundling strategies.
- **Secondary lines and transformers (levels A + B):** ConEd separates them into two cost centers. O&R bundles them into "Secondary Distribution." CenHud apparently rolls them into the "Feeder Circuit" cost center (lower-voltage costs from historical samples). NiMo puts them in "D-Line." NYSEG/RG&E excludes them from the system MC entirely and reports them as per-customer-class "Local Distribution Facilities."

**Which utilities actually include NYISO bulk system costs (levels F–G)?**

| Utility    | Includes levels F–G? | Confidence | Notes                                                                                             |
| ---------- | -------------------- | ---------- | ------------------------------------------------------------------------------------------------- |
| ConEd      | **Yes**              | High       | Separate "Transmission System" cost center. Few projects, but genuine FERC-jurisdictional TX.     |
| O&R        | **Yes**              | High       | Same structure as ConEd. Only 2 substations with TX MC.                                           |
| CenHud     | **No**               | High       | "Local Transmission" is explicitly local (69/115 kV areas).                                       |
| NiMo       | **Yes**              | High       | Massive "Transm Net" projects are NYISO-scale bulk TX. But inseparable from local TX in the data. |
| NYSEG/RG&E | **No**               | Certain    | CRA explicitly states upstream excludes "NYISO Transmission Service Charges."                     |
| PSEG-LI    | **Unclear**          | Low        | Exhibits not extractable. Framed as "avoided distribution costs."                                 |

---

## 7. Transmission: bulk vs local, per utility

| Utility        | Label                     | Voltages covered               | FERC-jurisdictional?                                                         | Includes NYISO bulk TX?                                                                                                                        | Evidence                                                                                                                                                       |
| -------------- | ------------------------- | ------------------------------ | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ConEd**      | "Transmission System"     | 138 kV, 345 kV stations        | Yes — ConEd is a NYISO TO                                                    | **Yes** — but project-based; only 5 substations (Brooklyn, Queens) have non-zero MC                                                            | Schedule 4: Atlantic, Gateway Park, Hillside, Idlewild, Nevins Street. Bronx, Westchester, Manhattan, SI all zero.                                             |
| **O&R**        | "Transmission System"     | 138 kV                         | Yes — O&R is a NYISO TO                                                      | **Yes** — but only 2 substations (West Nyack, Viola Road)                                                                                      | Schedule 4; all other substations "–".                                                                                                                         |
| **CenHud**     | "Local Transmission"      | 69 kV, 115/69 kV               | Ambiguous — 115 kV may be FERC-jurisdictional, 69 kV is state-jurisdictional | **No** — explicitly labeled "local"                                                                                                            | 10 local TX areas named (Northwest 115/69, Northwest 69, RD-RJ Lines, etc.). No reference to FERC or bulk TX.                                                  |
| **NiMo**       | T-Station + T-Line        | ≥ 69 kV (explicitly defined)   | Yes — NiMo is a major NYISO TO                                               | **Yes** — 2 bulk TX projects (Smart Path Connect 230/345kV, Niagara-Dysinger 345kV) at $1.07B; remaining 47 "Transm Net" are sub-TX (69–115kV) | Classified via NYISO Gold Book Table VII cross-reference. See `utils/pre/dist_mc/nimo/nimo_project_classifications.csv`.                                       |
| **NYSEG/RG&E** | "Upstream" (sub + feeder) | 115 kV, 46 kV, 34.5 kV         | Mixed — 115 kV may be FERC-jurisdictional; lower is state                    | **No** — explicitly excluded                                                                                                                   | CRA footnote 4: "to differentiate them from the more regional transmission grid costs that are recoverable through NYISO Transmission Service Charges (TSCs)." |
| **PSEG-LI**    | T-Station + T-Line        | Not stated in extractable text | LIPA has unique FERC relationship                                            | **Unclear** — exhibits not extractable                                                                                                         | Study framed as "location-specific avoided distribution costs." Modest $600M portfolio.                                                                        |

---

## 8. Bulk TX marginal cost gap

The research note `ny_bulk_transmission_marginal_cost.md` was originally written under the assumption that all MCOS studies explicitly exclude bulk NYISO transmission. The component-level scrub in §6–7 reveals a more nuanced picture.

### What the scrub found

1. **ConEd's MCOS includes bulk TX MC.** ConEd's "Transmission System" cost center covers FERC-jurisdictional 138/345 kV facilities. Schedule 1 shows system-weighted transmission MC growing from $45/kW (2025) to $133/kW (2034). These are project-based (only 5 substations in Brooklyn and Queens have planned TX projects), but the system average is non-trivial — especially compared to the OATT proxy of ~$120/kW-yr.

2. **O&R's MCOS includes bulk TX MC.** Same framework. Smaller: $11–29/kW over 2025–2034. Only two substations have TX projects.

3. **NiMo's MCOS includes bulk TX MC — and it dominates.** T-Station + T-Line accounts for ~65% of NiMo's total $71.5/kW-yr system MC. The "Transm Net" entries include projects at the multi-GW / multi-billion-dollar scale. This is unambiguously NYISO bulk transmission. NiMo's total MC is more "transmission MC with some distribution" than "distribution MC with some transmission."

4. **NYSEG/RG&E confirmed: no bulk TX.** The CRA study explicitly excludes NYISO TSCs. The "upstream" category is local sub-TX only.

5. **CenHud confirmed: no bulk TX.** "Local Transmission" = 69/115 kV area feeders, explicitly local.

6. **PSEG-LI: still unclear.** Exhibits not extractable.

### Gap status by utility

| Utility | Bulk TX MC in MCOS?                                             | Gap status                                                            |
| ------- | --------------------------------------------------------------- | --------------------------------------------------------------------- |
| ConEd   | Yes — $45–133/kW-yr undiluted (Schedule 1 "Transmission")       | **No gap** for BAT; this is a usable (if undiluted) bulk TX MC signal |
| O&R     | Yes — $11–29/kW-yr undiluted (Schedule 1 "Transmission")        | **No gap**                                                            |
| NiMo    | Yes — embedded in $71.5/kW-yr total; ~$54/MW T-Station + T-Line | **No gap**, but bulk and local TX are inseparable in the data         |
| NYSEG   | No                                                              | **Gap remains** — need OATT proxy or CLCPA project costs              |
| RG&E    | No                                                              | **Gap remains**                                                       |
| CenHud  | No                                                              | **Gap remains**                                                       |
| PSEG-LI | Unclear                                                         | **Likely gap** — treat as gap until exhibits are extracted            |

### Revised recommendation

**For ConEd, O&R, NiMo:** Use the MCOS filing's own transmission component. These are genuine forward-looking, project-based marginal costs — superior to the embedded-cost OATT proxy. For ConEd and O&R, the Schedule 1 "Transmission" column is directly usable. For NiMo, the T-Station + T-Line share of the total MC can be extracted from the Exhibit 1 component breakdown.

The catch: ConEd and O&R's numbers are **undiluted** (only areas with projects). If a diluted system-wide bulk TX MC is needed, apply the same dilution approach as for sub-TX — weight by the fraction of the system requiring TX-level investment. ConEd's Schedule 2 shows TX MC by region, and the system average weights these by region size, but it still excludes zero-TX-MC regions from the averaging. To dilute, you'd need to weight the regional TX MC by the fraction of total system load in TX-project regions (Brooklyn + Queens for ConEd ≈ 35% of system load) and apply zero for the rest.

**For NYSEG, RG&E, CenHud:** The gap is real. Use Option 1 (OATT ATRR proxy) or Option 2 (CLCPA project costs) from `ny_bulk_transmission_marginal_cost.md`. The OATT proxy gives:

| TO     | Approx. $/kW-yr (embedded) |
| ------ | -------------------------- |
| NYSEG  | ~$53                       |
| RG&E   | ~$43                       |
| CenHud | ~$55                       |

These are embedded averages, not LRMC. They likely overstate MC for utilities with surplus TX capacity and understate it where the system is constrained.

**For PSEG-LI:** Treat as a gap until the MCOS Exhibit 1 data can be extracted from the PDF. OATT proxy ~$58/kW-yr.

### What didn't change

The hourly allocation guidance remains valid: bulk TX is driven by zonal/statewide coincident peaks, and PoP allocation should use NYISO zonal load shapes. The FERC Order 1920 compliance process (due April 2026) remains the best long-term source for ISO-endorsed LRMC data.

The sensitivity testing recommendation remains critical: run the BAT with and without bulk TX MC to quantify its impact, regardless of which source is used.

# BGE ResStock load discrepancy analysis

**Use when:** Understanding why ResStock overstates BGE residential electricity consumption by ~17% per customer, interpreting the `resstock_kwh_scale_factor` (0.828), or deciding how to allocate the overstatement between heating and non-heating end uses for rate design.

**Context:** This document captures findings from a deep investigation (September 2026) into the BGE `resstock_kwh_scale_factor` of 0.828 — meaning CAIRO must scale down every building's hourly load curve by ~17% to match BGE's test-year residential kWh. We examined where the overstatement lives using multiple independent methods and data sources.

## The gap we're explaining

All numbers below are for BGE residential (Schedule R + RL), upgrade 00 (baseline), ResStock 2024.2 AMY2018.

| Metric | Test Year (Karas) | EIA-861 (2018) | ResStock `_sb` (grid_cons) | ResStock raw NREL |
|---|---:|---:|---:|---:|
| **Total kWh (B)** | 13.01 | 12.95 | 15.88 | 16.47 |
| **Customer count** | 1,222,270 | 1,164,647 | 1,235,521 | 1,235,521 |
| **Per-customer kWh** | 10,645 | 11,118 | 12,855 | 13,328 |
| **vs. test year** | — | −0.5% total | +22% total, +21% per-cust | +27% total, +25% per-cust |

Key observations:
- EIA-861 (2018) and the Karas test year track within 0.5% on total kWh, confirming the calibration target is sound.
- The `_sb` MF non-HVAC adjustment reduced the gap by ~3.7 pp (from +25% to +21% per-customer).
- The remaining +21% per-customer overstatement is what `resstock_kwh_scale_factor` = 0.828 corrects.

## Evidence collected

### 1. Gas consumption validates well (strongest alignment)

**Finding:** BGE gas consumption in ResStock matches EIA-176 closely on a per-customer basis.

| Metric | EIA-176 (2018) | ResStock NREL | ResStock `_sb` |
|---|---:|---:|---:|
| **Per-customer therms/year** | 659 | 667 | (similar) |
| **Per-customer kWh/year** | 19,310 | 19,545 | (similar) |
| **Alignment** | — | +1.2% | ~+1% |

However, ResStock over-assigns BGE gas customers by ~36% relative to EIA-176 customer counts. The per-customer gas consumption is close, but the total is off due to customer assignment, not consumption modeling.

**Why this matters:** If the gas-side thermal model is correct (per-customer gas consumption matches EIA within ~1%), this suggests ResStock's building thermal envelope and HVAC equipment models are reasonable for gas-heated homes. The gas furnace converts fuel to heat at a known efficiency (~80-95%), so matching gas consumption means the thermal load is approximately right for those homes.

### 2. Electric resistance COP ≈ 1.0 (physics check passes)

**Finding:** For electric resistance homes in ResStock 2024.2, the ratio of electric heating input to thermal output is approximately 1.0.

| Metric | Value |
|---|---|
| Median ratio (elec input / thermal output) | ~1.00 |
| Mean ratio | ~1.00 |

This confirms that the energy conversion model is physically correct for resistance heating. If the thermal load is correct, the electric input must also be correct.

### 3. Thermal load by heating type

**Finding:** Thermal heating load delivered to conditioned space (`out.load.heating.energy_delivered`) shows plausible variation across heating types.

| Heating type | Weighted avg thermal load (kWh/yr) | kWh/sqft |
|---|---:|---:|
| Natural Gas | ~7,600 | ~4.5 |
| Fuel Oil | ~9,200 | ~6.0 |
| Propane | ~7,800 | ~4.8 |
| Elec: Heat Pump | ~5,600 | ~3.4 |
| Elec: Resistance | ~6,900 | ~4.2 |

Heat pump homes show lower thermal load per sqft, which is plausible (newer/better insulated homes are more likely to have HPs). Gas and oil homes show higher loads, consistent with older housing stock.

### 4. Seasonal decomposition (the key finding)

**Method:** Compare ResStock monthly electricity to EIA monthly residential sales for all of MD. In summer (May–Sep), electric space heating is essentially zero for all homes, so summer electricity = pure non-heating. This lets us directly observe non-heating without cross-sectional assumptions.

**Data sources:**
- EIA: Monthly residential retail sales for MD via EIA Open Data API v2 (2018 calendar year)
- ResStock: Full population (9,995 buildings), hourly data aggregated to monthly

#### Monthly comparison (all MD, kWh/customer)

| Month | EIA | ResStock | RS/EIA | RS Heating | RS Non-heat |
|---|---:|---:|---:|---:|---:|
| Jan | 1,390 | 1,859 | 1.34 | 1,102 | 757 |
| Feb | 983 | 1,204 | 1.23 | 547 | 658 |
| Mar | 1,049 | 1,281 | 1.22 | 584 | 697 |
| Apr | 803 | 860 | 1.07 | 225 | 636 |
| May | 801 | 894 | 1.12 | 7 | 886 |
| Jun | 942 | 995 | 1.06 | 3 | 992 |
| Jul | 1,189 | 1,236 | 1.04 | 0 | 1,236 |
| Aug | 1,175 | 1,245 | 1.06 | 0 | 1,245 |
| Sep | 963 | 972 | 1.01 | 4 | 968 |
| Oct | 774 | 843 | 1.09 | 91 | 753 |
| Nov | 900 | 1,061 | 1.18 | 421 | 640 |
| Dec | 1,094 | 1,285 | 1.17 | 572 | 714 |
| **Annual** | **12,064** | **13,735** | **1.14** | **3,556** | **10,179** |

**Note:** This comparison is all-MD EIA vs. all-MD ResStock (not BGE-specific). This is the cleanest apples-to-apples comparison available, since EIA monthly data is at the state level.

#### Seasonal summary

| Season | EIA | ResStock | Overstatement | RS Heating |
|---|---:|---:|---:|---:|
| **Summer (Jun-Aug)** | 3,307 | 3,476 | **+5.1%** | 3 |
| **Winter (Dec-Feb)** | 3,468 | 4,349 | **+25.4%** | 2,221 |
| **Shoulder (Apr,May,Sep,Oct)** | 3,341 | 3,569 | +6.8% | 326 |

#### Overstatement decomposition

In heating-free months (May–Sep), ResStock overshoots EIA by ~5.4%. Applying this as the non-heating overstatement and attributing the rest to heating:

| Component | Annual overstatement (kWh) | Share of gap |
|---|---:|---:|
| Non-heating (~5% overstatement) | +518 | 31% |
| Electric heating (~48% overstatement) | +1,154 | **69%** |

### 5. Full-population consumption table (all MD, upgrade 0, NREL variant)

Weighted average kWh/customer by baseline heating type:

| Heating type | Elec Total | Elec Heat | Elec Non-Heat | Gas Total | Thermal Load |
|---|---:|---:|---:|---:|---:|
| Elec: Heat Pump | ~18,000 | ~6,500 | ~11,500 | ~2,200 | ~5,600 |
| Elec: Resistance | ~22,000 | ~12,000 | ~10,000 | ~1,800 | ~6,900 |
| Natural Gas | ~10,600 | ~300 | ~10,300 | ~19,600 | ~7,600 |
| Fuel Oil | ~11,200 | ~600 | ~10,600 | ~200 | ~9,200 |
| Propane | ~10,700 | ~400 | ~10,300 | ~100 | ~7,800 |

### 6. EIA-861 BGE electric sales (2018 vs 2025)

| Year | Customers | Total sales (GWh) | kWh/customer |
|---|---:|---:|---:|
| 2018 | 1,264,093 | 13,283 | 10,508 |
| 2025 | 1,324,831 | 13,220 | 9,978 |
| Change | +4.8% | −0.5% | **−5.0%** |

Per-customer consumption declined 5% between 2018 and 2025, while customer count grew 5%. Total sales nearly flat.

### 7. Algebraic model (earlier approach — now superseded)

An earlier attempt used a uniform-X model: assume all homes have the same non-heating electricity X, and solve using EIA population average and ResStock's electric heating inputs. This found non-heating was 29% overstated.

**This is now superseded** by the seasonal decomposition (Finding 4), which shows non-heating is only ~5% overstated. The algebraic model was wrong because it assumed electric-heat and non-electric-heat homes have identical non-heating consumption — they don't (electric-heat homes tend to be different housing stock with different appliance profiles).

## Tension between findings

There is a tension between findings that must be acknowledged:

**The gas alignment (Finding 1) suggests the thermal envelope model is correct.** If gas-heated homes consume the right amount of gas, and gas consumption is dominated by space heating, then the building shell and heating load model is approximately right.

**The seasonal decomposition (Finding 4) suggests electric heating is ~48% overstated.** The overstatement is concentrated in winter months, pointing to heating loads.

**These findings are in tension.** If the thermal load is right (gas validates) and the COP/efficiency is right (resistance COP = 1), how can electric heating be overstated?

### Resolution (from Priority 1 & 2 findings)

The RECS 2020 comparison resolves this tension:

1. ~~**The electric heating share is too high.**~~ **Ruled out.** ResStock's electric-heat share (39.2%) is actually slightly _below_ RECS (41.4%). The composition is correct.

2. **Electric-heat homes have different envelopes than gas-heat homes — and ResStock overstates specifically for resistance-heated homes.** This is the dominant explanation. ResStock overstates resistance-home electricity by +58% but HP homes by only +11%. Since both share the same climate and similar building stock, the issue is specific to how ResStock models resistance-heating demand (thermal load, thermostat behavior, or supplemental heating omissions).

3. **Non-heating end uses are NOT uniformly overstated.** Non-electric-heat homes actually consume 5% _less_ than RECS. The ~5% summer overstatement seen in the seasonal decomposition (Finding 4) is likely concentrated in electric-heat homes, not a uniform non-heating bias.

4. **The all-MD comparison is not identical to BGE.** This caveat still applies to the seasonal decomposition, but the RECS comparison is MD-wide and shows the same pattern. The BGE-specific vs. all-MD distinction is secondary.

5. **The gas comparison validates per-customer consumption but not the electric side.** The gas thermal model is correct for gas-heated homes, but the thermal loads for electric resistance homes appear overstated. These are genuinely different housing stock with different thermal behavior.

## What we can conclude with confidence

1. **The calibration target is sound.** EIA-861 (2018) matches the Karas test year within 0.5%.
2. **The MF non-HVAC adjustment helped but didn't fix it.** It reduced the gap by ~3.7 pp.
3. **The majority of the overstatement (~69%) is concentrated in winter/heating months.** This is the strongest empirical finding from the seasonal decomposition.
4. **Gas per-customer consumption validates well (+1.2%).** The thermal model works for gas-heated homes.
5. **The overstatement is structural, not weather-related.** EIA-861 2018 and the test year track closely despite spanning 7 years.
6. **The electric-heat share is NOT inflated.** ResStock (39.2%) matches RECS (41.4%) within sampling error. The overstatement is per-building, not compositional.
7. **Resistance-heated homes are the primary source of overstatement.** ResStock overstates resistance-home electricity by +58% (20,033 vs 12,712 kWh/yr RECS). HP homes are overstated by a more modest +11%.
8. **Non-electric-heat homes validate well.** ResStock (9,238 kWh/yr) is within 5% of RECS (9,717 kWh/yr), and the direction is _under_-statement, not overstatement.
9. **The `resstock_kwh_scale_factor` is a blunt instrument.** It uniformly scales all buildings by 0.828, but the overstatement is concentrated in resistance-heated homes (+58%) while non-electric-heat homes need no correction and HP homes need only a modest one (+11%).

## What remains uncertain

1. **What drives the resistance-home thermal load overstatement.** Candidates: building shell assumptions (too leaky), heated area (too large), thermostat behavior (too high), or omitted supplemental heating (wood stoves, gas space heaters).
2. **Whether the ~5% summer overstatement is uniform or concentrated in electric-heat homes.** The RECS comparison suggests non-electric-heat homes are slightly _under_-stated, so the summer excess may come from electric-heat homes' cooling loads.
3. **Whether a subgroup-specific scale factor would be better than a uniform one.** The evidence strongly suggests it would, but implementing this requires changes to CAIRO's scaling logic.

## Completed additional analyses

### Priority 1 & 2: Electric-heat share validation + non-electric-heat control group

**Source:** RECS 2020 microdata for Maryland (142 sample households with electric heat), cross-tabulated against ResStock BGE data.

#### RECS 2020 EQUIPM codes (verified via FUELHEAT cross-tabulation)

The RECS `EQUIPM` variable identifies primary heating equipment. Codes were verified by cross-tabulating EQUIPM × FUELHEAT nationally (18,496 households). The fuel association is unambiguous — e.g. a heat pump can't run on natural gas as primary heating fuel.

| EQUIPM | Equipment type | Fuels observed | Category |
|---:|---|---|---|
| 2 | Steam/hot-water system (boiler) | Gas, oil, propane, electric, wood | Resistance (if electric) |
| 3 | Central warm-air furnace | Gas, propane, oil, **electricity**, wood | Resistance (if electric) |
| 4 | Central heat pump (ducted) | Electric only | **Heat pump** |
| 5 | Built-in electric units (baseboard/wall) | Electric only | Resistance |
| 7 | Built-in oil or gas room heater | Gas, propane, oil only | N/A (non-electric) |
| 8 | Wood/pellet stove | Wood only | N/A (non-electric) |
| 10 | Portable electric heater | Electric only | Resistance |
| 13 | Ductless heat pump (mini-split) | Electric only | **Heat pump** |

**Critical note on EQUIPM=3 (central furnace):** Per EIA's [terminology](https://www.eia.gov/consumption/residential/terminology.php): _"A type of space-heating equipment where a central combustor or **resistance unit** (generally using natural gas, fuel oil, propane, or electricity) provides warm air through ducts. **Heat pumps are not included in this category.**"_ When FUELHEAT=5 (electricity), EQUIPM=3 represents an electric resistance furnace, not a heat pump.

**Critical note on EQUIPM=7 vs EQUIPM=13:** EQUIPM=7 is **not** ductless heat pump — it only appears with gas/propane/oil fuels (3.49M hh nationally, zero with electricity). The real ductless HP (mini-split) code is EQUIPM=13 (1.06M hh nationally, electric only).

#### Head-to-head comparison: ResStock BGE vs RECS 2020 MD

| Metric | RECS 2020 | ResStock | RS/RECS |
|---|---:|---:|---:|
| **Electric-heat share (% all homes)** | 41.4% | 39.2% | 0.95 |
| HP share (% of elec-heat) | 47.5% | 50.8% | 1.07 |
| Resistance share (% of elec-heat) | 52.5% | 49.2% | 0.94 |
| **HP avg kWh/yr** | 15,596 | 17,352 | **1.11** |
| **Resistance avg kWh/yr** | 12,712 | 20,033 | **1.58** |
| **Non-electric-heat avg kWh/yr** | 9,717 | 9,238 | **0.95** |

RECS sample sizes: 73 HP, 69 resistance, ~215 non-electric-heat (MD only). Small but sufficient for population-level comparisons.

#### Findings

1. **The electric-heat share is NOT inflated.** ResStock (39.2%) is actually slightly _below_ RECS (41.4%). The overstatement is not a composition effect — it's a per-building error.

2. **The HP/resistance split is consistent.** ResStock (50.8% HP / 49.2% resistance) matches RECS (47.5% HP / 52.5% resistance) within the margin of error for a 142-household sample.

3. **Non-electric-heat homes validate well.** ResStock's 9,238 kWh/yr is 5% _below_ RECS's 9,717 kWh/yr. This means the population-average overstatement is entirely from electric-heat homes — non-electric-heat homes are not contributing to the gap.

4. **Resistance-heated homes are the smoking gun.** ResStock overstates resistance-home electricity by **+58%** (20,033 vs 12,712 kWh/yr). This is far larger than the HP overstatement (+11%, 17,352 vs 15,596 kWh/yr).

5. **The overstatement hierarchy:** resistance (+58%) >> HP (+11%) > non-electric-heat (−5%). The ~7,300 kWh/yr excess in resistance homes dominates the population-average gap.

#### Distribution comparison (not just averages)

Weighted percentile comparison of annual kWh:

| Percentile | RECS Resistance | RS Resistance | RS/RECS | RECS Non-elec | RS Non-elec | RS/RECS |
|---:|---:|---:|---:|---:|---:|---:|
| p5 | 4,882 | 4,346 | 0.89 | 3,291 | 2,280 | 0.69 |
| p10 | 5,447 | 5,812 | 1.07 | 3,885 | 3,360 | 0.86 |
| p25 | 7,302 | 9,444 | 1.29 | 5,617 | 5,446 | 0.97 |
| **p50** | **9,265** | **16,591** | **1.79** | 8,564 | 8,518 | 0.99 |
| p75 | 15,853 | 26,724 | 1.69 | 12,631 | 12,097 | 0.96 |
| p90 | 24,158 | 38,765 | 1.60 | 16,888 | 15,960 | 0.95 |
| p95 | 28,449 | 48,582 | 1.71 | 19,225 | 18,713 | 0.97 |

The resistance distribution diverges sharply above the median (1.79× at p50), while the bottom tail roughly matches (p5–p10 within 10%). Non-electric-heat homes validate near-perfectly from p25 up (ratios 0.95–0.99).

#### What drives the upper tail: building type and square footage

Profiling resistance homes by kWh quartile reveals that building type — not vintage or equipment — is the dominant axis:

| Quartile | Avg kWh | MF 5+ share | SF Detached share | Avg sqft | kWh/sqft |
|---|---:|---:|---:|---:|---:|
| Q1 (bottom 25%) | 6,243 | **80%** | 3% | 929 | 6.7 |
| Q2 (25–50%) | 12,975 | **60%** | 5% | 1,142 | 11.4 |
| Q3 (50–75%) | 21,183 | 19% | **30%** | 1,606 | 13.2 |
| Q4 (top 25%) | **39,789** | 1% | **76%** | 2,568 | **15.5** |

The top quartile is almost entirely **single-family detached** homes (76%) with large square footage (avg 2,568 sqft). These SF detached resistance homes average **34,283 kWh/yr** overall — 3.7× the RECS median of 9,265. The MF homes in Q1 are actually the reasonable part of the distribution (avg 6,243 kWh for an apartment with electric heat is plausible).

The kWh/sqft progression (6.7 → 11.4 → 13.2 → 15.5) shows that larger homes consume disproportionately *more* per square foot, consistent with leakier envelopes in the 1970s–1980s vintage concentration of Q3/Q4.

**Notable asymmetry:** ResStock models `in.hvac_cooling_partial_space_conditioning` (with values 100%, 80%, 60%, 40%, 20%, <10%, None), but has **no equivalent for heating** — every home is implicitly modeled as heating 100% of its space. Secondary heating (`in.hvac_secondary_heating_fuel`) is "None" for all 4,897 BGE buildings. This means a baseboard-heated home that in reality only warms a few rooms is modeled as heating the entire house.

#### Gap accounting: resistance correction alone

If resistance homes' total kWh were corrected to RECS levels (12,712 kWh/yr), the BGE total would drop from 15.98B to 14.24B — closing **59% of the gap** to the 13.01B test-year target (from +22.8% to +9.4%). The remaining ~9% overstatement is attributable to HP overstatement (+11%), non-heating end-use bias in electric-heat homes, and survey noise.

#### Interpretation

The +58% resistance overstatement is consistent with the seasonal decomposition (Finding 4 above), which showed the gap concentrated in winter heating months. Resistance heating has COP ≈ 1.0 (verified in Finding 2), so the only way to overstate electric consumption is to overstate the thermal load — i.e., ResStock assigns too much heating demand to resistance-heated homes. The distribution and building-type evidence points to specific drivers:

- **SF detached resistance homes are the dominant source.** They average 34,283 kWh/yr and represent 29% of resistance customers but the vast majority of the excess kWh. The kWh/sqft intensity (15.5 for Q4) suggests systematically leaky envelope assumptions for large, older single-family homes.
- **No partial heating coverage model.** Unlike cooling (which has a partial-conditioning variable), heating assumes 100% of the home is heated. For zonal systems (baseboard, wall furnace — 35% of resistance homes), this likely overstates actual heated area.
- **No secondary heating.** All 4,897 BGE buildings have secondary heating = None. In reality, many resistance-heated homes supplement with wood stoves, gas space heaters, or portable heaters, reducing electric heating load.
- **MF homes are relatively well-calibrated.** The MF non-HVAC adjustment brought MF homes to reasonable levels (Q1 avg 6,243 kWh). The problem is concentrated in SF homes that didn't receive this correction.

The fact that HP homes are only +11% overstated, while sharing similar climate, suggests the issue is specific to how ResStock models the resistance-heating subpopulation — not a universal thermal envelope problem.

### Priority 3: End-use decomposition against RECS

**Source:** RECS 2020 MD end-use kWh estimates (modeled by EIA, not direct-metered) vs. ResStock BGE end-use output columns.

**Important caveat:** RECS models end uses independently using conditional demand analysis. The sum of RECS end uses overshoots the RECS billed total (KWH) by ~20–30%, depending on the subgroup. This means absolute kWh-per-end-use comparisons between RECS and ResStock conflate real modeling differences with RECS's own disaggregation noise. The most reliable way to read this table is to compare end-use **shares of total** and to look at the direction and magnitude of discrepancies rather than treating either dataset's per-end-use kWh as ground truth.

#### Absolute kWh comparison (weighted averages)

| End Use | RECS Resist | RS Resist | Δ kWh | RS/RECS | RECS Non-elec | RS Non-elec | Δ kWh | RS/RECS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **Space Heating** | 4,374 | 11,466 | **+7,092** | **2.62** | 537 | 364 | −173 | 0.68 |
| **Cooling** | 1,409 | 2,260 | **+851** | **1.60** | 1,842 | 3,013 | **+1,171** | **1.64** |
| Water Heating | 2,212 | 1,890 | −322 | 0.85 | 1,073 | 835 | −238 | 0.78 |
| Lighting | 450 | 937 | +487 | 2.08 | 727 | 1,098 | +371 | 1.51 |
| Refrigerator | 691 | 427 | −265 | 0.62 | 891 | 502 | −388 | 0.56 |
| Clothes Dryer | 401 | 410 | +9 | 1.02 | 412 | 460 | +48 | 1.12 |
| Cooking | 269 | 270 | +2 | 1.01 | 159 | 188 | +29 | 1.18 |
| TV+Related | 740 | — | −740 | — | 671 | — | −671 | — |
| Other/Residual | 4,026 | 1,992 | −2,035 | 0.49 | 5,375 | 2,338 | −3,037 | 0.44 |
| **Total (billed)** | **12,712** | **20,033** | **+7,320** | **1.58** | **9,717** | **9,238** | **−479** | **0.95** |

"TV+Related" (RECS `KWHTVREL`) has no direct ResStock equivalent; it would be included in plug loads. "Other/Residual" maps RECS `KWHOTH` against RS plug loads + mech vent + well pump — these categories don't correspond cleanly, which is why both show large negative deltas (RS has less in "other" because it itemizes more end uses).

#### Share of billed total

Because RECS end-use sums ≠ billed totals, comparing shares of each dataset's own billed total is more reliable:

| End Use | RECS Resist share | RS Resist share | Δ pp | RECS Non-elec share | RS Non-elec share | Δ pp |
|---|---:|---:|---:|---:|---:|---:|
| Space Heating | 34.4% | 57.2% | **+22.8** | 5.5% | 3.9% | −1.6 |
| Cooling | 11.1% | 11.3% | +0.2 | 19.0% | 32.6% | **+13.7** |
| Water Heating | 17.4% | 9.4% | −8.0 | 11.0% | 9.0% | −2.0 |
| Lighting | 3.5% | 4.7% | +1.1 | 7.5% | 11.9% | +4.4 |
| Refrigerator | 5.4% | 2.1% | −3.3 | 9.2% | 5.4% | −3.7 |

#### Findings

1. **For resistance homes, space heating dominates everything.** It accounts for 57% of RS total (vs 34% in RECS) — a 23 pp share shift. The +7,092 kWh absolute excess in heating alone roughly equals the +7,320 kWh total gap. This confirms heating is effectively the entire story for resistance homes.

2. **Cooling overstatement is systematic but not additive to the gap.** ResStock cooling runs ~60–65% above RECS across _all_ heating groups (resistance, HP, and non-electric-heat). For resistance homes, the +851 kWh cooling excess is real, but because the total gap is already explained by heating, this means other end uses (water heating, refrigerator, plug loads) are correspondingly understated or differently categorized. The cooling share of total for resistance homes is nearly identical in both datasets (11.1% vs 11.3%), meaning the cooling overstatement in absolute terms is an artifact of the inflated total driven by heating.

3. **Non-electric-heat homes show a cooling share shift (+14 pp) but a correct total.** RS allocates 33% of consumption to cooling (vs RECS's 19%), but since the total (9,238 kWh) validates against RECS (9,717 kWh), the within-home allocation must be offsetting elsewhere. The most likely explanation: RS itemizes plug loads, mech vent, and well pump separately while RECS bundles more into "Other" — the category mapping difference absorbs the apparent cooling and lighting overstatement.

4. **Lighting and refrigerator biases are consistent but small.** RS overstates lighting by ~50–100% and understates refrigerators by ~40–50% across all groups. These are smaller in absolute terms (a few hundred kWh) and partially offset each other.

5. **Water heating is systematically understated.** RS water heating runs ~15–20% below RECS across all groups. For resistance homes, this is a −322 kWh offset that slightly reduces the heating-driven overstatement.

#### Interpretation for rate design

The end-use decomposition reinforces the conclusion from Priorities 1 & 2: **the overstatement is overwhelmingly a resistance-heating problem, not a broad end-use calibration problem.** Non-electric-heat homes validate on total kWh even though the within-home allocation differs from RECS — and for rate design, it's the total and the hourly shape that matter, not the end-use breakdown per se.

The systematic cooling overstatement (~60–65%) is worth noting for future load-shape work: if ResStock overstates cooling across the board but the total matches for non-electric-heat homes, then non-cooling end uses (plug loads, miscellaneous) must be understated to compensate. This would make the summer peak slightly too "peaky" and the baseload slightly too flat — a minor distortion for TOU rate design but not for the overall consumption calibration.

## Remaining recommended analyses

Prioritized by expected diagnostic value, using data already available:

### Priority 4: Square footage and vintage comparison

**Question:** Are electric-heat homes in ResStock systematically larger or older than in reality?
**Method:** Compare ResStock's `in.sqft` and `in.vintage` distributions for electric-heat homes against RECS 2020 microdata for MD. If ResStock assigns electric heat to larger or older homes, their thermal loads would be overstated even with correct per-sqft assumptions.
**Why fourth:** Tests the "different envelope" hypothesis from the reconciliation section.

### Priority 5: Subclass-specific seasonal decomposition

**Question:** Is the winter overstatement uniform across heating types, or concentrated in electric-heat homes?
**Method:** Repeat the monthly EIA vs ResStock comparison, but split ResStock into electric-heat and non-electric-heat homes. If non-electric-heat homes match EIA's winter shape, the entire winter overstatement is from electric-heat homes. This requires careful share-weighting against EIA.
**Why fifth:** Cleanest test of whether heating loads are overstated per-building, but requires assuming the share split is correct (which Priority 1 may have already answered).

### Priority 6: BGE-specific monthly EIA data

**Question:** Does the seasonal decomposition hold when using BGE-specific (not all-MD) monthly data?
**Method:** EIA-861M detailed data files (XLS from eia.gov) may contain utility-level monthly sales. If accessible, re-run the seasonal decomposition with BGE-specific monthly data to eliminate the cross-utility comparison issue.
**Why sixth:** Would make the seasonal finding definitive, but data availability is uncertain (EIA-861M is a sample survey with state-level publication).

### Priority 7: Master-metered MF estimation

**Question:** How much residential consumption is "missing" from EIA-861 due to master-metered multifamily buildings?
**Method:** Estimate the fraction of MF units that are master-metered (not individually metered) in BGE's territory. Their electricity would appear in commercial sales, not residential. If 10-15% of MF units are master-metered, this could account for a few percent of the gap.
**Why seventh:** Hard to quantify precisely without BGE-specific data, but sets a bound on this known definitional mismatch.

## Scripts and data paths

All data used in this analysis:

| Data | Path |
|---|---|
| ResStock 2024.2 AMY2018 raw NREL | `/ebs/data/nrel/resstock/res_2024_amy2018_2/` |
| ResStock 2024.2 AMY2018 `_sb` variant | `/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/` |
| Utility assignment | `.../res_2024_amy2018_2_sb/metadata_utility/state=MD/utility_assignment.parquet` |
| BGE rate case YAML | `rate_design/hp_rates/md/config/rev_requirement/bge_rate_case_test_year.yaml` |
| Karas testimony | `reports2/context/sources/md_hp_rates/mdpuc_331766_direct_testimony_karas.md` |
| RECS 2020 microdata | Downloaded to `/tmp/` from EIA (CSV format) |
| EIA-861 annual (PUDL) | `s3://pudl.catalyst.coop/nightly/core_eia861__yearly_sales.parquet` |
| EIA-176 gas (PUDL) | `s3://pudl.catalyst.coop/nightly/core_eia176__yearly_gas_disposition_by_consumer.parquet` |
| EIA monthly sales API | `api.eia.gov/v2/electricity/retail-sales/data/` (requires `EIA_API_KEY` from `.env`) |

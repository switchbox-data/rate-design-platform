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

### Possible reconciliations

1. **Electric-heat homes have different envelopes than gas-heat homes.** ResStock might model gas-heated homes' thermal envelopes correctly but overstate the thermal load for electric-heated homes (e.g., by assigning too many older/leaky homes to electric heat, or by overstating square footage for electric-heat homes).

2. **The electric heating share is too high.** ResStock assigns ~39-43% of MD/BGE homes to electric heat. If the true share is lower (RECS 2020 suggests ~37-39%), the population-average heating is inflated. This is a composition effect, not a per-building error.

3. **The all-MD comparison is not identical to BGE.** The seasonal decomposition compares all-MD EIA to all-MD ResStock. If the electric-heat share or the consumption pattern differs between BGE and the rest of MD, the decomposition could be skewed.

4. **Non-heating end uses are modestly overstated across the board (+5%).** This accounts for ~31% of the gap. Possible drivers: cooling load overstatement, plug load overstatement, or appliance saturation differences between ResStock's national calibration and BGE's territory.

5. **The gas comparison validates per-customer consumption but not customer count.** ResStock over-assigns BGE gas customers by ~36%. If there's a similar over-assignment on the electric side for electric-heat homes (assigning more buildings to electric heat than reality), the population-average heating kWh would be inflated even if per-building heating is correct.

## What we can conclude with confidence

1. **The calibration target is sound.** EIA-861 (2018) matches the Karas test year within 0.5%.
2. **The MF non-HVAC adjustment helped but didn't fix it.** It reduced the gap by ~3.7 pp.
3. **Non-heating electricity is modestly overstated (~5%).** Validated by summer months matching EIA within 5%.
4. **The majority of the overstatement (~69%) is concentrated in winter/heating months.** This is the strongest empirical finding.
5. **Gas per-customer consumption validates well (+1.2%).** The thermal model works for gas-heated homes.
6. **The overstatement is structural, not weather-related.** EIA-861 2018 and the test year track closely despite spanning 7 years.

## What remains uncertain

1. **Whether the heating overstatement comes from per-building loads or composition (share of electric-heat homes).** These two mechanisms produce different corrections — one calls for scaling heating loads, the other for adjusting weights.
2. **Whether the ~5% non-heating overstatement is uniform or end-use-specific.** Could be cooling, could be plug loads, could be both.
3. **Whether the gas-side validation truly implies the electric-heat thermal envelope is correct.** Different housing stock, different vintage distribution.

## Recommended additional analyses

Prioritized by expected diagnostic value, using data already available:

### Priority 1: Electric-heat share validation

**Question:** Is the electric heating share in ResStock (39-43% for MD/BGE) correct, or is it inflated?
**Method:** Compare ResStock's electric heating share against RECS 2020 microdata (already downloaded), ACS housing data, and EIA-861 reported customer counts by heating fuel (if available). If the share is inflated, the population-average heating overstatement is a composition effect, not a per-building error.
**Why first:** This distinguishes between "buildings are too hot" and "too many buildings are assigned to electric heat" — fundamentally different corrections.

### Priority 2: Non-electric-heat homes as a control group

**Question:** Do gas-heated homes' total electricity match EIA expectations?
**Method:** Take only non-electric-heat homes from ResStock. Their annual electricity is almost entirely non-heating. Compute their weighted average electricity and compare to what EIA implies for non-electric-heat homes (using RECS heating fuel shares × EIA total). If gas homes' electricity is correct, the overstatement is entirely in electric-heat homes (either per-building or share).
**Why second:** Uses data already computed; isolates whether the non-heating overstatement is uniform across heating types or specific to electric-heat homes.

### Priority 3: End-use decomposition against RECS

**Question:** Which specific end use (cooling, water heating, plug loads, lighting) is overstated?
**Method:** RECS 2020 microdata (already downloaded at `/tmp/`) has end-use consumption estimates for MD (`KWHCOL`, `KWHWTH`, etc.). Compare ResStock's end-use outputs against RECS end-use estimates, end use by end use.
**Why third:** Pinpoints the non-heating overshoot (currently attributed to a ~5% catchall). If cooling is the outlier, it changes the interpretation — cooling overstatement would be concentrated in summer, not winter.

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

### Priority 7: Weather degree-day comparison

**Question:** Was 2018 a normal weather year for Baltimore, or did it have unusual HDD/CDD?
**Method:** Compare Baltimore AMY2018 heating and cooling degree days to TMY3 and to the 30-year average. Any excess would explain some of the overstatement.
**Why seventh:** Quick check, but we already showed AMY vs TMY differences were small for total consumption. Still worth quantifying for completeness.

### Priority 8: Master-metered MF estimation

**Question:** How much residential consumption is "missing" from EIA-861 due to master-metered multifamily buildings?
**Method:** Estimate the fraction of MF units that are master-metered (not individually metered) in BGE's territory. Their electricity would appear in commercial sales, not residential. If 10-15% of MF units are master-metered, this could account for a few percent of the gap.
**Why eighth:** Hard to quantify precisely without BGE-specific data, but sets a bound on this known definitional mismatch.

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

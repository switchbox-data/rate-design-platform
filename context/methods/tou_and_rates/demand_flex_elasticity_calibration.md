# Demand-Flex Elasticity Calibration for NY Utilities

## Overview

This document describes how we calibrate the demand-flexibility elasticity parameter (epsilon) for each NY utility's TOU rate design. The elasticity controls how much HP customers shift load from peak to off-peak hours in response to TOU price signals. We anchor to empirical evidence from the Arcturus 2.0 meta-analysis.

## The Arcturus 2.0 empirical anchor

### What it is

Arcturus 2.0 (Faruqui et al., 2017) is a meta-analysis of 62 time-varying pricing pilots conducted across North America, Europe, and Australia. Each pilot enrolled residential customers on some form of time-of-use or dynamic pricing and measured how much they reduced peak demand compared to a control group.

### What they found

Peak demand reduction is strongly predicted by the peak-to-off-peak price ratio -- the higher the ratio, the more customers shift. They fit regression models separately for pilots with and without enabling technology (smart thermostats, in-home displays, etc.). The "no enabling technology" model, which is the one relevant to us, is:

$$\text{peak\_reduction} = -0.011 + (-0.065) \times \ln(\text{price\_ratio})$$

So at a 2:1 ratio, customers reduced peak demand by about 5.5%. At 4:1, about 10.6%. The relationship is log-linear -- doubling the ratio doesn't double the response; there are diminishing returns.

### Why "no enabling technology"

Arcturus separates results into groups based on whether customers had enabling technology. The no-tech group represents customers responding to price signals alone with no automation. This is the conservative baseline. With enabling tech, responses are roughly 2x larger -- but we don't assume HP customers have smart controls, so we anchor to the lower bound.

### How we use it

Our model uses a different functional form -- constant elasticity ($Q_{\text{shifted}} = Q \times (P / P_{\text{flat}})^\varepsilon$) rather than Arcturus's log-linear regression. We can't directly equate the two. Instead, we:

1. Take each utility's actual TOU price ratio
2. Ask Arcturus: "what peak reduction would real customers achieve at this ratio?"
3. Run our constant-elasticity model at a range of epsilon values
4. Pick the epsilon that produces the same peak reduction as Arcturus predicts

This gives us an empirically grounded elasticity: not a theoretical value, but one that reproduces the aggregate demand response observed in actual pricing pilots at a comparable price ratio.

### What Arcturus doesn't tell us

It measures aggregate outcomes across heterogeneous customers -- some shift a lot, some don't shift at all. Our model applies the same epsilon uniformly to every building. The recommended epsilon is therefore an approximation that matches the *average* pilot outcome, not the distribution of individual responses.

## Diagnostic methodology

### What varies across utilities

Each NY utility has different TOU structures derived from their marginal cost profiles:

- **Peak/off-peak price ratios** range from 1.57 (CenHud/OR winter) to 4.33 (ConEd summer)
- **Peak window widths** are either 3 hours (ConEd, NiMo, NYSEG, RGE) or 5 hours (CenHud, OR, PSEG-LI)
- **HP customer share** ranges from 1.4% (PSEG-LI) to 4.4% (RGE) of weighted customers

Because HP customers heat with electricity, they consume disproportionately more in winter, when the price ratio is lower. We compute a demand-weighted annual price ratio that reflects this seasonal load distribution, giving us a single Arcturus target per utility.

### How the diagnostic works

The diagnostic script (`utils/post/diagnose_demand_flex.py`) does the following for each utility:

1. Loads the actual hourly electricity consumption for every HP building in the baseline stock (upgrade=00)
2. Loads the TOU derivation data (price ratios, peak hours, base rates per season)
3. At each candidate elasticity (-0.02 through -0.20 in 0.02 steps), applies the same constant-elasticity shifting formula CAIRO uses: $Q_{\text{shifted}} = Q_{\text{orig}} \times (P_{\text{period}} / P_{\text{flat}})^\varepsilon$
4. Measures the resulting peak reduction percentage
5. Compares to the Arcturus prediction for this utility's price ratio
6. Selects the elasticity whose peak reduction most closely matches Arcturus

We also compute rate arbitrage savings: the dollars an HP customer saves by consuming less at the peak rate and more at the off-peak rate.

### Two bill savings mechanisms

1. **Rate arbitrage** (primary): HP customers on a TOU tariff shift load from expensive peak hours to cheap off-peak hours. Savings = kWh shifted x (peak rate - off-peak rate). This is independent of the revenue requirement change.
2. **RR reduction** (secondary, negligible at delivery level): under frozen residual, shifting reduces total MC, lowering the RR. Delivery MC is nonzero in only 40-113 hours/year, so this effect is effectively zero.

### Key data

- **Building loads**: ResStock upgrade=00 HP buildings, local parquet at `/ebs/data/nrel/resstock/res_2024_amy2018_2_sb/load_curve_hourly/state=NY/upgrade=00/`
- **TOU derivation**: `rate_design/hp_rates/ny/config/tou_derivation/{utility}_hp_seasonalTOU_derivation.json`
- **MC data**: `s3://data.sb/switchbox/marginal_costs/ny/{dist_and_sub_tx,bulk_tx}/utility={util}/year=2025/data.parquet` (NOT Cambium -- derived from NYISO LBMP and utility PUC filings)
- **CAIRO ground truth**: `s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/all_utilities/ny_20260325b_r1-16/`

## Results (March 2026)

### Recommended elasticities

| Utility | Recommended ε | Annual savings/HP bldg | Peak window | Annual weighted ratio | Arcturus target |
| ------- | :-----------: | :--------------------: | :---------: | :-------------------: | :-------------: |
| CenHud  |    -0.12      |         $27.58         |   5 hours   |         2.14          |      6.1%       |
| ConEd   |    -0.10      |         $25.72         |   3 hours   |         3.01          |      8.3%       |
| NiMo    |    -0.10      |         $12.47         |   3 hours   |         2.17          |      6.1%       |
| NYSEG   |    -0.10      |         $12.57         |   3 hours   |         2.16          |      6.1%       |
| OR      |    -0.12      |         $23.60         |   5 hours   |         2.17          |      6.1%       |
| PSEG-LI |    -0.12      |         $46.77         |   5 hours   |         2.51          |      7.1%       |
| RGE     |    -0.10      |         $13.67         |   3 hours   |         2.16          |      6.1%       |

### Key takeaways

1. **Two natural groups**: Utilities with 3-hour peak windows land at ε = -0.10; utilities with 5-hour peak windows land at ε = -0.12. Wider peak windows shift more kWh per unit of epsilon, so less epsilon is needed to match the same Arcturus target.

2. **Savings are modest but real**: $12-$48 per HP building per year from rate arbitrage alone. This is on top of the much larger savings (~$590 for ConEd) from simply switching to the TOU rate structure in the first place.

3. **Marginal cost savings are negligible**: Delivery MC is nonzero in only 40-113 hours per year. The frozen-residual RR reduction from load shifting is effectively zero at the delivery level. Bill savings come entirely from the rate structure spread.

4. **Winter matters less than summer for shifting savings**: Winter price ratios (1.6-2.0) are much lower than summer (2.9-4.3), so the rate spread available for arbitrage is smaller. HP customers' heavy winter load contributes little to shifting savings despite being the season where they consume the most.

5. **These are conservative estimates**: Arcturus "no enabling technology" represents the low end of observed demand response. With smart thermostats or other enabling technology, larger elasticities would be justified.

### CAIRO ground truth comparison

At ε = -0.10 (current setting), CAIRO bill outputs for ConEd show:

- Run 3+4 (default tariff, no flex): mean elec bill = $2,329/year
- Run 11+12 (TOU, no flex): mean = $1,740/year (TOU structure saves $589)
- Run 15+16 (TOU flex, ε=-0.1): mean = $1,729/year (flex adds $10.51 more savings)

Note: the $10.51 CAIRO figure is for the counterfactual all-HP world (runs 15+16, upgrade=02). The diagnostic's $25.72 is per HP building in the realistic baseline stock (upgrade=00). These are different scenarios and are not directly comparable.

## Validation (March 2026)

The shift mechanics were validated against CAIRO's own outputs using `utils/post/validate_demand_flex_shift.py`. The script reproduces the constant-elasticity shift analytically using the same `utils/cairo.py` functions CAIRO uses, then checks three invariants.

### Validation results at ε = -0.10

All 7 NY utilities pass all checks:

| Check | Result |
| ----- | ------ |
| Energy conservation (per building, per season) | PASS -- max \|orig − shifted\| = 0.000 kWh (machine precision) |
| Direction (peak kWh ↓, off-peak kWh ↑) | PASS -- all utilities, both seasons |
| CAIRO tracker match (per-building realized ε) | max \|Δε\| < 0.015, mean \|Δε\| < 0.003 across all utilities |

The small tracker differences appear only on the **receiver (off-peak) period**, where CAIRO's zero-sum residual differs slightly from our analytical reproduction due to floating-point accumulation order. Donor (peak) period epsilons match CAIRO exactly to machine precision.

### Peak reduction at ε = -0.10 across utilities

| Utility | Summer ratio | Summer peak red | Winter ratio | Winter peak red |
| ------- | :----------: | :-------------: | :----------: | :-------------: |
| CenHud  |     3.05     |     6.43%       |     1.57     |     3.35%       |
| ConEd   |     4.33     |     9.83%       |     1.98     |     5.44%       |
| NiMo    |     2.91     |     7.67%       |     1.75     |     4.60%       |
| NYSEG   |     2.93     |     7.68%       |     1.75     |     4.60%       |
| OR      |     3.13     |     6.56%       |     1.57     |     3.29%       |
| PSEG-LI |     3.80     |     7.40%       |     1.67     |     3.69%       |
| RGE     |     2.87     |     7.54%       |     1.75     |     4.58%       |

### What the diagnostic plots show

Five plots per utility are written to `dev_plots/flex/{utility}/`:

- **`aggregate_daily_profile.png`**: Mean kW per HP building by hour of day, pre/post shift, for summer and winter. The shaded fill between curves shows where load is removed (red, peak hours) and where it accumulates (blue, off-peak). The shift is visually clear in summer (larger ratio, wider spread); winter shifting is smaller.

- **`net_shift_by_hour.png`**: Bar chart of mean kWh change per building per hour. Red bars during peak hours, blue bars during off-peak. Bars sum to zero within each season (energy conservation). The plot immediately confirms the model is moving load in the right direction.

- **`shift_heatmap.png`**: Month × hour heatmap of mean kWh shift per building (red = removed, blue = added). Shows the full seasonal and diurnal pattern: summer months (Jun-Sep) have the darkest red in peak hours; winter months have a shallower but still visible shift. The transition months (Apr, Oct) are visually distinct from both seasons, reflecting which season's TOU schedule applies.

- **`peak_reduction_distribution.png`**: Histogram of per-building peak reduction %. At ε = -0.10, the distribution collapses to a single value with near-zero variance -- because constant elasticity with a uniform rate ratio produces the same proportional reduction for every building regardless of their load level. This is a known limitation of the model.

- **`building_daily_profile_{bldg_id}.png`**: Per-building view for 5 illustrative buildings at different consumption percentiles. Shows original and shifted load curves overlaid on a representative summer and winter weekday, with TOU rate structure as a step function on the right axis. Suitable as a report visual; per-building hourly data for 100 buildings is written to `{utility}_building_hourly_shifted.parquet` for further use.

### Invoking the validation

```bash
# One utility with CAIRO tracker cross-check:
just -f rate_design/hp_rates/ny/Justfile validate-demand-flex coned -0.10 ny_20260325b_r1-16

# All utilities:
just -f rate_design/hp_rates/ny/Justfile validate-demand-flex-all -0.10 ny_20260325b_r1-16

# Elasticity sweep diagnostic:
just -f rate_design/hp_rates/ny/Justfile diagnose-demand-flex
```

## Known limitations

1. **Uniform vs heterogeneous response**: Arcturus measures aggregate outcomes from diverse customers; our model applies the same epsilon to every HP building. The peak reduction distribution confirms this -- all buildings get the same % reduction, which is unrealistic.
2. **Functional form mismatch**: Arcturus is log-linear; our model is power-law. The comparison is valid at specific price ratios, not across the full curve.
3. **Annual elasticity is a compromise**: A single epsilon can't perfectly match both winter and summer Arcturus targets simultaneously. We use a demand-weighted annual ratio as the calibration anchor.
4. **Delivery MC is too sparse for meaningful savings**: The frozen-residual channel produces negligible savings because delivery MC is concentrated in ~100 hours/year. Supply MC (NYISO LBMP) would add a broader spread, but supply MCs are zeroed in delivery-only runs.

## References

- Faruqui, A., Sergici, S., & Warner, C. (2017). Arcturus 2.0: A meta-analysis of time-varying rates for electricity. *The Electricity Journal*, 30(10), 64-72.
- Simenone, M. et al. (2023). Bill alignment test paper. *Utilities Policy*.

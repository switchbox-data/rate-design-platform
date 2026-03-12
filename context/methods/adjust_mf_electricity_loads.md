# Adjusting Multifamily Electricity Loads in `adjust_mf_electricity.py`

This document explains the high-level methodology implemented in `utils/pre/adjust_mf_electricity.py`.

The script applies a multifamily non-HVAC electricity adjustment to ResStock data. Its purpose is to reduce a systematic mismatch between ResStock electricity totals and EIA-861 residential electricity sales at the utility level.

## What Problem This Script Solves

Exploratory analysis comparing ResStock annual electricity consumption to EIA-861 residential sales found a clear pattern:

- utilities with a higher share of multifamily buildings tended to show a larger mismatch between ResStock and EIA totals

The core hypothesis was that ResStock was overstating multifamily non-HVAC electricity intensity relative to single-family homes. More specifically, multifamily buildings often appeared to have higher simulated non-HVAC electricity consumption per square foot than was plausible when compared across utilities and against EIA totals.

This script corrects that by scaling multifamily non-HVAC electricity consumption downward so that multifamily buildings more closely match the single-family electricity intensity benchmark.

The empirical motivation was strong. Before this adjustment, the ResStock-to-EIA mismatch for some utilities was as high as about `+30%`, meaning ResStock annual electricity consumption was roughly 30% higher than the corresponding EIA residential sales total. After applying the multifamily non-HVAC correction, that mismatch dropped to roughly `+/-6%`.

## Core Idea

The method treats single-family buildings as the reference group and multifamily buildings as the group to be adjusted.

For each non-HVAC electricity end use, the script:

1. Computes average electricity consumption intensity for single-family buildings.
2. Computes average electricity consumption intensity for multifamily buildings.
3. Forms a multifamily-to-single-family ratio.
4. Divides multifamily non-HVAC electricity consumption by that ratio.

Here, electricity consumption intensity means:

- annual electricity consumption for the end use
- divided by floor area in square feet

So if multifamily buildings have 1.5 times the single-family intensity for a given non-HVAC end use, the script scales that multifamily end use down by a factor of `1 / 1.5`.

## Why the Adjustment Focuses on Non-HVAC Loads

The adjustment is deliberately limited to non-HVAC electricity uses.

The motivating investigation found that the multifamily-related discrepancy was most plausibly tied to appliance and miscellaneous end uses rather than heating and cooling loads. HVAC behavior is therefore left unchanged, while non-HVAC electricity is rescaled.

This conclusion was supported by the intensity comparison itself. On average, multifamily non-HVAC electricity consumption intensity was about `1.5x` to `2.0x` higher than single-family intensity, while HVAC-related electricity consumption intensity was actually about 40% lower for multifamily buildings than for single-family buildings. That HVAC result is directionally sensible: multifamily buildings typically have less exposed exterior surface area per dwelling unit than single-family homes, so they generally require less heating and cooling energy per square foot.

Conceptually, the method says:

- keep the simulated HVAC behavior as-is
- reduce multifamily non-HVAC electricity toward the single-family per-square-foot benchmark

## Which Buildings Are Adjusted

The script adjusts:

- buildings classified as multifamily in `in.geometry_building_type_recs`
- only when they have not already been adjusted

Single-family buildings are not modified. They are used as the comparison baseline for the multifamily scaling ratios.

## How the Ratios Are Computed

The scaling ratios are estimated from annual ResStock data.

For each non-HVAC electricity column:

1. Join annual electricity results to metadata so each building has:
   - building type
   - floor area
2. Convert floor area into a numeric square-foot value.
3. Compute end-use electricity intensity as `kWh / sqft`.
4. Separate buildings into single-family and multifamily groups.
5. Restrict to buildings with non-zero consumption for that end use.
6. Compute the mean intensity for each group.
7. Define the adjustment ratio as:

```text
MF/SF ratio = mean multifamily kWh/sqft / mean single-family kWh/sqft
```

If there are too few usable observations for a stable estimate, the script falls back to a ratio of `1.0`, meaning no adjustment for that end use.

## Why Non-Zero Buildings Are Used

The ratio is computed using only buildings with non-zero consumption for a given end use.

This matters because many end uses are not present in every home. For example, some buildings may have no pool pump, no well pump, or no electric dryer. Including large numbers of zero-consumption buildings would blur the conditional intensity of homes that actually use that end use.

Using non-zero buildings makes the ratio answer the question:

"Among buildings that actually have this load, how different is multifamily electricity intensity from single-family electricity intensity?"

## What Gets Adjusted

The script adjusts non-HVAC electricity end uses such as:

- plug loads
- lighting
- appliances
- water heating
- other miscellaneous non-HVAC electric loads

It then propagates that adjustment into:

- annual total electricity consumption
- hourly non-HVAC electricity component columns
- hourly total electricity consumption

The main point is that the method is applied consistently at both annual and hourly levels, so the final hourly load curves remain aligned with the adjusted annual non-HVAC totals.

## What Does Not Get Adjusted

The script does not adjust:

- heating electricity
- cooling electricity
- other HVAC-related electricity behavior

So this is not a general electricity rescaling. It is specifically a multifamily non-HVAC correction.

## How the Hourly Adjustment Works

Once the annual multifamily-to-single-family ratios have been computed, the same end-use-specific scaling factors are applied to hourly multifamily load curves.

For each multifamily building:

1. Identify the hourly columns corresponding to each annual non-HVAC electricity end use.
2. Divide the hourly non-HVAC consumption and intensity columns by the same ratio used in the annual data.
3. Recompute total hourly electricity so that:
   - HVAC electricity is preserved
   - adjusted non-HVAC electricity is reflected in the total

This preserves the building's hourly shape within each adjusted non-HVAC end use while reducing its magnitude.

## Metadata Update

After a multifamily building is adjusted, the script marks it with:

- `mf_non_hvac_electricity_adjusted = True`

This serves as a lineage flag so later workflows can tell which buildings have already received the multifamily non-HVAC correction and avoid applying it twice.

## End Result

After the adjustment:

- multifamily buildings have lower non-HVAC electricity consumption where their simulated intensity exceeded the single-family benchmark
- single-family buildings remain unchanged
- HVAC electricity remains unchanged
- total annual and hourly electricity for multifamily buildings is reduced only through the non-HVAC channel

The intended effect is to reduce the utility-level bias where utilities with larger multifamily shares showed larger ResStock-versus-EIA mismatches.

## Main Modeling Assumption

The key assumption is that single-family electricity intensity per square foot is a better benchmark for multifamily non-HVAC electricity than the raw multifamily values produced by ResStock.

Put differently, the method assumes that at least part of the observed ResStock-EIA discrepancy comes from multifamily non-HVAC loads being too large, and that scaling them toward the single-family benchmark improves aggregate realism.

This is an empirical correction motivated by cross-utility comparison, not a claim that single-family and multifamily buildings should have identical load composition in every respect.

## Relationship to the EIA Comparison Work

This script is the operational follow-through from the comparison and investigation work documented elsewhere.

The earlier analysis showed:

- ResStock weighted electricity totals often exceeded EIA-861 residential sales
- the gap was correlated with the share of multifamily buildings in a utility
- column-by-column multifamily versus single-family analysis suggested non-HVAC electricity intensity as the likely source of the bias
- in some utilities, the pre-adjustment mismatch was about `+30%`, while after adjustment it was reduced to around `+/-6%`

`adjust_mf_electricity.py` turns that finding into a data-processing correction by modifying multifamily non-HVAC electricity before downstream rate-design analysis.

## Bottom Line

`adjust_mf_electricity.py` is a multifamily non-HVAC electricity correction. It uses annual ResStock data to estimate how much higher multifamily non-HVAC electricity intensity is than single-family intensity, scales multifamily non-HVAC loads down by those ratios, applies the same correction to hourly load curves, and records which buildings were adjusted.

The goal is to make utility-level ResStock electricity totals better align with EIA-861, especially for utilities with a high share of multifamily buildings.

# Reweighting Customer Counts in `reweight_customer_counts.py`

This document explains the high-level methodology implemented in `utils/pre/reweight_customer_counts.py`.

The script rescales the `weight` column in customer metadata so that the total represented customer count matches a utility-specific target. In other words, it replaces the default ResStock weighting for a selected building sample with a new set of weights aligned to the actual customer count for the utility being analyzed.

## What Problem This Script Solves

ResStock building samples come with default weights that describe how many real-world dwellings each sampled `bldg_id` represents. Those default weights are useful for stock-level analysis, but they do not always match the exact customer count that should be used for a specific utility rate-design study.

For utility-specific analysis, we often want the modeled building sample to represent the actual number of customers served by that utility, as established by a rate case or other utility-specific source. If the total sample weight is too high or too low relative to that target, downstream totals such as bills, revenues, or load aggregates will also be scaled incorrectly.

This script fixes that by reweighting the sample so the sum of building weights equals the desired utility customer count.

## Core Idea

The method is a uniform proportional rescaling of weights.

Given:

- a table of buildings with an existing `weight` column
- a target total customer count for the utility

the script computes a single scaling factor:

```text
scale_factor = target_customer_count / current_sum_of_weights
```

It then applies that same factor to every building:

```text
new_weight = old_weight * scale_factor
```

So the relative weighting across buildings is preserved, while the total represented customer count is changed to match the utility target.

## What Stays the Same

This script does not change:

- which `bldg_id` values are in the sample
- the relative differences between buildings
- any building characteristics or load shapes

It only changes the magnitude of the `weight` column.

That means the sample composition stays the same. The script is not reselecting buildings or changing the underlying building mix. It is only changing how much each sampled building counts toward utility-level totals.

## What Changes

After reweighting:

- the sum of `weight` matches the target utility customer count
- every building's contribution to aggregate metrics is scaled up or down proportionally

This affects any downstream quantity that uses the metadata weights for aggregation, such as:

- total customer counts
- utility-level load totals
- total bills or revenues
- weighted averages derived from the sample

## Why a Uniform Scaling Is Appropriate

The method assumes that the existing ResStock sample composition is acceptable, but that the total scale of the sample should be adjusted to match the utility's actual customer count.

Because every building is multiplied by the same factor, the procedure preserves the original distribution of building types, end uses, and load shapes within the sampled utility population. It only corrects the total represented population size.

## End Result

The output is the same customer metadata table with an updated `weight` column.

After this step, the building sample can be used in downstream rate-design analysis with weights that reflect the actual utility customer count rather than the default ResStock total.

## Bottom Line

`reweight_customer_counts.py` performs a simple proportional reweighting of ResStock building weights. Its goal is to make the modeled sample represent the actual number of customers for a utility, while leaving the composition of the sample unchanged.

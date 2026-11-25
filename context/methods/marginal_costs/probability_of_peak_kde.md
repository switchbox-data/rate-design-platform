# KDE probability of peak

How one annual `$/kW-yr` marginal cost becomes an hourly cost series: one or more years of load become a probability of peak for each month × day-type × hour, and that probability is placed on one calendar year's 8,760 hours. The code is `pop_kde` in [`utils/data_prep/marginal_costs/methods/pop_kde.py`](../../../utils/data_prep/marginal_costs/methods/pop_kde.py).

This document describes the method and the script only. Which load history, which `$/kW-yr`, and which years a given run uses are documented with that run's marginal-cost component.

The five steps are the reading of Eversource's OCC-733 response (PURA Docket 26-05-10) that reproduces the probability-of-peak table on OCC-863 Attachment 2 page 3. The delimiter in step 3 is the reading that matches that table. OCC-733's sentence says "the maximum hourly load in the year," which read as the single highest hour does not.

---

## 1. Inputs

The method has no knowledge of where its inputs come from. It needs four:

| Input         | What the caller passes                                                                                                                                                                             |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Load history  | One or more complete calendar years, stacked, with `timestamp` and `load_mw`. Each year needs at least 8,736 hours, because each year is divided by its own average. The units do not matter (§3). |
| Annual cost   | One `$/kW-yr` value, already in the dollar year the output should be in. The hourly costs sum to it.                                                                                               |
| Year          | The year stamped on the output timestamps.                                                                                                                                                         |
| Calendar year | The calendar that classifies weekdays, weekends, and holidays and that sets each cell's day count. Defaults to `year`. The two years must both be leap years or both not.                          |

The fifth argument, `output_column`, only names the cost column (default `mc_total_per_kwh`).

Scott bandwidth, truncation at 3 bandwidths, and the federal-holiday day types are fixed inside the script. `probability_of_peak` still accepts `truncation_sigmas` and `bandwidth_multiplier` for a sensitivity run; `pop_kde` uses the defaults.

Everything else is the caller's job: choosing and reading the load history, choosing and dollar-year-adjusting the `$/kW-yr`, validating the result, and writing outputs and provenance.

This script builds the 8,760 itself from `year` and `calendar_year`. It does not take a load that is already the output clock.

---

## 2. Formula

Let $C$ be the annual cost in `$/kW-yr`.

1. **Day type.** Weekend-and-holiday is Saturday, Sunday, and the eleven federal holidays in 5 U.S.C. § 6103. A fixed-date holiday on Saturday is observed the Friday before, and one on Sunday the Monday after, so December 31 is included when the next January 1 is a Saturday. Juneteenth is included from 2021. These day counts match the "No Days" rows on OCC-863 Attachment 2.

2. **Normalize.** Divide each hour by the average hourly load of its calendar year. Everything after this step is in those units, so the result does not depend on whether the input is MW or kW, or on a rescaling of one year relative to another.

3. **Delimiter.** For each year, take the highest of the 576 month × day-type × hour means of the normalized load. The delimiter is the average of that number over years. On Eversource's 2022–2025 substation net load it is 1.656, about 0.834 of the annual peak. The 0.834 figure is where the delimiter sits; it is not an input.

4. **Density per cell.** A Gaussian kernel density on the normalized loads in that cell. Bandwidth is Scott's rule, $h = s \, n^{-1/5}$, with $s$ the sample standard deviation (the `scipy.stats.gaussian_kde` default). Each kernel is cut at ±3 bandwidths and renormalized. The ±3 cut is what "truncated Gaussian KDE" names; dropping it barely moves the weights.

5. **Weight.** The share of that density above the delimiter, computed as the mean over kernels of the truncated Gaussian tail. That is the expectation of the share of simulated draws, so the function does not sample and has no seed. The share is not multiplied by the number of days in the cell. Probability of peak is the share divided by the sum over all 576 cells, so the probabilities sum to 1.

No solar series is subtracted, and no heat-pump adder is applied. The input is one load column.

For the target calendar year, count the days $N_{m,d}$ of each month and day type on that year's CAIRO 8760 (December 31 is dropped in a leap year). The price in a cell is

$$MC_{m,d,h} = C \times \frac{\mathrm{PoP}_{m,d,h}}{N_{m,d}}$$

Each hour of the 8760 takes its month-hour value from the weekday table or the weekend-and-holiday table, using the same expansion as the month-hour method (`expand_month_hour_mc_to_8760`). The day counts are applied here, not inside the probability. The uniform scale that month-hour uses is not applied: the day counts and the calendar match, so the sum is already $C$.

$MC_h$ is dollars per kW of demand in hour $h$. A customer who uses 1 kWh in that hour is charged $MC_h$ dollars, so the column reads as `$/kWh`.

`probability_of_peak` returns the 576 cells, the delimiter, and one row per history year. `allocate_annual_cost_by_pop` places a cell table on the 8760. `pop_kde` is those two steps. `month_hour_costs_from_pop` returns the 576 cells with `num_days` and the `$/kWh` column, for a caller that does not want the 8760.

---

## 3. Properties

- **Recovers the annual cost exactly.** $\sum_h MC_h = C$, so a flat 1 kW load pays $C$ over the year. The function raises if the 8760 does not sum to $C$.
- **Depends on the shape inside each cell, not on the level of the load.** Dividing a year by its own mean removes the units and a year-long rescaling. Two history years are weighted by the hours they contribute to each cell.
- **Spreads cost across every hour of a cell that has density above the delimiter.** A cell with probability zero stays zero. Unlike the top-N methods, cost is not confined to N hours of one load year.
- **Builds its own clock.** The history years and the output year need not match. `calendar_year` chooses the weekdays; `year` is stamped on the timestamps by position. A leap year paired with a non-leap year raises, because the rows would shift after February.
- **Raises** when a required column is missing, `timestamp` is not a datetime, a history year has fewer than 8,736 hours, a month × day-type × hour cell has fewer than two observations, no cell has density above the delimiter, or the probability table passed to the allocator is not 576 nonnegative cells that sum to 1.

---

## 4. What it is not

Other allocations give different hourly series and live in different code:

- **Top-N load-share** puts the annual cost on the N highest hours of one load year, each in proportion to its load. See [top_n_load_share_allocation.md](top_n_load_share_allocation.md).
- **Top-N exceedance** weights those same hours by load above a threshold. See [top_n_exceedance_allocation.md](top_n_exceedance_allocation.md).
- **Seasonal SCR exceedance** splits the year into summer and winter before that exceedance weighting. See [seasonal_scr_exceedance_allocation.md](seasonal_scr_exceedance_allocation.md).
- **Month-hour expansion** looks up a published month-hour table, including a published probability of peak, and then scales the 8760 so it sums to the annual cost. This method fits the probability from load and does not scale.

---

## 5. Running it directly

From Python:

```python
from utils.data_prep.marginal_costs.methods.pop_kde import pop_kde

hourly = pop_kde(load, annual_cost_kw_year=69.0, year=2025)
```

`calendar_year` defaults to `year`. `output_column` renames the cost column. A caller that already holds the `ProbabilityOfPeak` for that same load can pass it as `fitted` so the history is not fit twice.

From the command line, on a local parquet with `timestamp` and `load_mw`:

```bash
uv run python utils/data_prep/marginal_costs/methods/pop_kde.py \
  --path-load load.parquet \
  --annual-cost-kw-year 69.0 \
  --year 2025 \
  --path-output allocated.parquet
```

`--calendar-year` overrides the day-type calendar. The CLI reads and writes local parquet only; it does not read S3, adjust dollar years, or write a manifest.

Tests: `tests/test_pop_kde.py`.

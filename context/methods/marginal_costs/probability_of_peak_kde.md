# Probability of peak from a truncated Gaussian KDE

How `utils/data_prep/marginal_costs/probability_of_peak.py` turns an hourly load history into a month × day-type × hour probability of peak, and then into an 8760 marginal-cost signal. This is the reading of the five steps in Eversource's OCC-733 response (PURA Docket 26-05-10) that the `dev/eversource_pop_analysis/` reconstruction settled on. It is a library. The CT bulk transmission writer calls it by default: the CT Justfile passes `generate_bulk_tx_mc.py --iso isone --bulk-tx-method pop_kde`, which fits it on 2022–2025 ISO-NE system load ([ct_bulk_transmission_marginal_cost.md](ct_bulk_transmission_marginal_cost.md)). The four-year window there is chosen to match the history Eversource used. The shared CLI still defaults to the top-100 exceedance method, which is what RI runs.

It is a different calculation from the platform's existing "PoP" allocator (`calculate_pop_weights` in `generate_utility_tx_dx_mc.py`), which puts one `$/kW-yr` on the top 100 load hours. That allocator is what NY, RI, MD, and the CT `pop_occ863` / `pop_isone_zone` distribution runs use. See [ct_eversource_dist_mc_methodology.md](ct_eversource_dist_mc_methodology.md) §3.1.

---

## What it takes and what it returns

`probability_of_peak(load)` takes one row per hour: a naive hour-beginning local `timestamp` and a load column (default `load_mw`). Every calendar year in the frame must be essentially complete (at least 8,736 hours), because each year is divided by its own average. It returns a `ProbabilityOfPeak`:

| Field       | Contents                                                                                                                                                                                                      |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cells`     | 576 rows, one per month × day type × hour. `n_obs`, `bandwidth`, `p_exceed` (share of the cell's density above the delimiter), `probability_of_peak` (`p_exceed` divided by its sum, so the column sums to 1) |
| `delimiter` | The threshold, in units of a year's average load                                                                                                                                                              |
| `years`     | One row per calendar year: hours, mean load, peak load, that year's highest cell mean (`max_cell_mean`), and the delimiter in the year's own load units (`delimiter_load` = delimiter × mean load)            |

`allocate_annual_cost_by_pop(cells, annual_cost_kw_year, year)` turns that table into an 8760 of `$/kWh` that sums to `annual_cost_kw_year`. `calendar_year` (default `year`) is the calendar used to count days and to classify weekdays, weekends, and holidays; `year` is stamped on the timestamps, the same split `expand_month_hour_mc_to_8760` uses.

---

## Steps

1. **Day type.** Weekend-and-holiday is Saturday, Sunday, and the eleven federal holidays in 5 U.S.C. § 6103. A fixed-date holiday on Saturday is observed the Friday before, and one on Sunday the Monday after, so December 31 is included when the next January 1 is a Saturday. Juneteenth is included from 2021. This is the same rule `expand_month_hour_mc_to_8760` uses, and the one whose day counts match the "No Days" rows on OCC-863 Attachment 2.

2. **Normalize.** Divide each hour by the average hourly load of its calendar year. Everything after this step is in those units, so the result does not depend on whether the input is MW or kW, or on load growth between years.

3. **Delimiter.** For each year, take the highest of the 576 month × day-type × hour means of the normalized load. The delimiter is the average of that number over years. On Eversource's 2022–2025 substation net load it is 1.656, about 0.834 of the annual peak. The 0.834 figure is where the delimiter sits; it is not an input. OCC-733's sentence says "the maximum hourly load in the year," which read as the single highest hour does not reproduce the filed table. The highest _cell mean_ is the reading that does.

4. **Density per cell.** A Gaussian kernel density on the normalized loads in that cell. Bandwidth is Scott's rule, \(h = s \, n^{-1/5}\), with \(s\) the sample standard deviation (the `scipy.stats.gaussian_kde` default). Each kernel is cut at ±3 bandwidths and renormalized. The ±3 cut is what "truncated Gaussian KDE" names; dropping it barely moves the weights.

5. **Weight.** The share of that density above the delimiter, computed as the mean over kernels of the truncated Gaussian tail. That is the expectation of the share of simulated draws, so the function does not sample and has no seed. The share is not multiplied by the number of days in the cell. Probability of peak is the share divided by the sum over all 576 cells.

No solar series is subtracted, and no heat-pump adder is applied. The input is one load column.

---

## From the weights to `$/kWh`

For a target calendar year, count the days \(N_{m,d}\) of each month and day type on that year's CAIRO 8760 (December 31 is dropped in a leap year). The price in a cell is

$$
MC_{m,d,h} = C \times \frac{\mathrm{PoP}_{m,d,h}}{N_{m,d}}
$$

with \(C\) the annual `$/kW-yr`. Each hour of the 8760 takes its month-hour value from the weekday table or the weekend-and-holiday table. A flat 1 kW load then pays \(C\) over the year, and `allocate_annual_cost_by_pop` raises if the sum does not match. The day counts are applied here, not inside the probability.

`month_hour_costs_from_pop` returns the 576 cells with `num_days` and the `$/kWh` column, for a caller that does not want the 8760.

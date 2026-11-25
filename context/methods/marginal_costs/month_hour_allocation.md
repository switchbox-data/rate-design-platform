# Month-hour calendar expansion

How a 288-row month-hour table becomes an hourly cost series: each hour of one calendar year takes the weekday or the weekend-and-holiday value, and every hour is then multiplied by one factor so the 8,760 sums to an annual `$/kW-yr`. The code is `month_hour` in [`utils/data_prep/marginal_costs/methods/month_hour.py`](../../../utils/data_prep/marginal_costs/methods/month_hour.py).

This document describes the method and the script only. Which tables, which `$/kW-yr`, and which years a given run uses are documented with that run's marginal-cost component.

---

## 1. Inputs

The method has no knowledge of where its inputs come from. It needs:

| Input         | What the caller passes                                                                                                                |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Table kind    | `rates` or `probability`.                                                                                                             |
| Weekday       | 288 rows, `month` 1–12 and `hour` 0–23.                                                                                               |
| Weekend       | The same shape, for Saturday, Sunday, and (by default) holidays.                                                                      |
| Annual cost   | One `$/kW-yr` value, already in the dollar year the output should be in. The hourly costs sum to it.                                  |
| Year          | The year stamped on the output timestamps.                                                                                            |
| Calendar year | The calendar that classifies weekdays, weekends, and holidays. Defaults to `year`. The two years must both be leap years or both not. |
| Holidays      | When true (the default), federal holidays take the weekend table. When false, only Saturday and Sunday do.                            |

Columns by table kind:

| Table kind    | Required columns                                   | What the annual cost does                                                                                         |
| ------------- | -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `rates`       | `month`, `hour`, `mc_total_per_kwh`                | Scale target only. The table is already `$/kWh`.                                                                  |
| `probability` | `month`, `hour`, `num_days`, `probability_of_peak` | Each cell is first `annual cost × probability of peak / num_days`, then scaled. `num_days` is the table's column. |

The last argument, `output_column`, only names the cost column (default `mc_total_per_kwh`).

No load curve. This script builds the 8,760 itself from `year` and `calendar_year`.

Everything else is the caller's job: choosing and reading the tables, choosing and dollar-year-adjusting the `$/kW-yr`, validating the result, and writing outputs and provenance.

---

## 2. Formula

Let $C$ be the annual cost in `$/kW-yr`.

1. **Rates from a probability table.** Skipped when `table_kind` is `rates`. For a probability table,

$$P_{m,h} = C \times \frac{\mathrm{PoP}_{m,h}}{D_{m,h}}$$

where $D_{m,h}$ is that row's `num_days`. $D$ is the count stored on the published table. It is not counted from the target calendar.

2. **Day type.** Saturday and Sunday take the weekend table. With holidays included, so do the federal holidays in 5 U.S.C. § 6103. A fixed-date holiday on Saturday is observed the Friday before, and one on Sunday the Monday after, so December 31 is included when the next January 1 is a Saturday. Juneteenth is included from 2021. With holidays excluded, a holiday on Monday through Friday stays on the weekday table.

3. **Lookup.** Hour $h$ of the CAIRO 8760 takes $P$ from its month and hour on the table its day type selects. `calendar_year` chooses the weekdays. `year` is stamped on the timestamps by position: row $i$ of the calendar year's clock becomes row $i$ of `year`. December 31 is dropped in a leap year.

4. **Scale.** Multiply every hour, including the zeros, by one factor:

$$\mathrm{scale} = \frac{C}{\sum_h P_h}, \qquad MC_h = \mathrm{scale} \times P_h$$

The ratio between any two hours is unchanged, and a zero hour stays zero. The scale exists because the table's own day counts need not match the target calendar. When they do, the factor is 1.

$MC_h$ is dollars per kW of demand in hour $h$. A customer who uses 1 kWh in that hour is charged $MC_h$ dollars, so the column reads as `$/kWh`.

`expand_month_hour_to_8760` is steps 2 and 3. `scale_8760_to_annual` is step 4. `month_hour_costs_from_probability` is step 1. `month_hour` is all of the steps that apply. `pop_kde` calls the expander with cells it just computed and skips the scale: its day counts are taken from the same calendar, so the sum is already $C$.

---

## 3. Properties

- **Recovers the annual cost exactly.** After the scale, $\sum_h MC_h = C$, so a flat 1 kW load pays $C$ over the year.
- **Keeps the table's shape.** One factor multiplies every hour. Zeros stay zero.
- **Builds its own clock.** `calendar_year` chooses the weekdays. `year` is stamped on the timestamps. A leap year paired with a non-leap year raises, because the rows would shift after February.
- **Depends on the published day counts only in the probability path.** Those counts set the `$/kWh` before the lookup. The following scale removes a mismatch with the target calendar. The `rates` path never reads `num_days`.
- **Raises** when `table_kind` is unknown, a table is missing columns or is not 288 unique month-hours, a probability is outside 0–1 or a day count is not positive, the two years disagree on leap days, the lookup misses an hour, or the unscaled 8760 sums to zero while $C$ is not zero.

---

## 4. What it is not

Other allocations give different hourly series and live in different code:

- **KDE probability of peak** fits the probability from one or more years of load, divides by the target calendar's own day counts, and does not scale. See [probability_of_peak_kde.md](probability_of_peak_kde.md).
- **Top-N load-share** puts the annual cost on the N highest hours of one load year, each in proportion to its load. See [top_n_load_share_allocation.md](top_n_load_share_allocation.md).
- **Top-N exceedance** weights those same hours by load above a threshold. See [top_n_exceedance_allocation.md](top_n_exceedance_allocation.md).
- **Seasonal SCR exceedance** splits the year into summer and winter before that exceedance weighting. See [seasonal_scr_exceedance_allocation.md](seasonal_scr_exceedance_allocation.md).

---

## 5. Running it directly

From Python, on tables that are already `$/kWh`:

```python
from utils.data_prep.marginal_costs.methods.month_hour import month_hour

hourly = month_hour(
    weekday,
    weekend,
    annual_cost_kw_year=21.22,
    year=2025,
    table_kind="rates",
)
```

`table_kind="probability"` reads `num_days` and `probability_of_peak` instead of `mc_total_per_kwh`. `calendar_year` defaults to `year`. `include_holidays` defaults to true. `output_column` renames the cost column.

From the command line, on local parquets:

```bash
uv run python utils/data_prep/marginal_costs/methods/month_hour.py \
  --table-kind rates \
  --path-weekday weekday.parquet \
  --path-weekend weekend.parquet \
  --annual-cost-kw-year 21.22 \
  --year 2025 \
  --path-output allocated.parquet
```

`--table-kind probability` is the same command on probability parquets. `--calendar-year` overrides the day-type calendar. `--include-holidays false` leaves Monday–Friday holidays on the weekday table. The CLI reads and writes local parquet only; it does not read S3, adjust dollar years, or write a manifest.

Tests: `tests/test_month_hour.py`.

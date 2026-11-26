# Seasonal SCR exceedance allocation

How one annual `$/kW-yr` marginal cost becomes an hourly cost series: the year is split into summer and winter, the cost is split between those seasons, and each season's share goes on that season's N highest-load hours in proportion to how far each hour sits above the season's threshold. Every other hour is zero. The code is `seasonal_scr_exceedance` in [`utils/data_prep/marginal_costs/methods/seasonal_scr_exceedance.py`](../../../utils/data_prep/marginal_costs/methods/seasonal_scr_exceedance.py).

This document describes the method and the script only. Which load curve, which `$/kW-yr`, which N, and which winter months a given run uses are documented with that run's marginal-cost component.

---

## 1. Inputs

The method has no knowledge of where its inputs come from. It needs four:

| Input         | What the caller passes                                                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Load curve    | One row per hour with `timestamp` and `load_mw`, already on the output clock and stamped with the output year. Any hourly load works. The units do not matter (§3). |
| Annual cost   | One `$/kW-yr` value, already in the dollar year the output should be in. The hourly costs sum to it.                                                                |
| N per season  | How many of the highest-load hours in summer, and again in winter, are eligible for cost.                                                                           |
| Winter months | Which months are winter, numbered 1–12. Summer is every other month.                                                                                                |

The fifth argument, `output_column`, only names the cost column (default `mc_total_per_kwh`).

Everything else is the caller's job: choosing and reading the load, aligning it to the output clock, remapping a different load year onto the output year, choosing the `$/kW-yr`, deciding how many such calls to add together, validating the result, and writing outputs and provenance.

One call allocates one load and one scalar. A utility whose signal is a blend of several loads is several calls, summed by the caller.

---

## 2. Formula

Let the year be split into summer and winter by the winter-month list. For each season, let $S$ be the $N$ hours with the highest load, $P$ the season's peak load, and $T$ the season's threshold. $T$ is the $(N+1)$th-highest load in the season: the minimum of the top $N+1$ loads. When the season has exactly $N$ hours, $T$ is the lowest of those $N$.

Let $\tau = \min(T_{\text{summer}}, T_{\text{winter}})$ and $C$ the annual cost in `$/kW-yr`.

$$\phi_{\text{summer}} = \frac{P_{\text{summer}} - \tau}{(P_{\text{summer}} - \tau) + (P_{\text{winter}} - \tau)}$$

$$\phi_{\text{winter}} = 1 - \phi_{\text{summer}}$$

$$e_h = \max(L_h - T_{\text{season}}, 0) \quad \text{for } h \in S, \qquad e_h = 0 \quad \text{otherwise}$$

$$w_h = \frac{e_h}{\sum_{k \in S} e_k} \, \phi_{\text{season}}$$

$$MC_h = C \, w_h$$

Step by step:

1. Label each hour summer or winter from its month.
2. In each season, rank hours by load, highest first, and keep $N$ rows. Those are the eligible hours.
3. Set each season's threshold to the $(N+1)$th-highest load in that season.
4. Give each season a share of the annual cost from how far its peak sits above the lower of the two thresholds. The two shares sum to 1.
5. Inside a season, subtract that season's own threshold from each eligible hour's load, drop anything below zero, and divide by the sum of those exceedances.
6. Multiply that within-season weight by the season's share, then by $C$.
7. Give every other hour 0, and return every input hour sorted by timestamp.

$MC_h$ is dollars per kW of demand in hour $h$. A customer who uses 1 kWh in that hour is charged $MC_h$ dollars, so the column reads as `$/kWh`.

An eligible hour whose load equals its season's threshold has exceedance 0, so its weight is 0. That is how a tie at the cutoff can leave a season with fewer than $N$ hours with a positive cost. Exactly $N$ rows in the season stay eligible. Hours that tie the cutoff and are not in those $N$ rows stay at zero.

---

## 3. Properties

- **Recovers the annual cost exactly.** $\sum_h MC_h = C$, so a flat 1 kW load pays $C$ over the year. The winter share is defined as one minus the summer share, so the two seasons together exhaust $C$.
- **At most 2N nonzero hours.** All $2N$ are nonzero when every eligible hour sits strictly above its season's threshold.
- **Invariant to the units of the load.** Multiplying every load by a positive constant multiplies peaks, thresholds, surpluses, and exceedances by that constant, and the weights do not change. Adding the same constant to every load leaves the weights unchanged, because every peak and every threshold moves by that constant.
- **Puts a season's cost on that season's highest hours, then on the hours furthest above the threshold.** The eligible hour nearest the threshold gets almost nothing. The hour just outside the eligible set gets 0. A season whose peak barely clears the lower threshold gets a small share of $C$, even if its own hours sit well above its own threshold.
- **Follows the months the caller names.** Moving a month from summer to winter changes which hours compete for that season's $N$ slots. It does not change the formula.
- **Picks rows, not timestamps.** If a load curve has a repeated timestamp, only the rows that rank in the eligible set get cost. If several hours tie at the $N$th load, exactly $N$ rows are eligible, and which of the tied rows wins is not specified.
- **Raises** when $N < 1$, a required column is missing, `winter_months` is empty, outside 1–12, or contains every month, a season has fewer than $N$ rows, a season's peak or threshold is null, the two seasonal surpluses sum to zero or less, or a season's eligible exceedance sums to zero or less.

---

## 4. What it is not

Other peak-hour allocations give different hourly series and live in different code:

- **Top-N load-share** weights each of one window's top $N$ hours by its load, with no threshold and no seasonal split. See [top_n_load_share_allocation.md](top_n_load_share_allocation.md).
- **Top-N exceedance** weights one window of top $N$ hours by load above a threshold. The whole annual cost goes on that one window. Its threshold is the highest load strictly below the $N$th hour, so a tie at the cutoff still leaves those eligible hours with a positive weight. See [top_n_exceedance_allocation.md](top_n_exceedance_allocation.md).
- **KDE probability of peak** builds a month × day-type × hour probability from several years of load. See [probability_of_peak_kde.md](probability_of_peak_kde.md).
- **Month-hour expansion** looks up a published month-hour table and scales the 8760 so it sums to the annual cost. It uses no load curve. See [month_hour_allocation.md](month_hour_allocation.md).

---

## 5. Running it directly

From Python:

```python
from utils.data_prep.marginal_costs.methods.seasonal_scr_exceedance import (
    seasonal_scr_exceedance,
)

hourly = seasonal_scr_exceedance(
    load,
    annual_cost_kw_year=50.0,
    n_hours_per_season=40,
    winter_months=[10, 11, 12, 1, 2, 3],
)
```

From the command line, on a local parquet with `timestamp` and `load_mw`:

```bash
uv run python utils/data_prep/marginal_costs/methods/seasonal_scr_exceedance.py \
  --path-load load.parquet \
  --annual-cost-kw-year 50.0 \
  --n-hours-per-season 40 \
  --winter-months 10,11,12,1,2,3 \
  --path-output allocated.parquet
```

`--output-column` renames the cost column. The CLI reads and writes local parquet only; it does not read S3, adjust dollar years, or write a manifest.

Tests: `tests/test_seasonal_scr_exceedance.py`.

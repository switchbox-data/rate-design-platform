# Top N exceedance allocation

How one annual `$/kW-yr` marginal cost becomes an hourly cost series: the cost goes on the N highest-load hours, each in proportion to how far its load sits above a threshold, and every other hour is zero. The code is `top_n_exceedance` in [`utils/data_prep/marginal_costs/methods/top_n_exceedance.py`](../../../utils/data_prep/marginal_costs/methods/top_n_exceedance.py).

This document describes the method and the script only. Which load curve, which `$/kW-yr`, and which N a given run uses are documented with that run's marginal-cost component.

---

## 1. Inputs

The method has no knowledge of where its inputs come from. It needs three:

| Input       | What the caller passes                                                                                                                                              |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Load curve  | One row per hour with `timestamp` and `load_mw`, already on the output clock and stamped with the output year. Any hourly load works. The units do not matter (§3). |
| Annual cost | One `$/kW-yr` value, already in the dollar year the output should be in. The hourly costs sum to it.                                                                |
| N           | How many of the highest-load hours are eligible for cost.                                                                                                           |

The fourth argument, `output_column`, only names the cost column (default `mc_total_per_kwh`).

Everything else is the caller's job: choosing and reading the load, aligning it to the output clock, remapping a different load year onto the output year, choosing and dollar-year-adjusting the `$/kW-yr`, validating the result, and writing outputs and provenance.

---

## 2. Formula

Let $L_h$ be the load in hour $h$, $L_{(N)}$ the load of the $N$th-highest hour, and $C$ the annual cost in `$/kW-yr`. The threshold $T$ is the highest load strictly below $L_{(N)}$. When no hour is strictly below $L_{(N)}$, $T = 0$. Let $S$ be the $N$ hours with the highest load.

$$e_h = L_h - T \quad \text{for } h \in S, \qquad e_h = 0 \quad \text{otherwise}$$

$$w_h = \frac{e_h}{\sum_{k \in S} e_k}$$

$$MC_h = C \, w_h$$

Step by step:

1. Rank every hour by load, highest first.
2. Keep $N$ rows. Those are the eligible hours.
3. Set the threshold to the highest load strictly below the $N$th of those rows. If every other hour is at least that high, the threshold is 0.
4. Subtract the threshold from each eligible hour's load. Divide by the sum of those exceedances. These weights sum to 1.
5. Multiply each weight by $C$.
6. Give every other hour 0, and return every input hour sorted by timestamp.

$MC_h$ is dollars per kW of demand in hour $h$. A customer who uses 1 kWh in that hour is charged $MC_h$ dollars, so the column reads as `$/kWh`.

An eligible hour whose load equals the threshold has exceedance 0, so its weight is 0. That is how a tie at the cutoff can leave fewer than $N$ hours with a positive cost: the $N$th load is 0, nothing sits strictly below it, the threshold falls back to 0, and the eligible hours that are also 0 drop out. When the $N$th hour ties with hours below it, those extra hours stay at zero. Exactly $N$ rows stay eligible, the threshold is the highest load under that tied value, and each of the $N$ rows has a positive weight.

---

## 3. Properties

- **Recovers the annual cost exactly.** $\sum_h MC_h = C$, so a flat 1 kW load pays $C$ over the year.
- **At most N nonzero hours.** All $N$ are nonzero when every eligible hour sits strictly above the threshold.
- **Invariant to the units of the load.** Multiplying every load by a positive constant multiplies exceedance by the same constant, and the weights do not change. Adding the same constant to every load also leaves the weights unchanged whenever some hour sits strictly below the $N$th, because the threshold moves with the loads.
- **Puts more of the cost on the very top hours** than weighting those same hours by load. The eligible hour nearest the threshold gets almost nothing. The hour just outside the eligible set gets 0.
- **Picks rows, not timestamps.** If a load curve has a repeated timestamp, only the rows that rank in the eligible set get cost. If several hours tie at the $N$th load, exactly $N$ rows are eligible, and which of the tied rows wins is not specified.
- **Raises** when $N < 1$, a required column is missing, the load has fewer than $N$ rows, the $N$th-highest load is null, or the eligible exceedance sums to zero or less.

---

## 4. What it is not

Other peak-hour allocations give different hourly series and live in different code:

- **Top-N load-share** weights each of the top $N$ hours by its load, with no threshold. The $N$th hour keeps a large share. See [top_n_load_share_allocation.md](top_n_load_share_allocation.md).
- **Seasonal SCR exceedance** does this exceedance weighting inside each of two seasons, after splitting the annual cost between summer and winter. See [seasonal_scr_exceedance_allocation.md](seasonal_scr_exceedance_allocation.md).
- **KDE probability of peak** builds a month × day-type × hour probability from several years of load. See [probability_of_peak_kde.md](probability_of_peak_kde.md).

---

## 5. Running it directly

From Python:

```python
from utils.data_prep.marginal_costs.methods.top_n_exceedance import top_n_exceedance

hourly = top_n_exceedance(load, annual_cost_kw_year=69.0, n_hours=100)
```

From the command line, on a local parquet with `timestamp` and `load_mw`:

```bash
uv run python utils/data_prep/marginal_costs/methods/top_n_exceedance.py \
  --path-load load.parquet \
  --annual-cost-kw-year 69.0 \
  --n-hours 100 \
  --path-output allocated.parquet
```

`--output-column` renames the cost column. The CLI reads and writes local parquet only; it does not read S3, adjust dollar years, or write a manifest.

Tests: `tests/test_top_n_exceedance.py`.

# Peak N load hours allocation (top-N load-share)

How one annual `$/kW-yr` marginal cost becomes an hourly cost series: the cost goes on the N highest-load hours, each in proportion to its load, and every other hour is zero. The code is `top_n_load_share` in [`utils/data_prep/marginal_costs/methods/top_n_load_share.py`](../../../utils/data_prep/marginal_costs/methods/top_n_load_share.py).

This document describes the method and the script only. Which load curve, which `$/kW-yr`, and which N a given run uses are documented with that run's marginal-cost component.

---

## 1. Inputs

The method has no knowledge of where its inputs come from. It needs three:

| Input       | What the caller passes                                                                                                                                              |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Load curve  | One row per hour with `timestamp` and `load_mw`, already on the output clock and stamped with the output year. Any hourly load works. The units do not matter (§3). |
| Annual cost | One `$/kW-yr` value, already in the dollar year the output should be in. The hourly costs sum to it.                                                                |
| N           | How many of the highest-load hours receive cost.                                                                                                                    |

The fourth argument, `output_column`, only names the cost column (default `mc_total_per_kwh`).

Everything else is the caller's job: choosing and reading the load, aligning it to the output clock, remapping a different load year onto the output year, choosing and dollar-year-adjusting the `$/kW-yr`, validating the result, and writing outputs and provenance.

---

## 2. Formula

Let $L_h$ be the load in hour $h$, $T_N$ the set of the $N$ hours with the highest load, and $C$ the annual cost in `$/kW-yr`.

$$w_h = \frac{L_h}{\sum_{k \in T_N} L_k} \quad \text{for } h \in T_N, \qquad w_h = 0 \quad \text{otherwise}$$

$$MC_h = C \, w_h$$

Step by step:

1. Rank every hour by load, highest first.
2. Keep the top $N$.
3. Divide each kept hour's load by the sum of load over those $N$ hours. These weights sum to 1.
4. Multiply each weight by $C$.
5. Give every other hour 0, and return every input hour sorted by timestamp.

$MC_h$ is dollars per kW of demand in hour $h$. A customer who uses 1 kWh in that hour is charged $MC_h$ dollars, so the column reads as `$/kWh`.

---

## 3. Properties

- **Recovers the annual cost exactly.** $\sum_h MC_h = C$, so a flat 1 kW load pays $C$ over the year.
- **Exactly N nonzero hours** when load is positive.
- **Depends only on the shape of the top N hours.** Multiplying the load by any constant leaves the output unchanged, so MW versus kW, or a load that is a fixed share of another load, gives the same result.
- **Fairly flat inside the window, then a step to zero.** The $N$th hour gets $L_N / L_1$ of the peak hour's cost, and hour $N+1$ gets 0.
- **Picks rows, not timestamps.** If a load curve has a repeated timestamp, only the rows that rank in the top N get cost. If several hours tie at the $N$th load, exactly N rows still get cost, and which of the tied rows wins is not specified.
- **Raises** when $N < 1$, a required column is missing, the load has fewer than $N$ rows, or the top-N load sums to zero or less.

---

## 4. What it is not

Other peak-hour allocations give different hourly series and live in different code:

- **Top-N exceedance** weights each top-N hour by its load minus the highest load below the $N$th hour. It puts much more weight on the very top hours, and the $N$th hour gets almost nothing. See [top_n_exceedance_allocation.md](top_n_exceedance_allocation.md).
- **Seasonal SCR exceedance** splits the year into summer and winter, marks the top $N$ hours of each, and splits the annual cost between the seasons before weighting hours by exceedance. See [seasonal_scr_exceedance_allocation.md](seasonal_scr_exceedance_allocation.md).
- **KDE probability of peak** builds a month × day-type × hour probability from several years of load. See [probability_of_peak_kde.md](probability_of_peak_kde.md).

Older docs and CLI flags sometimes call this method "PoP" (`--method pop`) or "PCAF load-share". Those names mean the formula in §2, not the KDE probability of peak.

---

## 5. Running it directly

From Python:

```python
from utils.data_prep.marginal_costs.methods.top_n_load_share import top_n_load_share

hourly = top_n_load_share(load, annual_cost_kw_year=32.0, n_hours=100)
```

From the command line, on a local parquet with `timestamp` and `load_mw`:

```bash
uv run python utils/data_prep/marginal_costs/methods/top_n_load_share.py \
  --path-load load.parquet \
  --annual-cost-kw-year 32.0 \
  --n-hours 100 \
  --path-output allocated.parquet
```

`--output-column` renames the cost column. The CLI reads and writes local parquet only; it does not read S3, adjust dollar years, or write a manifest.

Tests: `tests/test_top_n_load_share.py`.

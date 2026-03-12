# TOU window width optimization

Automated selection of the optimal TOU window width (number of on-peak hours per day, $N$) for each utility. Implemented in `utils/pre/derive_seasonal_tou_window.py`, invoked via the `sweep-tou-window` Justfile recipe.

## Theoretical foundation

The optimization derives from a quadratic welfare-loss approximation. The system welfare loss from replacing hourly marginal-cost pricing with a two-period TOU structure is, for the HP class:

$$
\mathcal{L}^{HP} = \sum_{t \in \mathcal{T}} Q_t^{HP} \left( MC_t - p_t^{HP} \right)^2
$$

where $MC_t$ is the system marginal cost at hour $t$, $p_t^{HP}$ is the TOU price the HP class faces at hour $t$, and $Q_t^{HP}$ is HP-class demand at hour $t$.

### Why HP demand is the correct weight

When designing a tariff for the HP class specifically, the welfare objective separates into HP and non-HP terms:

$$
\mathcal{L} = \sum_t Q_t^{HP}(MC_t - p_t^{HP})^2 + \sum_{c \neq HP} \sum_t Q_t^c (MC_t - p_t^c)^2
$$

Taking the derivative with respect to $p_t^{HP}$, all non-HP terms drop out because they do not depend on the HP price. The HP tariff design problem therefore simplifies to minimizing:

$$
\min_{\{p_t^{HP}\}} \sum_t Q_t^{HP}(MC_t - p_t^{HP})^2
$$

The weight is the quantity of the class actually affected by the tariff — HP demand, not total system or residential demand.

### Optimal TOU prices

Restricting $p_t^{HP}$ to a two-period TOU form (on-peak price $p_{on}$ and off-peak price $p_{off}$), the first-order conditions give:

$$
p_{on}^{HP,*} = \frac{\sum_{t \in \mathcal{P}} Q_t^{HP} \, MC_t}{\sum_{t \in \mathcal{P}} Q_t^{HP}}, \qquad
p_{off}^{HP,*} = \frac{\sum_{t \in \mathcal{O}} Q_t^{HP} \, MC_t}{\sum_{t \in \mathcal{O}} Q_t^{HP}}
$$

Each TOU price equals the HP-demand-weighted average marginal cost in that period — exactly the demand-weighted averages described in `context/domain/cost_reflective_tou_rate_design.md`, restricted to HP loads.

### Window placement (fixed $N$)

For a fixed window length $N$, the optimal contiguous block starting at hour $s$ minimizes the residual welfare loss. Define:

$$
A_{on}(s,N) = \sum_{t \in \mathcal{P}(s,N)} Q_t^{HP}, \quad
B_{on}(s,N) = \sum_{t \in \mathcal{P}(s,N)} Q_t^{HP} \, MC_t
$$

and similarly for off-peak. Substituting optimal prices into the welfare loss and applying the weighted least-squares identity:

$$
W^{HP,*}(s,N) = \sum_t Q_t^{HP} \, MC_t^2 - \frac{B_{on}^2}{A_{on}} - \frac{B_{off}^2}{A_{off}}
$$

The first term is constant in $s$, so minimizing welfare loss is equivalent to maximizing:

$$
\frac{B_{on}(s,N)^2}{A_{on}(s,N)} + \frac{B_{off}(s,N)^2}{A_{off}(s,N)}
$$

This is what `find_tou_peak_window` implements via a sliding-window search.

### Choosing the optimal $N$

For each candidate $N \in \{1, \ldots, 23\}$, compute:

$$
V^{HP}(N) = \min_s W^{HP,*}(s,N)
$$

The optimal window length is:

$$
N^* \in \arg\min_N \, V^{HP}(N)
$$

This is what `sweep_tou_window_hours` implements: for each $N$, it finds the best window placement, computes the welfare-loss metric, and picks the $N$ with the lowest total metric summed across winter and summer seasons.

## Implementation

### Metric

`compute_tou_fit_metric(combined_mc, hourly_load, peak_hours)` computes the load-weighted sum of squared MC residuals:

$$
\text{metric}(N, \text{season}) = \sum_h \left( MC_h - \overline{MC}_{\text{period}(h)} \right)^2 L_h
$$

where $\overline{MC}_{\text{period}(h)}$ is the HP-demand-weighted average MC for hour $h$'s assigned period (peak or off-peak) and $L_h$ is HP load. The total metric sums winter and summer, naturally weighting each season by its load volume.

### Sweep

`sweep_tou_window_hours(combined_mc, hourly_load, seasons)` iterates $N = 1 \ldots 23$. For each $N$ and each season, it:

1. Calls `find_tou_peak_window` to find the best contiguous $N$-hour block
2. Calls `compute_tou_cost_causation_ratio` to compute the peak/off-peak ratio
3. Calls `compute_tou_fit_metric` to evaluate the fit

Results are returned sorted by total metric (ascending), so `results[0]` is the optimum.

### Seasonal handling

The same $N$ is enforced for both winter and summer. The metric sums across seasons, so a candidate $N$ that is good for winter but poor for summer will lose to a candidate that balances both. Within each season, the peak window placement is independent — summer and winter may have different peak hours for the same $N$.

### Load filtering

HP-only load filtering is applied consistently across all three places that compute TOU cost-causation ratios:

1. **Window sweep** (`derive_seasonal_tou_window.py`): `--has-hp true` (default)
2. **Runtime TOU derivation** (`derive_seasonal_tou.py`): `--has-hp true` (default)
3. **Demand flex Phase 1.75** (`demand_flex.py`): filters to TOU-assigned building IDs (= HP customers in our scenario structure)

The filter accepts `true` (HP only), `false` (non-HP only), or `all` (no filter).

Building metadata comes from `utility_assignment.parquet`, which provides `bldg_id`, `weight`, `sb.electric_utility`, and `postprocess_group.has_hp`. The `load_tou_inputs` function (shared by the sweep and runtime derivation) rescales weights to the EIA residential customer count **before** applying the HP filter, so that HP buildings' weights represent their actual share of the residential population, not the entire class. It then filters by `has_hp` and loads the corresponding ResStock building load curves.

### Output

- **Sweep CSV**: Written to `utils/pre/tou_window/<utility>_tou_window_sweep.csv` (default) with columns: `window_hours`, `peak_hours_<season>`, `ratio_<season>`, `metric_<season>`, `metric_total`.
- **periods YAML**: The optimal `tou_window_hours` is written to `rate_design/hp_rates/<state>/config/periods/<utility>.yaml` (unless `--no-write-yaml`). This is the same file `derive_seasonal_tou.py` reads at runtime, so no runtime changes are needed.
- **Log output**: A formatted table of all 23 candidates (sorted by metric) and a summary box with the optimal $N$, peak hours, ratios, and the runner-up.

## CLI

```bash
uv run python -m utils.pre.derive_seasonal_tou_window \
  --path-supply-energy-mc <path> \
  --path-supply-capacity-mc <path> \
  --state NY --utility coned --year 2025 \
  --path-dist-and-sub-tx-mc <path> \
  --path-utility-assignment <path> \
  --resstock-base <path> \
  --path-electric-utility-stats <path> \
  --has-hp true \
  --output-dir <path>
```

Optional flags: `--winter-months`, `--periods-yaml`, `--path-bulk-tx-mc` (NY), `--run-dir` (restrict to CAIRO output building set), `--no-write-yaml`, `--upgrade`.

## Justfile recipes

From `rate_design/hp_rates/<state>/`:

```bash
just s <state> sweep-tou-window                  # single utility (from UTILITY env)
just s <state> sweep-tou-window-all               # all utilities in the state
```

Both recipes accept optional overrides: `run_dir`, `output_dir`, `no_write_yaml`, `has_hp`.

## NY results (March 2026)

HP-only sweep across all 7 NY utilities:

| Utility | Optimal $N$ | Winter peak hours  | Summer peak hours  | Winter ratio | Summer ratio |
| ------- | ----------- | ------------------ | ------------------ | ------------ | ------------ |
| cenhud  | 5           | 16, 17, 18, 19, 20 | 15, 16, 17, 18, 19 | 1.57         | 3.04         |
| coned   | 3           | 16, 17, 18         | 15, 16, 17         | 1.99         | 4.28         |
| nimo    | 3           | 17, 18, 19         | 17, 18, 19         | 1.75         | 2.91         |
| nyseg   | 3           | 17, 18, 19         | 17, 18, 19         | 1.75         | 2.93         |
| or      | 5           | 16, 17, 18, 19, 20 | 15, 16, 17, 18, 19 | 1.57         | 3.12         |
| psegli  | 5           | 16, 17, 18, 19, 20 | 15, 16, 17, 18, 19 | 1.67         | 3.80         |
| rge     | 3           | 17, 18, 19         | 17, 18, 19         | 1.75         | 2.87         |

Most utilities land on 3 hours; CenHud, O&R, and PSEG-LI on 5. Summer ratios are consistently higher than winter (driven by generation capacity scarcity and cooling load concentration). All peak windows cluster in the late afternoon / early evening.

## When to run

The sweep is a manual pre-processing step, not part of `all-pre`. Its output (`tou_window_hours` in `periods.yaml`) persists across batches, so it only needs to be re-run when the inputs change. It does not depend on any CAIRO outputs.

**Re-run the sweep when any of these change:**

- **Marginal cost data** — new Cambium release (supply energy or capacity MCs), updated distribution or sub-TX MCs from MCOS studies, new or revised bulk TX MCs
- **ResStock loads** — new ResStock release, updated load curves, changes to non-HP load approximation
- **Load filtering** — changes to HP identification, utility assignment, or the `has_hp` filter setting

**Prerequisites:** ResStock data preparation must be complete (needs `utility_assignment.parquet` and load curves on local disk; see `context/tools/resstock_data_preparation_run_order.md`). MC data must be generated (distribution/sub-TX, supply energy/capacity, and optionally bulk TX).

**Run order relative to CAIRO batches:**

```text
ResStock data prep (steps 1-5)  ──┐
MC data generation                ├──> sweep-tou-window-all ──> all-pre ──> run-all-sequential
                                  │    (writes periods.yaml)    (runs 9/10 read periods.yaml
                                  │                              via create-seasonal-tou)
```

The sweep writes `tou_window_hours` to `periods.yaml`. At runtime, `create-seasonal-tou` (called inside `run-9` and `run-10`) reads that value. If the sweep has not been run, `derive_seasonal_tou.py` falls back to a default of 4 hours.

## Relationship to other modules

- **`compute_tou.py`** — Provides the core primitives (`find_tou_peak_window`, `compute_tou_cost_causation_ratio`) that the sweep calls per-candidate. Unchanged by the window optimization work.
- **`derive_seasonal_tou.py`** — Runtime TOU derivation. Reads `tou_window_hours` from `periods.yaml` (written by the sweep) and calls the same `compute_tou.py` functions. Shares `load_tou_inputs` with the sweep script.
- **`cost_reflective_tou_rate_design.md`** — Theoretical background on demand-weighted averages, cost-causation ratios, and the trade-offs of window width. The sweep operationalizes the "choosing the window width" section of that document.

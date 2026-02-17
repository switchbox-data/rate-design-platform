# MC-driven TOU tariff derivation (`compute_tou`)

## What it does

`utils/pre/compute_tou.py` derives a two-period Time-of-Use (TOU) electricity tariff directly from marginal cost data and system load, rather than relying on a manually authored tariff JSON. The peak/off-peak price differential reflects cost causation—the ratio of demand-weighted marginal costs during peak versus off-peak hours.

## Why

Static TOU tariffs embed assumptions about when system costs are highest. By computing the TOU structure from the same Cambium + distribution marginal costs that feed the Bill Alignment Test, the tariff automatically reflects the cost reality of the modeled year and region. This is the first step toward demand-flexibility analysis: once customers face a cost-causative TOU price signal, CAIRO's demand-response module can model load shifting in response to that signal.

## How it works

### 1. Combine marginal costs

`combine_marginal_costs(bulk_mc, distribution_mc)` aligns the Cambium bulk marginal costs (energy + generation capacity + bulk transmission, a DataFrame with a `total_cost_enduse` column) with the distribution marginal cost Series on their shared hourly time index and sums them into a single `combined_mc` Series ($/kWh).

### 2. Find the peak window

`find_tou_peak_window(combined_mc, hourly_system_load, window_hours=4)` identifies the contiguous block of `window_hours` hours (default 4) with the highest average demand-weighted marginal cost across the 24-hour day.

Steps:

- Build a 24-hour profile by grouping the 8 760-hour combined MC and system load by hour-of-day.
- For each hour-of-day, compute demand-weighted average MC = Σ(MC × load) / Σ(load).
- Slide a window of `window_hours` across the 24-hour circle (wrapping midnight) and pick the window with the highest mean demand-weighted MC.
- Return the sorted list of peak hours (e.g. `[16, 17, 18, 19]`).

### 3. Compute cost-causation ratio

`compute_tou_cost_causation_ratio(combined_mc, hourly_system_load, peak_hours)` calculates:

```
ratio = (demand-weighted avg MC during peak hours) / (demand-weighted avg MC during off-peak hours)
```

This ratio directly sets the peak-to-off-peak price differential.

### 4. Create the URDB v7 tariff

`create_tou_tariff(label, peak_hours, peak_offpeak_ratio, base_rate, fixed_charge, utility)` builds a complete URDB v7 JSON structure with:

- Two energy rate periods: period 0 (off-peak) at `base_rate`, period 1 (peak) at `base_rate × ratio`.
- A 12 × 24 TOU schedule matrix mapping each month/hour to the correct period.
- Fixed charge of $6.75/month (matching RIE A-16).
- Standard URDB fields (`label`, `utility`, `sector`, `source`, etc.).

### 5. Generate tariff map

`generate_tou_tariff_map(customer_metadata, tou_tariff_key, flat_tariff_key)` creates the `bldg_id, tariff_key` CSV mapping:

- Heat pump customers (`postprocess_group.has_hp == True`) → TOU tariff key
- All other customers → flat tariff key

## Integration with `run_scenario.py`

The RI `run_scenario.py` is organized into four phases:

1. **Load data** — customer metadata, building loads, bulk + distribution marginal costs
2. **Derive TOU** (optional) — when `compute_tou.enabled` is set in the YAML, calls the four functions above, writes the tariff JSON and tariff map to `config/tariffs/electric/` and `config/tariff_maps/electric/`, and overrides the settings so downstream phases use them
3. **Initialize tariffs** — `_initialize_tariffs`, precalc mapping, export compensation
4. **Run CAIRO** — `MeetRevenueSufficiencySystemWide.simulate()`

The `compute_tou` block in `scenarios.yaml` controls this:

```yaml
compute_tou:
  enabled: true
  tou_tariff_key: rie_tou_hp       # key for the generated TOU tariff
  flat_tariff_key: rie_a16         # existing flat tariff for non-HP customers
  tou_window_hours: 4              # contiguous peak window size
  tou_base_rate: 0.06              # off-peak $/kWh
  tou_fixed_charge: 6.75           # $/month (matches rie_a16)
```

When `enabled: false` (or absent), the run behaves exactly as before—tariff paths and maps come from the YAML as static file references.

## Standalone CLI

The module can also be run as a CLI for tariff-map generation without running the full scenario:

```bash
uv run python -m utils.pre.compute_tou \
  --metadata-path s3://data.sb/nrel/resstock/res_2024_amy2018_2/metadata \
  --state RI --upgrade-id 00 \
  --tou-tariff-key rie_tou_hp \
  --flat-tariff-key rie_a16 \
  --output-dir rate_design/ri/hp_rates/config/tariff_maps/electric
```

Or via the Justfile:

```bash
just -f rate_design/ri/hp_rates/Justfile map-electric-tou rie_tou_hp rie_a16
```

## Key assumptions and limitations

- **Window is contiguous and wraps midnight.** The peak window is always a single contiguous block (e.g. 4 consecutive hours). It can wrap around midnight (e.g. `[22, 23, 0, 1]`).
- **Uniform annual schedule.** The same peak hours apply to every month (no seasonal variation). This is a simplification; a future version could use seasonal or weekday/weekend schedules.
- **Base rate is an input, not derived from revenue requirement.** The `tou_base_rate` is set in config (default $0.06/kWh). CAIRO's precalc/calibration step will adjust the actual rate to meet the revenue requirement, but the *ratio* between peak and off-peak is fixed by cost causation.
- **HP-only assignment.** Only customers with `postprocess_group.has_hp == True` are assigned the TOU tariff. All others get the flat tariff. This reflects the demand-flex policy goal of giving HP customers a cost-causative price signal.

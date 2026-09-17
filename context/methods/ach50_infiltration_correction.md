# ACH50 infiltration correction

Methodology for correcting ResStock's systematic overestimation of building air leakage (ACH50) and the resulting inflated heating and cooling loads.

## Why we correct

ResStock assigns infiltration rates to each building via LBNL's Residential Diagnostics Database (ResDB). These assignments systematically overestimate ACH50 relative to field measurements, inflating simulated heating and cooling loads. @chan_AnalysisAirLeakage_2013 provide an empirical regression on ~134,000 homes that predicts expected air leakage from housing characteristics (vintage, climate zone, floor area, height, foundation type, duct location). We use their model as a benchmark: if ResStock assigns a home ACH50 = 20 but Chan et al. predicts ACH50 = 11 for that home's characteristics, the difference is likely overestimation.

The correction scales each building's HVAC end-use columns (heating and cooling electric kWh, gas heating therms) by a factor `(1 - frac)`, where `frac` represents the estimated share of load attributable to the overestimated infiltration.

## The Chan et al. (2013) benchmark

Chan et al. fit a log-linear regression on blower-door measurements from ~134,000 US homes (Table 1, $R^2 = 0.683$):

$$
\ln(\text{NL}) = \beta_{\text{year}} + \beta_{\text{cz}} + \beta_{\text{area}} \cdot A + \beta_h \cdot H
$$

where NL is the normalized leakage, $A$ is floor area in m², $H$ is house height in meters, and the $\beta$ coefficients are indicator variables for year-built category and IECC climate zone. Foundation type and duct location add further refinements (fit on residuals of the main model).

We convert NL to ACH50 via:

$$
\text{ACH50} = C \cdot \frac{\text{NL}}{H^{0.7}}
$$

where $C = H_{\text{ref}}^{0.7} / 0.055 \approx 39.3$ (calibrated from Chan Section 5 at $H_{\text{ref}} = 3.0$ m for 1-story homes).

Full regression coefficients are stored in `data/resstock/config/chan_2013_coefficients.yaml`. State-specific vintage mappings, stories-to-height mappings, and foundation/duct mappings live in `data/resstock/config/ach50_correction/<state>.yaml`.

## The within-ResStock WLS regression

Chan gives us a per-building benchmark ACH50 ($\text{ACH50}_{\text{Chan}}$). To translate the ACH50 gap into a heating/cooling kWh adjustment, we need to know how load intensity varies with infiltration _within the ResStock sample_. We fit a weighted least-squares regression for each (heating-type group × end-use):

$$
\frac{\text{kWh}}{\text{sqft}} = a + b \cdot \text{ACH50}
$$

where the weights are ResStock sample weights. The intercept $a$ represents the non-infiltration component of heating/cooling intensity (shell, equipment, climate, occupant behavior), and the slope $b$ is the marginal kWh/sqft per unit ACH50.

### Groups

Buildings are classified into heating-type groups defined in the state YAML (`groups:`). For MD:

| Group               | Condition                                 | End-uses corrected             |
| ------------------- | ----------------------------------------- | ------------------------------ |
| Heat Pump           | `heats_with_electricity` AND `has_hp`     | `elec_heating`, `elec_cooling` |
| Electric Resistance | `heats_with_electricity` AND NOT `has_hp` | `elec_heating`, `elec_cooling` |
| Natural Gas         | `heats_with_natgas`                       | `gas_heating`, `elec_cooling`  |
| Delivered Fuels     | `heats_with_oil` OR `heats_with_propane`  | `elec_heating`, `elec_cooling` |

Each end-use maps to specific hourly consumption columns (e.g., `out.electricity.heating.energy_consumption`, `out.electricity.heating_fans_pumps.energy_consumption`, etc.) and their annual `*.kwh` counterparts.

Buildings flagged as `approximated_hp_load` (their HVAC curves were replaced by nearest-neighbour HP profiles in the `approximate_non_hp_load` pipeline step) are excluded from regression fitting but still receive the correction.

## Computing the correction fraction

Two methods have been implemented. Both use the same Chan benchmark and WLS regression; they differ only in how the per-building fraction is computed from those inputs.

### Voucher method (original, `_sb_ach` release)

The voucher treats the WLS slope $b$ as a fixed kWh/sqft coupon sized for a typical building:

$$
V = b \cdot \text{sqft} \cdot \Delta\text{ACH50}
$$

$$
\text{frac} = \text{clip}\!\left(\frac{V}{E_{\text{annual}}},\; -0.5,\; 1.0\right)
$$

where $\Delta\text{ACH50} = \text{ACH50}_{\text{RS}} - \text{ACH50}_{\text{Chan}}$ and $E_{\text{annual}}$ is the building's actual annual kWh for that end-use group.

**Pathology:** When $V \geq E_{\text{annual}}$, `frac` clips to 1.0 and the entire end-use load is zeroed out. This happens to low-kWh/sqft buildings (often multi-family) whose actual intensity falls well below the regression line — they don't consume enough kWh for the coupon to fit. These are not unusually leaky buildings; their median ACH50 is similar to the population. The zeroing is a mathematical artifact of dividing a group-level coupon by an individual building's (small) annual total.

In the MD upgrade-01 BGE sample, the voucher zeroed out heating for 370 buildings. For ER homes in upgrade 00, 84 homes had their heating eliminated entirely, producing physically impossible COP values above 1.0 for electric resistance.

### Fitted-ratio method (current, `_sb_ach_ratio` release)

The fitted-ratio evaluates the WLS regression line at both ACH50 values and takes the proportional difference:

$$
\hat{e}_{\text{RS}} = a + b \cdot \text{ACH50}_{\text{RS}}
$$

$$
\hat{e}_{\text{Chan}} = a + b \cdot \text{ACH50}_{\text{Chan}}
$$

$$
\text{frac} = \text{clip}\!\left(1 - \frac{\hat{e}_{\text{Chan}}}{\hat{e}_{\text{RS}}},\; -0.5,\; 1.0\right)
$$

This can be rewritten as $\text{frac} = (\hat{e}_{\text{RS}} - \hat{e}_{\text{Chan}}) / \hat{e}_{\text{RS}}$: the gap in predicted intensity divided by the predicted intensity at the ResStock ACH50.

**Key properties:**

- `frac` depends only on $\text{ACH50}_{\text{RS}}$, $\text{ACH50}_{\text{Chan}}$, and the group's WLS coefficients $(a, b)$. It does not use the individual building's actual kWh. Two buildings with the same ACH50 pair get the same percent correction regardless of their actual consumption.
- **On the regression line**, the two methods are algebraically identical: when $E_{\text{annual}} / \text{sqft} = a + b \cdot \text{ACH50}_{\text{RS}}$, the voucher formula reduces to the fitted-ratio formula.
- **Below the line**, the voucher produces a larger `frac` (potentially 1.0); the fitted-ratio produces the same `frac` as for any other building at that ACH50.
- **`frac` cannot reach 1.0** as long as $a > 0$, because $\hat{e}_{\text{Chan}} = a + b \cdot \text{ACH50}_{\text{Chan}} > 0$ whenever the Chan ACH50 is non-negative. With the MD heating regression ($a = 1.37$, $b = 0.11$), `frac = 1` would require $\text{ACH50}_{\text{Chan}} = -a/b \approx -12.5$, which is impossible.
- When $\hat{e}_{\text{RS}} \leq 0$ or $\hat{e}_{\text{Chan}} \leq 0$, the correction is skipped (`frac = 0`).

**Trade-off:** The fitted-ratio may apply slightly larger absolute kWh reductions to high-intensity buildings (above the regression line), since the same percent times a larger base removes more kWh than the voucher would. In the MD BGE upgrade-01 comparison, aggregate heating was ~4% lower under fitted-ratio than the voucher. Whether this is more correct or slightly over-correcting the top end is not decidable without re-simulation, but it is a far smaller concern than eliminating real heating loads from hundreds of homes.

### Empirical comparison (MD BGE, upgrade 01)

| Metric                                          | Voucher (`_sb_ach`)      | Fitted-ratio (`_sb_ach_ratio`)                   |
| ----------------------------------------------- | ------------------------ | ------------------------------------------------ |
| Buildings with heating zeroed (>1 kWh → <1 kWh) | 370                      | 0                                                |
| Max heating `frac`                              | 1.000 (373 homes ≥ 0.99) | 0.695                                            |
| Median per-building heating kWh delta           | —                        | +12 kWh (methods nearly agree for typical homes) |
| Weighted BGE electric total                     | —                        | Ratio is 1.1% below voucher                      |

## Hourly application

The correction is not an hourly infiltration model. It does not re-simulate stack effect or wind-driven leakage at each timestep. EnergyPlus already baked infiltration into the 8760 load shape. We only rescale that existing shape.

Once `frac` is computed (a single scalar per building per end-use group), it is applied uniformly to every hour:

$$
h'_t = (1 - \text{frac}) \cdot h_t
$$

January peak and a 0.1 kWh shoulder hour get the same multiplier. This preserves the load shape (peak timing, seasonal profile) while adjusting the magnitude.

Because the multiply is uniform, annual and hourly application produce the same kWh totals:

$$
\sum_t (1 - \text{frac}) \cdot h_t = (1 - \text{frac}) \cdot \sum_t h_t
$$

After scaling end-use columns, fuel totals (`out.electricity.total`, `out.natural_gas.total`, `out.site_energy.total`) and net columns (`out.electricity.net`) are recomputed from their components.

## What is not corrected

- **Thermal output** (`out.load.*.energy_delivered.kbtu`): neither method adjusts thermal delivery, which means the implicit COP changes after correction. For electric resistance this creates a physical inconsistency (COP > 1.0). Scaling thermal output proportionally would fix this but is out of scope.
- **Oil and propane consumption**: the Delivered Fuels group corrects only electric heating (backup) and electric cooling, not the fuel consumption itself.
- **Non-HVAC loads**: only heating and cooling end-uses are scaled. Other end-uses (lighting, appliances, hot water) are unaffected.

## Implementation

| Component          | Path                                                                          |
| ------------------ | ----------------------------------------------------------------------------- |
| Chan coefficients  | `data/resstock/config/chan_2013_coefficients.yaml`                            |
| State config (MD)  | `data/resstock/config/ach50_correction/md.yaml`                               |
| Correction script  | `data/resstock/load_curve/correct_ach50_infiltration.py`                      |
| Hourly aggregation | `data/resstock/load_curve/aggregate_loads.py`                                 |
| Tests              | `tests/test_correct_ach50_infiltration.py`                                    |
| Report YAMLs       | `rate_design/hp_rates/<state>/config/load_adj/ach50_correction_report_*.yaml` |

### Justfile recipes

From `data/resstock/Justfile`:

- **`correct-ach50 <state> "<upgrade_ids>"`** — Full voucher pipeline (hourly → monthly → annual) into `_sb_ach`. _Legacy; **non-functional on current HEAD** (see "Reproducing a release" below) — kept for historical reference._
- **`correct-ach50-ratio <state> "<upgrade_ids>"`** — Full fitted-ratio pipeline (hourly → monthly → annual) into `_sb_ach_ratio`.
- **`upload-ach50-ratio <state> "<upgrade_ids>"`** — Upload `_sb_ach_ratio` to S3.

### Reproducing a release

Both recipes require the `_sb` release to already exist for the target state/upgrades (built via `just -f data/resstock/Justfile run-pipeline <state> --upgrade-ids <ids>`). If MF buildings need `approximate_non_hp_load`-adjusted profiles reflected in the regression (buildings flagged `approximated_hp_load` are excluded from WLS fitting but still corrected), run that step first.

**Fitted-ratio (current):**

```bash
just -f data/resstock/Justfile correct-ach50-ratio MD "00 01"
just -f data/resstock/Justfile upload-ach50-ratio MD "00 01"
```

This runs the 3-step pipeline (hourly correction → `aggregate_loads.py --add-monthly` → `--add-annual`) into `res_2024_amy2018_2_sb_ach_ratio`, then syncs the requested upgrades to S3. Verify with `just -f data/resstock/Justfile check-integrity <state>` or by diffing schema/row counts against `_sb`.

**Voucher (legacy, historical only):** the voucher formula (`frac = clip(slope × sqft × Δach50 / annual_kwh, ...)`) was replaced by the fitted-ratio implementation in commit `11592921`. The current script only implements fitted-ratio, and `run_correction()` refuses to write to the `_sb_ach` name (`_production_ach_release()` guard) to prevent silently mislabeling fitted-ratio output as the voucher release — **the `correct-ach50` recipe will always fail with this guard on current HEAD.** To regenerate a true voucher release, pull the script and Justfile from the last pre-rewrite commit and run from there:

```bash
git show 311312ec:data/resstock/load_curve/correct_ach50_infiltration.py > /tmp/correct_ach50_voucher.py
git show 311312ec:data/resstock/Justfile > /tmp/Justfile_voucher
# then invoke the script directly with its --input-release/--output-release/... flags
```

### Releases

| Release suffix  | Method       | Status                                                        |
| --------------- | ------------ | ------------------------------------------------------------- |
| `_sb_ach`       | Voucher      | Production (used by CAIRO runs through 2026-09)               |
| `_sb_ach_ratio` | Fitted-ratio | Generated for MD upgrades 00 and 01; pending CAIRO validation |

Both releases are schema-identical: same directory structure (hourly, monthly, annual, metadata, metadata_utility), same column set, same row count.

## References

Chan, W.R., Joh, J., & Sherman, M.H. (2013). Analysis of air leakage measurements of US houses. _Energy and Buildings_, 66, 616–625. doi:[10.1016/j.enbuild.2013.07.047](https://doi.org/10.1016/j.enbuild.2013.07.047)

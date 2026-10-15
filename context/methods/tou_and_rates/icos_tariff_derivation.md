# Incremental cost of service (ICOS) tariff derivation

## Motivation

The ICOS method derives a delivery rate that zeroes the weighted-average **overpayment** for the target population (typically fossil-fuel-heated homes switching to heat pumps). "Overpayment" is the gap between what a customer pays after retrofitting and what they _should_ pay given the incremental cost they impose on the grid:

$$\text{overpayment} = \text{bill}_\text{after} - (\text{bill}_\text{before} + \Delta MC)$$

where:

- $\text{bill}_\text{before} = r_\text{base} \cdot \overline{kWh}_\text{before}$ — the pre-retrofit bill at the existing rate
- $\text{bill}_\text{after} = r_\text{new} \cdot \overline{kWh}_\text{after}$ — the post-retrofit bill at the derived rate
- $\Delta MC = \text{economic\_burden}_\text{after} - \text{economic\_burden}_\text{before}$ — the change in marginal cost (delivery component) from CAIRO's BAT

Setting $\overline{\text{overpayment}} = 0$ and solving for $r_\text{new}$ produces a rate where the average retrofitted customer's bill exactly covers their pre-retrofit bill plus their incremental delivery cost.

## Flat rate

$$r_\text{new} = \frac{r_\text{base} \cdot \overline{kWh}_\text{before} + \overline{\Delta MC}}{\overline{kWh}_\text{after}}$$

A single volumetric rate applied uniformly across all months.

## Seasonal rate

Summer is pinned at $r_\text{base}$ (the existing rate). The annual overpayment constraint is solved for the winter rate:

$$r_\text{winter} = \frac{r_\text{base} \cdot \overline{kWh}_\text{before} + \overline{\Delta MC} - r_\text{base} \cdot \overline{kWh}_\text{after,summer}}{\overline{kWh}_\text{after,winter}}$$

**Proof:** substitute into the annual bill and verify the constraint:

$$\text{bill}_\text{after} = r_\text{base} \cdot \overline{kWh}_\text{after,summer} + r_\text{winter} \cdot \overline{kWh}_\text{after,winter}$$

$$= r_\text{base} \cdot \overline{kWh}_\text{after,summer} + \frac{r_\text{base} \cdot \overline{kWh}_\text{before} + \overline{\Delta MC} - r_\text{base} \cdot \overline{kWh}_\text{after,summer}}{\overline{kWh}_\text{after,winter}} \cdot \overline{kWh}_\text{after,winter}$$

$$= r_\text{base} \cdot \overline{kWh}_\text{before} + \overline{\Delta MC}$$

$$\Rightarrow \overline{\text{overpayment}} = 0 \quad \square$$

Winter months are read from the utility's `periods/{utility}.yaml`, not hardcoded.

## Population

The target population is configurable via `icos.heating_types` in the pipeline YAML. For CT Eversource, this is `[natgas, delivered_fuels]` — all fossil-fuel-heated homes. The formulas above use weighted averages over this population, with weights from CAIRO's `customer_metadata.csv`.

## Data sources

All inputs come from CAIRO output directories of the parent scenario's delivery runs:

| File                                                     | Stage                | Columns used                                             |
| -------------------------------------------------------- | -------------------- | -------------------------------------------------------- |
| `customer_metadata.csv`                                  | precalc (u00)        | `bldg_id`, `weight`, `postprocess_group.heating_type_v2` |
| `cross_subsidization/cross_subsidization_BAT_values.csv` | precalc + calibrated | `bldg_id`, `customer_level_economic_burden`              |
| `billing_kwh_annual.parquet`                             | precalc + calibrated | `bldg_id`, `annual_kwh_grid`                             |
| `billing_kwh_8760.parquet`                               | calibrated only      | `bldg_id`, `timestamp`, `grid_cons_kwh` (seasonal only)  |

## Supply tariff

The supply tariff is constructed by adding a supply component to each delivery rate period:

$$\text{supply\_component} = \overline{r_\text{base,supply}} - \overline{r_\text{base,delivery}}$$

where the averages are simple averages across all periods in the base tariff's `energyratestructure`. For a flat base tariff, this is just the difference between the two rates.

For CT Eversource (Rate 1 Proposed delivery at $0.04531/kWh$, supply at $0.15000/kWh$), the supply component is $0.10469/kWh$.

## Fixed charge

The fixed charge is always preserved from the base tariff, unchanged.

## Relationship to other methods

- **BAT allocation method** (multi-rate quartets): enforces revenue neutrality across subclasses via `compute_subclass_rr` + residual allocation. ICOS does not enforce revenue neutrality — the derived rate is evaluated by a `single_rate_uncalibrated` quartet.
- **Fair default rate** (`context/methods/tou_and_rates/fair_default_rate_design.md`): targets the _static_ cross-subsidy from BAT for a single default tariff serving all customers. ICOS targets the _incremental_ overpayment for retrofitting customers only.

## Pipeline integration

ICOS tariff derivation is triggered when a scenario declares `derive_method: icos` in the pipeline YAML. The pipeline's dependent-scenario loop calls `derive_icos_tariffs()` (from `utils/mid/derive_icos_tariff.py`) instead of the BAT-based `derive_tariffs()`. The derived JSONs are written to the tariff directory before the quartet runs.

See `context/code/orchestration/prefect_pipeline.md` for the pipeline architecture.

### Example YAML

```yaml
default_ry1_icos_fair_flat_uncalibrated:
  quartet: single_rate_uncalibrated
  tariff_base: icos_fair_flat
  depends_on: default_ry1_r1_uncalibrated
  derive_method: icos
  structure: flat
  icos:
    heating_types: [natgas, delivered_fuels]
```

### Config fields

| Field           | Type           | Purpose                                                                                                                         |
| --------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `derive_method` | `str \| None`  | Derivation method; `"icos"` for ICOS. When set, the pipeline calls the appropriate derive function instead of `derive_tariffs`. |
| `structure`     | `str \| None`  | Rate structure: `"flat"` or `"seasonal"`. Mirrors the `structure` field in multi-rate subgroup configs.                         |
| `icos`          | `dict \| None` | ICOS-specific parameters. Currently only `heating_types: list[str]`.                                                            |

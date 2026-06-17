# BGE sub-transmission + distribution marginal cost (Maryland)

How we set the `sub_tx_and_dist` BAT marginal-cost input for Baltimore Gas & Electric (BGE, Maryland
/ PJM). This is the **implementation** note. For the cross-platform choice of LRMC *definition*
(FLIC vs avoided cost), the full taxonomy, the source-number table, and the platform tension, see
[dist_mc_definition_choice.md](dist_mc_definition_choice.md). For the verbatim source citations see
[bge_case9692_dist_mc.md](../../sources/mcos/bge_case9692_dist_mc.md).

---

## 1. Value and approach

BGE's `sub_tx_and_dist` marginal cost is the **Brattle Group avoided distribution cost,
`$32/kW-yr` in 2022 dollars** (MD PSC Case No. 9692, MYP testimony,
[p. 138](https://www.documentcloud.org/documents/28269392-001-2-bge-myp-2-case-direct-testimony-final-w-att/#document/p138/a2821808)).

Unlike NY — where we derive an incremental-diluted FLIC value from per-utility MCOS workbooks — BGE
has no MCOS workbook, but it does publish an avoided-cost scalar. So BGE is handled **RI-style**: a
single published `$/kW-yr` figure in a config CSV, fed to the existing PoP allocator. There is no
local levelization or capital-plan build (the `$32` is already levelized by Brattle with a 12%
economic carrying charge).

This is the **avoided / AIC** definition of LRMC, not FLIC. That is a deliberate choice for BGE
(§3), and it matches what RI does and what the BAT paper's own CPUC ACC input does, while diverging
from NY's FLIC convention — see [dist_mc_definition_choice.md](dist_mc_definition_choice.md) §4 for
that tension.

---

## 2. Config and CPI handling

Stored at `rate_design/hp_rates/md/config/marginal_costs/md_marginal_costs_2025.csv`:

```csv
utility,sub_tx_and_dist_mc_kw_yr,dollar_year
bge,32.0,2022
```

The allocator
([generate_utility_tx_dx_mc.py](../../../utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py))
reads the optional `dollar_year` column and CPI-inflates the value (CPIAUCSL) from 2022 to the target
dollar year (defaults to `--year`). For a 2025 run that is roughly `$32` → `~$36/kW-yr`. No
carrying-charge math is applied on our side — the figure is pre-levelized.

Scope: the `$32` is electric **distribution**, which in BGE's ECOSS **includes 34 kV
sub-transmission** (BGE treats 34 kV / 13 kV / secondary as distribution voltage levels;
FERC transmission, 115 kV+, is excluded — see [bge_case9692_dist_mc.md](../../sources/mcos/bge_case9692_dist_mc.md)
§5). It therefore maps cleanly onto our combined `sub_tx_and_dist` bucket **without overlapping a
separate PJM bulk-transmission bucket**. Do not use the E3 `$203–258` combined-T&D figure here — it
includes FERC transmission and would double-count bulk TX.

---

## 3. Why avoided cost (consistent with BGE's own COS)

BGE's own filings establish that BGE conceives of distribution cost as peak-driven (the avoided / AIC
view), which is why the avoided-cost figure — not a diluted FLIC figure — is the defensible input for
an intervention in BGE's rate case:

- **Demand-classified, NCP-allocated**: BGE classifies 34 kV / 13 kV / secondary plant as
  demand-related and allocates on each class's contribution to NCP kW, because feeders and
  substations are "planned and sized based primarily on substation load center peak demands"
  (O'Neill, p. 15–16).
- **BGE's own marginal numbers use a peak denominator**: Brattle = replacement value ÷ peak
  (`$32`); E3 = capital ÷ peak *change* (`$203–258`). Neither uses capital-over-system-peak (FLIC).
- **Lumpy load growth**: BGE faces large, capacity-driving additions (e.g. a 400 MW data-center
  substation). The LRMC methodology doc
  ([§9](../bat_mc_residual/bat_lrmc_residual_allocation_methodology.md)) warns FLIC's steady-state
  assumption is weakest under exactly these structural transitions, where the per-kW-of-peak (AIC)
  view is the meaningful marginal cost.

---

## 4. Intervention hook

The cross-subsidy argument for the rate case does **not** rest on a small marginal cost. It rests on
BGE's own admission that it over-recovers peak-driven (largely fixed) distribution cost through
volumetric charges. Witness Fiery (p. 15–16):

> "…a volumetric component which currently recovers a significant amount of the distribution portion
> of the customer bill (approximately 82% … for electric … residential customers) — a greater
> percentage than what the ECOSS … support[s] being recovered through volumetric rates."

So BGE classifies the plant as peak-driven, computes an avoided cost on a peak basis, yet recovers
~82% of residential distribution volumetrically. That gap is the intervention.

---

## 5. Sensitivities

- **Primary**: Brattle `$32/kW-yr` (2022$), corroborated by OPC/Takahashi's `$25.1–34.09` range.
- **Lower bound**: FLIC `~$1.63/kW-yr` (BGE incremental capacity-expansion capital ÷ 6,102 MW
  system peak), the NY-comparable figure — report it to show how much the definition choice moves
  BAT results.
- **Upper bound**: E3 `$203–258/kW-yr`, **with the caveat** that it bundles FERC transmission
  (double-counts bulk TX); not for combined use.

---

## 6. Stage B dependency (hourly allocation — not yet wired)

This note covers **Stage A** (the levelized `$/kW-yr` figure). Producing the hourly `$/kWh` BAT
signal (Stage B) via the PoP allocator additionally requires:

1. Adding `"MD"` to the `--state` choices in
   [generate_utility_tx_dx_mc.py](../../../utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py)
   (currently `choices=["NY", "RI"]`).
2. A `get_state_config("MD")` entry in
   [data/eia/hourly_loads/eia_region_config.py](../../../data/eia/hourly_loads/eia_region_config.py)
   (`iso_region="pjm"`, BGE balancing-authority/subregion).
3. EIA Form 930 BGE hourly load on S3 to use as the PoP weighting profile.

Items 1–3 are tracked by the EIA BGE data-fetch Linear ticket and are out of scope for Stage A.

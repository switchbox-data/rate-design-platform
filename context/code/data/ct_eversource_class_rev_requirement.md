# CT Eversource whole-class revenue-requirement files (Docket 26-05-10)

Two Track 2 revenue-requirement (RR) files for CL&P/Eversource that size the CAIRO baseline to the
**whole residential rate class**, with every residential ResStock building modeled on **Rate 1**:

| File                                                                                                                                           | Rates                                 | Base delivery RR    |
| ---------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- | ------------------- |
| [`ct_eversource_rate_case_test_year.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/ct_eversource_rate_case_test_year.yaml)     | Test Year, **current** rates          | `$714,416,964.74`   |
| [`ct_eversource_rate_case_rate_year_1.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/ct_eversource_rate_case_rate_year_1.yaml) | Rate Year 1, **Proposed + GET** rates | `$1,427,326,050.00` |

These are the delivery + supply revenue-requirement targets for CAIRO. They are distinct from the
proposed _tariff_ JSONs documented in [`ct_eversource_custom_rates.md`](ct_eversource_custom_rates.md);
this file covers the _revenue requirement / test-year determinant_ side.

## What was done, and why

**The bug.** The prior `ct_eversource_rate_case_test_year.yaml` seeded **Rate-1-only** targets (base
DRR `$614M`, `8.44B` kWh, `1.05M` customers) but the build script populates it from the **whole
residential ResStock population** (`sb.electric_utility == ct_eversource` is the only CL&P residential
utility assignment in the CT ResStock data — `892,895.5` weighted customers, `7.63B` kWh). A
whole-class load population was therefore pinned to a Rate-1-sized target.

**The fix.** We size the base DRR, test-year kWh, and customer count to the **whole residential class**
— the sum of Rate 1 (Residential), Rate 5 (Residential Electric Heating), and Rate 7 (Residential
Time-of-Day) — while still applying **Rate 1's** tariff and rider stack to every building. The old
Rate-1-only file is archived at
[`ct_eversource_rate1_rate_case_test_year.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/ct_eversource_rate1_rate_case_test_year.yaml).

This matches the modeling decision that **all CL&P residential buildings are billed on Rate 1** (Rate 5
and Rate 7 are small — `135k` and `~731` customers vs. `~1.05M` on Rate 1 — and ResStock carries no
rate-schedule dimension to split them). We do **not** preserve the per-rate structure of Rate 5/7; we
only fold their revenue, sales, and customers into the class totals that drive the top-ups.

## How the numbers flow through the build

`utils/pre/rev_requirement/build_rate_case_test_year.py` completes each seeded partial. It reads only
four seeded keys (`utility`, `delivery_revenue_requirement_from_rate_case`, `test_year_residential_kwh`,
`test_year_customer_count`) plus the monthly-rates file, then:

- **Delivery top-ups** (riders): each `$/kWh` charge = `day_weighted_avg_rate × test_year_residential_kwh`;
  each `$/month` charge = `rate × test_year_customer_count × 12`. No per-rate (R5/R7) detail is used —
  the top-ups scale purely off the two class-total scalars.
- **Supply top-up**: ResStock monthly kWh scaled to `test_year_residential_kwh`, times the monthly
  supply rates.
- **ResStock scaling** (always recomputed from the load files, never read from the partial):
  `customer_scale_factor = test_year_customer_count / resstock_customer_count`;
  `kwh_scale_factor = test_year_residential_kwh / (resstock_total_kwh × customer_scale_factor)`.

So setting the two scalars to class totals is exactly what makes the baseline "every residential
customer on Rate 1, sized to the whole class."

- **Test Year** uses [`ct_eversource_monthly_rates_2025.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/top-ups/monthly_rates/ct_eversource_monthly_rates_2025.yaml)
  (current-vintage RateAcuity riders) and is rebuilt with `UTILITY=ct_eversource just s ct build-rate-case-test-year`.
- **Rate Year 1** uses [`ct_eversource_rate1_proposed_monthly_rates_2025.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/top-ups/monthly_rates/ct_eversource_rate1_proposed_monthly_rates_2025.yaml)
  (filed-2026 proposed riders, ESI/RDM rolled in) and is rebuilt with a direct script call (the recipe
  hardcodes the current monthly-rates file and the canonical output name); the exact command is in the
  file's header comment.

## Per-number provenance

All dollar figures are `$(000)`s in the exhibit; kWh = MWh × 1000. Line numbers are into the extract
[`context/sources/exhibit_clp_rates_2_residential.md`](../../sources/exhibit_clp_rates_2_residential.md)
(source workbook `Exhibit CLP-RATES-2.xlsx`, PURA Docket 26-05-10). Values we compute are marked
**derived** with the arithmetic shown.

### Test Year (current rates)

| Quantity                          | Rate    | Value                   | Source (exhibit location)                                                                                                                                                                       | Notes                                                                                                                             |
| --------------------------------- | ------- | ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Distribution functional (current) | R1      | `$614,065,520.87`       | line 63 (Exh 2.4 Test Year functional rollup)                                                                                                                                                   | Customer Charge `$120,786.56k` + Distribution energy `$493,279.36k`                                                               |
| Distribution functional (current) | R5      | `$99,778,890.00`        | lines 116-117 (Exh 2.4 R5 build) + [`ct_eversource_elecheat_rate_case_test_year.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/ct_eversource_elecheat_rate_case_test_year.yaml) | CC `1,620,878.4 bills × $23.75 = $38,495.86k` + Dist `1,593,008.20 MWh × $0.03847 = $61,283.03k`; R5 determinants unchanged TY→RY |
| Distribution functional (current) | R7      | `$572,553.87`           | lines 140, 152 (Exh 2.4 R7 build) + [`ct_eversource_tou_rate_case_test_year.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/ct_eversource_tou_rate_case_test_year.yaml)          | Computed at TY determinants × current rates: CC `731.3 × 12 × $9.62 = $84,421.27` + Dist `8,854,210 kWh × $0.05513 = $488,132.60` |
| **Base DRR (class)**              | **sum** | **`$714,416,964.74`**   | **derived**                                                                                                                                                                                     | `614,065,520.87 + 99,778,890.00 + 572,553.87`                                                                                     |
| Total Sales (kWh)                 | R1      | `8,440,795,309.21`      | line 200 (Exh 2.10 p.1, `8,440,795.31 MWh`)                                                                                                                                                     | Total Sales = Billed + Primary Metering Credit                                                                                    |
| Total Sales (kWh)                 | R5      | `1,593,009,920.00`      | line 201 (`1,593,009.92 MWh`)                                                                                                                                                                   |                                                                                                                                   |
| Total Sales (kWh)                 | R7      | `8,854,210.00`          | line 202 (`8,854.21 MWh`)                                                                                                                                                                       |                                                                                                                                   |
| **Test-year kWh (class)**         | **sum** | **`10,042,659,439.21`** | **derived**                                                                                                                                                                                     | `8,440,795,309.21 + 1,593,009,920 + 8,854,210`                                                                                    |
| Avg # Customer Bills Rendered     | R1      | `1,046,314.60`          | line 200 (Exh 2.10 p.1)                                                                                                                                                                         | Average monthly bills (× 12 = annual)                                                                                             |
| Avg # Customer Bills Rendered     | R5      | `135,073.20`            | line 201                                                                                                                                                                                        |                                                                                                                                   |
| Avg # Customer Bills Rendered     | R7      | `731.30`                | line 202                                                                                                                                                                                        |                                                                                                                                   |
| **Test-year customers (class)**   | **sum** | **`1,182,119.10`**      | **derived**                                                                                                                                                                                     | `1,046,314.60 + 135,073.20 + 731.30`                                                                                              |
| Fixed charge from rate case       | R1      | `$9.62/mo`              | line 50 (Exh 2.4 R1 current Customer Charge)                                                                                                                                                    | All buildings modeled on Rate 1                                                                                                   |

### Rate Year 1 (Proposed + GET)

| Quantity                               | Rate    | Value                   | Source (exhibit location)                                                         | Notes                                                                                                |
| -------------------------------------- | ------- | ----------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Distribution functional (Proposed+GET) | R1      | `$1,222,814,890.00`     | line 92 (Exh 2.4 Rate Year functional-category rollup, Distribution Proposed+GET) | Rebuilds from CC `$159,695.46k` (line 72) + Dist `$1,063,120.21k` (line 73) within workbook rounding |
| Distribution functional (Proposed+GET) | R5      | `$203,307,450.00`       | lines 116-117 (Exh 2.4 R5 build, Proposed+GET column)                             | CC `$48,626.35k` + Dist `1,593,008.20 MWh × $0.0971 = $154,681.10k`                                  |
| Distribution functional (Proposed+GET) | R7      | `$1,203,710.00`         | line 165 (Exh 2.4 R7 functional Distribution, Proposed+GET)                       | On-peak + off-peak Distribution summed                                                               |
| **Base DRR (class)**                   | **sum** | **`$1,427,326,050.00`** | **derived**                                                                       | `1,222,814,890 + 203,307,450 + 1,203,710`                                                            |
| RY Forecasted Sales (kWh)              | R1      | `8,716,970,840.00`      | line 211 (Exh 2.10 p.2, `8,716,970.84 MWh`)                                       |                                                                                                      |
| RY Forecasted Sales (kWh)              | R5      | `1,593,009,920.00`      | line 212 (`1,593,009.92 MWh`)                                                     | Unchanged TY→RY per footnote                                                                         |
| RY Forecasted Sales (kWh)              | R7      | `9,126,890.00`          | line 213 (`9,126.89 MWh`)                                                         |                                                                                                      |
| **Rate-year kWh (class)**              | **sum** | **`10,319,107,650.00`** | **derived**                                                                       | `8,716,970,840 + 1,593,009,920 + 9,126,890`                                                          |
| Avg # Customer Bills Rendered (RY)     | R1      | `1,076,695.40`          | line 211 (Exh 2.10 p.2) / line 38 (Exh 2.4 R1 RY determinant)                     |                                                                                                      |
| Avg # Customer Bills Rendered (RY)     | R5      | `135,073.20`            | line 212 (footnote: unchanged)                                                    |                                                                                                      |
| Avg # Customer Bills Rendered (RY)     | R7      | `752.35`                | line 213                                                                          |                                                                                                      |
| **Rate-year customers (class)**        | **sum** | **`1,212,520.95`**      | **derived**                                                                       | `1,076,695.40 + 135,073.20 + 752.35`                                                                 |
| Fixed charge from rate case            | R1      | `$12.36/mo`             | line 72 (Exh 2.4 R1 RY Proposed+GET Customer Charge)                              | All buildings modeled on Rate 1                                                                      |

### ResStock inputs (recomputed by the build script, not seeded)

| Quantity                         | Value              | Source                                                                                                                                                                     |
| -------------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `resstock_total_residential_kwh` | `7,634,855,865.62` | `sum(grid_cons × weight)` over `sb.electric_utility == ct_eversource`, upgrade 00, from `res_2024_amy2018_2_sb` monthly load curves; `grid_cons = max(total − abs(pv), 0)` |
| `resstock_customer_count`        | `892,895.50`       | `sum(weight)` for the same population (3,539 buildings)                                                                                                                    |
| `customer_scale_factor` (TY)     | `1.3239165173`     | **derived**: `1,182,119.10 / 892,895.50`                                                                                                                                   |
| `kwh_scale_factor` (TY)          | `0.9935444284`     | **derived**: `10,042,659,439.21 / (7,634,855,865.62 × 1.3239165173)`                                                                                                       |
| `customer_scale_factor` (RY)     | `1.3579651266`     | **derived**: `1,212,520.95 / 892,895.50`                                                                                                                                   |
| `kwh_scale_factor` (RY)          | `0.9952969731`     | **derived**: `10,319,107,650 / (7,634,855,865.62 × 1.3579651266)`                                                                                                          |

## Caveats

- **Why Rate Year uses Proposed + GET.** The proposed Rate 1 tariff encodes GET (Gross Earnings Tax)
  directly into its rates (distribution `$0.12196/kWh` incl. GET; customer charge `$12.36`). The
  calibration target must be on the same basis, so the rate-year DRR uses the **Proposed + GET**
  distribution functional (`$1,427,326,050`), not the pre-GET figure. Using pre-GET
  (`≈$1,330M`) would make CAIRO scale the +GET tariff down to collect only pre-GET revenue.
- **`already_in_drr` sanity check.** The build script cross-checks the base DRR against Rate 1's rates
  applied to the class determinants (`customer_charge × bills + core_delivery_rate × kWh`). Because Rate
  1 dominates the class, this lands within the script's 2% tolerance: `$723.4M` vs `$714.4M` (1.3%) for
  the test year, `$1,438.4M` vs `$1,427.3M` (0.8%) for the rate year. No warning is raised.
- **Test-year and rate-year files sit on different rider vintages.** The test-year build uses the
  current-vintage RateAcuity riders ([`ct_eversource_monthly_rates_2025.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/top-ups/monthly_rates/ct_eversource_monthly_rates_2025.yaml)),
  while the rate-year build uses the filed-2026 proposed riders ([`ct_eversource_rate1_proposed_monthly_rates_2025.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/top-ups/monthly_rates/ct_eversource_rate1_proposed_monthly_rates_2025.yaml),
  ESI/RDM rolled into base distribution). The two rider stacks are each internally consistent but are
  **not comparable line-for-line**: transmission is `$0.03401`/`$0.04433` (RateAcuity) vs `$0.0505`
  (filed); the test year **omits** SBC (`exclude_eligibility`) and RDM (`exclude_trueup`) while the rate
  year carries SBC at `-$0.00196` and RDM folded into base. A test-year → rate-year delta therefore
  conflates rate-design change with rider-vintage change and should **not** be read as a pure
  proposed-vs-current comparison. For a matched proposed-vintage Rate 1, use the tariff JSONs in
  [`ct_eversource_custom_rates.md`](ct_eversource_custom_rates.md).
- **Test-year NBFMCC provenance.** The test-year `non_bypassable_fmcc_nbfmcc` top-up
  (`+$300,687,403.84`, day-weighted rate `≈ +$0.02994/kWh`) comes from the current-vintage RateAcuity
  rider file, **not** from the CLP-RATES-2 exhibit. It is a positive non-bypassable composite and is
  distinct from — and opposite in sign to — the exhibit's delivery-side **FMCC-Delivery** credit
  (`-$0.01911/kWh`, exhibit line 58), which is what the rate-year file uses
  (`non_bypassable_fmcc_delivery`, `-$197,198,147.19`). This single rider swings the test-year delivery
  RR by `≈ $493M` versus applying the exhibit's `-0.01911` delivery basis, so it is the test-year figure
  most in need of an independent check against the RateAcuity tariff.
- **Large-number calibration files.** The large-number calibration files used by calibrated runs now
  carry the **whole-class** `test_year_customer_count` and `resstock_kwh_scale_factor`:
  [`ct_eversource_large_number_rate_case_test_year.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/ct_eversource_large_number_rate_case_test_year.yaml)
  (class test year: `1,182,119.10`, `0.9935444284`) and
  [`ct_eversource_large_number_rate_case_rate_year_1.yaml`](../../../rate_design/hp_rates/ct/config/rev_requirement/ct_eversource_large_number_rate_case_rate_year_1.yaml)
  (class rate year: `1,212,520.95`, `0.9952969731`). Wiring the rate-year variants into the
  scenarios/pipeline (which currently reference only the test-year large-number file) is out of scope
  here.

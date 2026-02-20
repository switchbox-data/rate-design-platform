# ISO-NE marginal cost analysis for rate design

## Overview

As an alternative to Cambium marginal costs, CAIRO can use **actual ISO-NE market data** for energy, capacity, and ancillary marginal costs. This is useful for comparing modeled (Cambium) costs against observed wholesale market prices and for states like RI that sit within ISO-NE.

The implementation lives in `utils/cairo.py` and is invoked when the scenario's `path_cambium_marginal_costs` points to an ISO-NE SMD Excel workbook (file matching `*_smd_hourly.xlsx`).

## Data sources

### Energy and ancillary costs: ISO-NE SMD hourly workbook

- **File pattern**: `{year}_smd_hourly.xlsx` (e.g. `2025_smd_hourly.xlsx`)
- **Stored at**: `rate_design/ri/hp_rates/config/marginal_costs/`
- **Sheets used**:
  - `RI` — RI-specific zonal demand (`RT_Demand`) and LMPs (`RT_LMP`). **Used by default** for RI analysis.
  - `ISO NE CA` — system-wide demand (used for capacity cost allocation), regulation prices (`Reg_Service_Price`, `Reg_Capacity_Price`)
  - `SEMA`, `WCMA`, `NEMA` — legacy fallback: if the `RI` sheet is missing from the workbook, demand and LMPs are load-weighted across these three MA sub-regions.
- **Source**: ISO-NE publishes System Marginal Data (SMD) hourly files. These are not publicly downloadable in bulk — they must be compiled from ISO-NE's data portal or obtained from ISO-NE directly.

**Energy marginal cost** = RI zonal LMP + ancillary (regulation service + regulation capacity). Units: $/MWh, converted to $/kWh in the loader.

**Ancillary cost** = `Reg_Service_Price + Reg_Capacity_Price` from the `ISO NE CA` sheet. Added to energy costs (non-zonal; applies system-wide).

### Capacity costs: Forward Capacity Market (FCM) auction results

Capacity costs are **not in the SMD workbook**. They are derived from ISO-NE Forward Capacity Auction (FCA) results, stored in a YAML assumptions file:

- **Config**: `rate_design/ri/hp_rates/config/marginal_costs/isone_fca_assumptions.yaml`

#### How the FCM works (relevant to this analysis)

- ISO-NE runs a Forward Capacity Auction (FCA) ~3 years ahead of each **Capacity Commitment Period (CCP)**.
- Each CCP runs from **June 1 to May 31** (not calendar year).
- For a calendar-year analysis (e.g. 2025), you need two FCA results:
  - **Jan–May** from the prior CCP (e.g. CCP 2024-2025 = FCA 15)
  - **Jun–Dec** from the current CCP (e.g. CCP 2025-2026 = FCA 16)
- This is why the YAML has a `months` field per segment — it captures the calendar-year overlap weighting.

#### Capacity zones and CSO source

ISO-NE has multiple capacity zones. **RI is in the Southeast New England (SENE) zone**. For RI analysis, we use:

- **FCA clearing price** ($/kW-month) — the SENE zone clearing price from the FCA results report. We use the raw clearing price, *not* the FCM effective charge rate. The effective charge rate includes adjustments for multi-year rate locks, winter IPR, self-supply variance, HQICC, and ARA reconfigurations — those adjustments do not belong in a marginal cost signal.
- **Capacity Supply Obligation (CSO)** (MW) — RI-specific CSO from the **2024 CELT Report** (Table 4.1 "Summary of CSOs"). Using RI-only CSO (not full SENE zone) means the total capacity cost pool reflects what RI ratepayers actually pay for.

The peak-hour allocation still uses ISO NE CA system-wide load shape.

#### Key sources

| Document | Location / URL | Contents |
| --- | --- | --- |
| FCA results report (all auctions) | <https://www.iso-ne.com/static-assets/documents/2018/05/fca-results-report.pdf> | Historical FCA clearing prices by zone |
| 2024 CELT Report | `rate_design/ri/hp_rates/config/marginal_costs/2024_celt_report.xlsx` (Table 4.1) | RI-specific CSOs by FCA/CCP |
| FCM cost allocation forecast CCP 2025-2026 | <https://www.iso-ne.com/static-assets/documents/2022/06/fcm_cost_allocation_forecast_ccp_2025-2026_draft.pdf> | Forecasted effective charge rates, capacity load obligations by zone (cross-reference) |

## How it works in the codebase

### Scenario configuration

In `rate_design/ri/hp_rates/config/scenarios.yaml`, point `path_cambium_marginal_costs` to the SMD workbook:

```yaml
path_cambium_marginal_costs: marginal_costs/2025_smd_hourly.xlsx
```

The run scenario script (`run_scenario.py`) detects `.xlsx` files with `smd` in the name and routes to `_load_iso_marginal_costs()` instead of `_load_cambium_marginal_costs()`.

### Code path (`utils/cairo.py`)

1. **`_load_iso_marginal_costs(analysis_year, market_data_path)`** — main entrypoint
   - Resolves the workbook path
   - Calls `_return_full_isone_costs()` → returns `(lmp, capacity, ancillary)` series
   - Combines: `energy = (LMP + ancillary) / 1000` ($/kWh), `capacity = capacity / 1000` ($/kWh)
   - Returns DataFrame with `Marginal Energy Costs ($/kWh)` and `Marginal Capacity Costs ($/kWh)`, index named `time`

2. **Energy path** (default for RI): `_return_full_isone_costs(region="RI")` reads the `RI` sheet directly via `_extract_regional_lmp()` → returns `RT_LMP` as a plain Series.
   - **Fallback**: if the `RI` sheet is missing from the workbook, falls back to `_return_prices_and_load_by_region()` → `_return_ISONE_lmps()` → load-weighted LMP across SEMA/WCMA/NEMA.

3. **Ancillary path**: `_extract_ancillary_prices()` → regulation prices from `ISO NE CA` sheet

4. **Capacity path**: `_return_ISONE_capacity_prices()`:
   - Loads FCA assumptions from YAML (`_load_isone_fca_segments()`)
   - Calculates total annual capacity cost = Σ(CSO_MW × 1000 × payment_rate × months)
   - Loads ISO NE CA total demand from the workbook
   - Finds peak-hour threshold (top 101 hours or 95% of max, whichever is lower)
   - Allocates total costs to hours above threshold, proportional to load above threshold
   - Returns $/MWh capacity cost per hour

### Output contract

The ISO loader returns the same DataFrame shape and column names as the Cambium loader:

| Column | Unit |
| --- | --- |
| `Marginal Energy Costs ($/kWh)` | $/kWh |
| `Marginal Capacity Costs ($/kWh)` | $/kWh |

Index: `time` (DatetimeIndex, EST-localized). Matches the Cambium loader output contract.

## Comparing Cambium vs ISO-NE marginal costs

A key use case is comparing Cambium modeled costs against actual ISO-NE market prices. Differences arise because:

- **Cambium** is a modeled projection (NREL's ReEDS capacity expansion + PLEXOS dispatch) — it captures long-run marginal costs including future grid evolution.
- **ISO-NE market data** reflects actual wholesale market outcomes — it captures real-time supply/demand dynamics, fuel prices, congestion, and ancillary services.
- **Capacity**: Cambium uses its own capacity cost methodology; ISO-NE uses FCA results. These can diverge significantly.

For rate design, the comparison helps assess whether Cambium-based rates are calibrated to real market conditions or diverge materially.

## Updating assumptions

### New year of SMD data

1. Obtain the `{year}_smd_hourly.xlsx` workbook from ISO-NE
2. Place it in `rate_design/ri/hp_rates/config/marginal_costs/`
3. Add a new YAML run in `scenarios.yaml` pointing to it

### New FCA / capacity assumptions

1. Find the relevant FCA clearing prices in the [FCA results report](https://www.iso-ne.com/static-assets/documents/2018/05/fca-results-report.pdf) — use the **SENE zone clearing price**, not the effective charge rate.
2. Get RI-specific CSOs from the latest CELT Report (Table 4.1 "Summary of CSOs").
3. Determine the calendar-year month split (e.g. 5 months from prior CCP, 7 from current).
4. Update `isone_fca_assumptions.yaml` with the new year entry.

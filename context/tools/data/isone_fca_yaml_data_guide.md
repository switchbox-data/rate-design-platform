# ISO-NE FCA assumptions YAML: data dictionary and sources

This guide documents the data stored in `rate_design/ri/hp_rates/config/marginal_costs/isone_fca_assumptions.yaml` and where to find the original source data online.

## What the YAML stores

The YAML captures **Forward Capacity Market (FCM) inputs** for computing hourly marginal capacity costs for Rhode Island. Each year entry contains one or more **segments** — because ISO-NE capacity commitment periods run June 1 – May 31, a calendar-year analysis (e.g. 2025) always spans two commitment periods.

### Fields per segment

| Field                           | Unit       | Meaning                                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `payment_rate_per_kw_month`     | $/kW-month | SENE zone FCA clearing price — the market-determined price generators receive for committing capacity. This is the **raw auction clearing price**, not the FCM effective charge rate (which includes adjustments for multi-year rate locks, winter IPR, self-supply variance, HQICC, and ARA reconfigurations that don't belong in a marginal cost signal). |
| `capacity_supply_obligation_mw` | MW         | RI-specific capacity supply obligation — the total MW of generation capacity RI ratepayers are obligated to procure. We use the **winter CSO** (not summer) because winter is the binding constraint for heat-pump-heavy load profiles. This is RI-only, not the full SENE zone (RI + SEMA).                                                                |
| `months`                        | count      | Number of calendar-year months this segment covers. Needed because two FCA commitment periods overlap a single calendar year (e.g. 5 months from CCP 2024-2025, 7 months from CCP 2025-2026).                                                                                                                                                               |
| `label`                         | string     | Human-readable identifier (e.g. `fca15_jan_to_may`). Not used in computation.                                                                                                                                                                                                                                                                               |

### How these fields combine

Total annual capacity cost = `Σ(CSO_MW × 1000 × payment_rate_per_kw_month × months)` across all segments for that year. This dollar pool is then allocated to the ~100 highest-demand hours using ISO-NE system load shape (from the SMD workbook's "ISO NE CA" sheet, not from this YAML).

## Where to find the source data

### FCA clearing prices

**Source:** ISO-NE FCA Results Report (covers all auctions historically).

- **URL:** <https://www.iso-ne.com/static-assets/documents/2018/05/fca-results-report.pdf>
- **What to look for:** Table of clearing prices by capacity zone and FCA number. Use the **SENE (Southeast New England)** row.
- **Also available at:** <https://www.iso-ne.com/markets-operations/markets/forward-capacity-market> → "Auction Reports and Supporting Data"

For specific auctions:

| FCA    | Commitment period   | SENE clearing price |
| ------ | ------------------- | ------------------- |
| FCA 15 | Jun 2024 – May 2025 | $3.980/kW-month     |
| FCA 16 | Jun 2025 – May 2026 | $2.639/kW-month     |

### Capacity supply obligations (RI-specific)

**Source:** ISO-NE CELT Report (Capacity, Energy, Loads, and Transmission), published annually.

- **URL:** <https://www.iso-ne.com/celt>
- **What to look for:** Table 4.1 "Summary of CSOs" — this breaks out CSOs by state (or sub-zone) and by FCA. Use the **Rhode Island** rows, **winter** CSO column.
- **Local copy:** `rate_design/ri/hp_rates/config/marginal_costs/2024_celt_report.xlsx`

For 2025 analysis:

| FCA    | RI winter CSO |
| ------ | ------------- |
| FCA 15 | 2,194.808 MW  |
| FCA 16 | 2,238.267 MW  |

### Cross-reference (optional)

The FCM cost allocation forecast provides an independent check on effective charge rates and capacity load obligations by zone:

- **URL:** <https://www.iso-ne.com/static-assets/documents/2022/06/fcm_cost_allocation_forecast_ccp_2025-2026_draft.pdf>

## Updating the YAML for a new year

1. Get the SENE clearing price for the relevant FCA(s) from the [FCA results report](https://www.iso-ne.com/static-assets/documents/2018/05/fca-results-report.pdf).
2. Get the RI winter CSO from the latest [CELT Report](https://www.iso-ne.com/celt) Table 4.1.
3. Determine the calendar-year month split (which months of your analysis year fall in which CCP).
4. Add a new year entry with the appropriate segments.

# Capacity market comparison: NYISO ICAP vs ISO-NE FCM

This document compares the two capacity market structures relevant to our BAT (Bill Alignment Test) analysis and discusses implications for marginal cost signals in New York vs. Rhode Island.

## Market structures

### NYISO Installed Capacity (ICAP)

NYISO procures capacity through three auction types:

- **Strip auction**: 6-month forward commitment, run twice per year (May for summer capability period, November for winter). Sets a base price for the upcoming capability period.
- **Monthly auction**: Run each month before the capability month begins. Adjusts obligations and prices based on updated supply/demand conditions.
- **Spot auction**: Run each month for that month's capacity. Reflects real-time capacity market conditions. The most granular and current price signal.

Our NYISO ICAP pipeline (`data/nyiso/icap/`) fetches all three auction types. The downstream marginal cost allocation (`utils/pre/generate_utility_supply_mc.py`) uses **Spot prices** as the capacity component of marginal cost, because Spot prices reflect the contemporaneous cost of capacity in each month.

### ISO-NE Forward Capacity Market (FCM)

ISO-NE procures capacity through three mechanisms:

- **Forward Capacity Auction (FCA)**: The primary procurement mechanism. Runs once per year (results in February), procuring capacity for a commitment period that begins **three years later**. For example, FCA 18 (conducted in Feb 2024) procured capacity for CCP 2027-28. Produces a single clearing price per capacity zone.
- **Annual Reconfiguration Auction (ARA)**: Up to 3 per commitment period, run in the years between the FCA and the start of the commitment period. Allows adjustments to capacity obligations as conditions change. Produces clearing prices by zone.
- **Monthly Reconfiguration Auction (MRA)**: Run each month during the commitment period. The most granular adjustment mechanism, allowing month-to-month rebalancing of capacity obligations. Closest to real-time capacity pricing.

Our ISO-NE capacity pipelines (`data/isone/capacity/`) store all three:

- `fca/` — curated FCA clearing prices (FCA 1-18)
- `ara/` — ARA clearing prices from ISO Express CSVs
- `mra/` — MRA clearing prices from ISO Express CSVs

## Structural comparison

| Feature               | NYISO                           | ISO-NE                                            | Analogy                             |
| --------------------- | ------------------------------- | ------------------------------------------------- | ----------------------------------- |
| Long-term forward     | Strip (6-month)                 | FCA (3-year)                                      | FCA ≈ Strip                         |
| Pre-period adjustment | Monthly                         | ARA (annual)                                      | ARA ≈ Monthly                       |
| In-period real-time   | Spot (monthly)                  | MRA (monthly)                                     | MRA ≈ Spot                          |
| Forward horizon       | 6 months                        | 3 years                                           | ISO-NE locks in prices much earlier |
| Price volatility      | Spot prices vary month-to-month | FCA prices are fixed for 3 years; MRA prices vary |                                     |

The key structural difference: NYISO Spot prices change every month, reflecting current supply/demand conditions. ISO-NE FCA prices are locked in three years ahead and never change — they reflect the expected cost at the time of the auction, not contemporaneous conditions.

## Implications for BAT marginal cost signal

The BAT measures cross-subsidization by comparing a customer's marginal cost of service to their bill. The capacity component of marginal cost depends on which price signal we use.

### Current approach

- **New York**: Uses NYISO **Spot** prices for the capacity marginal cost component. This reflects the monthly cost of capacity as experienced by the system.
- **Rhode Island**: The `isone_fca_assumptions.yaml` uses **FCA** clearing prices. This reflects the long-run procurement cost that ratepayers are committed to paying.

### Trade-offs

**Using FCA prices (current RI approach)**:

- Reflects what ratepayers are actually paying for capacity (the FCA price is the contracted rate)
- Stable and predictable — doesn't change month to month
- But doesn't reflect the current marginal cost of capacity, since the FCA was conducted 3 years ago under different conditions
- Analogous to using NYISO Strip prices instead of Spot

**Using MRA prices (alternative, consistent with NY approach)**:

- Reflects the current monthly cost of capacity, consistent with how NYISO Spot is used
- More volatile, which may better capture the true marginal cost signal
- MRA prices tend to be lower than FCA prices (monthly reconfiguration is often surplus-driven)
- More directly comparable to the NYISO methodology, making cross-state BAT results comparable

**Using ARA prices (middle ground)**:

- Annual adjustment, less volatile than MRA but more current than FCA
- Reflects pre-commitment adjustments that incorporate updated information
- Could be used as a "semi-forward" price signal

### Recommendation context

The choice between FCA and MRA prices for BAT depends on what question we're answering:

- If the question is "are customers paying their fair share of the capacity costs we're committed to?" → use FCA prices (what we've contracted)
- If the question is "are customers paying their fair share of what capacity actually costs right now?" → use MRA prices (current marginal cost)

The NYISO approach (Spot) answers the second question. For consistency across states and for the BAT's purpose of measuring cost-reflectiveness, MRA prices may be the more appropriate signal.

## Data availability for each approach

| Price signal      | NYISO data                           | ISO-NE data                              | Coverage                                                 |
| ----------------- | ------------------------------------ | ---------------------------------------- | -------------------------------------------------------- |
| Forward/Strip/FCA | Strip prices in `data/nyiso/icap/`   | FCA prices in `data/isone/capacity/fca/` | NYISO: 2019+; ISO-NE: FCA 1 (2010) through FCA 18 (2028) |
| Monthly/ARA       | Monthly prices in `data/nyiso/icap/` | ARA prices in `data/isone/capacity/ara/` | NYISO: 2019+; ISO-NE: CP 2019-20+                        |
| Spot/MRA          | Spot prices in `data/nyiso/icap/`    | MRA prices in `data/isone/capacity/mra/` | NYISO: 2019+; ISO-NE: Sep 2018+                          |

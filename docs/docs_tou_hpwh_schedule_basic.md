# First-Pass TOU Scheduling Decision Model for HPWHs in OCHRE
NREL/OCHRE Team
Invalid Date

# Introduction

This document outlines a simple, first-pass heuristic-based decision
model for consumer response to time-of-use (TOU) electricity rates in
residential building simulations using OCHRE, focusing on Heat Pump
Water Heaters (HPWHs). The goal is to integrate an adaptive control
schedule for the HPWH based on feedback from utility bills and
anticipated cost savings, while accounting for consumer effort in
shifting schedules.

# 1. Problem Definition

- **Objective:** Model how a consumer, with fixed (exogenous) hot water
  usage needs, might reschedule their HPWH operation in response to TOU
  rates to minimize their electricity bills, considering a “switching
  cost” for effort/comfort.
- **Assumptions:**
  - Hot water usage schedule is fixed (not flexible).
  - HPWH can be controlled (on/off) on a schedule.
  - TOU rate structure (on-peak/off-peak) is known and simple (e.g.,
    higher price during peak hours).
  - Consumer receives feedback on energy cost at each billing cycle
    (monthly).
  - Switching to a TOU-adapted schedule incurs a one-time “effort cost”
    (can be fixed or parameterized).
  - Decision process is repeated each billing cycle (feedback loop).

# 2. Key Variables and Parameters

| Symbol | Description | Units |
|----|----|----|
| ( S ) | Current HPWH schedule (default or TOU-adapted) | binary |
| ( U\_{HW} ) | Exogenous hot water usage schedule | L/hr |
| ( ) | Usage multiplier (building characteristic) | scalar |
| ( E ) | Electricity consumption profile (from HPWH operation) | kWh |
| ( r\_{on}, r\_{off} ) | TOU electricity rate (on-peak, off-peak) | \$/kWh |
| ( C\_{bill} ) | Monthly electricity bill (for water heating) | \$ |
| ( C\_{switch} ) | Consumer switching/“hassle” cost | \$ |
| ( C ) | Anticipated bill savings from switching | \$ |

# 3. Model Steps

## Step 1: Calculate Baseline

- **Hot Water Usage:** ( U\_{HW}(t) ) is fixed by schedule and
  multiplier.
- **HPWH Default Schedule:** Operates as needed to meet hot water draws.
- **Simulate:** Run OCHRE with default HPWH schedule to get (
  E\_{default}(t) ), ( C\_{bill,default} ).

## Step 2: Define TOU-Adaptive Schedule

- **HPWH TOU Schedule:** Only operates HPWH during off-peak hours
  (unless tank runs cold).
- **Simulate:** Run OCHRE with TOU-adaptive HPWH schedule to get (
  E\_{TOU}(t) ), ( C\_{bill,TOU} ).

## Step 3: Anticipated Savings

- **Calculate:** \[ C = C\_{bill,default} - C\_{bill,TOU} \]
- **Net Benefit:** \[ = C - C\_{switch} \]

## Step 4: Decision Rule

- **If** ( \> 0 ): Consumer switches to TOU-adapted schedule.
- **Else:** Remains on default schedule.

## Step 5: Feedback & Iteration

- At each monthly billing cycle, repeat Steps 1–4 using updated
  consumption and bills.
- Consumer can reconsider switching if conditions change (e.g., rate
  structure, switching cost).

# 4. Integration with OCHRE

## Inputs Required

- Hot water usage schedule: `HotWaterUsageSchedule`
- Usage multiplier: `UsageMultiplier`
- TOU rate definition: `r_{on}, r_{off}` (passed to cost calculation)
- HPWH scheduling control: ability to enforce on/off operation windows

## OCHRE Outputs Used

| Output Name                  | Use in Model                      |
|------------------------------|-----------------------------------|
| Hot Water Delivered (L/min)  | For checking comfort/unmet demand |
| Water Heating Delivered (W)  | For cost calculation              |
| Hot Water Outlet Temperature | For comfort check (optional)      |
| Hot Water Unmet Demand (kW)  | To quantify service/comfort loss  |

## Implementation Notes

- **Control:** Implement schedule-based control in OCHRE (see
  `update_external_control` in HPWH code).
- **Comfort Check:** If TOU schedule leads to unmet demand or low temp,
  can factor “comfort penalty” into switching cost.
- **Outputs:** Compare cost and comfort metrics for both schedules.

# 5. Example Calculation

Suppose:

- Default schedule: HPWH runs as needed.
- TOU schedule: HPWH only runs 10PM–6AM (off-peak).
- Monthly bill (default): \$30
- Monthly bill (TOU): \$22
- Switching cost: \$5 (one-time or amortized)

\[ C = 30 - 22 = \$8 \] \[ = 8 - 5 = \$3 \]

**Decision:** Consumer switches to TOU-adaptive schedule.

# 6. Equations

## Electricity Cost Calculation

\[ C\_{bill} = \_{t} E(t) r(t) \] Where: - ( E(t) ): Electricity used by
HPWH at time ( t ) - ( r(t) ): Rate (on-peak/off-peak) at time ( t )

## Comfort Penalty (optional)

\[ C\_{comfort} = \_{t} (t) \] Where: - ( ): Monetization factor for
comfort loss (can be set to zero for first-pass)

## Final Decision

\[ = C - C\_{switch} - C\_{comfort} \] Switch if Net Savings ( \> 0 ).

# 7. Summary Table

| Step | Action                         | OCHRE Integration                 |
|------|--------------------------------|-----------------------------------|
| 1    | Simulate default schedule      | Run with normal HPWH control      |
| 2    | Simulate TOU-adaptive schedule | Run with off-peak-restricted HPWH |
| 3    | Compare bills and comfort      | Use OCHRE output variables above  |
| 4    | Decision to switch?            | Apply decision rule               |
| 5    | Repeat monthly                 | Feedback loop                     |

# 8. References

- [OCHRE Inputs and
  Arguments](https://github.com/NREL/OCHRE/blob/main/docs/source/InputsAndArguments.rst)
- [OCHRE
  Outputs](https://github.com/NREL/OCHRE/blob/main/docs/source/Outputs.rst)
- [OCHRE Water
  Model](https://github.com/NREL/OCHRE/blob/main/ochre/Models/Water.py)
- [HPWH Control
  Logic](https://github.com/NREL/OCHRE/blob/main/ochre/Equipment/WaterHeater.py)

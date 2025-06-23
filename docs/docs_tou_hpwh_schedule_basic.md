---
title: First-Pass TOU Scheduling Decision Model for HPWHs in OCHRE
author: Switchbox
date: '2025-06-23'
format:
  gfm:
    html-math-method: mathjax
    variant: gfm+tex_math_dollars+pipe_tables
    preserve-yaml: true
    code-fold: true
    prefer-html: false
---


# First-Pass TOU Scheduling Decision Model for HPWHs in OCHRE
Switchbox
2025-06-23

# Introduction

This document outlines a simple, first-pass heuristic-based decision
model for consumer response to time-of-use (TOU) electricity rates in
residential building simulations using OCHRE, focusing on Heat Pump
Water Heaters (HPWHs). The goal is to integrate an adaptive control
schedule for the HPWH based on feedback from utility bills and
anticipated cost savings, while accounting for consumer effort in
shifting schedules.

# 1. Problem Definition

This section gives a high-level overview of the problem being addressed,
including the objectives, assumptions, and the context in which the
model operates.

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

Here, we describe the main variables and parameters used in the model,
providing definitions and units to clarify their roles in the
methodology.

| Symbol | Description | Units |
|----|----|----|
| $S$ | Current HPWH schedule (default or TOU-adapted) | binary |
| $U_{HW}$ | Exogenous hot water usage schedule | L/hr |
| $\lambda$ | Usage multiplier (building characteristic) | scalar |
| $E$ | Electricity consumption profile (from HPWH operation) | kWh |
| $r_{on}, r_{off}$ | TOU electricity rate (on-peak, off-peak) | \$/kWh |
| $C_{bill}$ | Monthly electricity bill (for water heating) | \$ |
| $C_{switch}$ | Consumer switching/“hassle” cost | \$ |
| $\Delta C$ | Anticipated bill savings from switching | \$ |

# 3. Model Steps

This section details the step-by-step process of the decision model,
from baseline calculation to feedback and iteration, explaining how each
step contributes to the overall decision-making process.

## Step 1: Calculate Baseline

- **Hot Water Usage:** $U_{HW}(t)$ is fixed by schedule and multiplier.
- **HPWH Default Schedule:** Operates as needed to meet hot water draws.
- **Simulate:** Run OCHRE with default HPWH schedule to get
  $E_{default}(t)$, $C_{bill,default}$.

## Step 2: Define TOU-Adaptive Schedule

- **HPWH TOU Schedule:** Only operates HPWH during off-peak hours
  (unless tank runs cold).
- **Simulate:** Run OCHRE with TOU-adaptive HPWH schedule to get
  $E_{TOU}(t)$, $C_{bill,TOU}$.

## Step 3: Anticipated Savings

- **Calculate:**

$$
\Delta C = C_{bill,default} - C_{bill,TOU}
$$

- **Net Benefit:**

$$
\text{Net Savings} = \Delta C - C_{switch}
$$

## Step 4: Decision Rule

- **If** $\text{Net Savings} > 0$: Consumer switches to TOU-adapted
  schedule.
- **Else:** Remains on default schedule.

## Step 5: Feedback & Iteration

- At each monthly billing cycle, repeat Steps 1–4 using updated
  consumption and bills.
- Consumer can reconsider switching if conditions change (e.g., rate
  structure, switching cost).

# 4. Integration with OCHRE

This section explains how the model interfaces with OCHRE, specifying
required inputs, outputs used, and implementation notes for practical
integration.

## Inputs Required

- Hot water usage schedule: `HotWaterUsageSchedule`
- Usage multiplier: `UsageMultiplier`
- TOU rate definition: $r_{on}, r_{off}$ (passed to cost calculation)
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

An example calculation is provided in this section to illustrate how the
model works in practice, using sample values for clarity.

Suppose:

- Default schedule: HPWH runs as needed.
- TOU schedule: HPWH only runs 10PM–6AM (off-peak).
- Monthly bill (default): \$30
- Monthly bill (TOU): \$22
- Switching cost: \$5 (one-time or amortized)

$$
\Delta C = 30 - 22 = \$8
$$

$$
\text{Net Savings} = 8 - 5 = \$3
$$

**Decision:** Consumer switches to TOU-adaptive schedule.

# 6. Equations

This section presents the key equations used in the model, including
cost and comfort penalty calculations, to formalize the methodology.

## Electricity Cost Calculation

$$
C_{bill} = \sum_{t} E(t) \cdot r(t)
$$

Where:

- $E(t)$: Electricity used by HPWH at time $t$
- $r(t)$: Rate (on-peak/off-peak) at time $t$

## Comfort Penalty for Unmet Hot Water Demand

If, under the TOU-adapted HPWH schedule, the consumer experiences unmet
hot water demand (i.e., water is delivered below setpoint temperature),
they incur a penalty reflecting discomfort or service loss. This penalty
is computed as:

$$
C_{comfort} = \alpha \cdot \left( \sum_{t} \text{Unmet Demand}(t) \right)
$$

Where:

- $\alpha$: Monetization factor for comfort loss (e.g., \$/kWh unmet or
  \$/event)
- $\text{Unmet Demand}(t)$: Unmet hot water demand per time step (from
  OCHRE output)

At each billing cycle, the consumer’s net savings from remaining on the
TOU-adapted schedule is:

$$
\text{Net Savings} = \Delta C - C_{switch} - C_{comfort}
$$

If Net Savings ≤ 0, the consumer switches back to the default schedule.

This approach ensures the model realistically captures the trade-off
between cost savings and comfort when responding to TOU rates.

## Final Decision

$$
\text{Net Savings} = \Delta C - C_{switch} - C_{comfort}
$$

Switch if Net Savings $> 0$.

# 7. Summary Table

A summary table is provided here to concisely outline the main steps and
their integration with OCHRE.

| Step | Action                         | OCHRE Integration                 |
|------|--------------------------------|-----------------------------------|
| 1    | Simulate default schedule      | Run with normal HPWH control      |
| 2    | Simulate TOU-adaptive schedule | Run with off-peak-restricted HPWH |
| 3    | Compare bills and comfort      | Use OCHRE output variables above  |
| 4    | Decision to switch?            | Apply decision rule               |
| 5    | Repeat monthly                 | Feedback loop                     |

# 8. References

This section lists references and resources for further information and
context regarding the model and its implementation.

- [OCHRE Inputs and
  Arguments](https://github.com/NREL/OCHRE/blob/main/docs/source/InputsAndArguments.rst)
- [OCHRE
  Outputs](https://github.com/NREL/OCHRE/blob/main/docs/source/Outputs.rst)
- [OCHRE Water
  Model](https://github.com/NREL/OCHRE/blob/main/ochre/Models/Water.py)
- [HPWH Control
  Logic](https://github.com/NREL/OCHRE/blob/main/ochre/Equipment/WaterHeater.py)

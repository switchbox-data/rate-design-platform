# Hot Water Demand Time Series Extraction
Rate Design Platform Team
2025-06-24

# Hot Water Demand Time Series Extraction

## Overview

This document describes the methodology for extracting and calculating
time series of hot water demand from ResStock simulation data,
specifically for heat pump water heater (HPWH) analysis in time-of-use
(TOU) rate design studies.

## Background

Understanding hot water usage patterns is critical for modeling
behavioral responses to TOU electricity rates. When HPWHs are curtailed
during peak periods, water temperature may drop, creating comfort
penalties for consumers. To quantify these penalties, we need detailed
time series of hot water demand.

## Calculation Framework and Notation

### Key Variables (Consistent with TOU Schedule Model)

| Symbol | Type | Description | Units | Dimension |
|----|----|----|----|----|
| **Sets** |  |  |  |  |
| $M$ | Set | Months in simulation year, $m \in \{1, 2, ..., 12\}$ | \- | 12 × 1 |
| $T$ | Set | Time periods in billing month, $t \in \{1, 2, ..., T\}$ where $T \approx 2976$ | \- |  |
| $H$ | Set | Peak hour periods, $H \subset T$ | \- |  |
| **Parameters** |  |  |  |  |
| $U_{m,t}^{HW}$ | Parameter | Exogenous hot water usage schedule at time $t$ in month $m$ | L/15min | M × T |
| $T_{m,t}^{setpoint}$ | Parameter | Hot water temperature setpoint at time $t$ in month $m$ | °C | M × T |
| $T_{m,t}^{ambient}$ | Parameter | Ambient water temperature at time $t$ in month $m$ | °C | M × T |
| $\rho$ | Parameter | Water density | kg/L | 1 × 1 |
| $c_p$ | Parameter | Specific heat of water | J/kg·°C | 1 × 1 |
| $COP$ | Parameter | Heat pump coefficient of performance | \- | 1 × 1 |
| **State Variables** |  |  |  |  |
| $T_{m,t}^{tank}$ | Variable | Tank water temperature at time $t$ in month $m$ | °C | M × T |
| $Q_{m,t}^{unmet}$ | Variable | Thermal unmet demand at time $t$ in month $m$ | J/15min | M × T |
| $D_{m,t}^{unmet}$ | Variable | Electrical equivalent unmet demand at time $t$ in month $m$ | kWh/15min | M × T |

## Data Sources and Components

### Input Data Structure

The hot water demand time series is constructed from three primary data
sources:

1.  **Schedule Files** (`res_2022_tmy3_1.1` ResStock release)
    - Hot water fixture schedules for each `bldg_id`
    - 15-minute interval data (35,040 intervals per year)
    - Normalized profiles representing timing and relative intensity -
      tells us when hot water is used throughout each time interval $t$
      and what % is used relative to the total daily hot water demand.
2.  **HPXML Files**
    - Building specifications and water heater parameters
    - Tank capacity, setpoint temperatures, and system configurations
    - Usage multipliers for scaling baseline demand, which converts
      normalized schedule to actual water volumes.
    - Accounts for household size and other building characteristics
3.  **Weather Data (EPW Files)**
    - Ambient temperature conditions $T_{m,t}^{ambient}$
    - Required for heat pump performance modeling

## Calculation Methodology

### Step 1: Water Draw Calculation

Hot water draws are calculated by combining schedule and usage data:

$$
U_{m,t}^{HW} = \text{Normalized\_Schedule}_{m,t} \times \text{Usage\_Multiplier}_m \times \text{Daily\_Base\_Demand}_m
$$

Where the normalized schedule provides temporal distribution and the
usage multiplier scales to building-specific consumption levels.

### Step 2: OCHRE Simulation Process

The OCHRE building physics framework processes water draws through:

1.  **Exogenous Water Demand**
    - Water usage $U_{m,t}^{HW}$ is specified by schedule (not
      responsive to system state)
    - Represents actual household consumption patterns
    - Independent of tank temperature $T_{m,t}^{tank}$ or heater
      availability
2.  **Tank Temperature Modeling**
    - Simulates water heater tank temperature stratification
      $T_{m,t}^{tank}$
    - Models thermal dynamics during draw events
    - Accounts for heat pump performance characteristics
3.  **Outlet Temperature Calculation**
    - Determines actual delivered water temperature
    - Based on tank conditions and draw magnitude
    - Varies with system operating state

### Step 3: Unmet Demand Calculation

For TOU analysis, “unmet demand” quantifies comfort penalties:

$$
Q_{m,t}^{\text{unmet}} = \begin{cases}
U_{m,t}^{\text{HW}} \cdot \rho \cdot c_p \cdot (T_{m,t}^{\text{setpoint}} - T_{m,t}^{\text{tank}}) & \text{if } T_{m,t}^{\text{tank}} < T_{m,t}^{\text{setpoint}} \text{ and } U_{m,t}^{\text{HW}} > 0 \\
0 & \text{otherwise}
\end{cases}
$$

**Convert to Electrical Equivalent:**

$$
D_{m,t}^{unmet} = \frac{Q_{m,t}^{unmet}}{COP \cdot 3,600,000}
$$

Where the factor 3,600,000 converts Joules to kWh.

## Output Time Series

The extraction process generates:

1.  **Hot Water Volume Time Series**
    - 15-minute interval data $U_{m,t}^{HW}$ for each building
    - Units: L/15min
    - Annual profiles (35,040 data points)
2.  **Temperature Profiles**
    - Tank temperature $T_{m,t}^{tank}$ over time
    - Setpoint tracking performance against $T_{m,t}^{setpoint}$
3.  **Unmet Demand Time Series**
    - Electrical equivalentof thermal energy shortfall $D_{m,t}^{unmet}$
      for comfort penalty calculations in W

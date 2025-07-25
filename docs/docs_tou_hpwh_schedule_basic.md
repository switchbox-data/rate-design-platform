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

This section establishes a high-level overview of the core research
question and modeling assumptions for consumer response to TOU rates.

**Objective:**

Model how a consumer, with fixed (exogenous) hot water usage needs,
might reschedule their HPWH operation in response to TOU rates to
minimize their electricity bills, considering a “switching cost” for
effort/comfort.

**Assumptions:**

- Hot water usage schedule is fixed (not flexible) and set by inputs to
  the model.

- HPWH can be controlled (on/off) on a schedule.

- TOU rate structure (on-peak/off-peak) is known and simple (e.g.,
  higher price during peak hours), such that the consumer can reasonably
  set a schedule to turn on HPWH during off-peak hours.

- Consumer receives feedback on energy cost and makes switching
  decisions at bill receipt each billing cycle (monthly) rather than at
  each operational time step (every 15 minutes).

- Switching to a TOU-adapted schedule incurs a one-time “effort cost”
  (can be fixed or parameterized).

- Decision process is repeated each billing cycle (feedback loop), with
  the decision model output feeding into the next iteration of the
  simulation.

# 2. Key Variables and Parameters

This section defines all variables, parameters, and their temporal
dimensions used throughout the decision model. Note that at the
beginning of each month, the model is initialized with the previous
month’s decision outcome, and the model is repeated for each month of
the year, or however many months are specified in the simulation.

| Symbol | Type | Description | Units | Dimension |
|----|----|----|----|----|
| **Sets** |  |  |  |  |
| $M$ | Set | Months in simulation year, $m \in \{1, 2, ..., 12\}$ | \- | 12 × 1 |
| $T$ | Set | Time periods in billing month, $t \in \{1, 2, ..., T\}$ where $T \approx 2976$ (15-min intervals) | \- | \|T\| × 1 |
| $H$ | Set | Peak hour periods, $H \subset T$ | \- | \|H\| × 1 |
| **Parameters** |  |  |  |  |
| $U_{m,t}^{HW}$ | Parameter | Exogenous hot water usage schedule at time $t$ in month $m$ | L/15min | M × T |
| $r^{on}$ | Parameter | TOU electricity rate during peak hours | \$/kWh | 1 × 1 |
| $r^{off}$ | Parameter | TOU electricity rate during off-peak hours | \$/kWh | 1 × 1 |
| $C^{switch}$ | Parameter | Consumer switching/“hassle” cost per schedule change | \$ | 1 × 1 |
| $\alpha$ | Parameter | Monetization factor for comfort penalty | \$/kWh | 1 × 1 |
| $T_{m,t}^{setpoint}$ | Parameter | Hot water temperature setpoint at time $t$ in month $m$ | °C | M × T |
| $T_{m,t}^{ambient}$ | Parameter | Ambient water temperature at time $t$ in month $m$ | °C | M × T |
| $\rho$ | Parameter | Water density | kg/L | 1 × 1 |
| $c_p$ | Parameter | Specific heat of water | J/kg·°C | 1 × 1 |
| $COP$ | Parameter | Heat pump coefficient of performance | \- | 1 × 1 |
| **Decision Variables** |  |  |  |  |
| $x_m^{switch}$ | Binary | Decision to switch schedule in month $m$ (1 = switch, 0 = stay) | binary | M × 1 |
| **State Variables** |  |  |  |  |
| $S_m^{current}$ | Binary | Current schedule state in month $m$ (1 = default, 0 = TOU-adapted) | binary | M × 1 |
| $s_{m,t}$ | Binary | HPWH operation permission at time $t$ in month $m$ (1 = allowed, 0 = restricted) | binary | M × T |
| $r_{m,t}$ | Variable | Electricity rate at time $t$ in month $m$ (determined by peak/off-peak) | \$/kWh | M × T |
| $E_{m,t}$ | Variable | Electricity consumption from HPWH operation at time $t$ in month $m$ | kWh/15min | M × T |
| $T_{m,t}^{tank}$ | Variable | Tank water temperature at time $t$ in month $m$ | °C | M × T |
| $Q_{m,t}^{unmet}$ | Variable | Thermal unmet demand at time $t$ in month $m$ | J/15min | M × T |
| $D_{m,t}^{unmet}$ | Variable | Electrical equivalent unmet demand at time $t$ in month $m$ | kWh/15min | M × T |
| **Derived Variables** |  |  |  |  |
| $C_m^{bill}$ | Variable | Monthly electricity bill for water heating in month $m$ | \$ | M × 1 |
| $C_m^{comfort}$ | Variable | Monthly comfort penalty from unmet demand in month $m$ | \$ | M × 1 |
| $\Delta C_m$ | Variable | Realized bill savings from TOU schedule vs. default in month $m$ | \$ | M × 1 |

# 3. Detailed Model Steps

This section outlines the complete sequential decision-making process
that consumers follow each month, including the initialization of all
state variables, decision logic, and state transitions. The model
operates on monthly billing cycles with two distinct decision contexts
based on the consumer’s current schedule state.

## Step 1: Initialize Monthly State Variables

This step loads the exogenous input data and sets the initial state
variables for month $m$’s simulation. The hot water usage profile
$U_{m,t}^{HW}$ defines when and how much hot water is demanded
throughout the month’s 2976 time periods. Temperature setpoints
$T_{m,t}^{setpoint}$ and ambient conditions $T_{m,t}^{ambient}$
establish the thermal boundary conditions for month $m$. The electricity
rate vector $r_{m,t}$ is constructed by mapping peak hours set $H$ to
the on-peak rate $r^{on}$ and all other periods to off-peak rate
$r^{off}$.

**Set Time-Varying Parameters for Month $m$:**

- Load hot water usage schedule: $U_{m,t}^{HW}$ for all $t \in T$

- Load temperature profiles: $T_{m,t}^{setpoint}$, $T_{m,t}^{ambient}$
  for all $t \in T$

- Set electricity rates: $r_{m,t} = r^{on}$ if $t \in H$, else
  $r_{m,t} = r^{off}$

**Initialize Schedule State for Month $m$:**

- If $m = 1$: set $S_m^{current} = 1$ (start on default schedule)

- Else: $S_m^{current} = S_{m-1}^{current,next}$ (use previous month’s
  decision outcome)

**Set Operational Schedule for Month $m$:**

The binary operation permission vector $s_{m,t}$ is derived from the
current schedule state $S_m^{current}$. When $S_m^{current} = 1$
(default), the HPWH can operate whenever needed ($s_{m,t} = 1$ for all
$t$). When $S_m^{current} = 0$ (TOU-adapted), operation is restricted
during peak hours ($s_{m,t} = 0$ when $t \in H$).

$$
s_{m,t} = \begin{cases}
1 & \text{if } S_m^{current} = 1 \text{ (default: always allowed)} \\
1 & \text{if } S_m^{current} = 0 \text{ and } t \notin H \text{ (TOU: off-peak only)} \\
0 & \text{if } S_m^{current} = 0 \text{ and } t \in H \text{ (TOU: peak restricted)}
\end{cases}
$$

## Step 2: Run OCHRE Simulation for Month $m$

OCHRE executes the building physics simulation for month $m$ using the
operational schedule $s_{m,t}$ as a constraint on HPWH operation. For
each 15-minute interval $t$ in month $m$, OCHRE determines whether the
HPWH can operate based on $s_{m,t}$, then calculates the resulting
electricity consumption $E_{m,t}$ and tank temperature $T_{m,t}^{tank}$
considering hot water draws $U_{m,t}^{HW}$, thermal losses, and ambient
conditions $T_{m,t}^{ambient}$. The monthly electricity bill is computed
by summing the product of consumption and time-varying rates across all
time periods in month $m$.

**Execute Monthly Simulation for Month $m$:**

- Input: $U_{m,t}^{HW}$, $s_{m,t}$, $T_{m,t}^{setpoint}$,
  $T_{m,t}^{ambient}$ for all $t \in T$

- Output: $E_{m,t}$, $T_{m,t}^{tank}$ for all $t \in T$

**Calculate Monthly Electricity Bill for Month $m$:**

$$
C_m^{bill} = \sum_{t \in T} E_{m,t} \cdot r_{m,t}
$$

Note that this bill is specific to the HPWH, and does not include other
electricity loads in the building. In practice, consumers get a bill
that includes all of their electricity usage, and the HPWH bill is a
subset of that. However, since other operations remain the same, and we
are only changing the HPWH, we don’t need to include other loads in the
model.

## Step 3: Assess Comfort Performance for Month $m$

Comfort assessment for month $m$ begins by identifying time periods
where tank temperature $T_{m,t}^{tank}$ falls below the setpoint
$T_{m,t}^{setpoint}$ during hot water usage events ($U_{m,t}^{HW} > 0$).
For each such period, the thermal energy shortfall $Q_{m,t}^{unmet}$ is
calculated as the energy required to heat the delivered water from tank
temperature to setpoint temperature, using water density $\rho$ and
specific heat $c_p$. This thermal deficit is then converted to
electrical energy equivalent $D_{m,t}^{unmet}$ by dividing by the heat
pump’s coefficient of performance $COP$ and converting from Joules to
kWh. The total comfort penalty for month $m$, $C_m^{comfort}$, monetizes
these electrical energy equivalents using the comfort parameter
$\alpha$.

**Calculate Thermal Unmet Demand for Month $m$:**

$$
Q_{m,t}^{\text{unmet}} = \begin{cases}
U_{m,t}^{\text{HW}} \cdot \rho \cdot c_p \cdot (T_{m,t}^{\text{setpoint}} - T_{m,t}^{\text{tank}}) & \text{if } T_{m,t}^{\text{tank}} < T_{m,t}^{\text{setpoint}} \text{ and } U_{m,t}^{\text{HW}} > 0 \\
0 & \text{otherwise}
\end{cases}
$$

**Convert to Electrical Equivalent for Month $m$:**

$$
D_{m,t}^{unmet} = \frac{Q_{m,t}^{unmet}}{COP \cdot 3,600,000}
$$

**Calculate Monthly Comfort Penalty for Month $m$:**

$$
C_m^{comfort} = \alpha \cdot \sum_{t \in T} D_{m,t}^{unmet}
$$

## Step 4: Decision Logic Branch for Month $m$

The decision logic branches based on the current schedule state
$S_m^{current}$, implementing different information sets and decision
criteria for consumers in different situations. See the end of this
section for a discussion regarding how consumers might calculate their
net savings with the information they have in practice, and how this
might differ from the model’s assumptions.

**Branch based on current schedule state $S_m^{current}$:**

### Case A: Currently on Default Schedule ($S_m^{current} = 1$)

Consumers on the default schedule in month $m$ evaluate TOU adoption by
simulating the alternative schedule and comparing anticipated costs.
Since they have no experience with TOU operation, the decision excludes
comfort penalties $C_m^{comfort}$, which are unknown at this stage.

**Step 4A.1: Simulate Alternative (TOU) Schedule for Month $m$**

A temporary TOU schedule is created for month $m$ by setting the
operational permissions to restrict peak-hour operation. OCHRE simulates
this alternative schedule using the same input conditions
($U_{m,t}^{HW}$, $T_{m,t}^{setpoint}$, $T_{m,t}^{ambient}$) but with the
modified operational constraints. This produces the electricity
consumption profile $E_{m,t}^{TOU}$ that would result under TOU
scheduling in month $m$.

- Temporarily set $S_m^{temp} = 0$
- Set TOU operational schedule: $s_{m,t}^{temp} = 1$ if $t \notin H$,
  else $s_{m,t}^{temp} = 0$
- Run OCHRE simulation to get $E_{m,t}^{TOU}$
- Calculate:
  $C_m^{bill,TOU} = \sum_{t \in T} E_{m,t}^{TOU} \cdot r_{m,t}$

**Step 4A.2: Calculate Anticipated Savings for Month $m$**

The anticipated bill savings $\Delta C_m^{anticipated}$ represent the
difference between current default schedule costs and projected TOU
schedule costs for month $m$. The net anticipated benefit subtracts the
one-time switching cost $C^{switch}$ but does not include comfort
penalties since the consumer cannot anticipate these impacts.

$$
\Delta C_m^{anticipated} = C_m^{bill} - C_m^{bill,TOU}
$$

$$
\text{Net Savings}_m^{anticipated} = \Delta C_m^{anticipated} - C^{switch}
$$

**Step 4A.3: Make Switching Decision for Month $m$**

The binary switching decision $x_m^{switch}$ is determined by whether
anticipated net savings are positive in month $m$. If
$\text{Net Savings}_m^{anticipated} > 0$, the consumer adopts TOU
scheduling ($x_m^{switch} = 1$); otherwise, they remain on the default
schedule ($x_m^{switch} = 0$).

$$
x_m^{switch} = \begin{cases}
1 & \text{if } \text{Net Savings}_m^{anticipated} > 0 \\
0 & \text{otherwise}
\end{cases}
$$

### Case B: Currently on TOU Schedule ($S_m^{current} = 0$)

Consumers already on TOU scheduling in month $m$ have experienced both
financial and comfort impacts during the current month. Their
continuation decision incorporates complete information including
realized comfort penalties $C_m^{comfort}$.

**Step 4B.1: Simulate Alternative (Default) Schedule for Month $m$**

The alternative default schedule simulation determines what electricity
costs would have been in month $m$ without TOU restrictions. All
operational permissions are set to allow HPWH operation
($s_{m,t}^{temp} = 1$ for all $t$), and OCHRE simulates the resulting
consumption $E_{m,t}^{default}$ and costs $C_m^{bill,default}$.

- Temporarily set $S_m^{temp} = 1$
- Set default operational schedule: $s_{m,t}^{temp} = 1$ for all
  $t \in T$
- Run OCHRE simulation to get $E_{m,t}^{default}$
- Calculate:
  $C_m^{bill,default} = \sum_{t \in T} E_{m,t}^{default} \cdot r_{m,t}$

**Step 4B.2: Calculate Realized Performance for Month $m$**

The realized savings $\Delta C_m^{realized}$ compare the counterfactual
default costs to actual TOU costs experienced in month $m$. The net
realized savings subtract both the switching cost $C^{switch}$ and the
comfort penalty $C_m^{comfort}$ that was actually experienced during TOU
operation.

$$
\Delta C_m^{realized} = C_m^{bill,default} - C_m^{bill}
$$

$$
\text{Net Savings}_m^{realized} = \Delta C_m^{realized} - C^{switch} - C_m^{comfort}
$$

**Step 4B.3: Make Continuation Decision for Month $m$**

The continuation decision evaluates whether to remain on TOU scheduling
based on complete cost information from month $m$. If realized net
savings are non-positive ($\text{Net Savings}_m^{realized} \leq 0$), the
consumer switches back to default scheduling ($x_m^{switch} = 1$);
otherwise, they continue with TOU ($x_m^{switch} = 0$).

$$
x_m^{switch} = \begin{cases}
1 & \text{if } \text{Net Savings}_m^{realized} \leq 0 \text{ (switch back to default)} \\
0 & \text{otherwise (stay on TOU)}
\end{cases}
$$

## Step 5: Update State for Next Month

The schedule state $S_{m+1}^{current}$ for the next month is determined
by the switching decision $x_m^{switch}$ made in month $m$. If switching
occurs ($x_m^{switch} = 1$), the state toggles to its opposite value
($1 - S_m^{current}$). If no switching occurs ($x_m^{switch} = 0$), the
state remains unchanged. Monthly results for month $m$ including
$C_m^{bill}$, $C_m^{comfort}$, $x_m^{switch}$, and $S_m^{current}$ are
recorded for annual analysis.

**Update Schedule State for Month $m+1$:**

$$
S_{m+1}^{current} = \begin{cases}
1 - S_m^{current} & \text{if } x_m^{switch} = 1 \\
S_m^{current} & \text{if } x_m^{switch} = 0
\end{cases}
$$

**Store Monthly Results for Month $m$:**

- Record: $C_m^{bill}$, $C_m^{comfort}$, $x_m^{switch}$, $S_m^{current}$

- Save for annual analysis and next month’s initialization

## Step 6: Monthly Iteration Control

The simulation checks whether the annual cycle is complete. If the
current month $m < 12$, the month counter increments and the process
returns to Step 1 with month $m+1$ and the updated schedule state
$S_{m+1}^{current}$. If month 12 is complete, the simulation proceeds to
annual evaluation metrics calculation.

**Check Simulation Status:**

- If $m < 12$: increment to month $m+1$, return to Step 1 with
  $S_{m+1}^{current}$

- If $m = 12$: proceed to annual evaluation (Step 7)

## Step 7: Annual Evaluation and State Reset

For multi-year simulations, the final month’s schedule state
$S_{13}^{current}$ becomes the initial state for the following year’s
first month, allowing persistence of consumer preferences across years.
Before resetting for the next annual cycle, comprehensive evaluation
metrics are calculated and key visualizations are generated to assess
model performance and consumer behavior patterns.

### Step 7.1: Calculate Annual Performance Metrics

**Financial Performance:**

$$
\text{Total Annual Savings} = \sum_{m=1}^{12} \Delta C_m^{realized}
$$

$$
\text{Total Switching Costs} = \sum_{m=1}^{12} x_m^{switch} \cdot C^{switch}
$$

$$
\text{Total Comfort Penalty} = \sum_{m=1}^{12} C_m^{comfort}
$$

$$
\text{Net Annual Benefit} = \text{Total Annual Savings} - \text{Total Switching Costs} - \text{Total Comfort Penalty}
$$

**Behavioral Metrics:**

$$
\text{TOU Adoption Rate} = \frac{\sum_{m=1}^{12} (1 - S_m^{current})}{12} \times 100\%
$$

$$
\text{Annual Switches} = \sum_{m=1}^{12} x_m^{switch}
$$

$$
\text{Average Monthly Bill} = \frac{1}{12} \sum_{m=1}^{12} C_m^{bill}
$$

**System Performance:**

$$
\text{Peak Load Reduction} = \frac{\sum_{m=1}^{12} \sum_{t \in H} (E_{m,t}^{baseline} - E_{m,t})}{\sum_{m=1}^{12} \sum_{t \in H} E_{m,t}^{baseline}} \times 100\%
$$

### Step 7.2: Generate Key Visualizations

**A. Annual Decision Timeline**

- Line plot showing $S_m^{current}$ across months with switching events
  $x_m^{switch}$ marked

- Purpose: Visualize adoption patterns and decision stability

**B. Monthly Cost Decomposition**

- Stacked bar chart with $C_m^{bill}$, $C_m^{comfort}$, and switching
  costs for each month

- Purpose: Show relative impact of each cost component

**C. Performance Scatter Plot**

- X-axis: $C_m^{comfort}$, Y-axis: $\Delta C_m^{realized}$, color-coded
  by $S_m^{current}$

- Purpose: Identify trade-offs between savings and comfort

**D. Load Profile Heatmap**

- 2D plot: months (x-axis) vs. hours (y-axis), color intensity = average
  $E_{m,t}$

- Purpose: Visualize seasonal and daily load shifting patterns

### Step 7.3: Reset for Next Year

**Prepare for Next Year:**

- Set $S_1^{current} = S_{13}^{current}$ (carry forward final state)

- Clear monthly arrays:
  $\{C_m^{bill}, C_m^{comfort}, x_m^{switch}, S_m^{current}\}_{m=1}^{12}$

- Update annual parameters (e.g., rate changes, equipment degradation)

- Export annual metrics to results database

- Return to Step 1 for new annual cycle with $m = 1$

This evaluation framework provides both quantitative metrics for model
validation and intuitive visualizations for understanding consumer
behavior patterns and system-wide impacts.

## State space diagram

``` mermaid
stateDiagram-v2
  [*] --> S_default: "First month"
  S_default --> S_TOU: Switch to TOU<br/>Net Anticipated Savings > 0
  S_default --> S_default: Stay<br/>Net Anticipated Savings ≤ 0
  S_TOU --> S_default: Switch back to Default<br/>Net Realized Savings ≤ 0
  S_TOU --> S_TOU: Stay<br/>Net Realized Savings > 0
  S_default: Default Schedule (S(current)=1)
  S_TOU: TOU-adapted Schedule (S(current)=0)
```

## Consumer Information and Decision-Making Reality

### How Consumers Actually Estimate HPWH Schedule Switching Benefits in Practice

In the real world, consumers who are already on TOU rates cannot run
sophisticated building simulations to predict whether changing their
water heater schedule will save money. Instead, they rely on simplified
mental models, basic calculations, and trial-and-error to estimate
potential benefits from shifting their HPWH operation to off-peak hours.

#### **The Typical Consumer Journey**

**Monthly Bill Review Phase:**

Sarah receives her TOU electricity bill showing \$92 this month, with a
breakdown: \$38 from peak hours (2-8 PM) and \$54 from off-peak. She
notices her electric water heater is one of her largest energy users.
She wonders: “What if I could get my water heater to run mostly during
off-peak times?”

**Basic Calculation Attempt:**

Sarah looks at her bill and sees she’s paying \$0.28/kWh during peak
vs. \$0.12/kWh off-peak. She estimates her water heater uses about 800
kWh/month and reasons: “If half of that is currently happening during
peak hours, that’s 400 kWh × (\$0.28 - \$0.12) = \$64 potential savings
per month. But realistically, maybe I can shift 70% of peak usage to
off-peak, so perhaps \$45 savings?”

**Social Information Gathering:**

At work, Sarah asks her colleague Tom about his programmable water
heater. Tom says: “I set mine to heat from 10 PM to 6 AM. My bill went
down maybe \$20-25 per month, though sometimes the water isn’t quite as
hot for evening dishes. It’s a trade-off, but worth it for the savings.”

**Switching Cost Assessment:**

Sarah considers the total effort required: analyzing her current usage
patterns (2 hours), figuring out the water heater programming (2-3
hours), and periodically checking bills to ensure it’s working. She
estimates this represents about \$35 worth of her time and mental
energy. With potential monthly savings of \$20-30, the payback period is
reasonable: “Even if I only save \$20/month, that’s \$240/year for maybe
5 hours of total effort.”

#### **What Consumers Actually Calculate**

**Peak Usage Estimation:**

- Look at TOU bill breakdown or estimate major appliance usage during
  peak hours

- Apply simple fractions: “Maybe 40% of my water heating happens during
  peak time”

**Rate Differential Application:**

- Calculate potential savings as: (estimated peak kWh) × (peak rate -
  off-peak rate) × (shifting efficiency)

- Use round numbers: “If I shift 300 kWh from \$0.28 to \$0.12, that’s
  \$48 savings”

**Potential Switching Cost Components:**

- **Information cost**: Time analyzing bills and researching strategies

- **Implementation cost**: Programming water heater controls and
  trial-and-error

- **Monitoring cost**: Ongoing bill checking and adjustments

#### **Implications for Model Design**

This realistic decision-making process suggests consumers estimate
$\Delta C^{anticipated}$ using:

$$
\hat{\Delta C}_m^{anticipated} = \hat{E}_m^{peak,WH} \times (r^{on} - r^{off}) \times \phi
$$

Where:

- $\hat{E}_m^{peak,WH}$ = consumer’s guess of peak-hour water heating
  usage

- $\phi$ = expected shifting effectiveness (0.6-0.8)

And evaluate switching against realistic transaction costs:

$$
C^{switch} = \$35 \text{ (representing 3-5 hours of consumer effort)}
$$

The model should reflect that consumers make switching decisions based
on rough mental calculations and significant uncertainty about both
benefits and comfort impacts, not sophisticated building physics
simulations.

# Examples

![Water Heating Electric Power Comparison: Default vs TOU
Schedule](docs_tou_hpwh_schedule_basic_files/figure-commonmark/plot-water-heating-comparison-output-1.png)

![Hot Water Outlet Temperature and TOU Rates Comparison: Default vs TOU
Schedule](docs_tou_hpwh_schedule_basic_files/figure-commonmark/plot-temperature-comparison-output-1.png)

![Monthly Electricity Bills with TOU State
Highlighting](docs_tou_hpwh_schedule_basic_files/figure-commonmark/plot-monthly-bills-output-1.png)

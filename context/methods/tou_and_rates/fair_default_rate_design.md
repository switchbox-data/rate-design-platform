# Fair default rate design: math and closed-form strategies

How to redesign a residential **default tariff** — a single tariff applied to every residential customer — so that it (a) collects the class revenue requirement and (b) eliminates the cross-subsidy of one identified subclass (e.g. heat-pump customers) measured by the Bill Alignment Test. The full design problem reduces to a $2 \times 3$ linear system; closing the one remaining degree of freedom three different ways gives three closed-form strategies (fixed-charge only, seasonal-rates only, cost-reflective combined).

This is the math behind issue #398 and the [`utils/mid/compute_fair_default_inputs.py`](utils/mid/compute_fair_default_inputs.py) / [`utils/mid/create_fair_default_tariff.py`](utils/mid/create_fair_default_tariff.py) modules.

## Contents

1. [Why a default tariff (and not a subclass-specific one)](#1-why-a-default-tariff-and-not-a-subclass-specific-one)
2. [Setup and notation](#2-setup-and-notation)
3. [The two design constraints](#3-the-two-design-constraints)
4. [Strategy A: fixed-charge only (flat volumetric)](#4-strategy-a-fixed-charge-only-flat-volumetric)
5. [Strategy B: seasonal rates only (preserve fixed charge)](#5-strategy-b-seasonal-rates-only-preserve-fixed-charge)
6. [Strategy C: combined, with a cost-reflective seasonal ratio](#6-strategy-c-combined-with-a-cost-reflective-seasonal-ratio)
7. [Geometry of the solution space, and the uniqueness theorem](#7-geometry-of-the-solution-space-and-the-uniqueness-theorem)
8. [Feasibility region](#8-feasibility-region)
9. [Worked example](#9-worked-example)
10. [Limitations and extensions](#10-limitations-and-extensions)
11. [Cross-references](#11-cross-references)

---

## 1. Why a default tariff (and not a subclass-specific one)

The existing seasonal-discount workflow ([`utils/mid/compute_seasonal_discount_inputs.py`](utils/mid/compute_seasonal_discount_inputs.py)) eliminates the heat-pump (HP) cross-subsidy by giving HP customers a _separate_ tariff. It works analytically but is regulatorily awkward: utilities have to identify HP customers in the meter database, defend a separate class in rate cases, and operate dual billing systems.

A **fair default** tariff is one tariff applied to the whole residential class whose structure is chosen so that the target subclass (HP, electric-resistance, EV-only — anything BAT-measurable) nets to zero cross-subsidy automatically. The price of that singularity is a smaller design space: the only knobs are

- the fixed charge $F$ (\$/customer/month), and
- the volumetric rate(s): one flat $r$ (\$/kWh), or two seasonal $r_{\text{win}}, r_{\text{sum}}$.

We will see below that these two-or-three knobs are exactly enough to satisfy two constraints (class revenue sufficiency and target-subclass cross-subsidy elimination), with one degree of freedom left over to play with.

---

## 2. Setup and notation

All quantities come from CAIRO outputs of a calibrated baseline run (e.g. `run-1` for delivery-only, `run-2` for delivery+supply); see [`context/code/orchestration/run_orchestration.md`](context/code/orchestration/run_orchestration.md).

We use a descriptive subclass subscript: **`cls`** is the whole residential class, **`hp`** is the target subclass (the math generalizes to any subclass; we write `hp` because heat pumps are the v1 use case). Bills are denoted $\text{Bill}$, kWh totals as $\text{kWh}$, etc., so formulas read like English.

| Symbol                                                                      | Meaning                                                          | Unit              | Source artifact                                                                                                                        |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| $N_{\text{cls}}, N_{\text{hp}}$                                             | weighted customer counts                                         | customers         | `customer_metadata.csv` (`weight` column × group filter)                                                                               |
| $\text{kWh}_{\text{cls}}, \text{kWh}_{\text{hp}}$                           | weighted annual kWh                                              | kWh/yr            | `scan_resstock_loads`, summed over the year                                                                                            |
| $\text{kWh}^{\text{win}}_{\text{cls}}, \text{kWh}^{\text{win}}_{\text{hp}}$ | weighted **winter** kWh                                          | kWh/yr            | same, restricted to winter months $\mathcal{H}_{\text{win}}$                                                                           |
| $\text{kWh}^{\text{sum}}_{\text{cls}}, \text{kWh}^{\text{sum}}_{\text{hp}}$ | weighted **summer** kWh                                          | kWh/yr            | same, restricted to summer months $\mathcal{H}_{\text{sum}}$                                                                           |
| $\text{Bill}_{\text{cls}}, \text{Bill}_{\text{hp}}$                         | weighted current annual bills under the baseline tariff          | \$/yr             | `bills/elec_bills_year_target.csv`                                                                                                     |
| $X_{\text{hp}}$                                                             | weighted target-subclass cross-subsidy under the baseline tariff | \$/yr             | `cross_subsidization/cross_subsidization_BAT_values.csv`                                                                               |
| $F_0$                                                                       | baseline calibrated fixed charge                                 | \$/customer/month | `_extract_fixed_charge_from_urdb` on `_calibrated.json`                                                                                |
| $\rho_{MC}$                                                                 | load-weighted winter/summer marginal-cost ratio                  | dimensionless     | [`context/methods/tou_and_rates/cost_reflective_tou_rate_design.md`](context/methods/tou_and_rates/cost_reflective_tou_rate_design.md) |

The seasonal split $\mathcal{H}_{\text{win}}, \mathcal{H}_{\text{sum}}$ is the per-utility winter/summer month set defined in the periods YAML ([`utils/pre/season_config.py`](utils/pre/season_config.py)), the same one used by the seasonal-discount workflow. Annual = winter + summer:

$$\text{kWh}_{\text{cls}} = \text{kWh}^{\text{win}}_{\text{cls}} + \text{kWh}^{\text{sum}}_{\text{cls}}, \qquad \text{kWh}_{\text{hp}} = \text{kWh}^{\text{win}}_{\text{hp}} + \text{kWh}^{\text{sum}}_{\text{hp}}.$$

**Sign convention.** $X_{\text{hp}} > 0$ means the target subclass is **overcharged** under the baseline tariff (their bill exceeds their BAT-allocated cost). To eliminate the cross-subsidy, the new tariff must reduce the subclass's total annual bill by exactly $X_{\text{hp}}$ dollars. (Same convention as `compute_subclass_seasonal_discount_inputs`.)

**Two derived targets** appear repeatedly:

- **Class revenue requirement**, $\boxed{RR \equiv \text{Bill}_{\text{cls}}}$. The baseline tariff is calibrated to recover $RR$, so the weighted sum of current bills equals $RR$ by construction.
- **Subclass fair allocated cost**, $\boxed{\text{TC}_{\text{hp}} \equiv \text{Bill}_{\text{hp}} - X_{\text{hp}}}$. By the sign convention this is what the subclass _should_ pay if the residual were allocated by the BAT residual method.

---

## 3. The two design constraints

For any candidate new default tariff $(F, r_{\text{win}}, r_{\text{sum}})$, the **class** and **subclass** annual bills are linear in the parameters:

$$\text{Bill}_{\text{cls}}^{\text{new}} = \underbrace{12 F \cdot N_{\text{cls}}}_{\text{fixed-charge revenue}} + \underbrace{r_{\text{win}} \cdot \text{kWh}^{\text{win}}_{\text{cls}}}_{\text{winter volumetric}} + \underbrace{r_{\text{sum}} \cdot \text{kWh}^{\text{sum}}_{\text{cls}}}_{\text{summer volumetric}}$$

$$\text{Bill}_{\text{hp}}^{\text{new}} = 12 F \cdot N_{\text{hp}} + r_{\text{win}} \cdot \text{kWh}^{\text{win}}_{\text{hp}} + r_{\text{sum}} \cdot \text{kWh}^{\text{sum}}_{\text{hp}}.$$

The two design constraints are:

**(C1) Class revenue sufficiency.** Collect the same total revenue as the calibrated baseline, $\text{Bill}_{\text{cls}}^{\text{new}} = RR$:

$$\boxed{\;12 F \cdot N_{\text{cls}} \;+\; r_{\text{win}} \cdot \text{kWh}^{\text{win}}_{\text{cls}} \;+\; r_{\text{sum}} \cdot \text{kWh}^{\text{sum}}_{\text{cls}} \;=\; RR\;}$$

**(C2) Subclass cross-subsidy elimination.** Charge the subclass exactly its BAT-allocated cost, $\text{Bill}_{\text{hp}}^{\text{new}} = \text{TC}_{\text{hp}}$:

$$\boxed{\;12 F \cdot N_{\text{hp}} \;+\; r_{\text{win}} \cdot \text{kWh}^{\text{win}}_{\text{hp}} \;+\; r_{\text{sum}} \cdot \text{kWh}^{\text{sum}}_{\text{hp}} \;=\; \text{TC}_{\text{hp}}\;}$$

**Both constraints are required.** (C2) on its own — "the subclass pays the right total" — has infinitely many solutions, including ones that fix the subclass's bill by lowering everyone's rates and under-collecting the utility's revenue. (C1) is what forces the redistribution to be **revenue-neutral**: every dollar the subclass no longer pays must come from non-subclass customers, not from the utility. A tariff satisfying (C2) alone is not a viable rate proposal, so the operational definition of "fair default tariff" is the conjunction (C1) ∧ (C2).

**Two equations in three unknowns.** Generically the solution set is a 1-dimensional affine line in $(F, r_{\text{win}}, r_{\text{sum}})$-space — a one-parameter family of tariffs all of which are fair-and-revenue-neutral. The three strategies in §§4–6 close that one degree of freedom three different ways, each producing a single named tariff.

---

## 4. Strategy A: fixed-charge only (flat volumetric)

**Closure:** force $r_{\text{win}} = r_{\text{sum}} = r$ (a single flat volumetric rate). Adding (C1) and (C2) and using $\text{kWh}^{\text{win}} + \text{kWh}^{\text{sum}} = \text{kWh}$:

$$\begin{pmatrix} 12 N_{\text{cls}} & \text{kWh}_{\text{cls}} \\ 12 N_{\text{hp}} & \text{kWh}_{\text{hp}} \end{pmatrix} \begin{pmatrix} F \\ r \end{pmatrix} = \begin{pmatrix} RR \\ \text{TC}_{\text{hp}} \end{pmatrix}.$$

**Determinant.** Define the per-customer kWh:

$$\overline{\text{kWh}}_{\text{cls}} = \frac{\text{kWh}_{\text{cls}}}{N_{\text{cls}}}, \qquad \overline{\text{kWh}}_{\text{hp}} = \frac{\text{kWh}_{\text{hp}}}{N_{\text{hp}}}.$$

Then

$$\Delta_A \;=\; 12 \cdot \big(N_{\text{cls}} \cdot \text{kWh}_{\text{hp}} - N_{\text{hp}} \cdot \text{kWh}_{\text{cls}}\big) \;=\; 12 \cdot N_{\text{cls}} \cdot N_{\text{hp}} \cdot \big(\overline{\text{kWh}}_{\text{hp}} - \overline{\text{kWh}}_{\text{cls}}\big).$$

Since $N_{\text{cls}}, N_{\text{hp}} > 0$, the sign of $\Delta_A$ is the sign of $(\overline{\text{kWh}}_{\text{hp}} - \overline{\text{kWh}}_{\text{cls}})$.

**Closed-form solution (absolute).** By Cramer's rule:

$$F^*_A = \frac{RR \cdot \text{kWh}_{\text{hp}} - \text{TC}_{\text{hp}} \cdot \text{kWh}_{\text{cls}}}{\Delta_A}, \qquad r^*_A = \frac{12 \cdot (\text{TC}_{\text{hp}} \cdot N_{\text{cls}} - RR \cdot N_{\text{hp}})}{\Delta_A}.$$

**Closed-form solution (delta from baseline).** Substituting the calibration identities $RR = 12 F_0 N_{\text{cls}} + r_0 \text{kWh}_{\text{cls}}$ and $\text{Bill}_{\text{hp}} = 12 F_0 N_{\text{hp}} + r_0 \text{kWh}_{\text{hp}}$ into the formulas above and simplifying:

$$\boxed{\;\Delta F_A \;=\; F^*_A - F_0 \;=\; \frac{X_{\text{hp}} \cdot \overline{\text{kWh}}_{\text{cls}}}{12 \cdot N_{\text{hp}} \cdot (\overline{\text{kWh}}_{\text{hp}} - \overline{\text{kWh}}_{\text{cls}})}, \qquad \Delta r_A \;=\; r^*_A - r_0 \;=\; \frac{-X_{\text{hp}}}{N_{\text{hp}} \cdot (\overline{\text{kWh}}_{\text{hp}} - \overline{\text{kWh}}_{\text{cls}})}.\;}$$

Both formulas have the same denominator — $N_{\text{hp}}$ times the per-customer kWh gap — so the relative magnitudes of the fixed-charge raise and the volumetric cut are tied together by a factor of $\overline{\text{kWh}}_{\text{cls}} / 12$.

**Number of solutions.**

| Condition                                                                                          | # solutions                                                                                                             |
| -------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| $\overline{\text{kWh}}_{\text{hp}} \ne \overline{\text{kWh}}_{\text{cls}}$ (i.e. $\Delta_A \ne 0$) | **Exactly one** $(F^*_A, r^*_A)$.                                                                                       |
| $\overline{\text{kWh}}_{\text{hp}} = \overline{\text{kWh}}_{\text{cls}}$ AND $X_{\text{hp}} = 0$   | **Infinitely many** — the constraints are the same equation, any $(F, r)$ on the class-RR line works.                   |
| $\overline{\text{kWh}}_{\text{hp}} = \overline{\text{kWh}}_{\text{cls}}$ AND $X_{\text{hp}} \ne 0$ | **Zero** — constraints are parallel-but-inconsistent. With identical per-customer kWh, no flat tariff can redistribute. |

In practice, electrification of heating shifts therms onto the kWh meter, so $\overline{\text{kWh}}_{\text{hp}} > \overline{\text{kWh}}_{\text{cls}}$ comfortably and the answer is always exactly one.

**Sign intuition (target overcharged, target uses more electricity per customer).** With $X_{\text{hp}} > 0$ and $\overline{\text{kWh}}_{\text{hp}} > \overline{\text{kWh}}_{\text{cls}}$:

- $\Delta F_A > 0$ — **raise** the fixed charge,
- $\Delta r_A < 0$ — **cut** the volumetric rate.

Why this redistributes from non-target to target: a fixed-charge raise costs every customer the same dollars, so non-target customers (who outnumber HP roughly 9-to-1 in NY/RI) absorb most of the new fixed-charge revenue, while a volumetric cut benefits target customers more in absolute dollars (because they consume more kWh). Net flow: dollars from non-target to target, with class RR preserved.

**Failure mode.** If $r^*_A < 0$, or if $F^*_A$ falls below a regulatory floor (e.g. \$5/month) or above a customer-acceptance ceiling (rate cases routinely cap fixed-charge increases at, say, doubling), the strategy is infeasible at face value. Report the clipped tariff and the residual cross-subsidy that remains.

---

## 5. Strategy B: seasonal rates only (preserve fixed charge)

**Closure:** hold $F = F_0$, the calibrated baseline value. The two unknowns are now $(r_{\text{win}}, r_{\text{sum}})$:

$$\begin{pmatrix} \text{kWh}^{\text{win}}_{\text{cls}} & \text{kWh}^{\text{sum}}_{\text{cls}} \\ \text{kWh}^{\text{win}}_{\text{hp}} & \text{kWh}^{\text{sum}}_{\text{hp}} \end{pmatrix} \begin{pmatrix} r_{\text{win}} \\ r_{\text{sum}} \end{pmatrix} = \begin{pmatrix} RR - 12 F_0 N_{\text{cls}} \\ \text{TC}_{\text{hp}} - 12 F_0 N_{\text{hp}} \end{pmatrix}.$$

**Determinant.** Let the **winter share** of each group be

$$\sigma_{\text{cls}} \;=\; \frac{\text{kWh}^{\text{win}}_{\text{cls}}}{\text{kWh}_{\text{cls}}}, \qquad \sigma_{\text{hp}} \;=\; \frac{\text{kWh}^{\text{win}}_{\text{hp}}}{\text{kWh}_{\text{hp}}}.$$

Substituting $\text{kWh}^{\text{win}}_{\text{cls}} = \sigma_{\text{cls}} \text{kWh}_{\text{cls}}$, $\text{kWh}^{\text{sum}}_{\text{cls}} = (1 - \sigma_{\text{cls}}) \text{kWh}_{\text{cls}}$, and analogously for `hp`, the determinant factors cleanly:

$$\boxed{\;D \;\equiv\; \text{kWh}^{\text{win}}_{\text{cls}} \cdot \text{kWh}^{\text{sum}}_{\text{hp}} - \text{kWh}^{\text{win}}_{\text{hp}} \cdot \text{kWh}^{\text{sum}}_{\text{cls}} \;=\; \text{kWh}_{\text{cls}} \cdot \text{kWh}_{\text{hp}} \cdot \big(\sigma_{\text{cls}} - \sigma_{\text{hp}}\big)\;}$$

Since $\text{kWh}_{\text{cls}}, \text{kWh}_{\text{hp}} > 0$, the **sign of $D$** is the sign of $(\sigma_{\text{cls}} - \sigma_{\text{hp}})$:

| Condition on winter shares                                            | Sign of $D$ | When this happens                                                                                                          |
| --------------------------------------------------------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------- |
| $\sigma_{\text{hp}} = \sigma_{\text{cls}}$                            | $D = 0$     | Degenerate. Never with real heat pumps.                                                                                    |
| $\sigma_{\text{hp}} > \sigma_{\text{cls}}$ — target more winter-heavy | $D < 0$     | **The realistic HP case.** $\sigma_{\text{hp}} \approx 0.65{-}0.75$ vs $\sigma_{\text{cls}} \approx 0.55{-}0.60$ in NY/RI. |
| $\sigma_{\text{hp}} < \sigma_{\text{cls}}$ — target less winter-heavy | $D > 0$     | Solar/EV-only or summer-AC-heavy subclasses.                                                                               |

**Closed-form solution (absolute).** By Cramer's rule:

$$r^*_{\text{win},B} = \frac{(RR - 12 F_0 N_{\text{cls}}) \cdot \text{kWh}^{\text{sum}}_{\text{hp}} - (\text{TC}_{\text{hp}} - 12 F_0 N_{\text{hp}}) \cdot \text{kWh}^{\text{sum}}_{\text{cls}}}{D},$$

$$r^*_{\text{sum},B} = \frac{(\text{TC}_{\text{hp}} - 12 F_0 N_{\text{hp}}) \cdot \text{kWh}^{\text{win}}_{\text{cls}} - (RR - 12 F_0 N_{\text{cls}}) \cdot \text{kWh}^{\text{win}}_{\text{hp}}}{D}.$$

**Closed-form solution (delta from baseline equivalent flat $r_0$).** Substituting $RR$ and $\text{TC}_{\text{hp}}$ as before and simplifying:

$$\boxed{\;\Delta r_{\text{win},B} \;=\; \frac{X_{\text{hp}} \cdot \text{kWh}^{\text{sum}}_{\text{cls}}}{D}, \qquad \Delta r_{\text{sum},B} \;=\; \frac{-X_{\text{hp}} \cdot \text{kWh}^{\text{win}}_{\text{cls}}}{D}.\;}$$

(For a baseline that is already seasonal, replace $r_0$ with $r_{\text{win},0}, r_{\text{sum},0}$ on the left-hand side; the deltas on the right are unchanged because (C1) and (C2) are linear.)

**Number of solutions.**

| Condition                                                            | # solutions                                                                                                                                                        |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| $\sigma_{\text{hp}} \ne \sigma_{\text{cls}}$ (i.e. $D \ne 0$)        | **Exactly one** $(r^*_{\text{win},B}, r^*_{\text{sum},B})$.                                                                                                        |
| $\sigma_{\text{hp}} = \sigma_{\text{cls}}$ AND $X_{\text{hp}} = 0$   | **Infinitely many.**                                                                                                                                               |
| $\sigma_{\text{hp}} = \sigma_{\text{cls}}$ AND $X_{\text{hp}} \ne 0$ | **Zero.** No seasonal rate pair can fix the cross-subsidy: target and class respond identically to seasonal price changes, so winter-vs-summer is the wrong lever. |

**Sign intuition (HP case, $D < 0$, $X_{\text{hp}} > 0$).**

- $\Delta r_{\text{win},B} = X_{\text{hp}} \text{kWh}^{\text{sum}}_{\text{cls}} / D < 0$ — **cut** the winter rate.
- $\Delta r_{\text{sum},B} = -X_{\text{hp}} \text{kWh}^{\text{win}}_{\text{cls}} / D > 0$ — **raise** the summer rate.

The baseline tariff is over-collecting from HP because winter kWh are priced too dearly relative to HP's high winter consumption. Lowering $r_{\text{win}}$ helps HP relatively more (HP has higher winter share); raising $r_{\text{sum}}$ recovers the lost revenue mostly from non-HP (non-HP has higher summer share). Net effect: dollars flow from non-HP to HP.

**Failure mode.** Negative seasonal rates. The closed form does not enforce $r_{\text{win}}, r_{\text{sum}} \ge 0$. If e.g. $r^*_{\text{win},B} < 0$, options:

1. **Clip to zero** and recompute the other rate from (C1) alone, then report the residual cross-subsidy the clipped tariff leaves uncorrected.
2. **Fall back to Strategy A or C** — different denominators, so one strategy may be feasible when another is not.

---

## 6. Strategy C: combined, with a cost-reflective seasonal ratio

**Closure:** use all three knobs $(F, r_{\text{win}}, r_{\text{sum}})$, and pin the seasonal differential to the **cost-reflective** ratio:

$$\frac{r_{\text{win}}}{r_{\text{sum}}} \;=\; \rho_{MC}, \qquad \rho_{MC} \;=\; \frac{\overline{MC}_{\text{win}}}{\overline{MC}_{\text{sum}}} \;=\; \frac{\sum_{h \in \mathcal{H}_{\text{win}}} MC_h L_h \,\big/\, \sum_{h \in \mathcal{H}_{\text{win}}} L_h}{\sum_{h \in \mathcal{H}_{\text{sum}}} MC_h L_h \,\big/\, \sum_{h \in \mathcal{H}_{\text{sum}}} L_h}.$$

The numerator and denominator are demand-weighted seasonal MC averages, computed exactly as in [`context/methods/tou_and_rates/cost_reflective_tou_rate_design.md`](context/methods/tou_and_rates/cost_reflective_tou_rate_design.md).

**Reduction to a $2 \times 2$ system.** Substituting $r_{\text{win}} = \rho_{MC} r_{\text{sum}}$ into (C1) and (C2), define the **MC-weighted kWh** for each group — the annual kWh figure each group would produce if winter kWh were "weighted up" by the MC ratio:

$$\widetilde{\text{kWh}}_{\text{cls}} \;\equiv\; \rho_{MC} \cdot \text{kWh}^{\text{win}}_{\text{cls}} + \text{kWh}^{\text{sum}}_{\text{cls}}, \qquad \widetilde{\text{kWh}}_{\text{hp}} \;\equiv\; \rho_{MC} \cdot \text{kWh}^{\text{win}}_{\text{hp}} + \text{kWh}^{\text{sum}}_{\text{hp}}.$$

Then (C1) and (C2) become

$$\begin{pmatrix} 12 N_{\text{cls}} & \widetilde{\text{kWh}}_{\text{cls}} \\ 12 N_{\text{hp}} & \widetilde{\text{kWh}}_{\text{hp}} \end{pmatrix} \begin{pmatrix} F \\ r_{\text{sum}} \end{pmatrix} = \begin{pmatrix} RR \\ \text{TC}_{\text{hp}} \end{pmatrix}.$$

This is **structurally identical to Strategy A** with $\widetilde{\text{kWh}}$ in place of $\text{kWh}$. Its determinant and solution are the Strategy A formulas with the substitution $\text{kWh} \to \widetilde{\text{kWh}}$:

$$\Delta_C \;=\; 12 \cdot N_{\text{cls}} \cdot N_{\text{hp}} \cdot \big(\overline{\widetilde{\text{kWh}}}_{\text{hp}} - \overline{\widetilde{\text{kWh}}}_{\text{cls}}\big),$$

$$F^*_C = \frac{RR \cdot \widetilde{\text{kWh}}_{\text{hp}} - \text{TC}_{\text{hp}} \cdot \widetilde{\text{kWh}}_{\text{cls}}}{\Delta_C}, \qquad r^*_{\text{sum},C} = \frac{12 \cdot (\text{TC}_{\text{hp}} \cdot N_{\text{cls}} - RR \cdot N_{\text{hp}})}{\Delta_C}, \qquad r^*_{\text{win},C} = \rho_{MC} \cdot r^*_{\text{sum},C}.$$

**Number of solutions.** Same three cases as Strategy A with MC-weighted kWh:

| Condition                                                                                                                  | # solutions                                                                                     |
| -------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| $\overline{\widetilde{\text{kWh}}}_{\text{hp}} \ne \overline{\widetilde{\text{kWh}}}_{\text{cls}}$ (i.e. $\Delta_C \ne 0$) | **Exactly one** $(F^*_C, r^*_{\text{sum},C})$, hence one $(F, r_{\text{win}}, r_{\text{sum}})$. |
| $\overline{\widetilde{\text{kWh}}}_{\text{hp}} = \overline{\widetilde{\text{kWh}}}_{\text{cls}}$ AND $X_{\text{hp}} = 0$   | **Infinitely many.**                                                                            |
| $\overline{\widetilde{\text{kWh}}}_{\text{hp}} = \overline{\widetilde{\text{kWh}}}_{\text{cls}}$ AND $X_{\text{hp}} \ne 0$ | **Zero.**                                                                                       |

The non-degeneracy condition is a positive linear combination of Strategy A's and Strategy B's: $\overline{\widetilde{\text{kWh}}}_{\text{hp}} - \overline{\widetilde{\text{kWh}}}_{\text{cls}} = \rho_{MC} (\overline{\text{kWh}^{\text{win}}}_{\text{hp}} - \overline{\text{kWh}^{\text{win}}}_{\text{cls}}) + (\overline{\text{kWh}^{\text{sum}}}_{\text{hp}} - \overline{\text{kWh}^{\text{sum}}}_{\text{cls}})$. Both summands are positive in the realistic HP case, so $\Delta_C \ne 0$ in practice.

**Why this is the preferred closure.** Among the one-parameter family of (C1,C2)-feasible tariffs (see §7), Strategy C is the one whose seasonal differential matches the cost-causation differential. It is the single member of the family that simultaneously (i) collects $RR$, (ii) zeros the subclass cross-subsidy, and (iii) sends the seasonal price signal an ideal cost-reflective two-period tariff would send.

---

## 7. Geometry of the solution space, and the uniqueness theorem

The user's question — _"For any given fixed charge, are there zero, one, or infinitely many seasonal rate pairs that satisfy the two constraints?"_ — has a clean answer directly from §3's dimension count.

**Dimension counting.**

| Setup                                        | Unknowns                                | Independent equations | Geometry of the solution set                             |
| -------------------------------------------- | --------------------------------------- | --------------------- | -------------------------------------------------------- |
| (C2) alone                                   | 3 ($F, r_{\text{win}}, r_{\text{sum}}$) | 1                     | 2-D plane in 3-space — **infinitely many** solutions     |
| (C1) + (C2), all three knobs free            | 3                                       | 2                     | **1-D line** in 3-space — infinitely many along the line |
| (C1) + (C2), pin one knob (Strategies A/B/C) | 2                                       | 2                     | **single point** — exactly one solution (generically)    |

**Theorem (uniqueness for fixed $F$).** Fix any value of $F$. Then (C1) and (C2) restricted to the unknowns $(r_{\text{win}}, r_{\text{sum}})$ form a $2 \times 2$ linear system

$$\begin{pmatrix} \text{kWh}^{\text{win}}_{\text{cls}} & \text{kWh}^{\text{sum}}_{\text{cls}} \\ \text{kWh}^{\text{win}}_{\text{hp}} & \text{kWh}^{\text{sum}}_{\text{hp}} \end{pmatrix} \begin{pmatrix} r_{\text{win}} \\ r_{\text{sum}} \end{pmatrix} = \begin{pmatrix} RR - 12 F N_{\text{cls}} \\ \text{TC}_{\text{hp}} - 12 F N_{\text{hp}} \end{pmatrix}$$

with determinant $D = \text{kWh}_{\text{cls}} \text{kWh}_{\text{hp}} (\sigma_{\text{cls}} - \sigma_{\text{hp}})$ (from §5). The number of seasonal-rate pairs $(r_{\text{win}}, r_{\text{sum}})$ that satisfy both (C1) and (C2) at this $F$ is:

| Case                         | Condition                                                | # solutions                                                                             |
| ---------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| **Generic**                  | $D \ne 0$ ($\sigma_{\text{hp}} \ne \sigma_{\text{cls}}$) | **Exactly one** — given by Cramer's rule.                                               |
| **Degenerate, consistent**   | $D = 0$ AND $X_{\text{hp}} = 0$                          | **Infinitely many** — any $(r_{\text{win}}, r_{\text{sum}})$ on a line in 2-D.          |
| **Degenerate, inconsistent** | $D = 0$ AND $X_{\text{hp}} \ne 0$                        | **Zero** — parallel-but-offset constraints. (Need to change $F$ to recover a solution.) |

**In the heat-pump case the answer is always exactly one,** because heat-pump electrification of heating makes $\sigma_{\text{hp}}$ meaningfully larger than $\sigma_{\text{cls}}$ ($\sigma_{\text{hp}} \approx 0.65{-}0.75$ vs $\sigma_{\text{cls}} \approx 0.55{-}0.60$ in NY/RI), so $D < 0 \ne 0$.

**Geometric picture.** As $F$ varies over $\mathbb{R}$, the unique $(r_{\text{win}}(F), r_{\text{sum}}(F))$ traces an affine line in 3-space. Strategies A, B, C are three named points on this line, each picked out by a different one-equation closure:

```mermaid
flowchart LR
    line["1-parameter family of (F, r_win, r_sum)<br/>satisfying class RR + subclass fairness"]
    A["Strategy A<br/>r_win = r_sum (flat)"]
    B["Strategy B<br/>F = F_0 (preserve fixed charge)"]
    C["Strategy C<br/>r_win / r_sum = rho_MC (cost-reflective)"]
    line --> A
    line --> B
    line --> C
```

Other reasonable closures — fix $F$ at the regulator's preferred value, fix the winter rate at the marginal energy cost, enforce a fuel-cost floor on $r_{\text{sum}}$ — give still other named points on the same line. In every case the system reduces to a $2 \times 2$ Cramer's-rule problem with a closed-form answer.

**Practical consequence.** This is not a problem that needs a numerical optimizer or a search. The hard problem is choosing the right closure (A vs B vs C, or a hybrid), not solving the equations once chosen.

---

## 8. Feasibility region

The closed-form solutions are real numbers; they still have to be physically meaningful tariffs. The feasibility constraints are

$$F \ge F_{\min}^{\text{reg}} \ge 0, \qquad r_{\text{win}} \ge 0, \qquad r_{\text{sum}} \ge 0,$$

where $F_{\min}^{\text{reg}}$ is an optional regulatory floor on the fixed charge.

Along the one-parameter family from §7, each non-negativity constraint is a linear inequality in $F$:

- $r_{\text{win}}(F) = r_{\text{win}}(0) + a_{\text{win}} \cdot F$ — slope $a_{\text{win}}$ comes from differentiating Cramer's rule with respect to $F$. Non-negativity flips into $F \le -r_{\text{win}}(0)/a_{\text{win}}$ if $a_{\text{win}} < 0$, or $F \ge -r_{\text{win}}(0)/a_{\text{win}}$ if $a_{\text{win}} > 0$.
- Same form for $r_{\text{sum}}(F)$.
- $F \ge F_{\min}^{\text{reg}}$ closes the lower end.

Intersect the three half-lines to get the feasible interval $F \in [F_{\min}, F_{\max}]$ (possibly empty). [`utils/mid/compute_fair_default_inputs.py`](utils/mid/compute_fair_default_inputs.py) reports this interval explicitly so downstream sensitivity analysis can plot the feasible family. If $F^*_A$, $F_0$, or $F^*_C$ falls outside $[F_{\min}, F_{\max}]$, the corresponding strategy is infeasible without rate clipping.

---

## 9. Worked example

Stylized numbers chosen to make the arithmetic transparent. For a residential class with $N_{\text{cls}} = 400{,}000$ customers:

- **Class:** $\text{kWh}_{\text{cls}} = 4 \cdot 10^9$, $\text{kWh}^{\text{win}}_{\text{cls}} = 2.4 \cdot 10^9$ (winter share $\sigma_{\text{cls}} = 60\%$), $\text{kWh}^{\text{sum}}_{\text{cls}} = 1.6 \cdot 10^9$. Per-customer: $\overline{\text{kWh}}_{\text{cls}} = 10{,}000$ kWh/yr.
- **HP subclass:** $N_{\text{hp}} = 40{,}000$ (10% of class), $\text{kWh}_{\text{hp}} = 6 \cdot 10^8$ (15% of class kWh), $\text{kWh}^{\text{win}}_{\text{hp}} = 4.5 \cdot 10^8$ (winter share $\sigma_{\text{hp}} = 75\%$), $\text{kWh}^{\text{sum}}_{\text{hp}} = 1.5 \cdot 10^8$. Per-customer: $\overline{\text{kWh}}_{\text{hp}} = 15{,}000$ kWh/yr (50% above class average).
- **Calibrated baseline:** $F_0 = \$15$/month, equivalent flat $r_0 = \$0.16$/kWh.
  - $RR = 12 \cdot 15 \cdot 400{,}000 + 0.16 \cdot 4 \cdot 10^9 = \$712\text{M}/\text{yr}$.
  - $\text{Bill}_{\text{hp}} = 12 \cdot 15 \cdot 40{,}000 + 0.16 \cdot 6 \cdot 10^8 = \$103.2\text{M}/\text{yr}$. Per-customer HP bill: $\$2{,}580$/yr.
- **HP cross-subsidy from BAT:** $X_{\text{hp}} = +\$5\text{M}/\text{yr}$ ⇒ $\text{TC}_{\text{hp}} = \$98.2\text{M}/\text{yr}$. Per-customer HP target bill: $\$2{,}455$/yr (a $\$125$/yr reduction).

**Strategy A.**

$$\Delta_A = 12 \cdot N_{\text{cls}} \cdot N_{\text{hp}} \cdot (\overline{\text{kWh}}_{\text{hp}} - \overline{\text{kWh}}_{\text{cls}}) = 12 \cdot 4 \cdot 10^5 \cdot 4 \cdot 10^4 \cdot 5 \cdot 10^3 = 9.6 \cdot 10^{14}.$$

$$\Delta F_A = \frac{X_{\text{hp}} \cdot \overline{\text{kWh}}_{\text{cls}}}{12 N_{\text{hp}} \cdot (\overline{\text{kWh}}_{\text{hp}} - \overline{\text{kWh}}_{\text{cls}})} = \frac{5 \cdot 10^6 \cdot 10^4}{12 \cdot 4 \cdot 10^4 \cdot 5 \cdot 10^3} = \frac{5 \cdot 10^{10}}{2.4 \cdot 10^9} \approx +\$20.83/\text{month}.$$

So $F^{*}_A \approx \$35.83$/month. Solve (C1) for the flat rate:

$$r^{*}_A = (RR - 12 F^{*}_A N_{\text{cls}}) / \text{kWh}_{\text{cls}} = (712 \cdot 10^6 - 172 \cdot 10^6) / (4 \cdot 10^9) = \$0.135/\text{kWh}.$$

Per-customer HP check: $12 \cdot 35.83 + 15{,}000 \cdot 0.135 = 430 + 2{,}025 = \$2{,}455$ ✓.

**Strategy B** (preserve $F_0 = \$15$/month).

$$D = \text{kWh}_{\text{cls}} \cdot \text{kWh}_{\text{hp}} \cdot (\sigma_{\text{cls}} - \sigma_{\text{hp}}) = 4 \cdot 10^9 \cdot 6 \cdot 10^8 \cdot (-0.15) = -3.6 \cdot 10^{17}.$$

$$\Delta r_{\text{win},B} = \frac{X_{\text{hp}} \cdot \text{kWh}^{\text{sum}}_{\text{cls}}}{D} = \frac{5 \cdot 10^6 \cdot 1.6 \cdot 10^9}{-3.6 \cdot 10^{17}} \approx -\$0.0222/\text{kWh}, \qquad \Delta r_{\text{sum},B} = \frac{-X_{\text{hp}} \cdot \text{kWh}^{\text{win}}_{\text{cls}}}{D} \approx +\$0.0333/\text{kWh}.$$

Adding the deltas to the baseline equivalent flat rate $r_0 = 0.16$:

$$r^{*}_{\text{win},B} \approx \$0.138/\text{kWh}, \qquad r^{*}_{\text{sum},B} \approx \$0.193/\text{kWh}.$$

Per-customer HP check: with $\overline{\text{kWh}^{\text{win}}}_{\text{hp}} = 11{,}250$ and $\overline{\text{kWh}^{\text{sum}}}_{\text{hp}} = 3{,}750$ (per HP customer), the HP bill is

$$12 \cdot 15 + 0.138 \cdot 11{,}250 + 0.193 \cdot 3{,}750 \approx 180 + 1{,}553 + 724 \approx \$2{,}456 \;\;\checkmark \;(\text{within rounding}).$$

**Strategy C** with stylized $\rho_{MC} = 1.4$ (winter MC 40% above summer).

$$\widetilde{\text{kWh}}_{\text{cls}} = 1.4 \cdot 2.4 \cdot 10^9 + 1.6 \cdot 10^9 = 4.96 \cdot 10^9, \qquad \widetilde{\text{kWh}}_{\text{hp}} = 1.4 \cdot 4.5 \cdot 10^8 + 1.5 \cdot 10^8 = 7.8 \cdot 10^8.$$

Per-customer: $\overline{\widetilde{\text{kWh}}}_{\text{cls}} = 12{,}400$, $\overline{\widetilde{\text{kWh}}}_{\text{hp}} = 19{,}500$.

$$\Delta_C = 12 \cdot N_{\text{cls}} \cdot N_{\text{hp}} \cdot (\overline{\widetilde{\text{kWh}}}_{\text{hp}} - \overline{\widetilde{\text{kWh}}}_{\text{cls}}) = 12 \cdot 4 \cdot 10^5 \cdot 4 \cdot 10^4 \cdot 7{,}100 \approx 1.363 \cdot 10^{15}.$$

$$F^*_C = \frac{RR \cdot \widetilde{\text{kWh}}_{\text{hp}} - \text{TC}_{\text{hp}} \cdot \widetilde{\text{kWh}}_{\text{cls}}}{\Delta_C} = \frac{712 \cdot 10^6 \cdot 7.8 \cdot 10^8 - 98.2 \cdot 10^6 \cdot 4.96 \cdot 10^9}{1.363 \cdot 10^{15}} \approx +\$50.10/\text{month}.$$

Solve (C2) for $r^*_{\text{sum},C}$ with $r_{\text{win}} = 1.4 r_{\text{sum}}$: $r^*_{\text{sum},C} = (\text{TC}_{\text{hp}} - 12 F^*_C N_{\text{hp}}) / \widetilde{\text{kWh}}_{\text{hp}} \approx \$0.0951/\text{kWh}$, then $r^*_{\text{win},C} = 1.4 \cdot r^*_{\text{sum},C} \approx \$0.1331/\text{kWh}$. Per-customer HP check: $12 \cdot 50.10 + 11{,}250 \cdot 0.1331 + 3{,}750 \cdot 0.0951 \approx 601 + 1{,}497 + 357 = \$2{,}455$ ✓.

**Three tariffs side-by-side.**

| Strategy              | $F^*$ (\$/mo) | $r^*_{\text{win}}$ (\$/kWh) | $r^*_{\text{sum}}$ (\$/kWh) | Cost-reflective ratio?                                     |
| --------------------- | ------------- | --------------------------- | --------------------------- | ---------------------------------------------------------- |
| Baseline              | 15.00         | 0.160                       | 0.160                       | 1.00 (flat)                                                |
| A — fixed-charge only | 35.83         | 0.135                       | 0.135                       | 1.00 (flat)                                                |
| B — seasonal only     | 15.00         | 0.138                       | 0.193                       | 0.71 (winter cheaper than summer — _anti-cost-reflective_) |
| C — combined          | 50.10         | 0.133                       | 0.095                       | 1.40 (winter / summer = $\rho_{MC}$)                       |

Note Strategy B's anti-cost-reflective signal: with this stylized setup it lowers the winter price to give HP a break, even though winter MC is higher than summer. Strategy C resolves that tension by using the fixed charge as a third lever, freeing the seasonal rates to track $\rho_{MC}$ exactly.

---

## 10. Limitations and extensions

- **Single subclass.** The framework eliminates one cross-subsidy (e.g. HP). Multi-subclass elimination — say, HP and electric-resistance and EV-only all zeroed simultaneously — adds constraints faster than the $(F, r_{\text{win}}, r_{\text{sum}})$ design space adds knobs, and requires either TOU/tiered structure or simultaneous adjustments to multiple class-wide parameters. Left to a follow-up issue.
- **Flat-vs-seasonal only.** Strategies A/B/C assume a flat or 2-period seasonal tariff. A natural "Strategy D" would carry $(F, r_{\text{win,peak}}, r_{\text{win,off}}, r_{\text{sum,peak}}, r_{\text{sum,off}})$ and close the now-3-D affine family by enforcing both the seasonal MC ratio $\rho_{MC}$ and the within-season peak ratios from [`context/methods/tou_and_rates/cost_reflective_tou_rate_design.md`](context/methods/tou_and_rates/cost_reflective_tou_rate_design.md).
- **Delivery vs supply.** Run-1 outputs cover delivery; run-2 covers delivery + supply. The strategies above can be applied to either, producing `_default_fair_<strategy>.json` and `_default_fair_<strategy>_supply.json` tariffs respectively. The supply variant uses the same math with supply-only $X_{\text{hp}}$ and supply-only baseline bills.
- **Interaction with the LMI discount.** Apply the LMI tier credit _after_ the fair-default tariff is constructed (the LMI credit is a flat per-tier dollar discount). The fair-default math is unchanged; the LMI credit reduces realized revenue but is funded outside the class RR, consistent with the existing [`utils/post/apply_ny_lmi_to_master_bills.py`](utils/post/apply_ny_lmi_to_master_bills.py) workflow.
- **Calibration drift.** The closed forms assume CAIRO's calibrated bills exactly recover $RR$ and the BAT residual exactly equals $\text{TC}_{\text{hp}}$. CAIRO's iterative calibration leaves small (sub-percent) residuals; the produced tariff needs a final precalc calibration pass, identical to the one used by the seasonal-discount workflow.

---

## 11. Cross-references

**Implementation:**

- [`utils/mid/compute_fair_default_inputs.py`](utils/mid/compute_fair_default_inputs.py) — emits the inputs CSV with $N_{\text{cls}}, N_{\text{hp}}, \text{kWh}^{\text{win/sum}}_{\text{cls/hp}}, RR, \text{Bill}_{\text{hp}}, X_{\text{hp}}, \rho_{MC}$ and the closed-form $F^*_A, r^*_A; r^*_{\text{win/sum},B}; F^*_C, r^*_{\text{win/sum},C}$ plus the feasible interval $[F_{\min}, F_{\max}]$.
- [`utils/mid/create_fair_default_tariff.py`](utils/mid/create_fair_default_tariff.py) — wraps `create_flat_rate` (Strategy A) and `create_seasonal_rate` (B, C) from [`utils/pre/create_tariff.py`](utils/pre/create_tariff.py); injects $F$ via a small `_with_fixed_charge` helper.
- Reuses helpers from [`utils/mid/compute_subclass_rr.py`](utils/mid/compute_subclass_rr.py): `_load_subclass_cross_subsidy_inputs` (for $X_{\text{hp}}$ and target weights) and `_extract_fixed_charge_from_urdb` (for $F_0$); the seasonal kWh load-aggregation pattern from `compute_subclass_seasonal_discount_inputs` is extended in `_load_usage_totals` to compute class totals in the same scan.

**Methodology context:**

- [`context/methods/bat_mc_residual/bat_lrmc_residual_allocation_methodology.md`](context/methods/bat_mc_residual/bat_lrmc_residual_allocation_methodology.md) — definition and decomposition of $X_{\text{hp}}$ from the BAT framework.
- [`context/methods/tou_and_rates/cost_reflective_tou_rate_design.md`](context/methods/tou_and_rates/cost_reflective_tou_rate_design.md) — derivation of $\rho_{MC}$ as the load-weighted seasonal MC ratio.
- [`context/code/orchestration/seasonal_discount_rate_workflow.md`](context/code/orchestration/seasonal_discount_rate_workflow.md) — the analogous subclass-tariff workflow that this default-tariff workflow generalizes.
- GitHub issue [#398](https://github.com/switchbox-data/rate-design-platform/issues/398) — original motivating ticket.

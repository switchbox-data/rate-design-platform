# Marginal cost methodology: LRMC decomposition, FLIC operationalization, and the residual

How we construct the hourly marginal cost signal used in the Bill Alignment Test, why the methodology amounts to forward-looking incremental cost (FLIC) pricing, and how it relates to the academic literature on marginal-cost-plus-residual rate design.

---

## 1. The framework: economic cost, residual, and bill alignment

A growing body of literature argues that measuring cross-subsidization — whether between solar and non-solar customers, heat pump and non-heat pump customers, or any other groupings — requires decomposing each customer's cost responsibility into two parts:

$$\text{Total Allocated Cost}_i = \underbrace{\sum_{h=1}^{8760} L_{i,h} \times MC_h}_{\text{Economic Burden}} + \underbrace{f(i, R)}_{\text{Residual Share}}$$

where $L_{i,h}$ is customer $i$'s hourly load, $MC_h$ is the hourly marginal price, and $R$ is the residual revenue requirement (Simeone et al. 2023, Eq. 5). The economic burden captures what a customer's load _causes_ the system to spend. The residual captures everything else: sunk infrastructure costs, policy costs, regulatory return on embedded rate base, and any other component of the utility's revenue requirement that marginal cost pricing does not recover (Pérez-Arriaga, Jenkins & Batlle 2017; Borenstein 2016).

The BAT paper is explicit that the residual arises because "long-run marginal costs tend to be lower than average costs for systems in the U.S." (Simeone et al. 2023, §2.3). The residual "cannot be allocated on a cost-finding basis" — deciding how much each customer should contribute "relies on principles other than economic efficiency" (ibid.). This two-part decomposition is the foundation of our methodology.

Simeone et al. (2023, §3.2) note that "the details of which costs are included and excluded in the definition of marginal costs, such as the time frame considered (short-run versus long-run), data inputs, calculation methods, and other details are likely to change with each jurisdiction." Our methodology is a jurisdiction-specific operationalization of this framework for New York (and adaptable to other states).

---

## 2. LRMC vs. SRMC: what belongs in the economic burden?

The literature on efficient electricity pricing decomposes the long-run marginal cost of electricity into an energy component and capacity components:

$$LRMC_h = MC^{\text{energy}}_h + MC^{\text{gen capacity}}_h + MC^{\text{TX}}_h + MC^{\text{dist}}_h$$

where each component varies hourly (the capacity components are zero in most hours and concentrated in peak hours). This decomposition appears in different forms across the literature:

- **Borenstein (2016)** decomposes "social marginal cost" (SMC) into energy, generation capacity, transmission, and distribution components, and argues that the efficient price signal is $p = SMC$, with any revenue shortfall recovered as a residual.
- **Pérez-Arriaga, Jenkins & Batlle (2017)** and the MIT _Utility of the Future_ framework distinguish "forward-looking incremental costs" from "residual costs of existing infrastructure" and recommend that only the former should be recovered through cost-reflective charges; the latter should be recovered through non-distortive mechanisms.
- **Schittekatte & Meeus (2020)** find via bilevel game-theoretic modeling that the welfare-maximizing tariff recovers incremental network costs via peak-coincident charges and residual costs via a fixed per-connection charge.
- **Dameto et al. (2023)**, cited by the BAT paper, recommend that "long-term network costs driven by peak load reinforcement expenditures should be recovered through coincident peak demand charges, while the remaining (or residual) network costs should be recovered through a fixed charge" (Simeone et al. 2023, §2.3).
- **The BAT paper itself** (Simeone et al. 2023, Appendix A) uses the CPUC Avoided Cost Calculator — which includes "marginal energy costs from hourly day-ahead LMP data from CAISO" for the energy component and "avoided or deferred costs of transmission/distribution projects resulting from peak demand reductions" for the capacity components. This is forward-looking incremental cost, not embedded cost.

The consensus is clear: the economic burden should be computed from a marginal cost signal that captures (a) the short-run marginal cost of energy and (b) the long-run marginal cost of capacity (generation, transmission, and distribution infrastructure). Everything else — the embedded cost of existing infrastructure, O&M on sunk assets, regulatory return on historical rate base — belongs in the residual.

---

## 3. Operationalizing LRMC as forward-looking incremental cost (FLIC)

Pure LRMC is a hypothetical: the cost of _permanently_ increasing output by one unit when all factors of production can be adjusted. In practice, no one observes LRMC directly. What we and the literature actually compute is **forward-looking incremental cost (FLIC)**: the cost of the _specific next increment_ of capacity that the system plans to build. The BAT paper uses the CPUC Avoided Cost Calculator (a FLIC measure); Borenstein, Fowlie & Sallee (2024) use a modified version of the same calculator; we use market prices and MCOS studies.

### Three definitions of network LRMC

The statement "LRMC is lower than average cost" is universal in the literature, but the _magnitude_ of the gap varies enormously depending on which definition of LRMC one uses. There are at least three common definitions of network LRMC, and they produce very different numbers:

1. **Perturbation LRMC (Turvey LRMC)**: The change in total discounted system cost from optimally accommodating a permanent 1 MW increase in demand. This requires optimizing the entire expansion plan with and without the increment and comparing the two. It captures cascading effects — adding load may trigger reinforcements at multiple voltage levels across the network. Turvey (1968) formalized this. It produces the largest MC values: systems using this approach have found that LRMC-based charges recover 49–111% of total costs (Brown & Faruqui 2014, citing UK and Australian regulatory studies). However, it requires a full network optimization model that few regulators possess.

2. **Average Incremental Cost (AIC)**: The total capital cost of a defined capacity increment (e.g., 500 MW) divided by the capacity added: $AIC = \Delta C / \Delta MW$. This is what the UK's Common Distribution Charging Model (CDCM) uses — "the costs associated with a 500 MW increment of demand" (Brown & Faruqui 2014, §3). It gives intermediate values: it captures economies (or diseconomies) of scale for the specific increment but not system-wide optimization effects.

3. **Forward-Looking Incremental Cost (FLIC)**: The cost of specific planned projects in the utility's current capital investment program, annualized and divided by system peak. This is the narrowest definition — it captures only what the utility is actually planning to build in a near-term window. It produces the smallest values because it reflects only the marginal slice of the investment pipeline, not the full long-run cost of accommodating load growth.

### Which definition we use and why

We use **FLIC** (definition 3). Our distribution MC, for example, takes each utility's MCOS project-level capital entering service in each year, annualizes it with the utility's composite carrying charge rate, divides by system peak, and levelizes over 7 years (see §7). Our generation capacity MC uses ICAP auction prices — the market's revealed FLIC for the next MW of generation adequacy. Our bulk TX MC uses NYISO incremental benefit studies — the FLIC for specific planned transmission projects. This is the same class of methodology used by the BAT paper's data source (the CPUC Avoided Cost Calculator, which reports "deferrable distribution capacity costs" — project-level forward-looking costs, not Turvey perturbation costs).

The consequence is that our marginal costs are a small share of the total revenue requirement — typically 2–15% for NY utilities, depending on the utility's capital program and system peak. This is not anomalous; it reflects the FLIC definition. The literature's reports of LRMC recovering 49–111% of costs come from systems using the Turvey or AIC definitions, which capture the full cost of expanding the network to accommodate growth, including cascading reinforcements at all voltage levels. A different definition of LRMC, not a different system.

The choice is deliberate. For the BAT analysis, what matters is not recovering costs — it is identifying the marginal cost signal that should drive efficient tariff structure. The Turvey approach answers "what is the full long-run cost of growth?" — useful for integrated resource planning. The FLIC approach answers "what is the specific cost that current load patterns are triggering in the current capital plan?" — useful for tariff design, because it identifies the cost that a cost-reflective charge should recover given the utility's actual investment trajectory. The remainder — which is large — is the residual: the accumulated sunk cost of past investments, which the Pérez-Arriaga framework says should be recovered through non-distortive mechanisms.

### The 8760 signal

Our 8760 hourly MC signal has four components:

| Component             | What it captures                                     | Data source                              | Time orientation                 |
| --------------------- | ---------------------------------------------------- | ---------------------------------------- | -------------------------------- |
| Energy                | Marginal cost of generation in each hour             | NYISO real-time LBMP                     | Observed market prices           |
| Generation capacity   | Cost of providing the next MW of generation adequacy | NYISO ICAP spot / ISO-NE FCA             | Forward-looking auction prices   |
| Bulk transmission     | Cost of the next MW of bulk TX capacity              | NYISO incremental benefit studies        | Forward-looking project analysis |
| Sub-TX + distribution | Cost of the next MW of local delivery capacity       | Utility MCOS incremental diluted capital | Forward-looking capital plans    |

The total MC in each hour is:

$$MC_h = MC^{\text{energy}}_h + MC^{\text{gen cap}}_h + MC^{\text{bulk TX}}_h + MC^{\text{sub-TX+dist}}_h$$

Each component is described below, with its formula, how it becomes an 8760 signal, and how it compares with the literature.

---

## 4. Energy marginal cost

### Formula

For a single-zone utility:

$$MC^{\text{energy}}_h = \overline{LBMP}_h$$

where $\overline{LBMP}_h$ is the mean of all 5-minute real-time LBMP intervals within hour $h$ for the utility's zone.

For a multi-zone utility:

$$MC^{\text{energy}}_h = \frac{\sum_z LBMP_{z,h} \times \text{Load}_{z,h}}{\sum_z \text{Load}_{z,h}}$$

where the sum is over all NYISO zones in the utility's service territory.

### What this captures

Real-time locational marginal prices reflect the short-run marginal cost of the marginal generator in each hour: fuel, variable O&M, and congestion. This is the standard energy component of LRMC in the economics literature.

### How it compares with the literature

- **The BAT paper** (Simeone et al. 2023, Appendix A) uses "hourly, day-ahead locational marginal price data from CAISO" — functionally identical to our use of NYISO real-time LBMP.
- **Borenstein, Fowlie & Sallee (2024)** use the same CAISO LMP data via the CPUC Avoided Cost Calculator.
- **Schittekatte & Meeus (2018, 2020)** separate energy charges from network charges, with energy priced at wholesale spot.

Using observed spot prices rather than forecasted prices is the standard practice for retrospective analysis. The BAT paper makes this explicit: its case study is "a retrospective analysis" using "the year contemporaneous with the load data" (§4.1). Our approach is the same: we use 2018 AMY (actual meteorological year) load profiles with LBMP prices from the same period, configurable via `--energy-load-year`.

### Classification

**SRMC** (short-run marginal cost). This is the correct classification — energy is the component where SRMC and the energy component of LRMC coincide, because in a competitive wholesale market the spot price equals the variable cost of the marginal unit.

---

## 5. Generation capacity marginal cost

### Formula

For each ICAP locality $\ell$ and month $m$:

1. Identify the top $K$ hours by system load (we use $K = 8$).
2. Compute a threshold $T$ = the maximum load below the $K$-th hour.
3. Compute exceedance weights for peak hours ($w_h = 0$ otherwise):

   $$w_h = \frac{\text{Load}_h - T}{\sum_{h' \in \text{top } K} (\text{Load}_{h'} - T)}$$

4. Allocate the monthly capacity cost:

   $$MC^{\text{gen cap}}_h = w_h \times P_{\ell,m} \times \alpha_\ell$$

where $P_{\ell,m}$ is the ICAP spot price (\$/kW-month) for locality $\ell$ in month $m$, and $\alpha_\ell$ is the capacity weight from the zone mapping (reflecting the fraction of the utility's load in that locality).

For utilities spanning multiple ICAP localities, the signals are computed component-by-component and summed.

### What this captures

ICAP spot prices and Forward Capacity Auction (FCA) clearing prices represent the market-revealed cost of providing one additional MW of generation adequacy. In NYISO, the ICAP market procures installed capacity to meet reliability targets; the spot price reflects the marginal cost of that capacity. This is LRMC for generation infrastructure.

### How it compares with the literature

- **The BAT paper** uses "a generation CapEx of \$30/kW-year based on resource adequacy cost estimates" (Appendix A). Our approach is more granular: we use actual ICAP market prices that vary by locality and month, reflecting geographic capacity scarcity.
- **Borenstein (2016)** includes generation capacity as a component of SMC, noting that the efficient price includes "the marginal investment cost of expanding system capacity."
- **Pérez-Arriaga et al. (2017)** classify generation capacity costs as forward-looking and recommend they be recovered through cost-reflective charges rather than bundled into the residual.

The exceedance-weighting method for allocating annual \$/kW-yr costs to specific hours is standard in marginal cost-of-service studies. The BAT paper's Appendix A describes their allocation as "based on the 500 highest load hours of the year."

### Classification

**LRMC-like**. Capacity auction prices are forward-looking: they reflect the cost of procuring the next MW of capacity for future delivery periods.

---

## 6. Bulk transmission marginal cost

### Formula

For each NYISO constraint group $g$ (a set of transmission projects serving a set of paying localities):

1. Compute the incremental benefit per MW: for each project $p$ with annual benefit $B_p$ (\$/yr) and incremental capacity $\Delta MW_p$:

$$v_p = \frac{B_p}{\Delta MW_p \times 1000}$$

2. Sort projects by $\Delta MW$ and compute cumulative secants: $\bar{v}_g = \frac{\sum_p B_p \times 1000}{\sum_p \Delta MW_p}$

3. Map constraint group values to paying localities (ROS, LHV, NYC, LI).

4. Allocate to hours using a seasonal coincident peak (SCR) method: identify top 40 hours per season by load; compute exceedance weights within each season.

$$MC^{\text{bulk TX}}_h = w_h \times v_z$$

where $v_z$ is the paying locality's \$/kW-yr value and $w_h$ is the SCR weight (zero in non-peak hours).

### What this captures

The incremental benefit per MW of planned bulk transmission projects — the forward-looking cost that additional load imposes on the transmission system. This is LRMC for bulk transmission infrastructure, derived from NYISO's own transmission planning studies.

### How it compares with the literature

- **The BAT paper** (Appendix A) uses "avoided or deferred costs of transmission projects resulting from peak demand reductions, averaged across ten years, reported as marginal transmission capacity costs on a \$/kW basis." For PG&E, this was \$29.11/kW-yr. This is functionally the same concept: the forward-looking cost of transmission infrastructure per kW of peak demand.
- **Pérez-Arriaga et al. (2017)** specifically distinguish incremental network costs (which should be priced at marginal cost) from embedded network costs (which belong in the residual). Our use of NYISO incremental benefit studies is the former.

### Classification

**LRMC**. Forward-looking, derived from prospective transmission planning studies.

---

## 7. Sub-transmission and distribution marginal cost

### Formula

For each utility, from MCOS (Marginal Cost of Service) study filings:

1. Parse project-level data: capital ($), capacity (MW), in-service year.
2. Classify projects into `bulk_tx` (excluded) and `sub_tx_and_dist` (the BAT input).
3. Compute **incremental diluted MC** for each year $Y$:

$$MC^{\text{dist}}(Y) = \frac{\text{Capital}(Y) \times r \times e(Y)}{\text{SystemPeak}}$$

where $\text{Capital}(Y)$ is the capital of projects entering service in year $Y$ only, $r$ is the utility-specific composite rate or ECCR (converting capital to annualized revenue requirement), and $e(Y)$ is the escalation factor.

4. **Levelize** over a 7-year window (2026–2032):

$$\overline{MC}^{\text{dist}} = \frac{1}{7} \sum_{Y=2026}^{2032} MC^{\text{dist}}_{\text{real}}(Y)$$

The levelized value is the BAT input, reported as \$/kW-yr. It represents the average annual forward-looking sub-TX and distribution investment cost per kW of system peak, in constant 2026 dollars.

5. Allocate to hours using a **Probability of Peak (PoP)** method: identify the top 100 hours by utility load; weight proportionally to load:

$$MC^{\text{dist}}_h = w_h \times \overline{MC}^{\text{dist}} \quad \text{where} \quad w_h = \frac{\text{Load}_h}{\sum_{h' \in \text{top 100}} \text{Load}_{h'}}$$

Non-peak hours receive zero MC.

### Why incremental, not cumulative

MCOS studies present marginal costs using accumulated capital — the running total of all projects in service through each year. This is the standard for utility cost allocation. But the BAT's economic cost concept requires the incremental perspective: the cost that _new load_ in a given year triggers, which depends on the capital _entering service_ that year, not the historical total. Prior investments are embedded costs — they are part of the revenue requirement but not marginal. They belong in the residual.

The BAT paper's own data source confirms this interpretation: its distribution CapEx input comes from the CPUC Avoided Cost Calculator, described as "deferrable distribution capacity costs related to peak demand reductions" (Simeone et al. 2023, Appendix A) — a prospective, deferral-based measure.

See `context/methods/marginal_costs/ny_dist_mc_bat_methodology.md` for the full rationale, per-utility deviations, and harmonization decisions across the seven NY MCOS studies.

### How it compares with the literature

- **The BAT paper** uses distribution CapEx of \$54.46/kW-yr for PG&E, from the CPUC Avoided Cost Calculator, "averaged over ten years." Our approach is methodologically parallel: project-level capital from MCOS filings, converted to annualized \$/kW-yr, levelized over a multi-year window.
- **Pérez-Arriaga et al. (2017)** advocate for forward-looking network charges based on "prospective costs of network capacity reinforcements," which is exactly what MCOS incremental diluted capital captures.
- **Schittekatte & Meeus (2020)** find that the efficient tariff recovers incremental network costs via peak-coincident charges — consistent with our PoP allocation to peak hours.
- **Dameto et al. (2023)** recommend that "long-term network costs driven by peak load reinforcement expenditures should be recovered through coincident peak demand charges" — matching our use of PoP-weighted peak-hour allocation.

### Classification

**LRMC** (incremental forward-looking cost). Derived from planned capital projects in MCOS filings, annualized using utility-specific carrying charges, and diluted by system peak.

---

## 8. The residual

### Definition

$$R = TRR - \sum_i \text{Economic Burden}_i = TRR - \sum_i \sum_{h=1}^{8760} L_{i,h} \times MC_h$$

The residual is the gap between the utility's total revenue requirement and the total revenue that marginal cost pricing would generate. This is the same definition used by the BAT paper (Eq. 1), Borenstein (2016), Pérez-Arriaga et al. (2017), and the broader marginal-cost-plus-residual literature.

### What's in our residual

Because our MC captures forward-looking incremental costs only, the residual contains:

1. **Embedded infrastructure costs**: carrying charges on all past T&D investment (the accumulated cost base that MCOS cumulative tables capture but our incremental MC excludes). This is typically the largest component.
2. **O&M and administrative costs** on existing infrastructure not fully captured in MC composite rates.
3. **Customer-related costs**: metering, billing, service drops.
4. **Regulatory return on embedded rate base** beyond what new investment's carrying charges capture.
5. **Policy costs** and any other components of the revenue requirement not attributable to marginal cost.

This is consistent with the literature's characterization. Simeone et al. (2023, §2.3): "Residual costs typically occur because long-run marginal costs tend to be lower than average costs for systems in the U.S., are often driven by fixed (as opposed to variable) costs, and can be exacerbated by policy-related costs." Pérez-Arriaga et al. (2017) describe the residual as "the revenue requirement associated with existing infrastructure that would not change with marginal variations in demand."

### How the residual is allocated

The choice of residual allocator is a normative decision, not an economic one — the literature is unanimous on this point (Borenstein 2016; Pérez-Arriaga et al. 2017; Batlle, Mastropietro & Rodilla 2020; Simeone et al. 2023). CAIRO computes three allocators:

| Allocator    | Formula                                | CAIRO metric      | Rationale                                                                              |
| ------------ | -------------------------------------- | ----------------- | -------------------------------------------------------------------------------------- |
| Per-customer | $R_i = R / N$                          | `BAT_percustomer` | Lump-sum; preserves marginal price signal (Borenstein 2016; Pérez-Arriaga et al. 2017) |
| Volumetric   | $R_i = (R / \sum q_j) \times q_i$      | `BAT_vol`         | Proportional to consumption; dominant U.S. practice (Borenstein 2016)                  |
| Peak         | $R_i \propto \text{CP contribution}_i$ | `BAT_peak`        | Proportional to system peak contribution; cost-causation logic applied to sunk costs   |

The project defaults to per-customer allocation (`BAT_percustomer`) for subclass revenue requirement analysis, consistent with the efficiency-oriented recommendation of the Pérez-Arriaga framework and Schittekatte & Meeus (2020). All three metrics are always computed so that the sensitivity of cross-subsidy findings to the allocator choice is transparent.

For the full survey of residual allocation methods — including demand-based, historical consumption, and Ramsey approaches not implemented in CAIRO — see `context/methods/bat_mc_residual/residual_allocation_lit_review_and_cairo.md`.

---

## 9. Interpretation: what the MC/residual split means and the steady-state assumption

### The residual as "the share of RR for sunk capacity"

Because we use FLIC — the narrowest LRMC definition — our marginal costs capture only the forward-looking incremental cost that current load behavior imposes on the system: the projects in the capital pipeline, the next MW of generation adequacy, the next bulk transmission upgrade. The residual — everything else in the revenue requirement — represents the embedded cost of sunk infrastructure: carrying charges on past T&D investment, O&M on existing assets, regulatory return on historical rate base, customer-related costs, and policy costs.

Under this framework, our total MC can be interpreted as _the share of the revenue requirement attributable to new capacity investments currently in the pipeline_. And the residual is its complement: _the share attributable to past capacity investments that have already entered the rate base_. For NY utilities, this means roughly 2–15% of the revenue requirement is MC-recoverable and 85–98% is residual — which is exactly what the BAT analysis finds.

### The temporal simplification

This interpretation requires one simplification: treating current-year load behavior as the cause of current-year investment. In reality, there is a temporal lag. The load patterns that triggered a substation now entering the rate base were the load patterns of several years ago, when that substation was planned. Similarly, the load behavior we observe today will cause investments that enter the rate base over the next several years — next year for a feeder, perhaps 5–6 years from now for a large substation. The FLIC we compute today reflects the projects entering service today, which were caused by _past_ behavior, not today's behavior.

This is not a flaw in our methodology. It is an inherent feature of marginal cost-of-service analysis. Every MCOS study that compares current-year loads against current or near-term planned capital implicitly makes the same simplification. When a New York utility's MCOS files "incremental diluted marginal cost" derived from its 5-year capital budget, it compares those costs against current system peak — not against the historical peak that triggered those investments. The same is true of the CPUC Avoided Cost Calculator used by the BAT paper, and of the UK's CDCM, and of every LRMC estimate that divides planned capital by current demand. The temporal mismatch is a known simplification, accepted as pragmatic across the literature.

### The steady-state assumption

The reason this simplification is accepted is a behavioral regularity: **if load patterns are relatively stable over time, current-year behavior is a reasonable proxy for the behavior that caused current-year capital additions.**

This is the steady-state assumption. It says: the customers whose peak-hour consumption today looks like the consumption that triggered the substation entering service this year are, statistically, the same _types_ of customers — not necessarily the same individuals, but the same behavioral profile in the same hours and the same locations. If the load shape that drives distribution investment in 2026 is similar to the load shape that drove the investments entering service in 2026 (which were planned 3–6 years earlier), then comparing 2026 loads against 2026 capital yields approximately the same MC signal as the historically precise comparison would.

The assumption is strongest in systems where load growth is gradual and load shapes evolve slowly — which describes most U.S. distribution systems outside of rapid-growth or rapid-electrification zones. It is weakest during structural transitions (e.g., rapid heat pump adoption that shifts the system peak from summer to winter), where the load shape driving future investment may differ substantially from the one that drove the capital now entering service. In those cases, the FLIC-based MC may under- or over-estimate the true marginal cost signal — but this is a limitation of all MCOS-based approaches, not specific to ours.

### Why this matters: accuracy as economics, not accounting

If the steady-state assumption holds, then the temporal mismatch is irrelevant for the purpose that actually matters: **getting the tariff structure right**.

The Pérez-Arriaga framework argues that the point of decomposing the revenue requirement into MC and residual is not to produce a precise historical accounting of which customer caused which asset. It is to inform the _structure_ of rates: what share of costs should be recovered through cost-reflective charges that send efficient price signals, and what share should be recovered through non-distortive fixed charges that do not distort consumption or investment decisions.

If current load behavior resembles the behavior that drives the capital pipeline, then:

- **The MC/residual split is correctly sized.** The share of the revenue requirement attributable to forward-looking costs — which should be recovered through cost-reflective charges — is approximately right. The residual share — which should be recovered through fixed charges — is also approximately right.
- **The hourly MC profile is correctly shaped.** The hours that carry non-zero capacity MC (the peak hours where new infrastructure investment is concentrated) are approximately the same hours where cost-reflective charges should send a price signal.
- **The cross-subsidy measurement is approximately valid.** If customer A's load profile is heavily peak-coincident and customer B's is flat, the BAT correctly identifies A as imposing more marginal cost than B, even if the specific assets entering service this year were triggered by a slightly different version of A's behavior from several years ago.

In other words, while the FLIC-based decomposition may not be accurate as a strict accounting exercise — precisely matching each dollar of cost to the historical load event that caused it — it is accurate as an **economics exercise**: it produces the right price signals, the right tariff structure, and the right measurement of which customers are over- or under-paying relative to their cost responsibility.

This distinction matters because the ultimate purpose of cost-reflective pricing is **dynamic efficiency**. When the cost-reflective component of a tariff tracks the actual marginal cost signal, customers face incentives aligned with the system's investment needs. Customers who shift load off-peak reduce future capacity investment. Customers who increase peak-hour consumption bear the cost of the capacity they will trigger. Over time, this alignment reduces total system cost — which means future residuals (the accumulated sunk cost of future investments) grow more slowly than they would under inefficient pricing. Getting the MC/residual split right is not a sleight of hand that happens to produce a convenient number. It has real consequences for the trajectory of infrastructure costs.

Getting it _wrong_ also has real consequences. If too much residual is allocated through volumetric charges (the dominant U.S. practice), the volumetric price exceeds marginal cost, discouraging efficient electrification — including heat pump adoption. If the MC signal is too weak, cost-reflective charges fail to encourage peak-shifting. If it is overstated, the residual is under-recovered and either fixed charges become unreasonably high or the utility fails its revenue requirement. The right split, even if approximately derived via FLIC under the steady-state assumption, produces better outcomes than either ignoring marginal costs (pure embedded cost allocation) or overstating them (Turvey LRMC in a jurisdiction where the data cannot support it).

### From the system level to the individual: the BAT

The MC/residual decomposition at the system level tells you how to _structure_ rates — how much to recover through cost-reflective charges vs. fixed charges. The BAT extends this to the individual customer level. By computing each customer's economic burden ($\sum_h L_{i,h} \times MC_h$) and allocating the residual via a chosen mechanism ($f(i, R)$), we measure **bill alignment**: whether each customer's actual tariff charges match their total allocated cost.

A bill alignment of 1.0 means the customer pays exactly their MC-based economic burden plus their fair residual share. Values below 1.0 indicate the customer is being cross-subsidized by others; values above 1.0 indicate the customer is cross-subsidizing others.

This measurement is only as good as the MC signal that feeds it. If the MC signal is correctly shaped — tracks the right hours, the right cost components, the right relative magnitudes, which the steady-state assumption ensures — then the BAT produces a meaningful diagnostic of tariff performance. It identifies which customer classes are overpaying under current rates, which are underpaying, and by how much. That diagnostic directly informs rate redesign: it tells you not just that the tariff structure is wrong, but _how_ it is wrong and _for whom_ — which is the information a regulator needs to fix it.

---

## 10. EPMC: the practitioner's alternative to explicit residual allocation

The academic consensus (§8, and `context/methods/bat_mc_residual/residual_allocation_lit_review_and_cairo.md`) treats residual recovery as a distinct, explicitly chosen allocation: volumetric, per-customer, peak, demand-based, historical consumption, or Ramsey. The regulator picks a method, computes each customer's residual share, and recovers it through a charge whose structure is separate from the cost-reflective charge. In practice, most U.S. jurisdictions do something quite different.

### What EPMC actually does

In a traditional MCOS-based rate design (e.g., California's CPUC proceedings, or any jurisdiction that starts from marginal cost and reconciles to the revenue requirement), the standard mechanism is **equi-proportional marginal cost (EPMC) scaling**. The procedure:

1. Compute MC-based rates for each TOU period: the average marginal cost within each period sets the initial rate.
2. Calculate total revenue at MC rates: $\text{MC Revenue} = \sum_p r^{MC}_p \times Q_p$, where $r^{MC}_p$ is the MC-based rate for period $p$ and $Q_p$ is total consumption in that period.
3. Compute the EPMC scalar: $K = TRR / \text{MC Revenue}$.
4. Set final rates: $r_p = K \times r^{MC}_p$ for each period.

The result is a set of volumetric rates where every TOU period's price is scaled up by the same multiplicative constant $K$. For NY utilities, where FLIC-based MC revenue is 2–15% of TRR, $K$ would be roughly 7–50x. (In practice, NY doesn't use EPMC for inter-class allocation — see `context/domain/bat_mc_residual/ny_psc_how_ecos_and_mcos_are_used.md` — but the mechanism is standard in California and Oregon, and is the reconciliation method used in the BAT paper's own tariff construction: "the equi-proportional rate adjustment method is used as a reconciliation method to cover noneconomic costs" (Simeone et al. 2023, Appendix B, Eq. B3).)

### Why EPMC is implicit volumetric residual allocation

EPMC does not _choose_ a residual allocation method. It does not compute a per-customer residual share. It does not separate the cost-reflective charge from the residual charge. What it does is bundle marginal cost recovery and residual recovery into a single volumetric price — which means the residual is allocated in proportion to consumption, within each TOU period. In other words, EPMC is volumetric residual allocation in disguise.

This matters because the Pérez-Arriaga framework (§2) says the residual should be recovered through non-distortive mechanisms — ideally fixed charges — precisely to avoid distorting the cost-reflective price signal. EPMC does the opposite: it multiplies every MC-based price by a constant $K > 1$, so the retail price in every hour exceeds marginal cost by the same proportion. The _shape_ of the price signal is preserved (peak-to-off-peak ratios are unchanged), but the _level_ is distorted (every price is $K$ times too high).

The distinction between shape and level matters:

- **Relative efficiency is preserved.** A customer considering whether to shift load from peak to off-peak sees the same price ratio $r_{\text{peak}} / r_{\text{off-peak}}$ under EPMC as under pure MC pricing. The time-shifting incentive is correct.
- **Absolute efficiency is destroyed.** A customer considering whether to _add_ load (e.g., install a heat pump, charge an EV, electrify a gas furnace) sees a retail price that is $K$ times the marginal cost. At $K = 10$, a customer whose heat pump operation costs the system 5¢/kWh of marginal cost faces a retail price of 50¢/kWh. The electrification incentive is distorted — the customer is being asked to pay for sunk infrastructure as though their consumption caused it.

This is the mechanism by which current U.S. rate design creates the "operating cost barrier" to heat pump adoption. The residual is bundled into volumetric rates via EPMC (or its informal equivalents), making electricity appear far more expensive than its marginal cost. The solution the literature recommends — and that the MC/residual decomposition enables — is to unbundle the two: recover marginal costs through cost-reflective volumetric charges at their actual level, and recover the residual through a fixed charge that does not distort consumption decisions.

### EPMC as the tariff-design foil for the BAT

Understanding EPMC clarifies what the BAT is measuring and why. When we compute bill alignment under current rates, we are asking: "does this customer's tariff charges match their economic burden plus a fair residual share?" For most U.S. tariffs — which are EPMC-structured or functionally equivalent — the answer is systematically no, and the direction of the misalignment depends on the customer's load profile relative to the system average.

A customer with high winter consumption (e.g., a heat pump home in a summer-peaking system) pays EPMC-scaled rates on all their winter kWh. Their marginal cost contribution is small in winter (no capacity MC in off-peak hours), but the EPMC scalar loads residual costs onto every kWh equally. The BAT reveals this as a bill alignment above 1.0 — the customer is cross-subsidizing others. Under a tariff that separated MC-level cost-reflective charges from a fixed residual charge, this customer's bill alignment would be closer to 1.0.

Conversely, a customer with high summer-peak consumption pays the same EPMC-scaled rate but has a high economic burden (capacity MC is concentrated in their peak hours). If the EPMC scalar is large, the residual embedded in their rate may _understate_ their economic burden relative to other customers — especially if a fixed residual charge would assign them less than the implicit volumetric residual they pay now. The net effect depends on the specific load profile and the MC/residual ratio.

The BAT, by making these dynamics visible at the individual customer level, provides the diagnostic that motivates the unbundling. Without the MC/residual decomposition, EPMC looks like a neutral reconciliation mechanism. With it, the cross-subsidies become measurable.

### The practitioner case for volumetric cost recovery: RAP and Lazar & Gonzalez (2015)

EPMC is not merely a computational convenience. It has a theoretical advocate: the practitioner tradition, most prominently represented by the Regulatory Assistance Project's _Smart Rate Design for a Smart Future_ (Lazar & Gonzalez 2015). This paper — widely cited in state regulatory proceedings — lays out three principles for modern rate design:

1. A customer should be able to connect to the grid for no more than the cost of connecting to the grid.
2. Customers should pay for grid services and power supply in proportion to how much they use these services and how much power they consume.
3. Customers who supply power to the grid should be fairly compensated for the full value of the power they supply.

Principle 1 limits fixed charges to customer-specific costs (service drop, meter, billing — typically $4–10/month). Principle 2 directs that nearly all remaining costs — including distribution, transmission, and generation — be recovered through volumetric charges. The paper's illustrative rate design recovers the entire revenue requirement through a small customer charge plus TOU energy charges; there is no residual, no fixed infrastructure charge, and no explicit MC/residual decomposition.

The theoretical basis is the claim that "in the long run all costs are variable" — distribution infrastructure should therefore be recovered "on the basis of end-use consumption." The paper recommends time-varying volumetric charges (TOU, CPP, or RTP) rather than flat rates, and its conclusion is that "bidirectional, time-sensitive prices that more accurately reflect costs most closely align with the principles of modern rate design."

This position is coherent if one adopts a broad LRMC definition (Turvey perturbation or AIC — see §3) under which LRMC-based charges recover most or all of the revenue requirement, leaving no large residual. Under that view, the volumetric TOU price _is_ approximately LRMC, and there is no p > MC distortion to worry about.

The position is inconsistent with the FLIC-based view used in this methodology and in the Pérez-Arriaga/Borenstein/BAT literature. Under FLIC, marginal cost pricing recovers only 2–15% of the revenue requirement. Bundling the remaining 85–98% into volumetric charges — which is what the RAP recommendation produces in practice — is EPMC scaling. The _shape_ of the TOU signal is preserved (the RAP paper is right that time-varying rates are better than flat rates), but the _level_ is inflated by a factor of $K$, creating the absolute-efficiency distortion described above: every kWh of consumption appears far more expensive than its marginal cost, discouraging efficient electrification.

The RAP paper does not address this tension because it never decomposes the revenue requirement into marginal cost and residual components. It operates in a framework where all costs are "variable in the long run" and therefore recoverable through usage-based charges. The MC/residual decomposition — the foundation of the BAT — is precisely the analytical step that makes the distortion visible. Without it, EPMC looks like cost-reflective pricing; with it, the cross-subsidies embedded in volumetric rates become measurable.

Brown & Faruqui (2014), in their Brattle Group survey of 140 items in the academic literature and two dozen industry experts, document why the RAP position has been so durable despite the academic consensus against it: regulators consistently prioritize equity and gradualism over efficiency. High fixed charges — even when set at the residual level, not the full revenue requirement — are perceived as unfair to low-usage customers (who tend to be lower-income). The RAP paper channels this concern. The academic literature's response — income-differentiated fixed charges, or the hybrid approach in `context/domain/bat_mc_residual/fairness_in_cost_allocation.md` §14 where fairness governs marginal costs and equity governs the residual — is a potential resolution, but one the RAP framework does not consider.

---

## 11. Summary: our methodology in the context of the literature

| Design choice                  | Our approach                                      | BAT paper (Simeone et al. 2023)       | Pérez-Arriaga framework       | Borenstein (2016)                    |
| ------------------------------ | ------------------------------------------------- | ------------------------------------- | ----------------------------- | ------------------------------------ |
| **Energy MC**                  | NYISO real-time LBMP (hourly, zonal)              | CAISO day-ahead LMP                   | Wholesale spot price          | Wholesale price                      |
| **Gen capacity MC**            | NYISO ICAP / ISO-NE FCA (market prices)           | \$30/kW-yr resource adequacy estimate | Forward-looking capacity cost | Included in SMC                      |
| **TX MC**                      | NYISO incremental benefit studies, SCR allocation | \$29.11/kW-yr from CPUC ACC           | Forward-looking network cost  | Included in SMC                      |
| **Dist MC**                    | MCOS incremental diluted capital, PoP allocation  | \$54.46/kW-yr from CPUC ACC           | Forward-looking network cost  | Included in SMC                      |
| **MC time profile**            | 8760 hourly; capacity allocated to peak hours     | 8760 hourly; top 500 hours            | Peak-coincident charges       | Hourly SMC                           |
| **Residual definition**        | $TRR - \sum \text{Economic Burden}$               | Same (Eq. 1)                          | Same                          | Same                                 |
| **Default residual allocator** | Per-customer (flat)                               | Per-customer and volumetric compared  | Per-customer fixed charge     | Recommends fixed + modest volumetric |

The methodological parallels are close. The key differences are jurisdictional, not conceptual:

- We use **market prices** for energy and generation capacity (LBMP and ICAP) rather than the CPUC Avoided Cost Calculator, because NY has observable wholesale and capacity markets.
- We derive distribution MC from **project-level MCOS filings** rather than a statewide avoided cost calculator, because NY's seven IOUs each file utility-specific MCOS studies.
- We use a **7-year levelization window** for distribution MC, comparable to the BAT paper's 10-year average.

In all cases, the underlying logic is the same: price energy at short-run marginal cost, price infrastructure capacity at forward-looking incremental cost, and treat the remainder as a residual to be allocated by normative principles.

---

## References

- Batlle, C., Mastropietro, P., & Rodilla, P. (2020). Redesigning residual cost allocation in electricity tariffs: A proposal to balance efficiency and equity. _Renewable Energy_, 155, 257–266.
- Borenstein, S. (2016). The economics of fixed cost recovery by utilities. _The Electricity Journal_, 29(7), 5–12.
- Borenstein, S., Fowlie, M., & Sallee, J. (2024). Designing electricity rates for an equitable energy transition. Working paper (data and ACC modifications published on GitHub).
- Brown, T., & Faruqui, A. (2014). Structure of electricity distribution network tariffs: Recovery of residual costs. Report for the Australian Energy Market Commission, The Brattle Group.
- Dameto, N., Valenzuela-Venegas, G., & Salom, J. (2023). A comprehensive method for designing dynamic electricity tariffs with cross-subsidization analysis for prosumer buildings. _Energy and Buildings_.
- Lazar, J., & Gonzalez, W. (2015). Smart rate design for a smart future. Regulatory Assistance Project.
- Pérez-Arriaga, I. J., Jenkins, J. D., & Batlle, C. (2017). A regulatory framework for an evolving electricity sector: Highlights of the MIT Utility of the Future study. _IEEE Power and Energy Magazine_, 15(3), 21–33.
- Schittekatte, T., & Meeus, L. (2018). Introduction to network tariffs and network codes for consumers, prosumers, and energy communities. FSR Technical Report.
- Schittekatte, T., & Meeus, L. (2020). Least-cost distribution network tariff design in theory and practice. _The Energy Journal_, 41(5).
- Simeone, C., et al. (2023). The bill alignment test: A measure of utility tariff performance. _Utilities Policy_, 85, 101676.
- Turvey, R. (1968). _Optimal Pricing and Investment in Electricity Supply_. Allen & Unwin.

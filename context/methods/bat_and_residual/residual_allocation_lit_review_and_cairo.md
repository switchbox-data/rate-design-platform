# Residual Allocation Methods: Literature Survey and CAIRO Implementation

This document maps residual cost allocation methods from the cross-subsidization literature to CAIRO's three BAT allocators. It is organized by allocation method, not by paper. For the full paper-by-paper literature review with embedded cost frameworks, demand allocation formulas, and distribution cost classification, see `context/domain/residual_allocation_in_solar_cross_subsidy_studies.md`.

## Background: the two-part decomposition

The Bill Alignment Test (Simeone et al. 2023) decomposes each customer's cost responsibility into:

$$\text{Total Allocated Cost}_i = \underbrace{\sum_{h=1}^{8760} L_{i,h} \times MC_h}_{\text{Economic Burden}} + \underbrace{f(i, R)}_{\text{Residual Share}}$$

where $L_{i,h}$ is customer $i$'s hourly load, $MC_h$ is the hourly marginal price (energy + capacity components), and $R = TRR - \sum_i \text{Economic Burden}_i$ is the residual revenue requirement. The economic burden is determined by cost-causation principles; the residual share is an inherently normative choice. CAIRO always computes three residual allocators — volumetric, peak, and per-customer — and reports a BAT metric for each. The project defaults to per-customer (`BAT_percustomer`) for subclass revenue requirement analysis.

The choice of residual allocator determines the measured cross-subsidy. Under volumetric allocation, HP customers with high winter kWh appear to overpay; under per-customer allocation, the cross-subsidy is smaller because each customer bears an equal residual share regardless of load. The BAT paper demonstrates that the residual allocation method can be more consequential to the bill alignment distribution than the choice between flat and TOU tariff structures.

---

## 1. Volumetric residual allocation

### Formula

$$R_i = \frac{R}{\sum_j q_j} \times q_i$$

Each customer's residual share is proportional to their net annual energy consumption (kWh).

### CAIRO status

**Implemented** as `BAT_vol`. CAIRO computes `customer_level_residual_share_volumetric` and reports `BAT_vol = Annual Bill - (Economic Burden + Volumetric Residual Share)`.

### Literature survey

Volumetric allocation is the dominant practice in U.S. electricity rate design and the implicit residual allocation method in virtually every embedded cost-of-service study. When a utility recovers demand-classified costs through a flat volumetric rate ($/kWh), the residual is allocated in proportion to consumption whether or not anyone names it as such.

**Borenstein (2016)** calls this "average-cost volumetric markup" and identifies it as the standard practice. He sets $p = AC = TRR / \sum q$, so the residual contribution per customer is $R_i = (AC - SMC) \times q_i$. Borenstein notes this is attractive on equity grounds (higher-usage customers pay more) but creates deadweight loss because the retail price exceeds marginal cost, discouraging efficient consumption and electrification. His 2025 paper with Fowlie and Sallee undermines the equity case: 59-82% of the consumption difference between high- and low-usage households is attributable to non-behavioral factors (household size, climate, solar adoption), meaning volumetric allocation penalizes circumstances rather than choices.

**Brattle Group / Sergici et al. (2019)** and **E3 California NEM studies (2013-2021)** do not perform explicit individual-level residual allocation. They compute cost shifts as (retail rate - avoided cost) x net export kWh, which implicitly assumes volumetric residual recovery because the retail rate bundles marginal and residual costs into a single $/kWh charge. The CPUC PAO found that PG&E's bundled residential NEM customers paid only 52% of their cost of service under this structure.

**Brown & Sappington (2017)** model the case where no fixed charge is available ($R = 0$), forcing all residual recovery into the volumetric price. This yields average-cost pricing with distorted DG compensation — both the retail price $r$ and the solar export rate $w$ are pushed away from their efficient levels by the revenue-balance constraint. In their California calibration, the welfare loss from forced volumetric recovery is 3.9%.

**Schittekatte & Meeus (2018)** test full volumetric cost recovery $p_{\text{vol}} = C_{\text{grid}} / \sum_i q_i$ and find it produces a "death spiral" dynamic: DER-adopting consumers reduce their bills, shifting costs to passive consumers, which incentivizes further DER adoption. This is the strongest game-theoretic case against volumetric residual allocation.

**Batlle, Mastropietro & Rodilla (2020)** include volumetric allocation in their taxonomy as $R_i = (R / \sum q) \times q_i$ and assess it as "inefficient; distorts consumption; enables gaming via DER."

### Effect on solar/HP cross-subsidy

Volumetric allocation maximizes the measured cross-subsidy from solar/HP customers. Solar customers who reduce net kWh by 30-50% through self-consumption avoid a proportional share of residual costs while maintaining similar peak demand — the core mechanism in every study that finds large NEM cost shifts ($20-100/month per Brattle, $740-1,600/year per E3). HP customers face the opposite problem: their high winter kWh consumption means they are allocated a disproportionate residual share, even though their winter-heavy load profile may not have driven the summer-peak investments that dominate the embedded cost base.

---

## 2. Per-customer (flat) residual allocation

### Formula

$$R_i = \frac{R}{N}$$

Each customer pays an equal share of residual costs, regardless of load.

### CAIRO status

**Implemented** as `BAT_percustomer`. CAIRO computes `customer_level_residual_share_percustomer` and reports `BAT_percustomer = Annual Bill - (Economic Burden + Per-Customer Residual Share)`. This is the **default allocator** used for subclass revenue requirement analysis (`compute_subclass_rr.py`).

### Literature survey

Per-customer allocation is the efficiency-oriented approach: residual costs are sunk, and recovering them through any consumption-based mechanism distorts the retail price above marginal cost. A fixed residual charge is a lump-sum transfer that preserves the marginal price signal.

**Borenstein (2016)** calls this "uniform fixed charges" and describes it as very attractive for minimizing deadweight loss — no incentive distortion on consumption decisions. His objection is distributional: a small apartment dweller pays the same as a mansion owner. He recommends a combination of fixed charges and modest volumetric markups as a practical compromise.

**Perez-Arriaga, Jenkins & Batlle (2017)** and the MIT _Utility of the Future_ framework explicitly recommend per-customer fixed charges for the residual, with the critical requirement that they be independent of current consumption, injection, or capacity decisions. The simplest form is $F_i = R/N$. Abdelmotteleb, Perez-Arriaga & Gomez (2018) permit differentiation by connection capacity, historical consumption, income, or property value, but insist the basis must be non-reactive to current behavior.

**Brown & Sappington (2017)** model the case where a fixed charge $R$ is available. It serves as the residual allocation mechanism, splitting the utility's revenue shortfall equally between solar-capable (D-type) and non-solar (N-type) consumers. With $R$ available, the retail price $r$ is set efficiently via a Ramsey-like condition.

**Schittekatte & Meeus (2020)** find via bilevel game-theoretic modeling that the least-cost tariff design combines forward-looking peak-coincident charges for incremental network costs with a fixed residual charge per connection point — uniform per-customer.

**Turk et al. (2025)** apply the Perez-Arriaga framework to ComEd under increasing EV penetration, recovering residual costs through fixed charges per connection ($/customer/month). A key finding: separating network charges from energy charges, with residuals recovered via fixed charges, reduces network expansion costs relative to bundled TOU rates that cause synchronized EV charging at off-peak onset.

**Batlle et al. (2020)** include flat fixed charges as $R_i = R/N$ in their taxonomy and assess them as "efficient but inequitable (same charge for all)."

### Effect on solar/HP cross-subsidy

Per-customer allocation minimizes the measured cross-subsidy between solar and non-solar customers (and between HP and non-HP customers) because the residual share is identical for all customers. The remaining bill alignment differences reflect only the gap between each customer's tariff payment and their economic burden — the marginal cost allocation. If the tariff structure itself is cost-reflective (e.g., a well-designed TOU rate), the BAT distribution under per-customer allocation will be tight around zero. The BAT paper's case study shows that the average per-customer cross-subsidy dropped from $133 (flat residual allocation with flat rate) to $15 (volumetric residual allocation with TOU rate), illustrating how the allocator dominates the tariff structure in determining the spread.

---

## 3. Peak residual allocation

### Formula

$$R_i = \frac{R \times \text{PeakContrib}_i}{\sum_j \text{PeakContrib}_j}$$

Each customer's residual share is proportional to their contribution to system peak demand.

### CAIRO status

**Implemented** as `BAT_peak`. CAIRO computes `customer_level_residual_share_peak` using the same peak hours that define the capacity marginal cost signal in the 8760. This is a coincident peak (CP) method — the allocation is based on each customer's demand during the hours of system peak, not their individual maximum demand.

### Literature survey

Peak allocation applies backward-looking cost-causation reasoning to sunk costs: customers whose load shaped the peaks that justified past investment bear the residual cost of that investment. Economists generally object to applying cost-causation to sunk costs (the efficient thing is to treat them as sunk), but the approach has a long regulatory tradition in embedded cost-of-service analysis.

**Simshauser (2016)** uses the closest analog to peak-based residual allocation in the embedded cost literature. His "Fair Share" allocates the _entire_ allowed revenue (not just the residual) by peak demand: $\text{Fair Share}_i = (\text{Peak kW}_i / \sum_k \text{Peak kW}_k) \times TRR$. The peak is defined as the highest 30-minute average demand during the 4-8 PM window in any month — a non-coincident peak (NCP) measure (each customer's own peak), which differs from CAIRO's coincident peak method. Using Energex interval data, Simshauser found solar households' evening peak demand (4-8 PM) was only 5-10% lower than non-solar households, but grid kWh was 30-50% lower. Under volumetric tariffs, the cross-subsidy was ~A$236/household/year; under a three-part demand tariff recovering network costs via peak kW, the cross-subsidy largely disappears because solar and non-solar households have similar peak demand.

**Schittekatte & Meeus (2018)** test capacity-based cost recovery $p_{\text{cap}} = C_{\text{grid}} / \sum_i \text{peak}_i$ and find it partially mitigates the death spiral but is not fully future-proof — batteries can reduce measured peak demand, enabling gaming. Their 2020 paper recommends peak-coincident charges only for _incremental_ (forward-looking) network costs, with sunk costs recovered via fixed per-customer charges.

**NERA / Average-and-Excess** methods blend energy and peak components: $AF_i = LF \times (AvgD_i / \sum AvgD) + (1-LF) \times (ExcD_i / \sum ExcD)$. For solar customers, the A&E method partially self-corrects: solar reduces average demand but maintains evening peaks, so the excess demand component increases, allocating _more_ cost to solar customers than pure volumetric would.

### Effect on solar/HP cross-subsidy

Peak allocation produces intermediate cross-subsidy results. For solar: solar customers reduce kWh but maintain evening peak demand, so they bear a residual share nearly as large as non-solar customers — much closer to "fair" than volumetric allocation. For HP: the effect depends on the peaking season. In summer-peaking systems (most of the U.S., including NYISO), HP customers' winter-heavy load contributes relatively little to system peak, so they receive a _small_ peak-based residual share. This means BAT_peak may show HP customers overpaying even more than BAT_percustomer if the tariff charges them volumetrically for costs that peak allocation would assign predominantly to summer-peaking customers.

---

## 4. Demand-based residual allocation

### Formula

Two variants appear in the literature:

**Measured peak demand (demand charge):**

$$R_i = \frac{R}{\sum_j \text{kW}^{\text{peak}}_j} \times \text{kW}^{\text{peak}}_i$$

where $\text{kW}^{\text{peak}}_i$ is each customer's measured peak demand (NCP or CP, depending on the variant).

**Contracted/subscribed capacity:**

$$R_i = \frac{R}{\sum_j \text{kW}^{\text{contracted}}_j} \times \text{kW}^{\text{contracted}}_i$$

where $\text{kW}^{\text{contracted}}_i$ is each customer's subscribed connection capacity (e.g., a 3 kW, 9 kW, or 15 kW service tier chosen a priori).

### CAIRO status

**Not implemented** as a residual allocator. CAIRO's `BAT_peak` is related but distinct: it allocates the residual based on contribution to _system_ peak hours (a CP method), not based on each customer's individual peak demand or subscribed capacity. Demand charges can appear in CAIRO tariffs (as a bill calculation mechanism), but demand-based residual allocation — where the counterfactual "should pay" is proportional to individual kW — is not computed.

### Literature survey

Demand-based allocation sits between volumetric and per-customer: it's consumption-responsive (unlike per-customer) but based on capacity rather than energy (unlike volumetric). In the cross-subsidy context, it has a natural appeal because network costs are driven by peak capacity, so allocating the residual by peak demand has a cost-causation logic.

**Simshauser (2016)** proposes a three-part demand tariff as the remedy for solar cross-subsidization: $\text{Bill}_i = \text{Fixed} \times 365 + \text{Demand rate} \times \text{Peak kW}_i + \text{Energy rate} \times \text{kWh}_i$. The demand charge component recovers network costs proportional to each customer's measured peak demand (NCP, 30-min average during 4-8 PM in any month). This is a tariff structure, not an explicit residual allocator, but the demand charge implicitly allocates residual costs by peak demand. Simshauser shows this largely eliminates the solar cross-subsidy because solar and non-solar households have similar peak demand.

**Schittekatte & Meeus (2018)** test capacity-based tariffs and find they partially address the death spiral but are gameable with batteries: customers can install storage to shave measured peaks and avoid network charges. The contracted capacity variant (choosing a fixed kW tier) is less gameable because the customer commits to a capacity level regardless of actual usage.

**Batlle et al. (2020)** review contracted capacity as $R_i = (R / \sum \text{kW}_{\text{contracted}}) \times \text{kW}_i$ and note the "risk of gaming if capacity is adjustable." If customers can freely choose a low capacity tier, they can reduce their residual share without reducing their actual impact on the network. The method works best when capacity tiers are bundled with physical service limits (as in France and Italy) so the contracted capacity reflects genuine infrastructure cost.

**Borenstein (2016)** argues economics does not support demand charges for _residual_ recovery because they create perverse incentives — specifically, they give customers a reason to distort their load shape to reduce measured peak demand, which is an efficiency loss if the network costs being recovered are sunk. He distinguishes this from demand charges for _marginal_ cost recovery, which do send efficient signals about future infrastructure investment.

**Burger, Knittel, Perez-Arriaga, Schneider & vom Scheidt (2020)** test demand-differentiated fixed charges (fixed charges that vary by measured peak demand) using 30-minute interval data for 100,000+ Chicago customers. They find a strong correlation between customers' peak demands and their income, implying that demand-based residual allocation has attractive distributional properties — higher-income customers in larger homes tend to have higher peaks and would pay more.

**France and Italy** have long used contracted capacity for network cost recovery. France: 3 kW connection ~EUR75/yr, 9 kW ~EUR160/yr, 15 kW ~EUR285/yr. Italy's three-part tariff uses contracted connection capacity. These are the closest real-world implementations of demand-based residual allocation.

### Effect on solar/HP cross-subsidy

Demand-based allocation partially resolves the solar cross-subsidy because solar customers maintain evening peak demand similar to non-solar customers. Simshauser's empirical finding — solar households' 4-8 PM peaks are only 5-10% lower than non-solar — means a demand-based residual allocator assigns nearly equal residual shares to both groups, eliminating most of the cross-subsidy. For HP customers, the effect is ambiguous: if the demand charge is based on NCP (individual peak), HP customers may have high winter peaks that lead to large residual shares. If based on CP (system peak), HP customers in summer-peaking systems may have low residual shares. Batteries introduce gaming risk: customers can install storage to shave measured peaks, reducing their demand-based residual share without reducing the underlying infrastructure need.

---

## 5. Historical consumption residual allocation

### Formula

$$R_i = \frac{R \times q_i^{\text{hist}}}{\sum_j q_j^{\text{hist}}}$$

Each customer's residual share is proportional to their historical consumption, frozen at a reference period (e.g., average of the past three years). Current consumption changes — including solar adoption, battery installation, or heat pump conversion — do not alter the residual share.

### CAIRO status

**Not implemented.** CAIRO works with a single year of synthetic ResStock loads and has no historical baseline for each building. Implementing this would require tracking per-customer consumption across multiple years, which is outside the simulation's scope.

### Literature survey

This is the innovation of **Batlle, Mastropietro & Rodilla (2020)**, the most targeted paper on residual allocation mechanics in the cross-subsidy literature. Their key insight: volumetric allocation has attractive equity properties (higher-usage customers pay more), and per-customer allocation has attractive efficiency properties (no distortion of consumption decisions). Historical consumption allocation combines both: it preserves the equity gradient of volumetric allocation (because historical consumption correlates with household size, income, and infrastructure usage) while being non-distortive (because the allocation is frozen and cannot be gamed by changing current behavior).

Batlle et al. assess it as "efficient, equitable, non-distortive" — the only allocation method in their taxonomy that receives positive marks on all three criteria. The reference period is typically the average of the past three years, long enough to smooth seasonal and annual variation.

**Abdelmotteleb, Perez-Arriaga & Gomez (2018)** include historical consumption as one permissible allocation weight in the Perez-Arriaga framework: $\text{Fixed}_i = \sum_e [(TRR_e - \sum_k PCNC_{k,e}) \times w_i]$ where $w_i$ can be proportional to historical consumption. The Comillas/MIT group insists only that the weight be non-reactive to current behavior; historical consumption satisfies this requirement.

### Effect on solar/HP cross-subsidy

Historical consumption allocation directly addresses the solar cross-subsidy: a customer who installs solar panels _after_ the reference period continues to pay the same residual share as before. DER adoption cannot reduce residual contributions, eliminating the cost-shift mechanism. For the same reason, a customer who installs a heat pump after the reference period does not see their residual share increase despite higher electricity consumption — they pay the same residual as when they were a gas-heating customer. This is favorable for HP adoption but raises the question of what happens as the reference period is periodically updated: eventually, HP customers' higher consumption enters the baseline, and their residual share rises.

---

## 6. Ramsey (inverse-elasticity) residual allocation

### Formula

$$\frac{p_g - MC_g}{p_g} = \frac{\lambda}{\varepsilon_g}$$

The markup above marginal cost for customer group $g$ is inversely proportional to the group's price elasticity of demand $\varepsilon_g$, where $\lambda$ is a scalar set to satisfy the revenue constraint $\sum_g p_g \times q_g = TRR$. In effect:

$$R_g \propto \frac{q_g}{\varepsilon_g}$$

### CAIRO status

**Not implemented.** Ramsey pricing requires demand elasticity estimates for each customer or customer group, which CAIRO does not model.

### Literature survey

**Borenstein (2016)** presents Ramsey pricing as the classical efficiency-maximizing approach: by charging higher markups to inelastic customers, it minimizes the total deadweight loss from above-marginal-cost pricing. However, he notes it raises significant equity concerns because inelastic customers — those who need electricity most and have fewest alternatives — bear the highest markups. Within the residential class, Ramsey logic supports recovering residuals through fixed/demand charges (which have low elasticity of avoidance) rather than volumetric charges (which are more elastic).

**Brown & Faruqui (2014)**, in their Brattle Group report for the Australian Energy Market Commission, identify Ramsey pricing as the guiding principle in the academic literature but note it has rarely been applied for within-class price discrimination because equity considerations have overridden efficiency arguments.

**Batlle et al. (2020)** include Ramsey/inverse-elasticity in their taxonomy as $R_i \propto q_i / \varepsilon_i$ and assess it as "theoretically efficient; impractical; regressive."

**Feldstein (1972)** shows that when the social welfare function includes equity weighting, the Ramsey formula gains a distributional correction: $(p_i - MC_i)/p_i = (\lambda - \beta_i)/\varepsilon_i$, where $\beta_i$ reflects the covariance between social marginal utility of income and consumption. This equity-adjusted version has not been implemented in practice.

### Effect on solar/HP cross-subsidy

The solar-specific tension is significant: if solar customers have more elastic demand (they can further reduce grid consumption via batteries and demand management), Ramsey logic dictates they should bear _less_ residual cost, potentially worsening the cross-subsidy from an equity perspective. Conversely, Ramsey principles support fixed-charge residual recovery — the component with the lowest elasticity of avoidance — which would reduce the solar cross-subsidy. The net effect is ambiguous and depends on which dimension of elasticity dominates. No empirical cross-subsidy study has implemented Ramsey pricing at the individual customer level.

---

## Summary

| Method                 | CAIRO             | Key                               | Efficiency                         | Equity                                 | Solar cross-subsidy                                               |
| ---------------------- | ----------------- | --------------------------------- | ---------------------------------- | -------------------------------------- | ----------------------------------------------------------------- |
| Volumetric             | `BAT_vol`         | $R_i \propto q_i$                 | Low (distorts consumption)         | Moderate (usage-proportional)          | Maximizes: solar avoids residual via self-consumption             |
| Per-customer           | `BAT_percustomer` | $R_i = R/N$                       | High (no distortion)               | Low (regressive)                       | Minimizes: equal share for all                                    |
| Peak (system CP)       | `BAT_peak`        | $R_i \propto \text{CP contrib}_i$ | Moderate                           | Moderate                               | Intermediate: solar maintains peak, so similar share to non-solar |
| Demand-based           | Not in CAIRO      | $R_i \propto \text{kW}_i$         | Moderate (gameable with batteries) | Moderate-high (correlates with income) | Partially resolves: solar maintains peak demand                   |
| Historical consumption | Not in CAIRO      | $R_i \propto q_i^{\text{hist}}$   | High (non-distortive)              | Moderate (usage-proportional)          | Fully resolves: DER adoption cannot reduce share                  |
| Ramsey                 | Not in CAIRO      | $R_i \propto q_i / \varepsilon_i$ | Highest (theory)                   | Low (regressive)                       | Ambiguous: elastic solar customers pay less                       |

# Allocation Mechanics in Solar Cross-Subsidization Studies: Embedded Cost and Marginal-Cost Frameworks

The literature on cross-subsidization between rooftop solar and non-solar residential customers rests on two fundamentally different cost-allocation paradigms — embedded cost-of-service (ECOS) methods and marginal-cost-plus-residual frameworks — and the specific formulas each study employs determine both the magnitude and direction of the measured cross-subsidy. No single "correct" allocation method exists; the choice of allocator can swing a solar customer's cost responsibility from 52% to over 100% of their assigned revenue requirement.

The embedded-cost tradition, rooted in NARUC's three-step process and operationalized by the Brattle Group, E3, CPUC, Simshauser, and the Comillas group, allocates the utility's total historical revenue requirement to customers using accounting-based factors. The marginal-cost tradition, advanced by Borenstein, Pérez-Arriaga, Brown & Sappington, Schittekatte & Meeus, and Turk et al., prices energy and capacity at forward-looking marginal cost and then must confront the residual — the gap between marginal-cost revenue and the total revenue requirement — which becomes the central design variable. Both traditions converge on a common finding: volumetric (per-kWh) recovery of fixed or sunk costs is the primary mechanism generating cross-subsidies, but they differ sharply on what should replace it.

---

## The NARUC Three-Step Engine: Functionalization, Classification, Allocation

Every embedded cost-of-service study follows the framework codified in the 1992 NARUC _Electric Utility Cost Allocation Manual_[^1] and updated by RAP's 2020 _Electric Cost Allocation for a New Era_.[^2] The process has three sequential steps.

**Step 1 — Functionalization** assigns each dollar of the revenue requirement to a functional category: generation (including purchased power), transmission, distribution (sub-functionalized into substations, primary lines, line transformers, secondary lines), customer service (meters, service drops, billing), and administrative & general (prorated across the other functions).[^3]

**Step 2 — Classification** tags each functionalized cost by its cost driver: demand-related (kW), energy-related (kWh), or customer-related (number of customers).[^4] Typical classifications are:

- Generation capacity and fixed O&M → demand (or split demand/energy via the equivalent-peaker method)
- Fuel and variable purchased power → 100% energy
- Transmission plant → 100% demand
- Distribution plant (poles, conductors, transformers, conduit — FERC accounts 364–368) → split between demand and customer (the most contentious step)
- Meters, service drops, billing → 100% customer

**Step 3 — Allocation** distributes each classified cost pool to rate classes using allocation factors.[^5] The general formula for class _j_'s share of any cost pool _C_:

$$C_j = C \times AF_j \quad \text{where} \quad \sum_j AF_j = 1$$

The allocation factor $AF_j$ differs by cost driver, and the choice of factor is where the real methodological action occurs.

---

## Demand Allocation Formulas: CP, NCP, and Average-and-Excess

Three families of demand allocation factors dominate the literature and regulatory practice.

### Coincident Peak (CP) Method

Costs are allocated based on each class's demand at the hour of the system peak.[^6] Under the single-CP (1-CP) variant:

$$AF_j = \frac{D_j^{\text{sys peak}}}{D_{\text{system peak}}}$$

where $D_j^{\text{sys peak}}$ is class $j$'s demand at the single hour of annual system peak. The N-CP variant (4-CP, 12-CP) averages across multiple monthly peaks:

$$AF_j = \frac{(1/N) \sum_m D_j(\text{month } m \text{ peak})}{(1/N) \sum_m D_{\text{system}}(\text{month } m \text{ peak})}$$

The 12-CP method is the historical FERC method for transmission cost allocation; 1-CP and 4-CP are common for generation capacity.[^7] The CP method is applied to costs where the system-wide peak drives investment — generation capacity and transmission.

### Non-Coincident Peak (NCP) Method

Costs are allocated based on each class's own maximum demand, regardless of when it occurs:[^8]

$$AF_j = \frac{NCP_j}{\sum_k NCP_k}$$

NCP is the standard allocator for distribution demand costs because distribution facilities are sized to meet local and class-level peaks that may not coincide with the system peak.[^9] Monthly NCP variants (2-NCP, 12-NCP) average across months.

### Average-and-Excess Demand (AED) Method

This two-part allocator blends energy usage and peakiness, weighted by the system load factor:[^10]

$$AF_j = LF_{\text{sys}} \times \frac{AvgD_j}{\sum_k AvgD_k} + (1 - LF_{\text{sys}}) \times \frac{ExcD_j}{\sum_k ExcD_k}$$

where $LF_{\text{sys}}$ = system load factor (average demand / peak demand), $AvgD_j$ = class $j$'s average demand (annual $\text{kWh}_j / 8{,}760$), and $ExcD_j = NCP_j - AvgD_j$ (the "excess" demand above average). The first term allocates the base-load portion of costs proportionally to energy usage; the second allocates the capacity portion proportionally to how "peaky" each class is. The AED method is commonly applied to generation demand costs and has been used by utilities such as AmerenUE for production capacity allocation.[^11] A variant, the peak-and-average method, substitutes coincident peak for NCP in the excess term.

---

## Distribution Cost Classification: The Minimum System vs. Basic Customer Divide

The most consequential methodological choice in embedded cost studies is how distribution plant costs (FERC accounts 364–368: poles, conductors, conduit, cable, transformers) are split between demand-related and customer-related. This classification directly determines how much cost is allocated per-customer versus per-kW, and thus how much a solar customer's reduced consumption shifts costs to others.

### The Minimum System Method

The minimum system method determines the cost of building the smallest physically functional distribution system — one that connects all customers but carries zero load. The minimum-system cost is classified as customer-related; everything above it is demand-related:[^12]

$$\text{Customer share} = \frac{N_{\text{units}} \times \text{Cost}_{\text{min size}}}{\text{Total}_{\text{actual cost}}}$$

$$\text{Demand share} = 1 - \text{Customer share}$$

where N_units is the total count of installed equipment and Cost_{min size} is the unit cost of the smallest standard equipment the utility installs. In a BC Hydro distribution cost-of-service study, four transformers (two at 10 kVA costing $2,300 each, two at 50 kVA costing $3,700 each) totaled $12,000 in actual cost. The minimum system (all priced at 10 kVA) yielded $9,200, so 77% was classified as customer-related and only 23% as demand-related.[^13] Missouri PUC filings show minimum system and zero-intercept methods classify 35.5% to 46.1% of distribution equipment (accounts 364–368) as customer-related.[^14]

### The Zero-Intercept Method

The zero-intercept method uses regression analysis to extrapolate equipment cost to zero capacity:[^15]

$$y = a + bx$$

where _y_ is unit cost, _x_ is capacity (kVA, conductor diameter), _a_ is the zero intercept (customer cost per unit), and _b_ is the incremental demand cost per unit of capacity. Using the same BC Hydro data, regression yields a = $1,950 and b = $35/kVA, classifying 65% as customer-related — lower than the minimum system method because the zero-intercept cost ($1,950) is less than the minimum standard unit cost ($2,300).[^16]

### The Basic Customer Method

The basic customer method takes the opposite extreme: only costs that literally vary with the number of customers (meters, billing systems, service drops) are classified as customer-related. All distribution plant in accounts 364–368 is classified as 100% demand-related.[^17] RAP's 2020 manual recommends this approach. The basic customer method produces a dramatically smaller customer component and a correspondingly larger demand component, which when recovered volumetrically increases the cost-shift from net-metered solar customers.

### Why the Choice Matters for Solar Cross-Subsidization

A minimum system classification of 77% customer / 23% demand means most distribution costs are recovered via fixed per-customer charges that solar customers cannot avoid. A basic customer classification of ~0% customer / 100% demand means most distribution costs flow through volumetric or demand charges that solar customers can substantially avoid through self-consumption. The classification decision alone can swing the measured cross-subsidy by hundreds of dollars per customer per year.

---

## How the Major Embedded Cost Studies Allocate to Solar Customers

Each of the principal study groups operationalizes the NARUC framework differently. All allocate to rate classes first, then compute individual customer impacts through rate design — no study attempts a true bottom-up individual customer ECOS allocation.

### Brattle Group / Sergici et al. (2019)

In "Quantifying Net Energy Metering Subsidies," the Brattle Group applied a consistent cost-of-service framework across 16 U.S. utilities.[^18] Their core formula:

$$\text{NEM Subsidy}_i = \sum_h \left[ (\text{Retail rate}_h - \text{Avoided cost}_h) \times \text{Net export kWh}_{i,h} \right]$$

where _h_ indexes hours or billing periods, the retail rate includes all embedded cost components (generation, delivery, customer service), and the avoided cost captures only genuinely avoidable commodity costs (wholesale energy price + line losses). Brattle treats essentially all distribution costs as fixed/demand-related and not avoided by DG. Customer costs (metering, billing) are allocated per-customer and also deemed unavoidable. The study uses class-level average cost allocation from utility FERC Form 1 filings and regulatory cost-of-service data, combined with typical residential solar generation profiles estimated from system size and location-specific insolation. Results: NEM subsidies ranged $20–$100 per customer per month, representing 25–200% of monthly bills.[^19]

### E3 California NEM Studies (2013–2021)

E3's methodology for the California NEM Ratepayer Impacts Evaluation uses three interlocking models: a NEM Bill Calculator, a NEM Avoided Cost Model, and a Summary Tool. The core formula:

$$\text{Cost Shift} = \sum_h \left[ \text{Retail rate}_h \times |\text{Net consumption reduction}_{i,h}| \right] - \sum_h \left[ \text{Avoided cost}_h \times \text{Gross generation}_{i,h} \right]$$

The E3 Avoided Cost Calculator (ACC), which is the CPUC's system of record, provides hourly, location-specific avoided cost values across eight-plus components:[^20] avoided energy (from CAISO wholesale prices, 8,760-hour granularity), avoided generation capacity (using effective load carrying capability of solar), avoided transmission capacity (based on CAISO Transmission Access Charge), avoided distribution capacity (utility-specific marginal distribution capacity costs, location-specific), avoided line losses (T&D loss multipliers), avoided ancillary services (~1% of energy value), avoided RPS costs, and avoided GHG compliance costs (California cap-and-trade allowance prices). Data sources include California Solar Initiative production meter data, AMI interval data, CAISO market data, and utility General Rate Case marginal cost filings. The 2013 study found NEM compensation at retail rates ($0.16–$0.30/kWh) vastly exceeded avoided cost value ($0.08–$0.11/kWh), creating per-customer cost shifts of $740–$1,600/year depending on utility territory.

### CPUC Public Advocates Office

The PAO uses the same E3 ACC framework but emphasizes the distinction between avoidable costs (GHG reductions, fuel savings, partially avoidable grid infrastructure) and non-avoidable fixed costs (wildfire safety, grid maintenance, public purpose programs, nuclear decommissioning, DWR bond charges).[^21] PG&E's embedded cost-of-service study in its General Rate Case Phase II found that bundled residential NEM customers paid only 52% of their cost of service,[^22] with a total NEM cost-of-service imbalance of approximately $538 million annually for PG&E alone. The PAO estimated total California NEM cost shifts grew from $3.4 billion/year in 2021 to $8.5 billion/year by end of 2024, adding $100–$234 annually to the average non-solar residential bill.[^23]

### Simshauser (2016)

Simshauser's Australian work is distinctive in using actual 30-minute interval meter data at the circuit level for individual households, rather than class-level averages.[^24] His cross-subsidy formula compares actual cost causation (driven by peak demand) against tariff payments (driven by energy volume):

$$\text{Cross-Subsidy}_i = \text{Network Payment}_i - \text{Fair Share}_i$$

where

$$\text{Fair Share}_i = \frac{\text{Peak kW}_i}{\sum_k \text{Peak kW}_k} \times \text{Total Allowed Revenue}$$

and

$$\text{Network Payment}_i = \text{Fixed charge} \times 365 + \text{Variable rate} \times \text{Annual kWh}_i$$

Using Energex (Southeast Queensland) interval data, Simshauser demonstrated that solar households' evening peak demand (4–8 PM, the network-driving period) was only 5–10% lower than non-solar households, but their grid kWh consumption was 30–50% lower. Under flat volumetric tariffs ($0.59/day fixed + $0.12/kWh variable), solar households therefore paid far less for essentially the same peak capacity usage. The measured cross-subsidy: approximately A$236/household/year from non-solar to solar under volumetric tariffs.[^25] Simshauser proposes a three-part demand tariff as the remedy:

$$\text{Bill}_i = \text{Fixed} \times 365 + \text{Demand rate} \times \text{Peak kW}_i + \text{Energy rate} \times \text{kWh}_i$$

where Peak_kW_i is measured as the highest 30-minute average demand during the peak window (4–8 PM) in any month. Under this structure, solar and non-solar households pay similar network charges because their peak demands are similar.[^26]

### Comillas / Picciariello et al. (2015)

The Comillas/IIT group uses a fundamentally different approach: a bottom-up Reference Network Model (RNM) that designs an optimized distribution network from scratch for different DG penetration scenarios.[^27] This is engineering-based cost causation rather than accounting allocation. The RNM decomposes total network cost into load-driven costs (C_load, costs attributable to serving load) and DG-driven costs (C_DG, incremental or decremental costs caused by DG). Under a volumetric tariff with net metering:

$$\text{Tariff rate} = \frac{\text{Total Network Revenue}}{\text{Total Net Energy}}$$

$$\text{Payment}_i = \text{Tariff rate} \times \text{Net kWh}_i$$

The cross-subsidy for the consumer group is:

$$\text{CS}_{\text{consumers}} = \text{Total payment}_{\text{consumers}} - C_{\text{load,consumers}}$$

And for prosumers:

$$\text{CS}_{\text{prosumers}} = \text{Total payment}_{\text{prosumers}} - (C_{\text{load,prosumers}} + C_{DG})$$

The normalized cross-subsidy rate is:

$$\text{CS rate} = \frac{\text{Payment} - \text{Cost caused}}{\text{Cost caused}} \times 100\%$$

Picciariello et al. tested 12 real-size U.S. distribution networks across 6 states, finding cross-subsidies of 5–40% of costs depending on PV penetration and network density, with low-density rural networks showing the highest rates. At 30% PV penetration, consumers paid 10–20% more than their fair share while prosumers paid 30–60% less. Their cost-reflective alternative creates two separate tariffs:[^28]

$$\text{Tariff}_{\text{load}} = \frac{C_{\text{load}}}{\text{Total load energy}}$$

$$\text{Tariff}_{DG} = \frac{C_{DG}}{\text{Total DG energy}}$$

All customers (including prosumers) pay Tariff_load on gross consumption; DG owners additionally pay Tariff_DG on gross generation.

---

## From Class Allocation to Individual Bills: How the Cross-Subsidy Materializes

A critical feature of all embedded cost studies is that the ECOS allocates to rate classes, not individual customers. The cross-subsidy arises in the within-class rate design step. For residential customers (who typically lack demand meters), demand-related costs are recovered through the volumetric energy charge:

$$\text{Volumetric rate}_j = \frac{\text{Class } j \text{ energy costs} + \text{Class } j \text{ demand costs}}{\text{Total class } j \text{ annual kWh}}$$

Customer _i_'s implicit shares are then:

$$\text{Energy cost share}_i = (\text{Class } j \text{ energy costs}) \times \frac{\text{kWh}_i}{\sum_k \text{kWh}_k}$$

$$\text{Demand cost share}_i = (\text{Class } j \text{ demand costs}) \times \frac{\text{kWh}_i}{\sum_k \text{kWh}_k}$$

(← via volumetric rate)

$$\text{Customer cost share}_i = \frac{\text{Class } j \text{ customer costs}}{N_j}$$

A net-metered solar customer who reduces net kWh purchases by 50% while maintaining similar peak demand avoids approximately half of the demand-related costs embedded in the volumetric rate. Those costs are reallocated to remaining kWh, raising the rate for non-solar customers. This is the mechanical core of the cross-subsidy in every embedded cost study. Load research data — historically from statistical samples of customers with interval meters, increasingly from universal AMI deployment — feed into this process by estimating class-level coincident peak demands and load shapes. The diversity factor (ratio of sum of individual NCPs to class NCP, typically 2–4× for residential customers) is critical for secondary distribution allocation.[^29]

---

## The Marginal-Cost Residual: Computing the Gap

The marginal-cost-plus-residual framework begins from the opposite direction. Instead of allocating historical costs, it sets prices at forward-looking marginal cost and then confronts the revenue shortfall.[^30] The residual is defined as:

$$R = TRR - \sum_i \sum_t (MC_{i,t} \times q_{i,t})$$

where TRR is the total revenue requirement (including fixed infrastructure costs, public purpose programs, sunk costs, and allowed return), MC_{i,t} is the marginal cost signal faced by customer _i_ at time _t_, and q_{i,t} is quantity consumed. Because distribution networks are natural monopolies (MC < AC) and because sunk network costs, renewable support programs, and policy charges are large, R is typically positive and substantial — Borenstein & Bushnell (2022) showed that roughly two-thirds of U.S. residential kWh face volumetric prices above social marginal cost.[^31]

---

## Borenstein's Framework: Efficiency versus Equity in Residual Recovery

Borenstein's central treatment of the residual appears in "The Economics of Fixed Cost Recovery by Utilities" (2016).[^32] He analyzes five allocation options with increasing theoretical sophistication.

**Average-cost volumetric markup.** Set p = AC = TRR / Σq. Customer _i_'s residual contribution:

$$R_i = (AC - SMC) \times q_i$$

This is proportional to consumption. Borenstein notes this is the dominant practice and attractive on equity grounds but creates deadweight loss because p > SMC discourages efficient consumption.

**Ramsey pricing.** The classical inverse-elasticity markup:

$$\frac{p_g - MC}{p_g} = \frac{\lambda}{\varepsilon_g}$$

where ε_g is the price elasticity for group _g_ and λ is a scalar set to satisfy the revenue constraint. Borenstein notes this minimizes deadweight loss but raises significant equity concerns because inelastic customers (those who need electricity most) bear higher markups.

**Uniform fixed charges.**

$$F = \frac{R}{N}$$

where $N$ is the total number of customers. $\text{Bill}_i = F + SMC \times q_i$. Borenstein calls this very attractive for minimizing deadweight loss — it creates no incentive to change electricity consumption choices — but raises distributional objections: why should a small apartment dweller pay the same as a mansion owner for common costs?

**Graduated fixed charges.** Fixed charges that increase at consumption thresholds. Borenstein warns these effectively create points at which the price for an incremental kWh is drastically greater than SMC and cause substantial deadweight loss.

**Demand charges.** Borenstein argues economics does not support demand charges for residual recovery, as they create perverse incentives.

Borenstein's recommended approach is a combination of fixed charges and increased volumetric prices — a two-part tariff where p > SMC but a fixed charge also contributes.[^33] His 2017 paper does not derive a residual allocation formula but empirically demonstrates that California's increasing-block tariff structure creates private solar value nearly as large as the 30% federal ITC for high-usage customers.[^34] His 2025 paper with Fowlie and Sallee ("Energy Hogs and Energy Angels") shows that 59–82% of the consumption difference between above- and below-median households is attributable to non-behavioral factors (household size, solar adoption, climate), undermining equity justifications for volumetric residual allocation.[^35]

---

## Pérez-Arriaga and the MIT Three-Component Tariff

The Comillas/MIT framework, developed in the _Utility of the Future_ report (2016) and formalized in Pérez-Arriaga, Jenkins & Batlle (2017),[^36] decomposes network costs into three charge components.

### Forward-Looking Peak-Coincident Network Charges (PCNC)

Set equal to the long-run incremental cost of future network reinforcement, allocated to customers based on their contribution to the peak of each network element:

$$PCNC_i = \sum_e \left[ \frac{\Delta C_e}{\Delta D_e} \times cf_{i,e} \right]$$

where ΔC_e is the incremental cost of reinforcing element _e_, ΔD_e is expected demand growth on element _e_, and cf_{i,e} is customer _i_'s contribution factor to the peak of element _e_, measured during the peak utilization hours of that element.

### Short-Run Marginal Cost Charges

Volumetric charges reflecting real-time energy losses and congestion.

### Residual Charges (Fixed)

The residual is:

$$\text{Residual} = TRR - \sum_i PCNC_i - \sum_i SRMC_i$$

The MIT/Comillas group explicitly recommends allocation via non-distortive fixed charges per connection point, with the critical requirement that these charges be independent of current consumption, injection, or capacity decisions so they cannot distort behavior.[^37] The simplest form is uniform per-customer:

$$F_i = \frac{\text{Residual}}{N}$$

Abdelmotteleb, Pérez-Arriaga & Gómez (2018) compute the residual element-by-element:[^38]

$$\text{Fixed}_i = \sum_e \left[ \left( TRR_e - \sum_k PCNC_{k,e} \right) \times w_i \right]$$

where w_i is an allocation weight (equal shares, proportional to contracted capacity, or proportional to historical consumption). The group permits differentiation by connection capacity, historical consumption, income, or property value — but insists the basis must be non-reactive to current behavior.

---

## Brown & Sappington's Optimal Pricing Model

Brown & Sappington (2017) build a formal welfare-maximizing model with two consumer types: D (can install solar) and N (cannot).[^39] The regulator sets four instruments: retail price _r_, DG compensation _w_, fixed charge _R_, and utility capacity _K_G_.

**When the fixed charge R is available**, it serves as the residual allocation mechanism. The zero-profit condition yields:[^40]

$$R = \frac{1}{2}\left[ \int (C^G(Q^v, K_G) - r \cdot X(\cdot)) \, dF(\theta) + w \cdot \theta^E \cdot K_D + C^K(K_G) + T(K_G, K_D) \right]$$

Each consumer (both D-type and N-type) pays R, splitting the utility's expected revenue shortfall equally. The retail price _r_ is set via a Ramsey-like condition where expected weighted deviations of _r_ from MC equal zero.

**When no fixed charge is permitted (R = 0)**, the residual must be recovered entirely through volumetric pricing, yielding average-cost pricing:[^41]

$$r = \frac{w \cdot \theta^E \cdot K_D + \int C^G(\cdot) \, dF(\theta) + C^K(K_G) + T(K_G, K_D)}{\int X(\cdot) \, dF(\theta)}$$

The residual is allocated proportionally to consumption. Additionally, DG compensation _w_ is distorted downward and _r_ is distorted upward by the shadow price of the zero-profit constraint. Their numerical results for a baseline California setting: optimal policy sets r = $273.4/MWh and w = $193.9/MWh (r is 41% above w). Under a net metering mandate (w = r), r = w = $313.7/MWh, with a welfare loss of 3.9%. Net metering benefits D consumers (+31% welfare) but harms N consumers (−9% welfare).[^42] The key insight: when R is available, the residual splits equally per customer; when unavailable, it shifts to consumption-proportional recovery with corresponding efficiency losses and cross-subsidization.

---

## Schittekatte, Meeus, and the Game-Theoretic Tariff Design

Schittekatte, Momber & Meeus (2018) model a non-cooperative game between reactive consumers (who can invest in PV + batteries) and passive consumers under different tariff structures.[^43] Under volumetric tariffs:

$$p_{\text{vol}} = \frac{C_{\text{grid}}}{\sum_i q_i}$$

(net or gross consumption)

Under capacity charges:

$$p_{\text{cap}} = \frac{C_{\text{grid}}}{\sum_i \text{peak}_i}$$

Their central finding: when costs are sunk, volumetric tariffs produce a "death spiral" dynamic — reactive consumers invest in DER to reduce bills, shifting costs to passive consumers. Capacity-based tariffs partially mitigate this but are not fully future-proof if batteries allow consumers to reduce measured peak demand.

In their 2020 paper, using a bilevel game-theoretical model, they find the least-cost tariff design combines a forward-looking peak-coincident charge (for incremental network costs) with a fixed residual charge (for sunk costs), uniform per connection point.[^44]

---

## Turk et al. (2025) and the ComEd Application

Turk, Schittekatte, Dueñas-Martínez, Joskow & Schmalensee (2025) apply the Pérez-Arriaga framework to a realistic Commonwealth Edison (ComEd) case study under increasing EV penetration.[^45] They separate network costs into forward-looking incremental costs (recovered through time-varying capacity charges at LRMC) and residual costs recovered through fixed charges per connection ($/customer/month). A key finding is that bundled TOU rates combining energy and network charges in a single $/kWh rate can perform worse than flat volumetric rates because they cause synchronized EV charging at TOU off-peak onset, creating new network peaks. Separating network charges from energy charges, with residuals recovered via fixed charges, significantly reduces network expansion costs.[^46]

---

## Batlle, Mastropietro & Rodilla's Differentiated Fixed Charge Proposal

The most targeted paper on residual allocation mechanics is Batlle, Mastropietro & Rodilla (2020).[^47] They define the residual formally as:

$$\text{Residual} = TRR - \text{Revenue}_{\text{cost-reflective charges}}$$

And review allocation methodologies with explicit formulas:

| Allocation Base            | Formula                                                             | Assessment                                                |
| -------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------- |
| Flat volumetric (€/kWh)    | $R_i = (R / \sum q) \times q_i$                                     | Inefficient; distorts consumption; enables gaming via DER |
| Flat fixed (€/customer)    | $R_i = R / N$                                                       | Efficient but inequitable (same charge for all)           |
| Contracted capacity        | $R_i = (R / \sum \text{kw}_{\text{contracted}}) \times \text{kw}_i$ | Risk of gaming if capacity is adjustable                  |
| Ramsey/inverse-elasticity  | $R_i \propto q_i / \varepsilon_i$                                   | Theoretically efficient; impractical; regressive          |
| **Historical consumption** | $FC_i = R \times (q_i^{\text{hist}} / \sum_j q_j^{\text{hist}})$    | Efficient, equitable, non-distortive                      |

Their innovation is allocating the residual via uneven fixed charges differentiated by backward-looking consumption frozen at a reference period (e.g., average of the past three years). This preserves the equity property of volumetric allocation (higher historical users pay more) while being non-distortive — customers cannot reduce their allocation by changing current behavior. This approach directly addresses the solar cross-subsidy problem because DER adoption after the reference period does not reduce a customer's residual contribution.[^48]

---

## Ramsey Pricing and Its Practical Limits for Residual Allocation

The classical Ramsey formula:

$$\frac{p_i - MC_i}{p_i} = \frac{\lambda}{\varepsilon_i}$$

has been extensively discussed but rarely implemented for electricity distribution. Brown & Faruqui's 2014 Brattle Group report for the Australian Energy Market Commission identifies Ramsey pricing as the guiding principle in the academic literature but notes it has rarely been applied for price discrimination across customers in the same class because equity considerations have stood in the way.[^49] At the class level, Ramsey logic suggests residential customers (less elastic) should bear a greater share of residual costs than industrial customers (more elastic). Within the residential class, it supports recovering residuals through fixed/demand charges (less elastic response) rather than volumetric charges (more elastic).

The solar-specific tension is significant: if solar customers have more elastic demand (they can further reduce grid consumption via batteries), Ramsey logic dictates they should bear _less_ residual cost — potentially worsening the cross-subsidy from an equity standpoint. Conversely, fixed charges are the component with the lowest elasticity of avoidance, so Ramsey principles support fixed-charge residual recovery, which would reduce the solar cross-subsidy.

Feldstein (1972) showed that when the social welfare function includes equity weighting, the Ramsey formula gains a distributional correction:[^50]

$$\frac{p_i - MC_i}{p_i} = \frac{\lambda - \beta_i}{\varepsilon_i}$$

where β_i is the "distributional characteristic" reflecting the covariance between social marginal utility of income and consumption of good _i_.

---

## Bonbright's Principles as the Evaluative Umbrella

Bonbright's _Principles of Public Utility Rates_ (1961; 2nd ed. 1988) remains the universally cited authority.[^51] His framework provides evaluative criteria — revenue sufficiency, fair cost apportionment, and optimal efficiency — rather than specific formulas. The key insight for residual allocation: long-run marginal or incremental costs should serve as the floor for rates, and the residual (costs not attributable on a causal/marginal basis) should be recovered in the least distortionary manner. Modern papers operationalize this through fixed charges (Pérez-Arriaga), historical-consumption-based charges (Batlle et al.), or income-graduated charges (Borenstein, Fowlie & Sallee 2021). The Regulatory Assistance Project's modern MCOS framework builds on these principles to advocate for marginal-cost-based rate design with explicit residual treatment.[^52]

---

## The NERA / Average-and-Excess Connection and Capacity-Based Alternatives

The "excess-load weighting" method associated with NERA Economic Consulting aligns closely with the average-and-excess (A&E) demand allocation method described above. The A&E formula — $AF_i = LF \times (AvgD_i / \sum AvgD) + (1-LF) \times (ExcD_i / \sum ExcD)$ — has been applied in Australian and South African contexts (the South African NRS 058:2000 "Cost of Supply Methodology" explicitly adopts the Excess Method). For solar customers specifically, the A&E method allocates more cost to "peaky" customers — and because solar customers reduce average demand but maintain evening peaks, their excess demand component _increases_, potentially resulting in more cost allocation and partially self-correcting the cross-subsidy.[^53]

Capacity-based alternatives have gained traction internationally. Italy's three-part tariff uses contracted connection capacity. France has used capacity-based charges since the 1950s (3kW connection ~€75/yr, 9kW ~€160/yr, 15kW ~€285/yr). Burger, Knittel, Pérez-Arriaga, Schneider & vom Scheidt (2020) tested uniform fixed charges, income-differentiated fixed charges, and demand-differentiated fixed charges using 30-minute interval data for 100,000+ Chicago customers, finding a strong correlation between customers' peak demands and their income, implying that retail tariffs with peak demand charges have attractive income distribution attributes.[^54] California's implementation of income-graduated fixed charges (CPUC 2024: $14–$22/month for CARE/low-income, $22–$42/month for others) represents the first large-scale application of income-based residual allocation, catalyzed by Borenstein, Fowlie & Sallee's finding that California's above-marginal-cost volumetric rates imposed an effective "electricity tax" of $500–$800/year that was more regressive than the state sales tax.[^55]

---

## Conclusion: Allocation Mechanics Drive the Measured Cross-Subsidy

The measured magnitude of solar cross-subsidization is not a fixed empirical quantity but an artifact of the allocation method chosen. Embedded cost studies using the minimum system method to classify distribution costs (assigning 65–77% as customer-related) find relatively small cross-subsidies because most costs are recovered through unavoidable per-customer charges. The same studies using the basic customer method (100% demand-related) find large cross-subsidies because demand costs recovered volumetrically are heavily avoided by solar customers. Marginal-cost studies face a different but analogous choice: whether to allocate the residual per-customer (minimizing the measured cross-subsidy), proportionally to consumption (maximizing it), or via some intermediate mechanism.

The emerging consensus across both traditions — from Pérez-Arriaga's MIT framework through Batlle et al.'s differentiated fixed charges to Borenstein's efficiency-equity analysis — converges on recovering residual costs through fixed charges that are non-reactive to current behavior. The key remaining disagreement is the differentiation basis: uniform per-customer (efficient but regressive), historical consumption (equitable and non-distortive but requiring a frozen baseline), contracted capacity (robust but gameable), or income (progressive but administratively complex). Each choice embeds a normative judgment about who should bear the costs of a shared infrastructure, and that judgment — not the engineering or economics — ultimately determines the cross-subsidy's measured size.

---

## Notes

[^1]: National Association of Regulatory Utility Commissioners, _Electric Utility Cost Allocation Manual_ (Washington, DC: NARUC, 1992). See also [NARUC Ratemaking Fundamentals and Principles](https://www.naruc.org/commissioners-desk-reference-manual/3-ratemaking-fundamentals-and-principles/).

[^2]: Jim Lazar, Paul Chernick, William Marcus & Mark LeBel, _Electric Cost Allocation for a New Era: A Manual of the National Association of Regulatory Utility Commissioners_ (Montpelier, VT: Regulatory Assistance Project, 2020). Available via [LPDD](https://lpdd.org/resources/rap-report-electric-cost-allocation-for-a-new-era/).

[^3]: The Prime Group, _Cost of Service Study Overview_ (2017). Available at [theprimegroupllc.com](http://www.theprimegroupllc.com/COSS_Overview.pdf).

[^4]: Ahmad Faruqui & Ryan Hledik, _Retail Costing and Pricing of Electricity_ (Cambridge, MA: The Brattle Group). Available at [brattle.com](https://www.brattle.com/wp-content/uploads/2017/10/5761_retail_costing_and_pricing_of_electricity.pdf).

[^5]: Faruqui & Hledik (n. 4).

[^6]: Faruqui & Hledik (n. 4). See also ScienceDirect's overview of [system peak demand allocation methods](https://www.sciencedirect.com/topics/engineering/system-peak-demand).

[^7]: NARUC (n. 1); Faruqui & Hledik (n. 4).

[^8]: Faruqui & Hledik (n. 4).

[^9]: Faruqui & Hledik (n. 4). The NCP method is standard for distribution facilities because distribution investment is driven by local/class peaks rather than system peaks.

[^10]: The Prime Group (n. 3); Faruqui & Hledik (n. 4).

[^11]: See Missouri Public Service Commission, _Cost Allocation and Rate Design Testimony_, available at [efis.psc.mo.gov](https://www.efis.psc.mo.gov/mpsc/commoncomponents/viewdocument.asp?DocId=4231158). See also [efis.psc.mo.gov](https://efis.psc.mo.gov/Document/Display/840207) for AmerenUE average-and-excess applications.

[^12]: BC Hydro, _Electric Distribution System Cost of Service Study_ (Vancouver: BC Hydro, 2015). Available at [bchydro.com](https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/regulatory-planning-documents/regulatory-matters/cos-workshop-electric-distribution-system-study.pdf).

[^13]: BC Hydro (n. 12).

[^14]: Missouri Public Service Commission (n. 11); The Prime Group (n. 3).

[^15]: BC Hydro (n. 12); The Prime Group (n. 3).

[^16]: BC Hydro (n. 12).

[^17]: Lazar et al. (n. 2).

[^18]: Sanem Sergici, Yang Yang, Marcelino Castañer & Ahmad Faruqui, "Quantifying Net Energy Metering Subsidies," _The Electricity Journal_ 32, no. 8 (2019). Available at [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S1040619019301861); see also [Brattle summary](https://www.brattle.com/insights-events/publications/brattle-economists-assess-the-magnitude-of-residential-net-metering-subsidies-in-several-u-s-states/).

[^19]: Sergici et al. (n. 18).

[^20]: Energy and Environmental Economics (E3), _Avoided Costs and the Benefits of Distributed Resources for an Evolving Grid_. See [E3 Avoided Costs](https://www.ethree.com/avoided-costs/).

[^21]: CPUC Public Advocates Office, _2024 NEM Cost Shift Fact Sheet_ (San Francisco: CPUC, 2024). Available at [publicadvocates.cpuc.ca.gov](https://www.publicadvocates.cpuc.ca.gov/-/media/cal-advocates-website/files/press-room/reports-and-analyses/240822-public-advocates-office-2024-nem-cost-shift-fact-sheet.pdf).

[^22]: CPUC Public Advocates Office (n. 21); see also Cal Advocates, _NEM Successor Tariff Proposal_ presentation slides, available at [cpuc.ca.gov](https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/energy-division/documents/net-energy-metering-nem/nemrevisit/public-advocates-office-slides.pdf).

[^23]: CPUC Public Advocates Office, _Rooftop Solar Incentive Cost Shift Report_ (San Francisco: CPUC, 2024). Available at [publicadvocates.cpuc.ca.gov](https://www.publicadvocates.cpuc.ca.gov/-/media/cal-advocates-website/files/press-room/reports-and-analyses/240208-cal-advocates-2024-rooftop-solar-incentive-cost-shift.pdf).

[^24]: Paul Simshauser, "Distribution Network Prices and Solar PV: Resolving Rate Instability and Wealth Transfers through Demand Tariffs," _Energy Economics_ 54 (2016): 108–122. Available at [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988315003060).

[^25]: Simshauser (n. 24).

[^26]: Simshauser (n. 24). See also Paul Simshauser, "Demand Tariffs: Resolving Rate Instability and Hidden Subsidies" (2015), available at [ResearchGate](https://www.researchgate.net/publication/276415820_Demand_Tariffs_resolving_rate_instability_and_hidden_subsidies); and Paul Simshauser, "Queensland Solar: Cause and Effects of Mass Rooftop Solar PV Take-Up Rates" (Griffith University Working Paper No. 2022-05), available at [griffith.edu.au](https://www.griffith.edu.au/__data/assets/pdf_file/0033/1799250/No.2022-05-Queensland-Solar-Cause-and-effects-of-mass-rooftop-solar-PV-take-up-rates.pdf).

[^27]: Andrés Picciariello, Claudio Vergara, Javier Reneses, Pablo Frías & Lennart Söder, "Electricity Distribution Tariffs and Distributed Generation: Quantifying Cross-Subsidies from Consumers to Prosumers," _Utilities Policy_ 37 (2015): 23–33. Available at [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0957178715300230); see also [ResearchGate](https://www.researchgate.net/publication/284085233_Electricity_distribution_tariffs_and_distributed_generation_Quantifying_cross-subsidies_from_consumers_to_prosumers).

[^28]: Picciariello et al. (n. 27).

[^29]: Faruqui & Hledik (n. 4); The Prime Group (n. 3).

[^30]: Regulatory Assistance Project, "Modern Marginal Cost of Service Studies" (blog post, 2020). Available at [raponline.org](https://www.raponline.org/blog/modern-marginal-cost-of-service-studies/).

[^31]: Severin Borenstein & James Bushnell, "Do Two Electricity Pricing Wrongs Make a Right? Cost Recovery, Externalities, and Efficiency," _American Economic Journal: Economic Policy_ 14, no. 4 (2022): 80–110. Working paper version available at [Berkeley Energy Institute WP-272](https://ei.haas.berkeley.edu/research/papers/WP272.pdf).

[^32]: Severin Borenstein, "The Economics of Fixed Cost Recovery by Utilities," _The Electricity Journal_ 29, no. 7 (2016): 5–12. Available at [ADS](https://ui.adsabs.harvard.edu/abs/2016ElecJ..29g...5B/abstract).

[^33]: Borenstein (n. 32).

[^34]: Severin Borenstein, "Private Net Benefits of Residential Solar PV: The Role of Electricity Tariffs, Tax Incentives and Rebates," _Journal of the Association of Environmental and Resource Economists_ 4, no. S1 (2017): S85–S122. Working paper available at [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2629947).

[^35]: Severin Borenstein, Meredith Fowlie & James Sallee, "Energy Hogs and Energy Angels: The Politics of Electricity Rate Design" (Berkeley Energy Institute Working Paper 341R, 2025). Available at [haas.berkeley.edu](https://haas.berkeley.edu/wp-content/uploads/WP341.pdf).

[^36]: Ignacio Pérez-Arriaga, Jesse Jenkins & Carlos Batlle, "A Regulatory Framework for an Evolving Electricity Sector: Highlights of the MIT Utility of the Future Study," _Economics of Energy & Environmental Policy_ 6, no. 1 (2017). See also Ignacio Pérez-Arriaga, "Efficient Tariff Structures for Distribution Network Services," available at [ResearchGate](https://www.researchgate.net/publication/285672765_Efficient_tariff_structures_for_distribution_network_services).

[^37]: Pérez-Arriaga et al. (n. 36); see also Florence School of Regulation overview, "Designing Distribution Grid Tariffs for Tomorrow," available at [fsr.eui.eu](https://fsr.eui.eu/designing-distribution-grid-tariffs-tomorrow/).

[^38]: Ibrahim Abdelmotteleb, Tomás Gómez & Ignacio Pérez-Arriaga, "Designing Efficient Distribution Network Charges in the Context of Active Customers" (MIT CEEPR Working Paper, 2018). See framework discussed in Pérez-Arriaga (n. 36).

[^39]: David Brown & David Sappington, "Designing Compensation for Distributed Solar Generation: Is Net Metering Ever Optimal?" _The Energy Journal_ 38, no. 3 (2017). Working paper available at [University of Florida PURC](https://bear.warrington.ufl.edu/centers/purc/docs/papers/1605_Sappington_Designing%20Compensation%20for%20Distributed%20Solar%20Generation.pdf).

[^40]: Brown & Sappington (n. 39).

[^41]: Brown & Sappington (n. 39).

[^42]: Brown & Sappington (n. 39).

[^43]: Tim Schittekatte, Ilan Momber & Leonardo Meeus, "Future-Proof Tariff Design: Recovering Sunk Grid Costs in a World Where Consumers Are Pushing Back," _Energy Economics_ 70 (2018): 484–498. Available at [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988318300367); see also [EUI repository](https://cadmus.eui.eu/handle/1814/46044).

[^44]: Tim Schittekatte & Leonardo Meeus, "Least-Cost Distribution Network Tariff Design in Theory and Practice," _The Energy Journal_ 41, no. 5 (2020). Available at [Semantic Scholar](https://www.semanticscholar.org/paper/Least-Cost-Distribution-Network-Tariff-Design-in-Schittekatte-Meeus/fd6df4fb933217e194f235ec0d64638fa512fb39).

[^45]: Graham Turk, Tim Schittekatte, Pablo Dueñas-Martínez, Paul L. Joskow & Richard Schmalensee, "Designing Distribution Network Tariffs in the US with an Application to Increased Electric Vehicle Adoption," _The Energy Journal_ 46, no. 6 (2025). Available at [Sage Journals](https://journals.sagepub.com/doi/10.1177/01956574251365616).

[^46]: Turk et al. (n. 45).

[^47]: Carlos Batlle, Paolo Mastropietro & Pablo Rodilla, "Redesigning Residual Cost Allocation in Electricity Tariffs: A Proposal to Balance Efficiency, Equity and Cost Recovery," _Renewable Energy_ 155 (2020): 257–267. Available at [ResearchGate](https://www.researchgate.net/publication/340302634_Redesigning_residual_cost_allocation_in_electricity_tariffs_A_proposal_to_balance_efficiency_equity_and_cost_recovery).

[^48]: Batlle et al. (n. 47).

[^49]: David Brown & Ahmad Faruqui, _Structure of Electricity Distribution Network Tariffs: Recovery of Residual Costs_ (report to the Australian Energy Market Commission, Cambridge, MA: The Brattle Group, 2014). See also Australian Energy Regulator, _Network Tariffs for the Distributed Energy Future_ (Argyle Consulting & Endgame Economics, 2022), available at [aer.gov.au](https://www.aer.gov.au/system/files/Argyle%20Consulting%20and%20Endgame%20Economics%20-%20Battery%20tariffs%20-%20Network%20tariffs%20for%20the%20DER%20future_0.pdf).

[^50]: Martin S. Feldstein, "Distributional Equity and the Optimal Structure of Public Prices," _American Economic Review_ 62, no. 1 (1972): 32–36.

[^51]: James C. Bonbright, Albert L. Danielsen & David R. Kamerschen, _Principles of Public Utility Rates_, 2nd ed. (Arlington, VA: Public Utilities Reports, 1988).

[^52]: Regulatory Assistance Project (n. 30); Lazar et al. (n. 2).

[^53]: Faruqui & Hledik (n. 4); Brown & Faruqui (n. 49).

[^54]: Severin Burger, Christopher R. Knittel, Ignacio Pérez-Arriaga, Ian Schneider & Frederik vom Scheidt, "The Efficiency and Distributional Effects of Alternative Residential Electricity Rate Designs," _The Energy Journal_ 41, no. 1 (2020). See [MIT CEEPR](https://ceepr.mit.edu/the-efficiency-and-distributional-effects-of-alternative-residential-electricity-rate-designs/).

[^55]: Borenstein, Fowlie & Sallee (n. 35).

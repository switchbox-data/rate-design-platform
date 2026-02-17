# The Bill Alignment Test: Identifying Trade-Offs of Residential Electricity Rate Design Options

**Source**: bill_alignment_test.pdf\
**Pages**: 30 total pages\
**Date**: © 2023 published by Elsevier\
**Author(s)**: Christina E. Simeone, PhD; Pieter Gagnon; Peter Cappers; Andrew Satchwell

**Author affiliations:**

- **Christina E. Simeone** (Author for correspondence): 245 Fairhill Rd, Wynnewood, PA 19096, [christina.e.simeone@gmail.com](mailto:christina.e.simeone@gmail.com)
- **Christina E. Simeone**: Advanced Energy Systems, Affiliate Faculty, Economics and Business, Colorado School of Mines, 1500 Illinois Street, Golden Co, 80401; Joint Institute for Strategic Energy Analysis, National Renewable Energy Laboratory, 15013 Denver West Parkway, Golden, CO 80401
- **Pieter Gagnon**: Senior Energy Systems Researcher, Grid Planning and Analysis Center, National Renewable Energy Laboratory, 15013 Denver West Parkway, Golden, CO 80401
- **Peter Cappers**: Staff Scientist, Lawrence Berkeley National Laboratory, Electricity Markets & Policy, Energy Technologies Area, 1 Cyclotron Road, MS 90R4000, Berkeley, CA 94720
- **Andrew Satchwell**: Research Scientist and Deputy Leader, Electricity Markets and Policy Department, Lawrence Berkeley National Laboratory, Electricity Markets & Policy, Energy Technologies Area, 1 Cyclotron Road, MS 90R4000, Berkeley, CA 94720

Version of Record: [ScienceDirect article](https://www.sciencedirect.com/science/article/pii/S0957178723000516).

---

## Abstract

The proliferation of smart meter data allows the application of new analytic methods to inform regulatory deliberations. The bill alignment test (BAT) method, which compares the costs allocated to each residential customer with their electric bill, is introduced to help regulators consider how a proposed rate design balances various regulatory criteria. The BAT requires an explicit statement of preferences by policymakers or stakeholders and choices about allocating residual costs unassociated with customer-level causality. The BAT is applied to more than 35,000 smart-meter customer load profiles to assess the trade-offs associated with proposed rate designs. This example demonstrates the impact of residual cost allocation preferences and tariff design choices on proposed tariff evaluation.

**Keywords**: Rate design, electric utility regulation, residual cost allocation, smart meter data

---

## 1. Introduction

The job of electric utility regulators involves balancing competing policy objectives and is becoming increasingly complicated due to multiple factors. For example, the time-varying of costs and value of electricity is shifting due to increasing wind and solar deployment, and customer adoption of distributed energy resources (DERs) is challenging previous frameworks for evaluating "fairness." At the same time, customer-level data has increased significantly as more than 50% of all electricity customers in the United States now have a smart meter [1]. The proliferation of smart meters creates an opportunity to develop analytic methods to inform regulatory decisions about innovative rate design proposals presented in response to the changing electric power landscape. This article introduces the bill alignment test (BAT), which compares the costs allocated to each customer with their electric bill, to help regulators consider how well a proposed rate design balances multiple regulatory criteria for designing retail tariffs.

The BAT assesses trade-offs among rate design options in achieving regulatory goals through various metrics associated with consumer cross-subsidies (i.e., intra-customer class transfers). For this analysis, we use the term "cross-subsidy" to mean one customer pays more than the regulator's stated preference and thus another pays less. To inform decision-making, the BAT outputs customer-level information that can be expressed through single metrics (e.g., total cross-subsidies between two consumer subclasses) or graphics such as histograms. We conduct a case study to implement the BAT method on a data set of more than 35,000 hourly-meter residential customer load profiles using proxy data for utility and system marginal costs and synthetically designed tariffs.

The BAT creates metrics that can be used to quantify how each rate design proposal performs against a particular regulatory goal. James Bonbright's _Principles of Utility Rates_ identified widely recognized principles for evaluating utility rates. The BAT enables the quantification of some, but not all, of these and other potential regulatory goals. This approach can facilitate an initial rank-ordering of proposed tariff designs as a helpful starting point in regulatory deliberations.

The BAT calculation of the costs allocated to a customer considers marginal costs directly related to that customer (e.g., usage) plus some share of residual costs (i.e., the revenue requirement minus total marginal costs) unrelated to the customer usage. Residual costs are expenses unassociated with customer-level causality. Therefore, apportionment of these costs to customers relies on alternative principles. Uniquely, the BAT method requires intended users (e.g., analysts, regulators, utilities, and other stakeholders) to choose a preferred residual cost apportionment method explicitly. Current regulatory practice may not state preferences explicitly or evaluate tariff performance against the criteria used to establish such preferences. This potential omission leaves a critical regulatory determination with significant consumer cost impacts unexamined. For example, Brown and Faruqui found that electric utility residual costs in Great Britain ranged from -11.2% to 51.8% of a utility's total revenue requirement [2]. This paper demonstrates the BAT metrics used to assess tariffs can be significantly influenced by the stated preference of residual cost allocation method. In this analysis, deadweight loss is estimated using simplifying assumptions to evaluate the economic efficiency of a rate design.

Through BAT graphics and metrics, we evaluate how different tariff designs (e.g., flat and time-of-use [TOU] rates) under several possible residual cost allocation methods perform in achieving potential regulatory goals. Our results highlight the importance of residual cost allocation method choice to the achievement of regulatory goals. Given the limited research on this topic, our application of the BAT seeks to move analytical processes forward by bringing in explicit customer-level considerations not previously analyzed in other approaches.

**Note**: The authors note this method could also be used to design rates, but in practical application, the parties evaluating rates (e.g., regulators, stakeholders) are often not the same parties designing rates (i.e., utilities).

**Note**: For example, Bonbright principles consider revenue sufficiency, fair apportionment of risks and costs, economic efficiency, consumer acceptability, and bill stability [23], [40], [41].

**Note**: These potential regulatory goals include minimizing total intra-class cross-subsidies, avoiding non-solar consumers subsidizing solar consumers, reducing the energy burden for low-income customers, and avoiding subsidies for high energy use customers.

The smart meter data in our analysis is limited to customer loads without on-site generation, which means on-site solar generation must be synthetically modeled. We restrict our analysis to tariffs that achieve a specified revenue target by collecting revenues exclusively through electric bills. We acknowledge proposals for achieving total cost recovery through non-bill mechanisms. For example, some suggest non-bill methods of residual recovery would maximize social welfare and facilitate equitable outcomes by apportioning some electricity charges based on personal income or property values or forms of taxation [3]–[6]. In theory, the BAT could be expanded to assess cost recovery approaches that include non-bill recovery methods; such exploration is beyond the scope of this paper.

This paper begins with background information and a literature review demonstrating the research gap filled by this work. The BAT methodology is described in Section 3, followed by an overview of proxy data used in place of utility-specific data to demonstrate the method's results in Section 4. Results are presented in Section 5, followed by conclusions in Section 6 and discussion and limitations in Section 7.

---

## 2. Background and Literature Review

In this section, we review quantitative methods for analyzing rate designs, discuss commonly used metrics, and outline the treatment of residual costs in the literature.

### 2.1 Quantitative Methods for Rate Design Proposal Analysis

Since the late 1970s, regulators in the United States have quantitatively evaluated proposed utility rate structures for accepted purposes, such as the ability to recover the revenue requirement [7]. Nakamura et al. detailed a TOU pricing computer program used by the Public Utilities Commission of Ohio to calculate the monthly charges to a specific user group based on a time-of-day pricing rate design [8]. Hourly load data for residential customers was unavailable and had to be estimated for the program to use (i.e., assuming residential class load was proportional to the system load and then adjusting using parameters informed by load survey studies). In addition, monthly and annual total consumption and charges were the only outputs.

Quantitative methods for rate design evaluation have advanced since the 1970s. For example, Christensen et al. developed a ranking method for assessing various dynamic distribution tariffs' technical, economic, social, and regulatory feasibility [9]. Technical readiness of a tariff was scored on a nine-level scale based on NASA's Technology Readiness Level method. Economic, social, and regulatory feasibility was determined by judging where to place the tariff on a three- or four-part readiness scale.

A few technically complex methods to quantitatively evaluate the performance of new tariff designs have been proposed, some of which use smart meter data. Abdelmotteleb et al. proposed a method to quantitatively analyze and rank-order distribution-level tariff designs across four performance attributes related to network cost recovery, deferral of network reinforcements, efficient consumer response, and recognition of side effects on consumers [10]. Li et al. proposed an integrated four-model process for quantitatively evaluating the economic efficiency of long-run network pricing models, where the optimal proposal is identified based on the lowest present value cost to meet the network's required reinforcements [11]. Jargstorf et al. developed a framework and metrics for assessing tariff design proposals based on embedded costs, emphasizing how customers with self-generation would react to these changes [12].

Nijhuis et al. developed a method and metrics for assessing the cost-reflectivity and predictability of different European Union tariff structures on network costs attributed to individual residential users [13]. Passey et al. developed a method for assessing the cost-reflectivity of demand charge tariffs by comparing the demand charge portion of a residential customer's bill with the customer's coincident demand [14].

A variety of analytic methods are available to regulators to study discrete aspects of utility proposals. In general, indexing, econometric methods, data envelop analysis, and other methods can be used to assess the performance of utilities based on characteristics such as reliability, customer satisfaction, capacity utilization, cost efficiency, and other methods [15]. Program or measure-specific analytics are widely used to test the costs and benefits of utility energy efficiency proposals (e.g., the total resource cost, societal cost test, and rate impact measure methods). For example, see [National Action Plan for Energy Efficiency (2008), Understanding Cost-Effectiveness of Energy Efficiency Programs](https://19january2017snapshot.epa.gov/sites/production/files/2015-08/documents/understanding_cost-effectiveness_of_energy_efficiency_programs_best_practices_technical_methods_and_emerging_issues_for_policy-makers.pdf). New benefit-cost analysis methods are also being proposed to screen utility proposals related to DERs. For example, see [National Standard Practice Manual for Benefit-Cost Analysis of Distributed Energy Resources](https://www.nationalenergyscreeningproject.org/national-standard-practice-manual/). Economic efficiency, cost causality, and revenue sufficiency are most straightforward to measure through methods such as deadweight loss, cost of service studies, and cost-based accounting. Existing measures to assess the protection of vulnerable populations may include the calculation of energy burdens (i.e., the portion of household income spent on electricity bills) and the assessment of the sufficiency and availability of economic assistance programs (e.g., weatherization programs, universal service funds, and reduced rates). The California Alternative Rates for Energy (CARE) program is one example; see [CPUC CARE/FERA](https://www.cpuc.ca.gov/industries-and-topics/electrical-energy/electric-costs/care-fera-program).

### 2.2 Metrics to Assess Rate Design Impacts

The results of any rate analysis should ideally be understandable and relevant to regulatory decision-making. The BAT produces graphics and metrics to give quantitative structure to a decision. However, these outputs must be flexible because there are objective and subjective aspects of utility ratemaking.

Costello presented a qualitative framework to help state regulators consider alternative rate mechanisms [16]. The framework recognizes that utility ratemaking requires subjective judgment about balancing and achieving multiple, often competing, regulatory goals. The framework suggests commissions should:

1. be proactive,
2. consider utility performance as a key criterion,
3. define the meaning of public interest,
4. seek to maximize overall social welfare, and
5. systematically process information.

Consistent with the qualitative framework outlined by Costello, quantitative rate design evaluation metrics should be flexible enough to inform regulators who may place different weights of importance on discrete and often competing regulatory goals.

The BAT adds structure by introducing previously unquantified metrics, but it maintains flexibility by allowing BAT users to prioritize which metrics are most important. For example, whether a rate design meets the regulatory principles of fairness and equity may be difficult to evaluate. As explored in detail by Burger et al., the concepts of equity become more complicated as DER penetration and subsidies increase [17], and concepts like fairness can mean different things to different people [18]. Some rate designs that improve economic efficiency may be opposed by consumer advocates who assert such rates are inequitable because of higher costs or less predictability for consumers [19]. On the other hand, some may object to the cross-subsidy-related inequities created by conventional flat rates [20]. In the presence of DERs, there are concerns about cross-subsidies from non-DER consumers to DER consumers and the potentially regressive nature of some DER subsidies [21]. The BAT metrics inform these debates by quantifying and comparing how a rate design impacts different consumer subgroups while allowing users to determine how to use (e.g., hierarchically rank) these metrics.

This study fills a research gap by presenting a method and metrics for quantitatively evaluating residential electric utility rate design proposals, and it emphasizes the impacts of residual cost allocation choices. The BAT does not identify the optimal rate design; rather, it enables users to understand the trade-offs in meeting regulatory goals associated with each design proposal. This method is most useful in jurisdictions that do not have rigorous cost allocation methodology requirements. For example, this method may or may not apply to European jurisdictions [22].

### 2.3 Stated Preference for Residual Cost Allocation

The difference between a utility's regulator-approved revenue requirement and its economic cost-of-service (e.g., the revenue the utility would collect if they charged economically efficient prices) is referred to as the residue of total costs or simply "the residual" [23]. Residual costs typically occur because long-run marginal costs tend to be lower than average costs for systems in the U.S., are often driven by fixed (as opposed to variable) costs, and can be exacerbated by policy-related costs [2], [23], [24].

Unlike economic costs that can be allocated to customers based on principles of cost-causality, residual costs cannot be allocated on a cost-finding basis [23]. Deciding how much each customer should contribute to the residual relies on principles other than economic efficiency, including but not limited to welfare maximization, bill stability, revenue stability, or myriad other regulatory objectives.

The recovery of residual costs through electricity rates can distort economic signals to customers or shift residual costs disproportionately onto certain users. The predominant method of collecting residual costs is on a volumetric basis ($/kWh) [2], [25]. Therefore, customers that can reduce their usage (kWh)—such as those with DERs—can decrease their contribution to economic and residual cost recovery. Reduced customer usage will decrease the utility's economic costs but will not reduce its residual costs. So, the utility will need to recover a larger portion of residual costs from other customers who have not reduced their usage.

Batlle et al. present an overview of alternative tariffed residual cost allocation (i.e., residual collection) methods widely discussed in contemporary literature, including increased volumetric charges (e.g., postage-stamp, average-cost, or net consumption charges), fixed charges, fixed charges with low-income exemptions, capacity-based charges, tiered pricing, minimum bills, and Ramsey pricing [26]. Engineering solutions focus on developing cost-reflective methods for allocating network charges, which can be drivers of residual costs [13], [14], [27]. Dameto et al. suggest that long-term network costs driven by peak load reinforcement expenditures should be recovered through coincident peak demand charges, while the remaining (or residual) network costs should be recovered through a fixed charge [28].

A discussion of the merits and drawbacks of these proposals is beyond the scope of this analysis; however, exploring alternatives to long-implemented residual cost allocation methods is likely to raise regulator and stakeholder concerns, as some customers or policy goals will be comparably better or worse off with such a transition. Currently, the preference for the residual cost allocation method is not explicitly stated in any step in the regulatory process. In practice, this may be a difficult choice on which to reach consensus.

---

## 3. Methodology

This section begins with a brief overview of the BAT method, followed by a detailed discussion of each step in its process. Utility data are proxied consistent with the methods described in Section 4 to highlight the method's capabilities.

### 3.1 BAT Theory Overview

What is missing from current rate evaluation procedures is an articulation from regulators about their preference for how the residual should be allocated to customers and a subsequent analysis of how well the resulting tariff collection aligns with that stated preference. The BAT addresses these simultaneously by requiring the user to select the preferred method of residual cost allocation and comparing each customer's bill with their allocated system costs. As shown on the right side of Figure 1, the rate-setting process assigns direct costs (e.g., marginal costs); functionalizes, classifies, and allocates joint or common costs (e.g., residual costs); and then reconciles the resultant rate to ensure revenue sufficiency. This process delivers a tariff rate (and customer charges, if applicable) applied to customer bills.

The actions in the orange box on the left side of Figure 1 are new steps in the regulatory review process introduced by this new method. The BAT fills a missing step by comparing bills at the customer level against the costs allocated to that consumer in the tariff rate-setting process. This extra step will help stakeholders understand whether the results of applying the tariff are consistent with the assumptions incorporated by the utility when designing the tariff, including the stated preference for the residual cost allocation method.

[DIAGRAM DESCRIPTION: Filling a missing step in the regulatory review process for rate setting]

A two-part flowchart. **Left side (new BAT steps, in orange):** A box labeled with BAT-specific actions: (1) State preference for residual cost allocation, (2) Calculate each customer's allocated system costs (economic + residual share), (3) Calculate each customer's bill under the proposed tariff, (4) Compare bill to allocated costs (bill alignment). These steps feed into an output such as "BAT metrics and graphics." **Right side (existing rate-setting process):** Sequential steps: assign direct costs (e.g., marginal costs); functionalize, classify, and allocate joint/common costs (e.g., residual costs); reconcile resultant rate for revenue sufficiency; output is the tariff rate and customer charges applied to bills. The diagram shows that the BAT adds a parallel evaluation track that compares customer-level bills to allocated costs once the tariff is set, filling a missing step in regulatory review.

[→ See original PDF page 7 for visual rendering]

As shown in Figure 1, the method calculates each customer's bill and the amount each customer would ideally pay (based on the stated principles of the regulator or user). By looking at the difference between these two numbers for a population of customers, the user can better understand how well a particular tariff achieves a regulator's objective(s). Said differently: the BAT method identifies misalignment between a regulator's objective(s) and a proposed tariff. Given that the method calculates customer-level costs (i.e., how much each customer "should" be paying), the process requires a method of residual cost allocation to be explicitly stated. In this way, the BAT results are sensitive to residual cost allocation method choice.

To explain this, we illustrate the following example (see Appendix C for details). Assume a tariff has been proposed to regulators, resulting in an annual bill of $889 for a customer using 750 kWh/month ($5/month customer charge, plus $0.092157/kWh). The right side of Figure 2 shows the customer's annual bill components, collected as revenue by the utility. The left side of Figure 2 shows the customer's allocated system costs based on two methods: per customer or flat residual cost allocation (left) and per kilowatt-hour or volumetric residual cost allocation (right). Economic costs are the same in both system cost allocation scenarios ($531). Using per-customer residual cost allocation, the total system cost allocated to the customer is $929 ($531 economic and $398 residual). Using per kilowatt-hour residual cost allocation, the total system costs allocated to the consumer is $882 ($531 economic and $351 residual). Therefore, the bill alignment value using per-customer residual cost allocation would be approximately -$40 (indicating an underpayment or receipt of a cross-subsidy) and $7 (indicating an overpayment or paying of cross-subsidy) using per kilowatt-hour residual cost allocation. Recall that at this step in the process, the tariff-based bill does not change based on residual cost allocation method choice.

[DIAGRAM DESCRIPTION: Example highlighting how residual cost allocation impacts utility cost allocation]

A split view for one example customer (750 kWh/month, annual bill $889). **Left side:** Two stacked bars for "Allocated system costs." First bar: "Per-customer (flat) residual allocation" — Economic $531, Residual $398, Total $929. Second bar: "Per kWh (volumetric) residual allocation" — Economic $531, Residual $351, Total $882. **Right side:** One bar for "Bill (revenue collected)" — fixed charge and volumetric charge totaling $889. The diagram shows that the same bill is compared to two different allocated-cost totals depending on residual allocation method, yielding bill alignment of -$40 (flat) or +$7 (volumetric).

[→ See original PDF page 8 for visual rendering]

So, how can the bill alignment values help determine whether this tariff is well-designed or poorly designed? For both system cost allocation methods, the regulator believes each customer should pay enough to cover their economic costs. If the per-customer residual cost allocation method is chosen, the stated preference of regulators assumes each customer should contribute equally to recovering residual revenues. If the per kilowatt-hour residual cost allocation method is chosen, regulators believe each customer should contribute proportionally to their annual consumption (kWh). The BAT user can determine how the tariff performs by comparing the results of these choices against the customer's annual bill (i.e., -$40 under-collection and $7 over-collection, respectively). By performing these calculations for the entire ratepaying population, the user could assemble a suite of metrics to judge the effectiveness of the tariff. The basic principle of effectiveness would be minimizing cross-subsidies (i.e., maximize the number of customers with bill alignment values of zero). In this example, the smallest absolute value from zero (i.e., $7) would be more effective. Section 5 describes how the BAT metrics can be used to judge tariff performance against other measures of regulatory effectiveness.

### 3.2 Calculate the Economic Cost

Economic theory suggests efficiency is maximized when prices are set at the cost of producing an additional unit of a good (i.e., the marginal cost). A customer's economic cost is constructed by multiplying the marginal cost of service by the quantity each customer consumes. The details of which costs are included and excluded in the definition of marginal costs, such as the time frame considered (short-run versus long-run), data inputs, calculation methods, and other details are likely to change with each jurisdiction. As a result, the definition of marginal costs may differ from jurisdiction to jurisdiction. Herein we use total societal marginal costs as described in Appendix A. For the sake of simplicity, we characterize each of these services as a marginal cost ($/kWh) hourly for the region being studied. We then calculate each customer's economic cost as their historical consumption multiplied by the cost ($/kWh) of each of those services in that hour of the year, summing the result across all hours and services. We stress that this approach is only one of many possible approaches for calculating the customer-level economic costs: the BAT only requires that an economic cost be calculated, and the methodology for doing so depends on the stakeholders and data available.

### 3.3 Calculate and Allocate the Residual Revenue Requirement

Utilities assign direct costs, such as the cost of energy consumed, to customer classes and consumers. Remaining costs, such as joint or common costs, are then allocated to customer classes and consumers based on a variety of functionalization (i.e., assign costs to specific utility operational functions, such as transmission or distribution) and classification (i.e., grouping functionalized cost by characteristics that bear a relationship to how these costs were incurred) methods.

A utility's revenue requirement is the revenue a utility's regulators authorize the utility to collect from customers to recover all prudent utility costs. It typically is a combination of remuneration for expenses plus a regulator-approved rate of return on the utility's rate base (e.g., capital investments net of depreciation). As discussed in Section 2.3, subtracting the annual economic cost (i.e., system marginal costs multiplied by the aggregate demand for the customer class) for all customers from the utility's revenue requirement yields the residual revenue requirement (herein referred to as the residual). Here, total economic costs are analogous to direct costs, whereas total residual costs are analogous to joint or common costs.

**Equation (1):**

$$
\text{Residual Revenue Requirement} = \text{Revenue Requirement} - \text{Economic Cost}
$$

The residual is the difference between the amount the utility is authorized to collect and the amount it would collect if it were competitively priced electricity (i.e., at its marginal cost). By authorizing a revenue requirement that deviates from the economically efficient level, regulators express an annual dollar size of the residual but not a preference for how it should be collected from ratepayers. Residual costs are often, but not always, volumetrically allocated to consumers [26], following the notion that customers' contributions to non-marginal costs should be proportional to how much electricity they consume. Alternative methods of residual cost allocation were discussed in Section 2.3.

We implement three different residual cost allocation methods: flat, volumetric, and volumetric excluding low-income customers. Dividing the total residual revenue by the total number of customers yields the flat rate residual, which is then applied to each customer $i$'s bill (Equation 2). This method could be used if there is a desire to assign all customers equal responsibility for covering the residual.

**Equation (2):** Flat residual allocation (equal share per customer)

$$
\text{Flat Share}_i = \frac{\text{Total Residual Revenue Requirement}}{\text{Total Number of Customers}}
$$

The volumetric allocation (Equation 3) divides the total residual revenue by the sum of total customer annual energy consumption, net of on-site solar generation (net AEC). This method yields a dollar per kWh allocation rate applied to each customer based on that customer's ($i$) kWh usage. This method could be used if there is a desire to assign responsibility for covering the residual based on how much each customer uses the grid, using net consumption as a proxy for "usage." The last method excludes low-income customers from being assessed a residual cost allocation and then allocates the residual to the remaining non-low-income customers ($i_{nl}$) on a volumetric basis ($/kWh), as shown in Equation 4. Regulators can explore this potential allocation approach to reduce the energy burden on low-income customers.

**Equation (3):** Volumetric residual allocation

$$
\text{Volumetric Share}_i = \frac{\text{Total Residual Revenue Requirement}}{\sum_j (\text{Net AEC}_j (\text{kWh}))} \times \text{Net AEC}_i (\text{kWh})
$$

**Equation (4):** Volumetric excluding low-income

$$
\text{Volumetric Exclude LI}_i = \frac{\text{Total Residual Revenue Requirement}}{\sum_{j_{nl}} (\text{Net AEC}_{j_{nl}} (\text{kWh}))} \times \text{Net AEC}_{i_{nl}} (\text{kWh})
$$

### 3.4 Calculate Each Customer's Total Cost

Each customer's share of the utility's costs is derived by adding their economic (marginal) costs and their share of the residual (noneconomic), as shown in Equation 5. The Total Allocated Costs is a statement of how much each customer would ideally contribute based on the stated preference of the regulator.

**Equation (5):**

$$
\text{Total Allocated Cost}_i = \text{Economic Cost}_i + \text{Residual Share}_i
$$

### 3.5 Calculate Each Customer's Annual Bill

Having specified how much each customer would ideally contribute in Steps 3.2–3.4, the analyzed tariffs are inputted, and each customer's bill is calculated using historical usage data. Although the tariff design process is not part of this method (it is assumed the method would be applied to a set of proposed tariffs), we design synthetic tariffs for this analysis (described in Section 4). Each customer's annual bill is calculated by applying the proposed synthetic tariff rate schedule.

### 3.6 Calculate Bill Alignments

Having specified how much each customer would ideally contribute and their expected contribution through bills, the next step is to compare those two values. We call the difference between the two values the customer's "bill alignment" (Equation 6). Based on the regulator's stated preference, it expresses how close each customer's annual bill would be compared to how much they should be paying.

**Equation (6):**

$$
\text{Bill Alignment}_i = \text{Annual Bill}_i - (\text{Economic Cost}_i + \text{Residual Share}_i)
$$

All else being equal, having a customer's bill alignment value be zero would be preferable. Although the bill alignment value implicitly incorporates many considerations, it does not incorporate all issues about which decision-makers might care.

### 3.7 Sample Assessment Graphics and Metrics

Following the above steps, the BAT user would arrive at customer-level bills, total system-allocated costs, and bill alignment values. Summary graphics and metrics are generally needed to make these values helpful to decision-makers. Section 5 describes how we perform a case study and produce such graphics and metrics. These are not exhaustive, as many other figures and metrics could be calculated depending on the specific objectives of the relevant regulators. However, one metric that generally is useful is the total per-customer cross-subsidy (i.e., sum the absolute value of each customer's bill alignment and divide by the number of customers). Although this metric does not capture everything a regulator likely cares about (e.g., it does not differentiate between different subclasses of customers), it would likely be a good starting point for rank-ordering several proposed tariffs. Metrics, such as statistical tests for distributional characteristics like normality, skewness, and kurtosis, could also be incorporated to assist in figure interpretation.

Additional metrics could be calculated to help a regulator understand the proposed tariffs' performance in ways not captured by total per-customer cross-subsidy. For example, if regulators care about subpopulations, an analyst may calculate the total transfer between solar and non-solar customers or low-income and non-low-income customers. If they care about economic efficiency, they may calculate the deadweight loss. If they care about bill stability, they may calculate each customer's annual bill under the current tariff and compare it against their annual bill under a new one. The BAT maintains these capabilities.

A critical facet of rate design theory is economic efficiency. Here, economic efficiency is measured by estimating deadweight loss (DWL) using Equation 7, where the total DWL is the squared difference between the hourly proposed rate ($ProposedRate_h$) and the marginal cost ($MC_h$), multiplied by the halved product of hourly quantity demanded ($Q_h$) and elasticity of demand ($\epsilon$), summed across all hours of the year. The absolute value of the $DWL_{total}$ for the year is then divided by net annual energy consumption for a per unit ($/kWh) value printed on each BAT figure. The deadweight loss value should be minimized to maximize economic efficiency.

**Equation (7):**

$$
\text{DWL}_{total} = \sum_{h=1}^{8760} \frac{Q_h \cdot \epsilon}{2} (ProposedRate_h - MC_h)^2
$$

Several simplifying assumptions are incorporated to allow for DWL estimation—namely, a linear demand curve with constant elasticity at each price—consistent with the assumptions approach in Borenstein and Bushnell [29], [30]. The user can choose between a range of potential elasticity values, but the short-term demand elasticity value of -0.2 is the default. A more in-depth discussion of the DWL equation and assumptions is included in Appendix D.

---

## 4. Example of Utility Data for Case Study

This method requires input data, including the tariff rate design, utility revenue requirements for the customer class, hourly customer loads, and marginal system cost data. In practice, these data can come from utilities, system operators, and other sources. As detailed in the following subsection, the proxy input data for load, cost, and revenue values were used to demonstrate the BAT method's capabilities and results.

### 4.1 Input Load, Cost, and Revenue Data

Customer load data included one year of hourly (i.e., 8760 hours) kilowatt (kW) usage by residential customers. This study uses 35,013 anonymized residential load profiles with low-income customers (6,934) indicated for a California electric utility in 2012 [31]. The average month-hour loads in megawatts per hour for the customer data set are shown in Figure 3. To incorporate on-site solar generation, we use a single solar PV generation profile produced by PV Watts for the same region as the customers' [32]. We assume 15% of customers (5,251) have solar PV that offsets 50% of their annual load.

[DIAGRAM DESCRIPTION: Average month-hour loads (MWh) for customer data set (2012)]

A heatmap with months on one axis (January–December) and hour of day (0–23) on the other. Cell color indicates average load in MWh: dark red indicates the highest loads, and dark green indicates the lowest. The figure shows typical residential patterns: higher evening peaks, seasonal variation (e.g., summer cooling), and lower overnight and midday (when some customers are away or solar offsets load in the synthetic case). Used to characterize the 35,013 customer load profiles.

[→ See original PDF page 12 for visual rendering]

Marginal cost inputs (e.g., from a marginal cost-of-service study) are also needed. The definition of marginal costs, especially regarding the inclusion of short-run or long-run costs, can be jurisdiction-specific. Short-run marginal costs assume some factors of production (e.g., capital facilities) are fixed, whereas long-run marginal costs assume all factors can vary (National Association of Regulatory Utility Commissioners 1992, p. 109). A discussion of marginal cost estimation methodologies is outside the scope of this study. We use publicly available marginal cost data from Borenstein et al., which are geographically appropriate by climate zone to the load data [34]. A detailed description of these marginal costs is included in Appendix A.

In practice, a utility's approved revenue requirement would be determined in a regulatory rate case. Here, we calculate a revenue requirement by multiplying the sum of customer energy consumption by the weighted average bundled residential, retail rate for all California utilities in the year contemporaneous with the load data [35]. This rate was $0.15325/kWh for our example.

### 4.2 Input Tariff and Rate Calculation

A BAT analysis would likely analyze a proposed tariff design, potentially against an existing tariff. For our case study, we create synthetic flat and TOU tariffs. The methods supporting these calculations are included in Appendix B. The basic procedure for calculating the flat rate ($/kWh) subtracts customer charge revenues from the revenue requirement net of solar avoided costs and then divides by all customer energy consumption net of on-site generation. The TOU rate was designed by grouping marginal system cost data into like hours and ranking by mean marginal costs to create three rate periods: peak, shoulder, and off-peak. The TOU rate is applied to customer loads to recover economic costs. Where applicable, the equi-proportional rate adjustment method is used as a reconciliation method to cover noneconomic costs (i.e., residual costs). A customer charge is also imposed.

---

## 5. Case Study Results and Analysis

This section describes the results of the case study based on the input data described in Section 4. Note that these inputs result in a residual of approximately 30% of the overall revenue requirement.

### 5.1 Interpreting Bill Alignments by Rate Design and Residual Cost Allocation Method

Bill alignment values for each customer are calculated and presented in histograms to show the distribution of customers underpaying or overpaying through rates compared to their share of the utility's allocated costs. The ideal bill alignment histogram would be centered on zero with no spread or outliers, indicating that each customer's bill is exactly covering their costs and no customers are overpaying or underpaying. In practice, the histogram distribution is likely to have a significant spread, may or may not be symmetrical (e.g., skewness), and could have outliers.

Figure 4 shows bill-alignment value histograms with DER customers highlighted in green. This figure shows two different rate designs: a flat rate on the left and a TOU rate on the right. The figure also shows two different residual cost allocation methods, flat residual cost allocation on the top row and volumetric residual cost allocation on the bottom row.

[DIAGRAM DESCRIPTION: Example of BAT histograms with DER customers highlighted (green)]

A 2×2 panel of histograms. **Columns:** Flat rate (left), TOU rate (right). **Rows:** Flat residual cost allocation (top), Volumetric residual cost allocation (bottom). Each panel shows distribution of bill alignment values (horizontal axis) vs. number of customers (vertical axis). DER/solar customers are highlighted in green; other customers in another color. Average Cross Subsidy (ACS) and Deadweight Loss (DWL) are printed on the figures. Top row: distributions are more spread and skewed; flat rate has slightly lower ACS ($133.09) than TOU ($140.42); DWL is 0.326 vs 0.318 $/kWh. Bottom row: distributions are tighter around zero; flat rate ACS $24.98, TOU ACS $14.99; solar customers (green) shift from right-of-center (flat) to left-of-center (TOU) under volumetric allocation.

[→ See original PDF page 14 for visual rendering]

For the top row of Figure 4, each customer's total annual system cost (i.e., economic cost plus flat residual cost allocation) was compared to their annual bills. This example shows a skewed distribution where most customers do not have the ideal bill alignment value of zero; however, distributions for these two rates look remarkably similar. The average per-customer cross-subsidy metric indicates that the flat rate ($133.09) with flat residual cost allocation performs slightly better than the TOU rate ($140.42) with flat residual cost allocation. On the other hand, the per unit deadweight loss of 0.326 cents per kWh is higher (i.e., less economically efficient) for this flat rate–flat allocation design compared to the 0.318 cents per kWh for this TOU rate–flat allocation design. Here, the stated policy preference is that all customers equally contribute to the recovery of residual costs.

A regulator may think flat residual cost allocation is not equitable and instead prefer customers contribute to residual cost allocation based on their usage. Correspondingly, the bottom row of Figure 4 shows bill alignment value histograms for the same flat and TOU rates using the volumetric residual cost allocation method. Both rates (left and right columns of the bottom row) show a clear centering of the distributions closer to the ideal zero-value bill alignment. Specifically, the average per-customer cross-subsidy value for flat rates is $24.98 and $14.99 for TOU rates. The residual cost allocation method does not impact the deadweight loss calculations.

It is worth reiterating that the rate designs in the bottom and top rows of Figure 4 are the same. Thus, the change in the shape of the bill alignment distributions depicted is due exclusively to the move from the flat to the volumetric residual cost allocation method; said another way, the BAT assessment of the two tariffs depends significantly on the stated preferences of how each customer should contribute to the residual. Interestingly, there is a relatively minor change in the distribution shape between the left side (flat rates) and the right side (TOU rates) in both rows of Figure 4. Given the inputted tariffs, the difference between this flat rate design and this TOU rate design seems less significant to the overall shape of the customer bill alignment distributions than the impact of choices about the residual. This finding may or may not be unique to these specific tariffs and other analysis inputs. As the size of the residual grows in relation to the size of total system marginal costs, there is the potential to distort price signals to consumers [3].

As seen in three of the four situations depicted in Figure 4, solar customers tended to pay less than their share of the utility's total costs, as evidenced by the left-of-center position in the distributions relative to their peers who did not invest in solar.

### 5.2 Assessing Regulatory Goals with Sample Graphics and Metrics

For illustrative purposes, potential regulatory goals are presented to show how the graphics and metrics can be used to assess whether or how a regulatory goal is being met by a proposed rate and the trade-offs that occur when choosing between rate design options. We are not advocating for a particular perspective; these goals are only used as examples.

**Table 1** – Comparing bill alignment metrics for flat and TOU rate design options using the volumetric residual cost allocation method. Numbers in brackets are negatives.

| Metric                                                             | Flat Rate | TOU Rate  |
| ------------------------------------------------------------------ | --------- | --------- |
| **Total Cross Subsidy Metrics**                                    |           |           |
| Customers Overpaying                                               | 17,852    | 17,785    |
| Average Overpayment                                                | 48.99 $   | 29.51 $   |
| Customers Underpaying                                              | 17,161    | 17,228    |
| Average Underpayment                                               | (50.96) $ | (30.46) $ |
| Average Cross Subsidy (per customer)                               | 24.98 $   | 14.99 $   |
| **DER (Solar) Customer Metrics**                                   |           |           |
| Solar Customers Underpaying (%)                                    | 6.3%      | 81.5%     |
| Average Solar Cross-Subsidy (per solar customer)                   | 85.96 $   | (34.00) $ |
| Non-Solar Customers Overpaying (%)                                 | 43.4%     | 56.5%     |
| Average Non-Solar Cross-Subsidy (per non-solar customer)           | (15.17) $ | 6.00 $    |
| **Low-Income Metrics**                                             |           |           |
| Average Low-Income Cross-Subsidy (per low-income customer)         | 3.34 $    | (3.18) $  |
| Average Non-Low-Income Cross-Subsidy (per non-low-income customer) | (0.82) $  | 0.78 $    |
| **Load-Use (Quartiles) Metrics**                                   |           |           |
| Low-Use Cross-Subsidy (per low-use customer)                       | 21.17 $   | (7.01) $  |
| Mid-Low-Use Cross Subsidy (per mid-low-use customer)               | 9.91 $    | (1.23) $  |
| Mid-High-Use Cross Subsidy (per mid-high-use customer)             | (10.20) $ | 6.31 $    |
| High-Use Cross-Subsidy (per high-use customer)                     | (20.88) $ | 1.94 $    |
| **Deadweight Loss**                                                |           |           |
| Deadweight Loss (total)                                            | 925,547 $ | 903,572 $ |
| Deadweight Loss (per kWh)                                          | 0.00326 $ | 0.00318 $ |

Bonbright's equity principles suggest regulators minimize unintentional cross-subsidies between customer types when setting rates [23]. If a commission were interested in this potential regulatory goal, the total cross-subsidies section of Table 1 would be informative. Total overpayments represent the total bill alignment values that are positive only; total underpayments represent the total bill alignment values that are negative only. Because tariffs are designed to be revenue neutral to the revenue requirement, the total overpayments will equal the total underpayments. However, knowing how many customers are overpaying and how much the average overpaying customer is overpaying (total overpaying ÷ total number of overpaying customers) versus how many customers are underpaying and what the average underpaying customer is underpaying (total underpayment ÷ the total number of underpaying customers) is relevant.

From an equity standpoint, it may be perceived as unfair for non-solar customers to subsidize higher-income solar customers. If a commission were interested in a potential regulatory goal regarding this perception, the DER section of Table 1 would be informative, as would the DER histograms (Figure 4) that compare flat and TOU rate designs using volumetric residual cost allocation. For example, from the DER histograms in the bottom row of Figure 4, it is apparent that subsidies to solar customers increase when moving from this flat rate to this TOU rate under a volumetric residual cost allocation. This effect can be seen by the distribution of solar customers (in green) moving from the right of the center (average overpayment) with the flat rate (left) to the left of the center (average underpayment) with the TOU rate (right). This is likely due to the coincidence of solar DER generation to system peak costs, which allows DER customers to enjoy substantial avoided system marginal costs from on-site generation. To the BAT, these customers have been allocated residual costs based on their consumption (kWh), which has been reduced due to on-site generation. Hence, these customers appear to be paying less than their allocated system cost based on the preferred volumetric residual cost allocation. Specifically, as seen in Table 1, the percentage of solar customers underpaying compared to their system costs increases from 6.3% to over 81.5%, and the average solar customer cross-subsidy goes from paying about $86 to receiving about $34. This result will likely change if the residual calculation is based on gross (instead of net) AEC. To highlight an example of a trade-off, a regulator who wants to keep overall cross-subsidies low would be compelled by the TOU rate. However, if the same regulator also wanted to prevent solar customers from paying less than their costs, the flat rate may seem more attractive.

Many commissions seek to structure rates so that the financial burden on low-income customers is lower than for non-low-income customers. Such assistance may take the form of discounted rates or customer assistance programs or occur through other means. If a commission were interested in the potential regulatory goal of reducing the burden on low-income consumers, the low-income section of Table 1 would be revealing, as would the income histograms in Figure 5. For example, Figure 5 shows the low-income BAT histograms using volumetric residual cost allocation, with the flat rate plan performing poorly with respect to the potential regulatory goal of reducing the burden on low-income customers. This effect may be challenging to understand through the figure alone. Table 1 shows that under this flat rate, low-income customers are, on average, paying more than their costs ($3.34 more per low-income customer), and non-low-income customers are paying less than their costs ($0.82 less per non-low-income customer). Now, the regulator can understand that this TOU rate does a better job (i.e., minimized the cross-subsidy) at meeting total cross-subsidy and low-income goals, but the flat rate does a better job meeting the DER/solar goal. In the Table 1 example, this particular TOU rate is also more economically efficient than this flat rate, as shown by the lower deadweight loss value.

[DIAGRAM DESCRIPTION: Comparison of bill alignment histograms for flat (left) and TOU (right) rates using volumetric residual cost allocation and highlighting low-income customers]

Two histograms side by side: flat rate (left), TOU rate (right). Both use volumetric residual cost allocation. Bill alignment value on horizontal axis, count of customers on vertical axis. Low-income customers are highlighted (distinct color). Average cross subsidy (ACS) and deadweight loss (DWL) are printed on the figures. The figure shows how the distribution of low-income customers differs between the two rate designs and how close each design comes to zero bill alignment for that subgroup.

[→ See original PDF page 17 for visual rendering]

As evidenced by practices such as Ramsey pricing, customers insensitive to price changes (i.e., demand inelastic) may be charged higher rates [33]. From this possibility, it may be deemed equitable to have higher energy use customers pay slightly more, thus allowing more demand elastic, lower energy use customers to pay slightly less. If a commission were interested in this potential regulatory goal, the load section of Table 1 would be informative. The data in Table 1 show that with this flat rate, lower-use customers are overpaying, and higher-use customers are underpaying. The opposite pertains to the TOU rates, where low-use customers are underpaying, and higher-use customers are overpaying.

In this example, the TOU rate meets three potential regulatory goals explored, while the flat rate only meets one. How a regulator weighs these and other goals will determine their alignment with alternative design options. The BAT does not recommend the best option; it simply illuminates the trade-offs associated with these design proposals. Also, it should be noted that the results of this example are specific to the rate designs and load patterns inputted into the BAT; it does not mean all flat rate designs and all TOU rate designs will have similar results.

### 5.3 Assessing Whether a Regulatory Goal is Achieved by Rate Design

Here, we highlight how the BAT can assess specific rate design strategies to achieve a regulatory goal. We compare two rate design options and assume one of the regulator's criteria for evaluating rates is the burden on low-income customers. **Scenario A** includes a three-part TOU rate, a customer charge on all customers, and attempts to reduce the burden on low-income customers by excluding these customers from being allocated a residual cost. **Scenario B** also exempts low-income customers from being allocated a residual. In addition, Scenario B includes a three-part TOU rate schedule for low-income customers set at marginal costs and exempts low-income customers from the monthly customer charge. All other customers pay a different three-part TOU rate schedule set at marginal costs adjusted to recover residual costs and are assessed a monthly customer charge.

As seen in Scenario A of Figure 6, almost all low-income customers have a positive bill alignment; in other words, they pay more than the specified cost criteria. This finding is because the tariff employed in Scenario A charges low-income and non-low-income customers similarly, despite the stated preference that low-income customers should not pay any portion of the residual. Specifically, residual cost recovery is still embedded in rates. If low-income customers are not allocated a portion of residual costs, but their bills are unchanged, the BAT will show they are overpaying (i.e., bill alignment values greater than zero) and not meeting the low-income regulatory criteria. The average per-customer cross-subsidy for Scenario A is $107.13, and the deadweight loss is 0.326 cents per kWh.

Scenario B of Figure 6 employs two totally separate tariffs, one for low-income customers and one for non-low-income customers. The low-income tariff has rates set by the average marginal energy cost within each TOU period, and the collection of the monthly charge and residual is shifted entirely to the non-low-income tariff. The non-low-income tariff has higher rates as the equi-proportional adjustment must increase to ensure recovery of the monthly charge and the proportionally larger residual that occurs from excluding low-income customer contributions. This two-tariff approach performs much better at meeting the low-income regulatory criteria than the previous scenario, as the average bill alignment is much closer to zero. There are still deviations from zero, likely due to the TOU periods imperfectly reflecting the substantial variation of marginal energy costs within each period. The average per-customer cross-subsidy is lowest in this scenario at $23.12. Deadweight loss is significantly lower in this scenario because low-income customers (~20% of total customers) are being charged marginal costs, and therefore zero deadweight loss is associated with these customers.

[DIAGRAM DESCRIPTION: Comparison of low-income burden reduction rate design strategies]

Two panels: Scenario A (left) and Scenario B (right). Each shows a histogram of bill alignment values with low-income customers highlighted. Scenario A: low-income customers mostly have positive bill alignment (overpaying); ACS $107.13, DWL 0.326 $/kWh. Scenario B: low-income customers' bill alignments clustered much closer to zero; ACS $23.12, lower DWL. Illustrates that exempting low-income from residual in rates (Scenario B) achieves the stated low-income goal much better than exempting them only from allocation (Scenario A) while keeping the same tariff.

[→ See original PDF page 18 for visual rendering]

---

## 6. Conclusion

This study presents a novel method for using utility smart meter data to better quantify and understand the trade-offs among different rate design options in meeting myriad and often competing regulatory goals. The bill alignment test (BAT) compares a customer's annual bill to their share of the utility's annual allocated system costs (i.e., economic costs plus residual cost allocation). To illustrate BAT's capabilities, we implement the BAT on a rich data set of actual hourly-metered consumption data from more than 35,000 residential customers. The method is intended to use utility data on marginal system costs, revenue requirements, and tariff design, which we synthetically derived to highlight the capability of the method. Though the method can incorporate numerous variations, for the sake of simplicity, we show examples with only three rate design options (a flat rate, a three-part TOU rate, and a three-part TOU rate with separate low-income rate) and three residual cost allocation methods (flat, volumetric, and volumetric excluding low-income customers).

The BAT-outputted figure distributions and metrics identify intra-residential class cross-subsidies, with trade-offs being further delineated among subpopulations of consumers by income, DER on-site generation, and levels of load consumption. The distribution shapes were less sensitive to the change in rate designs modeled, while the size and method of residual cost allocation were more impactful to the distribution shapes. The significance of the residual highlights the importance of stating a preferred residual cost allocation method and the rationale supporting that choice up front in the regulatory process, a key policy recommendation based on our analysis. Among other things, the BAT can be used to determine if the resultant tariff is consistent with the rationale supporting the regulator's stated preference. Understanding the residual is relevant when using the BAT to ensure users do not misinterpret the role of residual cost allocation in the BAT metrics (see Section 7).

The intended use of this method is to test multiple rate designs over a single residual cost allocation method. The BAT graphics and tabled metrics identified the trade-offs in achieving each potential regulatory goal when exploring flat and TOU rate design options with volumetric residual cost allocation. The limited set of potential regulatory goals explored includes reducing overall cross-subsidies, avoiding non-solar consumers subsidizing solar consumers, ensuring low-income customers reduce their energy burden, and avoiding high-load customers underpaying. This display of trade-offs was realistic given that none of the design options considered could achieve all the regulatory goals explored. Instead, each rate design performed better or worse on each goal. The BAT does not determine which rate plan is the best; the test provides information to BAT users on each plan's relative benefits and drawbacks. As regulators can identify weights associated with their regulatory goals, the BAT can facilitate the rank-ordering of rate design proposals. Alternatively, the information garnered from the method can be used to address rate plan shortcomings and maximize benefits.

---

## 7. Discussion and Limitations

From an application standpoint, a shortcoming of this method is the potential for misunderstanding the test results, especially if the user decides to compare a single rate design across multiple residual cost allocation methods. As shown by Table 2, comparing the performance of one rate design (here TOU) using various residual cost allocation methods may give the impression that specific customer subclasses are better or worse off. However, none of these three residual cost allocation methods impact customer bills. For example, looking at the "Average Cross Subsidy" row of Table 2 may lead the user to believe that customers are better off with the TOU rate under volumetric residual cost allocation. However, that would be a misreading of the table; customer bills are the same under each of the three scenarios. This point is explained in Section 3.1 and Appendix C. Rather, among these scenarios, the assumption of how customers should contribute to total utility system costs varies under the assumptions within BAT. The choice of residual cost allocation method is not driven by cost causation principles and can therefore be subjective. This variation on utility cost apportionment impacts the bill alignment values but does not impact actual customer bills (or deadweight loss), thus, giving the appearance but not the effect of changing BAT metrics related to equity and fairness.

**Table 2** – Misinterpretation of metrics from bill alignment output that compare a single rate design across multiple residual cost allocation methods. Numbers in brackets are negatives. (TOU rate only; three allocation methods: Flat, Volumetric, Exclude Low-Income.)

| Metric                                                             | Flat       | Volumetric | Exclude Low-Income |
| ------------------------------------------------------------------ | ---------- | ---------- | ------------------ |
| Customers Overpaying                                               | 14,859     | 17,785     | 6,987              |
| Average Overpayment                                                | 330.87 $   | 29.51 $    | 536.87 $           |
| Customers Underpaying                                              | 20,154     | 17,228     | 28,026             |
| Average Underpayment                                               | (243.94) $ | (30.46) $  | (133.84) $         |
| Average Cross Subsidy (per customer)                               | 140.42 $   | 14.99 $    | 107.13 $           |
| Solar Customers Underpaying (%)                                    | 95.0%      | 82%        | 80%                |
| Average Solar Cross-Subsidy (per solar customer)                   | (303.02) $ | (34.00) $  | (34.09) $          |
| Non-Solar Customers Overpaying (%)                                 | 49.1%      | 57%        | 20%                |
| Average Non-Solar Cross-Subsidy (per non-solar customer)           | 53.46 $    | 6.00 $     | 6.01 $             |
| Average Low-Income Cross-Subsidy (per low-income customer)         | (43.24) $  | (3.18) $   | 540.92 $           |
| Average Non-Low-Income Cross-Subsidy (per non-low-income customer) | 10.68 $    | 0.78 $     | (133.58) $         |
| Low-Use Cross-Subsidy (per low-use customer)                       | (378.85) $ | (7.01) $   | 6.65 $             |
| Mid-Low-Use Cross Subsidy (per mid-low-use customer)               | (168.86) $ | (1.23) $   | 10.93 $            |
| Mid-High-Use Cross Subsidy (per mid-high-use customer)             | 51.14 $    | 6.31 $     | 0.26 $             |
| High-Use Cross-Subsidy (per high-use customer)                     | 496.55 $   | 1.94 $     | (17.86) $          |

Another shortcoming of this method is that it does not factor in how customers will actively respond to price signal changes from rate design changes. Instead, customer loads are held constant under various rate design scenarios, which is particularly relevant to TOU rates. Moreover, this is a simplification given that individual customer price elasticities of demand are unknown. This shortcoming could potentially be addressed by incorporating proxy elasticity values. Correspondingly, the BAT method does not provide insights into affordability considerations specifically associated with the ability or inability of customers to manage variation in retail power prices.

The BAT requires hourly annual customer load data, which are available for most, but not all, U.S. residential electricity customers. Tariff adjustment clauses and riders (e.g., fuel adjustment mechanisms) and non-bypassable policy program charges (e.g., portfolio standards, energy efficiency standards) are used ubiquitously but are not currently explicitly modeled in the BAT method. The tariffs used for the BAT demonstration in this study were synthetically constructed less comprehensively than the methods employed by utilities. The case study results from these synthetic tariffs should not be extended to similar rate design types; said another way, not all flat rate designs or all TOU rate designs will have the same results as those seen in this article's case study.

Although the BAT method has limitations, it also has advantages. It is a parsimonious method that incorporates data readily available to many U.S. electric utilities and does not require computational complexity. It is a flexible tool that can facilitate rapid analysis among multiple proposed rate design solutions. The method uses objective cross-subsidy criteria to provide insights on fairness and equity trade-offs, leaving the subjective task of setting goals to regulators, stakeholders, and other potential BAT users. In addition, the software architecture can be easily expanded to deal with more complex or nuanced rate design components.

---

## Funding

This work was partly authored by the National Renewable Energy Laboratory, operated by Alliance for Sustainable Energy, LLC, for the U.S. Department of Energy (DOE) under Contract No. DE-AC36-08GO28308. The U.S. Department of Energy Office of Energy Efficiency and Renewable Energy Solar Energy Technologies Office provided funding. The views expressed in the article do not necessarily represent the views of the DOE or the U.S. Government. The U.S. Government retains, and by accepting the article for publication, the publisher acknowledges that the U.S. Government retains a nonexclusive, paid-up, irrevocable, worldwide license to publish or reproduce the published form of this work or allow others to do so for the U.S. Government purposes.

## Author Contributions

- **Christina Simeone**: Data curation; Formal analysis, Methodology, Writing – original draft
- **Pieter Gagnon**: Conceptualization, Data curation, Methodology, Writing - review and editing
- **Peter Cappers**: Writing – review and editing.
- **Andy Satchwell**: Project administration, Writing – review, and editing.

## Data Statement

The smart meter data used in this article is unavailable due to legal restrictions.

---

## References

[1] Federal Energy Regulatory Commission, "Demand Response and Advanced Metering 2020 Assessment of State Report Federal Energy Regulatory Commission December 2020," 2020. Accessed: Oct. 17, 2021. [Online]. Available: <https://cms.ferc.gov/sites/default/files/2020-12/2020%20Assessment%20of%20Demand%20Response%20and%20Advanced%20Metering_December%202020.pdf>

[2] T. Brown and A. Faruqui, "Structure of Electricity Distribution Network Tariffs: Recovery of Residual Costs," 2014.

[3] S. Borenstein, M. Fowlie, and J. Sallee, "Designing Electricity Rates for An Equitable Energy Transition Designing Electricity Rates for An Equitable Energy Transition," 2021.

[4] J. A. Beecher, "Policy Note: A Universal Equity–Efficiency Model for Pricing Water," _Water Economics and Policy_, vol. 6, no. 3, Jul. 2020, doi: 10.1142/S2382624X20710010.

[5] H. Hotelling, "The General Welfare in Relation to Problems of Taxation and of Railway and Utility Rates," _Econometrica_, vol. 6, no. 3, p. 242, Jul. 1938, doi: 10.2307/1907054.

[6] R. H. Coase, "The Marginal Cost Controversy," _Economica_, vol. 13, no. 51, p. 169, Aug. 1946, doi: 10.2307/2549764.

[7] M. S. Gerber and K. A. McDermott, "Computer Assisted Regulatory Analysis and its Potential Application to the Colorado Public Utilities Commission," 1979.

[8] S. Nakamura, M. Gerber, D. Miller, and K. Kelly, "Electric Utility Analysis Package," 1977.

[9] K. Christensen, Z. Ma, and B. N. Jørgensen, "Technical, Economic, Social and Regulatory Feasibility Evaluation of Dynamic Distribution Tariff Designs," _Energies_, vol. 14, no. 10, p. 2860, May 2021, doi: 10.3390/EN14102860.

[10] I. Abdelmotteleb, T. Gómez, and J. Reneses, "Evaluation Methodology for Tariff Design under Escalating Penetrations of Distributed Energy Resources," _Energies_, vol. 10, no. 6, p. 778, Jun. 2017, doi: 10.3390/EN10060778.

[11] F. Li, D. Tolley, N. P. Padhy, and J. Wang, "Framework for assessing the economic efficiencies of long-run network pricing models," _IEEE Transactions on Power Systems_, vol. 24, no. 4, pp. 1641–1648, 2009, doi: 10.1109/TPWRS.2009.2030283.

[12] J. Jargstorf, C. De Jonghe, and R. Belmans, "Assessing the reflectivity of residential grid tariffs for a user reaction through photovoltaics and battery storage," _Sustainable Energy, Grids and Networks_, vol. 1, pp. 85–98, Mar. 2015, doi: 10.1016/J.SEGAN.2015.01.003.

[13] M. Nijhuis, M. Gibescu, and J. F. G. Cobben, "Analysis of reflectivity & predictability of electricity network tariff structures for household consumers," 2017, doi: 10.1016/j.enpol.2017.07.049.

[14] R. Passey, N. Haghdadi, A. Bruce, and I. MacGill, "Designing more cost reflective electricity network tariffs with demand charges," _Energy Policy_, vol. 109, pp. 642–649, Oct. 2017, doi: 10.1016/J.ENPOL.2017.07.045.

[15] E. Shumilkina, "Utility Performance: How Can State Commissions Evaluate It Using Indexing, Econometrics, and Data Envelopment Analysis?," 2010.

[16] K. Costello, "Alternative Rate Mechanisms and Their Compatibility with State Utility Commission Objectives," 2014.

[17] S. Burger, I. Schneider, A. Botterud, and I. Pérez-Arriaga, "Fair, Equitable, and Efficient Tariffs in the Presence of Distributed Energy Resources Working Paper Series Fair, equitable, and efficient tariffs in the presence of distributed energy resources," 2018.

[18] S. Neuteleers, M. Mulder, and F. Hindriks, "Assessing fairness of dynamic grid tariffs," _Energy Policy_, vol. 108, pp. 111–120, Sep. 2017, doi: 10.1016/J.ENPOL.2017.05.028.

[19] AARP, "The Need for Essential Consumer Protections," 2010.

[20] P. Simshauser and D. Downer, "On the Inequity of Flat-rate Electricity Tariffs," _The Energy Journal_, vol. 37, no. 3, pp. 199–229, 2016.

[21] T. Nelson, P. Simshauser, and S. Kelley, "Australian Residential Solar Feed-in Tariffs: Industry Stimulus or Regressive form of Taxation?," _Econ Anal Policy_, vol. 41, no. 2, pp. 113–129, Sep. 2011, doi: 10.1016/S0313-5926(11)50015-3.

[22] European Union Agency for the Cooperation of Energy Regulators, "Report on Distribution Tariff Methodologies in Europe," 2021.

[23] J. C. Bonbright, "Principles of Public Utility Rates." Columbia University Press, 1961.

[24] I. Perez-Arriaga et al., "Utility of the future: an MIT Energy Initiative response to an industry in transition," 2016.

[25] Cambridge Economic Policy Associates LTD and TNEI Services LTD, "International review of cost recovery issues," 2017.

[26] C. Batlle, P. Mastropietro, and P. Rodilla, "Redesigning residual cost allocation in electricity tariffs: A proposal to balance efficiency, equity and cost recovery," _Renew Energy_, vol. 155, pp. 257–266, Aug. 2020, doi: 10.1016/J.RENENE.2020.03.152.

[27] I. Abdelmotteleb, T. G. S. Roman, and J. Reneses, "Distribution network cost allocation using a locational and temporal cost reflective methodology," 19th Power Systems Computation Conference, PSCC 2016, Aug. 2016, doi: 10.1109/PSCC.2016.7540878.

[28] N. M. Dameto, J. P. Chaves Avila, and T. G. San Roman, "Revisiting Electricity Network Tariffs in a Context of Decarbonization, Digitalization, and Decentralization," _Energies_, vol. 13, no. 12, p. 3111, Jun. 2020, doi: 10.3390/EN13123111.

[29] S. Borenstein and J. Bushnell, "Do Two Electricity Pricing Wrongs Make a Right? Cost Recovery, Externalities, and Efficiency (Working Paper 24756)," National Bureau of Economic Research, Sep. 2018, doi: 10.3386/w24756.

[30] S. Borenstein and J. B. Bushnell, "Do Two Electricity Pricing Wrongs Make a Right? Cost Recovery, Externalities, and Efficiency," _Am Econ J Econ Policy_, vol. 14, no. 4, pp. 80–110, Nov. 2022, doi: 10.1257/POL.20190758.

[31] J. M. Potter, S. S. George, and L. R. Jimenez, "SmartPricing Options Final Evaluation: The final report on pilot design, implementation, and evaluation of the Sacramento Municipal Utility District's Consumer Behavior Study," 2014.

[32] National Renewable Energy Laboratory, "PVWatts Calculator." <https://pvwatts.nrel.gov/> (accessed Aug. 12, 2021).

[33] National Association of Regulatory Utility Commissioners, "Electric Utility Cost Allocation Manual," 1992. Accessed: Sep. 15, 2021. [Online]. Available: <https://pubs.naruc.org/pub/53A3986F-2354-D714-51BD-23412BCFEDFD>

[34] S. Borenstein, M. Fowlie, and J. Sallee, "Designing Electricity Rates for an Equitable Energy Transition Online Appendix," 2021.

[35] U.S. Energy Information Administration, "Annual Electric Power Industry Report, Form EIA-861 detailed data files," 2012. <https://www.eia.gov/electricity/data/eia861/> (accessed Sep. 16, 2021).

[36] Energy and Environmental Economics, "Avoided Cost Calculator for Distributed Energy Resources (DER) - E3." <https://www.ethree.com/public_proceedings/energy-efficiency-calculator/> (accessed Sep. 16, 2021).

[37] R. L. Fares and C. W. King, "Trends in transmission, distribution, and administration costs for U.S. investor-owned electric utilities," _Energy Policy_, vol. 105, pp. 354–362, Jun. 2017, doi: 10.1016/J.ENPOL.2017.02.036.

[38] A. Pollock and E. Shumilkina, "How to Induce Customers to Consume Energy Efficiently: Rate Design Options and Methods," 2010.

[39] X. Zhu, L. Li, K. Zhou, X. Zhang, and S. Yang, "A meta-analysis on the price elasticity and income elasticity of residential electricity demand," _J Clean Prod_, vol. 201, pp. 169–177, Nov. 2018, doi: 10.1016/J.JCLEPRO.2018.08.027.

[40] J. C. Bonbright, A. Danielsen, D. R. Kamerschen, and J. B. Legler, "Principles of Public Utility Rates Second Edition," 1983.

[41] A. Abal, B. Hedman, B. Butterworth, and K. Kneeland, "Primer on Rate Design for Cost-Reflective Tariffs," 2021. Accessed: Jan. 03, 2022. [Online]. Available: <https://pubs.naruc.org/pub.cfm?id=7BFEF211-155D-0A36-31AA-F629ECB940DC>

---

## Appendix A – Detailed Description of Marginal Cost Data

The BAT requires an annual hourly ("8760") schedule of total marginal costs. What is included as "total marginal costs" will likely change from one jurisdiction to the next based on accepted practice or requirements. Total marginal costs for the BAT demonstration are taken from data published in Borenstein, Fowlie, and Sallee's "Designing Electricity Rates for an Equitable Transition" [3], specifically, data published on GitHub that modifies the California Public Utility Commission's (CPUC) avoided cost calculator (ACC) for the Pacific Gas and Electric (PGE) service territory. The CPUC's ACCs estimate cost impacts from demand growth over the long term. The original ACC's were developed by Energy and Environmental Economics and are available online [36]. The hourly levelized data from the modified ACC's represents a total societal marginal cost composed of hourly marginal costs for energy, losses, ancillary services, greenhouse gas emissions and compliance costs, and generation, transmission, and distribution capital expenditures (CapEx). To enable a retrospective analysis, Borenstein, Fowlie, and Sallee use the following data:

- **Marginal Operating Costs**: These include marginal energy costs from hourly, day-ahead locational marginal price data from the California Independent System Operator (CAISO), distribution system losses, and greenhouse gas compliance costs on a $/kWh basis.
- **Ancillary Services**: Day-ahead average annual ancillary services from CAISO are used as marginal costs on a $/kWh basis.
- **GHG Emissions Externalities**: A social cost of carbon of $50/ton is incorporated.
- **Transmission Capex**: Avoided or deferred costs of transmission projects resulting from peak demand reductions are averaged across ten years and reported as marginal transmission capacity costs by year on a $/kW basis. For PGE, this value was $29.11/kW-year.
- **Distribution Capex**: Deferrable distribution capacity costs related to peak demand reductions are averaged over ten years and are reported as marginal distribution costs by year on a $/kW basis. The value reported for PGE is $54.46/kW-year.
- **Generation Capex**: A generational CapEx of $30/kW-year is used based on resource adequacy cost estimates and noting peak demand has generally declined.

Allocation factors are based on the 500 highest load hours of the year between 2005 and 2019 and are forecasted based on a linear regression model. A detailed discussion of the methods used to generate marginal costs is included in the original report and accompanying appendix [34]. Total hourly levelized value of electricity data for the year and climate zones geographically appropriate to the load data are used.

Borenstein et al. did not include a per-customer marginal cost used to develop the fixed per-customer charge. A charge is derived from Appendix Equation 1 to enable the inclusion of a customer charge using the Borenstein marginal costs. This equation uses administrative CapEx and operating expenditure (OpEx) data from Fares and King derived from FERC Form 1 data [37].

**Equation (A1):** Administrative per-customer marginal cost (per customer charge)

$$
\text{Administrative Per Customer Marginal Cost} = \frac{\text{Admin CapEx (per customer)}}{2} + \frac{\text{Admin OpEx (per customer)}}{2} = \text{Per Customer Charge}
$$

---

## Appendix B – Synthetic Tariff Calculations

A utility's existing and proposed residential rate design can be incorporated into the bill alignment test. In the absence of these data, the following proxy data are presented. The summed avoided marginal cost (a negative number) of all customers' on-site DER generation, called the solar end-use credit, is added to the revenue requirement in the rate calculation process to yield the net revenue requirement. The solar end-use credit represents the customer's avoided cost of purchasing power from the grid, priced at the utility's marginal cost. Hence, the solar avoided cost represents avoided marginal costs and not net metering compensation. Equation B1 shows how the solar end-use credit is calculated for each customer ($i$) for each hour of solar generation ($Solar_{h,i}(kWh)$). The total marginal cost in Equation B1 corresponds to the total marginal cost for each hour ($t$), consistent with the applicable scenario method shown in Appendix A.

**Equation (B1):**

$$
\text{Solar End Use Credit}_i = \sum_{h=1}^{8760} \left[ -\text{Total Marginal Cost}_h \left(\frac{\$}{kWh}\right) \times Solar_{h,i}(kWh) \right]
$$

The BAT model run for this study incorporates two tariff options: a flat rate and a three-part TOU rate. The TOU tariff has three different structures: a fixed fee assessed to all customers, a fixed fee assessed to only non-low-income customers, and a TOU rate that includes a low-income-specific TOU sub-rate. These options are explicitly incorporated to compare regulatory goals and actual outcomes.

The flat rate is calculated by subtracting the product of the fixed charge times the total number of customers from the net revenue requirement and then dividing by the net (of on-site generation) annual energy consumption (kWh) of all customers, as described in Equation B2. The customer charge is subtracted as it will be recovered through a fixed charge on customer bills separate from the volumetric rate. Appendix Equation A1 describes how the customer charge is calculated.

**Equation (B2):**

$$
\text{Flat Rate}\left(\frac{\$}{kWh}\right) = \frac{\text{Net Revenue Requirement (\$) - (Customer Charge (\$) \times N}_{customers})}{\text{Total Customer Net AEC (kWh)}}
$$

The TOU tariff converts the system's total marginal cost into rates by grouping like hours together (e.g., hour 0 from each weekday in month 1). The mean system total marginal cost is calculated across each like-hour (e.g., hour 0–23 in a day) group of weekdays and weekends per month. For example, in January, there would be 8 weekend 0th hours and 23 weekday 0th hours. The like-hour groups are then ranked by mean marginal cost. The marginal cost-based ranking of like-hour groups is separated into three rate periods (peak, shoulder, and off-peak) based on fractional relationship. The fractions include the highest 1% of mean total marginal cost hours are assigned to the peak period, the next highest 49% of mean total marginal cost hours are assigned to the shoulder period, and the lowest 50% of mean total marginal cost hours are assigned to the off-peak period. The mean total marginal cost across all hours for each period (i.e., peak, shoulder, off-peak) is calculated and assigned as the rate for the respective period. This marginal cost-based rate only corresponds to the marginal cost incurred by the utility and excludes the residual cost allocation.

With a few exceptions, this method of TOU rate design is generally consistent with guidance for TOU rate design outlined by the National Regulatory Research Institute [38]. TOU rate design should be informed by both load and cost data that, in principle, should yield the same results. The TOU design best practice is to reflect seasonal changes in costs, which are not currently reflected. Instead, similar month hours across the year are grouped based on cost characteristics. The TOU breakpoint periods should be identified by statistical analysis (e.g., cluster, ANOVA, equal variance, or min-max) rather than by preselected fractions. The TOU rate schedule is not analyzed for adjacency of hours or other aspects of consumer convenience. The rate design does not incorporate the customer's price elasticity of demand or sensitivity analysis to elasticity values. A utility-designed rate would presumably incorporate these and other refinements.

The sum of the revenue generated by applying these group-average marginal cost-based TOU rates to each specific customer's hourly loads becomes the marginal cost revenues. The marginal cost and total customer charge revenues are subtracted from the net revenue requirement to yield the residual revenue requirement. Whereas marginal costs represent a utility's variable cost of serving customers, the residual revenue requirement typically (but not always) represents a utility's fixed costs. If the total marginal cost-based rate and customer charge revenues are less (or greater) than the utility's revenue requirement, the rates can be adjusted upward (or downward) to match established needs through a reconciliation process.

There are various revenue reconciliation methods, each with its own set of benefits and drawbacks, a discussion of which is beyond the scope of this study. The rates herein employ a method analogous to the equi-proportional adjustment of inter-class marginal cost assignments modified to remain intra-class, as shown in Equation B3. Here, each period's marginal cost-based rate is adjusted by the proportional constant $K$. In the TOU rate option, where low-income customers are exempt from paying the fixed charge and only pay the marginal cost, the net revenue requirement is decreased by non-low-income contributions to customer charges and low-income marginal cost contributions, thus increasing the proportional constant and the final rate (Equation B4).

**Equation (B3):**

$$
K = \frac{\text{Net Revenue Requirement} - \text{Customer Charge Revenue}}{(\text{Marginal Cost Revenue})}
$$

$$
Rate_{period}\left(\frac{\$}{kWh}\right) = K \times \frac{\text{Marginal Cost Revenue}_{period}}{\text{Total Customer Net AEC}_{period}}
$$

**Equation (B4):** (when low-income exempt from fixed charge and pay only marginal cost)

$$
K = \frac{\text{Net Revenue Requirement} - \text{Customer Charge Revenue} - \text{LI Revenue}}{\text{Total} - \text{LI} - \text{LI Marginal Cost Revenue}}
$$

$$
Rate_{period}\left(\frac{\$}{kWh}\right) = K \times \frac{(\text{Marginal Cost Revenue}_{period} - \text{LI Marginal Cost Revenue}_{period})}{\text{Total} - \text{LI} - \text{Total Customer Net AEC}_{period}}
$$

---

## Appendix C – Calculations for an Example Customer

Assume there is a utility with 1,000 customers, including 25% low-use customers (i.e., 750 kWh/month), 25% high-use customers (i.e., 950 kWh/month), and 50% average use customers (i.e., 850 kWh/month). The utility's revenue requirement is $1,000,000 for the year, the per kilowatt-hour marginal system cost is 5.9 cents/kWh, and the tariff includes a $5/month customer charge and bundled tariff rate of 9.2157 cents/kWh.

The annual bill for the low-use customers is their usage (kWh) times the bundled tariff rate plus $60 in customer charges, which yields a total annual bill of approximately $889. The economic portion of costs for the customer is unchanged between residual cost allocation scenarios because the customer is charged usage (750 kWh) times the marginal system rate (5.9 cents/kWh) time 12 months, yielding approximately $531 for the low-use customer.

Economic costs for all customers are approximately $601,800, yielding $398,200 in total residual costs (about 40% of the total revenue requirement). In this example, total residual costs are allocated to customers by dividing by total customers (flat, per customer method) or by total all-customer usage (volumetric, per kilowatt-hour method). Here, the total costs allocated to the low-use customer are slightly higher with the flat allocation ($929 = $531 economic + $398 residual) and slightly lower with the volumetric allocation ($882 = $531 economic + $351 residual).

When applying the BAT, the difference between each customer's annual bill and their utility-allocated system cost is calculated, and the result is called their bill alignment. The bill alignment value will be at, below, or above zero. A bill alignment value of zero means a customer's bill matches the system costs allocated to them. A value below zero means the customer is paying less than their allocated costs (i.e., making an underpayment, meaning receiving a cross-subsidy). Under flat residual cost allocation, the low-use customer pays $889 through bills but is allocated $929, yielding a bill alignment value of -$40. A value greater than zero indicates that the customer is paying more than their allocated costs. Under volumetric residual cost allocation, the low-use customer is paying $889 through bills but is allocated $882 in utility costs, yielding a bill alignment of $7 (i.e., making an overpayment, thus paying a cross-subsidy).

Even though customer bills and economic costs are constant, the change in residual cost allocation method meaningfully impacts the total allocated costs to which bills are compared, and hence the bill alignment values, as seen in Figure 2.

---

## Appendix D – Deadweight Loss (DWL) Calculations

The approach to DWL estimation (Equation 7) uses combined transformations of the standard DWL loss (D1), demand elasticity (D2), and slope (D3) equations while incorporating simplifying assumptions related to the slope of the demand curve and consumer price elasticity of demand. The assumptions are required because 1) the BAT inputs do not include the utility's demand curve (though the tool could be modified to incorporate these data, if available), and 2) the utility is unlikely to know individual consumer demand elasticity precisely.

**Equation (D1):** Standard deadweight loss (triangle)

$$
DWL = \frac{1}{2}(P_2 - P_1)(Q_1 - Q_2)
$$

**Equation (D2):** Elasticity

$$
\epsilon = \frac{dQ}{dP} \cdot \frac{P}{Q} \quad \text{or} \quad \frac{1}{\epsilon} \cdot \frac{Q}{P}
$$

**Equation (D3):** Slope

$$
Slope = \frac{(P_2 - P_1)}{(Q_1 - Q_2)}
$$

For Equation D1, $P_1$ is assumed to be the marginal cost, and $P_2$ is the proposed rate. For Equation 7, the slope Equation (D3) is first substituted for the quantity difference in DWL Equation D1. Per Borenstein and Bushnell [29], [30], the simplifying assumption of linear demand with constant elasticity at a utility's average (i.e., flat rate) price is incorporated first by assuming the inverse demand slope for all hours ($h$) for utility ($i$) is $S_{hi}$, which is the utility's constant average slope ($\hat{S}$) divided by the product of quantity and price.

$$
S_{hi} = \frac{\hat{S}}{P_h Q_h}
$$

$\hat{S}$ is incorporated into the previously revised DWL Equation D1, then average slope ($\hat{S}$) is defined on a unit basis (i.e., Q=1) using the elasticity Equation D3, so that:

$$
\hat{S} = -\frac{P}{\epsilon} = -\frac{P}{\epsilon}
$$

The final Equation 7 uses a range of potential short-run elasticity values defaulted at -0.2 based on Zhu et al. [39].

$$
DWL_{total} = \sum_{h=1}^{8760} \frac{Q_h \cdot \epsilon}{2} (ProposedRate_h - MC_h)^2
$$

The simplifying assumptions of linear demand and constant elasticity are built upon a utility's average or flat rate price. These assumptions are problematic for TOU rates, for example, where the average price leading to constant elasticity is less reasonable. Compared to our linear demand, an actual demand curve will likely become more inelastic as the slope becomes steeper (i.e., vertical/undefined) when prices are high and more elastic as the slope flattens (i.e., horizontal/zero) when prices decrease. The average linear slope and constant elasticity value render the calculations imprecise (i.e., underestimation) of DWL. Nonetheless, these estimates can be used to compare rate-design proposals.

In this version of the BAT, consumer load quantity is constrained and does not fluctuate based on consumer elasticity to rate changes, somewhat reducing the impacts of the DWL simplifying assumptions.

# Supply vs delivery cost allocation

How cost allocation works differently for supply (generation) and delivery (transmission + distribution), why the mechanisms differ, and what this means for cross-subsidization and the BAT.

## The core distinction

**Supply costs are incurred in real-time and traceable to load.** Wholesale markets generate prices every hour (or every 5 minutes for LBMP). Meters record consumption. Cost causation is, in principle, a multiplication: a customer's load shape × the price curve = their cost. The only reason we don't do perfect allocation is practical — billing simplicity, legacy meters, customer comprehension. The conceptual problem is solved.

**Delivery costs are committed historically and attributed by proxy.** The utility's rate base includes assets installed over decades. That transformer on the corner serves 30 homes — how do you split its cost among them? By how much each consumes? By their peak demand? By the fact that they exist? Reasonable people disagree. The ECOS is an elaborate exercise in imputing cost causation where the actual causal links are weak or severed by time.

## How supply cost allocation works

Supply costs flow from wholesale markets to retail customers through a chain of averaging steps. How much averaging occurs determines how much cross-subsidy exists.

### Institutional mechanics

In NY's deregulated market:

1. **FERC** oversees wholesale markets. NYISO clears energy (LBMP), capacity (ICAP), and ancillary services in real-time or periodic auctions.
2. **Utilities** procure wholesale power as a portfolio for all their full-service customers.
3. The utility allocates its total wholesale procurement cost to **service classes** (residential, small commercial, large commercial, industrial) — typically using load-weighted shares of hourly consumption.
4. Within each class, costs are recovered through a flat **supply rate** ($/kWh) that is adjusted monthly or quarterly. The PSC reviews the passthrough mechanism but doesn't conduct a contested cost allocation proceeding.

This means the "cost allocation" on the supply side is relatively mechanical — closer to accounting than to the adversarial methodological fights in an ECOS rate case. The utility doesn't have much discretion over how much total supply cost to assign to residential as a class; it's roughly whatever wholesale cost that class's load profile incurred. The discretion is in the rate design within the class (flat vs TOU vs RTP).

### The progression of supply rate designs

Each step in this progression reduces averaging and therefore reduces cross-subsidy. All four methods need to allocate the same total wholesale cost; they differ in how precisely they trace that cost to who caused it.

#### 1. Crude flat rate (kWh-share allocation)

Allocate total wholesale cost proportionally to each class's share of total kWh, then divide by kWh to get a flat rate.

**Example.** Two hours, two classes:

|             | Hour 1 (off-peak, $30/MWh) | Hour 2 (peak, $90/MWh) |   Total |
| ----------- | -------------------------: | ---------------------: | ------: |
| Residential |                     60 MWh |                 40 MWh | 100 MWh |
| Commercial  |                     40 MWh |                 60 MWh | 100 MWh |

Total wholesale cost: (100 × $30) + (100 × $90) = $12,000.

Each class used 50% of kWh → each pays $6,000 → rate = $60/MWh for both.

**Cross-subsidy:** Residential actually consumed more during the cheap hour, so they're overpaying. Commercial consumed more during the expensive hour, so they're underpaying. The kWh-share allocation hides this because it ignores _when_ consumption happened.

#### 2. Load-weighted class rate

Compute each class's actual incurred wholesale cost by summing (hourly consumption × hourly price), then divide by class kWh to get a flat rate per class.

**Same example:**

- Residential: (60 × $30) + (40 × $90) = $5,400. Rate = $54/MWh.
- Commercial: (40 × $30) + (60 × $90) = $6,600. Rate = $66/MWh.

**Cross-subsidy between classes:** Eliminated. Each class pays its actual incurred cost.

**Cross-subsidy within a class:** Still present. Residential customers who consume disproportionately during cheap hours subsidize those who consume during expensive hours, because all residential customers pay the same $54/MWh regardless of their individual load profiles.

#### 3. Time-of-use (TOU)

Set two or three price tiers that approximate the hourly wholesale price curve. Customers pay a higher rate during peak hours and a lower rate during off-peak.

**Same example, with a simple peak/off-peak split:**

- Off-peak rate (hour 1): $30/MWh
- Peak rate (hour 2): $90/MWh

Each customer's bill now reflects _when_ they consumed, at least approximately.

**Cross-subsidy:** Reduced but not eliminated. TOU rates are averages within each period — if peak hours span 2pm–7pm, a customer consuming at 2pm (moderate price) is paying the same rate as one consuming at 5pm (system peak, highest price). The coarser the TOU windows, the more residual cross-subsidy.

#### 4. Real-time pricing (RTP)

Each customer pays their own hourly consumption × that hour's wholesale price. No averaging across hours, no averaging across customers.

**Same example:**

Every customer pays exactly the wholesale price for each MWh they consume in each hour. Cost causation is a direct multiplication.

**Cross-subsidy:** Zero (within the limits of hourly metering resolution). RTP is the theoretical endpoint — perfect cost allocation at the individual customer level.

### Why we don't use RTP for everyone

RTP achieves perfect cost allocation but imposes costs of its own:

- **Price volatility risk** on customers who can't easily shift consumption
- **Billing complexity** that most residential customers don't want
- **Equity concerns** — LMI customers may lack the flexibility or technology to respond to price signals

So flat and TOU rates are deliberate tradeoffs: simplicity and price stability in exchange for some cross-subsidy. The cross-subsidy is a _choice_, not a necessity — we have the information to eliminate it.

## How delivery cost allocation works

Delivery costs (transmission + distribution) are allocated through a fundamentally different mechanism: the Embedded Cost of Service (ECOS) study, conducted within a PSC rate case.

### Institutional mechanics

1. **FERC** approves what transmission owners can recover (transmission revenue requirement) and the NYISO OATT structure.
2. **NYISO** allocates transmission costs to utilities/load zones via load ratio shares (based on each utility's contribution to system peak).
3. The utility's share of transmission costs becomes an input to its **delivery revenue requirement** — just another cost line item alongside distribution capital and O&M.
4. The **PSC rate case** ECOS study then allocates the full delivery revenue requirement (transmission + distribution) across customer classes using proxy allocators.

So from the PSC's perspective, the utility's transmission obligations are just a cost input. The FERC/NYISO step determines _how much_ the utility owes for transmission; the PSC rate case determines _how that cost gets spread across customer classes_.

### The ECOS methodology

The ECOS functionalized the utility's revenue requirement into cost categories, then classifies and allocates each category to customer classes using proxy allocators:

- **Demand-related costs** (transmission and distribution capacity): allocated by each class's contribution to system peak (coincident peak for transmission, non-coincident peak or mixed for distribution)
- **Energy-related costs** (losses, some variable O&M): allocated by kWh consumption
- **Customer-related costs** (meters, service drops, billing): allocated by customer count

These allocators are proxies for cost causation, not measurements of it. The transformer on the corner was built 20 years ago for reasons that may have nothing to do with the current customers on its circuit. The ECOS attributes its cost to today's customers using assumptions about what "causes" distribution capacity costs — and those assumptions are debatable. Rate case proceedings regularly feature intervenors arguing for different allocators because the underlying cost causation is genuinely ambiguous.

### Why delivery cost allocation is inherently imprecise

Supply costs are generated fresh every hour in observable markets. Delivery costs are _committed_ — they're depreciated assets, maintenance crews, property taxes, administrative overhead. The costs just _are_, regardless of what any particular customer does this year.

This creates fundamental attribution problems:

- **Temporal disconnect.** Capital costs were incurred years or decades ago. What was the "cause" of building a feeder in 2005? Current customers' load profiles may bear no resemblance to the load conditions that motivated the investment.
- **Shared infrastructure.** A distribution transformer serves all homes on a street. A transmission line serves millions. There's no principled way to assign "your share" of a shared asset — only conventions (peak demand contribution, customer count, etc.).
- **Fixed operating costs.** Vegetation management, storm restoration, inspections, property taxes, administrative overhead — these don't respond to what any individual customer does. They're driven by asset existence, not by load.
- **Methodological disagreement.** Should distribution capacity be allocated by coincident peak or non-coincident peak? Should energy allocators include capacity costs? Should customer charges recover more than just meter and billing costs? These are real disputes in rate cases because the "right" answer depends on philosophical assumptions about cost causation.

## What this means for the BAT

The BAT measures cross-subsidization by comparing what each customer pays to what their load profile costs the system at the margin. The cross-subsidization problem is worse on the delivery side for structural reasons:

**On supply:** cross-subsidization comes from a deliberate rate design choice (flat rates instead of RTP). The information to eliminate it exists. With interval meters and time-varying rates, supply-side cross-subsidy can be reduced to near zero. The BAT's supply-side signal is essentially asking: what would this customer pay under RTP vs what they actually pay under the flat rate?

**On delivery:** cross-subsidization comes from the inherent imprecision of the cost allocation itself. Even if you designed the best possible rate within the current allocation, the allocation is built on proxy allocators applied to committed costs. The BAT's delivery-side signal is asking: what marginal cost does this customer's load profile impose on T&D infrastructure vs what they pay under volumetric delivery rates? This is where the mismatch is most severe — because volumetric rates recover demand-related costs through an energy-based ($/kWh) rate, fundamentally misaligning the cost driver (peak demand) with the billing metric (consumption).

**Heat pump customers get caught in this imprecision.** Their load profiles are different enough from the average residential customer — higher consumption, winter-peaking — that the proxy allocators systematically mis-attribute delivery costs to them. They consume more kWh than a comparable non-HP customer, so under volumetric delivery rates they pay more, even if their peak demand contribution is not proportionally larger. The BAT reveals this by comparing each customer's marginal cost of service to what they actually pay.

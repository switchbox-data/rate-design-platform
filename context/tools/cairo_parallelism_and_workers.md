# How to think about parallelism for CAIRO runs

Reference for choosing worker counts and run strategy (series vs parallel tracks) when running CAIRO scenarios. Covers the infra instance, Dask worker count, and when to run multiple scenario runs in parallel.

## Where sims run: the EC2 instance

Simulations are run on an EC2 instance provisioned by **`infra/`** (Terraform + `dev-setup.sh`).

- **Default instance type** (in `infra/variables.tf`): **`m7i.2xlarge`**
- **Specs:** 8 vCPUs, 32 GiB RAM, x86_64 (Intel Sapphire Rapids)
- Instance type can be overridden (e.g. `terraform apply -var="instance_type=m7i.4xlarge"` for 16 vCPUs). See `infra/README.md`.

So by default you have **8 vCPUs** for CAIRO.

## Two levels of parallelism

1. **Within a single run:** CAIRO uses Dask to parallelize per-building work (load read, demand aggregation, bill calc, precalc). The platform sets `dask.config.set(scheduler="processes", num_workers=…)` so that work runs in multiple **processes** (real multi-core). Worker count is the main knob.
2. **Across runs:** The RI Justfile has 12 runs in a dependency chain. Delivery (odd) and supply (even) tracks are independent; you could run two runs at once (e.g. run-1 and run-2 in parallel). That’s “parallel tracks.”

Decisions: (a) how many workers per run, and (b) run all in series or split into two parallel tracks.

## Worker count: cap at CPU count

- **Rule:** Use at most **one worker per vCPU**. More workers than cores only adds context switching; it doesn’t speed up CPU-bound work.
- **Scenario YAMLs** set `process_workers: 20`. On an 8 vCPU instance that would oversubscribe. On a 20+ vCPU instance it’s fine.
- **Recommendation:** In the scenario runner, cap workers at the machine’s core count:
  - `num_workers = min(settings.process_workers, os.cpu_count() or 1)`
  - Then `dask.config.set(scheduler="processes", num_workers=num_workers)`.
- **Effect:** On the default m7i.2xlarge (8 vCPUs), you get 8 workers regardless of YAML. On a larger instance (or a 20-core EC2), you get up to 20. Same YAMLs work on laptops and big instances.

## Diminishing returns: why more workers don’t scale linearly

In the ideal case, doubling workers halves runtime (linear scaling). In practice you usually get **diminishing returns**:

1. **Serial fraction (Amdahl’s law):** Part of each run is not parallelized—single-threaded steps, the two undasked stages (`process_residential_hourly_demand`, BAT), I/O, Dask scheduling. So going from 4 to 8 workers doesn’t double speed; you might get only 20–40% faster.
2. **Overhead:** More workers ⇒ more processes, more pickling/scheduling. That cost grows with worker count.
3. **Memory / contention:** More processes can contend for memory bandwidth or cache.

So **time with 4 workers** might be only **1.2–1.4×** the time with 8 workers (not 2×). If T₈ = time per run with 8 workers and T₄ ≈ 1.3·T₈, then:

- **Series (8 workers):** 12·T₈ total.
- **Two tracks (4 workers each):** 6 waves × T₄ = 6 × 1.3·T₈ ≈ **7.8·T₈** total.

So **parallel tracks can beat series** when scaling is sublinear. The “sweet spot” depends on how strong the diminishing returns are on your workload and machine—hence the need to measure (see below).

## Series vs two parallel tracks (8 vCPUs)

**Option A — Series, full cores:** Run run-1 (8 workers) → run-2 (8 workers) → … → run-12. One run at a time, each using all 8 cores.

**Option B — Two tracks, half cores each:** Run (run-1 ‖ run-2) with 4 workers each, then (run-3 ‖ run-4), …, then (run-11 ‖ run-12). At any moment 8 processes total (4 + 4), no oversubscription.

**If scaling were linear** (4 workers ⇒ 2× time of 8 workers):

- Series: 12·T₈. Two tracks: 6 × 2·T₈ = 12·T₈. Same total time; series gives earlier first results.

**With diminishing returns** (4 workers ⇒ e.g. 1.3× time of 8 workers):

- Series: 12·T₈. Two tracks: 6 × 1.3·T₈ ≈ 7.8·T₈. **Parallel tracks win** on total wall time.

**Conclusion for 8 vCPUs:** It depends on your measured scaling. If T₄ is close to 2·T₈, prefer **series with 8 workers**. If T₄ is closer to 1.2–1.5·T₈, **two tracks with 4 workers each** can give better total time. Measure before deciding (see “How to test and find the sweet spot” below).

## When parallel tracks can help

- **8 vCPUs:** Two tracks with 4 workers each can beat series if diminishing returns are strong; measure to confirm.
- **16+ vCPUs** (e.g. m7i.4xlarge): Run two tracks with 8 workers each (2×8 = 16). Total wall time drops (e.g. ~6·T₈ instead of 12·T₈) without relying on diminishing returns.
- Rule of thumb: run two tracks in parallel when **2 × N ≤ vCPUs** and either you have the cores (16+) or measured scaling makes 2×N workers per run still efficient.

## How to test and find the sweet spot

Use these steps on the actual EC2 instance (or the machine you run on) to choose worker count and series vs parallel tracks.

### 1. Time a single run at different worker counts

Pick one run that’s representative (e.g. **run-1**). Run it multiple times with different effective worker counts and record wall time.

- **Override workers in code** (temporarily) or via config so that a single run uses 4 workers, then 8 workers. On an 8 vCPU box, cap at 8.
- Example: run run-1 twice with 8 workers, note time T₈; then run run-1 twice with 4 workers (e.g. set `process_workers: 4` in the scenario YAML for that run, or cap in code to 4), note time T₄.
- Compute ratio **r = T₄ / T₈**. If r ≈ 2, scaling is nearly linear. If r is in the 1.2–1.5 range, you have meaningful diminishing returns.

### 2. Decide series vs two parallel tracks (8 vCPUs)

- If **r ≥ ~1.8:** Series with 8 workers is fine (and gives earlier first results). Total time ≈ 12·T₈.
- If **r is in the 1.2–1.6 range:** Two tracks with 4 workers each is likely faster. Estimated total time ≈ 6·T₄ = 6·r·T₈. Compare to 12·T₈; if 6·r < 12 (i.e. r < 2), parallel wins.

### 3. (Optional) Time the full pipeline

To confirm end-to-end:

- Run **run-all-sequential** with 8 workers and note total wall time.
- Run a **two-track** sequence (e.g. run 1 and 2 in parallel with 4 workers each, then 3 and 4, …, then 11 and 12) and note total wall time.

Use the same branch and data so the comparison is fair. Document the outcome (e.g. “on m7i.2xlarge, two tracks with 4 workers each is ~X% faster than series with 8 workers”) and update this doc or the Justfile comments with the chosen strategy.

### 4. Document the sweet spot

Once you’ve found the best strategy for your instance type, record it:

- In this file (e.g. “On m7i.2xlarge we use two tracks, 4 workers each; measured ratio r ≈ 1.35”).
- Optionally in the scenario YAML or run_scenario.py (e.g. a comment or a cap that matches the chosen N).

Re-run the timing if you change instance type, building count, or CAIRO version.

## Summary

| Topic                            | Recommendation                                                                                                                                                                |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Machine**                      | Default: m7i.2xlarge (8 vCPUs). See `infra/variables.tf`.                                                                                                                     |
| **Workers per run**              | Cap at CPU count: `min(process_workers, os.cpu_count())`.                                                                                                                     |
| **On 8 vCPUs**                   | Start with 8 workers in series. **Measure** T₄ vs T₈; if ratio r = T₄/T₈ is well below 2, try two tracks with 4 workers each for better total time.                           |
| **Series vs 2 tracks (8 vCPUs)** | Depends on scaling. Linear (r ≈ 2) ⇒ series. Diminishing returns (r ≈ 1.2–1.5) ⇒ two tracks with 4 workers each can win. Use “How to test and find the sweet spot” to decide. |
| **16+ vCPUs**                    | Two parallel tracks with N workers each (2N ≤ vCPUs) for clear win without relying on diminishing returns.                                                                    |

Related: **`context/tools/cairo_performance_analysis.md`** (which stages use Dask, scheduler fix), **`context/tools/run_orchestration.md`** (dependency chain and Justfile).

## Measured scaling on m7i.2xlarge (2026-02-23)

Machine: m7i.2xlarge (8 vCPUs)
Run: run-1, utility rie, 1,910 buildings, warm filesystem cache
Patches: Phase 1/2/3 applied

| Workers | Wall time |
|---------|-----------|
| 8 | 172s |
| 4 | 149s |

**Ratio r = T4/T8 = 0.87**

Interpretation:
- r = 0.87 < 1.0: 4 workers is **faster** than 8 workers on this workload. 8 workers is actually slower — likely due to I/O contention, memory bandwidth saturation, or Dask scheduling overhead at high worker counts overwhelming the gains from additional parallelism.
- Because r < 1.8, parallel tracks (two runs × 4 workers) reduce total pipeline wall-time relative to series with 8 workers.
  - Total series (8 workers):    12 × 172s = 2064s
  - Total 2-tracks (4 workers):   6 × 149s = 894s  (~57% faster)
- Note: r < 1 also means even a single-track run with 4 workers is faster per-run than 8 workers. The parallel-tracks gain is additive on top of that.

Decision: **parallel-tracks** (two runs × 4 workers each) based on r = 0.87. 4 workers is the sweet spot on m7i.2xlarge for this workload; 8 workers oversubscribes the I/O or memory bandwidth and regresses.

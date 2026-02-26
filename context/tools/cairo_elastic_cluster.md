# Elastic Dask cluster for CAIRO: why, options, and what to change

Reference for running many CAIRO scenario runs (e.g. 16 runs × 10 utilities = 160 runs) without babysitting, using an elastic Dask cluster that scales compute only when needed. Covers why a cluster helps, the lowest-overhead options, and the concrete code changes required in **rate-design-platform** and **CAIRO** so that the pipeline works with remote workers.

## Why we might need a cluster

- **Scale:** Eventually we need on the order of **16 runs per utility × ~10 utilities** (e.g. RI + NY utilities). That’s **160 runs** in a dependency chain per utility, and 10 such chains. Running them one-by-one on a single instance means either a very long wall time or someone constantly kicking off the next run.
- **Cost:** A single huge instance (e.g. 64 vCPUs) running 24/7 is expensive. We’d prefer to **scale up compute only when jobs are running** and scale to zero (or a small driver) when idle.
- **Goal:** A small long-lived “driver” (or driver + scheduler) that submits work; **workers** that spin up when there are runs to do and shut down when the queue is empty. So we need a **Dask Distributed** setup with elastic workers rather than the current single-machine process pool.

## Options for getting a cluster (lowest overhead first)

1. **Dask Distributed + dask-cloudprovider (AWS)**
   - Use [dask-cloudprovider](https://github.com/dask/dask-cloudprovider) (e.g. `EC2Cluster` or `FargateCluster`) so that Dask starts and stops EC2 (or Fargate) workers automatically based on pending tasks. The driver runs on your existing small instance (or a laptop); the scheduler can run there too; workers are created on demand and terminated when idle.
   - **Overhead:** Low. You write a small launcher script that creates the cluster, runs your scenario loop (submit 160 runs as Dask jobs or as a loop that submits each run to the same cluster), then closes the cluster. No custom infra beyond IAM and possibly a VPC.

2. **Dask Distributed + AWS Batch (or ECS)**
   - Run the Dask scheduler on a small EC2 (or Fargate task); use AWS Batch (or ECS) to run worker containers that connect to the scheduler. Autoscale Batch job queue or ECS service based on queue depth.
   - **Overhead:** Medium. You manage Batch job definitions, queue, and possibly a VPC; worker images must have the same env as the driver (CAIRO, pandas, etc.).

3. **Single large instance + cron/Just**
   - Keep one big instance (e.g. 32 vCPUs), run all 160 runs in series or in a few parallel tracks via a Justfile or script. No distributed Dask; just “run the next N runs until done.”
   - **Overhead:** Lowest from a “cluster” perspective, but you pay for the big instance whenever it’s on, and you don’t get true scale-to-zero. Good if you’re okay with “start instance, run script, shut down when done” and don’t need elasticity within a day.

**Recommendation:** Start with **option 1 (dask-cloudprovider)** for elasticity with minimal custom infra. If you hit limits (e.g. Fargate cold starts, or need GPUs), consider Batch/ECS (option 2).

## How the current code behaves (single host)

- **Driver** (e.g. `run_scenario.py` on the EC2): builds paths to ResStock loads (via `build_bldg_id_to_load_filepath`), passes them into CAIRO. Sets `dask.config.set(scheduler="processes", num_workers=...)`. Calls CAIRO’s `simulate()`.
- **CAIRO** uses `dask.delayed` for: load reading (`_load_worker`), demand-by-period aggregation (`aggregate_load_worker`), bill calc, precalc. Each delayed task receives **paths** (and small config); the task runs on a **worker process** and does e.g. `pd.read_parquet(path)` **inside** the worker. So I/O happens on the worker. Results (DataFrames) are sent back to the driver when `dask.compute()` returns.
- **Undaskified steps** (`process_residential_hourly_demand`, `_return_cross_subsidization_metrics`): run **on the driver** in the main process. They use the DataFrames that were already gathered on the driver from the previous Dask steps. So for those two, data lives on the host and the op runs on the host; no workers involved.
- **Paths today:** The platform builds paths that are **local or FUSE-mounted** (e.g. `/data.sb/nrel/resstock/...`). CAIRO assumes **local** paths: it uses `pathlib.PurePath`, calls `.exists()` on paths, and never passes `storage_options` to `pd.read_parquet` or `pq.read_schema`. So **CAIRO does not accept S3 URIs** out of the box; it expects a local filesystem (or a mount that looks local).

## What has to change for a distributed cluster

With a **Dask Distributed** cluster, the driver and scheduler run on one node; workers run on **other nodes**. Those workers must be able to **read the same data** (ResStock parquets, metadata, etc.). Two high-level approaches:

- **A. Shared filesystem / mount on every node**\
  Mount S3 (or an EFS that syncs from S3) on the driver and on every worker so that the same path (e.g. `/data.sb/nrel/resstock/...`) works everywhere. Then **no CAIRO or platform code changes** are required for path handling; you only need to switch from the process scheduler to a Distributed client and point workers at the scheduler.
  - **Overhead:** You must deploy workers (e.g. EC2 or Batch) that have the same mount. With Fargate or generic Batch workers, that can be fiddly (custom AMI or entrypoint that mounts S3).

- **B. S3 URIs + storage_options (no shared mount)**\
  Pass **S3 URIs** (e.g. `s3://data.sb/nrel/resstock/...`) and **storage_options** (e.g. `{"region_name": "us-west-2"}` or from `get_aws_storage_options()`) through the stack so that workers can call `pd.read_parquet(s3_uri, storage_options=...)` and read directly from S3.
  - **Overhead:** Requires code changes in both codebases (see below). No need for a shared mount; any worker with AWS credentials and network can read.

Option **B** is more portable (works with Fargate, Batch, any node with S3 access) and is the one that forces the concrete changes below. Option A is “no code change” but constrains how you run workers.

---

### Changes in CAIRO

CAIRO today (in `rates_tool/loads.py` and any other module that reads parquet):

- Uses `pathlib.PurePath` for load and metadata paths.
- Calls `upgrade_fp.exists()` before reading (e.g. in `__load_buildingprofile__`). For S3, `.exists()` on a `Path("s3://...")` does not do an S3 check; you’d need to either skip the check for S3 or use an S3-capable check (e.g. via `fsspec` or `s3fs`).
- Calls `pd.read_parquet(path, engine="pyarrow", columns=...)` and `pq.read_schema(path)` **without** `storage_options`. PyArrow/pandas can read S3 if given a string URI and the right filesystem or `storage_options`; CAIRO does not pass them.

**Required changes in CAIRO:**

1. **Plumbing for storage_options**
   - Every entrypoint that eventually reads parquet (e.g. `_return_load`, `return_buildingstock`, and any caller of `__load_buildingprofile__` or metadata reads) must accept an optional `storage_options: dict | None` (or equivalent) and pass it through to every `pd.read_parquet`, `pq.read_schema`, and any other reader that can receive an S3 path.
   - If `storage_options` is `None`, keep current behavior (local paths). If provided, use it for all parquet reads that use a path that looks like S3 (e.g. `str(path).startswith("s3://")`).

2. **Path type and existence check**
   - Allow paths to be **strings** (e.g. `"s3://bucket/key.parquet"`) as well as `PurePath`. Where CAIRO currently does `upgrade_fp.exists()`, either:
     - Skip the existence check when the path is an S3 URI (or when `storage_options` is set), or
     - Use an S3-capable existence check (e.g. `s3fs` or `fsspec`) when the path is S3.
   - This avoids assuming a local filesystem on the worker.

3. **Specific call sites (non-exhaustive)**
   - **`__load_buildingprofile__`** (`loads.py`): today takes `load_input` as `PurePath` or DataFrame. Add optional `storage_options`. When `load_input` is path-like and S3 (or storage_options is not None), pass `storage_options` into `pd.read_parquet(..., storage_options=storage_options)` and into `pq.read_schema(...)` if used. Replace or conditionalize `upgrade_fp.exists()`.
   - **`_load_worker`**: receives `load_input` and calls `__load_buildingprofile__`; must pass through `storage_options` (and possibly accept it as an argument if it comes from the driver).
   - **`return_buildingstock`** (and any code that reads building stock parquet): same pattern—accept optional `storage_options`, pass into `pd.read_parquet` and `pq.read_schema`.
   - **Any other `pd.read_parquet` or `pq.read_*` in `rates_tool`** that can receive a path to ResStock or other S3-backed data: add the same optional parameter and pass `storage_options` when the path is S3.

4. **Docstrings and types**
   - Document that paths may be local or S3 URIs (string or Path), and that when using S3, callers must pass `storage_options` (e.g. for AWS). Update type hints to allow `str | PurePath` where appropriate.

CAIRO does **not** need to know about “the cluster”; it only needs to be able to read from S3 when given S3 paths and storage_options. The driver will run on one machine and pass S3 URIs + storage_options; workers will run the same code and do the read from S3 in the worker process.

---

### Changes in rate-design-platform

1. **Build S3 URIs for load and metadata when using a cluster**
   - Today `build_bldg_id_to_load_filepath(path_resstock_loads, ...)` expects a **local** path (or mount) and uses `.exists()` and `.glob("*.parquet")`. For a distributed cluster without a shared mount, the platform must be able to build **S3 URIs** (e.g. `s3://data.sb/nrel/resstock/.../state=RI/upgrade=00/<bldg_id>_<upgrade>.parquet`) instead of local paths.
   - Options:
     - Add a variant of `build_bldg_id_to_load_filepath` (or a parameter) that takes an S3 base URI and building IDs and returns a dict `bldg_id -> s3_uri` without touching the filesystem, or
     - Keep using a mount on the **driver** only to discover files (e.g. list keys via S3 API or a mount), then convert to S3 URIs before passing to CAIRO so that workers get S3 URIs.
   - The same idea applies to **metadata** and any other ResStock or S3-backed paths that are passed into CAIRO. The driver must pass URIs (and storage_options) that workers can use.

2. **Pass storage_options into CAIRO**
   - Where the platform calls CAIRO (e.g. `_return_load`, `return_buildingstock`, and the top-level `simulate()` or helpers that take paths), it must pass `storage_options` when the paths are S3. That implies:
     - CAIRO’s public API (e.g. `MeetRevenueSufficiencySystemWide`, `simulate()`, or the load/buildingstock functions) must accept an optional `storage_options` (or a “run config” that includes it) and pass it through to all parquet-reading code.
     - The platform already has `get_aws_storage_options()` (e.g. in `data.eia.hourly_loads.eia_region_config` or similar). When running in “cluster mode” with S3 URIs, the platform should pass that (or equivalent) into CAIRO.

3. **Use Dask Distributed instead of the process scheduler**
   - Instead of `dask.config.set(scheduler="processes", num_workers=...)`, the platform (or a cluster launcher script) would create a `distributed.Client` connected to a Dask scheduler (e.g. one started by dask-cloudprovider), and either:
     - Set the client as the default (so CAIRO’s existing `dask.compute()` calls use it), or
     - Refactor so that the “run scenario” logic explicitly uses that client for all `compute()` calls.
   - The driver then runs the same high-level flow (load metadata, build paths, call CAIRO), but CAIRO’s Dask tasks are executed on remote workers that read from S3 using the passed URIs and storage_options.

4. **Orchestration of many runs**
   - For 160 runs (or 16×10), the driver can loop over utilities and runs, and for each run call the same “run one scenario” function (which uses the shared Distributed client and S3 URIs). No change to CAIRO’s internal Dask usage is required once the client is set and paths are S3 + storage_options; the only change is “where” the tasks run (remote workers instead of local processes).

5. **Undaskified steps**
   - `process_residential_hourly_demand` and `_return_cross_subsidization_metrics` run on the **driver** with DataFrames that were already collected from Dask. So they require no change for distribution; they already run on the host that holds the data. Only the Dask-backed stages (and any code that reads from paths on the worker) must work with S3 + storage_options on the worker.

---

## Summary

| Topic                | Conclusion                                                                                                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Why a cluster**    | Many runs (e.g. 16×10) without babysitting; scale compute only when needed to control cost.                                                                                                                             |
| **Preferred option** | Dask Distributed + dask-cloudprovider (AWS) for elastic workers with minimal infra.                                                                                                                                     |
| **CAIRO changes**    | Add optional `storage_options` to all parquet-reading paths; support S3 URIs (string or Path); fix or skip `.exists()` for S3. Pass `storage_options` through from public API to every `read_parquet` / `read_schema`.  |
| **Platform changes** | Build S3 URIs for loads (and metadata) when in cluster mode; pass `storage_options` into CAIRO; switch to a Distributed client instead of the process scheduler; orchestrate many runs in a loop using the same client. |
| **Alternative**      | Shared mount (e.g. S3 FUSE or EFS) on driver and every worker so paths stay “local”; then no S3/storage_options changes, but worker deployment must provide the mount.                                                  |

Related: **`context/tools/cairo_parallelism_and_workers.md`** (single-machine parallelism and worker count), **`context/tools/cairo_performance_analysis.md`** (which stages use Dask), **`context/tools/cairo_parallelize_two_undasked_stages.md`** (parallelizing the two undaskified stages in CAIRO).

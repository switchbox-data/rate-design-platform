# Reading Parquet from local disk vs S3

A guide for data scientists on how Polars reads Parquet files under
different storage backends and file layouts, and why performance can
differ by orders of magnitude. The examples use our ResStock load curve
data but the lessons apply to any Hive-partitioned Parquet dataset with
many small files.

## Two things you need to know

Everything in this guide follows from two facts:

1. **Every S3 GET request costs ~50–100 ms of overhead**, regardless of
   how much data you download. A 1 KB footer read costs the same
   round-trip as a 2 MB file read. On local disk, opening a file takes
   microseconds.

2. **Polars discovers files by listing directories**, then reads each
   file individually. It cannot skip a file it hasn't at least probed
   (for stats). More files = more requests = more overhead, and that
   overhead is negligible on local disk but devastating on S3.

Keep these in mind. We'll return to both repeatedly.

## The data layout

Our ResStock load curves are Hive-partitioned with **one Parquet file
per building**:

```
load_curve_hourly/
  state=NY/
    upgrade=00/
      10000-0.parquet    (~1.4 MB)
      100058-0.parquet   (~1.8 MB)
      100084-0.parquet   (~1.7 MB)
      ...
```

For NY upgrade 00, that's **~33,790 files totaling ~46 GB**. Each file
is 0.4–1.8 MB. The monthly load curves have the same structure but are
~100x smaller per file (12 rows vs 8,760).

## Use case 1: Reading the whole state

You want all buildings in a state. Two approaches:

### Approach A: `scan_parquet` the directory

```python
lf = pl.scan_parquet("load_curve_hourly/state=NY/upgrade=00/")
```

Polars does three things:

1. **Lists all files** in the directory.
2. **Reads the Parquet footer** from each file (to learn the schema,
   row groups, and statistics).
3. **Reads the data** from each file.

On **local disk**, step 1 is a directory listing (milliseconds), step 2
is ~33,790 small reads (microseconds each, ~1 second total), and step 3
streams 46 GB off your NVMe at 3–7 GB/s (~7–15 seconds). **Total:
~10–20 seconds.** Perfectly fine.

On **S3**, step 1 requires paginated `ListObjects` API calls for 33,790
keys (~30–60 seconds). Step 2 is one GET per file just for the footer —
33,790 GETs at ~50 ms each = **~28 minutes** of pure request overhead.
Step 3 adds the actual data transfer. Total: **~30+ minutes**, most of
it spent waiting on round-trips, not actually downloading data.

The throughput of S3 is fine — you can stream data at 100+ MB/s per
connection, and parallelize many connections. **The killer is the
per-request overhead multiplied by thousands of files.** If those same
46 GB were in 46 files of ~1 GB each, the 33,790 GETs would collapse to
46 GETs (~2 seconds of overhead), and the total time would be dominated
by actual data transfer (~minutes).

### Approach B: Consolidate into fewer files

Rewrite the 33,790 small files into, say, 46 files of ~1 GB (or a
handful of files per utility). Same data, different packaging. Now
`scan_parquet` on S3 issues ~46 GETs instead of ~33,790 — the overhead
drops from ~28 minutes to ~2 seconds.

On local disk this doesn't matter much (going from 1 second of overhead
to essentially zero), but on S3 it's the difference between a
30-minute wait and a 2-minute download.

### Summary for use case 1

|                           | Local disk      | S3                                    |
| ------------------------- | --------------- | ------------------------------------- |
| Many small files (33,790) | ~10–20 s (fine) | ~30+ min (per-GET overhead dominates) |
| Few large files (~46)     | ~10–20 s (same) | ~2–5 min (data transfer dominates)    |

The key insight: **on local disk, file count barely matters. On S3, it's
everything.**

## Use case 2: Reading only one utility's buildings

Now you need a subset — say 2,000 buildings belonging to a specific
electric utility. You know which buildings from `metadata_utility` (a
few MB, fast to load). But there's no `utility` Hive partition in the
path — `bldg_id` is baked into the filename, not the directory
structure.

### Approach A: `scan_parquet` + filter

```python
lf = pl.scan_parquet("load_curve_hourly/state=NY/upgrade=00/").filter(
    pl.col("bldg_id").is_in(utility_bldg_ids)
)
```

When you filter on a Hive partition column (like `state`), Polars can
eliminate files without opening them — it reads the partition value from
the directory path. But `bldg_id` is **not** a Hive partition column.
Polars doesn't know the building ID is in the filename; from its
perspective, the data could be distributed across files in any way. To
evaluate the filter, it has to **open every file** and check the Parquet
row group statistics (min/max of `bldg_id`) to decide whether to read
the actual data.

On **local disk**, probing 33,790 footers takes ~1 second. Polars finds
the 2,000 files that match, reads them, and skips the rest. Total: a
few seconds — barely slower than if you had only the 2,000 files.

On **S3**, probing 33,790 footers is still ~28 minutes. Even though
Polars ultimately skips the data download for 31,790 files, it still
has to make one GET request per file just to read the statistics. The
overhead is almost identical to reading the entire state.

### Approach B: Construct file paths directly

Since the file naming convention is deterministic
(`{bldg_id}-{upgrade_id}.parquet`), you can skip the listing and
probing entirely. Load `metadata_utility` to get the building IDs, then
construct exact paths:

```python
base = "s3://data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_monthly/state=NY/upgrade=00"
paths = [f"{base}/{bldg_id}-0.parquet" for bldg_id in utility_bldg_ids]
lf = pl.scan_parquet(paths, storage_options=opts)
```

When Polars receives a list of fully resolved paths (not globs, not
directories), it skips file discovery entirely. No `ListObjects` call,
no footer probing of files you don't care about. It reads exactly the
files you listed.

For 2,000 utility buildings, that's 2,000 GETs on S3 — ~2 minutes of
overhead instead of ~28 minutes. You've cut out the cost of probing
31,790 irrelevant files.

On local disk this saves ~1 second (skipping footer probes), which is
rarely worth the extra code.

### Summary for use case 2

| Approach        | Local disk | S3                          |
| --------------- | ---------- | --------------------------- |
| Scan + filter   | ~2–3 s     | ~30 min (probes every file) |
| Pre-built paths | ~2 s       | ~2 min (2k GETs only)       |

Notice that **local disk + filter is still faster than S3 + pre-built
paths**. Even the optimal S3 strategy (skip irrelevant files entirely)
is slower than the naive local strategy (probe everything, skip the
non-matching ones) because you still pay ~50 ms per GET for the files
you do need.

## Best practices

### Reading the whole state

- **First choice: download the files locally** (e.g., `aws s3 sync`)
  and `scan_parquet` the local directory. You pay the download time
  once, then every subsequent read is fast. This is what the existing
  Justfile workflows do for hourly loads.
- **If you need remote-only access**: consolidate the data into fewer,
  larger files on S3 (e.g., one file per utility, or a handful of
  ~100 MB–1 GB files per state/upgrade). Then `scan_parquet` the
  consolidated copy.

### Reading a single utility's buildings

- **First choice: again, download locally.** Either sync the whole state
  and let Polars filter (takes ~1 second of overhead), or sync only the
  utility's files. Either way, local reads are fast regardless of
  strategy.
- **If you must read from S3**: construct explicit file paths from
  `metadata_utility` building IDs. This avoids probing every file in
  the state and limits your GET overhead to only the buildings you need.
  The code is straightforward:

  ```python
  bldg_ids = (
      pl.scan_parquet(path_metadata_utility, storage_options=opts)
      .filter(pl.col("electric_utility") == utility)
      .select("bldg_id")
      .collect()
      .to_series()
  )
  base = f"s3://data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_monthly/state={state}/upgrade={upgrade}"
  paths = [f"{base}/{bid}-{upgrade}.parquet" for bid in bldg_ids]
  lf = pl.scan_parquet(paths, storage_options=opts)
  ```

- **Avoid** `scan_parquet(s3_dir).filter(bldg_id.is_in(...))` — it
  probes every file in the state, which is the same overhead as reading
  everything.

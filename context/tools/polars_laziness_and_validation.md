# Polars laziness, collect, and runtime data validation

A guide for data scientists on how LazyFrames, deferred execution, and
streaming interact with runtime data-quality checks. The concepts are general
but the examples are from our codebase.

## What is a LazyFrame?

A LazyFrame is a **recipe**, not a result. When you write:

```python
lf = pl.scan_parquet("s3://bucket/big_table.parquet")
lf2 = lf.filter(pl.col("state") == "RI").select("bldg_id", "total")
```

No data has been read yet. Polars has recorded two steps in a query plan
("scan this file, then filter, then select these columns") but hasn't
executed anything. The parquet file hasn't been opened. Nothing is in memory.

A DataFrame, by contrast, is **data in memory right now**. When you call
`pl.read_parquet(...)`, the file is read immediately and the full result
lands in RAM.

## What does `.collect()` do?

`.collect()` tells Polars: "execute this recipe now and give me the result as
a DataFrame." At that moment, Polars:

1. Optimizes the query plan (pushes filters into the scan, drops unused
   columns, reorders joins).
2. Reads the source data from disk / S3.
3. Executes all the steps.
4. Returns a DataFrame that lives in memory.

The key mental model: **every `.collect()` starts from scratch.** Polars does
not cache intermediate results. If you call `.collect()` on the same
LazyFrame (or a downstream one built on the same inputs) twice, it reads the
source files twice.

```python
lf = pl.scan_csv("sales.csv").filter(pl.col("year") == 2025)

df1 = lf.collect()         # reads sales.csv, filters, returns DataFrame
df2 = lf.collect()         # reads sales.csv AGAIN, filters again
```

This is fine if you do it once. It becomes a problem when you scatter
`.collect()` calls through a pipeline — each one re-reads everything
upstream.

## What is streaming?

For large datasets that don't fit in memory, Polars can execute a query plan
in **streaming mode**: it processes data in batches (chunks), writes output
progressively (e.g. via `sink_parquet`), and discards each chunk after
processing. The full dataset never needs to fit in RAM.

```python
(
    pl.scan_parquet("s3://bucket/100GB_table.parquet")
    .filter(pl.col("state") == "RI")
    .group_by("bldg_id")
    .agg(pl.col("kwh").sum())
    .sink_parquet("output.parquet")
)
```

This can process a 100 GB file on a machine with 8 GB of RAM because it
never holds more than one chunk in memory at a time.

`.collect()` breaks streaming: it forces the entire result into memory at
once. If the result is too large, you get an out-of-memory error.

## Why default to lazy?

Two reasons:

1. **Query optimization.** Polars can push filters and projections into the
   scan (e.g. only read columns you use, skip row groups that don't match).
   With eager code (`read_parquet` followed by `.filter`), the full file is
   read before any filtering.

2. **Streaming.** If your data grows large enough, lazy + `sink_*` lets you
   process without blowing up memory. Eager code can't do this.

**Default to `scan_parquet` / `scan_csv` and build the pipeline lazily.
Collect once at the end, or sink to disk.**

## When you must collect

Some operations require materialized data:

- **Runtime asserts / data-quality checks.** Inspecting data (row counts,
  value ranges, key uniqueness) requires actual values. You cannot assert
  inside a lazy plan — there's nothing to inspect yet.
- **Control flow that depends on data.** If the next step depends on a value
  you just computed (e.g. "if this column has nulls, take path A"),
  you need to collect to read that value.
- **Libraries that require a DataFrame.** Plotting libraries need real data.

## The hidden cost of multiple collects

This is the most important section. Each `.collect()` re-executes the entire
upstream query plan from scratch. This means:

- If you collect a LazyFrame, throw away the result (e.g. an assert that
  doesn't return anything), and then collect a downstream LazyFrame built on
  the same inputs, **the source files are read again**.
- If you have N asserts that each collect independently, and then the main
  pipeline collects at the end, you read the source data **N+1 times**.

This is not obvious! The code looks clean — each assert is a pure function
that takes a LazyFrame and checks something. But under the hood, every one
of them is re-reading from S3.

## Asserts break streaming

Streaming means "process chunks, write output, discard chunks." There is no
way to attach a side-channel observer or callback to the streaming engine.

To assert, you must either:

1. **Collect the full result** — breaks streaming; the dataset at that point
   in the pipeline must fit in memory.
2. **Collect an aggregate** (e.g. `.select(pl.col("x").max()).collect()`) —
   only one row materializes, but it's still a separate execution of the
   full upstream query plan (another complete read of the source data).

Neither option lets you validate and stream in one pass.

## Strategies

### Small data: collect once, then validate and process eagerly

This is the right default for our scripts. Build the lazy pipeline, collect
once at a strategic point, then run all asserts on the materialized DataFrame
and pass it downstream as a DataFrame.

```python
pipeline = (
    pl.scan_parquet(...)
    .join(...)
    .filter(...)
    .with_columns(...)
)

df = pipeline.collect()  # single collect — one read of each source file

assert df.height > 0, "Empty result"
assert (df["weight_a"] - df["weight_b"]).abs().max() < 1e-9, "Weight mismatch"

result = do_stuff(df)  # df is a DataFrame; no more reads
```

The lazy pipeline still buys you filter/projection pushdown on the source
reads. You just don't try to defer past the validation boundary.

### Large data where streaming matters: embed checks in the output

If the dataset is too large to collect, embed validation signals as columns
in the lazy pipeline, stream to disk, then check after:

```python
pipeline = (
    pl.scan_parquet(...)
    .join(...)
    .with_columns(
        (pl.col("weight_a") - pl.col("weight_b")).abs().alias("_weight_diff"),
    )
)
pipeline.sink_parquet("output.parquet")  # streams — never holds full dataset

checks = pl.scan_parquet("output.parquet").select(
    pl.col("_weight_diff").max().alias("max_weight_diff"),
).collect()  # reads only the one column, returns one row

if checks["max_weight_diff"].item() > 1e-9:
    Path("output.parquet").unlink()
    raise ValueError("Weight mismatch detected")
```

The downside: bad data is written before you catch it, so you must clean up
on failure. And the diagnostic comes post-hoc.

## Examples

### Example 1: Large-data pipeline (streaming, no mid-pipeline asserts)

You have a 50 GB dataset of hourly building loads and a 2 GB marginal-cost
table. You need to join them, compute a cost allocation per building per
hour, group by building, and write the result. The joined intermediate would
be ~100 GB — it won't fit in memory.

```python
loads = pl.scan_parquet("s3://bucket/loads/")         # 50 GB, hive-partitioned
costs = pl.scan_parquet("s3://bucket/marginal_costs/") # 2 GB

allocated = (
    loads
    .join(costs, on=["hour", "region"], how="inner")
    .with_columns(
        (pl.col("kwh") * pl.col("marginal_cost")).alias("cost_allocated"),
        # Embed a validation signal: flag rows where the join brought nulls
        pl.col("marginal_cost").is_null().alias("_mc_null"),
    )
    .group_by("bldg_id")
    .agg(
        pl.col("cost_allocated").sum(),
        pl.col("_mc_null").sum().alias("_n_mc_null"),
    )
)

# Stream to disk — never holds the 100 GB join in memory
allocated.sink_parquet("output.parquet")

# Post-hoc validation: read just the check column
checks = pl.scan_parquet("output.parquet").select(
    pl.col("_n_mc_null").sum(),
).collect()

if checks["_n_mc_null"].item() > 0:
    Path("output.parquet").unlink()
    raise ValueError("Some hours had no marginal cost after join")
```

**Why this works:** The entire pipeline is one lazy plan that streams end to
end. No `.collect()` in the middle, so no out-of-memory risk from the 100 GB
intermediate. The validation signal (`_mc_null`) rides along as a column and
is checked after streaming completes. One read of each source.

**What would go wrong with mid-pipeline asserts:**

```python
# BAD: this would force the 100 GB join into memory
joined_df = loads.join(costs, ...).collect()  # OOM!
assert joined_df["marginal_cost"].null_count() == 0
```

You'd either crash with an out-of-memory error or (on a big-enough machine)
waste time materializing data you don't need to hold.

### Example 2: Small-data script with scattered collects (the 24-reads trap)

This is a simplified version of a real script in this repo. It reads 4 small
bill CSVs from S3 and a metadata parquet, joins them, validates, computes a
weighted median, and plots the result.

The **wrong** way (what we actually had — 24 file reads):

```python
def join_and_validate(delivery_lf, supply_lf, fixed):
    supply_renamed = supply_lf.select(...)
    joined = delivery_lf.join(supply_renamed, on=keys, how="inner")

    # Assert 1: check join preserved all keys (3 collects!)
    left_keys = delivery_lf.select(keys).unique().collect()   # reads delivery CSV
    right_keys = supply_renamed.select(keys).unique().collect() # reads supply CSV
    joined_check = joined.select(keys[0]).collect()  # reads BOTH CSVs (re-executes join)
    assert left_keys.height == right_keys.height == joined_check.height

    # Assert 2: check weights match (1 collect)
    weights = joined.select("weight", "weight_supply").collect()  # reads BOTH CSVs again
    assert (weights["weight"] - weights["weight_supply"]).abs().max() < 1e-9

    return joined.with_columns(...)  # returns LazyFrame — no collect here


def median_components(delivery_lf, supply_lf, metadata_lf, fixed):
    components = join_and_validate(delivery_lf, supply_lf, fixed)
    fossil = components.filter(...).join(metadata_lf, ...)

    # Assert 3: check non-empty (1 collect)
    fossil.collect()  # reads delivery + supply + metadata; result thrown away!
    assert ...

    return weighted_median(fossil)  # returns LazyFrame


# In main:
median_current = median_components(delivery_lf, supply_lf, meta_lf, fixed)
median_hp = median_components(delivery_hp_lf, supply_hp_lf, meta_lf, fixed)

# Final collect for the chart
chart_data = pl.concat([median_current, median_hp]).collect()
# ^^^ re-reads ALL inputs from scratch (both scenarios end-to-end)
```

**What happened:** Each assert collected independently, throwing away the
result. Then `weighted_median` returned a LazyFrame built on the same inputs.
When the chart finally collected that LazyFrame, it re-read everything. Per
scenario: delivery CSV read 5x, supply CSV read 5x, metadata read 2x. Total
across both scenarios: 24 file reads for 5 distinct files.

The **right** way (5 file reads):

```python
def join_and_validate(delivery_lf, supply_lf, fixed):
    # Collect each input ONCE
    delivery_df = delivery_lf.collect()
    supply_df = supply_lf.select(...).collect()

    # Join eagerly — no I/O, data is already in memory
    joined = delivery_df.join(supply_df, on=keys, how="inner")

    # All asserts run on DataFrames — zero additional reads
    assert delivery_df.select(keys).unique().height == supply_df.select(keys).unique().height
    assert joined.height == delivery_df.select(keys).unique().height
    assert (joined["weight"] - joined["weight_supply"]).abs().max() < 1e-9

    return joined.with_columns(...)  # returns DataFrame


def median_components(delivery_lf, supply_lf, metadata_df, fixed):
    components = join_and_validate(delivery_lf, supply_lf, fixed)  # collects bills inside
    fossil = components.filter(...).join(metadata_df, ...)  # metadata already a DataFrame
    assert not fossil.is_empty()
    return weighted_median(fossil)  # returns DataFrame


# In main:
metadata_df = meta_lf.collect()  # collect metadata ONCE, share across scenarios

median_current = median_components(delivery_lf, supply_lf, metadata_df, fixed)
median_hp = median_components(delivery_hp_lf, supply_hp_lf, metadata_df, fixed)

# No final collect needed — median_current and median_hp are already DataFrames
chart_data = pl.concat([unpivot(median_current), unpivot(median_hp)])
```

**What changed:** Each source is collected exactly once. Metadata is collected
in `main` and passed as a DataFrame to both scenario calls. The join function
collects each bill LazyFrame once, validates on the materialized data, and
returns a DataFrame. Everything downstream is eager — no surprise re-reads.

## Key takeaways

1. **A LazyFrame is a recipe, not a result.** Every `.collect()` re-executes
   the full plan from scratch, re-reading all source files.

2. **Avoid multiple collects on the same (or overlapping) query plans.** This
   is the most common mistake. If you need to validate AND use the data,
   collect once and do both on the DataFrame.

3. **Default to lazy, collect once at a strategic boundary.** The lazy
   pipeline gets you query optimization (filter/projection pushdown). The
   single collect is where you cross from "recipe" to "data." Put your
   asserts right after that boundary.

4. **Streaming and asserts don't mix.** If data is too large to collect, you
   can't assert mid-pipeline. Embed validation columns and check post-hoc,
   or accept that you're trading validation for memory efficiency.

| Scenario                                      | Strategy                                       | Source reads                         | Streams?        |
| --------------------------------------------- | ---------------------------------------------- | ------------------------------------ | --------------- |
| Small data, asserts needed                    | Collect once, validate, process eagerly        | 1 per source                         | No (not needed) |
| Large data, no asserts                        | Fully lazy, single collect or sink             | 1 per source                         | Yes             |
| Large data, asserts needed                    | Embed checks in output + post-hoc validate     | 1 per source (+ 1 re-scan of output) | Yes             |
| Small data, scattered collects (anti-pattern) | Multiple asserts each collecting independently | N+1 per source                       | No              |

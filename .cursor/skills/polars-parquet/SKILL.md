---
name: polars-parquet
description: >-
  Read and write Parquet with Polars in rate-design-platform, including
  ResStock load curves and other Hive datasets on S3. Use when scanning or
  collecting Parquet, joining LazyFrames, reading from s3://, choosing local
  disk vs S3, filtering buildings, or writing a query against hourly, monthly,
  or master-table Parquet.
---

# Polars and Parquet

Apply this before writing a Parquet query. The long versions of these rules are [laziness and validation](../../../context/code/data/polars_laziness_and_validation.md) and [local disk vs S3](../../../context/code/data/parquet_reads_local_vs_s3.md). Open one of those only for a case this file does not decide.

## Output

A query that has been checked against a real file, plus the schema and a few rows printed before the query is written. One source file is read once.

## Procedure

1. **Inspect one real file first.** Print `schema` and a few rows from one Parquet file at the path you will query. Do not invent column names from a path, a sibling script, or this skill. If `context/docs/` has a dictionary for that dataset (ResStock, Cambium, PUMS), use it and say so when the file disagrees with the dictionary.
2. **Pick the read from the layout**, using the table below.
3. **Stay lazy until one boundary.** Build the plan with `scan_parquet` and lazy ops. Collect once, then assert and continue on the DataFrame. A second `.collect()` on the same plan reads every source again.
4. **Only a LazyFrame has `.collect()`.** A DataFrame from `read_parquet`, `.collect()`, or a join does not. Aggregate a DataFrame with `.group_by().agg()` directly.

## Which read to use

S3 costs about 50–100 ms per GET, whatever the file size. ResStock hourly loads are one file per building (`{bldg_id}-{upgrade}.parquet`, upgrade not zero-padded, under `upgrade=00` which is zero-padded). New York upgrade 00 is about 34,000 files. Scanning that directory on S3 spends roughly half an hour probing footers.

| Layout                                                                        | Where                                         | Read                                                                                           |
| ----------------------------------------------------------------------------- | --------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| One file per building (ResStock hourly or monthly loads)                      | Local disk                                    | `scan_parquet` on the directory, then filter. File count is cheap locally.                     |
| One file per building                                                         | S3, whole state                               | `aws s3 sync` to disk, then scan the local directory.                                          |
| One file per building                                                         | S3, one utility or an explicit `bldg_id` list | Load building ids from metadata, build exact paths, pass that list to `scan_parquet`.          |
| Hive dataset with a file per partition (master tables, Cambium, HUD, EIA-861) | S3 or local                                   | `scan_parquet` on the partition root. Filter on Hive columns. Do not pre-list files with s3fs. |

`bldg_id` is in the filename, not a Hive column. `scan_parquet(s3_dir).filter(bldg_id.is_in(...))` still probes every file in the directory.

```python
base = f"{load_root}/state={state}/upgrade={upgrade}"  # upgrade is "00", "02", ...
paths = [f"{base}/{bldg_id}-{int(upgrade)}.parquet" for bldg_id in bldg_ids]
lf = pl.scan_parquet(paths)
```

On local disk, constructing paths saves about a second. Prefer the directory scan there.

## Joins

Default `coalesce=True` keeps the left join key and drops the right. Pass `coalesce=False` only when both key columns must remain.

## Collect and checks

Collect once at the validation boundary, then assert on the DataFrame:

```python
df = (
    pl.scan_parquet(path)
    .filter(...)
    .select(...)
    .collect()
)
assert df.height > 0
```

Use `sink_parquet` when the result does not fit in memory. Streaming has no mid-plan asserts: write a check column into the output and validate that column after the sink. An aggregate `.collect()` used only for an assert still re-executes the whole plan.

## Leave the long docs closed unless

- A streaming pipeline must embed checks in the output: [laziness doc](../../../context/code/data/polars_laziness_and_validation.md), "Large data where streaming matters".
- You are changing how a whole-state S3 dataset is packaged (many small files vs few large files): [local vs S3](../../../context/code/data/parquet_reads_local_vs_s3.md), use case 1.

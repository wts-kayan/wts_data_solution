# Goal — remove the spurious nested `runid=` partition level in `dbprojection.term_structure`

## Context

- Platform: CDP Cloudera, HDFS (`hahdfsnameservice`), Hive external ORC table, Spark 3.5.x, Dataiku notebooks.
- Table: `dbprojection.term_structure`
- HDFS location:
  `hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure`

## Problem observed

An `hdfs dfs -ls -R` on the table location shows a **double partition level**:

```
.../term_structure/runId=<uuid>/runid=<uuid>/part-*.orc
```

Instead of the expected single level:

```
.../term_structure/runId=<uuid>/part-*.orc
```

The inner directory uses the lowercase spelling `runid=` while the outer one uses `runId=`.
In the observed samples the UUID value is **identical** on both levels, i.e. the inner directory
is a pure duplication of the partition key, not a legitimate sub-partition.

Consequences:
- Hive sees the table as partitioned by one column only, so the ORC files are one directory
  deeper than the metastore expects → partitions register as empty, `MSCK REPAIR TABLE` fails or
  returns nothing, and reads return 0 rows for those partitions.
- Spark partition discovery may also infer a second, unwanted partition column.

## Suspected root cause (to confirm)

A writer applies `partitionBy` twice, or writes into an already partition-qualified path, e.g.:

```scala
df.write.partitionBy("runid").orc(s"$tablePath/runId=$runId")
```

The base path already carries `runId=<uuid>`, and `partitionBy("runid")` adds a second level.
The fix must be applied at the source, otherwise the anomaly reappears at the next run.

## Target outcome

1. **Cleanup (data layer)** — for every `runId=<uuid>` directory containing a nested
   `runid=<uuid>` directory:
   - move the content of the nested directory up into its parent `runId=<uuid>` directory
     (HDFS `rename` = metadata-only, no data copy);
   - delete the now-empty nested directory recursively.
   - Abort / flag manually any case where the nested UUID differs from the parent UUID.
2. **Metastore** — re-register partitions after the move
   (`MSCK REPAIR TABLE dbprojection.term_structure`, or preferably explicit
   `ALTER TABLE ... ADD IF NOT EXISTS PARTITION (runid='<uuid>') LOCATION '...'` as a
   deterministic pipeline step).
3. **Source fix** — locate the writer that produces the double level and remove the redundant
   `partitionBy`, so that either:
   - the writer writes to the table root with `partitionBy("runid")`, **or**
   - the writer writes directly to `$tablePath/runId=$runId` with **no** `partitionBy`.
   Also unify the case of the partition column (`runid` vs `runId`) between the writer, the Hive
   DDL and the readers.
4. **Verification** — `spark.read.orc(tablePath)` returns the expected schema (single `runId`
   partition column), row counts per `runId` are non-zero, and a re-listing shows no remaining
   `runid=` directory nested under a `runId=` directory.

## Constraints

- Destructive operation on a UAT HDFS path → the script must have a `dryRun` switch, print a full
  report (number of nested dirs, file count, size, UUID match) before doing anything, and must be
  idempotent / re-runnable.
- Only directories whose name starts with the exact lowercase prefix `runid=` and whose parent
  starts with `runId=` are eligible. Case-sensitive matching is mandatory.
- No `spark.read` / `spark.write` for the cleanup itself — use the Hadoop `FileSystem` API only,
  so nothing is rewritten or recompressed.

## Deliverables

- `cleanup_nested_runid_partitions.scala` — notebook script performing detection + fix + verification.
- A patch on the upstream writer removing the redundant partitioning.
- The partition-registration step added to the Oozie pipeline.

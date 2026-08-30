# Brief — Flatten double-nested `runId=/runid=` partitions (Dataiku PySpark)

**Target for:** Claude Code
**Deliverable:** one PySpark recipe / notebook cell runnable in Dataiku DSS (Spark-Scala kernel unavailable — use PySpark), dry-run by default, idempotent.

---

## 1. Context

Cluster: Cloudera CDP 7.1.9, HDFS `hahdfsnameservice`, Hive metastore, Spark 3.5.4.

Table root:
```
hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure
```
Same defect exists on `dbprojection.db/term_structure_detailed`. The script must take the table root as a parameter.

## 2. Observed defect

Two coexisting layouts under the table root:

**Correct (canonical, lowercase):**
```
term_structure/runid=44dcb489-9268-49c1-b993-eac7bcad442a/<part files>
term_structure/runid=55fe6899-8a7b-4fc4-a6c2-61a4cbce9218/<part files>
```

**Broken (double-nested, outer key uppercase `I`):**
```
term_structure/runId=cbe72414-706f-4bf3-b108-58f1f70da55b/runid=cbe72414-706f-4bf3-b108-58f1f70da55b/<part files>
term_structure/runId=7f936af4-171d-41d4-871c-b18b51a48ed9/runid=7f936af4-171d-41d4-871c-b18b51a48ed9/
term_structure/runId=cc843152-2de8-4125-a887-306674814388/runid=cc843152-2de8-4125-a887-306674814388/
term_structure/runId=952dc92d-2133-43a3-9d6e-a86a382d435b/runid=952dc92d-2133-43a3-9d6e-a86a382d435b/
term_structure/runId=0f220e04-25ff-42b4-ae3d-9ba6566abe00/runid=0f220e04-25ff-42b4-ae3d-9ba6566abe00/
term_structure/runId=01e06330-c2f1-4418-b1cb-b40af98fc0a8/runid=01e06330-c2f1-4418-b1cb-b40af98fc0a8/
term_structure/runId=8dffa840-c6a7-4e9c-b42a-a20122c7cb4d/runid=8dffa840-c6a7-4e9c-b42a-a20122c7cb4d/
```
(list above is what was visible on screen — **do not hardcode it**, discover at runtime)

Root cause (already diagnosed, not in scope here): the writer builds the path with `runId=` and then also calls `.partitionBy("runid")` on an already-qualified path.

Consequence: `spark.read.orc(<table root>)` fails / mixes partition depths — Spark sees inconsistent directory structures (depth 1 vs depth 2, and two distinct column names `runId` / `runid`).

## 2bis. Current metastore definition (verbatim `SHOW CREATE TABLE`)

```sql
CREATE EXTERNAL TABLE `term_structure`(
`matrixmigrationname` string,
`asofdate` string,
`scenario` string,
`notationcode` string,
  `matrix` array<array<double>>)
PARTITIONED BY (
`runid` string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure'
TBLPROPERTIES (
'bucketing_version'='2',
'spark.sql.create.version'='3.3.2.3.3.7190.2-1',
'spark.sql.partitionProvider'='catalog',
'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"matrixMigrationName","type":"string","nullable":true,"metadata":{}},{"name":"asOfDate","type":"string","nullable":true,"metadata":{}},
{"name":"scenario","type":"string","nullable":true,"metadata":{}},
{"name":"notationCode","type":"string","nullable":true,"metadata":{}},{"name":"matrix","type":{"type":"array","elementType":{"type":"array","elementType":"double","containsNull":true},"containsNull":true},"nullable":true,"metadata":{}},
{"name":"runid","type":"string","nullable":true,"metadata":{}}]}',
'spark.sql.sources.schema.numPartCols'='1',
'spark.sql.sources.schema.partCol.0'='runid',
'transient_lastDdlTime'='1777022339')
```

**Transcription flags (OCR):** `'spark.sql.create.version'='3.3.2.3.3.7190.2-1'` — the CDP build string is low-confidence; `7190` may be `7180`. `transient_lastDdlTime` digits are low-confidence. Neither value is used by the script.

### What this confirms — the script must rely on these facts

- **`EXTERNAL` table.** `ALTER TABLE ... DROP PARTITION` will remove the metastore entry only and leave the HDFS files intact. Partition drop/re-add in §5 is therefore non-destructive. Verify this is still true at runtime (`DESCRIBE FORMATTED` → `Table Type: EXTERNAL_TABLE`) and **abort** if the table is `MANAGED_TABLE`.
- **`'spark.sql.partitionProvider'='catalog'`.** Spark does *not* discover partitions from the directory tree for this table — it reads them from the metastore. Consequences:
  - explicit `ALTER TABLE ... ADD PARTITION ... LOCATION` is mandatory; `MSCK REPAIR` and filesystem discovery are not options;
  - a partition can be registered at a `runId=<uuid>/runid=<uuid>` location even though the layout is malformed, which is why the table "worked" until now;
  - the read error surfaced only via `spark.read.orc(<table root>)` (§2), which *does* do path-based discovery and chokes on mixed depth / mixed key casing.
- **Single partition column, lowercase: `PARTITIONED BY (runid string)`, `numPartCols=1`, `partCol.0=runid`.** So `runid` (all lowercase) is the canonical form and `runId=` on disk is unambiguously the writer defect. No case ambiguity to resolve — flatten toward lowercase.
- **`spark.sql.sources.schema` carries camelCase data columns** (`matrixMigrationName`, `asOfDate`, `notationCode`) while the Hive columns are lowercase. This is normal Spark-on-Hive behaviour — **do not "fix" it**, and do not rewrite the table definition. The script touches partitions only, never the table schema or TBLPROPERTIES.
- **`matrix array<array<double>>`.** Nested-array ORC column: do not attempt any read/rewrite of file contents. The remediation is pure HDFS metadata movement (`rename`) plus metastore DDL — no Spark job reads or rewrites these files.

### Extra pre-flight check to add to the script

Before mutating anything, list the registered partitions and their locations:

```python
parts = spark.sql(f"SHOW PARTITIONS {HIVE_TABLE}").collect()
# for each: DESCRIBE FORMATTED {HIVE_TABLE} PARTITION (runid='<X>') -> Location
```

Classify each registered partition as:
- `OK` — location is `<root>/runid=<X>` and exists on HDFS;
- `NESTED` — location points inside a `runId=<X>/runid=<X>` branch → must be re-pointed after flattening;
- `ORPHAN` — location does not exist on HDFS → drop (report only in dry run).

Report the three buckets before touching HDFS, and cross-check the count against the directory scan. If a wrapper found on disk has no corresponding metastore partition, flatten it and `ADD PARTITION` it as new — but flag it loudly, since it means a run wrote data that was never registered.

## 3. Required behaviour

**Do NOT simply delete the nested branch** — the data files only exist at the inner leaf. Deleting `runId=<uuid>/` destroys that run. The fix is *flatten then delete the empty wrapper*:

For each directory matching `runId=<X>` (case-sensitive match on the uppercase `I`, or more generally: any first-level dir whose key is not exactly `runid`):

1. Assert it contains exactly one child directory and that child is `runid=<Y>`.
2. Assert `X == Y` (case-insensitive UUID compare). If not, **skip and report** — never guess.
3. Determine target `term_structure/runid=<X>` (canonical lowercase).
   - If target does not exist → `fs.rename(inner, target)` (single atomic HDFS rename, cheapest path).
   - If target already exists → move files one by one into it, renaming on collision (prefix with `merged_<shortuuid>_`), then delete the inner dir.
4. Delete the now-empty wrapper `runId=<X>` (`fs.delete(path, recursive=True)` only after verifying it is empty).
5. Never touch files named `_SUCCESS`, `.hive-staging*`, `_temporary`, or anything starting with `.` — list them in the report instead.

Edge cases to handle explicitly and report rather than crash on:
- wrapper containing data files directly *and* a nested `runid=` dir
- wrapper containing more than one nested dir
- nested dir empty
- already-flattened tree (script must be a no-op → idempotency requirement)

## 3bis. Existing Dataiku notebook bootstrap (verbatim, cell `In [1]`)

The notebook currently in use is a **Spark-Scala** kernel. Its first cell is the Dataiku boilerplate:

```scala
import com.dataiku.dss.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
val sparkConf    = DataikuSparkContext.buildSparkConf()
val sparkContext = new SparkContext(sparkConf)
val spark        = SparkSession.builder().config(sparkConf).getOrCreate()
val sqlContext   = spark.sqlContext
val dkuContext   = DataikuSparkContext.getContext(sparkContext)
```

Returns:
```
Out[1]: com.dataiku.dip.spark.notebook.SparkScalaNotebookEntryPoint$$anon$1@45c547e
```

The stock cell `In [2]` (`dkuContext.getDataFrame(sqlContext, "mydataset")`) throws `APIError$APIErrorException: Dataset not found (UTILITIES_ENGINE . mydataset)` — that is just the unedited placeholder from the Dataiku template and is **not** related to the partition problem. Ignore it; the remediation does not use `dkuContext` or any Dataiku dataset at all.

### Kernel decision — read this before writing code

The request was for PySpark, but the live notebook is Spark-Scala. Produce **both**, with the Python version as the primary deliverable:

- `flatten_nested_runid_partitions.py` — for a **Python (PySpark)** notebook or recipe. Bootstrap:
  ```python
  from pyspark import SparkContext
  from pyspark.sql import SparkSession
  sc    = SparkContext.getOrCreate()
  spark = SparkSession.builder.getOrCreate()
  ```
  No `dataiku` / `dkuspark` imports needed — nothing here reads a DSS dataset.

- `flatten_nested_runid_partitions.scala` — a **drop-in cell 2** for the existing Spark-Scala notebook, reusing the `spark` / `sparkContext` vals already bound by the cell above. Do not re-create the SparkSession.

Both must implement identical logic and produce the same report and the same `.sql` output.

### Hadoop FileSystem handle per kernel

Python (through the JVM gateway):
```python
jvm  = spark._jvm
conf = spark._jsc.hadoopConfiguration()
Path = jvm.org.apache.hadoop.fs.Path
fs   = jvm.org.apache.hadoop.fs.FileSystem.get(
           jvm.java.net.URI.create(TABLE_ROOT), conf)
```

Scala (native, in the existing notebook):
```scala
import org.apache.hadoop.fs.{FileSystem, Path}
val conf = sparkContext.hadoopConfiguration
val fs   = FileSystem.get(new java.net.URI(TABLE_ROOT), conf)
```

Note the Scala version returns real `FileStatus` objects, so `listStatus`/`isDirectory`/`getPath.getName` are direct; the Python version goes through Py4J and every returned object is a Java proxy — call `.getPath().getName()` and wrap in `str()` before any Python string comparison.

## 4. Implementation constraints

- **Dataiku PySpark recipe or notebook.** Get the FS handle through the JVM gateway, not through `subprocess`/`hdfs dfs`:
  ```python
  jvm  = spark._jvm
  jsc  = spark._jsc
  conf = jsc.hadoopConfiguration()
  Path = jvm.org.apache.hadoop.fs.Path
  fs   = jvm.org.apache.hadoop.fs.FileSystem.get(
             jvm.java.net.URI.create(TABLE_ROOT), conf)
  ```
- No `dbutils`, no `os.path`, no local FS assumptions.
- Module-level config block at the top:
  ```python
  TABLE_ROOT = "hdfs://hahdfsnameservice/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db/term_structure"
  DRY_RUN    = True          # must default to True
  HIVE_TABLE = "dbprojection.term_structure"
  EMIT_HIVE_DDL = True
  ```
- **DRY_RUN=True must perform zero mutations** and print the full planned action list (`RENAME src -> dst`, `MERGE n files -> dst`, `DELETE dir`).
- Structured console report at the end: counts of wrappers found / flattened / merged / skipped, plus a per-skip reason table.
- Log every action with the full path. This runs on UAT data that has no backup.
- Python 3, PySpark 3.5.x API. No third-party deps.

## 5. Hive re-registration (second half of the script)

After flattening, the metastore still points at the old locations. On CDP, `MSCK REPAIR TABLE` is unreliable — **generate explicit DDL**.

For each flattened run id, emit (and execute only when `DRY_RUN is False`):

```sql
ALTER TABLE dbprojection.term_structure
  DROP IF EXISTS PARTITION (runid='<X>');

ALTER TABLE dbprojection.term_structure
  ADD IF NOT EXISTS PARTITION (runid='<X>')
  LOCATION 'hdfs://hahdfsnameservice/.../term_structure/runid=<X>';
```

Also detect and report **stale metastore partitions** whose `LOCATION` no longer exists on HDFS (query via `spark.sql(f"SHOW PARTITIONS {HIVE_TABLE}")` + `DESCRIBE FORMATTED`, or catalog API) — list them, drop them only when not in dry run.

Write the generated DDL to a `.sql` file as well so it can be reviewed / replayed by hand.

## 6. Validation step

At the end, when `DRY_RUN is False`, run and print:

```python
spark.read.orc(TABLE_ROOT).select("runid").distinct().count()
spark.sql(f"SELECT runid, count(*) FROM {HIVE_TABLE} GROUP BY runid").show(100, False)
```

and assert that no first-level directory under `TABLE_ROOT` matches `runId=` (uppercase) any more.

## 7. Out of scope

- Fixing the writers (`str-bigData`, `str_projection_engine`, `str_sicr3_input_engine`, `str_file_transform_engine`) — tracked separately. The permanent fix is: align the column casing to `runid` and remove the redundant `partitionBy` on an already-qualified path.

## 8. Deliverables

1. `flatten_nested_runid_partitions.py` — the PySpark version (primary).
2. `flatten_nested_runid_partitions.scala` — drop-in cell 2 for the existing Spark-Scala notebook (§3bis), same logic, same output.
3. `generated_partition_ddl.sql` — produced at runtime by whichever version is run.
4. Short README section at the top of each file explaining: run with `DRY_RUN=true`, review, then flip to `false`.

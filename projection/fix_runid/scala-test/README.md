# Compile + test harness for the projection Scala cells

The three `.scala` scripts in the parent directory are Dataiku notebook **cells**:
a flat sequence of top-level statements. `scalac` cannot compile that — Scala 2
only allows definitions at the top level of a file — so until now the only way
to find a syntax error in them was to paste one into a live notebook and read
the stack trace. Two compile errors reached the cluster that way.

This harness compiles and runs them offline, in about a minute.

## Run it

```bash
cd projection/fix_runid/scala-test
mvn -o test
```

That is the whole command: wrapper generation is bound to `generate-sources`,
so the cells are re-wrapped from source on every run. It used to be a separate
`python generate_wrappers.py` step, which made `mvn -o test` on its own compile
and test the wrappers left over from the previous run -- an edit to a cell
would silently report green. `./run.sh` and `run.ps1` still work and do the
same thing. Pass `-Dpython.exe=python3` if `python` is not the interpreter on
PATH.

`mvn -o` is **offline**: every dependency and plugin version in `pom.xml` was
chosen because it is already in the local `~/.m2`. No network access is needed
or attempted.

Compile only, when that is all you want:

```bash
mvn -o compile
```

## How the wrapping works

`generate_wrappers.py` reads each cell and emits

```scala
object FlattenCell {
  def run(spark: SparkSession, cfg: Map[String, String] = Map.empty): Unit = {
    val sparkContext = spark.sparkContext     // as Dataiku's cell 1 binds it
    <the cell body, verbatim>
  }
}
```

which *is* compilable: Scala allows `import`, `def`, `val` and even `case class`
inside a method body. The generated files land in `src/main/scala/generated/`
and are overwritten on every run — never edit them, edit the cell.

Each CONFIG value is rewritten into a lookup that keeps the original literal as
its default:

```scala
val DRY_RUN    = true             ->  cfg.getOrElse("DRY_RUN", "true").toBoolean
val TABLE_ROOT = "/Projects/..."  ->  cfg.getOrElse("TABLE_ROOT", "/Projects/...")
```

so a test can point a cell at a temp directory without editing the script, and
a wrapper run with an empty `cfg` behaves exactly like the committed cell.

## What the tests cover

`src/main/scala/com/bnp/runid/CellTests.scala` starts a **real** local Spark
with Hive support (throwaway derby metastore), writes **real** ORC files,
registers **real** external partitions, then runs the cells and asserts on the
resulting tree and metastore.

| Cell | Checks |
|---|---|
| flatten | dry run changes nothing; `runId=X/runid=X` collapses to `runId=X` in one pass and the metastore is re-pointed; second apply is a no-op; a UUID mismatch is skipped, never merged; a name collision on merge keeps both files; markers stay put and their dir survives; `DELETE_MARKERS_ON_MERGE=true` clears it; a marker-only nested dir is left and named by default, removed with the flag, and never dropped if data appeared in it; a leftover is cleared by a re-run with the flag on; a surviving nested dir is named in the report |
| rename | `runid=X` becomes `runId=X` and is re-pointed; a still-nested dir is refused; `MERGE_ON_COLLISION=false` refuses a colliding target; `MERGE_ON_COLLISION=true` loses no file, colliding or not |
| recreate | dry run touches nothing; `alter` mode refuses to execute and leaves the table alone; `recreate` mode really lands `runId` in Spark's schema; aborts on MANAGED; aborts once, with diagnostics, on an invisible table; the backup replays the pre-drop definition; every partition is re-registered, not just the first; a nested `array<array<double>>` column survives |
| fix-all (batch) | only tables partitioned by `runid` are in scope; dry run renames and recreates nothing; both phases run over every in-scope table; a nested table is skipped by both phases and reported; `DO_RENAME=false` still refuses to recreate a nested table; a second run is a clean no-op; `ONLY_TABLES` limits the batch; the batch lands where the two single-table cells land |
| unpurge-drop | dry run mutates nothing; a non-partitioned table is never altered nor dropped; a MANAGED table is reported and never dropped; the purge flag is flipped and verified BEFORE the drop and the data directory survives; a table already at `false` is dropped without an ALTER; the artefacts name every table on the right side; a second run is a clean no-op; an unwritable DDL backup aborts before any change |

The nested-column test uses the real `term_structure` shape rather than the
four plain strings the other fixtures use. Production carries `matrix
array<array<double>>`, and `recreate` rebuilds the table from a schema JSON in
which a nested type has to be a nested JSON object, not the DDL string. It is
the one column shape whose failure would land *after* the `DROP`.

### The leftover `runid=` directory

A nested `runid=` dir can outlive a flatten in two ways, and both are covered:

- it held data **and** a marker, so the data was promoted but `_SUCCESS` kept
  the directory alive (`DELETE_MARKERS_ON_MERGE` is `false` by default);
- it held **only** markers, so the merge path never ran at all.

The second used to be reported as "remove manually" and the first was not
reported at all -- the validation scan only looked at *first-level* dirs with a
non-canonical key, so `runId=X/runid=X` was invisible to it and the run read as
a clean success. The validation now re-scans the final tree for any nested
`runId=` dir and names what it holds, and `DELETE_MARKERS_ON_MERGE = true`
removes marker-only dirs as well as markers on a merge. That branch is the only
one that deletes a non-empty directory, so it re-lists the directory
immediately before deleting and abandons the delete if anything that is not a
marker has appeared.

`fix_all_runid_tables.scala` re-implements the rename and recreate algorithms
so they can be driven over many tables in one pass, which means they can drift
from the single-table cells. The last test above is the guard against that: it
runs the batch on one table and the two single-table cells on an identical one,
then asserts both land in the same state -- same directory layout, same Spark
schema, same partition count, same rows. File *names* are normalised first,
since Spark gives every ORC file its own random `part-<uuid>`.

The merge tests are the ones that matter most, because merging is the only
place the cells can lose data: files are moved into a directory that already
holds files, and an HDFS rename onto an existing name fails. The cell renames
the incoming file to `merged_<8hex>_<name>` instead. Those tests therefore
assert on file *contents* -- real ORC written under a chosen name, carrying a
tag that says which directory it came from -- rather than on a file count, and
they read the partition back through the table afterwards.

The local filesystem stands in for HDFS. That is faithful for what the cells do
— they only ever go through the Hadoop `FileSystem` API, the same code path for
`file://` and `hdfs://`.

## Environment notes (Windows)

All 40 tests run on Windows. Two things had to be arranged for that, both
handled automatically:

**Case sensitivity.** Telling `runId=` from `runid=` is the entire point, and
NTFS is case-insensitive by default. The harness turns case sensitivity on for
each temp root with `fsutil file setCaseSensitiveInfo <dir> enable` (no admin
needed; child directories inherit it). If that ever fails, the probe reports it
and the case-mixing tests SKIP rather than assert something false.

**Hadoop native IO.** Hive creating a directory in the warehouse calls
`HdfsUtils.setFullFileStatus` -> `getGroup()` -> `NativeIO$POSIX.stat`, a symbol
the Windows `hadoop.dll` does not export. Installing winutils does not fix it.
Nor can the DLL simply be left unloaded: without it even
`RawLocalFileSystem.listStatus` fails in `NativeIO$Windows.access0`. The session
therefore sets `hive.warehouse.subdir.inherit.perms=false`, which skips the
permission-inheritance call altogether. No effect on Linux.

A skip is never counted as a pass, and the exit code is non-zero only on a real
failure.

## What this harness already caught

Worth stating, because it is the argument for keeping it:

1. **An unterminated string literal** in `recreate_table_partcol_runid.scala` —
   the exact compile error that reached the cluster.
2. **`pathOnly` mishandling `scheme:/path`.** Hive returns locations both as
   `hdfs://nameservice/a/b` and as `file:/a/b`. Only the first was parsed, so a
   partition was misclassified as `OUTSIDE the table root` and silently left
   out of the re-registration.
3. **Spark refuses `spark.sql.*` table properties.** `ALTER TABLE ... SET
   TBLPROPERTIES ('spark.sql.sources.schema.partCol.0'='runId')` fails with
   *"table property keys may not start with 'spark.sql.'"*. That invalidated
   the original design of **both** recreate modes — and in `recreate` mode the
   `CREATE` would have failed *after* the `DROP`, leaving the table gone. The
   fix is to create a datasource table (`USING ORC` + `PARTITIONED BY`) and let
   Spark persist the properties itself.

## Layout

```
scala-test/
  pom.xml                  offline build, versions pinned to what is in ~/.m2
  generate_wrappers.py     cells -> compilable objects
  run.sh / run.ps1         thin wrappers around mvn -o test
  src/main/scala/
    generated/             GENERATED, do not edit
    com/bnp/runid/CellTests.scala
```

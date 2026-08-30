# Compile + test harness for the `fix_runid` Scala cells

The three `.scala` scripts in the parent directory are Dataiku notebook **cells**:
a flat sequence of top-level statements. `scalac` cannot compile that — Scala 2
only allows definitions at the top level of a file — so until now the only way
to find a syntax error in them was to paste one into a live notebook and read
the stack trace. Two compile errors reached the cluster that way.

This harness compiles and runs them offline, in about a minute.

## Run it

```bash
cd projection/fix_runid/scala-test
python generate_wrappers.py     # wrap the cells into compilable objects
mvn -o test                     # compile + run
```

or the one-liner: `./run.sh` (Git Bash / Linux) or `powershell -File run.ps1`.

`mvn -o` is **offline**: every dependency and plugin version in `pom.xml` was
chosen because it is already in the local `~/.m2`. No network access is needed
or attempted.

Compile only, when that is all you want:

```bash
python generate_wrappers.py && mvn -o compile
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
| flatten | dry run changes nothing; `runId=X/runid=X` collapses to `runId=X` in one pass and the metastore is re-pointed; second apply is a no-op; a UUID mismatch is skipped, never merged |
| rename | `runid=X` becomes `runId=X` and is re-pointed; a still-nested dir is refused |
| recreate | dry run touches nothing; `alter` mode refuses to execute and leaves the table alone; `recreate` mode really lands `runId` in Spark's schema; aborts on MANAGED; aborts once, with diagnostics, on an invisible table |

The local filesystem stands in for HDFS. That is faithful for what the cells do
— they only ever go through the Hadoop `FileSystem` API, the same code path for
`file://` and `hdfs://`.

## Two environment limits, reported as SKIP not PASS

Both are limits of *this machine*, not of the scripts. The runner prints why.

**Case sensitivity.** Windows and default macOS collapse `runId=x` and
`runid=x` into one directory; HDFS does not. Tests that need both casings to
coexist are skipped there. Run on any Linux box to exercise them.

**Hadoop native IO.** Hive's directory operations call
`NativeIO.POSIX.stat` for permission info, which needs `winutils.exe` /
`hadoop.dll` on Windows. Without it those tests are skipped. To run them here,
point `HADOOP_HOME` at a winutils distribution for Hadoop 3.x and put
`%HADOOP_HOME%\bin` on `PATH`. On Linux and on the cluster they run normally.

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
  run.sh / run.ps1         generate + mvn -o test
  src/main/scala/
    generated/             GENERATED, do not edit
    com/bnp/runid/CellTests.scala
```
